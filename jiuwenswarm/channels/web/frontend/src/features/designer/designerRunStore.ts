import { create } from 'zustand';
import { generateUuidV4 } from '../../utils/uuid';
import {
  DESIGNER_FAKE_TEXT,
  ensureDesignerFakeAssets,
} from './designerFakeAssets';
import {
  allNodesCompleted,
  computeNextLayer,
  computeRootLayer,
  derivePrimaryAction,
  initialNodeStates,
  markNodesStatus,
  type DesignerRunPrimaryAction,
} from './designerLayerRun';
import {
  DESIGNER_NODE_STATUS_COMPLETED,
  DESIGNER_NODE_STATUS_RUNNING,
  DESIGNER_NODE_TYPE_AUDIO,
  DESIGNER_NODE_TYPE_IMAGE,
  DESIGNER_NODE_TYPE_TABLE,
  DESIGNER_NODE_TYPE_TEXT,
  DESIGNER_NODE_TYPE_VIDEO,
  DESIGNER_RUN_SCHEMA_VERSION,
  DESIGNER_NODE_STATUS_PENDING,
  DESIGNER_RUN_STATUS_CANCELLED,
  DESIGNER_RUN_STATUS_COMPLETED,
  DESIGNER_RUN_STATUS_DRAFT,
  DESIGNER_RUN_STATUS_PAUSED,
  DESIGNER_RUN_STATUS_RUNNING,
  type AssetRef,
  type DesignerExecutionGraph,
  type DesignerExecutionRun,
  type DesignerNodeState,
} from './executionGraphTypes';

type DesignerRunStore = {
  run: DesignerExecutionRun | null;
  nodeStates: Record<string, DesignerNodeState>;
  currentLayerNodeIds: string[];
  isRunning: boolean;
  primaryAction: DesignerRunPrimaryAction;
  boundGraphId: string | null;
  /** Monotonic token so pause/cancel can abort an in-flight fake layer. */
  runGeneration: number;
  resetForGraph: (graph: DesignerExecutionGraph | null) => void;
  getPrimaryAction: (graph: DesignerExecutionGraph | null) => DesignerRunPrimaryAction;
  /** 执行 / 继续 / 重试失败节点 */
  advance: (graph: DesignerExecutionGraph) => Promise<void>;
  /** 重跑当前层 */
  rerunCurrentLayer: (graph: DesignerExecutionGraph) => Promise<void>;
  /** 重新开始：清空后跑第一层 */
  restart: (graph: DesignerExecutionGraph) => Promise<void>;
  /** 暂停当前层执行（mock） */
  pause: () => void;
  /** 取消运行并回到草稿（mock） */
  cancel: (graph: DesignerExecutionGraph | null) => void;
  /** 将上传素材应用到节点预览（标记 completed） */
  applyUploadedOutput: (nodeId: string, outputRef: AssetRef) => void;
  /** 清除某节点因上传产生的预览态 */
  clearUploadedOutput: (nodeId: string) => void;
};

function emptyRun(graph: DesignerExecutionGraph): DesignerExecutionRun {
  const now = Date.now();
  return {
    schema_version: DESIGNER_RUN_SCHEMA_VERSION,
    run_id: `run_${generateUuidV4().replace(/-/g, '').slice(0, 12)}`,
    graph_id: graph.graph_id,
    project_id: graph.project_id,
    status: DESIGNER_RUN_STATUS_DRAFT,
    node_states: initialNodeStates(graph),
    current_node_ids: [],
    created_at: now,
    updated_at: now,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function refreshPrimary(
  graph: DesignerExecutionGraph | null,
  nodeStates: Record<string, DesignerNodeState>,
  currentLayerNodeIds: string[],
  isRunning: boolean,
): DesignerRunPrimaryAction {
  return derivePrimaryAction({
    graph,
    nodeStates,
    currentLayerNodeIds,
    isRunning,
  });
}

export const useDesignerRunStore = create<DesignerRunStore>((set, get) => ({
  run: null,
  nodeStates: {},
  currentLayerNodeIds: [],
  isRunning: false,
  primaryAction: 'execute',
  boundGraphId: null,
  runGeneration: 0,

  resetForGraph: (graph) => {
    if (!graph || graph.nodes.length === 0) {
      set({
        run: null,
        nodeStates: {},
        currentLayerNodeIds: [],
        isRunning: false,
        primaryAction: 'execute',
        boundGraphId: graph?.graph_id ?? null,
        runGeneration: get().runGeneration + 1,
      });
      return;
    }
    const run = emptyRun(graph);
    set({
      run,
      nodeStates: run.node_states,
      currentLayerNodeIds: [],
      isRunning: false,
      primaryAction: 'execute',
      boundGraphId: graph.graph_id,
      runGeneration: get().runGeneration + 1,
    });
  },

  getPrimaryAction: (graph) => {
    const state = get();
    return refreshPrimary(graph, state.nodeStates, state.currentLayerNodeIds, state.isRunning);
  },

  advance: async (graph) => {
    const state = get();
    if (state.isRunning || graph.nodes.length === 0) return;

    let layerIds: string[];
    const primary = refreshPrimary(
      graph,
      state.nodeStates,
      state.currentLayerNodeIds,
      false,
    );

    if (primary === 'retry_failed') {
      layerIds = [...state.currentLayerNodeIds];
    } else if (primary === 'continue') {
      const incompleteCurrent = state.currentLayerNodeIds.filter(
        (nodeId) => state.nodeStates[nodeId]?.status !== DESIGNER_NODE_STATUS_COMPLETED,
      );
      layerIds =
        incompleteCurrent.length > 0
          ? incompleteCurrent
          : computeNextLayer(graph, state.currentLayerNodeIds, state.nodeStates);
    } else if (primary === 'done') {
      // 全部完成后点主按钮：等同重新开始
      await get().restart(graph);
      return;
    } else {
      layerIds = computeRootLayer(graph);
    }

    if (layerIds.length === 0) {
      set({
        primaryAction: allNodesCompleted(graph, state.nodeStates) ? 'done' : 'execute',
      });
      return;
    }

    await runFakeLayer(graph, layerIds, set, get);
  },

  rerunCurrentLayer: async (graph) => {
    const state = get();
    if (state.isRunning || graph.nodes.length === 0) return;
    const layerIds = state.currentLayerNodeIds;
    if (layerIds.length === 0) return;
    await runFakeLayer(graph, layerIds, set, get);
  },

  restart: async (graph) => {
    const state = get();
    if (state.isRunning || graph.nodes.length === 0) return;
    const run = emptyRun(graph);
    set({
      run,
      nodeStates: run.node_states,
      currentLayerNodeIds: [],
      isRunning: false,
      primaryAction: 'execute',
      boundGraphId: graph.graph_id,
      runGeneration: state.runGeneration + 1,
    });
    const layerIds = computeRootLayer(graph);
    if (layerIds.length === 0) return;
    await runFakeLayer(graph, layerIds, set, get);
  },

  pause: () => {
    const state = get();
    if (!state.isRunning) return;
    const layerIds = state.currentLayerNodeIds;
    const nodeStates = markNodesStatus(
      state.nodeStates,
      layerIds,
      DESIGNER_NODE_STATUS_PENDING,
    );
    const run = state.run;
    set({
      run: run
        ? {
            ...run,
            status: DESIGNER_RUN_STATUS_PAUSED,
            node_states: nodeStates,
            current_node_ids: layerIds,
            updated_at: Date.now(),
          }
        : null,
      nodeStates,
      isRunning: false,
      primaryAction: 'continue',
      runGeneration: state.runGeneration + 1,
    });
  },

  cancel: (graph) => {
    const state = get();
    const generation = state.runGeneration + 1;
    if (!graph || graph.nodes.length === 0) {
      set({
        run: null,
        nodeStates: {},
        currentLayerNodeIds: [],
        isRunning: false,
        primaryAction: 'execute',
        boundGraphId: graph?.graph_id ?? null,
        runGeneration: generation,
      });
      return;
    }
    const run = emptyRun(graph);
    set({
      run: {
        ...run,
        status: DESIGNER_RUN_STATUS_CANCELLED,
      },
      nodeStates: run.node_states,
      currentLayerNodeIds: [],
      isRunning: false,
      primaryAction: 'execute',
      boundGraphId: graph.graph_id,
      runGeneration: generation,
    });
  },

  applyUploadedOutput: (nodeId, outputRef) => {
    const state = get();
    const current = state.nodeStates[nodeId] ?? { status: DESIGNER_NODE_STATUS_PENDING };
    const nodeStates = {
      ...state.nodeStates,
      [nodeId]: {
        ...current,
        status: DESIGNER_NODE_STATUS_COMPLETED,
        output_ref: outputRef,
        error: null,
        completed_at: Date.now(),
      },
    };
    const run = state.run
      ? {
          ...state.run,
          node_states: nodeStates,
          updated_at: Date.now(),
        }
      : null;
    set({
      run,
      nodeStates,
    });
  },

  clearUploadedOutput: (nodeId) => {
    const state = get();
    if (!state.nodeStates[nodeId]) return;
    const nodeStates = {
      ...state.nodeStates,
      [nodeId]: {
        ...state.nodeStates[nodeId],
        status: DESIGNER_NODE_STATUS_PENDING,
        output_ref: null,
        error: null,
        completed_at: null,
      },
    };
    const run = state.run
      ? {
          ...state.run,
          node_states: nodeStates,
          updated_at: Date.now(),
        }
      : null;
    set({
      run,
      nodeStates,
    });
  },
}));

async function runFakeLayer(
  graph: DesignerExecutionGraph,
  layerIds: string[],
  set: (
    partial:
      | Partial<DesignerRunStore>
      | ((state: DesignerRunStore) => Partial<DesignerRunStore>),
  ) => void,
  get: () => DesignerRunStore,
): Promise<void> {
  const generation = get().runGeneration;
  const existing = get().run ?? emptyRun(graph);
  let nodeStates = {
    ...initialNodeStates(graph),
    ...get().nodeStates,
  };
  nodeStates = markNodesStatus(nodeStates, layerIds, DESIGNER_NODE_STATUS_RUNNING);

  const runningRun: DesignerExecutionRun = {
    ...existing,
    graph_id: graph.graph_id,
    project_id: graph.project_id,
    status: DESIGNER_RUN_STATUS_RUNNING,
    node_states: nodeStates,
    current_node_ids: layerIds,
    updated_at: Date.now(),
  };

  set({
    run: runningRun,
    nodeStates,
    currentLayerNodeIds: layerIds,
    isRunning: true,
    primaryAction: 'running',
    boundGraphId: graph.graph_id,
  });

  await Promise.all([sleep(2000), ensureDesignerFakeAssets()]);

  // Aborted by pause/cancel/reset while waiting.
  if (get().runGeneration !== generation || !get().isRunning) {
    return;
  }

  // Fake backend: always succeed + attach procedural preview assets.
  const assets = await ensureDesignerFakeAssets();
  nodeStates = markNodesStatus(get().nodeStates, layerIds, DESIGNER_NODE_STATUS_COMPLETED);
  const nodeTypeById = new Map(graph.nodes.map((node) => [node.id, String(node.type || '')]));
  for (const nodeId of layerIds) {
    const nodeType = nodeTypeById.get(nodeId) || 'text';
    const outputRef = fakeOutputRefForType(nodeType, assets);
    const current = nodeStates[nodeId] ?? { status: DESIGNER_NODE_STATUS_COMPLETED };
    nodeStates[nodeId] = {
      ...current,
      status: DESIGNER_NODE_STATUS_COMPLETED,
      output_ref: outputRef,
      error: null,
      completed_at: current.completed_at ?? Date.now(),
    };
  }
  const done = allNodesCompleted(graph, nodeStates);
  const pausedRun: DesignerExecutionRun = {
    ...runningRun,
    status: done ? DESIGNER_RUN_STATUS_COMPLETED : DESIGNER_RUN_STATUS_PAUSED,
    node_states: nodeStates,
    current_node_ids: layerIds,
    updated_at: Date.now(),
  };

  if (get().runGeneration !== generation) {
    return;
  }

  set({
    run: pausedRun,
    nodeStates,
    currentLayerNodeIds: layerIds,
    isRunning: false,
    primaryAction: refreshPrimary(graph, nodeStates, layerIds, false),
  });
}

function fakeOutputRefForType(
  nodeType: string,
  assets: { imageUrl: string; videoUrl: string },
): AssetRef {
  if (nodeType === DESIGNER_NODE_TYPE_IMAGE) {
    return { kind: 'image', uri: assets.imageUrl, mime_type: 'image/jpeg', label: 'fake-image' };
  }
  if (nodeType === DESIGNER_NODE_TYPE_VIDEO) {
    return {
      kind: 'video',
      uri: assets.videoUrl,
      mime_type: 'video/webm',
      label: 'fake-video',
    };
  }
  if (nodeType === DESIGNER_NODE_TYPE_AUDIO) {
    return { kind: 'audio', uri: 'designer://fake/audio', label: 'fake-audio' };
  }
  if (nodeType === DESIGNER_NODE_TYPE_TABLE) {
    return { kind: 'table', uri: 'designer://fake/table', label: 'fake-table' };
  }
  if (nodeType === DESIGNER_NODE_TYPE_TEXT) {
    return { kind: 'text', uri: 'designer://fake/text', label: DESIGNER_FAKE_TEXT };
  }
  return { kind: nodeType, uri: 'designer://fake/unknown', label: DESIGNER_FAKE_TEXT };
}
