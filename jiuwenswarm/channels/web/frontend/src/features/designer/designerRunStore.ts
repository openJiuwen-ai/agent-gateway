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
  resetForGraph: (graph: DesignerExecutionGraph | null) => void;
  getPrimaryAction: (graph: DesignerExecutionGraph | null) => DesignerRunPrimaryAction;
  /** 执行 / 继续 / 重试失败节点 */
  advance: (graph: DesignerExecutionGraph) => Promise<void>;
  /** 重跑当前层 */
  rerunCurrentLayer: (graph: DesignerExecutionGraph) => Promise<void>;
  /** 重新开始：清空后跑第一层 */
  restart: (graph: DesignerExecutionGraph) => Promise<void>;
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

  resetForGraph: (graph) => {
    if (!graph || graph.nodes.length === 0) {
      set({
        run: null,
        nodeStates: {},
        currentLayerNodeIds: [],
        isRunning: false,
        primaryAction: 'execute',
        boundGraphId: graph?.graph_id ?? null,
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
      layerIds = computeNextLayer(graph, state.currentLayerNodeIds, state.nodeStates);
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
    });
    const layerIds = computeRootLayer(graph);
    if (layerIds.length === 0) return;
    await runFakeLayer(graph, layerIds, set, get);
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
