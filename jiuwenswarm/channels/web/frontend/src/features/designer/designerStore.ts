import { create } from 'zustand';
import { designerGraphClient } from './designerGraphClient';
import type { DesignerReactFlowGraph } from './designerGraphAdapter';
import type { AssetRef, DesignerExecutionGraph } from './executionGraphTypes';

export type DesignerLoadStatus =
  | 'idle'
  | 'loading'
  | 'bootstrapping'
  | 'ready'
  | 'empty'
  | 'error';

const SAVE_DEBOUNCE_MS = 500;

let saveTimer: ReturnType<typeof setTimeout> | null = null;
let saveSeq = 0;

type DesignerStore = {
  graphId: string | null;
  domainGraph: DesignerExecutionGraph | null;
  loadStatus: DesignerLoadStatus;
  loadError: string | null;
  /** True while Tasks→Design bootstrap owns the page load (blocks list/get race). */
  bootstrapInProgress: boolean;
  selectedNodeId: string | null;
  saveStatus: 'idle' | 'saving' | 'saved' | 'error';
  setSelectedNodeId: (nodeId: string | null) => void;
  loadForProject: (projectId: string | undefined) => Promise<void>;
  beginBootstrapEntry: () => void;
  failBootstrapEntry: (message: string) => void;
  applyGraph: (graph: DesignerExecutionGraph) => void;
  updateNodeConfig: (
    nodeId: string,
    updater: (config: Record<string, unknown>) => Record<string, unknown>,
  ) => void;
  setNodeOutputRef: (nodeId: string, outputRef: AssetRef | null) => void;
  clearAssetReferences: (assetId: string) => void;
  addEdge: (connection: { source: string; target: string; id?: string; label?: string }) => void;
  removeEdges: (edgeIds: string[]) => void;
  persistReactFlowLayout: (reactFlow: DesignerReactFlowGraph) => void;
  scheduleSave: () => void;
  flushSave: () => Promise<void>;
  reset: () => void;
};

const initialState = {
  graphId: null as string | null,
  domainGraph: null as DesignerExecutionGraph | null,
  loadStatus: 'idle' as DesignerLoadStatus,
  loadError: null as string | null,
  bootstrapInProgress: false,
  selectedNodeId: null as string | null,
  saveStatus: 'idle' as DesignerStore['saveStatus'],
};

function clearSaveTimer() {
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
}

export const useDesignerStore = create<DesignerStore>((set, get) => ({
  ...initialState,

  setSelectedNodeId: (nodeId) => set({ selectedNodeId: nodeId }),

  beginBootstrapEntry: () => {
    clearSaveTimer();
    set({
      graphId: null,
      domainGraph: null,
      loadStatus: 'bootstrapping',
      loadError: null,
      bootstrapInProgress: true,
      selectedNodeId: null,
      saveStatus: 'idle',
    });
  },

  failBootstrapEntry: (message) =>
    set({
      graphId: null,
      domainGraph: null,
      loadStatus: 'error',
      loadError: message,
      bootstrapInProgress: false,
      selectedNodeId: null,
    }),

  applyGraph: (graph) =>
    set({
      graphId: graph.graph_id,
      domainGraph: graph,
      loadStatus: 'ready',
      loadError: null,
      bootstrapInProgress: false,
    }),

  updateNodeConfig: (nodeId, updater) => {
    const graph = get().domainGraph;
    if (!graph) return;
    const nodes = graph.nodes.map((node) => {
      if (node.id !== nodeId) return node;
      const nextConfig = updater({ ...(node.config ?? {}) });
      return { ...node, config: nextConfig };
    });
    set({
      domainGraph: {
        ...graph,
        nodes,
        updated_at: Date.now(),
      },
    });
    get().scheduleSave();
  },

  setNodeOutputRef: (nodeId, outputRef) => {
    const graph = get().domainGraph;
    if (!graph) return;
    let changed = false;
    const nodes = graph.nodes.map((node) => {
      if (node.id !== nodeId) return node;
      changed = true;
      return { ...node, output_ref: outputRef };
    });
    if (!changed) return;
    set({
      domainGraph: {
        ...graph,
        nodes,
        updated_at: Date.now(),
      },
    });
    get().scheduleSave();
  },

  clearAssetReferences: (assetId) => {
    const graph = get().domainGraph;
    if (!graph || !assetId) return;
    let changed = false;
    const nodes = graph.nodes.map((node) => {
      const config = { ...(node.config ?? {}) };
      const upload = (config.upload ?? null) as
        | { asset_id?: string; filename?: string; mime_type?: string }
        | null;
      const uploadMatched = upload?.asset_id === assetId;
      let touched = false;

      if (uploadMatched && upload) {
        config.upload = {
          ...upload,
          asset_id: '',
          filename: '',
          mime_type: '',
        };
        touched = true;
      }

      if (Array.isArray(config.materials)) {
        const filtered = config.materials.filter((item) => {
          if (!item || typeof item !== 'object') return true;
          return (item as { asset_id?: string }).asset_id !== assetId;
        });
        if (filtered.length !== config.materials.length) {
          config.materials = filtered;
          touched = true;
        }
      }

      if (!touched) return node;
      changed = true;
      return {
        ...node,
        config,
        ...(uploadMatched ? { output_ref: null } : {}),
      };
    });
    if (!changed) return;
    set({
      domainGraph: {
        ...graph,
        nodes,
        updated_at: Date.now(),
      },
    });
    get().scheduleSave();
  },

  addEdge: (connection) => {
    const graph = get().domainGraph;
    if (!graph) return;
    const source = String(connection.source ?? '').trim();
    const target = String(connection.target ?? '').trim();
    if (!source || !target) return;
    const nodeIds = new Set(graph.nodes.map((node) => node.id));
    if (!nodeIds.has(source) || !nodeIds.has(target)) return;
    const duplicate = graph.edges.some(
      (edge) => edge.source === source && edge.target === target,
    );
    if (duplicate) return;
    const id =
      String(connection.id ?? '').trim() ||
      `e_${source}_${target}_${Date.now().toString(36)}`;
    const nextEdge = {
      id,
      source,
      target,
      ...(connection.label ? { label: connection.label } : {}),
    };
    set({
      domainGraph: {
        ...graph,
        edges: [...graph.edges, nextEdge],
        updated_at: Date.now(),
      },
    });
    get().scheduleSave();
  },

  removeEdges: (edgeIds) => {
    const graph = get().domainGraph;
    if (!graph || edgeIds.length === 0) return;
    const removeSet = new Set(edgeIds);
    const edges = graph.edges.filter((edge) => !removeSet.has(edge.id));
    if (edges.length === graph.edges.length) return;
    set({
      domainGraph: {
        ...graph,
        edges,
        updated_at: Date.now(),
      },
    });
    get().scheduleSave();
  },

  persistReactFlowLayout: (reactFlow) => {
    const graph = get().domainGraph;
    if (!graph) return;
    const rfById = new Map(reactFlow.nodes.map((node) => [node.id, node]));
    const nodes = graph.nodes.map((node) => {
      const rfNode = rfById.get(node.id);
      if (!rfNode) return node;
      const width =
        typeof rfNode.style?.width === 'number'
          ? rfNode.style.width
          : node.layout?.width;
      const height =
        typeof rfNode.style?.height === 'number'
          ? rfNode.style.height
          : node.layout?.height;
      return {
        ...node,
        layout: {
          x: rfNode.position.x,
          y: rfNode.position.y,
          ...(typeof width === 'number' ? { width } : {}),
          ...(typeof height === 'number' ? { height } : {}),
        },
      };
    });
    set({
      domainGraph: {
        ...graph,
        nodes,
        updated_at: Date.now(),
      },
    });
    get().scheduleSave();
  },

  scheduleSave: () => {
    clearSaveTimer();
    saveTimer = setTimeout(() => {
      void get().flushSave();
    }, SAVE_DEBOUNCE_MS);
  },

  flushSave: async () => {
    clearSaveTimer();
    const graph = get().domainGraph;
    if (!graph || get().bootstrapInProgress) return;
    const seq = ++saveSeq;
    set({ saveStatus: 'saving' });
    try {
      const { graph: saved } = await designerGraphClient.save(graph);
      if (seq !== saveSeq) return;
      set({
        domainGraph: saved,
        graphId: saved.graph_id,
        saveStatus: 'saved',
      });
    } catch {
      if (seq !== saveSeq) return;
      set({ saveStatus: 'error' });
    }
  },

  reset: () => {
    clearSaveTimer();
    set({ ...initialState });
  },

  loadForProject: async (projectId) => {
    if (get().bootstrapInProgress) {
      return;
    }

    const normalizedProjectId = String(projectId ?? '').trim();
    const graphProjectId = String(get().domainGraph?.project_id ?? '').trim();
    const effectiveProjectId = normalizedProjectId || graphProjectId;

    if (!effectiveProjectId) {
      // 没有 project 上下文时，保留已有 ready 图（例如 bootstrap 刚写入），避免刷成 empty。
      if (get().loadStatus === 'ready' && get().domainGraph) {
        return;
      }
      set({
        graphId: null,
        domainGraph: null,
        loadStatus: 'empty',
        loadError: null,
        bootstrapInProgress: false,
        selectedNodeId: null,
      });
      return;
    }

    const previousGraph = get().domainGraph;
    const previousStatus = get().loadStatus;
    const previousGraphId = get().graphId;

    set({
      loadStatus: 'loading',
      loadError: null,
    });

    try {
      const { graphs } = await designerGraphClient.list(effectiveProjectId);
      if (get().bootstrapInProgress) {
        return;
      }
      const latest = graphs[0];
      if (!latest?.graph_id) {
        if (previousStatus === 'ready' && previousGraph) {
          set({
            graphId: previousGraphId,
            domainGraph: previousGraph,
            loadStatus: 'ready',
            loadError: null,
          });
          return;
        }
        set({
          graphId: null,
          domainGraph: null,
          loadStatus: 'empty',
          loadError: null,
          selectedNodeId: null,
        });
        return;
      }

      const { graph } = await designerGraphClient.get(latest.graph_id);
      if (get().bootstrapInProgress) {
        return;
      }
      set({
        graphId: graph.graph_id,
        domainGraph: graph,
        loadStatus: 'ready',
        loadError: null,
      });
    } catch (error) {
      if (get().bootstrapInProgress) {
        return;
      }
      if (previousStatus === 'ready' && previousGraph) {
        set({
          graphId: previousGraphId,
          domainGraph: previousGraph,
          loadStatus: 'ready',
          loadError: null,
        });
        return;
      }
      set({
        graphId: null,
        domainGraph: null,
        loadStatus: 'error',
        loadError: error instanceof Error ? error.message : String(error),
        selectedNodeId: null,
      });
    }
  },
}));
