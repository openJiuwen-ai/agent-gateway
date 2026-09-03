import { create } from 'zustand';
import { designerGraphClient } from './designerGraphClient';
import type { DesignerExecutionGraph } from './executionGraphTypes';

export type DesignerLoadStatus =
  | 'idle'
  | 'loading'
  | 'bootstrapping'
  | 'ready'
  | 'empty'
  | 'error';

type DesignerStore = {
  graphId: string | null;
  domainGraph: DesignerExecutionGraph | null;
  loadStatus: DesignerLoadStatus;
  loadError: string | null;
  chatCollapsed: boolean;
  /** True while Tasks→Design bootstrap owns the page load (blocks list/get race). */
  bootstrapInProgress: boolean;
  setChatCollapsed: (collapsed: boolean) => void;
  loadForProject: (projectId: string | undefined) => Promise<void>;
  beginBootstrapEntry: () => void;
  failBootstrapEntry: (message: string) => void;
  applyGraph: (graph: DesignerExecutionGraph) => void;
  reset: () => void;
};

const initialState = {
  graphId: null,
  domainGraph: null,
  loadStatus: 'idle' as DesignerLoadStatus,
  loadError: null,
  chatCollapsed: false,
  bootstrapInProgress: false,
};

export const useDesignerStore = create<DesignerStore>((set, get) => ({
  ...initialState,

  setChatCollapsed: (collapsed) => set({ chatCollapsed: collapsed }),

  beginBootstrapEntry: () =>
    set({
      graphId: null,
      domainGraph: null,
      loadStatus: 'bootstrapping',
      loadError: null,
      bootstrapInProgress: true,
      chatCollapsed: false,
    }),

  failBootstrapEntry: (message) =>
    set({
      graphId: null,
      domainGraph: null,
      loadStatus: 'error',
      loadError: message,
      bootstrapInProgress: false,
    }),

  applyGraph: (graph) =>
    set({
      graphId: graph.graph_id,
      domainGraph: graph,
      loadStatus: 'ready',
      loadError: null,
      bootstrapInProgress: false,
    }),

  reset: () => set({ ...initialState }),

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
      });
    }
  },
}));
