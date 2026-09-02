import { create } from 'zustand';
import { designerGraphClient } from './designerGraphClient';
import type { DesignerExecutionGraph } from './executionGraphTypes';

export type DesignerLoadStatus = 'idle' | 'loading' | 'ready' | 'empty' | 'error';

type DesignerStore = {
  graphId: string | null;
  domainGraph: DesignerExecutionGraph | null;
  loadStatus: DesignerLoadStatus;
  loadError: string | null;
  chatCollapsed: boolean;
  setChatCollapsed: (collapsed: boolean) => void;
  loadForProject: (projectId: string | undefined) => Promise<void>;
  applyGraph: (graph: DesignerExecutionGraph) => void;
  reset: () => void;
};

const initialState = {
  graphId: null,
  domainGraph: null,
  loadStatus: 'idle' as DesignerLoadStatus,
  loadError: null,
  chatCollapsed: false,
};

export const useDesignerStore = create<DesignerStore>((set) => ({
  ...initialState,

  setChatCollapsed: (collapsed) => set({ chatCollapsed: collapsed }),

  applyGraph: (graph) =>
    set({
      graphId: graph.graph_id,
      domainGraph: graph,
      loadStatus: 'ready',
      loadError: null,
    }),

  reset: () => set({ ...initialState }),

  loadForProject: async (projectId) => {
    const normalizedProjectId = String(projectId ?? '').trim();
    if (!normalizedProjectId) {
      set({
        ...initialState,
        loadStatus: 'empty',
      });
      return;
    }

    set({
      loadStatus: 'loading',
      loadError: null,
    });

    try {
      const { graphs } = await designerGraphClient.list(normalizedProjectId);
      const latest = graphs[0];
      if (!latest?.graph_id) {
        set({
          graphId: null,
          domainGraph: null,
          loadStatus: 'empty',
          loadError: null,
        });
        return;
      }

      const { graph } = await designerGraphClient.get(latest.graph_id);
      set({
        graphId: graph.graph_id,
        domainGraph: graph,
        loadStatus: 'ready',
        loadError: null,
      });
    } catch (error) {
      set({
        graphId: null,
        domainGraph: null,
        loadStatus: 'error',
        loadError: error instanceof Error ? error.message : String(error),
      });
    }
  },
}));
