import { create } from 'zustand';

type DesignerUiStore = {
  selectedMaterialId: string;
  viewerOpen: boolean;
  chooserNodeId: string;
  editRequestKey: number;
  inspectNode: (nodeId: string, materialIndex?: number) => void;
  startEdit: (materialId: string) => void;
  openViewer: (id: string) => void;
  closeViewer: () => void;
  openRevision: (nodeId: string) => void;
  closeRevision: () => void;
  setSelectedMaterialId: (id: string) => void;
  reset: () => void;
};

const initialState = {
  selectedMaterialId: '',
  viewerOpen: false,
  chooserNodeId: '',
  editRequestKey: 0,
};

export const useDesignerUiStore = create<DesignerUiStore>((set) => ({
  ...initialState,

  inspectNode: (nodeId, materialIndex) =>
    set({
      selectedMaterialId:
        typeof materialIndex === 'number' ? `${nodeId}:${materialIndex}` : nodeId,
      viewerOpen: true,
    }),

  startEdit: (materialId) =>
    set((state) => ({
      selectedMaterialId: materialId,
      viewerOpen: true,
      editRequestKey: state.editRequestKey + 1,
    })),

  openViewer: (id) => set({ selectedMaterialId: id, viewerOpen: true }),

  closeViewer: () => set({ viewerOpen: false }),

  openRevision: (nodeId) => set({ chooserNodeId: nodeId }),

  closeRevision: () => set({ chooserNodeId: '' }),

  setSelectedMaterialId: (id) => set({ selectedMaterialId: id }),

  reset: () => set({ ...initialState }),
}));
