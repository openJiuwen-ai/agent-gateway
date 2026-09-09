import { create } from 'zustand';
import { generateUuidV4 } from '../../utils/uuid';

export type DesignerChatRole = 'user' | 'assistant' | 'system';

export type DesignerChatMessageKind =
  | 'user'
  | 'thinking'
  | 'bootstrap_done'
  | 'bootstrap_error'
  | 'not_implemented';

export type DesignerChatMessage = {
  id: string;
  role: DesignerChatRole;
  content: string;
  kind: DesignerChatMessageKind;
  createdAt: number;
};

export type DesignerBootstrapPhase = 'idle' | 'thinking' | 'bootstrapping' | 'done' | 'error';

type DesignerChatStore = {
  activeGraphId: string | null;
  messages: DesignerChatMessage[];
  messagesByGraphId: Record<string, DesignerChatMessage[]>;
  bootstrapPhase: DesignerBootstrapPhase;
  reset: () => void;
  bindGraph: (graphId: string | null) => void;
  appendMessage: (message: Omit<DesignerChatMessage, 'id' | 'createdAt'> & {
    id?: string;
    createdAt?: number;
  }) => string;
  removeMessage: (id: string) => void;
  setBootstrapPhase: (phase: DesignerBootstrapPhase) => void;
};

function archiveCurrent(
  activeGraphId: string | null,
  messages: DesignerChatMessage[],
  messagesByGraphId: Record<string, DesignerChatMessage[]>,
): Record<string, DesignerChatMessage[]> {
  if (!activeGraphId) return messagesByGraphId;
  return { ...messagesByGraphId, [activeGraphId]: messages };
}

export const useDesignerChatStore = create<DesignerChatStore>((set, get) => ({
  activeGraphId: null,
  messages: [],
  messagesByGraphId: {},
  bootstrapPhase: 'idle',

  reset: () => {
    const { activeGraphId, messages, messagesByGraphId } = get();
    set({
      messages: [],
      bootstrapPhase: 'idle',
      activeGraphId: null,
      messagesByGraphId: archiveCurrent(activeGraphId, messages, messagesByGraphId),
    });
  },

  bindGraph: (graphId) => {
    const id = String(graphId ?? '').trim() || null;
    const { activeGraphId, messages, messagesByGraphId } = get();
    if (id === activeGraphId) return;
    const archived = archiveCurrent(activeGraphId, messages, messagesByGraphId);
    const pending = !activeGraphId && messages.length > 0 ? messages : [];
    const nextMessages = id ? (archived[id] ?? pending) : pending;
    const nextArchive = id && pending.length > 0
      ? { ...archived, [id]: pending }
      : archived;
    set({
      activeGraphId: id,
      messages: nextMessages,
      messagesByGraphId: nextArchive,
    });
  },

  appendMessage: (message) => {
    const id = message.id ?? generateUuidV4();
    const createdAt = message.createdAt ?? Date.now();
    set((state) => {
      const next = [
        ...state.messages,
        {
          id,
          role: message.role,
          content: message.content,
          kind: message.kind,
          createdAt,
        },
      ];
      const messagesByGraphId = state.activeGraphId
        ? { ...state.messagesByGraphId, [state.activeGraphId]: next }
        : state.messagesByGraphId;
      return { messages: next, messagesByGraphId };
    });
    return id;
  },

  removeMessage: (id) =>
    set((state) => {
      const next = state.messages.filter((item) => item.id !== id);
      const messagesByGraphId = state.activeGraphId
        ? { ...state.messagesByGraphId, [state.activeGraphId]: next }
        : state.messagesByGraphId;
      return { messages: next, messagesByGraphId };
    }),

  setBootstrapPhase: (phase) => set({ bootstrapPhase: phase }),
}));
