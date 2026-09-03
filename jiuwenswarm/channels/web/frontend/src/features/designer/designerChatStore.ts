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
  messages: DesignerChatMessage[];
  bootstrapPhase: DesignerBootstrapPhase;
  reset: () => void;
  appendMessage: (message: Omit<DesignerChatMessage, 'id' | 'createdAt'> & {
    id?: string;
    createdAt?: number;
  }) => string;
  removeMessage: (id: string) => void;
  setBootstrapPhase: (phase: DesignerBootstrapPhase) => void;
};

export const useDesignerChatStore = create<DesignerChatStore>((set) => ({
  messages: [],
  bootstrapPhase: 'idle',

  reset: () => set({ messages: [], bootstrapPhase: 'idle' }),

  appendMessage: (message) => {
    const id = message.id ?? generateUuidV4();
    const createdAt = message.createdAt ?? Date.now();
    set((state) => ({
      messages: [
        ...state.messages,
        {
          id,
          role: message.role,
          content: message.content,
          kind: message.kind,
          createdAt,
        },
      ],
    }));
    return id;
  },

  removeMessage: (id) =>
    set((state) => ({
      messages: state.messages.filter((item) => item.id !== id),
    })),

  setBootstrapPhase: (phase) => set({ bootstrapPhase: phase }),
}));
