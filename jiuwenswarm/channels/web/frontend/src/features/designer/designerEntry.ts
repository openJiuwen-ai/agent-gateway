import { useDesignerStore } from './designerStore';
import { useDesignerChatStore } from './designerChatStore';
import { designerGraphClient } from './designerGraphClient';

export const DESIGNER_BOOTSTRAP_THINKING_MS = 2000;

export type LaunchDesignerFromTaskParams = {
  prompt: string;
  projectId?: string;
  projectDir?: string;
  workMode?: 'work' | 'code';
  /** Navigate to Design nav before/while bootstrap runs. */
  onNavigateToDesign: () => void;
  thinkingMs?: number;
  thinkingText?: string;
  doneText?: string;
  errorText?: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

/**
 * Tasks 页选「设计」后发送：跳转设计栏 → 聊天区展示用户消息 → 模拟思考 → bootstrap 上屏。
 * 不写入主 chatStore，也不走主 agent 链路。
 */
export async function launchDesignerFromTask(params: LaunchDesignerFromTaskParams): Promise<void> {
  const prompt = params.prompt.trim();
  if (!prompt) return;

  const thinkingMs = params.thinkingMs ?? DESIGNER_BOOTSTRAP_THINKING_MS;
  const thinkingText = params.thinkingText ?? '正在思考如何搭建设计工作流…';
  const doneText = params.doneText ?? '已为你搭建初始设计工作流。';
  const errorText = params.errorText ?? '搭建工作流失败，请稍后重试。';

  const designerStore = useDesignerStore.getState();
  const chatStore = useDesignerChatStore.getState();

  chatStore.reset();
  designerStore.beginBootstrapEntry();
  chatStore.appendMessage({
    role: 'user',
    content: prompt,
    kind: 'user',
  });
  params.onNavigateToDesign();

  chatStore.setBootstrapPhase('thinking');
  const thinkingId = chatStore.appendMessage({
    role: 'assistant',
    content: thinkingText,
    kind: 'thinking',
  });

  await sleep(thinkingMs);

  // 用户可能已离开设计页；仍继续完成 bootstrap，图会写入 store。
  chatStore.removeMessage(thinkingId);
  chatStore.setBootstrapPhase('bootstrapping');

  try {
    const result = await designerGraphClient.bootstrap({
      prompt,
      projectId: params.projectId,
      projectDir: params.projectDir,
      workMode: params.workMode,
    });
    const graph = result?.graph;
    if (!graph?.graph_id || !Array.isArray(graph.nodes)) {
      throw new Error('bootstrap response missing graph');
    }
    useDesignerStore.getState().applyGraph(graph);
    useDesignerChatStore.getState().appendMessage({
      role: 'assistant',
      content: doneText,
      kind: 'bootstrap_done',
    });
    useDesignerChatStore.getState().setBootstrapPhase('done');
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    useDesignerStore.getState().failBootstrapEntry(message);
    useDesignerChatStore.getState().appendMessage({
      role: 'assistant',
      content: `${errorText}${message ? `（${message}）` : ''}`,
      kind: 'bootstrap_error',
    });
    useDesignerChatStore.getState().setBootstrapPhase('error');
  }
}
