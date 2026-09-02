import { webRequest } from '../../services/webClient';
import type {
  DesignerExecutionGraph,
  DesignerExecutionRun,
  DesignerGraphBootstrapResult,
} from './executionGraphTypes';

export const designerGraphClient = {
  get: (graphId: string) =>
    webRequest<{ graph: DesignerExecutionGraph }>('designer.graph.get', { graph_id: graphId }),

  list: (projectId: string) =>
    webRequest<{ graphs: DesignerExecutionGraph[] }>('designer.graph.list', {
      project_id: projectId,
    }),

  save: (graph: DesignerExecutionGraph) =>
    webRequest<{ graph: DesignerExecutionGraph }>('designer.graph.save', { graph }),

  bootstrap: (params: {
    prompt: string;
    title?: string;
    name?: string;
    projectId?: string;
    projectDir?: string;
    workMode?: 'work' | 'code';
  }) =>
    webRequest<DesignerGraphBootstrapResult>('designer.graph.bootstrap', {
      prompt: params.prompt,
      ...(params.title ? { title: params.title } : {}),
      ...(params.name ? { name: params.name } : {}),
      ...(params.projectId ? { project_id: params.projectId } : {}),
      ...(params.projectDir ? { project_dir: params.projectDir } : {}),
      ...(params.workMode ? { work_mode: params.workMode } : {}),
    }),

  startRun: (params: { graphId?: string; runId?: string }) =>
    webRequest<{ run: DesignerExecutionRun }>('designer.run.start', {
      ...(params.graphId ? { graph_id: params.graphId } : {}),
      ...(params.runId ? { run_id: params.runId } : {}),
    }),

  getRun: (params: { runId?: string; graphId?: string }) =>
    webRequest<{ run: DesignerExecutionRun }>('designer.run.get', {
      ...(params.runId ? { run_id: params.runId } : {}),
      ...(params.graphId ? { graph_id: params.graphId } : {}),
    }),

  pauseRun: (runId: string) =>
    webRequest<{ run: DesignerExecutionRun }>('designer.run.pause', { run_id: runId }),

  cancelRun: (runId: string) =>
    webRequest<{ run: DesignerExecutionRun }>('designer.run.cancel', { run_id: runId }),
};
