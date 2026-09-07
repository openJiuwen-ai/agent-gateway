import { webRequest } from '../../services/webClient';
import type {
  DesignerExecutionGraph,
  DesignerExecutionRun,
  DesignerGraphBootstrapResult,
  DesignerGraphPatch,
  DesignerGraphSummary,
} from './executionGraphTypes';

export const designerGraphClient = {
  get: (graphId: string) =>
    webRequest<{ graph: DesignerExecutionGraph }>('designer.graph.get', { graph_id: graphId }),

  list: (projectId?: string) =>
    webRequest<{ graphs: DesignerExecutionGraph[]; summaries?: DesignerGraphSummary[] }>(
      'designer.graph.list',
      projectId ? { project_id: projectId } : {},
    ),

  save: (graph: DesignerExecutionGraph) =>
    webRequest<{ graph: DesignerExecutionGraph }>('designer.graph.save', { graph }),

  patch: (graphId: string, patch: DesignerGraphPatch) =>
    webRequest<{ graph: DesignerExecutionGraph }>('designer.graph.patch', {
      graph_id: graphId,
      patch,
    }),

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

  startRun: (params: { graphId?: string; runId?: string; nodeId?: string }) =>
    webRequest<{ run: DesignerExecutionRun }>('designer.run.start', {
      ...(params.graphId ? { graph_id: params.graphId } : {}),
      ...(params.runId ? { run_id: params.runId } : {}),
      ...(params.nodeId ? { node_id: params.nodeId } : {}),
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

  chooseOutput: (params: { runId: string; nodeId: string; choice: 'original' | 'new' }) =>
    webRequest<{ run: DesignerExecutionRun }>('designer.run.choose_output', {
      run_id: params.runId,
      node_id: params.nodeId,
      choice: params.choice,
    }),
};
