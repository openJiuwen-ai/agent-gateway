export const DESIGNER_GRAPH_SCHEMA_VERSION = 'designer-execution-graph.v1' as const;
export const DESIGNER_RUN_SCHEMA_VERSION = 'designer-execution-run.v1' as const;

export const DESIGNER_NODE_TYPE_TEXT = 'text' as const;
export const DESIGNER_NODE_TYPE_TABLE = 'table' as const;
export const DESIGNER_NODE_TYPE_IMAGE = 'image' as const;
export const DESIGNER_NODE_TYPE_VIDEO = 'video' as const;
export const DESIGNER_NODE_TYPE_AUDIO = 'audio' as const;

export const DESIGNER_NODE_TYPES = [
  DESIGNER_NODE_TYPE_TEXT,
  DESIGNER_NODE_TYPE_TABLE,
  DESIGNER_NODE_TYPE_IMAGE,
  DESIGNER_NODE_TYPE_VIDEO,
  DESIGNER_NODE_TYPE_AUDIO,
] as const;

export type DesignerNodeType = (typeof DESIGNER_NODE_TYPES)[number];

export const DESIGNER_GRAPH_SOURCE_PROMPT = 'prompt' as const;
export const DESIGNER_GRAPH_SOURCE_MANUAL = 'manual' as const;

export const DESIGNER_NODE_STATUS_PENDING = 'pending' as const;
export const DESIGNER_NODE_STATUS_RUNNING = 'running' as const;
export const DESIGNER_NODE_STATUS_COMPLETED = 'completed' as const;
export const DESIGNER_NODE_STATUS_FAILED = 'failed' as const;
export const DESIGNER_NODE_STATUS_CANCELLED = 'cancelled' as const;

export const DESIGNER_RUN_STATUS_DRAFT = 'draft' as const;
export const DESIGNER_RUN_STATUS_RUNNING = 'running' as const;
export const DESIGNER_RUN_STATUS_PAUSED = 'paused' as const;
export const DESIGNER_RUN_STATUS_COMPLETED = 'completed' as const;
export const DESIGNER_RUN_STATUS_FAILED = 'failed' as const;
export const DESIGNER_RUN_STATUS_CANCELLED = 'cancelled' as const;

export type AssetRef = {
  kind: string;
  uri: string;
  mime_type?: string;
  label?: string;
};

export type NodeLayout = {
  x?: number;
  y?: number;
  width?: number;
  height?: number;
};

export type DesignerGraphNode = {
  id: string;
  type: DesignerNodeType | string;
  label: string;
  config?: Record<string, unknown>;
  layout?: NodeLayout;
  output_ref?: AssetRef | null;
};

export type DesignerGraphEdge = {
  id: string;
  source: string;
  target: string;
  label?: string;
};

export type DesignerExecutionGraph = {
  schema_version: typeof DESIGNER_GRAPH_SCHEMA_VERSION | string;
  graph_id: string;
  project_id: string;
  title: string;
  description?: string;
  source?: typeof DESIGNER_GRAPH_SOURCE_PROMPT | typeof DESIGNER_GRAPH_SOURCE_MANUAL | string;
  nodes: DesignerGraphNode[];
  edges: DesignerGraphEdge[];
  metadata?: Record<string, unknown>;
  created_at?: number;
  updated_at?: number;
};

export type DesignerNodeState = {
  status: string;
  started_at?: number | null;
  completed_at?: number | null;
  output_ref?: AssetRef | null;
  error?: string | null;
  blocked_by?: string[];
};

export type DesignerExecutionRun = {
  schema_version: typeof DESIGNER_RUN_SCHEMA_VERSION | string;
  run_id: string;
  graph_id: string;
  project_id: string;
  status: string;
  node_states: Record<string, DesignerNodeState>;
  current_node_ids: string[];
  created_at?: number;
  updated_at?: number;
};

export type DesignerGraphBootstrapResult = {
  graph: DesignerExecutionGraph;
  project_id: string;
  project?: {
    project_id: string;
    project_dir: string;
    restored: boolean;
    work_mode: string;
  };
};
