import type { ReactNode } from 'react';
import { Handle, Position, type Node, type NodeProps } from '@xyflow/react';
import {
  DESIGNER_NODE_TYPE_AUDIO,
  DESIGNER_NODE_TYPE_IMAGE,
  DESIGNER_NODE_TYPE_TABLE,
  DESIGNER_NODE_TYPE_TEXT,
  DESIGNER_NODE_TYPE_VIDEO,
} from '../../executionGraphTypes';
import type { DesignerReactFlowNode } from '../../designerGraphAdapter';

type DesignerNodeData = DesignerReactFlowNode['data'];
type DesignerFlowNode = Node<DesignerNodeData>;

function DesignerNodeShell({
  label,
  nodeType,
  body,
  media = false,
}: {
  label: string;
  nodeType: string;
  body: ReactNode;
  media?: boolean;
}) {
  return (
    <div className={`designer-node${media ? ' designer-node--media' : ''}`} data-testid="designer-node">
      <Handle type="target" position={Position.Left} />
      <div className="designer-node__header">
        <span className="designer-node__label">{label}</span>
        <span className="designer-node__type">{nodeType}</span>
      </div>
      <div className="designer-node__body">{body}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export function DesignerTextNode({ data }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  return (
    <DesignerNodeShell
      label={nodeData.label}
      nodeType={DESIGNER_NODE_TYPE_TEXT}
      body={nodeData.config.prompt ? String(nodeData.config.prompt).slice(0, 80) : 'Text node'}
    />
  );
}

export function DesignerTableNode({ data }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  return (
    <DesignerNodeShell
      label={nodeData.label}
      nodeType={DESIGNER_NODE_TYPE_TABLE}
      body="Table preview"
    />
  );
}

export function DesignerMediaNode({ data }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  const nodeType = nodeData.nodeType;
  const previewLabel =
    nodeType === DESIGNER_NODE_TYPE_VIDEO
      ? 'Video preview'
      : nodeType === DESIGNER_NODE_TYPE_AUDIO
        ? 'Audio preview'
        : 'Image preview';

  return (
    <DesignerNodeShell
      label={nodeData.label}
      nodeType={nodeType}
      media={nodeType === DESIGNER_NODE_TYPE_IMAGE || nodeType === DESIGNER_NODE_TYPE_VIDEO || nodeType === DESIGNER_NODE_TYPE_AUDIO}
      body={previewLabel}
    />
  );
}

export const designerNodeTypes = {
  [DESIGNER_NODE_TYPE_TEXT]: DesignerTextNode,
  [DESIGNER_NODE_TYPE_TABLE]: DesignerTableNode,
  [DESIGNER_NODE_TYPE_IMAGE]: DesignerMediaNode,
  [DESIGNER_NODE_TYPE_VIDEO]: DesignerMediaNode,
  [DESIGNER_NODE_TYPE_AUDIO]: DesignerMediaNode,
};
