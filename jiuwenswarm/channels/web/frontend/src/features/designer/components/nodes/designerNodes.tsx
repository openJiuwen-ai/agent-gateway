import type { ReactNode } from 'react';
import { Handle, NodeToolbar, Position, type Node, type NodeProps } from '@xyflow/react';
import {
  DESIGNER_NODE_TYPE_AUDIO,
  DESIGNER_NODE_TYPE_IMAGE,
  DESIGNER_NODE_TYPE_TABLE,
  DESIGNER_NODE_TYPE_TEXT,
  DESIGNER_NODE_TYPE_VIDEO,
} from '../../executionGraphTypes';
import type { DesignerReactFlowNode } from '../../designerGraphAdapter';
import { isMediaNodeType } from '../../mediaNodeConfig';
import { DesignerNodeToolbar } from '../controls/DesignerNodeToolbar';

type DesignerNodeData = DesignerReactFlowNode['data'];
type DesignerFlowNode = Node<DesignerNodeData>;

function DesignerNodeShell({
  label,
  nodeType,
  body,
  media = false,
  selected = false,
  toolbar = null,
}: {
  label: string;
  nodeType: string;
  body: ReactNode;
  media?: boolean;
  selected?: boolean;
  toolbar?: ReactNode;
}) {
  return (
    <div
      className={`designer-node${media ? ' designer-node--media' : ''}${selected ? ' is-selected' : ''}`}
      data-testid="designer-node"
      data-selected={selected ? 'true' : 'false'}
    >
      <Handle type="target" position={Position.Left} />
      <div className="designer-node__header">
        <span className="designer-node__label">{label}</span>
        <span className="designer-node__type">{nodeType}</span>
      </div>
      <div className="designer-node__body">{body}</div>
      <Handle type="source" position={Position.Right} />
      {toolbar}
    </div>
  );
}

export function DesignerTextNode({ data, selected }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  return (
    <DesignerNodeShell
      label={nodeData.label}
      nodeType={DESIGNER_NODE_TYPE_TEXT}
      selected={selected}
      body={nodeData.config.prompt ? String(nodeData.config.prompt).slice(0, 80) : 'Text node'}
    />
  );
}

export function DesignerTableNode({ data, selected }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  return (
    <DesignerNodeShell
      label={nodeData.label}
      nodeType={DESIGNER_NODE_TYPE_TABLE}
      selected={selected}
      body="Table preview"
    />
  );
}

export function DesignerMediaNode({ id, data, selected }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  const nodeType = nodeData.nodeType;
  const previewLabel =
    nodeType === DESIGNER_NODE_TYPE_VIDEO
      ? 'Video preview'
      : nodeType === DESIGNER_NODE_TYPE_AUDIO
        ? 'Audio preview'
        : 'Image preview';

  const toolbar = isMediaNodeType(nodeType) ? (
    <NodeToolbar
      isVisible={selected}
      position={Position.Bottom}
      offset={16}
      align="center"
      className="designer-node-toolbar-portal"
    >
      <DesignerNodeToolbar nodeId={id} nodeType={nodeType} />
    </NodeToolbar>
  ) : null;

  return (
    <DesignerNodeShell
      label={nodeData.label}
      nodeType={nodeType}
      media={isMediaNodeType(nodeType)}
      selected={selected}
      body={previewLabel}
      toolbar={toolbar}
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
