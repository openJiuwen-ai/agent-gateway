import {
  FileText,
  Headphones,
  Image as ImageIcon,
  Loader2,
  Sheet,
  Video,
  type LucideIcon,
} from 'lucide-react';
import type { ReactNode } from 'react';
import { Handle, NodeToolbar, Position, type Node, type NodeProps } from '@xyflow/react';
import {
  DESIGNER_FAKE_TABLE,
  DESIGNER_FAKE_TEXT,
  getCachedFakeImageUrl,
  getCachedFakeVideoUrl,
} from '../../designerFakeAssets';
import {
  DESIGNER_NODE_STATUS_COMPLETED,
  DESIGNER_NODE_STATUS_FAILED,
  DESIGNER_NODE_STATUS_RUNNING,
  DESIGNER_NODE_TYPE_AUDIO,
  DESIGNER_NODE_TYPE_IMAGE,
  DESIGNER_NODE_TYPE_TABLE,
  DESIGNER_NODE_TYPE_TEXT,
  DESIGNER_NODE_TYPE_VIDEO,
} from '../../executionGraphTypes';
import type { DesignerReactFlowNode } from '../../designerGraphAdapter';
import { useDesignerRunStore } from '../../designerRunStore';
import { useDesignerStore } from '../../designerStore';
import { isMediaNodeType, supportsNodeToolbar } from '../../mediaNodeConfig';
import { DesignerNodeToolbar } from '../controls/DesignerNodeToolbar';

type DesignerNodeData = DesignerReactFlowNode['data'];
type DesignerFlowNode = Node<DesignerNodeData>;

function modalityIcon(nodeType: string): LucideIcon {
  switch (nodeType) {
    case DESIGNER_NODE_TYPE_TABLE:
      return Sheet;
    case DESIGNER_NODE_TYPE_IMAGE:
      return ImageIcon;
    case DESIGNER_NODE_TYPE_VIDEO:
      return Video;
    case DESIGNER_NODE_TYPE_AUDIO:
      return Headphones;
    case DESIGNER_NODE_TYPE_TEXT:
    default:
      return FileText;
  }
}

function PlaceholderBody({ nodeType }: { nodeType: string }) {
  const Icon = modalityIcon(nodeType);
  return (
    <span className="designer-node__placeholder" data-testid="designer-node-placeholder" data-node-type={nodeType}>
      <Icon className="designer-node__placeholder-icon" size={40} strokeWidth={1.5} aria-hidden />
    </span>
  );
}

function DesignerNodeShell({
  nodeId,
  label,
  nodeType,
  body,
  media = false,
  mediaFilled = false,
  selected = false,
  toolbar = null,
}: {
  nodeId: string;
  label: string;
  nodeType: string;
  body: ReactNode;
  media?: boolean;
  mediaFilled?: boolean;
  selected?: boolean;
  toolbar?: ReactNode;
}) {
  const status = useDesignerRunStore(
    (state) => state.nodeStates[nodeId]?.status ?? 'pending',
  );
  const statusClass =
    status === DESIGNER_NODE_STATUS_RUNNING
      ? ' is-running'
      : status === DESIGNER_NODE_STATUS_COMPLETED
        ? ' is-completed'
        : status === DESIGNER_NODE_STATUS_FAILED
          ? ' is-failed'
          : '';
  const TypeIcon = modalityIcon(nodeType);
  const showMediaFill = media && (mediaFilled || status === DESIGNER_NODE_STATUS_COMPLETED);

  return (
    <div
      className={`designer-node${media ? ' designer-node--media' : ''}${showMediaFill ? ' designer-node--media-filled' : ''}${selected ? ' is-selected' : ''}${statusClass} group`}
      data-testid="designer-node"
      data-selected={selected ? 'true' : 'false'}
      data-status={status}
    >
      <Handle type="target" position={Position.Left} className='size-2 bg-gray-500 transition-all ease-out group-hover:size-3' />
      <div className="designer-node__header">
        <span className="designer-node__type-icon" aria-hidden data-testid="designer-node-type-icon" data-node-type={nodeType}>
          <TypeIcon size={14} strokeWidth={1.75} />
        </span>
        <span className="designer-node__label">{label}</span>
      </div>
      <div className="designer-node__body">{body}</div>
      <Handle type="source" position={Position.Right} className='size-2 bg-gray-500 transition-all ease-out group-hover:size-3' />
      {toolbar}
    </div>
  );
}

function RunningBody() {
  return (
    <span className="designer-node__running" data-testid="designer-node-running">
      <Loader2 className="designer-node__running-icon" size={22} aria-hidden />
    </span>
  );
}

function FakeTableBody() {
  return (
    <table className="designer-node__table" data-testid="designer-node-fake-table">
      <thead>
        <tr>
          {DESIGNER_FAKE_TABLE.headers.map((header) => (
            <th key={header}>{header}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {DESIGNER_FAKE_TABLE.rows.map((row) => (
          <tr key={row.join('|')}>
            {row.map((cell) => (
              <td key={`${row[0]}-${cell}`}>{cell}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function DesignerTextNode({ id, data, selected }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  const status = useDesignerRunStore((state) => state.nodeStates[id]?.status ?? 'pending');
  const body =
    status === DESIGNER_NODE_STATUS_RUNNING ? (
      <RunningBody />
    ) : status === DESIGNER_NODE_STATUS_COMPLETED ? (
      <p className="designer-node__text" data-testid="designer-node-fake-text">
        {DESIGNER_FAKE_TEXT}
      </p>
    ) : (
      <PlaceholderBody nodeType={DESIGNER_NODE_TYPE_TEXT} />
    );

  const toolbar = (
    <NodeToolbar
      isVisible={selected}
      position={Position.Bottom}
      offset={16}
      align="center"
      className="designer-node-toolbar-portal"
    >
      <DesignerNodeToolbar nodeId={id} nodeType={DESIGNER_NODE_TYPE_TEXT} />
    </NodeToolbar>
  );

  return (
    <DesignerNodeShell
      nodeId={id}
      label={nodeData.label}
      nodeType={DESIGNER_NODE_TYPE_TEXT}
      selected={selected}
      body={body}
      toolbar={toolbar}
    />
  );
}

export function DesignerTableNode({ id, data, selected }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  const status = useDesignerRunStore((state) => state.nodeStates[id]?.status ?? 'pending');
  const body =
    status === DESIGNER_NODE_STATUS_RUNNING ? (
      <RunningBody />
    ) : status === DESIGNER_NODE_STATUS_COMPLETED ? (
      <FakeTableBody />
    ) : (
      <PlaceholderBody nodeType={DESIGNER_NODE_TYPE_TABLE} />
    );

  return (
    <DesignerNodeShell
      nodeId={id}
      label={nodeData.label}
      nodeType={DESIGNER_NODE_TYPE_TABLE}
      selected={selected}
      body={body}
    />
  );
}

export function DesignerMediaNode({ id, data, selected }: NodeProps<DesignerFlowNode>) {
  const nodeData = data as DesignerNodeData;
  const nodeType = nodeData.nodeType;
  const status = useDesignerRunStore((state) => state.nodeStates[id]?.status ?? 'pending');
  const outputUri = useDesignerRunStore(
    (state) => state.nodeStates[id]?.output_ref?.uri ?? null,
  );
  const domainOutputUri = useDesignerStore(
    (state) => state.domainGraph?.nodes.find((node) => node.id === id)?.output_ref?.uri ?? null,
  );
  const previewUri = outputUri || domainOutputUri;
  const hasPreview = Boolean(previewUri);
  const showCompletedMedia =
    status === DESIGNER_NODE_STATUS_COMPLETED || hasPreview;

  let body: ReactNode;
  if (status === DESIGNER_NODE_STATUS_RUNNING) {
    body = <RunningBody />;
  } else if (showCompletedMedia && nodeType === DESIGNER_NODE_TYPE_IMAGE) {
    const src = previewUri || getCachedFakeImageUrl();
    body = src ? (
      <img
        className="designer-node__media-preview"
        src={src}
        alt={nodeData.label}
        data-testid="designer-node-fake-image"
      />
    ) : (
      <PlaceholderBody nodeType={nodeType} />
    );
  } else if (showCompletedMedia && nodeType === DESIGNER_NODE_TYPE_VIDEO) {
    const src = previewUri || getCachedFakeVideoUrl();
    body = src ? (
      <video
        className="designer-node__media-preview"
        src={src}
        playsInline
        controls={true}
        autoPlay={false}
        data-testid="designer-node-fake-video"
      />
    ) : (
      <PlaceholderBody nodeType={nodeType} />
    );
  } else if (showCompletedMedia && nodeType === DESIGNER_NODE_TYPE_AUDIO) {
    body = previewUri ? (
      <audio
        className="designer-node__audio-preview"
        src={previewUri}
        controls
        data-testid="designer-node-uploaded-audio"
      />
    ) : (
      <PlaceholderBody nodeType={DESIGNER_NODE_TYPE_AUDIO} />
    );
  } else {
    body = <PlaceholderBody nodeType={nodeType} />;
  }

  const toolbar = supportsNodeToolbar(nodeType) ? (
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
      nodeId={id}
      label={nodeData.label}
      nodeType={nodeType}
      media={isMediaNodeType(nodeType)}
      mediaFilled={hasPreview || status === DESIGNER_NODE_STATUS_COMPLETED}
      selected={selected}
      body={body}
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
