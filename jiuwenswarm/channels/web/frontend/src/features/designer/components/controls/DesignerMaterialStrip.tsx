import { Headphones, Image as ImageIcon, Plus, Video, X } from 'lucide-react';
import { useCallback, useMemo, useRef, type ChangeEvent, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useDesignerStore } from '../../designerStore';
import {
  DESIGNER_NODE_TYPE_AUDIO,
  DESIGNER_NODE_TYPE_VIDEO,
} from '../../executionGraphTypes';
import {
  isMediaNodeType,
  readMediaConfig,
  writeMediaMaterials,
  type MediaMaterialSlot,
} from '../../mediaNodeConfig';

type DesignerMaterialStripProps = {
  nodeId: string;
  nodeType: string;
};

type LinkedMaterial = {
  kind: 'linked';
  key: string;
  edgeId: string;
  sourceNodeId: string;
  label: string;
  mediaType: string;
};

type UploadMaterial = {
  kind: 'upload';
  key: string;
  materialId: string;
  label: string;
  mimeType?: string;
};

type DisplayMaterial = LinkedMaterial | UploadMaterial;

function mediaIcon(mediaTypeOrMime: string | undefined): ReactNode {
  const value = (mediaTypeOrMime ?? '').toLowerCase();
  if (value === DESIGNER_NODE_TYPE_VIDEO || value.startsWith('video/')) {
    return <Video size={20} aria-hidden />;
  }
  if (value === DESIGNER_NODE_TYPE_AUDIO || value.startsWith('audio/')) {
    return <Headphones size={20} aria-hidden />;
  }
  return <ImageIcon size={20} aria-hidden />;
}

function newMaterialId(): string {
  return `mat_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

export function DesignerMaterialStrip({ nodeId, nodeType }: DesignerMaterialStripProps) {
  const { t } = useTranslation();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const domainGraph = useDesignerStore((state) => state.domainGraph);
  const updateNodeConfig = useDesignerStore((state) => state.updateNodeConfig);
  const removeEdges = useDesignerStore((state) => state.removeEdges);

  const config = useMemo(() => {
    const node = domainGraph?.nodes.find((item) => item.id === nodeId);
    return node?.config ?? {};
  }, [domainGraph, nodeId]);

  const uploads = readMediaConfig(config, nodeType).materials ?? [];

  const linked = useMemo((): LinkedMaterial[] => {
    if (!domainGraph) return [];
    const nodesById = new Map(domainGraph.nodes.map((node) => [node.id, node]));
    const items: LinkedMaterial[] = [];
    for (const edge of domainGraph.edges) {
      if (edge.target !== nodeId) continue;
      const source = nodesById.get(edge.source);
      if (!source || !isMediaNodeType(String(source.type))) continue;
      items.push({
        kind: 'linked',
        key: `linked:${edge.id}`,
        edgeId: edge.id,
        sourceNodeId: source.id,
        label: source.label || source.id,
        mediaType: String(source.type),
      });
    }
    return items;
  }, [domainGraph, nodeId]);

  const materials = useMemo((): DisplayMaterial[] => {
    const uploaded: UploadMaterial[] = uploads.map((slot) => ({
      kind: 'upload',
      key: `upload:${slot.id}`,
      materialId: slot.id,
      label: slot.filename,
      mimeType: slot.mime_type,
    }));
    return [...linked, ...uploaded];
  }, [linked, uploads]);

  const addFiles = useCallback(
    (files: FileList | null) => {
      if (!files || files.length === 0) return;
      const next: MediaMaterialSlot[] = [...uploads];
      for (const file of Array.from(files)) {
        if (!file.type.startsWith('image/') && !file.type.startsWith('video/') && !file.type.startsWith('audio/')) {
          continue;
        }
        next.push({
          id: newMaterialId(),
          filename: file.name,
          mime_type: file.type || undefined,
        });
      }
      if (next.length === uploads.length) return;
      updateNodeConfig(nodeId, (current) => writeMediaMaterials(current, next));
    },
    [nodeId, updateNodeConfig, uploads],
  );

  const onFileChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      addFiles(event.target.files);
      event.target.value = '';
    },
    [addFiles],
  );

  const onRemove = useCallback(
    (item: DisplayMaterial) => {
      if (item.kind === 'linked') {
        removeEdges([item.edgeId]);
        return;
      }
      const next = uploads.filter((slot) => slot.id !== item.materialId);
      updateNodeConfig(nodeId, (current) => writeMediaMaterials(current, next));
    },
    [nodeId, removeEdges, updateNodeConfig, uploads],
  );

  return (
    <div className="designer-node-toolbar__materials" data-testid="designer-node-toolbar-materials">
      <input
        ref={fileInputRef}
        type="file"
        className="designer-node-toolbar__file-input"
        accept="image/*,video/*,audio/*"
        multiple
        data-testid="designer-node-toolbar-material-file-input"
        onChange={onFileChange}
      />

      {materials.map((item) => (
        <div
          key={item.key}
          className="designer-node-toolbar__material-item"
          data-testid="designer-node-toolbar-material"
          data-material-kind={item.kind}
        >
          <span className="designer-node-toolbar__material-label" title={item.label}>
            {item.label}
          </span>
          <div className="designer-node-toolbar__material-tile">
            {mediaIcon(item.kind === 'linked' ? item.mediaType : item.mimeType)}
            <button
              type="button"
              className="designer-node-toolbar__material-remove"
              aria-label={t('designer.toolbar.removeMaterial')}
              title={t('designer.toolbar.removeMaterial')}
              data-testid="designer-node-toolbar-material-remove"
              onClick={(event) => {
                event.stopPropagation();
                onRemove(item);
              }}
            >
              <X size={10} strokeWidth={2.5} aria-hidden />
            </button>
          </div>
        </div>
      ))}

      <div className="designer-node-toolbar__material-item designer-node-toolbar__material-item--add">
        <span className="designer-node-toolbar__material-label" aria-hidden>
          {'\u00a0'}
        </span>
        <button
          type="button"
          className="designer-node-toolbar__material-add"
          aria-label={t('designer.toolbar.addMaterial')}
          title={t('designer.toolbar.addMaterial')}
          data-testid="designer-node-toolbar-material-add"
          onClick={() => fileInputRef.current?.click()}
        >
          <Plus size={18} strokeWidth={2.25} aria-hidden />
        </button>
      </div>
    </div>
  );
}
