import { Headphones, Image as ImageIcon, Trash2, Video } from 'lucide-react';
import { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import {
  useDesignerAssetLibraryStore,
  type DesignerLibraryAsset,
} from '../designerAssetLibraryStore';
import { useDesignerRunStore } from '../designerRunStore';
import { useDesignerStore } from '../designerStore';

function formatBytes(size: number): string {
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function AssetKindIcon({ asset }: { asset: DesignerLibraryAsset }) {
  if (asset.kind === 'video') return <Video size={18} aria-hidden />;
  if (asset.kind === 'audio') return <Headphones size={18} aria-hidden />;
  return <ImageIcon size={18} aria-hidden />;
}

export function DesignerAssetsPanel() {
  const { t } = useTranslation();
  const assets = useDesignerAssetLibraryStore((state) => state.assets);
  const removeAsset = useDesignerAssetLibraryStore((state) => state.removeAsset);
  const domainGraph = useDesignerStore((state) => state.domainGraph);
  const clearAssetReferences = useDesignerStore((state) => state.clearAssetReferences);
  const clearUploadedOutput = useDesignerRunStore((state) => state.clearUploadedOutput);

  const canvasAssetIds = useMemo(() => {
    const ids = new Set<string>();
    if (!domainGraph) return ids;
    for (const node of domainGraph.nodes) {
      const upload = node.config?.upload as { asset_id?: string } | undefined;
      if (upload?.asset_id) ids.add(upload.asset_id);
      const materials = node.config?.materials;
      if (Array.isArray(materials)) {
        for (const item of materials) {
          if (item && typeof item === 'object' && typeof (item as { asset_id?: string }).asset_id === 'string') {
            ids.add((item as { asset_id: string }).asset_id);
          }
        }
      }
    }
    return ids;
  }, [domainGraph]);

  const onDelete = useCallback(
    (assetId: string) => {
      const affectedNodeIds: string[] = [];
      if (domainGraph) {
        for (const node of domainGraph.nodes) {
          const upload = node.config?.upload as { asset_id?: string } | undefined;
          if (upload?.asset_id === assetId) {
            affectedNodeIds.push(node.id);
          }
        }
      }
      clearAssetReferences(assetId);
      for (const nodeId of affectedNodeIds) {
        clearUploadedOutput(nodeId);
      }
      removeAsset(assetId);
    },
    [clearAssetReferences, clearUploadedOutput, domainGraph, removeAsset],
  );

  if (assets.length === 0) {
    return (
      <div className="designer-assets-panel" data-testid="designer-assets-panel">
        <p className="designer-assets-panel__empty">{t('designer.assets.empty')}</p>
      </div>
    );
  }

  return (
    <div className="designer-assets-panel" data-testid="designer-assets-panel">
      <ul className="designer-assets-panel__list">
        {assets.map((asset) => {
          const onCanvas = canvasAssetIds.has(asset.id);
          return (
            <li
              key={asset.id}
              className="designer-assets-panel__item"
              data-testid="designer-assets-panel-item"
              data-asset-id={asset.id}
            >
              <div className="designer-assets-panel__thumb">
                {asset.kind === 'image' ? (
                  <img src={asset.objectUrl} alt="" />
                ) : (
                  <AssetKindIcon asset={asset} />
                )}
              </div>
              <div className="designer-assets-panel__meta">
                <span className="designer-assets-panel__name" title={asset.filename}>
                  {asset.filename}
                </span>
                <span className="designer-assets-panel__sub">
                  {formatBytes(asset.size)}
                  {' · '}
                  {onCanvas ? t('designer.assets.onCanvas') : t('designer.assets.libraryOnly')}
                </span>
              </div>
              <button
                type="button"
                className="designer-assets-panel__delete"
                aria-label={t('designer.assets.delete')}
                title={t('designer.assets.delete')}
                data-testid="designer-assets-panel-delete"
                onClick={() => onDelete(asset.id)}
              >
                <Trash2 size={14} aria-hidden />
              </button>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
