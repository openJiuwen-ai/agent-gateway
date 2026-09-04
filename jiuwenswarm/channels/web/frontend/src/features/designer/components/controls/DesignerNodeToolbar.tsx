import { useCallback, useRef, useState, type ChangeEvent, type DragEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { useDesignerStore } from '../../designerStore';
import {
  DESIGNER_NODE_TYPE_TEXT,
} from '../../executionGraphTypes';
import {
  readMediaConfig,
  writeMediaEditPatch,
  writeMediaGeneratePatch,
  writeMediaInteractionMode,
  writeMediaUploadPatch,
  type MediaInteractionMode,
} from '../../mediaNodeConfig';
import { DesignerMaterialStrip } from './DesignerMaterialStrip';

type DesignerNodeToolbarProps = {
  nodeId: string;
  nodeType: string;
};

function notifyNotImplemented(message: string) {
  window.alert(message);
}

export function DesignerNodeToolbar({ nodeId, nodeType }: DesignerNodeToolbarProps) {
  const { t } = useTranslation();
  const isTextNode = nodeType === DESIGNER_NODE_TYPE_TEXT;
  const updateNodeConfig = useDesignerStore((state) => state.updateNodeConfig);
  const config = useDesignerStore(
    (state) => state.domainGraph?.nodes.find((node) => node.id === nodeId)?.config ?? {},
  );
  const media = readMediaConfig(config, nodeType);
  const mode: MediaInteractionMode = media.interaction_mode ?? 'generate';
  const secondaryMode: MediaInteractionMode = isTextNode ? 'edit' : 'upload';
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  const setMode = useCallback(
    (next: MediaInteractionMode) => {
      updateNodeConfig(nodeId, (current) => writeMediaInteractionMode(current, next));
    },
    [nodeId, updateNodeConfig],
  );

  const patchGenerate = useCallback(
    (patch: Parameters<typeof writeMediaGeneratePatch>[1]) => {
      updateNodeConfig(nodeId, (current) => writeMediaGeneratePatch(current, patch));
    },
    [nodeId, updateNodeConfig],
  );

  const patchEdit = useCallback(
    (content: string) => {
      updateNodeConfig(nodeId, (current) => writeMediaEditPatch(current, { content }));
    },
    [nodeId, updateNodeConfig],
  );

  const applyUploadFile = useCallback(
    (file: File | undefined) => {
      if (!file) return;
      updateNodeConfig(nodeId, (current) =>
        writeMediaUploadPatch(current, { filename: file.name }),
      );
    },
    [nodeId, updateNodeConfig],
  );

  const onFileChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      applyUploadFile(event.target.files?.[0]);
      event.target.value = '';
    },
    [applyUploadFile],
  );

  const onDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      event.stopPropagation();
      setDragging(false);
      applyUploadFile(event.dataTransfer.files?.[0]);
    },
    [applyUploadFile],
  );

  return (
    <div
      className="designer-node-toolbar"
      data-testid="designer-node-toolbar"
      data-node-type={nodeType}
      data-mode={mode}
      onClick={(event) => event.stopPropagation()}
      onMouseDown={(event) => event.stopPropagation()}
    >
      <div className="designer-node-toolbar__tabs" role="tablist" aria-label={t('designer.toolbar.modeLabel')}>
        <button
          type="button"
          role="tab"
          aria-selected={mode === secondaryMode}
          className={`designer-node-toolbar__tab${mode === secondaryMode ? ' is-active' : ''}`}
          data-testid={isTextNode ? 'designer-node-toolbar-tab-edit' : 'designer-node-toolbar-tab-upload'}
          onClick={() => setMode(secondaryMode)}
        >
          {isTextNode ? t('designer.toolbar.edit') : t('designer.toolbar.upload')}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={mode === 'generate'}
          className={`designer-node-toolbar__tab${mode === 'generate' ? ' is-active' : ''}`}
          data-testid="designer-node-toolbar-tab-generate"
          onClick={() => setMode('generate')}
        >
          {t('designer.toolbar.generate')}
        </button>
      </div>

      {mode === 'generate' ? (
        <div
          className="designer-node-toolbar__panel"
          role="tabpanel"
          data-testid="designer-node-toolbar-panel-generate"
        >
          <DesignerMaterialStrip nodeId={nodeId} nodeType={nodeType} />
          <textarea
            className="designer-node-toolbar__prompt"
            value={media.generate?.prompt ?? ''}
            placeholder={t('designer.toolbar.promptPlaceholder')}
            rows={3}
            data-testid="designer-node-toolbar-prompt"
            onChange={(event) => patchGenerate({ prompt: event.target.value })}
          />
          {!isTextNode ? (
            <div className="designer-node-toolbar__params" data-testid="designer-node-toolbar-params">
              <label className="designer-node-toolbar__param">
                <span>{t('designer.toolbar.aspectRatio')}</span>
                <select
                  value={media.generate?.aspect_ratio ?? '16:9'}
                  onChange={(event) => patchGenerate({ aspect_ratio: event.target.value })}
                >
                  <option value="16:9">16:9</option>
                  <option value="9:16">9:16</option>
                  <option value="1:1">1:1</option>
                  <option value="4:3">4:3</option>
                </select>
              </label>
              <label className="designer-node-toolbar__param">
                <span>{t('designer.toolbar.resolution')}</span>
                <select
                  value={media.generate?.resolution ?? '1080p'}
                  onChange={(event) => patchGenerate({ resolution: event.target.value })}
                >
                  <option value="720p">720p</option>
                  <option value="1080p">1080p</option>
                  <option value="4k">4K</option>
                </select>
              </label>
              <label className="designer-node-toolbar__param">
                <span>{t('designer.toolbar.duration')}</span>
                <select
                  value={media.generate?.duration ?? '5s'}
                  onChange={(event) => patchGenerate({ duration: event.target.value })}
                >
                  <option value="3s">3s</option>
                  <option value="5s">5s</option>
                  <option value="10s">10s</option>
                </select>
              </label>
              <label className="designer-node-toolbar__param designer-node-toolbar__param--check">
                <input
                  type="checkbox"
                  checked={Boolean(media.generate?.has_audio)}
                  onChange={(event) => patchGenerate({ has_audio: event.target.checked })}
                />
                <span>{t('designer.toolbar.hasAudio')}</span>
              </label>
              <label className="designer-node-toolbar__param">
                <span>{t('designer.toolbar.count')}</span>
                <select
                  value={String(media.generate?.count ?? 1)}
                  onChange={(event) => patchGenerate({ count: Number(event.target.value) || 1 })}
                >
                  <option value="1">1</option>
                  <option value="2">2</option>
                  <option value="4">4</option>
                </select>
              </label>
            </div>
          ) : null}
          <button
            type="button"
            className="designer-node-toolbar__action"
            data-testid="designer-node-toolbar-generate-action"
            onClick={() => notifyNotImplemented(t('designer.toolbar.actionNotImplemented'))}
          >
            {t('designer.toolbar.generateAction')}
          </button>
        </div>
      ) : isTextNode ? (
        <div
          className="designer-node-toolbar__panel"
          role="tabpanel"
          data-testid="designer-node-toolbar-panel-edit"
        >
          <textarea
            className="designer-node-toolbar__prompt designer-node-toolbar__prompt--edit"
            value={media.edit?.content ?? ''}
            placeholder={t('designer.toolbar.editPlaceholder')}
            rows={6}
            data-testid="designer-node-toolbar-edit-content"
            onChange={(event) => patchEdit(event.target.value)}
          />
          <button
            type="button"
            className="designer-node-toolbar__action"
            data-testid="designer-node-toolbar-edit-action"
            onClick={() => notifyNotImplemented(t('designer.toolbar.actionNotImplemented'))}
          >
            {t('designer.toolbar.editAction')}
          </button>
        </div>
      ) : (
        <div
          className={`designer-node-toolbar__panel designer-node-toolbar__panel--upload${dragging ? ' is-dragging' : ''}`}
          role="tabpanel"
          data-testid="designer-node-toolbar-panel-upload"
          onDragEnter={(event) => {
            event.preventDefault();
            setDragging(true);
          }}
          onDragOver={(event) => {
            event.preventDefault();
            setDragging(true);
          }}
          onDragLeave={() => setDragging(false)}
          onDrop={onDrop}
        >
          <input
            ref={fileInputRef}
            type="file"
            className="designer-node-toolbar__file-input"
            accept={
              nodeType === 'audio'
                ? 'audio/*'
                : nodeType === 'video'
                  ? 'video/*'
                  : 'image/*'
            }
            data-testid="designer-node-toolbar-file-input"
            onChange={onFileChange}
          />
          <button
            type="button"
            className="designer-node-toolbar__browse"
            data-testid="designer-node-toolbar-browse"
            onClick={() => fileInputRef.current?.click()}
          >
            {t('designer.toolbar.browseFiles')}
          </button>
          <p className="designer-node-toolbar__drop-hint">{t('designer.toolbar.dropHint')}</p>
          {media.upload?.filename ? (
            <p className="designer-node-toolbar__filename" data-testid="designer-node-toolbar-filename">
              {media.upload.filename}
            </p>
          ) : null}
          <button
            type="button"
            className="designer-node-toolbar__action"
            data-testid="designer-node-toolbar-upload-action"
            onClick={() => notifyNotImplemented(t('designer.toolbar.actionNotImplemented'))}
          >
            {t('designer.toolbar.uploadAction')}
          </button>
        </div>
      )}
    </div>
  );
}
