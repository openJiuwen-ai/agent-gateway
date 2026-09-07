import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { saveDesignerTextFile } from './designerAssetUrl';
import {
  DESIGNER_MATERIAL_SAVED_EVENT,
  isEditableDesignerMaterial,
  type DesignerMaterial,
} from './designerMaterials';

type DesignerTextEditorProps = {
  material: DesignerMaterial;
  compact?: boolean;
  showStartButton?: boolean;
  startEditKey?: number;
  onEditingChange?: (editing: boolean) => void;
};

export function DesignerTextEditor({
  material,
  compact = false,
  showStartButton = true,
  startEditKey = 0,
  onEditingChange,
}: DesignerTextEditorProps) {
  const { t } = useTranslation();
  const editable = isEditableDesignerMaterial(material);
  const [text, setText] = useState('');
  const [draft, setDraft] = useState('');
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [reloadAt, setReloadAt] = useState(0);
  const onEditingChangeRef = useRef(onEditingChange);
  onEditingChangeRef.current = onEditingChange;

  useEffect(() => {
    setText('');
    setDraft('');
    setEditing(false);
    setError('');
    onEditingChangeRef.current?.(false);
  }, [material.id]);

  useEffect(() => {
    if (!material.textUrl) return;
    let cancelled = false;
    const url = `${material.textUrl}${material.textUrl.includes('?') ? '&' : '?'}t=${reloadAt}`;
    setError('');
    void fetch(url)
      .then((response) => {
        if (!response.ok) throw new Error(String(response.status));
        return response.text();
      })
      .then((value) => {
        if (cancelled) return;
        setText(value);
        setDraft(value);
      })
      .catch(() => {
        if (!cancelled) setError(t('designer.materials.textError'));
      });
    return () => {
      cancelled = true;
    };
  }, [material.textUrl, reloadAt, t]);

  const startEdit = () => {
    setDraft(text);
    setEditing(true);
    setError('');
    onEditingChange?.(true);
  };

  useEffect(() => {
    if (!startEditKey || !editable) return;
    startEdit();
    // startEdit reads current text; only re-run when the viewer asks again.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [startEditKey]);

  const cancelEdit = () => {
    setDraft(text);
    setEditing(false);
    setError('');
    onEditingChange?.(false);
  };

  const saveEdit = async () => {
    setSaving(true);
    setError('');
    try {
      await saveDesignerTextFile(material.uri, draft);
      setText(draft);
      setEditing(false);
      setReloadAt(Date.now());
      onEditingChange?.(false);
      window.dispatchEvent(
        new CustomEvent(DESIGNER_MATERIAL_SAVED_EVENT, { detail: { uri: material.uri } }),
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t('designer.materials.saveFailed'));
    } finally {
      setSaving(false);
    }
  };

  if (!material.textUrl) return <p>{t('designer.materials.placeholderHint')}</p>;
  if (error && !text && !editing) return <p>{error}</p>;

  return (
    <div
      className={
        compact
          ? 'designer-text-editor designer-text-editor--compact'
          : 'designer-text-editor'
      }
      data-testid="designer-text-editor"
    >
      {editable && (editing || showStartButton) ? (
        <div className="designer-text-editor__toolbar">
          {editing ? (
            <>
              <button
                type="button"
                className="btn primary"
                disabled={saving || draft === text}
                onClick={() => void saveEdit()}
                data-testid="designer-text-save"
              >
                {saving ? t('designer.materials.saving') : t('designer.materials.save')}
              </button>
              <button
                type="button"
                className="btn"
                disabled={saving}
                onClick={cancelEdit}
                data-testid="designer-text-cancel"
              >
                {t('designer.materials.cancelEdit')}
              </button>
            </>
          ) : (
            <button
              type="button"
              className="btn"
              onClick={startEdit}
              data-testid="designer-text-edit"
            >
              {t('designer.materials.edit')}
            </button>
          )}
        </div>
      ) : null}
      {editing ? (
        <textarea
          className="designer-text-editor__textarea"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Escape') {
              event.stopPropagation();
              cancelEdit();
            }
            if ((event.ctrlKey || event.metaKey) && event.key === 's') {
              event.preventDefault();
              event.stopPropagation();
              if (!saving && draft !== text) void saveEdit();
            }
          }}
          spellCheck={false}
          data-testid="designer-text-draft"
        />
      ) : (
        <pre className="designer-text-editor__text">{text}</pre>
      )}
      {error && editing ? <p className="designer-text-editor__error">{error}</p> : null}
    </div>
  );
}
