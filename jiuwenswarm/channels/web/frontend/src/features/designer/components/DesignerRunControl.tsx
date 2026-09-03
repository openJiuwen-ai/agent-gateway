import { ChevronDown, Loader2, Play, RotateCcw, SkipForward } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { derivePrimaryAction } from '../designerLayerRun';
import type { DesignerExecutionGraph } from '../executionGraphTypes';
import { useDesignerRunStore } from '../designerRunStore';

type DesignerRunControlProps = {
  graph: DesignerExecutionGraph | null;
  disabled?: boolean;
};

export function DesignerRunControl({ graph, disabled = false }: DesignerRunControlProps) {
  const { t } = useTranslation();
  const isRunning = useDesignerRunStore((state) => state.isRunning);
  const nodeStates = useDesignerRunStore((state) => state.nodeStates);
  const currentLayerNodeIds = useDesignerRunStore((state) => state.currentLayerNodeIds);
  const advance = useDesignerRunStore((state) => state.advance);
  const rerunCurrentLayer = useDesignerRunStore((state) => state.rerunCurrentLayer);
  const restart = useDesignerRunStore((state) => state.restart);

  const primaryAction = useMemo(
    () =>
      derivePrimaryAction({
        graph,
        nodeStates,
        currentLayerNodeIds,
        isRunning,
      }),
    [currentLayerNodeIds, graph, isRunning, nodeStates],
  );

  const [menuOpen, setMenuOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  const controlDisabled = disabled || !graph || graph.nodes.length === 0;
  const busy = isRunning;

  useEffect(() => {
    if (!menuOpen) return;
    const onPointerDown = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', onPointerDown);
    return () => document.removeEventListener('mousedown', onPointerDown);
  }, [menuOpen]);

  const onPrimary = useCallback(() => {
    if (!graph || controlDisabled || busy) return;
    void advance(graph);
  }, [advance, busy, controlDisabled, graph]);

  const onRerunCurrent = useCallback(() => {
    if (!graph || controlDisabled || busy) return;
    setMenuOpen(false);
    void rerunCurrentLayer(graph);
  }, [busy, controlDisabled, graph, rerunCurrentLayer]);

  const onRestart = useCallback(() => {
    if (!graph || controlDisabled || busy) return;
    setMenuOpen(false);
    void restart(graph);
  }, [busy, controlDisabled, graph, restart]);

  const primaryLabel =
    primaryAction === 'continue'
      ? t('designer.run.continue')
      : primaryAction === 'retry_failed'
        ? t('designer.run.retryFailed')
        : primaryAction === 'running'
          ? t('designer.run.running')
          : t('designer.run.execute');

  const PrimaryIcon =
    primaryAction === 'continue'
      ? SkipForward
      : primaryAction === 'retry_failed'
        ? RotateCcw
        : Play;

  const canRerunCurrent = !controlDisabled && !busy && currentLayerNodeIds.length > 0;

  return (
    <div
      ref={rootRef}
      className={`designer-run-control${controlDisabled ? ' is-disabled' : ''}${busy ? ' is-busy' : ''}`}
      data-testid="designer-run-control"
      data-primary-action={primaryAction}
    >
      <div className="designer-run-control__group" role="group" aria-label={t('designer.run.controlLabel')}>
        <button
          type="button"
          className="designer-run-control__primary"
          disabled={controlDisabled || busy}
          onClick={onPrimary}
          data-testid="designer-run-control-primary"
        >
          {busy ? (
            <Loader2 className="designer-run-control__spin" size={14} aria-hidden />
          ) : (
            <PrimaryIcon size={14} aria-hidden />
          )}
          <span>{primaryLabel}</span>
        </button>
        <button
          type="button"
          className="designer-run-control__menu-trigger"
          disabled={controlDisabled || busy}
          aria-haspopup="menu"
          aria-expanded={menuOpen}
          onClick={() => setMenuOpen((open) => !open)}
          data-testid="designer-run-control-menu-trigger"
          aria-label={t('designer.run.moreActions')}
        >
          <ChevronDown size={14} aria-hidden />
        </button>
      </div>

      {menuOpen && !controlDisabled ? (
        <div className="designer-run-control__menu" role="menu" data-testid="designer-run-control-menu">
          <button
            type="button"
            role="menuitem"
            className="designer-run-control__menu-item"
            disabled={!canRerunCurrent}
            onClick={onRerunCurrent}
            data-testid="designer-run-control-rerun-current"
          >
            {t('designer.run.rerunCurrent')}
          </button>
          <button
            type="button"
            role="menuitem"
            className="designer-run-control__menu-item"
            disabled={busy}
            onClick={onRestart}
            data-testid="designer-run-control-restart"
          >
            {t('designer.run.restart')}
          </button>
        </div>
      ) : null}
    </div>
  );
}
