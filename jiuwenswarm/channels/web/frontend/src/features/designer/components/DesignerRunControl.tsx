import { ChevronDown, Loader2, Pause, Play, RefreshCcwDot, RotateCcw, RotateCw, SkipForward, Square } from 'lucide-react';
import { useCallback, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useClickOutside } from '../../../components/CronPanel/useClickOutside';
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
  const runStatus = useDesignerRunStore((state) => state.run?.status ?? null);
  const advance = useDesignerRunStore((state) => state.advance);
  const rerunCurrentLayer = useDesignerRunStore((state) => state.rerunCurrentLayer);
  const restart = useDesignerRunStore((state) => state.restart);
  const pause = useDesignerRunStore((state) => state.pause);
  const cancel = useDesignerRunStore((state) => state.cancel);

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
  const menuWrapRef = useRef<HTMLDivElement>(null);
  const closeMenu = useCallback(() => setMenuOpen(false), []);
  useClickOutside(menuWrapRef, menuOpen, closeMenu);

  const controlDisabled = disabled || !graph || graph.nodes.length === 0;
  const busy = isRunning;
  const canPause = busy;
  const canCancel =
    !controlDisabled &&
    (busy ||
      runStatus === 'running' ||
      runStatus === 'paused' ||
      currentLayerNodeIds.length > 0);

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

  const onPause = useCallback(() => {
    if (!canPause) return;
    pause();
  }, [canPause, pause]);

  const onCancel = useCallback(() => {
    if (!canCancel) return;
    cancel(graph);
  }, [canCancel, cancel, graph]);

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
      className={`designer-run-control${controlDisabled ? ' is-disabled' : ''}${busy ? ' is-busy' : ''}`}
      data-testid="designer-run-control"
      data-primary-action={primaryAction}
    >
      <div ref={menuWrapRef} className="designer-run-control__primary-wrap">
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
              <RotateCw size={14} aria-hidden />
              <span>{t('designer.run.rerunCurrent')}</span>
            </button>
            <button
              type="button"
              role="menuitem"
              className="designer-run-control__menu-item designer-run-control__menu-item--warning"
              disabled={busy}
              onClick={onRestart}
              data-testid="designer-run-control-restart"
            >
              <RefreshCcwDot size={14} aria-hidden />
              <span>{t('designer.run.restart')}</span>
            </button>
          </div>
        ) : null}
      </div>

      <button
        type="button"
        className="designer-run-control__secondary"
        disabled={!canPause}
        onClick={onPause}
        data-testid="designer-run-control-pause"
      >
        <Pause size={14} aria-hidden />
        <span>{t('designer.run.pause')}</span>
      </button>
      <button
        type="button"
        className="designer-run-control__secondary"
        disabled={!canCancel}
        onClick={onCancel}
        data-testid="designer-run-control-cancel"
      >
        <Square size={14} aria-hidden />
        <span>{t('designer.run.cancel')}</span>
      </button>
    </div>
  );
}
