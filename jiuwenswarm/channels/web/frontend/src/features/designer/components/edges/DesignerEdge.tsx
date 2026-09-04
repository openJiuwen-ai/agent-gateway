import {
  BaseEdge,
  EdgeToolbar,
  getBezierPath,
  type Edge,
  type EdgeProps,
} from '@xyflow/react';
import { Sparkles, Trash2 } from 'lucide-react';
import { useCallback, type MouseEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { useDesignerRunStore } from '../../designerRunStore';
import { useDesignerStore } from '../../designerStore';

export type DesignerEdgeType = Edge<{ label?: string }>;

export function DesignerEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style,
  markerEnd,
  selected,
  target,
}: EdgeProps<DesignerEdgeType>) {
  const { t } = useTranslation();
  const removeEdges = useDesignerStore((state) => state.removeEdges);
  const domainGraph = useDesignerStore((state) => state.domainGraph);
  const isRunning = useDesignerRunStore((state) => state.isRunning);
  const runNodes = useDesignerRunStore((state) => state.runNodes);
  const [edgePath, centerX, centerY] = getBezierPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  });

  const onDelete = useCallback(
    (event: MouseEvent) => {
      event.stopPropagation();
      event.preventDefault();
      removeEdges([id]);
    },
    [id, removeEdges],
  );

  const onGenerate = useCallback(
    (event: MouseEvent) => {
      event.stopPropagation();
      event.preventDefault();
      if (!domainGraph || !target || isRunning) return;
      void runNodes(domainGraph, [target]);
    },
    [domainGraph, isRunning, runNodes, target],
  );

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={style}
        interactionWidth={24}
      />
      <EdgeToolbar
        edgeId={id}
        x={centerX}
        y={centerY}
        isVisible={Boolean(selected)}
        className="designer-edge-toolbar-portal"
        data-testid="designer-edge-toolbar"
      >
        <div className="designer-edge-toolbar" role="group">
          <button
            type="button"
            className="designer-edge-generate"
            aria-label={t('designer.edge.generate')}
            title={t('designer.edge.generate')}
            data-testid="designer-edge-generate"
            disabled={isRunning || !target}
            onClick={onGenerate}
            onMouseDown={(event) => event.stopPropagation()}
          >
            <Sparkles size={14} strokeWidth={2.25} aria-hidden />
          </button>
          <button
            type="button"
            className="designer-edge-delete"
            aria-label={t('designer.edge.delete')}
            title={t('designer.edge.delete')}
            data-testid="designer-edge-delete"
            onClick={onDelete}
            onMouseDown={(event) => event.stopPropagation()}
          >
            <Trash2 size={14} strokeWidth={2.25} aria-hidden />
          </button>
        </div>
      </EdgeToolbar>
    </>
  );
}

export const designerEdgeTypes = {
  designer: DesignerEdge,
};
