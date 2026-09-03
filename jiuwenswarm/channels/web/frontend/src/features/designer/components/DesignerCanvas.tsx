import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useOnSelectionChange,
  useReactFlow,
  type Node,
  type OnNodeDrag,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useCallback, useEffect, useMemo, useRef } from 'react';
import type { DesignerExecutionGraph } from '../executionGraphTypes';
import {
  toReactFlowGraph,
  type DesignerReactFlowEdge,
  type DesignerReactFlowNode,
} from '../designerGraphAdapter';
import { useDesignerStore } from '../designerStore';
import { designerNodeTypes } from './nodes/designerNodes';

type DesignerCanvasProps = {
  graph: DesignerExecutionGraph;
};

function toPersistableGraph(
  nodes: Node[],
  edges: DesignerReactFlowEdge[],
): { nodes: DesignerReactFlowNode[]; edges: DesignerReactFlowEdge[] } {
  return {
    nodes: nodes.map((node) => ({
      id: node.id,
      type: String(node.type ?? 'text'),
      position: node.position,
      style: node.style as DesignerReactFlowNode['style'],
      data: node.data as DesignerReactFlowNode['data'],
    })),
    edges,
  };
}

/** Sync RF view when graph identity / topology / layout changes — not on config-only edits. */
function buildLayoutSyncKey(graph: DesignerExecutionGraph): string {
  const nodesKey = graph.nodes
    .map((node) => {
      const layout = node.layout ?? {};
      return `${node.id}:${node.type}:${layout.x ?? 0}:${layout.y ?? 0}:${layout.width ?? 0}:${layout.height ?? 0}`;
    })
    .join(';');
  const edgesKey = graph.edges.map((edge) => `${edge.id}:${edge.source}->${edge.target}`).join(';');
  return `${graph.graph_id}|${nodesKey}|${edgesKey}`;
}

function DesignerCanvasInner({ graph }: DesignerCanvasProps) {
  const layoutSyncKey = useMemo(() => buildLayoutSyncKey(graph), [graph]);
  const graphRef = useRef(graph);
  graphRef.current = graph;
  const reactFlowGraph = useMemo(
    () => toReactFlowGraph(graphRef.current),
    [layoutSyncKey],
  );
  const [nodes, setNodes, onNodesChange] = useNodesState(reactFlowGraph.nodes as Node[]);
  const [edges, setEdges, onEdgesChange] = useEdgesState(reactFlowGraph.edges);
  const { fitView } = useReactFlow();
  const persistReactFlowLayout = useDesignerStore((state) => state.persistReactFlowLayout);
  const setSelectedNodeId = useDesignerStore((state) => state.setSelectedNodeId);
  const fittedGraphIdRef = useRef<string | null>(null);

  useEffect(() => {
    setNodes((previous) => {
      const selectedIds = new Set(previous.filter((node) => node.selected).map((node) => node.id));
      return reactFlowGraph.nodes.map((node) => ({
        ...(node as Node),
        selected: selectedIds.has(node.id),
      }));
    });
    setEdges(reactFlowGraph.edges);

    if (fittedGraphIdRef.current !== graph.graph_id) {
      fittedGraphIdRef.current = graph.graph_id;
      requestAnimationFrame(() => {
        void fitView({ padding: 0.2, duration: 200 });
      });
    }
  }, [graph.graph_id, reactFlowGraph, setNodes, setEdges, fitView]);

  useOnSelectionChange({
    onChange: ({ nodes: selectedNodes }) => {
      const only = selectedNodes.length === 1 ? selectedNodes[0] : null;
      setSelectedNodeId(only?.id ?? null);
    },
  });

  const onNodeDragStop: OnNodeDrag = useCallback(
    (_event, _node, currentNodes) => {
      persistReactFlowLayout(toPersistableGraph(currentNodes, edges));
    },
    [edges, persistReactFlowLayout],
  );

  return (
    <ReactFlow
      className="designer-page__canvas"
      nodes={nodes}
      edges={edges}
      nodeTypes={designerNodeTypes}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onNodeDragStop={onNodeDragStop}
      fitView
      minZoom={0.2}
      maxZoom={1.5}
      proOptions={{ hideAttribution: true }}
      data-testid="designer-canvas"
    >
      <Background gap={20} size={1} />
      <Controls showInteractive={false} />
      <MiniMap pannable zoomable />
    </ReactFlow>
  );
}

export function DesignerCanvas({ graph }: DesignerCanvasProps) {
  return (
    <ReactFlowProvider>
      <DesignerCanvasInner graph={graph} />
    </ReactFlowProvider>
  );
}
