import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  addEdge as appendReactFlowEdge,
  useEdgesState,
  useNodesState,
  useOnSelectionChange,
  useReactFlow,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type OnConnect,
  type OnEdgesChange,
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
import { designerEdgeTypes } from './edges/DesignerEdge';
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

function toDesignerEdges(edges: Edge[]): DesignerReactFlowEdge[] {
  return edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    type: edge.type ?? 'designer',
    label: typeof edge.label === 'string' ? edge.label : undefined,
  }));
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
  const [edges, setEdges, onEdgesChangeBase] = useEdgesState(reactFlowGraph.edges as Edge[]);
  const { fitView } = useReactFlow();
  const persistReactFlowLayout = useDesignerStore((state) => state.persistReactFlowLayout);
  const addDomainEdge = useDesignerStore((state) => state.addEdge);
  const removeEdges = useDesignerStore((state) => state.removeEdges);
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
    setEdges((previous) => {
      const selectedIds = new Set(previous.filter((edge) => edge.selected).map((edge) => edge.id));
      return reactFlowGraph.edges.map((edge) => ({
        ...(edge as Edge),
        selected: selectedIds.has(edge.id),
      }));
    });

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
      persistReactFlowLayout(toPersistableGraph(currentNodes, toDesignerEdges(edges)));
    },
    [edges, persistReactFlowLayout],
  );

  const onConnect: OnConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target) return;
      const source = connection.source;
      const target = connection.target;
      if (edges.some((edge) => edge.source === source && edge.target === target)) {
        return;
      }
      const id = `e_${source}_${target}_${Date.now().toString(36)}`;
      setEdges((current) =>
        appendReactFlowEdge(
          {
            ...connection,
            id,
            type: 'designer',
          },
          current,
        ),
      );
      addDomainEdge({ id, source, target });
    },
    [addDomainEdge, edges, setEdges],
  );

  const onEdgesChange: OnEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      onEdgesChangeBase(changes);
      const removedIds = changes
        .filter((change): change is Extract<EdgeChange, { type: 'remove' }> => change.type === 'remove')
        .map((change) => change.id);
      if (removedIds.length > 0) {
        removeEdges(removedIds);
      }
    },
    [onEdgesChangeBase, removeEdges],
  );

  return (
    <ReactFlow
      className="designer-page__canvas"
      nodes={nodes}
      edges={edges}
      nodeTypes={designerNodeTypes}
      edgeTypes={designerEdgeTypes}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
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
