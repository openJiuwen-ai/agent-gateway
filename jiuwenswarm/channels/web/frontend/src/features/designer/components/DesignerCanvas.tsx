import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useEffect, useMemo } from 'react';
import type { DesignerExecutionGraph } from '../executionGraphTypes';
import { toReactFlowGraph } from '../designerGraphAdapter';
import { designerNodeTypes } from './nodes/designerNodes';

type DesignerCanvasProps = {
  graph: DesignerExecutionGraph;
};

function DesignerCanvasInner({ graph }: DesignerCanvasProps) {
  const reactFlowGraph = useMemo(() => toReactFlowGraph(graph), [graph]);
  const [nodes, setNodes, onNodesChange] = useNodesState(reactFlowGraph.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(reactFlowGraph.edges);
  const { fitView } = useReactFlow();

  useEffect(() => {
    setNodes(reactFlowGraph.nodes);
    setEdges(reactFlowGraph.edges);
    requestAnimationFrame(() => {
      void fitView({ padding: 0.2, duration: 200 });
    });
  }, [reactFlowGraph, setNodes, setEdges, fitView]);

  return (
    <ReactFlow
      className="designer-page__canvas"
      nodes={nodes}
      edges={edges}
      nodeTypes={designerNodeTypes}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
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
