import { useMemo, useEffect, useCallback, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type NodeTypes,
  MarkerType,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import type { LineageGraph, LineageNode, LineageEdge } from "../types/graph";
import { getLayoutedElements } from "../utils/graphLayout";
import { TableNode } from "./TableNode";
import { Toolbar } from "./Toolbar";
import { ScanProgressBar } from "./ScanProgressBar";
import { NodeDetailPanel } from "./NodeDetailPanel";
import { EdgeDetailPanel } from "./EdgeDetailPanel";
import { ManualEdgeModal } from "./ManualEdgeModal";
import { useColumnSearch } from "../hooks/useColumnSearch";
import { useScanProgress } from "../hooks/useScanProgress";
import { exportGraphJSON } from "../api/client";
import type { ScanConfig } from "../types/graph";

interface ManualEdgeModalState {
  anchorNodeId: string;
  direction: "upstream" | "downstream";
  editingEdge?: LineageEdge;
}

const nodeTypes: NodeTypes = {
  tableNode: TableNode,
};

interface GraphCanvasProps {
  graph: LineageGraph;
  onGraphReload: () => void;
}

function buildFlowElements(
  graph: LineageGraph,
  traceNodeIds: Set<string> | null,
  traceEdgeIds: Set<string> | null,
  getHighlightedColumns: (nodeId: string) => string[]
): { nodes: Node[]; edges: Edge[] } {
  const hasTrace = traceNodeIds !== null;

  const flowNodes: Node[] = Object.entries(graph.nodes).map(
    ([id, node]: [string, LineageNode]) => ({
      id,
      type: "tableNode",
      position: { x: 0, y: 0 },
      data: {
        lineageNode: { ...node, id },
        highlightedColumns: getHighlightedColumns(id),
        dimmed: hasTrace && !traceNodeIds.has(id),
      },
    })
  );

  const flowEdges: Edge[] = graph.edges.map((edge: LineageEdge) => {
    const dimmed = hasTrace && !traceEdgeIds?.has(edge.id);
    const edgeColor = dimmed
      ? "rgba(148,163,184,0.15)"
      : edge.edge_type === "manual"
        ? "#a78bfa"
        : "#22d3ee";

    return {
      id: edge.id,
      source: edge.source_node,
      target: edge.target_node,
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 14,
        height: 14,
        color: edgeColor,
      },
      style: {
        stroke: edgeColor,
        strokeWidth: !dimmed && hasTrace ? 2.5 : 1.5,
        strokeDasharray: edge.edge_type === "manual" ? "6 4" : undefined,
      },
      data: { lineageEdge: edge },
    };
  });

  return getLayoutedElements(flowNodes, flowEdges);
}

export function GraphCanvas({ graph, onGraphReload }: GraphCanvasProps) {
  const {
    query,
    setQuery,
    searchResults,
    selectResult,
    activeTrace,
    clearTrace,
    traceNodeIds,
    traceEdgeIds,
    getHighlightedColumns,
  } = useColumnSearch(graph);

  const { scanning, messages, scanError, runScan } = useScanProgress();

  const [selectedNode, setSelectedNode] = useState<LineageNode | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<LineageEdge | null>(null);
  const [manualEdgeModal, setManualEdgeModal] =
    useState<ManualEdgeModalState | null>(null);

  const openManualEdgeModal = useCallback(
    (
      anchorNodeId: string,
      direction: "upstream" | "downstream",
      editingEdge?: LineageEdge
    ) => {
      setManualEdgeModal({ anchorNodeId, direction, editingEdge });
    },
    []
  );

  const handleManualEdgeSaved = useCallback(() => {
    setManualEdgeModal(null);
    setSelectedNode(null);
    setSelectedEdge(null);
    onGraphReload();
  }, [onGraphReload]);

  const { nodes: initialNodes, edges: initialEdges } = useMemo(
    () =>
      buildFlowElements(
        graph,
        traceNodeIds,
        traceEdgeIds,
        getHighlightedColumns
      ),
    [graph, traceNodeIds, traceEdgeIds, getHighlightedColumns]
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Sync when graph/trace changes
  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  const onNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      const lineageNode = graph.nodes[node.id];
      if (lineageNode) {
        setSelectedNode({ ...lineageNode, id: node.id } as LineageNode);
        setSelectedEdge(null);
      }
    },
    [graph]
  );

  const onEdgeClick = useCallback(
    (_event: React.MouseEvent, edge: Edge) => {
      const lineageEdge = graph.edges.find((e) => e.id === edge.id);
      if (lineageEdge) {
        setSelectedEdge(lineageEdge);
        setSelectedNode(null);
      }
    },
    [graph]
  );

  const handleRescan = useCallback(
    (config: ScanConfig) => {
      runScan(config, onGraphReload);
    },
    [runScan, onGraphReload]
  );

  return (
    <div className="flex flex-col h-screen bg-[var(--bg-deep)]">
      <Toolbar
        graph={graph}
        searchQuery={query}
        onSearchQueryChange={setQuery}
        searchResults={searchResults}
        onSearchSelect={selectResult}
        hasActiveTrace={activeTrace !== null}
        onClearTrace={clearTrace}
        onRescan={handleRescan}
        onExport={() => exportGraphJSON(graph)}
        scanning={scanning}
      />

      <ScanProgressBar
        messages={messages}
        scanning={scanning}
        error={scanError}
      />

      <div className="flex flex-1 overflow-hidden">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            nodeTypes={nodeTypes}
            fitView
            minZoom={0.1}
            maxZoom={2}
            proOptions={{ hideAttribution: true }}
          >
            <Background
              gap={24}
              size={1}
              color="rgba(148, 163, 184, 0.06)"
            />
            <Controls />
            <MiniMap
              nodeColor={(node) => {
                const dimmed = node.data?.dimmed as boolean;
                return dimmed ? "rgba(30,41,59,0.5)" : "#22d3ee";
              }}
              maskColor="rgba(10, 14, 26, 0.85)"
            />
          </ReactFlow>

        {selectedNode && (
          <NodeDetailPanel
            node={selectedNode}
            edges={graph.edges}
            onClose={() => setSelectedNode(null)}
            onAddUpstream={(nodeId) =>
              openManualEdgeModal(nodeId, "upstream")
            }
            onAddDownstream={(nodeId) =>
              openManualEdgeModal(nodeId, "downstream")
            }
          />
        )}

        {selectedEdge && (
          <EdgeDetailPanel
            edge={selectedEdge}
            onClose={() => setSelectedEdge(null)}
            onEdit={
              selectedEdge.edge_type === "manual"
                ? (edge) =>
                    openManualEdgeModal(
                      edge.target_node,
                      "upstream",
                      edge
                    )
                : undefined
            }
          />
        )}
      </div>

      {manualEdgeModal && (
        <ManualEdgeModal
          graph={graph}
          anchorNodeId={manualEdgeModal.anchorNodeId}
          direction={manualEdgeModal.direction}
          editingEdge={manualEdgeModal.editingEdge}
          onClose={() => setManualEdgeModal(null)}
          onSaved={handleManualEdgeSaved}
        />
      )}
    </div>
  );
}
