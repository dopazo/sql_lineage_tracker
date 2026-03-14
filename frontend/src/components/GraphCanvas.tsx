import { useMemo, useEffect, useCallback, useState, useRef } from "react";
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
  type OnNodeDrag,
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
import { NodeContextMenu } from "./NodeContextMenu";
import { FilterPanel } from "./FilterPanel";
import { useColumnSearch } from "../hooks/useColumnSearch";
import { useGraphFilters } from "../hooks/useGraphFilters";
import { useScanProgress } from "../hooks/useScanProgress";
import { exportGraphJSON } from "../api/client";
import type { ScanConfig } from "../types/graph";

// Position persistence via localStorage
const POSITION_KEY_PREFIX = "sql-lineage-positions-";

function loadPositions(projectId: string): Record<string, { x: number; y: number }> {
  try {
    const stored = localStorage.getItem(POSITION_KEY_PREFIX + projectId);
    return stored ? JSON.parse(stored) : {};
  } catch {
    return {};
  }
}

function savePositions(
  projectId: string,
  positions: Record<string, { x: number; y: number }>
) {
  try {
    localStorage.setItem(POSITION_KEY_PREFIX + projectId, JSON.stringify(positions));
  } catch {
    // Ignore storage errors
  }
}

interface ManualEdgeModalState {
  anchorNodeId: string;
  direction: "upstream" | "downstream";
  editingEdge?: LineageEdge;
}

const rfNodeTypes: NodeTypes = {
  tableNode: TableNode,
};

interface GraphCanvasProps {
  graph: LineageGraph;
  onGraphReload: (silent?: boolean) => void;
}

function buildFlowElements(
  graph: LineageGraph,
  traceNodeIds: Set<string> | null,
  traceEdgeIds: Set<string> | null,
  getHighlightedColumns: (nodeId: string) => string[],
  onGapClick?: (nodeId: string, direction: "upstream" | "downstream") => void,
  onExpandNode?: (nodeId: string) => void,
  onColumnClick?: (nodeId: string, columnName: string) => void
): { nodes: Node[]; edges: Edge[] } {
  const hasTrace = traceNodeIds !== null;

  // Compute which nodes have incoming/outgoing edges
  const hasIncoming = new Set<string>();
  const hasOutgoing = new Set<string>();
  for (const edge of graph.edges) {
    hasOutgoing.add(edge.source_node);
    hasIncoming.add(edge.target_node);
  }

  const flowNodes: Node[] = Object.entries(graph.nodes).map(
    ([id, node]: [string, LineageNode]) => ({
      id,
      type: "tableNode",
      position: { x: 0, y: 0 },
      data: {
        lineageNode: { ...node, id },
        highlightedColumns: getHighlightedColumns(id),
        dimmed: hasTrace && !traceNodeIds.has(id),
        missingUpstream: !hasIncoming.has(id) && node.type !== "table",
        missingDownstream: !hasOutgoing.has(id),
        onGapClick,
        onExpandNode,
        onColumnClick,
      },
    })
  );

  const flowEdges: Edge[] = graph.edges.map((edge: LineageEdge) => {
    const dimmed = hasTrace && !traceEdgeIds?.has(edge.id);
    const edgeColor = dimmed
      ? "rgba(148,163,184,0.15)"
      : edge.edge_type === "manual"
        ? "var(--accent-purple)"
        : "var(--accent-cyan)";

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

  const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(flowNodes, flowEdges);

  return { nodes: layoutedNodes, edges: layoutedEdges };
}

export function GraphCanvas({ graph, onGraphReload }: GraphCanvasProps) {
  const projectId = graph.metadata.project_id;

  const [savedPositions, setSavedPositions] = useState<
    Record<string, { x: number; y: number }>
  >(() => loadPositions(projectId));
  const [layoutVersion, setLayoutVersion] = useState(0);
  const forceLayoutRef = useRef(false);

  const handleNodeDragStop = useCallback<OnNodeDrag>(
    (_event, node) => {
      setSavedPositions((prev) => {
        const updated = { ...prev, [node.id]: node.position };
        savePositions(projectId, updated);
        return updated;
      });
    },
    [projectId]
  );

  const handleResetLayout = useCallback(() => {
    localStorage.removeItem(POSITION_KEY_PREFIX + projectId);
    setSavedPositions({});
    forceLayoutRef.current = true;
    setLayoutVersion((v) => v + 1);
  }, [projectId]);

  const {
    filters,
    filteredGraph,
    isFiltered,
    datasets,
    nodeTypes: availableNodeTypes,
    edgeTypes: availableEdgeTypes,
    maxGraphDepth,
    toggleDataset,
    toggleNodeType,
    toggleEdgeType,
    setMaxDepth,
    resetFilters,
    pruneUpstream,
    restorePrune,
    prunePoints,
    hasPrunedNodes,
    clearAllPrunes,
  } = useGraphFilters(graph);

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
    traceOrigin,
  } = useColumnSearch(filteredGraph);

  const { scanning, messages, scanError, completed, runScan, runExpand, dismissMessages } = useScanProgress();

  const [showFilters, setShowFilters] = useState(false);

  const [selectedNode, setSelectedNode] = useState<LineageNode | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<LineageEdge | null>(null);
  const [manualEdgeModal, setManualEdgeModal] =
    useState<ManualEdgeModalState | null>(null);

  // Context menu state
  const [contextMenu, setContextMenu] = useState<{
    node: LineageNode;
    position: { x: number; y: number };
  } | null>(null);

  const closeContextMenu = useCallback(() => setContextMenu(null), []);

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

  const handleExpandNode = useCallback(
    (nodeId: string) => {
      runExpand(nodeId, () => {
        setSelectedNode(null);
        onGraphReload();
      });
    },
    [runExpand, onGraphReload]
  );

  const handleColumnClick = useCallback(
    (nodeId: string, columnName: string) => {
      // Toggle off if clicking the same column that originated the trace
      if (
        traceOrigin &&
        traceOrigin.nodeId === nodeId &&
        traceOrigin.columnName === columnName
      ) {
        clearTrace();
        return;
      }
      // Trace the clicked column
      const node = filteredGraph.nodes[nodeId];
      if (node) {
        selectResult({
          nodeId,
          nodeName: `${node.dataset}.${node.name}`,
          columnName,
          dataType: node.columns.find((c) => c.name === columnName)?.data_type ?? "",
        });
      }
    },
    [traceOrigin, clearTrace, selectResult, filteredGraph]
  );

  const { nodes: layoutedNodes, edges: initialEdges } = useMemo(
    () =>
      buildFlowElements(
        filteredGraph,
        traceNodeIds,
        traceEdgeIds,
        getHighlightedColumns,
        openManualEdgeModal,
        handleExpandNode,
        handleColumnClick
      ),
    [filteredGraph, traceNodeIds, traceEdgeIds, getHighlightedColumns, openManualEdgeModal, handleExpandNode, handleColumnClick]
  );

  // Apply user-saved positions on top of dagre layout (separate from layout computation)
  const initialNodes = useMemo(
    () =>
      layoutedNodes.map((node) =>
        savedPositions[node.id]
          ? { ...node, position: savedPositions[node.id] }
          : node
      ),
    [layoutedNodes, savedPositions]
  );

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Sync when graph/trace changes, preserving positions of existing nodes
  useEffect(() => {
    setNodes((currentNodes) => {
      // On layout reset, force dagre positions
      if (forceLayoutRef.current) {
        forceLayoutRef.current = false;
        return layoutedNodes;
      }
      const currentPositions = new Map(
        currentNodes.map((n) => [n.id, n.position])
      );
      // If we only removed nodes (subset), preserve all existing positions
      const allExist = initialNodes.every((n) => currentPositions.has(n.id));
      if (allExist && currentNodes.length > 0) {
        return initialNodes.map((node) => ({
          ...node,
          position: currentPositions.get(node.id) ?? node.position,
        }));
      }
      // New nodes appeared — use dagre layout for all
      return initialNodes;
    });
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges, layoutVersion]);

  const onNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      const lineageNode = filteredGraph.nodes[node.id];
      if (lineageNode) {
        setSelectedNode({ ...lineageNode, id: node.id } as LineageNode);
        setSelectedEdge(null);
      }
    },
    [filteredGraph]
  );

  const onEdgeClick = useCallback(
    (_event: React.MouseEvent, edge: Edge) => {
      const lineageEdge = filteredGraph.edges.find((e) => e.id === edge.id);
      if (lineageEdge) {
        setSelectedEdge(lineageEdge);
        setSelectedNode(null);
      }
    },
    [filteredGraph]
  );

  const onNodeContextMenu = useCallback(
    (event: React.MouseEvent, node: Node) => {
      event.preventDefault();
      const lineageNode = filteredGraph.nodes[node.id];
      if (lineageNode) {
        setContextMenu({
          node: { ...lineageNode, id: node.id } as LineageNode,
          position: { x: event.clientX, y: event.clientY },
        });
      }
    },
    [filteredGraph]
  );

  // Close context menu on pane click
  const onPaneClick = useCallback(() => {
    closeContextMenu();
  }, [closeContextMenu]);

  const handleRescan = useCallback(
    (config: ScanConfig) => {
      runScan(config, () => onGraphReload(true));
    },
    [runScan, onGraphReload]
  );

  const handleDismissScanMessages = useCallback(() => {
    dismissMessages();
  }, [dismissMessages]);

  return (
    <div className="flex flex-col h-screen bg-[var(--bg-deep)]">
      <Toolbar
        graph={filteredGraph}
        searchQuery={query}
        onSearchQueryChange={setQuery}
        searchResults={searchResults}
        onSearchSelect={selectResult}
        hasActiveTrace={activeTrace !== null}
        onClearTrace={clearTrace}
        onRescan={handleRescan}
        onExport={() => exportGraphJSON(graph)}
        onResetLayout={handleResetLayout}
        scanning={scanning}
        showFilters={showFilters}
        onToggleFilters={() => setShowFilters((v) => !v)}
        isFiltered={isFiltered}
        onGraphReload={onGraphReload}
      />

      <ScanProgressBar
        messages={messages}
        scanning={scanning}
        error={scanError}
        completed={completed}
        onDismiss={handleDismissScanMessages}
      />

      <div className="relative z-0 flex flex-1 overflow-hidden">
        {showFilters && (
          <FilterPanel
            filters={filters}
            datasets={datasets}
            nodeTypes={availableNodeTypes}
            edgeTypes={availableEdgeTypes}
            maxGraphDepth={maxGraphDepth}
            isFiltered={isFiltered}
            onToggleDataset={toggleDataset}
            onToggleNodeType={toggleNodeType}
            onToggleEdgeType={toggleEdgeType}
            onSetMaxDepth={setMaxDepth}
            onReset={resetFilters}
            prunePoints={prunePoints}
            hasPrunedNodes={hasPrunedNodes}
            onRestorePrune={restorePrune}
            onClearAllPrunes={clearAllPrunes}
          />
        )}
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onNodeContextMenu={onNodeContextMenu}
            onPaneClick={onPaneClick}
            onNodeDragStop={handleNodeDragStop}
            nodeTypes={rfNodeTypes}
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
            edges={filteredGraph.edges}
            onClose={() => setSelectedNode(null)}
            onAddUpstream={(nodeId) =>
              openManualEdgeModal(nodeId, "upstream")
            }
            onAddDownstream={(nodeId) =>
              openManualEdgeModal(nodeId, "downstream")
            }
            onExpandNode={handleExpandNode}
            expanding={scanning}
            onColumnClick={handleColumnClick}
            highlightedColumns={getHighlightedColumns(selectedNode.id)}
            activeTraceColumn={
              traceOrigin && traceOrigin.nodeId === selectedNode.id
                ? traceOrigin.columnName
                : null
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

      {contextMenu && (
        <NodeContextMenu
          node={contextMenu.node}
          position={contextMenu.position}
          hasUpstreamEdges={filteredGraph.edges.some(
            (e) => e.target_node === contextMenu.node.id
          )}
          hasDownstreamEdges={filteredGraph.edges.some(
            (e) => e.source_node === contextMenu.node.id
          )}
          onClose={closeContextMenu}
          onViewDetails={() => {
            setSelectedNode(contextMenu.node);
            setSelectedEdge(null);
          }}
          onAddUpstream={() =>
            openManualEdgeModal(contextMenu.node.id, "upstream")
          }
          onAddDownstream={() =>
            openManualEdgeModal(contextMenu.node.id, "downstream")
          }
          onExpandNode={
            contextMenu.node.status === "truncated"
              ? () => handleExpandNode(contextMenu.node.id)
              : undefined
          }
          onFocusConnections={() => {
            const nodeId = contextMenu.node.id;
            const node = filteredGraph.nodes[nodeId];
            if (node) {
              selectResult({
                nodeId,
                nodeName: `${node.dataset}.${node.name}`,
                columnName: "",
                dataType: node.type,
                isTableResult: true,
              });
            }
          }}
          onPruneUpstream={() => pruneUpstream(contextMenu.node.id)}
          onRestorePrune={
            prunePoints.has(contextMenu.node.id)
              ? () => restorePrune(contextMenu.node.id)
              : undefined
          }
          onCopyName={() => {
            const node = contextMenu.node;
            navigator.clipboard.writeText(`${node.dataset}.${node.name}`);
          }}
        />
      )}
    </div>
  );
}
