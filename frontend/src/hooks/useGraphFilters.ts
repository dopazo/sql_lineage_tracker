import { useState, useMemo, useCallback, useEffect } from "react";
import type {
  LineageGraph,
  LineageNode,
  GraphFilters,
} from "../types/graph";

/** Compute topological depth of each node (distance from root nodes). */
function computeNodeDepths(graph: LineageGraph): Map<string, number> {
  const depths = new Map<string, number>();
  const incomingMap = new Map<string, string[]>();
  const outgoingMap = new Map<string, string[]>();

  for (const id of Object.keys(graph.nodes)) {
    incomingMap.set(id, []);
    outgoingMap.set(id, []);
  }

  for (const edge of graph.edges) {
    incomingMap.get(edge.target_node)?.push(edge.source_node);
    outgoingMap.get(edge.source_node)?.push(edge.target_node);
  }

  // BFS from root nodes (no incoming edges)
  const queue: string[] = [];
  for (const [id, incoming] of incomingMap) {
    if (incoming.length === 0) {
      depths.set(id, 0);
      queue.push(id);
    }
  }

  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    const currentDepth = depths.get(nodeId)!;
    for (const child of outgoingMap.get(nodeId) ?? []) {
      const existing = depths.get(child);
      if (existing === undefined || existing < currentDepth + 1) {
        depths.set(child, currentDepth + 1);
        queue.push(child);
      }
    }
  }

  // Nodes not reachable from roots get depth 0
  for (const id of Object.keys(graph.nodes)) {
    if (!depths.has(id)) depths.set(id, 0);
  }

  // Invert depths so the target table (leaf) is depth 0
  // and source tables have the highest depths.
  // This way, reducing maxDepth removes the most distant sources first.
  let maxDepth = 0;
  for (const d of depths.values()) {
    if (d > maxDepth) maxDepth = d;
  }
  for (const [id, d] of depths) {
    depths.set(id, maxDepth - d);
  }

  return depths;
}

/** Extract all unique datasets from the graph. */
function getDatasets(graph: LineageGraph): string[] {
  const ds = new Set<string>();
  for (const node of Object.values(graph.nodes)) {
    ds.add(node.dataset);
  }
  return [...ds].sort();
}

/** Extract all unique node types from the graph. */
function getNodeTypes(graph: LineageGraph): LineageNode["type"][] {
  const types = new Set<LineageNode["type"]>();
  for (const node of Object.values(graph.nodes)) {
    types.add(node.type);
  }
  return [...types].sort();
}

const ALL_EDGE_TYPES: ("automatic" | "manual")[] = ["automatic", "manual"];

/** Collect all upstream node IDs from a starting node (exclusive). */
function collectUpstream(graph: LineageGraph, startNodeId: string): Set<string> {
  // Build reverse adjacency: target → sources
  const reverseAdj = new Map<string, string[]>();
  for (const edge of graph.edges) {
    const sources = reverseAdj.get(edge.target_node) ?? [];
    sources.push(edge.source_node);
    reverseAdj.set(edge.target_node, sources);
  }

  // Also build forward adjacency to check if a node is reachable
  // from any non-pruned path (we only prune nodes exclusively upstream of startNode)
  const visited = new Set<string>();
  const queue = [startNodeId];

  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    for (const source of reverseAdj.get(nodeId) ?? []) {
      if (!visited.has(source)) {
        visited.add(source);
        queue.push(source);
      }
    }
  }

  return visited;
}

export function useGraphFilters(graph: LineageGraph) {
  const datasets = useMemo(() => getDatasets(graph), [graph]);
  const nodeTypes = useMemo(() => getNodeTypes(graph), [graph]);
  const nodeDepths = useMemo(() => computeNodeDepths(graph), [graph]);
  const maxGraphDepth = useMemo(() => {
    let max = 0;
    for (const d of nodeDepths.values()) {
      if (d > max) max = d;
    }
    return max;
  }, [nodeDepths]);

  const [filters, setFilters] = useState<GraphFilters>({
    datasets: new Set(datasets),
    nodeTypes: new Set(nodeTypes),
    edgeTypes: new Set(ALL_EDGE_TYPES),
    maxDepth: null,
  });

  // Pruning state: tracks which nodes have been pruned and which nodes are prune points
  const [prunePoints, setPrunePoints] = useState<Set<string>>(new Set());
  const [prunedNodes, setPrunedNodes] = useState<Set<string>>(new Set());

  // Reset filters when graph changes (new datasets/types may appear)
  useEffect(() => {
    setFilters({
      datasets: new Set(datasets),
      nodeTypes: new Set(nodeTypes),
      edgeTypes: new Set(ALL_EDGE_TYPES),
      maxDepth: null,
    });
    setPrunePoints(new Set());
    setPrunedNodes(new Set());
  }, [datasets, nodeTypes]);

  const toggleDataset = useCallback((ds: string) => {
    setFilters((prev) => {
      const next = new Set(prev.datasets);
      if (next.has(ds)) next.delete(ds);
      else next.add(ds);
      return { ...prev, datasets: next };
    });
  }, []);

  const toggleNodeType = useCallback((t: LineageNode["type"]) => {
    setFilters((prev) => {
      const next = new Set(prev.nodeTypes);
      if (next.has(t)) next.delete(t);
      else next.add(t);
      return { ...prev, nodeTypes: next };
    });
  }, []);

  const toggleEdgeType = useCallback((t: "automatic" | "manual") => {
    setFilters((prev) => {
      const next = new Set(prev.edgeTypes);
      if (next.has(t)) next.delete(t);
      else next.add(t);
      return { ...prev, edgeTypes: next };
    });
  }, []);

  const setMaxDepth = useCallback((d: number | null) => {
    setFilters((prev) => ({ ...prev, maxDepth: d }));
  }, []);

  const pruneUpstream = useCallback((nodeId: string) => {
    const upstream = collectUpstream(graph, nodeId);
    setPrunePoints((prev) => new Set([...prev, nodeId]));
    setPrunedNodes((prev) => new Set([...prev, ...upstream]));
  }, [graph]);

  const restorePrune = useCallback((nodeId: string) => {
    // Recompute: remove this prune point and recalculate prunedNodes from remaining prune points
    setPrunePoints((prev) => {
      const next = new Set(prev);
      next.delete(nodeId);

      // Recompute all pruned nodes from remaining prune points
      const allPruned = new Set<string>();
      for (const pp of next) {
        const upstream = collectUpstream(graph, pp);
        for (const id of upstream) allPruned.add(id);
      }
      setPrunedNodes(allPruned);

      return next;
    });
  }, [graph]);

  const clearAllPrunes = useCallback(() => {
    setPrunePoints(new Set());
    setPrunedNodes(new Set());
  }, []);

  const hasPrunedNodes = prunedNodes.size > 0;

  const resetFilters = useCallback(() => {
    setFilters({
      datasets: new Set(datasets),
      nodeTypes: new Set(nodeTypes),
      edgeTypes: new Set(ALL_EDGE_TYPES),
      maxDepth: null,
    });
    setPrunePoints(new Set());
    setPrunedNodes(new Set());
  }, [datasets, nodeTypes]);

  const isFiltered =
    filters.datasets.size !== datasets.length ||
    filters.nodeTypes.size !== nodeTypes.length ||
    filters.edgeTypes.size !== ALL_EDGE_TYPES.length ||
    filters.maxDepth !== null ||
    hasPrunedNodes;

  /** Apply filters to the graph, returning a new filtered graph. */
  const filteredGraph = useMemo((): LineageGraph => {
    if (!isFiltered) return graph;

    const filteredNodes: Record<string, LineageNode> = {};

    for (const [id, node] of Object.entries(graph.nodes)) {
      if (!filters.datasets.has(node.dataset)) continue;
      if (!filters.nodeTypes.has(node.type)) continue;
      if (
        filters.maxDepth !== null &&
        (nodeDepths.get(id) ?? 0) > filters.maxDepth
      )
        continue;
      if (prunedNodes.has(id)) continue;
      filteredNodes[id] = node;
    }

    const filteredEdges = graph.edges.filter((edge) => {
      if (!filters.edgeTypes.has(edge.edge_type)) return false;
      if (!(edge.source_node in filteredNodes)) return false;
      if (!(edge.target_node in filteredNodes)) return false;
      return true;
    });

    // Recompute stats
    const nodesByType: Record<string, number> = {};
    for (const node of Object.values(filteredNodes)) {
      nodesByType[node.type] = (nodesByType[node.type] ?? 0) + 1;
    }

    return {
      metadata: {
        ...graph.metadata,
        scan_stats: {
          ...graph.metadata.scan_stats,
          total_nodes: Object.keys(filteredNodes).length,
          total_edges: filteredEdges.length,
          nodes_by_type: nodesByType,
        },
      },
      nodes: filteredNodes,
      edges: filteredEdges,
    };
  }, [graph, filters, isFiltered, nodeDepths, prunedNodes]);

  return {
    filters,
    filteredGraph,
    isFiltered,
    datasets,
    nodeTypes,
    edgeTypes: ALL_EDGE_TYPES,
    maxGraphDepth,
    toggleDataset,
    toggleNodeType,
    toggleEdgeType,
    setMaxDepth,
    resetFilters,
    pruneUpstream,
    restorePrune,
    clearAllPrunes,
    prunePoints,
    hasPrunedNodes,
  };
}
