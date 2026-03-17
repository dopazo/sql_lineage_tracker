import type { LineageGraph, LineageEdge } from "../types/graph";

export interface ColumnTraceEntry {
  nodeId: string;
  columnName: string;
}

/**
 * Build adjacency maps for efficient edge lookup by node.
 */
export function buildAdjacency(edges: LineageEdge[]) {
  const bySource = new Map<string, LineageEdge[]>();
  const byTarget = new Map<string, LineageEdge[]>();

  for (const edge of edges) {
    let s = bySource.get(edge.source_node);
    if (!s) { s = []; bySource.set(edge.source_node, s); }
    s.push(edge);

    let t = byTarget.get(edge.target_node);
    if (!t) { t = []; byTarget.set(edge.target_node, t); }
    t.push(edge);
  }

  return { bySource, byTarget };
}

/**
 * Trace a column through the lineage graph in both directions,
 * following renames and transformations.
 */
export function traceColumn(
  graph: LineageGraph,
  startNodeId: string,
  startColumn: string
): ColumnTraceEntry[] {
  // Normalize to lowercase for consistent matching against ColumnMapping
  // (ColumnMapping uses lowercase for both target_column and source_columns)
  const normalizedCol = startColumn.toLowerCase();
  const trace: ColumnTraceEntry[] = [
    { nodeId: startNodeId, columnName: normalizedCol },
  ];
  const visited = new Set<string>([`${startNodeId}:${normalizedCol}`]);
  const { bySource, byTarget } = buildAdjacency(graph.edges);

  traceDirection(bySource, startNodeId, normalizedCol, "downstream", trace, visited);
  traceDirection(byTarget, startNodeId, normalizedCol, "upstream", trace, visited);

  return trace;
}

function traceDirection(
  adjacency: Map<string, LineageEdge[]>,
  nodeId: string,
  columnName: string,
  direction: "upstream" | "downstream",
  trace: ColumnTraceEntry[],
  visited: Set<string>
): void {
  const queue: Array<{ nodeId: string; columnName: string }> = [
    { nodeId, columnName },
  ];

  while (queue.length > 0) {
    const current = queue.shift()!;
    const relevantEdges = adjacency.get(current.nodeId) ?? [];

    for (const edge of relevantEdges) {
      for (const mapping of edge.column_mappings) {
        // Use case-insensitive matching: BigQuery column names are
        // case-insensitive, and ColumnMapping normalizes to lowercase
        const currentLower = current.columnName.toLowerCase();
        if (direction === "downstream") {
          if (mapping.source_columns.some(sc => sc.toLowerCase() === currentLower)) {
            const nextNodeId = edge.target_node;
            const nextColumn = mapping.target_column.toLowerCase();
            const key = `${nextNodeId}:${nextColumn}`;
            if (!visited.has(key)) {
              visited.add(key);
              trace.push({ nodeId: nextNodeId, columnName: nextColumn });
              queue.push({ nodeId: nextNodeId, columnName: nextColumn });
            }
          }
        } else {
          if (mapping.target_column.toLowerCase() === currentLower) {
            for (const srcCol of mapping.source_columns) {
              const nextNodeId = edge.source_node;
              const srcLower = srcCol.toLowerCase();
              const key = `${nextNodeId}:${srcLower}`;
              if (!visited.has(key)) {
                visited.add(key);
                trace.push({ nodeId: nextNodeId, columnName: srcLower });
                queue.push({ nodeId: nextNodeId, columnName: srcLower });
              }
            }
          }
        }
      }
    }
  }
}

/**
 * Get all node IDs that are part of a column trace.
 */
export function getTraceNodeIds(trace: ColumnTraceEntry[]): Set<string> {
  return new Set(trace.map((t) => t.nodeId));
}

/**
 * Trace all connections of a table node (upstream + downstream),
 * without filtering by specific column.
 * Returns a trace entry for every column in every connected node.
 */
export function traceTable(
  graph: LineageGraph,
  startNodeId: string
): ColumnTraceEntry[] {
  const visited = new Set<string>([startNodeId]);
  const queue = [startNodeId];

  // BFS through all connected nodes in both directions
  const { bySource, byTarget } = buildAdjacency(graph.edges);

  while (queue.length > 0) {
    const current = queue.shift()!;

    // Downstream: current is source_node → follow to target_node
    for (const edge of bySource.get(current) ?? []) {
      if (!visited.has(edge.target_node)) {
        visited.add(edge.target_node);
        queue.push(edge.target_node);
      }
    }

    // Upstream: current is target_node → follow to source_node
    for (const edge of byTarget.get(current) ?? []) {
      if (!visited.has(edge.source_node)) {
        visited.add(edge.source_node);
        queue.push(edge.source_node);
      }
    }
  }

  // Build trace entries: all columns of all connected nodes
  const trace: ColumnTraceEntry[] = [];
  for (const nodeId of visited) {
    const node = graph.nodes[nodeId];
    if (node) {
      for (const col of node.columns) {
        trace.push({ nodeId, columnName: col.name });
      }
      // If node has no columns, still add it with a placeholder
      if (node.columns.length === 0) {
        trace.push({ nodeId, columnName: "" });
      }
    }
  }

  return trace;
}

/**
 * Get all edge IDs connecting nodes in a table-level trace.
 * Simpler than column-level: just check if both endpoints are in the set.
 */
export function getTraceEdgeIdsForTable(
  graph: LineageGraph,
  traceNodes: Set<string>
): Set<string> {
  const edgeIds = new Set<string>();
  for (const edge of graph.edges) {
    if (traceNodes.has(edge.source_node) && traceNodes.has(edge.target_node)) {
      edgeIds.add(edge.id);
    }
  }
  return edgeIds;
}

/**
 * Get all edge IDs that connect nodes in a column trace.
 * Accepts pre-computed traceNodes set to avoid recomputation.
 */
export function getTraceEdgeIds(
  graph: LineageGraph,
  trace: ColumnTraceEntry[],
  traceNodes?: Set<string>
): Set<string> {
  const nodeSet = traceNodes ?? getTraceNodeIds(trace);
  const edgeIds = new Set<string>();

  for (const edge of graph.edges) {
    if (nodeSet.has(edge.source_node) && nodeSet.has(edge.target_node)) {
      const sourceTraceCols = trace
        .filter((t) => t.nodeId === edge.source_node)
        .map((t) => t.columnName.toLowerCase());
      const targetTraceCols = trace
        .filter((t) => t.nodeId === edge.target_node)
        .map((t) => t.columnName.toLowerCase());

      for (const mapping of edge.column_mappings) {
        const hasSourceMatch = mapping.source_columns.some((sc) =>
          sourceTraceCols.includes(sc.toLowerCase())
        );
        const hasTargetMatch = targetTraceCols.includes(mapping.target_column.toLowerCase());
        if (hasSourceMatch && hasTargetMatch) {
          edgeIds.add(edge.id);
          break;
        }
      }
    }
  }

  return edgeIds;
}
