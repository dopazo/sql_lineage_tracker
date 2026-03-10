import type { LineageGraph, LineageEdge } from "../types/graph";

export interface ColumnTraceEntry {
  nodeId: string;
  columnName: string;
}

/**
 * Build adjacency maps for efficient edge lookup by node.
 */
function buildAdjacency(edges: LineageEdge[]) {
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
  const trace: ColumnTraceEntry[] = [
    { nodeId: startNodeId, columnName: startColumn },
  ];
  const visited = new Set<string>([`${startNodeId}:${startColumn}`]);
  const { bySource, byTarget } = buildAdjacency(graph.edges);

  traceDirection(bySource, startNodeId, startColumn, "downstream", trace, visited);
  traceDirection(byTarget, startNodeId, startColumn, "upstream", trace, visited);

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
        if (direction === "downstream") {
          if (mapping.source_columns.includes(current.columnName)) {
            const nextNodeId = edge.target_node;
            const nextColumn = mapping.target_column;
            const key = `${nextNodeId}:${nextColumn}`;
            if (!visited.has(key)) {
              visited.add(key);
              trace.push({ nodeId: nextNodeId, columnName: nextColumn });
              queue.push({ nodeId: nextNodeId, columnName: nextColumn });
            }
          }
        } else {
          if (mapping.target_column === current.columnName) {
            for (const srcCol of mapping.source_columns) {
              const nextNodeId = edge.source_node;
              const key = `${nextNodeId}:${srcCol}`;
              if (!visited.has(key)) {
                visited.add(key);
                trace.push({ nodeId: nextNodeId, columnName: srcCol });
                queue.push({ nodeId: nextNodeId, columnName: srcCol });
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
        .map((t) => t.columnName);
      const targetTraceCols = trace
        .filter((t) => t.nodeId === edge.target_node)
        .map((t) => t.columnName);

      for (const mapping of edge.column_mappings) {
        const hasSourceMatch = mapping.source_columns.some((sc) =>
          sourceTraceCols.includes(sc)
        );
        const hasTargetMatch = targetTraceCols.includes(mapping.target_column);
        if (hasSourceMatch && hasTargetMatch) {
          edgeIds.add(edge.id);
          break;
        }
      }
    }
  }

  return edgeIds;
}
