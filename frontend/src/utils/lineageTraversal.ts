import type { LineageGraph } from "../types/graph";

export interface ColumnTraceEntry {
  nodeId: string;
  columnName: string;
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

  // Trace downstream: follow edges where source_columns includes our column
  traceDirection(graph, startNodeId, startColumn, "downstream", trace, visited);

  // Trace upstream: follow edges where target_column matches our column
  traceDirection(graph, startNodeId, startColumn, "upstream", trace, visited);

  return trace;
}

function traceDirection(
  graph: LineageGraph,
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

    const relevantEdges =
      direction === "downstream"
        ? graph.edges.filter((e) => e.source_node === current.nodeId)
        : graph.edges.filter((e) => e.target_node === current.nodeId);

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
 */
export function getTraceEdgeIds(
  graph: LineageGraph,
  trace: ColumnTraceEntry[]
): Set<string> {
  const traceNodes = getTraceNodeIds(trace);
  const edgeIds = new Set<string>();

  for (const edge of graph.edges) {
    if (traceNodes.has(edge.source_node) && traceNodes.has(edge.target_node)) {
      // Check if any column mapping in this edge is part of the trace
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
