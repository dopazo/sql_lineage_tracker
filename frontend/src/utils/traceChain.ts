import type {
  LineageGraph,
  LineageNode,
} from "../types/graph";
import { buildAdjacency, type ColumnTraceEntry } from "./lineageTraversal";
import type { TraceOrigin } from "../hooks/useColumnSearch";

export interface ChainStep {
  nodeId: string;
  nodeName: string;
  nodeType: string;
  columnName: string;
  /** Display-case column name (original case from node.columns) */
  columnDisplay: string;
  dataType: string;
  /**
   * Transformation that connects THIS step to the NEXT step in display order.
   * For upstream steps: how this column feeds into the next step toward origin.
   * For downstream steps: how the previous step feeds into this column.
   */
  transformation?: string;
  expression?: string | null;
  isOrigin: boolean;
}

export function buildOrderedChain(
  graph: LineageGraph,
  traceOrigin: TraceOrigin,
  activeTrace: ColumnTraceEntry[]
): { upstream: ChainStep[]; origin: ChainStep; downstream: ChainStep[] } {
  // Build lookup: which columns are traced per node
  const traceSet = new Set(
    activeTrace.map((t) => `${t.nodeId}:${t.columnName.toLowerCase()}`)
  );

  const { bySource, byTarget } = buildAdjacency(graph.edges);

  function resolveColumn(nodeId: string, colNameLower: string) {
    const node = graph.nodes[nodeId];
    if (!node) return { display: colNameLower, dataType: "" };
    if (colNameLower === "*") {
      return { display: "*", dataType: "(all rows)" };
    }
    const col = node.columns.find(
      (c) => c.name.toLowerCase() === colNameLower
    );
    return {
      display: col?.name ?? colNameLower,
      dataType: col?.data_type ?? "",
    };
  }

  function nodeInfo(nodeId: string): { name: string; type: string } {
    const node = graph.nodes[nodeId] as LineageNode | undefined;
    if (!node) return { name: nodeId, type: "?" };
    return { name: `${node.dataset}.${node.name}`, type: node.type };
  }

  // Walk upstream from origin (BFS)
  const upstream: ChainStep[] = [];
  {
    const visited = new Set<string>([
      `${traceOrigin.nodeId}:${traceOrigin.columnName.toLowerCase()}`,
    ]);
    const queue: Array<{
      nodeId: string;
      columnName: string;
    }> = [{ nodeId: traceOrigin.nodeId, columnName: traceOrigin.columnName.toLowerCase() }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const edges = byTarget.get(current.nodeId) ?? [];

      for (const edge of edges) {
        for (const mapping of edge.column_mappings) {
          if (mapping.target_column.toLowerCase() !== current.columnName)
            continue;

          for (const srcCol of mapping.source_columns) {
            const srcLower = srcCol.toLowerCase();
            const key = `${edge.source_node}:${srcLower}`;
            if (visited.has(key) || !traceSet.has(key)) continue;

            visited.add(key);
            const info = nodeInfo(edge.source_node);
            const col = resolveColumn(edge.source_node, srcLower);

            upstream.push({
              nodeId: edge.source_node,
              nodeName: info.name,
              nodeType: info.type,
              columnName: srcLower,
              columnDisplay: col.display,
              dataType: col.dataType,
              transformation: mapping.transformation,
              expression: mapping.expression,
              isOrigin: false,
            });

            queue.push({ nodeId: edge.source_node, columnName: srcLower });
          }
        }
      }
    }
  }

  // Walk downstream from origin (BFS)
  const downstream: ChainStep[] = [];
  {
    const visited = new Set<string>([
      `${traceOrigin.nodeId}:${traceOrigin.columnName.toLowerCase()}`,
    ]);
    const queue: Array<{
      nodeId: string;
      columnName: string;
    }> = [{ nodeId: traceOrigin.nodeId, columnName: traceOrigin.columnName.toLowerCase() }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const edges = bySource.get(current.nodeId) ?? [];

      for (const edge of edges) {
        for (const mapping of edge.column_mappings) {
          if (
            !mapping.source_columns.some(
              (sc) => sc.toLowerCase() === current.columnName
            )
          )
            continue;

          const tgtLower = mapping.target_column.toLowerCase();
          const key = `${edge.target_node}:${tgtLower}`;
          if (visited.has(key) || !traceSet.has(key)) continue;

          visited.add(key);
          const info = nodeInfo(edge.target_node);
          const col = resolveColumn(edge.target_node, tgtLower);

          downstream.push({
            nodeId: edge.target_node,
            nodeName: info.name,
            nodeType: info.type,
            columnName: tgtLower,
            columnDisplay: col.display,
            dataType: col.dataType,
            transformation: mapping.transformation,
            expression: mapping.expression,
            isOrigin: false,
          });

          queue.push({ nodeId: edge.target_node, columnName: tgtLower });
        }
      }
    }
  }

  // Origin step
  const originInfo = nodeInfo(traceOrigin.nodeId);
  const originCol = resolveColumn(
    traceOrigin.nodeId,
    traceOrigin.columnName.toLowerCase()
  );
  const origin: ChainStep = {
    nodeId: traceOrigin.nodeId,
    nodeName: originInfo.name,
    nodeType: originInfo.type,
    columnName: traceOrigin.columnName.toLowerCase(),
    columnDisplay: originCol.display,
    dataType: originCol.dataType,
    isOrigin: true,
  };

  // After reverse, upstream is ordered: [furthest_source, ..., closest_to_origin]
  return { upstream: upstream.reverse(), origin, downstream };
}
