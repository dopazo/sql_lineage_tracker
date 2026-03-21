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

/**
 * Build all distinct source-to-sink paths through the trace DAG.
 * Each path is an array of ChainSteps from furthest source → origin → furthest consumer.
 * When a column depends on multiple sources, this produces multiple paths.
 */
export function buildTracePaths(
  graph: LineageGraph,
  traceOrigin: TraceOrigin,
  activeTrace: ColumnTraceEntry[]
): ChainStep[][] {
  type Key = string; // "nodeId:columnNameLower"

  const traceSet = new Set(
    activeTrace.map((t) => `${t.nodeId}:${t.columnName.toLowerCase()}`)
  );

  const { bySource, byTarget } = buildAdjacency(graph.edges);

  function resolveColumn(nodeId: string, colNameLower: string) {
    const node = graph.nodes[nodeId];
    if (!node) return { display: colNameLower, dataType: "" };
    if (colNameLower === "*") return { display: "*", dataType: "(all rows)" };
    const col = node.columns.find((c) => c.name.toLowerCase() === colNameLower);
    return { display: col?.name ?? colNameLower, dataType: col?.data_type ?? "" };
  }

  function nodeInfo(nodeId: string) {
    const node = graph.nodes[nodeId] as LineageNode | undefined;
    if (!node) return { name: nodeId, type: "?" };
    return { name: `${node.dataset}.${node.name}`, type: node.type };
  }

  function makeStep(nodeId: string, colLower: string, isOrigin: boolean, transformation?: string, expression?: string | null): ChainStep {
    const info = nodeInfo(nodeId);
    const col = resolveColumn(nodeId, colLower);
    return { nodeId, nodeName: info.name, nodeType: info.type, columnName: colLower, columnDisplay: col.display, dataType: col.dataType, transformation, expression, isOrigin };
  }

  const originKey = `${traceOrigin.nodeId}:${traceOrigin.columnName.toLowerCase()}`;

  // Build upstream DAG: child → parents (towards sources)
  const upstreamParents = new Map<Key, { key: Key; transformation?: string; expression?: string | null }[]>();
  {
    const visited = new Set<Key>([originKey]);
    const queue = [{ nodeId: traceOrigin.nodeId, columnName: traceOrigin.columnName.toLowerCase() }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const currentKey = `${current.nodeId}:${current.columnName}`;
      const edges = byTarget.get(current.nodeId) ?? [];

      for (const edge of edges) {
        for (const mapping of edge.column_mappings) {
          if (mapping.target_column.toLowerCase() !== current.columnName) continue;
          for (const srcCol of mapping.source_columns) {
            const srcLower = srcCol.toLowerCase();
            const srcKey = `${edge.source_node}:${srcLower}`;
            if (!traceSet.has(srcKey)) continue;

            if (!upstreamParents.has(currentKey)) upstreamParents.set(currentKey, []);
            const parents = upstreamParents.get(currentKey)!;
            if (!parents.some((p) => p.key === srcKey)) {
              parents.push({ key: srcKey, transformation: mapping.transformation, expression: mapping.expression });
            }

            if (!visited.has(srcKey)) {
              visited.add(srcKey);
              queue.push({ nodeId: edge.source_node, columnName: srcLower });
            }
          }
        }
      }
    }
  }

  // Build downstream DAG: parent → children (towards consumers)
  const downstreamChildren = new Map<Key, { key: Key; transformation?: string; expression?: string | null }[]>();
  {
    const visited = new Set<Key>([originKey]);
    const queue = [{ nodeId: traceOrigin.nodeId, columnName: traceOrigin.columnName.toLowerCase() }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const currentKey = `${current.nodeId}:${current.columnName}`;
      const edges = bySource.get(current.nodeId) ?? [];

      for (const edge of edges) {
        for (const mapping of edge.column_mappings) {
          if (!mapping.source_columns.some((sc) => sc.toLowerCase() === current.columnName)) continue;
          const tgtLower = mapping.target_column.toLowerCase();
          const tgtKey = `${edge.target_node}:${tgtLower}`;
          if (!traceSet.has(tgtKey)) continue;

          if (!downstreamChildren.has(currentKey)) downstreamChildren.set(currentKey, []);
          const children = downstreamChildren.get(currentKey)!;
          if (!children.some((c) => c.key === tgtKey)) {
            children.push({ key: tgtKey, transformation: mapping.transformation, expression: mapping.expression });
          }

          if (!visited.has(tgtKey)) {
            visited.add(tgtKey);
            queue.push({ nodeId: edge.target_node, columnName: tgtLower });
          }
        }
      }
    }
  }

  function parseKey(key: Key): { nodeId: string; columnName: string } {
    const i = key.indexOf(":");
    return { nodeId: key.slice(0, i), columnName: key.slice(i + 1) };
  }

  // Enumerate all paths from leaves to origin (upstream)
  function getUpstreamPaths(key: Key, visited: Set<Key>): { key: Key; transformation?: string; expression?: string | null }[][] {
    const parents = upstreamParents.get(key);
    if (!parents || parents.length === 0) return [[{ key }]];
    const paths: { key: Key; transformation?: string; expression?: string | null }[][] = [];
    for (const parent of parents) {
      if (visited.has(parent.key)) continue; // cycle guard
      visited.add(parent.key);
      for (const parentPath of getUpstreamPaths(parent.key, visited)) {
        paths.push([...parentPath, { key, transformation: parent.transformation, expression: parent.expression }]);
      }
      visited.delete(parent.key);
    }
    return paths.length > 0 ? paths : [[{ key }]];
  }

  // Enumerate all paths from origin to leaves (downstream)
  function getDownstreamPaths(key: Key, visited: Set<Key>): { key: Key; transformation?: string; expression?: string | null }[][] {
    const children = downstreamChildren.get(key);
    if (!children || children.length === 0) return [[{ key }]];
    const paths: { key: Key; transformation?: string; expression?: string | null }[][] = [];
    for (const child of children) {
      if (visited.has(child.key)) continue;
      visited.add(child.key);
      for (const childPath of getDownstreamPaths(child.key, visited)) {
        paths.push([{ key, transformation: child.transformation, expression: child.expression }, ...childPath]);
      }
      visited.delete(child.key);
    }
    return paths.length > 0 ? paths : [[{ key }]];
  }

  const upPaths = getUpstreamPaths(originKey, new Set([originKey]));
  const downPaths = getDownstreamPaths(originKey, new Set([originKey]));

  // Combine: each upstream path + each downstream path, sharing origin
  const result: ChainStep[][] = [];

  for (const up of upPaths) {
    for (const down of downPaths) {
      const steps: ChainStep[] = [];

      // Upstream steps (excluding origin which is the last element)
      for (const entry of up) {
        const { nodeId, columnName } = parseKey(entry.key);
        const isOrig = entry.key === originKey;
        steps.push(makeStep(nodeId, columnName, isOrig, entry.transformation, entry.expression));
      }

      // Downstream steps (skip first element which is origin, already added)
      for (let i = 1; i < down.length; i++) {
        const entry = down[i];
        const { nodeId, columnName } = parseKey(entry.key);
        steps.push(makeStep(nodeId, columnName, false, entry.transformation, entry.expression));
      }

      if (steps.length > 1) result.push(steps);
    }
  }

  // If no paths found (origin only), return single path with origin
  if (result.length === 0) {
    const { nodeId, columnName } = parseKey(originKey);
    result.push([makeStep(nodeId, columnName, true)]);
  }

  return result;
}
