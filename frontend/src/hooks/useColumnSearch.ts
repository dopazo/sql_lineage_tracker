import { useState, useMemo, useCallback } from "react";
import type { LineageGraph } from "../types/graph";
import {
  traceColumn,
  getTraceNodeIds,
  getTraceEdgeIds,
  type ColumnTraceEntry,
} from "../utils/lineageTraversal";

export interface SearchResult {
  nodeId: string;
  nodeName: string;
  columnName: string;
  dataType: string;
}

export function useColumnSearch(graph: LineageGraph | null) {
  const [query, setQuery] = useState("");
  const [activeTrace, setActiveTrace] = useState<ColumnTraceEntry[] | null>(
    null
  );

  const searchResults = useMemo<SearchResult[]>(() => {
    if (!graph || query.length < 2) return [];

    const lowerQuery = query.toLowerCase();
    const results: SearchResult[] = [];

    for (const [nodeId, node] of Object.entries(graph.nodes)) {
      for (const col of node.columns) {
        if (col.name.toLowerCase().includes(lowerQuery)) {
          results.push({
            nodeId,
            nodeName: `${node.dataset}.${node.name}`,
            columnName: col.name,
            dataType: col.data_type,
          });
        }
      }
    }

    return results.slice(0, 50);
  }, [graph, query]);

  const selectResult = useCallback(
    (result: SearchResult) => {
      if (!graph) return;
      const trace = traceColumn(graph, result.nodeId, result.columnName);
      setActiveTrace(trace);
    },
    [graph]
  );

  const clearTrace = useCallback(() => {
    setActiveTrace(null);
    setQuery("");
  }, []);

  const traceNodeIds = useMemo(
    () => (activeTrace ? getTraceNodeIds(activeTrace) : null),
    [activeTrace]
  );

  const traceEdgeIds = useMemo(
    () => (graph && activeTrace ? getTraceEdgeIds(graph, activeTrace) : null),
    [graph, activeTrace]
  );

  const getHighlightedColumns = useCallback(
    (nodeId: string): string[] => {
      if (!activeTrace) return [];
      return activeTrace
        .filter((t) => t.nodeId === nodeId)
        .map((t) => t.columnName);
    },
    [activeTrace]
  );

  return {
    query,
    setQuery,
    searchResults,
    selectResult,
    activeTrace,
    clearTrace,
    traceNodeIds,
    traceEdgeIds,
    getHighlightedColumns,
  };
}
