import { useState, useMemo, useCallback } from "react";
import type { LineageGraph } from "../types/graph";
import {
  traceColumn,
  traceTable,
  getTraceNodeIds,
  getTraceEdgeIds,
  getTraceEdgeIdsForTable,
  type ColumnTraceEntry,
} from "../utils/lineageTraversal";

export interface SearchResult {
  nodeId: string;
  nodeName: string;
  columnName: string;
  dataType: string;
  isTableResult?: boolean;
}

export interface TraceOrigin {
  nodeId: string;
  columnName: string;
}

export function useColumnSearch(graph: LineageGraph | null) {
  const [query, setQuery] = useState("");
  const [activeTrace, setActiveTrace] = useState<ColumnTraceEntry[] | null>(
    null
  );
  const [isTableTrace, setIsTableTrace] = useState(false);
  const [traceOrigin, setTraceOrigin] = useState<TraceOrigin | null>(null);

  const searchResults = useMemo<SearchResult[]>(() => {
    if (!graph || query.length < 2) return [];

    const lowerQuery = query.toLowerCase();
    const tableResults: SearchResult[] = [];
    const columnResults: SearchResult[] = [];

    for (const [nodeId, node] of Object.entries(graph.nodes)) {
      const fullName = `${node.dataset}.${node.name}`;

      // Match table/view name
      if (
        node.name.toLowerCase().includes(lowerQuery) ||
        fullName.toLowerCase().includes(lowerQuery)
      ) {
        tableResults.push({
          nodeId,
          nodeName: fullName,
          columnName: "",
          dataType: node.type,
          isTableResult: true,
        });
      }

      // Match column names
      for (const col of node.columns) {
        if (col.name.toLowerCase().includes(lowerQuery)) {
          columnResults.push({
            nodeId,
            nodeName: fullName,
            columnName: col.name,
            dataType: col.data_type,
          });
        }
      }
    }

    // Tables first, then columns, capped at 50
    return [...tableResults, ...columnResults].slice(0, 50);
  }, [graph, query]);

  const selectResult = useCallback(
    (result: SearchResult) => {
      if (!graph) return;
      if (result.isTableResult) {
        const trace = traceTable(graph, result.nodeId);
        setActiveTrace(trace);
        setIsTableTrace(true);
        setTraceOrigin(null);
      } else {
        const trace = traceColumn(graph, result.nodeId, result.columnName);
        setActiveTrace(trace);
        setIsTableTrace(false);
        setTraceOrigin({ nodeId: result.nodeId, columnName: result.columnName });
      }
    },
    [graph]
  );

  const clearTrace = useCallback(() => {
    setActiveTrace(null);
    setIsTableTrace(false);
    setTraceOrigin(null);
    setQuery("");
  }, []);

  const traceNodeIds = useMemo(
    () => (activeTrace ? getTraceNodeIds(activeTrace) : null),
    [activeTrace]
  );

  const traceEdgeIds = useMemo(
    () => {
      if (!graph || !activeTrace || !traceNodeIds) return null;
      if (isTableTrace) {
        return getTraceEdgeIdsForTable(graph, traceNodeIds);
      }
      return getTraceEdgeIds(graph, activeTrace, traceNodeIds);
    },
    [graph, activeTrace, traceNodeIds, isTableTrace]
  );

  const getHighlightedColumns = useCallback(
    (nodeId: string): string[] => {
      if (!activeTrace) return [];
      // For table trace, don't highlight specific columns
      if (isTableTrace) return [];
      return activeTrace
        .filter((t) => t.nodeId === nodeId)
        .map((t) => t.columnName);
    },
    [activeTrace, isTableTrace]
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
    traceOrigin,
    isTableTrace,
  };
}
