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
import { fuzzyMatch } from "../utils/fuzzySearch";

export interface SearchResult {
  nodeId: string;
  nodeName: string;
  columnName: string;
  dataType: string;
  isTableResult?: boolean;
  matchScore?: number;
  /** Matched character indices in nodeName (for table results) or columnName (for column results) */
  matchIndices?: number[];
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

    const tableResults: SearchResult[] = [];
    const columnResults: SearchResult[] = [];

    for (const [nodeId, node] of Object.entries(graph.nodes)) {
      const fullName = `${node.dataset}.${node.name}`;

      // Fuzzy match table/view name — pick the higher-scoring match
      const nameMatch = fuzzyMatch(query, node.name);
      const fullMatch = fuzzyMatch(query, fullName);
      let bestTableMatch = nameMatch;
      if (fullMatch && (!nameMatch || fullMatch.score > nameMatch.score)) {
        bestTableMatch = fullMatch;
      }

      if (bestTableMatch) {
        // If matched on short name, offset indices to full name
        const indices =
          bestTableMatch === nameMatch
            ? bestTableMatch.indices.map((i) => i + node.dataset.length + 1)
            : bestTableMatch.indices;
        tableResults.push({
          nodeId,
          nodeName: fullName,
          columnName: "",
          dataType: node.type,
          isTableResult: true,
          matchScore: bestTableMatch.score,
          matchIndices: indices,
        });
      }

      // Fuzzy match column names
      for (const col of node.columns) {
        const colMatch = fuzzyMatch(query, col.name);
        if (colMatch) {
          columnResults.push({
            nodeId,
            nodeName: fullName,
            columnName: col.name,
            dataType: col.data_type,
            matchScore: colMatch.score,
            matchIndices: colMatch.indices,
          });
        }
      }
    }

    // Sort each group by score descending
    tableResults.sort((a, b) => (b.matchScore ?? 0) - (a.matchScore ?? 0));
    columnResults.sort((a, b) => (b.matchScore ?? 0) - (a.matchScore ?? 0));

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
