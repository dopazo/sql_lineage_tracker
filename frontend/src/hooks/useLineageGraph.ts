import { useState, useEffect, useCallback } from "react";
import type { LineageGraph } from "../types/graph";
import { getGraph, getHealth } from "../api/client";

export type AppState = "loading" | "setup" | "graph" | "error";

export function useLineageGraph() {
  const [graph, setGraph] = useState<LineageGraph | null>(null);
  const [appState, setAppState] = useState<AppState>("loading");
  const [error, setError] = useState<string | null>(null);

  const loadGraph = useCallback(async (silent = false) => {
    try {
      if (!silent) {
        setAppState("loading");
        setError(null);
      }
      const data = await getGraph();
      if (data && data.nodes && Object.keys(data.nodes).length > 0) {
        setGraph(data);
        if (!silent) setAppState("graph");
      } else {
        if (!silent) setAppState("setup");
      }
    } catch (err) {
      if (silent) {
        console.warn("Silent graph load failed:", err);
      } else {
        // If graph endpoint returns error (no graph), show setup
        const health = await getHealth().catch(() => null);
        if (health) {
          setAppState("setup");
        } else {
          setError(
            err instanceof Error ? err.message : "Failed to connect to server"
          );
          setAppState("error");
        }
      }
    }
  }, []);

  useEffect(() => {
    loadGraph();
  }, [loadGraph]);

  return {
    graph,
    setGraph,
    appState,
    setAppState,
    error,
    loadGraph,
  };
}
