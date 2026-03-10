import { useState, useEffect, useCallback } from "react";
import type { LineageGraph } from "../types/graph";
import { getGraph, getHealth } from "../api/client";

export type AppState = "loading" | "setup" | "graph" | "error";

export function useLineageGraph() {
  const [graph, setGraph] = useState<LineageGraph | null>(null);
  const [appState, setAppState] = useState<AppState>("loading");
  const [error, setError] = useState<string | null>(null);

  const loadGraph = useCallback(async () => {
    try {
      setAppState("loading");
      setError(null);
      const data = await getGraph();
      if (data && data.nodes && Object.keys(data.nodes).length > 0) {
        setGraph(data);
        setAppState("graph");
      } else {
        setAppState("setup");
      }
    } catch (err) {
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
