import { useEffect, useRef } from "react";
import { useLineageGraph } from "./hooks/useLineageGraph";
import { useScanProgress } from "./hooks/useScanProgress";
import { ScanSetupScreen } from "./components/ScanSetupScreen";
import { GraphCanvas } from "./components/GraphCanvas";
import type { ScanConfig } from "./types/graph";

export function App() {
  const { graph, appState, setAppState, error, loadGraph } = useLineageGraph();
  const { scanning, messages, scanError, completed, runScan, dismissMessages } = useScanProgress();
  const autoTransitionRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleStartScan = (config: ScanConfig) => {
    runScan(config, () => {
      // Load graph data silently (don't change appState yet)
      loadGraph(true);
    });
  };

  // Auto-transition to graph view 3 seconds after scan completes (setup screen only)
  const hasGraphData = !!(graph && Object.keys(graph.nodes).length > 0);
  useEffect(() => {
    if (completed && !scanError && appState === "setup" && hasGraphData) {
      autoTransitionRef.current = setTimeout(() => {
        setAppState("graph");
      }, 3000);
    }
    return () => {
      if (autoTransitionRef.current) {
        clearTimeout(autoTransitionRef.current);
        autoTransitionRef.current = null;
      }
    };
  }, [completed, scanError, appState, hasGraphData, setAppState]);

  const handleDismissScan = () => {
    // Clear auto-transition timer if user dismisses manually
    if (autoTransitionRef.current) {
      clearTimeout(autoTransitionRef.current);
      autoTransitionRef.current = null;
    }
    dismissMessages();
    // Now transition to graph view if we have data
    if (graph && Object.keys(graph.nodes).length > 0) {
      setAppState("graph");
    }
  };

  if (appState === "loading") {
    return (
      <div className="flex items-center justify-center min-h-screen bg-[var(--bg-deep)] bg-grid">
        <div className="text-center animate-fade-in">
          <div className="w-10 h-10 border-2 border-[var(--accent-cyan)] border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <p className="text-sm text-[var(--text-muted)] font-[var(--font-mono)]">
            Connecting to server...
          </p>
        </div>
      </div>
    );
  }

  if (appState === "error") {
    return (
      <div className="flex items-center justify-center min-h-screen bg-[var(--bg-deep)] bg-grid">
        <div className="text-center max-w-md animate-fade-in glass rounded-xl p-8">
          <div className="w-12 h-12 rounded-full bg-red-500/10 border border-red-500/20 flex items-center justify-center mx-auto mb-4">
            <span className="text-red-400 text-xl">!</span>
          </div>
          <p className="text-lg font-semibold text-red-400 mb-2">
            Connection Error
          </p>
          <p className="text-sm text-[var(--text-muted)] mb-6">{error}</p>
          <button onClick={() => loadGraph()} className="btn-primary">
            Retry Connection
          </button>
        </div>
      </div>
    );
  }

  if (appState === "setup") {
    return (
      <ScanSetupScreen
        onStartScan={handleStartScan}
        scanning={scanning}
        scanMessages={messages}
        scanError={scanError}
        scanCompleted={completed}
        onDismissScan={handleDismissScan}
      />
    );
  }

  if (appState === "graph" && graph) {
    return (
      <GraphCanvas
        graph={graph}
        onGraphReload={loadGraph}
      />
    );
  }

  return null;
}
