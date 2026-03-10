import { useLineageGraph } from "./hooks/useLineageGraph";
import { useScanProgress } from "./hooks/useScanProgress";
import { ScanSetupScreen } from "./components/ScanSetupScreen";
import { GraphCanvas } from "./components/GraphCanvas";
import type { ScanConfig } from "./types/graph";

export function App() {
  const { graph, appState, error, loadGraph } = useLineageGraph();
  const { scanning, messages, scanError, runScan } = useScanProgress();

  const handleStartScan = (config: ScanConfig) => {
    runScan(config, () => {
      loadGraph();
    });
  };

  if (appState === "loading") {
    return (
      <div className="flex items-center justify-center min-h-screen bg-slate-50">
        <div className="text-center">
          <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-3" />
          <p className="text-sm text-slate-500">Connecting to server...</p>
        </div>
      </div>
    );
  }

  if (appState === "error") {
    return (
      <div className="flex items-center justify-center min-h-screen bg-slate-50">
        <div className="text-center max-w-md">
          <p className="text-lg font-semibold text-red-600 mb-2">
            Connection Error
          </p>
          <p className="text-sm text-slate-500 mb-4">{error}</p>
          <button
            onClick={loadGraph}
            className="px-4 py-2 bg-blue-500 hover:bg-blue-600 text-white rounded-md text-sm"
          >
            Retry
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
      />
    );
  }

  if (appState === "graph" && graph) {
    return (
      <GraphCanvas
        graph={graph}
        onGraphReload={() => {
          loadGraph();
        }}
      />
    );
  }

  return null;
}
