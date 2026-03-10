import { useState } from "react";
import type { LineageGraph, ScanConfig } from "../types/graph";
import { SearchBar } from "./SearchBar";
import type { SearchResult } from "../hooks/useColumnSearch";

interface ToolbarProps {
  graph: LineageGraph;
  searchQuery: string;
  onSearchQueryChange: (q: string) => void;
  searchResults: SearchResult[];
  onSearchSelect: (result: SearchResult) => void;
  hasActiveTrace: boolean;
  onClearTrace: () => void;
  onRescan: (config: ScanConfig) => void;
  onExport: () => void;
  scanning: boolean;
}

export function Toolbar({
  graph,
  searchQuery,
  onSearchQueryChange,
  searchResults,
  onSearchSelect,
  hasActiveTrace,
  onClearTrace,
  onRescan,
  onExport,
  scanning,
}: ToolbarProps) {
  const [showRescan, setShowRescan] = useState(false);
  const [target, setTarget] = useState(
    graph.metadata.scan_config.target ?? ""
  );
  const [datasets, setDatasets] = useState(
    graph.metadata.scan_config.datasets.join(", ")
  );
  const [depth, setDepth] = useState(
    graph.metadata.scan_config.depth?.toString() ?? ""
  );

  const handleRescan = () => {
    const dsArr = datasets
      .split(",")
      .map((d) => d.trim())
      .filter(Boolean);
    onRescan({
      target: target || null,
      datasets: dsArr,
      depth: depth ? parseInt(depth, 10) : null,
    });
    setShowRescan(false);
  };

  return (
    <div className="flex items-center gap-3 px-4 py-2 bg-white border-b border-slate-200 shadow-sm">
      <h1 className="text-sm font-semibold text-slate-700 whitespace-nowrap">
        SQL Lineage Tracker
      </h1>

      <div className="text-xs text-slate-400">
        {graph.metadata.project_id} | {graph.metadata.scan_stats.total_nodes}{" "}
        nodes, {graph.metadata.scan_stats.total_edges} edges
      </div>

      <div className="flex-1" />

      <SearchBar
        query={searchQuery}
        onQueryChange={onSearchQueryChange}
        results={searchResults}
        onSelect={onSearchSelect}
        hasActiveTrace={hasActiveTrace}
        onClear={onClearTrace}
      />

      <div className="relative">
        <button
          onClick={() => setShowRescan(!showRescan)}
          disabled={scanning}
          className="px-3 py-1.5 text-sm bg-blue-500 hover:bg-blue-600 disabled:bg-blue-300 text-white rounded-md"
        >
          {scanning ? "Scanning..." : "Re-scan"}
        </button>

        {showRescan && (
          <div className="absolute top-full right-0 mt-1 w-80 bg-white border border-slate-200 rounded-md shadow-lg z-50 p-3">
            <div className="space-y-2">
              <div>
                <label className="text-xs text-slate-500">Target</label>
                <input
                  type="text"
                  value={target}
                  onChange={(e) => setTarget(e.target.value)}
                  placeholder="dataset.table"
                  className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
                />
              </div>
              <div>
                <label className="text-xs text-slate-500">
                  Datasets (comma-separated)
                </label>
                <input
                  type="text"
                  value={datasets}
                  onChange={(e) => setDatasets(e.target.value)}
                  placeholder="staging, raw_data"
                  className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
                />
              </div>
              <div>
                <label className="text-xs text-slate-500">
                  Depth (dataset hops)
                </label>
                <input
                  type="number"
                  value={depth}
                  onChange={(e) => setDepth(e.target.value)}
                  placeholder="No limit"
                  className="w-full px-2 py-1 text-sm border border-slate-300 rounded"
                />
              </div>
              <button
                onClick={handleRescan}
                className="w-full px-3 py-1.5 text-sm bg-blue-500 hover:bg-blue-600 text-white rounded-md"
              >
                Start Scan
              </button>
            </div>
          </div>
        )}
      </div>

      <button
        onClick={onExport}
        className="px-3 py-1.5 text-sm bg-slate-100 hover:bg-slate-200 text-slate-700 rounded-md"
      >
        Export JSON
      </button>
    </div>
  );
}
