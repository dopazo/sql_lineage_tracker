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

  const stats = graph.metadata.scan_stats;

  return (
    <div className="flex items-center gap-4 px-5 py-2.5 glass border-b border-[var(--border-subtle)]">
      {/* Logo + Title */}
      <div className="flex items-center gap-2.5 shrink-0">
        <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-[var(--accent-teal)] to-[var(--accent-cyan)] flex items-center justify-center">
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
            <path d="M2 4L8 1L14 4L8 7L2 4Z" fill="#0a0e1a" opacity="0.8"/>
            <path d="M2 8L8 5L14 8L8 11L2 8Z" fill="#0a0e1a" opacity="0.6"/>
            <path d="M2 12L8 9L14 12L8 15L2 12Z" fill="#0a0e1a" opacity="0.4"/>
          </svg>
        </div>
        <h1 className="text-sm font-semibold text-[var(--text-primary)] whitespace-nowrap">
          SQL Lineage
        </h1>
      </div>

      {/* Stats */}
      <div className="flex items-center gap-3 text-[11px] text-[var(--text-muted)] font-[var(--font-mono)] shrink-0">
        <span className="px-2 py-1 rounded bg-[var(--bg-deep)] border border-[var(--border-subtle)]">
          {graph.metadata.project_id}
        </span>
        <span>
          <span className="text-[var(--accent-cyan)]">{stats.total_nodes}</span> nodes
        </span>
        <span>
          <span className="text-[var(--accent-teal)]">{stats.total_edges}</span> edges
        </span>
      </div>

      <div className="flex-1" />

      {/* Search */}
      <SearchBar
        query={searchQuery}
        onQueryChange={onSearchQueryChange}
        results={searchResults}
        onSelect={onSearchSelect}
        hasActiveTrace={hasActiveTrace}
        onClear={onClearTrace}
      />

      {/* Rescan */}
      <div className="relative">
        <button
          onClick={() => setShowRescan(!showRescan)}
          disabled={scanning}
          className="btn-primary text-sm py-2 px-4"
        >
          {scanning ? (
            <span className="flex items-center gap-2">
              <span className="w-3 h-3 border-2 border-[#0a0e1a]/40 border-t-[#0a0e1a] rounded-full animate-spin" />
              Scanning
            </span>
          ) : (
            "Re-scan"
          )}
        </button>

        {showRescan && (
          <>
            {/* Backdrop to close */}
            <div
              className="fixed inset-0 z-40"
              onClick={() => setShowRescan(false)}
            />
            <div className="absolute top-full right-0 mt-2 w-80 glass-elevated rounded-xl z-50 p-4 animate-fade-in">
              <div className="space-y-3">
                <div>
                  <label className="label-dark">Target</label>
                  <input
                    type="text"
                    value={target}
                    onChange={(e) => setTarget(e.target.value)}
                    placeholder="dataset.table"
                    className="input-dark w-full"
                  />
                </div>
                <div>
                  <label className="label-dark">Datasets</label>
                  <input
                    type="text"
                    value={datasets}
                    onChange={(e) => setDatasets(e.target.value)}
                    placeholder="staging, raw_data"
                    className="input-dark w-full"
                  />
                </div>
                <div>
                  <label className="label-dark">Depth</label>
                  <input
                    type="number"
                    value={depth}
                    onChange={(e) => setDepth(e.target.value)}
                    placeholder="No limit"
                    className="input-dark w-full"
                  />
                </div>
                <button onClick={handleRescan} className="btn-primary w-full">
                  Start Scan
                </button>
              </div>
            </div>
          </>
        )}
      </div>

      {/* Export */}
      <button onClick={onExport} className="btn-ghost text-sm">
        Export
      </button>
    </div>
  );
}
