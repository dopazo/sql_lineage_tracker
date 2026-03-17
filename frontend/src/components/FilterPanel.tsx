import { useState, useMemo } from "react";
import type { LineageNode, LineageEdge, GraphFilters } from "../types/graph";

const NODE_TYPE_LABELS: Record<LineageNode["type"], string> = {
  table: "Table",
  view: "View",
  materialized: "Materialized",
  routine: "Routine",
};

const EDGE_TYPE_LABELS: Record<LineageEdge["edge_type"], string> = {
  automatic: "Automatic",
  manual: "Manual",
};

const NODE_TYPE_COLORS: Record<LineageNode["type"], string> = {
  table: "var(--accent-cyan)",
  view: "var(--accent-teal)",
  materialized: "var(--accent-amber)",
  routine: "var(--accent-purple)",
};

const EDGE_TYPE_COLORS: Record<LineageEdge["edge_type"], string> = {
  automatic: "var(--accent-cyan)",
  manual: "var(--accent-purple)",
};

const STATUS_LABELS: Record<LineageNode["status"], string> = {
  ok: "Resolved",
  warning: "Warning",
  error: "Error",
  truncated: "Truncated",
};

const STATUS_COLORS: Record<LineageNode["status"], string> = {
  ok: "var(--accent-teal)",
  warning: "var(--accent-amber)",
  error: "#f87171",
  truncated: "#60a5fa",
};

interface FilterPanelProps {
  filters: GraphFilters;
  datasets: string[];
  nodeTypes: LineageNode["type"][];
  nodeStatuses: LineageNode["status"][];
  edgeTypes: LineageEdge["edge_type"][];
  maxGraphDepth: number;
  isFiltered: boolean;
  onToggleDataset: (ds: string) => void;
  onToggleNodeType: (t: LineageNode["type"]) => void;
  onToggleEdgeType: (t: LineageEdge["edge_type"]) => void;
  onToggleStatus: (s: LineageNode["status"]) => void;
  onSetMaxDepth: (d: number | null) => void;
  onSetNameFilter: (name: string) => void;
  onReset: () => void;
  prunePoints?: Set<string>;
  hasPrunedNodes?: boolean;
  onRestorePrune?: (nodeId: string) => void;
  onClearAllPrunes?: () => void;
}

function Checkbox({
  checked,
  onToggle,
  label,
  color,
}: {
  checked: boolean;
  onToggle: () => void;
  label: string;
  color?: string;
}) {
  return (
    <div className="flex items-center gap-2 cursor-pointer group py-0.5" onClick={onToggle}>
      <span
        className="w-3.5 h-3.5 rounded border flex items-center justify-center shrink-0 transition-all"
        style={{
          borderColor: checked
            ? color ?? "var(--accent-cyan)"
            : "var(--border-medium)",
          background: checked
            ? color ?? "var(--accent-cyan)"
            : "transparent",
        }}
      >
        {checked && (
          <svg width="8" height="8" viewBox="0 0 10 10" fill="none">
            <path
              d="M2 5L4.5 7.5L8 2.5"
              stroke="#0a0e1a"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        )}
      </span>
      <span className="text-xs text-[var(--text-secondary)] group-hover:text-[var(--text-primary)] transition-colors truncate">
        {label}
      </span>
    </div>
  );
}

export function FilterPanel({
  filters,
  datasets,
  nodeTypes,
  nodeStatuses,
  edgeTypes,
  maxGraphDepth,
  isFiltered,
  onToggleDataset,
  onToggleNodeType,
  onToggleEdgeType,
  onToggleStatus,
  onSetMaxDepth,
  onSetNameFilter,
  onReset,
  prunePoints,
  hasPrunedNodes,
  onRestorePrune,
  onClearAllPrunes,
}: FilterPanelProps) {
  // Local dataset search state (Feature A)
  const [datasetSearch, setDatasetSearch] = useState("");

  const filteredDatasets = useMemo(() => {
    if (!datasetSearch) return datasets;
    const lower = datasetSearch.toLowerCase();
    return datasets.filter((ds) => ds.toLowerCase().includes(lower));
  }, [datasets, datasetSearch]);

  const allVisibleSelected = filteredDatasets.length > 0 && filteredDatasets.every((ds) => filters.datasets.has(ds));

  const selectAllVisible = () => {
    for (const ds of filteredDatasets) {
      if (!filters.datasets.has(ds)) onToggleDataset(ds);
    }
  };

  const deselectAllVisible = () => {
    for (const ds of filteredDatasets) {
      if (filters.datasets.has(ds)) onToggleDataset(ds);
    }
  };

  return (
    <div className="w-56 glass-elevated border-r border-[var(--border-subtle)] overflow-y-auto animate-fade-in shrink-0">
      <div className="p-3">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-xs font-semibold text-[var(--text-primary)] uppercase tracking-wider font-[var(--font-mono)]">
            Filters
          </h2>
          {isFiltered && (
            <button
              onClick={onReset}
              className="text-[10px] text-[var(--accent-cyan)] hover:text-[var(--accent-teal)] transition-colors cursor-pointer"
            >
              Reset
            </button>
          )}
        </div>

        {/* Table name filter (Feature B) */}
        <section className="mb-4">
          <h3 className="label-dark mb-1.5">Table name</h3>
          <input
            type="text"
            value={filters.nameFilter}
            onChange={(e) => onSetNameFilter(e.target.value)}
            placeholder="Filter by name..."
            className="input-dark w-full text-xs !py-1.5 !px-2.5 !rounded-md"
          />
        </section>

        {/* Dataset filter (Feature A: with search + select/deselect all) */}
        <section className="mb-4">
          <div className="flex items-center justify-between mb-1.5">
            <h3 className="label-dark !mb-0">
              Dataset
              {filters.datasets.size < datasets.length && (
                <span className="ml-1 text-[var(--accent-cyan)]">
                  ({filters.datasets.size}/{datasets.length})
                </span>
              )}
            </h3>
            {datasets.length > 1 && (
              <button
                onClick={allVisibleSelected ? deselectAllVisible : selectAllVisible}
                className="text-[10px] text-[var(--text-muted)] hover:text-[var(--accent-cyan)] transition-colors cursor-pointer"
              >
                {allVisibleSelected ? "None" : "All"}
              </button>
            )}
          </div>
          {datasets.length > 5 && (
            <input
              type="text"
              value={datasetSearch}
              onChange={(e) => setDatasetSearch(e.target.value)}
              placeholder="Search datasets..."
              className="input-dark w-full text-xs !py-1 !px-2 !rounded-md mb-1.5"
            />
          )}
          <div className={`space-y-0.5 ${datasets.length > 8 ? "max-h-36 overflow-y-auto pr-0.5" : ""}`}>
            {filteredDatasets.length === 0 ? (
              <p className="text-[10px] text-[var(--text-muted)] py-1">No match</p>
            ) : (
              filteredDatasets.map((ds) => (
                <Checkbox
                  key={ds}
                  checked={filters.datasets.has(ds)}
                  onToggle={() => onToggleDataset(ds)}
                  label={ds}
                />
              ))
            )}
          </div>
        </section>

        {/* Node type filter */}
        <section className="mb-4">
          <h3 className="label-dark mb-1.5">Node type</h3>
          <div className="space-y-0.5">
            {nodeTypes.map((t) => (
              <Checkbox
                key={t}
                checked={filters.nodeTypes.has(t)}
                onToggle={() => onToggleNodeType(t)}
                label={NODE_TYPE_LABELS[t]}
                color={NODE_TYPE_COLORS[t]}
              />
            ))}
          </div>
        </section>

        {/* Node status filter (Feature C) */}
        {nodeStatuses.length > 1 && (
          <section className="mb-4">
            <h3 className="label-dark mb-1.5">Status</h3>
            <div className="space-y-0.5">
              {nodeStatuses.map((s) => (
                <Checkbox
                  key={s}
                  checked={filters.statuses.has(s)}
                  onToggle={() => onToggleStatus(s)}
                  label={STATUS_LABELS[s]}
                  color={STATUS_COLORS[s]}
                />
              ))}
            </div>
          </section>
        )}

        {/* Edge type filter */}
        <section className="mb-4">
          <h3 className="label-dark mb-1.5">Edge type</h3>
          <div className="space-y-0.5">
            {edgeTypes.map((t) => (
              <Checkbox
                key={t}
                checked={filters.edgeTypes.has(t)}
                onToggle={() => onToggleEdgeType(t)}
                label={EDGE_TYPE_LABELS[t]}
                color={EDGE_TYPE_COLORS[t]}
              />
            ))}
          </div>
        </section>

        {/* Depth filter */}
        {maxGraphDepth > 0 && (
          <section className="mb-4">
            <h3 className="label-dark mb-1.5">Max depth</h3>
            <div className="space-y-2">
              <input
                type="range"
                min={0}
                max={maxGraphDepth}
                value={filters.maxDepth ?? maxGraphDepth}
                onChange={(e) => {
                  const v = parseInt(e.target.value, 10);
                  onSetMaxDepth(v >= maxGraphDepth ? null : v);
                }}
                className="w-full accent-[var(--accent-cyan)] h-1 cursor-pointer"
              />
              <div className="flex items-center justify-between">
                <span className="text-[10px] text-[var(--text-muted)] font-[var(--font-mono)]">
                  0
                </span>
                <span className="text-[11px] text-[var(--text-secondary)] font-[var(--font-mono)]">
                  {filters.maxDepth === null
                    ? "All levels"
                    : `${filters.maxDepth} level${filters.maxDepth !== 1 ? "s" : ""}`}
                </span>
                <span className="text-[10px] text-[var(--text-muted)] font-[var(--font-mono)]">
                  {maxGraphDepth}
                </span>
              </div>
            </div>
          </section>
        )}

        {/* Pruned branches */}
        {hasPrunedNodes && prunePoints && prunePoints.size > 0 && (
          <section>
            <div className="flex items-center justify-between mb-1.5">
              <h3 className="label-dark">Pruned branches</h3>
              {onClearAllPrunes && (
                <button
                  onClick={onClearAllPrunes}
                  className="text-[10px] text-[var(--accent-cyan)] hover:text-[var(--accent-teal)] transition-colors cursor-pointer"
                >
                  Restore all
                </button>
              )}
            </div>
            <div className="space-y-1">
              {[...prunePoints].map((nodeId) => {
                const parts = nodeId.split(".");
                const displayName = parts.length > 1 ? parts.slice(1).join(".") : nodeId;
                return (
                  <div
                    key={nodeId}
                    className="flex items-center justify-between gap-1 py-0.5 group"
                  >
                    <span className="text-xs text-[var(--text-muted)] truncate" title={nodeId}>
                      {displayName}
                    </span>
                    {onRestorePrune && (
                      <button
                        onClick={() => onRestorePrune(nodeId)}
                        className="text-[10px] text-[var(--text-muted)] hover:text-green-400 transition-colors shrink-0 opacity-0 group-hover:opacity-100 cursor-pointer"
                        title="Restore this branch"
                      >
                        &#x21A9;
                      </button>
                    )}
                  </div>
                );
              })}
            </div>
          </section>
        )}
      </div>
    </div>
  );
}
