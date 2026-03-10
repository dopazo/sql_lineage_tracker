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

interface FilterPanelProps {
  filters: GraphFilters;
  datasets: string[];
  nodeTypes: LineageNode["type"][];
  edgeTypes: LineageEdge["edge_type"][];
  maxGraphDepth: number;
  isFiltered: boolean;
  onToggleDataset: (ds: string) => void;
  onToggleNodeType: (t: LineageNode["type"]) => void;
  onToggleEdgeType: (t: LineageEdge["edge_type"]) => void;
  onSetMaxDepth: (d: number | null) => void;
  onReset: () => void;
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
  edgeTypes,
  maxGraphDepth,
  isFiltered,
  onToggleDataset,
  onToggleNodeType,
  onToggleEdgeType,
  onSetMaxDepth,
  onReset,
}: FilterPanelProps) {
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

        {/* Dataset filter */}
        <section className="mb-4">
          <h3 className="label-dark mb-1.5">Dataset</h3>
          <div className="space-y-0.5">
            {datasets.map((ds) => (
              <Checkbox
                key={ds}
                checked={filters.datasets.has(ds)}
                onToggle={() => onToggleDataset(ds)}
                label={ds}
              />
            ))}
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
          <section>
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
      </div>
    </div>
  );
}
