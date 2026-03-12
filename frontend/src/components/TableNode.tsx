import { memo, useState } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { LineageNode } from "../types/graph";

const STATUS_ACCENTS: Record<string, string> = {
  ok: "border-[var(--border-subtle)]",
  warning: "border-amber-500/40",
  error: "border-red-500/40",
  truncated: "border-blue-500/40 border-dashed",
};

const TYPE_BADGES: Record<string, { label: string; bg: string; text: string }> = {
  table: { label: "TABLE", bg: "bg-emerald-500/15", text: "text-emerald-400" },
  view: { label: "VIEW", bg: "bg-cyan-500/15", text: "text-cyan-400" },
  materialized: { label: "MAT", bg: "bg-purple-500/15", text: "text-purple-400" },
  routine: { label: "RTN", bg: "bg-orange-500/15", text: "text-orange-400" },
};

interface TableNodeData {
  lineageNode: LineageNode;
  highlightedColumns: string[];
  dimmed: boolean;
  missingUpstream: boolean;
  missingDownstream: boolean;
  onGapClick?: (nodeId: string, direction: "upstream" | "downstream") => void;
  onExpandNode?: (nodeId: string) => void;
  [key: string]: unknown;
}

function TableNodeComponent({ data }: NodeProps) {
  const {
    lineageNode,
    highlightedColumns = [],
    dimmed = false,
    missingUpstream = false,
    missingDownstream = false,
    onGapClick,
    onExpandNode,
  } = data as TableNodeData;
  const [expanded, setExpanded] = useState(false);
  const badge = TYPE_BADGES[lineageNode.type] ?? TYPE_BADGES.table;
  const borderColor = STATUS_ACCENTS[lineageNode.status] ?? STATUS_ACCENTS.ok;

  return (
    <div
      className={`node-card ${borderColor} ${dimmed ? "dimmed" : ""}`}
    >
      <Handle
        type="target"
        position={Position.Left}
        className="!w-2 !h-2 !bg-[var(--accent-cyan)] !border-[var(--bg-surface)] !border-2 !-left-1"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!w-2 !h-2 !bg-[var(--accent-teal)] !border-[var(--bg-surface)] !border-2 !-right-1"
      />

      {/* Gap indicators */}
      {missingUpstream && (
        <button
          className="absolute -left-5 top-1/2 -translate-y-1/2 w-4 h-4 rounded-full bg-amber-500/20 border border-amber-500/50 flex items-center justify-center text-amber-400 text-[10px] leading-none hover:bg-amber-500/40 hover:scale-110 transition-all cursor-pointer z-10"
          title="No upstream source — click to add manual edge"
          onClick={(e) => {
            e.stopPropagation();
            onGapClick?.(lineageNode.id, "upstream");
          }}
        >
          +
        </button>
      )}
      {missingDownstream && (
        <button
          className="absolute -right-5 top-1/2 -translate-y-1/2 w-4 h-4 rounded-full bg-amber-500/20 border border-amber-500/50 flex items-center justify-center text-amber-400 text-[10px] leading-none hover:bg-amber-500/40 hover:scale-110 transition-all cursor-pointer z-10"
          title="No downstream consumer — click to add manual edge"
          onClick={(e) => {
            e.stopPropagation();
            onGapClick?.(lineageNode.id, "downstream");
          }}
        >
          +
        </button>
      )}

      {/* Header */}
      <div
        className="flex items-center gap-2.5 px-3.5 py-2.5 cursor-pointer select-none group"
        onClick={() => setExpanded(!expanded)}
      >
        <span
          className={`text-[9px] font-semibold px-1.5 py-0.5 rounded ${badge.bg} ${badge.text} font-[var(--font-mono)] tracking-wider`}
        >
          {badge.label}
        </span>
        <div className="flex-1 min-w-0">
          <div className="text-[10px] text-[var(--text-muted)] truncate font-[var(--font-mono)]">
            {lineageNode.dataset}
          </div>
          <div className="text-sm font-medium text-[var(--text-primary)] truncate">
            {lineageNode.name}
          </div>
        </div>
        {lineageNode.status !== "ok" && (
          <span
            className={`text-xs ${
              lineageNode.status === "warning" ? "text-amber-400" :
              lineageNode.status === "error" ? "text-red-400" : "text-blue-400"
            }`}
            title={lineageNode.status_message ?? lineageNode.status}
          >
            {lineageNode.status === "warning" ? "\u26A0" :
             lineageNode.status === "error" ? "\u2716" : "\u22EF"}
          </span>
        )}
        <span className="text-[var(--text-muted)] text-xs font-[var(--font-mono)] group-hover:text-[var(--text-secondary)] transition-colors">
          {expanded ? "\u25B2" : "\u25BC"} {lineageNode.columns.length}
        </span>
      </div>

      {/* Columns */}
      {expanded && lineageNode.columns.length > 0 && (
        <div className="border-t border-[var(--border-subtle)] max-h-[300px] overflow-y-auto">
          {lineageNode.columns.map((col) => {
            const highlighted = highlightedColumns.includes(col.name);
            return (
              <div
                key={col.name}
                className={`flex items-center gap-2 px-3.5 py-1.5 text-xs transition-colors ${
                  highlighted
                    ? "bg-cyan-500/10 border-l-2 border-l-[var(--accent-cyan)]"
                    : "hover:bg-[var(--bg-hover)]"
                }`}
              >
                <span
                  className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                    col.lineage_status === "resolved"
                      ? "bg-emerald-400"
                      : "bg-amber-400"
                  }`}
                  title={
                    col.lineage_status === "resolved"
                      ? "Lineage resolved"
                      : "Lineage unknown"
                  }
                />
                <span className="text-[var(--text-primary)] truncate flex-1 font-[var(--font-mono)]">
                  {col.name}
                </span>
                <span className="text-[var(--text-muted)] flex-shrink-0 font-[var(--font-mono)] text-[10px]">
                  {col.data_type}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {/* Expand truncated node */}
      {lineageNode.status === "truncated" && onExpandNode && (
        <button
          className="w-full px-3.5 py-2 text-xs text-blue-400 hover:bg-blue-500/10 border-t border-dashed border-blue-500/30 transition-colors flex items-center justify-center gap-1.5"
          onClick={(e) => {
            e.stopPropagation();
            onExpandNode(lineageNode.id);
          }}
          title="Scan dependencies from this node"
        >
          <span className="text-[10px]">&#x25B6;</span>
          Expand
        </button>
      )}
    </div>
  );
}

export const TableNode = memo(TableNodeComponent);
