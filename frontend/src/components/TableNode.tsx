import { memo, useState } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import type { LineageNode } from "../types/graph";

const STATUS_COLORS: Record<string, string> = {
  ok: "border-slate-300",
  warning: "border-amber-400",
  error: "border-red-400",
  truncated: "border-blue-400 border-dashed",
};

const TYPE_BADGES: Record<string, { label: string; color: string }> = {
  table: { label: "T", color: "bg-emerald-100 text-emerald-700" },
  view: { label: "V", color: "bg-blue-100 text-blue-700" },
  materialized: { label: "M", color: "bg-purple-100 text-purple-700" },
  routine: { label: "R", color: "bg-orange-100 text-orange-700" },
};

interface TableNodeData {
  lineageNode: LineageNode;
  highlightedColumns: string[];
  dimmed: boolean;
  [key: string]: unknown;
}

function TableNodeComponent({ data }: NodeProps) {
  const { lineageNode, highlightedColumns = [], dimmed = false } = data as TableNodeData;
  const [expanded, setExpanded] = useState(false);
  const badge = TYPE_BADGES[lineageNode.type] ?? TYPE_BADGES.table;
  const borderColor = STATUS_COLORS[lineageNode.status] ?? STATUS_COLORS.ok;

  return (
    <div
      className={`bg-white rounded-lg border-2 shadow-sm min-w-[240px] transition-opacity ${borderColor} ${
        dimmed ? "opacity-30" : ""
      }`}
    >
      <Handle type="target" position={Position.Left} className="!bg-slate-400" />
      <Handle type="source" position={Position.Right} className="!bg-slate-400" />

      {/* Header */}
      <div
        className="flex items-center gap-2 px-3 py-2 cursor-pointer select-none"
        onClick={() => setExpanded(!expanded)}
      >
        <span
          className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${badge.color}`}
        >
          {badge.label}
        </span>
        <div className="flex-1 min-w-0">
          <div className="text-[11px] text-slate-400 truncate">
            {lineageNode.dataset}
          </div>
          <div className="text-sm font-medium text-slate-800 truncate">
            {lineageNode.name}
          </div>
        </div>
        {lineageNode.status !== "ok" && (
          <span
            className="text-xs"
            title={lineageNode.status_message ?? lineageNode.status}
          >
            {lineageNode.status === "warning"
              ? "\u26A0"
              : lineageNode.status === "error"
                ? "\u2716"
                : "\u22EF"}
          </span>
        )}
        <span className="text-slate-400 text-xs">
          {expanded ? "\u25B2" : "\u25BC"} {lineageNode.columns.length}
        </span>
      </div>

      {/* Columns */}
      {expanded && lineageNode.columns.length > 0 && (
        <div className="border-t border-slate-100 max-h-[300px] overflow-y-auto">
          {lineageNode.columns.map((col) => {
            const highlighted = highlightedColumns.includes(col.name);
            return (
              <div
                key={col.name}
                className={`flex items-center gap-2 px-3 py-1 text-xs ${
                  highlighted
                    ? "bg-yellow-50 font-medium"
                    : "hover:bg-slate-50"
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
                <span className="text-slate-700 truncate flex-1">
                  {col.name}
                </span>
                <span className="text-slate-400 flex-shrink-0">
                  {col.data_type}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export const TableNode = memo(TableNodeComponent);
