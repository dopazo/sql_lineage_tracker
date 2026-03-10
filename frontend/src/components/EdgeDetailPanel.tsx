import type { LineageEdge } from "../types/graph";

interface EdgeDetailPanelProps {
  edge: LineageEdge;
  onClose: () => void;
}

const TRANSFORM_COLORS: Record<string, string> = {
  direct: "bg-emerald-100 text-emerald-700",
  rename: "bg-blue-100 text-blue-700",
  expression: "bg-purple-100 text-purple-700",
  aggregation: "bg-orange-100 text-orange-700",
  external: "bg-pink-100 text-pink-700",
  new_field: "bg-cyan-100 text-cyan-700",
  unknown: "bg-slate-100 text-slate-500",
};

export function EdgeDetailPanel({ edge, onClose }: EdgeDetailPanelProps) {
  return (
    <div className="w-80 bg-white border-l border-slate-200 overflow-y-auto flex flex-col">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200">
        <h2 className="text-sm font-semibold text-slate-800">Edge Detail</h2>
        <button
          onClick={onClose}
          className="text-slate-400 hover:text-slate-600 text-lg"
        >
          &times;
        </button>
      </div>

      <div className="p-4 space-y-4 flex-1">
        <div>
          <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
            Connection
          </div>
          <div className="text-sm">
            <span className="font-medium">{edge.source_node}</span>
            <span className="text-slate-400 mx-2">&rarr;</span>
            <span className="font-medium">{edge.target_node}</span>
          </div>
        </div>

        <div>
          <div className="text-xs text-slate-400 uppercase tracking-wide mb-1">
            Type
          </div>
          <div className="text-sm">
            {edge.edge_type}
            {edge.description && (
              <span className="text-slate-400 ml-2">
                &mdash; {edge.description}
              </span>
            )}
          </div>
        </div>

        <div>
          <div className="text-xs text-slate-400 uppercase tracking-wide mb-2">
            Column Mappings ({edge.column_mappings.length})
          </div>
          <div className="space-y-2">
            {edge.column_mappings.map((mapping, i) => (
              <div
                key={i}
                className="border border-slate-200 rounded p-2 text-xs"
              >
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-slate-600">
                    {mapping.source_columns.join(", ") || "(none)"}
                  </span>
                  <span className="text-slate-400">&rarr;</span>
                  <span className="font-medium text-slate-800">
                    {mapping.target_column}
                  </span>
                </div>
                <span
                  className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${
                    TRANSFORM_COLORS[mapping.transformation] ??
                    TRANSFORM_COLORS.unknown
                  }`}
                >
                  {mapping.transformation}
                </span>
                {mapping.expression && (
                  <code className="block mt-1 text-[11px] text-slate-500 bg-slate-50 px-1 py-0.5 rounded">
                    {mapping.expression}
                  </code>
                )}
                {mapping.description && (
                  <div className="mt-1 text-slate-400">
                    {mapping.description}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
