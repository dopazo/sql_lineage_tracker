import { useState, useMemo, useCallback, useEffect } from "react";
import type {
  LineageGraph,
  LineageNode,
  LineageEdge,
  ColumnInfo,
  ColumnMapping,
  ManualEdgeRequest,
} from "../types/graph";
import {
  createManualEdge,
  updateManualEdge,
  deleteManualEdge,
  getColumns,
} from "../api/client";

/* ── Types ───────────────────────────────────────────────── */

type TransformationType = ColumnMapping["transformation"];

interface MappingRow {
  sourceColumns: string[];
  targetColumn: string;
  transformation: TransformationType;
  expression: string;
  description: string;
}

export interface ManualEdgeModalProps {
  graph: LineageGraph;
  /** Pre-selected node id (from context menu or gap click) */
  anchorNodeId: string;
  /** Whether the anchor node is the source or target of the new edge */
  direction: "upstream" | "downstream";
  /** If editing an existing edge, pass it here */
  editingEdge?: LineageEdge;
  onClose: () => void;
  onSaved: () => void;
}

const TRANSFORMATIONS: { value: TransformationType; label: string }[] = [
  { value: "direct", label: "Direct" },
  { value: "rename", label: "Rename" },
  { value: "expression", label: "Expression" },
  { value: "aggregation", label: "Aggregation" },
  { value: "literal", label: "Literal" },
  { value: "external", label: "External" },
  { value: "new_field", label: "New Field" },
  { value: "unknown", label: "Unknown" },
];

import { TRANSFORM_STYLES } from "../constants/transforms";

/* ── Helpers ─────────────────────────────────────────────── */

function emptyRow(): MappingRow {
  return {
    sourceColumns: [],
    targetColumn: "",
    transformation: "direct",
    expression: "",
    description: "",
  };
}

function buildDefaultMappings(
  sourceCols: ColumnInfo[],
  targetCols: ColumnInfo[]
): MappingRow[] {
  if (targetCols.length === 0 && sourceCols.length === 0) return [emptyRow()];

  const sourceNames = new Set(sourceCols.map((c) => c.name));
  const rows: MappingRow[] = [];

  for (const tc of targetCols) {
    if (sourceNames.has(tc.name)) {
      rows.push({
        sourceColumns: [tc.name],
        targetColumn: tc.name,
        transformation: "direct",
        expression: "",
        description: "",
      });
    } else {
      rows.push({
        sourceColumns: [],
        targetColumn: tc.name,
        transformation: "unknown",
        expression: "",
        description: "",
      });
    }
  }

  return rows.length > 0 ? rows : [emptyRow()];
}

function edgeToMappings(edge: LineageEdge): MappingRow[] {
  return edge.column_mappings.map((m) => ({
    sourceColumns: [...m.source_columns],
    targetColumn: m.target_column,
    transformation: m.transformation,
    expression: m.expression ?? "",
    description: m.description ?? "",
  }));
}

/* ── Node Autocomplete ───────────────────────────────────── */

function NodeSelect({
  value,
  onChange,
  nodes,
  label,
  disabled,
}: {
  value: string;
  onChange: (v: string) => void;
  nodes: Record<string, LineageNode>;
  label: string;
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState("");

  const nodeIds = useMemo(() => Object.keys(nodes).sort(), [nodes]);

  const filtered = useMemo(() => {
    if (!filter) return nodeIds;
    const lf = filter.toLowerCase();
    return nodeIds.filter((id) => id.toLowerCase().includes(lf));
  }, [nodeIds, filter]);

  const handleSelect = useCallback(
    (id: string) => {
      onChange(id);
      setFilter("");
      setOpen(false);
    },
    [onChange]
  );

  return (
    <div>
      <label className="label-dark">{label}</label>
      <div className="relative">
        {disabled ? (
          <div className="input-dark w-full opacity-60 font-[var(--font-mono)] text-sm">
            {value}
          </div>
        ) : (
          <input
            type="text"
            value={open ? filter : value}
            onChange={(e) => {
              setFilter(e.target.value);
              if (!open) setOpen(true);
            }}
            onFocus={() => {
              setOpen(true);
              setFilter(value);
            }}
            onBlur={() => setTimeout(() => setOpen(false), 200)}
            placeholder="dataset.table"
            className="input-dark w-full font-[var(--font-mono)] text-sm"
          />
        )}
        {open && filtered.length > 0 && (
          <div className="absolute z-50 top-full left-0 right-0 mt-1 max-h-48 overflow-y-auto bg-[var(--bg-elevated)] border border-[var(--border-medium)] rounded-lg shadow-xl">
            {filtered.map((id) => (
              <button
                key={id}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => handleSelect(id)}
                className="w-full text-left px-3 py-2 text-xs font-[var(--font-mono)] text-[var(--text-secondary)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)] transition-colors"
              >
                {id}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Main Modal ──────────────────────────────────────────── */

export function ManualEdgeModal({
  graph,
  anchorNodeId,
  direction,
  editingEdge,
  onClose,
  onSaved,
}: ManualEdgeModalProps) {
  const isEdit = !!editingEdge;

  // Source = upstream node, Target = downstream node
  // "upstream" direction: anchor is target, user picks source
  // "downstream" direction: anchor is source, user picks target
  const [otherNodeId, setOtherNodeId] = useState(() => {
    if (editingEdge) {
      return direction === "upstream"
        ? editingEdge.source_node
        : editingEdge.target_node;
    }
    return "";
  });
  const [edgeDescription, setEdgeDescription] = useState(
    editingEdge?.description ?? ""
  );
  const [mappings, setMappings] = useState<MappingRow[]>(() =>
    editingEdge ? edgeToMappings(editingEdge) : [emptyRow()]
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadingColumns, setLoadingColumns] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);

  const sourceNodeId =
    direction === "upstream" ? otherNodeId : anchorNodeId;
  const targetNodeId =
    direction === "upstream" ? anchorNodeId : otherNodeId;

  const sourceNode = graph.nodes[sourceNodeId] ?? null;
  const targetNode = graph.nodes[targetNodeId] ?? null;

  const sourceColumns: ColumnInfo[] = sourceNode?.columns ?? [];
  const targetColumns: ColumnInfo[] = targetNode?.columns ?? [];

  // When the other node changes and both nodes are known, auto-build mappings
  const [lastAutoBuilt, setLastAutoBuilt] = useState("");
  useEffect(() => {
    const key = `${sourceNodeId}::${targetNodeId}`;
    if (isEdit || !sourceNodeId || !targetNodeId || key === lastAutoBuilt)
      return;
    if (sourceColumns.length > 0 || targetColumns.length > 0) {
      setMappings(buildDefaultMappings(sourceColumns, targetColumns));
      setLastAutoBuilt(key);
    }
  }, [
    sourceNodeId,
    targetNodeId,
    sourceColumns,
    targetColumns,
    isEdit,
    lastAutoBuilt,
  ]);

  // Try fetching columns from BigQuery if the other node is known but has no columns
  useEffect(() => {
    if (!otherNodeId || isEdit) return;
    const otherNode = graph.nodes[otherNodeId];
    if (!otherNode || otherNode.columns.length > 0) return;
    setLoadingColumns(true);
    getColumns(otherNode.dataset, otherNode.name)
      .then((cols) => {
        // Merge fetched columns into the node so auto-mapping can use them
        if (cols.length > 0) {
          otherNode.columns = cols;
        }
      })
      .catch(() => {})
      .finally(() => setLoadingColumns(false));
  }, [otherNodeId, graph.nodes, isEdit]);

  /* ── Mapping Row Helpers ─────────────────────────────── */
  const updateMapping = useCallback(
    (idx: number, update: Partial<MappingRow>) => {
      setMappings((prev) =>
        prev.map((m, i) => (i === idx ? { ...m, ...update } : m))
      );
    },
    []
  );

  const addMapping = useCallback(() => {
    setMappings((prev) => [...prev, emptyRow()]);
  }, []);

  const removeMapping = useCallback(
    (idx: number) => {
      setMappings((prev) => {
        const next = prev.filter((_, i) => i !== idx);
        return next.length === 0 ? [emptyRow()] : next;
      });
    },
    []
  );

  /* ── Submit ──────────────────────────────────────────── */
  const canSubmit =
    sourceNodeId &&
    targetNodeId &&
    sourceNodeId !== targetNodeId &&
    mappings.some((m) => m.targetColumn.trim());

  const handleSubmit = async () => {
    if (!canSubmit) return;
    setSaving(true);
    setError(null);

    const payload: ManualEdgeRequest = {
      source_node: sourceNodeId,
      target_node: targetNodeId,
      description: edgeDescription || undefined,
      column_mappings: mappings
        .filter((m) => m.targetColumn.trim())
        .map((m) => ({
          source_columns: m.sourceColumns.filter(Boolean),
          target_column: m.targetColumn.trim(),
          transformation: m.transformation,
          expression: m.expression || undefined,
          description: m.description || undefined,
        })),
    };

    try {
      if (isEdit && editingEdge) {
        await updateManualEdge(editingEdge.id, payload);
      } else {
        await createManualEdge(payload);
      }
      onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save edge");
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!editingEdge) return;
    setSaving(true);
    setError(null);
    try {
      await deleteManualEdge(editingEdge.id);
      onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete edge");
    } finally {
      setSaving(false);
    }
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-50 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4 pointer-events-none">
        <div
          className="glass-elevated rounded-xl w-full max-w-2xl max-h-[85vh] flex flex-col animate-fade-in pointer-events-auto"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border-subtle)]">
            <h2 className="text-sm font-semibold text-[var(--text-primary)]">
              {isEdit ? "Edit Manual Edge" : "Create Manual Edge"}
            </h2>
            <button
              onClick={onClose}
              className="w-6 h-6 rounded-md flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
            >
              &times;
            </button>
          </div>

          {/* Body */}
          <div className="flex-1 overflow-y-auto p-5 space-y-5">
            {/* Connection */}
            <div className="flex items-end gap-3">
              <div className="flex-1">
                <NodeSelect
                  value={sourceNodeId}
                  onChange={(v) => {
                    if (direction === "upstream") setOtherNodeId(v);
                  }}
                  nodes={graph.nodes}
                  label="Source"
                  disabled={direction === "downstream"}
                />
              </div>
              <div className="pb-2 text-[var(--text-muted)]">
                <svg
                  width="20"
                  height="12"
                  viewBox="0 0 20 12"
                  className="shrink-0"
                >
                  <path
                    d="M0 6h16M13 2l4 4-4 4"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    fill="none"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </div>
              <div className="flex-1">
                <NodeSelect
                  value={targetNodeId}
                  onChange={(v) => {
                    if (direction === "downstream") setOtherNodeId(v);
                  }}
                  nodes={graph.nodes}
                  label="Target"
                  disabled={direction === "upstream"}
                />
              </div>
            </div>

            {/* Description */}
            <div>
              <label className="label-dark">Description</label>
              <input
                type="text"
                value={edgeDescription}
                onChange={(e) => setEdgeDescription(e.target.value)}
                placeholder="e.g., Python ETL process, external pipeline..."
                className="input-dark w-full text-sm"
              />
            </div>

            {/* Column Mappings */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <span className="label-dark !mb-0">
                  Column Mappings ({mappings.length})
                </span>
                <button
                  onClick={addMapping}
                  className="text-xs text-[var(--accent-cyan)] hover:text-[var(--accent-teal)] transition-colors font-medium"
                >
                  + Add mapping
                </button>
              </div>

              {loadingColumns && (
                <div className="text-xs text-[var(--text-muted)] mb-2">
                  Loading columns...
                </div>
              )}

              <div className="space-y-2 max-h-[40vh] overflow-y-auto pr-1">
                {mappings.map((row, idx) => (
                  <MappingRowEditor
                    key={idx}
                    row={row}
                    index={idx}
                    sourceColumns={sourceColumns}
                    targetColumns={targetColumns}
                    onChange={updateMapping}
                    onRemove={removeMapping}
                    canRemove={mappings.length > 1}
                  />
                ))}
              </div>
            </div>

            {/* Error */}
            {error && (
              <div className="text-xs text-red-400 bg-red-500/10 px-3 py-2 rounded-lg border border-red-500/20">
                {error}
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between px-5 py-4 border-t border-[var(--border-subtle)]">
            <div>
              {isEdit && !showDeleteConfirm && (
                <button
                  onClick={() => setShowDeleteConfirm(true)}
                  className="text-xs text-red-400 hover:text-red-300 transition-colors"
                  disabled={saving}
                >
                  Delete edge
                </button>
              )}
              {isEdit && showDeleteConfirm && (
                <div className="flex items-center gap-2">
                  <span className="text-xs text-red-400">Delete?</span>
                  <button
                    onClick={handleDelete}
                    className="text-xs text-red-400 font-semibold hover:text-red-300"
                    disabled={saving}
                  >
                    Yes
                  </button>
                  <button
                    onClick={() => setShowDeleteConfirm(false)}
                    className="text-xs text-[var(--text-muted)] hover:text-[var(--text-secondary)]"
                    disabled={saving}
                  >
                    No
                  </button>
                </div>
              )}
            </div>
            <div className="flex gap-2">
              <button onClick={onClose} className="btn-ghost text-sm">
                Cancel
              </button>
              <button
                onClick={handleSubmit}
                disabled={!canSubmit || saving}
                className="btn-primary text-sm"
              >
                {saving ? "Saving..." : isEdit ? "Update" : "Create"}
              </button>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

/* ── Mapping Row Editor ──────────────────────────────────── */

function MappingRowEditor({
  row,
  index,
  sourceColumns,
  targetColumns,
  onChange,
  onRemove,
  canRemove,
}: {
  row: MappingRow;
  index: number;
  sourceColumns: ColumnInfo[];
  targetColumns: ColumnInfo[];
  onChange: (idx: number, update: Partial<MappingRow>) => void;
  onRemove: (idx: number) => void;
  canRemove: boolean;
}) {
  const [expanded, setExpanded] = useState(false);

  const needsExpression =
    row.transformation === "expression" ||
    row.transformation === "aggregation";

  return (
    <div className="bg-[var(--bg-deep)] border border-[var(--border-subtle)] rounded-lg p-3 text-xs">
      <div className="flex items-start gap-2">
        {/* Source columns */}
        <div className="flex-1 min-w-0">
          <ColumnMultiSelect
            label="Source"
            selected={row.sourceColumns}
            available={sourceColumns}
            onChange={(cols) => onChange(index, { sourceColumns: cols })}
          />
        </div>

        {/* Arrow */}
        <div className="pt-6 text-[var(--text-muted)]">
          <svg width="14" height="10" viewBox="0 0 16 10" className="shrink-0">
            <path
              d="M0 5h13M10 1l4 4-4 4"
              stroke="currentColor"
              strokeWidth="1.5"
              fill="none"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </div>

        {/* Target column */}
        <div className="flex-1 min-w-0">
          <ColumnSingleSelect
            label="Target"
            value={row.targetColumn}
            available={targetColumns}
            onChange={(col) => onChange(index, { targetColumn: col })}
          />
        </div>

        {/* Transformation */}
        <div className="w-32 shrink-0">
          <span className="label-dark text-[10px]">Type</span>
          <select
            value={row.transformation}
            onChange={(e) =>
              onChange(index, {
                transformation: e.target.value as TransformationType,
              })
            }
            className="input-dark w-full text-xs !py-1.5 !px-2"
          >
            {TRANSFORMATIONS.map((t) => (
              <option key={t.value} value={t.value}>
                {t.label}
              </option>
            ))}
          </select>
        </div>

        {/* Actions */}
        <div className="flex flex-col gap-1 pt-5">
          <button
            onClick={() => setExpanded(!expanded)}
            className="w-5 h-5 rounded flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
            title="Toggle details"
          >
            <svg
              width="10"
              height="10"
              viewBox="0 0 10 10"
              className={`transition-transform ${expanded ? "rotate-180" : ""}`}
            >
              <path
                d="M2 4l3 3 3-3"
                stroke="currentColor"
                strokeWidth="1.5"
                fill="none"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </button>
          {canRemove && (
            <button
              onClick={() => onRemove(index)}
              className="w-5 h-5 rounded flex items-center justify-center text-[var(--text-muted)] hover:text-red-400 hover:bg-red-500/10 transition-colors"
              title="Remove mapping"
            >
              &times;
            </button>
          )}
        </div>
      </div>

      {/* Transformation badge */}
      <div className="mt-2">
        <span
          className={`inline-block px-2 py-0.5 rounded text-[10px] font-semibold font-[var(--font-mono)] uppercase tracking-wider ${
            TRANSFORM_STYLES[row.transformation] ?? TRANSFORM_STYLES.unknown
          }`}
        >
          {row.transformation}
        </span>
      </div>

      {/* Expanded details */}
      {(expanded || needsExpression) && (
        <div className="mt-2 space-y-2">
          {needsExpression && (
            <div>
              <span className="label-dark text-[10px]">Expression</span>
              <input
                type="text"
                value={row.expression}
                onChange={(e) =>
                  onChange(index, { expression: e.target.value })
                }
                placeholder="e.g., SUM(amount), UPPER(name)"
                className="input-dark w-full text-xs !py-1.5"
              />
            </div>
          )}
          {expanded && (
            <div>
              <span className="label-dark text-[10px]">Description</span>
              <input
                type="text"
                value={row.description}
                onChange={(e) =>
                  onChange(index, { description: e.target.value })
                }
                placeholder="Optional note..."
                className="input-dark w-full text-xs !py-1.5"
              />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ── Column Selectors ────────────────────────────────────── */

function ColumnMultiSelect({
  label,
  selected,
  available,
  onChange,
}: {
  label: string;
  selected: string[];
  available: ColumnInfo[];
  onChange: (cols: string[]) => void;
}) {
  const [inputVal, setInputVal] = useState("");
  const [open, setOpen] = useState(false);

  const filtered = useMemo(() => {
    const lf = inputVal.toLowerCase();
    return available
      .filter((c) => !selected.includes(c.name))
      .filter((c) => !lf || c.name.toLowerCase().includes(lf));
  }, [available, selected, inputVal]);

  const addCol = (name: string) => {
    onChange([...selected, name]);
    setInputVal("");
  };

  const removeCol = (name: string) => {
    onChange(selected.filter((c) => c !== name));
  };

  return (
    <div>
      <span className="label-dark text-[10px]">{label}</span>
      <div className="relative">
        <div className="input-dark w-full !p-1.5 flex flex-wrap gap-1 min-h-[30px]">
          {selected.map((col) => (
            <span
              key={col}
              className="inline-flex items-center gap-1 bg-[var(--bg-hover)] text-[var(--text-secondary)] rounded px-1.5 py-0.5 text-[10px] font-[var(--font-mono)]"
            >
              {col}
              <button
                onClick={() => removeCol(col)}
                className="text-[var(--text-muted)] hover:text-red-400 transition-colors"
              >
                &times;
              </button>
            </span>
          ))}
          <input
            type="text"
            value={inputVal}
            onChange={(e) => {
              setInputVal(e.target.value);
              if (!open) setOpen(true);
            }}
            onFocus={() => setOpen(true)}
            onBlur={() => setTimeout(() => setOpen(false), 200)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && inputVal.trim()) {
                e.preventDefault();
                // Add typed value if it's not in dropdown
                addCol(inputVal.trim());
              }
            }}
            placeholder={selected.length === 0 ? "column..." : ""}
            className="flex-1 min-w-[60px] bg-transparent text-[10px] font-[var(--font-mono)] text-[var(--text-primary)] outline-none placeholder:text-[var(--text-muted)]"
          />
        </div>
        {open && filtered.length > 0 && (
          <div className="absolute z-50 top-full left-0 right-0 mt-1 max-h-32 overflow-y-auto bg-[var(--bg-elevated)] border border-[var(--border-medium)] rounded-lg shadow-xl">
            {filtered.map((c) => (
              <button
                key={c.name}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => addCol(c.name)}
                className="w-full text-left px-2 py-1.5 text-[10px] font-[var(--font-mono)] text-[var(--text-secondary)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)] transition-colors flex justify-between"
              >
                <span>{c.name}</span>
                <span className="text-[var(--text-muted)]">{c.data_type}</span>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function ColumnSingleSelect({
  label,
  value,
  available,
  onChange,
}: {
  label: string;
  value: string;
  available: ColumnInfo[];
  onChange: (col: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState("");

  const filtered = useMemo(() => {
    const lf = filter.toLowerCase();
    return available.filter(
      (c) => !lf || c.name.toLowerCase().includes(lf)
    );
  }, [available, filter]);

  return (
    <div>
      <span className="label-dark text-[10px]">{label}</span>
      <div className="relative">
        <input
          type="text"
          value={open ? filter : value}
          onChange={(e) => {
            setFilter(e.target.value);
            if (!open) setOpen(true);
            // Also update value directly for free-form input
            onChange(e.target.value);
          }}
          onFocus={() => {
            setOpen(true);
            setFilter(value);
          }}
          onBlur={() => setTimeout(() => setOpen(false), 200)}
          placeholder="column..."
          className="input-dark w-full text-[10px] font-[var(--font-mono)] !py-1.5 !px-2"
        />
        {open && filtered.length > 0 && (
          <div className="absolute z-50 top-full left-0 right-0 mt-1 max-h-32 overflow-y-auto bg-[var(--bg-elevated)] border border-[var(--border-medium)] rounded-lg shadow-xl">
            {filtered.map((c) => (
              <button
                key={c.name}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => {
                  onChange(c.name);
                  setFilter("");
                  setOpen(false);
                }}
                className="w-full text-left px-2 py-1.5 text-[10px] font-[var(--font-mono)] text-[var(--text-secondary)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)] transition-colors flex justify-between"
              >
                <span>{c.name}</span>
                <span className="text-[var(--text-muted)]">{c.data_type}</span>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
