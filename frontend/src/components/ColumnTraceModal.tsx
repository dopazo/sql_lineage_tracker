import { useMemo } from "react";
import { createPortal } from "react-dom";
import type {
  LineageGraph,
  LineageEdge,
  LineageNode,
} from "../types/graph";
import type { ColumnTraceEntry } from "../utils/lineageTraversal";
import type { TraceOrigin } from "../hooks/useColumnSearch";
import { TRANSFORM_STYLES } from "../constants/transforms";
import { SqlHighlight } from "./SqlHighlight";

/* ── Types ─────────────────────────────────────────────────── */

interface ChainStep {
  nodeId: string;
  nodeName: string;
  nodeType: string;
  columnName: string;
  /** Display-case column name (original case from node.columns) */
  columnDisplay: string;
  dataType: string;
  /**
   * Transformation that connects THIS step to the NEXT step in display order.
   * For upstream steps: how this column feeds into the next step toward origin.
   * For downstream steps: how the previous step feeds into this column.
   */
  transformation?: string;
  expression?: string | null;
  isOrigin: boolean;
}

interface ColumnTraceModalProps {
  graph: LineageGraph;
  activeTrace: ColumnTraceEntry[];
  traceOrigin: TraceOrigin;
  onClose: () => void;
}

/* ── Chain builder ─────────────────────────────────────────── */

function buildOrderedChain(
  graph: LineageGraph,
  traceOrigin: TraceOrigin,
  activeTrace: ColumnTraceEntry[]
): { upstream: ChainStep[]; origin: ChainStep; downstream: ChainStep[] } {
  // Build lookup: which columns are traced per node
  const traceSet = new Set(
    activeTrace.map((t) => `${t.nodeId}:${t.columnName.toLowerCase()}`)
  );

  // Build edge adjacency
  const byTarget = new Map<string, LineageEdge[]>();
  const bySource = new Map<string, LineageEdge[]>();
  for (const edge of graph.edges) {
    let t = byTarget.get(edge.target_node);
    if (!t) { t = []; byTarget.set(edge.target_node, t); }
    t.push(edge);
    let s = bySource.get(edge.source_node);
    if (!s) { s = []; bySource.set(edge.source_node, s); }
    s.push(edge);
  }

  function resolveColumn(nodeId: string, colNameLower: string) {
    const node = graph.nodes[nodeId];
    if (!node) return { display: colNameLower, dataType: "" };
    // Handle wildcard "*" from COUNT(*) etc.
    if (colNameLower === "*") {
      return { display: "*", dataType: "(all rows)" };
    }
    const col = node.columns.find(
      (c) => c.name.toLowerCase() === colNameLower
    );
    return {
      display: col?.name ?? colNameLower,
      dataType: col?.data_type ?? "",
    };
  }

  function nodeInfo(nodeId: string): { name: string; type: string } {
    const node = graph.nodes[nodeId] as LineageNode | undefined;
    if (!node) return { name: nodeId, type: "?" };
    return { name: `${node.dataset}.${node.name}`, type: node.type };
  }

  // Walk upstream from origin (BFS)
  // Each step stores the transformation of the edge that was traversed to find it.
  // Since we walk backward (target→source), step.transformation describes
  // how this step's column is transformed INTO the column of the step that found it.
  // i.e., step.transformation = edge(this_step → finder_step).transformation
  const upstream: ChainStep[] = [];
  {
    const visited = new Set<string>([
      `${traceOrigin.nodeId}:${traceOrigin.columnName.toLowerCase()}`,
    ]);
    const queue: Array<{
      nodeId: string;
      columnName: string;
    }> = [{ nodeId: traceOrigin.nodeId, columnName: traceOrigin.columnName.toLowerCase() }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const edges = byTarget.get(current.nodeId) ?? [];

      for (const edge of edges) {
        for (const mapping of edge.column_mappings) {
          if (mapping.target_column.toLowerCase() !== current.columnName)
            continue;

          for (const srcCol of mapping.source_columns) {
            const srcLower = srcCol.toLowerCase();
            const key = `${edge.source_node}:${srcLower}`;
            if (visited.has(key) || !traceSet.has(key)) continue;

            visited.add(key);
            const info = nodeInfo(edge.source_node);
            const col = resolveColumn(edge.source_node, srcLower);

            upstream.push({
              nodeId: edge.source_node,
              nodeName: info.name,
              nodeType: info.type,
              columnName: srcLower,
              columnDisplay: col.display,
              dataType: col.dataType,
              // This transformation describes: this_step → finder (toward origin)
              transformation: mapping.transformation,
              expression: mapping.expression,
              isOrigin: false,
            });

            queue.push({ nodeId: edge.source_node, columnName: srcLower });
          }
        }
      }
    }
  }

  // Walk downstream from origin (BFS)
  // Each step stores the transformation of the edge from the previous step to this step.
  const downstream: ChainStep[] = [];
  {
    const visited = new Set<string>([
      `${traceOrigin.nodeId}:${traceOrigin.columnName.toLowerCase()}`,
    ]);
    const queue: Array<{
      nodeId: string;
      columnName: string;
    }> = [{ nodeId: traceOrigin.nodeId, columnName: traceOrigin.columnName.toLowerCase() }];

    while (queue.length > 0) {
      const current = queue.shift()!;
      const edges = bySource.get(current.nodeId) ?? [];

      for (const edge of edges) {
        for (const mapping of edge.column_mappings) {
          if (
            !mapping.source_columns.some(
              (sc) => sc.toLowerCase() === current.columnName
            )
          )
            continue;

          const tgtLower = mapping.target_column.toLowerCase();
          const key = `${edge.target_node}:${tgtLower}`;
          if (visited.has(key) || !traceSet.has(key)) continue;

          visited.add(key);
          const info = nodeInfo(edge.target_node);
          const col = resolveColumn(edge.target_node, tgtLower);

          downstream.push({
            nodeId: edge.target_node,
            nodeName: info.name,
            nodeType: info.type,
            columnName: tgtLower,
            columnDisplay: col.display,
            dataType: col.dataType,
            transformation: mapping.transformation,
            expression: mapping.expression,
            isOrigin: false,
          });

          queue.push({ nodeId: edge.target_node, columnName: tgtLower });
        }
      }
    }
  }

  // Origin step
  const originInfo = nodeInfo(traceOrigin.nodeId);
  const originCol = resolveColumn(
    traceOrigin.nodeId,
    traceOrigin.columnName.toLowerCase()
  );
  const origin: ChainStep = {
    nodeId: traceOrigin.nodeId,
    nodeName: originInfo.name,
    nodeType: originInfo.type,
    columnName: traceOrigin.columnName.toLowerCase(),
    columnDisplay: originCol.display,
    dataType: originCol.dataType,
    isOrigin: true,
  };

  // After reverse, upstream is ordered: [furthest_source, ..., closest_to_origin]
  // Each step's transformation describes: this_step → next_step (toward origin)
  // So the connector AFTER step[i] should use step[i].transformation.
  return { upstream: upstream.reverse(), origin, downstream };
}

/* ── Sub-components ────────────────────────────────────────── */

function TransformationBadge({ type }: { type: string }) {
  const style =
    TRANSFORM_STYLES[type] ?? TRANSFORM_STYLES.unknown;
  return (
    <span
      className={`inline-block px-2 py-0.5 rounded text-[10px] font-semibold font-[var(--font-mono)] uppercase tracking-wider ${style}`}
    >
      {type}
    </span>
  );
}

function StepConnector({ transformation, expression }: { transformation: string; expression?: string | null }) {
  return (
    <div className="flex items-center gap-3 pl-6 py-1">
      <div className="w-px h-full min-h-[24px] bg-[var(--border-medium)]" />
      <div className="flex items-center gap-2">
        <svg
          className="text-[var(--text-muted)] shrink-0"
          width="10"
          height="10"
          viewBox="0 0 16 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
        >
          <path d="M8 2v12M4 10l4 4 4-4" />
        </svg>
        <TransformationBadge type={transformation} />
        {expression && (
          <div className="max-w-xs">
            <SqlHighlight code={expression} />
          </div>
        )}
      </div>
    </div>
  );
}

function StepCard({ step }: { step: ChainStep }) {
  const isWildcard = step.columnName === "*";

  return (
    <div
      className={`flex items-start gap-3 px-4 py-3 rounded-lg border transition-colors ${
        step.isOrigin
          ? "bg-cyan-500/10 border-[var(--accent-cyan)]/40"
          : "bg-[var(--bg-deep)] border-[var(--border-subtle)] hover:border-[var(--border-medium)]"
      }`}
    >
      {/* Node type indicator */}
      <div
        className={`mt-0.5 w-2 h-2 rounded-full shrink-0 ${
          step.isOrigin ? "bg-[var(--accent-cyan)]" : "bg-[var(--accent-purple)]"
        }`}
      />

      <div className="flex-1 min-w-0">
        {isWildcard ? (
          /* Wildcard source: show as aggregation source (table rows), not a column */
          <>
            <div className="flex items-center gap-2">
              <span className="font-medium text-sm text-[var(--text-primary)]">
                {step.nodeName}
              </span>
              <span className="text-[10px] text-[var(--text-muted)] uppercase font-[var(--font-mono)] shrink-0">
                {step.nodeType}
              </span>
            </div>
            <div className="text-xs text-[var(--text-muted)] mt-0.5 italic">
              row-level aggregation source
            </div>
          </>
        ) : (
          /* Normal column step */
          <>
            <div className="flex items-center gap-2">
              <span className="font-medium font-[var(--font-mono)] text-sm text-[var(--text-primary)]">
                {step.columnDisplay}
              </span>
              <span className="text-[10px] font-[var(--font-mono)] text-[var(--text-muted)] uppercase">
                {step.dataType}
              </span>
              {step.isOrigin && (
                <span className="text-[10px] px-1.5 py-0.5 rounded bg-[var(--accent-cyan)]/20 text-[var(--accent-cyan)] font-semibold font-[var(--font-mono)]">
                  ORIGIN
                </span>
              )}
            </div>
            <div className="flex items-center gap-2 mt-1">
              <span className="text-xs text-[var(--text-muted)] truncate" title={step.nodeName}>
                {step.nodeName}
              </span>
              <span className="text-[10px] text-[var(--text-muted)] uppercase font-[var(--font-mono)] shrink-0">
                {step.nodeType}
              </span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

/* ── Main Modal ────────────────────────────────────────────── */

export function ColumnTraceModal({
  graph,
  activeTrace,
  traceOrigin,
  onClose,
}: ColumnTraceModalProps) {
  const { upstream, origin, downstream } = useMemo(
    () => buildOrderedChain(graph, traceOrigin, activeTrace),
    [graph, traceOrigin, activeTrace]
  );

  const allSteps = [...upstream, origin, ...downstream];
  const totalNodes = new Set(allSteps.map((s) => s.nodeId)).size;

  return createPortal(
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-50 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="fixed inset-0 z-50 flex items-center justify-center p-8 pointer-events-none">
        <div
          className="glass-elevated rounded-xl w-full max-w-2xl max-h-[85vh] flex flex-col animate-fade-in pointer-events-auto"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between px-5 py-4 border-b border-[var(--border-subtle)]">
            <div className="min-w-0 flex-1">
              <h2 className="text-sm font-semibold text-[var(--text-primary)]">
                Column Trace
              </h2>
              <div className="text-xs text-[var(--text-muted)] mt-0.5 flex items-center gap-3">
                <span className="font-[var(--font-mono)]">
                  {origin.columnDisplay}
                </span>
                <span>
                  from{" "}
                  <span className="font-[var(--font-mono)]">
                    {origin.nodeName}
                  </span>
                </span>
              </div>
            </div>

            {/* Stats */}
            <div className="flex items-center gap-3 mr-4 text-[10px] font-[var(--font-mono)] text-[var(--text-muted)]">
              <span>
                <span className="text-[var(--accent-cyan)]">{totalNodes}</span>{" "}
                nodes
              </span>
              <span>
                <span className="text-[var(--accent-cyan)]">
                  {allSteps.length}
                </span>{" "}
                steps
              </span>
              {upstream.length > 0 && (
                <span>
                  <span className="text-[var(--accent-teal)]">
                    {upstream.length}
                  </span>{" "}
                  sources
                </span>
              )}
              {downstream.length > 0 && (
                <span>
                  <span className="text-[var(--accent-purple)]">
                    {downstream.length}
                  </span>{" "}
                  dependents
                </span>
              )}
            </div>

            <button
              onClick={onClose}
              className="w-6 h-6 rounded-md flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--bg-hover)] transition-colors"
            >
              &times;
            </button>
          </div>

          {/* Timeline: destination at top, sources at bottom */}
          <div className="flex-1 overflow-y-auto px-5 py-4">
            <div className="space-y-0">
              {/* ── Used by (downstream) section ─────────────── */}
              {downstream.length > 0 && (
                <div className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)] font-[var(--font-mono)] mb-3 flex items-center gap-2">
                  <svg width="10" height="10" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                    <path d="M8 14V2M4 6l4-4 4 4" />
                  </svg>
                  Used by
                </div>
              )}

              {/* Downstream in reverse: furthest consumer first, closest to origin last */}
              {[...downstream].reverse().map((step, i) => (
                <div key={`d-${step.nodeId}-${step.columnName}-${i}`}>
                  <StepCard step={step} />
                  {/* Connector after: this step's transformation describes
                      how the previous step (closer to origin) feeds into it,
                      so it connects downward toward origin */}
                  {step.transformation && (
                    <StepConnector
                      transformation={step.transformation}
                      expression={step.expression}
                    />
                  )}
                </div>
              ))}

              {/* ── Origin ─────────────────────────────────── */}
              <StepCard step={origin} />

              {/* ── Sources (upstream) section ────────────────── */}
              {upstream.length > 0 && (
                <div className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)] font-[var(--font-mono)] my-3 flex items-center gap-2">
                  <svg width="10" height="10" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                    <path d="M8 2v12M4 10l4 4 4-4" />
                  </svg>
                  Sources
                </div>
              )}

              {/* Upstream reversed back: closest to origin first, furthest source last */}
              {[...upstream].reverse().map((step, i) => (
                <div key={`u-${step.nodeId}-${step.columnName}-${i}`}>
                  {/* Connector before: this step's transformation describes
                      how it feeds into the next step toward origin (above) */}
                  {step.transformation && (
                    <StepConnector
                      transformation={step.transformation}
                      expression={step.expression}
                    />
                  )}
                  <StepCard step={step} />
                </div>
              ))}
            </div>

            {/* Empty trace */}
            {upstream.length === 0 && downstream.length === 0 && (
              <div className="text-center py-8 text-sm text-[var(--text-muted)]">
                No sources or dependents found for this column.
              </div>
            )}
          </div>
        </div>
      </div>
    </>,
    document.body
  );
}
