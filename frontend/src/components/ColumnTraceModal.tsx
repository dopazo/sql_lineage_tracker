import { useMemo } from "react";
import { createPortal } from "react-dom";
import type { LineageGraph } from "../types/graph";
import type { ColumnTraceEntry } from "../utils/lineageTraversal";
import type { TraceOrigin } from "../hooks/useColumnSearch";
import {
  buildOrderedChain,
  buildUpstreamTree,
  countTree,
  countLeaves,
  type ChainStep,
  type TraceTreeNode,
} from "../utils/traceChain";
import { TRANSFORM_STYLES } from "../constants/transforms";
import { SqlHighlight } from "./SqlHighlight";

interface ColumnTraceModalProps {
  graph: LineageGraph;
  activeTrace: ColumnTraceEntry[];
  traceOrigin: TraceOrigin;
  onClose: () => void;
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

/* ── Recursive tree renderer ───────────────────────────────── */

function SourceTree({ node }: { node: TraceTreeNode }) {
  const { sources } = node;

  if (sources.length === 0) return null;

  if (sources.length === 1) {
    const src = sources[0];
    return (
      <>
        {src.transformation && (
          <StepConnector
            transformation={src.transformation}
            expression={src.expression}
          />
        )}
        <StepCard step={src.node.step} />
        <SourceTree node={src.node} />
      </>
    );
  }

  // Multiple sources — check if all share the same transformation+expression
  const sharedTransform = sources.every(
    (s) =>
      s.transformation === sources[0].transformation &&
      s.expression === sources[0].expression
  );

  return (
    <>
      {/* Show shared connector once above the split */}
      {sharedTransform && sources[0].transformation && (
        <StepConnector
          transformation={sources[0].transformation}
          expression={sources[0].expression}
        />
      )}
      <div className="flex gap-3 mt-2">
        {sources.map((src, i) => (
          <div
            key={`${src.node.step.nodeId}-${src.node.step.columnName}-${i}`}
            className="flex-1 min-w-0 border-l-2 border-[var(--border-medium)] pl-3"
          >
            {/* Only show per-branch connector if not already shown above */}
            {!sharedTransform && src.transformation && (
              <StepConnector
                transformation={src.transformation}
                expression={src.expression}
              />
            )}
            <StepCard step={src.node.step} />
            <SourceTree node={src.node} />
          </div>
        ))}
      </div>
    </>
  );
}

/* ── Main Modal ────────────────────────────────────────────── */

export function ColumnTraceModal({
  graph,
  activeTrace,
  traceOrigin,
  onClose,
}: ColumnTraceModalProps) {
  // Flat chain for downstream rendering (kept as-is)
  const { origin, downstream } = useMemo(
    () => buildOrderedChain(graph, traceOrigin, activeTrace),
    [graph, traceOrigin, activeTrace]
  );

  // Tree for upstream (supports branching)
  const upstreamTree = useMemo(
    () => buildUpstreamTree(graph, traceOrigin, activeTrace),
    [graph, traceOrigin, activeTrace]
  );

  const hasSources = upstreamTree.sources.length > 0;
  const hasBranching = upstreamTree.sources.length > 1;
  const treeStats = useMemo(() => countTree(upstreamTree), [upstreamTree]);
  const totalSteps = treeStats.steps + downstream.length;
  const sourceCount = countLeaves(upstreamTree);

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
          className={`glass-elevated rounded-xl w-full max-h-[85vh] flex flex-col animate-fade-in pointer-events-auto ${
            hasBranching ? "max-w-4xl" : "max-w-2xl"
          }`}
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
                <span className="text-[var(--accent-cyan)]">{treeStats.nodes}</span>{" "}
                nodes
              </span>
              <span>
                <span className="text-[var(--accent-cyan)]">
                  {totalSteps}
                </span>{" "}
                steps
              </span>
              {hasSources && (
                <span>
                  <span className="text-[var(--accent-teal)]">
                    {sourceCount}
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

          {/* Timeline */}
          <div className="flex-1 overflow-auto px-5 py-4">
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

              {[...downstream].reverse().map((step, i) => (
                <div key={`d-${step.nodeId}-${step.columnName}-${i}`}>
                  <StepCard step={step} />
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

              {/* ── Sources (upstream tree) ─────────────────── */}
              {hasSources && (
                <div className="text-[10px] font-semibold uppercase tracking-widest text-[var(--text-muted)] font-[var(--font-mono)] my-3 flex items-center gap-2">
                  <svg width="10" height="10" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                    <path d="M8 2v12M4 10l4 4 4-4" />
                  </svg>
                  Sources
                </div>
              )}

              <SourceTree node={upstreamTree} />
            </div>

            {/* Empty trace */}
            {!hasSources && downstream.length === 0 && (
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
