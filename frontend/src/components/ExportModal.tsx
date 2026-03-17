import { useState } from "react";
import type { LineageGraph } from "../types/graph";
import { exportGraphJSON, exportGraphMermaid } from "../api/client";

interface ExportModalProps {
  /** Full unfiltered graph (for JSON export) */
  fullGraph: LineageGraph;
  /** Filtered/visible graph (for Mermaid export) */
  filteredGraph: LineageGraph;
  onClose: () => void;
}

type Format = "json" | "mermaid";
type Step = "format" | "detail";

export function ExportModal({
  fullGraph,
  filteredGraph,
  onClose,
}: ExportModalProps) {
  const [step, setStep] = useState<Step>("format");

  const handleFormat = (format: Format) => {
    if (format === "json") {
      exportGraphJSON(fullGraph);
      onClose();
    } else {
      setStep("detail");
    }
  };

  const handleDetail = (detailed: boolean) => {
    exportGraphMermaid(filteredGraph, detailed);
    onClose();
  };

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />

      <div className="relative glass-elevated rounded-2xl p-6 w-96 animate-fade-in">
        {/* Header */}
        <div className="flex items-center justify-between mb-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)]">
            {step === "format" ? "Exportar grafo" : "Nivel de detalle"}
          </h2>
          <button
            onClick={onClose}
            className="text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
          >
            <svg
              width="16"
              height="16"
              viewBox="0 0 16 16"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
            >
              <path d="M4 4l8 8M12 4l-8 8" strokeLinecap="round" />
            </svg>
          </button>
        </div>

        {step === "format" && (
          <div className="space-y-3">
            <button
              onClick={() => handleFormat("json")}
              className="w-full text-left px-4 py-3 rounded-xl border border-[var(--border-subtle)] hover:border-[var(--accent-cyan)] hover:bg-[var(--bg-deep)] transition-all group"
            >
              <div className="text-sm font-medium text-[var(--text-primary)] group-hover:text-[var(--accent-cyan)]">
                JSON
              </div>
              <div className="text-[11px] text-[var(--text-muted)] mt-0.5">
                Compatible con SQL Lineage Tracker
              </div>
            </button>

            <button
              onClick={() => handleFormat("mermaid")}
              className="w-full text-left px-4 py-3 rounded-xl border border-[var(--border-subtle)] hover:border-[var(--accent-teal)] hover:bg-[var(--bg-deep)] transition-all group"
            >
              <div className="text-sm font-medium text-[var(--text-primary)] group-hover:text-[var(--accent-teal)]">
                Mermaid
              </div>
              <div className="text-[11px] text-[var(--text-muted)] mt-0.5">
                Compatible con GitHub, Notion, Confluence y otras aplicaciones
              </div>
            </button>
          </div>
        )}

        {step === "detail" && (
          <div className="space-y-3">
            <button
              onClick={() => setStep("format")}
              className="flex items-center gap-1 text-[11px] text-[var(--text-muted)] hover:text-[var(--text-secondary)] transition-colors mb-1"
            >
              <svg
                width="12"
                height="12"
                viewBox="0 0 16 16"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
              >
                <path d="M10 3L5 8l5 5" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              Volver
            </button>

            <button
              onClick={() => handleDetail(false)}
              className="w-full text-left px-4 py-3 rounded-xl border border-[var(--border-subtle)] hover:border-[var(--accent-cyan)] hover:bg-[var(--bg-deep)] transition-all group"
            >
              <div className="text-sm font-medium text-[var(--text-primary)] group-hover:text-[var(--accent-cyan)]">
                Simple
              </div>
              <div className="text-[11px] text-[var(--text-muted)] mt-0.5">
                Solo nombres de tablas/vistas y sus conexiones
              </div>
            </button>

            <button
              onClick={() => handleDetail(true)}
              className="w-full text-left px-4 py-3 rounded-xl border border-[var(--border-subtle)] hover:border-[var(--accent-teal)] hover:bg-[var(--bg-deep)] transition-all group"
            >
              <div className="text-sm font-medium text-[var(--text-primary)] group-hover:text-[var(--accent-teal)]">
                Detallado
              </div>
              <div className="text-[11px] text-[var(--text-muted)] mt-0.5">
                Incluye nombres de columnas en cada nodo
              </div>
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
