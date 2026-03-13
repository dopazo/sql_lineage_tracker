import { useState, useEffect } from "react";
import type { DatasetInfo, ScanConfig } from "../types/graph";
import { getDatasets } from "../api/client";
import { ScanProgressBar } from "./ScanProgressBar";

interface ScanSetupScreenProps {
  onStartScan: (config: ScanConfig) => void;
  scanning: boolean;
  scanMessages: string[];
  scanError: string | null;
  scanCompleted?: boolean;
  onDismissScan?: () => void;
}

export function ScanSetupScreen({
  onStartScan,
  scanning,
  scanMessages,
  scanError,
  scanCompleted,
  onDismissScan,
}: ScanSetupScreenProps) {
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [selectedDatasets, setSelectedDatasets] = useState<Set<string>>(
    new Set()
  );
  const [target, setTarget] = useState("");
  const [depth, setDepth] = useState("");

  useEffect(() => {
    getDatasets()
      .then((ds) => {
        setDatasets(ds);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  const toggleDataset = (id: string) => {
    setSelectedDatasets((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleScan = () => {
    onStartScan({
      target: target || null,
      datasets: Array.from(selectedDatasets),
      depth: depth ? parseInt(depth, 10) : null,
    });
  };

  return (
    <div className="flex items-center justify-center min-h-screen bg-[var(--bg-deep)] bg-grid">
      <div className="w-full max-w-lg glass-elevated rounded-2xl p-8 animate-fade-in">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-[var(--accent-teal)] to-[var(--accent-cyan)] flex items-center justify-center">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path d="M2 4L8 1L14 4L8 7L2 4Z" fill="#0a0e1a" opacity="0.8"/>
                <path d="M2 8L8 5L14 8L8 11L2 8Z" fill="#0a0e1a" opacity="0.6"/>
                <path d="M2 12L8 9L14 12L8 15L2 12Z" fill="#0a0e1a" opacity="0.4"/>
              </svg>
            </div>
            <h1 className="text-xl font-semibold text-[var(--text-primary)]">
              SQL Lineage Tracker
            </h1>
          </div>
          <p className="text-sm text-[var(--text-muted)]">
            Configure your scan to trace column-level lineage across datasets.
          </p>
        </div>

        {/* Target */}
        <div className="mb-5">
          <label className="label-dark">Target table / view</label>
          <input
            type="text"
            value={target}
            onChange={(e) => setTarget(e.target.value)}
            placeholder="dataset.table_name"
            className="input-dark w-full"
          />
          <p className="text-xs text-[var(--text-muted)] mt-1.5">
            Trace lineage backward from this target. Leave empty to scan all
            selected datasets.
          </p>
        </div>

        {/* Datasets */}
        <div className="mb-5">
          <label className="label-dark">Datasets</label>
          {loading ? (
            <div className="text-sm text-[var(--text-muted)] py-6 text-center">
              <div className="w-4 h-4 border-2 border-[var(--accent-cyan)] border-t-transparent rounded-full animate-spin mx-auto mb-2" />
              Loading datasets...
            </div>
          ) : error ? (
            <div className="text-sm text-red-400 py-2 px-3 bg-red-500/10 rounded-lg border border-red-500/20">
              {error}
            </div>
          ) : datasets.length === 0 ? (
            <div className="text-sm text-[var(--text-muted)] py-4 text-center bg-[var(--bg-deep)] rounded-lg border border-[var(--border-subtle)]">
              No datasets found.
            </div>
          ) : (
            <div className="bg-[var(--bg-deep)] border border-[var(--border-subtle)] rounded-lg max-h-52 overflow-y-auto">
              {datasets.map((ds) => (
                <label
                  key={ds.id}
                  className="flex items-center gap-3 px-4 py-2.5 hover:bg-[var(--bg-hover)] cursor-pointer transition-colors border-b border-[var(--border-subtle)] last:border-b-0"
                >
                  <input
                    type="checkbox"
                    checked={selectedDatasets.has(ds.id)}
                    onChange={() => toggleDataset(ds.id)}
                    className="rounded accent-[var(--accent-cyan)]"
                  />
                  <span className="text-sm text-[var(--text-primary)] flex-1 font-[var(--font-mono)]">
                    {ds.id}
                  </span>
                  <span className="text-xs text-[var(--text-muted)] font-[var(--font-mono)]">
                    {ds.table_count}T / {ds.view_count}V
                  </span>
                </label>
              ))}
            </div>
          )}
        </div>

        {/* Depth */}
        <div className="mb-6">
          <label className="label-dark">Max depth (dataset hops)</label>
          <input
            type="number"
            value={depth}
            onChange={(e) => setDepth(e.target.value)}
            placeholder="No limit"
            min={1}
            className="input-dark w-32"
          />
        </div>

        {/* Scan progress */}
        <ScanProgressBar
          messages={scanMessages}
          scanning={scanning}
          error={scanError}
          completed={scanCompleted}
          onDismiss={onDismissScan}
        />

        {/* Actions */}
        <button
          onClick={handleScan}
          disabled={scanning}
          className="btn-primary w-full mt-4"
        >
          {scanning ? (
            <span className="flex items-center justify-center gap-2">
              <span className="w-4 h-4 border-2 border-[#0a0e1a]/40 border-t-[#0a0e1a] rounded-full animate-spin" />
              Scanning...
            </span>
          ) : (
            "Start Scan"
          )}
        </button>
      </div>
    </div>
  );
}
