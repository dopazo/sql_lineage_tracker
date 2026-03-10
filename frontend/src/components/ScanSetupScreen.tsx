import { useState, useEffect } from "react";
import type { DatasetInfo, ScanConfig } from "../types/graph";
import { getDatasets } from "../api/client";
import { ScanProgressBar } from "./ScanProgressBar";

interface ScanSetupScreenProps {
  onStartScan: (config: ScanConfig) => void;
  scanning: boolean;
  scanMessages: string[];
  scanError: string | null;
}

export function ScanSetupScreen({
  onStartScan,
  scanning,
  scanMessages,
  scanError,
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
    <div className="flex items-center justify-center min-h-screen bg-slate-50">
      <div className="w-full max-w-lg bg-white rounded-lg shadow-md p-6">
        <h1 className="text-xl font-semibold text-slate-800 mb-1">
          SQL Lineage Tracker
        </h1>
        <p className="text-sm text-slate-500 mb-6">
          Configure your scan to trace column-level lineage.
        </p>

        {/* Target */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-slate-700 mb-1">
            Target table/view (optional)
          </label>
          <input
            type="text"
            value={target}
            onChange={(e) => setTarget(e.target.value)}
            placeholder="dataset.table_name"
            className="w-full px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
          <p className="text-xs text-slate-400 mt-1">
            Trace lineage backward from this target. Leave empty to scan all
            selected datasets.
          </p>
        </div>

        {/* Datasets */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-slate-700 mb-1">
            Datasets
          </label>
          {loading ? (
            <div className="text-sm text-slate-400 py-4 text-center">
              Loading datasets...
            </div>
          ) : error ? (
            <div className="text-sm text-red-500 py-2">{error}</div>
          ) : datasets.length === 0 ? (
            <div className="text-sm text-slate-400 py-2">
              No datasets found.
            </div>
          ) : (
            <div className="border border-slate-200 rounded-md max-h-48 overflow-y-auto">
              {datasets.map((ds) => (
                <label
                  key={ds.id}
                  className="flex items-center gap-3 px-3 py-2 hover:bg-slate-50 cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={selectedDatasets.has(ds.id)}
                    onChange={() => toggleDataset(ds.id)}
                    className="rounded"
                  />
                  <span className="text-sm text-slate-700 flex-1">
                    {ds.id}
                  </span>
                  <span className="text-xs text-slate-400">
                    {ds.table_count}T / {ds.view_count}V
                  </span>
                </label>
              ))}
            </div>
          )}
        </div>

        {/* Depth */}
        <div className="mb-6">
          <label className="block text-sm font-medium text-slate-700 mb-1">
            Max depth (dataset hops)
          </label>
          <input
            type="number"
            value={depth}
            onChange={(e) => setDepth(e.target.value)}
            placeholder="No limit"
            min={1}
            className="w-32 px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
          />
        </div>

        {/* Scan progress */}
        <ScanProgressBar
          messages={scanMessages}
          scanning={scanning}
          error={scanError}
        />

        {/* Actions */}
        <button
          onClick={handleScan}
          disabled={scanning}
          className="w-full px-4 py-2 bg-blue-500 hover:bg-blue-600 disabled:bg-blue-300 text-white rounded-md text-sm font-medium mt-4"
        >
          {scanning ? "Scanning..." : "Start Scan"}
        </button>
      </div>
    </div>
  );
}
