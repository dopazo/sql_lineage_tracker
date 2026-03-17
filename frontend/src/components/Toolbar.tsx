import { useState, useEffect, useCallback } from "react";
import type { LineageGraph, ScanConfig } from "../types/graph";
import { SearchBar } from "./SearchBar";
import type { SearchResult } from "../hooks/useColumnSearch";
import {
  listScans,
  saveScan,
  loadScan,
  deleteScan,
  type SavedScanInfo,
} from "../api/client";

interface ToolbarProps {
  graph: LineageGraph;
  searchQuery: string;
  onSearchQueryChange: (q: string) => void;
  searchResults: SearchResult[];
  onSearchSelect: (result: SearchResult) => void;
  hasActiveTrace: boolean;
  hasColumnTrace: boolean;
  onClearTrace: () => void;
  onOpenTraceDetail?: () => void;
  onRescan: (config: ScanConfig) => void;
  onExport: () => void;
  onResetLayout?: () => void;
  scanning: boolean;
  showFilters?: boolean;
  onToggleFilters?: () => void;
  isFiltered?: boolean;
  filterSummary?: string;
  onGraphReload?: () => void;
}

export function Toolbar({
  graph,
  searchQuery,
  onSearchQueryChange,
  searchResults,
  onSearchSelect,
  hasActiveTrace,
  hasColumnTrace,
  onClearTrace,
  onOpenTraceDetail,
  onRescan,
  onExport,
  onResetLayout,
  scanning,
  showFilters,
  onToggleFilters,
  isFiltered,
  filterSummary,
  onGraphReload,
}: ToolbarProps) {
  const [showRescan, setShowRescan] = useState(false);
  const [target, setTarget] = useState(
    graph.metadata.scan_config.target ?? ""
  );
  const [datasets, setDatasets] = useState(
    graph.metadata.scan_config.datasets.join(", ")
  );
  const [depth, setDepth] = useState(
    graph.metadata.scan_config.depth?.toString() ?? ""
  );

  // Save scan state
  const [showSave, setShowSave] = useState(false);
  const [saveName, setSaveName] = useState("");
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [saveConflict, setSaveConflict] = useState(false);
  const [saving, setSaving] = useState(false);

  // Load scan state
  const [showLoad, setShowLoad] = useState(false);
  const [scans, setScans] = useState<SavedScanInfo[]>([]);
  const [loadingScans, setLoadingScans] = useState(false);
  const [loadingScanName, setLoadingScanName] = useState<string | null>(null);
  const [deletingName, setDeletingName] = useState<string | null>(null);

  const handleRescan = () => {
    const dsArr = datasets
      .split(",")
      .map((d) => d.trim())
      .filter(Boolean);
    onRescan({
      target: target || null,
      datasets: dsArr,
      depth: depth ? parseInt(depth, 10) : null,
    });
    setShowRescan(false);
  };

  const fetchScans = useCallback(async () => {
    setLoadingScans(true);
    try {
      const data = await listScans();
      setScans(data);
    } catch {
      // ignore
    } finally {
      setLoadingScans(false);
    }
  }, []);

  // Fetch scans when Load panel opens
  useEffect(() => {
    if (showLoad) {
      fetchScans();
    }
  }, [showLoad, fetchScans]);

  const handleSave = async (overwrite = false) => {
    const trimmed = saveName.trim();
    if (!trimmed) return;

    // Validate name client-side
    if (!/^[\w-]+$/.test(trimmed)) {
      setSaveError("Solo letras, números, guiones y guiones bajos");
      return;
    }

    setSaving(true);
    setSaveError(null);
    setSaveConflict(false);

    try {
      await saveScan(trimmed, overwrite);
      setSaveSuccess(true);
      setTimeout(() => {
        setShowSave(false);
        setSaveSuccess(false);
        setSaveName("");
      }, 1200);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes("409")) {
        setSaveConflict(true);
      } else {
        setSaveError(msg);
      }
    } finally {
      setSaving(false);
    }
  };

  const handleLoad = async (name: string) => {
    setLoadingScanName(name);
    try {
      await loadScan(name);
      setShowLoad(false);
      onGraphReload?.();
    } catch (err: unknown) {
      console.error("Failed to load scan:", err);
    } finally {
      setLoadingScanName(null);
    }
  };

  const handleDelete = async (name: string) => {
    setDeletingName(name);
    try {
      await deleteScan(name);
      setScans((prev) => prev.filter((s) => s.name !== name));
    } catch (err: unknown) {
      console.error("Failed to delete scan:", err);
    } finally {
      setDeletingName(null);
    }
  };

  const closeSave = () => {
    setShowSave(false);
    setSaveName("");
    setSaveError(null);
    setSaveConflict(false);
    setSaveSuccess(false);
  };

  const stats = graph.metadata.scan_stats;

  return (
    <div className="relative z-50 flex items-center gap-4 px-5 py-2.5 glass border-b border-[var(--border-subtle)]">
      {/* Logo + Title */}
      <div className="flex items-center gap-2.5 shrink-0">
        <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-[var(--accent-teal)] to-[var(--accent-cyan)] flex items-center justify-center">
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
            <path d="M2 4L8 1L14 4L8 7L2 4Z" fill="#0a0e1a" opacity="0.8"/>
            <path d="M2 8L8 5L14 8L8 11L2 8Z" fill="#0a0e1a" opacity="0.6"/>
            <path d="M2 12L8 9L14 12L8 15L2 12Z" fill="#0a0e1a" opacity="0.4"/>
          </svg>
        </div>
        <h1 className="text-sm font-semibold text-[var(--text-primary)] whitespace-nowrap">
          SQL Lineage
        </h1>
      </div>

      {/* Stats */}
      <div className="flex items-center gap-3 text-[11px] text-[var(--text-muted)] font-[var(--font-mono)] shrink-0">
        <span className="px-2 py-1 rounded bg-[var(--bg-deep)] border border-[var(--border-subtle)]">
          {graph.metadata.project_id}
        </span>
        <span>
          <span className="text-[var(--accent-cyan)]">{stats.total_nodes}</span> nodes
        </span>
        <span>
          <span className="text-[var(--accent-teal)]">{stats.total_edges}</span> edges
        </span>
      </div>

      {/* Filters toggle */}
      {onToggleFilters && (
        <button
          onClick={onToggleFilters}
          className={`btn-ghost text-sm relative ${showFilters ? "border-[var(--accent-cyan)] text-[var(--accent-cyan)]" : ""}`}
        >
          <span className="flex items-center gap-1.5">
            <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
              <path d="M1 3h14M3 8h10M5.5 13h5" strokeLinecap="round" />
            </svg>
            Filters
            {isFiltered && !showFilters && filterSummary && (
              <span className="text-[10px] font-[var(--font-mono)] text-[var(--accent-cyan)] font-normal opacity-80">
                {filterSummary}
              </span>
            )}
          </span>
          {isFiltered && (
            <span className="absolute -top-1 -right-1 w-2 h-2 rounded-full bg-[var(--accent-cyan)]" />
          )}
        </button>
      )}

      <div className="flex-1" />

      {/* Search */}
      <SearchBar
        query={searchQuery}
        onQueryChange={onSearchQueryChange}
        results={searchResults}
        onSelect={onSearchSelect}
        hasActiveTrace={hasActiveTrace}
        hasColumnTrace={hasColumnTrace}
        onClear={onClearTrace}
        onOpenTraceDetail={onOpenTraceDetail}
      />

      {/* Save Scan */}
      <div className="relative">
        <button
          onClick={() => { setShowLoad(false); setShowRescan(false); setShowSave(!showSave); }}
          className="btn-ghost text-sm"
          title="Save current scan"
        >
          Save
        </button>

        {showSave && (
          <>
            <div className="fixed inset-0 z-40" onClick={closeSave} />
            <div className="absolute top-full right-0 mt-2 w-72 glass-elevated rounded-xl z-50 p-4 animate-fade-in">
              <div className="space-y-3">
                <div className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider">
                  Save Scan
                </div>
                <div>
                  <input
                    type="text"
                    value={saveName}
                    onChange={(e) => { setSaveName(e.target.value); setSaveError(null); setSaveConflict(false); }}
                    placeholder="Scan name"
                    className="input-dark w-full"
                    autoFocus
                    onKeyDown={(e) => { if (e.key === "Enter") handleSave(); if (e.key === "Escape") closeSave(); }}
                  />
                </div>

                {saveError && (
                  <p className="text-xs text-red-400">{saveError}</p>
                )}

                {saveConflict && (
                  <div className="space-y-2">
                    <p className="text-xs text-amber-400">
                      Ya existe un scan con este nombre.
                    </p>
                    <div className="flex gap-2">
                      <button
                        onClick={() => handleSave(true)}
                        disabled={saving}
                        className="btn-primary text-xs flex-1"
                      >
                        Sobreescribir
                      </button>
                      <button
                        onClick={() => setSaveConflict(false)}
                        className="btn-ghost text-xs flex-1"
                      >
                        Cancelar
                      </button>
                    </div>
                  </div>
                )}

                {saveSuccess && (
                  <p className="text-xs text-emerald-400">Guardado</p>
                )}

                {!saveConflict && !saveSuccess && (
                  <button
                    onClick={() => handleSave()}
                    disabled={saving || !saveName.trim()}
                    className="btn-primary w-full text-sm"
                  >
                    {saving ? "Guardando..." : "Guardar"}
                  </button>
                )}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Load Scan */}
      <div className="relative">
        <button
          onClick={() => { setShowSave(false); setShowRescan(false); setShowLoad(!showLoad); }}
          className="btn-ghost text-sm"
          title="Load a saved scan"
        >
          Load
        </button>

        {showLoad && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => setShowLoad(false)} />
            <div className="absolute top-full right-0 mt-2 w-96 glass-elevated rounded-xl z-50 p-4 animate-fade-in">
              <div className="space-y-3">
                <div className="text-xs font-semibold text-[var(--text-secondary)] uppercase tracking-wider">
                  Load Scan
                </div>

                {loadingScans ? (
                  <div className="flex items-center justify-center py-6">
                    <div className="w-4 h-4 border-2 border-[var(--accent-cyan)] border-t-transparent rounded-full animate-spin" />
                  </div>
                ) : scans.length === 0 ? (
                  <p className="text-xs text-[var(--text-muted)] py-4 text-center">
                    No hay scans guardados
                  </p>
                ) : (
                  <div className="space-y-1.5 max-h-64 overflow-y-auto pr-1">
                    {scans.map((scan) => (
                      <div
                        key={scan.name}
                        className="group flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-[var(--bg-deep)] transition-colors"
                      >
                        <button
                          onClick={() => handleLoad(scan.name)}
                          disabled={loadingScanName !== null}
                          className="flex-1 text-left min-w-0"
                        >
                          <div className="text-sm text-[var(--text-primary)] font-medium truncate">
                            {scan.name}
                          </div>
                          <div className="text-[10px] text-[var(--text-muted)] font-[var(--font-mono)] flex gap-2 mt-0.5">
                            {scan.target && (
                              <span className="truncate max-w-[140px]" title={scan.target}>{scan.target}</span>
                            )}
                            {scan.total_nodes !== undefined && (
                              <span>{scan.total_nodes}n / {scan.total_edges}e</span>
                            )}
                            {scan.generated_at && (
                              <span>{new Date(scan.generated_at).toLocaleDateString()}</span>
                            )}
                          </div>
                        </button>

                        {loadingScanName === scan.name && (
                          <div className="w-3 h-3 border-2 border-[var(--accent-cyan)] border-t-transparent rounded-full animate-spin shrink-0" />
                        )}

                        <button
                          onClick={(e) => { e.stopPropagation(); handleDelete(scan.name); }}
                          disabled={deletingName !== null}
                          className="opacity-0 group-hover:opacity-100 text-[var(--text-muted)] hover:text-red-400 transition-all shrink-0 p-1"
                          title="Eliminar scan"
                        >
                          <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
                            <path d="M4 4l8 8M12 4l-8 8" strokeLinecap="round" />
                          </svg>
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Rescan */}
      <div className="relative">
        <button
          onClick={() => { setShowSave(false); setShowLoad(false); setShowRescan(!showRescan); }}
          disabled={scanning}
          className="btn-primary text-sm py-2 px-4"
        >
          {scanning ? (
            <span className="flex items-center gap-2">
              <span className="w-3 h-3 border-2 border-[#0a0e1a]/40 border-t-[#0a0e1a] rounded-full animate-spin" />
              Scanning
            </span>
          ) : (
            "Re-scan"
          )}
        </button>

        {showRescan && (
          <>
            {/* Backdrop to close */}
            <div
              className="fixed inset-0 z-40"
              onClick={() => setShowRescan(false)}
            />
            <div className="absolute top-full right-0 mt-2 w-80 glass-elevated rounded-xl z-50 p-4 animate-fade-in">
              <div className="space-y-3">
                <div>
                  <label className="label-dark">Target</label>
                  <input
                    type="text"
                    value={target}
                    onChange={(e) => setTarget(e.target.value)}
                    placeholder="dataset.table"
                    className="input-dark w-full"
                  />
                </div>
                <div>
                  <label className="label-dark">Datasets</label>
                  <input
                    type="text"
                    value={datasets}
                    onChange={(e) => setDatasets(e.target.value)}
                    placeholder="staging, raw_data"
                    className="input-dark w-full"
                  />
                </div>
                <div>
                  <label className="label-dark">Depth</label>
                  <input
                    type="number"
                    value={depth}
                    onChange={(e) => setDepth(e.target.value)}
                    placeholder="No limit"
                    className="input-dark w-full"
                  />
                </div>
                <button onClick={handleRescan} className="btn-primary w-full">
                  Start Scan
                </button>
              </div>
            </div>
          </>
        )}
      </div>

      {/* Reset Layout */}
      {onResetLayout && (
        <button onClick={onResetLayout} className="btn-ghost text-sm" title="Reset node positions to auto-layout">
          Reset Layout
        </button>
      )}

      {/* Export */}
      <button onClick={onExport} className="btn-ghost text-sm">
        Export
      </button>
    </div>
  );
}
