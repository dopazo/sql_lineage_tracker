interface ScanProgressBarProps {
  messages: string[];
  scanning: boolean;
  error: string | null;
}

export function ScanProgressBar({
  messages,
  scanning,
  error,
}: ScanProgressBarProps) {
  if (!scanning && messages.length === 0 && !error) return null;

  return (
    <div className="rounded-lg bg-[var(--bg-deep)] border border-[var(--border-subtle)] overflow-hidden">
      {/* Animated scan line */}
      {scanning && (
        <div className="h-0.5 bg-[var(--bg-elevated)] overflow-hidden">
          <div
            className="h-full w-1/3 bg-gradient-to-r from-transparent via-[var(--accent-cyan)] to-transparent"
            style={{ animation: "scan-line 1.5s ease-in-out infinite" }}
          />
        </div>
      )}

      <div className="px-4 py-3">
        {scanning && (
          <div className="flex items-center gap-2 mb-2">
            <div className="w-2 h-2 rounded-full bg-[var(--accent-cyan)] animate-pulse-glow" />
            <span className="text-xs font-medium text-[var(--accent-cyan)] font-[var(--font-mono)] uppercase tracking-wider">
              Scanning
            </span>
          </div>
        )}

        {error && (
          <div className="text-sm text-red-400 mb-2 flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-red-400" />
            {error}
          </div>
        )}

        <div className="max-h-24 overflow-y-auto space-y-0.5">
          {messages.map((msg, i) => (
            <div
              key={i}
              className="text-xs text-[var(--text-muted)] font-[var(--font-mono)]"
              style={{
                opacity: i === messages.length - 1 ? 1 : 0.5,
              }}
            >
              <span className="text-[var(--text-muted)] mr-2 select-none">
                {">"}
              </span>
              {msg}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
