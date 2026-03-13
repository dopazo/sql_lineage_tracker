import { useEffect, useRef } from "react";

interface ScanProgressBarProps {
  messages: string[];
  scanning: boolean;
  error: string | null;
  completed?: boolean;
  onDismiss?: () => void;
}

export function ScanProgressBar({
  messages,
  scanning,
  error,
  completed,
  onDismiss,
}: ScanProgressBarProps) {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
    }
  }, [messages]);

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

      {/* Completed accent line */}
      {completed && !scanning && (
        <div className="h-0.5 bg-[var(--accent-cyan)]/30" />
      )}

      <div className="px-4 py-3">
        <div className="flex items-center justify-between mb-2">
          {scanning && (
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-[var(--accent-cyan)] animate-pulse-glow" />
              <span className="text-xs font-medium text-[var(--accent-cyan)] font-[var(--font-mono)] uppercase tracking-wider">
                Scanning
              </span>
            </div>
          )}

          {completed && !scanning && (
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-emerald-400" />
              <span className="text-xs font-medium text-emerald-400 font-[var(--font-mono)] uppercase tracking-wider">
                Complete
              </span>
            </div>
          )}

          {error && !scanning && !completed && (
            <div className="text-sm text-red-400 flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-red-400" />
              {error}
            </div>
          )}

          {!scanning && (messages.length > 0 || error) && onDismiss && (
            <button
              onClick={onDismiss}
              className="text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors p-0.5 -mr-1"
              title="Close"
            >
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
                <path d="M3 3l8 8M11 3l-8 8" />
              </svg>
            </button>
          )}
        </div>

        {error && (completed || scanning) && (
          <div className="text-sm text-red-400 mb-2 flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-red-400" />
            {error}
          </div>
        )}

        <div ref={scrollRef} className="max-h-48 overflow-y-auto space-y-0.5">
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
