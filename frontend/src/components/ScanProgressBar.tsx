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
    <div className="px-4 py-2 bg-slate-50 border-b border-slate-200">
      {scanning && (
        <div className="flex items-center gap-2 mb-1">
          <div className="w-3 h-3 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
          <span className="text-sm text-slate-600">Scanning...</span>
        </div>
      )}

      {error && (
        <div className="text-sm text-red-600 mb-1">Error: {error}</div>
      )}

      <div className="max-h-24 overflow-y-auto text-xs text-slate-500 space-y-0.5">
        {messages.map((msg, i) => (
          <div key={i}>{msg}</div>
        ))}
      </div>
    </div>
  );
}
