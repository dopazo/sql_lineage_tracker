import { useState, useCallback, useRef } from "react";
import type { ScanConfig, ScanEvent } from "../types/graph";
import { startScan, subscribeScanEvents } from "../api/client";

export function useScanProgress() {
  const [scanning, setScanning] = useState(false);
  const [messages, setMessages] = useState<string[]>([]);
  const [scanError, setScanError] = useState<string | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);
  const doneRef = useRef(false);

  const runScan = useCallback(
    async (config: ScanConfig, onComplete: () => void) => {
      setScanning(true);
      setMessages([]);
      setScanError(null);
      doneRef.current = false;

      try {
        await startScan(config);

        cleanupRef.current = subscribeScanEvents(
          (event: ScanEvent) => {
            setMessages((prev) => [...prev, event.message]);

            if (event.type === "complete") {
              doneRef.current = true;
              setScanning(false);
              const cleanup = cleanupRef.current;
              cleanupRef.current = null;
              cleanup?.();
              onComplete();
            } else if (event.type === "error") {
              doneRef.current = true;
              setScanError(event.message);
              setScanning(false);
              const cleanup = cleanupRef.current;
              cleanupRef.current = null;
              cleanup?.();
            }
          },
          () => {
            // Only treat as error if scan hasn't already completed/errored
            if (!doneRef.current) {
              setScanError("Lost connection to server");
              setScanning(false);
            }
          }
        );
      } catch (err) {
        setScanError(
          err instanceof Error ? err.message : "Failed to start scan"
        );
        setScanning(false);
      }
    },
    []
  );

  const cancelScan = useCallback(() => {
    doneRef.current = true;
    cleanupRef.current?.();
    cleanupRef.current = null;
    setScanning(false);
  }, []);

  return { scanning, messages, scanError, runScan, cancelScan };
}
