import { useState, useCallback, useRef } from "react";
import type { ScanConfig, ScanEvent } from "../types/graph";
import { startScan, subscribeScanEvents } from "../api/client";

export function useScanProgress() {
  const [scanning, setScanning] = useState(false);
  const [messages, setMessages] = useState<string[]>([]);
  const [scanError, setScanError] = useState<string | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);

  const runScan = useCallback(
    async (config: ScanConfig, onComplete: () => void) => {
      setScanning(true);
      setMessages([]);
      setScanError(null);

      try {
        await startScan(config);

        cleanupRef.current = subscribeScanEvents(
          (event: ScanEvent) => {
            setMessages((prev) => [...prev, event.message]);

            if (event.type === "complete") {
              setScanning(false);
              cleanupRef.current?.();
              cleanupRef.current = null;
              onComplete();
            } else if (event.type === "error") {
              setScanError(event.message);
              setScanning(false);
              cleanupRef.current?.();
              cleanupRef.current = null;
            }
          },
          () => {
            setScanError("Lost connection to server");
            setScanning(false);
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
    cleanupRef.current?.();
    cleanupRef.current = null;
    setScanning(false);
  }, []);

  return { scanning, messages, scanError, runScan, cancelScan };
}
