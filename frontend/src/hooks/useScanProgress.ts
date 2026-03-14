import { useState, useCallback, useRef, useEffect } from "react";
import type { ScanConfig, ScanEvent } from "../types/graph";
import { startScan, expandNode, subscribeScanEvents } from "../api/client";

export function useScanProgress() {
  const [scanning, setScanning] = useState(false);
  const [messages, setMessages] = useState<string[]>([]);
  const [scanError, setScanError] = useState<string | null>(null);
  const [completed, setCompleted] = useState(false);
  const cleanupRef = useRef<(() => void) | null>(null);
  const onCompleteRef = useRef<(() => void) | null>(null);

  // Close EventSource on unmount
  useEffect(() => {
    return () => {
      cleanupRef.current?.();
      cleanupRef.current = null;
    };
  }, []);

  const teardown = () => {
    setScanning(false);
    const cleanup = cleanupRef.current;
    cleanupRef.current = null;
    cleanup?.();
  };

  const dismissMessages = useCallback(() => {
    setMessages([]);
    setCompleted(false);
    setScanError(null);
  }, []);

  const runScan = useCallback(
    async (config: ScanConfig, onComplete: () => void) => {
      setScanning(true);
      setMessages([]);
      setScanError(null);
      setCompleted(false);
      onCompleteRef.current = onComplete;

      try {
        await startScan(config);

        cleanupRef.current = subscribeScanEvents(
          (event: ScanEvent) => {
            setMessages((prev) => [...prev, event.message]);

            if (event.type === "complete") {
              teardown();
              setCompleted(true);
              // Immediately trigger graph refresh on completion
              const cb = onCompleteRef.current;
              onCompleteRef.current = null;
              cb?.();
            } else if (event.type === "error") {
              setScanError(event.message);
              teardown();
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

  const runExpand = useCallback(
    async (nodeId: string, onComplete: () => void) => {
      setScanning(true);
      setMessages([]);
      setScanError(null);
      setCompleted(false);
      onCompleteRef.current = onComplete;

      try {
        await expandNode(nodeId);

        cleanupRef.current = subscribeScanEvents(
          (event: ScanEvent) => {
            setMessages((prev) => [...prev, event.message]);

            if (event.type === "complete") {
              teardown();
              setCompleted(true);
              // Immediately trigger graph refresh on completion
              const cb = onCompleteRef.current;
              onCompleteRef.current = null;
              cb?.();
            } else if (event.type === "error") {
              setScanError(event.message);
              teardown();
            }
          },
          () => {
            setScanError("Lost connection to server");
            setScanning(false);
          }
        );
      } catch (err) {
        setScanError(
          err instanceof Error ? err.message : "Failed to expand node"
        );
        setScanning(false);
      }
    },
    []
  );

  const cancelScan = useCallback(() => {
    teardown();
  }, []);

  return { scanning, messages, scanError, completed, runScan, runExpand, cancelScan, dismissMessages };
}
