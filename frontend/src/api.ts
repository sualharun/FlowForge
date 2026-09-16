import { useCallback, useEffect, useRef, useState } from 'react';

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...options,
    headers: { Accept: 'application/json', ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers },
  });
  if (!response.ok) {
    const body = await response.text();
    let message = `${response.status} ${response.statusText}`;
    try {
      const error = JSON.parse(body) as { detail?: string; message?: string; error?: string };
      message = error.detail || error.message || error.error || message;
    } catch { /* Proxy error pages are deliberately not rendered as HTML. */ }
    throw new Error(message);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

// A recursive timeout avoids overlapping requests. Abort on navigation/unmount,
// pause in background tabs, and retain the last successful snapshot on errors.
export function usePolling<T>(path: string, intervalMs = 4000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const refreshRef = useRef<() => void>(() => {});
  const refresh = useCallback(() => refreshRef.current(), []);

  useEffect(() => {
    let disposed = false;
    let inFlight = false;
    let pendingRefresh = false;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;
    setData(null);
    setError(null);
    setLoading(true);
    setUpdatedAt(null);

    async function load() {
      clearTimeout(timeout);
      if (disposed) return;
      if (inFlight) { pendingRefresh = true; return; }
      if (document.visibilityState === 'hidden') {
        timeout = setTimeout(load, intervalMs);
        return;
      }
      inFlight = true;
      controller = new AbortController();
      try {
        const next = await api<T>(path, { signal: controller.signal });
        if (!disposed) { setData(next); setError(null); setUpdatedAt(new Date()); }
      } catch (cause) {
        if (!disposed && !(cause instanceof DOMException && cause.name === 'AbortError')) {
          setError(cause instanceof Error ? cause.message : 'Unable to reach the API.');
        }
      } finally {
        inFlight = false;
        if (!disposed) {
          setLoading(false);
          const delay = pendingRefresh ? 0 : intervalMs;
          pendingRefresh = false;
          timeout = setTimeout(load, delay);
        }
      }
    }

    function visible() { if (document.visibilityState === 'visible') void load(); }
    refreshRef.current = () => void load();
    document.addEventListener('visibilitychange', visible);
    void load();
    return () => {
      disposed = true;
      clearTimeout(timeout);
      controller?.abort();
      document.removeEventListener('visibilitychange', visible);
    };
  }, [path, intervalMs]);

  return { data, error, loading, updatedAt, refresh };
}
