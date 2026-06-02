// Pure formatting helpers, kept framework-agnostic so they're trivially unit-testable. None of
// these throw on edge inputs — operators see "—" for undefined values rather than NaN.

const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'] as const;

export function formatBytes(bytes: number | undefined | null): string {
  if (bytes == null || !Number.isFinite(bytes)) return '—';
  if (bytes < 1024) return `${bytes} B`;
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < BYTE_UNITS.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${BYTE_UNITS[i]}`;
}

export function formatCount(n: number | undefined | null): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return n.toLocaleString('en-US');
}

export function formatRelativeTime(epochMillis: number | undefined | null): string {
  if (!epochMillis) return '—';
  const delta = Date.now() - epochMillis;
  const absSec = Math.round(Math.abs(delta) / 1000);
  if (absSec < 60) return `${absSec}s ago`;
  if (absSec < 3600) return `${Math.round(absSec / 60)}m ago`;
  if (absSec < 86400) return `${Math.round(absSec / 3600)}h ago`;
  return `${Math.round(absSec / 86400)}d ago`;
}

export function formatAbsoluteTime(epochMillis: number | undefined | null): string {
  if (!epochMillis) return '—';
  return new Date(epochMillis).toISOString().replace('T', ' ').slice(0, 19);
}
