export function normalizeUnixTimestampSec(value, fallbackSec = Math.floor(Date.now() / 1000)) {
  const fallback = Number(fallbackSec);
  const normalizedFallback = Number.isFinite(fallback) && fallback > 0
    ? Math.floor(fallback > 1e10 ? fallback / 1000 : fallback)
    : Math.floor(Date.now() / 1000);
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return normalizedFallback;
  return Math.floor(n > 1e10 ? n / 1000 : n);
}
