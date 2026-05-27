// Consumer pattern with cache + last-good-value fallback.
//
// Behavior:
//   - Fresh cache (<1d old) → use it, don't hit the network.
//   - Stale or missing → try to refresh.
//   - Refresh failed → return the most recent cached value (even if stale), with a warning.
//   - No cache AND refresh failed → throw to caller. Don't invent a number.
//
// Run with: bun run examples/consume-with-cache.ts

import { fetchEthMedianApr } from '../types'

const CACHE_KEY = 'eth-median-apr'
const CACHE_TTL_MS = 24 * 60 * 60 * 1000

type CacheEntry = {
  value_pct: number
  fetched_at: number
  computed_at: string
}

function readCache(): CacheEntry | null {
  try {
    const raw = localStorage.getItem(CACHE_KEY)
    return raw ? (JSON.parse(raw) as CacheEntry) : null
  } catch {
    return null
  }
}

function writeCache(entry: CacheEntry): void {
  try {
    localStorage.setItem(CACHE_KEY, JSON.stringify(entry))
  } catch {
    // localStorage disabled / quota — best effort
  }
}

export async function getBenchmark(): Promise<number> {
  const cached = readCache()

  // Fresh cache: use without fetching.
  if (cached && Date.now() - cached.fetched_at < CACHE_TTL_MS) {
    return cached.value_pct
  }

  // Stale or missing: try to refresh.
  try {
    const feed = await fetchEthMedianApr()
    const value_pct = feed.windows['90d'].value_pct
    writeCache({
      value_pct,
      fetched_at: Date.now(),
      computed_at: feed.computed_at,
    })
    return value_pct
  } catch (err) {
    // Refresh failed: fall back to the last good value if we have one.
    if (cached) {
      console.warn(
        `eth-median-apr-feed unreachable, using cached value ${cached.value_pct}% from ${cached.computed_at}`,
        err,
      )
      return cached.value_pct
    }
    // No cache and no fetch: caller must handle.
    throw err
  }
}

// Demo:
if (typeof localStorage !== 'undefined') {
  const value = await getBenchmark()
  console.log(`ETH median validator APR: ${value}%`)
}
