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

// Use localStorage in the browser; fall back to an in-memory store under Bun/Node so
// this example actually exercises the cache path when run with `bun run` (in a server
// you'd swap this for Redis, a file, or your DB).
const store: Pick<Storage, 'getItem' | 'setItem'> =
  typeof localStorage !== 'undefined'
    ? localStorage
    : (() => {
        const mem = new Map<string, string>()
        return {
          getItem: (k: string) => mem.get(k) ?? null,
          setItem: (k: string, v: string) => void mem.set(k, v),
        }
      })()

function readCache(): CacheEntry | null {
  try {
    const raw = store.getItem(CACHE_KEY)
    return raw ? (JSON.parse(raw) as CacheEntry) : null
  } catch {
    return null
  }
}

function writeCache(entry: CacheEntry): void {
  try {
    store.setItem(CACHE_KEY, JSON.stringify(entry))
  } catch {
    // store unavailable / quota — best effort
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

// Demo — only when run directly (`bun run`), not when getBenchmark is imported,
// so importing this module never triggers a network fetch as a side effect.
if (import.meta.main) {
  const value = await getBenchmark()
  console.log(`ETH median validator APR: ${value}%`)
}
