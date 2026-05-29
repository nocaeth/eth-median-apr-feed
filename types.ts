/**
 * TypeScript types for the eth-median-apr-feed JSON output.
 *
 * Fetch it from:
 *   https://raw.githubusercontent.com/nocaeth/eth-median-apr-feed/main/data/eth-median-apr.json
 *
 * Three rolling windows are published in every run: 7d, 30d, 90d.
 * For an ETH outperformance benchmark, 90d is typically the right denominator.
 *
 * For a cached + last-good-value fallback pattern, see examples/consume-with-cache.ts.
 */

/** Per-validator APR distribution (percent units, 4 decimal places). */
export type DistributionPct = {
  p10: number
  p25: number
  median: number
  mean: number
  p75: number
  p90: number
  p99: number
}

/** One rolling window's worth of data. Window length is the parent key (`7d` → 7 days). */
export type WindowResult = {
  /** ISO date YYYY-MM-DD — window starts here, inclusive. */
  start_date: string
  /** Actual start epoch read from the snapshot file. */
  start_epoch: number
  /** Median APR as percentage, e.g. 2.23. Convenience alias for distribution_pct.median. */
  value_pct: number
  distribution_pct: DistributionPct
  /** Active validators included in this window's calculation. */
  n_validators: number
  /** Withdrawal events summed across this window. */
  n_withdrawal_events: number
}

/**
 * The published feed always carries exactly these three windows — the weekly job
 * runs the default 7/30/90. (A local `--windows` override can emit other keys, but
 * that output is never published, so consumers of the feed only ever see these.)
 */
export type WindowKey = '7d' | '30d' | '90d'

export type EthMedianAprFeed = {
  schema_version: string
  metric: 'eth_validator_median_apr'
  unit: 'percent_per_year'

  /** Shared end of every window (Xatu has a 1–3 day publication lag). */
  end_date: string
  end_epoch: number

  /** Each rolling window's result, keyed by `{days}d`. */
  windows: Record<WindowKey, WindowResult>

  /** ISO 8601 UTC. */
  computed_at: string

  source: {
    name: string
    operator: string
    url: string
    license: string
    tables: string[]
  }
}

export const ETH_MEDIAN_APR_FEED_URL =
  'https://raw.githubusercontent.com/nocaeth/eth-median-apr-feed/main/data/eth-median-apr.json'

export async function fetchEthMedianApr(): Promise<EthMedianAprFeed> {
  const res = await fetch(ETH_MEDIAN_APR_FEED_URL)
  if (!res.ok) {
    throw new Error(`eth-median-apr-feed: HTTP ${res.status}`)
  }
  const data = (await res.json()) as EthMedianAprFeed
  if (typeof data?.schema_version !== 'string') {
    throw new Error('eth-median-apr-feed: malformed response (missing or non-string schema_version)')
  }
  if (!data.schema_version.startsWith('2.')) {
    throw new Error(`eth-median-apr-feed: incompatible schema ${data.schema_version}`)
  }
  if (typeof data.windows !== 'object' || data.windows === null) {
    throw new Error('eth-median-apr-feed: malformed response (missing windows)')
  }
  return data
}
