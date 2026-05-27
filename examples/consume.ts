// Example TypeScript consumer for the eth-median-apr-feed.
// Run with: bun run examples/consume.ts

import { fetchEthMedianApr } from '../types'

const feed = await fetchEthMedianApr()

console.log(`ETH validator median APR (computed ${feed.computed_at}, window ends ${feed.end_date}):`)
for (const key of ['7d', '30d', '90d'] as const) {
  const w = feed.windows[key]
  console.log(`  ${key.padEnd(4)} ${w.value_pct.toFixed(2)}%   (mean ${w.distribution_pct.mean.toFixed(2)}%, n=${w.n_validators.toLocaleString()})`)
}
console.log()
console.log(`Recommended benchmark for quarterly outperformance: ${feed.windows['90d'].value_pct}%`)
