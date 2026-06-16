# eth-median-apr-feed

Median ETH **consensus-layer** validator APR over rolling 7-day, 30-day, and 90-day windows, plus an **execution-layer** (MEV-Boost) aggregate and a combined **total** APR. Recomputed weekly, published as JSON.

```
https://raw.githubusercontent.com/nocaeth/eth-median-apr-feed/main/data/eth-median-apr.json
```

## Schema

```jsonc
{
  "schema_version": "2.1.0",
  "metric": "eth_validator_median_apr",
  "unit": "percent_per_year",
  "end_date": "2026-05-24",
  "end_epoch": 449888,
  "windows": {
    "7d":  { "start_date": "...", "start_epoch": ..., "value_pct": 2.23,
             "distribution_pct": { "p10": ..., "p25": ..., "median": ..., "mean": ...,
                                   "p75": ..., "p90": ..., "p99": ... },
             "consensus_apr_pct": 2.60,   // stake-weighted CL aggregate — added in 2.1.0
             "n_validators": ..., "n_withdrawal_events": ...,
             // execution-layer (MEV-Boost) — added in 2.1.0
             "execution": { "apr_pct": 0.19,
                            "mev_eth_per_block": { "mean": ..., "median": ... },
                            "n_mev_blocks": ..., "note": "..." },
             "total_apr_pct": 2.79 },
    "30d": { ... },
    "90d": { ... }
  },
  "computed_at": "2026-05-27T06:15:00Z",
  "source": { ... }
}
```

`value_pct` and `total_apr_pct` rounded to 2 decimals (headline); `distribution_pct` to 4 (the middle quantiles span ~1 bp); `mev_eth_per_block` to 6 (payments are O(0.01 ETH)). **2.1.0 is additive** — every 2.0.0 field is unchanged, so existing consumers keep working. TypeScript types in [`types.ts`](./types.ts).

## Methodology

For each active validator over the window:

```
rewards_gwei = end_balance − start_balance + Σ withdrawals
validator_apr = rewards_gwei / start_effective_balance × 365 / window_days
```

Take the median across validators whose status is `active_ongoing` at both endpoints **and** whose effective balance was unchanged across the window (excludes consolidations, slashings, 0x02 compounder EB increments).

### What `distribution_pct` measures

CL-only rewards (attestations, proposer CL portion, sync committee). EL rewards (priority fees + MEV) go to the fee_recipient address and never touch CL balance, so they are excluded from **every** percentile here, not just the median. The median validator earns only attestation rewards over the window, so p50 reflects baseline CL yield; the upper tail (p75+) is lifted by lumpy *CL* income — block-proposal and sync-committee rewards — not by EL. The `execution` block (below) adds EL back as a separate dimension.

### Execution layer (`execution`, `total_apr_pct`)

EL rewards land on the execution layer at the proposer's fee_recipient, so they need a different source — the MEV-relay `mev_relay_proposer_payload_delivered` table, whose `value` is the payment delivered to the proposer per block:

```
execution.apr_pct = Σ proposer_value (deduped by block_hash) / Σ active_effective_balance × 365 / window_days
```

Two deliberate choices:

- **Aggregate, not median.** Most validators propose no block in a window, so a per-validator EL *median* is ~0 — the signal is in the network aggregate. `mev_eth_per_block.{mean,median}` exposes the raw per-block distribution (it's heavily right-skewed: a few fat MEV blocks pull the mean well above the median).
- **Lower bound.** Only MEV-Boost blocks are counted. Locally-built blocks (~10%) earn priority fees not captured here; closing that gap is a future pass over `canonical_execution_*`.

Rows are deduped by `block_hash` (each delivered block is logged once per relay and per Xatu sentry). The denominator is **all** active stake (not the unchanged-eb subset the CL median filters to), since EL accrues network-wide.

`total_apr_pct = consensus_apr_pct + execution.apr_pct` — the **stake-weighted** consensus aggregate plus the EL aggregate, ≈ what a large, well-run operator actually realizes.

`consensus_apr_pct` (Σ CL rewards ÷ Σ stake) is the stake-weighted counterpart to `distribution_pct.mean` (per-validator, unweighted). They differ because the mean gives a 32-ETH and a 2048-ETH validator equal weight, so the small-validator proposer/sync lottery inflates it — stake-weighting is the correct basis for an aggregate-capital yield, and the only term consistent with the (also stake-weighted) EL APR. Both consensus figures use the unchanged-EB set; the EL denominator is all active stake, so `total_apr_pct` layers two per-unit-stake rates.

### Withdrawal correction

Active validators auto-withdraw rewards every ~8 days. Over 30 days `end_balance − start_balance` is negative without summing withdrawals back. Necessary.

### Effective-balance denominator

Post-Pectra (EIP-7251) validators can have effective balance up to 2048 ETH. Dividing rewards by each validator's own `start_effective_balance` is the correct stake-weighted denominator; a constant 32 ETH divisor would inflate consolidated validators' apparent APR.

## Run locally

```bash
uv run python compute.py                              # all 3 windows
uv run python compute.py --windows 30                 # 30d only
uv run python compute.py --out data/eth-median-apr.json
```

~3–4 minutes (the 90d MEV-relay read dominates). Streams Parquet from `data.ethpandaops.io`; nothing is stored.

## How it stays up to date

`.github/workflows/compute.yml` runs every Monday at 06:00 UTC, executes `compute.py`, and commits the result if the JSON changed. No infrastructure.

## Sources

- **[Xatu](https://github.com/ethpandaops/xatu-data)** — public Ethereum dataset (`canonical_beacon_validators`, `canonical_beacon_block_withdrawal`, `mev_relay_proposer_payload_delivered`). Maintained by EthPandaOps. CC BY 4.0.
- **[DuckDB](https://duckdb.org/)** — streaming Parquet query engine.

## License

Code: MIT. Data: CC BY 4.0.
