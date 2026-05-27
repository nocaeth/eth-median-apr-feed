# eth-median-apr-feed

Median ETH validator APR over rolling 7-day, 30-day, and 90-day windows. Recomputed weekly, published as JSON.

```
https://raw.githubusercontent.com/nocaeth/eth-median-apr-feed/main/data/eth-median-apr.json
```

## Schema

```jsonc
{
  "schema_version": "2.0.0",
  "metric": "eth_validator_median_apr",
  "unit": "percent_per_year",
  "end_date": "2026-05-24",
  "end_epoch": 449888,
  "windows": {
    "7d":  { "start_date": "...", "start_epoch": ..., "value_pct": 2.23,
             "distribution_pct": { "p10": ..., "p25": ..., "median": ..., "mean": ...,
                                   "p75": ..., "p90": ..., "p99": ... },
             "n_validators": ..., "n_withdrawal_events": ... },
    "30d": { ... },
    "90d": { ... }
  },
  "computed_at": "2026-05-27T06:15:00Z",
  "source": { ... }
}
```

`value_pct` rounded to 2 decimals (headline). `distribution_pct` rounded to 4 decimals (the middle quantiles span ~1 bp). TypeScript types in [`types.ts`](./types.ts).

## Methodology

For each active validator over the window:

```
rewards_gwei = end_balance − start_balance + Σ withdrawals
validator_apr = rewards_gwei / start_effective_balance × 365 / window_days
```

Take the median across validators whose status is `active_ongoing` at both endpoints **and** whose effective balance was unchanged across the window (excludes consolidations, slashings, 0x02 compounder EB increments).

### What it measures

CL-only rewards (attestations, proposer CL portion, sync committee) — EL rewards (priority fees + MEV) go to the fee_recipient address and don't accrue to CL balance. This does not move the median: the median validator proposes zero blocks in the window, so EL contributions are mechanically zero at p50. EL only lifts p75 and above.

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

~45 seconds. Streams ~1 GB of Parquet from `data.ethpandaops.io`.

## How it stays up to date

`.github/workflows/compute.yml` runs every Monday at 06:00 UTC, executes `compute.py`, and commits the result if the JSON changed. No infrastructure.

## Sources

- **[Xatu](https://github.com/ethpandaops/xatu-data)** — public Ethereum dataset (`canonical_beacon_validators`, `canonical_beacon_block_withdrawal`). Maintained by EthPandaOps. CC BY 4.0.
- **[DuckDB](https://duckdb.org/)** — streaming Parquet query engine.

## License

Code: MIT. Data: CC BY 4.0.
