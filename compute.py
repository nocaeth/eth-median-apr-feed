"""Compute the median ETH validator APR over 7d / 30d / 90d rolling windows.

Source: Xatu public Parquet dataset (https://github.com/ethpandaops/xatu-data).
No API keys. CC BY 4.0.

Per-validator rate = (end_balance − start_balance + sum_withdrawals) / start_effective_balance.
Annualize, then take the median across the active set. Dividing by each validator's
own effective_balance (not a constant 32 ETH) is what makes the metric correct in the
post-Pectra MAXEB regime where validators can hold up to 2048 ETH.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import numpy as np

XATU_BASE = "https://data.ethpandaops.io/xatu/mainnet/databases/default"
SCHEMA_VERSION = "2.0.0"
DEFAULT_WINDOWS = [7, 30, 90]
SAFETY_LAG_DAYS = 3  # Xatu publishes daily files with 1–3 day delay.
MIN_ACTIVE_EFFECTIVE_BALANCE_GWEI = 31_000_000_000  # 31 ETH floor for active validators.
VALUE_PCT_DIGITS = 2     # Headline value_pct rounded to 2 dp for readability.
DISTRIBUTION_PCT_DIGITS = 4  # Distribution percentiles need finer precision; the
                              # middle of the validator distribution spans ~1 bp,
                              # which would collapse if rounded to 2 dp.


def validators_url(d: date) -> str:
    return f"{XATU_BASE}/canonical_beacon_validators/{d.year}/{d.month}/{d.day}/0.parquet"


def withdrawal_url(d: date) -> str:
    return f"{XATU_BASE}/canonical_beacon_block_withdrawal/{d.year}/{d.month}/{d.day}.parquet"


def find_latest_available_date(con) -> date:
    """Walk back from today until we find a published validators snapshot."""
    today = datetime.now(timezone.utc).date()
    for days_back in range(SAFETY_LAG_DAYS, 14):
        d = today - timedelta(days=days_back)
        url = validators_url(d)
        try:
            con.execute(f"SELECT 1 FROM read_parquet('{url}') LIMIT 1").fetchone()
            return d
        except duckdb.Error:
            continue
    raise RuntimeError("No Xatu validators snapshot available in the last 14 days")


def daterange(start: date, end: date):
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(days=1)


def decode_withdrawal_amounts(blobs) -> np.ndarray:
    """Decode N × 16-byte little-endian uint128 blobs to a uint64 numpy array.

    Mainnet withdrawal amounts always fit in uint64 (max ~18B ETH), so the upper
    8 bytes are zero. This is a single C-level numpy operation; ~50x faster than
    a Python struct.unpack loop on millions of rows.
    """
    if len(blobs) == 0:
        return np.array([], dtype=np.uint64)
    if blobs[0] is None or len(blobs[0]) != 16:
        raise ValueError(f"Unexpected withdrawal_amount blob shape: first row = {blobs[0]!r}")
    buf = b"".join(blobs)
    if len(buf) != len(blobs) * 16:
        raise ValueError(
            f"Withdrawal blob size mismatch: {len(buf)} bytes for {len(blobs)} rows"
        )
    return np.frombuffer(buf, dtype="<u8").reshape(-1, 2)[:, 0]


def epoch_of(con, url: str) -> int:
    return int(con.execute(f"SELECT MIN(epoch) FROM read_parquet('{url}')").fetchone()[0])


def compute_window(con, end_date: date, end_epoch: int, end_url: str, window_days: int):
    """Compute one window. Returns the window-result dict on success.

    Reuses the shared end snapshot via DuckDB's object cache; each window's
    distinct start snapshot still pays full fetch cost.
    """
    if window_days < 1:
        raise ValueError(f"window_days must be >= 1, got {window_days}")

    start_date = end_date - timedelta(days=window_days)
    start_url = validators_url(start_date)
    start_epoch = epoch_of(con, start_url)
    print(f"  [{window_days}d] window {start_date} → {end_date}", file=sys.stderr)

    # Withdrawals over the half-open interval [start_date, end_date).
    withdraw_urls = [withdrawal_url(d) for d in daterange(start_date, end_date)]
    withdraw_globs = "[" + ",".join(f"'{u}'" for u in withdraw_urls) + "]"
    wdf = con.execute(
        f"SELECT withdrawal_validator_index AS idx, withdrawal_amount AS amt "
        f"FROM read_parquet({withdraw_globs})"
    ).df()
    n_withdrawals = len(wdf)

    amounts_gwei = decode_withdrawal_amounts(wdf["amt"].values)
    wdf = wdf.assign(amount_gwei=amounts_gwei)
    sum_per_validator = wdf.groupby("idx")["amount_gwei"].sum().reset_index()
    sum_per_validator.columns = ["index", "withdrawn"]
    # Register under a window-specific name so multiple windows can coexist.
    con.register(f"w_{window_days}", sum_per_validator)

    row = con.execute(
        f"""
        WITH
        s AS (
            SELECT
                index,
                balance::BIGINT AS balance,
                effective_balance::BIGINT AS eb
            FROM read_parquet('{start_url}')
            WHERE epoch = {start_epoch}
              AND CAST(status AS VARCHAR) = 'active_ongoing'
              AND effective_balance >= {MIN_ACTIVE_EFFECTIVE_BALANCE_GWEI}
        ),
        e AS (
            SELECT
                index,
                balance::BIGINT AS balance,
                effective_balance::BIGINT AS eb
            FROM read_parquet('{end_url}')
            WHERE epoch = {end_epoch}
              AND CAST(status AS VARCHAR) = 'active_ongoing'
              AND effective_balance >= {MIN_ACTIVE_EFFECTIVE_BALANCE_GWEI}
        ),
        rates AS (
            SELECT
                (e.balance - s.balance + COALESCE(w.withdrawn::BIGINT, 0))::DOUBLE
                    / s.eb::DOUBLE
                    * 365.0 / {window_days}
                    * 100 AS apr_pct
            FROM s JOIN e USING (index) LEFT JOIN w_{window_days} w USING (index)
            -- Exclude validators whose effective_balance changed during the window:
            -- consolidations (post-Pectra) and slashings both manifest as eb deltas,
            -- and both produce fake balance deltas that distort APR.
            WHERE e.eb = s.eb
        )
        SELECT
            COUNT(*),
            quantile_cont(apr_pct, [0.10, 0.25, 0.50, 0.75, 0.90, 0.99]),
            AVG(apr_pct)
        FROM rates
        """
    ).fetchone()
    n, quantiles, mean = row
    p10, p25, median, p75, p90, p99 = quantiles

    return {
        "start_date": start_date.isoformat(),
        "start_epoch": start_epoch,
        "value_pct": round(median, VALUE_PCT_DIGITS),
        "distribution_pct": {
            "p10": round(p10, DISTRIBUTION_PCT_DIGITS),
            "p25": round(p25, DISTRIBUTION_PCT_DIGITS),
            "median": round(median, DISTRIBUTION_PCT_DIGITS),
            "mean": round(mean, DISTRIBUTION_PCT_DIGITS),
            "p75": round(p75, DISTRIBUTION_PCT_DIGITS),
            "p90": round(p90, DISTRIBUTION_PCT_DIGITS),
            "p99": round(p99, DISTRIBUTION_PCT_DIGITS),
        },
        "n_validators": int(n),
        "n_withdrawal_events": int(n_withdrawals),
        "_raw_median_pct": float(median),  # consumed by the all-or-nothing gate below
    }


def compute(windows: list[int] | None = None):
    if windows is None:
        windows = DEFAULT_WINDOWS
    # Dedupe + sort + reject non-positive.
    windows = sorted({int(w) for w in windows if int(w) >= 1})
    if not windows:
        raise ValueError("windows must contain at least one positive integer")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET enable_object_cache=true;")  # shares the end snapshot across windows

    end_date = find_latest_available_date(con)
    end_url = validators_url(end_date)
    end_epoch = epoch_of(con, end_url)
    print(f"End: {end_date} (epoch {end_epoch})", file=sys.stderr)
    print(f"Windows: {windows}", file=sys.stderr)

    t_total = time.time()
    window_results = {}
    for w in windows:
        t0 = time.time()
        window_results[f"{w}d"] = compute_window(con, end_date, end_epoch, end_url, w)
        print(f"  [{w}d] computed in {time.time() - t0:.1f}s", file=sys.stderr)
    print(f"Total {time.time() - t_total:.1f}s.", file=sys.stderr)

    # All-or-nothing sanity gate: every window must pass before we publish anything.
    for key, w in window_results.items():
        median = w["_raw_median_pct"]
        n = w["n_validators"]
        if not (0.5 <= median <= 10.0):
            raise RuntimeError(f"{key} median {median:.4f}% outside sane band [0.5, 10.0]")
        if n < 500_000:
            raise RuntimeError(f"{key} has only {n} validators — sample seems wrong")
    for w in window_results.values():
        w.pop("_raw_median_pct", None)

    return {
        "schema_version": SCHEMA_VERSION,
        "metric": "eth_validator_median_apr",
        "unit": "percent_per_year",
        "end_date": end_date.isoformat(),
        "end_epoch": end_epoch,
        "windows": window_results,
        "computed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": {
            "name": "Xatu",
            "operator": "EthPandaOps / Ethereum Foundation",
            "url": "https://github.com/ethpandaops/xatu-data",
            "license": "CC BY 4.0",
            "tables": [
                "canonical_beacon_validators",
                "canonical_beacon_block_withdrawal",
            ],
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    def parse_windows(s: str) -> list[int]:
        return [int(x) for x in s.split(",") if x.strip()]

    parser.add_argument(
        "--windows",
        type=parse_windows,
        default=DEFAULT_WINDOWS,
        help="Comma-separated list of window sizes in days, e.g. 7,30,90",
    )
    parser.add_argument("--out", type=Path, default=None, help="Write to file (default stdout)")
    args = parser.parse_args()

    try:
        result = compute(args.windows)
    except ValueError as e:
        parser.error(str(e))
    payload = json.dumps(result, indent=2) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(payload)
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(payload)


if __name__ == "__main__":
    main()
