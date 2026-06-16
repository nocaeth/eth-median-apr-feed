"""Compute ETH validator APR over 7d / 30d / 90d rolling windows.

Source: Xatu public Parquet dataset (https://github.com/ethpandaops/xatu-data).
No API keys. CC BY 4.0.

Consensus layer (the median + distribution): per-validator rate =
(end_balance − start_balance + sum_withdrawals) / start_effective_balance, annualized, then
the median/percentiles across the active set. Dividing by each validator's own
effective_balance (not a constant 32 ETH) is what makes the metric correct in the post-Pectra
MAXEB regime where validators can hold up to 2048 ETH.

Execution layer (the `execution` block + `total_apr_pct`): MEV-Boost proposer payments from the
relay data, aggregated network-wide (Σ value / Σ active stake). EL rewards never touch the
validator's CL balance, so they require this separate source — see compute_execution.
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
SCHEMA_VERSION = "2.1.0"  # 2.1.0 adds additive execution-layer fields; 2.x consumers unaffected.
DEFAULT_WINDOWS = [7, 30, 90]
SAFETY_LAG_DAYS = 3  # Xatu publishes daily files with 1–3 day delay.
MIN_ACTIVE_EFFECTIVE_BALANCE_GWEI = 31_000_000_000  # 31 ETH floor for active validators.
SECONDS_PER_EPOCH = 384  # 32 slots × 12s; stable on mainnet since the Beacon Chain genesis.
ANNUALIZATION_TOLERANCE_DAYS = 0.5  # Realized epoch span must match the nominal window this closely.
REMOTE_READ_ATTEMPTS = 4    # Total tries for a known-good remote read before giving up.
REMOTE_READ_BASE_DELAY = 2.0  # Backoff base: 2s, 4s, 8s between retries.
VALUE_PCT_DIGITS = 2     # Headline value_pct rounded to 2 dp for readability.
DISTRIBUTION_PCT_DIGITS = 4  # Distribution percentiles need finer precision; the
                              # middle of the validator distribution spans ~1 bp,
                              # which would collapse if rounded to 2 dp.
MEV_ETH_DIGITS = 6  # Per-block MEV-Boost payments are O(0.01 ETH); needs sub-gwei precision.
MIN_MEV_BLOCKS_PER_DAY = 2000  # Sanity floor (real mainnet ~6500/day); below this = relay data gap.


def validators_url(d: date) -> str:
    return f"{XATU_BASE}/canonical_beacon_validators/{d.year}/{d.month}/{d.day}/0.parquet"


def withdrawal_url(d: date) -> str:
    return f"{XATU_BASE}/canonical_beacon_block_withdrawal/{d.year}/{d.month}/{d.day}.parquet"


def mev_payload_url(d: date) -> str:
    return f"{XATU_BASE}/mev_relay_proposer_payload_delivered/{d.year}/{d.month}/{d.day}.parquet"


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


def read_with_retry(thunk, what: str):
    """Run a DuckDB read that targets a known-good URL, retrying transient errors.

    A 404 on a genuinely-missing file is indistinguishable from a transient network
    blip at the duckdb.Error level, so we retry either way and let the last attempt
    re-raise. Callers should only wrap reads whose target is expected to exist (the
    end snapshot is already resolved; start snapshots and withdrawal files are
    historical), never the find_latest_available_date probe whose 404s are expected.
    """
    for i in range(REMOTE_READ_ATTEMPTS):
        try:
            return thunk()
        except duckdb.Error as e:
            if i == REMOTE_READ_ATTEMPTS - 1:
                raise
            delay = REMOTE_READ_BASE_DELAY * (2**i)
            print(
                f"  transient read error on {what} ({e}); retry {i + 1}/{REMOTE_READ_ATTEMPTS - 1} in {delay:.0f}s",
                file=sys.stderr,
            )
            time.sleep(delay)


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
    try:
        buf = b"".join(blobs)
    except TypeError as e:
        # A None (or other non-bytes) value past row 0 lands here; surface it as a
        # clear ValueError instead of an opaque join TypeError.
        raise ValueError(f"withdrawal_amount column contains a non-bytes row: {e}") from e
    if len(buf) != len(blobs) * 16:
        raise ValueError(
            f"Withdrawal blob size mismatch: {len(buf)} bytes for {len(blobs)} rows"
        )
    return np.frombuffer(buf, dtype="<u8").reshape(-1, 2)[:, 0]


def decode_payload_values_eth(blobs) -> np.ndarray:
    """Decode MEV-Boost `value` blobs (little-endian uint256 wei) to ETH (float64).

    Unlike withdrawal amounts (always ≤ uint64), a block's MEV value can in principle
    exceed uint64, so we decode each blob exactly with int.from_bytes rather than slicing
    a fixed low word. After dedup the row count is one-per-block (sub-million even at 90d),
    so the Python-level decode is cheap; the float64 result is exact at ETH scale.
    """
    n = len(blobs)
    if n == 0:
        return np.array([], dtype=np.float64)
    wei = np.fromiter(
        (int.from_bytes(bytes(b), "little") for b in blobs),
        dtype=np.float64,
        count=n,
    )
    return wei / 1e18


def epoch_of(con, url: str) -> int:
    row = read_with_retry(
        lambda: con.execute(f"SELECT MIN(epoch) FROM read_parquet('{url}')").fetchone(),
        what=f"epoch_of({url})",
    )
    return int(row[0])


def check_realized_span(start_epoch: int, end_epoch: int, window_days: int) -> float:
    """Realized span (days) between two snapshots; raise if it deviates from nominal.

    The annualization assumes the snapshots are exactly `window_days` apart. Validate
    that against the realized epoch span so a shifted/anomalous snapshot aborts loudly
    rather than silently mis-annualizing the published APR.
    """
    realized_days = (end_epoch - start_epoch) * SECONDS_PER_EPOCH / 86400.0
    if abs(realized_days - window_days) > ANNUALIZATION_TOLERANCE_DAYS:
        raise RuntimeError(
            f"[{window_days}d] realized span {realized_days:.3f}d (epochs {start_epoch}→{end_epoch}) "
            f"deviates >{ANNUALIZATION_TOLERANCE_DAYS}d from the nominal {window_days}d window; "
            f"refusing to annualize"
        )
    return realized_days


def normalize_windows(windows: list[int]) -> list[int]:
    """Dedupe, sort, and reject non-positive window sizes. Raises ValueError if empty."""
    windows = sorted({int(w) for w in windows if int(w) >= 1})
    if not windows:
        raise ValueError("windows must contain at least one positive integer")
    return windows


def compute_execution(con, start_date: date, end_date: date, start_url: str,
                      start_epoch: int, window_days: int) -> dict:
    """Aggregate execution-layer (MEV-Boost) reward for the window.

    Execution rewards (priority fees + MEV) are paid to the proposer's fee_recipient on
    the EL — they never touch the validator's beacon balance, so the consensus metric in
    compute_window cannot see them. We read the MEV-relay `proposer_payload_delivered`
    table, where `value` is the payment delivered to the proposer per block.

    Two deliberate methodology choices:
    - AGGREGATE, not median: most validators propose no block in a window, so a per-validator
      median EL APR is ~0. The meaningful figure is network-aggregate: total payments over
      total active stake. We still surface mean/median ETH-per-block for the raw distribution.
    - LOWER BOUND: this covers only MEV-Boost blocks. Locally-built blocks (~10%) earn priority
      fees not captured here; closing that gap is a future pass over canonical_execution_*.
    """
    # Denominator: total active effective balance at the start snapshot (gwei), reusing the
    # object-cached start parquet. Unlike the CL median (which filters to the unchanged-eb
    # subset), the EL aggregate is a network rate, so it uses ALL active validators.
    total_eb_gwei = read_with_retry(
        lambda: con.execute(
            f"""
        SELECT SUM(effective_balance)::DOUBLE
        FROM read_parquet('{start_url}')
        WHERE epoch = {start_epoch}
          AND CAST(status AS VARCHAR) = 'active_ongoing'
          AND effective_balance >= {MIN_ACTIVE_EFFECTIVE_BALANCE_GWEI}
        """
        ).fetchone(),
        what=f"{window_days}d total effective balance",
    )[0]
    if not total_eb_gwei:
        raise RuntimeError(f"[{window_days}d] zero active effective balance at start snapshot")

    # Dedup by block_hash: the same delivered block is logged once per relay AND per Xatu
    # sentry, so the raw rows multi-count. One distinct block_hash = one proposer payment.
    mev_urls = [mev_payload_url(d) for d in daterange(start_date, end_date)]
    mev_globs = "[" + ",".join(f"'{u}'" for u in mev_urls) + "]"
    rows = read_with_retry(
        lambda: con.execute(
            f"""
        SELECT any_value(value) AS value
        FROM read_parquet({mev_globs})
        GROUP BY block_hash
        """
        ).df(),
        what=f"{window_days}d MEV payloads",
    )
    n_blocks = len(rows)
    values_eth = decode_payload_values_eth(rows["value"].values)
    total_value_eth = float(values_eth.sum()) if n_blocks else 0.0
    total_eb_eth = float(total_eb_gwei) / 1e9
    apr_pct = total_value_eth / total_eb_eth * 365.0 / window_days * 100.0

    return {
        "apr_pct": round(apr_pct, VALUE_PCT_DIGITS),
        "mev_eth_per_block": {
            "mean": round(float(values_eth.mean()), MEV_ETH_DIGITS) if n_blocks else 0.0,
            "median": round(float(np.median(values_eth)), MEV_ETH_DIGITS) if n_blocks else 0.0,
        },
        "n_mev_blocks": n_blocks,
        "note": "MEV-Boost proposer payments only; lower bound (excludes locally-built blocks)",
        "_raw_apr_pct": apr_pct,  # consumed by the caller for total + the sanity gate
    }


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

    check_realized_span(start_epoch, end_epoch, window_days)

    # Withdrawals over the half-open interval [start_date, end_date).
    withdraw_urls = [withdrawal_url(d) for d in daterange(start_date, end_date)]
    withdraw_globs = "[" + ",".join(f"'{u}'" for u in withdraw_urls) + "]"
    wdf = read_with_retry(
        lambda: con.execute(
            f"SELECT withdrawal_validator_index AS idx, withdrawal_amount AS amt "
            f"FROM read_parquet({withdraw_globs})"
        ).df(),
        what=f"{window_days}d withdrawals",
    )
    n_withdrawals = len(wdf)

    amounts_gwei = decode_withdrawal_amounts(wdf["amt"].values)
    wdf = wdf.assign(amount_gwei=amounts_gwei)
    sum_per_validator = wdf.groupby("idx")["amount_gwei"].sum().reset_index()
    sum_per_validator.columns = ["index", "withdrawn"]
    # Register under a window-specific name so multiple windows can coexist.
    con.register(f"w_{window_days}", sum_per_validator)

    row = read_with_retry(
        lambda: con.execute(
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
                (e.balance - s.balance + COALESCE(w.withdrawn::BIGINT, 0))::DOUBLE AS reward_gwei,
                s.eb::DOUBLE AS eb
            FROM s JOIN e USING (index) LEFT JOIN w_{window_days} w USING (index)
            -- Exclude validators whose effective_balance changed during the window:
            -- consolidations (post-Pectra) and slashings both manifest as eb deltas,
            -- and both produce fake balance deltas that distort APR.
            WHERE e.eb = s.eb
        )
        SELECT
            COUNT(*),
            -- Per-validator distribution (each validator one vote, regardless of stake).
            quantile_cont(reward_gwei / eb * 365.0 / {window_days} * 100,
                          [0.10, 0.25, 0.50, 0.75, 0.90, 0.99]),
            AVG(reward_gwei / eb * 365.0 / {window_days} * 100),
            -- Stake-weighted aggregate (Σ rewards ÷ Σ stake): a 2048-ETH validator counts 64×
            -- a 32-ETH one. This is the correct basis for a network / large-operator yield,
            -- and the consensus term added to the (also stake-denominated) execution APR.
            SUM(reward_gwei) / SUM(eb) * 365.0 / {window_days} * 100
        FROM rates
        """
        ).fetchone(),
        what=f"{window_days}d rates",
    )
    # COUNT(*) is 0 and the quantile list is NULL when no validator survives the
    # join + filters; guard before unpacking so we fail loud instead of hitting an
    # opaque "cannot unpack NoneType" on the quantile row.
    if not row[0] or row[1] is None:
        raise RuntimeError(
            f"[{window_days}d] no validators survived filtering (start {start_date} → end {end_date}); "
            f"cannot compute a median"
        )
    n, quantiles, mean, consensus_apr = row
    p10, p25, median, p75, p90, p99 = quantiles

    execution = compute_execution(con, start_date, end_date, start_url, start_epoch, window_days)
    raw_exec_apr = execution.pop("_raw_apr_pct")
    # Total ≈ a large operator's realized yield: the STAKE-WEIGHTED consensus aggregate (not the
    # per-validator mean, which over-weights small validators) plus the (also stake-weighted)
    # execution aggregate. The headline median/distribution stay per-validator and consensus-only.
    total_apr_pct = consensus_apr + raw_exec_apr

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
        "consensus_apr_pct": round(consensus_apr, VALUE_PCT_DIGITS),
        "n_validators": int(n),
        "n_withdrawal_events": int(n_withdrawals),
        "execution": execution,
        "total_apr_pct": round(total_apr_pct, VALUE_PCT_DIGITS),
        "_raw_median_pct": float(median),  # consumed by the all-or-nothing gate below
        "_raw_consensus_apr_pct": float(consensus_apr),
        "_raw_execution_apr_pct": float(raw_exec_apr),
    }


def compute(windows: list[int] | None = None):
    if windows is None:
        windows = DEFAULT_WINDOWS
    windows = normalize_windows(windows)

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
        consensus_apr = w["_raw_consensus_apr_pct"]
        if not (0.5 <= consensus_apr <= 10.0):
            raise RuntimeError(
                f"{key} consensus aggregate APR {consensus_apr:.4f}% outside sane band [0.5, 10.0]"
            )
        exec_apr = w["_raw_execution_apr_pct"]
        if not (0.0 <= exec_apr <= 5.0):
            raise RuntimeError(f"{key} execution APR {exec_apr:.4f}% outside sane band [0.0, 5.0]")
        window_days = int(key[:-1])  # "7d" -> 7
        n_mev = w["execution"]["n_mev_blocks"]
        if n_mev < window_days * MIN_MEV_BLOCKS_PER_DAY:
            raise RuntimeError(
                f"{key} has only {n_mev} MEV blocks (<{window_days * MIN_MEV_BLOCKS_PER_DAY}); "
                f"relay data gap?"
            )
    for w in window_results.values():
        w.pop("_raw_median_pct", None)
        w.pop("_raw_consensus_apr_pct", None)
        w.pop("_raw_execution_apr_pct", None)

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
                "mev_relay_proposer_payload_delivered",
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

    # Bad --windows is a usage error (exit 2); anything that fails later inside
    # compute() is a data/runtime error and should propagate as a normal failure
    # (traceback, exit 1) so the CI job fails and the on-failure issue step fires.
    try:
        windows = normalize_windows(args.windows)
    except ValueError as e:
        parser.error(str(e))

    result = compute(windows)
    payload = json.dumps(result, indent=2) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(payload)
        print(f"Wrote {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(payload)


if __name__ == "__main__":
    main()
