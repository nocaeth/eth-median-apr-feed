"""Unit tests for the network-free pieces of compute.py.

These don't touch Xatu — they pin the pure decode/date logic and the retry wrapper,
which is where the regressions a weekly unattended job would silently ship live.
"""

import duckdb
import numpy as np
import pytest

import compute


def _blob(value: int) -> bytes:
    """A withdrawal_amount blob: 16-byte little-endian uint128 (high 8 bytes zero)."""
    return value.to_bytes(16, "little")


class TestDecodeWithdrawalAmounts:
    def test_empty(self):
        out = compute.decode_withdrawal_amounts(np.array([], dtype=object))
        assert out.dtype == np.uint64
        assert len(out) == 0

    def test_single_and_multiple(self):
        out = compute.decode_withdrawal_amounts(np.array([_blob(0), _blob(12345), _blob(2**63)], dtype=object))
        assert out.tolist() == [0, 12345, 2**63]

    def test_high_8_bytes_ignored(self):
        # Only the low uint64 is meaningful for mainnet; high bytes must be dropped.
        blob = (777).to_bytes(8, "little") + (999).to_bytes(8, "little")
        out = compute.decode_withdrawal_amounts(np.array([blob], dtype=object))
        assert out.tolist() == [777]

    def test_accepts_bytearray(self):
        # DuckDB hands back BLOB columns as bytearray, not bytes. Build the object
        # array by assignment so numpy keeps the bytearray as one element (a plain
        # np.array([bytearray(...)]) would expand it into a 2-D array of ints).
        arr = np.empty(2, dtype=object)
        arr[0] = bytearray(_blob(42))
        arr[1] = bytearray(_blob(7))
        out = compute.decode_withdrawal_amounts(arr)
        assert out.tolist() == [42, 7]

    def test_first_row_none_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unexpected withdrawal_amount blob shape"):
            compute.decode_withdrawal_amounts(np.array([None, _blob(1)], dtype=object))

    def test_first_row_wrong_size_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unexpected withdrawal_amount blob shape"):
            compute.decode_withdrawal_amounts(np.array([b"\x00" * 8, _blob(1)], dtype=object))

    def test_mid_row_none_raises_clear_valueerror(self):
        # Regression: previously raised an opaque TypeError from b"".join.
        with pytest.raises(ValueError, match="non-bytes row"):
            compute.decode_withdrawal_amounts(np.array([_blob(1), None], dtype=object))

    def test_mid_row_wrong_size_raises_valueerror(self):
        with pytest.raises(ValueError, match="size mismatch"):
            compute.decode_withdrawal_amounts(np.array([_blob(1), b"\x00" * 8], dtype=object))


class TestDecodePayloadValuesEth:
    def test_empty(self):
        out = compute.decode_payload_values_eth(np.array([], dtype=object))
        assert out.dtype == np.float64
        assert len(out) == 0

    def test_decodes_le_wei_to_eth(self):
        # 0.1 ETH and 1 ETH as 32-byte little-endian uint256, like the relay `value` column.
        vals = np.array([(10**17).to_bytes(32, "little"), (10**18).to_bytes(32, "little")], dtype=object)
        out = compute.decode_payload_values_eth(vals)
        assert out.tolist() == pytest.approx([0.1, 1.0])

    def test_accepts_bytearray_and_mixed_widths(self):
        # DuckDB hands BLOBs back as bytearray; int.from_bytes handles any width exactly.
        arr = np.empty(2, dtype=object)
        arr[0] = bytearray((5 * 10**16).to_bytes(32, "little"))  # 0.05 ETH, 32-byte
        arr[1] = (3 * 10**16).to_bytes(16, "little")             # 0.03 ETH, 16-byte
        out = compute.decode_payload_values_eth(arr)
        assert out.tolist() == pytest.approx([0.05, 0.03])

    def test_value_above_uint64(self):
        # A whale MEV block (>18.4 ETH) overflows uint64; exact decode must still hold.
        big = 25 * 10**18  # 25 ETH in wei, > 2**64
        out = compute.decode_payload_values_eth(np.array([big.to_bytes(32, "little")], dtype=object))
        assert out[0] == pytest.approx(25.0)


class TestDaterange:
    def test_half_open(self):
        from datetime import date

        days = list(compute.daterange(date(2026, 1, 1), date(2026, 1, 4)))
        assert days == [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]  # end excluded

    def test_empty_when_start_equals_end(self):
        from datetime import date

        assert list(compute.daterange(date(2026, 1, 1), date(2026, 1, 1))) == []


class TestCheckRealizedSpan:
    def test_aligned_daily_snapshots_pass(self):
        # 225 epochs/day, so an N-day window of aligned snapshots spans exactly 225*N.
        for window in (7, 30, 90):
            assert compute.check_realized_span(450338 - 225 * window, 450338, window) == float(window)

    def test_within_tolerance_passes(self):
        # ~0.4 day of drift (under the 0.5d tolerance) is accepted.
        drift = int(0.4 * 225)
        assert compute.check_realized_span(450338 - 225 * 7 - drift, 450338, 7) == pytest.approx(7.4, abs=0.01)

    def test_full_day_shift_raises(self):
        with pytest.raises(RuntimeError, match="deviates"):
            compute.check_realized_span(450338 - 225 * 8, 450338, 7)  # 8-day span, 7-day nominal

    def test_collapsed_window_raises(self):
        with pytest.raises(RuntimeError, match="deviates"):
            compute.check_realized_span(450338, 450338, 7)


class TestNormalizeWindows:
    def test_dedupes_and_sorts(self):
        assert compute.normalize_windows([90, 7, 30, 7]) == [7, 30, 90]

    def test_drops_non_positive(self):
        assert compute.normalize_windows([7, 0, -5, 30]) == [7, 30]

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="at least one positive integer"):
            compute.normalize_windows([])

    def test_all_non_positive_raises(self):
        with pytest.raises(ValueError, match="at least one positive integer"):
            compute.normalize_windows([0, -1])


class TestReadWithRetry:
    def test_returns_on_first_success(self):
        calls = []
        assert compute.read_with_retry(lambda: calls.append(1) or "ok", what="x") == "ok"
        assert len(calls) == 1

    def test_retries_then_succeeds(self, monkeypatch):
        monkeypatch.setattr(compute.time, "sleep", lambda _s: None)
        state = {"n": 0}

        def thunk():
            state["n"] += 1
            if state["n"] < 3:
                raise duckdb.Error("transient")
            return "recovered"

        assert compute.read_with_retry(thunk, what="x") == "recovered"
        assert state["n"] == 3

    def test_reraises_after_exhausting_attempts(self, monkeypatch):
        monkeypatch.setattr(compute.time, "sleep", lambda _s: None)
        state = {"n": 0}

        def thunk():
            state["n"] += 1
            raise duckdb.Error("always down")

        with pytest.raises(duckdb.Error, match="always down"):
            compute.read_with_retry(thunk, what="x")
        assert state["n"] == compute.REMOTE_READ_ATTEMPTS

    def test_does_not_swallow_non_duckdb_errors(self, monkeypatch):
        monkeypatch.setattr(compute.time, "sleep", lambda _s: None)
        state = {"n": 0}

        def thunk():
            state["n"] += 1
            raise ValueError("programmer error")

        with pytest.raises(ValueError, match="programmer error"):
            compute.read_with_retry(thunk, what="x")
        assert state["n"] == 1  # not retried
