from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd

TEST_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_ROOT.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
for _path in (TEST_ROOT, PROJECT_ROOT, WORKSPACE_ROOT, PROJECT_ROOT / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from arbitrage.full_market_runner import attach_entry_signals, parse_args, resolve_output_dir
from arbitrage.hbt_helpers import hbt_asset_audit, infer_hbt_asset_tick_size
from scripts.io_utils import write_parquet


EVENT_DTYPE = np.dtype(
    [
        ("ev", "<u8"),
        ("exch_ts", "<i8"),
        ("local_ts", "<i8"),
        ("px", "<f8"),
        ("qty", "<f8"),
    ]
)


class FastPipelineTest(unittest.TestCase):
    def test_fast_defaults_use_1325_and_sample_market(self) -> None:
        args = parse_args([])
        self.assertEqual(args.session_end, "13:25:00")
        self.assertEqual(args.build_session_end, "13:25:00")
        self.assertEqual(args.record_market_every_steps, 60)
        self.assertEqual(args.strategy_engine, "numba")
        self.assertEqual(args.spot_input_csv_template, "")
        self.assertEqual(args.data_platform_base, "/mnt/z/數據平台")
        self.assertGreaterEqual(args.workers, 1)

    def test_default_output_dir_tracks_dates_and_latency(self) -> None:
        args = parse_args(
            [
                "--start-date", "2026-06-01",
                "--end-date", "2026-06-30",
                "--order-latency-ms", "10",
                "--response-latency-ms", "10",
                "--feed-latency-offset-ms", "10",
            ]
        )
        self.assertEqual(
            resolve_output_dir(args).name,
            "hbt_daily_full_market_20260601_20260630_latency_10ms",
        )

    def test_tick_inference_and_audit_use_vectorized_min_price(self) -> None:
        data = np.zeros(3, dtype=EVENT_DTYPE)
        data["ev"] = [1, 2, 1]
        data["exch_ts"] = [1, 2, 3]
        data["local_ts"] = [2, 3, 4]
        data["px"] = [501.0, 499.5, 505.0]
        data["qty"] = 1.0
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.npz"
            np.savez_compressed(path, data=data)
            self.assertEqual(infer_hbt_asset_tick_size(path, "future", trade_date="2026-05-26"), 0.5)
            tick, summary = hbt_asset_audit(path, "future", trade_date="2026-05-26")
        self.assertEqual(tick, 0.5)
        self.assertEqual(summary["rows"], 3)
        self.assertEqual(summary["trade_events"], 1)

    def test_attach_entry_signals_is_vectorized(self) -> None:
        pair = SimpleNamespace(
            entry_threshold_pct=0.1,
            min_effective_tick_multiple=2,
            stock_min_ask_size=2,
            future_min_bid_size=1,
            allow_short_spot=False,
            stock_min_bid_size=2,
            future_min_ask_size=1,
        )
        market = pd.DataFrame(
            [
                {
                    "long_spot_short_future_pct": 0.2,
                    "long_spot_short_future_ticks": 3,
                    "spot_ask_size": 2,
                    "future_bid_size": 1,
                    "short_spot_long_future_pct": 0.0,
                    "short_spot_long_future_ticks": 0,
                    "spot_bid_size": 0,
                    "future_ask_size": 0,
                },
                {
                    "long_spot_short_future_pct": 0.0,
                    "long_spot_short_future_ticks": 0,
                    "spot_ask_size": 0,
                    "future_bid_size": 0,
                    "short_spot_long_future_pct": 0.0,
                    "short_spot_long_future_ticks": 0,
                    "spot_bid_size": 0,
                    "future_ask_size": 0,
                },
            ]
        )
        result = attach_entry_signals(market, pair)
        self.assertEqual(result["entry_signal_hit"].tolist(), [True, False])

    def test_write_parquet_preserves_nullable_epoch_nanoseconds(self) -> None:
        timestamp = 1_777_857_193_502_000_000
        frame = pd.DataFrame(
            {
                "flatten_local_timestamp": pd.Series([timestamp, None, 0.0], dtype=object),
                "status": pd.Series(["FILLED", None, "UNFILLED"], dtype=object),
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.parquet"
            write_parquet(frame, path)
            restored = pd.read_parquet(path)

        self.assertEqual(int(restored.loc[0, "flatten_local_timestamp"]), timestamp)
        self.assertTrue(pd.isna(restored.loc[1, "flatten_local_timestamp"]))
        self.assertEqual(int(restored.loc[2, "flatten_local_timestamp"]), 0)
        self.assertEqual(restored.loc[0, "status"], "FILLED")


if __name__ == "__main__":
    unittest.main()
