from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

TEST_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_ROOT.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
for _path in (TEST_ROOT, PROJECT_ROOT, WORKSPACE_ROOT, PROJECT_ROOT / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from arbitrage.full_market_runner import (
    attach_entry_signals,
    build_pair_hbt_config,
    leg_latency_ms,
    filter_excluded_run_records,
    parse_args,
    prepare_future_events,
    resolve_output_dir,
    select_trade_dates,
)
from arbitrage.hbt_helpers import hbt_asset_audit, infer_hbt_asset_tick_size
from arbitrage.models import PairConfig
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
        self.assertTrue(args.low_memory_reports)
        self.assertEqual(args.report_mode, "summary")
        self.assertIsNone(args.full_report_max_rows)
        self.assertEqual(args.report_chunk_rows, 25_000)
        self.assertGreaterEqual(args.workers, 1)
        self.assertEqual(len(args.excluded_dates), 7)
        self.assertEqual(len(args.excluded_run_keys), 8)

    def test_full_report_requires_explicit_positive_row_budget(self) -> None:
        with self.assertRaises(SystemExit):
            parse_args(["--report-mode", "full"])
        args = parse_args(
            ["--report-mode", "full", "--full-report-max-rows", "100000"]
        )
        self.assertEqual(args.full_report_max_rows, 100_000)

    def test_run_key_exclusion_removes_exact_record(self) -> None:
        records = [
            SimpleNamespace(run_key="2026-04-15::1802_KUFD6"),
            SimpleNamespace(run_key="2026-04-15::2330_TXFD6"),
        ]
        filtered = filter_excluded_run_records(
            records,
            excluded_run_keys=["2026-04-15::1802_KUFD6"],
        )
        self.assertEqual([record.run_key for record in filtered], ["2026-04-15::2330_TXFD6"])

    def test_trade_date_selection_excludes_known_bad_dates(self) -> None:
        calendar = pd.DataFrame({"trade_dates": ["2026-04-22", "2026-04-23", "2026-04-24"]})
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "Calendar.csv"
            calendar.to_csv(path, index=False)
            selected = select_trade_dates(
                path,
                "2026-04-22",
                "2026-04-24",
                excluded_dates=["2026-04-23"],
            )
        self.assertEqual(selected, ["2026-04-22", "2026-04-24"])

    def test_future_event_preparation_batches_missing_symbols_by_date(self) -> None:
        records = [
            SimpleNamespace(trade_date="2026-03-02", pair=SimpleNamespace(future_symbol="CAFC6")),
            SimpleNamespace(trade_date="2026-03-02", pair=SimpleNamespace(future_symbol="CBFC6")),
        ]
        args = SimpleNamespace(
            rebuild_event_data=False,
            no_convert_missing_event_data=False,
            session_start="09:00:00",
            session_end="13:25:00",
            event_futures_parquet_dir=Path("/mock/futures"),
            conversion_qa_sample_rows=1000,
            npz_compression="compressed",
        )
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def event_path(_args, symbol, source_kind, trade_date):
                self.assertEqual(source_kind, "stock_future")
                return root / f"{symbol}_{trade_date}.npz"

            existing = event_path(args, "CAFC6", "stock_future", "2026-03-02")
            existing.touch()

            def batch_convert(**kwargs):
                calls.append(kwargs)
                return dict(kwargs["output_by_symbol"]), {}

            with patch(
                "arbitrage.full_market_runner.expected_event_path",
                side_effect=event_path,
            ), patch(
                "arbitrage.full_market_runner.convert_tw_stock_future_batch_to_npz",
                side_effect=batch_convert,
            ):
                results = prepare_future_events(args, records)

        self.assertEqual(len(calls), 1)
        self.assertEqual(list(calls[0]["symbols"]), ["CBFC6"])
        self.assertEqual(results[("2026-03-02", "CAFC6")].status, "existing")
        self.assertEqual(results[("2026-03-02", "CBFC6")].status, "generated")

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

    def test_leg_latency_uses_common_values_as_fallback(self) -> None:
        args = parse_args(
            [
                "--order-latency-ms", "2",
                "--response-latency-ms", "3",
                "--feed-latency-offset-ms", "4",
            ]
        )
        self.assertEqual(leg_latency_ms(args, "spot"), (2.0, 3.0, 4.0))
        self.assertEqual(leg_latency_ms(args, "future"), (2.0, 3.0, 4.0))

    def test_leg_latency_overrides_and_output_dir_are_separate(self) -> None:
        args = parse_args(
            [
                "--start-date", "2026-01-01",
                "--end-date", "2026-07-31",
                "--future-order-latency-ms", "1",
                "--future-response-latency-ms", "1",
                "--future-feed-latency-offset-ms", "0",
                "--spot-order-latency-ms", "1",
                "--spot-response-latency-ms", "35",
                "--spot-feed-latency-offset-ms", "0",
            ]
        )
        self.assertEqual(leg_latency_ms(args, "future"), (1.0, 1.0, 0.0))
        self.assertEqual(leg_latency_ms(args, "spot"), (1.0, 35.0, 0.0))
        self.assertEqual(
            resolve_output_dir(args).name,
            "hbt_daily_full_market_20260101_20260731_"
            "future_order_1ms_response_1ms_feed_0ms_"
            "spot_order_1ms_response_35ms_feed_0ms",
        )

        pair = PairConfig(
            name="2330_TXF",
            spot_symbol="2330",
            future_symbol="TXF",
            spot_shares_per_pair=1000,
            future_shares_per_pair=1000,
            spot_order_qty=1,
            future_order_qty=1,
            future_pnl_multiplier=1000,
            entry_threshold_pct=0.1,
            exit_threshold_pct=0.0,
            stop_loss_pct=1.0,
            spot_tick_size=1.0,
            future_tick_size=1.0,
        )
        config = build_pair_hbt_config(
            args,
            pair,
            {"spot": Path("spot.npz"), "future": Path("future.npz")},
        )
        self.assertEqual(config.future.order_entry_latency_ns, 1_000_000)
        self.assertEqual(config.future.order_response_latency_ns, 1_000_000)
        self.assertEqual(config.future.feed_latency_offset_ns, 0)
        self.assertEqual(config.spot.order_entry_latency_ns, 1_000_000)
        self.assertEqual(config.spot.order_response_latency_ns, 35_000_000)
        self.assertEqual(config.spot.feed_latency_offset_ns, 0)

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
