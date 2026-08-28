from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from future_spot.arbitrage.full_market_runner import (
    DailyPairRecord,
    execute_hbt_runs,
    run_backtests_with_position_carry,
)
from future_spot.arbitrage.models import PairConfig, Signal


def _record(trade_date: str) -> DailyPairRecord:
    pair = PairConfig(
        name="2330_CDF",
        spot_symbol="2330",
        future_symbol="CDF",
        spot_shares_per_pair=1,
        future_shares_per_pair=1,
        spot_order_qty=1,
        future_order_qty=1,
        future_pnl_multiplier=1,
        entry_threshold_pct=0.1,
        exit_threshold_pct=0.0,
        stop_loss_pct=-0.1,
    )
    return DailyPairRecord(
        trade_date,
        f"{trade_date}::{pair.name}",
        pair,
        Path(f"{trade_date}.json"),
    )


def _daily_result(record: DailyPairRecord):
    summary = pd.DataFrame(
        {
            "run_key": [record.run_key],
            "trade_date": [record.trade_date],
            "final_quantity": [0],
            "final_direction": [Signal.HOLD.value],
        }
    )
    result = {
        "summary": summary,
        "trades": pd.DataFrame(),
        "market": pd.DataFrame(),
        "latency": pd.DataFrame(),
    }
    return ({record.run_key: result}, summary, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())


class DailyResultPipelineTest(unittest.TestCase):
    def test_summary_mode_persists_dates_releases_details_and_resumes(self) -> None:
        records = [_record("2026-03-02"), _record("2026-03-03")]
        with tempfile.TemporaryDirectory() as tmp:
            args = SimpleNamespace(
                output_dir=Path(tmp),
                workers=1,
                continue_on_error=False,
                report_mode="summary",
                rebuild_hbt_results=False,
                skip_entry_exit_by_pair=True,
                step_ms=1000.0,
                calendar=Path("Calendar.csv"),
                carry_positions=True,
                excluded_run_keys=(),
            )

            def event_data(_args, date_records):
                return (
                    {
                        record.run_key: {"spot": Path("spot.npz"), "future": Path("future.npz")}
                        for record in date_records
                    },
                    pd.DataFrame({"trade_date": [date_records[0].trade_date]}),
                )

            def backtests(_args, date_records, _paths, *, executor=None):
                self.assertIsNone(executor)
                return _daily_result(date_records[0])

            common_patches = (
                patch("future_spot.arbitrage.full_market_runner.load_calendar_trade_dates", return_value=[]),
                patch("future_spot.arbitrage.full_market_runner.build_event_data", side_effect=event_data),
                patch(
                    "future_spot.arbitrage.full_market_runner.hbt_settings_frame",
                    side_effect=lambda _args, date_records, _paths: pd.DataFrame(
                        {"trade_date": [date_records[0].trade_date]}
                    ),
                ),
                patch(
                    "future_spot.arbitrage.full_market_runner.hbt_manifest_payload",
                    side_effect=lambda _args, date_records: {"trade_date": date_records[0].trade_date},
                ),
            )
            with common_patches[0], common_patches[1], common_patches[2], common_patches[3], patch(
                "future_spot.arbitrage.full_market_runner.run_backtests",
                side_effect=backtests,
            ) as run_mock:
                first = run_backtests_with_position_carry(
                    args,
                    records,
                    ["2026-03-02", "2026-03-03"],
                )

            self.assertEqual(run_mock.call_count, 2)
            self.assertTrue(first.daily_partitions)
            self.assertFalse(first.pair_results)
            self.assertTrue(first.trades.empty)
            self.assertTrue(first.market.empty)
            self.assertEqual(len(first.summary), 2)
            self.assertEqual(len(pd.read_csv(Path(tmp) / "summary_all_daily_pairs.csv")), 2)
            self.assertTrue(
                {
                    "daily_identity_validation",
                    "event_data",
                    "event_audit",
                    "pair_matching",
                    "carry_and_entry_exit",
                    "daily_result_publish",
                    "date_total",
                    "compatibility_csv_stream",
                }.issubset(set(first.stage_timings["stage"]))
            )
            self.assertTrue(first.stage_timings["elapsed_seconds"].ge(0).all())

            with common_patches[0], common_patches[1], common_patches[2], common_patches[3], patch(
                "future_spot.arbitrage.full_market_runner.run_backtests",
                side_effect=AssertionError("verified dates should resume without execution"),
            ):
                resumed = execute_hbt_runs(
                    args,
                    records,
                    ["2026-03-02", "2026-03-03"],
                )
            self.assertEqual(len(resumed.summary), 2)
            self.assertEqual(resumed.daily_dates_reused, 2)
            self.assertEqual(resumed.daily_dates_executed, 0)
            self.assertIn("daily_resume_load", set(resumed.stage_timings["stage"]))
            self.assertNotIn("pair_matching", set(resumed.stage_timings["stage"]))


if __name__ == "__main__":
    unittest.main()
