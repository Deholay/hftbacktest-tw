from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from future_spot.arbitrage.full_market_runner import HbtRunOutputs
from future_spot.test.backtest_pipeline import run_backtest_pipeline


class BacktestPipelineTimingTest(unittest.TestCase):
    def test_pipeline_persists_stage_timing_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = SimpleNamespace(
                output_dir=Path(tmp),
                calendar=Path("Calendar.csv"),
                start_date="2026-03-02",
                end_date="2026-03-02",
                excluded_dates=(),
                post_first_feed_wait="none",
                skip_entry_exit_by_pair=True,
            )
            outputs = HbtRunOutputs(
                records=[],
                event_paths={},
                pair_results={},
                summary=pd.DataFrame(),
                trades=pd.DataFrame(),
                market=pd.DataFrame(),
                latency=pd.DataFrame(),
                run_errors=pd.DataFrame(),
                conversion_status=pd.DataFrame(),
                settings=pd.DataFrame(),
                position_carry_status=pd.DataFrame(),
                cache_hit=True,
                daily_partitions=True,
                stage_timings=pd.DataFrame(
                    [
                        {
                            "trade_date": "2026-03-02",
                            "stage": "pair_matching",
                            "elapsed_seconds": 1.25,
                            "pair_count": 2,
                            "mode": "executed",
                        }
                    ]
                ),
            )

            with patch(
                "future_spot.test.backtest_pipeline.prepare_args",
                return_value=args,
            ), patch(
                "future_spot.test.backtest_pipeline.daily_pipeline.select_trade_dates",
                return_value=["2026-03-02"],
            ), patch(
                "future_spot.test.backtest_pipeline.daily_pipeline.build_daily_pair_records",
                return_value=([], pd.DataFrame()),
            ), patch(
                "future_spot.test.backtest_pipeline.hbt_pipeline.execute_hbt_runs",
                return_value=outputs,
            ):
                artifacts = run_backtest_pipeline(args)

            timings = artifacts.frame("stage_timings")
            self.assertIn("pair_matching", set(timings["stage"]))
            self.assertIn("pipeline_total", set(timings["stage"]))
            persisted = pd.read_csv(Path(tmp) / "stage_timings.csv")
            self.assertEqual(len(persisted), len(timings))


if __name__ == "__main__":
    unittest.main()
