from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pandas as pd

from future_spot.arbitrage.full_market_runner import (
    DailyPairRecord,
    balanced_backtest_shards,
    run_backtests,
)
from future_spot.arbitrage.models import PairConfig


def _pair(name: str) -> PairConfig:
    return PairConfig(
        name=name,
        spot_symbol=f"S{name}",
        future_symbol=f"F{name}",
        spot_shares_per_pair=1,
        future_shares_per_pair=1,
        spot_order_qty=1,
        future_order_qty=1,
        future_pnl_multiplier=1,
        entry_threshold_pct=0.1,
        exit_threshold_pct=0.0,
        stop_loss_pct=-0.1,
    )


class InlineExecutor:
    def __init__(self) -> None:
        self.submissions = 0

    def submit(self, function, *args):
        self.submissions += 1
        future = Future()
        try:
            future.set_result(function(*args))
        except Exception as exc:  # pragma: no cover - mirrors Executor behavior
            future.set_exception(exc)
        return future


class PersistentExecutorTest(unittest.TestCase):
    def test_run_backtests_uses_caller_owned_executor(self) -> None:
        records = [
            DailyPairRecord("2026-03-02", f"2026-03-02::{name}", _pair(name), Path(f"{name}.json"))
            for name in ("a", "b")
        ]
        paths = {
            record.run_key: {"spot": Path(f"{record.pair.name}-s.npz"), "future": Path(f"{record.pair.name}-f.npz")}
            for record in records
        }
        args = SimpleNamespace(workers=2, continue_on_error=False)
        executor = InlineExecutor()

        def result(_args, record, _paths):
            return {
                "summary": pd.DataFrame({"run_key": [record.run_key]}),
                "trades": pd.DataFrame(),
                "market": pd.DataFrame(),
                "latency": pd.DataFrame(),
            }

        with patch(
            "future_spot.arbitrage.full_market_runner._run_single_pair_backtest",
            side_effect=result,
        ), patch(
            "future_spot.arbitrage.full_market_runner.ProcessPoolExecutor"
        ) as pool_class:
            completed, summary, *_ = run_backtests(
                args,
                records,
                paths,
                executor=executor,  # type: ignore[arg-type]
            )

        pool_class.assert_not_called()
        self.assertEqual(executor.submissions, 2)
        self.assertEqual(set(completed), {record.run_key for record in records})
        self.assertEqual(len(summary), 2)

    def test_balanced_shards_use_combined_leg_event_rows(self) -> None:
        records = [
            DailyPairRecord("2026-03-02", f"2026-03-02::{name}", _pair(name), Path(f"{name}.json"))
            for name in ("heavy", "medium", "small_a", "small_b")
        ]
        runnable = [
            (
                record,
                {
                    "spot": Path(f"{record.pair.name}-s.npz"),
                    "future": Path(f"{record.pair.name}-f.npz"),
                },
            )
            for record in records
        ]
        weights = {
            "heavy-s.npz": 45,
            "heavy-f.npz": 45,
            "medium-s.npz": 25,
            "medium-f.npz": 25,
            "small_a-s.npz": 10,
            "small_a-f.npz": 10,
            "small_b-s.npz": 10,
            "small_b-f.npz": 10,
        }

        shards = balanced_backtest_shards(runnable, 2, weights)
        totals = sorted(sum(item[2] for item in shard) for shard in shards)

        self.assertEqual(totals, [90, 90])


if __name__ == "__main__":
    unittest.main()
