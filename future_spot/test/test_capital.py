from __future__ import annotations

import pandas as pd
import unittest

from future_spot.arbitrage.capital import (
    CapitalAllocationConfig,
    build_capital_constraint_outputs,
)


def _trade(
    *,
    timestamp: int,
    signal: str,
    spot_price: float = 100.0,
    future_price: float = 102.0,
) -> dict[str, object]:
    return {
        "trade_date": "2026-01-02",
        "run_key": "2026-01-02::2330_CDF",
        "pair_name": "2330_CDF",
        "spot_symbol": "2330",
        "future_symbol": "CDF",
        "signal": signal,
        "timestamp": timestamp,
        "completion_timestamp": timestamp,
        "spot_exec_price": spot_price,
        "future_exec_price": future_price,
        "spot_order_qty": 2_000,
        "future_order_qty": 1,
        "future_pnl_multiplier": 2_000,
        "stock_commission_rate": 0.001425,
        "stock_commission_discount": 0.28,
        "stock_transaction_tax_rate": 0.003,
    }


class CapitalAllocationTest(unittest.TestCase):
    def test_capital_allocation_uses_one_to_two_cash_split(self) -> None:
        config = CapitalAllocationConfig(
            total_capital=50_000_000,
            futures_margin_rate=0.20,
            spot_equity_rate=0.40,
        )

        self.assertAlmostEqual(config.futures_capital_limit, 16_666_666.6667, places=3)
        self.assertAlmostEqual(config.spot_capital_limit, 33_333_333.3333, places=3)
        self.assertAlmostEqual(config.matched_notional_limit, 83_333_333.3333, places=3)

    def test_replay_rejects_entry_over_shared_cap_and_releases_on_exit(self) -> None:
        trades = pd.DataFrame(
            [
                _trade(timestamp=1, signal="ENTER_LONG_SPOT_SHORT_FUTURE"),
                _trade(timestamp=2, signal="ENTER_LONG_SPOT_SHORT_FUTURE"),
                _trade(timestamp=3, signal="EXIT", spot_price=101.0, future_price=101.0),
            ]
        )
        config = CapitalAllocationConfig(
            total_capital=130_000,
            futures_margin_rate=0.20,
            spot_equity_rate=0.40,
        )

        outputs = build_capital_constraint_outputs(trades, config)
        events = outputs["capital_constraint_events"]
        summary = outputs["capital_constraint_summary"].iloc[0]

        self.assertEqual(
            events["capital_action"].tolist(),
            ["accepted_entry", "rejected_entry", "accepted_exit"],
        )
        self.assertEqual(summary["accepted_entries"], 1)
        self.assertEqual(summary["rejected_entries"], 1)
        self.assertEqual(summary["accepted_exits"], 1)
        self.assertAlmostEqual(events.iloc[-1]["total_capital_used"], 0.0)
        self.assertGreater(summary["capital_filtered_realized_pnl"], 0)

    def test_replay_resets_independent_hbt_days(self) -> None:
        first = _trade(timestamp=1, signal="ENTER_LONG_SPOT_SHORT_FUTURE")
        second = _trade(timestamp=2, signal="ENTER_LONG_SPOT_SHORT_FUTURE")
        second["trade_date"] = "2026-01-03"
        second["run_key"] = "2026-01-03::2330_CDF"

        outputs = build_capital_constraint_outputs(pd.DataFrame([first, second]))
        daily = outputs["daily_capital_constraint"]

        self.assertEqual(daily["accepted_entries"].tolist(), [1, 1])
        self.assertEqual(daily["ending_open_lots"].tolist(), [1, 1])

    def test_no_leverage_charges_full_capital_to_both_legs(self) -> None:
        config = CapitalAllocationConfig(total_capital=50_000_000, leverage=False)

        self.assertEqual(config.effective_futures_margin_rate, 1.0)
        self.assertEqual(config.effective_spot_equity_rate, 1.0)
        self.assertEqual(config.futures_capital_limit, 25_000_000)
        self.assertEqual(config.spot_capital_limit, 25_000_000)
        self.assertEqual(config.matched_notional_limit, 25_000_000)

    def test_continuous_replay_matches_next_day_exit(self) -> None:
        entry = _trade(timestamp=1, signal="ENTER_LONG_SPOT_SHORT_FUTURE")
        exit_trade = _trade(timestamp=2, signal="EXIT", spot_price=101.0, future_price=101.0)
        exit_trade["trade_date"] = "2026-01-03"
        exit_trade["run_key"] = "2026-01-03::2330_CDF"

        outputs = build_capital_constraint_outputs(
            pd.DataFrame([entry, exit_trade]),
            CapitalAllocationConfig(carry_positions=True),
        )
        events = outputs["capital_constraint_events"]
        daily = outputs["daily_capital_constraint"]

        self.assertEqual(events["capital_action"].tolist(), ["accepted_entry", "accepted_exit"])
        self.assertEqual(daily["ending_open_lots"].tolist(), [1, 0])
        self.assertEqual(outputs["capital_constraint_summary"].iloc[0]["replay_scope"], "continuous_position_candidate_replay")


if __name__ == "__main__":
    unittest.main()
