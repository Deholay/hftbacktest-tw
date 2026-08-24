from __future__ import annotations

import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

TEST_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_ROOT.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
for _path in (TEST_ROOT, PROJECT_ROOT, WORKSPACE_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from arbitrage.hbt_numba import (  # noqa: E402
    FUTURE_TICK_SCHEDULE_FROM_TIMESTAMP,
    SIGNAL_EXIT,
    SIGNAL_HOLD,
    SIGNAL_LONG,
    SIGNAL_SHORT,
    pricing_values_from_bbo,
    signal_from_pricing_values,
)
from arbitrage.hbt_backtest import HbtPairBacktester  # noqa: E402
from arbitrage.hbt_types import HbtPairBacktestConfig  # noqa: E402
from arbitrage.models import PairConfig, PairMarket, PairPosition, Quote, Signal  # noqa: E402
from arbitrage.strategy import PairPricer, StopLossAwareSignalEngine  # noqa: E402
from scripts.hbt_types import HbtAssetConfig  # noqa: E402
from scripts.tw_stock_hftbacktest import import_hftbacktest  # noqa: E402
from scripts.tw_stock_data_to_npz import (  # noqa: E402
    EVENT_DTYPE,
    correct_event_order,
    iter_depth_events,
)


SIGNAL_CODES = {
    Signal.HOLD: SIGNAL_HOLD,
    Signal.ENTER_LONG_SPOT_SHORT_FUTURE: SIGNAL_LONG,
    Signal.ENTER_SHORT_SPOT_LONG_FUTURE: SIGNAL_SHORT,
    Signal.EXIT: SIGNAL_EXIT,
}


def pair_config(**updates) -> PairConfig:
    values = {
        "name": "2330_test",
        "spot_symbol": "2330",
        "future_symbol": "CDF",
        "spot_shares_per_pair": 1,
        "future_shares_per_pair": 1,
        "spot_order_qty": 1000,
        "future_order_qty": 1,
        "future_pnl_multiplier": 1000,
        "entry_threshold_pct": 0.01,
        "exit_threshold_pct": 0.004,
        "stop_loss_pct": -0.01,
        "exit_tick_multiple": 1.0,
        "exit_tick_rule": "lte",
        "min_effective_tick_multiple": 0.0,
        "allow_short_spot": True,
    }
    values.update(updates)
    return PairConfig(**values)


class HbtNumbaTest(unittest.TestCase):
    def test_numba_pricing_matches_pair_pricer_across_tick_schedule(self) -> None:
        pair = pair_config()
        for timestamp, prices in (
            (1_779_300_000_000_000_000, (998.0, 999.0, 1005.0, 1006.0)),
            (1_783_300_000_000_000_000, (2498.0, 2499.0, 2501.0, 2506.0)),
        ):
            spot_bid, spot_ask, future_bid, future_ask = prices
            raw = {"timestamp": timestamp}
            market = PairMarket(
                pair=pair,
                spot=Quote(pair.spot_symbol, spot_bid, spot_ask, raw=raw),
                future=Quote(pair.future_symbol, future_bid, future_ask, raw=raw),
            )
            expected = PairPricer().price(market)
            actual = pricing_values_from_bbo(
                spot_bid,
                spot_ask,
                future_bid,
                future_ask,
                pair.spot_shares_per_pair,
                pair.future_shares_per_pair,
                0.0,
                0.0,
                FUTURE_TICK_SCHEDULE_FROM_TIMESTAMP,
                timestamp,
            )
            expected_values = (
                expected.long_spot_short_future_pct,
                expected.short_spot_long_future_pct,
                expected.mid_basis_pct,
                expected.long_spot_short_future_ticks,
                expected.short_spot_long_future_ticks,
                expected.long_spot_short_future_exit_ticks,
                expected.short_spot_long_future_exit_ticks,
            )
            for expected_value, actual_value in zip(expected_values, actual):
                self.assertAlmostEqual(actual_value, expected_value, places=12)

    def test_numba_signal_matches_stop_loss_aware_engine(self) -> None:
        cases = (
            (pair_config(), (99.0, 100.0, 102.0, 103.0), PairPosition("p")),
            (pair_config(), (100.0, 101.0, 100.0, 101.0), PairPosition("p")),
            (
                pair_config(),
                (99.0, 100.0, 102.0, 103.0),
                PairPosition("p", quantity=1, direction=Signal.ENTER_LONG_SPOT_SHORT_FUTURE, entry_basis_pct=0.02),
            ),
            (
                pair_config(stop_loss_pct=-0.005, exit_tick_multiple=-100.0),
                (90.0, 91.0, 102.0, 103.0),
                PairPosition("p", quantity=1, direction=Signal.ENTER_LONG_SPOT_SHORT_FUTURE, entry_basis_pct=0.01),
            ),
        )
        timestamp = 1_779_300_000_000_000_000
        engine = StopLossAwareSignalEngine()
        for pair, prices, position in cases:
            spot_bid, spot_ask, future_bid, future_ask = prices
            raw = {"timestamp": timestamp}
            market = PairMarket(
                pair=pair,
                spot=Quote(pair.spot_symbol, spot_bid, spot_ask, raw=raw),
                future=Quote(pair.future_symbol, future_bid, future_ask, raw=raw),
            )
            pricing = PairPricer().price(market)
            expected = engine.evaluate(pair, pricing, position)
            code = signal_from_pricing_values(
                pricing.long_spot_short_future_pct,
                pricing.short_spot_long_future_pct,
                pricing.long_spot_short_future_ticks,
                pricing.short_spot_long_future_ticks,
                pricing.long_spot_short_future_exit_ticks,
                pricing.short_spot_long_future_exit_ticks,
                pair.entry_threshold_pct,
                pair.exit_threshold_pct,
                pair.stop_loss_pct,
                pair.exit_tick_multiple,
                pair.exit_tick_rule == "gte",
                pair.min_effective_tick_multiple,
                pair.allow_short_spot,
                position.quantity,
                SIGNAL_CODES[position.direction],
                float("nan") if position.entry_basis_pct is None else position.entry_basis_pct,
            )
            self.assertEqual(code, SIGNAL_CODES[expected])

    def test_compiled_scanner_matches_python_market_rows(self) -> None:
        pair = pair_config(
            allow_short_spot=False,
            spot_tick_size=1.0,
            future_tick_size=1.0,
            min_effective_tick_multiple=2.0,
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spot_path = root / "spot.npz"
            future_path = root / "future.npz"
            self._write_hold_market(spot_path, bid=99.0, ask=100.0)
            self._write_hold_market(future_path, bid=99.0, ask=100.0)
            base = HbtPairBacktestConfig(
                pair=pair,
                spot=HbtAssetConfig(
                    symbol=pair.spot_symbol,
                    data=spot_path,
                    instrument="stock",
                    contract_size=1000.0,
                    tick_size=1.0,
                ),
                future=HbtAssetConfig(
                    symbol=pair.future_symbol,
                    data=future_path,
                    instrument="future",
                    contract_size=1000.0,
                    tick_size=1.0,
                ),
                max_steps=200,
                record_market_every_steps=10,
            )

            hbtpkg = import_hftbacktest(WORKSPACE_ROOT)
            python_backtester = HbtPairBacktester(
                replace(base, strategy_engine="python"),
                hbtpkg=hbtpkg,
            )
            python_trades, python_summary = python_backtester.run()
            numba_backtester = HbtPairBacktester(
                replace(base, strategy_engine="numba"),
                hbtpkg=hbtpkg,
            )
            numba_trades, numba_summary = numba_backtester.run()

        pd.testing.assert_frame_equal(python_trades, numba_trades)
        pd.testing.assert_frame_equal(
            python_backtester.market_frame(),
            numba_backtester.market_frame(),
        )
        for column in ("rows", "filled_pairs", "final_quantity", "final_direction"):
            self.assertEqual(python_summary.loc[0, column], numba_summary.loc[0, column])
        self.assertGreater(python_backtester.python_decisions, numba_backtester.python_decisions)
        self.assertGreater(numba_backtester.scan_calls, 0)

    @staticmethod
    def _write_hold_market(path: Path, bid: float, ask: float) -> None:
        events: list[tuple] = []
        start = 1_779_300_000_000_000_000
        row = {
            "bid_price1": bid,
            "bid_volume1": 10.0,
            "ask_price1": ask,
            "ask_volume1": 10.0,
        }
        for index in range(120):
            timestamp = start + index * 1_000_000_000
            events.extend(iter_depth_events(row, timestamp, timestamp, levels=1, volume_scale=1.0))
        data = correct_event_order(np.asarray(events, dtype=EVENT_DTYPE))
        np.savez(path, data=data)


if __name__ == "__main__":
    unittest.main()
