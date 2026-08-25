from __future__ import annotations

from pathlib import Path
import unittest

import pandas as pd

from future_spot.arbitrage.full_market_runner import (
    DailyPairRecord,
    advance_position_carry,
    augment_records_with_position_carry,
)
from future_spot.arbitrage.models import PairConfig, Signal
from future_spot.arbitrage.position_carry import futures_contract_expiry_date


def _pair(future_symbol: str = "CAFG6", *, entry_threshold_pct: float = 0.01) -> PairConfig:
    return PairConfig(
        name=f"1303_{future_symbol}",
        spot_symbol="1303",
        future_symbol=future_symbol,
        spot_shares_per_pair=1,
        future_shares_per_pair=1,
        spot_order_qty=2_000,
        future_order_qty=1,
        future_pnl_multiplier=2_000,
        entry_threshold_pct=entry_threshold_pct,
        exit_threshold_pct=0.004,
        stop_loss_pct=-0.2,
    )


def _record(trade_date: str, pair: PairConfig) -> DailyPairRecord:
    return DailyPairRecord(
        trade_date=trade_date,
        run_key=f"{trade_date}::{pair.name}",
        pair=pair,
        config_path=Path(f"/tmp/{trade_date}.json"),
    )


def _open_summary(record: DailyPairRecord, quantity: int = 2) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "run_key": record.run_key,
                "final_quantity": quantity,
                "final_direction": Signal.ENTER_LONG_SPOT_SHORT_FUTURE.value,
                "final_entry_basis_pct": 0.02,
                "final_entry_spot_price": 100.0,
                "final_entry_future_price": 102.0,
            }
        ]
    )


class PositionCarryTest(unittest.TestCase):
    def test_held_pair_is_added_to_next_day_universe(self) -> None:
        day_one = _record("2026-06-01", _pair())
        carry, audit, errors = advance_position_carry({}, [day_one], _open_summary(day_one), [])

        day_two_records = augment_records_with_position_carry([], carry, "2026-06-02")

        self.assertTrue(errors.empty)
        self.assertEqual(audit.iloc[0]["status"], "carried_forward")
        self.assertEqual(len(day_two_records), 1)
        restored = day_two_records[0]
        self.assertEqual(restored.universe_source, "carried_position")
        self.assertEqual(restored.pair.initial_position.quantity, 2)
        self.assertEqual(
            restored.pair.initial_position.direction,
            Signal.ENTER_LONG_SPOT_SHORT_FUTURE,
        )
        self.assertEqual(restored.pair.initial_position.entry_spot_price, 100.0)

    def test_selected_pair_uses_current_settings_and_carried_position(self) -> None:
        day_one = _record("2026-06-01", _pair(entry_threshold_pct=0.01))
        carry, _, _ = advance_position_carry({}, [day_one], _open_summary(day_one), [])
        current = _record("2026-06-02", _pair(entry_threshold_pct=0.02))

        restored = augment_records_with_position_carry([current], carry, "2026-06-02")[0]

        self.assertEqual(restored.universe_source, "selected+carried")
        self.assertEqual(restored.pair.entry_threshold_pct, 0.02)
        self.assertEqual(restored.pair.initial_position.quantity, 2)

    def test_expiry_residual_is_not_rolled(self) -> None:
        expiry = futures_contract_expiry_date(
            "CAFA6",
            "2026-01-21",
            ["2026-01-19", "2026-01-20", "2026-01-21"],
        )
        record = _record("2026-01-21", _pair("CAFA6"))

        carry, audit, errors = advance_position_carry(
            {},
            [record],
            _open_summary(record),
            ["2026-01-19", "2026-01-20", "2026-01-21"],
        )

        self.assertEqual(expiry, "2026-01-21")
        self.assertFalse(carry)
        self.assertEqual(audit.iloc[0]["status"], "expiry_position_remaining")
        self.assertEqual(len(errors), 1)


if __name__ == "__main__":
    unittest.main()
