"""Cross-day position snapshots for futures/spot HBT runs."""

from __future__ import annotations

import calendar
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .models import InitialPositionConfig, PairConfig, Signal


MONTH_CODES = {code: index for index, code in enumerate("ABCDEFGHIJKL", start=1)}
PositionKey = tuple[str, str]


@dataclass(frozen=True)
class PositionSnapshot:
    """Everything needed to restore one matched pair on the next trade day."""

    source_trade_date: str
    pair: PairConfig
    config_path: Path
    quantity: int
    direction: Signal
    entry_basis_pct: float | None
    entry_spot_price: float | None
    entry_future_price: float | None

    @property
    def key(self) -> PositionKey:
        return position_key(self.pair.spot_symbol, self.pair.future_symbol)

    def restored_pair(self, pair: PairConfig | None = None) -> PairConfig:
        target = pair or self.pair
        return replace(
            target,
            initial_position=InitialPositionConfig(
                quantity=self.quantity,
                direction=self.direction,
                entry_basis_pct=self.entry_basis_pct,
                entry_spot_price=self.entry_spot_price,
                entry_future_price=self.entry_future_price,
            ),
        )


def position_key(spot_symbol: Any, future_symbol: Any) -> PositionKey:
    return str(spot_symbol), str(future_symbol)


def snapshot_from_summary_row(row: Any, record: Any) -> PositionSnapshot | None:
    """Build a carry snapshot from one HBT summary row and its run record."""

    quantity_value = pd.to_numeric(getattr(row, "final_quantity", 0), errors="coerce")
    if pd.isna(quantity_value) or float(quantity_value) <= 0:
        return None
    quantity_float = float(quantity_value)
    quantity = int(round(quantity_float))
    if abs(quantity_float - quantity) > 1e-9:
        raise ValueError(f"non-integral final_quantity for {record.run_key}: {quantity_float}")

    direction = Signal(str(getattr(row, "final_direction")))
    if direction not in {
        Signal.ENTER_LONG_SPOT_SHORT_FUTURE,
        Signal.ENTER_SHORT_SPOT_LONG_FUTURE,
    }:
        raise ValueError(f"open quantity has invalid direction for {record.run_key}: {direction.value}")

    entry_spot = _optional_positive_float(getattr(row, "final_entry_spot_price", None))
    entry_future = _optional_positive_float(getattr(row, "final_entry_future_price", None))
    if entry_spot is None or entry_future is None:
        raise ValueError(
            f"open position summary is missing final entry prices for {record.run_key}; "
            "rerun HBT with the current implementation"
        )
    entry_basis = _optional_float(getattr(row, "final_entry_basis_pct", None))
    if entry_basis is None:
        if direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            entry_basis = (entry_future - entry_spot) / entry_spot
        else:
            entry_basis = (entry_spot - entry_future) / entry_spot

    return PositionSnapshot(
        source_trade_date=str(record.trade_date),
        pair=record.pair,
        config_path=Path(record.config_path),
        quantity=quantity,
        direction=direction,
        entry_basis_pct=entry_basis,
        entry_spot_price=entry_spot,
        entry_future_price=entry_future,
    )


def futures_contract_expiry_date(
    future_symbol: str,
    reference_trade_date: str,
    calendar_trade_dates: Iterable[str] = (),
) -> str | None:
    """Return the contract's final trade date (third Wednesday, holiday-adjusted)."""

    text = str(future_symbol).strip().upper()
    if len(text) < 5:
        return None
    month = MONTH_CODES.get(text[-2])
    if month is None or not text[-1].isdigit():
        return None

    reference = pd.Timestamp(reference_trade_date)
    year_digit = int(text[-1])
    candidates = [year for year in range(reference.year - 1, reference.year + 11) if year % 10 == year_digit]
    year = min(
        candidates,
        key=lambda value: abs((value - reference.year) * 12 + month - reference.month),
    )

    month_calendar = calendar.monthcalendar(year, month)
    wednesdays = [week[calendar.WEDNESDAY] for week in month_calendar if week[calendar.WEDNESDAY]]
    nominal = date(year, month, wednesdays[2])
    available = sorted(
        pd.Timestamp(value).date()
        for value in calendar_trade_dates
        if pd.Timestamp(value).year == year
        and pd.Timestamp(value).month == month
        and pd.Timestamp(value).date() <= nominal
    )
    return (available[-1] if available else nominal).isoformat()


def _optional_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    return None if pd.isna(number) else float(number)


def _optional_positive_float(value: Any) -> float | None:
    number = _optional_float(value)
    return number if number is not None and number > 0 else None
