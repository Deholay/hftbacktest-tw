from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo


TW_TZ = ZoneInfo("Asia/Taipei")
STOCK_FUTURE_TICK_CHANGE_DATE = date(2026, 7, 6)


def tw_stock_tick_size(price: float) -> float:
    if price < 0:
        raise ValueError(f"price must be non-negative: {price}")
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1.0
    return 5.0


def tw_stock_future_tick_size(price: float, trade_date: Any = None) -> float:
    if price < 0:
        raise ValueError(f"price must be non-negative: {price}")
    if price < 500:
        return tw_stock_tick_size(price)

    effective_date = coerce_trade_date(trade_date)
    if effective_date is not None and effective_date >= STOCK_FUTURE_TICK_CHANGE_DATE:
        if price < 2500:
            return 1.0
        return 5.0

    if price < 1000:
        return 1.0
    return 5.0


def pair_leg_tick_size(
    pair: Any,
    leg: str,
    price: float,
    raw: dict[str, Any] | None = None,
    trade_date: Any = None,
) -> float:
    if leg == "stock":
        configured = getattr(pair, "spot_tick_size", None)
        return configured or tw_stock_tick_size(price)
    if leg == "future":
        configured = getattr(pair, "future_tick_size", None)
        return configured or tw_stock_future_tick_size(
            price,
            trade_date if trade_date is not None else trade_date_from_raw(raw),
        )
    raise ValueError(f"unknown leg: {leg}")


def tick_size_for_prices(
    prices: Any,
    instrument: str,
    trade_date: Any = None,
    fallback: float = 1.0,
) -> float:
    ticks: list[float] = []
    for value in prices:
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(price) or price <= 0:
            continue
        if instrument == "stock":
            ticks.append(tw_stock_tick_size(price))
        elif instrument in {"future", "stock_future"}:
            ticks.append(tw_stock_future_tick_size(price, trade_date))
        else:
            raise ValueError(f"unknown instrument: {instrument}")
    return min(ticks) if ticks else fallback


def trade_date_from_raw(raw: dict[str, Any] | None) -> date | None:
    if not raw:
        return None
    for key in (
        "exchtime_tw",
        "timestamp_tw",
        "exchtime",
        "timestamp",
        "time",
        "date",
        "datetime",
        "latency_timestamp",
    ):
        parsed = coerce_trade_date(raw.get(key))
        if parsed is not None:
            return parsed

    last_trade = raw.get("lastTrade")
    if isinstance(last_trade, dict):
        for key in ("time", "timestamp", "date", "datetime"):
            parsed = coerce_trade_date(last_trade.get(key))
            if parsed is not None:
                return parsed
    return None


def coerce_trade_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.date()
        return value.astimezone(TW_TZ).date()
    if isinstance(value, date):
        return value

    to_pydatetime = getattr(value, "to_pydatetime", None)
    if callable(to_pydatetime):
        return coerce_trade_date(to_pydatetime())

    if isinstance(value, (int, float)):
        return _numeric_timestamp_to_date(float(value))

    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        try:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        except ValueError:
            return None
    normalized = text.replace("/", "-")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return date.fromisoformat(normalized[:10])
        except ValueError:
            return None
    return coerce_trade_date(parsed)


def _numeric_timestamp_to_date(value: float) -> date | None:
    if not math.isfinite(value):
        return None
    abs_value = abs(value)
    if abs_value >= 10_000_000_000_000:
        seconds = value / 1_000_000_000
    elif abs_value >= 10_000_000_000:
        seconds = value / 1_000
    else:
        seconds = value
    try:
        return datetime.fromtimestamp(seconds, tz=TW_TZ).date()
    except (OverflowError, OSError, ValueError):
        return None
