from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np

from scripts.tw_stock_data_to_npz import (
    DEPTH_CLEAR_EVENT,
    DEPTH_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EVENT_FLAG_MASK,
    TRADE_EVENT,
)

from .models import PairConfig, PairPosition, Quote, Signal
from .ticks import tw_stock_future_tick_size, tw_stock_tick_size
from .utils import STOCK_BOARD_LOT_SHARES


STOCK_ASSET_NO = 0
FUTURE_ASSET_NO = 1


def infer_hbt_asset_tick_size(
    data: Path,
    instrument: str,
    fallback: float = 1.0,
    trade_date: str | date | None = None,
) -> float:
    """Infer the smallest price-grid tick without a Python loop over events.

    Taiwan stock/future tick schedules are monotonic within a trading date, so
    the minimum positive event price determines the minimum grid used by HBT.
    Older files only contain ``data``; newer converters may also persist the
    scalar ``min_price`` metadata, which avoids decompressing the event array.
    """
    with np.load(data) as archive:
        if "min_price" in archive.files:
            min_price = float(np.asarray(archive["min_price"]).reshape(-1)[0])
        else:
            event_data = archive["data"]
            prices = event_data["px"]
            valid = np.isfinite(prices) & (prices > 0)
            if not np.any(valid):
                return fallback
            min_price = float(np.min(prices[valid]))
    if instrument == "stock":
        return tw_stock_tick_size(min_price)
    if instrument in {"future", "stock_future"}:
        return tw_stock_future_tick_size(min_price, trade_date)
    raise ValueError(f"unknown instrument: {instrument}")


def hbt_asset_audit(
    path: Path,
    instrument: str,
    trade_date: str | date | None = None,
    fallback: float = 1.0,
) -> tuple[float, dict[str, int | None]]:
    """Load an event archive once and return its tick size and audit summary."""
    with np.load(path) as archive:
        scalar_names = {
            "event_rows",
            "first_exch_ts",
            "last_exch_ts",
            "min_latency_ns",
            "max_latency_ns",
            "depth_events",
            "trade_events",
        }
        has_summary = scalar_names.issubset(archive.files)
        has_min_price = "min_price" in archive.files
        event_data = None if has_summary and has_min_price else archive["data"]
        if has_min_price:
            min_price = float(np.asarray(archive["min_price"]).reshape(-1)[0])
        else:
            assert event_data is not None
            prices = event_data["px"]
            valid = np.isfinite(prices) & (prices > 0)
            min_price = float(np.min(prices[valid])) if np.any(valid) else math.nan

        if has_summary:
            summary = {name: int(np.asarray(archive[name]).reshape(-1)[0]) for name in scalar_names}
            summary = {name: (None if value < 0 else value) for name, value in summary.items()}
            summary["rows"] = summary.pop("event_rows")
        else:
            assert event_data is not None
            event_kind_mask = np.uint64(~EVENT_FLAG_MASK & np.iinfo(np.uint64).max)
            kinds = event_data["ev"].astype(np.uint64, copy=False) & event_kind_mask
            latency = event_data["local_ts"] - event_data["exch_ts"]
            rows = len(event_data)
            summary = {
                "rows": rows,
                "first_exch_ts": int(event_data["exch_ts"][0]) if rows else None,
                "last_exch_ts": int(event_data["exch_ts"][-1]) if rows else None,
                "min_latency_ns": int(np.min(latency)) if rows else None,
                "max_latency_ns": int(np.max(latency)) if rows else None,
                "depth_events": int(
                    np.sum(np.isin(kinds, [DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT]))
                ),
                "trade_events": int(np.sum(kinds == TRADE_EVENT)),
            }

    if not math.isfinite(min_price) or min_price <= 0:
        tick_size = fallback
    elif instrument == "stock":
        tick_size = tw_stock_tick_size(min_price)
    elif instrument in {"future", "stock_future"}:
        tick_size = tw_stock_future_tick_size(min_price, trade_date)
    else:
        raise ValueError(f"unknown instrument: {instrument}")
    return tick_size, summary


def quote_from_depth(depth: Any, symbol: str, timestamp: int) -> Quote | None:
    best_bid = float(depth.best_bid)
    best_ask = float(depth.best_ask)
    if not math.isfinite(best_bid) or not math.isfinite(best_ask):
        return None
    bid_tick = int(depth.best_bid_tick)
    ask_tick = int(depth.best_ask_tick)
    return Quote(
        symbol=symbol,
        bid=best_bid,
        ask=best_ask,
        bid_size=float(depth.bid_qty_at_tick(bid_tick)),
        ask_size=float(depth.ask_qty_at_tick(ask_tick)),
        raw={"exchtime": int(timestamp), "timestamp": int(timestamp), "source": "hbt"},
    )


def hbt_order_qty(pair: PairConfig, leg: str) -> float:
    if leg == "stock":
        return pair.spot_order_qty / STOCK_BOARD_LOT_SHARES
    if leg == "future":
        return float(pair.future_order_qty)
    raise ValueError(f"unknown leg: {leg}")


def asset_no_for_leg(leg: str) -> int:
    if leg == "stock":
        return STOCK_ASSET_NO
    if leg == "future":
        return FUTURE_ASSET_NO
    raise ValueError(f"unknown leg: {leg}")


def leg_side(signal: Signal, position: PairPosition, leg: str) -> str | None:
    if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        return "buy" if leg == "stock" else "sell"
    if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        return "sell" if leg == "stock" else "buy"
    if signal == Signal.EXIT and position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        return "sell" if leg == "stock" else "buy"
    if signal == Signal.EXIT and position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        return "buy" if leg == "stock" else "sell"
    return None


def opposite_side(side: str | None) -> str | None:
    if side == "buy":
        return "sell"
    if side == "sell":
        return "buy"
    return None


def time_in_force_for_leg(pair: PairConfig, leg: str, first_leg: str) -> str:
    if leg == first_leg:
        return pair.first_leg_time_in_force
    return pair.second_leg_time_in_force


def realized_pair_pnl(
    pair: PairConfig,
    position: PairPosition,
    exit_spot_price: float | None,
    exit_future_price: float | None,
) -> float | None:
    if exit_spot_price is None or exit_future_price is None:
        return None
    if position.entry_spot_price is None or position.entry_future_price is None:
        return None
    spot_qty = pair.spot_order_qty
    future_multiplier = pair.future_pnl_multiplier * pair.future_order_qty
    commission_rate = pair.stock_commission_rate * pair.stock_commission_discount
    entry_stock_fee = position.entry_spot_price * spot_qty * commission_rate
    exit_stock_fee = exit_spot_price * spot_qty * commission_rate

    if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        spot_pnl = (exit_spot_price - position.entry_spot_price) * spot_qty
        future_pnl = (position.entry_future_price - exit_future_price) * future_multiplier
        stock_tax = exit_spot_price * spot_qty * pair.stock_transaction_tax_rate
    elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        spot_pnl = (position.entry_spot_price - exit_spot_price) * spot_qty
        future_pnl = (exit_future_price - position.entry_future_price) * future_multiplier
        stock_tax = position.entry_spot_price * spot_qty * pair.stock_transaction_tax_rate
    else:
        return None
    return spot_pnl + future_pnl - entry_stock_fee - exit_stock_fee - stock_tax
