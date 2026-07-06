from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np

from .models import PairConfig, PairPosition, Quote, Signal
from .ticks import tick_size_for_prices, trade_date_from_raw, tw_stock_future_tick_size
from .utils import STOCK_BOARD_LOT_SHARES


STOCK_ASSET_NO = 0
FUTURE_ASSET_NO = 1


def infer_hbt_asset_tick_size(data: Path, instrument: str, fallback: float = 1.0) -> float:
    event_data = np.load(data)["data"]
    prices = event_data["px"][np.isfinite(event_data["px"]) & (event_data["px"] > 0)]
    if instrument not in {"future", "stock_future"}:
        return tick_size_for_prices(prices, instrument, fallback=fallback)

    ticks: list[float] = []
    for row in event_data:
        price = float(row["px"])
        if not math.isfinite(price) or price <= 0:
            continue
        trade_date = trade_date_from_raw({"exchtime": int(row["exch_ts"])})
        ticks.append(tw_stock_future_tick_size(price, trade_date))
    return min(ticks) if ticks else fallback


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
