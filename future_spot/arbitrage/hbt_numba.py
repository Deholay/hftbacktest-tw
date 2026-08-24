"""Numba hot loop for the default futures/spot HBT strategy.

The compiled loop intentionally stops before risk checks and order execution.
Those paths are comparatively rare and remain in Python so the existing
execution, audit rows, and custom-strategy interface stay unchanged.
"""

from __future__ import annotations

import math

from numba import njit


SIGNAL_HOLD = 0
SIGNAL_LONG = 1
SIGNAL_SHORT = 2
SIGNAL_EXIT = 3

SCAN_END_OF_DATA = 1
SCAN_SIGNAL = 2
SCAN_PERIODIC_RECORD = 3
SCAN_MAX_STEPS = 4

FUTURE_TICK_SCHEDULE_OLD = 0
FUTURE_TICK_SCHEDULE_NEW = 1
FUTURE_TICK_SCHEDULE_FROM_TIMESTAMP = 2

# 2026-07-06 00:00:00 Asia/Taipei, expressed as UTC epoch nanoseconds.
FUTURE_TICK_CHANGE_TIMESTAMP_NS = 1_783_267_200_000_000_000
EFFECTIVE_TICK_COST_RATE = 0.004


@njit(cache=True)
def tw_stock_tick_size_numba(price: float) -> float:
    if price < 0.0:
        raise ValueError("price must be non-negative")
    if price < 10.0:
        return 0.01
    if price < 50.0:
        return 0.05
    if price < 100.0:
        return 0.1
    if price < 500.0:
        return 0.5
    if price < 1000.0:
        return 1.0
    return 5.0


@njit(cache=True)
def tw_stock_future_tick_size_numba(price: float, schedule_mode: int, timestamp_ns: int) -> float:
    if price < 0.0:
        raise ValueError("price must be non-negative")
    if price < 500.0:
        return tw_stock_tick_size_numba(price)

    use_new_schedule = schedule_mode == FUTURE_TICK_SCHEDULE_NEW
    if schedule_mode == FUTURE_TICK_SCHEDULE_FROM_TIMESTAMP:
        use_new_schedule = timestamp_ns >= FUTURE_TICK_CHANGE_TIMESTAMP_NS
    if use_new_schedule:
        if price < 2500.0:
            return 1.0
        return 5.0

    if price < 1000.0:
        return 1.0
    return 5.0


@njit(cache=True)
def pricing_values_from_bbo(
    spot_bid: float,
    spot_ask: float,
    future_bid: float,
    future_ask: float,
    spot_shares_per_pair: float,
    future_shares_per_pair: float,
    configured_spot_tick: float,
    configured_future_tick: float,
    future_tick_schedule_mode: int,
    timestamp_ns: int,
):
    """Return the seven PairPricing values used by the signal engine."""
    long_spot_tick = configured_spot_tick
    if long_spot_tick <= 0.0:
        long_spot_tick = tw_stock_tick_size_numba(spot_ask)
    short_spot_tick = configured_spot_tick
    if short_spot_tick <= 0.0:
        short_spot_tick = tw_stock_tick_size_numba(spot_bid)

    long_future_tick = configured_future_tick
    if long_future_tick <= 0.0:
        long_future_tick = tw_stock_future_tick_size_numba(
            future_bid,
            future_tick_schedule_mode,
            timestamp_ns,
        )
    short_future_tick = configured_future_tick
    if short_future_tick <= 0.0:
        short_future_tick = tw_stock_future_tick_size_numba(
            future_ask,
            future_tick_schedule_mode,
            timestamp_ns,
        )

    spot_buy_notional = spot_ask * spot_shares_per_pair
    spot_sell_notional = spot_bid * spot_shares_per_pair
    future_sell_notional = future_bid * future_shares_per_pair
    future_buy_notional = future_ask * future_shares_per_pair

    long_pct = (future_sell_notional - spot_buy_notional) / spot_buy_notional
    short_pct = (future_buy_notional - spot_sell_notional) / spot_sell_notional
    spot_mid = (spot_bid + spot_ask) / 2.0
    future_mid = (future_bid + future_ask) / 2.0
    mid_basis_pct = (future_mid - spot_mid) / spot_mid
    long_ticks = (
        (future_bid - spot_ask) - (spot_ask * EFFECTIVE_TICK_COST_RATE)
    ) / (long_spot_tick + long_future_tick)
    short_ticks = (
        (spot_bid - future_ask) - (spot_bid * EFFECTIVE_TICK_COST_RATE)
    ) / (short_spot_tick + short_future_tick)
    long_exit_ticks = (future_ask - spot_bid) / (short_spot_tick + short_future_tick)
    short_exit_ticks = (spot_ask - future_bid) / (long_spot_tick + long_future_tick)
    return (
        long_pct,
        short_pct,
        mid_basis_pct,
        long_ticks,
        short_ticks,
        long_exit_ticks,
        short_exit_ticks,
    )


@njit(cache=True)
def signal_from_pricing_values(
    long_pct: float,
    short_pct: float,
    long_ticks: float,
    short_ticks: float,
    long_exit_ticks: float,
    short_exit_ticks: float,
    entry_threshold_pct: float,
    exit_threshold_pct: float,
    stop_loss_pct: float,
    exit_tick_multiple: float,
    exit_tick_rule_gte: bool,
    min_effective_tick_multiple: float,
    allow_short_spot: bool,
    position_quantity: float,
    position_direction: int,
    entry_basis_pct: float,
) -> int:
    has_position = position_quantity != 0.0
    has_entry_basis = not math.isnan(entry_basis_pct)

    if has_position and has_entry_basis:
        if position_direction == SIGNAL_LONG:
            pnl_pct = entry_basis_pct - short_pct
            if pnl_pct <= stop_loss_pct:
                return SIGNAL_EXIT
        elif position_direction == SIGNAL_SHORT:
            pnl_pct = entry_basis_pct + long_pct
            if pnl_pct <= stop_loss_pct:
                return SIGNAL_EXIT

    if has_position:
        if position_direction == SIGNAL_LONG:
            tick_exit = long_exit_ticks >= exit_tick_multiple
            if not exit_tick_rule_gte:
                tick_exit = long_exit_ticks <= exit_tick_multiple
            if tick_exit or short_pct <= exit_threshold_pct:
                return SIGNAL_EXIT
            if long_pct >= entry_threshold_pct and long_ticks > min_effective_tick_multiple:
                return SIGNAL_LONG
        elif position_direction == SIGNAL_SHORT:
            tick_exit = short_exit_ticks >= exit_tick_multiple
            if not exit_tick_rule_gte:
                tick_exit = short_exit_ticks <= exit_tick_multiple
            if tick_exit or long_pct >= -exit_threshold_pct:
                return SIGNAL_EXIT
            if (
                allow_short_spot
                and short_pct <= -entry_threshold_pct
                and short_ticks > min_effective_tick_multiple
            ):
                return SIGNAL_SHORT
        return SIGNAL_HOLD

    if long_pct >= entry_threshold_pct and long_ticks > min_effective_tick_multiple:
        return SIGNAL_LONG
    if (
        allow_short_spot
        and short_pct <= -entry_threshold_pct
        and short_ticks > min_effective_tick_multiple
    ):
        return SIGNAL_SHORT
    return SIGNAL_HOLD


# HBT's jitclass carries dynamic ctypes pointers, so Numba cannot persist this
# dispatcher to disk. It still compiles once and is reused within each worker.
@njit
def scan_until_wakeup(
    hbt,
    step_ns: int,
    start_step: int,
    remaining_steps: int,
    record_market_every_steps: int,
    spot_shares_per_pair: float,
    future_shares_per_pair: float,
    configured_spot_tick: float,
    configured_future_tick: float,
    future_tick_schedule_mode: int,
    entry_threshold_pct: float,
    exit_threshold_pct: float,
    stop_loss_pct: float,
    exit_tick_multiple: float,
    exit_tick_rule_gte: bool,
    min_effective_tick_multiple: float,
    allow_short_spot: bool,
    position_quantity: float,
    position_direction: int,
    entry_basis_pct: float,
):
    """Advance through HOLD steps until Python has meaningful work to do.

    ``remaining_steps`` uses ``-1`` for unlimited. The returned tuple is
    ``(reason, absolute_step, signal_code, timestamp, eight BBO values)``.
    The BBO snapshot preserves the final successful Python step because a
    failed end-of-data ``elapse`` may still mutate HBT's visible depth.
    """
    step = start_step
    advanced = 0
    last_timestamp = -1
    last_spot_bid = math.nan
    last_spot_ask = math.nan
    last_spot_bid_size = math.nan
    last_spot_ask_size = math.nan
    last_future_bid = math.nan
    last_future_ask = math.nan
    last_future_bid_size = math.nan
    last_future_ask_size = math.nan
    while remaining_steps < 0 or advanced < remaining_steps:
        if hbt.elapse(step_ns) != 0:
            return (
                SCAN_END_OF_DATA,
                step,
                SIGNAL_HOLD,
                last_timestamp,
                last_spot_bid,
                last_spot_ask,
                last_spot_bid_size,
                last_spot_ask_size,
                last_future_bid,
                last_future_ask,
                last_future_bid_size,
                last_future_ask_size,
            )
        step += 1
        advanced += 1

        spot_depth = hbt.depth(0)
        future_depth = hbt.depth(1)
        spot_bid = spot_depth.best_bid
        spot_ask = spot_depth.best_ask
        future_bid = future_depth.best_bid
        future_ask = future_depth.best_ask
        valid_market = (
            math.isfinite(spot_bid)
            and math.isfinite(spot_ask)
            and math.isfinite(future_bid)
            and math.isfinite(future_ask)
        )
        if not valid_market:
            continue

        last_timestamp = hbt.current_timestamp
        last_spot_bid = spot_bid
        last_spot_ask = spot_ask
        last_spot_bid_size = spot_depth.bid_qty_at_tick(spot_depth.best_bid_tick)
        last_spot_ask_size = spot_depth.ask_qty_at_tick(spot_depth.best_ask_tick)
        last_future_bid = future_bid
        last_future_ask = future_ask
        last_future_bid_size = future_depth.bid_qty_at_tick(future_depth.best_bid_tick)
        last_future_ask_size = future_depth.ask_qty_at_tick(future_depth.best_ask_tick)

        pricing = pricing_values_from_bbo(
            spot_bid,
            spot_ask,
            future_bid,
            future_ask,
            spot_shares_per_pair,
            future_shares_per_pair,
            configured_spot_tick,
            configured_future_tick,
            future_tick_schedule_mode,
            hbt.current_timestamp,
        )
        signal = signal_from_pricing_values(
            pricing[0],
            pricing[1],
            pricing[3],
            pricing[4],
            pricing[5],
            pricing[6],
            entry_threshold_pct,
            exit_threshold_pct,
            stop_loss_pct,
            exit_tick_multiple,
            exit_tick_rule_gte,
            min_effective_tick_multiple,
            allow_short_spot,
            position_quantity,
            position_direction,
            entry_basis_pct,
        )
        if signal != SIGNAL_HOLD:
            return (
                SCAN_SIGNAL,
                step,
                signal,
                last_timestamp,
                last_spot_bid,
                last_spot_ask,
                last_spot_bid_size,
                last_spot_ask_size,
                last_future_bid,
                last_future_ask,
                last_future_bid_size,
                last_future_ask_size,
            )
        if record_market_every_steps > 0 and step % record_market_every_steps == 0:
            return (
                SCAN_PERIODIC_RECORD,
                step,
                SIGNAL_HOLD,
                last_timestamp,
                last_spot_bid,
                last_spot_ask,
                last_spot_bid_size,
                last_spot_ask_size,
                last_future_bid,
                last_future_ask,
                last_future_bid_size,
                last_future_ask_size,
            )

    return (
        SCAN_MAX_STEPS,
        step,
        SIGNAL_HOLD,
        last_timestamp,
        last_spot_bid,
        last_spot_ask,
        last_spot_bid_size,
        last_spot_ask_size,
        last_future_bid,
        last_future_ask,
        last_future_bid_size,
        last_future_ask_size,
    )
