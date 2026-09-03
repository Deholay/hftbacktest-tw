from __future__ import annotations

from typing import Any

from .models import PairConfig, PairMarket, PairPosition, Signal


def base_row(
    *,
    hbt: Any,
    step: int,
    pair: PairConfig,
    signal: Signal,
    status: str,
    market: PairMarket,
    pricing: Any,
    position: PairPosition,
    resolved_tick_sizes: dict[str, float],
    failure_reason: str | None,
) -> dict[str, Any]:
    return {
        "timestamp": int(hbt.current_timestamp),
        "step": step,
        "pair_name": pair.name,
        "spot_symbol": pair.spot_symbol,
        "future_symbol": pair.future_symbol,
        "signal": signal.value,
        "status": status,
        "failure_reason": failure_reason,
        "spot_bid": market.spot.bid,
        "spot_ask": market.spot.ask,
        "spot_bid_size": market.spot.bid_size,
        "spot_ask_size": market.spot.ask_size,
        "future_bid": market.future.bid,
        "future_ask": market.future.ask,
        "future_bid_size": market.future.bid_size,
        "future_ask_size": market.future.ask_size,
        "long_spot_short_future_pct": pricing.long_spot_short_future_pct,
        "short_spot_long_future_pct": pricing.short_spot_long_future_pct,
        "mid_basis_pct": pricing.mid_basis_pct,
        "long_spot_short_future_ticks": pricing.long_spot_short_future_ticks,
        "short_spot_long_future_ticks": pricing.short_spot_long_future_ticks,
        "pricing_spot_tick_size": pricing.spot_tick_size,
        "pricing_future_tick_size": pricing.future_tick_size,
        "position_quantity": position.quantity,
        "position_direction": position.direction.value,
        **resolved_tick_sizes,
    }
