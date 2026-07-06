"""Project-wide HftBacktest helper functions."""

from __future__ import annotations

from typing import Any

from .hbt_types import HbtAssetConfig, HbtLegFill


def apply_queue_model(asset: Any, config: HbtAssetConfig):
    if config.queue_model == "risk_adverse":
        return asset.risk_adverse_queue_model()
    if config.queue_model == "log_prob":
        return asset.log_prob_queue_model()
    if config.queue_model == "log_prob2":
        return asset.log_prob_queue_model2()
    if config.queue_model == "power_prob":
        return asset.power_prob_queue_model(config.queue_model_param)
    if config.queue_model == "power_prob2":
        return asset.power_prob_queue_model2(config.queue_model_param)
    if config.queue_model == "power_prob3":
        return asset.power_prob_queue_model3(config.queue_model_param)
    raise ValueError(f"unknown queue_model: {config.queue_model}")


def get_order(hbt: Any, asset_no: int, order_id: int | None):
    if order_id is None:
        return None
    try:
        return hbt.orders(asset_no).get(order_id)
    except Exception:
        return None


def order_is_active(order: Any, hbtpkg: Any) -> bool:
    return order is not None and int(order.status) == hbtpkg.NEW and float(order.leaves_qty) > 0


def round_price_to_tick(price: float, tick_size: float) -> float:
    return round(price / tick_size) * tick_size


def hbt_time_in_force(hbtpkg: Any, value: str) -> int:
    attr = getattr(hbtpkg, value.upper(), None)
    if attr is not None:
        return attr
    return hbtpkg.GTC


def hbt_feed_latency(hbt: Any, asset_no: int) -> tuple[int, int] | None:
    latency = hbt.feed_latency(asset_no)
    if latency is None:
        return None
    return int(latency[0]), int(latency[1])


def hbt_order_latency(hbt: Any, asset_no: int) -> tuple[int | None, int | None, int | None]:
    latency = hbt.order_latency(asset_no)
    if latency is None:
        return None, None, None
    return int(latency[0]), int(latency[1]), int(latency[2])


def feed_exch_ts(feed_latency: tuple[int, int] | None) -> int | None:
    return None if feed_latency is None else feed_latency[0]


def feed_local_ts(feed_latency: tuple[int, int] | None) -> int | None:
    return None if feed_latency is None else feed_latency[1]


def feed_latency_ns(feed_latency: tuple[int, int] | None) -> int | None:
    if feed_latency is None:
        return None
    return feed_latency[1] - feed_latency[0]


def feed_refreshed(before: tuple[int, int] | None, after: tuple[int, int] | None) -> bool:
    if after is None:
        return False
    if before is None:
        return True
    return after[1] > before[1]


def latency_event_local_ts(hbt: Any, fill: HbtLegFill | None, local_ts: int | None) -> int:
    if local_ts is not None:
        return int(local_ts)
    if fill is not None and fill.order_req_local_ts is not None:
        return int(fill.order_req_local_ts)
    return int(hbt.current_timestamp)


def fill_columns(prefix: str, fill: HbtLegFill | None) -> dict[str, Any]:
    if fill is None:
        return {
            f"{prefix}_leg": None,
            f"{prefix}_side": None,
            f"{prefix}_order_id": None,
            f"{prefix}_requested_price": None,
            f"{prefix}_requested_qty": None,
            f"{prefix}_response": None,
            f"{prefix}_status": None,
            f"{prefix}_filled": False,
            f"{prefix}_exec_price": None,
            f"{prefix}_exec_qty": None,
            f"{prefix}_local_timestamp": None,
            f"{prefix}_exch_timestamp": None,
            f"{prefix}_order_req_local_ts": None,
            f"{prefix}_order_exch_ts": None,
            f"{prefix}_order_resp_local_ts": None,
            f"{prefix}_order_entry_latency_ns": None,
            f"{prefix}_order_response_latency_ns": None,
        }
    order_entry_latency_ns = None
    order_response_latency_ns = None
    if fill.order_req_local_ts is not None and fill.order_exch_ts is not None:
        order_entry_latency_ns = fill.order_exch_ts - fill.order_req_local_ts
    if fill.order_resp_local_ts is not None and fill.order_exch_ts is not None:
        order_response_latency_ns = fill.order_resp_local_ts - fill.order_exch_ts
    return {
        f"{prefix}_leg": fill.leg,
        f"{prefix}_side": fill.side,
        f"{prefix}_order_id": fill.order_id,
        f"{prefix}_requested_price": fill.requested_price,
        f"{prefix}_requested_qty": fill.requested_qty,
        f"{prefix}_response": fill.response,
        f"{prefix}_status": fill.status,
        f"{prefix}_filled": fill.filled,
        f"{prefix}_exec_price": fill.exec_price,
        f"{prefix}_exec_qty": fill.exec_qty,
        f"{prefix}_local_timestamp": fill.local_timestamp,
        f"{prefix}_exch_timestamp": fill.exch_timestamp,
        f"{prefix}_order_req_local_ts": fill.order_req_local_ts,
        f"{prefix}_order_exch_ts": fill.order_exch_ts,
        f"{prefix}_order_resp_local_ts": fill.order_resp_local_ts,
        f"{prefix}_order_entry_latency_ns": order_entry_latency_ns,
        f"{prefix}_order_response_latency_ns": order_response_latency_ns,
    }
