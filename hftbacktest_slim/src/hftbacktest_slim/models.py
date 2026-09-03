"""Immutable neutral views returned by the slim runtime."""

from __future__ import annotations

from dataclasses import dataclass

from .enums import OrderStatus, Side, TimeInForce


@dataclass(frozen=True, slots=True)
class DepthView:
    """Latest local BBO for one asset.

    Prices and quantities are copied directly from the native view. Callers
    must check ``valid`` before treating them as a usable two-sided quote.
    """

    best_bid: float
    best_ask: float
    best_bid_quantity: float
    best_ask_quantity: float
    exchange_timestamp_ns: int
    local_timestamp_ns: int
    valid: bool

    @property
    def bid_quantity(self) -> float:
        return self.best_bid_quantity

    @property
    def ask_quantity(self) -> float:
        return self.best_ask_quantity


@dataclass(frozen=True, slots=True)
class FeedLatency:
    """Latest exchange/local feed timestamps for one asset."""

    exchange_timestamp_ns: int
    local_timestamp_ns: int

    @property
    def latency_ns(self) -> int:
        return self.local_timestamp_ns - self.exchange_timestamp_ns


@dataclass(frozen=True, slots=True)
class OrderLatency:
    """Latest request, exchange-arrival, and response-visible timestamps."""

    request_local_timestamp_ns: int
    exchange_timestamp_ns: int
    response_local_timestamp_ns: int

    @property
    def entry_latency_ns(self) -> int:
        return self.exchange_timestamp_ns - self.request_local_timestamp_ns

    @property
    def response_latency_ns(self) -> int:
        return self.response_local_timestamp_ns - self.exchange_timestamp_ns

    @property
    def total_latency_ns(self) -> int:
        return self.response_local_timestamp_ns - self.request_local_timestamp_ns


@dataclass(frozen=True, slots=True)
class OrderView:
    """One response-visible immediate limit order."""

    order_id: int
    asset_no: int
    side: Side
    time_in_force: TimeInForce
    status: OrderStatus
    requested_price: float
    requested_quantity: float
    execution_price: float
    execution_quantity: float
    leaves_quantity: float
    request_local_timestamp_ns: int
    exchange_timestamp_ns: int
    response_local_timestamp_ns: int
    response_visible: bool


__all__ = ("DepthView", "FeedLatency", "OrderLatency", "OrderView")
