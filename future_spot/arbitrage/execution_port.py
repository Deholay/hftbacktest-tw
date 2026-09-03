"""Strategy-owned execution boundary for futures/spot pair replay.

The port deliberately exposes only the BBO, latency, immediate-order, and
lifecycle operations used by :mod:`future_spot.arbitrage.hbt_backtest`.
Reference-HBT and slim-native representations are normalized by their adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ExecutionDepth:
    """HBT-independent top-of-book view with tick-index quantity access."""

    best_bid: float
    best_ask: float
    bid_quantity: float
    ask_quantity: float
    best_bid_tick: int
    best_ask_tick: int

    def bid_qty_at_tick(self, tick: int) -> float:
        return self.bid_quantity if int(tick) == self.best_bid_tick else 0.0

    def ask_qty_at_tick(self, tick: int) -> float:
        return self.ask_quantity if int(tick) == self.best_ask_tick else 0.0


@dataclass(frozen=True, slots=True)
class ExecutionOrder:
    """Strategy-facing response-visible order state."""

    order_id: int
    asset_no: int
    status: int
    exec_price: float
    exec_qty: float
    leaves_qty: float
    local_timestamp: int
    exch_timestamp: int


class ExecutionInvariantError(RuntimeError):
    """Raised when an engine violates the selected execution profile."""


@runtime_checkable
class ExecutionEngine(Protocol):
    """Minimal backend port consumed by the futures/spot strategy runner."""

    @property
    def current_timestamp(self) -> int: ...

    @property
    def scanner_backend(self) -> Any | None: ...

    def advance(self, duration_ns: int) -> bool: ...

    def depth(self, asset_no: int) -> ExecutionDepth: ...

    def feed_latency(self, asset_no: int) -> tuple[int, int] | None: ...

    def order_latency(self, asset_no: int) -> tuple[int | None, int | None, int | None]: ...

    def resolve_time_in_force(self, value: str) -> Any: ...

    def resolve_side(self, value: str) -> Any: ...

    def submit_limit(
        self,
        asset_no: int,
        order_id: int,
        side: str,
        price: float,
        quantity: float,
        time_in_force: str,
    ) -> int: ...

    def wait_order_response(self, asset_no: int, order_id: int, timeout_ns: int) -> int: ...

    def order(self, asset_no: int, order_id: int) -> ExecutionOrder | None: ...

    def order_is_active(self, order: ExecutionOrder | None) -> bool: ...

    def cancel_active_order(self, asset_no: int, order_id: int, timeout_ns: int) -> None: ...

    def clear_inactive_orders(self, asset_no: int) -> None: ...

    def close(self) -> None: ...


__all__ = (
    "ExecutionDepth",
    "ExecutionEngine",
    "ExecutionInvariantError",
    "ExecutionOrder",
)
