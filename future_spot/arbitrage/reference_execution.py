"""Installed-HftBacktest adapter for the futures/spot execution port."""

from __future__ import annotations

import math
from typing import Any

from scripts.hbt_common import apply_queue_model, get_order, hbt_time_in_force, order_is_active
from scripts.hbt_types import HbtAssetConfig
from scripts.tw_stock_hftbacktest import import_hftbacktest, workspace_root

from .execution_port import ExecutionDepth, ExecutionOrder
from .hbt_helpers import infer_hbt_asset_tick_size


class ReferenceExecutionAdapter:
    """Map the reference HBT engine onto the strategy-owned execution port."""

    def __init__(self, backend: Any, hbtpkg: Any, tick_sizes: tuple[float, float]) -> None:
        self._backend = backend
        self._hbtpkg = hbtpkg
        self._tick_sizes = tick_sizes
        self._closed = False

    @classmethod
    def open(
        cls,
        spot: HbtAssetConfig,
        future: HbtAssetConfig,
        *,
        hbtpkg: Any | None = None,
    ) -> tuple["ReferenceExecutionAdapter", dict[str, float]]:
        package = hbtpkg or import_hftbacktest(workspace_root(spot.data))
        spot_asset, spot_tick = cls._build_asset(package, spot)
        future_asset, future_tick = cls._build_asset(package, future)
        backend = package.HashMapMarketDepthBacktest([spot_asset, future_asset])
        adapter = cls(backend, package, (spot_tick, future_tick))
        return adapter, {"spot_tick_size": spot_tick, "future_tick_size": future_tick}

    @staticmethod
    def _build_asset(hbtpkg: Any, config: HbtAssetConfig) -> tuple[Any, float]:
        tick_size = config.tick_size or infer_hbt_asset_tick_size(
            config.data,
            config.instrument,
            fallback=1.0,
            trade_date=config.trade_date,
        )
        asset = (
            hbtpkg.BacktestAsset()
            .data(str(config.data))
            .linear_asset(config.contract_size)
            .constant_order_latency(config.order_entry_latency_ns, config.order_response_latency_ns)
            .no_partial_fill_exchange()
            .trading_value_fee_model(config.maker_fee, config.taker_fee)
            .tick_size(tick_size)
            .lot_size(config.lot_size)
            .last_trades_capacity(config.last_trades_capacity)
        )
        if config.feed_latency_offset_ns:
            asset = asset.latency_offset(config.feed_latency_offset_ns)
        return apply_queue_model(asset, config), tick_size

    @property
    def current_timestamp(self) -> int:
        return int(self._backend.current_timestamp)

    @property
    def scanner_backend(self) -> Any:
        return self._backend

    def advance(self, duration_ns: int) -> bool:
        return int(self._backend.elapse(int(duration_ns))) == 0

    def depth(self, asset_no: int) -> ExecutionDepth:
        raw = self._backend.depth(asset_no)
        bid = float(raw.best_bid)
        ask = float(raw.best_ask)
        bid_tick = int(raw.best_bid_tick)
        ask_tick = int(raw.best_ask_tick)
        return ExecutionDepth(
            best_bid=bid,
            best_ask=ask,
            bid_quantity=float(raw.bid_qty_at_tick(bid_tick)) if math.isfinite(bid) else math.nan,
            ask_quantity=float(raw.ask_qty_at_tick(ask_tick)) if math.isfinite(ask) else math.nan,
            best_bid_tick=bid_tick,
            best_ask_tick=ask_tick,
        )

    def feed_latency(self, asset_no: int) -> tuple[int, int] | None:
        value = self._backend.feed_latency(asset_no)
        return None if value is None else (int(value[0]), int(value[1]))

    def order_latency(self, asset_no: int) -> tuple[int | None, int | None, int | None]:
        value = self._backend.order_latency(asset_no)
        return (None, None, None) if value is None else tuple(int(item) for item in value)

    def resolve_time_in_force(self, value: str) -> int:
        return hbt_time_in_force(self._hbtpkg, value)

    def resolve_side(self, value: str) -> int:
        if value == "buy":
            return int(self._hbtpkg.BUY)
        if value == "sell":
            return int(self._hbtpkg.SELL)
        raise RuntimeError(f"unsupported side: {value}")

    def submit_limit(
        self,
        asset_no: int,
        order_id: int,
        side: str,
        price: float,
        quantity: float,
        time_in_force: str,
    ) -> int:
        resolved_side = self.resolve_side(side)
        tif = self.resolve_time_in_force(time_in_force)
        if resolved_side == int(self._hbtpkg.BUY):
            return int(
                self._backend.submit_buy_order(
                    asset_no, order_id, price, quantity, tif, self._hbtpkg.LIMIT, False
                )
            )
        return int(
            self._backend.submit_sell_order(
                asset_no, order_id, price, quantity, tif, self._hbtpkg.LIMIT, False
            )
        )

    def wait_order_response(self, asset_no: int, order_id: int, timeout_ns: int) -> int:
        return int(self._backend.wait_order_response(asset_no, order_id, timeout_ns))

    def order(self, asset_no: int, order_id: int) -> ExecutionOrder | None:
        raw = get_order(self._backend, asset_no, order_id)
        if raw is None:
            return None
        return ExecutionOrder(
            order_id=int(raw.order_id),
            asset_no=int(asset_no),
            status=int(raw.status),
            exec_price=float(raw.exec_price),
            exec_qty=float(raw.exec_qty),
            leaves_qty=float(raw.leaves_qty),
            local_timestamp=int(raw.local_timestamp),
            exch_timestamp=int(raw.exch_timestamp),
        )

    def order_is_active(self, order: ExecutionOrder | None) -> bool:
        return order_is_active(order, self._hbtpkg)

    def cancel_active_order(self, asset_no: int, order_id: int, timeout_ns: int) -> None:
        self._backend.cancel(asset_no, order_id, False)
        self._backend.wait_order_response(asset_no, order_id, timeout_ns)

    def clear_inactive_orders(self, asset_no: int) -> None:
        self._backend.clear_inactive_orders(asset_no)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._backend.close()


__all__ = ("ReferenceExecutionAdapter",)
