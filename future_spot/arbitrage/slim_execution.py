"""Neutral ``hftbacktest_slim`` adapter for futures/spot execution."""

from __future__ import annotations

import math

from hftbacktest_slim import AssetConfig, OrderStatus, Side, SlimEngine, TimeInForce
from scripts.hbt_types import HbtAssetConfig

from .execution_port import ExecutionDepth, ExecutionInvariantError, ExecutionOrder


class SlimExecutionAdapter:
    """Map the neutral slim API onto the strategy-owned execution port."""

    def __init__(self, backend: SlimEngine, tick_sizes: tuple[float, float]) -> None:
        self._backend = backend
        self._tick_sizes = tick_sizes

    @classmethod
    def open(
        cls,
        spot: HbtAssetConfig,
        future: HbtAssetConfig,
        *,
        spot_tick_size: float,
        future_tick_size: float,
    ) -> tuple["SlimExecutionAdapter", dict[str, float]]:
        assets = (
            cls.asset_config(spot, spot_tick_size),
            cls.asset_config(future, future_tick_size),
        )
        adapter = cls(SlimEngine(assets), (spot_tick_size, future_tick_size))
        return adapter, {
            "spot_tick_size": spot_tick_size,
            "future_tick_size": future_tick_size,
        }

    @staticmethod
    def asset_config(config: HbtAssetConfig, tick_size: float) -> AssetConfig:
        return AssetConfig(
            symbol=config.symbol,
            data_path=config.data,
            tick_size=tick_size,
            feed_latency_offset_ns=config.feed_latency_offset_ns,
            order_entry_latency_ns=config.order_entry_latency_ns,
            order_response_latency_ns=config.order_response_latency_ns,
        )

    @staticmethod
    def validate_time_in_force(value: str) -> TimeInForce:
        text = str(value).upper()
        try:
            return TimeInForce[text]
        except KeyError as exc:
            raise ValueError(f"slim engine supports only immediate FOK/IOC; got {text!r}") from exc

    @classmethod
    def validate_pair(cls, pair: object) -> None:
        for field in (
            "first_leg_time_in_force",
            "second_leg_time_in_force",
            "flatten_first_leg_time_in_force",
        ):
            value = getattr(pair, field)
            try:
                cls.validate_time_in_force(value)
            except ValueError as exc:
                raise ValueError(
                    f"slim engine supports only immediate FOK/IOC; {field}={str(value).upper()!r}"
                ) from exc

    @property
    def current_timestamp(self) -> int:
        return self._backend.current_timestamp

    @property
    def scanner_backend(self) -> None:
        return None

    def advance(self, duration_ns: int) -> bool:
        return self._backend.advance(duration_ns)

    def depth(self, asset_no: int) -> ExecutionDepth:
        raw = self._backend.depth(asset_no)
        tick_size = self._tick_sizes[asset_no]
        bid = raw.best_bid if raw.valid else math.nan
        ask = raw.best_ask if raw.valid else math.nan
        return ExecutionDepth(
            best_bid=bid,
            best_ask=ask,
            bid_quantity=raw.best_bid_quantity if raw.valid else math.nan,
            ask_quantity=raw.best_ask_quantity if raw.valid else math.nan,
            best_bid_tick=round(bid / tick_size) if raw.valid else 0,
            best_ask_tick=round(ask / tick_size) if raw.valid else 0,
        )

    def feed_latency(self, asset_no: int) -> tuple[int, int] | None:
        value = self._backend.feed_latency(asset_no)
        if value is None:
            return None
        return value.exchange_timestamp_ns, value.local_timestamp_ns

    def order_latency(self, asset_no: int) -> tuple[int | None, int | None, int | None]:
        value = self._backend.order_latency(asset_no)
        if value is None:
            return None, None, None
        return (
            value.request_local_timestamp_ns,
            value.exchange_timestamp_ns,
            value.response_local_timestamp_ns,
        )

    def resolve_time_in_force(self, value: str) -> TimeInForce:
        return self.validate_time_in_force(value)

    @staticmethod
    def resolve_side(value: str) -> Side:
        if value == "buy":
            return Side.BUY
        if value == "sell":
            return Side.SELL
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
        self._backend.submit_order(
            asset_no=asset_no,
            order_id=order_id,
            side=self.resolve_side(side),
            price=price,
            quantity=quantity,
            time_in_force=self.resolve_time_in_force(time_in_force),
        )
        return 0

    def wait_order_response(self, asset_no: int, order_id: int, timeout_ns: int) -> int:
        return 0 if self._backend.wait_order_response(asset_no, order_id, timeout_ns) else 1

    def order(self, asset_no: int, order_id: int) -> ExecutionOrder | None:
        raw = self._backend.order(asset_no, order_id)
        if raw is None:
            return None
        return ExecutionOrder(
            order_id=raw.order_id,
            asset_no=raw.asset_no,
            status=int(raw.status),
            exec_price=raw.execution_price,
            exec_qty=raw.execution_quantity,
            leaves_qty=raw.leaves_quantity,
            local_timestamp=raw.request_local_timestamp_ns,
            exch_timestamp=raw.exchange_timestamp_ns,
        )

    @staticmethod
    def order_is_active(order: ExecutionOrder | None) -> bool:
        return order is not None and order.status == int(OrderStatus.NEW) and order.leaves_qty > 0

    def cancel_active_order(self, asset_no: int, order_id: int, timeout_ns: int) -> None:
        del asset_no, order_id, timeout_ns
        raise ExecutionInvariantError(
            "slim immediate FOK/IOC order remained active; passive cancellation is unsupported"
        )

    def clear_inactive_orders(self, asset_no: int) -> None:
        del asset_no

    def close(self) -> None:
        self._backend.close()


__all__ = ("SlimExecutionAdapter",)
