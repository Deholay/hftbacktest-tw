"""HftBacktest-shaped facade retained for current futures/spot consumers.

New code must use :class:`hftbacktest_slim.SlimEngine`. This adapter preserves
the legacy integer return codes, visibility rules, mapping behavior, and no-op
cancel/clear operations while delegating runtime work to the neutral engine.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from os import PathLike
from pathlib import Path
from typing import Any, Iterator

from ..config import AssetConfig
from ..engine.binding import development_library_path
from ..engine.replay import SlimEngine
from ..engine.validation import supported_time_in_force
from ..errors import UnsupportedCapabilityError
from ..models import DepthView, OrderView
from ..version import SLIM_ENGINE_VERSION


# Retain the legacy diagnostic constant. Normal construction passes no explicit
# path so the neutral resolver can honor environment and packaged overrides.
SLIM_LIBRARY = development_library_path()


class SlimHbtConstants:
    BUY = 1
    SELL = -1
    LIMIT = 0
    NEW = 1
    EXPIRED = 2
    FILLED = 3
    CANCELED = 4
    GTC = 0
    GTX = 1
    FOK = 2
    IOC = 3


class SlimOrder:
    def __init__(self, value: OrderView):
        self.order_id = value.order_id
        self.status = int(value.status)
        self.exec_price = value.execution_price
        self.exec_qty = value.execution_quantity
        self.leaves_qty = value.leaves_quantity
        # HftBacktest's Order.local_timestamp is the local request timestamp;
        # response visibility is exposed separately through order_latency().
        self.local_timestamp = value.request_local_timestamp_ns
        self.exch_timestamp = value.exchange_timestamp_ns


class SlimDepth:
    def __init__(self, view: DepthView, tick_size: float):
        self.best_bid = view.best_bid if view.valid else math.nan
        self.best_ask = view.best_ask if view.valid else math.nan
        self._bid_qty = view.best_bid_quantity if view.valid else math.nan
        self._ask_qty = view.best_ask_quantity if view.valid else math.nan
        self.best_bid_tick = round(self.best_bid / tick_size) if view.valid else 0
        self.best_ask_tick = round(self.best_ask / tick_size) if view.valid else 0

    def bid_qty_at_tick(self, tick: int) -> float:
        return self._bid_qty if tick == self.best_bid_tick else 0.0

    def ask_qty_at_tick(self, tick: int) -> float:
        return self._ask_qty if tick == self.best_ask_tick else 0.0


class _Orders(Mapping[int, SlimOrder]):
    def __init__(self, backtest: "SlimBacktest", asset_no: int):
        self.backtest = backtest
        self.asset_no = asset_no

    def get(self, key: int, default=None):
        value = self.backtest._order(self.asset_no, int(key))
        return default if value is None else value

    def __getitem__(self, key: int) -> SlimOrder:
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __iter__(self) -> Iterator[int]:
        return iter(())

    def __len__(self) -> int:
        return 0


def _structural_asset_config(asset: Any) -> AssetConfig:
    if isinstance(asset, AssetConfig):
        return asset
    data = getattr(asset, "data", None)
    if data is None:
        data = getattr(asset, "data_path", None)
    tick_size = getattr(asset, "tick_size", None) or 1.0
    return AssetConfig(
        symbol=getattr(asset, "symbol"),
        data_path=data,
        tick_size=tick_size,
        feed_latency_offset_ns=getattr(asset, "feed_latency_offset_ns", 0),
        order_entry_latency_ns=getattr(asset, "order_entry_latency_ns", 0),
        order_response_latency_ns=getattr(asset, "order_response_latency_ns", 0),
    )


class SlimBacktest:
    """Legacy facade implementing the HBT methods used by HbtPairBacktester."""

    def __init__(
        self,
        assets: Sequence[Any],
        library_path: str | PathLike[str] | Path | None = None,
    ) -> None:
        neutral_assets = [_structural_asset_config(asset) for asset in assets]
        self._engine = SlimEngine(neutral_assets, library_path=library_path)
        self._tick_sizes = [asset.tick_size for asset in neutral_assets]

    @property
    def current_timestamp(self) -> int:
        return self._engine.current_timestamp

    def elapse(self, duration_ns: int) -> int:
        return self._engine._elapse_code(int(duration_ns))

    def depth(self, asset_no: int) -> SlimDepth:
        return SlimDepth(self._engine.depth(asset_no), self._tick_sizes[asset_no])

    def feed_latency(self, asset_no: int):
        value = self._engine.feed_latency(asset_no)
        if value is None:
            return None
        return value.exchange_timestamp_ns, value.local_timestamp_ns

    def order_latency(self, asset_no: int):
        value = self._engine.order_latency(asset_no)
        if value is None:
            return None
        return (
            value.request_local_timestamp_ns,
            value.exchange_timestamp_ns,
            value.response_local_timestamp_ns,
        )

    def submit_buy_order(
        self,
        asset_no: int,
        order_id: int,
        price: float,
        qty: float,
        tif: int,
        _order_type: int,
        _wait: bool,
    ) -> int:
        return self._submit(asset_no, order_id, SlimHbtConstants.BUY, price, qty, tif)

    def submit_sell_order(
        self,
        asset_no: int,
        order_id: int,
        price: float,
        qty: float,
        tif: int,
        _order_type: int,
        _wait: bool,
    ) -> int:
        return self._submit(asset_no, order_id, SlimHbtConstants.SELL, price, qty, tif)

    def _submit(
        self,
        asset_no: int,
        order_id: int,
        side: int,
        price: float,
        qty: float,
        tif: int,
    ) -> int:
        return self._engine._submit_code(asset_no, order_id, side, price, qty, tif)

    def wait_order_response(self, asset_no: int, order_id: int, timeout_ns: int) -> int:
        return self._engine._wait_order_response_code(asset_no, order_id, timeout_ns)

    def _order(self, asset_no: int, order_id: int) -> SlimOrder | None:
        value = self._engine.order(asset_no, order_id)
        return None if value is None else SlimOrder(value)

    def orders(self, asset_no: int) -> Mapping[int, SlimOrder]:
        return _Orders(self, asset_no)

    def cancel(self, _asset_no: int, _order_id: int, _wait: bool) -> int:
        return 0

    def clear_inactive_orders(self, _asset_no: int) -> None:
        return None

    def close(self) -> None:
        self._engine.close()

    def __enter__(self) -> "SlimBacktest":
        self._engine.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self._engine.__exit__(exc_type, exc, traceback)


def validate_slim_pair_config(pair: Any) -> None:
    for field in (
        "first_leg_time_in_force",
        "second_leg_time_in_force",
        "flatten_first_leg_time_in_force",
    ):
        value = getattr(pair, field)
        try:
            supported_time_in_force(str(value), strip=False)
        except UnsupportedCapabilityError as exc:
            raise ValueError(
                f"slim engine supports only immediate FOK/IOC; {field}={str(value).upper()!r}"
            ) from exc


__all__ = (
    "SLIM_ENGINE_VERSION",
    "SLIM_LIBRARY",
    "SlimBacktest",
    "SlimDepth",
    "SlimHbtConstants",
    "SlimOrder",
    "validate_slim_pair_config",
)
