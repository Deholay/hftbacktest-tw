"""Neutral two-asset lifecycle and operations for the slim replay engine."""

from __future__ import annotations

from collections.abc import Sequence
from os import PathLike
from pathlib import Path
from typing import Any

from ..config import AssetConfig
from ..enums import OrderStatus, Side, TimeInForce
from ..errors import EngineClosedError, NativeCallError, OrderSubmissionError
from ..models import DepthView, FeedLatency, OrderLatency, OrderView
from .binding import NativeBinding
from .validation import (
    supported_side,
    supported_time_in_force,
    validate_asset_no,
    validate_assets,
    validate_nanoseconds,
    validate_order_id,
)


class SlimEngine:
    """Caller-clocked compact-BBO replay for exactly two assets.

    ``advance`` and ``wait_order_response`` return ``True`` on successful
    advancement/visibility and ``False`` at end-of-data or timeout. ``depth``
    returns a :class:`DepthView`; latency lookups return their immutable model
    or ``None`` until a sample exists; ``order`` returns a response-visible
    :class:`OrderView` or ``None``.

    The implemented order profile is immediate crossing FOK/IOC limit matching
    with no partial fills and no displayed-size cap.
    """

    def __init__(
        self,
        assets: Sequence[AssetConfig],
        library_path: str | PathLike[str] | Path | None = None,
    ) -> None:
        left, right = validate_assets(assets)
        self._assets = (left, right)
        self._binding = NativeBinding(library_path)
        self.library_path = self._binding.path
        self._handle: int | None = None

        # Arrow/NumPy are imported only when an engine is opened. Root-package
        # import remains dependency-light and never loads the native artifact.
        from .arrow_reader import read_rows

        loaded = [read_rows(asset.data_path) for asset in self._assets]
        row_arrays = [partition.rows for partition in loaded]
        adjustments = [
            partition.local_timestamp_adjustment_ns for partition in loaded
        ]
        # Keep every NumPy array live through hbt_slim_create. ABI v1 copies
        # the rows before returning, so they can be released afterward.
        handle = self._binding.create(row_arrays, adjustments, self._assets)
        if not handle:
            raise NativeCallError(
                f"failed to construct slim native engine with {self.library_path}"
            )
        self._handle = handle

    @classmethod
    def open(
        cls,
        assets: Sequence[AssetConfig],
        library_path: str | PathLike[str] | Path | None = None,
    ) -> "SlimEngine":
        """Open an engine; equivalent to direct construction."""

        return cls(assets, library_path=library_path)

    def _open_handle(self) -> int:
        if self._handle is None:
            raise EngineClosedError("slim engine is closed")
        return self._handle

    @property
    def closed(self) -> bool:
        return self._handle is None

    @property
    def current_timestamp(self) -> int:
        return self._binding.current_timestamp(self._open_handle())

    @property
    def current_timestamp_ns(self) -> int:
        """Alias that makes the timestamp unit explicit."""

        return self.current_timestamp

    def _elapse_code(self, duration_ns: int) -> int:
        return self._binding.elapse(self._open_handle(), int(duration_ns))

    def advance(self, duration_ns: int) -> bool:
        duration = validate_nanoseconds("duration_ns", duration_ns)
        rc = self._elapse_code(duration)
        if rc == 0:
            return True
        if rc == 1:
            return False
        raise NativeCallError(f"slim clock advancement failed with native code {rc}")

    def depth(self, asset_no: int) -> DepthView:
        asset = validate_asset_no(asset_no)
        rc, value = self._binding.depth(self._open_handle(), asset)
        if rc != 0:
            raise NativeCallError(f"slim depth lookup failed with native code {rc}")
        return DepthView(
            best_bid=float(value.bid_px),
            best_ask=float(value.ask_px),
            best_bid_quantity=float(value.bid_qty),
            best_ask_quantity=float(value.ask_qty),
            exchange_timestamp_ns=int(value.exch_ts),
            local_timestamp_ns=int(value.local_ts),
            valid=bool(value.valid),
        )

    def feed_latency(self, asset_no: int) -> FeedLatency | None:
        asset = validate_asset_no(asset_no)
        rc, exchange, local = self._binding.feed_latency(self._open_handle(), asset)
        if rc == 1:
            return None
        if rc != 0:
            raise NativeCallError(f"slim feed-latency lookup failed with native code {rc}")
        return FeedLatency(exchange_timestamp_ns=exchange, local_timestamp_ns=local)

    def order_latency(self, asset_no: int) -> OrderLatency | None:
        asset = validate_asset_no(asset_no)
        rc, request, exchange, response = self._binding.order_latency(
            self._open_handle(), asset
        )
        if rc == 1:
            return None
        if rc != 0:
            raise NativeCallError(f"slim order-latency lookup failed with native code {rc}")
        return OrderLatency(
            request_local_timestamp_ns=request,
            exchange_timestamp_ns=exchange,
            response_local_timestamp_ns=response,
        )

    def _submit_code(
        self,
        asset_no: int,
        order_id: int,
        side: int,
        price: float,
        quantity: float,
        time_in_force: int,
    ) -> int:
        return self._binding.submit(
            self._open_handle(),
            int(asset_no),
            int(order_id),
            int(side),
            float(price),
            float(quantity),
            int(time_in_force),
        )

    def submit_order(
        self,
        *,
        asset_no: int,
        order_id: int,
        side: Side | int,
        price: float,
        quantity: float,
        time_in_force: TimeInForce | int | str,
    ) -> None:
        asset = validate_asset_no(asset_no)
        identifier = validate_order_id(order_id)
        resolved_side = supported_side(side)
        resolved_tif = supported_time_in_force(time_in_force)
        rc = self._submit_code(
            asset,
            identifier,
            resolved_side,
            price,
            quantity,
            resolved_tif,
        )
        if rc == 0:
            return
        if rc == -2:
            raise OrderSubmissionError(
                f"duplicate slim order_id {identifier} for asset {asset}"
            )
        raise OrderSubmissionError(
            f"slim order submission failed with native code {rc}"
        )

    def _wait_order_response_code(
        self, asset_no: int, order_id: int, timeout_ns: int
    ) -> int:
        return self._binding.wait_order_response(
            self._open_handle(),
            int(asset_no),
            int(order_id),
            int(timeout_ns),
        )

    def wait_order_response(
        self, asset_no: int, order_id: int, timeout_ns: int
    ) -> bool:
        asset = validate_asset_no(asset_no)
        identifier = validate_order_id(order_id)
        timeout = validate_nanoseconds("timeout_ns", timeout_ns)
        rc = self._wait_order_response_code(asset, identifier, timeout)
        if rc == 0:
            return True
        if rc == 1:
            return False
        raise NativeCallError(f"slim order-response wait failed with native code {rc}")

    def order(self, asset_no: int, order_id: int) -> OrderView | None:
        asset = validate_asset_no(asset_no)
        identifier = validate_order_id(order_id)
        rc, value = self._binding.order(self._open_handle(), asset, identifier)
        if rc == 1:
            return None
        if rc != 0:
            raise NativeCallError(f"slim order lookup failed with native code {rc}")
        return OrderView(
            order_id=int(value.order_id),
            asset_no=int(value.asset_no),
            side=Side(int(value.side)),
            time_in_force=TimeInForce(int(value.tif)),
            status=OrderStatus(int(value.status)),
            requested_price=float(value.requested_price),
            requested_quantity=float(value.requested_qty),
            execution_price=float(value.exec_price),
            execution_quantity=float(value.exec_qty),
            leaves_quantity=float(value.leaves_qty),
            request_local_timestamp_ns=int(value.req_local_ts),
            exchange_timestamp_ns=int(value.exch_ts),
            response_local_timestamp_ns=int(value.resp_local_ts),
            response_visible=bool(value.response_visible),
        )

    def close(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        self._binding.free(handle)

    def __enter__(self) -> "SlimEngine":
        self._open_handle()
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            # Interpreter shutdown and partially constructed instances must not
            # turn defensive cleanup into an unraisable exception.
            pass


__all__ = ("SlimEngine",)
