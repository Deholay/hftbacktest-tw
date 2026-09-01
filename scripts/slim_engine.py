"""ctypes binding and HftBacktest-compatible facade for the Rust slim engine."""

from __future__ import annotations

import ctypes
import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterator, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from scripts.hbt_types import HbtAssetConfig


SLIM_ENGINE_VERSION = "rust-0.2.0"
SLIM_LIBRARY = Path(__file__).resolve().parents[1] / "target" / "release" / "libhbt_slim.so"

SLIM_ROW_DTYPE = np.dtype(
    [
        ("source_seq", "u8"),
        ("exch_ts", "i8"),
        ("local_ts_raw", "i8"),
        ("bid_px", "f8"),
        ("ask_px", "f8"),
        ("bid_qty", "f8"),
        ("ask_qty", "f8"),
        ("last_px", "f8"),
        ("total_volume", "i8"),
    ],
    align=True,
)


class _BboView(ctypes.Structure):
    _fields_ = [
        ("bid_px", ctypes.c_double),
        ("ask_px", ctypes.c_double),
        ("bid_qty", ctypes.c_double),
        ("ask_qty", ctypes.c_double),
        ("exch_ts", ctypes.c_int64),
        ("local_ts", ctypes.c_int64),
        ("valid", ctypes.c_int32),
    ]


class _OrderView(ctypes.Structure):
    _fields_ = [
        ("order_id", ctypes.c_uint64),
        ("asset_no", ctypes.c_uint32),
        ("side", ctypes.c_int32),
        ("tif", ctypes.c_int32),
        ("status", ctypes.c_int32),
        ("requested_price", ctypes.c_double),
        ("requested_qty", ctypes.c_double),
        ("exec_price", ctypes.c_double),
        ("exec_qty", ctypes.c_double),
        ("leaves_qty", ctypes.c_double),
        ("req_local_ts", ctypes.c_int64),
        ("exch_ts", ctypes.c_int64),
        ("resp_local_ts", ctypes.c_int64),
        ("response_visible", ctypes.c_int32),
    ]


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
    def __init__(self, value: _OrderView):
        self.order_id = int(value.order_id)
        self.status = int(value.status)
        self.exec_price = float(value.exec_price)
        self.exec_qty = float(value.exec_qty)
        self.leaves_qty = float(value.leaves_qty)
        # HftBacktest's Order.local_timestamp is the local request timestamp;
        # response visibility is exposed separately through order_latency().
        self.local_timestamp = int(value.req_local_ts)
        self.exch_timestamp = int(value.exch_ts)


class SlimDepth:
    def __init__(self, view: _BboView, tick_size: float):
        self.best_bid = float(view.bid_px) if view.valid else math.nan
        self.best_ask = float(view.ask_px) if view.valid else math.nan
        self._bid_qty = float(view.bid_qty) if view.valid else math.nan
        self._ask_qty = float(view.ask_qty) if view.valid else math.nan
        self.best_bid_tick = round(self.best_bid / tick_size) if view.valid else 0
        self.best_ask_tick = round(self.best_ask / tick_size) if view.valid else 0

    def bid_qty_at_tick(self, tick: int) -> float:
        return self._bid_qty if tick == self.best_bid_tick else 0.0

    def ask_qty_at_tick(self, tick: int) -> float:
        return self._ask_qty if tick == self.best_ask_tick else 0.0


class _Orders(Mapping[int, SlimOrder]):
    def __init__(self, engine: "SlimBacktest", asset_no: int):
        self.engine = engine
        self.asset_no = asset_no

    def get(self, key: int, default=None):
        value = self.engine._order(self.asset_no, int(key))
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


def _load_library(path: Path = SLIM_LIBRARY):
    if not path.is_file():
        raise RuntimeError(
            f"Rust slim engine is not built: {path}; run cargo build --workspace --release"
        )
    library = ctypes.CDLL(str(path))
    pointer = ctypes.c_void_p
    row_pointer = ctypes.POINTER(ctypes.c_ubyte)
    library.hbt_slim_version.restype = ctypes.c_uint32
    library.hbt_slim_create.argtypes = [
        row_pointer, ctypes.c_size_t, ctypes.c_int64, ctypes.c_int64, ctypes.c_int64, ctypes.c_int64, ctypes.c_double,
        row_pointer, ctypes.c_size_t, ctypes.c_int64, ctypes.c_int64, ctypes.c_int64, ctypes.c_int64, ctypes.c_double,
    ]
    library.hbt_slim_create.restype = pointer
    library.hbt_slim_free.argtypes = [pointer]
    library.hbt_slim_current_timestamp.argtypes = [pointer]
    library.hbt_slim_current_timestamp.restype = ctypes.c_int64
    library.hbt_slim_elapse.argtypes = [pointer, ctypes.c_int64]
    library.hbt_slim_elapse.restype = ctypes.c_int32
    library.hbt_slim_depth.argtypes = [pointer, ctypes.c_size_t, ctypes.POINTER(_BboView)]
    library.hbt_slim_feed_latency.argtypes = [pointer, ctypes.c_size_t, ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_int64)]
    library.hbt_slim_order_latency.argtypes = [pointer, ctypes.c_size_t, ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_int64)]
    library.hbt_slim_submit.argtypes = [pointer, ctypes.c_size_t, ctypes.c_uint64, ctypes.c_int32, ctypes.c_double, ctypes.c_double, ctypes.c_int32]
    library.hbt_slim_wait_order_response.argtypes = [pointer, ctypes.c_size_t, ctypes.c_uint64, ctypes.c_int64]
    library.hbt_slim_order.argtypes = [pointer, ctypes.c_size_t, ctypes.c_uint64, ctypes.POINTER(_OrderView)]
    return library


def _read_rows(path: Path) -> tuple[np.ndarray, int]:
    with pa.memory_map(str(path), "r") as handle:
        table = ipc.open_file(handle).read_all().combine_chunks()
    metadata = {key.decode(): value.decode() for key, value in (table.schema.metadata or {}).items()}
    adjustment = int(metadata.get("local_timestamp_adjustment_ns", 0))
    rows = np.empty(table.num_rows, dtype=SLIM_ROW_DTYPE)
    for name in SLIM_ROW_DTYPE.names or ():
        rows[name] = table[name].to_numpy(zero_copy_only=False)
    return rows, adjustment


class SlimBacktest:
    """Small facade implementing the HBT methods used by HbtPairBacktester."""

    def __init__(self, assets: Sequence[HbtAssetConfig], library_path: Path = SLIM_LIBRARY):
        if len(assets) != 2:
            raise ValueError("slim engine requires exactly two assets")
        self._library = _load_library(library_path)
        if self._library.hbt_slim_version() != 1:
            raise RuntimeError("unsupported Rust slim ABI version")
        rows_and_adjustments = [_read_rows(Path(asset.data)) for asset in assets]
        self._tick_sizes = [float(asset.tick_size or 1.0) for asset in assets]
        params: list[Any] = []
        for (rows, adjustment), asset in zip(rows_and_adjustments, assets):
            params.extend(
                [
                    rows.ctypes.data_as(ctypes.POINTER(ctypes.c_ubyte)),
                    len(rows),
                    adjustment,
                    asset.feed_latency_offset_ns,
                    asset.order_entry_latency_ns,
                    asset.order_response_latency_ns,
                    float(asset.tick_size or 1.0),
                ]
            )
        self._handle = self._library.hbt_slim_create(*params)
        if not self._handle:
            raise RuntimeError("failed to construct Rust slim engine")

    @property
    def current_timestamp(self) -> int:
        return int(self._library.hbt_slim_current_timestamp(self._handle))

    def elapse(self, duration_ns: int) -> int:
        return int(self._library.hbt_slim_elapse(self._handle, int(duration_ns)))

    def depth(self, asset_no: int) -> SlimDepth:
        view = _BboView()
        rc = self._library.hbt_slim_depth(self._handle, asset_no, ctypes.byref(view))
        if rc != 0:
            raise RuntimeError(f"slim depth failed: {rc}")
        return SlimDepth(view, self._tick_sizes[asset_no])

    def feed_latency(self, asset_no: int):
        exch = ctypes.c_int64()
        local = ctypes.c_int64()
        rc = self._library.hbt_slim_feed_latency(self._handle, asset_no, ctypes.byref(exch), ctypes.byref(local))
        return None if rc != 0 else (int(exch.value), int(local.value))

    def order_latency(self, asset_no: int):
        req = ctypes.c_int64()
        exch = ctypes.c_int64()
        resp = ctypes.c_int64()
        rc = self._library.hbt_slim_order_latency(
            self._handle, asset_no, ctypes.byref(req), ctypes.byref(exch), ctypes.byref(resp)
        )
        return None if rc != 0 else (int(req.value), int(exch.value), int(resp.value))

    def submit_buy_order(self, asset_no: int, order_id: int, price: float, qty: float, tif: int, _order_type: int, _wait: bool) -> int:
        return self._submit(asset_no, order_id, 1, price, qty, tif)

    def submit_sell_order(self, asset_no: int, order_id: int, price: float, qty: float, tif: int, _order_type: int, _wait: bool) -> int:
        return self._submit(asset_no, order_id, -1, price, qty, tif)

    def _submit(self, asset_no: int, order_id: int, side: int, price: float, qty: float, tif: int) -> int:
        return int(self._library.hbt_slim_submit(self._handle, asset_no, order_id, side, price, qty, tif))

    def wait_order_response(self, asset_no: int, order_id: int, timeout_ns: int) -> int:
        return int(self._library.hbt_slim_wait_order_response(self._handle, asset_no, order_id, timeout_ns))

    def _order(self, asset_no: int, order_id: int) -> SlimOrder | None:
        value = _OrderView()
        rc = self._library.hbt_slim_order(self._handle, asset_no, order_id, ctypes.byref(value))
        return None if rc != 0 else SlimOrder(value)

    def orders(self, asset_no: int) -> Mapping[int, SlimOrder]:
        return _Orders(self, asset_no)

    def cancel(self, _asset_no: int, _order_id: int, _wait: bool) -> int:
        return 0

    def clear_inactive_orders(self, _asset_no: int) -> None:
        return None

    def close(self) -> None:
        if getattr(self, "_handle", None):
            self._library.hbt_slim_free(self._handle)
            self._handle = None


def validate_slim_pair_config(pair: Any) -> None:
    for field in (
        "first_leg_time_in_force",
        "second_leg_time_in_force",
        "flatten_first_leg_time_in_force",
    ):
        value = str(getattr(pair, field)).upper()
        if value not in {"FOK", "IOC"}:
            raise ValueError(f"slim engine supports only immediate FOK/IOC; {field}={value!r}")
