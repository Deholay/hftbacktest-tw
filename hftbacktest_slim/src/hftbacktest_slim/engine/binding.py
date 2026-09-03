"""ctypes declarations and instance-owned calls for native ABI version 1."""

from __future__ import annotations

import ctypes
import os
from os import PathLike
from pathlib import Path
from typing import Any, Sequence

from ..config import AssetConfig
from ..errors import AbiMismatchError, NativeLibraryError, NativeLibraryNotFoundError


NATIVE_ABI_VERSION = 1
LIBRARY_ENVIRONMENT_VARIABLE = "HFTBACKTEST_SLIM_LIBRARY"


def _library_filename() -> str:
    if os.name == "nt":
        return "hbt_slim.dll"
    if sys_platform() == "darwin":
        return "libhbt_slim.dylib"
    return "libhbt_slim.so"


def sys_platform() -> str:
    """Small indirection kept testable without loading a native library."""

    import sys

    return sys.platform


LIBRARY_FILENAME = _library_filename()


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


def package_library_candidates() -> tuple[Path, ...]:
    """Return deterministic wheel/package locations without checking them."""

    package_root = Path(__file__).resolve().parents[1]
    return (
        package_root / "_native" / LIBRARY_FILENAME,
        package_root / "native" / LIBRARY_FILENAME,
        package_root / LIBRARY_FILENAME,
    )


def development_library_path() -> Path:
    """Return the root Cargo release path for a source-tree checkout."""

    repository_root = Path(__file__).resolve().parents[4]
    return repository_root / "target" / "release" / LIBRARY_FILENAME


def _required_file(path: str | PathLike[str], source: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise NativeLibraryNotFoundError(
            f"slim native library from {source} does not exist or is not a file: {resolved}"
        )
    return resolved


def resolve_library_path(
    library_path: str | PathLike[str] | Path | None = None,
) -> Path:
    """Resolve the native artifact without searching system library paths.

    Priority is explicit override, environment override, packaged artifact,
    then the repository's root Cargo release artifact. An explicit or
    environment override is authoritative: a bad override raises immediately.
    """

    if library_path is not None:
        return _required_file(library_path, "explicit library_path")

    environment_path = os.environ.get(LIBRARY_ENVIRONMENT_VARIABLE)
    if environment_path:
        return _required_file(
            environment_path,
            f"{LIBRARY_ENVIRONMENT_VARIABLE} environment variable",
        )

    attempted: list[Path] = []
    for candidate in (*package_library_candidates(), development_library_path()):
        resolved = candidate.resolve()
        attempted.append(resolved)
        if resolved.is_file():
            return resolved
    rendered = ", ".join(str(path) for path in attempted)
    raise NativeLibraryNotFoundError(
        "slim native library was not found; build from the package project with "
        "'cargo build --manifest-path native/Cargo.toml --release --target-dir target' "
        f"and set {LIBRARY_ENVIRONMENT_VARIABLE}, or build the repository workspace. "
        f"Checked: {rendered}"
    )


class NativeBinding:
    """Configured ABI-v1 library handle with no global mutable engine state."""

    def __init__(
        self,
        library_path: str | PathLike[str] | Path | None = None,
    ) -> None:
        self.path = resolve_library_path(library_path)
        try:
            self.library = ctypes.CDLL(str(self.path))
        except OSError as exc:
            raise NativeLibraryError(
                f"failed to load slim native library {self.path}: {exc}"
            ) from exc
        try:
            version_function = self.library.hbt_slim_version
        except AttributeError as exc:
            raise NativeLibraryError(
                f"native library {self.path} does not export hbt_slim_version"
            ) from exc
        version_function.argtypes = []
        version_function.restype = ctypes.c_uint32
        actual_abi = int(version_function())
        if actual_abi != NATIVE_ABI_VERSION:
            raise AbiMismatchError(
                f"slim native ABI mismatch for {self.path}: "
                f"expected {NATIVE_ABI_VERSION}, got {actual_abi}"
            )
        self._configure_signatures()

    def _configure_signatures(self) -> None:
        library = self.library
        pointer = ctypes.c_void_p
        row_pointer = ctypes.POINTER(ctypes.c_ubyte)
        library.hbt_slim_version.argtypes = []
        library.hbt_slim_version.restype = ctypes.c_uint32
        library.hbt_slim_create.argtypes = [
            row_pointer,
            ctypes.c_size_t,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_double,
            row_pointer,
            ctypes.c_size_t,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_double,
        ]
        library.hbt_slim_create.restype = pointer
        library.hbt_slim_free.argtypes = [pointer]
        library.hbt_slim_free.restype = None
        library.hbt_slim_current_timestamp.argtypes = [pointer]
        library.hbt_slim_current_timestamp.restype = ctypes.c_int64
        library.hbt_slim_elapse.argtypes = [pointer, ctypes.c_int64]
        library.hbt_slim_elapse.restype = ctypes.c_int32
        library.hbt_slim_depth.argtypes = [
            pointer,
            ctypes.c_size_t,
            ctypes.POINTER(_BboView),
        ]
        library.hbt_slim_depth.restype = ctypes.c_int32
        library.hbt_slim_feed_latency.argtypes = [
            pointer,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_int64),
        ]
        library.hbt_slim_feed_latency.restype = ctypes.c_int32
        library.hbt_slim_order_latency.argtypes = [
            pointer,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_int64),
        ]
        library.hbt_slim_order_latency.restype = ctypes.c_int32
        library.hbt_slim_submit.argtypes = [
            pointer,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_int32,
            ctypes.c_double,
            ctypes.c_double,
            ctypes.c_int32,
        ]
        library.hbt_slim_submit.restype = ctypes.c_int32
        library.hbt_slim_wait_order_response.argtypes = [
            pointer,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.c_int64,
        ]
        library.hbt_slim_wait_order_response.restype = ctypes.c_int32
        library.hbt_slim_order.argtypes = [
            pointer,
            ctypes.c_size_t,
            ctypes.c_uint64,
            ctypes.POINTER(_OrderView),
        ]
        library.hbt_slim_order.restype = ctypes.c_int32

    def create(
        self,
        rows: Sequence[Any],
        adjustments_ns: Sequence[int],
        assets: Sequence[AssetConfig],
    ) -> int | None:
        params: list[Any] = []
        for row_array, adjustment, asset in zip(rows, adjustments_ns, assets):
            params.extend(
                [
                    row_array.ctypes.data_as(ctypes.POINTER(ctypes.c_ubyte)),
                    len(row_array),
                    int(adjustment),
                    asset.feed_latency_offset_ns,
                    asset.order_entry_latency_ns,
                    asset.order_response_latency_ns,
                    asset.tick_size,
                ]
            )
        return self.library.hbt_slim_create(*params)

    def free(self, handle: int) -> None:
        self.library.hbt_slim_free(handle)

    def current_timestamp(self, handle: int) -> int:
        return int(self.library.hbt_slim_current_timestamp(handle))

    def elapse(self, handle: int, duration_ns: int) -> int:
        return int(self.library.hbt_slim_elapse(handle, int(duration_ns)))

    def depth(self, handle: int, asset_no: int) -> tuple[int, _BboView]:
        value = _BboView()
        rc = int(self.library.hbt_slim_depth(handle, asset_no, ctypes.byref(value)))
        return rc, value

    def feed_latency(self, handle: int, asset_no: int) -> tuple[int, int, int]:
        exchange = ctypes.c_int64()
        local = ctypes.c_int64()
        rc = int(
            self.library.hbt_slim_feed_latency(
                handle, asset_no, ctypes.byref(exchange), ctypes.byref(local)
            )
        )
        return rc, int(exchange.value), int(local.value)

    def order_latency(self, handle: int, asset_no: int) -> tuple[int, int, int, int]:
        request = ctypes.c_int64()
        exchange = ctypes.c_int64()
        response = ctypes.c_int64()
        rc = int(
            self.library.hbt_slim_order_latency(
                handle,
                asset_no,
                ctypes.byref(request),
                ctypes.byref(exchange),
                ctypes.byref(response),
            )
        )
        return rc, int(request.value), int(exchange.value), int(response.value)

    def submit(
        self,
        handle: int,
        asset_no: int,
        order_id: int,
        side: int,
        price: float,
        quantity: float,
        time_in_force: int,
    ) -> int:
        return int(
            self.library.hbt_slim_submit(
                handle,
                asset_no,
                order_id,
                side,
                price,
                quantity,
                time_in_force,
            )
        )

    def wait_order_response(
        self, handle: int, asset_no: int, order_id: int, timeout_ns: int
    ) -> int:
        return int(
            self.library.hbt_slim_wait_order_response(
                handle, asset_no, order_id, timeout_ns
            )
        )

    def order(self, handle: int, asset_no: int, order_id: int) -> tuple[int, _OrderView]:
        value = _OrderView()
        rc = int(
            self.library.hbt_slim_order(
                handle, asset_no, order_id, ctypes.byref(value)
            )
        )
        return rc, value


__all__ = (
    "LIBRARY_ENVIRONMENT_VARIABLE",
    "LIBRARY_FILENAME",
    "NATIVE_ABI_VERSION",
    "NativeBinding",
    "development_library_path",
    "package_library_candidates",
    "resolve_library_path",
)
