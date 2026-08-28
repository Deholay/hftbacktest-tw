"""Canonical daily-versus-symbol market-data parity checks."""

from __future__ import annotations

import hashlib
from argparse import Namespace
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np

from scripts.tw_stock_data_to_npz import _float_column, _timestamp_column_to_ns, daily_parquet_columns


@dataclass(frozen=True)
class ParityResult:
    equal: bool
    left_rows: int
    right_rows: int
    left_sha256: str
    right_sha256: str
    mismatched_columns: tuple[str, ...]
    first_mismatch: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def canonical_source_arrays(frame, *, symbol: str, trade_date: str, levels: int = 5) -> dict[str, np.ndarray]:
    """Cast provider-specific dtypes into the converter's value semantics."""
    import polars as pl

    args = Namespace(timestamp_unit="auto", timezone="Asia/Taipei", date=trade_date)
    rows = frame.height
    arrays: dict[str, np.ndarray] = {
        "symbol": np.full(rows, str(symbol), dtype=f"U{max(1, len(str(symbol)))}"),
        "exchtime": _timestamp_column_to_ns(frame, "exchtime", args),
    }
    arrays["localtime"] = _timestamp_column_to_ns(
        frame, "localtime", args, fallback=arrays["exchtime"]
    )
    if "status" in frame.columns:
        arrays["status"] = np.asarray(
            frame.get_column("status").cast(pl.Utf8, strict=False).fill_null("").to_list(), dtype=str
        )
    else:
        arrays["status"] = np.full(rows, "", dtype="U1")
    for name in ("last_price",):
        arrays[name] = _float_column(frame, name)
    arrays["total_volume"] = np.nan_to_num(
        _float_column(frame, "total_volume", 0.0), nan=0.0
    ).astype(np.int64)
    if "sequence" in frame.columns:
        arrays["sequence"] = (
            frame.get_column("sequence").cast(pl.Int64, strict=False).fill_null(-1).to_numpy()
        ).astype(np.int64, copy=False)
    else:
        arrays["sequence"] = np.full(rows, -1, dtype=np.int64)
    for name in daily_parquet_columns(levels):
        if name in arrays or name in {"symbol", "symbol_id", "exchtime", "localtime", "status", "sequence"}:
            continue
        arrays[name] = _float_column(frame, name)

    # Stable source order resolves otherwise-identical timestamp collisions.
    source_row = np.arange(rows, dtype=np.int64)
    order = np.lexsort((source_row, arrays["sequence"], arrays["localtime"], arrays["exchtime"]))
    return {name: np.ascontiguousarray(values[order]) for name, values in arrays.items()}


def compare_source_frames(left, right, *, symbol: str, trade_date: str, levels: int = 5) -> ParityResult:
    left_arrays = canonical_source_arrays(left, symbol=symbol, trade_date=trade_date, levels=levels)
    right_arrays = canonical_source_arrays(right, symbol=symbol, trade_date=trade_date, levels=levels)
    left_hash = _arrays_sha256(left_arrays)
    right_hash = _arrays_sha256(right_arrays)
    mismatches: list[str] = []
    first = None
    for name in left_arrays:
        lhs = left_arrays[name]
        rhs = right_arrays[name]
        same = lhs.shape == rhs.shape and (
            np.array_equal(lhs, rhs, equal_nan=True)
            if lhs.dtype.kind == "f" and rhs.dtype.kind == "f"
            else np.array_equal(lhs, rhs)
        )
        if same:
            continue
        mismatches.append(name)
        if first is None:
            if lhs.shape != rhs.shape:
                first = {"column": name, "left_shape": lhs.shape, "right_shape": rhs.shape}
            else:
                unequal = ~((lhs == rhs) | (_is_nan(lhs) & _is_nan(rhs)))
                index = int(np.flatnonzero(unequal)[0])
                first = {
                    "column": name,
                    "row": index,
                    "left": _json_scalar(lhs[index]),
                    "right": _json_scalar(rhs[index]),
                }
    return ParityResult(
        equal=not mismatches,
        left_rows=left.height,
        right_rows=right.height,
        left_sha256=left_hash,
        right_sha256=right_hash,
        mismatched_columns=tuple(mismatches),
        first_mismatch=first,
    )


def _arrays_sha256(arrays: dict[str, np.ndarray]) -> str:
    digest = hashlib.sha256()
    for name, values in arrays.items():
        digest.update(name.encode())
        digest.update(str(values.dtype).encode())
        digest.update(np.asarray(values.shape, dtype=np.int64).tobytes())
        if values.dtype.kind in {"U", "O"}:
            for value in values:
                encoded = str(value).encode("utf-8")
                digest.update(len(encoded).to_bytes(4, "little"))
                digest.update(encoded)
        else:
            canonical = values.copy()
            if canonical.dtype.kind == "f":
                canonical[np.isnan(canonical)] = np.nan
            digest.update(canonical.tobytes())
    return digest.hexdigest()


def _is_nan(values: np.ndarray) -> np.ndarray:
    return np.isnan(values) if values.dtype.kind == "f" else np.zeros(values.shape, dtype=bool)


def _json_scalar(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    return value
