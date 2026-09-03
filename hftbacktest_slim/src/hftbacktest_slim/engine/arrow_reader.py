"""Read the temporary Phase 3 runtime-side contract for compact ``bbo_v1``.

Compact schema ownership and builders remain in ``scripts.compact_cache``
until Phase 4. This module intentionally duplicates only the nine physical
fields consumed by ABI version 1; it does not import cache implementation code.
"""

from __future__ import annotations

from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import ArrowDataError


SUPPORTED_SCHEMA_VERSION = "bbo_v1"
PHYSICAL_FIELDS: tuple[tuple[str, pa.DataType], ...] = (
    ("source_seq", pa.uint64()),
    ("exch_ts", pa.int64()),
    ("local_ts_raw", pa.int64()),
    ("bid_px", pa.float64()),
    ("ask_px", pa.float64()),
    ("bid_qty", pa.float64()),
    ("ask_qty", pa.float64()),
    ("last_px", pa.float64()),
    ("total_volume", pa.int64()),
)

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


@dataclass(frozen=True, slots=True)
class LoadedRows:
    """One native row array plus its per-symbol local-time correction."""

    rows: np.ndarray
    local_timestamp_adjustment_ns: int


def _decoded_metadata(schema: pa.Schema, path: Path) -> dict[str, str]:
    try:
        return {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (schema.metadata or {}).items()
        }
    except UnicodeDecodeError as exc:
        raise ArrowDataError(f"invalid UTF-8 schema metadata in {path}") from exc


def _validate_physical_schema(schema: pa.Schema, path: Path) -> None:
    for name, expected_type in PHYSICAL_FIELDS:
        index = schema.get_field_index(name)
        if index < 0:
            raise ArrowDataError(f"compact Arrow partition {path} is missing required field {name!r}")
        actual_type = schema.field(index).type
        if actual_type != expected_type:
            raise ArrowDataError(
                f"compact Arrow field {name!r} in {path} has incompatible type "
                f"{actual_type}; expected {expected_type}"
            )


def read_rows(path: str | PathLike[str] | Path) -> LoadedRows:
    """Load one valid (including empty) compact Arrow file into ABI row order.

    Missing ``local_timestamp_adjustment_ns`` metadata retains the legacy
    compatible default of zero. A present schema version must be ``bbo_v1``.
    """

    resolved = Path(path)
    try:
        with pa.memory_map(str(resolved), "r") as handle:
            table = ipc.open_file(handle).read_all().combine_chunks()
    except (OSError, pa.ArrowException) as exc:
        raise ArrowDataError(f"failed to read compact Arrow partition {resolved}: {exc}") from exc

    _validate_physical_schema(table.schema, resolved)
    metadata = _decoded_metadata(table.schema, resolved)
    declared_version = metadata.get("schema_version")
    if declared_version is not None and declared_version != SUPPORTED_SCHEMA_VERSION:
        raise ArrowDataError(
            f"compact Arrow partition {resolved} declares schema version "
            f"{declared_version!r}; expected {SUPPORTED_SCHEMA_VERSION!r}"
        )
    try:
        adjustment = int(metadata.get("local_timestamp_adjustment_ns", "0"))
    except ValueError as exc:
        raise ArrowDataError(
            f"compact Arrow partition {resolved} has invalid "
            "local_timestamp_adjustment_ns metadata"
        ) from exc

    rows = np.empty(table.num_rows, dtype=SLIM_ROW_DTYPE)
    for name in SLIM_ROW_DTYPE.names or ():
        try:
            rows[name] = table[name].to_numpy(zero_copy_only=False)
        except (pa.ArrowException, TypeError, ValueError) as exc:
            raise ArrowDataError(
                f"failed to copy compact Arrow field {name!r} from {resolved}: {exc}"
            ) from exc
    return LoadedRows(rows=rows, local_timestamp_adjustment_ns=adjustment)


def _read_rows(path: str | PathLike[str] | Path) -> tuple[np.ndarray, int]:
    """Internal tuple-shaped bridge for pre-move binding-oriented tests."""

    loaded = read_rows(path)
    return loaded.rows, loaded.local_timestamp_adjustment_ns


__all__ = (
    "LoadedRows",
    "PHYSICAL_FIELDS",
    "SLIM_ROW_DTYPE",
    "SUPPORTED_SCHEMA_VERSION",
    "read_rows",
)
