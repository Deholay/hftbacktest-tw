"""Read package-owned compact ``bbo_v1`` partitions for native ABI version 1."""

from __future__ import annotations

from dataclasses import dataclass
from os import PathLike
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import ArrowDataError
from ..market_data.schema import (
    COMPACT_SCHEMA_VERSION,
    PHYSICAL_FIELDS,
    SLIM_ROW_DTYPE,
    decoded_metadata,
    validate_bbo_schema,
)


SUPPORTED_SCHEMA_VERSION = COMPACT_SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class LoadedRows:
    """One native row array plus its per-symbol local-time correction."""

    rows: np.ndarray
    local_timestamp_adjustment_ns: int


def _validate_physical_schema(schema: pa.Schema, path: Path) -> None:
    validate_bbo_schema(schema, path)


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
    metadata = decoded_metadata(table.schema, resolved)
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
