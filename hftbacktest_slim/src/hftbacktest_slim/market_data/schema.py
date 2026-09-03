"""Canonical physical contract for compact ``bbo_v1`` rows."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa

from ..errors import ArrowDataError


COMPACT_SCHEMA_VERSION = "bbo_v1"

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

BBO_SCHEMA = pa.schema(PHYSICAL_FIELDS)

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

PROJECTED_COLUMNS = [
    "symbol",
    "symbol_id",
    "exchtime",
    "localtime",
    "status",
    "last_price",
    "total_volume",
    "sequence",
] + [
    f"{side}_{kind}{level}"
    for level in range(1, 6)
    for side in ("bid", "ask")
    for kind in ("price", "volume")
]


def decoded_metadata(schema: pa.Schema, path: Path | None = None) -> dict[str, str]:
    """Decode UTF-8 schema metadata with a typed data error on corruption."""

    try:
        return {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (schema.metadata or {}).items()
        }
    except UnicodeDecodeError as exc:
        label = "" if path is None else f" in {path}"
        raise ArrowDataError(f"invalid UTF-8 compact schema metadata{label}") from exc


def validate_bbo_schema(schema: pa.Schema, path: Path | None = None) -> None:
    """Require exact field names, order, types, widths, and nullability."""

    physical = schema.remove_metadata()
    if physical == BBO_SCHEMA:
        return
    label = "compact Arrow schema" if path is None else f"compact Arrow partition {path}"
    if physical.names != BBO_SCHEMA.names:
        raise ArrowDataError(
            f"{label} has incompatible fields/order {physical.names}; expected {BBO_SCHEMA.names}"
        )
    for actual, expected in zip(physical, BBO_SCHEMA):
        if actual.type != expected.type or actual.nullable != expected.nullable:
            raise ArrowDataError(
                f"compact Arrow field {actual.name!r} in {label} has incompatible type/nullability "
                f"{actual.type}/{actual.nullable}; expected {expected.type}/{expected.nullable}"
            )
    raise ArrowDataError(f"{label} does not match the canonical bbo_v1 schema")


def validate_schema_metadata(
    schema: pa.Schema,
    path: Path | None = None,
    *,
    require: bool = True,
) -> dict[str, str]:
    """Validate schema identity and timestamp-adjustment metadata."""

    metadata = decoded_metadata(schema, path)
    declared = metadata.get("schema_version")
    if declared is None and require:
        raise ArrowDataError("compact Arrow schema is missing schema_version metadata")
    if declared is not None and declared != COMPACT_SCHEMA_VERSION:
        raise ArrowDataError(
            f"compact Arrow schema declares {declared!r}; expected {COMPACT_SCHEMA_VERSION!r}"
        )
    adjustment = metadata.get("local_timestamp_adjustment_ns")
    if adjustment is None and require:
        raise ArrowDataError(
            "compact Arrow schema is missing local_timestamp_adjustment_ns metadata"
        )
    if adjustment is not None:
        try:
            int(adjustment)
        except ValueError as exc:
            raise ArrowDataError(
                "compact Arrow schema has invalid local_timestamp_adjustment_ns metadata"
            ) from exc
    return metadata


__all__ = (
    "BBO_SCHEMA",
    "COMPACT_SCHEMA_VERSION",
    "PHYSICAL_FIELDS",
    "PROJECTED_COLUMNS",
    "SLIM_ROW_DTYPE",
    "decoded_metadata",
    "validate_bbo_schema",
    "validate_schema_metadata",
)
