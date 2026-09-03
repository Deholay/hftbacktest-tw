"""Per-symbol timestamp correction and deterministic order sidecars."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import CompactCacheError


ORDER_SIDECAR_SCHEMA = pa.schema([("row_index", pa.uint64())])


def timestamp_ordering_facts(
    exchange: np.ndarray,
    local_raw: np.ndarray,
    source_seq: np.ndarray,
    *,
    base_latency_ns: int,
) -> dict[str, Any]:
    """Derive the legacy per-symbol correction and ordering facts."""

    if not len(exchange):
        return {
            "raw_min_feed_latency_ns": None,
            "local_timestamp_adjustment_ns": 0,
            "exchange_ordered": True,
            "local_ordered": True,
            "requires_dual_order": False,
            "exchange_order": None,
            "local_order": None,
        }
    raw_latency = local_raw - exchange
    minimum = int(np.min(raw_latency))
    adjustment = -minimum + int(base_latency_ns) if minimum < 0 else 0
    corrected_local = local_raw + adjustment
    exchange_ordered = bool(np.all(exchange[1:] >= exchange[:-1]))
    local_ordered = bool(np.all(corrected_local[1:] >= corrected_local[:-1]))
    return {
        "raw_min_feed_latency_ns": minimum,
        "local_timestamp_adjustment_ns": adjustment,
        "exchange_ordered": exchange_ordered,
        "local_ordered": local_ordered,
        "requires_dual_order": not (exchange_ordered and local_ordered),
        "exchange_order": (
            None if exchange_ordered else np.lexsort((source_seq, exchange)).astype(np.uint64)
        ),
        "local_order": (
            None
            if local_ordered
            else np.lexsort((source_seq, corrected_local)).astype(np.uint64)
        ),
    }


def write_order_sidecar(
    output: Path,
    kind: str,
    order: np.ndarray,
    *,
    write_arrow: Callable[[Path, pa.Table, str], None],
    file_sha256: Callable[[Path], str],
) -> dict[str, Any]:
    """Write one deterministic LZ4 row-index sidecar."""

    path = output.with_name(f"{output.stem}.{kind}_order.arrow")
    table = pa.Table.from_arrays([pa.array(order, type=pa.uint64())], schema=ORDER_SIDECAR_SCHEMA)
    write_arrow(path, table, "lz4")
    return {
        "file": path.name,
        "rows": len(order),
        "bytes": path.stat().st_size,
        "sha256": file_sha256(path),
    }


def validate_order_sidecar(
    path: Path,
    *,
    expected_order: np.ndarray,
    expected_details: dict[str, Any],
) -> None:
    """Validate checksum-independent sidecar schema and deterministic content."""

    try:
        with pa.memory_map(str(path), "r") as handle:
            table = ipc.open_file(handle).read_all().combine_chunks()
    except (OSError, pa.ArrowException) as exc:
        raise CompactCacheError(f"invalid compact sidecar: {path}") from exc
    if table.schema.remove_metadata() != ORDER_SIDECAR_SCHEMA:
        raise CompactCacheError(f"compact sidecar schema mismatch: {path}")
    actual = table["row_index"].to_numpy(zero_copy_only=False)
    if table.num_rows != int(expected_details.get("rows", -1)):
        raise CompactCacheError(f"compact sidecar row-count mismatch: {path}")
    if not np.array_equal(actual, expected_order.astype(np.uint64, copy=False)):
        raise CompactCacheError(f"compact sidecar ordering mismatch: {path}")


__all__ = (
    "ORDER_SIDECAR_SCHEMA",
    "timestamp_ordering_facts",
    "validate_order_sidecar",
    "write_order_sidecar",
)
