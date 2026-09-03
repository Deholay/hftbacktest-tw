"""Provider- and strategy-neutral compact partition facts."""

from __future__ import annotations

from os import PathLike
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import CompactCacheError
from .schema import decoded_metadata, validate_bbo_schema, validate_schema_metadata


def compact_partition_audit(
    data_path: str | PathLike[str] | Path,
) -> dict[str, Any]:
    """Return raw compact facts without applying any strategy tick schedule."""

    path = Path(data_path)
    try:
        with pa.memory_map(str(path), "r") as handle:
            table = ipc.open_file(handle).read_all().combine_chunks()
    except (OSError, pa.ArrowException) as exc:
        raise CompactCacheError(f"failed to read compact partition {path}: {exc}") from exc
    validate_bbo_schema(table.schema, path)
    validate_schema_metadata(table.schema, path, require=True)
    metadata = decoded_metadata(table.schema, path)
    adjustment = int(metadata["local_timestamp_adjustment_ns"])
    exchange = table["exch_ts"].to_numpy(zero_copy_only=False)
    local_raw = table["local_ts_raw"].to_numpy(zero_copy_only=False)
    local = local_raw + adjustment
    bid = table["bid_px"].to_numpy(zero_copy_only=False)
    ask = table["ask_px"].to_numpy(zero_copy_only=False)
    prices = np.concatenate((bid, ask))
    prices = prices[np.isfinite(prices) & (prices > 0)]
    volume = table["total_volume"].to_numpy(zero_copy_only=False)
    raw_latency = local_raw - exchange
    corrected_latency = local - exchange
    return {
        "rows": table.num_rows,
        "first_exch_ts": int(exchange.min()) if len(exchange) else None,
        "last_exch_ts": int(exchange.max()) if len(exchange) else None,
        "raw_min_feed_latency_ns": int(raw_latency.min()) if len(exchange) else None,
        "raw_max_feed_latency_ns": int(raw_latency.max()) if len(exchange) else None,
        "local_timestamp_adjustment_ns": adjustment,
        "min_latency_ns": int(corrected_latency.min()) if len(exchange) else None,
        "max_latency_ns": int(corrected_latency.max()) if len(exchange) else None,
        "min_price": float(prices.min()) if len(prices) else None,
        "max_price": float(prices.max()) if len(prices) else None,
        "depth_events": None,
        "trade_events": int(np.sum(np.diff(volume) > 0)) if len(volume) > 1 else 0,
        "metadata": metadata,
        "schema_valid": True,
    }


__all__ = ("compact_partition_audit",)
