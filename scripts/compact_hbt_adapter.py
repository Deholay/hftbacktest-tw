"""Reference-HBT adapter for the versioned compact BBO cache."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pyarrow as pa

from hftbacktest_slim import COMPACT_SCHEMA_VERSION  # noqa: E402
from scripts.tw_stock_data_to_npz import (
    ConversionStats,
    build_events_from_parquet_frame,
    save_event_data,
)


ADAPTER_VERSION = 1


def compact_to_reference_events(
    table: pa.Table,
    *,
    trade_date: str,
    base_latency_ns: int = 0,
    volume_scale: float = 1.0,
    trade_side: str = "infer",
    no_trades: bool = False,
) -> tuple[np.ndarray, ConversionStats]:
    """Reconstruct the BBO-only reference event stream from a compact partition."""
    metadata = {
        key.decode(): value.decode() for key, value in (table.schema.metadata or {}).items()
    }
    schema_version = metadata.get("schema_version")
    if schema_version not in {None, COMPACT_SCHEMA_VERSION}:
        raise ValueError(f"unsupported compact schema: {schema_version}")
    source_seq = table["source_seq"].to_numpy(zero_copy_only=False)
    exchange_ts = table["exch_ts"].to_numpy(zero_copy_only=False)
    local_ts = table["local_ts_raw"].to_numpy(zero_copy_only=False)
    order = np.lexsort((source_seq, local_ts, exchange_ts))
    table = table.take(pa.array(order))
    frame = pl.from_arrow(
        pa.table(
            {
                "exchtime": table["exch_ts"],
                "localtime": table["local_ts_raw"],
                "last_price": table["last_px"],
                "total_volume": table["total_volume"],
                "bid_price1": table["bid_px"],
                "bid_volume1": table["bid_qty"],
                "ask_price1": table["ask_px"],
                "ask_volume1": table["ask_qty"],
            }
        )
    )
    args = argparse.Namespace(
        levels=1,
        timestamp_unit="ns",
        timezone="Asia/Taipei",
        date=trade_date,
        base_latency_ns=base_latency_ns,
        volume_scale=volume_scale,
        price_only_depth_qty=None,
        trade_side=trade_side,
        no_trades=no_trades,
        no_depth=False,
        qa_sample_rows=1000,
    )
    return build_events_from_parquet_frame(frame, args)


def write_reference_npz_from_compact(
    table: pa.Table,
    output: Path,
    *,
    trade_date: str,
    compact_identity_sha256: str,
    base_latency_ns: int = 0,
    volume_scale: float = 1.0,
    trade_side: str = "infer",
    no_trades: bool = False,
    npz_compression: str = "uncompressed",
) -> dict[str, Any]:
    """Atomically publish a reference NPZ and its compact-source identity."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    events, stats = compact_to_reference_events(
        table,
        trade_date=trade_date,
        base_latency_ns=base_latency_ns,
        volume_scale=volume_scale,
        trade_side=trade_side,
        no_trades=no_trades,
    )
    identity = {
        "adapter_version": ADAPTER_VERSION,
        "compact_schema_version": COMPACT_SCHEMA_VERSION,
        "compact_identity_sha256": compact_identity_sha256,
        "trade_date": trade_date,
        "base_latency_ns": base_latency_ns,
        "volume_scale": volume_scale,
        "trade_side": trade_side,
        "no_trades": no_trades,
        "npz_compression": npz_compression,
    }
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{output.stem}-", suffix=".npz", dir=output.parent)
    os.close(descriptor)
    temporary = Path(temporary_name)
    args = argparse.Namespace(output=temporary, npz_compression=npz_compression)
    try:
        save_event_data(events, stats, args)
        os.replace(temporary, output)
        manifest_path = output.with_suffix(output.suffix + ".compact.json")
        manifest_temp = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
        manifest_temp.write_text(
            json.dumps({**identity, "event_rows": len(events)}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(manifest_temp, manifest_path)
    finally:
        temporary.unlink(missing_ok=True)
    return {**identity, "event_rows": len(events), "output": str(output)}
