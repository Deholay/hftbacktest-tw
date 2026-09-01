#!/usr/bin/env python3
"""Compare compact BBO reconstruction with direct canonical source conversion."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from scripts.compact_cache import CompactBuildConfig, CompactCacheStore
from scripts.compact_hbt_adapter import compact_to_reference_events
from scripts.tw_stock_data_to_npz import (
    _float_matrix,
    build_events_from_parquet_frame,
    normalized_bbo_from_depth_columns,
    symbol_filter_values,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--raw-file", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    store = CompactCacheStore(
        CompactBuildConfig(cache_root=args.cache_root, max_cache_bytes=2**63 - 1, min_free_bytes=0)
    )
    compact = store.read_symbol(args.date, args.source, args.symbol)
    compact_events, _ = compact_to_reference_events(compact, trade_date=args.date)

    schema = pl.scan_parquet(args.raw_file).collect_schema().names()
    symbol_column = "symbol" if "symbol" in schema else "symbol_id"
    raw = (
        pl.scan_parquet(args.raw_file)
        .filter(pl.col(symbol_column).cast(pl.Utf8).is_in(symbol_filter_values(args.symbol)))
        .collect()
    )
    sort_columns = [name for name in ("exchtime", "localtime", "sequence") if name in raw.columns]
    raw = raw.sort(sort_columns, maintain_order=True)
    bid_px, bid_qty = normalized_bbo_from_depth_columns(
        _float_matrix(raw, [f"bid_price{level}" for level in range(1, 6)]),
        _float_matrix(raw, [f"bid_volume{level}" for level in range(1, 6)]),
        1.0,
        0.0,
        False,
        True,
    )
    ask_px, ask_qty = normalized_bbo_from_depth_columns(
        _float_matrix(raw, [f"ask_price{level}" for level in range(1, 6)]),
        _float_matrix(raw, [f"ask_volume{level}" for level in range(1, 6)]),
        1.0,
        0.0,
        False,
        False,
    )
    direct = raw.select("exchtime", "localtime", "last_price", "total_volume").with_columns(
        pl.Series("bid_price1", bid_px),
        pl.Series("bid_volume1", bid_qty),
        pl.Series("ask_price1", ask_px),
        pl.Series("ask_volume1", ask_qty),
    )
    converter_args = argparse.Namespace(
        levels=1,
        timestamp_unit="auto",
        timezone="Asia/Taipei",
        date=args.date,
        base_latency_ns=0,
        volume_scale=1.0,
        price_only_depth_qty=None,
        trade_side="infer",
        no_trades=False,
        no_depth=False,
        qa_sample_rows=1000,
    )
    direct_events, _ = build_events_from_parquet_frame(direct, converter_args)
    equal = np.array_equal(compact_events, direct_events)
    payload = {
        "date": args.date,
        "source": args.source,
        "symbol": args.symbol,
        "source_rows": raw.height,
        "compact_rows": compact.num_rows,
        "direct_event_rows": len(direct_events),
        "compact_event_rows": len(compact_events),
        "events_equal": equal,
        "direct_sha256": hashlib.sha256(direct_events.tobytes()).hexdigest(),
        "compact_sha256": hashlib.sha256(compact_events.tobytes()).hexdigest(),
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if equal else 1


if __name__ == "__main__":
    raise SystemExit(main())
