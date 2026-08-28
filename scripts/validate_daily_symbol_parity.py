#!/usr/bin/env python3
"""Validate canonical parity between stock daily Parquet and DataAPI history."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from scripts.source_parity import compare_source_frames
from scripts.tw_stock_data_to_npz import (
    DEFAULT_DATA_PLATFORM_BASE,
    _collect_source_frame,
    default_data_api_module_dir,
    load_data_api_frame,
)


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument(
        "--daily-file-template",
        default="/mnt/z/數據平台/ticker_store/daily_parquet/twstock_{date_nodash}.parquet",
    )
    parser.add_argument("--data-platform-base", default=DEFAULT_DATA_PLATFORM_BASE)
    parser.add_argument("--index-backend", default="duckdb", choices=("duckdb", "parquet"))
    parser.add_argument("--data-api-module-dir", type=Path, default=default_data_api_module_dir(root))
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def _loader_args(args: argparse.Namespace, symbol: str) -> argparse.Namespace:
    return argparse.Namespace(
        data_api_module_dir=args.data_api_module_dir,
        data_platform_base=args.data_platform_base,
        index_backend=args.index_backend,
        symbol=symbol,
        start_date=args.date,
        end_date=args.date,
        levels=5,
        status_allow=None,
        start_exch_ts=None,
        end_exch_ts=None,
    )


def main() -> int:
    args = parse_args()
    daily_path = Path(
        args.daily_file_template.format(
            date=args.date, date_dash=args.date, date_nodash=args.date.replace("-", "")
        )
    )
    if not daily_path.is_file():
        raise FileNotFoundError(daily_path)
    results = []
    for symbol in args.symbols:
        loader = _loader_args(args, symbol)
        api_frame = load_data_api_frame(loader)
        daily_frame = _collect_source_frame(
            pl.scan_parquet(daily_path), loader, filter_symbol=True, symbols=[symbol]
        )
        result = compare_source_frames(
            api_frame, daily_frame, symbol=symbol, trade_date=args.date
        ).to_dict()
        result["symbol"] = symbol
        results.append(result)
    payload = {
        "date": args.date,
        "daily_file": str(daily_path.resolve()),
        "all_equal": all(item["equal"] for item in results),
        "symbols": results,
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if payload["all_equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
