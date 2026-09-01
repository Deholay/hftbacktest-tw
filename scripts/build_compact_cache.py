#!/usr/bin/env python3
"""Build or validate one date of the shared compact BBO cache."""

from __future__ import annotations

import argparse
import json
import resource
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from scripts.compact_cache import CompactBuildConfig, CompactCacheStore, CompactSource


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--stock-path", type=Path)
    parser.add_argument("--future-path", type=Path)
    parser.add_argument("--spot-symbols", nargs="*", default=[])
    parser.add_argument("--future-symbols", nargs="*", default=[])
    parser.add_argument(
        "--settings-parquet",
        type=Path,
        help="Read spot/future symbol universes from a persisted settings partition.",
    )
    parser.add_argument("--compression", choices=("none", "lz4", "zstd"), default="lz4")
    parser.add_argument("--batch-rows", type=int, default=131_072)
    parser.add_argument("--max-gb", type=float, default=200.0)
    parser.add_argument("--min-free-gb", type=float, default=200.0)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def _settings_symbols(path: Path) -> tuple[list[str], list[str]]:
    table = pq.ParquetFile(path).read(columns=["leg", "symbol"])
    legs = table["leg"].to_pylist()
    symbols = table["symbol"].to_pylist()
    spot = sorted({str(symbol) for leg, symbol in zip(legs, symbols) if leg == "spot"})
    future = sorted({str(symbol) for leg, symbol in zip(legs, symbols) if leg == "future"})
    return spot, future


def main() -> int:
    args = parse_args()
    spots = list(dict.fromkeys(str(value) for value in args.spot_symbols))
    futures = list(dict.fromkeys(str(value) for value in args.future_symbols))
    if args.settings_parquet:
        settings_spot, settings_future = _settings_symbols(args.settings_parquet)
        spots = list(dict.fromkeys([*spots, *settings_spot]))
        futures = list(dict.fromkeys([*futures, *settings_future]))
    sources = []
    if args.stock_path and spots:
        sources.append(CompactSource("stock", (args.stock_path,), tuple(spots)))
    if args.future_path and futures:
        sources.append(CompactSource("stock_future", (args.future_path,), tuple(futures)))
    if not sources:
        raise ValueError("provide at least one source path and its symbol universe")

    config = CompactBuildConfig(
        cache_root=args.cache_root,
        compression=args.compression,
        batch_rows=args.batch_rows,
        max_cache_bytes=int(args.max_gb * 1024**3),
        min_free_bytes=int(args.min_free_gb * 1024**3),
        rebuild=args.rebuild,
    )
    started_wall = time.perf_counter()
    started_cpu = time.process_time()
    manifest = CompactCacheStore(config).build_date(args.date, sources)
    payload = {
        "date": args.date,
        "cache_root": str(args.cache_root.resolve()),
        "compression": args.compression,
        "cache_state": manifest["cache_state"],
        "wall_seconds": time.perf_counter() - started_wall,
        "cpu_seconds": time.process_time() - started_cpu,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "manifest": manifest,
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
