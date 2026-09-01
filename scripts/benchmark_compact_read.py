#!/usr/bin/env python3
"""Benchmark complete warm reads of one validated compact-cache date."""

from __future__ import annotations

import argparse
import json
import math
import resource
import statistics
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from scripts.compact_cache import CompactBuildConfig, CompactCacheStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    store = CompactCacheStore(
        CompactBuildConfig(cache_root=args.cache_root, max_cache_bytes=2**63 - 1, min_free_bytes=0)
    )
    manifest = store.validate_date(args.date)
    files = [
        store.date_path(args.date) / f"source={source}" / details["file"]
        for source, source_details in manifest["sources"].items()
        for details in source_details["symbols"].values()
        if details.get("status") == "valid"
    ]
    runs = []
    for _ in range(args.repetitions):
        rows = 0
        checksum = 0.0
        started_wall = time.perf_counter()
        started_cpu = time.process_time()
        for path in files:
            with pa.memory_map(str(path), "r") as handle:
                table = ipc.open_file(handle).read_all()
                rows += table.num_rows
                value = float(table["bid_px"].chunk(0)[0].as_py() or 0.0) if table.num_rows else 0.0
                checksum += value if math.isfinite(value) else 0.0
        wall = time.perf_counter() - started_wall
        runs.append(
            {
                "wall_seconds": wall,
                "cpu_seconds": time.process_time() - started_cpu,
                "rows": rows,
                "rows_per_second": rows / wall,
                "checksum": checksum,
            }
        )
    payload = {
        "date": args.date,
        "cache_root": str(args.cache_root.resolve()),
        "files": len(files),
        "bytes": sum(path.stat().st_size for path in files),
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "runs": runs,
        "median_wall_seconds": statistics.median(item["wall_seconds"] for item in runs),
        "median_rows_per_second": statistics.median(item["rows_per_second"] for item in runs),
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
