from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from hftbacktest_slim.cli import benchmark_read, build_cache


def _raw(path: Path) -> None:
    values: dict[str, list] = {
        "symbol": ["0050"],
        "exchtime": [100],
        "localtime": [110],
        "last_price": [77.95],
        "total_volume": [1],
    }
    for level in range(1, 6):
        values[f"bid_price{level}"] = [77.90 if level == 1 else None]
        values[f"ask_price{level}"] = [77.95 if level == 1 else None]
        values[f"bid_volume{level}"] = [1.0 if level == 1 else None]
        values[f"ask_volume{level}"] = [1.0 if level == 1 else None]
    pq.write_table(pa.table(values), path)


def test_package_build_and_benchmark_json_shapes(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    cache = tmp_path / "cache"
    _raw(raw)
    build_args = Namespace(
        date="2026-03-02",
        cache_root=cache,
        stock_path=raw,
        future_path=None,
        spot_symbols=["0050"],
        future_symbols=[],
        settings_parquet=None,
        compression="lz4",
        batch_rows=2,
        max_gb=1.0,
        min_free_gb=0.0,
        rebuild=False,
        output=None,
    )
    built = build_cache.run(build_args)
    assert set(built) == {
        "date",
        "cache_root",
        "compression",
        "cache_state",
        "wall_seconds",
        "cpu_seconds",
        "peak_rss_kib",
        "manifest",
    }
    assert built["cache_state"] == "miss"
    benchmark = benchmark_read.run(
        Namespace(
            cache_root=cache,
            date="2026-03-02",
            repetitions=1,
            output=None,
        )
    )
    assert set(benchmark) == {
        "date",
        "cache_root",
        "files",
        "bytes",
        "peak_rss_kib",
        "runs",
        "median_wall_seconds",
        "median_rows_per_second",
    }
    assert set(benchmark["runs"][0]) == {
        "wall_seconds",
        "cpu_seconds",
        "rows",
        "rows_per_second",
        "checksum",
    }
    assert benchmark["runs"][0]["rows"] == 1
