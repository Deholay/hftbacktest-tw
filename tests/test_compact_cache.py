from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import polars as pl

from hftbacktest_slim import (
    BBO_SCHEMA,
    CompactBuildConfig,
    CompactCacheBudgetError,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
)
from scripts.compact_hbt_adapter import compact_to_reference_events
from scripts.tw_stock_data_to_npz import build_events_from_parquet_frame


def _source(path: Path) -> None:
    rows = {
        "symbol": ["0050", "2330", "0050", "0050"],
        "exchtime": [200, 100, 100, 300],
        "localtime": [180, 101, 120, 330],
        "status": ["OK"] * 4,
        "last_price": [78.0, 1000.0, 77.9, 78.1],
        "total_volume": [20, 1, 10, 30],
    }
    for level in range(1, 6):
        rows[f"bid_price{level}"] = [
            [77.9, 77.9, 77.8, None, None][level - 1],
            [999.0, None, None, None, None][level - 1],
            [77.85, 77.9, None, None, None][level - 1],
            [78.0, None, None, None, None][level - 1],
        ]
        rows[f"ask_price{level}"] = [
            [78.1, 78.0, 78.0, None, None][level - 1],
            [1001.0, None, None, None, None][level - 1],
            [78.0, None, None, None, None][level - 1],
            [78.2, None, None, None, None][level - 1],
        ]
        rows[f"bid_volume{level}"] = [
            [2.0, 3.0, 5.0, None, None][level - 1],
            [1.0, None, None, None, None][level - 1],
            [4.0, 6.0, None, None, None][level - 1],
            [7.0, None, None, None, None][level - 1],
        ]
        rows[f"ask_volume{level}"] = [
            [2.0, 3.0, 4.0, None, None][level - 1],
            [1.0, None, None, None, None][level - 1],
            [8.0, None, None, None, None][level - 1],
            [9.0, None, None, None, None][level - 1],
        ]
    pq.write_table(pa.table(rows), path, row_group_size=2)


def _store(tmp_path: Path, **overrides) -> CompactCacheStore:
    values = {
        "cache_root": tmp_path / "cache",
        "batch_rows": 2,
        "max_cache_bytes": 1024**3,
        "min_free_bytes": 0,
    }
    values.update(overrides)
    return CompactCacheStore(CompactBuildConfig(**values))


def test_cold_build_is_one_scan_and_warm_build_is_zero_scan(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    source = CompactSource("stock", (raw,), ("0050", "2330", "9999"), status_allow=("OK",))

    cold = store.build_date("2026-03-02", [source])
    assert cold["cache_state"] == "miss"
    assert cold["sources"]["stock"]["scan_count"] == 1
    assert cold["sources"]["stock"]["input_rows"] == 4
    assert cold["sources"]["stock"]["input_bytes"] == raw.stat().st_size
    assert cold["sources"]["stock"]["output_bytes"] > 0
    assert cold["sources"]["stock"]["elapsed_seconds"] >= 0
    assert cold["sources"]["stock"]["missing_symbols"] == []
    assert cold["sources"]["stock"]["empty_symbols"] == ["9999"]
    empty = store.read_symbol("2026-03-02", "stock", "9999")
    assert empty.num_rows == 0
    assert empty.schema.remove_metadata() == BBO_SCHEMA
    empty_events, _ = compact_to_reference_events(empty, trade_date="2026-03-02")
    assert len(empty_events) == 0

    table = store.read_symbol("2026-03-02", "stock", "0050")
    assert table.schema.remove_metadata() == BBO_SCHEMA
    assert table["source_seq"].to_pylist() == [0, 2, 3]
    assert table["bid_px"].to_pylist() == [77.9, 77.9, 78.0]
    assert table["bid_qty"].to_pylist() == [5.0, 6.0, 7.0]
    assert table["ask_px"].to_pylist() == [78.0, 78.0, 78.2]
    assert table["ask_qty"].to_pylist() == [7.0, 8.0, 9.0]
    details = cold["sources"]["stock"]["symbols"]["0050"]
    assert details["local_timestamp_adjustment_ns"] == 20
    assert set(details["sidecars"]) == {"exchange_order", "local_order"}

    warm = store.build_date("2026-03-02", [source])
    assert warm["cache_state"] == "hit"
    assert warm["build_invocation_scan_count"] == 0
    assert warm["sources"]["stock"]["scan_count"] == 1


def test_corrupt_symbol_or_sidecar_is_not_reusable(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    source = CompactSource("stock", (raw,), ("0050",))
    manifest = store.build_date("2026-03-02", [source])
    details = manifest["sources"]["stock"]["symbols"]["0050"]
    sidecar = next(iter(details["sidecars"].values()))
    path = store.date_path("2026-03-02") / "source=stock" / sidecar["file"]
    path.write_bytes(b"corrupt")
    with pytest.raises(CompactCacheError, match="sidecar"):
        store.validate_date("2026-03-02")


def test_budget_failure_leaves_no_final_or_temporary_date(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path, max_cache_bytes=1)
    source = CompactSource("stock", (raw,), ("0050",))
    with pytest.raises(CompactCacheBudgetError):
        store.build_date("2026-03-02", [source])
    assert not store.date_path("2026-03-02").exists()
    assert not list((tmp_path / "cache").glob(".tmp-*")) if (tmp_path / "cache").exists() else True


def test_free_space_reserve_failure_leaves_no_partial_date(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path, min_free_bytes=2**63 - 1)
    with pytest.raises(CompactCacheBudgetError, match="reserve"):
        store.build_date("2026-03-02", [CompactSource("stock", (raw,), ("0050",))])
    assert not store.date_path("2026-03-02").exists()


def test_incomplete_date_is_never_a_hit(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    final = store.date_path("2026-03-02")
    final.mkdir(parents=True)
    (final / "manifest.json").write_text(json.dumps({"build_complete": False}), encoding="utf-8")
    source = CompactSource("stock", (raw,), ("0050",))
    with pytest.raises(CompactCacheError, match="incompatible identity"):
        store.build_date("2026-03-02", [source])


def test_source_stat_change_conservatively_invalidates_cache(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    source = CompactSource("stock", (raw,), ("0050",))
    store.build_date("2026-03-02", [source])
    stat = raw.stat()
    os.utime(raw, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1))
    with pytest.raises(CompactCacheError, match="incompatible identity"):
        store.build_date("2026-03-02", [source])


def test_reference_adapter_matches_direct_bbo_conversion(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    source = CompactSource("stock", (raw,), ("0050",))
    store.build_date("2026-03-02", [source])
    compact = store.read_symbol("2026-03-02", "stock", "0050")
    compact_events, _ = compact_to_reference_events(compact, trade_date="2026-03-02")

    direct_raw = pl.read_parquet(raw).filter(pl.col("symbol") == "0050").sort(
        ["exchtime", "localtime"], maintain_order=True
    )
    direct = direct_raw.select(
        "exchtime",
        "localtime",
        "last_price",
        "total_volume",
        pl.max_horizontal("bid_price1", "bid_price2", "bid_price3", "bid_price4", "bid_price5").alias("bid_price1"),
        pl.sum_horizontal(
            *[
                pl.when(
                    pl.col(f"bid_price{level}")
                    == pl.max_horizontal("bid_price1", "bid_price2", "bid_price3", "bid_price4", "bid_price5")
                )
                .then(pl.col(f"bid_volume{level}"))
                .otherwise(0.0)
                for level in range(1, 6)
            ]
        ).alias("bid_volume1"),
        pl.min_horizontal("ask_price1", "ask_price2", "ask_price3", "ask_price4", "ask_price5").alias("ask_price1"),
        pl.sum_horizontal(
            *[
                pl.when(
                    pl.col(f"ask_price{level}")
                    == pl.min_horizontal("ask_price1", "ask_price2", "ask_price3", "ask_price4", "ask_price5")
                )
                .then(pl.col(f"ask_volume{level}"))
                .otherwise(0.0)
                for level in range(1, 6)
            ]
        ).alias("ask_volume1"),
    )
    args = type(
        "Args",
        (),
        {
            "levels": 1,
            "timestamp_unit": "ns",
            "timezone": "Asia/Taipei",
            "date": "2026-03-02",
            "base_latency_ns": 0,
            "volume_scale": 1.0,
            "price_only_depth_qty": None,
            "trade_side": "infer",
            "no_trades": False,
            "no_depth": False,
            "qa_sample_rows": 1000,
        },
    )()
    direct_events, _ = build_events_from_parquet_frame(direct, args)
    np.testing.assert_array_equal(compact_events, direct_events)
