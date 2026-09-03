from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from hftbacktest_slim import (
    BBO_SCHEMA,
    COMPACT_BUILDER_VERSION,
    CompactBuildConfig,
    CompactCacheBudgetError,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
)
from hftbacktest_slim.cache import builder
from hftbacktest_slim.cache import store as store_module
from hftbacktest_slim.cache.manifest import canonical_sha256, file_sha256
from hftbacktest_slim.cache.manifest import implementation_paths
from hftbacktest_slim.cache.publication import cleanup_incomplete_date


def _source(path: Path, *, independent_orders: bool = False) -> None:
    exchange = [300, 100, 200, 100]
    local = [100, 300, 200, 80] if independent_orders else [280, 120, 220, 80]
    rows: dict[str, list] = {
        "symbol": ["0050", "0050", "0050", "2330"],
        "exchtime": exchange,
        "localtime": local,
        "status": ["OK"] * 4,
        "last_price": [78.00, 77.90, 78.05, 1000.0],
        "total_volume": [30, 10, 20, 1],
    }
    for level in range(1, 6):
        rows[f"bid_price{level}"] = [
            [77.95, 77.95, 77.90, None, None][level - 1],
            [77.85, 77.90, None, None, None][level - 1],
            [78.00, None, None, None, None][level - 1],
            [999.0, None, None, None, None][level - 1],
        ]
        rows[f"ask_price{level}"] = [
            [78.10, 78.00, 78.00, None, None][level - 1],
            [78.00, None, None, None, None][level - 1],
            [78.10, None, None, None, None][level - 1],
            [1001.0, None, None, None, None][level - 1],
        ]
        rows[f"bid_volume{level}"] = [
            [2.0, 3.0, 4.0, None, None][level - 1],
            [4.0, 6.0, None, None, None][level - 1],
            [7.0, None, None, None, None][level - 1],
            [1.0, None, None, None, None][level - 1],
        ]
        rows[f"ask_volume{level}"] = [
            [2.0, 3.0, 4.0, None, None][level - 1],
            [8.0, None, None, None, None][level - 1],
            [9.0, None, None, None, None][level - 1],
            [1.0, None, None, None, None][level - 1],
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


def test_cold_multi_symbol_build_scans_once_and_warm_reuse_scans_zero(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    source = CompactSource("stock", (raw,), ("0050", "2330", "9999"))
    with patch.object(
        builder, "iter_source_batches", wraps=builder.iter_source_batches
    ) as scan:
        cold = store.build_date("2026-03-02", [source])
        warm = store.build_date("2026-03-02", [source])
    assert scan.call_count == 1
    assert cold["cache_state"] == "miss"
    assert cold["build_invocation_scan_count"] == 1
    assert warm["cache_state"] == "hit"
    assert warm["build_invocation_scan_count"] == 0
    assert cold["sources"]["stock"]["empty_symbols"] == ["9999"]
    assert store.read_symbol("2026-03-02", "stock", "9999").schema.remove_metadata() == BBO_SCHEMA


def test_each_required_source_scans_once(tmp_path: Path) -> None:
    stock = tmp_path / "stock.parquet"
    future = tmp_path / "future.parquet"
    _source(stock)
    _source(future)
    store = _store(tmp_path)
    with patch.object(
        builder, "iter_source_batches", wraps=builder.iter_source_batches
    ) as scan:
        manifest = store.build_date(
            "2026-03-02",
            [
                CompactSource("stock", (stock,), ("0050", "2330")),
                CompactSource("stock_future", (future,), ("0050", "2330")),
            ],
        )
    assert scan.call_count == 2
    assert manifest["sources"]["stock"]["scan_count"] == 1
    assert manifest["sources"]["stock_future"]["scan_count"] == 1
    assert manifest["build_invocation_scan_count"] == 2


def test_duplicate_physical_source_is_rejected_before_any_payload_scan(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    with patch.object(
        builder, "iter_source_batches", wraps=builder.iter_source_batches
    ) as scan:
        with pytest.raises(CompactCacheError, match="at most once"):
            store.build_date(
                "2026-03-02",
                [
                    CompactSource("stock", (raw,), ("0050",)),
                    CompactSource("stock_future", (raw,), ("2330",)),
                ],
            )
    assert scan.call_count == 0


@pytest.mark.parametrize("compression", ["none", "lz4", "zstd"])
def test_all_supported_ipc_compressions_round_trip(
    tmp_path: Path, compression: str
) -> None:
    raw = tmp_path / f"{compression}.parquet"
    _source(raw)
    store = CompactCacheStore(
        CompactBuildConfig(
            cache_root=tmp_path / f"cache-{compression}",
            compression=compression,
            batch_rows=2,
            max_cache_bytes=1024**3,
            min_free_bytes=0,
        )
    )
    manifest = store.build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
    )
    assert manifest["identity"]["compression"] == compression
    assert store.read_symbol("2026-03-02", "stock", "0050").num_rows == 3


def test_symbols_receive_independent_latency_adjustments_and_sidecar_orders(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw, independent_orders=True)
    store = _store(tmp_path, base_latency_ns=5)
    manifest = store.build_date(
        "2026-03-02",
        [CompactSource("stock", (raw,), ("0050", "2330"))],
    )
    details = manifest["sources"]["stock"]["symbols"]
    assert details["0050"]["local_timestamp_adjustment_ns"] == 205
    assert details["2330"]["local_timestamp_adjustment_ns"] == 25
    assert set(details["0050"]["sidecars"]) == {
        "exchange_order",
        "local_order",
    }
    source_dir = store.date_path("2026-03-02") / "source=stock"
    with pa.memory_map(
        str(source_dir / details["0050"]["sidecars"]["exchange_order"]["file"]),
        "r",
    ) as handle:
        exchange_order = ipc.open_file(handle).read_all()["row_index"].to_pylist()
    with pa.memory_map(
        str(source_dir / details["0050"]["sidecars"]["local_order"]["file"]),
        "r",
    ) as handle:
        local_order = ipc.open_file(handle).read_all()["row_index"].to_pylist()
    assert exchange_order == [1, 2, 0]
    assert local_order == [0, 2, 1]


def test_date_manifest_is_the_last_staged_write(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    calls: list[Path] = []
    original_builder_write = builder.write_json
    original_store_write = store_module.write_json

    def builder_write(path, payload):
        calls.append(path)
        return original_builder_write(path, payload)

    def store_write(path, payload):
        calls.append(path)
        return original_store_write(path, payload)

    with patch.object(builder, "write_json", side_effect=builder_write), patch.object(
        store_module, "write_json", side_effect=store_write
    ):
        store.build_date(
            "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
        )
    assert calls[-1].name == "manifest.json"
    assert calls[-1].parent.name.startswith(".tmp-2026-03-02-")


def test_builder_v1_and_changed_implementation_never_validate_as_v2(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    store.build_date("2026-03-02", [CompactSource("stock", (raw,), ("0050",))])
    manifest_path = store.date_path("2026-03-02") / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["builder_version"] = 1
    payload["identity"]["builder_version"] = 1
    payload["identity_sha256"] = canonical_sha256(payload["identity"])
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(CompactCacheError, match="builder version"):
        store.validate_date("2026-03-02")

    payload["builder_version"] = COMPACT_BUILDER_VERSION
    payload["identity"]["builder_version"] = COMPACT_BUILDER_VERSION
    payload["identity_sha256"] = canonical_sha256(payload["identity"])
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with patch.object(store_module, "implementation_fingerprint", return_value="changed"):
        with pytest.raises(CompactCacheError, match="implementation identity"):
            store.validate_date("2026-03-02")


def test_source_stat_change_invalidates_without_an_extra_payload_scan(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    source = CompactSource("stock", (raw,), ("0050",))
    store.build_date("2026-03-02", [source])
    stat = raw.stat()
    os.utime(raw, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1))
    with patch.object(
        builder, "iter_source_batches", wraps=builder.iter_source_batches
    ) as scan:
        with pytest.raises(CompactCacheError, match="incompatible identity"):
            store.build_date("2026-03-02", [source])
    assert scan.call_count == 0


def test_corrupted_partition_and_semantically_corrupted_sidecar_are_rejected(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw, independent_orders=True)
    store = _store(tmp_path)
    manifest = store.build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
    )
    details = manifest["sources"]["stock"]["symbols"]["0050"]
    source_dir = store.date_path("2026-03-02") / "source=stock"
    sidecar_details = details["sidecars"]["exchange_order"]
    sidecar_path = source_dir / sidecar_details["file"]
    wrong = pa.table({"row_index": pa.array([0, 1, 2], type=pa.uint64())})
    builder.write_arrow(sidecar_path, wrong, "lz4")
    sidecar_details["bytes"] = sidecar_path.stat().st_size
    sidecar_details["sha256"] = file_sha256(sidecar_path)
    source_manifest_path = source_dir / "manifest.json"
    source_manifest_path.write_text(json.dumps(manifest["sources"]["stock"]), encoding="utf-8")
    top_path = store.date_path("2026-03-02") / "manifest.json"
    top_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(CompactCacheError, match="sidecar ordering"):
        store.validate_date("2026-03-02")


def test_corrupted_symbol_partition_is_rejected(tmp_path: Path) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    manifest = store.build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
    )
    details = manifest["sources"]["stock"]["symbols"]["0050"]
    path = store.date_path("2026-03-02") / "source=stock" / details["file"]
    path.write_bytes(b"corrupt")
    with pytest.raises(CompactCacheError, match="changed compact symbol"):
        store.validate_date("2026-03-02")


def test_manifest_partition_facts_are_recomputed_from_compact_rows(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    store = _store(tmp_path)
    manifest = store.build_date(
        "2026-03-02", [CompactSource("stock", (raw,), ("0050",))]
    )
    details = manifest["sources"]["stock"]["symbols"]["0050"]
    details["min_price"] = -1.0
    source_dir = store.date_path("2026-03-02") / "source=stock"
    (source_dir / "manifest.json").write_text(
        json.dumps(manifest["sources"]["stock"]), encoding="utf-8"
    )
    (store.date_path("2026-03-02") / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    with pytest.raises(CompactCacheError, match="partition fact mismatch"):
        store.validate_date("2026-03-02")


def test_preflight_and_runtime_failures_never_delete_completed_cache(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    source = CompactSource("stock", (raw,), ("0050",))
    original = _store(tmp_path)
    original_manifest = original.build_date("2026-03-02", [source])

    too_small = _store(
        tmp_path, max_cache_bytes=1, rebuild=True, compression="zstd"
    )
    with pytest.raises(CompactCacheBudgetError):
        too_small.build_date("2026-03-02", [source])
    assert original.validate_date("2026-03-02")["identity_sha256"] == original_manifest[
        "identity_sha256"
    ]

    rebuilding = _store(tmp_path, rebuild=True, compression="zstd")
    with patch.object(
        builder,
        "runtime_budget_check",
        side_effect=CompactCacheBudgetError("during-build budget"),
    ):
        with pytest.raises(CompactCacheBudgetError, match="during-build"):
            rebuilding.build_date("2026-03-02", [source])
    assert original.validate_date("2026-03-02")["identity_sha256"] == original_manifest[
        "identity_sha256"
    ]
    assert not list((tmp_path / "cache").glob(".tmp-2026-03-02-*"))


def test_free_space_preflight_and_incomplete_publication_are_not_reused(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    source = CompactSource("stock", (raw,), ("0050",))
    reserve_store = _store(tmp_path, min_free_bytes=2**63 - 1)
    with pytest.raises(CompactCacheBudgetError, match="reserve"):
        reserve_store.build_date("2026-03-02", [source])
    assert not reserve_store.date_path("2026-03-02").exists()

    incomplete = _store(tmp_path)
    final = incomplete.date_path("2026-03-02")
    final.mkdir(parents=True)
    (final / "manifest.json").write_text(
        json.dumps({"build_complete": False}), encoding="utf-8"
    )
    with pytest.raises(CompactCacheError, match="incompatible identity"):
        incomplete.build_date("2026-03-02", [source])


def test_explicit_rebuild_is_recoverable_and_preserves_superseded_date(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "daily.parquet"
    _source(raw)
    source = CompactSource("stock", (raw,), ("0050",))
    _store(tmp_path).build_date("2026-03-02", [source])
    rebuilt = _store(tmp_path, rebuild=True, compression="zstd").build_date(
        "2026-03-02", [source]
    )
    assert rebuilt["identity"]["compression"] == "zstd"
    assert len(list((tmp_path / "cache").glob(".superseded-date=20260302-*"))) == 1


def test_temporary_cleanup_refuses_non_build_or_broad_targets(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    root.mkdir()
    completed = root / "date=20260302"
    completed.mkdir()
    with pytest.raises(CompactCacheError, match="refusing broad"):
        cleanup_incomplete_date(root, completed, "2026-03-02")
    assert completed.is_dir()


def test_implementation_paths_cover_all_sorted_cache_and_market_modules() -> None:
    paths = implementation_paths()
    relative = [
        path.relative_to(Path(__file__).resolve().parents[1] / "src" / "hftbacktest_slim")
        .as_posix()
        for path in paths
    ]
    expected = sorted(
        [
            path.relative_to(Path(__file__).resolve().parents[1] / "src" / "hftbacktest_slim")
            .as_posix()
            for folder in ("cache", "market_data")
            for path in (
                Path(__file__).resolve().parents[1] / "src" / "hftbacktest_slim" / folder
            ).rglob("*.py")
        ]
    )
    assert relative == expected
