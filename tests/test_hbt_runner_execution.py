from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

from future_spot.arbitrage.full_market_runner import (
    DailyPairRecord,
    _hbt_implementation_paths,
    balanced_backtest_shards,
    build_compact_event_data,
    compact_asset_audit,
    run_backtests,
)
from future_spot.arbitrage.models import PairConfig
from hftbacktest_slim import BBO_SCHEMA


def _pair(name: str) -> PairConfig:
    return PairConfig(
        name=name,
        spot_symbol=f"S{name}",
        future_symbol=f"F{name}",
        spot_shares_per_pair=1,
        future_shares_per_pair=1,
        spot_order_qty=1,
        future_order_qty=1,
        future_pnl_multiplier=1,
        entry_threshold_pct=0.1,
        exit_threshold_pct=0.0,
        stop_loss_pct=-0.1,
    )


class InlineExecutor:
    def __init__(self) -> None:
        self.submissions = 0

    def submit(self, function, *args):
        self.submissions += 1
        future = Future()
        try:
            future.set_result(function(*args))
        except Exception as exc:  # pragma: no cover - mirrors Executor behavior
            future.set_exception(exc)
        return future


class PersistentExecutorTest(unittest.TestCase):
    def test_strategy_compact_audit_combines_package_facts_with_tick_rules(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "future.arrow"
            table = pa.Table.from_pylist(
                [
                    dict(
                        zip(
                            BBO_SCHEMA.names,
                            (0, 100, 90, 499.5, 500.0, 1.0, 1.0, 499.5, 1),
                        )
                    ),
                    dict(
                        zip(
                            BBO_SCHEMA.names,
                            (1, 110, 105, 500.0, 500.5, 1.0, 1.0, 500.0, 2),
                        )
                    ),
                ],
                schema=BBO_SCHEMA,
            ).replace_schema_metadata(
                {
                    b"schema_version": b"bbo_v1",
                    b"local_timestamp_adjustment_ns": b"10",
                }
            )
            with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)

            tick, summary = compact_asset_audit(path, "future", "2026-05-26")

        self.assertEqual(tick, 0.5)
        self.assertEqual(
            summary,
            {
                "rows": 2,
                "first_exch_ts": 100,
                "last_exch_ts": 110,
                "min_latency_ns": 0,
                "max_latency_ns": 5,
                "depth_events": None,
                "trade_events": 1,
            },
        )

    def test_manifest_fingerprints_every_sorted_relocated_native_source(self) -> None:
        native_root = Path(__file__).resolve().parents[1] / "hftbacktest_slim" / "native"
        expected = [
            native_root / "Cargo.toml",
            *sorted((native_root / "src").rglob("*.rs"), key=lambda path: path.as_posix()),
        ]
        selected = [
            path
            for path in _hbt_implementation_paths()
            if path.is_relative_to(native_root)
        ]

        self.assertEqual(selected, expected)
        self.assertTrue(all(path.is_file() for path in selected))
        self.assertNotIn(
            Path(__file__).resolve().parents[1] / "crates" / "hbt_slim" / "src" / "lib.rs",
            _hbt_implementation_paths(),
        )

    def test_manifest_fingerprints_package_owned_python_runtime(self) -> None:
        package_root = (
            Path(__file__).resolve().parents[1]
            / "hftbacktest_slim"
            / "src"
            / "hftbacktest_slim"
        )
        expected = sorted(
            [
                *(package_root / "engine").rglob("*.py"),
                *(package_root / "cache").rglob("*.py"),
                *(package_root / "market_data").rglob("*.py"),
                package_root / "__init__.py",
                package_root / "api.py",
                package_root / "config.py",
                package_root / "enums.py",
                package_root / "errors.py",
                package_root / "models.py",
                package_root / "version.py",
            ],
            key=lambda path: path.as_posix(),
        )
        selected = [path for path in _hbt_implementation_paths() if path in expected]

        self.assertEqual(selected, expected)
        self.assertTrue(all(path.is_file() for path in selected))
        implementation_paths = _hbt_implementation_paths()
        self.assertTrue(all(path.is_file() for path in implementation_paths))
        arbitrage_root = Path(__file__).resolve().parents[1] / "future_spot" / "arbitrage"
        for name in (
            "capital.py",
            "config.py",
            "execution_port.py",
            "reference_execution.py",
            "slim_execution.py",
            "hbt_backtest.py",
            "hbt_helpers.py",
            "hbt_numba.py",
            "hbt_rows.py",
            "hbt_types.py",
            "models.py",
            "position_carry.py",
            "strategy.py",
            "strategy_adapter.py",
            "ticks.py",
            "utils.py",
        ):
            self.assertIn(arbitrage_root / name, _hbt_implementation_paths())
        scripts_root = Path(__file__).resolve().parents[1] / "scripts"
        for name in (
            "daily_result_store.py",
            "hbt_common.py",
            "hbt_types.py",
            "io_utils.py",
            "strategy_api.py",
        ):
            self.assertIn(scripts_root / name, _hbt_implementation_paths())

        reference_event_paths = _hbt_implementation_paths("reference", "event_npz")
        self.assertFalse(
            any(path.is_relative_to(package_root / "engine") for path in reference_event_paths)
        )
        self.assertNotIn(
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "compact_hbt_adapter.py",
            _hbt_implementation_paths("slim", "compact"),
        )
        self.assertIn(
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "compact_hbt_adapter.py",
            _hbt_implementation_paths("reference", "compact"),
        )

    def test_compact_missing_date_preserves_reference_error_path(self) -> None:
        record = DailyPairRecord(
            "2026-07-10",
            "2026-07-10::carry",
            _pair("carry"),
            Path("carry.json"),
        )
        args = SimpleNamespace(
            stock_tick_parquet_template="/missing/stock_{date_nodash}.parquet",
            event_futures_parquet_dir=Path("/missing/futures"),
            session_start="09:00:00",
            session_end="13:25:00",
            compact_cache_root=Path("/unused/cache"),
            compact_cache_compression="lz4",
            compact_cache_profile="bbo",
            compact_cache_batch_rows=1024,
            compact_cache_max_gb=1.0,
            compact_cache_min_free_gb=0.0,
            rebuild_compact_cache=False,
            continue_on_error=True,
            engine="slim",
        )
        with patch(
            "future_spot.arbitrage.full_market_runner.CompactCacheStore.build_date",
            side_effect=FileNotFoundError("missing date source"),
        ):
            paths, audit = build_compact_event_data(args, [record])
        self.assertFalse(paths)
        self.assertEqual(audit["compact_cache_state"].tolist(), ["error"])
        self.assertEqual(audit["compact_build_invocation_scan_count"].tolist(), [0])
        self.assertIn("missing date source", audit.loc[0, "future_error"])

    def test_compact_build_routes_multiple_pairs_through_one_source_per_kind(self) -> None:
        records = [
            DailyPairRecord(
                "2026-07-10",
                f"2026-07-10::{name}",
                _pair(name),
                Path(f"{name}.json"),
            )
            for name in ("a", "b")
        ]
        args = SimpleNamespace(
            stock_tick_parquet_template="/data/stock_{date_nodash}.parquet",
            event_futures_parquet_dir=Path("/data/futures"),
            session_start="09:00:00",
            session_end="13:25:00",
            compact_cache_root=Path("/cache"),
            compact_cache_compression="lz4",
            compact_cache_profile="bbo",
            compact_cache_batch_rows=1024,
            compact_cache_max_gb=1.0,
            compact_cache_min_free_gb=0.0,
            rebuild_compact_cache=False,
            continue_on_error=False,
            engine="slim",
        )
        symbols = {
            "stock": {f"S{name}": {"status": "valid", "file": f"S{name}.arrow"} for name in ("a", "b")},
            "stock_future": {f"F{name}": {"status": "valid", "file": f"F{name}.arrow"} for name in ("a", "b")},
        }
        manifest = {
            "cache_state": "miss",
            "identity_sha256": "identity",
            "build_invocation_scan_count": 2,
            "sources": {
                kind: {"symbols": values} for kind, values in symbols.items()
            },
        }
        with patch(
            "future_spot.arbitrage.full_market_runner.CompactCacheStore.build_date",
            return_value=manifest,
        ) as build:
            paths, audit = build_compact_event_data(args, records)

        self.assertEqual(build.call_count, 1)
        sources = build.call_args.args[1]
        self.assertEqual([(source.kind, source.symbols) for source in sources], [
            ("stock", ("Sa", "Sb")),
            ("stock_future", ("Fa", "Fb")),
        ])
        self.assertEqual(set(paths), {record.run_key for record in records})
        self.assertEqual(audit["compact_build_invocation_scan_count"].tolist(), [2, 2])

    def test_run_backtests_uses_caller_owned_executor(self) -> None:
        records = [
            DailyPairRecord("2026-03-02", f"2026-03-02::{name}", _pair(name), Path(f"{name}.json"))
            for name in ("a", "b")
        ]
        paths = {
            record.run_key: {"spot": Path(f"{record.pair.name}-s.npz"), "future": Path(f"{record.pair.name}-f.npz")}
            for record in records
        }
        args = SimpleNamespace(workers=2, continue_on_error=False)
        executor = InlineExecutor()

        def result(_args, record, _paths):
            return {
                "summary": pd.DataFrame({"run_key": [record.run_key]}),
                "trades": pd.DataFrame(),
                "market": pd.DataFrame(),
                "latency": pd.DataFrame(),
            }

        with patch(
            "future_spot.arbitrage.full_market_runner._run_single_pair_backtest",
            side_effect=result,
        ), patch(
            "future_spot.arbitrage.full_market_runner.ProcessPoolExecutor"
        ) as pool_class:
            completed, summary, *_ = run_backtests(
                args,
                records,
                paths,
                executor=executor,  # type: ignore[arg-type]
            )

        pool_class.assert_not_called()
        self.assertEqual(executor.submissions, 2)
        self.assertEqual(set(completed), {record.run_key for record in records})
        self.assertEqual(len(summary), 2)

    def test_balanced_shards_use_combined_leg_event_rows(self) -> None:
        records = [
            DailyPairRecord("2026-03-02", f"2026-03-02::{name}", _pair(name), Path(f"{name}.json"))
            for name in ("heavy", "medium", "small_a", "small_b")
        ]
        runnable = [
            (
                record,
                {
                    "spot": Path(f"{record.pair.name}-s.npz"),
                    "future": Path(f"{record.pair.name}-f.npz"),
                },
            )
            for record in records
        ]
        weights = {
            "heavy-s.npz": 45,
            "heavy-f.npz": 45,
            "medium-s.npz": 25,
            "medium-f.npz": 25,
            "small_a-s.npz": 10,
            "small_a-f.npz": 10,
            "small_b-s.npz": 10,
            "small_b-f.npz": 10,
        }

        shards = balanced_backtest_shards(runnable, 2, weights)
        totals = sorted(sum(item[2] for item in shard) for shard in shards)

        self.assertEqual(totals, [90, 90])


if __name__ == "__main__":
    unittest.main()
