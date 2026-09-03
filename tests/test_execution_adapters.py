from __future__ import annotations

import ast
from dataclasses import replace
import math
import pickle
from pathlib import Path
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from future_spot.arbitrage.execution_port import ExecutionInvariantError, ExecutionOrder
from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
from future_spot.arbitrage.hbt_types import HbtPairBacktestConfig
from future_spot.arbitrage.models import PairConfig
from future_spot.arbitrage.reference_execution import ReferenceExecutionAdapter
from future_spot.arbitrage.slim_execution import SlimExecutionAdapter
from hftbacktest_slim import (
    AssetConfig,
    BBO_SCHEMA,
    EngineClosedError,
    OrderStatus,
    Side,
    TimeInForce,
)
from scripts.compact_hbt_adapter import compact_to_reference_events
from scripts.hbt_types import HbtAssetConfig
from scripts.strategy_api import StrategyDecision
from scripts.tw_stock_hftbacktest import import_hftbacktest


def _compact(path: Path, rows: list[tuple]) -> pa.Table:
    table = pa.Table.from_pylist(
        [dict(zip(BBO_SCHEMA.names, row)) for row in rows], schema=BBO_SCHEMA
    ).replace_schema_metadata(
        {b"schema_version": b"bbo_v1", b"local_timestamp_adjustment_ns": b"0"}
    )
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return table


def _configs(tmp_path: Path) -> tuple[HbtAssetConfig, HbtAssetConfig, pa.Table, pa.Table]:
    spot_path = tmp_path / "spot.arrow"
    future_path = tmp_path / "future.arrow"
    spot_table = _compact(
        spot_path,
        [
            (0, 100, 110, 99.0, 100.0, 10.0, 10.0, 100.0, 1),
            (1, 200, 210, 100.0, 101.0, 10.0, 10.0, 100.0, 2),
        ],
    )
    future_table = _compact(
        future_path,
        [
            (0, 100, 110, 109.0, 110.0, 1.0, 1.0, 110.0, 1),
            (1, 200, 210, 110.0, 111.0, 1.0, 1.0, 110.0, 2),
        ],
    )
    return (
        HbtAssetConfig(
            "SPOT",
            spot_path,
            "stock",
            1000.0,
            tick_size=1.0,
            feed_latency_offset_ns=1,
            order_entry_latency_ns=3,
            order_response_latency_ns=4,
        ),
        HbtAssetConfig(
            "FUT",
            future_path,
            "future",
            1000.0,
            tick_size=1.0,
            order_entry_latency_ns=2,
            order_response_latency_ns=3,
        ),
        spot_table,
        future_table,
    )


def test_slim_adapter_construction_mapping_order_and_close(tmp_path: Path) -> None:
    spot, future, _, _ = _configs(tmp_path)
    adapter, ticks = SlimExecutionAdapter.open(
        spot, future, spot_tick_size=1.0, future_tick_size=1.0
    )
    assert ticks == {"spot_tick_size": 1.0, "future_tick_size": 1.0}
    assert tuple(asset.symbol for asset in adapter._backend._assets) == ("SPOT", "FUT")
    assert adapter._backend._assets[0] == SlimExecutionAdapter.asset_config(spot, 1.0)
    assert adapter.resolve_side("buy") is Side.BUY
    assert adapter.resolve_side("sell") is Side.SELL
    assert adapter.resolve_time_in_force("FOK") is TimeInForce.FOK
    assert adapter.resolve_time_in_force("ioc") is TimeInForce.IOC

    assert adapter.advance(11)
    depth = adapter.depth(0)
    assert depth.best_ask == 100.0
    assert depth.ask_qty_at_tick(100) == 10.0
    assert depth.ask_qty_at_tick(101) == 0.0
    assert adapter.submit_limit(0, 17, "buy", 100.0, 11.0, "FOK") == 0
    assert adapter.wait_order_response(0, 17, 10) == 0
    order = adapter.order(0, 17)
    assert order is not None
    assert order.asset_no == 0
    assert order.status == int(OrderStatus.FILLED)
    assert order.exec_qty == 11.0
    assert order.exec_qty > depth.ask_quantity
    assert adapter.order_latency(0) == (111, 114, 118)
    assert adapter.feed_latency(0) == (100, 111)
    assert not adapter.advance(1_000)

    adapter.close()
    adapter.close()
    with pytest.raises(EngineClosedError):
        _ = adapter.current_timestamp


def test_slim_adapter_rejects_unsupported_tif_and_active_order() -> None:
    pair = PairConfig(
        "x",
        "S",
        "F",
        1,
        1,
        1,
        1,
        1,
        0.01,
        0.0,
        -1.0,
        first_leg_time_in_force="ROD",
        second_leg_time_in_force="FOK",
        flatten_first_leg_time_in_force="IOC",
    )
    with pytest.raises(ValueError, match="first_leg_time_in_force='ROD'"):
        SlimExecutionAdapter.validate_pair(pair)
    with pytest.raises(ValueError, match="FOK/IOC"):
        SlimExecutionAdapter.validate_time_in_force("GTC")
    active = ExecutionOrder(1, 0, int(OrderStatus.NEW), math.nan, 0.0, 1.0, 1, 1)
    assert SlimExecutionAdapter.order_is_active(active)
    adapter = object.__new__(SlimExecutionAdapter)
    with pytest.raises(ExecutionInvariantError, match="passive cancellation is unsupported"):
        adapter.cancel_active_order(0, 1, 10)


def test_neutral_asset_configuration_is_picklable(tmp_path: Path) -> None:
    config = AssetConfig("S", tmp_path / "s.arrow", 0.5, 1, 2, 3)
    assert pickle.loads(pickle.dumps(config)) == config


def test_reference_adapter_construction_mapping_and_close(tmp_path: Path) -> None:
    spot, future, spot_table, future_table = _configs(tmp_path)
    spot_events, _ = compact_to_reference_events(spot_table, trade_date="2026-03-02")
    future_events, _ = compact_to_reference_events(future_table, trade_date="2026-03-02")
    spot_npz = tmp_path / "spot.npz"
    future_npz = tmp_path / "future.npz"
    np.savez(spot_npz, data=spot_events)
    np.savez(future_npz, data=future_events)
    spot = HbtAssetConfig(**{**spot.__dict__, "data": spot_npz, "feed_latency_offset_ns": 0})
    future = HbtAssetConfig(**{**future.__dict__, "data": future_npz})
    hbtpkg = import_hftbacktest(Path(__file__).resolve().parents[1])

    adapter, ticks = ReferenceExecutionAdapter.open(spot, future, hbtpkg=hbtpkg)
    assert ticks == {"spot_tick_size": 1.0, "future_tick_size": 1.0}
    assert adapter.scanner_backend is not None
    assert adapter.resolve_side("buy") == int(hbtpkg.BUY)
    assert adapter.resolve_side("sell") == int(hbtpkg.SELL)
    assert adapter.resolve_time_in_force("ROD") == int(hbtpkg.GTC)
    assert adapter.resolve_time_in_force("FOK") != int(hbtpkg.GTC)
    with pytest.raises(ValueError, match="unknown HftBacktest time_in_force"):
        adapter.resolve_time_in_force("mystery")

    assert adapter.advance(10)
    assert adapter.submit_limit(0, 23, "buy", 100.0, 1.0, "FOK") == 0
    assert adapter.wait_order_response(0, 23, 20) == 0
    order = adapter.order(0, 23)
    assert order is not None
    assert order.asset_no == 0
    assert order.exec_qty == 1.0
    assert not adapter.order_is_active(order)
    adapter.clear_inactive_orders(0)
    adapter.close()
    adapter.close()


def test_active_future_spot_slim_path_uses_only_the_neutral_root_api() -> None:
    root = Path(__file__).resolve().parents[1] / "future_spot" / "arbitrage"
    selected = [
        root / "hbt_backtest.py",
        root / "execution_port.py",
        root / "slim_execution.py",
    ]
    package_imports: list[str] = []
    for path in selected:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            for module in modules:
                if module.partition(".")[0] == "hftbacktest_slim":
                    package_imports.append(module)
    assert package_imports == ["hftbacktest_slim"]


def test_reference_pair_module_import_does_not_load_slim_package() -> None:
    root = Path(__file__).resolve().parents[1]
    code = """
import sys
from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
assert HbtPairBacktester
assert not any(name == 'hftbacktest_slim' or name.startswith('hftbacktest_slim.') for name in sys.modules)
"""
    subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        check=True,
        text=True,
        capture_output=True,
    )


def test_custom_strategy_remains_python_reference_compatible(tmp_path: Path) -> None:
    spot, future, spot_table, future_table = _configs(tmp_path)
    spot_events, _ = compact_to_reference_events(spot_table, trade_date="2026-03-02")
    future_events, _ = compact_to_reference_events(future_table, trade_date="2026-03-02")
    spot_npz = tmp_path / "custom-spot.npz"
    future_npz = tmp_path / "custom-future.npz"
    np.savez(spot_npz, data=spot_events)
    np.savez(future_npz, data=future_events)
    pair = PairConfig("custom", "SPOT", "FUT", 1, 1, 1, 1, 1, 9.0, 0.0, -1.0)
    config = HbtPairBacktestConfig(
        pair=pair,
        spot=HbtAssetConfig(**{**spot.__dict__, "data": spot_npz, "feed_latency_offset_ns": 0}),
        future=HbtAssetConfig(**{**future.__dict__, "data": future_npz}),
        execution_engine="reference",
        strategy_engine="python",
        step_ns=10,
        max_steps=1,
    )

    class HoldStrategy:
        name = "custom-hold"

        def __init__(self) -> None:
            self.calls = 0

        def decide(self, _context):
            self.calls += 1
            return StrategyDecision("HOLD", reason="custom")

    strategy = HoldStrategy()
    trades, summary = HbtPairBacktester(
        config,
        hbtpkg=import_hftbacktest(Path(__file__).resolve().parents[1]),
        strategy=strategy,
    ).run()
    assert trades.empty
    assert strategy.calls == 1
    assert summary.loc[0, "python_decisions"] == 1

    with pytest.raises(ValueError, match="only the default future/spot strategy"):
        HbtPairBacktester(
            replace(config, strategy_engine="numba"),
            hbtpkg=import_hftbacktest(Path(__file__).resolve().parents[1]),
            strategy=HoldStrategy(),
        ).run()
