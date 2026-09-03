from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

from hftbacktest_slim import (
    AssetConfig,
    BBO_SCHEMA,
    OrderStatus,
    Side,
    SlimEngine,
    TimeInForce,
)


def _write(path: Path, rows: list[tuple]) -> None:
    table = pa.Table.from_pylist(
        [dict(zip(BBO_SCHEMA.names, row)) for row in rows], schema=BBO_SCHEMA
    ).replace_schema_metadata({b"schema_version": b"bbo_v1", b"local_timestamp_adjustment_ns": b"0"})
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)


def test_python_binding_exposes_latency_and_immediate_fill(tmp_path: Path) -> None:
    left = tmp_path / "left.arrow"
    right = tmp_path / "right.arrow"
    _write(left, [(0, 100, 110, 99.0, 101.0, 2.0, 1.0, 100.0, 1)])
    _write(right, [(0, 100, 110, 199.0, 201.0, 2.0, 1.0, 200.0, 1)])
    assets = [
        AssetConfig("A", left, 1.0, order_entry_latency_ns=3, order_response_latency_ns=4),
        AssetConfig("B", right, 1.0),
    ]
    with SlimEngine(assets) as engine:
        assert engine.advance(10)
        assert engine.depth(0).best_ask == 101.0
        latency = engine.feed_latency(0)
        assert latency is not None
        assert (latency.exchange_timestamp_ns, latency.local_timestamp_ns) == (100, 110)
        engine.submit_order(
            asset_no=0,
            order_id=7,
            side=Side.BUY,
            price=101.0,
            quantity=10.0,
            time_in_force=TimeInForce.FOK,
        )
        assert engine.wait_order_response(0, 7, 10)
        order = engine.order(0, 7)
        assert order is not None
        assert order.status is OrderStatus.FILLED
        assert order.execution_price == 101.0
        assert order.execution_quantity == 10.0
        assert order.request_local_timestamp_ns == 110
        assert order.exchange_timestamp_ns == 113
        order_latency = engine.order_latency(0)
        assert order_latency is not None
        assert (
            order_latency.request_local_timestamp_ns,
            order_latency.exchange_timestamp_ns,
            order_latency.response_local_timestamp_ns,
        ) == (110, 113, 117)


def test_python_binding_accepts_valid_empty_asset_partition(tmp_path: Path) -> None:
    left = tmp_path / "left.arrow"
    right = tmp_path / "right.arrow"
    _write(left, [(0, 100, 110, 99.0, 101.0, 2.0, 1.0, 100.0, 1)])
    _write(right, [])
    with SlimEngine(
        [
            AssetConfig("A", left, 1.0),
            AssetConfig("B", right, 1.0),
        ]
    ) as engine:
        assert engine.advance(10)
        assert engine.depth(0).best_ask == 101.0
        assert not engine.depth(1).valid
        assert engine.feed_latency(1) is None
