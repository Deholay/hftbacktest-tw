from __future__ import annotations

import math
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

from scripts.compact_cache import BBO_SCHEMA
from scripts.hbt_types import HbtAssetConfig
from scripts.slim_engine import SlimBacktest, SlimHbtConstants


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
        HbtAssetConfig("A", left, "stock", 1.0, tick_size=1.0, order_entry_latency_ns=3, order_response_latency_ns=4),
        HbtAssetConfig("B", right, "future", 1.0, tick_size=1.0),
    ]
    hbt = SlimBacktest(assets)
    try:
        assert hbt.elapse(10) == 0
        assert hbt.depth(0).best_ask == 101.0
        assert hbt.feed_latency(0) == (100, 110)
        assert hbt.submit_buy_order(0, 7, 101.0, 10.0, SlimHbtConstants.FOK, 0, False) == 0
        assert hbt.wait_order_response(0, 7, 10) == 0
        order = hbt.orders(0).get(7)
        assert order is not None
        assert order.status == SlimHbtConstants.FILLED
        assert order.exec_price == 101.0
        assert order.exec_qty == 10.0
        assert order.local_timestamp == 110
        assert order.exch_timestamp == 113
        assert hbt.order_latency(0) == (110, 113, 117)
    finally:
        hbt.close()


def test_python_binding_accepts_valid_empty_asset_partition(tmp_path: Path) -> None:
    left = tmp_path / "left.arrow"
    right = tmp_path / "right.arrow"
    _write(left, [(0, 100, 110, 99.0, 101.0, 2.0, 1.0, 100.0, 1)])
    _write(right, [])
    hbt = SlimBacktest(
        [
            HbtAssetConfig("A", left, "stock", 1.0, tick_size=1.0),
            HbtAssetConfig("B", right, "future", 1.0, tick_size=1.0),
        ]
    )
    try:
        assert hbt.elapse(10) == 0
        assert hbt.depth(0).best_ask == 101.0
        assert math.isnan(hbt.depth(1).best_ask)
        assert hbt.feed_latency(1) is None
    finally:
        hbt.close()
