from __future__ import annotations

from pathlib import Path

import pytest

from hftbacktest_slim import (
    AbiMismatchError,
    AssetConfig,
    EngineClosedError,
    NativeLibraryNotFoundError,
    OrderStatus,
    Side,
    SlimConfigurationError,
    SlimEngine,
    TimeInForce,
)
from hftbacktest_slim.engine import replay


def _assets(
    tmp_path: Path,
    write_partition,
    *,
    entry_ns: int = 3,
    response_ns: int = 4,
    empty_right: bool = False,
) -> list[AssetConfig]:
    left = write_partition(
        tmp_path / "left.arrow",
        [(0, 100, 110, 99.0, 101.0, 0.25, 0.5, 100.0, 1)],
    )
    right_rows = [] if empty_right else [
        (0, 100, 110, 199.0, 201.0, 0.25, 0.5, 200.0, 1)
    ]
    right = write_partition(tmp_path / "right.arrow", right_rows)
    return [
        AssetConfig(
            "A",
            left,
            1.0,
            order_entry_latency_ns=entry_ns,
            order_response_latency_ns=response_ns,
        ),
        AssetConfig("B", right, 1.0),
    ]


def test_two_asset_explicit_library_context_lifecycle_and_clock(
    tmp_path: Path, write_partition, native_library_path: Path
) -> None:
    assets = _assets(tmp_path, write_partition)
    with SlimEngine.open(assets, library_path=native_library_path) as engine:
        assert engine.library_path == native_library_path.resolve()
        assert engine.current_timestamp == 100
        assert engine.advance(10) is True
        assert engine.current_timestamp == 110
        depth = engine.depth(0)
        assert depth.valid
        assert depth.best_bid == 99.0
        assert depth.best_ask == 101.0
        assert depth.best_bid_quantity == 0.25
        assert depth.best_ask_quantity == 0.5
        assert engine.feed_latency(0).latency_ns == 10  # type: ignore[union-attr]
    assert engine.closed
    engine.close()
    with pytest.raises(EngineClosedError, match="closed"):
        _ = engine.current_timestamp
    with pytest.raises(EngineClosedError, match="closed"):
        engine.depth(0)


def test_engine_requires_exactly_two_neutral_assets(
    tmp_path: Path, write_partition, native_library_path: Path
) -> None:
    assets = _assets(tmp_path, write_partition)
    with pytest.raises(SlimConfigurationError, match="exactly two"):
        SlimEngine(assets[:1], library_path=native_library_path)
    with pytest.raises(SlimConfigurationError, match="AssetConfig"):
        SlimEngine([assets[0], object()], library_path=native_library_path)  # type: ignore[list-item]


def test_neutral_construction_reports_missing_library(
    tmp_path: Path, write_partition
) -> None:
    assets = _assets(tmp_path, write_partition)
    with pytest.raises(NativeLibraryNotFoundError, match="explicit library_path"):
        SlimEngine(assets, library_path=tmp_path / "missing.so")


def test_neutral_construction_preserves_typed_abi_mismatch(
    tmp_path: Path, write_partition, monkeypatch: pytest.MonkeyPatch
) -> None:
    assets = _assets(tmp_path, write_partition)

    def mismatch(_path):
        raise AbiMismatchError("expected 1, got 99")

    monkeypatch.setattr(replay, "NativeBinding", mismatch)
    with pytest.raises(AbiMismatchError, match="expected 1, got 99"):
        SlimEngine(assets, library_path=tmp_path / "wrong.so")


@pytest.mark.parametrize("time_in_force", [TimeInForce.FOK, TimeInForce.IOC])
def test_crossing_buy_and_sell_fill_without_displayed_size_cap(
    tmp_path: Path,
    write_partition,
    native_library_path: Path,
    time_in_force: TimeInForce,
) -> None:
    engine = SlimEngine(
        _assets(tmp_path, write_partition, entry_ns=0, response_ns=0),
        library_path=native_library_path,
    )
    try:
        assert engine.advance(10)
        engine.submit_order(
            asset_no=0,
            order_id=1,
            side=Side.BUY,
            price=101.0,
            quantity=10.0,
            time_in_force=time_in_force,
        )
        assert engine.wait_order_response(0, 1, 0)
        buy = engine.order(0, 1)
        assert buy is not None
        assert buy.status is OrderStatus.FILLED
        assert buy.side is Side.BUY
        assert buy.time_in_force is time_in_force
        assert buy.requested_price == 101.0
        assert buy.requested_quantity == 10.0
        assert buy.execution_price == 101.0
        assert buy.execution_quantity == 10.0
        assert buy.leaves_quantity == 0.0
        assert buy.response_visible

        engine.submit_order(
            asset_no=0,
            order_id=2,
            side=Side.SELL,
            price=99.0,
            quantity=11.0,
            time_in_force=time_in_force,
        )
        assert engine.wait_order_response(0, 2, 0)
        sell = engine.order(0, 2)
        assert sell is not None
        assert sell.status is OrderStatus.FILLED
        assert sell.execution_price == 99.0
        assert sell.execution_quantity == 11.0
    finally:
        engine.close()


@pytest.mark.parametrize(
    ("side", "price"),
    [(Side.BUY, 100.0), (Side.SELL, 100.0)],
)
@pytest.mark.parametrize("time_in_force", [TimeInForce.FOK, TimeInForce.IOC])
def test_non_crossing_immediate_order_expires(
    tmp_path: Path,
    write_partition,
    native_library_path: Path,
    side: Side,
    price: float,
    time_in_force: TimeInForce,
) -> None:
    with SlimEngine(
        _assets(tmp_path, write_partition, entry_ns=0, response_ns=0),
        library_path=native_library_path,
    ) as engine:
        assert engine.advance(10)
        engine.submit_order(
            asset_no=0,
            order_id=9,
            side=side,
            price=price,
            quantity=4.0,
            time_in_force=time_in_force,
        )
        assert engine.wait_order_response(0, 9, 0)
        order = engine.order(0, 9)
        assert order is not None
        assert order.status is OrderStatus.EXPIRED
        assert order.execution_price == 0.0
        assert order.execution_quantity == 0.0
        assert order.leaves_quantity == 0.0


def test_response_timeout_visibility_and_order_latency(
    tmp_path: Path, write_partition, native_library_path: Path
) -> None:
    with SlimEngine(
        _assets(tmp_path, write_partition, entry_ns=3, response_ns=4),
        library_path=native_library_path,
    ) as engine:
        assert engine.advance(10)
        engine.submit_order(
            asset_no=0,
            order_id=7,
            side=Side.BUY,
            price=101.0,
            quantity=1.0,
            time_in_force=TimeInForce.FOK,
        )
        assert engine.order(0, 7) is None
        assert engine.wait_order_response(0, 7, 3) is False
        assert engine.order(0, 7) is None
        assert engine.wait_order_response(0, 7, 4) is True
        order = engine.order(0, 7)
        latency = engine.order_latency(0)
        assert order is not None and latency is not None
        assert order.request_local_timestamp_ns == 110
        assert order.exchange_timestamp_ns == 113
        assert order.response_local_timestamp_ns == 117
        assert latency.entry_latency_ns == 3
        assert latency.response_latency_ns == 4
        assert latency.total_latency_ns == 7


def test_empty_partition_is_a_valid_asset(
    tmp_path: Path, write_partition, native_library_path: Path
) -> None:
    with SlimEngine(
        _assets(tmp_path, write_partition, empty_right=True),
        library_path=native_library_path,
    ) as engine:
        assert engine.advance(10)
        depth = engine.depth(1)
        assert not depth.valid
        assert depth.best_bid == 0.0
        assert depth.best_ask == 0.0
        assert engine.feed_latency(1) is None
