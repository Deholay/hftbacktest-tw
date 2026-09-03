from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from hftbacktest_slim.compat.hbt import (
    SLIM_ENGINE_VERSION,
    SLIM_LIBRARY,
    SlimBacktest,
    SlimHbtConstants,
    validate_slim_pair_config,
)


def _legacy_assets(tmp_path: Path, write_partition):
    left = write_partition(
        tmp_path / "legacy-left.arrow",
        [(0, 100, 110, 99.0, 101.0, 2.0, 1.0, 100.0, 1)],
    )
    right = write_partition(
        tmp_path / "legacy-right.arrow",
        [(0, 100, 110, 199.0, 201.0, 2.0, 1.0, 200.0, 1)],
    )
    return [
        SimpleNamespace(
            symbol="A",
            data=left,
            tick_size=1.0,
            feed_latency_offset_ns=0,
            order_entry_latency_ns=3,
            order_response_latency_ns=4,
            queue_model="ignored",
        ),
        SimpleNamespace(
            symbol="B",
            data=right,
            tick_size=1.0,
            feed_latency_offset_ns=0,
            order_entry_latency_ns=0,
            order_response_latency_ns=0,
        ),
    ]


def test_compat_constants_and_engine_identity_are_unchanged() -> None:
    assert {
        name: getattr(SlimHbtConstants, name)
        for name in ("BUY", "SELL", "LIMIT", "NEW", "EXPIRED", "FILLED", "CANCELED", "GTC", "GTX", "FOK", "IOC")
    } == {
        "BUY": 1,
        "SELL": -1,
        "LIMIT": 0,
        "NEW": 1,
        "EXPIRED": 2,
        "FILLED": 3,
        "CANCELED": 4,
        "GTC": 0,
        "GTX": 1,
        "FOK": 2,
        "IOC": 3,
    }
    assert SLIM_ENGINE_VERSION == "rust-0.2.0"
    assert SLIM_LIBRARY.name == "libhbt_slim.so"


def test_structural_hbt_asset_mapping_submit_wait_mapping_and_noops(
    tmp_path: Path, write_partition, native_library_path: Path
) -> None:
    hbt = SlimBacktest(
        _legacy_assets(tmp_path, write_partition), library_path=native_library_path
    )
    try:
        assert hbt.elapse(10) == 0
        assert hbt.depth(0).best_ask == 101.0
        assert hbt.feed_latency(0) == (100, 110)
        orders = hbt.orders(0)
        sentinel = object()
        assert orders.get(77) is None
        assert orders.get(77, sentinel) is sentinel
        assert hbt.submit_buy_order(0, 77, 101.0, 10.0, 2, 0, False) == 0
        assert hbt.wait_order_response(0, 77, 10) == 0
        order = orders.get(77)
        assert order is not None
        assert order.status == SlimHbtConstants.FILLED
        assert order.exec_price == 101.0
        assert order.exec_qty == 10.0
        assert order.local_timestamp == 110
        assert order.exch_timestamp == 113
        assert hbt.order_latency(0) == (110, 113, 117)
        assert hbt.cancel(0, 77, False) == 0
        assert hbt.clear_inactive_orders(0) is None
    finally:
        hbt.close()
        hbt.close()


def test_legacy_wrapper_reexports_the_package_facade() -> None:
    from scripts.slim_engine import (
        SlimBacktest as LegacyBacktest,
        SlimHbtConstants as LegacyConstants,
    )

    assert LegacyBacktest is SlimBacktest
    assert LegacyConstants is SlimHbtConstants


def test_compat_pair_validation_delegates_supported_immediate_tif_rules() -> None:
    pair = SimpleNamespace(
        first_leg_time_in_force="FOK",
        second_leg_time_in_force="ioc",
        flatten_first_leg_time_in_force="FOK",
    )
    assert validate_slim_pair_config(pair) is None
    pair.second_leg_time_in_force = "GTC"
    with pytest.raises(ValueError, match="second_leg_time_in_force='GTC'"):
        validate_slim_pair_config(pair)
