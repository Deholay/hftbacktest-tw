from __future__ import annotations

import dataclasses
import math
import tomllib
from pathlib import Path

import pytest

import hftbacktest_slim
from hftbacktest_slim import (
    AbiMismatchError,
    ArrowDataError,
    AssetConfig,
    DepthView,
    EngineClosedError,
    FeedLatency,
    NativeCallError,
    NativeLibraryError,
    NativeLibraryNotFoundError,
    OrderLatency,
    OrderStatus,
    OrderSubmissionError,
    OrderType,
    OrderView,
    Side,
    SlimConfigurationError,
    SlimError,
    SlimEngine,
    SLIM_ENGINE_VERSION,
    TimeInForce,
    UnsupportedCapabilityError,
)


EXPECTED_PUBLIC_EXPORTS = {
    "AbiMismatchError",
    "ArrowDataError",
    "AssetConfig",
    "DepthView",
    "EngineClosedError",
    "FeedLatency",
    "NativeCallError",
    "NativeLibraryError",
    "NativeLibraryNotFoundError",
    "OrderLatency",
    "OrderStatus",
    "OrderSubmissionError",
    "OrderType",
    "OrderView",
    "Side",
    "SlimConfigurationError",
    "SlimError",
    "SlimEngine",
    "SLIM_ENGINE_VERSION",
    "TimeInForce",
    "UnsupportedCapabilityError",
    "__version__",
}


def test_public_exports_are_the_implemented_neutral_runtime() -> None:
    assert set(hftbacktest_slim.__all__) == EXPECTED_PUBLIC_EXPORTS
    assert hftbacktest_slim.SlimEngine is SlimEngine
    assert not hasattr(hftbacktest_slim, "CompactCacheStore")


def test_package_version_matches_project_metadata() -> None:
    project_root = Path(__file__).resolve().parents[1]
    metadata = tomllib.loads((project_root / "pyproject.toml").read_text(encoding="utf-8"))
    assert hftbacktest_slim.__version__ == "0.3.0a1"
    assert metadata["project"]["version"] == hftbacktest_slim.__version__


def test_enum_integer_values_match_the_current_native_abi() -> None:
    assert set(TimeInForce) == {TimeInForce.FOK, TimeInForce.IOC}
    assert set(OrderStatus) == {
        OrderStatus.NEW,
        OrderStatus.EXPIRED,
        OrderStatus.FILLED,
    }
    assert int(Side.BUY) == 1
    assert int(Side.SELL) == -1
    assert int(TimeInForce.FOK) == 2
    assert int(TimeInForce.IOC) == 3
    assert int(OrderStatus.NEW) == 1
    assert int(OrderStatus.EXPIRED) == 2
    assert int(OrderStatus.FILLED) == 3
    assert int(OrderType.LIMIT) == 0


def test_asset_config_is_immutable_and_normalizes_path_like_values(tmp_path: Path) -> None:
    relative_path = Path("compact") / "0050.arrow"
    config = AssetConfig(
        symbol="0050",
        data_path=relative_path,
        tick_size=0.05,
        feed_latency_offset_ns=-10,
        order_entry_latency_ns=20,
        order_response_latency_ns=30,
    )

    assert config.data_path == relative_path
    assert isinstance(config.data_path, Path)
    assert config.feed_latency_offset_ns == -10
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.tick_size = 0.1  # type: ignore[misc]

    other = AssetConfig("2330", tmp_path / "2330.arrow", 1.0)
    assert other.data_path != config.data_path
    assert other.order_entry_latency_ns == 0


def test_asset_config_contains_only_slim_runtime_inputs() -> None:
    field_names = {field.name for field in dataclasses.fields(AssetConfig)}
    assert field_names == {
        "symbol",
        "data_path",
        "tick_size",
        "feed_latency_offset_ns",
        "order_entry_latency_ns",
        "order_response_latency_ns",
    }
    assert field_names.isdisjoint(
        {
            "instrument",
            "contract_size",
            "lot_size",
            "maker_fee",
            "taker_fee",
            "queue_model",
            "queue_model_param",
            "last_trades_capacity",
            "recorder",
        }
    )


@pytest.mark.parametrize("tick_size", [0.0, -1.0, math.inf, math.nan])
def test_asset_config_rejects_invalid_tick_sizes(tick_size: float) -> None:
    with pytest.raises(SlimConfigurationError, match="tick_size"):
        AssetConfig("0050", "0050.arrow", tick_size)


def test_exception_hierarchy() -> None:
    assert issubclass(SlimConfigurationError, (SlimError, ValueError))
    assert issubclass(UnsupportedCapabilityError, SlimError)
    assert issubclass(NativeLibraryError, SlimError)
    assert issubclass(NativeLibraryNotFoundError, (NativeLibraryError, FileNotFoundError))
    assert issubclass(AbiMismatchError, NativeLibraryError)
    assert issubclass(ArrowDataError, (SlimError, ValueError))
    assert issubclass(EngineClosedError, SlimError)
    assert issubclass(NativeCallError, (SlimError, RuntimeError))
    assert issubclass(OrderSubmissionError, NativeCallError)


def test_engine_implementation_version_is_unchanged() -> None:
    assert SLIM_ENGINE_VERSION == "rust-0.2.0"
