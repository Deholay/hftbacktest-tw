"""Neutral public boundary for the project-owned slim replay runtime."""

from .api import (
    AbiMismatchError,
    ArrowDataError,
    AssetConfig,
    CompactCacheBudgetError,
    CompactCacheError,
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
    __version__,
)


_COMPACT_EXPORTS = {
    "BBO_SCHEMA": (".market_data.schema", "BBO_SCHEMA"),
    "COMPACT_BUILDER_VERSION": (".cache.config", "COMPACT_BUILDER_VERSION"),
    "COMPACT_SCHEMA_VERSION": (".market_data.schema", "COMPACT_SCHEMA_VERSION"),
    "CompactBuildConfig": (".cache.config", "CompactBuildConfig"),
    "CompactCacheStore": (".cache.store", "CompactCacheStore"),
    "CompactSource": (".cache.config", "CompactSource"),
    "aggregate_depth_side": (".market_data.normalize", "_aggregate_depth_side"),
    "normalized_bbo_from_depth_columns": (
        ".market_data.normalize",
        "normalized_bbo_from_depth_columns",
    ),
}


def __getattr__(name: str):
    """Load PyArrow/Numba-backed cache objects only when requested."""

    target = _COMPACT_EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    from importlib import import_module

    module = import_module(target[0], __name__)
    value = getattr(module, target[1])
    globals()[name] = value
    return value

__all__ = (
    "AbiMismatchError",
    "ArrowDataError",
    "AssetConfig",
    "BBO_SCHEMA",
    "COMPACT_BUILDER_VERSION",
    "COMPACT_SCHEMA_VERSION",
    "CompactBuildConfig",
    "CompactCacheBudgetError",
    "CompactCacheError",
    "CompactCacheStore",
    "CompactSource",
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
    "aggregate_depth_side",
    "normalized_bbo_from_depth_columns",
)
