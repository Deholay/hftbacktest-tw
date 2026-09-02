"""Neutral public boundary for the project-owned slim replay runtime.

Phase 1 intentionally exports configuration and type definitions only. The
engine and compact-cache implementations remain in their legacy locations.
"""

from .api import (
    AbiMismatchError,
    AssetConfig,
    NativeLibraryError,
    NativeLibraryNotFoundError,
    OrderStatus,
    OrderType,
    Side,
    SlimConfigurationError,
    SlimError,
    TimeInForce,
    UnsupportedCapabilityError,
    __version__,
)

__all__ = (
    "AbiMismatchError",
    "AssetConfig",
    "NativeLibraryError",
    "NativeLibraryNotFoundError",
    "OrderStatus",
    "OrderType",
    "Side",
    "SlimConfigurationError",
    "SlimError",
    "TimeInForce",
    "UnsupportedCapabilityError",
    "__version__",
)
