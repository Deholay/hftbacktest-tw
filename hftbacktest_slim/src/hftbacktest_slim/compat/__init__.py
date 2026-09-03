"""Temporary compatibility namespaces for repository consumers."""

from .hbt import (
    SLIM_ENGINE_VERSION,
    SLIM_LIBRARY,
    SlimBacktest,
    SlimDepth,
    SlimHbtConstants,
    SlimOrder,
    validate_slim_pair_config,
)

__all__ = (
    "SLIM_ENGINE_VERSION",
    "SLIM_LIBRARY",
    "SlimBacktest",
    "SlimDepth",
    "SlimHbtConstants",
    "SlimOrder",
    "validate_slim_pair_config",
)
