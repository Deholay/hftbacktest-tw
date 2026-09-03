"""Transitional legacy import path for the package-owned slim HBT facade."""

from __future__ import annotations

import sys
from pathlib import Path


# Phase 3 repository bootstrap: current consumers import this wrapper before
# the src-layout package is installed. Remove this narrow path shim when those
# consumers migrate to the neutral package API in Phase 5/6.
_PACKAGE_SOURCE = Path(__file__).resolve().parents[1] / "hftbacktest_slim" / "src"
if str(_PACKAGE_SOURCE) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_SOURCE))

from hftbacktest_slim.compat.hbt import (  # noqa: E402
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
