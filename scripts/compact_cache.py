"""Transitional import path for the package-owned compact BBO cache."""

from __future__ import annotations

import sys
from pathlib import Path


_PACKAGE_SOURCE = Path(__file__).resolve().parents[1] / "hftbacktest_slim" / "src"
if str(_PACKAGE_SOURCE) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_SOURCE))

from hftbacktest_slim import (  # noqa: E402
    BBO_SCHEMA,
    COMPACT_BUILDER_VERSION,
    COMPACT_SCHEMA_VERSION,
    CompactBuildConfig,
    CompactCacheBudgetError,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
)
from hftbacktest_slim.cache.config import COMPACT_ROW_ESTIMATE_BYTES  # noqa: E402
from hftbacktest_slim.market_data.schema import PROJECTED_COLUMNS  # noqa: E402


__all__ = (
    "BBO_SCHEMA",
    "COMPACT_BUILDER_VERSION",
    "COMPACT_ROW_ESTIMATE_BYTES",
    "COMPACT_SCHEMA_VERSION",
    "PROJECTED_COLUMNS",
    "CompactBuildConfig",
    "CompactCacheBudgetError",
    "CompactCacheError",
    "CompactCacheStore",
    "CompactSource",
)
