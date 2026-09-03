#!/usr/bin/env python3
"""Transitional CLI delegate for ``hftbacktest_slim.cli.build_cache``."""

from __future__ import annotations

import sys
from pathlib import Path


_PACKAGE_SOURCE = Path(__file__).resolve().parents[1] / "hftbacktest_slim" / "src"
if str(_PACKAGE_SOURCE) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_SOURCE))

from hftbacktest_slim.cli.build_cache import (  # noqa: E402
    main,
    parse_args,
    run,
    settings_symbols as _settings_symbols,
)

__all__ = ("main", "parse_args", "run")


if __name__ == "__main__":
    raise SystemExit(main())
