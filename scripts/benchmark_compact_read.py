#!/usr/bin/env python3
"""Transitional CLI delegate for ``hftbacktest_slim.cli.benchmark_read``."""

from __future__ import annotations

import sys
from pathlib import Path


_PACKAGE_SOURCE = Path(__file__).resolve().parents[1] / "hftbacktest_slim" / "src"
if str(_PACKAGE_SOURCE) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_SOURCE))

from hftbacktest_slim.cli.benchmark_read import main, parse_args, run  # noqa: E402

__all__ = ("main", "parse_args", "run")


if __name__ == "__main__":
    raise SystemExit(main())
