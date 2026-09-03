"""Disk budgets and recoverable same-filesystem compact publication."""

from __future__ import annotations

import json
import math
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from ..errors import CompactCacheBudgetError, CompactCacheError
from .config import COMPACT_ROW_ESTIMATE_BYTES, CompactBuildConfig


def directory_bytes(path: Path) -> int:
    return (
        sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
        if path.exists()
        else 0
    )


def projected_bytes(source_rows: int) -> int:
    return math.ceil(source_rows * COMPACT_ROW_ESTIMATE_BYTES * 1.20)


def preflight_space(
    root: Path,
    config: CompactBuildConfig,
    identity: dict[str, Any],
) -> None:
    """Enforce metadata-derived worst-case space before any temp write."""

    source_rows = sum(
        int(file_identity["rows"])
        for source in identity["sources"]
        for file_identity in source["files"]
    )
    estimate = projected_bytes(source_rows)
    existing = directory_bytes(root)
    if existing + estimate > config.max_cache_bytes:
        raise CompactCacheBudgetError(
            f"compact cache budget exceeded: existing={existing} projected={estimate} "
            f"limit={config.max_cache_bytes}"
        )
    probe = root.parent if root.parent.exists() else Path.cwd()
    free = shutil.disk_usage(probe).free
    if free - estimate < config.min_free_bytes:
        raise CompactCacheBudgetError(
            f"compact cache free-space reserve would be crossed: free={free} "
            f"projected={estimate} reserve={config.min_free_bytes}"
        )


def runtime_budget_check(
    root: Path,
    temp: Path,
    config: CompactBuildConfig,
) -> None:
    """Recheck actual cache use and filesystem reserve after each batch."""

    used = directory_bytes(root)
    if used > config.max_cache_bytes:
        raise CompactCacheBudgetError(f"compact cache exceeded limit during build: {used}")
    free = shutil.disk_usage(temp).free
    if free < config.min_free_bytes:
        raise CompactCacheBudgetError(
            f"compact cache free-space reserve crossed during build: {free}"
        )


def create_temporary_date(root: Path, trade_date: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=f".tmp-{trade_date}-", dir=root))


def cleanup_incomplete_date(root: Path, temp: Path, trade_date: str) -> None:
    """Remove only the current incomplete date directory."""

    root_resolved = root.resolve()
    temp_resolved = temp.resolve()
    expected_prefix = f".tmp-{trade_date}-"
    if temp_resolved.parent != root_resolved or not temp_resolved.name.startswith(
        expected_prefix
    ):
        raise CompactCacheError(f"refusing broad compact temporary cleanup: {temp}")
    if temp_resolved.exists():
        shutil.rmtree(temp_resolved)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def publish_date_atomically(
    *,
    root: Path,
    temp: Path,
    final: Path,
    validate_staged: Callable[[Path], None],
) -> Path | None:
    """Validate and publish a date while preserving any completed predecessor."""

    validate_staged(temp)
    superseded: Path | None = None
    if final.exists():
        superseded = root / f".superseded-{final.name}-{time.time_ns()}"
        os.replace(final, superseded)
    try:
        os.replace(temp, final)
    except Exception:
        if superseded is not None and superseded.exists() and not final.exists():
            os.replace(superseded, final)
        raise
    return superseded


__all__ = (
    "cleanup_incomplete_date",
    "create_temporary_date",
    "directory_bytes",
    "preflight_space",
    "projected_bytes",
    "publish_date_atomically",
    "runtime_budget_check",
    "write_json",
)
