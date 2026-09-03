"""Deterministic compact-cache identities, JSON hashing, and source facts."""

from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from ..market_data.schema import COMPACT_SCHEMA_VERSION, PROJECTED_COLUMNS
from .config import COMPACT_BUILDER_VERSION, CompactBuildConfig, CompactSource


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def source_rows(path: Path) -> int:
    """Read row counts from file metadata without scanning source columns."""

    if path.suffix.lower() == ".parquet":
        return int(pq.ParquetFile(path).metadata.num_rows)
    with pa.memory_map(str(path), "r") as handle:
        reader = ipc.open_file(handle)
        return sum(
            reader.get_batch(index).num_rows for index in range(reader.num_record_batches)
        )


def source_identity(path: Path) -> dict[str, Any]:
    stat = path.stat()
    identity = {
        "path": str(path.resolve()),
        "bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "rows": source_rows(path),
    }
    if path.suffix.lower() == ".parquet":
        identity["parquet_metadata_sha256"] = parquet_metadata_sha256(path)
    return identity


def parquet_metadata_sha256(path: Path) -> str:
    """Hash only the Parquet footer already authorized for preflight identity."""

    with path.open("rb") as handle:
        handle.seek(-8, 2)
        trailer = handle.read(8)
        if len(trailer) != 8 or trailer[4:] != b"PAR1":
            raise ValueError(f"invalid Parquet footer: {path}")
        footer_bytes = struct.unpack("<I", trailer[:4])[0]
        handle.seek(-(footer_bytes + 8), 2)
        footer = handle.read(footer_bytes)
    if len(footer) != footer_bytes:
        raise ValueError(f"truncated Parquet footer: {path}")
    return hashlib.sha256(footer).hexdigest()


def implementation_paths() -> tuple[Path, ...]:
    """Return every result-defining cache/market-data module in sorted order."""

    package_root = Path(__file__).resolve().parents[1]
    return tuple(
        sorted(
            [
                *(package_root / "cache").rglob("*.py"),
                *(package_root / "market_data").rglob("*.py"),
            ],
            key=lambda path: path.relative_to(package_root).as_posix(),
        )
    )


def implementation_fingerprint() -> str:
    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in implementation_paths():
        relative = path.relative_to(package_root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def build_identity(
    trade_date: str,
    sources: Sequence[CompactSource],
    config: CompactBuildConfig,
) -> dict[str, Any]:
    """Build the complete reusable-cache identity without reading row payloads."""

    package_root = Path(__file__).resolve().parents[1]
    fingerprint = implementation_fingerprint()
    normalize_path = package_root / "market_data" / "normalize.py"
    return {
        "trade_date": trade_date,
        "schema_version": COMPACT_SCHEMA_VERSION,
        "builder_version": COMPACT_BUILDER_VERSION,
        # Legacy field names remain for manifest readers, but their values now
        # describe the canonical package implementation rather than wrappers.
        "builder_sha256": fingerprint,
        "top5_implementation_sha256": file_sha256(normalize_path),
        "implementation_sha256": fingerprint,
        "implementation_paths": [
            path.relative_to(package_root).as_posix() for path in implementation_paths()
        ],
        "compression": config.compression,
        "profile": config.profile,
        "timezone": config.timezone,
        "session_start_ns": config.session_start_ns,
        "session_end_ns": config.session_end_ns,
        "base_latency_ns": config.base_latency_ns,
        "projected_columns": list(PROJECTED_COLUMNS),
        "sources": [
            {
                "kind": source.kind,
                "symbols": list(source.symbols),
                "status_allow": list(source.status_allow),
                "price_only_depth_qty": source.price_only_depth_qty,
                "volume_scale": source.volume_scale,
                "files": [source_identity(Path(path)) for path in source.paths],
            }
            for source in sources
        ],
    }


__all__ = (
    "build_identity",
    "canonical_sha256",
    "file_sha256",
    "implementation_fingerprint",
    "implementation_paths",
    "parquet_metadata_sha256",
    "source_identity",
    "source_rows",
)
