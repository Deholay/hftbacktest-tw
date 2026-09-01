"""Atomic, date-partitioned storage for bounded backtest result pipelines."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
import time
from dataclasses import asdict, dataclass, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

import pandas as pd

from .io_utils import write_parquet


DAILY_RESULT_SCHEMA_VERSION = 1
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class DailyResultStoreError(RuntimeError):
    """Base error for a non-reusable or conflicting result partition."""


class DailyResultConflictError(DailyResultStoreError):
    """A completed date exists with a different result-defining identity."""


@dataclass(frozen=True)
class DailyResultManifest:
    trade_date: str
    path: Path
    payload: dict[str, Any]

    @property
    def carry_in_sha256(self) -> str:
        return str(self.payload["carry_in_sha256"])

    @property
    def carry_out_sha256(self) -> str:
        return str(self.payload["carry_out_sha256"])


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        default=_json_default,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"value is not JSON serializable: {type(value).__name__}")


class DailyResultStore:
    """Publish one complete date with a same-filesystem directory rename.

    The physical layout groups each date atomically under ``dates/``. Each
    table remains a separate Parquet file, and compatibility/report readers can
    stream dates without retaining annual detail frames.
    """

    def __init__(self, root: Path):
        self.root = Path(root)
        self.dates_root = self.root / "dates"

    def date_path(self, trade_date: str) -> Path:
        self._validate_date(trade_date)
        return self.dates_root / f"trade_date={trade_date}"

    def publish(
        self,
        trade_date: str,
        tables: Mapping[str, pd.DataFrame],
        *,
        input_identity: Any,
        carry_in: Any,
        carry_out: Any,
        run_keys: Sequence[str],
        metadata: Mapping[str, Any] | None = None,
        replace_existing: bool = False,
    ) -> DailyResultManifest:
        """Validate and atomically publish a completed date.

        Completed partitions are immutable. An identical publication is a
        cache hit; a different identity raises instead of overwriting research
        output. Only an incomplete temporary directory is removed on failure.
        """
        self._validate_date(trade_date)
        input_sha = canonical_sha256(input_identity)
        carry_in_sha = canonical_sha256(carry_in)
        carry_out_sha = canonical_sha256(carry_out)
        final_path = self.date_path(trade_date)
        if final_path.exists():
            existing = self.validate(trade_date)
            payload = existing.payload
            if not replace_existing and (
                payload.get("input_identity_sha256") == input_sha
                and payload.get("carry_in_sha256") == carry_in_sha
                and payload.get("carry_out_sha256") == carry_out_sha
                and payload.get("run_keys") == list(run_keys)
            ):
                return existing
            if not replace_existing:
                raise DailyResultConflictError(
                    f"completed result date has a different identity: {final_path}"
                )

        self.dates_root.mkdir(parents=True, exist_ok=True)
        temp_path = Path(tempfile.mkdtemp(prefix=f".tmp-{trade_date}-", dir=self.dates_root))
        try:
            table_manifest: dict[str, dict[str, Any]] = {}
            for name, frame in sorted(tables.items()):
                self._validate_table_name(name)
                path = temp_path / f"{name}.parquet"
                write_parquet(frame, path)
                persisted = pd.read_parquet(path)
                if len(persisted) != len(frame):
                    raise DailyResultStoreError(
                        f"row-count validation failed for {trade_date}/{name}: "
                        f"expected={len(frame)} actual={len(persisted)}"
                    )
                table_manifest[name] = {
                    "file": path.name,
                    "rows": len(frame),
                    "columns": list(frame.columns),
                    "bytes": path.stat().st_size,
                    "sha256": self._file_sha256(path),
                }

            payload = {
                "schema_version": DAILY_RESULT_SCHEMA_VERSION,
                "trade_date": trade_date,
                "build_complete": True,
                "input_identity": input_identity,
                "input_identity_sha256": input_sha,
                "carry_in_sha256": carry_in_sha,
                "carry_out_sha256": carry_out_sha,
                "run_keys": list(run_keys),
                "tables": table_manifest,
                "metadata": dict(metadata or {}),
            }
            manifest_path = temp_path / "manifest.json"
            manifest_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            stale_path = None
            if final_path.exists():
                stale_path = self.dates_root / f".superseded-{final_path.name}-{time.time_ns()}"
                os.replace(final_path, stale_path)
            try:
                os.replace(temp_path, final_path)
            except Exception:
                if stale_path is not None and stale_path.exists() and not final_path.exists():
                    os.replace(stale_path, final_path)
                raise
        except Exception:
            if temp_path.exists():
                shutil.rmtree(temp_path)
            raise
        return self.validate(trade_date)

    def validate(self, trade_date: str) -> DailyResultManifest:
        date_path = self.date_path(trade_date)
        manifest_path = date_path / "manifest.json"
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
            raise DailyResultStoreError(f"invalid or incomplete result date: {date_path}") from exc
        if payload.get("schema_version") != DAILY_RESULT_SCHEMA_VERSION:
            raise DailyResultStoreError(f"unsupported result schema for {date_path}")
        if payload.get("trade_date") != trade_date or payload.get("build_complete") is not True:
            raise DailyResultStoreError(f"incomplete result manifest for {date_path}")
        if payload.get("input_identity_sha256") != canonical_sha256(payload.get("input_identity")):
            raise DailyResultStoreError(f"input identity mismatch for {date_path}")
        tables = payload.get("tables")
        if not isinstance(tables, dict):
            raise DailyResultStoreError(f"missing table manifest for {date_path}")
        for name, details in tables.items():
            self._validate_table_name(name)
            path = date_path / str(details.get("file"))
            if not path.is_file() or path.stat().st_size != details.get("bytes"):
                raise DailyResultStoreError(f"missing or changed result table: {path}")
            if self._file_sha256(path) != details.get("sha256"):
                raise DailyResultStoreError(f"result table checksum mismatch: {path}")
        return DailyResultManifest(trade_date, manifest_path, payload)

    def load_table(self, trade_date: str, name: str) -> pd.DataFrame:
        manifest = self.validate(trade_date)
        self._validate_table_name(name)
        details = manifest.payload["tables"].get(name)
        if details is None:
            return pd.DataFrame()
        frame = pd.read_parquet(manifest.path.parent / details["file"])
        if len(frame) != details["rows"]:
            raise DailyResultStoreError(f"result table row count changed: {trade_date}/{name}")
        return frame

    def iter_table(self, trade_dates: Sequence[str], name: str) -> Iterator[pd.DataFrame]:
        for trade_date in trade_dates:
            yield self.load_table(trade_date, name)

    def write_table_csv(self, trade_dates: Sequence[str], name: str, output: Path) -> Path:
        """Stream date partitions into one atomic compatibility CSV."""
        self.write_tables_csv(trade_dates, {name: output})
        return Path(output)

    def write_tables_csv(
        self,
        trade_dates: Sequence[str],
        outputs: Mapping[str, Path],
    ) -> dict[str, Path]:
        """Validate dates once, then stream multiple compatibility CSVs."""
        normalized = {name: Path(path) for name, path in outputs.items()}
        for name in normalized:
            self._validate_table_name(name)
        manifests = [self.validate(trade_date) for trade_date in trade_dates]
        columns = {name: [] for name in normalized}
        for manifest in manifests:
            for name in normalized:
                details = manifest.payload["tables"].get(name)
                if details is None:
                    continue
                for column in details.get("columns", []):
                    if column not in columns[name]:
                        columns[name].append(column)

        temporary: dict[str, Path] = {}
        wrote_header = {name: False for name in normalized}
        try:
            for name, output in normalized.items():
                output.parent.mkdir(parents=True, exist_ok=True)
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=f".{output.name}.",
                    suffix=".tmp",
                    dir=output.parent,
                )
                os.close(descriptor)
                temporary[name] = Path(temporary_name)

            for manifest in manifests:
                for name in normalized:
                    details = manifest.payload["tables"].get(name)
                    if details is None:
                        continue
                    frame = pd.read_parquet(manifest.path.parent / details["file"])
                    if len(frame) != details["rows"]:
                        raise DailyResultStoreError(
                            f"result table row count changed: {manifest.trade_date}/{name}"
                        )
                    if frame.empty:
                        continue
                    frame.reindex(columns=columns[name]).to_csv(
                        temporary[name],
                        mode="a",
                        header=not wrote_header[name],
                        index=False,
                        encoding="utf-8-sig" if not wrote_header[name] else "utf-8",
                    )
                    wrote_header[name] = True

            for name, output in normalized.items():
                if not wrote_header[name]:
                    pd.DataFrame(columns=columns[name]).to_csv(
                        temporary[name],
                        index=False,
                        encoding="utf-8-sig",
                    )
                os.replace(temporary[name], output)
        except Exception:
            for path in temporary.values():
                path.unlink(missing_ok=True)
            raise
        return normalized

    def validated_prefix(
        self,
        trade_dates: Sequence[str],
        *,
        first_carry: Any,
        input_identities: Mapping[str, Any] | None = None,
    ) -> list[DailyResultManifest]:
        """Return only the verified contiguous prefix with an exact carry chain."""
        expected_carry = canonical_sha256(first_carry)
        prefix: list[DailyResultManifest] = []
        for trade_date in trade_dates:
            try:
                manifest = self.validate(trade_date)
            except DailyResultStoreError:
                break
            payload = manifest.payload
            if payload.get("carry_in_sha256") != expected_carry:
                break
            if input_identities is not None:
                identity = input_identities.get(trade_date)
                if identity is None or payload.get("input_identity_sha256") != canonical_sha256(identity):
                    break
            prefix.append(manifest)
            expected_carry = manifest.carry_out_sha256
        return prefix

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _validate_date(trade_date: str) -> None:
        if not _DATE_RE.fullmatch(str(trade_date)):
            raise ValueError(f"trade_date must be YYYY-MM-DD: {trade_date!r}")

    @staticmethod
    def _validate_table_name(name: str) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", str(name)):
            raise ValueError(f"invalid result table name: {name!r}")
