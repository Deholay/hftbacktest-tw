"""Public compact-cache build, validation, reuse, and read operations."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from ..errors import ArrowDataError, CompactCacheError
from ..market_data.ordering import timestamp_ordering_facts, validate_order_sidecar
from ..market_data.schema import (
    COMPACT_SCHEMA_VERSION,
    PROJECTED_COLUMNS,
    decoded_metadata,
    validate_bbo_schema,
    validate_schema_metadata,
)
from .builder import build_source, source_dirname
from .config import COMPACT_BUILDER_VERSION, CompactBuildConfig, CompactSource
from .manifest import (
    build_identity,
    canonical_sha256,
    file_sha256,
    implementation_fingerprint,
    implementation_paths,
    source_identity,
)
from .publication import (
    cleanup_incomplete_date,
    create_temporary_date,
    preflight_space,
    publish_date_atomically,
    write_json,
)


class CompactCacheStore:
    """Versioned, conservative compact cache rooted on one filesystem."""

    def __init__(self, config: CompactBuildConfig):
        self.config = config
        self.root = Path(config.cache_root)

    def date_path(self, trade_date: str) -> Path:
        validate_date_value(trade_date)
        return self.root / f"date={trade_date.replace('-', '')}"

    def build_date(
        self,
        trade_date: str,
        sources: Sequence[CompactSource],
    ) -> dict[str, Any]:
        validate_date_value(trade_date)
        _validate_source_request(sources)
        expected = self._identity(trade_date, sources)
        final = self.date_path(trade_date)
        if final.exists():
            try:
                current = self.validate_date(trade_date)
            except CompactCacheError:
                current = None
            if current is not None and current.get("identity_sha256") == canonical_sha256(
                expected
            ):
                return {
                    **current,
                    "cache_state": "hit",
                    "build_invocation_scan_count": 0,
                }
            if not self.config.rebuild:
                raise CompactCacheError(
                    "compact date exists with an incompatible identity; use rebuild "
                    f"or a new root: {final}"
                )

        preflight_space(self.root, self.config, expected)
        temp = create_temporary_date(self.root, trade_date)
        started = time.perf_counter()
        try:
            source_manifests: dict[str, Any] = {}
            for source in sources:
                if source.kind in source_manifests:
                    raise CompactCacheError(f"duplicate compact source kind: {source.kind}")
                source_manifests[source.kind] = build_source(
                    temp,
                    trade_date,
                    source,
                    self.config,
                )
            payload = {
                "schema_version": COMPACT_SCHEMA_VERSION,
                "builder_version": COMPACT_BUILDER_VERSION,
                "trade_date": trade_date,
                "build_complete": True,
                "identity": expected,
                "identity_sha256": canonical_sha256(expected),
                "sources": source_manifests,
                "elapsed_seconds": time.perf_counter() - started,
            }
            # Validate every closed partition/source manifest first. The date
            # manifest is the final staged write and is required for reuse.
            self._validate_sources(temp, trade_date, source_manifests)
            write_json(temp / "manifest.json", payload)
            publish_date_atomically(
                root=self.root,
                temp=temp,
                final=final,
                validate_staged=lambda path: self._validate_path(path, trade_date),
            )
        except Exception:
            if temp.exists():
                cleanup_incomplete_date(self.root, temp, trade_date)
            raise
        validated = self.validate_date(trade_date)
        return {
            **validated,
            "cache_state": "miss",
            "build_invocation_scan_count": sum(
                int(source.get("scan_count", 0))
                for source in validated.get("sources", {}).values()
            ),
        }

    def validate_date(self, trade_date: str) -> dict[str, Any]:
        return self._validate_path(self.date_path(trade_date), trade_date)

    def read_symbol(self, trade_date: str, source: str, symbol: str) -> pa.Table:
        manifest = self.validate_date(trade_date)
        details = manifest["sources"].get(source, {}).get("symbols", {}).get(str(symbol))
        if details is None or details.get("status") != "valid":
            raise CompactCacheError(
                f"symbol not present in compact cache: {trade_date}/{source}/{symbol}"
            )
        path = self.date_path(trade_date) / source_dirname(source) / details["file"]
        try:
            with pa.memory_map(str(path), "r") as handle:
                return ipc.open_file(handle).read_all()
        except (OSError, pa.ArrowException) as exc:
            raise CompactCacheError(f"failed to read compact symbol: {path}") from exc

    def _identity(
        self,
        trade_date: str,
        sources: Sequence[CompactSource],
    ) -> dict[str, Any]:
        return build_identity(trade_date, sources, self.config)

    def _validate_path(self, path: Path, trade_date: str) -> dict[str, Any]:
        try:
            payload = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CompactCacheError(f"invalid or incomplete compact date: {path}") from exc
        if payload.get("schema_version") != COMPACT_SCHEMA_VERSION:
            raise CompactCacheError(f"unsupported compact schema version: {path}")
        if payload.get("builder_version") != COMPACT_BUILDER_VERSION:
            raise CompactCacheError(f"unsupported compact builder version: {path}")
        if payload.get("build_complete") is not True:
            raise CompactCacheError(f"unsupported or incomplete compact date: {path}")
        if payload.get("trade_date") != trade_date:
            raise CompactCacheError(f"compact date identity mismatch: {path}")
        identity = payload.get("identity")
        if not isinstance(identity, dict):
            raise CompactCacheError(f"compact identity metadata is missing: {path}")
        if identity.get("schema_version") != COMPACT_SCHEMA_VERSION:
            raise CompactCacheError(f"compact identity schema mismatch: {path}")
        if identity.get("builder_version") != COMPACT_BUILDER_VERSION:
            raise CompactCacheError(f"compact identity builder mismatch: {path}")
        current_implementation = implementation_fingerprint()
        if identity.get("implementation_sha256") != current_implementation:
            raise CompactCacheError(f"compact implementation identity mismatch: {path}")
        if identity.get("builder_sha256") != current_implementation:
            raise CompactCacheError(f"compact builder identity mismatch: {path}")
        package_root = Path(__file__).resolve().parents[1]
        current_paths = [
            item.relative_to(package_root).as_posix() for item in implementation_paths()
        ]
        if identity.get("implementation_paths") != current_paths:
            raise CompactCacheError(f"compact implementation path identity mismatch: {path}")
        normalize_sha = file_sha256(package_root / "market_data" / "normalize.py")
        if identity.get("top5_implementation_sha256") != normalize_sha:
            raise CompactCacheError(f"compact normalization identity mismatch: {path}")
        if identity.get("projected_columns") != list(PROJECTED_COLUMNS):
            raise CompactCacheError(f"compact projected-column identity mismatch: {path}")
        self._validate_raw_source_identities(identity, path)
        if payload.get("identity_sha256") != canonical_sha256(identity):
            raise CompactCacheError(f"compact identity checksum mismatch: {path}")
        sources = payload.get("sources")
        if not isinstance(sources, dict):
            raise CompactCacheError(f"compact source manifests are missing: {path}")
        self._validate_sources(path, trade_date, sources)
        return payload

    def _validate_raw_source_identities(
        self,
        identity: dict[str, Any],
        cache_path: Path,
    ) -> None:
        sources = identity.get("sources")
        if not isinstance(sources, list):
            raise CompactCacheError(f"compact raw-source identity is missing: {cache_path}")
        for source in sources:
            if not isinstance(source, dict) or not isinstance(source.get("files"), list):
                raise CompactCacheError(
                    f"compact raw-source file identity is missing: {cache_path}"
                )
            for saved in source["files"]:
                if not isinstance(saved, dict) or not isinstance(saved.get("path"), str):
                    raise CompactCacheError(
                        f"compact raw-source path identity is missing: {cache_path}"
                    )
                try:
                    current = source_identity(Path(saved["path"]))
                except (OSError, ValueError, pa.ArrowException) as exc:
                    raise CompactCacheError(
                        f"compact raw source cannot be validated: {saved.get('path')}"
                    ) from exc
                if current != saved:
                    raise CompactCacheError(
                        f"compact raw-source identity changed: {saved['path']}"
                    )

    def _validate_sources(
        self,
        path: Path,
        trade_date: str,
        sources: dict[str, Any],
    ) -> None:
        for source, source_details in sources.items():
            _safe_component(source, "source")
            source_manifest_path = path / source_dirname(source) / "manifest.json"
            try:
                source_manifest = json.loads(
                    source_manifest_path.read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError) as exc:
                raise CompactCacheError(
                    f"missing compact source manifest: {source_manifest_path}"
                ) from exc
            if source_manifest != source_details:
                raise CompactCacheError(
                    f"compact source manifest mismatch: {source_manifest_path}"
                )
            symbols = source_details.get("symbols")
            if not isinstance(symbols, dict):
                raise CompactCacheError(
                    f"compact symbol manifest is missing: {source_manifest_path}"
                )
            for symbol, details in symbols.items():
                _safe_component(symbol, "symbol")
                if details.get("status") == "missing":
                    continue
                if details.get("status") != "valid":
                    raise CompactCacheError(
                        f"unknown compact symbol status: {source}/{symbol}"
                    )
                filename = _safe_component(details.get("file"), "symbol file")
                file_path = path / source_dirname(source) / filename
                self._validate_symbol(
                    file_path,
                    trade_date=trade_date,
                    source=source,
                    symbol=symbol,
                    details=details,
                )

    def _validate_symbol(
        self,
        file_path: Path,
        *,
        trade_date: str,
        source: str,
        symbol: str,
        details: dict[str, Any],
    ) -> None:
        if not file_path.is_file() or file_path.stat().st_size != details.get("bytes"):
            raise CompactCacheError(f"missing or changed compact symbol: {file_path}")
        if file_sha256(file_path) != details.get("sha256"):
            raise CompactCacheError(f"compact symbol checksum mismatch: {file_path}")
        try:
            with pa.memory_map(str(file_path), "r") as handle:
                table = ipc.open_file(handle).read_all().combine_chunks()
            validate_bbo_schema(table.schema, file_path)
            metadata = validate_schema_metadata(table.schema, file_path, require=True)
        except ArrowDataError as exc:
            raise CompactCacheError(str(exc)) from exc
        for key, expected in (
            ("trade_date", trade_date),
            ("source", source),
            ("symbol", symbol),
        ):
            if metadata.get(key) != expected:
                raise CompactCacheError(
                    f"compact symbol metadata mismatch for {key}: {file_path}"
                )
        if table.num_rows != int(details.get("rows", -1)):
            raise CompactCacheError(f"compact symbol row-count mismatch: {file_path}")
        adjustment = int(metadata["local_timestamp_adjustment_ns"])
        if adjustment != int(details.get("local_timestamp_adjustment_ns", -1)):
            raise CompactCacheError(
                f"compact timestamp-adjustment metadata mismatch: {file_path}"
            )
        exchange = table["exch_ts"].to_numpy(zero_copy_only=False)
        local = table["local_ts_raw"].to_numpy(zero_copy_only=False)
        sequence = table["source_seq"].to_numpy(zero_copy_only=False)
        prices = np.concatenate(
            (
                table["bid_px"].to_numpy(zero_copy_only=False),
                table["ask_px"].to_numpy(zero_copy_only=False),
            )
        )
        prices = prices[np.isfinite(prices) & (prices > 0)]
        observed_bounds = {
            "first_exch_ts": int(exchange.min()) if len(exchange) else None,
            "last_exch_ts": int(exchange.max()) if len(exchange) else None,
            "min_price": float(prices.min()) if len(prices) else None,
            "max_price": float(prices.max()) if len(prices) else None,
        }
        for key, observed in observed_bounds.items():
            if details.get(key) != observed:
                raise CompactCacheError(
                    f"compact partition fact mismatch for {key}: {file_path}"
                )
        ordering = timestamp_ordering_facts(
            exchange,
            local,
            sequence,
            base_latency_ns=int(metadata.get("base_latency_ns", "0")),
        )
        for key in (
            "raw_min_feed_latency_ns",
            "local_timestamp_adjustment_ns",
            "exchange_ordered",
            "local_ordered",
            "requires_dual_order",
        ):
            if details.get(key) != ordering[key]:
                raise CompactCacheError(
                    f"compact ordering metadata mismatch for {key}: {file_path}"
                )
        expected_sidecars = {
            f"{kind}_order": order
            for kind, order in (
                ("exchange", ordering["exchange_order"]),
                ("local", ordering["local_order"]),
            )
            if order is not None
        }
        sidecars = details.get("sidecars", {})
        if set(sidecars) != set(expected_sidecars):
            raise CompactCacheError(f"compact sidecar set mismatch: {file_path}")
        for key, expected_order in expected_sidecars.items():
            sidecar = sidecars[key]
            sidecar_name = _safe_component(sidecar.get("file"), "sidecar file")
            sidecar_path = file_path.parent / sidecar_name
            if (
                not sidecar_path.is_file()
                or sidecar_path.stat().st_size != sidecar.get("bytes")
            ):
                raise CompactCacheError(
                    f"missing or changed compact sidecar: {sidecar_path}"
                )
            if file_sha256(sidecar_path) != sidecar.get("sha256"):
                raise CompactCacheError(
                    f"compact sidecar checksum mismatch: {sidecar_path}"
                )
            validate_order_sidecar(
                sidecar_path,
                expected_order=expected_order,
                expected_details=sidecar,
            )


def validate_date_value(value: str) -> None:
    try:
        time.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"trade_date must be YYYY-MM-DD: {value!r}") from exc


def _safe_component(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or Path(value).name != value or any(
        separator in value for separator in ("/", "\\")
    ):
        raise CompactCacheError(f"invalid compact {label} path component: {value!r}")
    return value


def _validate_source_request(sources: Sequence[CompactSource]) -> None:
    kinds = [source.kind for source in sources]
    if len(kinds) != len(set(kinds)):
        raise CompactCacheError("compact source kinds must be unique per date build")
    paths = [Path(path).resolve() for source in sources for path in source.paths]
    if len(paths) != len(set(paths)):
        raise CompactCacheError(
            "each physical compact source path may be scanned at most once per date build"
        )


__all__ = ("CompactCacheStore",)
