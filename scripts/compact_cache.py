"""Versioned, atomic Arrow BBO cache for bounded daily market-data builds."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from scripts.tw_stock_data_to_npz import normalized_bbo_from_depth_columns


COMPACT_SCHEMA_VERSION = "bbo_v1"
COMPACT_BUILDER_VERSION = 1
COMPACT_ROW_ESTIMATE_BYTES = 96

BBO_SCHEMA = pa.schema(
    [
        ("source_seq", pa.uint64()),
        ("exch_ts", pa.int64()),
        ("local_ts_raw", pa.int64()),
        ("bid_px", pa.float64()),
        ("ask_px", pa.float64()),
        ("bid_qty", pa.float64()),
        ("ask_qty", pa.float64()),
        ("last_px", pa.float64()),
        ("total_volume", pa.int64()),
    ]
)

PROJECTED_COLUMNS = [
    "symbol",
    "symbol_id",
    "exchtime",
    "localtime",
    "status",
    "last_price",
    "total_volume",
    "sequence",
] + [
    f"{side}_{kind}{level}"
    for level in range(1, 6)
    for side in ("bid", "ask")
    for kind in ("price", "volume")
]


class CompactCacheError(RuntimeError):
    pass


class CompactCacheBudgetError(CompactCacheError):
    pass


@dataclass(frozen=True)
class CompactSource:
    kind: str
    paths: tuple[Path, ...]
    symbols: tuple[str, ...]
    status_allow: tuple[str, ...] = ()
    price_only_depth_qty: float | None = None
    volume_scale: float = 1.0


@dataclass(frozen=True)
class CompactBuildConfig:
    cache_root: Path
    compression: str = "lz4"
    profile: str = "bbo"
    timezone: str = "Asia/Taipei"
    session_start_ns: int | None = None
    session_end_ns: int | None = None
    base_latency_ns: int = 0
    batch_rows: int = 131_072
    max_cache_bytes: int = 200 * 1024**3
    min_free_bytes: int = 200 * 1024**3
    rebuild: bool = False

    def __post_init__(self) -> None:
        if self.profile != "bbo":
            raise ValueError("only compact profile 'bbo' is supported")
        if self.compression not in {"lz4", "zstd", "none"}:
            raise ValueError("compression must be lz4, zstd, or none")
        if self.batch_rows <= 0:
            raise ValueError("batch_rows must be positive")


@dataclass
class _SymbolState:
    rows: int = 0
    parts: list[Path] = field(default_factory=list)
    first_exch_ts: int | None = None
    last_exch_ts: int | None = None
    min_px: float | None = None
    max_px: float | None = None


class CompactCacheStore:
    def __init__(self, config: CompactBuildConfig):
        self.config = config
        self.root = Path(config.cache_root)

    def date_path(self, trade_date: str) -> Path:
        _validate_date(trade_date)
        return self.root / f"date={trade_date.replace('-', '')}"

    def build_date(self, trade_date: str, sources: Sequence[CompactSource]) -> dict[str, Any]:
        _validate_date(trade_date)
        expected = self._identity(trade_date, sources)
        final = self.date_path(trade_date)
        if final.exists():
            try:
                current = self.validate_date(trade_date)
            except CompactCacheError:
                current = None
            if current is not None and current.get("identity_sha256") == _canonical_sha256(expected):
                return {**current, "cache_state": "hit", "build_invocation_scan_count": 0}
            if not self.config.rebuild:
                raise CompactCacheError(
                    f"compact date exists with an incompatible identity; use rebuild or a new root: {final}"
                )

        self._preflight(sources)
        self.root.mkdir(parents=True, exist_ok=True)
        temp = Path(tempfile.mkdtemp(prefix=f".tmp-{trade_date}-", dir=self.root))
        started = time.perf_counter()
        try:
            source_manifests = {}
            for source in sources:
                source_manifests[source.kind] = self._build_source(temp, trade_date, source)
            payload = {
                "schema_version": COMPACT_SCHEMA_VERSION,
                "builder_version": COMPACT_BUILDER_VERSION,
                "trade_date": trade_date,
                "build_complete": True,
                "identity": expected,
                "identity_sha256": _canonical_sha256(expected),
                "sources": source_manifests,
                "elapsed_seconds": time.perf_counter() - started,
            }
            (temp / "manifest.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            self._validate_path(temp, trade_date)
            stale = None
            if final.exists():
                stale = self.root / f".superseded-{final.name}-{int(time.time_ns())}"
                os.replace(final, stale)
            try:
                os.replace(temp, final)
            except Exception:
                if stale is not None and stale.exists() and not final.exists():
                    os.replace(stale, final)
                raise
        except Exception:
            if temp.exists():
                shutil.rmtree(temp)
            raise
        validated = self.validate_date(trade_date)
        return {
            **validated,
            "cache_state": "miss",
            "build_invocation_scan_count": sum(
                int(source.get("scan_count", 0)) for source in validated.get("sources", {}).values()
            ),
        }

    def validate_date(self, trade_date: str) -> dict[str, Any]:
        return self._validate_path(self.date_path(trade_date), trade_date)

    def read_symbol(self, trade_date: str, source: str, symbol: str) -> pa.Table:
        manifest = self.validate_date(trade_date)
        details = manifest["sources"].get(source, {}).get("symbols", {}).get(str(symbol))
        if details is None:
            raise CompactCacheError(f"symbol not present in compact cache: {trade_date}/{source}/{symbol}")
        path = self.date_path(trade_date) / _source_dirname(source) / details["file"]
        with pa.memory_map(str(path), "r") as handle:
            return ipc.open_file(handle).read_all()

    def _identity(self, trade_date: str, sources: Sequence[CompactSource]) -> dict[str, Any]:
        return {
            "trade_date": trade_date,
            "schema_version": COMPACT_SCHEMA_VERSION,
            "builder_version": COMPACT_BUILDER_VERSION,
            "builder_sha256": _file_sha256(Path(__file__)),
            "top5_implementation_sha256": _file_sha256(
                Path(__file__).with_name("tw_stock_data_to_npz.py")
            ),
            "compression": self.config.compression,
            "profile": self.config.profile,
            "timezone": self.config.timezone,
            "session_start_ns": self.config.session_start_ns,
            "session_end_ns": self.config.session_end_ns,
            "base_latency_ns": self.config.base_latency_ns,
            "projected_columns": PROJECTED_COLUMNS,
            "sources": [
                {
                    "kind": source.kind,
                    "symbols": list(source.symbols),
                    "status_allow": list(source.status_allow),
                    "price_only_depth_qty": source.price_only_depth_qty,
                    "volume_scale": source.volume_scale,
                    "files": [_source_identity(path) for path in source.paths],
                }
                for source in sources
            ],
        }

    def _preflight(self, sources: Sequence[CompactSource]) -> None:
        source_rows = sum(_source_rows(path) for source in sources for path in source.paths)
        projected = math.ceil(source_rows * COMPACT_ROW_ESTIMATE_BYTES * 1.20)
        existing = _directory_bytes(self.root)
        if existing + projected > self.config.max_cache_bytes:
            raise CompactCacheBudgetError(
                f"compact cache budget exceeded: existing={existing} projected={projected} "
                f"limit={self.config.max_cache_bytes}"
            )
        free = shutil.disk_usage(self.root.parent if self.root.parent.exists() else Path.cwd()).free
        if free - projected < self.config.min_free_bytes:
            raise CompactCacheBudgetError(
                f"compact cache free-space reserve would be crossed: free={free} "
                f"projected={projected} reserve={self.config.min_free_bytes}"
            )

    def _build_source(self, temp: Path, trade_date: str, source: CompactSource) -> dict[str, Any]:
        started = time.perf_counter()
        source_dir = temp / _source_dirname(source.kind)
        parts_dir = source_dir / ".parts"
        parts_dir.mkdir(parents=True)
        states = {symbol: _SymbolState() for symbol in source.symbols}
        aliases = _symbol_aliases(source.symbols)
        scans = 0
        input_rows = 0
        source_offset = 0
        for path in source.paths:
            scans += 1
            for batch in _iter_source_batches(path, self.config.batch_rows):
                input_rows += batch.num_rows
                seq = np.arange(source_offset, source_offset + batch.num_rows, dtype=np.uint64)
                source_offset += batch.num_rows
                symbols = _string_array(batch, "symbol", fallback="symbol_id")
                for raw_symbol in np.unique(symbols):
                    symbol = aliases.get(str(raw_symbol))
                    if symbol is None:
                        continue
                    mask = symbols == raw_symbol
                    compact = _compact_batch(batch, seq, mask, source, self.config)
                    if compact.num_rows == 0:
                        continue
                    state = states[symbol]
                    part = parts_dir / f"{symbol}-{len(state.parts):06d}.arrow"
                    _write_arrow(part, compact, self.config.compression)
                    state.parts.append(part)
                    state.rows += compact.num_rows
                    exch = compact["exch_ts"].to_numpy(zero_copy_only=False)
                    state.first_exch_ts = int(exch.min()) if state.first_exch_ts is None else min(state.first_exch_ts, int(exch.min()))
                    state.last_exch_ts = int(exch.max()) if state.last_exch_ts is None else max(state.last_exch_ts, int(exch.max()))
                self._runtime_budget_check(temp)

        symbol_manifest = {}
        for symbol, state in states.items():
            if not state.parts:
                output = source_dir / f"{symbol}.arrow"
                symbol_manifest[symbol] = _write_empty_symbol(
                    output,
                    symbol=symbol,
                    source=source.kind,
                    trade_date=trade_date,
                    compression=self.config.compression,
                    base_latency_ns=self.config.base_latency_ns,
                )
                continue
            output = source_dir / f"{symbol}.arrow"
            details = _consolidate_symbol(
                output,
                state.parts,
                symbol=symbol,
                source=source.kind,
                trade_date=trade_date,
                compression=self.config.compression,
                base_latency_ns=self.config.base_latency_ns,
            )
            symbol_manifest[symbol] = details
            self._runtime_budget_check(temp)
        shutil.rmtree(parts_dir)
        output_bytes = _directory_bytes(source_dir)
        payload = {
            "kind": source.kind,
            "scan_count": scans,
            "input_rows": input_rows,
            "input_bytes": sum(path.stat().st_size for path in source.paths),
            "output_rows": sum(item.get("rows", 0) for item in symbol_manifest.values()),
            "output_bytes": output_bytes,
            "elapsed_seconds": time.perf_counter() - started,
            "missing_symbols": sorted(
                symbol for symbol, item in symbol_manifest.items() if item.get("status") == "missing"
            ),
            "empty_symbols": sorted(
                symbol for symbol, item in symbol_manifest.items() if item.get("empty") is True
            ),
            "symbols": symbol_manifest,
        }
        (source_dir / "manifest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return payload

    def _runtime_budget_check(self, temp: Path) -> None:
        used = _directory_bytes(self.root)
        if used > self.config.max_cache_bytes:
            raise CompactCacheBudgetError(f"compact cache exceeded limit during build: {used}")
        free = shutil.disk_usage(temp).free
        if free < self.config.min_free_bytes:
            raise CompactCacheBudgetError(f"compact cache free-space reserve crossed during build: {free}")

    def _validate_path(self, path: Path, trade_date: str) -> dict[str, Any]:
        try:
            payload = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CompactCacheError(f"invalid or incomplete compact date: {path}") from exc
        if payload.get("schema_version") != COMPACT_SCHEMA_VERSION or payload.get("build_complete") is not True:
            raise CompactCacheError(f"unsupported or incomplete compact date: {path}")
        if payload.get("trade_date") != trade_date:
            raise CompactCacheError(f"compact date identity mismatch: {path}")
        if payload.get("identity_sha256") != _canonical_sha256(payload.get("identity")):
            raise CompactCacheError(f"compact identity checksum mismatch: {path}")
        for source, source_details in payload.get("sources", {}).items():
            source_manifest_path = path / _source_dirname(source) / "manifest.json"
            try:
                source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise CompactCacheError(f"missing compact source manifest: {source_manifest_path}") from exc
            if source_manifest != source_details:
                raise CompactCacheError(f"compact source manifest mismatch: {source_manifest_path}")
            for symbol, details in source_details.get("symbols", {}).items():
                if details.get("status") == "missing":
                    continue
                file_path = path / _source_dirname(source) / details["file"]
                if not file_path.is_file() or file_path.stat().st_size != details["bytes"]:
                    raise CompactCacheError(f"missing or changed compact symbol: {file_path}")
                if _file_sha256(file_path) != details["sha256"]:
                    raise CompactCacheError(f"compact symbol checksum mismatch: {file_path}")
                for sidecar in details.get("sidecars", {}).values():
                    sidecar_path = path / _source_dirname(source) / sidecar["file"]
                    if not sidecar_path.is_file() or sidecar_path.stat().st_size != sidecar["bytes"]:
                        raise CompactCacheError(f"missing or changed compact sidecar: {sidecar_path}")
                    if _file_sha256(sidecar_path) != sidecar["sha256"]:
                        raise CompactCacheError(f"compact sidecar checksum mismatch: {sidecar_path}")
        return payload


def _compact_batch(
    batch: pa.RecordBatch,
    source_seq: np.ndarray,
    mask: np.ndarray,
    source: CompactSource,
    config: CompactBuildConfig,
) -> pa.Table:
    indexes = np.flatnonzero(mask)
    exch = _numeric(batch, "exchtime", indexes, np.int64)
    local = _numeric(batch, "localtime", indexes, np.int64, fallback=exch)
    keep = np.ones(len(indexes), dtype=bool)
    if source.status_allow and "status" in batch.schema.names:
        status = _string_array(batch, "status")[indexes]
        keep &= np.isin(status, np.asarray(source.status_allow, dtype=str))
    if config.session_start_ns is not None:
        keep &= exch >= config.session_start_ns
    if config.session_end_ns is not None:
        keep &= exch <= config.session_end_ns
    indexes = indexes[keep]
    exch = exch[keep]
    local = local[keep]
    seq = source_seq[indexes]
    if not len(indexes):
        return pa.Table.from_batches([], schema=BBO_SCHEMA)
    bid_prices = _matrix(batch, "bid_price", indexes)
    ask_prices = _matrix(batch, "ask_price", indexes)
    bid_qtys = _matrix(batch, "bid_volume", indexes)
    ask_qtys = _matrix(batch, "ask_volume", indexes)
    bid_px, bid_qty = _best_side(bid_prices, bid_qtys, True, source)
    ask_px, ask_qty = _best_side(ask_prices, ask_qtys, False, source)
    last_px = _numeric(batch, "last_price", indexes, np.float64, default=np.nan)
    total_volume = _numeric(batch, "total_volume", indexes, np.int64, default=0)
    return pa.Table.from_arrays(
        [seq, exch, local, bid_px, ask_px, bid_qty, ask_qty, last_px, total_volume],
        schema=BBO_SCHEMA,
    )


def _best_side(prices: np.ndarray, quantities: np.ndarray, bid: bool, source: CompactSource) -> tuple[np.ndarray, np.ndarray]:
    return normalized_bbo_from_depth_columns(
        np.ascontiguousarray(prices),
        np.ascontiguousarray(quantities),
        source.volume_scale,
        0.0 if source.price_only_depth_qty is None else source.price_only_depth_qty,
        source.price_only_depth_qty is not None,
        bid,
    )


def _consolidate_symbol(output: Path, parts: Sequence[Path], **metadata: Any) -> dict[str, Any]:
    tables = []
    for part in parts:
        with pa.memory_map(str(part), "r") as handle:
            tables.append(ipc.open_file(handle).read_all())
    table = pa.concat_tables(tables)
    exch = table["exch_ts"].to_numpy(zero_copy_only=False)
    local = table["local_ts_raw"].to_numpy(zero_copy_only=False)
    source_seq = table["source_seq"].to_numpy(zero_copy_only=False)
    min_latency = int(np.min(local - exch))
    adjustment = -min_latency + int(metadata["base_latency_ns"]) if min_latency < 0 else 0
    exchange_ordered = bool(np.all(exch[1:] >= exch[:-1]))
    corrected_local = local + adjustment
    local_ordered = bool(np.all(corrected_local[1:] >= corrected_local[:-1]))
    file_metadata = {
        **{key: str(value) for key, value in metadata.items()},
        "schema_version": COMPACT_SCHEMA_VERSION,
        "local_timestamp_adjustment_ns": str(adjustment),
    }
    table = table.replace_schema_metadata({key.encode(): value.encode() for key, value in file_metadata.items()})
    _write_arrow(output, table, metadata["compression"])
    sidecars = {}
    if not exchange_ordered:
        sidecars["exchange_order"] = _write_order_sidecar(output, "exchange", np.lexsort((source_seq, exch)))
    if not local_ordered:
        sidecars["local_order"] = _write_order_sidecar(output, "local", np.lexsort((source_seq, corrected_local)))
    prices = np.concatenate(
        [table["bid_px"].to_numpy(zero_copy_only=False), table["ask_px"].to_numpy(zero_copy_only=False)]
    )
    prices = prices[np.isfinite(prices) & (prices > 0)]
    return {
        "file": output.name,
        "rows": table.num_rows,
        "bytes": output.stat().st_size,
        "sha256": _file_sha256(output),
        "first_exch_ts": int(exch.min()),
        "last_exch_ts": int(exch.max()),
        "raw_min_feed_latency_ns": min_latency,
        "local_timestamp_adjustment_ns": adjustment,
        "exchange_ordered": exchange_ordered,
        "local_ordered": local_ordered,
        "requires_dual_order": not (exchange_ordered and local_ordered),
        "min_price": float(prices.min()) if len(prices) else None,
        "max_price": float(prices.max()) if len(prices) else None,
        "sidecars": sidecars,
        "status": "valid",
    }


def _write_empty_symbol(output: Path, **metadata: Any) -> dict[str, Any]:
    """Publish a source-present, zero-row symbol as a valid empty partition.

    The reference converter emits an empty NPZ for this case. Treating it as a
    missing input would incorrectly drop the configured pair and change carry,
    error, and summary semantics around contract expiry.
    """
    file_metadata = {
        **{key: str(value) for key, value in metadata.items()},
        "schema_version": COMPACT_SCHEMA_VERSION,
        "local_timestamp_adjustment_ns": "0",
    }
    table = pa.Table.from_batches([], schema=BBO_SCHEMA).replace_schema_metadata(
        {key.encode(): value.encode() for key, value in file_metadata.items()}
    )
    _write_arrow(output, table, metadata["compression"])
    return {
        "file": output.name,
        "rows": 0,
        "bytes": output.stat().st_size,
        "sha256": _file_sha256(output),
        "first_exch_ts": None,
        "last_exch_ts": None,
        "raw_min_feed_latency_ns": None,
        "local_timestamp_adjustment_ns": 0,
        "exchange_ordered": True,
        "local_ordered": True,
        "requires_dual_order": False,
        "min_price": None,
        "max_price": None,
        "sidecars": {},
        "empty": True,
        "status": "valid",
    }


def _write_order_sidecar(output: Path, kind: str, order: np.ndarray) -> dict[str, Any]:
    path = output.with_name(f"{output.stem}.{kind}_order.arrow")
    _write_arrow(path, pa.table({"row_index": pa.array(order.astype(np.uint64))}), "lz4")
    return {"file": path.name, "rows": len(order), "bytes": path.stat().st_size, "sha256": _file_sha256(path)}


def _iter_source_batches(path: Path, batch_rows: int) -> Iterator[pa.RecordBatch]:
    names: list[str]
    if path.suffix.lower() == ".parquet":
        parquet = pq.ParquetFile(path)
        names = [name for name in PROJECTED_COLUMNS if name in parquet.schema_arrow.names]
        yield from parquet.iter_batches(batch_size=batch_rows, columns=names, use_threads=True)
        return
    with pa.memory_map(str(path), "r") as handle:
        reader = ipc.open_file(handle)
        names = [name for name in PROJECTED_COLUMNS if name in reader.schema.names]
        for index in range(reader.num_record_batches):
            table = pa.Table.from_batches([reader.get_batch(index)]).select(names)
            yield from table.to_batches(max_chunksize=batch_rows)


def _numeric(batch: pa.RecordBatch, name: str, indexes: np.ndarray, dtype, *, fallback=None, default=0):
    if name not in batch.schema.names:
        return np.asarray(fallback if fallback is not None else np.full(len(indexes), default), dtype=dtype)
    values = pc.cast(batch.column(batch.schema.get_field_index(name)), pa.float64() if dtype == np.float64 else pa.int64(), safe=False)
    values = pc.fill_null(values, pa.scalar(default, type=values.type)).to_numpy(zero_copy_only=False)[indexes]
    if fallback is not None:
        values = np.where(values == 0, fallback, values)
    return np.asarray(values, dtype=dtype)


def _matrix(batch: pa.RecordBatch, prefix: str, indexes: np.ndarray) -> np.ndarray:
    return np.column_stack(
        [_numeric(batch, f"{prefix}{level}", indexes, np.float64, default=np.nan) for level in range(1, 6)]
    )


def _string_array(batch: pa.RecordBatch, name: str, fallback: str | None = None) -> np.ndarray:
    if name not in batch.schema.names:
        if fallback is None or fallback not in batch.schema.names:
            raise CompactCacheError(f"source is missing {name!r} and {fallback!r}")
        name = fallback
    column = batch.column(batch.schema.get_field_index(name))
    return np.asarray(pc.cast(column, pa.string(), safe=False).to_pylist(), dtype=str)


def _symbol_aliases(symbols: Iterable[str]) -> dict[str, str]:
    aliases = {}
    for symbol in symbols:
        for value in (str(symbol), str(symbol).lstrip("0")):
            if value and value in aliases and aliases[value] != symbol:
                raise CompactCacheError(f"ambiguous symbol alias: {value}")
            if value:
                aliases[value] = str(symbol)
    return aliases


def _write_arrow(path: Path, table: pa.Table, compression: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    options = ipc.IpcWriteOptions(compression=None if compression == "none" else compression)
    with path.open("wb") as sink, ipc.new_file(sink, table.schema, options=options) as writer:
        writer.write_table(table)


def _source_rows(path: Path) -> int:
    if path.suffix.lower() == ".parquet":
        return int(pq.ParquetFile(path).metadata.num_rows)
    with pa.memory_map(str(path), "r") as handle:
        reader = ipc.open_file(handle)
        return sum(reader.get_batch(index).num_rows for index in range(reader.num_record_batches))


def _source_identity(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {"path": str(path.resolve()), "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns, "rows": _source_rows(path)}


def _directory_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file()) if path.exists() else 0


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _validate_date(value: str) -> None:
    try:
        time.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"trade_date must be YYYY-MM-DD: {value!r}") from exc


def _source_dirname(source: str) -> str:
    return f"source={source}"
