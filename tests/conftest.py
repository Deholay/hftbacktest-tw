from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest


PHYSICAL_SCHEMA = pa.schema(
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


@pytest.fixture
def native_library_path() -> Path:
    path = Path(__file__).resolve().parents[1] / "target" / "release" / "libhbt_slim.so"
    assert path.is_file(), "run cargo build --workspace --release"
    return path


@pytest.fixture
def write_partition() -> Callable[..., Path]:
    def write(
        path: Path,
        rows: list[tuple[Any, ...]],
        *,
        adjustment_ns: int | None = 0,
        schema: pa.Schema = PHYSICAL_SCHEMA,
        schema_version: str | None = "bbo_v1",
    ) -> Path:
        table = pa.Table.from_pylist(
            [dict(zip(schema.names, row)) for row in rows], schema=schema
        )
        metadata: dict[bytes, bytes] = {}
        if schema_version is not None:
            metadata[b"schema_version"] = schema_version.encode()
        if adjustment_ns is not None:
            metadata[b"local_timestamp_adjustment_ns"] = str(adjustment_ns).encode()
        table = table.replace_schema_metadata(metadata)
        with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        return path

    return write
