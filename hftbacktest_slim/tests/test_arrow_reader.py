from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from hftbacktest_slim import ArrowDataError
from hftbacktest_slim.engine.arrow_reader import SLIM_ROW_DTYPE, read_rows


EXPECTED_NAMES = (
    "source_seq",
    "exch_ts",
    "local_ts_raw",
    "bid_px",
    "ask_px",
    "bid_qty",
    "ask_qty",
    "last_px",
    "total_volume",
)


def test_native_dtype_has_exact_names_offsets_and_item_size() -> None:
    assert SLIM_ROW_DTYPE.names == EXPECTED_NAMES
    assert SLIM_ROW_DTYPE.itemsize == 72
    assert [SLIM_ROW_DTYPE.fields[name][1] for name in EXPECTED_NAMES] == list(
        range(0, 72, 8)
    )
    assert SLIM_ROW_DTYPE.isalignedstruct


def test_reader_preserves_metadata_values_order_and_decimal_prices(
    tmp_path: Path, write_partition
) -> None:
    path = write_partition(
        tmp_path / "rows.arrow",
        [
            (9, 101, 81, 77.90, 77.95, 12.5, 3.25, 77.925, 8),
            (2, 100, 80, 78.00, 78.05, 2.0, 1.0, 78.025, 9),
        ],
        adjustment_ns=20,
    )

    loaded = read_rows(path)

    assert loaded.local_timestamp_adjustment_ns == 20
    assert loaded.rows.dtype == SLIM_ROW_DTYPE
    assert loaded.rows["source_seq"].tolist() == [9, 2]
    assert loaded.rows["exch_ts"].tolist() == [101, 100]
    assert loaded.rows["local_ts_raw"].tolist() == [81, 80]
    np.testing.assert_array_equal(loaded.rows["bid_px"], [77.90, 78.00])
    np.testing.assert_array_equal(loaded.rows["ask_px"], [77.95, 78.05])


def test_reader_accepts_empty_partition_and_legacy_zero_adjustment_default(
    tmp_path: Path, write_partition
) -> None:
    path = write_partition(
        tmp_path / "empty.arrow", [], adjustment_ns=None, schema_version=None
    )
    loaded = read_rows(path)
    assert loaded.rows.shape == (0,)
    assert loaded.rows.dtype == SLIM_ROW_DTYPE
    assert loaded.local_timestamp_adjustment_ns == 0


def test_reader_rejects_missing_column(tmp_path: Path, write_partition) -> None:
    schema = pa.schema(
        [(name, pa.float64()) for name in EXPECTED_NAMES if name != "source_seq"]
    )
    path = write_partition(tmp_path / "missing.arrow", [], schema=schema)
    with pytest.raises(ArrowDataError, match="source_seq"):
        read_rows(path)


def test_reader_rejects_incompatible_physical_type(tmp_path: Path, write_partition) -> None:
    fields = [
        ("source_seq", pa.int64()),
        ("exch_ts", pa.int64()),
        ("local_ts_raw", pa.int64()),
        ("bid_px", pa.float64()),
        ("ask_px", pa.float64()),
        ("bid_qty", pa.float64()),
        ("ask_qty", pa.float64()),
        ("last_px", pa.float64()),
        ("total_volume", pa.int64()),
    ]
    path = write_partition(tmp_path / "wrong.arrow", [], schema=pa.schema(fields))
    with pytest.raises(ArrowDataError, match="source_seq.*incompatible type"):
        read_rows(path)


def test_reader_rejects_incompatible_declared_schema(tmp_path: Path, write_partition) -> None:
    path = write_partition(tmp_path / "future.arrow", [], schema_version="bbo_v2")
    with pytest.raises(ArrowDataError, match="bbo_v2"):
        read_rows(path)


def test_reader_rejects_extra_or_reordered_physical_fields(
    tmp_path: Path, write_partition
) -> None:
    extra = pa.schema([*PHYSICAL_EXTRA_BASE(), ("extra", pa.int64())])
    extra_path = write_partition(tmp_path / "extra.arrow", [], schema=extra)
    with pytest.raises(ArrowDataError, match="fields/order"):
        read_rows(extra_path)

    fields = PHYSICAL_EXTRA_BASE()
    reordered = pa.schema([fields[1], fields[0], *fields[2:]])
    reordered_path = write_partition(
        tmp_path / "reordered.arrow", [], schema=reordered
    )
    with pytest.raises(ArrowDataError, match="fields/order"):
        read_rows(reordered_path)


def PHYSICAL_EXTRA_BASE() -> list[tuple[str, pa.DataType]]:
    return [
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
