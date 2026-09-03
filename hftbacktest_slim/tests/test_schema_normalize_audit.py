from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa

from hftbacktest_slim import (
    BBO_SCHEMA,
    COMPACT_SCHEMA_VERSION,
    aggregate_depth_side,
    normalized_bbo_from_depth_columns,
)
from hftbacktest_slim.market_data import (
    SLIM_ROW_DTYPE,
    compact_partition_audit,
)
from hftbacktest_slim.market_data.schema import PHYSICAL_FIELDS


EXPECTED_FIELDS = (
    ("source_seq", pa.uint64()),
    ("exch_ts", pa.int64()),
    ("local_ts_raw", pa.int64()),
    ("bid_px", pa.float64()),
    ("ask_px", pa.float64()),
    ("bid_qty", pa.float64()),
    ("ask_qty", pa.float64()),
    ("last_px", pa.float64()),
    ("total_volume", pa.int64()),
)


def test_canonical_schema_and_native_dtype_are_one_exact_contract() -> None:
    assert COMPACT_SCHEMA_VERSION == "bbo_v1"
    assert PHYSICAL_FIELDS == EXPECTED_FIELDS
    assert [(field.name, field.type, field.nullable) for field in BBO_SCHEMA] == [
        (name, data_type, True) for name, data_type in EXPECTED_FIELDS
    ]
    assert SLIM_ROW_DTYPE.names == tuple(BBO_SCHEMA.names)
    assert [SLIM_ROW_DTYPE.fields[name][1] for name in BBO_SCHEMA.names] == list(
        range(0, 72, 8)
    )
    assert SLIM_ROW_DTYPE.itemsize == 72
    assert SLIM_ROW_DTYPE.isalignedstruct


def test_normalization_aggregates_sorts_filters_and_preserves_decimals() -> None:
    prices = np.ascontiguousarray(
        [
            [77.90, 77.95, 77.95, 77.85, np.nan],
            [np.nan, 78.00, 77.90, 0.0, -1.0],
            [np.inf, np.nan, 0.0, -1.0, np.nan],
        ],
        dtype=np.float64,
    )
    quantities = np.ascontiguousarray(
        [
            [1.0, 2.0, 3.0, 4.0, 5.0],
            [1.0, 2.5, 9.0, 1.0, 1.0],
            [1.0, np.nan, 1.0, 1.0, 1.0],
        ],
        dtype=np.float64,
    )
    bid_px, bid_qty = normalized_bbo_from_depth_columns(
        prices, quantities, 2.0, 0.0, False, True
    )
    ask_px, ask_qty = normalized_bbo_from_depth_columns(
        prices, quantities, 2.0, 0.0, False, False
    )
    np.testing.assert_equal(bid_px, [77.95, 78.00, np.nan])
    np.testing.assert_equal(bid_qty, [10.0, 5.0, np.nan])
    np.testing.assert_equal(ask_px, [77.85, 77.90, np.nan])
    np.testing.assert_equal(ask_qty, [8.0, 18.0, np.nan])


def test_normalization_price_only_quantity_and_nonpositive_quantity_rules() -> None:
    prices = np.ascontiguousarray([[10.0, 11.0, 12.0, 13.0, 14.0]])
    quantities = np.ascontiguousarray([[np.nan, 0.0, -1.0, np.inf, 2.0]])
    best_px, best_qty = normalized_bbo_from_depth_columns(
        prices, quantities, 3.0, 1.5, True, False
    )
    # Missing/non-finite quantities use the placeholder. Explicit zero and
    # negative quantities remain invalid. The locked/crossed relation is not
    # altered by per-side normalization.
    np.testing.assert_equal(best_px, [10.0])
    np.testing.assert_equal(best_qty, [4.5])


def test_normalization_preserves_locked_and_crossed_best_prices() -> None:
    bids = np.ascontiguousarray([[101.0, 100.0], [100.0, 99.0]])
    asks = np.ascontiguousarray([[100.0, 102.0], [100.0, 101.0]])
    quantities = np.ascontiguousarray([[1.0, 1.0], [1.0, 1.0]])
    bid_px, _ = normalized_bbo_from_depth_columns(
        bids, quantities, 1.0, 0.0, False, True
    )
    ask_px, _ = normalized_bbo_from_depth_columns(
        asks, quantities, 1.0, 0.0, False, False
    )
    assert bid_px.tolist() == [101.0, 100.0]
    assert ask_px.tolist() == [100.0, 100.0]


def test_reference_converter_uses_the_package_normalization_object() -> None:
    from scripts import tw_stock_data_to_npz as converter

    assert converter.aggregate_depth_side is aggregate_depth_side
    assert converter.normalized_bbo_from_depth_columns is normalized_bbo_from_depth_columns


def test_generic_audit_preserves_corrected_latency_and_raw_price_facts(
    tmp_path: Path, write_partition
) -> None:
    path = write_partition(
        tmp_path / "audit.arrow",
        [
            (0, 100, 90, 77.90, 77.95, 2.0, 3.0, 77.95, 1),
            (1, 110, 105, 78.00, 78.05, 2.0, 3.0, 78.00, 3),
        ],
        adjustment_ns=10,
    )
    facts = compact_partition_audit(path)
    assert facts["rows"] == 2
    assert facts["first_exch_ts"] == 100
    assert facts["last_exch_ts"] == 110
    assert facts["raw_min_feed_latency_ns"] == -10
    assert facts["min_latency_ns"] == 0
    assert facts["max_latency_ns"] == 5
    assert facts["min_price"] == 77.90
    assert facts["max_price"] == 78.05
    assert facts["trade_events"] == 1
