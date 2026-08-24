from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

import numpy as np
import polars as pl

from scripts.tw_stock_data_to_npz import (
    EVENT_DTYPE,
    build_events_from_parquet_frame,
    build_events_from_rows,
    convert_tw_stock_to_npz,
)


def converter_args(*, levels: int = 2, price_only_depth_qty: float | None = None) -> argparse.Namespace:
    return argparse.Namespace(
        input_csv=None,
        symbol="0050",
        status_allow=None,
        start_exch_ts=None,
        end_exch_ts=None,
        timestamp_unit="auto",
        date="2023-11-14",
        timezone="Asia/Taipei",
        no_trades=False,
        trade_side="infer",
        volume_scale=1.0,
        no_depth=False,
        levels=levels,
        price_only_depth_qty=price_only_depth_qty,
        qa_sample_rows=1000,
        base_latency_ns=0,
    )


def sample_rows() -> list[dict[str, object]]:
    return [
        {
            "symbol": "0050",
            "exchtime": 1_700_000_000_000_000_000,
            "localtime": 1_700_000_000_001_000_000,
            "status": 0,
            "last_price": 77.95,
            "total_volume": 0,
            "sequence": 1,
            "ask_price1": 78.00,
            "ask_volume1": 10,
            "bid_price1": 77.95,
            "bid_volume1": 8,
            "ask_price2": 78.05,
            "ask_volume2": 5,
            "bid_price2": 77.90,
            "bid_volume2": 7,
        },
        {
            "symbol": "0050",
            "exchtime": 1_700_000_000_010_000_000,
            "localtime": 1_700_000_000_011_000_000,
            "status": 0,
            "last_price": 78.00,
            "total_volume": 3,
            "sequence": 2,
            # Same-price asks exercise the required MBP aggregation.
            "ask_price1": 78.05,
            "ask_volume1": 4,
            "bid_price1": 78.00,
            "bid_volume1": 6,
            "ask_price2": 78.05,
            "ask_volume2": 2,
            "bid_price2": 77.95,
            "bid_volume2": 5,
        },
        {
            "symbol": "0050",
            "exchtime": 1_700_000_000_020_000_000,
            "localtime": None,
            "status": 0,
            "last_price": 77.95,
            "total_volume": 5,
            "sequence": 3,
            "ask_price1": 78.00,
            "ask_volume1": 9,
            "bid_price1": 77.95,
            "bid_volume1": 10,
            "ask_price2": None,
            "ask_volume2": None,
            "bid_price2": 77.90,
            "bid_volume2": 3,
        },
    ]


class ParquetConversionTest(unittest.TestCase):
    def test_parquet_column_builder_matches_legacy_row_builder(self) -> None:
        rows = sample_rows()
        args = converter_args()

        legacy, legacy_stats = build_events_from_rows(rows, args)
        columnar, columnar_stats = build_events_from_parquet_frame(pl.DataFrame(rows), args)

        np.testing.assert_array_equal(columnar, legacy)
        self.assertEqual(columnar.dtype, EVENT_DTYPE)
        self.assertEqual(columnar_stats.raw_events, legacy_stats.raw_events)
        self.assertEqual(columnar_stats.depth_events, legacy_stats.depth_events)
        self.assertEqual(columnar_stats.trade_events, legacy_stats.trade_events)
        self.assertEqual(columnar_stats.opening_jump_qty, legacy_stats.opening_jump_qty)
        self.assertEqual(columnar_stats.best_bid_mismatches, 0)
        self.assertEqual(columnar_stats.best_ask_mismatches, 0)

    def test_parquet_column_builder_matches_price_only_depth(self) -> None:
        rows = sample_rows()
        for row in rows:
            for level in (1, 2):
                row.pop(f"ask_volume{level}")
                row.pop(f"bid_volume{level}")
        args = converter_args(price_only_depth_qty=1.0)

        legacy, _ = build_events_from_rows(rows, args)
        columnar, _ = build_events_from_parquet_frame(pl.DataFrame(rows), args)

        np.testing.assert_array_equal(columnar, legacy)

    def test_parquet_column_builder_matches_string_time_of_day(self) -> None:
        rows = sample_rows()[:2]
        rows[0]["exchtime"] = "09:00:00.000001"
        rows[0]["localtime"] = "09:00:00.001001"
        rows[1]["exchtime"] = "09:00:00.010001"
        rows[1]["localtime"] = "09:00:00.011001"
        args = converter_args()

        legacy, _ = build_events_from_rows(rows, args)
        columnar, _ = build_events_from_parquet_frame(pl.DataFrame(rows), args)

        np.testing.assert_array_equal(columnar, legacy)

    def test_daily_parquet_conversion_supports_uncompressed_npz(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            parquet_dir = root / "parquet"
            parquet_dir.mkdir()
            pl.DataFrame(sample_rows()).write_parquet(parquet_dir / "2023-11-14.parquet")
            output = root / "0050.npz"

            output_path, converted = convert_tw_stock_to_npz(
                symbol="0050",
                start_date="2023-11-14",
                output=output,
                workspace_root=root,
                data_api=False,
                daily_parquet=True,
                daily_parquet_dir=parquet_dir,
                source_kind="stock",
                levels=2,
                npz_compression="uncompressed",
            )

            self.assertEqual(output_path, output)
            with np.load(output) as saved:
                np.testing.assert_array_equal(saved["data"], converted)
                self.assertEqual(int(saved["event_rows"][0]), len(converted))
                self.assertEqual(float(saved["min_price"][0]), 77.90)
                self.assertEqual(float(saved["max_price"][0]), 78.05)


if __name__ == "__main__":
    unittest.main()
