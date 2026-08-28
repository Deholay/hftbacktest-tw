from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from scripts.daily_result_store import (
    DailyResultConflictError,
    DailyResultStore,
    DailyResultStoreError,
)


class DailyResultStoreTest(unittest.TestCase):
    def test_publish_validates_and_reuses_identical_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DailyResultStore(Path(tmp) / "core")
            tables = {
                "summary": pd.DataFrame({"run_key": ["d::p"], "pnl": [1.25]}),
                "trades": pd.DataFrame({"run_key": ["d::p"], "qty": [1]}),
            }
            first = store.publish(
                "2026-03-02",
                tables,
                input_identity={"engine": "reference", "version": 1},
                carry_in={},
                carry_out={"p": 1},
                run_keys=["d::p"],
            )
            second = store.publish(
                "2026-03-02",
                tables,
                input_identity={"engine": "reference", "version": 1},
                carry_in={},
                carry_out={"p": 1},
                run_keys=["d::p"],
            )

            self.assertEqual(first.path, second.path)
            pd.testing.assert_frame_equal(store.load_table("2026-03-02", "summary"), tables["summary"])
            self.assertTrue(first.payload["build_complete"])

    def test_completed_date_is_immutable_on_identity_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DailyResultStore(Path(tmp) / "core")
            store.publish(
                "2026-03-02",
                {"summary": pd.DataFrame({"rows": [1]})},
                input_identity={"version": 1},
                carry_in={},
                carry_out={},
                run_keys=[],
            )
            with self.assertRaises(DailyResultConflictError):
                store.publish(
                    "2026-03-02",
                    {"summary": pd.DataFrame({"rows": [2]})},
                    input_identity={"version": 2},
                    carry_in={},
                    carry_out={},
                    run_keys=[],
                )

    def test_incomplete_date_is_not_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DailyResultStore(Path(tmp) / "core")
            date_path = store.date_path("2026-03-02")
            date_path.mkdir(parents=True)
            (date_path / "manifest.json").write_text(
                json.dumps({"schema_version": 1, "build_complete": False}),
                encoding="utf-8",
            )
            with self.assertRaises(DailyResultStoreError):
                store.validate("2026-03-02")

    def test_validated_prefix_stops_at_broken_carry_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DailyResultStore(Path(tmp) / "core")
            store.publish(
                "2026-03-02",
                {"summary": pd.DataFrame()},
                input_identity={"date": 1},
                carry_in={},
                carry_out={"p": 1},
                run_keys=[],
            )
            store.publish(
                "2026-03-03",
                {"summary": pd.DataFrame()},
                input_identity={"date": 2},
                carry_in={"different": 1},
                carry_out={},
                run_keys=[],
            )
            prefix = store.validated_prefix(
                ["2026-03-02", "2026-03-03"],
                first_carry={},
                input_identities={
                    "2026-03-02": {"date": 1},
                    "2026-03-03": {"date": 2},
                },
            )
            self.assertEqual([item.trade_date for item in prefix], ["2026-03-02"])

    def test_compatibility_csv_streams_dates_with_schema_union(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store = DailyResultStore(root / "core")
            store.publish(
                "2026-03-02",
                {"trades": pd.DataFrame({"run_key": ["a"], "qty": [1]})},
                input_identity={"date": 1},
                carry_in={},
                carry_out={},
                run_keys=["a"],
            )
            store.publish(
                "2026-03-03",
                {"trades": pd.DataFrame({"run_key": ["b"], "price": [10.0]})},
                input_identity={"date": 2},
                carry_in={},
                carry_out={},
                run_keys=["b"],
            )
            output = store.write_table_csv(
                ["2026-03-02", "2026-03-03"],
                "trades",
                root / "trades.csv",
            )

            actual = pd.read_csv(output)
            self.assertEqual(list(actual.columns), ["run_key", "qty", "price"])
            self.assertEqual(actual["run_key"].tolist(), ["a", "b"])


if __name__ == "__main__":
    unittest.main()
