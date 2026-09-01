from __future__ import annotations

from pathlib import Path

import pandas as pd

from future_spot.test.compare_engine_outputs import _compare_csv


def test_streaming_csv_comparison_is_bounded_and_ignores_engine_diagnostics(
    tmp_path: Path,
) -> None:
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    pd.DataFrame(
        {
            "trade_date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "value": [1.0, 2.0, float("nan")],
            "strategy_engine": ["numba"] * 3,
        }
    ).to_csv(left, index=False)
    pd.DataFrame(
        {
            "trade_date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "value": [1.0, 2.0, float("nan")],
            "strategy_engine": ["python"] * 3,
        }
    ).to_csv(right, index=False)

    result = _compare_csv(left, right, {"strategy_engine"}, chunk_rows=2)

    assert result == {"equal": True, "rows": 3, "mismatches": []}


def test_streaming_csv_comparison_reports_global_first_row(tmp_path: Path) -> None:
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    pd.DataFrame({"value": [1, 2, 3]}).to_csv(left, index=False)
    pd.DataFrame({"value": [1, 2, 4]}).to_csv(right, index=False)

    result = _compare_csv(left, right, set(), chunk_rows=2)

    assert not result["equal"]
    assert result["mismatches"] == [
        {"column": "value", "count": 1, "first_row": 2, "left": 3, "right": 4}
    ]
