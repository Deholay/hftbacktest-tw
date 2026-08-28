from __future__ import annotations

import polars as pl

from scripts.source_parity import compare_source_frames


def _frame(*, timestamps_as_text: bool, changed: bool = False) -> pl.DataFrame:
    numeric_timestamps = [1_772_425_800_000_000_000, 1_772_425_801_000_000_000]
    timestamps = [str(value) for value in numeric_timestamps] if timestamps_as_text else numeric_timestamps
    values = {
        "symbol": ["50", "50"] if timestamps_as_text else ["0050", "0050"],
        "exchtime": timestamps,
        "localtime": timestamps,
        "status": ["OK", "OK"],
        "last_price": [77.9, 78.0 if not changed else 78.05],
        "total_volume": [1, 2],
        "sequence": [1, 2],
    }
    for level in range(1, 6):
        values[f"bid_price{level}"] = [77.8 if level == 1 else None] * 2
        values[f"ask_price{level}"] = [78.0 if level == 1 else None] * 2
        values[f"bid_volume{level}"] = [1 if level == 1 else None] * 2
        values[f"ask_volume{level}"] = [1 if level == 1 else None] * 2
    return pl.DataFrame(values)


def test_canonical_parity_ignores_provider_dtype_and_symbol_representation() -> None:
    result = compare_source_frames(
        _frame(timestamps_as_text=True),
        _frame(timestamps_as_text=False),
        symbol="0050",
        trade_date="2026-03-02",
    )
    assert result.equal
    assert result.left_sha256 == result.right_sha256


def test_canonical_parity_reports_first_value_mismatch() -> None:
    result = compare_source_frames(
        _frame(timestamps_as_text=False),
        _frame(timestamps_as_text=False, changed=True),
        symbol="0050",
        trade_date="2026-03-02",
    )
    assert not result.equal
    assert result.first_mismatch == {
        "column": "last_price",
        "row": 1,
        "left": 78.0,
        "right": 78.05,
    }
