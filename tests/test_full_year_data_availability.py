from scripts.audit_full_year_data_availability import _ranges


def test_ranges_groups_adjacent_trading_dates() -> None:
    assert _ranges(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-12"]) == [
        {"start": "2026-01-02", "end": "2026-01-06", "dates": 3},
        {"start": "2026-01-12", "end": "2026-01-12", "dates": 1},
    ]
