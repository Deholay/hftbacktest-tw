"""Event-data preparation for futures/spot HBT runs."""

from __future__ import annotations

from .full_market_runner import (
    DEFAULT_DATA_PLATFORM_BASE,
    DEFAULT_SPOT_INPUT_CSV_TEMPLATE,
    EventDataResult,
    build_event_data,
    ensure_future_events,
    ensure_spot_events,
    expected_event_path,
    first_csv_line,
    prepare_future_events,
    prepare_spot_input_csvs,
    split_daily_spot_csv,
    split_daily_spot_csv_stream,
    split_daily_spot_csv_with_rg,
    spot_input_csv_path,
)

__all__ = [
    "DEFAULT_DATA_PLATFORM_BASE",
    "DEFAULT_SPOT_INPUT_CSV_TEMPLATE",
    "EventDataResult",
    "build_event_data",
    "ensure_future_events",
    "ensure_spot_events",
    "expected_event_path",
    "first_csv_line",
    "prepare_future_events",
    "prepare_spot_input_csvs",
    "split_daily_spot_csv",
    "split_daily_spot_csv_stream",
    "split_daily_spot_csv_with_rg",
    "spot_input_csv_path",
]
