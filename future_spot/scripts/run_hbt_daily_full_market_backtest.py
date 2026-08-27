from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
for path in (PROJECT_ROOT, WORKSPACE_ROOT):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from arbitrage.daily_pipeline import (  # noqa: E402,F401
    DEFAULT_FUTURES_PARQUET_TEMPLATE,
    DEFAULT_TPEX_DAILY_TEMPLATE,
    DEFAULT_TPEX_DAYTRADE_TEMPLATE,
    DEFAULT_TWSE_DAILY_TEMPLATE,
    DEFAULT_TWSE_DAYTRADE_TEMPLATE,
    DailyPairRecord,
    build_daily_pair_records,
    build_status_row,
    get_trade_dates,
    pair_name_filter,
    pair_run_key,
    pair_universe_frame,
    resolve_output_dir,
    resolve_project_path,
    select_trade_dates,
)
from arbitrage.event_data import (  # noqa: E402,F401
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
from arbitrage.full_market_runner import main, parse_args  # noqa: E402,F401
from arbitrage.hbt_pipeline import (  # noqa: E402,F401
    build_pair_hbt_config,
    frame_for_run_key,
    hbt_result_csv_paths,
    hbt_result_csvs_exist,
    hbt_settings_frame,
    load_hbt_result_csvs,
    pair_results_from_frames,
    pair_with_overrides,
    read_csv_if_exists,
    run_backtests,
    run_or_load_backtests,
    summarize_asset,
)
from arbitrage.reporting import *  # noqa: E402,F401,F403


if __name__ == "__main__":
    raise SystemExit(main())
