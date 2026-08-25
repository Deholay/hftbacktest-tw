"""HBT execution and cached-result helpers for futures/spot runs."""

from __future__ import annotations

from .full_market_runner import (
    build_pair_hbt_config,
    execute_hbt_runs,
    frame_for_run_key,
    hbt_result_csv_paths,
    hbt_result_csvs_exist,
    hbt_cache_is_valid,
    hbt_settings_frame,
    load_hbt_result_csvs,
    pair_results_from_frames,
    pair_with_overrides,
    raise_for_expiry_position_errors,
    read_csv_if_exists,
    run_backtests,
    run_or_load_backtests,
    write_hbt_manifest,
    summarize_asset,
)

__all__ = [
    "build_pair_hbt_config",
    "execute_hbt_runs",
    "frame_for_run_key",
    "hbt_result_csv_paths",
    "hbt_result_csvs_exist",
    "hbt_cache_is_valid",
    "hbt_settings_frame",
    "load_hbt_result_csvs",
    "pair_results_from_frames",
    "pair_with_overrides",
    "raise_for_expiry_position_errors",
    "read_csv_if_exists",
    "run_backtests",
    "run_or_load_backtests",
    "write_hbt_manifest",
    "summarize_asset",
]
