"""Reusable stages for the full-market futures/spot HBT workflow."""

from __future__ import annotations

import gc
import logging
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from arbitrage import daily_pipeline, hbt_pipeline, reporting
from arbitrage.daily_pipeline import DailyPairRecord

try:  # Package import (future_spot.test) and direct script/notebook import.
    from .backtest_config import prepare_args
except ImportError:  # pragma: no cover - exercised by direct script execution
    from backtest_config import prepare_args


@dataclass
class BacktestArtifacts:
    args: Namespace
    trade_dates: list[str]
    records: list[DailyPairRecord]
    event_paths: dict[str, Any]
    pair_results: dict[str, Any]
    frames: dict[str, pd.DataFrame]

    @property
    def output_dir(self) -> Path:
        return Path(self.args.output_dir)

    def frame(self, name: str) -> pd.DataFrame:
        return self.frames[name]

    def release_large_frames(self) -> None:
        """Drop report inputs that can be streamed back from the persisted CSVs."""
        for name in ("trades", "market", "latency", "entry_exit_all"):
            self.frames[name] = pd.DataFrame()
        self.pair_results.clear()
        gc.collect()


def run_backtest_pipeline(args: Namespace) -> BacktestArtifacts:
    """Run every data/config/HBT stage and persist the core CSV outputs."""
    args = prepare_args(args)
    trade_dates = daily_pipeline.select_trade_dates(
        args.calendar,
        args.start_date,
        args.end_date,
        excluded_dates=args.excluded_dates,
    )

    base_records, build_status = daily_pipeline.build_daily_pair_records(args, trade_dates)
    outputs = hbt_pipeline.execute_hbt_runs(args, base_records, trade_dates)
    records = outputs.records
    event_paths = outputs.event_paths
    pair_results = outputs.pair_results
    summary = outputs.summary
    trades = outputs.trades
    market = outputs.market
    latency = outputs.latency
    run_errors = outputs.run_errors
    conversion_status = outputs.conversion_status
    settings = outputs.settings
    pair_universe = daily_pipeline.pair_universe_frame(records)
    _write_frames(args.output_dir, {
        "daily_config_build_status": build_status,
        "daily_pair_universe": pair_universe,
        "conversion_status": conversion_status,
        "hbt_settings": settings,
        "position_carry_status": outputs.position_carry_status,
    })
    if args.post_first_feed_wait != "none" and not summary.empty and "post_first_feed_wait" not in summary.columns:
        raise RuntimeError(
            "Loaded incompatible cached results: summary is missing post_first_feed_wait. "
            "Rerun with --rebuild-hbt-results."
        )
    core_frames = {
        "summary_all_daily_pairs": summary,
        "trades_all_daily_pairs": trades,
        "market_all_daily_pairs": market,
        "latency_all_daily_pairs": latency,
        "run_errors": run_errors,
    }
    _write_frames(args.output_dir, core_frames)
    if not outputs.cache_hit:
        manifest = hbt_pipeline.write_hbt_manifest(args, records)
        logging.info("wrote backtest manifest to %s", manifest)

    entry_by_pair, entry_exit_all, entry_exit_index = reporting.build_entry_exit_outputs(pair_results, records)
    reporting.write_csv(entry_exit_all, args.output_dir / "entry_exit_all_daily_pairs.csv")
    reporting.write_csv(entry_exit_index, args.output_dir / "entry_exit_index.csv")
    if not getattr(args, "skip_entry_exit_by_pair", False):
        reporting.write_entry_exit_by_pair(entry_by_pair, args.output_dir / "entry_exit_by_pair")

    frames = {
        "build_status": build_status,
        "pair_universe": pair_universe,
        "conversion_status": conversion_status,
        "settings": settings,
        "summary": summary,
        "trades": trades,
        "market": market,
        "latency": latency,
        "run_errors": run_errors,
        "entry_exit_all": entry_exit_all,
        "entry_exit_index": entry_exit_index,
        "position_carry_status": outputs.position_carry_status,
    }
    logging.info("core backtest outputs saved to %s", args.output_dir)
    hbt_pipeline.raise_for_expiry_position_errors(args, outputs.position_carry_status)
    return BacktestArtifacts(args, trade_dates, records, event_paths, pair_results, frames)


def _write_frames(output_dir: Path, frames: dict[str, pd.DataFrame]) -> None:
    for stem, frame in frames.items():
        reporting.write_csv(frame, output_dir / f"{stem}.csv")
