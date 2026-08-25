"""Build and persist notebook-equivalent analysis tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from arbitrage import reporting
from arbitrage.capital import capital_allocation_config_from_args
try:  # Package import (future_spot.test) and direct script/notebook import.
    from .backtest_pipeline import BacktestArtifacts
except ImportError:  # pragma: no cover - exercised by direct script execution
    from backtest_pipeline import BacktestArtifacts


@dataclass
class ReportArtifacts:
    frames: dict[str, pd.DataFrame]
    output_dir: Path
    selected_pair: str | None

    def frame(self, name: str) -> pd.DataFrame:
        return self.frames[name]


DETAILED_REPORTS = {
    "failed_trades",
    "failure_trade_windows",
    "failure_market_tick_windows",
    "pair_cash_settings",
    "filled_trades",
    "stuck_cash_by_pair",
    "roi_by_pair",
    "open_lots",
    "capital_constraint_events",
    "capital_constraint_open_lots",
}


def build_report_tables(artifacts: BacktestArtifacts) -> ReportArtifacts:
    """Create profit, failure, cash/ROI, and latency report DataFrames."""
    summary = artifacts.frame("summary")
    trades = artifacts.frame("trades")
    market = artifacts.frame("market")
    latency = artifacts.frame("latency")
    pair_universe = artifacts.frame("pair_universe")

    symbol_profit = _symbol_profit(summary)
    pair_profit = _pair_profit(summary, pair_universe)
    failure = reporting.build_second_leg_failure_outputs(summary, trades, market, window_rows=10)
    cash_roi = reporting.build_cash_roi_outputs(
        trades,
        market,
        artifacts.records,
        capital_allocation_config_from_args(artifacts.args),
    )
    latency_summary, latency_event_counts = _latency_tables(latency)

    frames = {
        "symbol_profit": symbol_profit,
        "pair_profit": pair_profit,
        "latency_summary": latency_summary,
        "latency_event_counts": latency_event_counts,
        **failure,
        **cash_roi,
    }
    report_dir = artifacts.output_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    skip_detailed = bool(getattr(artifacts.args, "skip_detailed_reports", False))
    detailed_format = getattr(artifacts.args, "detailed_report_format", "parquet")
    for name, frame in frames.items():
        if name not in DETAILED_REPORTS:
            reporting.write_csv(frame, report_dir / f"{name}.csv")
            continue
        if skip_detailed:
            continue
        if detailed_format in {"parquet", "both"}:
            path = report_dir / f"{name}.parquet"
            reporting.write_parquet(frame, path)
        if detailed_format in {"csv", "both"}:
            reporting.write_csv(frame, report_dir / f"{name}.csv")
    selected_pair = None if pair_profit.empty else str(pair_profit.iloc[0]["pair_name"])
    return ReportArtifacts(frames=frames, output_dir=report_dir, selected_pair=selected_pair)


def _symbol_profit(summary: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "spot_symbol", "trade_days", "pair_names", "estimated_profit", "avg_daily_profit",
        "filled_pairs", "second_leg_failures", "flatten_count", "final_quantity_abs",
    ]
    if summary.empty:
        return pd.DataFrame(columns=columns)
    return (
        summary.groupby("spot_symbol", as_index=False)
        .agg(
            trade_days=("trade_date", "nunique"),
            pair_names=("pair_name", "nunique"),
            estimated_profit=("realized_pnl", "sum"),
            avg_daily_profit=("realized_pnl", "mean"),
            filled_pairs=("filled_pairs", "sum"),
            second_leg_failures=("second_leg_failures", "sum"),
            flatten_count=("flatten_count", "sum"),
            final_quantity_abs=("final_quantity", lambda values: values.abs().sum()),
        )
        .sort_values("estimated_profit", ascending=False)
        .reset_index(drop=True)
    )


def _pair_profit(summary: pd.DataFrame, pair_universe: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "trade_date", "pair_name", "spot_symbol", "future_symbol", "estimated_profit",
        "filled_pairs", "second_leg_failures", "flatten_count", "final_quantity",
        "entry_threshold_pct", "min_effective_tick_multiple", "spot_order_qty", "future_order_qty",
    ]
    if summary.empty:
        return pd.DataFrame(columns=columns)
    metadata = [
        name for name in (
            "entry_threshold_pct", "min_effective_tick_multiple", "spot_order_qty", "future_order_qty"
        ) if name in pair_universe.columns and name not in summary.columns
    ]
    merged = summary.merge(pair_universe[["run_key", *metadata]], on="run_key", how="left") if metadata else summary.copy()
    merged = merged.rename(columns={"realized_pnl": "estimated_profit"})
    for name in columns:
        if name not in merged.columns:
            merged[name] = pd.NA
    return merged[columns].sort_values("estimated_profit", ascending=False).reset_index(drop=True)


def _latency_tables(latency: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_columns = [
        "pair_name", "event_type", "rows", "spot_feed_latency_ms", "future_feed_latency_ms",
        "order_entry_latency_ms", "order_response_latency_ms",
    ]
    count_columns = ["event_type", "leg", "side", "rows"]
    if latency.empty:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame(columns=count_columns)

    def mean_ms(values: pd.Series) -> float:
        return pd.to_numeric(values, errors="coerce").mean() / 1_000_000

    latency_summary = (
        latency.groupby(["pair_name", "event_type"], dropna=False)
        .agg(
            rows=("event_type", "size"),
            spot_feed_latency_ms=("spot_feed_latency_ns", mean_ms),
            future_feed_latency_ms=("future_feed_latency_ns", mean_ms),
            order_entry_latency_ms=("order_entry_latency_ns", mean_ms),
            order_response_latency_ms=("order_response_latency_ns", mean_ms),
        )
        .reset_index()
    )
    count_group = [name for name in ("event_type", "leg", "side") if name in latency.columns]
    latency_counts = latency.value_counts(count_group, dropna=False).reset_index(name="rows")
    return latency_summary, latency_counts
