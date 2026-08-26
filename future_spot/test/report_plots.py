"""Save all futures/spot report plots as PNG files."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

try:  # Package import (future_spot.test) and direct script/notebook import.
    from .backtest_pipeline import BacktestArtifacts
    from .report_tables import ReportArtifacts
except ImportError:  # pragma: no cover - exercised by direct script execution
    from backtest_pipeline import BacktestArtifacts
    from report_tables import ReportArtifacts


def save_report_plots(
    artifacts: BacktestArtifacts,
    reports: ReportArtifacts,
    *,
    dpi: int = 160,
) -> dict[str, Path]:
    """Build the notebook charts, save them, and return their absolute paths."""
    figure_dir = artifacts.output_dir / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)
    logging.getLogger("matplotlib.category").setLevel(logging.WARNING)
    plt.style.use("seaborn-v0_8-whitegrid")

    builders = {
        "performance_overview": lambda: _performance_overview(artifacts.frame("summary"), reports.frame("symbol_profit")),
        "pair_profit_scatter": lambda: _pair_profit_scatter(reports.frame("pair_profit")),
        "stuck_cash": lambda: _stuck_cash(reports.frame("daily_stuck_cash"), reports.frame("top_stuck_cash_pairs")),
        "roi_including_open": lambda: _roi(reports.frame("daily_roi_including_open"), reports.frame("pair_roi_including_open")),
        "latency_timelines": lambda: _latency(reports.frame("latency_plot_sample"), reports.selected_pair),
    }
    paths: dict[str, Path] = {}
    for name, build in builders.items():
        figure = build()
        path = (figure_dir / f"{name}.png").resolve()
        figure.savefig(path, dpi=dpi, bbox_inches="tight")
        plt.close(figure)
        paths[name] = path
    return paths


def _empty_figure(title: str, message: str = "No data"):
    figure, axis = plt.subplots(figsize=(10, 4))
    axis.set_title(title)
    axis.text(0.5, 0.5, message, ha="center", va="center", transform=axis.transAxes)
    axis.set_axis_off()
    return figure


def _performance_overview(summary: pd.DataFrame, symbol_profit: pd.DataFrame):
    if summary.empty:
        return _empty_figure("Performance Overview")
    plot = summary.copy()
    plot["trade_date"] = pd.to_datetime(plot["trade_date"])
    daily = plot.groupby("trade_date", as_index=False).agg(
        estimated_profit=("realized_pnl", "sum"),
        filled_pairs=("filled_pairs", "sum"),
        second_leg_failures=("second_leg_failures", "sum"),
    )
    figure, axes = plt.subplots(2, 2, figsize=(16, 10))
    labels = daily["trade_date"].dt.strftime("%m-%d")
    axes[0, 0].bar(labels, daily["estimated_profit"], color="#2563eb")
    axes[0, 0].axhline(0, color="#111827", linewidth=0.8)
    axes[0, 0].set_title("Daily Estimated Profit")
    top = symbol_profit.head(15).sort_values("estimated_profit")
    axes[0, 1].barh(top["spot_symbol"].astype(str), top["estimated_profit"], color="#059669")
    axes[0, 1].set_title("Top 15 Symbols by Estimated Profit")
    bottom = symbol_profit.tail(15).sort_values("estimated_profit")
    axes[1, 0].barh(bottom["spot_symbol"].astype(str), bottom["estimated_profit"], color="#dc2626")
    axes[1, 0].axvline(0, color="#111827", linewidth=0.8)
    axes[1, 0].set_title("Bottom 15 Symbols by Estimated Profit")
    axes[1, 1].plot(labels, daily["filled_pairs"], marker="o", label="filled")
    axes[1, 1].plot(labels, daily["second_leg_failures"], marker="o", label="second-leg failures")
    axes[1, 1].set_title("Daily Execution Count")
    axes[1, 1].legend()
    figure.tight_layout()
    return figure


def _pair_profit_scatter(pair_profit: pd.DataFrame):
    if pair_profit.empty:
        return _empty_figure("Pair Estimated Profit vs Filled Pairs")
    plot = pair_profit.copy()
    plot["date_label"] = pd.to_datetime(plot["trade_date"]).dt.strftime("%m-%d")
    figure, axis = plt.subplots(figsize=(16, 5))
    for date_label, part in plot.groupby("date_label"):
        axis.scatter(part["filled_pairs"], part["estimated_profit"], s=32, alpha=0.65, label=date_label)
    axis.axhline(0, color="#111827", linewidth=0.8)
    axis.set(title="Pair Estimated Profit vs Filled Pairs", xlabel="filled_pairs", ylabel="estimated_profit")
    axis.legend(title="trade date", ncols=4)
    return figure


def _stuck_cash(daily: pd.DataFrame, top_pairs: pd.DataFrame):
    if daily.empty:
        return _empty_figure("Stock Cash Stuck")
    figure, axes = plt.subplots(1, 2, figsize=(16, 5))
    labels = pd.to_datetime(daily["trade_date"]).dt.strftime("%m-%d")
    axes[0].bar(labels, daily["net_stock_cash_stuck"], color="#0f766e")
    axes[0].set(title="Daily Net Stock Cash Stuck", xlabel="trade date", ylabel="cash")
    top = top_pairs.head(15).sort_values("net_stock_cash_stuck")
    axes[1].barh(top["pair_name"], top["net_stock_cash_stuck"], color="#b45309")
    axes[1].set(title="Top 15 Pairs by Stuck Stock Cash", xlabel="cash")
    figure.tight_layout()
    return figure


def _roi(daily: pd.DataFrame, pair_roi: pd.DataFrame):
    if daily.empty:
        return _empty_figure("ROI Including Open Positions")
    figure, axes = plt.subplots(1, 2, figsize=(16, 5))
    labels = pd.to_datetime(daily["trade_date"]).dt.strftime("%m-%d")
    axes[0].bar(labels, daily["realized_pnl"], label="realized", color="#2563eb")
    axes[0].bar(labels, daily["open_locked_pnl"], bottom=daily["realized_pnl"], label="open locked", color="#ea580c")
    axes[0].axhline(0, color="#111827", linewidth=0.8)
    axes[0].set(title="Daily PnL Including Open Locked Spread", xlabel="trade date", ylabel="PnL")
    axes[0].legend()
    top = pair_roi.head(15).sort_values("total_roi_on_entry_cash_pct")
    axes[1].barh(top["pair_name"], top["total_roi_on_entry_cash_pct"] * 100, color="#0f766e")
    axes[1].axvline(0, color="#111827", linewidth=0.8)
    axes[1].set(title="Top 15 Pairs: Total ROI on Entry Cash", xlabel="ROI %")
    figure.tight_layout()
    return figure


def _latency(latency: pd.DataFrame, selected_pair: str | None):
    if latency.empty or selected_pair is None:
        return _empty_figure("Three Timelines")
    plot = latency.loc[latency["pair_name"].eq(selected_pair)].head(80).reset_index(drop=True)
    if plot.empty:
        return _empty_figure("Three Timelines", f"No latency rows for {selected_pair}")
    figure, axis = plt.subplots(figsize=(16, 5))
    x = plot.index
    for column, label in (("local_ts", "local"), ("spot_exch_ts", "spot_exch"), ("future_exch_ts", "future_exch")):
        if column in plot.columns:
            axis.plot(x, pd.to_numeric(plot[column], errors="coerce") / 1_000_000, marker="o", label=label)
    axis.set(title=f"Three Timelines: {selected_pair}", xlabel="latency event index", ylabel="timestamp ms")
    axis.legend()
    if "event_type" in plot.columns:
        top = axis.twiny()
        top.set_xlim(axis.get_xlim())
        top.set_xticks(x)
        top.set_xticklabels(plot["event_type"], rotation=60, ha="left", fontsize=8)
    figure.tight_layout()
    return figure
