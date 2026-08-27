"""Save all futures/spot report plots as PNG files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Sequence

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
    excluded_dates: Sequence[str] = (),
) -> dict[str, Path]:
    """Build the notebook charts, optionally excluding complete trade dates."""
    figure_dir = artifacts.output_dir / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)
    logging.getLogger("matplotlib.category").setLevel(logging.WARNING)
    plt.style.use("seaborn-v0_8-whitegrid")

    effective_exclusions = tuple(excluded_dates or getattr(artifacts.args, "excluded_dates", ()))
    _write_exclusion_audit(artifacts.frame("summary"), effective_exclusions, figure_dir / "plot_exclusions.csv")
    summary = _exclude_trade_dates(artifacts.frame("summary"), effective_exclusions)
    pair_profit = _exclude_trade_dates(reports.frame("pair_profit"), effective_exclusions)
    daily_stuck_cash = _exclude_trade_dates(reports.frame("daily_stuck_cash"), effective_exclusions)
    top_stuck_cash_pairs = _exclude_trade_dates(reports.frame("top_stuck_cash_pairs"), effective_exclusions)
    daily_capital = _exclude_trade_dates(reports.frame("daily_capital_constraint"), effective_exclusions)
    capital_summary = reports.frame("capital_constraint_summary")

    builders = {
        "portfolio_overview": lambda: _portfolio_overview(
            daily_capital,
            capital_summary,
            excluded_dates=effective_exclusions,
        ),
        "performance_overview": lambda: _performance_overview(summary, _symbol_profit(summary)),
        "pair_profit_scatter": lambda: _pair_profit_scatter(pair_profit),
        "stuck_cash": lambda: _stuck_cash(daily_stuck_cash, top_stuck_cash_pairs),
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


def _portfolio_overview(
    daily: pd.DataFrame,
    capital_summary: pd.DataFrame,
    *,
    excluded_dates: Sequence[str] = (),
):
    """One-page realized portfolio view under the shared-capital replay."""
    if daily.empty or capital_summary.empty:
        return _empty_figure("Realized Portfolio Overview")

    plot = daily.copy()
    plot["trade_date"] = pd.to_datetime(plot["trade_date"], errors="raise")
    plot = plot.sort_values("trade_date").reset_index(drop=True)
    for column in (
        "realized_pnl",
        "ending_spot_capital",
        "ending_futures_capital",
        "ending_total_capital",
        "peak_capital_utilization",
        "rejected_entries",
    ):
        plot[column] = pd.to_numeric(plot[column], errors="coerce").fillna(0.0)

    total_capital = float(pd.to_numeric(capital_summary.iloc[0]["total_capital"], errors="raise"))
    final_pnl = float(plot["realized_pnl"].sum())
    final_roi = final_pnl / total_capital
    plot["cumulative_realized_pnl"] = plot["realized_pnl"].cumsum()
    constrained = plot["rejected_entries"].gt(0)
    constrained_days = int(constrained.sum())
    rejected_entries = int(plot["rejected_entries"].sum())
    peak_utilization = float(plot["peak_capital_utilization"].max())
    discarded_open_lots = int(
        pd.to_numeric(capital_summary.iloc[0].get("discarded_open_lots", 0), errors="coerce") or 0
    )

    blue = "#2563eb"
    blue_light = "#93c5fd"
    orange = "#f59e0b"
    ink = "#111827"
    muted = "#6b7280"
    grid = "#e5e7eb"

    figure = plt.figure(figsize=(18, 11), facecolor="white")
    layout = figure.add_gridspec(2, 2, top=0.82, hspace=0.33, wspace=0.17)
    daily_axis = figure.add_subplot(layout[0, 0])
    curve_axis = figure.add_subplot(layout[0, 1])
    capital_axis = figure.add_subplot(layout[1, :])

    dates = plot["trade_date"]
    daily_colors = [blue if value >= 0 else orange for value in plot["realized_pnl"]]
    daily_axis.bar(dates, plot["realized_pnl"], color=daily_colors, edgecolor=ink, linewidth=0.25)
    daily_axis.axhline(0, color=ink, linewidth=0.8)
    daily_axis.set(title="Daily realized profit", ylabel="TWD")

    curve_axis.plot(dates, plot["cumulative_realized_pnl"], color=blue, linewidth=2.2)
    curve_axis.axhline(0, color=ink, linewidth=0.8)
    curve_axis.set(title="Accumulated realized profit (equity curve)", ylabel="TWD")

    capital_axis.stackplot(
        dates,
        plot["ending_spot_capital"],
        plot["ending_futures_capital"],
        labels=("Spot capital stuck", "Futures margin stuck"),
        colors=(blue_light, "#fde68a"),
        alpha=0.85,
    )
    capital_axis.axhline(total_capital, color=ink, linestyle="--", linewidth=1.2, label="Capital limit")
    capital_axis.set(title="Stuck / occupied capital at day end", ylabel="TWD", xlabel="Trade date")
    capital_axis.legend(frameon=False, ncols=3, loc="upper left")

    constrained_dates = dates.loc[constrained]
    if not constrained_dates.empty:
        daily_axis.scatter(
            constrained_dates,
            plot.loc[constrained, "realized_pnl"],
            marker="x",
            color=ink,
            s=32,
            linewidths=1.1,
            label="Capital-constrained day",
            zorder=4,
        )
        curve_axis.scatter(
            constrained_dates,
            plot.loc[constrained, "cumulative_realized_pnl"],
            marker="x",
            color=ink,
            s=32,
            linewidths=1.1,
            zorder=4,
        )
        capital_axis.scatter(
            constrained_dates,
            plot.loc[constrained, "ending_total_capital"],
            marker="x",
            color=ink,
            s=32,
            linewidths=1.1,
            label="Entry rejected: capital/leg limit reached",
            zorder=4,
        )
        daily_axis.legend(frameon=False, loc="best")

    for axis in (daily_axis, curve_axis, capital_axis):
        axis.grid(axis="y", color=grid, linewidth=0.7)
        axis.grid(axis="x", visible=False)
        axis.tick_params(axis="x", rotation=35)
        axis.spines[["top", "right"]].set_visible(False)

    start = dates.min().strftime("%Y-%m-%d")
    end = dates.max().strftime("%Y-%m-%d")
    figure.suptitle("Futures / Spot Backtest — Realized Portfolio Overview", fontsize=20, color=ink, y=0.975)
    figure.text(
        0.5,
        0.94,
        (
            f"{start} to {end} | profit recognized only on matched EXIT dates | "
            f"{len(excluded_dates)} incomplete-tick dates excluded | "
            f"{discarded_open_lots} expiry residual lots removed"
        ),
        ha="center",
        color=muted,
        fontsize=11,
    )
    kpis = (
        f"REALIZED PnL\nNT${final_pnl:,.0f}",
        f"ROI ON NT${total_capital / 1_000_000:,.0f}M CAPITAL\n{final_roi:.2%}",
        f"PEAK CAPITAL USE\n{peak_utilization:.1%}",
        f"CAPITAL-CONSTRAINED\n{constrained_days} days / {rejected_entries:,} entries",
    )
    for x, value in zip((0.13, 0.38, 0.63, 0.86), kpis):
        figure.text(x, 0.875, value, ha="center", va="center", color=ink, fontsize=12, linespacing=1.45)
    figure.text(
        0.01,
        0.012,
        "ROI = cumulative capital-constrained realized PnL / starting own capital. Open-position marks are excluded.",
        color=muted,
        fontsize=9,
    )
    return figure


def _exclude_trade_dates(frame: pd.DataFrame, excluded_dates: Sequence[str]) -> pd.DataFrame:
    if frame.empty or not excluded_dates or "trade_date" not in frame.columns:
        return frame.copy()
    dates = pd.DatetimeIndex(pd.to_datetime(list(excluded_dates), errors="raise")).normalize()
    trade_dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.normalize()
    return frame.loc[~trade_dates.isin(dates)].copy()


def _write_exclusion_audit(summary: pd.DataFrame, excluded_dates: Sequence[str], path: Path) -> None:
    if not excluded_dates:
        return
    trade_dates = pd.to_datetime(summary["trade_date"], errors="raise").dt.normalize()
    rows = []
    for value in excluded_dates:
        date = pd.Timestamp(value).normalize()
        selected = summary.loc[trade_dates.eq(date)]
        rows.append({
            "trade_date": date.strftime("%Y-%m-%d"),
            "found_in_summary": not selected.empty,
            "summary_rows_removed": len(selected),
            "realized_pnl_removed": pd.to_numeric(selected.get("realized_pnl"), errors="coerce").sum(),
        })
    pd.DataFrame(rows).to_csv(path, index=False)


def _symbol_profit(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame(columns=["spot_symbol", "estimated_profit"])
    return (
        summary.groupby("spot_symbol", as_index=False)
        .agg(estimated_profit=("realized_pnl", "sum"))
        .sort_values("estimated_profit", ascending=False)
        .reset_index(drop=True)
    )


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
