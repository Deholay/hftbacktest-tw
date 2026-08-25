"""Replot already-computed futures/spot results with date filters.

This module deliberately reads persisted summary CSVs only.  It does not build
event data or invoke hftbacktest.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


SUMMARY_FILE = "summary_all_daily_pairs.csv"
REQUIRED_COLUMNS = {
    "trade_date",
    "run_key",
    "pair_name",
    "spot_symbol",
    "realized_pnl",
    "filled_pairs",
    "second_leg_failures",
}


@dataclass(frozen=True)
class PlotInterval:
    """One independently filtered chart interval."""

    name: str
    start_date: str
    end_date: str
    excluded_dates: tuple[str, ...] = ()


def load_precomputed_summary(output_dirs: Iterable[str | Path]) -> pd.DataFrame:
    """Load and combine persisted summaries without rerunning a backtest.

    Exact duplicate rows are removed.  Conflicting duplicate ``run_key`` rows
    are rejected because silently choosing one would make the chart ambiguous.
    """
    frames: list[pd.DataFrame] = []
    for raw_dir in output_dirs:
        output_dir = Path(raw_dir).expanduser().resolve()
        summary_path = output_dir / SUMMARY_FILE
        if not summary_path.exists():
            raise FileNotFoundError(f"Missing precomputed summary: {summary_path}")
        frame = pd.read_csv(summary_path)
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"{summary_path} is missing columns: {sorted(missing)}")
        frame = frame.copy()
        frame["source_output_dir"] = str(output_dir)
        frames.append(frame)

    if not frames:
        raise ValueError("At least one output directory is required")

    combined = pd.concat(frames, ignore_index=True)
    combined["trade_date"] = pd.to_datetime(combined["trade_date"], errors="raise").dt.normalize()
    combined["realized_pnl"] = pd.to_numeric(combined["realized_pnl"], errors="coerce").fillna(0.0)
    for column in ("filled_pairs", "second_leg_failures"):
        combined[column] = pd.to_numeric(combined[column], errors="coerce").fillna(0.0)

    value_columns = [column for column in combined.columns if column != "source_output_dir"]
    combined = combined.drop_duplicates(subset=value_columns).reset_index(drop=True)
    duplicate_keys = combined.duplicated("run_key", keep=False)
    if duplicate_keys.any():
        examples = combined.loc[duplicate_keys, ["trade_date", "run_key", "source_output_dir"]].head(10)
        raise ValueError(
            "Conflicting duplicate run_key rows were found across output directories:\n"
            + examples.to_string(index=False)
        )
    return combined.sort_values(["trade_date", "pair_name"]).reset_index(drop=True)


def daily_performance(summary: pd.DataFrame) -> pd.DataFrame:
    """Aggregate pair-level summary rows to one row per trading date."""
    if summary.empty:
        return pd.DataFrame({
            "trade_date": pd.Series(dtype="datetime64[ns]"),
            "realized_pnl": pd.Series(dtype="float64"),
            "filled_pairs": pd.Series(dtype="float64"),
            "second_leg_failures": pd.Series(dtype="float64"),
            "pair_count": pd.Series(dtype="int64"),
        })
    return (
        summary.groupby("trade_date", as_index=False)
        .agg(
            realized_pnl=("realized_pnl", "sum"),
            filled_pairs=("filled_pairs", "sum"),
            second_leg_failures=("second_leg_failures", "sum"),
            pair_count=("pair_name", "nunique"),
        )
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def filter_summary(
    summary: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    excluded_dates: Sequence[str] = (),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return included summary rows and a daily audit of excluded rows."""
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if start > end:
        raise ValueError(f"start_date must be <= end_date: {start_date} > {end_date}")
    excluded = pd.DatetimeIndex(pd.to_datetime(list(excluded_dates), errors="raise")).normalize()
    in_range = summary["trade_date"].between(start, end)
    excluded_mask = in_range & summary["trade_date"].isin(excluded)
    included = summary.loc[in_range & ~excluded_mask].copy()
    excluded_daily = daily_performance(summary.loc[excluded_mask].copy())
    return included, excluded_daily


def interval_diagnostics(summary: pd.DataFrame, interval: PlotInterval) -> pd.DataFrame:
    """Show every daily result in an interval, including exclusion status."""
    start = pd.Timestamp(interval.start_date)
    end = pd.Timestamp(interval.end_date)
    excluded = pd.DatetimeIndex(pd.to_datetime(list(interval.excluded_dates))).normalize()
    daily = daily_performance(summary.loc[summary["trade_date"].between(start, end)]).copy()
    daily["excluded"] = daily["trade_date"].isin(excluded)
    return daily.sort_values("realized_pnl").reset_index(drop=True)


def build_performance_figure(summary: pd.DataFrame, interval: PlotInterval):
    """Build a four-panel filtered performance figure for one interval."""
    filtered, excluded_daily = filter_summary(
        summary,
        start_date=interval.start_date,
        end_date=interval.end_date,
        excluded_dates=interval.excluded_dates,
    )
    if filtered.empty:
        raise ValueError(f"No included rows for interval {interval.name!r}")

    daily = daily_performance(filtered)
    daily["cumulative_pnl"] = daily["realized_pnl"].cumsum()
    symbol = (
        filtered.groupby("spot_symbol", as_index=False)
        .agg(realized_pnl=("realized_pnl", "sum"), filled_pairs=("filled_pairs", "sum"))
        .sort_values("realized_pnl")
    )
    extremes = pd.concat([symbol.head(8), symbol.tail(8)]).drop_duplicates("spot_symbol")

    plt.style.use("seaborn-v0_8-whitegrid")
    figure, axes = plt.subplots(2, 2, figsize=(16, 10))
    ink = "#1f2937"
    blue = "#2563eb"
    orange = "#ea580c"
    light_blue = "#93c5fd"
    date_labels = daily["trade_date"].dt.strftime("%m-%d")

    daily_colors = np.where(daily["realized_pnl"].ge(0), blue, orange)
    axes[0, 0].bar(date_labels, daily["realized_pnl"], color=daily_colors, edgecolor=ink, linewidth=0.35)
    axes[0, 0].axhline(0, color=ink, linewidth=0.9)
    axes[0, 0].set(title="Daily Realized PnL", xlabel="trade date", ylabel="PnL")

    axes[0, 1].plot(date_labels, daily["cumulative_pnl"], color=blue, marker="o", linewidth=2)
    axes[0, 1].axhline(0, color=ink, linewidth=0.9)
    axes[0, 1].set(title="Cumulative Realized PnL", xlabel="trade date", ylabel="PnL")

    axes[1, 0].plot(date_labels, daily["filled_pairs"], color=blue, marker="o", label="filled pairs")
    axes[1, 0].plot(
        date_labels,
        daily["second_leg_failures"],
        color=orange,
        marker="s",
        linestyle="--",
        label="second-leg failures",
    )
    axes[1, 0].set(title="Daily Execution Count", xlabel="trade date", ylabel="count")
    axes[1, 0].legend(frameon=False)

    extreme_colors = np.where(extremes["realized_pnl"].ge(0), blue, light_blue)
    axes[1, 1].barh(
        extremes["spot_symbol"].astype(str),
        extremes["realized_pnl"],
        color=extreme_colors,
        edgecolor=ink,
        linewidth=0.35,
    )
    axes[1, 1].axvline(0, color=ink, linewidth=0.9)
    axes[1, 1].set(title="Symbol PnL Extremes", xlabel="realized PnL", ylabel="spot symbol")

    for axis in axes.flat:
        axis.tick_params(axis="x", rotation=45)
        axis.grid(axis="x", visible=False)
    excluded_text = ", ".join(
        pd.to_datetime(excluded_daily["trade_date"], errors="raise").dt.strftime("%Y-%m-%d")
    ) or "none"
    total = daily["realized_pnl"].sum()
    figure.suptitle(f"{interval.name}: Filtered Futures/Spot Performance", fontsize=16, color=ink, y=0.99)
    figure.text(
        0.5,
        0.955,
        f"{interval.start_date} to {interval.end_date} | excluded: {excluded_text} | total PnL: {total:,.0f}",
        ha="center",
        color="#4b5563",
    )
    figure.tight_layout(rect=(0, 0, 1, 0.91))
    return figure


def save_interval_figures(
    summary: pd.DataFrame,
    intervals: Sequence[PlotInterval | Mapping[str, object]],
    output_dir: str | Path,
    *,
    dpi: int = 160,
) -> pd.DataFrame:
    """Save one PNG per interval and return a compact audit table."""
    destination = Path(output_dir).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)
    audit_rows: list[dict[str, object]] = []
    for raw_interval in intervals:
        interval = raw_interval if isinstance(raw_interval, PlotInterval) else PlotInterval(
            name=str(raw_interval["name"]),
            start_date=str(raw_interval["start_date"]),
            end_date=str(raw_interval["end_date"]),
            excluded_dates=tuple(str(value) for value in raw_interval.get("excluded_dates", ())),
        )
        filtered, excluded_daily = filter_summary(
            summary,
            start_date=interval.start_date,
            end_date=interval.end_date,
            excluded_dates=interval.excluded_dates,
        )
        figure = build_performance_figure(summary, interval)
        safe_name = "".join(character if character.isalnum() or character in "-_" else "_" for character in interval.name)
        path = destination / f"{safe_name}_filtered_performance.png"
        figure.savefig(path, dpi=dpi, bbox_inches="tight")
        plt.close(figure)
        daily = daily_performance(filtered)
        audit_rows.append(
            {
                "interval": interval.name,
                "start_date": interval.start_date,
                "end_date": interval.end_date,
                "included_dates": daily["trade_date"].nunique(),
                "excluded_dates_found": ", ".join(
                    pd.to_datetime(excluded_daily["trade_date"], errors="raise").dt.strftime("%Y-%m-%d")
                ),
                "realized_pnl": daily["realized_pnl"].sum(),
                "filled_pairs": daily["filled_pairs"].sum(),
                "figure_path": str(path),
            }
        )
    audit = pd.DataFrame(audit_rows)
    audit.to_csv(destination / "filtered_plot_audit.csv", index=False)
    return audit
