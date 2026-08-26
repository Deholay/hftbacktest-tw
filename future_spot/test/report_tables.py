"""Build and persist notebook-equivalent analysis tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

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

TRADE_REPORT_COLUMNS = {
    "trade_date", "run_key", "pair_name", "spot_symbol", "future_symbol",
    "timestamp", "completion_timestamp", "step", "signal", "status", "failure_reason",
    "first_leg", "first_side", "first_exec_price", "first_exec_qty",
    "second_leg", "second_side", "second_exec_price", "second_exec_qty",
    "flatten_leg", "flatten_side", "flatten_exec_price", "flatten_exec_qty",
    "flatten_filled", "realized_pnl", "position_quantity",
}
MARKET_LATEST_COLUMNS = {
    "run_key", "timestamp", "spot_bid", "spot_ask", "future_bid", "future_ask",
}
LATENCY_REPORT_COLUMNS = {
    "pair_name", "event_type", "leg", "side", "spot_feed_latency_ns",
    "future_feed_latency_ns", "order_entry_latency_ns", "order_response_latency_ns",
    "local_ts", "spot_exch_ts", "future_exch_ts",
}


def build_report_tables(artifacts: BacktestArtifacts) -> ReportArtifacts:
    """Create profit, failure, cash/ROI, and latency report DataFrames."""
    summary = artifacts.frame("summary")
    pair_universe = artifacts.frame("pair_universe")

    symbol_profit = _symbol_profit(summary)
    pair_profit = _pair_profit(summary, pair_universe)
    selected_pair = None if pair_profit.empty else str(pair_profit.iloc[0]["pair_name"])
    report_mode = str(getattr(artifacts.args, "report_mode", "summary"))
    include_details = report_mode == "full" and not bool(getattr(artifacts.args, "skip_detailed_reports", False))
    chunk_rows = int(getattr(artifacts.args, "report_chunk_rows", 25_000))
    if chunk_rows <= 0:
        raise ValueError("report_chunk_rows must be > 0")

    low_memory = bool(getattr(artifacts.args, "low_memory_reports", True))
    source_paths = {
        "trades": artifacts.output_dir / "trades_all_daily_pairs.csv",
        "market": artifacts.output_dir / "market_all_daily_pairs.csv",
        "latency": artifacts.output_dir / "latency_all_daily_pairs.csv",
    }
    low_memory = low_memory and all(path.exists() for path in source_paths.values())
    if low_memory:
        artifacts.release_large_frames()
        trades, failure_trades = _stream_trade_inputs(
            source_paths["trades"],
            chunk_rows=chunk_rows,
            include_failure_windows=include_details,
        )
        failed_run_keys = set(failure_trades.get("run_key", pd.Series(dtype=str)).dropna().astype(str))
        market, failure_market = _stream_market_inputs(
            source_paths["market"],
            failed_run_keys=failed_run_keys,
            chunk_rows=chunk_rows,
            include_failure_windows=include_details,
        )
        latency_summary, latency_event_counts, latency_plot_sample = _stream_latency_tables(
            source_paths["latency"],
            selected_pair=selected_pair,
            chunk_rows=chunk_rows,
        )
    else:
        trades = artifacts.frame("trades")
        market = artifacts.frame("market")
        latency = artifacts.frame("latency")
        failure_trades = trades
        failure_market = market
        latency_summary, latency_event_counts = _latency_tables(latency)
        latency_plot_sample = (
            latency.loc[latency["pair_name"].eq(selected_pair)].head(80).copy()
            if selected_pair is not None and not latency.empty and "pair_name" in latency.columns
            else pd.DataFrame()
        )

    failure = reporting.build_second_leg_failure_outputs(
        summary,
        failure_trades,
        failure_market,
        window_rows=10,
        include_windows=include_details,
    )
    cash_roi = reporting.build_cash_roi_outputs(
        trades,
        market,
        artifacts.records,
        capital_allocation_config_from_args(artifacts.args),
        include_capital_details=include_details,
    )

    frames = {
        "symbol_profit": symbol_profit,
        "pair_profit": pair_profit,
        "latency_summary": latency_summary,
        "latency_event_counts": latency_event_counts,
        "latency_plot_sample": latency_plot_sample,
        "report_build_info": pd.DataFrame(
            [{
                "mode": report_mode,
                "low_memory": low_memory,
                "chunk_rows": chunk_rows,
                "details_included": include_details,
            }]
        ),
        **failure,
        **cash_roi,
    }
    if not include_details:
        for name in DETAILED_REPORTS:
            frames.pop(name, None)
    report_dir = artifacts.output_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    detailed_format = getattr(artifacts.args, "detailed_report_format", "parquet")
    for name, frame in frames.items():
        if name not in DETAILED_REPORTS:
            reporting.write_csv(frame, report_dir / f"{name}.csv")
            continue
        if detailed_format in {"parquet", "both"}:
            path = report_dir / f"{name}.parquet"
            reporting.write_parquet(frame, path)
        if detailed_format in {"csv", "both"}:
            reporting.write_csv(frame, report_dir / f"{name}.csv")
    return ReportArtifacts(frames=frames, output_dir=report_dir, selected_pair=selected_pair)


def _csv_columns(path: Path) -> list[str]:
    return pd.read_csv(path, nrows=0).columns.tolist()


def _concat(parts: Iterable[pd.DataFrame], columns: Iterable[str] = ()) -> pd.DataFrame:
    frames = list(parts)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(columns))


def _stream_trade_inputs(
    path: Path,
    *,
    chunk_rows: int,
    include_failure_windows: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return FILLED rows and the minimum trade pool needed for failure reports."""
    header = _csv_columns(path)
    usecols = None if include_failure_windows else [name for name in header if name in TRADE_REPORT_COLUMNS]
    filled_parts: list[pd.DataFrame] = []
    failed_parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunk_rows, low_memory=False):
        status = chunk.get("status", pd.Series(index=chunk.index, dtype=str)).astype(str)
        filled = chunk.loc[status.eq("FILLED")]
        failed = chunk.loc[status.isin(reporting.SECOND_LEG_FAILURE_STATUSES)]
        if not filled.empty:
            filled_parts.append(filled.copy())
        if not failed.empty:
            failed_parts.append(failed.copy())

    filled_trades = _concat(filled_parts, usecols or header)
    failed_trades = _concat(failed_parts, usecols or header)
    if not include_failure_windows or failed_trades.empty or "run_key" not in failed_trades.columns:
        return filled_trades, failed_trades

    failed_run_keys = set(failed_trades["run_key"].dropna().astype(str))
    window_parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, chunksize=chunk_rows, low_memory=False):
        selected = chunk.loc[chunk["run_key"].astype(str).isin(failed_run_keys)]
        if not selected.empty:
            window_parts.append(selected.copy())
    return filled_trades, _concat(window_parts, header)


def _latest_rows(previous: pd.DataFrame, chunk: pd.DataFrame) -> pd.DataFrame:
    if chunk.empty or "run_key" not in chunk.columns or "timestamp" not in chunk.columns:
        return previous
    candidates = chunk.loc[chunk["run_key"].notna()].copy()
    candidates["timestamp"] = pd.to_numeric(candidates["timestamp"], errors="coerce")
    candidates = candidates.loc[candidates["timestamp"].notna()]
    if candidates.empty:
        return previous
    candidates = (
        candidates.sort_values(["run_key", "timestamp"], kind="stable")
        .drop_duplicates("run_key", keep="last")
    )
    combined = pd.concat([previous, candidates], ignore_index=True) if not previous.empty else candidates
    return (
        combined.sort_values(["run_key", "timestamp"], kind="stable")
        .drop_duplicates("run_key", keep="last")
        .reset_index(drop=True)
    )


def _stream_market_inputs(
    path: Path,
    *,
    failed_run_keys: set[str],
    chunk_rows: int,
    include_failure_windows: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reduce the market file to one mark per run and optional failed-run windows."""
    header = _csv_columns(path)
    usecols = None if include_failure_windows else [name for name in header if name in MARKET_LATEST_COLUMNS]
    latest = pd.DataFrame(columns=usecols or header)
    failure_parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunk_rows, low_memory=False):
        latest = _latest_rows(latest, chunk)
        if include_failure_windows and failed_run_keys:
            selected = chunk.loc[chunk["run_key"].astype(str).isin(failed_run_keys)]
            if not selected.empty:
                failure_parts.append(selected.copy())
    return latest, _concat(failure_parts, usecols or header)


def _stream_latency_tables(
    path: Path,
    *,
    selected_pair: str | None,
    chunk_rows: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    header = _csv_columns(path)
    usecols = [name for name in header if name in LATENCY_REPORT_COLUMNS]
    metrics = (
        "spot_feed_latency_ns",
        "future_feed_latency_ns",
        "order_entry_latency_ns",
        "order_response_latency_ns",
    )
    summary_parts: list[pd.DataFrame] = []
    count_parts: list[pd.DataFrame] = []
    sample_parts: list[pd.DataFrame] = []
    sample_rows = 0

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunk_rows, low_memory=False):
        if "pair_name" not in chunk.columns or "event_type" not in chunk.columns:
            continue
        work = chunk[["pair_name", "event_type"]].copy()
        for name in metrics:
            work[name] = pd.to_numeric(chunk[name], errors="coerce") if name in chunk.columns else pd.NA
        grouped = work.groupby(["pair_name", "event_type"], dropna=False)
        partial = grouped.size().rename("rows").to_frame()
        for name in metrics:
            partial[f"{name}_sum"] = grouped[name].sum(min_count=1)
            partial[f"{name}_count"] = grouped[name].count()
        summary_parts.append(partial.reset_index())

        count_group = [name for name in ("event_type", "leg", "side") if name in chunk.columns]
        if count_group:
            count_parts.append(chunk.groupby(count_group, dropna=False).size().reset_index(name="rows"))

        if selected_pair is not None and sample_rows < 80:
            selected = chunk.loc[chunk["pair_name"].eq(selected_pair)].head(80 - sample_rows)
            if not selected.empty:
                sample_parts.append(selected.copy())
                sample_rows += len(selected)

    if summary_parts:
        partials = pd.concat(summary_parts, ignore_index=True)
        numeric_columns = [name for name in partials.columns if name not in {"pair_name", "event_type"}]
        aggregate = partials.groupby(["pair_name", "event_type"], dropna=False)[numeric_columns].sum().reset_index()
        latency_summary = aggregate[["pair_name", "event_type", "rows"]].copy()
        output_names = {
            "spot_feed_latency_ns": "spot_feed_latency_ms",
            "future_feed_latency_ns": "future_feed_latency_ms",
            "order_entry_latency_ns": "order_entry_latency_ms",
            "order_response_latency_ns": "order_response_latency_ms",
        }
        for source, target in output_names.items():
            latency_summary[target] = (
                aggregate[f"{source}_sum"] / aggregate[f"{source}_count"].replace(0, pd.NA) / 1_000_000
            )
    else:
        latency_summary, _ = _latency_tables(pd.DataFrame())

    if count_parts:
        counts = pd.concat(count_parts, ignore_index=True)
        count_group = [name for name in ("event_type", "leg", "side") if name in counts.columns]
        latency_counts = counts.groupby(count_group, dropna=False)["rows"].sum().reset_index()
    else:
        latency_counts = pd.DataFrame(columns=["event_type", "leg", "side", "rows"])
    return latency_summary, latency_counts, _concat(sample_parts, usecols)


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
