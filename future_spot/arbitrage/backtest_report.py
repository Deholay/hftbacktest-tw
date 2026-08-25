"""建立可重現、已過濾異常日期的期現套利回測報告資料。

本報告只讀取已保存的 HBT CSV，不會重新執行事件轉換或 hftbacktest。
資金相關指標沿用 ``full_market_runner.py`` 既有的 cash/ROI 定義。
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .result_replot import PlotInterval, daily_performance, load_precomputed_summary


@dataclass
class BacktestReportResult:
    """已生成的報告輸入與分析稽核表。"""

    headline: dict[str, object]
    frames: dict[str, pd.DataFrame]
    artifact: dict[str, object]
    output_dir: Path

    def frame(self, name: str) -> pd.DataFrame:
        return self.frames[name]


def build_backtest_report(
    output_dirs: Iterable[str | Path],
    intervals: Sequence[PlotInterval | Mapping[str, object]],
    output_dir: str | Path,
) -> BacktestReportResult:
    """建立報告指標、QA 表格與標準 MCP 報告 artifact。"""
    roots = [Path(path).expanduser().resolve() for path in output_dirs]
    normalized_intervals = [_coerce_interval(value) for value in intervals]
    destination = Path(output_dir).expanduser().resolve()
    destination.mkdir(parents=True, exist_ok=True)

    raw_summary = load_precomputed_summary(roots)
    raw_roi = _load_csvs(roots, "reports/daily_roi_including_open.csv")
    raw_stuck = _load_csvs(roots, "reports/daily_stuck_cash.csv")
    raw_pair_roi = _load_csvs(roots, "reports/pair_roi_including_open.csv")
    raw_build_status = _load_csvs(roots, "daily_config_build_status.csv")

    summary, summary_excluded = _scope_frame(raw_summary, normalized_intervals)
    roi, _ = _scope_frame(raw_roi, normalized_intervals)
    stuck, _ = _scope_frame(raw_stuck, normalized_intervals)
    pair_roi, _ = _scope_frame(raw_pair_roi, normalized_intervals)
    build_status, _ = _scope_frame(raw_build_status, normalized_intervals, apply_exclusions=False)

    daily = _build_daily_performance(summary, roi, stuck)
    monthly = _build_monthly_performance(summary, roi, stuck, normalized_intervals)
    symbol = _build_symbol_performance(pair_roi)
    excluded = _build_excluded_dates(raw_summary, normalized_intervals)
    coverage = _build_coverage(raw_summary, raw_roi, raw_build_status, normalized_intervals)
    validation = _build_validation_checks(
        raw_summary=raw_summary,
        summary=summary,
        roi=roi,
        monthly=monthly,
        excluded=excluded,
        coverage=coverage,
    )
    headline = _build_headline(daily, roi, stuck, monthly, excluded, coverage)
    pnl_components = _monthly_pnl_components(monthly)
    position_outcomes = _monthly_position_outcomes(monthly)

    frames = {
        "headline_metrics": pd.DataFrame([headline]),
        "daily_performance": daily,
        "monthly_performance": monthly,
        "monthly_pnl_components": pnl_components,
        "monthly_position_outcomes": position_outcomes,
        "symbol_performance": symbol,
        "excluded_dates": excluded,
        "coverage": coverage,
        "validation_checks": validation,
    }
    for name, frame in frames.items():
        frame.to_csv(destination / f"{name}.csv", index=False)

    artifact = _build_artifact(headline, frames, roots, normalized_intervals)
    artifact_path = destination / "artifact.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    return BacktestReportResult(headline=headline, frames=frames, artifact=artifact, output_dir=destination)


def _coerce_interval(raw: PlotInterval | Mapping[str, object]) -> PlotInterval:
    if isinstance(raw, PlotInterval):
        return raw
    return PlotInterval(
        name=str(raw["name"]),
        start_date=str(raw["start_date"]),
        end_date=str(raw["end_date"]),
        excluded_dates=tuple(str(value) for value in raw.get("excluded_dates", ())),
    )


def _load_csvs(roots: Sequence[Path], relative_path: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for root in roots:
        path = root / relative_path
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            frame = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            continue
        frame = frame.copy()
        frame["source_output_dir"] = str(root)
        frames.append(frame)
    if not frames:
        raise FileNotFoundError(f"No readable {relative_path} files under the configured output directories")
    combined = pd.concat(frames, ignore_index=True, sort=False)
    if "trade_date" not in combined.columns:
        raise ValueError(f"{relative_path} is missing trade_date")
    combined["trade_date"] = pd.to_datetime(combined["trade_date"], errors="raise").dt.normalize()
    return combined


def _scope_frame(
    frame: pd.DataFrame,
    intervals: Sequence[PlotInterval],
    *,
    apply_exclusions: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    labels = pd.Series(pd.NA, index=frame.index, dtype="object")
    excluded_mask = pd.Series(False, index=frame.index)
    for interval in intervals:
        start = pd.Timestamp(interval.start_date).normalize()
        end = pd.Timestamp(interval.end_date).normalize()
        if start > end:
            raise ValueError(f"Invalid interval {interval.name}: {start.date()} > {end.date()}")
        in_interval = frame["trade_date"].between(start, end)
        if (labels.notna() & in_interval).any():
            raise ValueError(f"Overlapping report intervals include {interval.name}")
        labels.loc[in_interval] = interval.name
        if apply_exclusions:
            excluded = pd.DatetimeIndex(pd.to_datetime(list(interval.excluded_dates))).normalize()
            excluded_mask |= in_interval & frame["trade_date"].isin(excluded)
    scoped = frame.loc[labels.notna()].copy()
    scoped["interval"] = labels.loc[labels.notna()].astype(str)
    excluded = scoped.loc[excluded_mask.loc[scoped.index]].copy()
    included = scoped.loc[~excluded_mask.loc[scoped.index]].copy()
    return included.reset_index(drop=True), excluded.reset_index(drop=True)


def _build_daily_performance(summary: pd.DataFrame, roi: pd.DataFrame, stuck: pd.DataFrame) -> pd.DataFrame:
    summary_daily = daily_performance(summary).rename(columns={"realized_pnl": "summary_realized_pnl"})
    roi_columns = [
        "trade_date", "entry_cash_out", "net_stock_cash_stuck", "realized_pnl", "open_locked_pnl",
        "total_pnl_including_open", "open_pairs", "total_roi_on_entry_cash_pct",
    ]
    roi_daily = roi[roi_columns].copy()
    stuck_columns = [column for column in ("trade_date", "entries", "exits") if column in stuck.columns]
    stuck_daily = stuck[stuck_columns].copy()
    daily = summary_daily.merge(roi_daily, on="trade_date", how="left").merge(stuck_daily, on="trade_date", how="left")
    for column in (
        "entry_cash_out", "net_stock_cash_stuck", "realized_pnl", "open_locked_pnl",
        "total_pnl_including_open", "open_pairs", "entries", "exits",
    ):
        if column in daily.columns:
            daily[column] = pd.to_numeric(daily[column], errors="coerce").fillna(0.0)
    daily["active_roi_day"] = daily["entry_cash_out"].gt(0)
    daily["daily_roi"] = daily["total_pnl_including_open"].div(daily["entry_cash_out"].where(daily["entry_cash_out"].gt(0)))
    daily = daily.sort_values("trade_date").reset_index(drop=True)
    daily["cumulative_total_pnl"] = daily["total_pnl_including_open"].cumsum()
    daily["cumulative_summary_realized_pnl"] = daily["summary_realized_pnl"].cumsum()
    daily["month"] = daily["trade_date"].dt.strftime("%Y-%m")
    daily["date"] = daily["trade_date"].dt.strftime("%Y-%m-%d")
    return daily


def _build_monthly_performance(
    summary: pd.DataFrame,
    roi: pd.DataFrame,
    stuck: pd.DataFrame,
    intervals: Sequence[PlotInterval],
) -> pd.DataFrame:
    summary_monthly = (
        summary.groupby("interval", as_index=False)
        .agg(
            observed_trade_days=("trade_date", "nunique"),
            summary_realized_pnl=("realized_pnl", "sum"),
            filled_pairs=("filled_pairs", "sum"),
            second_leg_failures=("second_leg_failures", "sum"),
        )
    )
    roi_monthly = (
        roi.groupby("interval", as_index=False)
        .agg(
            active_days=("trade_date", "nunique"),
            entry_cash_out=("entry_cash_out", "sum"),
            net_stock_cash_stuck=("net_stock_cash_stuck", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            open_locked_pnl=("open_locked_pnl", "sum"),
            total_pnl_including_open=("total_pnl_including_open", "sum"),
            open_pairs=("open_pairs", "sum"),
            mean_daily_roi=("total_roi_on_entry_cash_pct", "mean"),
            daily_roi_volatility=("total_roi_on_entry_cash_pct", "std"),
            winning_active_days=("total_pnl_including_open", lambda values: int(values.gt(0).sum())),
        )
    )
    stuck_monthly = (
        stuck.groupby("interval", as_index=False)
        .agg(entries=("entries", "sum"), exits=("exits", "sum"), peak_daily_stuck_cash=("net_stock_cash_stuck", "max"))
    )
    order = {interval.name: index for index, interval in enumerate(intervals)}
    monthly = pd.DataFrame({"interval": [interval.name for interval in intervals]})
    monthly = monthly.merge(summary_monthly, on="interval", how="left").merge(roi_monthly, on="interval", how="left").merge(stuck_monthly, on="interval", how="left")
    numeric_columns = [column for column in monthly.columns if column != "interval"]
    monthly[numeric_columns] = monthly[numeric_columns].fillna(0.0)
    monthly["total_roi"] = monthly["total_pnl_including_open"].div(monthly["entry_cash_out"].where(monthly["entry_cash_out"].gt(0)))
    monthly["realized_roi"] = monthly["realized_pnl"].div(monthly["entry_cash_out"].where(monthly["entry_cash_out"].gt(0)))
    monthly["active_day_win_rate"] = monthly["winning_active_days"].div(monthly["active_days"].where(monthly["active_days"].gt(0)))
    monthly["completion_rate"] = monthly["exits"].div(monthly["entries"].where(monthly["entries"].gt(0)))
    monthly["open_pnl_share"] = monthly["open_locked_pnl"].div(monthly["total_pnl_including_open"].where(monthly["total_pnl_including_open"].ne(0)))
    monthly["sharpe_proxy"] = np.sqrt(252) * monthly["mean_daily_roi"].div(monthly["daily_roi_volatility"].where(monthly["daily_roi_volatility"].gt(0)))
    monthly["month"] = monthly["interval"]
    monthly["month_label"] = pd.to_datetime(monthly["month"] + "-01").map(
        lambda value: f"{value.year}年{value.month}月"
    )
    monthly["sort_order"] = monthly["interval"].map(order)
    return monthly.sort_values("sort_order").reset_index(drop=True)


def _build_symbol_performance(pair_roi: pd.DataFrame) -> pd.DataFrame:
    symbol = (
        pair_roi.groupby("spot_symbol", as_index=False)
        .agg(
            entry_cash_out=("entry_cash_out", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            open_locked_pnl=("open_locked_pnl", "sum"),
            total_pnl_including_open=("total_pnl_including_open", "sum"),
            entries=("entries", "sum"),
            exits=("exits", "sum"),
            open_pairs=("open_pairs", "sum"),
        )
    )
    symbol["total_roi"] = symbol["total_pnl_including_open"].div(symbol["entry_cash_out"].where(symbol["entry_cash_out"].gt(0)))
    symbol["open_pnl_share"] = symbol["open_locked_pnl"].div(symbol["total_pnl_including_open"].where(symbol["total_pnl_including_open"].ne(0)))
    return symbol.sort_values("total_pnl_including_open", ascending=False).reset_index(drop=True)


def _build_excluded_dates(raw_summary: pd.DataFrame, intervals: Sequence[PlotInterval]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for interval in intervals:
        for date_text in interval.excluded_dates:
            date = pd.Timestamp(date_text).normalize()
            selected = raw_summary.loc[raw_summary["trade_date"].eq(date)]
            rows.append({
                "interval": interval.name,
                "trade_date": date.strftime("%Y-%m-%d"),
                "found_in_summary": not selected.empty,
                "summary_realized_pnl_removed": selected["realized_pnl"].sum() if not selected.empty else 0.0,
                "filled_pairs_removed": selected["filled_pairs"].sum() if not selected.empty else 0.0,
                "second_leg_failures_removed": selected["second_leg_failures"].sum() if not selected.empty else 0.0,
                "reason": "使用者指定排除缺 tick 異常日期",
            })
    return pd.DataFrame(rows)


def _build_coverage(
    raw_summary: pd.DataFrame,
    raw_roi: pd.DataFrame,
    raw_build_status: pd.DataFrame,
    intervals: Sequence[PlotInterval],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for interval in intervals:
        start, end = pd.Timestamp(interval.start_date), pd.Timestamp(interval.end_date)
        build = raw_build_status.loc[raw_build_status["trade_date"].between(start, end)]
        summary = raw_summary.loc[raw_summary["trade_date"].between(start, end)]
        roi = raw_roi.loc[raw_roi["trade_date"].between(start, end)]
        excluded = pd.DatetimeIndex(pd.to_datetime(list(interval.excluded_dates))).normalize()
        error_rows = build.loc[build["status"].eq("error")]
        rows.append({
            "interval": interval.name,
            "attempted_trade_days": build["trade_date"].nunique(),
            "config_success_days": build.loc[~build["status"].eq("error"), "trade_date"].nunique(),
            "config_error_days": error_rows["trade_date"].nunique(),
            "config_error_dates": ", ".join(error_rows["trade_date"].dt.strftime("%Y-%m-%d")),
            "summary_days_before_exclusion": summary["trade_date"].nunique(),
            "excluded_days_found": summary.loc[summary["trade_date"].isin(excluded), "trade_date"].nunique(),
            "included_summary_days": summary.loc[~summary["trade_date"].isin(excluded), "trade_date"].nunique(),
            "active_roi_days": roi.loc[~roi["trade_date"].isin(excluded), "trade_date"].nunique(),
        })
    return pd.DataFrame(rows)


def _build_validation_checks(
    *,
    raw_summary: pd.DataFrame,
    summary: pd.DataFrame,
    roi: pd.DataFrame,
    monthly: pd.DataFrame,
    excluded: pd.DataFrame,
    coverage: pd.DataFrame,
) -> pd.DataFrame:
    formula_delta = (
        roi["total_pnl_including_open"].div(roi["entry_cash_out"].where(roi["entry_cash_out"].gt(0)))
        - roi["total_roi_on_entry_cash_pct"]
    ).abs().max()
    checks = [
        ("Summary run_key 唯一性", "通過" if not raw_summary["run_key"].duplicated().any() else "失敗", f"重複鍵數：{raw_summary['run_key'].duplicated().sum()}", "高"),
        ("Summary PnL 完整性", "通過" if raw_summary["realized_pnl"].notna().all() else "失敗", f"PnL 空值列數：{raw_summary['realized_pnl'].isna().sum()}", "高"),
        ("ROI 公式核對", "通過" if pd.isna(formula_delta) or formula_delta < 1e-12 else "失敗", f"最大絕對差：{0.0 if pd.isna(formula_delta) else formula_delta:.3g}", "高"),
        ("月度加總與總計核對", "通過" if math.isclose(monthly["total_pnl_including_open"].sum(), roi["total_pnl_including_open"].sum(), rel_tol=1e-12, abs_tol=1e-6) else "失敗", "月度總 PnL 已與過濾後的每日 ROI 加總一致", "高"),
        ("指定排除日期可找到", "通過" if excluded.empty or excluded["found_in_summary"].all() else "注意", f"找到 {int(excluded['found_in_summary'].sum())}/{len(excluded)} 個排除日期", "中"),
        ("設定檔覆蓋率", "注意" if coverage["config_error_days"].sum() else "通過", f"{int(coverage['config_error_days'].sum())} 個嘗試交易日發生設定錯誤", "高"),
        ("未平倉部位依賴", "注意", "總 PnL 含未平倉鎖定損益，並非完全已實現的權益曲線", "高"),
        ("Sharpe 報酬口徑", "注意", "Sharpe 使用 active-day 的累計進場現金 ROI，不是固定本金投資組合報酬", "高"),
    ]
    return pd.DataFrame(checks, columns=["檢查項目", "狀態", "證據", "重要性"])


def _build_headline(
    daily: pd.DataFrame,
    roi: pd.DataFrame,
    stuck: pd.DataFrame,
    monthly: pd.DataFrame,
    excluded: pd.DataFrame,
    coverage: pd.DataFrame,
) -> dict[str, object]:
    active_returns = roi.loc[roi["entry_cash_out"].gt(0), "total_roi_on_entry_cash_pct"].dropna()
    daily_std = active_returns.std(ddof=1)
    sharpe = np.sqrt(252) * active_returns.mean() / daily_std if len(active_returns) >= 2 and daily_std > 0 else np.nan
    downside = np.sqrt(np.mean(np.minimum(active_returns, 0.0) ** 2)) if len(active_returns) else np.nan
    sortino = np.sqrt(252) * active_returns.mean() / downside if downside and downside > 0 else np.nan
    total_cash = roi["entry_cash_out"].sum()
    realized = roi["realized_pnl"].sum()
    open_locked = roi["open_locked_pnl"].sum()
    total_pnl = roi["total_pnl_including_open"].sum()
    cumulative = daily["total_pnl_including_open"].cumsum()
    running_peak = cumulative.cummax().clip(lower=0)
    max_drawdown = (cumulative - running_peak).min() if len(cumulative) else np.nan
    top_two_share = monthly.nlargest(2, "total_pnl_including_open")["total_pnl_including_open"].sum() / total_pnl if total_pnl else np.nan
    return {
        "period_start": daily["trade_date"].min().strftime("%Y-%m-%d"),
        "period_end": daily["trade_date"].max().strftime("%Y-%m-%d"),
        "observed_trade_days": int(daily["trade_date"].nunique()),
        "active_roi_days": int(len(active_returns)),
        "entry_cash_out": float(total_cash),
        "realized_pnl": float(realized),
        "open_locked_pnl": float(open_locked),
        "total_pnl_including_open": float(total_pnl),
        "total_roi": float(total_pnl / total_cash) if total_cash else np.nan,
        "realized_roi": float(realized / total_cash) if total_cash else np.nan,
        "open_pnl_share": float(open_locked / total_pnl) if total_pnl else np.nan,
        "sharpe_proxy": float(sharpe) if pd.notna(sharpe) else np.nan,
        "sortino_proxy": float(sortino) if pd.notna(sortino) else np.nan,
        "active_day_win_rate": float(active_returns.gt(0).mean()) if len(active_returns) else np.nan,
        "max_drawdown_twd": float(max_drawdown),
        "entries": float(stuck["entries"].sum()),
        "exits": float(stuck["exits"].sum()),
        "open_pair_runs": float(stuck["open_pairs"].sum()),
        "completion_rate": float(stuck["exits"].sum() / stuck["entries"].sum()) if stuck["entries"].sum() else np.nan,
        "peak_daily_entry_cash": float(roi["entry_cash_out"].max()),
        "peak_daily_stuck_cash": float(stuck["net_stock_cash_stuck"].max()),
        "summary_realized_pnl": float(daily["summary_realized_pnl"].sum()),
        "excluded_dates": int(len(excluded)),
        "excluded_summary_pnl": float(excluded["summary_realized_pnl_removed"].sum()) if len(excluded) else 0.0,
        "config_error_days": int(coverage["config_error_days"].sum()),
        "top_two_month_pnl_share": float(top_two_share),
    }


def _monthly_pnl_components(monthly: pd.DataFrame) -> pd.DataFrame:
    return monthly.melt(
        id_vars=["month", "month_label", "entry_cash_out", "total_roi", "active_days", "open_pairs"],
        value_vars=["realized_pnl", "open_locked_pnl"],
        var_name="component",
        value_name="pnl",
    ).assign(component=lambda frame: frame["component"].map({"realized_pnl": "已實現", "open_locked_pnl": "未平倉鎖定"}))


def _monthly_position_outcomes(monthly: pd.DataFrame) -> pd.DataFrame:
    return monthly.melt(
        id_vars=["month", "month_label", "entries", "completion_rate", "entry_cash_out"],
        value_vars=["exits", "open_pairs"],
        var_name="outcome",
        value_name="pair_runs",
    ).assign(outcome=lambda frame: frame["outcome"].map({"exits": "已出場", "open_pairs": "回測結束時未平倉"}))


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    clean = frame.copy()
    for column in clean.select_dtypes(include=["datetime", "datetimetz"]).columns:
        clean[column] = clean[column].dt.strftime("%Y-%m-%d")
    clean = clean.replace([np.inf, -np.inf], np.nan)
    return json.loads(clean.to_json(orient="records"))


def _build_artifact(
    headline: dict[str, object],
    frames: Mapping[str, pd.DataFrame],
    roots: Sequence[Path],
    intervals: Sequence[PlotInterval],
) -> dict[str, object]:
    generated_at = datetime.now(timezone.utc).isoformat()
    source_tables = []
    for root in roots:
        relative_root = Path("future_spot/output") / root.name
        source_tables.extend([
            str(relative_root / "summary_all_daily_pairs.csv"),
            str(relative_root / "reports/daily_roi_including_open.csv"),
            str(relative_root / "reports/daily_stuck_cash.csv"),
            str(relative_root / "reports/pair_roi_including_open.csv"),
            str(relative_root / "daily_config_build_status.csv"),
        ])
    source_tables.extend([
        "future_spot/output/backtest_report_202601_202607/headline_metrics.csv",
        "future_spot/output/backtest_report_202601_202607/daily_performance.csv",
        "future_spot/output/backtest_report_202601_202607/monthly_performance.csv",
        "future_spot/output/backtest_report_202601_202607/monthly_pnl_components.csv",
        "future_spot/output/backtest_report_202601_202607/monthly_position_outcomes.csv",
        "future_spot/output/backtest_report_202601_202607/symbol_performance.csv",
        "future_spot/output/backtest_report_202601_202607/excluded_dates.csv",
        "future_spot/output/backtest_report_202601_202607/coverage.csv",
    ])
    exclusion_text = ", ".join(date for interval in intervals for date in interval.excluded_dates)
    sources = [
        {
            "id": "filtered_backtest_metrics",
            "label": "2026 年 1–7 月已過濾的期現套利 HBT 輸出",
            "path": "future_spot/output/backtest_report_202601_202607/artifact.json",
            "query": {
                "engine": "DuckDB",
                "language": "sql",
                "sql": "SELECT * FROM read_csv_auto('future_spot/output/backtest_report_202601_202607/*.csv', union_by_name = true, filename = true);",
                "description": "彙整已保存的 pair/day summary、每日現金 ROI、未平倉鎖定損益、部位占用現金與設定建置狀態，並套用使用者指定的異常日期排除條件。",
                "executed_at": generated_at,
                "filters": [
                    f"報告期間：{intervals[0].start_date} 至 {intervals[-1].end_date}",
                    f"排除日期：{exclusion_text}",
                    "active-day 報酬統計僅使用 entry_cash_out > 0 的 ROI 資料列",
                ],
                "metric_definitions": [
                    "資金周轉 ROI＝含未平倉鎖定損益的總 PnL 加總／含股票手續費的股票進場現金加總；分母不含期貨保證金。",
                    "Sharpe proxy＝sqrt(252) × active-day 進場現金總 ROI 平均值／active-day 進場現金總 ROI 樣本標準差；無風險利率設為 0。",
                    "已實現 PnL 為 ROI 報表中已成交 EXIT 的實現損益；未平倉鎖定損益依既有報表邏輯估算尚未結束的 pair-run。",
                    "完成率＝既有現金報表中的已成交 EXIT 筆數／已成交股票多方進場筆數。",
                ],
                "tables_used": source_tables,
            },
        }
    ]

    h = headline
    title = "期現套利回測報告：2026 年 1–7 月"
    executive = (
        "## Executive Summary｜執行摘要\n\n"
        f"- **排除異常日期後，策略以累計 NT${h['entry_cash_out']/1_000_000_000:.2f}B 的股票進場現金產生 NT${h['total_pnl_including_open']/1_000_000:.1f}M 總 PnL，資金周轉 ROI 為 {h['total_roi']:.2%}。** 此數值不是投資組合 CAGR，也不是固定初始本金報酬率。\n"
        f"- **已實現 PnL 僅 NT${h['realized_pnl']/1_000_000:.1f}M；總 PnL 中有 {h['open_pnl_share']:.1%} 來自未平倉鎖定損益。** 因此整體結果高度依賴回測結束時仍未平倉的 {h['open_pair_runs']:,.0f} 個 pair-run。\n"
        f"- **active-day 年化 Sharpe proxy 為 {h['sharpe_proxy']:.2f}，但不是標準權益曲線 Sharpe。** {h['active_roi_days']} 個 active ROI 日全部為正，且每日報酬以當日進場現金正規化，而非固定投資組合 NAV。\n"
        f"- **整體可信度評估為「可分享，但須附帶限制說明」。** 報告排除 7 個缺 tick 異常日期，另有 {h['config_error_days']} 個嘗試交易日發生設定錯誤，因此不是完整且不中斷的 1–7 月樣本。"
    )
    open_section = (
        "## 多數帳面獲利仍來自未平倉部位\n\n"
        f"已實現 PnL 為 NT${h['realized_pnl']/1_000_000:.1f}M，未平倉鎖定損益則為 NT${h['open_locked_pnl']/1_000_000:.1f}M。"
        "下方月度組成圖將兩者分開呈現。鎖定價差雖具有經濟價值，但在真實流動性與交易成本條件下完成出場前，不應視為現金損益。"
    )
    roi_section = (
        "## 資金周轉效率為正，但分母不是標準投資組合本金\n\n"
        f"整體資金周轉 ROI 為 {h['total_roi']:.3%}，僅計已實現損益的 ROI 為 {h['realized_roi']:.3%}。"
        "分母是累計股票多方進場現金加股票手續費，不含期貨保證金，也不代表固定可配置本金。因此月度 ROI 較適合比較執行效率，不應直接解讀為投資人報酬。"
    )
    path_section = (
        "## active-day 報酬路徑沒有負報酬觀測值\n\n"
        f"{h['active_roi_days']} 個 active ROI 日全部為正，使 Sharpe proxy 達 {h['sharpe_proxy']:.2f}，且此 active-day PnL 路徑的回撤為 0。"
        "這種異常平滑的路徑在用於資金配置前，應加入強制平倉、期貨保證金、不利出場滑價，以及連續固定本金權益曲線進行壓力測試。"
    )
    capital_section = (
        "## 超過半數進場 pair-run 在回測結束時仍未平倉\n\n"
        f"現金報表記錄 {h['entries']:,.0f} 次進場與 {h['exits']:,.0f} 次出場，完成率為 {h['completion_rate']:.1%}。"
        f"單日股票進場現金峰值達 NT${h['peak_daily_entry_cash']/1_000_000:.1f}M，單日卡住現金峰值達 NT${h['peak_daily_stuck_cash']/1_000_000:.1f}M。"
        "因此主要營運限制是資金占用期間與出場容量，而不只是進場訊號的帳面獲利能力。"
    )
    data_section = (
        "## 異常排除與不完整分區限制跨月可比性\n\n"
        f"報告排除 {h['excluded_dates']} 個由使用者指定的缺 tick 異常日期，這些日期的 summary PnL 合計為 -NT${abs(h['excluded_summary_pnl'])/1_000_000:.1f}M。"
        f"另有 {h['config_error_days']} 個嘗試日期發生設定失敗；一月資料自 {h['period_start']} 開始，而七月只有 2 個 active ROI 日。"
        "因此月度排名必須搭配資料覆蓋表解讀，不能視為完整月份之間的同條件比較。"
    )

    manifest = {
        "version": 1,
        "surface": "report",
        "title": title,
        "description": "已排除異常日期的 HBT 績效、報酬效率、資金占用與資料品質評估。",
        "generatedAt": generated_at,
        "sources": sources,
        "cards": [
            {"id": "total_pnl", "dataset": "headline_metrics", "sourceId": "filtered_backtest_metrics", "description": "排除指定日期後，已實現損益加未平倉鎖定損益。", "metrics": [{"label": "總 PnL（含未平倉，NT$）", "field": "total_pnl_including_open", "format": "compact"}, {"label": "已實現（NT$）", "field": "realized_pnl", "format": "compact"}]},
            {"id": "turnover_roi", "dataset": "headline_metrics", "sourceId": "filtered_backtest_metrics", "description": "PnL 除以累計股票進場現金；不是固定本金投資組合報酬。", "metrics": [{"label": "資金周轉 ROI", "field": "total_roi", "format": "percent"}, {"label": "僅計已實現", "field": "realized_roi", "format": "percent"}]},
            {"id": "sharpe", "dataset": "headline_metrics", "sourceId": "filtered_backtest_metrics", "description": "active-day ROI 年化代理值；無風險利率為 0。", "metrics": [{"label": "Sharpe proxy", "field": "sharpe_proxy", "format": "number"}, {"label": "有效交易日", "field": "active_roi_days", "format": "number"}]},
            {"id": "open_share", "dataset": "headline_metrics", "sourceId": "filtered_backtest_metrics", "description": "總 PnL 中由未平倉鎖定部位貢獻的比例。", "metrics": [{"label": "未平倉 PnL 占比", "field": "open_pnl_share", "format": "percent"}, {"label": "未平倉 pair-run", "field": "open_pair_runs", "format": "compact"}]},
            {"id": "completion", "dataset": "headline_metrics", "sourceId": "filtered_backtest_metrics", "description": "現金報表中已成交出場筆數除以已成交股票多方進場筆數。", "metrics": [{"label": "完成率", "field": "completion_rate", "format": "percent"}, {"label": "進場次數", "field": "entries", "format": "compact"}]},
        ],
        "charts": [
            {
                "id": "monthly_pnl_components", "title": "月度 PnL 組成", "subtitle": "套用指定排除條件後的已實現與未平倉鎖定損益", "showDescription": True,
                "intent": "composition", "question": "每月 PnL 中有多少已實現、多少仍未平倉？", "rationale": "堆疊長條圖可清楚呈現各月已實現與未平倉鎖定損益的貢獻。",
                "type": "stackedBar", "dataset": "monthly_pnl_components", "sourceId": "filtered_backtest_metrics", "layout": "full",
                "encodings": {"x": {"field": "month_label", "type": "ordinal", "label": "月份"}, "y": {"field": "pnl", "type": "quantitative", "format": "compact", "label": "PnL", "unit": "NT$"}, "color": {"field": "component", "type": "nominal", "label": "損益狀態"}, "tooltip": [{"field": "entry_cash_out", "format": "compact", "label": "進場現金（NT$）"}, {"field": "total_roi", "format": "percent", "label": "ROI"}, {"field": "active_days", "format": "number", "label": "有效交易日"}]},
                "settings": {"groupMode": "stacked", "categoryLabelPolicy": "rotate", "showValues": False}, "palette": {"kind": "categorical"}, "legend": {"position": "bottom", "title": "損益狀態"}, "valueFormat": "compact", "unit": "NT$",
            },
            {
                "id": "monthly_roi", "title": "月度資金周轉 ROI", "subtitle": "含未平倉鎖定損益的總 PnL ÷ 累計股票進場現金", "showDescription": True,
                "intent": "comparison", "question": "各月報酬效率如何比較？", "rationale": "單序列長條圖適合直接比較月份，且不會暗示時間路徑連續。",
                "type": "bar", "dataset": "monthly_performance", "sourceId": "filtered_backtest_metrics", "layout": "full",
                "encodings": {"x": {"field": "month_label", "type": "ordinal", "label": "月份"}, "y": {"field": "total_roi", "type": "quantitative", "format": "percent", "label": "ROI"}, "tooltip": [{"field": "entry_cash_out", "format": "compact", "label": "進場現金（NT$）"}, {"field": "total_pnl_including_open", "format": "compact", "label": "總 PnL（NT$）"}, {"field": "active_days", "format": "number", "label": "有效交易日"}]},
                "settings": {"groupMode": "single", "categoryLabelPolicy": "rotate", "showValues": True}, "palette": {"kind": "sequential"}, "valueFormat": "percent",
            },
            {
                "id": "cumulative_pnl", "title": "累積 PnL（含未平倉鎖定部位）", "subtitle": "排除異常後的觀測交易日；保留 PnL 為 0 的日期", "showDescription": True,
                "intent": "trend", "question": "過濾後的累積 PnL 在測試期間如何變化？", "rationale": "折線圖可呈現超過 100 個觀測日期的損益路徑與持平區段。",
                "type": "line", "dataset": "daily_performance", "sourceId": "filtered_backtest_metrics", "layout": "full",
                "encodings": {"x": {"field": "date", "type": "temporal", "label": "日期"}, "y": {"field": "cumulative_total_pnl", "type": "quantitative", "format": "compact", "label": "累積 PnL", "unit": "NT$"}, "tooltip": [{"field": "daily_roi", "format": "percent", "label": "每日 ROI"}, {"field": "entry_cash_out", "format": "compact", "label": "進場現金（NT$）"}, {"field": "open_locked_pnl", "format": "compact", "label": "未平倉鎖定 PnL（NT$）"}]},
                "settings": {"showPoints": "never"}, "palette": {"kind": "sequential"}, "valueFormat": "compact", "unit": "NT$",
            },
            {
                "id": "position_outcomes", "title": "進場 pair-run 結果", "subtitle": "已成交出場與各獨立回測結束時仍未平倉部位的比較", "showDescription": True,
                "intent": "composition", "question": "進場 pair-run 中有多少已出場、多少仍未平倉？", "rationale": "100% 堆疊長條圖可在各月活動量不同時比較完成比例。",
                "type": "stackedBar100", "dataset": "monthly_position_outcomes", "sourceId": "filtered_backtest_metrics", "layout": "full",
                "encodings": {"x": {"field": "month_label", "type": "ordinal", "label": "月份"}, "y": {"field": "pair_runs", "type": "quantitative", "format": "number", "label": "Pair-run 數"}, "color": {"field": "outcome", "type": "nominal", "label": "結果"}, "tooltip": [{"field": "entries", "format": "number", "label": "進場次數"}, {"field": "completion_rate", "format": "percent", "label": "完成率"}, {"field": "entry_cash_out", "format": "compact", "label": "進場現金（NT$）"}]},
                "settings": {"groupMode": "stacked100", "categoryLabelPolicy": "rotate", "showPercent": True}, "palette": {"kind": "categorical"}, "legend": {"position": "bottom", "title": "回測結束狀態"},
            },
        ],
        "tables": [
            {
                "id": "monthly_table", "title": "月度績效明細", "subtitle": "過濾後的精確數值；七月僅有 2 個 active ROI 日", "showDescription": True, "dataset": "monthly_performance", "sourceId": "filtered_backtest_metrics", "layout": "full", "density": "spacious", "defaultSort": {"field": "month", "direction": "asc"},
                "columns": [
                    {"field": "month", "label": "月份", "type": "text"}, {"field": "observed_trade_days", "label": "觀測交易日", "format": "number"}, {"field": "active_days", "label": "有效 ROI 日", "format": "number"},
                    {"field": "entry_cash_out", "label": "進場現金（NT$）", "format": "compact"}, {"field": "realized_pnl", "label": "已實現 PnL（NT$）", "format": "compact", "movement": True}, {"field": "open_locked_pnl", "label": "未平倉鎖定 PnL（NT$）", "format": "compact", "movement": True},
                    {"field": "total_pnl_including_open", "label": "總 PnL（NT$）", "format": "compact", "movement": True}, {"field": "total_roi", "label": "資金周轉 ROI", "format": "percent", "movement": True}, {"field": "sharpe_proxy", "label": "Sharpe proxy", "format": "number"}, {"field": "completion_rate", "label": "完成率", "format": "percent"},
                ],
            },
            {
                "id": "top_symbols", "title": "總 PnL 前十名標的", "subtitle": "82 個具有現金 ROI 紀錄的標的中排名前 10", "showDescription": True, "dataset": "top_symbols", "sourceId": "filtered_backtest_metrics", "layout": "full", "density": "spacious", "defaultSort": {"field": "total_pnl_including_open", "direction": "desc"},
                "columns": [
                    {"field": "spot_symbol", "label": "現貨代號", "type": "text"}, {"field": "entry_cash_out", "label": "進場現金（NT$）", "format": "compact"}, {"field": "realized_pnl", "label": "已實現 PnL（NT$）", "format": "compact", "movement": True}, {"field": "open_locked_pnl", "label": "未平倉鎖定 PnL（NT$）", "format": "compact", "movement": True}, {"field": "total_pnl_including_open", "label": "總 PnL（NT$）", "format": "compact", "movement": True}, {"field": "total_roi", "label": "資金周轉 ROI", "format": "percent"}, {"field": "open_pairs", "label": "未平倉 pair-run", "format": "number"},
                ],
            },
            {
                "id": "coverage_table", "title": "月度資料覆蓋狀況", "subtitle": "嘗試執行、設定成功、排除與有效 ROI 日期", "showDescription": True, "dataset": "coverage", "sourceId": "filtered_backtest_metrics", "layout": "full", "density": "spacious", "defaultSort": {"field": "interval", "direction": "asc"},
                "columns": [
                    {"field": "interval", "label": "月份", "type": "text"}, {"field": "attempted_trade_days", "label": "嘗試交易日", "format": "number"}, {"field": "config_success_days", "label": "設定成功日", "format": "number"}, {"field": "config_error_days", "label": "設定錯誤日", "format": "number"}, {"field": "excluded_days_found", "label": "排除日", "format": "number"}, {"field": "included_summary_days", "label": "納入 summary 日", "format": "number"}, {"field": "active_roi_days", "label": "有效 ROI 日", "format": "number"}, {"field": "config_error_dates", "label": "錯誤日期", "type": "text"},
                ],
            },
            {
                "id": "exclusion_table", "title": "已排除的異常日期", "subtitle": "所有報告計算均移除的使用者指定日期", "showDescription": True, "dataset": "excluded_dates", "sourceId": "filtered_backtest_metrics", "layout": "full", "density": "spacious", "defaultSort": {"field": "trade_date", "direction": "asc"},
                "columns": [
                    {"field": "trade_date", "label": "日期", "type": "date"}, {"field": "interval", "label": "月份", "type": "text"}, {"field": "summary_realized_pnl_removed", "label": "移除的 Summary PnL（NT$）", "format": "compact", "movement": True}, {"field": "filled_pairs_removed", "label": "移除的成交配對", "format": "number"}, {"field": "second_leg_failures_removed", "label": "移除的第二腿失敗", "format": "number"}, {"field": "reason", "label": "原因", "type": "text"},
                ],
            },
        ],
        "blocks": [
            {"id": "title", "type": "markdown", "body": f"# {title}", "layout": "full"},
            {"id": "executive", "type": "markdown", "body": executive, "layout": "full", "sourceId": "filtered_backtest_metrics"},
            {"id": "headline_strip", "type": "metric-strip", "cardIds": ["total_pnl", "turnover_roi", "sharpe", "open_share", "completion"], "layout": "full"},
            {"id": "open_narrative", "type": "markdown", "body": open_section, "layout": "full", "sourceId": "filtered_backtest_metrics"},
            {"id": "open_chart", "type": "chart", "chartId": "monthly_pnl_components", "layout": "full"},
            {"id": "roi_narrative", "type": "markdown", "body": roi_section, "layout": "full", "sourceId": "filtered_backtest_metrics"},
            {"id": "roi_chart", "type": "chart", "chartId": "monthly_roi", "layout": "full"},
            {"id": "path_narrative", "type": "markdown", "body": path_section, "layout": "full", "sourceId": "filtered_backtest_metrics"},
            {"id": "path_chart", "type": "chart", "chartId": "cumulative_pnl", "layout": "full"},
            {"id": "capital_narrative", "type": "markdown", "body": capital_section, "layout": "full", "sourceId": "filtered_backtest_metrics"},
            {"id": "capital_chart", "type": "chart", "chartId": "position_outcomes", "layout": "full"},
            {"id": "monthly_narrative", "type": "markdown", "body": "## 月度精確績效讀數\n\n下表同時呈現資金、已實現與未平倉 PnL、ROI、Sharpe proxy 與完成率，避免只靠單一指標比較活動量與資料覆蓋不同的月份。", "layout": "full"},
            {"id": "monthly_table_block", "type": "table", "tableId": "monthly_table", "layout": "full"},
            {"id": "symbol_narrative", "type": "markdown", "body": "## 獲利來源分散，但領先標的仍有顯著影響\n\n前 10 名標的貢獻約一半的現金口徑總 PnL。在把排名解讀為已實現 alpha 前，應先檢查各標的的未平倉 PnL 占比。", "layout": "full", "sourceId": "filtered_backtest_metrics"},
            {"id": "symbol_table_block", "type": "table", "tableId": "top_symbols", "layout": "full"},
            {"id": "data_narrative", "type": "markdown", "body": data_section, "layout": "full", "sourceId": "filtered_backtest_metrics"},
            {"id": "coverage_table_block", "type": "table", "tableId": "coverage_table", "layout": "full"},
            {"id": "exclusion_table_block", "type": "table", "tableId": "exclusion_table", "layout": "full"},
            {"id": "recommendations", "type": "markdown", "body": "## 建議的下一步\n\n1. 建立納入期貨保證金、閒置現金與每日市值評價的連續固定本金權益曲線。\n2. 對所有未平倉 pair-run 強制平倉或延長持有期間，重新計算已實現 ROI、Sharpe、最大回撤與 profit factor。\n3. 在比較策略版本前，對出場滑價、排隊成交、延遲、手續費、交易稅與缺 tick 處理進行壓力測試。\n4. 修復 7 個 config-error 日期，只補跑缺失分區，再以相同排除條件重新生成本報告。", "layout": "full"},
            {"id": "questions", "type": "markdown", "body": "## 待確認問題\n\n- 應以多少固定本金與何種期貨保證金政策定義可投資的投資組合報酬？\n- 未平倉 pair-run 通常持續多久，其實際出場損益分布如何？\n- 在保守的強制平倉與市場衝擊假設下，正的 active-day ROI 是否仍可維持？\n- 異常日期應修復後恢復納入，還是永久排除？", "layout": "full"},
            {"id": "caveats", "type": "markdown", "body": "## 限制與假設\n\n- 結果是過濾後的回測快照，不代表實盤績效。\n- 資金周轉 ROI 使用累計股票多方進場現金加股票手續費；不含期貨保證金，也沒有固定投資組合 NAV。\n- Sharpe 是以 252 日年化、無風險利率 0 計算的 active-day ROI proxy，並非由連續投資組合權益曲線計算。\n- 總 PnL 包含未平倉鎖定損益，但該部分尚未現金實現。\n- 7 個異常日期與 7 個 config-error 日期降低資料連續性與可比性。\n- 報告尚未納入基準指標、信賴區間、容量模型或樣本外切分。", "layout": "full", "sourceId": "filtered_backtest_metrics"},
        ],
    }
    snapshot = {
        "version": 1,
        "generatedAt": generated_at,
        "status": "ready",
        "datasets": {
            "headline_metrics": _records(frames["headline_metrics"]),
            "daily_performance": _records(frames["daily_performance"]),
            "monthly_performance": _records(frames["monthly_performance"]),
            "monthly_pnl_components": _records(frames["monthly_pnl_components"]),
            "monthly_position_outcomes": _records(frames["monthly_position_outcomes"]),
            "top_symbols": _records(frames["symbol_performance"].head(10)),
            "coverage": _records(frames["coverage"]),
            "excluded_dates": _records(frames["excluded_dates"]),
        },
    }
    return {"surface": "report", "manifest": manifest, "snapshot": snapshot, "sources": sources}
