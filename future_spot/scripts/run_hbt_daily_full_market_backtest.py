from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCRIPT_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_ROOT.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
for path in (SCRIPT_ROOT, WORKSPACE_ROOT, PROJECT_ROOT):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from scripts.tw_stock_data_to_npz import (  # noqa: E402
    convert_tw_stock_future_to_npz,
    convert_tw_stock_to_npz,
    default_output_path,
)
from scripts.tw_stock_hftbacktest import BacktestConfig, event_summary  # noqa: E402
from arbitrage.config import load_config  # noqa: E402
from arbitrage.hbt_backtest import (  # noqa: E402
    HbtAssetConfig,
    HbtPairBacktestConfig,
    HbtPairBacktester,
    infer_hbt_asset_tick_size,
)
from arbitrage.models import PairConfig, Signal  # noqa: E402
from build_arbitrage_config_from_date import (  # noqa: E402
    BuildArbitrageConfigResult,
    build_arbitrage_config_from_date,
    normalize_date,
)


DEFAULT_FUTURES_PARQUET_TEMPLATE = r"Z:\ticks_parquet_stock_future\{ldate}.parquet"
DEFAULT_TWSE_DAYTRADE_TEMPLATE = r"Z:\TWSE\每日個股狀況\{date_nodash}.csv"
DEFAULT_TPEX_DAYTRADE_TEMPLATE = r"Z:\TPEX\每日個股狀況\{date_nodash}.csv"
DEFAULT_TWSE_DAILY_TEMPLATE = r"Z:\TWSE\每日個股行情\{ldate_nodash}.ftr"
DEFAULT_TPEX_DAILY_TEMPLATE = r"Z:\TPEX\每日個股行情\{ldate_nodash}.ftr"


@dataclass(frozen=True)
class DailyPairRecord:
    trade_date: str
    run_key: str
    pair: PairConfig
    config_path: Path


@dataclass(frozen=True)
class EventDataResult:
    path: Path | None
    status: str
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build daily stock-future/spot pairs, convert HBT events, and run "
            "full-market paired HftBacktest."
        )
    )
    parser.add_argument("--start-date", default="2026-05-21", help="First trade date, YYYY-MM-DD.")
    parser.add_argument("--end-date", default="2026-05-26", help="Last trade date, YYYY-MM-DD.")
    parser.add_argument("--base-config", type=Path, default=Path("arbitrage_config_20260702.json"))
    parser.add_argument("--calendar", type=Path, default=Path("Calendar.csv"))
    parser.add_argument("--stockinfo", type=Path, default=Path("stockinfo.csv"))
    parser.add_argument("--output-dir", type=Path, default=None)

    parser.add_argument("--futures-parquet-template", default=DEFAULT_FUTURES_PARQUET_TEMPLATE)
    parser.add_argument("--twse-daytrade-template", default=DEFAULT_TWSE_DAYTRADE_TEMPLATE)
    parser.add_argument("--tpex-daytrade-template", default=DEFAULT_TPEX_DAYTRADE_TEMPLATE)
    parser.add_argument("--twse-daily-template", default=DEFAULT_TWSE_DAILY_TEMPLATE)
    parser.add_argument("--tpex-daily-template", default=DEFAULT_TPEX_DAILY_TEMPLATE)
    parser.add_argument("--build-session-start", default="08:45:00")
    parser.add_argument("--build-session-end", default="13:45:00")
    parser.add_argument("--min-future-volume", type=int, default=1000)
    parser.add_argument("--min-stock-volume", type=int, default=20_000_000)
    parser.add_argument("--required-unit", type=int, default=2000)
    parser.add_argument("--name-template", default="{spot_symbol}_{future_symbol}")
    parser.add_argument("--rebuild-daily-configs", action="store_true")

    parser.add_argument("--session-start", default="09:00:00")
    parser.add_argument("--session-end", default="13:30:00")
    parser.add_argument("--pair-name", action="append", default=[], help="Pair name filter. Can repeat or use commas.")
    parser.add_argument("--max-pairs", type=int, default=None, help="Global cap after date/pair filtering.")

    parser.add_argument("--no-convert-missing-event-data", action="store_true")
    parser.add_argument("--rebuild-event-data", action="store_true")
    parser.add_argument("--conversion-qa-sample-rows", type=int, default=1000)
    parser.add_argument(
        "--data-platform-base",
        default=r"\\DC_TW\taiwan_stock\數據平台",
        help="Base directory for stock top-5 data platform conversion.",
    )
    parser.add_argument(
        "--event-futures-parquet-dir",
        type=Path,
        default=None,
        help="Directory containing stock-future event conversion parquet files named YYYY-MM-DD.parquet.",
    )

    parser.add_argument("--first-leg", choices=("stock", "future"), default="future")
    parser.add_argument("--step-ms", type=float, default=1000.0)
    parser.add_argument("--order-latency-ms", type=float, default=0.0)
    parser.add_argument("--response-latency-ms", type=float, default=0.0)
    parser.add_argument("--feed-latency-offset-ms", type=float, default=0.0)
    parser.add_argument("--second-leg-delay-ms", type=float, default=0.0)
    parser.add_argument("--response-timeout-ms", type=float, default=50.0)
    parser.add_argument("--max-steps", type=int, default=None)
    parser.add_argument("--max-trades-per-pair", type=int, default=None)
    parser.add_argument("--record-market-every-steps", type=int, default=1, help="Use 0 to disable market rows.")
    parser.add_argument("--queue-model", default="risk_adverse")
    parser.add_argument("--entry-threshold-pct", type=float, default=None)
    parser.add_argument("--exit-threshold-pct", type=float, default=None)
    parser.add_argument("--min-effective-tick-multiple", type=float, default=None)
    parser.add_argument("--min-second-leg-adjusted-basis-pct", type=float, default=None)
    parser.add_argument("--no-second-leg-profit-check", action="store_true")
    parser.add_argument("--no-flatten", action="store_true")

    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    args.base_config = resolve_project_path(args.base_config)
    args.calendar = resolve_project_path(args.calendar)
    args.stockinfo = resolve_project_path(args.stockinfo)
    args.output_dir = resolve_output_dir(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    trade_dates = select_trade_dates(args.calendar, args.start_date, args.end_date)
    records, build_status = build_daily_pair_records(args, trade_dates)
    write_csv(build_status, args.output_dir / "daily_config_build_status.csv")
    pair_universe = pair_universe_frame(records)
    write_csv(pair_universe, args.output_dir / "daily_pair_universe.csv")

    event_paths, conversion_status = build_event_data(args, records)
    write_csv(conversion_status, args.output_dir / "conversion_status.csv")

    settings = hbt_settings_frame(args, records, event_paths)
    write_csv(settings, args.output_dir / "hbt_settings.csv")

    pair_results, summary, trades, market, run_errors = run_backtests(args, records, event_paths)
    write_csv(summary, args.output_dir / "summary_all_daily_pairs.csv")
    write_csv(trades, args.output_dir / "trades_all_daily_pairs.csv")
    write_csv(market, args.output_dir / "market_all_daily_pairs.csv")
    write_csv(run_errors, args.output_dir / "run_errors.csv")

    entry_exit_by_pair, entry_exit_all, entry_exit_index = build_entry_exit_outputs(pair_results, records)
    write_csv(entry_exit_all, args.output_dir / "entry_exit_all_daily_pairs.csv")
    write_csv(entry_exit_index, args.output_dir / "entry_exit_index.csv")
    write_entry_exit_by_pair(entry_exit_by_pair, args.output_dir / "entry_exit_by_pair")

    logging.info(
        "done dates=%s daily_pairs=%s ready_pairs=%s completed_pairs=%s errors=%s output=%s",
        len(trade_dates),
        len(records),
        len(event_paths),
        len(pair_results),
        len(run_errors),
        args.output_dir,
    )
    return 0


def resolve_project_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    candidate = PROJECT_ROOT / path
    if candidate.exists():
        return candidate.resolve()
    return (Path.cwd() / path).resolve()


def resolve_output_dir(args: argparse.Namespace) -> Path:
    if args.output_dir is not None:
        return args.output_dir if args.output_dir.is_absolute() else (PROJECT_ROOT / args.output_dir).resolve()
    start = normalize_date(args.start_date).replace("-", "")
    end = normalize_date(args.end_date).replace("-", "")
    return PROJECT_ROOT / "output" / f"hbt_daily_full_market_{start}_{end}"


def select_trade_dates(calendar_path: Path, start_date: str, end_date: str) -> list[str]:
    calendar = pd.read_csv(calendar_path, dtype=str)
    start = normalize_date(start_date)
    end = normalize_date(end_date)
    dates = [normalize_date(value) for value in calendar["trade_dates"].dropna().astype(str)]
    selected = [value for value in dates if start <= value <= end]
    if not selected:
        raise ValueError(f"No trade dates found between {start} and {end} in {calendar_path}")
    return selected


def pair_name_filter(values: list[str]) -> set[str]:
    names: set[str] = set()
    for value in values:
        names.update(item.strip() for item in value.split(",") if item.strip())
    return names


def build_daily_pair_records(
    args: argparse.Namespace,
    trade_dates: list[str],
) -> tuple[list[DailyPairRecord], pd.DataFrame]:
    config_dir = args.output_dir / "daily_configs"
    target_dir = args.output_dir / "daily_targets"
    config_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    allowed_names = pair_name_filter(args.pair_name)
    records: list[DailyPairRecord] = []
    status_rows: list[dict[str, Any]] = []
    for trade_date in trade_dates:
        date_nodash = trade_date.replace("-", "")
        config_path = config_dir / f"arbitrage_config_{date_nodash}.json"
        target_path = target_dir / f"target_futures_{date_nodash}.csv"
        status = "existing"
        result: BuildArbitrageConfigResult | None = None
        if args.rebuild_daily_configs or not config_path.exists() or not target_path.exists():
            try:
                result = build_arbitrage_config_from_date(
                    trade_date,
                    base_config=args.base_config,
                    calendar=args.calendar,
                    stockinfo=args.stockinfo,
                    futures_parquet_template=args.futures_parquet_template,
                    twse_daytrade_template=args.twse_daytrade_template,
                    tpex_daytrade_template=args.tpex_daytrade_template,
                    twse_daily_template=args.twse_daily_template,
                    tpex_daily_template=args.tpex_daily_template,
                    session_start=args.build_session_start,
                    session_end=args.build_session_end,
                    min_future_volume=args.min_future_volume,
                    min_stock_volume=args.min_stock_volume,
                    required_unit=args.required_unit,
                    output=config_path,
                    target_output=target_path,
                    name_template=args.name_template,
                )
                status = "generated"
            except SystemExit as exc:
                error = str(exc) or f"SystemExit({exc.code})"
                status_rows.append(build_status_row(trade_date, config_path, target_path, "error", error))
                if not args.continue_on_error:
                    raise RuntimeError(f"daily config build failed date={trade_date}: {error}") from exc
                continue
            except Exception as exc:
                status_rows.append(build_status_row(trade_date, config_path, target_path, "error", repr(exc)))
                if not args.continue_on_error:
                    raise
                continue

        config = load_config(config_path, replay_date_override=trade_date)
        pairs = list(config.pairs)
        if allowed_names:
            pairs = [pair for pair in pairs if pair.name in allowed_names]
        status_rows.append(
            build_status_row(
                trade_date,
                config_path,
                target_path,
                status,
                None,
                ldate=None if result is None else result.ldate,
                target_count=None if result is None else result.target_count,
                pair_count=len(pairs),
            )
        )
        for pair in pairs:
            records.append(DailyPairRecord(trade_date, pair_run_key(trade_date, pair.name), pair, config_path))

    if args.max_pairs is not None:
        records = records[: args.max_pairs]
    return records, pd.DataFrame(status_rows)


def build_status_row(
    trade_date: str,
    config_path: Path,
    target_path: Path,
    status: str,
    error: str | None,
    *,
    ldate: str | None = None,
    target_count: int | None = None,
    pair_count: int | None = None,
) -> dict[str, Any]:
    return {
        "trade_date": trade_date,
        "ldate": ldate,
        "status": status,
        "config_path": str(config_path),
        "target_path": str(target_path),
        "target_count": target_count,
        "pair_count": pair_count,
        "error": error,
    }


def pair_run_key(trade_date: str, pair_name: str) -> str:
    return f"{trade_date}::{pair_name}"


def pair_universe_frame(records: list[DailyPairRecord]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": record.trade_date,
                "run_key": record.run_key,
                "pair_name": record.pair.name,
                "spot_symbol": record.pair.spot_symbol,
                "future_symbol": record.pair.future_symbol,
                "entry_threshold_pct": record.pair.entry_threshold_pct,
                "min_effective_tick_multiple": record.pair.min_effective_tick_multiple,
                "spot_order_qty": record.pair.spot_order_qty,
                "future_order_qty": record.pair.future_order_qty,
                "future_pnl_multiplier": record.pair.future_pnl_multiplier,
                "daily_config_path": str(record.config_path),
            }
            for record in records
        ]
    )


def build_event_data(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
) -> tuple[dict[str, dict[str, Path]], pd.DataFrame]:
    cache: dict[tuple[str, str, str], EventDataResult] = {}
    paths_by_run_key: dict[str, dict[str, Path]] = {}
    rows: list[dict[str, Any]] = []
    for record in records:
        spot_key = (record.trade_date, "stock", record.pair.spot_symbol)
        future_key = (record.trade_date, "stock_future", record.pair.future_symbol)
        if spot_key not in cache:
            cache[spot_key] = ensure_spot_events(args, record.pair.spot_symbol, record.trade_date)
        if future_key not in cache:
            cache[future_key] = ensure_future_events(args, record.pair.future_symbol, record.trade_date)

        spot = cache[spot_key]
        future = cache[future_key]
        ok = spot.path is not None and future.path is not None
        if ok:
            paths_by_run_key[record.run_key] = {"spot": spot.path, "future": future.path}
        rows.append(
            {
                "trade_date": record.trade_date,
                "run_key": record.run_key,
                "pair_name": record.pair.name,
                "spot_symbol": record.pair.spot_symbol,
                "future_symbol": record.pair.future_symbol,
                "spot_status": spot.status,
                "future_status": future.status,
                "spot_path": None if spot.path is None else str(spot.path),
                "future_path": None if future.path is None else str(future.path),
                "ok": ok,
                "spot_error": spot.error,
                "future_error": future.error,
            }
        )
        if not ok and not args.continue_on_error:
            raise RuntimeError(f"event data missing for {record.run_key}: spot={spot.error} future={future.error}")
    return paths_by_run_key, pd.DataFrame(rows)


def expected_event_path(args: argparse.Namespace, symbol: str, source_kind: str, trade_date: str) -> Path:
    return default_output_path(
        WORKSPACE_ROOT,
        symbol,
        trade_date,
        trade_date,
        args.session_start,
        args.session_end,
        source_kind=source_kind,
    )


def ensure_spot_events(args: argparse.Namespace, symbol: str, trade_date: str) -> EventDataResult:
    output = expected_event_path(args, symbol, "stock", trade_date)
    if output.exists() and not args.rebuild_event_data:
        return EventDataResult(output, "existing")
    if args.no_convert_missing_event_data and not args.rebuild_event_data:
        return EventDataResult(None, "missing", f"missing spot npz: {output}")
    try:
        path, _ = convert_tw_stock_to_npz(
            symbol=symbol,
            start_date=trade_date,
            end_date=trade_date,
            start_time=args.session_start,
            end_time=args.session_end,
            output=output,
            workspace_root=WORKSPACE_ROOT,
            data_api=True,
            daily_parquet=False,
            data_platform_base=args.data_platform_base,
            levels=5,
            qa_sample_rows=args.conversion_qa_sample_rows,
        )
        return EventDataResult(path, "generated")
    except Exception as exc:
        return EventDataResult(None, "error", repr(exc))


def ensure_future_events(args: argparse.Namespace, symbol: str, trade_date: str) -> EventDataResult:
    output = expected_event_path(args, symbol, "stock_future", trade_date)
    if output.exists() and not args.rebuild_event_data:
        return EventDataResult(output, "existing")
    if args.no_convert_missing_event_data and not args.rebuild_event_data:
        return EventDataResult(None, "missing", f"missing future npz: {output}")
    try:
        path, _ = convert_tw_stock_future_to_npz(
            symbol=symbol,
            start_date=trade_date,
            end_date=trade_date,
            start_time=args.session_start,
            end_time=args.session_end,
            output=output,
            workspace_root=WORKSPACE_ROOT,
            path_config=WORKSPACE_ROOT / "path.toml",
            daily_parquet_dir=args.event_futures_parquet_dir,
            levels=5,
            qa_sample_rows=args.conversion_qa_sample_rows,
        )
        return EventDataResult(path, "generated")
    except Exception as exc:
        return EventDataResult(None, "error", repr(exc))


def hbt_settings_frame(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
    event_paths: dict[str, dict[str, Path]],
) -> pd.DataFrame:
    rows = []
    for record in records:
        paths = event_paths.get(record.run_key)
        if not paths:
            continue
        rows.append(summarize_asset(args, record, "spot", paths["spot"]))
        rows.append(summarize_asset(args, record, "future", paths["future"]))
    return pd.DataFrame(rows)


def summarize_asset(args: argparse.Namespace, record: DailyPairRecord, leg: str, data_path: Path) -> dict[str, Any]:
    instrument = "stock" if leg == "spot" else "future"
    contract_size = 1000.0 if leg == "spot" else float(record.pair.future_pnl_multiplier)
    tick_size = infer_hbt_asset_tick_size(data_path, instrument)
    hbt_config = BacktestConfig(
        data=data_path,
        contract_size=contract_size,
        tick_size=tick_size,
        lot_size=1.0,
        maker_fee=0.0,
        taker_fee=0.0,
        order_latency_ns=ms_to_ns(args.order_latency_ms),
        queue_model=args.queue_model,
    )
    data = np.load(data_path)["data"]
    summary = event_summary(data)
    return {
        "trade_date": record.trade_date,
        "run_key": record.run_key,
        "pair_name": record.pair.name,
        "leg": leg,
        "symbol": record.pair.spot_symbol if leg == "spot" else record.pair.future_symbol,
        "data": str(hbt_config.data),
        "contract_size": hbt_config.contract_size,
        "tick_size": hbt_config.tick_size,
        "lot_size": hbt_config.lot_size,
        "order_latency_ns": hbt_config.order_latency_ns,
        "queue_model": hbt_config.queue_model,
        "rows": summary["rows"],
        "first_exch_ts": summary["first_exch_ts"],
        "last_exch_ts": summary["last_exch_ts"],
        "min_feed_latency_ns": summary["min_latency_ns"],
        "max_feed_latency_ns": summary["max_latency_ns"],
        "depth_events": summary["depth_events"],
        "trade_events": summary["trade_events"],
    }


def run_backtests(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
    event_paths: dict[str, dict[str, Path]],
) -> tuple[dict[str, dict[str, pd.DataFrame]], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    results: dict[str, dict[str, pd.DataFrame]] = {}
    summary_frames = []
    trade_frames = []
    market_frames = []
    error_rows = []
    for record in records:
        paths = event_paths.get(record.run_key)
        if paths is None:
            error_rows.append(run_error_row(record, "missing converted event data"))
            continue
        try:
            config = build_pair_hbt_config(args, record.pair, paths)
            backtester = HbtPairBacktester(config)
            trades, summary = backtester.run()
            market = backtester.market_frame()
            trades = add_run_columns(add_execution_latency_columns(with_time_columns(trades)), record)
            market = add_run_columns(attach_entry_signals(with_time_columns(market), config.pair), record)
            summary = add_run_columns(summary, record)
            results[record.run_key] = {"trades": trades, "summary": summary, "market": market}
            summary_frames.append(summary)
            if not trades.empty:
                trade_frames.append(trades)
            if not market.empty:
                market_frames.append(market)
        except Exception as exc:
            error_rows.append(run_error_row(record, repr(exc)))
            if not args.continue_on_error:
                raise
    return (
        results,
        concat_frames(summary_frames),
        concat_frames(trade_frames),
        concat_frames(market_frames),
        pd.DataFrame(error_rows),
    )


def build_pair_hbt_config(args: argparse.Namespace, pair: PairConfig, paths: dict[str, Path]) -> HbtPairBacktestConfig:
    pair = pair_with_overrides(args, pair)
    return HbtPairBacktestConfig(
        pair=pair,
        spot=HbtAssetConfig(
            symbol=pair.spot_symbol,
            data=paths["spot"],
            instrument="stock",
            contract_size=1000.0,
            order_entry_latency_ns=ms_to_ns(args.order_latency_ms),
            order_response_latency_ns=ms_to_ns(args.response_latency_ms),
            feed_latency_offset_ns=ms_to_ns(args.feed_latency_offset_ms),
            queue_model=args.queue_model,
        ),
        future=HbtAssetConfig(
            symbol=pair.future_symbol,
            data=paths["future"],
            instrument="future",
            contract_size=float(pair.future_pnl_multiplier),
            order_entry_latency_ns=ms_to_ns(args.order_latency_ms),
            order_response_latency_ns=ms_to_ns(args.response_latency_ms),
            feed_latency_offset_ns=ms_to_ns(args.feed_latency_offset_ms),
            queue_model=args.queue_model,
        ),
        first_leg=args.first_leg,
        step_ns=ms_to_ns(args.step_ms),
        response_timeout_ns=ms_to_ns(args.response_timeout_ms),
        second_leg_delay_ns=ms_to_ns(args.second_leg_delay_ms),
        max_steps=args.max_steps,
        max_trades=args.max_trades_per_pair,
        flatten_on_second_leg_failure=not args.no_flatten,
        second_leg_profit_check=not args.no_second_leg_profit_check,
        record_market_every_steps=None if args.record_market_every_steps <= 0 else args.record_market_every_steps,
    )


def pair_with_overrides(args: argparse.Namespace, pair: PairConfig) -> PairConfig:
    updates = {}
    for arg_name, field_name in (
        ("entry_threshold_pct", "entry_threshold_pct"),
        ("exit_threshold_pct", "exit_threshold_pct"),
        ("min_effective_tick_multiple", "min_effective_tick_multiple"),
        ("min_second_leg_adjusted_basis_pct", "min_second_leg_adjusted_basis_pct"),
    ):
        value = getattr(args, arg_name)
        if value is not None:
            updates[field_name] = value
    return replace(pair, **updates) if updates else pair


def with_time_columns(df: pd.DataFrame, timestamp_col: str = "timestamp") -> pd.DataFrame:
    if df.empty or timestamp_col not in df.columns:
        return df
    result = df.copy()
    result["time"] = pd.to_datetime(result[timestamp_col], unit="ns", utc=True).dt.tz_convert("Asia/Taipei")
    return result


def add_execution_latency_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    if "signal_timestamp" in result.columns:
        signal_ts = pd.to_numeric(result["signal_timestamp"], errors="coerce")
        for col in (
            "first_exch_timestamp",
            "first_local_timestamp",
            "second_exch_timestamp",
            "second_local_timestamp",
            "completion_timestamp",
        ):
            if col in result.columns:
                result[f"{col}_from_signal_ms"] = (pd.to_numeric(result[col], errors="coerce") - signal_ts) / 1_000_000
    if {"first_exch_timestamp", "second_exch_timestamp"}.issubset(result.columns):
        result["first_to_second_exch_ms"] = (
            pd.to_numeric(result["second_exch_timestamp"], errors="coerce")
            - pd.to_numeric(result["first_exch_timestamp"], errors="coerce")
        ) / 1_000_000
    return result


def add_run_columns(df: pd.DataFrame, record: DailyPairRecord) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    result.insert(0, "run_key", record.run_key)
    result.insert(0, "trade_date", record.trade_date)
    return result


def attach_entry_signals(market: pd.DataFrame, pair: PairConfig) -> pd.DataFrame:
    if market.empty:
        return market
    result = market.copy()
    result["entry_signal"] = result.apply(lambda row: entry_signal(row, pair), axis=1)
    result["entry_signal_hit"] = result["entry_signal"].ne(Signal.HOLD.value)
    return result


def entry_signal(row: pd.Series, pair: PairConfig) -> str:
    long_ok = (
        row["long_spot_short_future_pct"] >= pair.entry_threshold_pct
        and row["long_spot_short_future_ticks"] > pair.min_effective_tick_multiple
        and row["spot_ask_size"] >= pair.stock_min_ask_size
        and row["future_bid_size"] >= pair.future_min_bid_size
    )
    if long_ok:
        return Signal.ENTER_LONG_SPOT_SHORT_FUTURE.value
    short_ok = (
        pair.allow_short_spot
        and row["short_spot_long_future_pct"] <= -pair.entry_threshold_pct
        and row["short_spot_long_future_ticks"] > pair.min_effective_tick_multiple
        and row["spot_bid_size"] >= pair.stock_min_bid_size
        and row["future_ask_size"] >= pair.future_min_ask_size
    )
    if short_ok:
        return Signal.ENTER_SHORT_SPOT_LONG_FUTURE.value
    return Signal.HOLD.value


def build_entry_exit_outputs(
    pair_results: dict[str, dict[str, pd.DataFrame]],
    records: list[DailyPairRecord],
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    record_by_key = {record.run_key: record for record in records}
    by_pair = {run_key: build_entry_exit_frame(run_key, result) for run_key, result in pair_results.items()}
    all_rows = concat_frames(list(by_pair.values()))
    index_rows = []
    for run_key, frame in by_pair.items():
        record = record_by_key[run_key]
        index_rows.append(
            {
                "trade_date": record.trade_date,
                "run_key": run_key,
                "pair_name": record.pair.name,
                "rows": len(frame),
                "entry_signal_rows": count_row_type(frame, "entry_signal"),
                "entry_execution_rows": count_row_type(frame, "entry_execution"),
                "exit_execution_rows": count_row_type(frame, "exit_execution"),
            }
        )
    return by_pair, all_rows, pd.DataFrame(index_rows)


def build_entry_exit_frame(run_key: str, result: dict[str, pd.DataFrame]) -> pd.DataFrame:
    market = result.get("market", pd.DataFrame())
    trades = result.get("trades", pd.DataFrame())
    signals = entry_signal_output(market)
    if not signals.empty:
        signals = signals.copy()
        signals["row_type"] = "entry_signal"
        signals["status"] = pd.NA
        signals["realized_pnl"] = pd.NA

    if not trades.empty:
        trade_cols = [
            "trade_date",
            "run_key",
            "time",
            "pair_name",
            "spot_symbol",
            "future_symbol",
            "signal",
            "status",
            "failure_reason",
            "spot_bid",
            "spot_ask",
            "future_bid",
            "future_ask",
            "long_spot_short_future_pct",
            "long_spot_short_future_ticks",
            "first_leg",
            "first_side",
            "first_requested_price",
            "first_exec_price",
            "first_exec_qty",
            "second_leg",
            "second_side",
            "second_requested_price",
            "second_exec_price",
            "second_exec_qty",
            "first_to_second_exch_ms",
            "realized_pnl",
        ]
        executions = trades[[col for col in trade_cols if col in trades.columns]].copy()
        executions["row_type"] = np.where(executions["signal"].eq(Signal.EXIT.value), "exit_execution", "entry_execution")
        executions = executions.rename(columns={"signal": "entry_signal"})
    else:
        executions = pd.DataFrame()

    combined = pd.concat([signals, executions], ignore_index=True, sort=False)
    if not combined.empty and "time" in combined.columns:
        combined = combined.sort_values("time").reset_index(drop=True)
    return combined


def entry_signal_output(market: pd.DataFrame) -> pd.DataFrame:
    if market.empty or "entry_signal_hit" not in market.columns:
        return pd.DataFrame()
    cols = [
        "trade_date",
        "run_key",
        "time",
        "pair_name",
        "spot_symbol",
        "future_symbol",
        "entry_signal",
        "spot_bid",
        "spot_ask",
        "spot_ask_size",
        "future_bid",
        "future_ask",
        "future_bid_size",
        "long_spot_short_future_pct",
        "long_spot_short_future_ticks",
        "short_spot_long_future_pct",
        "short_spot_long_future_ticks",
    ]
    return market.loc[market["entry_signal_hit"], [col for col in cols if col in market.columns]].reset_index(drop=True)


def count_row_type(frame: pd.DataFrame, value: str) -> int:
    if frame.empty or "row_type" not in frame.columns:
        return 0
    return int(frame["row_type"].eq(value).sum())


def run_error_row(record: DailyPairRecord, error: str) -> dict[str, Any]:
    return {
        "trade_date": record.trade_date,
        "run_key": record.run_key,
        "pair_name": record.pair.name,
        "spot_symbol": record.pair.spot_symbol,
        "future_symbol": record.pair.future_symbol,
        "error": error,
    }


def ms_to_ns(value: float) -> int:
    return int(round(value * 1_000_000))


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    return pd.concat(non_empty, ignore_index=True, sort=False) if non_empty else pd.DataFrame()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    logging.info("wrote %s rows to %s", len(frame), path)


def write_entry_exit_by_pair(frames: dict[str, pd.DataFrame], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for run_key, frame in frames.items():
        write_csv(frame, output_dir / f"{safe_filename(run_key)}.csv")


def safe_filename(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z._-]+", "_", value)


if __name__ == "__main__":
    raise SystemExit(main())
