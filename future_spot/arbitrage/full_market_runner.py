from __future__ import annotations

import argparse
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ARBITRAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ARBITRAGE_ROOT.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
ROOT_SCRIPT_ROOT = WORKSPACE_ROOT / "scripts"
FUTURE_SPOT_SCRIPT_ROOT = PROJECT_ROOT / "scripts"
for path in (ROOT_SCRIPT_ROOT, FUTURE_SPOT_SCRIPT_ROOT, WORKSPACE_ROOT, PROJECT_ROOT):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from scripts.tw_stock_data_to_npz import (  # noqa: E402
    DEFAULT_DATA_PLATFORM_BASE,
    convert_tw_stock_future_batch_to_npz,
    convert_tw_stock_future_to_npz,
    convert_tw_stock_to_npz,
    default_output_path,
    parse_timestamp,
)
from scripts.compact_cache import (  # noqa: E402
    COMPACT_SCHEMA_VERSION,
    CompactBuildConfig,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
)
from scripts.compact_hbt_adapter import write_reference_npz_from_compact  # noqa: E402
from scripts.slim_engine import SLIM_ENGINE_VERSION  # noqa: E402
from scripts.tw_stock_hftbacktest import BacktestConfig  # noqa: E402
from scripts.io_utils import (  # noqa: E402
    concat_frames,
    ms_to_ns,
    read_csv_if_exists,
    safe_filename,
    write_csv,
    write_parquet,
)
from scripts.daily_result_store import (  # noqa: E402
    DAILY_RESULT_SCHEMA_VERSION,
    DailyResultStore,
    DailyResultStoreError,
    canonical_sha256,
)
from scripts.hbt_common import HBT_TIME_IN_FORCE_SEMANTICS  # noqa: E402
from arbitrage.config import load_config  # noqa: E402
from arbitrage.capital import (  # noqa: E402
    CapitalAllocationConfig,
    build_capital_constraint_outputs,
)
from arbitrage.hbt_backtest import (  # noqa: E402
    HbtAssetConfig,
    HbtPairBacktestConfig,
    HbtPairBacktester,
)
from arbitrage.hbt_helpers import hbt_asset_audit  # noqa: E402
from arbitrage.models import PairConfig, Signal  # noqa: E402
from arbitrage.position_carry import (  # noqa: E402
    PositionKey,
    PositionSnapshot,
    futures_contract_expiry_date,
    position_key,
    snapshot_from_summary_row,
)
from build_arbitrage_config_from_date import (  # noqa: E402
    BuildArbitrageConfigResult,
    build_arbitrage_config_from_date,
    format_template as format_daily_template,
    get_ldate as get_calendar_ldate,
    normalize_date,
)


DEFAULT_FUTURES_PARQUET_TEMPLATE = '/mnt/z/ticks_parquet_stock_future/{ldate}.parquet'
DEFAULT_STOCK_TICK_PARQUET_TEMPLATE = (
    '/mnt/z/數據平台/ticker_store/daily_parquet/twstock_{date_nodash}.parquet'
)
DEFAULT_SPOT_INPUT_CSV_TEMPLATE = ''
DEFAULT_TWSE_DAYTRADE_TEMPLATE = '/mnt/z/TWSE/每日個股狀況/{date_nodash}.csv'
DEFAULT_TPEX_DAYTRADE_TEMPLATE = '/mnt/z/TPEX/每日個股狀況/{date_nodash}.csv'
# The mounted Linux/WSL data layout uses 每日資料. Keep the CLI defaults
# aligned with the notebook and with build_arbitrage_config_from_date.py.
DEFAULT_TWSE_DAILY_TEMPLATE = '/mnt/z/TWSE/每日資料/{ldate_nodash}.ftr'
DEFAULT_TPEX_DAILY_TEMPLATE = '/mnt/z/TPEX/每日資料/{ldate_nodash}.ftr'

# Known incomplete-tick dates.  They are excluded from the HBT date sequence,
# so carried positions pass directly from the prior included day to the next.
KNOWN_BAD_TRADE_DATES = (
    "2026-04-23",
    "2026-05-04",
    "2026-05-19",
    "2026-06-24",
    "2026-07-01",
    "2026-07-22",
    "2026-07-27",
)

# Runs whose expiry-day residual positions are intentionally removed from the
# HBT universe and downstream performance/capital reports.  Unlike an audit
# waiver, these keys must not contribute trades, realized PnL, or stuck cash.
KNOWN_EXCLUDED_RUN_KEYS = (
    "2026-04-15::1802_KUFD6",
    "2026-05-20::6173_PKFE6",
    "2026-07-15::2340_FYFG6",
    "2026-07-15::2408_CYFG6",
    "2026-07-15::3714_QBFG6",
    "2026-07-15::4919_REFG6",
    "2026-07-15::5371_NMFG6",
    "2026-07-15::6257_MQFG6",
)

@dataclass(frozen=True)
class DailyPairRecord:
    trade_date: str
    run_key: str
    pair: PairConfig
    config_path: Path
    universe_source: str = "selected"
    carried_from_date: str | None = None


@dataclass(frozen=True)
class EventDataResult:
    path: Path | None
    status: str
    error: str | None = None


@dataclass
class HbtRunOutputs:
    records: list[DailyPairRecord]
    event_paths: dict[str, dict[str, Path]]
    pair_results: dict[str, dict[str, pd.DataFrame]]
    summary: pd.DataFrame
    trades: pd.DataFrame
    market: pd.DataFrame
    latency: pd.DataFrame
    run_errors: pd.DataFrame
    conversion_status: pd.DataFrame
    settings: pd.DataFrame
    position_carry_status: pd.DataFrame
    cache_hit: bool
    daily_partitions: bool = False
    daily_dates_reused: int = 0
    daily_dates_executed: int = 0
    stage_timings: pd.DataFrame = field(default_factory=pd.DataFrame)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build daily stock-future/spot pairs, convert HBT events, and run "
            "full-market paired HftBacktest."
        )
    )
    parser.add_argument("--start-date", default="2026-05-21", help="First trade date, YYYY-MM-DD.")
    parser.add_argument("--end-date", default="2026-05-26", help="Last trade date, YYYY-MM-DD.")
    parser.add_argument(
        "--exclude-date",
        dest="excluded_dates",
        action="append",
        default=list(KNOWN_BAD_TRADE_DATES),
        help="Trade date to omit from config building, event conversion, HBT, reports, and plots. Can repeat.",
    )
    parser.add_argument(
        "--exclude-run-key",
        dest="excluded_run_keys",
        action="append",
        default=list(KNOWN_EXCLUDED_RUN_KEYS),
        help="Exact YYYY-MM-DD::pair_name run to remove from HBT, trades, performance, and capital replay. Can repeat.",
    )
    parser.add_argument("--base-config", type=Path, default=Path("arbitrage_config_base.json"))
    parser.add_argument("--calendar", type=Path, default=Path("Calendar.csv"))
    parser.add_argument("--stockinfo", type=Path, default=Path("stockinfo.csv"))
    parser.add_argument("--output-dir", type=Path, default=None)

    parser.add_argument("--futures-parquet-template", default=DEFAULT_FUTURES_PARQUET_TEMPLATE)
    parser.add_argument("--twse-daytrade-template", default=DEFAULT_TWSE_DAYTRADE_TEMPLATE)
    parser.add_argument("--tpex-daytrade-template", default=DEFAULT_TPEX_DAYTRADE_TEMPLATE)
    parser.add_argument("--twse-daily-template", default=DEFAULT_TWSE_DAILY_TEMPLATE)
    parser.add_argument("--tpex-daily-template", default=DEFAULT_TPEX_DAILY_TEMPLATE)
    parser.add_argument("--build-session-start", default="08:45:00")
    parser.add_argument("--build-session-end", default="13:25:00")
    parser.add_argument("--min-future-volume", type=int, default=1000)
    parser.add_argument("--min-stock-volume", type=int, default=20_000_000)
    parser.add_argument("--required-unit", type=int, default=2000)
    parser.add_argument("--name-template", default="{spot_symbol}_{future_symbol}")
    parser.add_argument("--rebuild-daily-configs", action="store_true")

    parser.add_argument("--session-start", default="09:00:00")
    parser.add_argument("--session-end", default="13:25:00")
    parser.add_argument("--pair-name", action="append", default=[], help="Pair name filter. Can repeat or use commas.")
    parser.add_argument("--max-pairs", type=int, default=None, help="Global cap after date/pair filtering.")

    parser.add_argument("--no-convert-missing-event-data", action="store_true")
    parser.add_argument("--rebuild-event-data", action="store_true")
    parser.add_argument("--conversion-qa-sample-rows", type=int, default=1000)
    parser.add_argument(
        "--npz-compression",
        choices=("compressed", "uncompressed"),
        default="compressed",
        help="Use uncompressed for faster event-file writes and reloads at the cost of disk space.",
    )
    parser.add_argument(
        "--spot-input-csv-template",
        default=DEFAULT_SPOT_INPUT_CSV_TEMPLATE,
        help=(
            "Optional fallback spot tick CSV template. Supports {date}, "
            "{date_dash}, and {date_nodash}. Empty uses data_platform_client."
        ),
    )
    parser.add_argument(
        "--data-platform-base",
        default=DEFAULT_DATA_PLATFORM_BASE,
        help="data_platform_client parquet-store root used when --spot-input-csv-template is empty.",
    )
    parser.add_argument(
        "--event-futures-parquet-dir",
        type=Path,
        default=None,
        help="Directory containing stock-future event conversion parquet files named YYYY-MM-DD.parquet.",
    )
    parser.add_argument(
        "--engine",
        choices=("reference", "slim"),
        default="reference",
        help="Execution engine. Slim supports only immediate crossing FOK/IOC BBO mode.",
    )
    parser.add_argument(
        "--market-data-cache",
        choices=("event_npz", "compact"),
        default="event_npz",
        help="Reference may use legacy NPZ or compact-to-reference reconstruction; slim requires compact.",
    )
    parser.add_argument(
        "--compact-cache-root",
        type=Path,
        default=WORKSPACE_ROOT / "data" / "tw_compact_v1",
    )
    parser.add_argument(
        "--stock-tick-parquet-template",
        default=DEFAULT_STOCK_TICK_PARQUET_TEMPLATE,
    )
    parser.add_argument("--compact-cache-compression", choices=("none", "lz4", "zstd"), default="lz4")
    parser.add_argument("--compact-cache-profile", choices=("bbo",), default="bbo")
    parser.add_argument("--compact-cache-max-gb", type=float, default=200.0)
    parser.add_argument("--compact-cache-min-free-gb", type=float, default=200.0)
    parser.add_argument("--compact-cache-batch-rows", type=int, default=131_072)
    parser.add_argument("--rebuild-compact-cache", action="store_true")

    parser.add_argument("--first-leg", choices=("stock", "future"), default="future")
    parser.add_argument("--step-ms", type=float, default=1000.0)
    parser.add_argument(
        "--strategy-engine",
        choices=("numba", "python"),
        default="numba",
        help="Numba scans HOLD-only spans in compiled code; Python is the reference/fallback engine.",
    )
    parser.add_argument(
        "--order-latency-ms",
        type=float,
        default=0.0,
        help="Fallback order-entry latency for both legs when a leg-specific value is omitted.",
    )
    parser.add_argument(
        "--response-latency-ms",
        type=float,
        default=0.0,
        help="Fallback order-response latency for both legs when a leg-specific value is omitted.",
    )
    parser.add_argument(
        "--feed-latency-offset-ms",
        type=float,
        default=0.0,
        help="Fallback market-feed offset for both legs when a leg-specific value is omitted.",
    )
    parser.add_argument("--spot-order-latency-ms", type=float, default=None)
    parser.add_argument("--spot-response-latency-ms", type=float, default=None)
    parser.add_argument("--spot-feed-latency-offset-ms", type=float, default=None)
    parser.add_argument("--future-order-latency-ms", type=float, default=None)
    parser.add_argument("--future-response-latency-ms", type=float, default=None)
    parser.add_argument("--future-feed-latency-offset-ms", type=float, default=None)
    parser.add_argument("--second-leg-delay-ms", type=float, default=0.0)
    parser.add_argument("--post-first-feed-wait", choices=("none", "spot", "future", "any", "both"), default="none")
    parser.add_argument("--post-first-feed-timeout-ms", type=float, default=0.0)
    parser.add_argument("--post-first-feed-poll-ms", type=float, default=10.0)
    parser.add_argument("--response-timeout-ms", type=float, default=50.0)
    parser.add_argument("--max-steps", type=int, default=None)
    parser.add_argument("--max-trades-per-pair", type=int, default=None)
    parser.add_argument(
        "--record-market-every-steps",
        type=int,
        default=60,
        help="Periodic market sampling interval in strategy steps. Signal rows are always retained; use 0 for signals only.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(6, os.cpu_count() or 1),
        help="Pair backtest worker processes. Use 1 for serial debugging.",
    )
    parser.add_argument("--rebuild-hbt-results", action="store_true", help="Rerun HBT even when result CSVs already exist.")
    parser.add_argument("--queue-model", default="risk_adverse")
    parser.add_argument("--entry-threshold-pct", type=float, default=None)
    parser.add_argument("--exit-threshold-pct", type=float, default=None)
    parser.add_argument("--min-effective-tick-multiple", type=float, default=None)
    parser.add_argument("--min-second-leg-adjusted-basis-pct", type=float, default=None)
    parser.add_argument("--no-second-leg-profit-check", action="store_true")
    parser.add_argument("--no-flatten", action="store_true")
    parser.add_argument(
        "--carry-positions",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Carry final pair positions into the next trade day and keep held contracts in the daily universe. "
            "Use --no-carry-positions for independent pair/day runs."
        ),
    )

    parser.add_argument(
        "--total-capital",
        type=float,
        default=50_000_000.0,
        help="Shared futures/spot own-capital budget used by the saved-fill capital replay.",
    )
    parser.add_argument(
        "--futures-margin-rate",
        type=float,
        default=0.20,
        help="Futures initial-margin assumption used by the capital replay.",
    )
    parser.add_argument(
        "--spot-equity-rate",
        type=float,
        default=0.40,
        help="Spot own-funds ratio after margin financing used by the capital replay.",
    )
    parser.add_argument(
        "--leverage",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Enable the configured futures margin and spot financing ratios. "
            "Use --no-leverage to charge 100%% capital to both legs."
        ),
    )

    parser.add_argument("--skip-entry-exit-by-pair", action="store_true")
    parser.add_argument("--skip-detailed-reports", action="store_true")
    parser.add_argument(
        "--low-memory-reports",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Release large in-memory result frames and build reports from CSV chunks.",
    )
    parser.add_argument(
        "--report-mode",
        choices=("summary", "full"),
        default="summary",
        help="Summary omits large diagnostic detail tables; full builds them from bounded chunks.",
    )
    parser.add_argument(
        "--full-report-max-rows",
        type=int,
        default=None,
        help="Required hard cap on retained detail rows when --report-mode full is selected.",
    )
    parser.add_argument(
        "--report-chunk-rows",
        type=int,
        default=25_000,
        help="Rows per CSV batch while building low-memory reports.",
    )
    parser.add_argument("--no-plots", action="store_true")
    parser.add_argument(
        "--detailed-report-format",
        choices=("parquet", "csv", "both"),
        default="parquet",
        help="Storage format for large detailed report tables.",
    )

    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args(argv)
    if args.engine == "slim":
        args.market_data_cache = "compact"
    if args.report_mode == "full" and (
        args.full_report_max_rows is None or args.full_report_max_rows <= 0
    ):
        parser.error("--report-mode full requires a positive --full-report-max-rows budget")
    return args


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    args.base_config = resolve_project_path(args.base_config)
    args.calendar = resolve_project_path(args.calendar)
    args.stockinfo = resolve_project_path(args.stockinfo)
    args.output_dir = resolve_output_dir(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    trade_dates = select_trade_dates(
        args.calendar,
        args.start_date,
        args.end_date,
        excluded_dates=args.excluded_dates,
    )
    base_records, build_status = build_daily_pair_records(args, trade_dates)
    write_csv(build_status, args.output_dir / "daily_config_build_status.csv")
    outputs = execute_hbt_runs(args, base_records, trade_dates)
    records = outputs.records
    pair_universe = pair_universe_frame(records)
    write_csv(pair_universe, args.output_dir / "daily_pair_universe.csv")
    write_csv(outputs.conversion_status, args.output_dir / "conversion_status.csv")
    write_csv(outputs.settings, args.output_dir / "hbt_settings.csv")
    write_csv(outputs.position_carry_status, args.output_dir / "position_carry_status.csv")
    if not outputs.daily_partitions:
        write_csv(outputs.summary, args.output_dir / "summary_all_daily_pairs.csv")
        write_csv(outputs.trades, args.output_dir / "trades_all_daily_pairs.csv")
        write_csv(outputs.market, args.output_dir / "market_all_daily_pairs.csv")
        write_csv(outputs.latency, args.output_dir / "latency_all_daily_pairs.csv")
        write_csv(outputs.run_errors, args.output_dir / "run_errors.csv")
    if not outputs.cache_hit:
        write_hbt_manifest(args, records)

    if not outputs.daily_partitions:
        entry_exit_by_pair, entry_exit_all, entry_exit_index = build_entry_exit_outputs(outputs.pair_results, records)
        write_csv(entry_exit_all, args.output_dir / "entry_exit_all_daily_pairs.csv")
        write_csv(entry_exit_index, args.output_dir / "entry_exit_index.csv")
        if not args.skip_entry_exit_by_pair:
            write_entry_exit_by_pair(entry_exit_by_pair, args.output_dir / "entry_exit_by_pair")

    logging.info(
        "done dates=%s daily_pairs=%s ready_pairs=%s completed_pairs=%s errors=%s output=%s",
        len(trade_dates),
        len(records),
        len(outputs.event_paths),
        len(outputs.summary),
        len(outputs.run_errors),
        args.output_dir,
    )
    raise_for_expiry_position_errors(args, outputs.position_carry_status)
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
    spot_latency = leg_latency_ms(args, "spot")
    future_latency = leg_latency_ms(args, "future")
    has_leg_specific_latency = any(
        getattr(args, name, None) is not None
        for name in (
            "spot_order_latency_ms",
            "spot_response_latency_ms",
            "spot_feed_latency_offset_ms",
            "future_order_latency_ms",
            "future_response_latency_ms",
            "future_feed_latency_offset_ms",
        )
    )
    if has_leg_specific_latency:
        future_order, future_response, future_feed = map(_output_name_number, future_latency)
        spot_order, spot_response, spot_feed = map(_output_name_number, spot_latency)
        latency_suffix = (
            f"future_order_{future_order}ms_response_{future_response}ms_feed_{future_feed}ms_"
            f"spot_order_{spot_order}ms_response_{spot_response}ms_feed_{spot_feed}ms"
        )
    else:
        order_latency, response_latency, feed_latency = map(_output_name_number, spot_latency)
        if order_latency == response_latency == feed_latency:
            latency_suffix = f"latency_{order_latency}ms"
        else:
            latency_suffix = (
                f"order_{order_latency}ms_response_{response_latency}ms_"
                f"feed_{feed_latency}ms"
            )
    return PROJECT_ROOT / "output" / f"hbt_daily_full_market_{start}_{end}_{latency_suffix}"


def _output_name_number(value: float) -> str:
    """Format a numeric setting as a filesystem-safe, compact token."""
    return f"{float(value):g}".replace("-", "neg").replace(".", "p")


def leg_latency_ms(args: argparse.Namespace, leg: str) -> tuple[float, float, float]:
    """Return effective order, response, and feed latency for one market leg."""
    if leg not in {"spot", "future"}:
        raise ValueError(f"Unsupported latency leg: {leg!r}")
    values = []
    for specific_name, fallback_name in (
        (f"{leg}_order_latency_ms", "order_latency_ms"),
        (f"{leg}_response_latency_ms", "response_latency_ms"),
        (f"{leg}_feed_latency_offset_ms", "feed_latency_offset_ms"),
    ):
        specific = getattr(args, specific_name, None)
        values.append(float(getattr(args, fallback_name, 0.0) if specific is None else specific))
    return values[0], values[1], values[2]


def select_trade_dates(
    calendar_path: Path,
    start_date: str,
    end_date: str,
    *,
    excluded_dates: Iterable[str] = (),
) -> list[str]:
    calendar = pd.read_csv(calendar_path, dtype=str)
    start = normalize_date(start_date)
    end = normalize_date(end_date)
    excluded = {normalize_date(value) for value in excluded_dates}
    dates = [normalize_date(value) for value in calendar["trade_dates"].dropna().astype(str)]
    selected = [value for value in dates if start <= value <= end and value not in excluded]
    if not selected:
        raise ValueError(f"No trade dates found between {start} and {end} in {calendar_path}")
    return selected


def get_trade_dates(start_date: str, end_date: str, calendar_path: Path) -> list[str]:
    return select_trade_dates(calendar_path, start_date, end_date)


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
    build_manifest_path = config_dir / "build_manifest.json"
    try:
        build_manifest = json.loads(build_manifest_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        build_manifest = {"schema_version": 1, "dates": {}}
    manifest_dates = build_manifest.setdefault("dates", {})

    allowed_names = pair_name_filter(args.pair_name)
    records: list[DailyPairRecord] = []
    status_rows: list[dict[str, Any]] = []
    for trade_date in trade_dates:
        date_nodash = trade_date.replace("-", "")
        config_path = config_dir / f"arbitrage_config_{date_nodash}.json"
        target_path = target_dir / f"target_futures_{date_nodash}.csv"
        status = "existing"
        result: BuildArbitrageConfigResult | None = None
        build_signature = daily_config_build_signature(args, trade_date)
        needs_rebuild = (
            args.rebuild_daily_configs
            or not config_path.exists()
            or not target_path.exists()
            or manifest_dates.get(trade_date) != build_signature
        )
        if needs_rebuild:
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
                manifest_dates[trade_date] = build_signature
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
    build_manifest_path.write_text(
        json.dumps(build_manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return records, pd.DataFrame(status_rows)


def daily_config_build_signature(args: argparse.Namespace, trade_date: str) -> dict[str, Any]:
    ldate = get_calendar_ldate(args.calendar, trade_date)
    source_templates = (
        args.futures_parquet_template,
        args.twse_daytrade_template,
        args.tpex_daytrade_template,
        args.twse_daily_template,
        args.tpex_daily_template,
    )
    sources = [
        Path(format_daily_template(template, trade_date, ldate)).resolve()
        for template in source_templates
    ]
    return {
        "trade_date": trade_date,
        "ldate": ldate,
        "build_session_start": args.build_session_start,
        "build_session_end": args.build_session_end,
        "min_future_volume": args.min_future_volume,
        "min_stock_volume": args.min_stock_volume,
        "required_unit": args.required_unit,
        "name_template": args.name_template,
        "base_config": _content_fingerprint(args.base_config),
        "calendar": _content_fingerprint(args.calendar),
        "stockinfo": _content_fingerprint(args.stockinfo),
        "sources": [_stat_fingerprint(path) for path in sources],
    }


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


def excluded_run_key_set(
    args: argparse.Namespace | None = None,
    *,
    excluded_run_keys: Iterable[str] = (),
) -> set[str]:
    configured = excluded_run_keys if args is None else getattr(args, "excluded_run_keys", ())
    return {str(value).strip() for value in configured if str(value).strip()}


def filter_excluded_run_records(
    records: list[DailyPairRecord],
    args: argparse.Namespace | None = None,
    *,
    excluded_run_keys: Iterable[str] = (),
) -> list[DailyPairRecord]:
    excluded = excluded_run_key_set(args, excluded_run_keys=excluded_run_keys)
    if not excluded:
        return records
    return [record for record in records if record.run_key not in excluded]


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
                "initial_quantity": record.pair.initial_position.quantity,
                "initial_direction": record.pair.initial_position.direction.value,
                "universe_source": record.universe_source,
                "carried_from_date": record.carried_from_date,
                "daily_config_path": str(record.config_path),
            }
            for record in records
        ]
    )


def execute_hbt_runs(
    args: argparse.Namespace,
    base_records: list[DailyPairRecord],
    trade_dates: list[str],
) -> HbtRunOutputs:
    """Run independent daily HBTs or the date-sequential position-carry workflow."""

    base_records = filter_excluded_run_records(base_records, args)

    if not getattr(args, "carry_positions", True):
        cache_hit = hbt_cache_is_valid(args, base_records)
        if cache_hit:
            logging.info("valid result manifest found; skip event preparation and NPZ audit")
            event_paths: dict[str, dict[str, Path]] = {}
            conversion_status = read_csv_if_exists(args.output_dir / "conversion_status.csv")
            settings = read_csv_if_exists(args.output_dir / "hbt_settings.csv")
        else:
            event_paths, conversion_status = build_event_data(args, base_records)
            settings = hbt_settings_frame(args, base_records, event_paths)
        pair_results, summary, trades, market, latency, run_errors = run_or_load_backtests(
            args, base_records, event_paths
        )
        return HbtRunOutputs(
            records=base_records,
            event_paths=event_paths,
            pair_results=pair_results,
            summary=summary,
            trades=trades,
            market=market,
            latency=latency,
            run_errors=run_errors,
            conversion_status=conversion_status,
            settings=settings,
            position_carry_status=pd.DataFrame(),
            cache_hit=cache_hit,
        )

    cached_summary = read_csv_if_exists(args.output_dir / "summary_all_daily_pairs.csv")
    try:
        cached_records, reconstructed_status = reconstruct_position_carry_records(
            base_records,
            trade_dates,
            cached_summary,
            load_calendar_trade_dates(args.calendar),
            excluded_run_keys=getattr(args, "excluded_run_keys", ()),
        )
    except (TypeError, ValueError, KeyError) as exc:
        logging.info("cached carry state cannot be reconstructed: %s", exc)
        cached_records, reconstructed_status = [], pd.DataFrame()
    use_daily_resume = (
        getattr(args, "report_mode", "summary") == "summary"
        and (Path(args.output_dir) / "core" / "dates").is_dir()
    )
    cache_hit = (
        not use_daily_resume
        and bool(cached_records)
        and hbt_cache_is_valid(args, cached_records)
    )
    if cache_hit:
        logging.info("valid continuous-position result manifest found; reuse saved HBT results")
        pair_results, summary, trades, market, latency, run_errors = run_or_load_backtests(
            args, cached_records, {}
        )
        carry_status = read_csv_if_exists(args.output_dir / "position_carry_status.csv")
        if carry_status.empty:
            carry_status = reconstructed_status
        return HbtRunOutputs(
            records=cached_records,
            event_paths={},
            pair_results=pair_results,
            summary=summary,
            trades=trades,
            market=market,
            latency=latency,
            run_errors=run_errors,
            conversion_status=read_csv_if_exists(args.output_dir / "conversion_status.csv"),
            settings=read_csv_if_exists(args.output_dir / "hbt_settings.csv"),
            position_carry_status=carry_status,
            cache_hit=True,
        )

    return run_backtests_with_position_carry(args, base_records, trade_dates)


def run_backtests_with_position_carry(
    args: argparse.Namespace,
    base_records: list[DailyPairRecord],
    trade_dates: list[str],
) -> HbtRunOutputs:
    """Run dates in order while retaining same-day pair parallelism."""

    base_by_date: dict[str, list[DailyPairRecord]] = {
        trade_date: [record for record in base_records if record.trade_date == trade_date]
        for trade_date in trade_dates
    }
    calendar_trade_dates = load_calendar_trade_dates(args.calendar)
    carry: dict[PositionKey, PositionSnapshot] = {}
    all_records: list[DailyPairRecord] = []
    all_event_paths: dict[str, dict[str, Path]] = {}
    all_pair_results: dict[str, dict[str, pd.DataFrame]] = {}
    summary_frames: list[pd.DataFrame] = []
    trade_frames: list[pd.DataFrame] = []
    market_frames: list[pd.DataFrame] = []
    latency_frames: list[pd.DataFrame] = []
    error_frames: list[pd.DataFrame] = []
    conversion_frames: list[pd.DataFrame] = []
    settings_frames: list[pd.DataFrame] = []
    carry_frames: list[pd.DataFrame] = []
    result_store = DailyResultStore(Path(args.output_dir) / "core")
    stream_daily_details = getattr(args, "report_mode", "summary") == "summary"
    completed_dates: list[str] = []
    reused_dates = 0
    executed_dates = 0
    timing_rows: list[dict[str, Any]] = []
    full_detail_rows = 0

    configured_workers = max(1, int(getattr(args, "workers", 1)))
    executor = ProcessPoolExecutor(max_workers=configured_workers) if configured_workers > 1 else None
    try:
        for trade_date in trade_dates:
            date_started = time.perf_counter()
            date_records = augment_records_with_position_carry(base_by_date.get(trade_date, []), carry, trade_date)
            date_records = filter_excluded_run_records(date_records, args)
            logging.info(
                "continuous HBT date=%s selected=%s carried=%s total=%s",
                trade_date,
                len(base_by_date.get(trade_date, [])),
                sum(record.universe_source != "selected" for record in date_records),
                len(date_records),
            )
            identity_started = time.perf_counter()
            carry_in = position_carry_identity(carry)
            input_identity = hbt_manifest_payload(args, date_records)
            if not getattr(args, "rebuild_hbt_results", False):
                try:
                    persisted = result_store.validate(trade_date)
                    payload = persisted.payload
                    reusable = (
                        payload.get("input_identity_sha256") == canonical_sha256(input_identity)
                        and payload.get("carry_in_sha256") == canonical_sha256(carry_in)
                        and payload.get("run_keys") == [record.run_key for record in date_records]
                    )
                except DailyResultStoreError:
                    persisted = None
                    reusable = False
                timing_rows.append(
                    stage_timing_row(
                        trade_date,
                        "daily_identity_validation",
                        identity_started,
                        len(date_records),
                        "resume_check",
                    )
                )
                if reusable and persisted is not None:
                    resume_started = time.perf_counter()
                    summary = result_store.load_table(trade_date, "summary")
                    trades = result_store.load_table(trade_date, "trades")
                    market = result_store.load_table(trade_date, "market")
                    latency = result_store.load_table(trade_date, "latency")
                    run_errors = result_store.load_table(trade_date, "run_errors")
                    conversion_status = result_store.load_table(trade_date, "conversion")
                    settings = result_store.load_table(trade_date, "settings")
                    carry_status = result_store.load_table(trade_date, "position_carry")
                    next_carry, _, _ = advance_position_carry(
                        carry,
                        date_records,
                        summary,
                        calendar_trade_dates,
                    )
                    if canonical_sha256(position_carry_identity(next_carry)) == persisted.carry_out_sha256:
                        logging.info("reusing verified daily result partition date=%s", trade_date)
                        pair_results = pair_results_from_frames(trades, summary, market, latency)
                        if not getattr(args, "skip_entry_exit_by_pair", False):
                            entry_by_pair, _, _ = build_entry_exit_outputs(pair_results, date_records)
                            write_entry_exit_by_pair(
                                entry_by_pair,
                                Path(args.output_dir) / "entry_exit_by_pair",
                            )
                        carry = next_carry
                        all_records.extend(date_records)
                        if not stream_daily_details:
                            full_detail_rows = account_full_report_rows(
                                args, full_detail_rows, trades, market, latency
                            )
                            summary_frames.append(summary)
                            all_pair_results.update(pair_results)
                            trade_frames.append(trades)
                            market_frames.append(market)
                            latency_frames.append(latency)
                            error_frames.append(run_errors)
                            conversion_frames.append(conversion_status)
                            settings_frames.append(settings)
                            carry_frames.append(carry_status)
                        completed_dates.append(trade_date)
                        reused_dates += 1
                        timing_rows.append(
                            stage_timing_row(
                                trade_date,
                                "daily_resume_load",
                                resume_started,
                                len(date_records),
                                "reused",
                            )
                        )
                        timing_rows.append(
                            stage_timing_row(
                                trade_date,
                                "date_total",
                                date_started,
                                len(date_records),
                                "reused",
                            )
                        )
                        continue
                    logging.warning("daily result carry-out mismatch; rebuild required date=%s", trade_date)
            else:
                timing_rows.append(
                    stage_timing_row(
                        trade_date,
                        "daily_identity_validation",
                        identity_started,
                        len(date_records),
                        "forced_rebuild",
                    )
                )

            stage_started = time.perf_counter()
            event_paths, conversion_status = build_event_data(args, date_records)
            timing_rows.append(
                stage_timing_row(trade_date, "event_data", stage_started, len(date_records), "executed")
            )
            stage_started = time.perf_counter()
            settings = hbt_settings_frame(args, date_records, event_paths)
            timing_rows.append(
                stage_timing_row(trade_date, "event_audit", stage_started, len(date_records), "executed")
            )
            stage_started = time.perf_counter()
            pair_results, summary, trades, market, latency, run_errors = run_backtests(
                args,
                date_records,
                event_paths,
                executor=executor,
            )
            timing_rows.append(
                stage_timing_row(trade_date, "pair_matching", stage_started, len(date_records), "executed")
            )
            # run_backtests waits for every submitted future. Carry is therefore
            # resolved before the next date can submit work to the same pool.
            stage_started = time.perf_counter()
            carry, carry_status, expiry_errors = advance_position_carry(
                carry,
                date_records,
                summary,
                calendar_trade_dates,
            )
            entry_by_pair, entry_exit, entry_exit_index = build_entry_exit_outputs(pair_results, date_records)
            if not getattr(args, "skip_entry_exit_by_pair", False):
                write_entry_exit_by_pair(
                    entry_by_pair,
                    Path(args.output_dir) / "entry_exit_by_pair",
                )
            date_errors = concat_frames([run_errors, expiry_errors])
            timing_rows.append(
                stage_timing_row(
                    trade_date,
                    "carry_and_entry_exit",
                    stage_started,
                    len(date_records),
                    "executed",
                )
            )
            stage_started = time.perf_counter()
            result_store.publish(
                trade_date,
                {
                    "summary": summary,
                    "trades": trades,
                    "market": market,
                    "latency": latency,
                    "entry_exit": entry_exit,
                    "entry_exit_index": entry_exit_index,
                    "run_errors": date_errors,
                    "conversion": conversion_status,
                    "settings": settings,
                    "position_carry": carry_status,
                },
                input_identity=input_identity,
                carry_in=carry_in,
                carry_out=position_carry_identity(carry),
                run_keys=[record.run_key for record in date_records],
                metadata={
                    "engine": getattr(args, "engine", "reference"),
                    "engine_version": execution_engine_version(args),
                    "compact_schema_version": (
                        COMPACT_SCHEMA_VERSION
                        if getattr(args, "market_data_cache", "event_npz") == "compact"
                        else None
                    ),
                    "strategy_clock": "step_ms",
                    "step_ms": getattr(args, "step_ms", None),
                    "time_in_force_semantics": HBT_TIME_IN_FORCE_SEMANTICS,
                },
                replace_existing=bool(getattr(args, "rebuild_hbt_results", False)),
            )
            timing_rows.append(
                stage_timing_row(
                    trade_date,
                    "daily_result_publish",
                    stage_started,
                    len(date_records),
                    "executed",
                )
            )

            all_records.extend(date_records)
            if not stream_daily_details:
                full_detail_rows = account_full_report_rows(
                    args, full_detail_rows, trades, market, latency
                )
                all_event_paths.update(event_paths)
                summary_frames.append(summary)
                all_pair_results.update(pair_results)
                trade_frames.append(trades)
                market_frames.append(market)
                latency_frames.append(latency)
                error_frames.extend((run_errors, expiry_errors))
                conversion_frames.append(conversion_status)
                settings_frames.append(settings)
                carry_frames.append(carry_status)
            completed_dates.append(trade_date)
            executed_dates += 1
            timing_rows.append(
                stage_timing_row(
                    trade_date,
                    "date_total",
                    date_started,
                    len(date_records),
                    "executed",
                )
            )
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=True)

    if stream_daily_details:
        compatibility_tables = {
            "summary": "summary_all_daily_pairs.csv",
            "trades": "trades_all_daily_pairs.csv",
            "market": "market_all_daily_pairs.csv",
            "latency": "latency_all_daily_pairs.csv",
            "run_errors": "run_errors.csv",
            "entry_exit": "entry_exit_all_daily_pairs.csv",
            "entry_exit_index": "entry_exit_index.csv",
            "conversion": "conversion_status.csv",
            "settings": "hbt_settings.csv",
            "position_carry": "position_carry_status.csv",
        }
        compatibility_started = time.perf_counter()
        result_store.write_tables_csv(
            completed_dates,
            {
                table: Path(args.output_dir) / filename
                for table, filename in compatibility_tables.items()
            },
        )
        timing_rows.append(
            stage_timing_row(
                None,
                "compatibility_csv_stream",
                compatibility_started,
                len(all_records),
                "run",
            )
        )

        # Default annual runs release every daily frame before advancing. Read
        # back only the small Python-boundary tables after the bounded Parquet
        # to CSV stream is complete; detailed tables stay on disk.
        summary_output = read_csv_if_exists(Path(args.output_dir) / compatibility_tables["summary"])
        error_output = read_csv_if_exists(Path(args.output_dir) / compatibility_tables["run_errors"])
        conversion_output = read_csv_if_exists(Path(args.output_dir) / compatibility_tables["conversion"])
        settings_output = read_csv_if_exists(Path(args.output_dir) / compatibility_tables["settings"])
        carry_output = read_csv_if_exists(Path(args.output_dir) / compatibility_tables["position_carry"])
    else:
        summary_output = concat_frames(summary_frames)
        error_output = concat_frames(error_frames)
        conversion_output = concat_frames(conversion_frames)
        settings_output = concat_frames(settings_frames)
        carry_output = concat_frames(carry_frames)

    return HbtRunOutputs(
        records=all_records,
        event_paths=all_event_paths,
        pair_results=all_pair_results,
        summary=summary_output,
        trades=concat_frames(trade_frames),
        market=concat_frames(market_frames),
        latency=concat_frames(latency_frames),
        run_errors=error_output,
        conversion_status=conversion_output,
        settings=settings_output,
        position_carry_status=carry_output,
        cache_hit=False,
        daily_partitions=stream_daily_details,
        daily_dates_reused=reused_dates,
        daily_dates_executed=executed_dates,
        stage_timings=pd.DataFrame(timing_rows),
    )


def stage_timing_row(
    trade_date: str | None,
    stage: str,
    started: float,
    pair_count: int,
    mode: str,
) -> dict[str, Any]:
    return {
        "trade_date": trade_date,
        "stage": stage,
        "elapsed_seconds": time.perf_counter() - started,
        "pair_count": pair_count,
        "mode": mode,
    }


def account_full_report_rows(
    args: argparse.Namespace,
    current_rows: int,
    *frames: pd.DataFrame,
) -> int:
    """Enforce the explicit retained-row budget required by diagnostic full mode."""
    total = current_rows + sum(len(frame) for frame in frames)
    limit = getattr(args, "full_report_max_rows", None)
    if limit is None or int(limit) <= 0:
        raise RuntimeError("full report mode requires a positive full_report_max_rows budget")
    if total > int(limit):
        raise RuntimeError(
            f"full report retained-row budget exceeded: rows={total} limit={int(limit)}"
        )
    return total


def position_carry_identity(
    carry: dict[PositionKey, PositionSnapshot],
) -> list[dict[str, Any]]:
    """Return deterministic, result-defining carry state for manifest chaining."""
    return [
        {
            "spot_symbol": key[0],
            "future_symbol": key[1],
            "source_trade_date": snapshot.source_trade_date,
            "config_path": str(snapshot.config_path),
            "quantity": snapshot.quantity,
            "direction": snapshot.direction.value,
            "entry_basis_pct": snapshot.entry_basis_pct,
            "entry_spot_price": snapshot.entry_spot_price,
            "entry_future_price": snapshot.entry_future_price,
            "pair": snapshot.pair,
        }
        for key, snapshot in sorted(carry.items())
    ]


def augment_records_with_position_carry(
    base_records: list[DailyPairRecord],
    carry: dict[PositionKey, PositionSnapshot],
    trade_date: str,
) -> list[DailyPairRecord]:
    """Restore selected held pairs and append held pairs absent from today's universe."""

    augmented: list[DailyPairRecord] = []
    selected_keys: set[PositionKey] = set()
    for record in base_records:
        key = position_key(record.pair.spot_symbol, record.pair.future_symbol)
        selected_keys.add(key)
        snapshot = carry.get(key)
        if snapshot is None:
            augmented.append(record)
            continue
        augmented.append(
            replace(
                record,
                pair=snapshot.restored_pair(record.pair),
                universe_source="selected+carried",
                carried_from_date=snapshot.source_trade_date,
            )
        )

    for key, snapshot in sorted(carry.items(), key=lambda item: item[0]):
        if key in selected_keys:
            continue
        pair = snapshot.restored_pair()
        augmented.append(
            DailyPairRecord(
                trade_date=trade_date,
                run_key=pair_run_key(trade_date, pair.name),
                pair=pair,
                config_path=snapshot.config_path,
                universe_source="carried_position",
                carried_from_date=snapshot.source_trade_date,
            )
        )
    return augmented


def advance_position_carry(
    previous: dict[PositionKey, PositionSnapshot],
    records: list[DailyPairRecord],
    summary: pd.DataFrame,
    calendar_trade_dates: list[str],
) -> tuple[dict[PositionKey, PositionSnapshot], pd.DataFrame, pd.DataFrame]:
    """Create next-day snapshots and reject residual positions at contract expiry."""

    summary_by_key = {
        str(row.run_key): row
        for row in summary.itertuples(index=False)
        if hasattr(row, "run_key")
    }
    next_carry: dict[PositionKey, PositionSnapshot] = {}
    audit_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []

    for record in records:
        key = position_key(record.pair.spot_symbol, record.pair.future_symbol)
        prior = previous.get(key)
        row = summary_by_key.get(record.run_key)
        snapshot = prior if row is None else snapshot_from_summary_row(row, record)
        expiry_date = futures_contract_expiry_date(
            record.pair.future_symbol,
            record.trade_date,
            calendar_trade_dates,
        )
        expired_with_position = bool(
            snapshot is not None and expiry_date is not None and record.trade_date >= expiry_date
        )

        if expired_with_position:
            status = "expiry_position_remaining"
            error_rows.append(
                run_error_row(
                    record,
                    f"open position remained on/after futures expiry {expiry_date}; position was not rolled",
                )
            )
        elif row is None and prior is not None:
            status = "carried_without_result"
            next_carry[key] = prior
        elif row is None:
            status = "no_result"
        elif snapshot is None:
            status = "closed"
        else:
            status = "carried_forward"
            next_carry[key] = snapshot

        initial = record.pair.initial_position
        audit_rows.append(
            {
                "trade_date": record.trade_date,
                "run_key": record.run_key,
                "pair_name": record.pair.name,
                "spot_symbol": record.pair.spot_symbol,
                "future_symbol": record.pair.future_symbol,
                "universe_source": record.universe_source,
                "carried_from_date": record.carried_from_date,
                "initial_quantity": initial.quantity,
                "initial_direction": initial.direction.value,
                "final_quantity": 0 if snapshot is None else snapshot.quantity,
                "final_direction": Signal.HOLD.value if snapshot is None else snapshot.direction.value,
                "expiry_date": expiry_date,
                "is_expiry_date": expiry_date == record.trade_date,
                "carry_to_next_date": key in next_carry,
                "status": status,
            }
        )

    return next_carry, pd.DataFrame(audit_rows), pd.DataFrame(error_rows)


def reconstruct_position_carry_records(
    base_records: list[DailyPairRecord],
    trade_dates: list[str],
    summary: pd.DataFrame,
    calendar_trade_dates: list[str],
    *,
    excluded_run_keys: Iterable[str] = (),
) -> tuple[list[DailyPairRecord], pd.DataFrame]:
    """Rebuild the dynamic universe from saved summaries for cache validation."""

    if summary.empty or "trade_date" not in summary.columns:
        return [], pd.DataFrame()
    base_by_date = {
        trade_date: [record for record in base_records if record.trade_date == trade_date]
        for trade_date in trade_dates
    }
    summary_dates = summary["trade_date"].astype(str).str.slice(0, 10)
    carry: dict[PositionKey, PositionSnapshot] = {}
    all_records: list[DailyPairRecord] = []
    audit_frames: list[pd.DataFrame] = []
    for trade_date in trade_dates:
        records = augment_records_with_position_carry(base_by_date.get(trade_date, []), carry, trade_date)
        records = filter_excluded_run_records(records, args=None, excluded_run_keys=excluded_run_keys)
        day_summary = summary.loc[summary_dates.eq(trade_date)].copy()
        carry, audit, _ = advance_position_carry(carry, records, day_summary, calendar_trade_dates)
        all_records.extend(records)
        audit_frames.append(audit)
    return all_records, concat_frames(audit_frames)


def load_calendar_trade_dates(calendar_path: Path) -> list[str]:
    calendar_frame = pd.read_csv(calendar_path, dtype=str)
    return [normalize_date(value) for value in calendar_frame["trade_dates"].dropna().astype(str)]


def raise_for_expiry_position_errors(args: argparse.Namespace, carry_status: pd.DataFrame) -> None:
    if getattr(args, "continue_on_error", False) or carry_status.empty or "status" not in carry_status.columns:
        return
    violations = carry_status.loc[carry_status["status"].eq("expiry_position_remaining")]
    if not violations.empty:
        contracts = ", ".join(
            f"{row.future_symbol}@{row.trade_date}" for row in violations.itertuples(index=False)
        )
        raise RuntimeError(f"positions remained at futures expiry (no rollover performed): {contracts}")


def build_event_data(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
) -> tuple[dict[str, dict[str, Path]], pd.DataFrame]:
    if getattr(args, "market_data_cache", "event_npz") == "compact":
        return build_compact_event_data(args, records)
    args.spot_input_csv_by_symbol = prepare_spot_input_csvs(args, records)
    future_results = prepare_future_events(args, records)
    cache: dict[tuple[str, str, str], EventDataResult] = {}
    paths_by_run_key: dict[str, dict[str, Path]] = {}
    rows: list[dict[str, Any]] = []
    for record in records:
        spot_key = (record.trade_date, "stock", record.pair.spot_symbol)
        future_key = (record.trade_date, "stock_future", record.pair.future_symbol)
        if spot_key not in cache:
            cache[spot_key] = ensure_spot_events(args, record.pair.spot_symbol, record.trade_date)
        if future_key not in cache:
            cache[future_key] = future_results[(record.trade_date, record.pair.future_symbol)]

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


def build_compact_event_data(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
) -> tuple[dict[str, dict[str, Path]], pd.DataFrame]:
    """Build one date's compact partitions and return slim or reference-adapter paths."""
    if not records:
        return {}, pd.DataFrame()
    trade_dates = sorted({record.trade_date for record in records})
    if len(trade_dates) != 1:
        paths: dict[str, dict[str, Path]] = {}
        frames = []
        for trade_date in trade_dates:
            date_paths, date_frame = build_compact_event_data(
                args, [record for record in records if record.trade_date == trade_date]
            )
            paths.update(date_paths)
            frames.append(date_frame)
        return paths, concat_frames(frames)

    trade_date = trade_dates[0]
    date_nodash = trade_date.replace("-", "")
    stock_path = Path(
        str(args.stock_tick_parquet_template).format(
            date=trade_date, date_dash=trade_date, date_nodash=date_nodash
        )
    )
    futures_dir = getattr(args, "event_futures_parquet_dir", None)
    future_path = (
        Path(futures_dir) / f"{trade_date}.parquet"
        if futures_dir is not None
        else Path(f"/mnt/z/ticks_parquet_stock_future/{trade_date}.parquet")
    )
    spots = tuple(sorted({str(record.pair.spot_symbol) for record in records}))
    futures = tuple(sorted({str(record.pair.future_symbol) for record in records}))
    timezone = ZoneInfo("Asia/Taipei")
    session_start_ns = parse_timestamp(args.session_start, "auto", trade_date, timezone)
    session_end_ns = parse_timestamp(args.session_end, "auto", trade_date, timezone)
    store = CompactCacheStore(
        CompactBuildConfig(
            cache_root=Path(args.compact_cache_root),
            compression=args.compact_cache_compression,
            profile=args.compact_cache_profile,
            session_start_ns=session_start_ns,
            session_end_ns=session_end_ns,
            batch_rows=args.compact_cache_batch_rows,
            max_cache_bytes=int(args.compact_cache_max_gb * 1024**3),
            min_free_bytes=int(args.compact_cache_min_free_gb * 1024**3),
            rebuild=args.rebuild_compact_cache,
        )
    )
    try:
        manifest = store.build_date(
            trade_date,
            [
                CompactSource("stock", (stock_path,), spots),
                CompactSource("stock_future", (future_path,), futures),
            ],
        )
    except (CompactCacheError, OSError) as exc:
        if not args.continue_on_error:
            raise
        error = repr(exc)
        logging.error("compact date build failed date=%s error=%s", trade_date, error)
        return {}, pd.DataFrame(
            [
                {
                    "trade_date": trade_date,
                    "run_key": record.run_key,
                    "pair_name": record.pair.name,
                    "spot_symbol": record.pair.spot_symbol,
                    "future_symbol": record.pair.future_symbol,
                    "spot_status": "missing",
                    "future_status": "missing",
                    "spot_path": None,
                    "future_path": None,
                    "ok": False,
                    "spot_error": error,
                    "future_error": error,
                    "compact_cache_state": "error",
                    "compact_identity_sha256": None,
                    "compact_build_invocation_scan_count": 0,
                }
                for record in records
            ]
        )
    paths_by_run_key: dict[str, dict[str, Path]] = {}
    audit_rows: list[dict[str, Any]] = []
    reference_mode = getattr(args, "engine", "reference") == "reference"
    for record in records:
        leg_paths: dict[str, Path] = {}
        errors = {}
        for leg, source, symbol in (
            ("spot", "stock", record.pair.spot_symbol),
            ("future", "stock_future", record.pair.future_symbol),
        ):
            details = manifest["sources"][source]["symbols"].get(str(symbol), {})
            if details.get("status") != "valid":
                errors[leg] = f"missing compact partition: {source}/{symbol}"
                continue
            compact_path = store.date_path(trade_date) / f"source={source}" / details["file"]
            if reference_mode:
                output = (
                    WORKSPACE_ROOT
                    / "data"
                    / "tw_compact_reference_events"
                    / f"date={date_nodash}"
                    / f"source={source}"
                    / f"{symbol}.npz"
                )
                adapter_manifest = output.with_suffix(output.suffix + ".compact.json")
                reusable = False
                if output.is_file() and adapter_manifest.is_file() and not args.rebuild_event_data:
                    try:
                        saved = json.loads(adapter_manifest.read_text(encoding="utf-8"))
                        reusable = saved.get("compact_identity_sha256") == manifest["identity_sha256"]
                    except (OSError, json.JSONDecodeError):
                        reusable = False
                if not reusable:
                    write_reference_npz_from_compact(
                        store.read_symbol(trade_date, source, str(symbol)),
                        output,
                        trade_date=trade_date,
                        compact_identity_sha256=manifest["identity_sha256"],
                        npz_compression=args.npz_compression,
                    )
                leg_paths[leg] = output
            else:
                leg_paths[leg] = compact_path
        ok = not errors and len(leg_paths) == 2
        if ok:
            paths_by_run_key[record.run_key] = leg_paths
        audit_rows.append(
            {
                "trade_date": trade_date,
                "run_key": record.run_key,
                "pair_name": record.pair.name,
                "spot_symbol": record.pair.spot_symbol,
                "future_symbol": record.pair.future_symbol,
                "spot_status": "compact" if "spot" in leg_paths else "missing",
                "future_status": "compact" if "future" in leg_paths else "missing",
                "spot_path": str(leg_paths["spot"]) if "spot" in leg_paths else None,
                "future_path": str(leg_paths["future"]) if "future" in leg_paths else None,
                "ok": ok,
                "spot_error": errors.get("spot"),
                "future_error": errors.get("future"),
                "compact_cache_state": manifest["cache_state"],
                "compact_identity_sha256": manifest["identity_sha256"],
                "compact_build_invocation_scan_count": manifest["build_invocation_scan_count"],
            }
        )
        if not ok and not args.continue_on_error:
            raise RuntimeError(f"compact data missing for {record.run_key}: {errors}")
    return paths_by_run_key, pd.DataFrame(audit_rows)


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
        input_csv = spot_input_csv_path(args, trade_date)
        split_input_csv = getattr(args, "spot_input_csv_by_symbol", {}).get((trade_date, symbol))
        if split_input_csv is not None:
            input_csv = split_input_csv
        if input_csv is not None and not input_csv.exists():
            return EventDataResult(None, "missing", f"missing spot input csv: {input_csv}")
        path, _ = convert_tw_stock_to_npz(
            symbol=symbol,
            start_date=trade_date,
            end_date=trade_date,
            start_time=args.session_start,
            end_time=args.session_end,
            output=output,
            workspace_root=WORKSPACE_ROOT,
            input_csv=input_csv,
            data_api=input_csv is None,
            daily_parquet=False,
            data_platform_base=args.data_platform_base,
            levels=5,
            qa_sample_rows=args.conversion_qa_sample_rows,
            npz_compression=args.npz_compression,
        )
        return EventDataResult(path, "generated")
    except Exception as exc:
        return EventDataResult(None, "error", repr(exc))


def prepare_spot_input_csvs(args: argparse.Namespace, records: list[DailyPairRecord]) -> dict[tuple[str, str], Path]:
    records = [
        record
        for record in records
        if args.rebuild_event_data
        or not expected_event_path(args, record.pair.spot_symbol, "stock", record.trade_date).exists()
    ]
    if not records:
        logging.info("all spot event NPZ files exist; skip daily CSV splitting")
        return {}
    if spot_input_csv_path(args, records[0].trade_date) is None:
        return {}
    result: dict[tuple[str, str], Path] = {}
    symbols_by_date: dict[str, set[str]] = {}
    for record in records:
        symbols_by_date.setdefault(record.trade_date, set()).add(str(record.pair.spot_symbol))

    for trade_date, symbols in symbols_by_date.items():
        date_nodash = trade_date.replace("-", "")
        # This cache is shared across output directories. A parameter sweep no
        # longer duplicates several GB of per-symbol source CSV files.
        split_dir = WORKSPACE_ROOT / "data" / "spot_tick_csv_by_symbol" / date_nodash
        split_dir.mkdir(parents=True, exist_ok=True)
        expected = {symbol: split_dir / f"{symbol}.csv" for symbol in symbols}
        existing = {
            symbol: path
            for symbol, path in expected.items()
            if path.exists() and not args.rebuild_event_data
        }
        result.update({(trade_date, symbol): path for symbol, path in existing.items()})
        missing_symbols = sorted(set(expected) - set(existing))
        if not missing_symbols:
            continue

        source = spot_input_csv_path(args, trade_date)
        if source is None or not source.exists():
            continue
        split_daily_spot_csv(source, expected, missing_symbols)
        result.update(
            {
                (trade_date, symbol): path
                for symbol, path in expected.items()
                if path.exists()
            }
        )
    return result


def split_daily_spot_csv(source: Path, outputs: dict[str, Path], symbols: list[str]) -> None:
    if split_daily_spot_csv_with_rg(source, outputs, symbols):
        return
    split_daily_spot_csv_stream(source, outputs, symbols)


def split_daily_spot_csv_with_rg(source: Path, outputs: dict[str, Path], symbols: list[str]) -> bool:
    rg = shutil.which("rg")
    if rg is None:
        return False
    header = first_csv_line(source)
    if header is None:
        return False
    pattern = "^(?:" + "|".join(re.escape(symbol) for symbol in symbols) + "),"
    process = subprocess.Popen(
        [rg, "--no-heading", "--no-line-number", pattern, str(source)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    files: dict[str, Any] = {}
    try:
        for line in process.stdout:
            symbol = line.split(",", 1)[0]
            path = outputs.get(symbol)
            if path is None:
                continue
            file = files.get(symbol)
            if file is None:
                path.parent.mkdir(parents=True, exist_ok=True)
                file = path.open("w", newline="", encoding="utf-8-sig")
                files[symbol] = file
                file.write(header)
            file.write(line)
    finally:
        for file in files.values():
            file.close()
    _, stderr = process.communicate()
    if process.returncode not in (0, 1):
        raise RuntimeError(f"rg failed while splitting {source}: {stderr.strip()}")
    return True


def first_csv_line(path: Path) -> str | None:
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        return file.readline() or None


def split_daily_spot_csv_stream(source: Path, outputs: dict[str, Path], symbols: list[str]) -> None:
    symbol_set = set(symbols)
    writers: dict[str, csv.DictWriter] = {}
    files: dict[str, Any] = {}
    try:
        with source.open("r", newline="", encoding="utf-8-sig") as source_file:
            reader = csv.DictReader(source_file)
            if reader.fieldnames is None or "symbol" not in reader.fieldnames:
                raise ValueError(f"spot input csv missing symbol column: {source}")
            for row in reader:
                symbol = str(row.get("symbol", ""))
                if symbol not in symbol_set:
                    continue
                path = outputs.get(symbol)
                if path is None:
                    continue
                writer = writers.get(symbol)
                if writer is None:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    file = path.open("w", newline="", encoding="utf-8-sig")
                    files[symbol] = file
                    writer = csv.DictWriter(file, fieldnames=reader.fieldnames)
                    writers[symbol] = writer
                    writer.writeheader()
                writer.writerow(row)
    finally:
        for file in files.values():
            file.close()


def spot_input_csv_path(args: argparse.Namespace, trade_date: str) -> Path | None:
    template = getattr(args, "spot_input_csv_template", DEFAULT_SPOT_INPUT_CSV_TEMPLATE)
    if template is None or str(template).strip() == "":
        return None
    date_dash = normalize_date(trade_date)
    date_nodash = date_dash.replace("-", "")
    path = Path(
        str(template).format(
            date=date_dash,
            date_dash=date_dash,
            date_nodash=date_nodash,
        )
    )
    return path if path.is_absolute() else (PROJECT_ROOT / path).resolve()


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
            npz_compression=args.npz_compression,
        )
        return EventDataResult(path, "generated")
    except Exception as exc:
        return EventDataResult(None, "error", repr(exc))


def prepare_future_events(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
) -> dict[tuple[str, str], EventDataResult]:
    """Prepare futures event files with one source-parquet scan per trade date."""
    symbols_by_date: dict[str, set[str]] = {}
    for record in records:
        symbols_by_date.setdefault(record.trade_date, set()).add(str(record.pair.future_symbol))

    results: dict[tuple[str, str], EventDataResult] = {}
    rebuild = bool(getattr(args, "rebuild_event_data", False))
    skip_missing = bool(getattr(args, "no_convert_missing_event_data", False)) and not rebuild
    for trade_date, symbol_set in symbols_by_date.items():
        pending: dict[str, Path] = {}
        for symbol in sorted(symbol_set):
            output = expected_event_path(args, symbol, "stock_future", trade_date)
            key = (trade_date, symbol)
            if output.exists() and not rebuild:
                results[key] = EventDataResult(output, "existing")
            elif skip_missing:
                results[key] = EventDataResult(None, "missing", f"missing future npz: {output}")
            else:
                pending[symbol] = output

        if not pending:
            continue
        logging.info(
            "batch converting futures date=%s symbols=%s with one parquet scan",
            trade_date,
            len(pending),
        )
        try:
            generated, errors = convert_tw_stock_future_batch_to_npz(
                symbols=pending,
                start_date=trade_date,
                start_time=args.session_start,
                end_time=args.session_end,
                output_by_symbol=pending,
                workspace_root=WORKSPACE_ROOT,
                path_config=WORKSPACE_ROOT / "path.toml",
                daily_parquet_dir=args.event_futures_parquet_dir,
                levels=5,
                qa_sample_rows=args.conversion_qa_sample_rows,
                npz_compression=args.npz_compression,
            )
        except Exception as exc:
            error = repr(exc)
            generated, errors = {}, {symbol: error for symbol in pending}

        for symbol, output in pending.items():
            key = (trade_date, symbol)
            if symbol in generated:
                results[key] = EventDataResult(generated[symbol], "generated")
            else:
                results[key] = EventDataResult(
                    None,
                    "error",
                    errors.get(symbol, f"future batch conversion did not produce {output}"),
                )
    return results


def hbt_settings_frame(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
    event_paths: dict[str, dict[str, Path]],
) -> pd.DataFrame:
    rows = []
    args.hbt_tick_sizes = {}
    args.hbt_event_rows = {}
    for record in records:
        paths = event_paths.get(record.run_key)
        if not paths:
            continue
        spot = summarize_asset(args, record, "spot", paths["spot"])
        future = summarize_asset(args, record, "future", paths["future"])
        rows.extend((spot, future))
        args.hbt_tick_sizes[str(paths["spot"])] = spot["tick_size"]
        args.hbt_tick_sizes[str(paths["future"])] = future["tick_size"]
        args.hbt_event_rows[str(paths["spot"])] = int(spot["rows"])
        args.hbt_event_rows[str(paths["future"])] = int(future["rows"])
    return pd.DataFrame(rows)


def summarize_asset(args: argparse.Namespace, record: DailyPairRecord, leg: str, data_path: Path) -> dict[str, Any]:
    instrument = "stock" if leg == "spot" else "future"
    contract_size = 1000.0 if leg == "spot" else float(record.pair.future_pnl_multiplier)
    configured_tick = record.pair.spot_tick_size if leg == "spot" else record.pair.future_tick_size
    audit_cache = getattr(args, "hbt_asset_audits", None)
    if audit_cache is None:
        audit_cache = args.hbt_asset_audits = {}
    audit_key = (str(data_path), instrument, record.trade_date)
    if audit_key not in audit_cache:
        if getattr(args, "engine", "reference") == "slim":
            audit_cache[audit_key] = compact_asset_audit(data_path, instrument, record.trade_date)
        else:
            audit_cache[audit_key] = hbt_asset_audit(data_path, instrument, trade_date=record.trade_date)
    tick_size, summary = audit_cache[audit_key]
    if configured_tick is not None:
        tick_size = configured_tick
    order_latency_ms, response_latency_ms, feed_latency_offset_ms = leg_latency_ms(args, leg)
    hbt_config = BacktestConfig(
        data=data_path,
        contract_size=contract_size,
        tick_size=tick_size,
        lot_size=1.0,
        maker_fee=0.0,
        taker_fee=0.0,
        order_latency_ns=ms_to_ns(order_latency_ms),
        queue_model=args.queue_model,
    )
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
        "response_latency_ns": ms_to_ns(response_latency_ms),
        "feed_latency_offset_ns": ms_to_ns(feed_latency_offset_ms),
        "second_leg_delay_ns": ms_to_ns(args.second_leg_delay_ms),
        "post_first_feed_wait": getattr(args, "post_first_feed_wait", "none"),
        "post_first_feed_timeout_ns": ms_to_ns(getattr(args, "post_first_feed_timeout_ms", 0.0)),
        "post_first_feed_poll_ns": ms_to_ns(getattr(args, "post_first_feed_poll_ms", 10.0)),
        "queue_model": hbt_config.queue_model,
        "rows": summary["rows"],
        "first_exch_ts": summary["first_exch_ts"],
        "last_exch_ts": summary["last_exch_ts"],
        "min_feed_latency_ns": summary["min_latency_ns"],
        "max_feed_latency_ns": summary["max_latency_ns"],
        "depth_events": summary["depth_events"],
        "trade_events": summary["trade_events"],
        "engine": getattr(args, "engine", "reference"),
        "compact_schema_version": COMPACT_SCHEMA_VERSION
        if getattr(args, "market_data_cache", "event_npz") == "compact"
        else None,
    }


def compact_asset_audit(
    data_path: Path,
    instrument: str,
    trade_date: str,
) -> tuple[float, dict[str, int | None]]:
    import pyarrow as pa
    import pyarrow.ipc as ipc

    from arbitrage.ticks import tw_stock_future_tick_size, tw_stock_tick_size

    with pa.memory_map(str(data_path), "r") as handle:
        table = ipc.open_file(handle).read_all()
    metadata = {key.decode(): value.decode() for key, value in (table.schema.metadata or {}).items()}
    adjustment = int(metadata.get("local_timestamp_adjustment_ns", 0))
    exchange = table["exch_ts"].to_numpy(zero_copy_only=False)
    local = table["local_ts_raw"].to_numpy(zero_copy_only=False) + adjustment
    bid = table["bid_px"].to_numpy(zero_copy_only=False)
    ask = table["ask_px"].to_numpy(zero_copy_only=False)
    prices = np.concatenate((bid, ask))
    prices = prices[np.isfinite(prices) & (prices > 0)]
    if len(prices):
        min_price = float(prices.min())
        tick_size = (
            tw_stock_tick_size(min_price)
            if instrument == "stock"
            else tw_stock_future_tick_size(min_price, trade_date)
        )
    else:
        # Match infer_hbt_asset_tick_size for a valid zero-event reference NPZ.
        tick_size = 1.0
    volume = table["total_volume"].to_numpy(zero_copy_only=False)
    trade_events = int(np.sum(np.diff(volume) > 0)) if len(volume) > 1 else 0
    return tick_size, {
        "rows": table.num_rows,
        "first_exch_ts": int(exchange.min()) if len(exchange) else None,
        "last_exch_ts": int(exchange.max()) if len(exchange) else None,
        "min_latency_ns": int(np.min(local - exchange)) if len(exchange) else None,
        "max_latency_ns": int(np.max(local - exchange)) if len(exchange) else None,
        "depth_events": None,
        "trade_events": trade_events,
    }


def run_backtests(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
    event_paths: dict[str, dict[str, Path]],
    *,
    executor: ProcessPoolExecutor | None = None,
) -> tuple[dict[str, dict[str, pd.DataFrame]], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    results: dict[str, dict[str, pd.DataFrame]] = {}
    summary_frames = []
    trade_frames = []
    market_frames = []
    latency_frames = []
    error_rows = []
    runnable: list[tuple[DailyPairRecord, dict[str, Path]]] = []
    for record in records:
        paths = event_paths.get(record.run_key)
        if paths is None:
            error_rows.append(run_error_row(record, "missing converted event data"))
            continue
        runnable.append((record, paths))

    completed: dict[str, dict[str, pd.DataFrame]] = {}
    failures: dict[str, str] = {}
    workers = max(1, min(int(getattr(args, "workers", 1)), len(runnable) or 1))
    logging.info("running %s pair backtests with workers=%s", len(runnable), workers)
    if workers == 1:
        for index, (record, paths) in enumerate(runnable, start=1):
            try:
                completed[record.run_key] = _run_single_pair_backtest(args, record, paths)
                if index == len(runnable) or index % 10 == 0:
                    logging.info("pair backtest progress=%s/%s", index, len(runnable))
            except Exception as exc:
                failures[record.run_key] = repr(exc)
                if not args.continue_on_error:
                    raise
    elif executor is None:
        with ProcessPoolExecutor(max_workers=workers) as date_executor:
            _collect_parallel_backtests(
                args,
                runnable,
                date_executor,
                completed,
                failures,
            )
    else:
        _collect_parallel_backtests(args, runnable, executor, completed, failures)

    for record in records:
        if record.run_key in failures:
            error_rows.append(run_error_row(record, failures[record.run_key]))
            continue
        result = completed.get(record.run_key)
        if result is None:
            continue
        results[record.run_key] = result
        summary = result["summary"]
        trades = result["trades"]
        market = result["market"]
        latency = result["latency"]
        summary_frames.append(summary)
        if not trades.empty:
            trade_frames.append(trades)
        if not market.empty:
            market_frames.append(market)
        if not latency.empty:
            latency_frames.append(latency)

    return (
        results,
        concat_frames(summary_frames),
        concat_frames(trade_frames),
        concat_frames(market_frames),
        concat_frames(latency_frames),
        pd.DataFrame(error_rows),
    )


def _collect_parallel_backtests(
    args: argparse.Namespace,
    runnable: list[tuple[DailyPairRecord, dict[str, Path]]],
    executor: ProcessPoolExecutor,
    completed: dict[str, dict[str, pd.DataFrame]],
    failures: dict[str, str],
) -> None:
    workers = max(1, min(int(getattr(args, "workers", 1)), len(runnable) or 1))
    shards = balanced_backtest_shards(runnable, workers, getattr(args, "hbt_event_rows", {}))
    args.hbt_shard_weights = [sum(item[2] for item in shard) for shard in shards]
    future_shards = {
        executor.submit(
            _run_backtest_shard,
            args,
            [(record, paths) for record, paths, _ in shard],
        ): shard
        for shard in shards
    }
    finished = 0
    worker_pids = set(getattr(args, "hbt_worker_pids", set()))
    for future in as_completed(future_shards):
        try:
            worker_pid, shard_results = future.result()
            worker_pids.add(worker_pid)
            for run_key, result, error in shard_results:
                if error is None and result is not None:
                    completed[run_key] = result
                elif error is not None:
                    failures[run_key] = error
                finished += 1
                if finished == len(runnable) or finished % 10 == 0:
                    logging.info("pair backtest progress=%s/%s", finished, len(runnable))
        except Exception as exc:
            if not args.continue_on_error:
                for pending in future_shards:
                    pending.cancel()
                raise
            for record, _, _ in future_shards[future]:
                failures.setdefault(record.run_key, repr(exc))
                finished += 1
    args.hbt_worker_pids = worker_pids


def balanced_backtest_shards(
    runnable: list[tuple[DailyPairRecord, dict[str, Path]]],
    workers: int,
    event_rows: dict[str, int] | None = None,
) -> list[list[tuple[DailyPairRecord, dict[str, Path], int]]]:
    """Balance pair work using the combined event rows of both legs."""
    shard_count = max(1, min(int(workers), len(runnable) or 1))
    shards: list[list[tuple[DailyPairRecord, dict[str, Path], int]]] = [
        [] for _ in range(shard_count)
    ]
    totals = [0] * shard_count
    row_counts = event_rows or {}
    weighted: list[tuple[DailyPairRecord, dict[str, Path], int]] = []
    for record, paths in runnable:
        weight = sum(max(0, int(row_counts.get(str(path), 0))) for path in paths.values())
        if weight <= 0:
            weight = sum(
                max(1, int(path.stat().st_size)) if path.exists() else 1
                for path in paths.values()
            )
        weighted.append((record, paths, weight))
    for item in sorted(weighted, key=lambda value: (-value[2], value[0].run_key)):
        index = min(range(shard_count), key=lambda shard_index: (totals[shard_index], shard_index))
        shards[index].append(item)
        totals[index] += item[2]
    return [shard for shard in shards if shard]


def _run_backtest_shard(
    args: argparse.Namespace,
    shard: list[tuple[DailyPairRecord, dict[str, Path]]],
) -> tuple[int, list[tuple[str, dict[str, pd.DataFrame] | None, str | None]]]:
    results: list[tuple[str, dict[str, pd.DataFrame] | None, str | None]] = []
    for record, paths in shard:
        try:
            result = _run_single_pair_backtest(args, record, paths)
            results.append((record.run_key, result, None))
        except Exception as exc:
            if not args.continue_on_error:
                raise RuntimeError(f"pair backtest failed run_key={record.run_key}: {exc!r}") from exc
            results.append((record.run_key, None, repr(exc)))
    return os.getpid(), results


def _run_single_pair_backtest(
    args: argparse.Namespace,
    record: DailyPairRecord,
    paths: dict[str, Path],
) -> dict[str, pd.DataFrame]:
    """Process-safe unit of work for one independent pair backtest."""
    config = build_pair_hbt_config(args, record.pair, paths, trade_date=record.trade_date)
    backtester = HbtPairBacktester(config)
    trades, summary = backtester.run()
    market = backtester.market_frame()
    latency = backtester.latency_frame()
    trades = add_run_columns(add_execution_latency_columns(with_time_columns(trades)), record)
    market = add_run_columns(attach_entry_signals(with_time_columns(market), config.pair), record)
    latency = add_run_columns(with_time_columns(latency, "local_ts"), record)
    summary = add_run_columns(summary, record)
    return {"trades": trades, "summary": summary, "market": market, "latency": latency}


def run_or_load_backtests(
    args: argparse.Namespace,
    records: list[DailyPairRecord],
    event_paths: dict[str, dict[str, Path]],
) -> tuple[dict[str, dict[str, pd.DataFrame]], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if hbt_cache_is_valid(args, records):
        logging.info("reuse existing HBT result CSVs from %s", args.output_dir)
        summary, trades, market, latency, run_errors = load_hbt_result_csvs(args.output_dir)
        summary = filter_frames_to_records(summary, records)
        trades = filter_frames_to_records(trades, records)
        market = filter_frames_to_records(market, records)
        latency = filter_frames_to_records(latency, records)
        run_errors = filter_frames_to_records(run_errors, records)
        pair_results = pair_results_from_frames(trades, summary, market, latency)
        return pair_results, summary, trades, market, latency, run_errors
    return run_backtests(args, records, event_paths)


def filter_frames_to_records(frame: pd.DataFrame, records: list[DailyPairRecord]) -> pd.DataFrame:
    if frame.empty or "run_key" not in frame.columns:
        return frame
    run_keys = {record.run_key for record in records}
    return frame.loc[frame["run_key"].astype(str).isin(run_keys)].copy()


def hbt_result_csv_paths(output_dir: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "summary_all_daily_pairs.csv",
        "trades": output_dir / "trades_all_daily_pairs.csv",
        "market": output_dir / "market_all_daily_pairs.csv",
        "latency": output_dir / "latency_all_daily_pairs.csv",
        "run_errors": output_dir / "run_errors.csv",
    }


def hbt_result_csvs_exist(output_dir: Path) -> bool:
    paths = hbt_result_csv_paths(output_dir)
    required = ("summary", "trades", "market", "latency", "run_errors")
    return all(paths[name].exists() for name in required)


HBT_CACHE_SCHEMA_VERSION = 7
HBT_MANIFEST_NAME = "backtest_manifest.json"
REFERENCE_ENGINE_VERSION = "reference-v1"
HBT_RESULT_ARG_NAMES = (
    "start_date",
    "end_date",
    "excluded_dates",
    "excluded_run_keys",
    "session_start",
    "session_end",
    "engine",
    "market_data_cache",
    "compact_cache_compression",
    "compact_cache_profile",
    "first_leg",
    "step_ms",
    "strategy_engine",
    "order_latency_ms",
    "response_latency_ms",
    "feed_latency_offset_ms",
    "spot_order_latency_ms",
    "spot_response_latency_ms",
    "spot_feed_latency_offset_ms",
    "future_order_latency_ms",
    "future_response_latency_ms",
    "future_feed_latency_offset_ms",
    "second_leg_delay_ms",
    "post_first_feed_wait",
    "post_first_feed_timeout_ms",
    "post_first_feed_poll_ms",
    "response_timeout_ms",
    "max_steps",
    "max_trades_per_pair",
    "record_market_every_steps",
    "queue_model",
    "entry_threshold_pct",
    "exit_threshold_pct",
    "min_effective_tick_multiple",
    "min_second_leg_adjusted_basis_pct",
    "no_second_leg_profit_check",
    "no_flatten",
    "carry_positions",
    "total_capital",
    "futures_margin_rate",
    "spot_equity_rate",
    "leverage",
    "workers",
    "report_mode",
    "full_report_max_rows",
    "low_memory_reports",
    "report_chunk_rows",
    "skip_entry_exit_by_pair",
    "skip_detailed_reports",
    "detailed_report_format",
    "continue_on_error",
)


def hbt_manifest_path(output_dir: Path) -> Path:
    return output_dir / HBT_MANIFEST_NAME


def hbt_manifest_payload(args: argparse.Namespace, records: list[DailyPairRecord]) -> dict[str, Any]:
    config_paths = sorted({record.config_path.resolve() for record in records}, key=str)
    if getattr(args, "market_data_cache", "event_npz") == "compact":
        event_paths = sorted(
            {
                compact_raw_source_path(args, record.trade_date, "stock").resolve()
                for record in records
            }
            | {
                compact_raw_source_path(args, record.trade_date, "stock_future").resolve()
                for record in records
            },
            key=str,
        )
    else:
        event_paths = sorted(
            {
                expected_event_path(args, record.pair.spot_symbol, "stock", record.trade_date).resolve()
                for record in records
            }
            | {
                expected_event_path(args, record.pair.future_symbol, "stock_future", record.trade_date).resolve()
                for record in records
            },
            key=str,
        )
    implementation_paths = [
        ARBITRAGE_ROOT / "hbt_backtest.py",
        ARBITRAGE_ROOT / "hbt_numba.py",
        ARBITRAGE_ROOT / "hbt_helpers.py",
        ARBITRAGE_ROOT / "strategy.py",
        ARBITRAGE_ROOT / "strategy_adapter.py",
        ARBITRAGE_ROOT / "position_carry.py",
        ROOT_SCRIPT_ROOT / "compact_cache.py",
        ROOT_SCRIPT_ROOT / "compact_hbt_adapter.py",
        ROOT_SCRIPT_ROOT / "slim_engine.py",
        WORKSPACE_ROOT / "crates" / "hbt_slim" / "src" / "lib.rs",
        Path(__file__),
    ]
    return {
        "schema_version": HBT_CACHE_SCHEMA_VERSION,
        "engine": getattr(args, "engine", "reference"),
        "engine_version": execution_engine_version(args),
        "compact_schema_version": (
            COMPACT_SCHEMA_VERSION
            if getattr(args, "market_data_cache", "event_npz") == "compact"
            else None
        ),
        "daily_result_schema_version": DAILY_RESULT_SCHEMA_VERSION,
        "strategy_clock": {
            "kind": "step_ms",
            "step_ms": _json_value(getattr(args, "step_ms", None)),
        },
        "time_in_force_semantics": HBT_TIME_IN_FORCE_SEMANTICS,
        "arguments": {name: _json_value(getattr(args, name, None)) for name in HBT_RESULT_ARG_NAMES},
        "run_keys": [record.run_key for record in records],
        "daily_configs": [_content_fingerprint(path) for path in config_paths],
        "event_files": [_stat_fingerprint(path) for path in event_paths],
        "implementation_sha256": _combined_content_sha256(implementation_paths),
    }


def execution_engine_version(args: argparse.Namespace) -> str:
    return SLIM_ENGINE_VERSION if getattr(args, "engine", "reference") == "slim" else REFERENCE_ENGINE_VERSION


def compact_raw_source_path(args: argparse.Namespace, trade_date: str, source: str) -> Path:
    if source == "stock":
        return Path(
            str(args.stock_tick_parquet_template).format(
                date=trade_date,
                date_dash=trade_date,
                date_nodash=trade_date.replace("-", ""),
            )
        )
    futures_dir = getattr(args, "event_futures_parquet_dir", None)
    return (
        Path(futures_dir) / f"{trade_date}.parquet"
        if futures_dir is not None
        else Path(f"/mnt/z/ticks_parquet_stock_future/{trade_date}.parquet")
    )


def hbt_cache_is_valid(args: argparse.Namespace, records: list[DailyPairRecord]) -> bool:
    if getattr(args, "rebuild_hbt_results", False) or not hbt_result_csvs_exist(args.output_dir):
        return False
    path = hbt_manifest_path(args.output_dir)
    if not path.exists():
        logging.info("cached CSVs have no manifest; rebuild required")
        return False
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    current = hbt_manifest_payload(args, records)
    if stored != current:
        logging.info("backtest cache manifest changed; rebuild required")
        return False
    return True


def write_hbt_manifest(args: argparse.Namespace, records: list[DailyPairRecord]) -> Path:
    path = hbt_manifest_path(args.output_dir)
    path.write_text(
        json.dumps(hbt_manifest_payload(args, records), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _json_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _stat_fingerprint(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
        return {"path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    except FileNotFoundError:
        return {"path": str(path), "missing": True}


def _content_fingerprint(path: Path) -> dict[str, Any]:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return {"path": str(path), "missing": True}
    return {"path": str(path), "sha256": hashlib.sha256(data).hexdigest()}


def _combined_content_sha256(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path.name).encode())
        try:
            digest.update(path.read_bytes())
        except FileNotFoundError:
            digest.update(b"<missing>")
    return digest.hexdigest()


def load_hbt_result_csvs(output_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    paths = hbt_result_csv_paths(output_dir)
    return (
        read_csv_if_exists(paths["summary"]),
        read_csv_if_exists(paths["trades"]),
        read_csv_if_exists(paths["market"]),
        read_csv_if_exists(paths["latency"]),
        read_csv_if_exists(paths["run_errors"]),
    )


def pair_results_from_frames(
    trades: pd.DataFrame,
    summary: pd.DataFrame,
    market: pd.DataFrame,
    latency: pd.DataFrame,
) -> dict[str, dict[str, pd.DataFrame]]:
    run_keys: set[str] = set()
    for frame in (trades, summary, market, latency):
        if not frame.empty and "run_key" in frame.columns:
            run_keys.update(frame["run_key"].dropna().astype(str).unique())
    return {
        run_key: {
            "trades": frame_for_run_key(trades, run_key),
            "summary": frame_for_run_key(summary, run_key),
            "market": frame_for_run_key(market, run_key),
            "latency": frame_for_run_key(latency, run_key),
        }
        for run_key in sorted(run_keys)
    }


def frame_for_run_key(frame: pd.DataFrame, run_key: str) -> pd.DataFrame:
    if frame.empty or "run_key" not in frame.columns:
        return pd.DataFrame()
    return frame.loc[frame["run_key"].astype(str).eq(run_key)].copy()


def build_pair_hbt_config(
    args: argparse.Namespace,
    pair: PairConfig,
    paths: dict[str, Path],
    trade_date: str | None = None,
) -> HbtPairBacktestConfig:
    pair = pair_with_overrides(args, pair)
    tick_sizes = getattr(args, "hbt_tick_sizes", {})
    spot_order_ms, spot_response_ms, spot_feed_ms = leg_latency_ms(args, "spot")
    future_order_ms, future_response_ms, future_feed_ms = leg_latency_ms(args, "future")
    return HbtPairBacktestConfig(
        pair=pair,
        spot=HbtAssetConfig(
            symbol=pair.spot_symbol,
            data=paths["spot"],
            instrument="stock",
            trade_date=trade_date,
            tick_size=pair.spot_tick_size or tick_sizes.get(str(paths["spot"])),
            contract_size=1000.0,
            order_entry_latency_ns=ms_to_ns(spot_order_ms),
            order_response_latency_ns=ms_to_ns(spot_response_ms),
            feed_latency_offset_ns=ms_to_ns(spot_feed_ms),
            queue_model=args.queue_model,
        ),
        future=HbtAssetConfig(
            symbol=pair.future_symbol,
            data=paths["future"],
            instrument="future",
            trade_date=trade_date,
            tick_size=pair.future_tick_size or tick_sizes.get(str(paths["future"])),
            contract_size=float(pair.future_pnl_multiplier),
            order_entry_latency_ns=ms_to_ns(future_order_ms),
            order_response_latency_ns=ms_to_ns(future_response_ms),
            feed_latency_offset_ns=ms_to_ns(future_feed_ms),
            queue_model=args.queue_model,
        ),
        first_leg=args.first_leg,
        step_ns=ms_to_ns(args.step_ms),
        response_timeout_ns=ms_to_ns(args.response_timeout_ms),
        second_leg_delay_ns=ms_to_ns(args.second_leg_delay_ms),
        post_first_feed_wait=getattr(args, "post_first_feed_wait", "none"),
        post_first_feed_timeout_ns=ms_to_ns(getattr(args, "post_first_feed_timeout_ms", 0.0)),
        post_first_feed_poll_ns=ms_to_ns(getattr(args, "post_first_feed_poll_ms", 10.0)),
        max_steps=args.max_steps,
        max_trades=args.max_trades_per_pair,
        flatten_on_second_leg_failure=not args.no_flatten,
        second_leg_profit_check=not args.no_second_leg_profit_check,
        record_market_every_steps=None if args.record_market_every_steps <= 0 else args.record_market_every_steps,
        strategy_engine=(
            "python" if getattr(args, "engine", "reference") == "slim"
            else getattr(args, "strategy_engine", "numba")
        ),
        execution_engine=getattr(args, "engine", "reference"),
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
    if {"entry_signal", "entry_signal_hit"}.issubset(result.columns):
        return result
    long_ok = (
        result["long_spot_short_future_pct"].ge(pair.entry_threshold_pct)
        & result["long_spot_short_future_ticks"].gt(pair.min_effective_tick_multiple)
        & result["spot_ask_size"].ge(pair.stock_min_ask_size)
        & result["future_bid_size"].ge(pair.future_min_bid_size)
    )
    if pair.allow_short_spot:
        short_ok = (
            result["short_spot_long_future_pct"].le(-pair.entry_threshold_pct)
            & result["short_spot_long_future_ticks"].gt(pair.min_effective_tick_multiple)
            & result["spot_bid_size"].ge(pair.stock_min_bid_size)
            & result["future_ask_size"].ge(pair.future_min_ask_size)
        )
    else:
        short_ok = pd.Series(False, index=result.index)
    result["entry_signal"] = np.select(
        [long_ok, short_ok],
        [Signal.ENTER_LONG_SPOT_SHORT_FUTURE.value, Signal.ENTER_SHORT_SPOT_LONG_FUTURE.value],
        default=Signal.HOLD.value,
    )
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


SECOND_LEG_FAILURE_STATUSES = frozenset(
    {
        "SECOND_LEG_UNFILLED",
        "SECOND_LEG_PROFIT_CHECK_FAILED",
        "POST_FIRST_FEED_TIMEOUT",
    }
)


def build_second_leg_failure_outputs(
    summary: pd.DataFrame,
    trades: pd.DataFrame,
    market: pd.DataFrame,
    window_rows: int = 10,
    include_windows: bool = True,
) -> dict[str, pd.DataFrame]:
    failed_pairs = second_leg_failed_pairs(summary)
    failed_trades = second_leg_failed_trades(trades)
    failure_by_date = second_leg_failure_by_date(failed_pairs, failed_trades)
    failure_by_status = second_leg_failure_by_status(failed_trades)
    failure_by_pair = second_leg_failure_by_pair(failed_pairs, failed_trades)
    failure_trade_windows = (
        second_leg_failure_trade_windows(trades, failed_trades, window_rows=window_rows)
        if include_windows
        else pd.DataFrame()
    )
    failure_market_tick_windows = (
        second_leg_failure_market_tick_windows(market, failed_trades, window_rows=window_rows)
        if include_windows
        else pd.DataFrame()
    )
    return {
        "failure_overview": second_leg_failure_overview(failed_pairs, failed_trades, failure_market_tick_windows),
        "failed_pairs": failed_pairs,
        "failed_trades": failed_trades,
        "failure_by_date": failure_by_date,
        "failure_by_status": failure_by_status,
        "failure_by_pair": failure_by_pair,
        "failure_trade_windows": failure_trade_windows,
        "failure_market_tick_windows": failure_market_tick_windows,
    }


def second_leg_failed_pairs(summary: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "trade_date",
        "run_key",
        "pair_name",
        "spot_symbol",
        "future_symbol",
        "second_leg_failures",
        "flatten_count",
        "post_first_feed_wait",
        "post_first_feed_timeout_ns",
    ]
    if summary.empty or "second_leg_failures" not in summary.columns:
        return pd.DataFrame(columns=cols)
    failed = summary.loc[pd.to_numeric(summary["second_leg_failures"], errors="coerce").fillna(0).gt(0)]
    return sort_by_existing(
        failed[existing_columns(failed, cols)].drop_duplicates(),
        ["trade_date", "pair_name"],
    )


def second_leg_failed_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty or "status" not in trades.columns:
        return pd.DataFrame()
    failed = trades.loc[trades["status"].isin(SECOND_LEG_FAILURE_STATUSES)].copy()
    if failed.empty:
        return failed
    if "timestamp" in failed.columns:
        failed["failure_time"] = pd.to_datetime(failed["timestamp"], unit="ns", utc=True, errors="coerce").dt.tz_convert("Asia/Taipei")
    return sort_by_existing(failed, ["trade_date", "pair_name", "timestamp"]).reset_index(drop=True)


def second_leg_failure_overview(
    failed_pairs: pd.DataFrame,
    failed_trades: pd.DataFrame,
    failure_market_tick_windows: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "pairs_with_summary_failures": len(failed_pairs),
                "failure_trade_rows": len(failed_trades),
                "market_window_rows": len(failure_market_tick_windows),
                "trade_dates": nunique_if_present(failed_pairs, "trade_date"),
                "pair_names": nunique_if_present(failed_pairs, "pair_name"),
                "flatten_count": numeric_sum_if_present(failed_pairs, "flatten_count"),
            }
        ]
    )


def second_leg_failure_by_date(failed_pairs: pd.DataFrame, failed_trades: pd.DataFrame) -> pd.DataFrame:
    cols = ["trade_date", "summary_pairs", "summary_second_leg_failures", "failure_trade_rows", "flatten_count"]
    if failed_pairs.empty and (failed_trades.empty or "trade_date" not in failed_trades.columns):
        return pd.DataFrame(columns=cols)

    summary_by_date = (
        failed_pairs.groupby("trade_date", as_index=False)
        .agg(
            summary_pairs=("pair_name", "nunique") if "pair_name" in failed_pairs.columns else ("trade_date", "size"),
            summary_second_leg_failures=("second_leg_failures", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum())
            if "second_leg_failures" in failed_pairs.columns
            else ("trade_date", "size"),
            flatten_count=("flatten_count", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum())
            if "flatten_count" in failed_pairs.columns
            else ("trade_date", "size"),
        )
        if not failed_pairs.empty and "trade_date" in failed_pairs.columns
        else pd.DataFrame(columns=["trade_date", "summary_pairs", "summary_second_leg_failures", "flatten_count"])
    )
    trades_by_date = (
        failed_trades.groupby("trade_date", as_index=False).agg(failure_trade_rows=("status", "size"))
        if not failed_trades.empty and "trade_date" in failed_trades.columns
        else pd.DataFrame(columns=["trade_date", "failure_trade_rows"])
    )
    result = summary_by_date.merge(trades_by_date, on="trade_date", how="outer").fillna(
        {
            "summary_pairs": 0,
            "summary_second_leg_failures": 0,
            "failure_trade_rows": 0,
            "flatten_count": 0,
        }
    )
    return result.sort_values("trade_date").reset_index(drop=True)


def second_leg_failure_by_status(failed_trades: pd.DataFrame) -> pd.DataFrame:
    cols = ["trade_date", "status", "failure_reason", "rows", "pair_names"]
    if failed_trades.empty or "status" not in failed_trades.columns:
        return pd.DataFrame(columns=cols)
    group_cols = existing_columns(failed_trades, ["trade_date", "status", "failure_reason"])
    result = (
        failed_trades.groupby(group_cols, dropna=False)
        .agg(
            rows=("status", "size"),
            pair_names=("pair_name", "nunique") if "pair_name" in failed_trades.columns else ("status", "size"),
        )
        .reset_index()
    )
    if "trade_date" in result.columns and "rows" in result.columns:
        return result.sort_values(["trade_date", "rows"], ascending=[True, False]).reset_index(drop=True)
    return sort_by_existing(result, ["rows"]).reset_index(drop=True)


def second_leg_failure_by_pair(failed_pairs: pd.DataFrame, failed_trades: pd.DataFrame) -> pd.DataFrame:
    if not failed_trades.empty and "pair_name" in failed_trades.columns:
        group_cols = existing_columns(failed_trades, ["trade_date", "run_key", "pair_name", "spot_symbol", "future_symbol", "status", "failure_reason"])
        by_pair = (
            failed_trades.groupby(group_cols, dropna=False)
            .agg(
                failure_trade_rows=("status", "size"),
                first_failure_ts=("timestamp", "min") if "timestamp" in failed_trades.columns else ("status", "size"),
                last_failure_ts=("timestamp", "max") if "timestamp" in failed_trades.columns else ("status", "size"),
            )
            .reset_index()
        )
    else:
        by_pair = pd.DataFrame()

    if by_pair.empty:
        return failed_pairs.copy()

    if not failed_pairs.empty and "run_key" in failed_pairs.columns:
        summary_cols = existing_columns(failed_pairs, ["run_key", "second_leg_failures", "flatten_count", "post_first_feed_wait", "post_first_feed_timeout_ns"])
        by_pair = by_pair.merge(failed_pairs[summary_cols].drop_duplicates("run_key"), on="run_key", how="left")
    return sort_by_existing(by_pair, ["trade_date", "pair_name", "status"]).reset_index(drop=True)


def second_leg_failure_trade_windows(
    trades: pd.DataFrame,
    failed_trades: pd.DataFrame,
    window_rows: int = 10,
) -> pd.DataFrame:
    if trades.empty or failed_trades.empty or "run_key" not in trades.columns or "run_key" not in failed_trades.columns:
        return pd.DataFrame()
    cols = existing_columns(
        trades,
        [
            "time",
            "timestamp",
            "step",
            "signal",
            "status",
            "failure_reason",
            "spot_bid",
            "spot_ask",
            "spot_bid_size",
            "spot_ask_size",
            "future_bid",
            "future_ask",
            "future_bid_size",
            "future_ask_size",
            "long_spot_short_future_pct",
            "long_spot_short_future_ticks",
            "first_leg",
            "first_side",
            "first_exec_price",
            "first_exec_qty",
            "second_leg",
            "second_side",
            "second_exec_price",
            "second_exec_qty",
            "flatten_leg",
            "flatten_side",
            "flatten_exec_price",
            "flatten_exec_qty",
            "position_quantity",
        ],
    )
    windows = []
    for failure_no, (_, failure) in enumerate(failed_trades.iterrows(), start=1):
        pair_trades = sort_by_existing(
            trades.loc[trades["run_key"].eq(failure["run_key"])],
            ["timestamp"],
        ).reset_index(drop=True)
        pos = matching_position(pair_trades, failure)
        if pos is None:
            continue
        start = max(0, pos - window_rows)
        end = min(len(pair_trades), pos + window_rows + 1)
        window = pair_trades.iloc[start:end][cols].copy()
        window.insert(0, "failure_no", failure_no)
        window.insert(1, "failure_label", failure_label(failure_no, failure))
        window.insert(2, "trade_date", failure.get("trade_date", ""))
        window.insert(3, "run_key", failure.get("run_key", ""))
        window.insert(4, "pair_name", failure.get("pair_name", ""))
        window.insert(5, "relative_row", range(start - pos, end - pos))
        windows.append(window)
    return concat_frames(windows)


def second_leg_failure_market_tick_windows(
    market: pd.DataFrame,
    failed_trades: pd.DataFrame,
    window_rows: int = 10,
) -> pd.DataFrame:
    if market.empty or failed_trades.empty or "run_key" not in market.columns or "run_key" not in failed_trades.columns or "timestamp" not in market.columns:
        return pd.DataFrame()
    cols = existing_columns(
        market,
        [
            "time",
            "timestamp",
            "step",
            "signal",
            "status",
            "spot_bid",
            "spot_ask",
            "spot_bid_size",
            "spot_ask_size",
            "future_bid",
            "future_ask",
            "future_bid_size",
            "future_ask_size",
            "long_spot_short_future_pct",
            "long_spot_short_future_ticks",
            "short_spot_long_future_pct",
            "short_spot_long_future_ticks",
            "position_quantity",
        ],
    )
    windows = []
    for failure_no, (_, failure) in enumerate(failed_trades.iterrows(), start=1):
        failure_ts = numeric_value(failure.get("timestamp"))
        if pd.isna(failure_ts):
            continue
        pair_market = market.loc[market["run_key"].eq(failure["run_key"])].sort_values("timestamp").reset_index(drop=True)
        if pair_market.empty:
            continue
        market_ts = pd.to_numeric(pair_market["timestamp"], errors="coerce")
        if market_ts.notna().sum() == 0:
            continue
        nearest_pos = int((market_ts - int(failure_ts)).abs().idxmin())
        start = max(0, nearest_pos - window_rows)
        end = min(len(pair_market), nearest_pos + window_rows + 1)
        window = pair_market.iloc[start:end][cols].copy()
        window.insert(0, "failure_no", failure_no)
        window.insert(1, "failure_label", failure_label(failure_no, failure))
        window.insert(2, "trade_date", failure.get("trade_date", ""))
        window.insert(3, "run_key", failure.get("run_key", ""))
        window.insert(4, "pair_name", failure.get("pair_name", ""))
        window.insert(5, "relative_tick", range(start - nearest_pos, end - nearest_pos))
        window.insert(6, "failure_timestamp", int(failure_ts))
        window.insert(7, "failure_status", failure.get("status", ""))
        window.insert(8, "failure_reason", failure.get("failure_reason", ""))
        windows.append(window)
    return concat_frames(windows)


def existing_columns(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column in frame.columns]


def sort_by_existing(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    by = existing_columns(frame, columns)
    if not by:
        return frame
    return frame.sort_values(by)


def numeric_value(value: Any) -> float:
    return pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]


def numeric_sum_if_present(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum())


def nunique_if_present(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].nunique(dropna=True))


def matching_position(pair_trades: pd.DataFrame, failure: pd.Series) -> int | None:
    if pair_trades.empty or "timestamp" not in pair_trades.columns:
        return None
    matches = pair_trades.index[pair_trades["timestamp"].eq(failure.get("timestamp"))]
    if len(matches):
        return int(matches[0])
    failure_ts = numeric_value(failure.get("timestamp"))
    if pd.isna(failure_ts):
        return None
    trade_ts = pd.to_numeric(pair_trades["timestamp"], errors="coerce")
    if trade_ts.notna().sum() == 0:
        return None
    return int((trade_ts - int(failure_ts)).abs().idxmin())


def failure_label(failure_no: int, failure: pd.Series) -> str:
    return (
        f"{failure_no}: {failure.get('trade_date', '')} {failure.get('pair_name', '')} "
        f"{failure.get('status', '')}"
    ).strip()


def build_cash_roi_outputs(
    trades: pd.DataFrame,
    market: pd.DataFrame,
    records: list[DailyPairRecord],
    capital_config: CapitalAllocationConfig | None = None,
    include_capital_details: bool = True,
    excluded_run_keys: Iterable[str] = (),
) -> dict[str, pd.DataFrame]:
    excluded = {str(value) for value in excluded_run_keys}
    if excluded:
        if not trades.empty and "run_key" in trades.columns:
            trades = trades.loc[~trades["run_key"].astype(str).isin(excluded)].copy()
        if not market.empty and "run_key" in market.columns:
            market = market.loc[~market["run_key"].astype(str).isin(excluded)].copy()
        records = [record for record in records if record.run_key not in excluded]
    pair_cash_settings = pair_cash_settings_frame(records)
    filled_trades = filled_trade_frame(trades, pair_cash_settings)
    stuck_outputs = build_stuck_cash_outputs(filled_trades)
    roi_outputs = build_locked_roi_outputs(
        filled_trades,
        market,
        stuck_outputs["stuck_cash_by_pair"],
    )
    capital_outputs = build_capital_constraint_outputs(
        filled_trades,
        capital_config,
        include_details=include_capital_details,
        excluded_run_keys=excluded,
    )
    return {
        "pair_cash_settings": pair_cash_settings,
        "filled_trades": filled_trades,
        **stuck_outputs,
        **roi_outputs,
        **capital_outputs,
    }


def pair_cash_settings_frame(records: list[DailyPairRecord]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "run_key": record.run_key,
                "spot_order_qty": record.pair.spot_order_qty,
                "future_order_qty": record.pair.future_order_qty,
                "future_pnl_multiplier": record.pair.future_pnl_multiplier,
                "stock_commission_rate": record.pair.stock_commission_rate,
                "stock_commission_discount": record.pair.stock_commission_discount,
                "stock_transaction_tax_rate": record.pair.stock_transaction_tax_rate,
            }
            for record in records
        ]
    )


def filled_trade_frame(trades: pd.DataFrame, pair_cash_settings: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    filled = trades.loc[trades["status"].eq("FILLED")].copy()
    if filled.empty:
        return pd.DataFrame()
    for column in ("first_exec_price", "second_exec_price", "realized_pnl"):
        if column in filled.columns:
            filled[column] = pd.to_numeric(filled[column], errors="coerce")
    filled["spot_exec_price"] = filled["second_exec_price"].where(
        filled["second_leg"].eq("stock"),
        filled["first_exec_price"],
    )
    filled["spot_side"] = filled["second_side"].where(
        filled["second_leg"].eq("stock"),
        filled["first_side"],
    )
    filled["future_exec_price"] = filled["first_exec_price"].where(
        filled["first_leg"].eq("future"),
        filled["second_exec_price"],
    )
    return filled.merge(pair_cash_settings, on="run_key", how="left")


def build_stuck_cash_outputs(filled_trades: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if filled_trades.empty:
        empty_pair = empty_stuck_cash_by_pair()
        return {
            "stuck_cash_summary": empty_stuck_cash_summary(),
            "daily_stuck_cash": empty_daily_stuck_cash(),
            "stuck_cash_by_pair": empty_pair,
            "top_stuck_cash_pairs": empty_pair,
        }

    cash_trades = filled_trades.copy()
    cash_trades["stock_notional"] = cash_trades["spot_exec_price"] * cash_trades["spot_order_qty"]
    cash_trades["commission_rate"] = cash_trades["stock_commission_rate"] * cash_trades["stock_commission_discount"]
    cash_trades["stock_fee"] = cash_trades["stock_notional"] * cash_trades["commission_rate"]
    cash_trades["stock_tax"] = cash_trades["stock_notional"] * cash_trades["stock_transaction_tax_rate"]

    entry_cash = cash_trades.loc[cash_trades["signal"].eq(Signal.ENTER_LONG_SPOT_SHORT_FUTURE.value)].copy()
    exit_cash = cash_trades.loc[cash_trades["signal"].eq(Signal.EXIT.value)].copy()
    if entry_cash.empty:
        empty_pair = empty_stuck_cash_by_pair()
        return {
            "stuck_cash_summary": empty_stuck_cash_summary(),
            "daily_stuck_cash": empty_daily_stuck_cash(),
            "stuck_cash_by_pair": empty_pair,
            "top_stuck_cash_pairs": empty_pair,
        }

    entry_cash["entry_cash_out"] = entry_cash["stock_notional"] + entry_cash["stock_fee"]
    if not exit_cash.empty:
        exit_cash["exit_cash_in"] = exit_cash["stock_notional"] - exit_cash["stock_fee"] - exit_cash["stock_tax"]

    entry_by_pair = entry_cash.groupby(
        ["trade_date", "run_key", "pair_name", "spot_symbol", "future_symbol"],
        as_index=False,
    ).agg(
        entries=("signal", "size"),
        entry_cash_out=("entry_cash_out", "sum"),
        entry_stock_notional=("stock_notional", "sum"),
        avg_entry_spot=("spot_exec_price", "mean"),
    )
    exit_by_pair = (
        exit_cash.groupby(["trade_date", "run_key", "pair_name"], as_index=False)
        .agg(
            exits=("signal", "size"),
            exit_cash_in=("exit_cash_in", "sum"),
            exit_stock_notional=("stock_notional", "sum"),
            avg_exit_spot=("spot_exec_price", "mean"),
        )
        if not exit_cash.empty
        else pd.DataFrame(columns=["trade_date", "run_key", "pair_name", "exits", "exit_cash_in", "exit_stock_notional", "avg_exit_spot"])
    )

    stuck_cash_by_pair = entry_by_pair.merge(
        exit_by_pair,
        on=["trade_date", "run_key", "pair_name"],
        how="left",
    ).fillna({"exits": 0, "exit_cash_in": 0.0, "exit_stock_notional": 0.0})
    stuck_cash_by_pair["open_pairs"] = stuck_cash_by_pair["entries"] - stuck_cash_by_pair["exits"]
    stuck_cash_by_pair["net_stock_cash_stuck"] = (
        stuck_cash_by_pair["entry_cash_out"] - stuck_cash_by_pair["exit_cash_in"]
    )

    daily_stuck_cash = (
        stuck_cash_by_pair.groupby("trade_date", as_index=False)
        .agg(
            entries=("entries", "sum"),
            exits=("exits", "sum"),
            open_pairs=("open_pairs", "sum"),
            entry_cash_out=("entry_cash_out", "sum"),
            exit_cash_in=("exit_cash_in", "sum"),
            net_stock_cash_stuck=("net_stock_cash_stuck", "sum"),
        )
    )
    stuck_cash_summary = pd.DataFrame(
        [
            {
                "entry_rows": len(entry_cash),
                "exit_rows": len(exit_cash),
                "open_pairs": stuck_cash_by_pair["open_pairs"].sum(),
                "entry_cash_out": entry_cash["entry_cash_out"].sum(),
                "exit_cash_in": exit_cash["exit_cash_in"].sum() if "exit_cash_in" in exit_cash else 0.0,
                "net_stock_cash_stuck": stuck_cash_by_pair["net_stock_cash_stuck"].sum(),
            }
        ]
    )
    top_stuck_cash_pairs = stuck_cash_by_pair.sort_values("net_stock_cash_stuck", ascending=False)[
        [
            "trade_date",
            "pair_name",
            "spot_symbol",
            "future_symbol",
            "entries",
            "exits",
            "open_pairs",
            "entry_cash_out",
            "exit_cash_in",
            "net_stock_cash_stuck",
            "avg_entry_spot",
            "avg_exit_spot",
        ]
    ]
    return {
        "stuck_cash_summary": stuck_cash_summary,
        "daily_stuck_cash": daily_stuck_cash,
        "stuck_cash_by_pair": stuck_cash_by_pair,
        "top_stuck_cash_pairs": top_stuck_cash_pairs,
    }


def build_locked_roi_outputs(
    filled_trades: pd.DataFrame,
    market: pd.DataFrame,
    stuck_cash_by_pair: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    if filled_trades.empty or stuck_cash_by_pair.empty:
        empty_pair = empty_pair_roi_including_open()
        return {
            "roi_summary_including_open": empty_roi_summary(),
            "daily_roi_including_open": empty_daily_roi(),
            "roi_by_pair": empty_pair,
            "pair_roi_including_open": empty_pair,
            "open_lots": pd.DataFrame(),
        }

    latest_market = latest_market_frame(market)
    open_lots = open_entry_lots(filled_trades)
    open_pnl_by_pair = open_locked_pnl_by_pair(open_lots, latest_market)
    realized_by_pair = (
        filled_trades.loc[filled_trades["signal"].eq(Signal.EXIT.value)]
        .groupby(["trade_date", "run_key", "pair_name"], as_index=False)
        .agg(realized_pnl=("realized_pnl", "sum"))
    )

    roi_by_pair = (
        stuck_cash_by_pair.merge(
            open_pnl_by_pair,
            on=["trade_date", "run_key", "pair_name", "spot_symbol", "future_symbol"],
            how="left",
        )
        .merge(realized_by_pair, on=["trade_date", "run_key", "pair_name"], how="left")
    )
    for column in ("realized_pnl", "open_pairs", "open_locked_pnl"):
        roi_by_pair[column] = roi_by_pair[column].fillna(0.0)
    roi_by_pair["total_pnl_including_open"] = roi_by_pair["realized_pnl"] + roi_by_pair["open_locked_pnl"]
    roi_by_pair["total_roi_on_entry_cash_pct"] = roi_by_pair["total_pnl_including_open"] / roi_by_pair["entry_cash_out"]
    roi_by_pair["open_roi_on_stuck_cash_pct"] = roi_by_pair["open_locked_pnl"] / roi_by_pair[
        "net_stock_cash_stuck"
    ].where(roi_by_pair["net_stock_cash_stuck"].gt(0) & roi_by_pair["open_pairs"].gt(0))

    daily_roi_including_open = (
        roi_by_pair.groupby("trade_date", as_index=False)
        .agg(
            entry_cash_out=("entry_cash_out", "sum"),
            net_stock_cash_stuck=("net_stock_cash_stuck", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            open_locked_pnl=("open_locked_pnl", "sum"),
            total_pnl_including_open=("total_pnl_including_open", "sum"),
            open_pairs=("open_pairs", "sum"),
        )
    )
    daily_roi_including_open["total_roi_on_entry_cash_pct"] = (
        daily_roi_including_open["total_pnl_including_open"] / daily_roi_including_open["entry_cash_out"]
    )
    daily_roi_including_open["open_roi_on_stuck_cash_pct"] = daily_roi_including_open["open_locked_pnl"] / daily_roi_including_open[
        "net_stock_cash_stuck"
    ].where(daily_roi_including_open["net_stock_cash_stuck"].gt(0))

    roi_summary_including_open = pd.DataFrame(
        [
            {
                "entry_cash_out": roi_by_pair["entry_cash_out"].sum(),
                "net_stock_cash_stuck": roi_by_pair["net_stock_cash_stuck"].sum(),
                "realized_pnl": roi_by_pair["realized_pnl"].sum(),
                "open_locked_pnl": roi_by_pair["open_locked_pnl"].sum(),
                "total_pnl_including_open": roi_by_pair["total_pnl_including_open"].sum(),
                "total_roi_on_entry_cash_pct": safe_divide(
                    roi_by_pair["total_pnl_including_open"].sum(),
                    roi_by_pair["entry_cash_out"].sum(),
                ),
                "open_roi_on_stuck_cash_pct": safe_divide(
                    roi_by_pair["open_locked_pnl"].sum(),
                    roi_by_pair["net_stock_cash_stuck"].sum(),
                ),
                "open_pairs": roi_by_pair["open_pairs"].sum(),
            }
        ]
    )
    pair_roi_including_open = roi_by_pair[
        [
            "trade_date",
            "pair_name",
            "spot_symbol",
            "future_symbol",
            "entries",
            "exits",
            "open_pairs",
            "entry_cash_out",
            "net_stock_cash_stuck",
            "realized_pnl",
            "open_locked_pnl",
            "total_pnl_including_open",
            "total_roi_on_entry_cash_pct",
            "open_roi_on_stuck_cash_pct",
            "avg_entry_spot",
            "avg_open_entry_future",
            "mark_spot_bid",
            "mark_future_ask",
        ]
    ].sort_values("total_pnl_including_open", ascending=False)
    return {
        "roi_summary_including_open": roi_summary_including_open,
        "daily_roi_including_open": daily_roi_including_open,
        "roi_by_pair": roi_by_pair,
        "pair_roi_including_open": pair_roi_including_open,
        "open_lots": open_lots,
    }


def latest_market_frame(market: pd.DataFrame) -> pd.DataFrame:
    if market.empty:
        return pd.DataFrame(columns=["run_key", "mark_timestamp", "spot_bid", "spot_ask", "future_bid", "future_ask"])
    return (
        market.sort_values(["run_key", "timestamp"])
        .groupby("run_key", as_index=False)
        .tail(1)[["run_key", "timestamp", "spot_bid", "spot_ask", "future_bid", "future_ask"]]
        .rename(columns={"timestamp": "mark_timestamp"})
    )


def open_entry_lots(filled_trades: pd.DataFrame) -> pd.DataFrame:
    open_lots: list[dict[str, Any]] = []
    sort_cols = [col for col in ("run_key", "timestamp", "completion_timestamp") if col in filled_trades.columns]
    sorted_trades = filled_trades.sort_values(sort_cols) if sort_cols else filled_trades
    for _, run_trades in sorted_trades.groupby("run_key", sort=False):
        remaining_lots: list[dict[str, Any]] = []
        for row in run_trades.itertuples(index=False):
            if row.signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE.value:
                remaining_lots.append(
                    {
                        "trade_date": row.trade_date,
                        "run_key": row.run_key,
                        "pair_name": row.pair_name,
                        "spot_symbol": row.spot_symbol,
                        "future_symbol": row.future_symbol,
                        "entry_spot_price": row.spot_exec_price,
                        "entry_future_price": row.future_exec_price,
                        "spot_order_qty": row.spot_order_qty,
                        "future_order_qty": row.future_order_qty,
                        "future_pnl_multiplier": row.future_pnl_multiplier,
                        "stock_commission_rate": row.stock_commission_rate,
                        "stock_commission_discount": row.stock_commission_discount,
                        "stock_transaction_tax_rate": row.stock_transaction_tax_rate,
                    }
                )
            elif row.signal == Signal.EXIT.value and remaining_lots:
                remaining_lots.pop(0)
        open_lots.extend(remaining_lots)
    return pd.DataFrame(open_lots)


def open_locked_pnl_by_pair(open_lots: pd.DataFrame, latest_market: pd.DataFrame) -> pd.DataFrame:
    if open_lots.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "run_key",
                "pair_name",
                "spot_symbol",
                "future_symbol",
                "open_lot_count",
                "open_locked_pnl",
                "avg_open_entry_spot",
                "avg_open_entry_future",
                "mark_spot_bid",
                "mark_future_ask",
            ]
        )
    lots = open_lots.merge(latest_market, on="run_key", how="left")
    lots["commission_rate"] = lots["stock_commission_rate"] * lots["stock_commission_discount"]
    lots["future_multiplier"] = lots["future_pnl_multiplier"] * lots["future_order_qty"]
    lots["convergence_price"] = lots["spot_bid"]
    lots["gross_locked_pnl"] = (
        lots["entry_future_price"] * lots["future_multiplier"]
        - lots["entry_spot_price"] * lots["spot_order_qty"]
        + lots["convergence_price"] * (lots["spot_order_qty"] - lots["future_multiplier"])
    )
    lots["entry_stock_fee"] = lots["entry_spot_price"] * lots["spot_order_qty"] * lots["commission_rate"]
    lots["estimated_exit_stock_fee"] = lots["convergence_price"] * lots["spot_order_qty"] * lots["commission_rate"]
    lots["estimated_exit_stock_tax"] = lots["convergence_price"] * lots["spot_order_qty"] * lots["stock_transaction_tax_rate"]
    lots["open_locked_pnl"] = (
        lots["gross_locked_pnl"]
        - lots["entry_stock_fee"]
        - lots["estimated_exit_stock_fee"]
        - lots["estimated_exit_stock_tax"]
    )
    return lots.groupby(
        ["trade_date", "run_key", "pair_name", "spot_symbol", "future_symbol"],
        as_index=False,
    ).agg(
        open_lot_count=("open_locked_pnl", "size"),
        open_locked_pnl=("open_locked_pnl", "sum"),
        avg_open_entry_spot=("entry_spot_price", "mean"),
        avg_open_entry_future=("entry_future_price", "mean"),
        mark_spot_bid=("spot_bid", "last"),
        mark_future_ask=("future_ask", "last"),
    )


def empty_stuck_cash_summary() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["entry_rows", "exit_rows", "open_pairs", "entry_cash_out", "exit_cash_in", "net_stock_cash_stuck"]
    )


def empty_daily_stuck_cash() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["trade_date", "entries", "exits", "open_pairs", "entry_cash_out", "exit_cash_in", "net_stock_cash_stuck"]
    )


def empty_stuck_cash_by_pair() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "run_key",
            "pair_name",
            "spot_symbol",
            "future_symbol",
            "entries",
            "entry_cash_out",
            "entry_stock_notional",
            "avg_entry_spot",
            "exits",
            "exit_cash_in",
            "exit_stock_notional",
            "avg_exit_spot",
            "open_pairs",
            "net_stock_cash_stuck",
        ]
    )


def empty_roi_summary() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "entry_cash_out",
            "net_stock_cash_stuck",
            "realized_pnl",
            "open_locked_pnl",
            "total_pnl_including_open",
            "total_roi_on_entry_cash_pct",
            "open_roi_on_stuck_cash_pct",
            "open_pairs",
        ]
    )


def empty_daily_roi() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "entry_cash_out",
            "net_stock_cash_stuck",
            "realized_pnl",
            "open_locked_pnl",
            "total_pnl_including_open",
            "open_pairs",
            "total_roi_on_entry_cash_pct",
            "open_roi_on_stuck_cash_pct",
        ]
    )


def empty_pair_roi_including_open() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "trade_date",
            "pair_name",
            "spot_symbol",
            "future_symbol",
            "entries",
            "exits",
            "open_pairs",
            "entry_cash_out",
            "net_stock_cash_stuck",
            "realized_pnl",
            "open_locked_pnl",
            "total_pnl_including_open",
            "total_roi_on_entry_cash_pct",
            "open_roi_on_stuck_cash_pct",
            "avg_entry_spot",
            "avg_open_entry_future",
            "mark_spot_bid",
            "mark_future_ask",
        ]
    )


def safe_divide(numerator: float, denominator: float) -> float:
    return float("nan") if denominator == 0 else numerator / denominator


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


def write_entry_exit_by_pair(frames: dict[str, pd.DataFrame], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for run_key, frame in frames.items():
        write_csv(frame, output_dir / f"{safe_filename(run_key)}.csv")


if __name__ == "__main__":
    raise SystemExit(main())
