from __future__ import annotations

import argparse
import contextlib
import gc
import glob
import logging
import re
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage.app import run  # noqa: E402
from arbitrage.config import build_initial_position, load_config, parse_historical_config  # noqa: E402
from arbitrage.models import Mode  # noqa: E402
from arbitrage.providers import HistoricalParquetReplayProvider, HistoricalReplayEventCache  # noqa: E402
from scripts.build_arbitrage_config_from_date import (  # noqa: E402
    extract_pair_defaults,
    filter_front_month_targets,
    front_month_only_enabled,
    get_ldate,
    read_json,
    read_stockinfo_frame,
)
from scripts.run_multi_day_backtest import (  # noqa: E402
    apply_initial_positions,
    build_carry_state,
    build_daily_summary_row,
    build_daily_targets,
    merge_carry_pairs,
    positions_to_rows,
    select_trade_dates,
    trade_to_row,
    write_daily_config,
    write_report,
)


@dataclass
class BacktestState:
    base_config_path: Path
    output_dir: Path
    base: dict[str, Any]
    pair_defaults: dict[str, Any]
    config_dir: Path
    target_dir: Path
    event_log_dir: Path
    carry_pairs: dict[str, dict[str, Any]] = field(default_factory=dict)
    carry_positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    daily_rows: list[dict[str, Any]] = field(default_factory=list)
    trade_rows: list[dict[str, Any]] = field(default_factory=list)
    position_rows: list[dict[str, Any]] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the same multi-day historical backtest across multiple base configs."
    )
    parser.add_argument("--start-date", required=True, help="First trade date, YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Last trade date, YYYY-MM-DD.")
    parser.add_argument(
        "--base-config",
        action="append",
        nargs="+",
        default=[],
        help="One or more base config paths. Can be specified multiple times.",
    )
    parser.add_argument(
        "--config-glob",
        action="append",
        default=[],
        help="Glob pattern for base configs, e.g. output/grid_configs/*.json.",
    )
    parser.add_argument("--calendar", default="Calendar.csv")
    parser.add_argument("--stockinfo", default="stockinfo.csv")
    parser.add_argument("--output-dir", default="output/multi_config_backtest")
    parser.add_argument("--futures-parquet-template", default=r"Z:\ticks_parquet_stock_future\{ldate}.parquet")
    parser.add_argument("--twse-daytrade-template", default=r"Z:\TWSE\每日個股狀況\{date_nodash}.csv")
    parser.add_argument("--tpex-daytrade-template", default=r"Z:\TPEX\每日個股狀況\{date_nodash}.csv")
    parser.add_argument("--twse-daily-template", default=r"Z:\TWSE\每日資料\{ldate_nodash}.ftr")
    parser.add_argument("--tpex-daily-template", default=r"Z:\TPEX\每日資料\{ldate_nodash}.ftr")
    parser.add_argument("--session-start", default="08:45:00")
    parser.add_argument("--session-end", default="13:45:00")
    parser.add_argument("--min-future-volume", type=int, default=1000)
    parser.add_argument("--min-stock-volume", type=int, default=20_000_000)
    parser.add_argument("--required-unit", type=int, default=2000)
    parser.add_argument("--name-template", default="{spot_symbol}_{future_symbol}")
    parser.add_argument("--iterations", type=int, default=None)
    parser.add_argument("--show-events", action="store_true", help="Print per-event backtest output to terminal.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config_paths = resolve_config_paths(args)
    if not config_paths:
        raise SystemExit("No configs supplied. Use --base-config or --config-glob.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    calendar = pd.read_csv(args.calendar, dtype=str)
    trade_dates = select_trade_dates(calendar, args.start_date, args.end_date)
    stock_info = read_stockinfo_frame(Path(args.stockinfo))
    targets_by_date = build_targets_once(args, trade_dates, stock_info)
    event_cache = HistoricalReplayEventCache()
    states = [
        create_backtest_state(args, config_path, output_dir)
        for config_path in config_paths
    ]

    for trade_date in trade_dates:
        stock_symbols, future_symbols = build_replay_symbol_union_for_date(
            states,
            trade_date,
            targets_by_date,
            args.name_template,
        )
        preload_replay_events_for_date(
            states=states,
            trade_date=trade_date,
            event_cache=event_cache,
            stock_symbols=stock_symbols,
            future_symbols=future_symbols,
        )
        for state in states:
            run_config_backtest_date(
                args=args,
                state=state,
                trade_date=trade_date,
                targets_by_date=targets_by_date,
                event_cache=event_cache,
            )
        event_cache.clear()
        gc.collect()
        logging.info("cleared replay event cache after date=%s", trade_date)

    summary_rows = [
        finalize_config_backtest(state)
        for state in states
    ]
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(output_dir / "grid_summary.csv", index=False, encoding="utf-8-sig")
    logging.info("wrote grid summary to %s", output_dir / "grid_summary.csv")


def create_backtest_state(args: argparse.Namespace, base_config_path: Path, output_dir: Path) -> BacktestState:
    config_output_dir = output_dir / slug_for_config(base_config_path)
    config_dir = config_output_dir / "configs"
    target_dir = config_output_dir / "targets"
    event_log_dir = config_output_dir / "event_logs"
    config_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    event_log_dir.mkdir(parents=True, exist_ok=True)
    logging.info("=== config=%s output=%s ===", base_config_path, config_output_dir)
    base = read_json(base_config_path)
    return BacktestState(
        base_config_path=base_config_path,
        output_dir=config_output_dir,
        base=base,
        pair_defaults=extract_pair_defaults(base, base_config_path),
        config_dir=config_dir,
        target_dir=target_dir,
        event_log_dir=event_log_dir,
    )


def resolve_config_paths(args: argparse.Namespace) -> list[Path]:
    paths: list[Path] = []
    for group in args.base_config:
        paths.extend(Path(item) for item in group)
    for pattern in args.config_glob:
        paths.extend(Path(item) for item in sorted(glob.glob(pattern)))

    seen = set()
    result = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if not resolved.exists():
            raise FileNotFoundError(f"base config does not exist: {path}")
        result.append(resolved)
    return result


def build_targets_once(
    args: argparse.Namespace,
    trade_dates: list[str],
    stock_info: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    targets_by_date = {}
    for trade_date in trade_dates:
        ldate = get_ldate(Path(args.calendar), trade_date)
        logging.info("building shared targets date=%s ldate=%s", trade_date, ldate)
        targets_by_date[trade_date] = build_daily_targets(args, trade_date, ldate, stock_info)
    return targets_by_date


def build_replay_symbol_union_for_date(
    states: list[BacktestState],
    trade_date: str,
    targets_by_date: dict[str, pd.DataFrame],
    name_template: str,
) -> tuple[set[str], set[str]]:
    stock_symbols: set[str] = set()
    future_symbols: set[str] = set()
    for state in states:
        targets = targets_by_date[trade_date].copy()
        if front_month_only_enabled(state.base):
            targets = filter_front_month_targets(targets, trade_date=trade_date)
        pairs = build_pairs_for_config(targets, state.pair_defaults, name_template)
        pairs = merge_carry_pairs(
            pairs,
            state.carry_pairs,
            state.carry_positions,
        )
        for pair in pairs:
            stock_symbols.add(str(pair["spot_symbol"]))
            future_symbols.add(str(pair["future_symbol"]))

    logging.info(
        "built replay preload symbol union date=%s stock_symbols=%s future_symbols=%s",
        trade_date,
        len(stock_symbols),
        len(future_symbols),
    )
    return stock_symbols, future_symbols


def preload_replay_events_for_date(
    states: list[BacktestState],
    trade_date: str,
    event_cache: HistoricalReplayEventCache,
    stock_symbols: set[str],
    future_symbols: set[str],
) -> None:
    if not stock_symbols and not future_symbols:
        return
    logging.info(
        "preloading replay events date=%s stock_symbols=%s future_symbols=%s",
        trade_date,
        len(stock_symbols),
        len(future_symbols),
    )
    for state in states:
        historical = parse_historical_config(
            state.base.get("historical") or {},
            state.base_config_path.parent,
            replay_date_override=trade_date,
        )
        if stock_symbols:
            event_cache.preload_source_events("stock", historical.stock, stock_symbols)
        if future_symbols:
            event_cache.preload_source_events("future", historical.futures, future_symbols)


def run_config_backtest_date(
    args: argparse.Namespace,
    state: BacktestState,
    trade_date: str,
    targets_by_date: dict[str, pd.DataFrame],
    event_cache: HistoricalReplayEventCache,
) -> None:
    logging.info("config=%s date=%s", state.base_config_path.name, trade_date)
    targets = targets_by_date[trade_date].copy()
    if front_month_only_enabled(state.base):
        targets = filter_front_month_targets(targets, trade_date=trade_date)
    targets.to_csv(
        state.target_dir / f"target_futures_{trade_date.replace('-', '')}.csv",
        index=False,
        encoding="utf-8-sig",
    )

    pairs = build_pairs_for_config(targets, state.pair_defaults, args.name_template)
    pairs = merge_carry_pairs(pairs, state.carry_pairs, state.carry_positions)
    apply_initial_positions(pairs, state.carry_positions)

    config_path = state.config_dir / f"arbitrage_config_{trade_date.replace('-', '')}.json"
    write_daily_config(state.base, pairs, state.base_config_path.parent, config_path)
    config = load_config(config_path, replay_date_override=trade_date)
    if config.mode != Mode.PAPER:
        config = config.with_mode(Mode.PAPER)

    starting_positions = {pair.name: build_initial_position(pair) for pair in config.pairs}
    stop_event = threading.Event()
    provider = HistoricalParquetReplayProvider(config, stop_event, event_cache=event_cache)
    try:
        if args.show_events:
            execution, positions = run(
                config,
                provider,
                args.iterations,
                stop_event,
                enforce_risk_limits_in_paper=True,
            )
        else:
            event_log_path = state.event_log_dir / f"events_{trade_date.replace('-', '')}.log"
            with event_log_path.open("w", encoding="utf-8") as event_log:
                with contextlib.redirect_stdout(event_log):
                    previous_disable_level = logging.root.manager.disable
                    logging.disable(logging.INFO)
                    try:
                        execution, positions = run(
                            config,
                            provider,
                            args.iterations,
                            stop_event,
                            enforce_risk_limits_in_paper=True,
                        )
                    finally:
                        logging.disable(previous_disable_level)
    finally:
        provider.close()

    cumulative_realized_pnl = 0.0
    for trade_no, trade in enumerate(execution.trades, start=1):
        if trade.realized_pnl is not None:
            cumulative_realized_pnl += trade.realized_pnl
        state.trade_rows.append(trade_to_row(trade_date, trade_no, trade, cumulative_realized_pnl))

    state.carry_pairs, state.carry_positions = build_carry_state(pairs, positions)
    state.position_rows.extend(positions_to_rows(trade_date, config, positions))
    state.daily_rows.append(
        build_daily_summary_row(
            trade_date,
            execution.trades,
            config,
            starting_positions,
            positions,
            target_count=len(targets),
            pair_count=len(pairs),
        )
    )


def finalize_config_backtest(state: BacktestState) -> dict[str, Any]:
    write_report(state.output_dir, state.daily_rows, state.trade_rows, state.position_rows)
    return summarize_config(
        state.base_config_path,
        state.output_dir,
        state.pair_defaults,
        state.daily_rows,
        state.trade_rows,
    )


def build_pairs_for_config(
    targets: pd.DataFrame,
    pair_defaults: dict[str, Any],
    name_template: str,
) -> list[dict[str, Any]]:
    from scripts.build_arbitrage_config_from_date import build_pairs

    return build_pairs(targets, pair_defaults, name_template) if not targets.empty else []


def summarize_config(
    config_path: Path,
    output_dir: Path,
    pair_defaults: dict[str, Any],
    daily_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    daily = pd.DataFrame(daily_rows)
    trades = pd.DataFrame(trade_rows)
    closed = trades[trades["is_close"]] if not trades.empty else trades
    traded_notional = 0.0 if trades.empty else float((trades["spot_price"] * trades["spot_qty"].abs()).sum())
    realized_pnl = float(daily["realized_pnl"].sum()) if not daily.empty else 0.0
    max_total_spot_notional = (
        None
        if daily.empty or daily["max_total_spot_notional"].isna().all()
        else float(daily["max_total_spot_notional"].dropna().max())
    )
    base_config = read_json(config_path)
    backtest_execution = base_config.get("backtest_execution", {})
    return {
        "config": str(config_path),
        "output_dir": str(output_dir),
        "entry_threshold_pct": pair_defaults.get("entry_threshold_pct"),
        "exit_threshold_pct": pair_defaults.get("exit_threshold_pct"),
        "exit_tick_multiple": pair_defaults.get("exit_tick_multiple"),
        "exit_tick_rule": pair_defaults.get("exit_tick_rule"),
        "min_effective_tick_multiple": pair_defaults.get("min_effective_tick_multiple"),
        "min_exit_realized_pnl": pair_defaults.get("min_exit_realized_pnl"),
        "min_entry_interval_sec": pair_defaults.get("min_entry_interval_sec", 0.0),
        "send_order_latency_ms": backtest_execution.get("send_order_latency_ms"),
        "match_order_report_latency_ms": backtest_execution.get("match_order_report_latency_ms"),
        "second_leg_profit_check": backtest_execution.get("second_leg_profit_check"),
        "max_quote_age_sec": backtest_execution.get("max_quote_age_sec"),
        "trading_session_start_time": base_config.get("trading_session_start_time", "09:00:00"),
        "trading_session_end_time": base_config.get("trading_session_end_time", "13:25:00"),
        "days": len(daily),
        "entry_trades": int(daily["entry_trades"].sum()) if not daily.empty else 0,
        "closed_trades": int(daily["closed_trades"].sum()) if not daily.empty else 0,
        "trades": int(daily["trades"].sum()) if not daily.empty else 0,
        "traded_notional": traded_notional,
        "turnover_vs_max_spot_notional": (
            None
            if not max_total_spot_notional
            else traded_notional / max_total_spot_notional
        ),
        "realized_pnl": realized_pnl,
        "avg_realized_pnl_per_close": (
            None
            if closed.empty
            else float(closed["realized_pnl"].mean())
        ),
        "win_rate": (
            None
            if closed.empty
            else float((closed["realized_pnl"] > 0).mean())
        ),
        "pnl_per_traded_notional": None if traded_notional == 0 else realized_pnl / traded_notional,
        "ending_positions": int(daily["ending_positions"].iloc[-1]) if not daily.empty else 0,
        "ending_spot_notional": float(daily["ending_spot_notional"].iloc[-1]) if not daily.empty else 0.0,
    }


def slug_for_config(config_path: Path) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", config_path.stem).strip("._-")
    return slug or "config"


if __name__ == "__main__":
    main()
