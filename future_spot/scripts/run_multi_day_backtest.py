from __future__ import annotations

import argparse
import contextlib
import json
import logging
import sys
import threading
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage.app import calculate_total_spot_notional, run  # noqa: E402
from arbitrage.config import build_initial_position, load_config  # noqa: E402
from arbitrage.models import Mode, PairPosition, Signal  # noqa: E402
from arbitrage.providers import HistoricalParquetReplayProvider  # noqa: E402
from scripts.build_arbitrage_config_from_date import (  # noqa: E402
    PRODUCT_KEYS,
    build_futures_session_ohlcv,
    build_pairs,
    build_targets,
    contract_month_sort_key,
    extract_pair_defaults,
    filter_front_month_targets,
    front_month_only_enabled,
    format_template,
    get_ldate,
    normalize_date,
    read_json,
    read_stock_daily,
    read_stockinfo_frame,
    rebase_fubon_cert_paths,
    remove_replay_date,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate daily configs, run historical replay, and carry remaining positions forward."
    )
    parser.add_argument("--start-date", required=True, help="First trade date, YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Last trade date, YYYY-MM-DD.")
    parser.add_argument("--base-config", default="arbitrage_unified_config.example.json")
    parser.add_argument(
        "--switch-date",
        default=None,
        help="First trade date that should use --switch-base-config, YYYY-MM-DD.",
    )
    parser.add_argument(
        "--switch-base-config",
        default=None,
        help="Base config to use from --switch-date onward.",
    )
    parser.add_argument(
        "--switch-days-before-settlement",
        type=int,
        default=None,
        help=(
            "Use --switch-base-config when the front-month contract is this many "
            "trading days or fewer from settlement. Settlement day is 0."
        ),
    )
    parser.add_argument("--calendar", default="Calendar.csv")
    parser.add_argument("--stockinfo", default="stockinfo.csv")
    parser.add_argument("--output-dir", default="output/multi_day_backtest")
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

    output_dir = Path(args.output_dir)
    config_dir = output_dir / "configs"
    target_dir = output_dir / "targets"
    output_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    event_log_dir = output_dir / "event_logs"
    event_log_dir.mkdir(parents=True, exist_ok=True)

    calendar = pd.read_csv(args.calendar, dtype=str)
    trade_dates = select_trade_dates(calendar, args.start_date, args.end_date)
    calendar_trade_dates = [normalize_date(date) for date in calendar["trade_dates"].dropna().astype(str)]
    validate_config_switch_args(args)
    base_cache: dict[Path, tuple[dict[str, Any], dict[str, Any]]] = {}
    stock_info = read_stockinfo_frame(Path(args.stockinfo))

    carry_pairs: dict[str, dict[str, Any]] = {}
    carry_positions: dict[str, dict[str, Any]] = {}
    daily_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []

    for trade_date in trade_dates:
        ldate = get_ldate(Path(args.calendar), trade_date)
        logging.info("=== backtest date=%s ldate=%s ===", trade_date, ldate)

        targets = build_daily_targets(args, trade_date, ldate, stock_info)
        base_config_path = select_base_config_path(
            args=args,
            trade_date=trade_date,
            targets=targets,
            carried_future_symbols=set(carry_pairs),
            trade_dates=calendar_trade_dates,
        )
        base, pair_defaults = load_cached_base_config(base_config_path, base_cache)
        logging.info("using base config: %s", base_config_path)
        if front_month_only_enabled(base):
            targets = filter_front_month_targets(targets, trade_date=trade_date)
        target_path = target_dir / f"target_futures_{trade_date.replace('-', '')}.csv"
        targets.to_csv(target_path, index=False, encoding="utf-8-sig")

        pairs = build_pairs(targets, pair_defaults, args.name_template) if not targets.empty else []
        pairs = merge_carry_pairs(pairs, carry_pairs, carry_positions, pair_defaults)
        apply_initial_positions(pairs, carry_positions)

        config_path = config_dir / f"arbitrage_config_{trade_date.replace('-', '')}.json"
        write_daily_config(base, pairs, base_config_path.parent, config_path)

        config = load_config(config_path, replay_date_override=trade_date)
        if config.mode != Mode.PAPER:
            config = config.with_mode(Mode.PAPER)
        starting_positions = {pair.name: build_initial_position(pair) for pair in config.pairs}
        stop_event = threading.Event()
        provider = HistoricalParquetReplayProvider(config, stop_event)
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
                event_log_path = event_log_dir / f"events_{trade_date.replace('-', '')}.log"
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
            trade_rows.append(trade_to_row(trade_date, trade_no, trade, cumulative_realized_pnl))

        carry_pairs, carry_positions = build_carry_state(pairs, positions)
        day_position_rows = positions_to_rows(trade_date, config, positions)
        position_rows.extend(day_position_rows)
        daily_rows.append(
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

        logging.info(
            "date=%s closed_trades=%s realized_pnl=%.2f ending_positions=%s",
            trade_date,
            sum(1 for trade in execution.trades if trade.realized_pnl is not None),
            sum(trade.realized_pnl or 0 for trade in execution.trades),
            len(carry_positions),
        )

    write_report(output_dir, daily_rows, trade_rows, position_rows)


def select_trade_dates(calendar: pd.DataFrame, start_date: str, end_date: str) -> list[str]:
    start = normalize_date(start_date)
    end = normalize_date(end_date)
    dates = calendar["trade_dates"].dropna().astype(str)
    selected = [normalize_date(date) for date in dates if start <= normalize_date(date) <= end]
    if not selected:
        raise ValueError(f"No trade dates found between {start} and {end}")
    return selected


def validate_config_switch_args(args: argparse.Namespace) -> None:
    switch_rules = [args.switch_date is not None, args.switch_days_before_settlement is not None]
    if sum(switch_rules) > 1:
        raise ValueError("Use only one of --switch-date or --switch-days-before-settlement")
    if any(switch_rules) != bool(args.switch_base_config):
        raise ValueError("--switch-base-config must be supplied with a switch rule")
    if args.switch_date is not None:
        normalize_date(args.switch_date)
    if args.switch_days_before_settlement is not None and args.switch_days_before_settlement < 0:
        raise ValueError("--switch-days-before-settlement must be >= 0")


def select_base_config_path(
    args: argparse.Namespace,
    trade_date: str,
    targets: pd.DataFrame,
    carried_future_symbols: set[str],
    trade_dates: list[str],
) -> Path:
    if args.switch_date is not None:
        if normalize_date(trade_date) >= normalize_date(args.switch_date):
            return Path(args.switch_base_config)
        return Path(args.base_config)
    if args.switch_days_before_settlement is None:
        return Path(args.base_config)

    days_remaining = days_to_front_month_settlement(
        trade_date=trade_date,
        targets=targets,
        carried_future_symbols=carried_future_symbols,
        trade_dates=trade_dates,
    )
    if days_remaining is not None:
        logging.info(
            "front-month settlement distance date=%s trading_days_remaining=%s threshold=%s",
            trade_date,
            days_remaining,
            args.switch_days_before_settlement,
        )
    if days_remaining is not None and days_remaining <= args.switch_days_before_settlement:
        return Path(args.switch_base_config)
    return Path(args.base_config)


def days_to_front_month_settlement(
    trade_date: str,
    targets: pd.DataFrame,
    carried_future_symbols: set[str],
    trade_dates: list[str],
) -> int | None:
    symbol = front_month_symbol(targets, carried_future_symbols, trade_date)
    if symbol is None:
        return None
    settlement_date = settlement_date_for_contract(symbol, trade_date, trade_dates)
    normalized_trade_dates = [normalize_date(date) for date in trade_dates]
    return sum(1 for date in normalized_trade_dates if normalize_date(trade_date) < date <= settlement_date)


def front_month_symbol(
    targets: pd.DataFrame,
    carried_future_symbols: set[str],
    trade_date: str,
) -> str | None:
    symbols: list[str] = []
    if not targets.empty and "symbol" in targets.columns:
        symbols.extend(str(symbol) for symbol in targets["symbol"].dropna())
    symbols.extend(carried_future_symbols)
    if not symbols:
        return None
    return min(symbols, key=lambda symbol: contract_month_sort_key(symbol, trade_date))


def settlement_date_for_contract(symbol: str, trade_date: str, trade_dates: list[str]) -> str:
    year, month, _ = contract_month_sort_key(symbol, trade_date)
    if year > 9000 or month > 12:
        raise ValueError(f"Cannot infer contract month from future symbol: {symbol}")
    settlement = third_wednesday(year, month)
    available_dates = sorted(normalize_date(date) for date in trade_dates)
    candidates = [date for date in available_dates if date <= settlement]
    if candidates:
        return candidates[-1]
    return normalize_date(settlement)


def third_wednesday(year: int, month: int) -> str:
    first_day = pd.Timestamp(year=year, month=month, day=1)
    days_until_wednesday = (2 - first_day.weekday()) % 7
    third = first_day + pd.Timedelta(days=days_until_wednesday + 14)
    return third.strftime("%Y-%m-%d")


def load_cached_base_config(
    base_config_path: Path,
    cache: dict[Path, tuple[dict[str, Any], dict[str, Any]]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved = base_config_path.resolve()
    cached = cache.get(resolved)
    if cached is None:
        base = read_json(resolved)
        pair_defaults = extract_pair_defaults(base, resolved)
        cached = (base, pair_defaults)
        cache[resolved] = cached
    return cached


def build_daily_targets(args: argparse.Namespace, trade_date: str, ldate: str, stock_info: pd.DataFrame) -> pd.DataFrame:
    futures_ohlcv = build_futures_session_ohlcv(
        path=Path(format_template(args.futures_parquet_template, trade_date, ldate)),
        session_start=args.session_start,
        session_end=args.session_end,
    )
    stock_daily = read_stock_daily(
        trade_date=trade_date,
        ldate=ldate,
        twse_daytrade_template=args.twse_daytrade_template,
        tpex_daytrade_template=args.tpex_daytrade_template,
        twse_daily_template=args.twse_daily_template,
        tpex_daily_template=args.tpex_daily_template,
    )
    return build_targets(
        futures_ohlcv=futures_ohlcv,
        stock_info=stock_info,
        stock_daily=stock_daily,
        min_future_volume=args.min_future_volume,
        min_stock_volume=args.min_stock_volume,
        required_unit=args.required_unit,
    )


def merge_carry_pairs(
    pairs: list[dict[str, Any]],
    carry_pairs: dict[str, dict[str, Any]],
    carry_positions: dict[str, dict[str, Any]],
    pair_defaults: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    by_future = {pair["future_symbol"]: pair for pair in pairs}
    for future_symbol, carry_pair in carry_pairs.items():
        if future_symbol not in carry_positions:
            continue
        if future_symbol not in by_future:
            pair = merge_pair_defaults(carry_pair, pair_defaults)
            pairs.append(pair)
            by_future[future_symbol] = pair
            logging.info("added carried position pair not in target: %s", future_symbol)
    return pairs


def merge_pair_defaults(carry_pair: dict[str, Any], pair_defaults: dict[str, Any] | None) -> dict[str, Any]:
    if pair_defaults is None:
        return dict(carry_pair)
    pair = dict(pair_defaults)
    for key in PRODUCT_KEYS:
        pair[key] = carry_pair[key]
    return pair


def apply_initial_positions(pairs: list[dict[str, Any]], carry_positions: dict[str, dict[str, Any]]) -> None:
    for pair in pairs:
        carried = carry_positions.get(pair["future_symbol"])
        if carried is not None:
            pair["initial_position"] = dict(carried)


def write_daily_config(base: dict[str, Any], pairs: list[dict[str, Any]], base_dir: Path, output: Path) -> None:
    if not pairs:
        raise ValueError(f"No pairs available for daily config {output}")
    output_config = {key: value for key, value in base.items() if key not in {"pair_defaults", "pairs"}}
    remove_replay_date(output_config)
    rebase_fubon_cert_paths(output_config, base_dir)
    output_config["pairs"] = pairs
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(output_config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_carry_state(
    pairs: list[dict[str, Any]],
    positions: dict[str, PairPosition],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    pairs_by_name = {pair["name"]: pair for pair in pairs}
    carry_pairs: dict[str, dict[str, Any]] = {}
    carry_positions: dict[str, dict[str, Any]] = {}
    for pair_name, position in positions.items():
        if not position.has_position:
            continue
        pair = pairs_by_name.get(pair_name)
        if pair is None:
            continue
        future_symbol = pair["future_symbol"]
        carry_pair = dict(pair)
        carry_pair["initial_position"] = empty_initial_position()
        carry_pairs[future_symbol] = carry_pair
        carry_positions[future_symbol] = position_to_initial_position(position)
    return carry_pairs, carry_positions


def position_to_initial_position(position: PairPosition) -> dict[str, Any]:
    return {
        "quantity": int(position.quantity) if float(position.quantity).is_integer() else position.quantity,
        "direction": position.direction.value,
        "entry_basis_pct": position.entry_basis_pct,
        "entry_spot_price": position.entry_spot_price,
        "entry_future_price": position.entry_future_price,
    }


def empty_initial_position() -> dict[str, Any]:
    return {
        "quantity": 0,
        "direction": Signal.HOLD.value,
        "entry_basis_pct": None,
        "entry_spot_price": None,
        "entry_future_price": None,
    }


def build_daily_summary_row(
    trade_date: str,
    trades: list[Any],
    config: Any,
    starting_positions: dict[str, PairPosition],
    positions: dict[str, PairPosition],
    target_count: int,
    pair_count: int,
) -> dict[str, Any]:
    realized = [trade for trade in trades if trade.realized_pnl is not None]
    closed = [trade for trade in realized if not is_latency_first_leg_flatten(trade)]
    entries = [
        trade
        for trade in trades
        if trade.signal in (Signal.ENTER_LONG_SPOT_SHORT_FUTURE, Signal.ENTER_SHORT_SPOT_LONG_FUTURE)
        and not is_latency_first_leg_flatten(trade)
    ]
    realized_pnl = sum(trade.realized_pnl or 0 for trade in realized)
    starting_spot_notional = calculate_total_spot_notional(config, starting_positions)
    ending_spot_notional = calculate_total_spot_notional(config, positions)
    max_total_spot_notional = config.max_total_spot_notional
    return {
        "date": trade_date,
        "target_count": target_count,
        "pair_count": pair_count,
        "starting_positions": sum(1 for position in starting_positions.values() if position.has_position),
        "starting_total_pairs": sum(abs(position.quantity) for position in starting_positions.values()),
        "starting_spot_notional": starting_spot_notional,
        "trades": len(trades),
        "entry_trades": len(entries),
        "closed_trades": len(closed),
        "gross_pnl": sum(trade.gross_pnl or 0 for trade in realized),
        "stock_cost": sum(trade.stock_cost or 0 for trade in realized),
        "stock_fee": sum(trade.stock_fee or 0 for trade in realized),
        "stock_tax": sum(trade.stock_tax or 0 for trade in realized),
        "realized_pnl": realized_pnl,
        "spot_pnl": sum(trade.spot_pnl or 0 for trade in realized),
        "future_pnl": sum(trade.future_pnl or 0 for trade in realized),
        "ending_positions": sum(1 for position in positions.values() if position.has_position),
        "ending_total_pairs": sum(abs(position.quantity) for position in positions.values()),
        "ending_spot_notional": ending_spot_notional,
        "max_total_spot_notional": max_total_spot_notional,
        "ending_available_spot_notional": (
            None if max_total_spot_notional is None else max_total_spot_notional - ending_spot_notional
        ),
    }


def trade_to_row(
    trade_date: str,
    trade_no: int,
    trade: Any,
    cumulative_realized_pnl: float,
) -> dict[str, Any]:
    return {
        "date": trade_date,
        "trade_no": trade_no,
        "pair_name": trade.pair_name,
        "signal": trade.signal.value,
        "is_close": trade.realized_pnl is not None and not is_latency_first_leg_flatten(trade),
        "spot_symbol": trade.spot_symbol,
        "future_symbol": trade.future_symbol,
        "spot_side": trade.spot_side,
        "future_side": trade.future_side,
        "spot_qty": trade.spot_qty,
        "future_qty": trade.future_qty,
        "spot_price": trade.spot_price,
        "future_price": trade.future_price,
        "basis_pct": trade.basis_pct,
        "reason": trade.reason,
        "gross_pnl": trade.gross_pnl,
        "stock_cost": trade.stock_cost,
        "stock_fee": trade.stock_fee,
        "stock_tax": trade.stock_tax,
        "realized_pnl": trade.realized_pnl,
        "cumulative_realized_pnl": cumulative_realized_pnl,
        "realized_pnl_pct": trade.realized_pnl_pct,
        "spot_pnl": trade.spot_pnl,
        "future_pnl": trade.future_pnl,
        "spot_exchtime": trade.spot_exchtime,
        "future_exchtime": trade.future_exchtime,
        "trigger_source": trade.trigger_source,
        "trigger_symbol": trade.trigger_symbol,
        "latency_first_leg": getattr(trade, "latency_first_leg", None),
        "latency_failure_reason": getattr(trade, "latency_failure_reason", None),
        "latency_signal_time": getattr(trade, "latency_signal_time", None),
        "latency_first_fill_time": getattr(trade, "latency_first_fill_time", None),
        "latency_decision_time": getattr(trade, "latency_decision_time", None),
        "latency_second_fill_time": getattr(trade, "latency_second_fill_time", None),
        "latency_flatten_time": getattr(trade, "latency_flatten_time", None),
        "latency_first_leg_entry_price": getattr(trade, "latency_first_leg_entry_price", None),
        "latency_flatten_price": getattr(trade, "latency_flatten_price", None),
    }


def is_latency_first_leg_flatten(trade: Any) -> bool:
    return str(getattr(trade, "reason", "")).startswith("LATENCY_FIRST_LEG_FLATTEN")


def positions_to_rows(trade_date: str, config: Any, positions: dict[str, PairPosition]) -> list[dict[str, Any]]:
    pairs_by_name = {pair.name: pair for pair in config.pairs}
    rows = []
    for pair_name, position in positions.items():
        if not position.has_position and not position.has_leg_exposure:
            continue
        pair = pairs_by_name[pair_name]
        rows.append(
            {
                "date": trade_date,
                "pair_name": pair_name,
                "spot_symbol": pair.spot_symbol,
                "future_symbol": pair.future_symbol,
                "quantity": position.quantity,
                "direction": position.direction.value,
                "stock_units": position.stock_units,
                "future_units": position.future_units,
                "entry_basis_pct": position.entry_basis_pct,
                "entry_spot_price": position.entry_spot_price,
                "entry_future_price": position.entry_future_price,
                "last_entry_time": position.last_entry_time,
                "spot_notional": (
                    None
                    if position.entry_spot_price is None
                    else position.entry_spot_price * pair.spot_order_qty * abs(position.stock_units or position.quantity)
                ),
            }
        )
    return rows


def write_report(
    output_dir: Path,
    daily_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    position_rows: list[dict[str, Any]],
) -> None:
    pd.DataFrame(daily_rows).to_csv(output_dir / "daily_pnl.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(trade_rows).to_csv(output_dir / "trades.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(position_rows).to_csv(output_dir / "remaining_positions.csv", index=False, encoding="utf-8-sig")
    logging.info("wrote daily report to %s", output_dir)


if __name__ == "__main__":
    main()
