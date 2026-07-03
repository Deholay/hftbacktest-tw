from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from .models import (
    AppConfig,
    BacktestExecutionConfig,
    FubonConfig,
    FubonLoginConfig,
    HistoricalReplayConfig,
    HistoricalSourceConfig,
    InitialPositionConfig,
    Mode,
    PairConfig,
    PairPosition,
    Signal,
)
from .utils import pct

def load_config(path: str | Path, replay_date_override: str | None = None) -> AppConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = json.load(file)

    pair_configs = tuple(parse_pair_config(item, config_path) for item in raw.get("pairs", []))
    if not pair_configs:
        raise ValueError(f"{config_path} must contain at least one pair in 'pairs'")

    return AppConfig(
        mode=Mode(raw.get("mode", Mode.DRY_RUN.value)),
        allow_live_order=bool(raw.get("allow_live_order", False)),
        poll_interval_sec=float(raw.get("poll_interval_sec", 2.0)),
        max_total_pairs=parse_optional_int(raw.get("max_total_pairs")),
        max_total_spot_notional=parse_optional_non_negative_float(raw.get("max_total_spot_notional"), "max_total_spot_notional"),
        log_threshold_only=bool(raw.get("log_threshold_only", False)),
        log_order_only=bool(raw.get("log_order_only", False)),
        sync_system_time_on_start=bool(raw.get("sync_system_time_on_start", True)),
        trading_session_start_time=parse_optional_str(raw.get("trading_session_start_time", "09:00:00")),
        trading_session_end_time=parse_optional_str(raw.get("trading_session_end_time", "13:25:00")),
        fubon=parse_fubon_config(raw.get("fubon") or {}, config_path.parent),
        historical=parse_historical_config(
            raw.get("historical") or {},
            config_path.parent,
            replay_date_override=replay_date_override,
        ),
        backtest_execution=parse_backtest_execution_config(raw.get("backtest_execution") or {}),
        pairs=pair_configs,
    )


def parse_historical_config(
    raw: dict[str, Any],
    base_dir: Path,
    replay_date_override: str | None = None,
) -> HistoricalReplayConfig:
    replay_date = parse_optional_str(replay_date_override) or parse_optional_str(raw.get("replay_date"))
    replacements = build_replay_date_replacements(replay_date)
    return HistoricalReplayConfig(
        stock=parse_historical_source_config(raw.get("stock") or {}, base_dir, replacements),
        futures=parse_historical_source_config(raw.get("futures") or {}, base_dir, replacements),
        replay_interval_sec=float(raw.get("replay_interval_sec", 0.0)),
        replay_date=replay_date,
    )


def parse_backtest_execution_config(raw: dict[str, Any]) -> BacktestExecutionConfig:
    return BacktestExecutionConfig(
        send_order_latency_ms=parse_non_negative_float(
            raw.get("send_order_latency_ms", 0.0),
            "backtest_execution.send_order_latency_ms",
        ),
        match_order_report_latency_ms=parse_non_negative_float(
            raw.get("match_order_report_latency_ms", 0.0),
            "backtest_execution.match_order_report_latency_ms",
        ),
        second_leg_profit_check=bool(raw.get("second_leg_profit_check", True)),
        max_quote_age_sec=parse_optional_non_negative_float(
            raw.get("max_quote_age_sec"),
            "backtest_execution.max_quote_age_sec",
        ),
    )


def build_replay_date_replacements(replay_date: str | None) -> dict[str, str]:
    if not replay_date:
        return {}
    normalized = replay_date.strip()
    if len(normalized) == 8 and normalized.isdigit():
        date_nodash = normalized
        date_dash = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}"
    else:
        date_dash = normalized
        date_nodash = normalized.replace("-", "")
    if len(date_nodash) != 8 or not date_nodash.isdigit():
        raise ValueError(f"historical.replay_date must be YYYY-MM-DD or YYYYMMDD: {replay_date}")
    return {
        "date_dash": date_dash,
        "date_nodash": date_nodash,
        "yyyy": date_nodash[:4],
        "mm": date_nodash[4:6],
        "dd": date_nodash[6:8],
    }


def format_replay_path(path: str, replacements: dict[str, str]) -> str:
    if not path or not replacements:
        return path
    try:
        return path.format(**replacements)
    except KeyError as exc:
        raise ValueError(f"Unsupported historical path placeholder: {exc.args[0]}") from exc


def parse_historical_source_config(
    raw: dict[str, Any],
    base_dir: Path,
    replacements: dict[str, str],
) -> HistoricalSourceConfig:
    path = format_replay_path(str(raw.get("path", "")), replacements)
    if path:
        source_path = Path(path)
        if not source_path.is_absolute():
            path = str((base_dir / source_path).resolve())

    return HistoricalSourceConfig(
        path=path,
        timestamp_col=str(raw.get("timestamp_col", "timestamp")),
        symbol_col=str(raw.get("symbol_col", "symbol")),
        bid_col=str(raw.get("bid_col", "bid")),
        ask_col=str(raw.get("ask_col", "ask")),
        bid_size_col=parse_optional_str(raw.get("bid_size_col", "bid_size")),
        ask_size_col=parse_optional_str(raw.get("ask_size_col", "ask_size")),
        last_col=parse_optional_str(raw.get("last_col")),
        status_col=parse_optional_str(raw.get("status_col", "status")),
        filter_trial_status=bool(raw.get("filter_trial_status", True)),
        session_start_time=parse_optional_str(raw.get("session_start_time")),
        session_end_time=parse_optional_str(raw.get("session_end_time")),
        default_size=int(raw.get("default_size", 1)),
    )


def parse_optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def parse_exit_tick_rule(value: Any) -> str:
    text = str(value or "lte").strip().lower()
    if text not in {"lte", "gte"}:
        raise ValueError(f"exit_tick_rule must be 'lte' or 'gte': {value}")
    return text


def parse_time_in_force(value: Any, field_name: str) -> str:
    text = str(value or "ROD").strip().upper()
    if text not in {"ROD", "IOC", "FOK"}:
        raise ValueError(f"{field_name} must be ROD, IOC, or FOK: {value}")
    return text


def parse_second_leg_failure_action(value: Any) -> str:
    text = str(value or "none").strip().lower()
    if text not in {"none", "flatten_first_leg"}:
        raise ValueError(f"second_leg_failure_action must be none or flatten_first_leg: {value}")
    return text


def parse_non_negative_int(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{field_name} must be >= 0: {value}")
    return parsed


def parse_non_negative_float(value: Any, field_name: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise ValueError(f"{field_name} must be >= 0: {value}")
    return parsed


def parse_fubon_config(raw: dict[str, Any], base_dir: Path) -> FubonConfig:
    return FubonConfig(
        stock=parse_fubon_login_config(raw.get("stock") or {}, base_dir),
        futures=parse_fubon_login_config(raw.get("futures") or {}, base_dir),
    )


def parse_fubon_login_config(raw: dict[str, Any], base_dir: Path) -> FubonLoginConfig:
    cert_path = str(raw.get("cert_path", ""))
    if cert_path:
        path = Path(cert_path)
        if not path.is_absolute():
            cert_path = str((base_dir / path).resolve())

    return FubonLoginConfig(
        personal_id=str(raw.get("personal_id", "")),
        password=str(raw.get("password", "")),
        cert_path=cert_path,
        cert_pass=str(raw.get("cert_pass", "")),
    )


def parse_pair_config(raw: dict[str, Any], config_path: Path) -> PairConfig:
    required = (
        "name",
        "spot_symbol",
        "future_symbol",
        "spot_shares_per_pair",
        "future_shares_per_pair",
        "spot_order_qty",
        "future_order_qty",
        "entry_threshold_pct",
        "stop_loss_pct",
    )
    missing = [key for key in required if key not in raw]
    if missing:
        name = raw.get("name", "<unnamed>")
        raise ValueError(f"{config_path} pair {name} missing required keys: {', '.join(missing)}")

    return PairConfig(
        name=str(raw["name"]),
        spot_symbol=str(raw["spot_symbol"]),
        future_symbol=str(raw["future_symbol"]),
        spot_shares_per_pair=int(raw["spot_shares_per_pair"]),
        future_shares_per_pair=int(raw["future_shares_per_pair"]),
        spot_order_qty=int(raw["spot_order_qty"]),
        future_order_qty=int(raw["future_order_qty"]),
        future_pnl_multiplier=int(raw.get("future_pnl_multiplier", 2000)),
        entry_threshold_pct=float(raw["entry_threshold_pct"]),
        exit_threshold_pct=float(raw.get("exit_threshold_pct", 0.0)),
        stop_loss_pct=float(raw["stop_loss_pct"]),
        exit_tick_multiple=float(raw.get("exit_tick_multiple", 1.0)),
        exit_tick_rule=parse_exit_tick_rule(raw.get("exit_tick_rule", "lte")),
        min_exit_realized_pnl=parse_optional_float(raw.get("min_exit_realized_pnl")),
        min_effective_tick_multiple=float(raw.get("min_effective_tick_multiple", 0.0)),
        min_entry_interval_sec=parse_non_negative_float(raw.get("min_entry_interval_sec", 0.0), "min_entry_interval_sec"),
        spot_tick_size=parse_optional_positive_float(raw.get("spot_tick_size"), "spot_tick_size"),
        future_tick_size=parse_optional_positive_float(raw.get("future_tick_size"), "future_tick_size"),
        stock_commission_rate=float(raw.get("stock_commission_rate", 0.001425)),
        stock_commission_discount=float(raw.get("stock_commission_discount", 0.28)),
        stock_transaction_tax_rate=float(raw.get("stock_transaction_tax_rate", 0.003)),
        max_pairs=int(raw.get("max_pairs", 1)),
        allow_short_spot=bool(raw.get("allow_short_spot", False)),
        futures_after_hours=bool(raw.get("futures_after_hours", False)),
        min_bid_size=int(raw.get("min_bid_size", 1)),
        min_ask_size=int(raw.get("min_ask_size", 1)),
        stock_min_bid_size=int(raw.get("stock_min_bid_size", raw.get("min_bid_size", 2))),
        stock_min_ask_size=int(raw.get("stock_min_ask_size", raw.get("min_ask_size", 2))),
        future_min_bid_size=int(raw.get("future_min_bid_size", 1)),
        future_min_ask_size=int(raw.get("future_min_ask_size", 1)),
        first_leg_time_in_force=parse_time_in_force(raw.get("first_leg_time_in_force", "ROD"), "first_leg_time_in_force"),
        second_leg_tick_offset=parse_non_negative_float(raw.get("second_leg_tick_offset", 0.0), "second_leg_tick_offset"),
        second_leg_time_in_force=parse_time_in_force(raw.get("second_leg_time_in_force", "ROD"), "second_leg_time_in_force"),
        second_leg_failure_action=parse_second_leg_failure_action(raw.get("second_leg_failure_action", "none")),
        min_second_leg_adjusted_basis_pct=parse_optional_non_negative_float(
            raw.get("min_second_leg_adjusted_basis_pct"),
            "min_second_leg_adjusted_basis_pct",
        ),
        flatten_first_leg_tick_offset=parse_non_negative_float(
            raw.get("flatten_first_leg_tick_offset", 2.0),
            "flatten_first_leg_tick_offset",
        ),
        flatten_first_leg_time_in_force=parse_time_in_force(
            raw.get("flatten_first_leg_time_in_force", "IOC"),
            "flatten_first_leg_time_in_force",
        ),
        cooldown_after_second_leg_failure_sec=parse_non_negative_float(
            raw.get("cooldown_after_second_leg_failure_sec", 30.0),
            "cooldown_after_second_leg_failure_sec",
        ),
        fok_order_timeout_sec=parse_non_negative_float(
            raw.get("fok_order_timeout_sec", 3.0),
            "fok_order_timeout_sec",
        ),
        initial_position=parse_initial_position_config(raw.get("initial_position") or {}, config_path, raw.get("name", "<unnamed>")),
    )


def parse_initial_position_config(
    raw: dict[str, Any],
    config_path: Path,
    pair_name: Any,
) -> InitialPositionConfig:
    quantity = int(raw.get("quantity", 0))
    direction = parse_position_direction(raw.get("direction", Signal.HOLD.value))
    entry_basis_raw = raw.get("entry_basis_pct")
    entry_basis_pct = None if entry_basis_raw in (None, "") else float(entry_basis_raw)
    entry_spot_price = parse_optional_float(raw.get("entry_spot_price"))
    entry_future_price = parse_optional_float(raw.get("entry_future_price"))
    if entry_spot_price is not None and entry_spot_price <= 0:
        raise ValueError(f"{config_path} pair {pair_name} initial_position.entry_spot_price must be > 0")
    if entry_future_price is not None and entry_future_price <= 0:
        raise ValueError(f"{config_path} pair {pair_name} initial_position.entry_future_price must be > 0")

    if quantity == 0:
        return InitialPositionConfig()

    if direction not in (
        Signal.ENTER_LONG_SPOT_SHORT_FUTURE,
        Signal.ENTER_SHORT_SPOT_LONG_FUTURE,
    ):
        raise ValueError(
            f"{config_path} pair {pair_name} initial_position.quantity > 0 "
            "requires direction ENTER_LONG_SPOT_SHORT_FUTURE or ENTER_SHORT_SPOT_LONG_FUTURE"
        )

    if entry_basis_pct is None and entry_spot_price and entry_future_price:
        if direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            entry_basis_pct = (entry_future_price - entry_spot_price) / entry_spot_price
        elif direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            entry_basis_pct = (entry_spot_price - entry_future_price) / entry_spot_price

    return InitialPositionConfig(
        quantity=quantity,
        direction=direction,
        entry_basis_pct=entry_basis_pct,
        entry_spot_price=entry_spot_price,
        entry_future_price=entry_future_price,
    )


def parse_optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def parse_optional_positive_float(value: Any, name: str) -> float | None:
    parsed = parse_optional_float(value)
    if parsed is not None and parsed <= 0:
        raise ValueError(f"{name} must be > 0")
    return parsed


def parse_optional_non_negative_float(value: Any, field_name: str) -> float | None:
    parsed = parse_optional_float(value)
    if parsed is not None and parsed < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return parsed


def parse_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    parsed = int(value)
    if parsed < 0:
        raise ValueError("max_total_pairs must be >= 0")
    return parsed


def parse_position_direction(value: Any) -> Signal:
    text = str(value or Signal.HOLD.value).strip().upper()
    aliases = {
        "HOLD": Signal.HOLD,
        "NONE": Signal.HOLD,
        "FLAT": Signal.HOLD,
        "LONG_SPOT_SHORT_FUTURE": Signal.ENTER_LONG_SPOT_SHORT_FUTURE,
        "LS_SF": Signal.ENTER_LONG_SPOT_SHORT_FUTURE,
        "ENTER_LONG_SPOT_SHORT_FUTURE": Signal.ENTER_LONG_SPOT_SHORT_FUTURE,
        "SHORT_SPOT_LONG_FUTURE": Signal.ENTER_SHORT_SPOT_LONG_FUTURE,
        "SS_LF": Signal.ENTER_SHORT_SPOT_LONG_FUTURE,
        "ENTER_SHORT_SPOT_LONG_FUTURE": Signal.ENTER_SHORT_SPOT_LONG_FUTURE,
    }
    if text not in aliases:
        raise ValueError(f"Unsupported initial_position.direction: {value}")
    return aliases[text]


def build_initial_position(pair: PairConfig) -> PairPosition:
    initial = pair.initial_position
    stock_units = 0.0
    future_units = 0.0
    if initial.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        stock_units = float(initial.quantity)
        future_units = float(initial.quantity)
    elif initial.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        stock_units = -float(initial.quantity)
        future_units = -float(initial.quantity)

    position = PairPosition(
        pair_name=pair.name,
        quantity=float(initial.quantity),
        direction=initial.direction,
        entry_basis_pct=initial.entry_basis_pct,
        entry_spot_price=initial.entry_spot_price,
        entry_future_price=initial.entry_future_price,
        stock_units=stock_units,
        future_units=future_units,
    )
    if position.has_position:
        basis_text = "unknown" if position.entry_basis_pct is None else pct(position.entry_basis_pct)
        logging.info(
            "[%s] loaded initial position quantity=%s direction=%s entry_basis=%s",
            pair.name,
            position.quantity,
            position.direction.value,
            basis_text,
        )
    return position
