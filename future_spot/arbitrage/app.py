from __future__ import annotations

import argparse
import logging
import sys
import threading
from datetime import datetime, time
from pathlib import Path

from .config import build_initial_position, load_config
from .models import AppConfig, MarketDataProvider, Mode, PairConfig, PairMarket, PairPosition
from .providers import FubonMarketDataProvider, HistoricalParquetReplayProvider, SimulatedMarketDataProvider
from .models import Signal
from .strategy import ExecutionEngine, PairPricer, RiskManager, SignalEngine, StopLossAwareSignalEngine
from .utils import pct


class MicrosecondFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        timestamp = datetime.fromtimestamp(record.created)
        if datefmt:
            return timestamp.strftime(datefmt)
        return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")


def run(
    config: AppConfig,
    provider: MarketDataProvider,
    iterations: int | None,
    stop_event: threading.Event,
    confirm_startup_positions: bool = False,
    enforce_risk_limits_in_paper: bool = False,
) -> tuple[ExecutionEngine, dict[str, PairPosition]]:
    pricer = PairPricer()
    signal_engine = StopLossAwareSignalEngine()
    risk = RiskManager()
    execution = ExecutionEngine(config.mode, config.allow_live_order, provider, config.backtest_execution)
    positions = {pair.name: build_initial_position(pair) for pair in config.pairs}
    set_position_store = getattr(provider, "set_position_store", None)
    if set_position_store is not None:
        set_position_store(positions)
    verify_startup_positions = getattr(provider, "verify_startup_positions", None)
    if config.mode != Mode.PAPER and verify_startup_positions is not None:
        verify_startup_positions(positions)
    if confirm_startup_positions:
        confirm_positions_before_start(config, positions)

    pairs_by_future: dict[str, list[PairConfig]] = {}
    pairs_by_stock: dict[str, list[PairConfig]] = {}
    for pair in config.pairs:
        pairs_by_future.setdefault(pair.future_symbol, []).append(pair)
        pairs_by_stock.setdefault(pair.spot_symbol, []).append(pair)

    try:
        count = 0
        while not stop_event.is_set() and (iterations is None or count < iterations):
            quote_update = provider.wait_for_future_update(timeout=1.0)
            if quote_update is None:
                if provider.is_finished():
                    logging.info("market data provider finished; stopping run loop")
                    break
                continue

            if quote_update.source == "future":
                pairs = pairs_by_future.get(quote_update.symbol, [])
            elif quote_update.source == "stock":
                pairs = pairs_by_stock.get(quote_update.symbol, [])
            else:
                logging.debug("received unconfigured quote update source=%s symbol=%s", quote_update.source, quote_update.symbol)
                continue
            if not pairs:
                logging.debug("received unconfigured %s update: %s", quote_update.source, quote_update.symbol)
                continue

            for pair in pairs:
                if stop_event.is_set():
                    break
                evaluate_pair(
                    config,
                    pair,
                    provider,
                    pricer,
                    signal_engine,
                    risk,
                    execution,
                    positions,
                    positions[pair.name],
                    enforce_risk_limits_in_paper=enforce_risk_limits_in_paper,
                )
                count += 1
                if iterations is not None and count >= iterations:
                    break
    finally:
        execution.print_summary(positions)
    return execution, positions


def confirm_positions_before_start(config: AppConfig, positions: dict[str, PairPosition]) -> None:
    print_startup_positions(config, positions)
    if not sys.stdin.isatty():
        logging.warning("stdin is not interactive; skipping startup position confirmation")
        return
    try:
        input("Press Enter to start arbitrage loop...")
    except EOFError:
        logging.warning("stdin closed while waiting for startup position confirmation; continuing startup")


def print_startup_positions(config: AppConfig, positions: dict[str, PairPosition]) -> None:
    pairs_by_name = {pair.name: pair for pair in config.pairs}
    active_positions = [
        position
        for position in positions.values()
        if position.has_position or position.has_leg_exposure
    ]

    print("\n=== Startup Positions ===")
    print(f"mode={config.mode.value} active_positions={len(active_positions)}")
    if not active_positions:
        print("no initial positions")
        print("=== End Startup Positions ===")
        return

    for position in sorted(active_positions, key=lambda item: item.pair_name):
        pair = pairs_by_name.get(position.pair_name)
        symbols = ""
        if pair is not None:
            symbols = f" spot={pair.spot_symbol} future={pair.future_symbol}"
        basis_text = "unknown" if position.entry_basis_pct is None else pct(position.entry_basis_pct)
        spot_price = "unknown" if position.entry_spot_price is None else f"{position.entry_spot_price:.2f}"
        future_price = "unknown" if position.entry_future_price is None else f"{position.entry_future_price:.2f}"
        print(
            f"{position.pair_name}:{symbols} quantity={position.quantity:g} "
            f"direction={position.direction.value} stock_units={position.stock_units:g} "
            f"future_units={position.future_units:g} entry_basis={basis_text} "
            f"entry_spot={spot_price} entry_future={future_price}"
        )
    print("=== End Startup Positions ===")


def evaluate_pair(
    config: AppConfig,
    pair: PairConfig,
    provider: MarketDataProvider,
    pricer: PairPricer,
    signal_engine: SignalEngine,
    risk: RiskManager,
    execution: ExecutionEngine,
    positions: dict[str, PairPosition],
    position: PairPosition,
    enforce_risk_limits_in_paper: bool = False,
) -> None:
    try:
        market = provider.get_pair_market(pair)
        pricing = pricer.price(market)
        signal = signal_engine.evaluate(pair, pricing, position)
        session_ok, session_reason = trading_session_check(config, market)
        freshness_ok, freshness_reason = quote_freshness_check(config, market)
        if signal != Signal.HOLD and not session_ok:
            signal = Signal.HOLD
        if signal != Signal.HOLD and not freshness_ok:
            signal = Signal.HOLD
        enforce_risk_limits = config.mode != Mode.PAPER or enforce_risk_limits_in_paper
        max_total_pairs = config.max_total_pairs if enforce_risk_limits else None
        max_total_spot_notional = config.max_total_spot_notional if enforce_risk_limits else None
        total_open_pairs = 0 if max_total_pairs is None else sum(abs(item.quantity) for item in positions.values())
        total_spot_notional = (
            0 if max_total_spot_notional is None else calculate_total_spot_notional(config, positions)
        )
        ok, reason = risk.check(
            pair,
            market,
            signal,
            position,
            total_open_pairs=total_open_pairs,
            max_total_pairs=max_total_pairs,
            total_spot_notional=total_spot_notional,
            max_total_spot_notional=max_total_spot_notional,
            enforce_pair_max=enforce_risk_limits,
        )
        if not session_ok:
            ok, reason = False, session_reason
        elif not freshness_ok:
            ok, reason = False, freshness_reason

        spread_parts = [
            f"LS/SF={pct(pricing.long_spot_short_future_pct)}",
            f"LS/SF_ticks={pricing.long_spot_short_future_ticks:.2f}",
        ]
        if pair.allow_short_spot:
            spread_parts.append(f"SS/LF={pct(pricing.short_spot_long_future_pct)}")
            spread_parts.append(f"SS/LF_ticks={pricing.short_spot_long_future_ticks:.2f}")
        spot_raw = market.spot.raw or {}
        future_raw = market.future.raw or {}
        trigger_text = (
            f"{market.trigger_source}:{market.trigger_symbol}"
            if market.trigger_source and market.trigger_symbol
            else "unknown"
        )
        spot_quote_time = quote_time_text(spot_raw)
        future_quote_time = quote_time_text(future_raw)
        spot_trial_match = spot_raw.get("status_trial_status_tag")
        time_parts = []
        if spot_quote_time is not None:
            time_parts.append(f"spot_quote_time={spot_quote_time}")
        if spot_trial_match is not None:
            time_parts.append(f"spot_trial_match={spot_trial_match}")
        if future_quote_time is not None:
            time_parts.append(f"future_quote_time={future_quote_time}")
        time_text = f" {' '.join(time_parts)}" if time_parts else ""

        def log_market_row() -> None:
            print(f"[{pair.name}] signal={signal.value} risk_check={'OK' if ok else 'FAIL'} ({reason})")
            logging.info(
                (
                    "[%s] spot bid/ask %.2f/%.2f future bid/ask %.2f/%.2f "
                    "%s mid=%s trigger=%s signal=%s risk=%s%s"
                ),
                pair.name,
                market.spot.bid,
                market.spot.ask,
                market.future.bid,
                market.future.ask,
                " ".join(spread_parts),
                pct(pricing.mid_basis_pct),
                trigger_text,
                signal.value,
                reason,
                time_text,
            )

        should_log_before_execute = (
            not config.log_order_only
            and (not config.log_threshold_only or signal != Signal.HOLD)
        )
        if should_log_before_execute:
            log_market_row()

        if ok:
            execution.execute(signal, market, pricing, position)
            if config.log_order_only and signal != Signal.HOLD:
                log_market_row()
    except RuntimeError as exc:
        if str(exc).startswith("Waiting for websocket books quote"):
            logging.debug("[%s] %s", pair.name, exc)
            return
        if str(exc) == "LIVE mode requires allow_live_order=True":
            logging.warning("[%s] signal skipped because live order is disabled: %s", pair.name, signal.value)
            return
        logging.exception("[%s] failed to process pair", pair.name)
    except Exception:
        logging.exception("[%s] failed to process pair", pair.name)


def quote_time_text(raw: dict[str, object]) -> object | None:
    for key in (
        "exchtime_tw",
        "timestamp_tw",
        "exchtime",
        "timestamp",
        "time",
        "date",
        "datetime",
    ):
        value = raw.get(key)
        if value not in (None, ""):
            return value

    last_trade = raw.get("lastTrade")
    if isinstance(last_trade, dict):
        for key in ("time", "timestamp", "date", "datetime"):
            value = last_trade.get(key)
            if value not in (None, ""):
                return value
    return None


def trading_session_check(config: AppConfig, market: PairMarket) -> tuple[bool, str]:
    if not config.trading_session_start_time and not config.trading_session_end_time:
        return True, "session allowed"
    event_time = market_event_datetime(market)
    if event_time is None:
        return False, "cannot determine market event time for trading session"
    event_clock = event_time.time()
    if config.trading_session_start_time:
        start = parse_clock_time(config.trading_session_start_time)
        if event_clock < start:
            return False, f"outside trading session before {config.trading_session_start_time}"
    if config.trading_session_end_time:
        end = parse_clock_time(config.trading_session_end_time)
        if event_clock > end:
            return False, f"outside trading session after {config.trading_session_end_time}"
    return True, "session allowed"


def quote_freshness_check(config: AppConfig, market: PairMarket) -> tuple[bool, str]:
    max_age = config.backtest_execution.max_quote_age_sec
    if max_age is None:
        return True, "quotes fresh"
    event_time = market_event_datetime(market)
    spot_time = quote_raw_datetime(market.spot.raw)
    future_time = quote_raw_datetime(market.future.raw)
    if event_time is None or spot_time is None or future_time is None:
        return False, "cannot determine quote freshness"
    spot_age = (event_time - spot_time).total_seconds()
    future_age = (event_time - future_time).total_seconds()
    if spot_age < 0 or spot_age > max_age:
        return False, f"stale spot quote age={spot_age:.3f}s max={max_age:.3f}s"
    if future_age < 0 or future_age > max_age:
        return False, f"stale future quote age={future_age:.3f}s max={max_age:.3f}s"
    return True, "quotes fresh"


def market_event_datetime(market: PairMarket) -> datetime | None:
    if market.trigger_source == "stock":
        raws = [market.spot.raw, market.future.raw]
    elif market.trigger_source == "future":
        raws = [market.future.raw, market.spot.raw]
    else:
        raws = [market.spot.raw, market.future.raw]
    for raw in raws:
        value = quote_raw_datetime(raw)
        if value is not None:
            return value
    return None


def quote_raw_datetime(raw: dict[str, object] | None) -> datetime | None:
    if not raw:
        return None
    for key in ("exchtime_tw", "timestamp_tw", "exchtime", "timestamp", "time", "date", "datetime"):
        value = raw.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000_000:
                timestamp /= 1_000_000_000
            elif timestamp > 10_000_000_000:
                timestamp /= 1_000
            return datetime.fromtimestamp(timestamp)
        to_pydatetime = getattr(value, "to_pydatetime", None)
        if callable(to_pydatetime):
            return to_pydatetime()
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            continue
    return None


def parse_clock_time(value: str) -> time:
    text = value.strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"trading session time must be HH:MM or HH:MM:SS: {value}")


def calculate_total_spot_notional(config: AppConfig, positions: dict[str, PairPosition]) -> float:
    pairs_by_name = {pair.name: pair for pair in config.pairs}
    total = 0.0
    for pair_name, position in positions.items():
        pair = pairs_by_name.get(pair_name)
        if pair is None or position.entry_spot_price in (None, 0):
            continue
        units = abs(position.stock_units) if position.stock_units else abs(position.quantity)
        total += position.entry_spot_price * pair.spot_order_qty * units
    return total


def sync_system_time_on_start(config: AppConfig, use_historical_replay: bool, use_simulation: bool) -> None:
    if not config.sync_system_time_on_start:
        return
    if config.mode != Mode.LIVE or use_historical_replay or use_simulation:
        return

    try:
        import ntplib
    except ModuleNotFoundError:
        logging.warning("ntplib is not installed; skipping NTP time check. Install it with: pip install ntplib")
        return

    server = "pool.ntp.org"
    logging.info("checking NTP network time before live startup: %s", server)
    try:
        response = ntplib.NTPClient().request(server, timeout=5)
    except Exception as exc:
        logging.warning("NTP time check failed; continuing startup: %s", exc)
        return

    ntp_time = datetime.fromtimestamp(response.tx_time)
    local_time = datetime.now()
    offset_sec = response.tx_time - local_time.timestamp()
    logging.info(
        "NTP network time=%s local_time=%s offset_sec=%.3f",
        ntp_time.strftime("%Y-%m-%d %H:%M:%S"),
        local_time.strftime("%Y-%m-%d %H:%M:%S"),
        offset_sec,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stock futures vs spot arbitrage monitor")
    parser.add_argument("--config", default="arbitrage_config.example.json", help="Path to JSON config file.")
    parser.add_argument("--iterations", type=int, default=None, help="Stop after N futures quote events. Default runs forever.")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write runtime logs to this file. Default: output/logs/arbitrage_YYYYMMDD_HHMMSS.log.",
    )
    parser.add_argument("--simulate", action="store_true", help="Use simulated quotes instead of Fubon websockets.")
    parser.add_argument("--replay", action="store_true", help="Replay historical parquet quotes instead of Fubon websockets.")
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Skip the startup position confirmation prompt for scheduled or service runs.",
    )
    parser.add_argument(
        "--simulate-date",
        "--replay-date",
        dest="replay_date",
        default=None,
        help="Override historical replay date from config. Accepts YYYY-MM-DD or YYYYMMDD.",
    )
    return parser.parse_args()


def configure_logging(log_level: str, log_file: str | None) -> Path:
    log_path = Path(log_file) if log_file else default_log_path()
    if not log_path.is_absolute():
        log_path = Path.cwd() / log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = MicrosecondFormatter("%(asctime)s %(levelname)s %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=level,
        handlers=[console_handler, file_handler],
        force=True,
    )
    logging.info("runtime log file: %s", log_path)
    return log_path


def default_log_path() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("output") / "logs" / f"arbitrage_{timestamp}.log"


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level, args.log_file)

    stop_event = threading.Event()
    config = load_config(args.config, replay_date_override=args.replay_date)
    if args.replay_date:
        logging.info("historical replay date overridden by CLI: %s", config.historical.replay_date)
    provider: MarketDataProvider
    use_historical_replay = (
        args.replay
        or config.mode == Mode.REPLAY
        or (args.simulate and bool(config.historical.stock.path and config.historical.futures.path))
    )
    use_simulation = bool(args.simulate or config.mode == Mode.SIM) and not use_historical_replay
    sync_system_time_on_start(config, use_historical_replay, use_simulation)
    if use_historical_replay:
        if config.mode == Mode.LIVE:
            logging.info("historical replay selected; overriding LIVE mode to PAPER")
            config = config.with_mode(Mode.PAPER)
        elif config.mode in (Mode.REPLAY, Mode.SIM):
            config = config.with_mode(Mode.PAPER)
        provider = HistoricalParquetReplayProvider(config, stop_event)
    elif use_simulation:
        if config.mode == Mode.LIVE:
            logging.info("--simulate selected; overriding LIVE mode to PAPER")
            config = config.with_mode(Mode.PAPER)
        provider = SimulatedMarketDataProvider(config, stop_event)
    else:
        provider = FubonMarketDataProvider(config)

    try:
        run(config, provider, args.iterations, stop_event, confirm_startup_positions=not args.no_confirm)
    except KeyboardInterrupt:
        logging.info("received Ctrl+C, shutting down")
        stop_event.set()
    finally:
        provider.close()


if __name__ == "__main__":
    main()
