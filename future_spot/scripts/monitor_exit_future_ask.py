from __future__ import annotations

import argparse
import logging
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage.config import build_initial_position, load_config
from arbitrage.models import AppConfig, MarketDataProvider, Mode, PairConfig, PairPosition, Quote, Signal
from arbitrage.providers import FubonMarketDataProvider, HistoricalParquetReplayProvider, SimulatedMarketDataProvider
from arbitrage.strategy import PairPricer, RiskManager, StopLossAwareSignalEngine
from arbitrage.ticks import pair_leg_tick_size
from arbitrage.utils import pct


@dataclass(frozen=True)
class LongSpotShortFutureExitLevel:
    signal_max_ask: float | None
    pct_max_ask: float
    tick_max_ask: float | None
    pnl_max_ask: float | None
    final_max_ask: float | None
    exit_tick_rule: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor configured inventory positions and print the futures ask1 level "
            "that would allow exit when spot bid1 changes."
        )
    )
    parser.add_argument("--config", default="output/arbitrage_config_20260622.json", help="Path to JSON config file.")
    parser.add_argument("--iterations", type=int, default=None, help="Stop after N processed stock bid1 changes.")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write runtime logs to this file. Default: output/logs/exit_ask_YYYYMMDD_HHMMSS.log.",
    )
    parser.add_argument("--simulate", action="store_true", help="Use simulated quotes instead of Fubon websockets.")
    parser.add_argument("--replay", action="store_true", help="Replay historical parquet quotes instead of Fubon websockets.")
    parser.add_argument(
        "--simulate-date",
        "--replay-date",
        dest="replay_date",
        default=None,
        help="Override historical replay date from config. Accepts YYYY-MM-DD or YYYYMMDD.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level, args.log_file)
    config = load_config(args.config, replay_date_override=args.replay_date)
    active_pairs = active_position_pairs(config)
    if not active_pairs:
        raise RuntimeError(f"{args.config} has no pairs with initial_position.quantity > 0")
    config = config_with_pairs(config, active_pairs)

    stop_event = threading.Event()
    provider = build_provider(config, args, stop_event)
    try:
        monitor_exit_ask(config, provider, stop_event, iterations=args.iterations)
    except KeyboardInterrupt:
        logging.info("received Ctrl+C, shutting down")
        stop_event.set()
    finally:
        provider.close()


def active_position_pairs(config: AppConfig) -> tuple[PairConfig, ...]:
    return tuple(pair for pair in config.pairs if pair.initial_position.quantity > 0)


def config_with_pairs(config: AppConfig, pairs: tuple[PairConfig, ...]) -> AppConfig:
    return AppConfig(
        mode=config.mode,
        allow_live_order=False,
        poll_interval_sec=config.poll_interval_sec,
        max_total_pairs=config.max_total_pairs,
        max_total_spot_notional=config.max_total_spot_notional,
        log_threshold_only=config.log_threshold_only,
        sync_system_time_on_start=config.sync_system_time_on_start,
        fubon=config.fubon,
        historical=config.historical,
        pairs=pairs,
    )


def build_provider(config: AppConfig, args: argparse.Namespace, stop_event: threading.Event) -> MarketDataProvider:
    use_historical_replay = (
        args.replay
        or config.mode == Mode.REPLAY
        or (args.simulate and bool(config.historical.stock.path and config.historical.futures.path))
    )
    use_simulation = bool(args.simulate or config.mode == Mode.SIM) and not use_historical_replay

    if use_historical_replay:
        return HistoricalParquetReplayProvider(config.with_mode(Mode.PAPER), stop_event)
    if use_simulation:
        return SimulatedMarketDataProvider(config.with_mode(Mode.PAPER), stop_event)
    return FubonMarketDataProvider(config)


def monitor_exit_ask(
    config: AppConfig,
    provider: MarketDataProvider,
    stop_event: threading.Event,
    iterations: int | None,
) -> None:
    pricer = PairPricer()
    signal_engine = StopLossAwareSignalEngine()
    risk = RiskManager()
    positions = {pair.name: build_initial_position(pair) for pair in config.pairs}

    set_position_store = getattr(provider, "set_position_store", None)
    if set_position_store is not None:
        set_position_store(positions)
    verify_startup_positions = getattr(provider, "verify_startup_positions", None)
    if config.mode == Mode.LIVE and verify_startup_positions is not None:
        verify_startup_positions(positions)

    pairs_by_stock: dict[str, list[PairConfig]] = {}
    for pair in config.pairs:
        pairs_by_stock.setdefault(pair.spot_symbol, []).append(pair)

    last_spot_bid: dict[str, float] = {}
    processed = 0
    print_startup(config, positions)

    while not stop_event.is_set() and (iterations is None or processed < iterations):
        quote_update = provider.wait_for_future_update(timeout=1.0)
        if quote_update is None:
            if provider.is_finished():
                logging.info("market data provider finished; stopping monitor")
                break
            continue
        if quote_update.source != "stock":
            continue

        for pair in pairs_by_stock.get(quote_update.symbol, []):
            try:
                market = provider.get_pair_market(pair)
            except RuntimeError as exc:
                if str(exc).startswith("Waiting for websocket books quote"):
                    logging.debug("[%s] %s", pair.name, exc)
                    continue
                raise

            old_bid = last_spot_bid.get(pair.spot_symbol)
            if old_bid == market.spot.bid:
                continue
            last_spot_bid[pair.spot_symbol] = market.spot.bid

            position = positions[pair.name]
            pricing = pricer.price(market)
            signal = signal_engine.evaluate(pair, pricing, position)
            exit_ok, exit_reason = risk.check(pair, market, Signal.EXIT, position, enforce_pair_max=False)
            print_exit_line(pair, position, market, pricing, signal, exit_ok, exit_reason)
            processed += 1
            if iterations is not None and processed >= iterations:
                break


def print_startup(config: AppConfig, positions: dict[str, PairPosition]) -> None:
    print("\n=== Exit Ask Monitor ===")
    print(f"mode={config.mode.value} active_positions={len(config.pairs)}")
    for pair in config.pairs:
        position = positions[pair.name]
        print(
            f"{pair.name} spot={pair.spot_symbol} future={pair.future_symbol} "
            f"quantity={position.quantity:g} direction={position.direction.value} "
            f"entry_spot={format_optional_price(position.entry_spot_price)} "
            f"entry_future={format_optional_price(position.entry_future_price)}"
        )
    print("=== Waiting for spot bid1 changes ===")


def print_exit_line(
    pair: PairConfig,
    position: PairPosition,
    market: object,
    pricing: object,
    signal: Signal,
    risk_ok: bool,
    risk_reason: str,
) -> None:
    if position.direction != Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        print(
            f"[{pair.name}] spot_bid1={market.spot.bid:.2f} future_bid/ask={market.future.bid:.2f}/{market.future.ask:.2f} "
            f"direction={position.direction.value}; futures ask1 threshold is only defined for long-spot/short-future positions"
        )
        return

    level = calculate_long_spot_short_future_exit_level(pair, position, market.spot, market.future)
    now_text = "YES" if signal == Signal.EXIT and risk_ok else "NO"
    final_text = "N/A" if level.final_max_ask is None else f"{level.final_max_ask:.2f}"
    tick_text = "N/A" if level.tick_max_ask is None else f"{level.tick_max_ask:.2f}"
    pnl_text = "N/A" if level.pnl_max_ask is None else f"{level.pnl_max_ask:.2f}"
    print(
        f"[{pair.name}] spot_bid1={market.spot.bid:.2f} spot_ask1={market.spot.ask:.2f} "
        f"future_bid1={market.future.bid:.2f} future_ask1={market.future.ask:.2f} "
        f"exit_when_future_ask1<={final_text} now_exit={now_text} risk={risk_reason} "
        f"pct_limit={level.pct_max_ask:.2f} tick_limit={tick_text} pnl_limit={pnl_text} "
        f"exit_basis={pct(pricing.short_spot_long_future_pct)} "
        f"exit_ticks={pricing.long_spot_short_future_exit_ticks:.2f}"
    )


def calculate_long_spot_short_future_exit_level(
    pair: PairConfig,
    position: PairPosition,
    spot: Quote,
    future: Quote,
) -> LongSpotShortFutureExitLevel:
    pct_max_ask = (
        (1 + pair.exit_threshold_pct)
        * spot.bid
        * pair.spot_shares_per_pair
        / pair.future_shares_per_pair
    )
    tick_max_ask = calculate_tick_exit_ask(pair, spot.bid, future.ask)

    signal_limits = [pct_max_ask]
    if tick_max_ask is not None:
        signal_limits.append(tick_max_ask)
    signal_max_ask = max(signal_limits)

    pnl_max_ask = calculate_min_pnl_exit_ask(pair, position, spot.bid)
    final_max_ask = signal_max_ask
    if pnl_max_ask is not None:
        final_max_ask = min(final_max_ask, pnl_max_ask)

    return LongSpotShortFutureExitLevel(
        signal_max_ask=signal_max_ask,
        pct_max_ask=pct_max_ask,
        tick_max_ask=tick_max_ask,
        pnl_max_ask=pnl_max_ask,
        final_max_ask=final_max_ask,
        exit_tick_rule=pair.exit_tick_rule,
    )


def calculate_tick_exit_ask(pair: PairConfig, spot_bid: float, current_future_ask: float) -> float | None:
    if pair.exit_tick_rule != "lte":
        return None
    spot_tick_size = pair_leg_tick_size(pair, "stock", spot_bid, spot.raw)
    future_tick_size = pair_leg_tick_size(pair, "future", current_future_ask, future.raw)
    return spot_bid + pair.exit_tick_multiple * (spot_tick_size + future_tick_size)


def calculate_min_pnl_exit_ask(
    pair: PairConfig,
    position: PairPosition,
    spot_bid: float,
) -> float | None:
    if pair.min_exit_realized_pnl is None:
        return None
    if position.entry_spot_price is None or position.entry_future_price is None:
        return None

    quantity_multiplier = max(abs(position.quantity), 1)
    spot_qty = pair.spot_order_qty * quantity_multiplier
    future_multiplier = pair.future_pnl_multiplier * pair.future_order_qty * quantity_multiplier
    commission_rate = pair.stock_commission_rate * pair.stock_commission_discount
    entry_stock_fee = position.entry_spot_price * spot_qty * commission_rate
    exit_stock_fee = spot_bid * spot_qty * commission_rate
    stock_tax = spot_bid * spot_qty * pair.stock_transaction_tax_rate
    spot_pnl = (spot_bid - position.entry_spot_price) * spot_qty
    fixed_pnl = spot_pnl + (position.entry_future_price * future_multiplier)
    fixed_cost = entry_stock_fee + exit_stock_fee + stock_tax
    return (fixed_pnl - fixed_cost - pair.min_exit_realized_pnl) / future_multiplier


def format_optional_price(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.2f}"


def configure_logging(log_level: str, log_file: str | None) -> Path:
    log_path = Path(log_file) if log_file else default_log_path()
    if not log_path.is_absolute():
        log_path = Path.cwd() / log_path
    log_path.parent.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, log_level.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logging.basicConfig(level=level, handlers=[console_handler, file_handler], force=True)
    logging.info("runtime log file: %s", log_path)
    return log_path


def default_log_path() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("output") / "logs" / f"exit_ask_{timestamp}.log"


if __name__ == "__main__":
    main()
