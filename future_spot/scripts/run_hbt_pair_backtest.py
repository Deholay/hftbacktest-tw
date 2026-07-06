from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
for path in (PROJECT_ROOT, WORKSPACE_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from arbitrage.config import load_config  # noqa: E402
from arbitrage.hbt_backtest import (  # noqa: E402
    HbtPairBacktestConfig,
    HbtPairBacktester,
)
from scripts.hbt_types import HbtAssetConfig  # noqa: E402
from scripts.io_utils import ms_to_ns  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a two-asset HftBacktest futures/spot arbitrage backtest.")
    parser.add_argument("--config", default="arbitrage_config_20260702.json")
    parser.add_argument("--pair-name", default=None)
    parser.add_argument("--spot-symbol", default=None)
    parser.add_argument("--future-symbol", default=None)
    parser.add_argument("--spot-data", type=Path, required=True)
    parser.add_argument("--future-data", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("output/hbt_pair_backtest"))
    parser.add_argument("--first-leg", choices=["stock", "future"], default="future")
    parser.add_argument("--step-ms", type=float, default=1000.0)
    parser.add_argument("--order-latency-ms", type=float, default=0.0)
    parser.add_argument("--response-latency-ms", type=float, default=0.0)
    parser.add_argument("--feed-latency-offset-ms", type=float, default=0.0)
    parser.add_argument("--second-leg-delay-ms", type=float, default=0.0)
    parser.add_argument("--response-timeout-ms", type=float, default=50.0)
    parser.add_argument("--max-steps", type=int, default=200_000)
    parser.add_argument("--max-trades", type=int, default=None)
    parser.add_argument("--record-market-every-steps", type=int, default=None)
    parser.add_argument("--queue-model", default="risk_adverse")
    parser.add_argument("--spot-tick-size", type=float, default=None)
    parser.add_argument("--future-tick-size", type=float, default=None)
    parser.add_argument("--entry-threshold-pct", type=float, default=None)
    parser.add_argument("--exit-threshold-pct", type=float, default=None)
    parser.add_argument("--min-effective-tick-multiple", type=float, default=None)
    parser.add_argument("--min-second-leg-adjusted-basis-pct", type=float, default=None)
    parser.add_argument("--no-second-leg-profit-check", action="store_true")
    parser.add_argument("--no-flatten", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(resolve_path(args.config, PROJECT_ROOT))
    pair = select_pair(config.pairs, args.pair_name, args.spot_symbol, args.future_symbol)
    pair = apply_pair_overrides(pair, args)

    order_latency_ns = ms_to_ns(args.order_latency_ms)
    response_latency_ns = ms_to_ns(args.response_latency_ms)
    feed_latency_ns = ms_to_ns(args.feed_latency_offset_ms)
    run_config = HbtPairBacktestConfig(
        pair=pair,
        spot=HbtAssetConfig(
            symbol=pair.spot_symbol,
            data=resolve_path(args.spot_data, WORKSPACE_ROOT),
            instrument="stock",
            contract_size=1000.0,
            tick_size=args.spot_tick_size,
            order_entry_latency_ns=order_latency_ns,
            order_response_latency_ns=response_latency_ns,
            feed_latency_offset_ns=feed_latency_ns,
            queue_model=args.queue_model,
        ),
        future=HbtAssetConfig(
            symbol=pair.future_symbol,
            data=resolve_path(args.future_data, WORKSPACE_ROOT),
            instrument="future",
            contract_size=float(pair.future_pnl_multiplier),
            tick_size=args.future_tick_size,
            order_entry_latency_ns=order_latency_ns,
            order_response_latency_ns=response_latency_ns,
            feed_latency_offset_ns=feed_latency_ns,
            queue_model=args.queue_model,
        ),
        first_leg=args.first_leg,
        step_ns=ms_to_ns(args.step_ms),
        response_timeout_ns=ms_to_ns(args.response_timeout_ms),
        second_leg_delay_ns=ms_to_ns(args.second_leg_delay_ms),
        max_steps=args.max_steps,
        max_trades=args.max_trades,
        flatten_on_second_leg_failure=not args.no_flatten,
        second_leg_profit_check=not args.no_second_leg_profit_check,
        record_market_every_steps=args.record_market_every_steps,
    )

    backtester = HbtPairBacktester(run_config)
    trades, summary = backtester.run()
    market = backtester.market_frame()
    output_dir = resolve_path(args.output_dir, PROJECT_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    trades_path = output_dir / "trades.csv"
    summary_path = output_dir / "summary.csv"
    market_path = output_dir / "market.csv"
    trades.to_csv(trades_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    if not market.empty:
        market.to_csv(market_path, index=False, encoding="utf-8-sig")
    print(f"trades={trades_path}")
    print(f"summary={summary_path}")
    if not market.empty:
        print(f"market={market_path}")
    if not summary.empty:
        print(summary.to_string(index=False))


def select_pair(pairs, pair_name: str | None, spot_symbol: str | None, future_symbol: str | None):
    candidates = list(pairs)
    if pair_name:
        candidates = [pair for pair in candidates if pair.name == pair_name]
    if spot_symbol:
        candidates = [pair for pair in candidates if pair.spot_symbol == spot_symbol]
    if future_symbol:
        candidates = [pair for pair in candidates if pair.future_symbol == future_symbol]
    if len(candidates) != 1:
        names = ", ".join(pair.name for pair in candidates[:10])
        raise ValueError(f"pair selector must match exactly one pair; matched={len(candidates)} {names}")
    return candidates[0]


def apply_pair_overrides(pair, args: argparse.Namespace):
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


def resolve_path(path: str | Path, base: Path) -> Path:
    value = Path(path)
    if value.is_absolute():
        return value
    if value.exists():
        return value.resolve()
    if value.parts and value.parts[0] == PROJECT_ROOT.name:
        return (PROJECT_ROOT.parent / value).resolve()
    return (base / value).resolve()
if __name__ == "__main__":
    main()
