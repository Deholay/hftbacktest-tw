"""Aggressive fill smoke test for Taiwan stock HftBacktest events.

This script intentionally crosses the spread with GTC limit orders so the
backtest must produce fills. It prints state after each step to verify position,
balance, fee, marked equity, and trade counters.
"""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

import numpy as np


def import_hftbacktest(workspace_root: Path):
    """Import installed hftbacktest while avoiding the local repo path shadow."""
    root = workspace_root.resolve()
    original_path = list(sys.path)
    try:
        filtered_path: list[str] = []
        for path_entry in original_path:
            if path_entry == "":
                continue
            try:
                if Path(path_entry).resolve() == root:
                    continue
            except OSError:
                pass
            filtered_path.append(path_entry)
        sys.path = filtered_path
        sys.modules.pop("hftbacktest", None)
        module = importlib.import_module("hftbacktest")
        if not hasattr(module, "BacktestAsset"):
            raise ImportError(f"hftbacktest loaded from {getattr(module, '__file__', None)} without BacktestAsset")
        return module
    finally:
        sys.path = original_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("data/tw_stock_events/2330_20250909.npz"),
        help="HftBacktest .npz event file.",
    )
    parser.add_argument("--contract-size", type=float, default=1000.0, help="Shares per board lot.")
    parser.add_argument("--tick-size", type=float, default=5.0)
    parser.add_argument("--lot-size", type=float, default=1.0)
    parser.add_argument("--qty", type=float, default=1.0, help="Order qty in board lots.")
    parser.add_argument("--round-trips", type=int, default=1, help="Number of buy-hit/sell-hit pairs.")
    parser.add_argument("--warmup-ns", type=int, default=1_000_000_000, help="Initial elapse time before first order.")
    parser.add_argument("--response-timeout-ns", type=int, default=10_000_000, help="Order response wait timeout.")
    return parser.parse_args()


def build_backtest(hbtpkg, args: argparse.Namespace):
    asset = (
        hbtpkg.BacktestAsset()
        .data(str(args.data))
        .linear_asset(args.contract_size)
        .constant_order_latency(0, 0)
        .risk_adverse_queue_model()
        .no_partial_fill_exchange()
        .trading_value_fee_model(0.0, 0.0)
        .tick_size(args.tick_size)
        .lot_size(args.lot_size)
        .last_trades_capacity(100)
    )
    return hbtpkg.HashMapMarketDepthBacktest([asset])


def state_snapshot(hbt, asset_no: int, contract_size: float) -> dict[str, float]:
    depth = hbt.depth(asset_no)
    state = hbt.state_values(asset_no)
    best_bid = float(depth.best_bid)
    best_ask = float(depth.best_ask)
    if np.isfinite(best_bid) and np.isfinite(best_ask):
        mark_px = (best_bid + best_ask) / 2.0
    elif np.isfinite(best_bid):
        mark_px = best_bid
    elif np.isfinite(best_ask):
        mark_px = best_ask
    else:
        mark_px = 0.0
    position = float(state.position)
    balance = float(state.balance)
    fee = float(state.fee)
    equity = balance + position * mark_px * contract_size - fee
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mark_px": mark_px,
        "position": position,
        "balance": balance,
        "fee": fee,
        "equity": equity,
        "num_trades": int(state.num_trades),
        "trading_value": float(state.trading_value),
        "trading_volume": float(state.trading_volume),
    }


def print_state(label: str, order_id: int | None, snap: dict[str, float]) -> None:
    order_text = "" if order_id is None else f" order_id={order_id}"
    print(
        f"{label:<18}{order_text:<14}"
        f" bid={snap['best_bid']:.2f}"
        f" ask={snap['best_ask']:.2f}"
        f" mark={snap['mark_px']:.2f}"
        f" pos={snap['position']:.4f}"
        f" balance={snap['balance']:.2f}"
        f" fee={snap['fee']:.2f}"
        f" equity={snap['equity']:.2f}"
        f" trades={int(snap['num_trades'])}"
        f" value={snap['trading_value']:.2f}"
        f" volume={snap['trading_volume']:.4f}"
    )


def wait_for_bbo(hbt, asset_no: int, warmup_ns: int, max_steps: int = 10) -> None:
    for _ in range(max_steps):
        result = hbt.elapse(warmup_ns)
        if result != 0:
            raise RuntimeError("Backtest ended before a valid BBO was available.")
        depth = hbt.depth(asset_no)
        if np.isfinite(depth.best_bid) and np.isfinite(depth.best_ask):
            return
    raise RuntimeError("No valid BBO found during warmup.")


def submit_and_report(
    hbt,
    hbtpkg,
    asset_no: int,
    order_id: int,
    side: str,
    px: float,
    qty: float,
    response_timeout_ns: int,
    contract_size: float,
) -> None:
    before = state_snapshot(hbt, asset_no, contract_size)
    print_state(f"before_{side}", order_id, before)

    if side == "buy":
        rc = hbt.submit_buy_order(asset_no, order_id, px, qty, hbtpkg.GTC, hbtpkg.LIMIT, False)
    elif side == "sell":
        rc = hbt.submit_sell_order(asset_no, order_id, px, qty, hbtpkg.GTC, hbtpkg.LIMIT, False)
    else:
        raise ValueError(f"unknown side: {side}")
    print(f"submit_{side:<11} order_id={order_id:<6} px={px:.2f} qty={qty:.4f} rc={rc}")

    response = hbt.wait_order_response(asset_no, order_id, response_timeout_ns)
    after = state_snapshot(hbt, asset_no, contract_size)
    print(f"response_{side:<9} order_id={order_id:<6} response={response}")
    print_state(f"after_{side}", order_id, after)

    hbt.clear_inactive_orders(asset_no)
    print_state(f"clear_{side}", order_id, state_snapshot(hbt, asset_no, contract_size))


def main() -> int:
    args = parse_args()
    workspace_root = Path(__file__).resolve().parents[1]
    args.data = args.data if args.data.is_absolute() else workspace_root / args.data
    if not args.data.exists():
        raise FileNotFoundError(args.data)

    hbtpkg = import_hftbacktest(workspace_root)
    print(f"hftbacktest={getattr(hbtpkg, '__version__', 'unknown')} file={getattr(hbtpkg, '__file__', None)}")
    print(f"data={args.data}")
    print("fee model: maker=0, taker=0; order latency: 0 ns; qty unit: board lots")

    hbt = build_backtest(hbtpkg, args)
    asset_no = 0
    try:
        wait_for_bbo(hbt, asset_no, args.warmup_ns)
        print_state("initial_bbo", None, state_snapshot(hbt, asset_no, args.contract_size))

        next_order_id = 10_001
        for round_no in range(1, args.round_trips + 1):
            depth = hbt.depth(asset_no)
            print(f"\nround={round_no} aggressive buy at best ask")
            submit_and_report(
                hbt,
                hbtpkg,
                asset_no,
                next_order_id,
                "buy",
                float(depth.best_ask),
                args.qty,
                args.response_timeout_ns,
                args.contract_size,
            )
            next_order_id += 1

            depth = hbt.depth(asset_no)
            print(f"\nround={round_no} aggressive sell at best bid")
            submit_and_report(
                hbt,
                hbtpkg,
                asset_no,
                next_order_id,
                "sell",
                float(depth.best_bid),
                args.qty,
                args.response_timeout_ns,
                args.contract_size,
            )
            next_order_id += 1

        print("\nfinal")
        print_state("final_state", None, state_snapshot(hbt, asset_no, args.contract_size))
    finally:
        hbt.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
