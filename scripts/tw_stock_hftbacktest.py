"""Reusable HftBacktest setup and reporting helpers for Taiwan stock notebooks."""

from __future__ import annotations

import importlib
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

try:
    from .tw_stock_data_to_npz import (
        BUY_EVENT,
        DEPTH_CLEAR_EVENT,
        DEPTH_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        EVENT_FLAG_MASK,
        SELL_EVENT,
        TRADE_EVENT,
        event_kind,
    )
except ImportError:
    from tw_stock_data_to_npz import (
        BUY_EVENT,
        DEPTH_CLEAR_EVENT,
        DEPTH_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        EVENT_FLAG_MASK,
        SELL_EVENT,
        TRADE_EVENT,
        event_kind,
    )


@dataclass(frozen=True)
class BacktestConfig:
    data: Path
    contract_size: float = 1000.0
    tick_size: float = 5.0
    lot_size: float = 1.0
    maker_fee: float = 0.0
    taker_fee: float = 0.0
    order_latency_ns: int = 1_000_000
    last_trades_capacity: int = 100
    queue_model: str = "risk_adverse"
    queue_model_param: float = 3.0


def workspace_root(anchor: Path | None = None) -> Path:
    if anchor is not None:
        start = anchor.resolve()
    else:
        start = Path.cwd().resolve()
    if start.is_file():
        start = start.parent
    for path in (start, *start.parents):
        if (path / "scripts").is_dir() and (path / "notebooks").is_dir():
            return path
    raise FileNotFoundError(f"cannot find workspace root from {start}")


def default_data_path(root: Path | None = None, symbol: str = "2330", date: str = "20250909") -> Path:
    base = workspace_root() if root is None else root
    return base / "data" / "tw_stock_events" / f"{symbol}_{date}.npz"


def import_hftbacktest(workspace: Path | None = None):
    """Import installed hftbacktest while avoiding the local repo path shadow."""
    root = workspace_root() if workspace is None else workspace.resolve()
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


def load_event_data(path: Path) -> np.ndarray:
    return np.load(path)["data"]


def event_summary(data: np.ndarray) -> dict[str, Any]:
    kinds = np.array([event_kind(int(ev)) for ev in data["ev"]])
    return {
        "rows": len(data),
        "dtype": data.dtype,
        "first": data[0] if len(data) else None,
        "last": data[-1] if len(data) else None,
        "first_exch_ts": int(data["exch_ts"][0]) if len(data) else None,
        "last_exch_ts": int(data["exch_ts"][-1]) if len(data) else None,
        "min_latency_ns": int(np.min(data["local_ts"] - data["exch_ts"])) if len(data) else None,
        "max_latency_ns": int(np.max(data["local_ts"] - data["exch_ts"])) if len(data) else None,
        "depth_events": int(np.sum(np.isin(kinds, [DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT]))),
        "trade_events": int(np.sum(kinds == TRADE_EVENT)),
        "unique_event_kinds": sorted(set(kinds.tolist())),
    }


def replay_bbo_summary(data: np.ndarray, sample_limit: int = 10) -> dict[str, Any]:
    bid_depth: dict[float, float] = {}
    ask_depth: dict[float, float] = {}
    samples: list[tuple[int, float, float, float, float]] = []
    crossed = 0
    last_ts: int | None = None

    def check_batch(ts: int | None) -> None:
        nonlocal crossed
        if ts is None or not bid_depth or not ask_depth:
            return
        best_bid = max(bid_depth)
        best_ask = min(ask_depth)
        if best_bid >= best_ask:
            crossed += 1
        if len(samples) < sample_limit:
            samples.append((int(ts), best_bid, best_ask, bid_depth[best_bid], ask_depth[best_ask]))

    for row in data:
        ts = int(row["exch_ts"])
        if last_ts is not None and ts != last_ts:
            check_batch(last_ts)
        last_ts = ts

        ev = int(row["ev"])
        kind = event_kind(ev)
        px = float(row["px"])
        qty = float(row["qty"])

        if ev & BUY_EVENT:
            if kind == DEPTH_CLEAR_EVENT:
                for price in list(bid_depth):
                    if price >= px:
                        del bid_depth[price]
            elif kind in (DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT):
                if qty > 0:
                    bid_depth[px] = qty
                else:
                    bid_depth.pop(px, None)
        elif ev & SELL_EVENT:
            if kind == DEPTH_CLEAR_EVENT:
                for price in list(ask_depth):
                    if price <= px:
                        del ask_depth[price]
            elif kind in (DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT):
                if qty > 0:
                    ask_depth[px] = qty
                else:
                    ask_depth.pop(px, None)

    check_batch(last_ts)
    return {"crossed_batches": crossed, "samples": samples}


def apply_queue_model(asset, config: BacktestConfig):
    if config.queue_model == "risk_adverse":
        return asset.risk_adverse_queue_model()
    if config.queue_model == "log_prob":
        return asset.log_prob_queue_model()
    if config.queue_model == "log_prob2":
        return asset.log_prob_queue_model2()
    if config.queue_model == "power_prob":
        return asset.power_prob_queue_model(config.queue_model_param)
    if config.queue_model == "power_prob2":
        return asset.power_prob_queue_model2(config.queue_model_param)
    if config.queue_model == "power_prob3":
        return asset.power_prob_queue_model3(config.queue_model_param)
    raise ValueError(f"unknown queue_model: {config.queue_model}")


def build_asset(hbtpkg, config: BacktestConfig):
    asset = (
        hbtpkg.BacktestAsset()
        .data(str(config.data))
        .linear_asset(config.contract_size)
        .constant_order_latency(config.order_latency_ns, config.order_latency_ns)
        .no_partial_fill_exchange()
        .trading_value_fee_model(config.maker_fee, config.taker_fee)
        .tick_size(config.tick_size)
        .lot_size(config.lot_size)
        .last_trades_capacity(config.last_trades_capacity)
    )
    return apply_queue_model(asset, config)


def build_backtest(config: BacktestConfig, hbtpkg=None):
    package = import_hftbacktest(workspace_root(config.data)) if hbtpkg is None else hbtpkg
    return package.HashMapMarketDepthBacktest([build_asset(package, config)])


def state_snapshot(hbt, asset_no: int = 0, contract_size: float = 1000.0) -> dict[str, float]:
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


def wait_for_bbo(hbt, asset_no: int = 0, step_ns: int = 1_000_000_000, max_steps: int = 10) -> None:
    for _ in range(max_steps):
        result = hbt.elapse(step_ns)
        if result != 0:
            raise RuntimeError("Backtest ended before a valid BBO was available.")
        depth = hbt.depth(asset_no)
        if np.isfinite(depth.best_bid) and np.isfinite(depth.best_ask):
            return
    raise RuntimeError("No valid BBO found during warmup.")


def submit_limit_order(hbt, hbtpkg, asset_no: int, order_id: int, side: str, px: float, qty: float) -> int:
    if side == "buy":
        return hbt.submit_buy_order(asset_no, order_id, px, qty, hbtpkg.GTC, hbtpkg.LIMIT, False)
    if side == "sell":
        return hbt.submit_sell_order(asset_no, order_id, px, qty, hbtpkg.GTC, hbtpkg.LIMIT, False)
    raise ValueError(f"unknown side: {side}")


def close_backtest(hbt) -> None:
    close = getattr(hbt, "close", None)
    if close is not None:
        close()
