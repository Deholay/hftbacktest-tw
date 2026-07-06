"""Reusable TW stock hftbacktest strategies and reporting helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from typing import Any

import numpy as np
import pandas as pd

try:
    from .hbt_common import get_order
    from .tw_stock_data_to_npz import (
        BUY_EVENT,
        DEPTH_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        SELL_EVENT,
        TRADE_EVENT,
        event_kind,
    )
    from .tw_stock_hftbacktest import (
        BacktestConfig,
        build_backtest,
        close_backtest,
        state_snapshot,
        submit_limit_order,
        wait_for_bbo,
    )
except ImportError:
    from hbt_common import get_order
    from tw_stock_data_to_npz import (
        BUY_EVENT,
        DEPTH_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        SELL_EVENT,
        TRADE_EVENT,
        event_kind,
    )
    from tw_stock_hftbacktest import (
        BacktestConfig,
        build_backtest,
        close_backtest,
        state_snapshot,
        submit_limit_order,
        wait_for_bbo,
    )


DEFAULT_QUEUE_MODELS = ["risk_adverse", "log_prob"]
STRATEGY3_COMPARISON_COLUMNS = [
    "candidate_id",
    "queue_model",
    "side",
    "order_id",
    "price",
    "qty",
    "send_order_ts",
    "send_order_time",
    "send_best_bid",
    "send_best_ask",
    "depth_level",
    "selected_qty",
    "max_window_s",
    "candidate_window_start_time",
    "depth_reduce_events",
    "depth_reduce_qty",
    "candidate_trade_events",
    "candidate_trade_qty",
    "candidate_score",
    "fill_ts",
    "fill_time",
    "fill_exch_ts",
    "fill_local_ts",
    "exec_qty",
    "exec_price",
    "fill_step",
    "fill_position",
    "fill_balance",
    "fill_equity",
    "was_filled",
    "time_to_fill_ns",
    "time_to_fill_s",
    "queue_model_fill_delta_ns",
]
LEVEL_COMPARISON_COLUMNS = [
    "queue_model",
    "side",
    "level",
    "actual_level",
    "order_id",
    "price",
    "qty",
    "send_best_bid",
    "send_best_ask",
    "send_order_ts",
    "send_order_time",
    "fill_ts",
    "fill_time",
    "was_filled",
    "time_to_fill_ns",
    "time_to_fill_s",
    "queue_model_fill_delta_ns",
    "exec_qty",
    "exec_price",
    "fill_step",
    "position",
    "balance",
    "equity",
]


def ns_to_datetime(ns: int | float | pd._libs.missing.NAType):
    if pd.isna(ns):
        return pd.NaT
    return pd.to_datetime(ns, unit="ns", utc=True).tz_convert("Asia/Taipei")


def infer_tick_size(event_data: np.ndarray, default: float = 5.0) -> float:
    prices = np.unique(event_data["px"][np.isfinite(event_data["px"]) & (event_data["px"] > 0)])
    if len(prices) < 2:
        return default
    diffs = np.diff(np.sort(prices))
    diffs = diffs[diffs > 0]
    return float(diffs.min()) if len(diffs) else default


def config_with_queue_model(config: BacktestConfig, queue_model: str) -> BacktestConfig:
    return replace(config, queue_model=queue_model)


def order_is_active(order, hbtpkg) -> bool:
    return order is not None and int(order.status) != hbtpkg.FILLED and float(order.leaves_qty) > 0


def event_local_ts_for_exch_ts(event_data: np.ndarray, exch_ts, fallback=pd.NA):
    if event_data is None or pd.isna(exch_ts):
        return fallback
    matches = event_data["exch_ts"] == int(exch_ts)
    if matches.any():
        return int(event_data["local_ts"][matches][0])
    return fallback


def with_backtest(config: BacktestConfig, hbtpkg, fn):
    hbt = build_backtest(config, hbtpkg)
    try:
        return fn(hbt)
    finally:
        close_backtest(hbt)


def record_order_state(
    hbt,
    hbtpkg,
    asset_no: int,
    config: BacktestConfig,
    label: str,
    strategy: str,
    *,
    event_data: np.ndarray | None = None,
    queue_model: str | None = None,
    round_no: int | None = None,
    side: str | None = None,
    order_id: int | None = None,
    px: float | None = None,
    qty: float | None = None,
    rc: int | None = None,
    response: int | None = None,
    step: int | None = None,
    is_fill: bool = False,
) -> dict[str, Any]:
    order = get_order(hbt, asset_no, order_id)
    current_ts = int(hbt.current_timestamp)
    exch_ts = fill_ts = order_exch_ts = order_local_ts = send_order_ts = pd.NA
    local_ts = current_ts
    order_status = exec_qty = exec_price = leaves_qty = pd.NA

    if order is not None:
        order_exch_ts = int(order.exch_timestamp)
        order_local_ts = int(order.local_timestamp)
        send_order_ts = order_local_ts
        exch_ts = order_exch_ts
        order_status = int(order.status)
        exec_qty = float(order.exec_qty)
        exec_price = float(order.exec_price)
        leaves_qty = float(order.leaves_qty)
        if is_fill:
            local_ts = event_local_ts_for_exch_ts(event_data, order_exch_ts, fallback=current_ts)
            fill_ts = local_ts

    row = {
        "strategy": strategy,
        "queue_model": queue_model,
        "label": label,
        "round": round_no,
        "step": step,
        "side": side,
        "order_id": order_id,
        "price": px,
        "qty": qty,
        "rc": rc,
        "response": response,
        "current_ts": current_ts,
        "exch_ts": exch_ts,
        "local_ts": local_ts,
        "fill_ts": fill_ts,
        "send_order_ts": send_order_ts,
        "order_exch_ts": order_exch_ts,
        "order_local_ts": order_local_ts,
        "current_time": ns_to_datetime(current_ts),
        "exch_time": ns_to_datetime(exch_ts),
        "local_time": ns_to_datetime(local_ts),
        "fill_time": ns_to_datetime(fill_ts),
        "send_order_time": ns_to_datetime(send_order_ts),
        "order_exch_time": ns_to_datetime(order_exch_ts),
        "order_local_time": ns_to_datetime(order_local_ts),
        "order_status": order_status,
        "exec_qty": exec_qty,
        "exec_price": exec_price,
        "leaves_qty": leaves_qty,
    }
    row.update(state_snapshot(hbt, asset_no, config.contract_size))
    return row


def run_aggressive_fill_strategy(
    config: BacktestConfig,
    hbtpkg,
    event_data: np.ndarray,
    qty: float = 1.0,
    round_trips: int = 1,
    response_timeout_ns: int = 10_000_000,
) -> pd.DataFrame:
    def run(hbt):
        asset_no = 0
        rows = []

        def record(label, round_no=None, side=None, order_id=None, px=None, rc=None, response=None, is_fill=False):
            rows.append(
                record_order_state(
                    hbt,
                    hbtpkg,
                    asset_no,
                    config,
                    label,
                    "aggressive_bbo",
                    event_data=event_data,
                    queue_model=config.queue_model,
                    round_no=round_no,
                    side=side,
                    order_id=order_id,
                    px=px,
                    qty=qty if side is not None else None,
                    rc=rc,
                    response=response,
                    is_fill=is_fill,
                )
            )

        wait_for_bbo(hbt, asset_no)
        record("initial_bbo")

        order_id = 10_001
        for round_no in range(1, round_trips + 1):
            depth = hbt.depth(asset_no)
            px = float(depth.best_ask)
            record("before_buy", round_no, "buy", order_id, px)
            rc = submit_limit_order(hbt, hbtpkg, asset_no, order_id, "buy", px, qty)
            response = hbt.wait_order_response(asset_no, order_id, response_timeout_ns)
            record("after_buy", round_no, "buy", order_id, px, rc, response, is_fill=True)
            hbt.clear_inactive_orders(asset_no)
            order_id += 1

            depth = hbt.depth(asset_no)
            px = float(depth.best_bid)
            record("before_sell", round_no, "sell", order_id, px)
            rc = submit_limit_order(hbt, hbtpkg, asset_no, order_id, "sell", px, qty)
            response = hbt.wait_order_response(asset_no, order_id, response_timeout_ns)
            record("after_sell", round_no, "sell", order_id, px, rc, response, is_fill=True)
            hbt.clear_inactive_orders(asset_no)
            order_id += 1

        record("final_state")
        return pd.DataFrame(rows)

    return with_backtest(config, hbtpkg, run)


def summarize_fills(output: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    fills = output[output["label"].eq("filled")].copy()
    if fills.empty:
        return fills
    fills = fills.sort_values(group_cols + ["fill_ts", "queue_model"]).reset_index(drop=True)
    fills["fill_rank"] = fills.groupby(group_cols).cumcount() + 1
    fills["time_to_fill_ns"] = fills["fill_ts"] - fills["send_order_ts"]
    fills["time_to_fill_s"] = fills["time_to_fill_ns"] / 1_000_000_000
    fills["queue_model_fill_delta_ns"] = fills["fill_ts"] - fills.groupby(group_cols)["fill_ts"].transform("min")
    return fills


def run_passive_bid_ask_strategy(
    config: BacktestConfig,
    hbtpkg,
    event_data: np.ndarray,
    queue_model: str,
    qty: float = 1.0,
    step_ns: int = 1_000_000_000,
    max_steps: int = 3_600,
    response_timeout_ns: int = 10_000_000,
) -> pd.DataFrame:
    queue_config = config_with_queue_model(config, queue_model)

    def run(hbt):
        asset_no = 0
        rows = []
        filled_order_ids = set()

        wait_for_bbo(hbt, asset_no)
        depth = hbt.depth(asset_no)
        orders = [
            {"side": "buy", "order_id": 20_001, "price": float(depth.best_bid)},
            {"side": "sell", "order_id": 20_002, "price": float(depth.best_ask)},
        ]

        rows.append(
            record_order_state(
                hbt,
                hbtpkg,
                asset_no,
                queue_config,
                "initial_bbo",
                "passive_bid_ask",
                event_data=event_data,
                queue_model=queue_model,
            )
        )

        for spec in orders:
            rc = submit_limit_order(hbt, hbtpkg, asset_no, spec["order_id"], spec["side"], spec["price"], qty)
            response = hbt.wait_order_response(asset_no, spec["order_id"], response_timeout_ns)
            order = get_order(hbt, asset_no, spec["order_id"])
            if order is not None and float(order.exec_qty) > 0:
                raise RuntimeError(f"passive {spec['side']} order filled on accept: {spec}")
            rows.append(
                record_order_state(
                    hbt,
                    hbtpkg,
                    asset_no,
                    queue_config,
                    "accepted",
                    "passive_bid_ask",
                    event_data=event_data,
                    queue_model=queue_model,
                    side=spec["side"],
                    order_id=spec["order_id"],
                    px=spec["price"],
                    qty=qty,
                    rc=rc,
                    response=response,
                    step=0,
                )
            )

        for step in range(1, max_steps + 1):
            active_orders = [
                spec for spec in orders if order_is_active(get_order(hbt, asset_no, spec["order_id"]), hbtpkg)
            ]
            if not active_orders:
                break
            if hbt.elapse(step_ns) != 0:
                break

            for spec in active_orders:
                order = get_order(hbt, asset_no, spec["order_id"])
                if order is None or spec["order_id"] in filled_order_ids:
                    continue
                if float(order.exec_qty) > 0 or int(order.status) == hbtpkg.FILLED:
                    rows.append(
                        record_order_state(
                            hbt,
                            hbtpkg,
                            asset_no,
                            queue_config,
                            "filled",
                            "passive_bid_ask",
                            event_data=event_data,
                            queue_model=queue_model,
                            side=spec["side"],
                            order_id=spec["order_id"],
                            px=spec["price"],
                            qty=qty,
                            step=step,
                            is_fill=True,
                        )
                    )
                    filled_order_ids.add(spec["order_id"])

        rows.append(
            record_order_state(
                hbt,
                hbtpkg,
                asset_no,
                queue_config,
                "final_state",
                "passive_bid_ask",
                event_data=event_data,
                queue_model=queue_model,
                step=step if "step" in locals() else 0,
            )
        )
        return pd.DataFrame(rows)

    return with_backtest(queue_config, hbtpkg, run)


def run_queue_model_comparison(
    config: BacktestConfig,
    hbtpkg,
    event_data: np.ndarray,
    queue_models: Sequence[str] = DEFAULT_QUEUE_MODELS,
    qty: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    output = pd.concat(
        [run_passive_bid_ask_strategy(config, hbtpkg, event_data, queue_model, qty=qty) for queue_model in queue_models],
        ignore_index=True,
    )
    fills = summarize_fills(output, ["side"])
    fills["fill_time_delta_ns"] = fills["queue_model_fill_delta_ns"]
    return output, fills


def build_event_frame(data: np.ndarray) -> pd.DataFrame:
    ev = data["ev"].astype(np.uint64)
    kind = np.array([event_kind(int(value)) for value in ev])
    side = np.where((ev & BUY_EVENT) == BUY_EVENT, "buy", np.where((ev & SELL_EVENT) == SELL_EVENT, "sell", ""))
    return pd.DataFrame(
        {
            "exch_ts": data["exch_ts"],
            "local_ts": data["local_ts"],
            "px": data["px"],
            "qty": data["qty"],
            "kind": kind,
            "side": side,
        }
    )


def scan_strategy3_candidates(
    data: np.ndarray,
    window_minutes: int = 30,
    min_depth_reduce_events: int = 20,
    max_trade_events: int = 8,
    require_trade: bool = True,
    limit: int = 40,
) -> pd.DataFrame:
    events = build_event_frame(data)
    window_ns = int(window_minutes * 60 * 1_000_000_000)
    first_ts = int(events["exch_ts"].min())
    events["window_start"] = ((events["exch_ts"] - first_ts) // window_ns) * window_ns + first_ts

    depth = events[
        events["kind"].isin([DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT])
        & events["qty"].gt(0)
        & events["side"].ne("")
    ].copy()
    depth = depth.sort_values(["side", "px", "exch_ts"])
    depth["prev_qty"] = depth.groupby(["side", "px"])["qty"].shift()
    depth["delta_qty"] = depth["qty"] - depth["prev_qty"]

    reductions = (
        depth[depth["delta_qty"].lt(0)]
        .groupby(["window_start", "side", "px"], as_index=False)
        .agg(
            depth_reduce_events=("delta_qty", "size"),
            depth_reduce_qty=("delta_qty", lambda values: -float(values.sum())),
        )
    )

    trades = events[events["kind"].eq(TRADE_EVENT)].copy()
    trade_counts = trades.groupby(["window_start", "side", "px"], as_index=False).agg(
        trade_events=("qty", "size"), trade_qty=("qty", "sum")
    )
    trade_counts["side"] = trade_counts["side"].map({"sell": "buy", "buy": "sell"})

    candidates = reductions.merge(trade_counts, on=["window_start", "side", "px"], how="left")
    candidates[["trade_events", "trade_qty"]] = candidates[["trade_events", "trade_qty"]].fillna(0)
    candidates["score"] = candidates["depth_reduce_events"] / (candidates["trade_events"] + 1)

    mask = candidates["depth_reduce_events"].ge(min_depth_reduce_events) & candidates["trade_events"].le(max_trade_events)
    if require_trade:
        mask &= candidates["trade_events"].ge(1)

    candidates = (
        candidates[mask]
        .sort_values(["score", "depth_reduce_events", "depth_reduce_qty"], ascending=False)
        .head(limit)
        .reset_index(drop=True)
    )
    if candidates.empty and require_trade:
        fallback_candidates = reductions.merge(trade_counts, on=["window_start", "side", "px"], how="left")
        fallback_candidates[["trade_events", "trade_qty"]] = fallback_candidates[["trade_events", "trade_qty"]].fillna(0)
        fallback_candidates["score"] = fallback_candidates["depth_reduce_events"] / (
            fallback_candidates["trade_events"] + 1
        )
        candidates = (
            fallback_candidates[
                fallback_candidates["depth_reduce_events"].ge(min_depth_reduce_events)
                & fallback_candidates["trade_events"].le(max_trade_events)
            ]
            .sort_values(["score", "depth_reduce_events", "depth_reduce_qty"], ascending=False)
            .head(limit)
            .reset_index(drop=True)
        )
    candidates.insert(0, "candidate_id", candidates.index + 1)
    candidates["window_end"] = candidates["window_start"] + window_ns
    candidates["window_start_time"] = candidates["window_start"].map(ns_to_datetime)
    candidates["window_end_time"] = candidates["window_end"].map(ns_to_datetime)
    return candidates


def price_depth_level(side: str, px: float, best_bid: float, best_ask: float, tick_size: float) -> int:
    if side == "buy":
        return int(round((best_bid - px) / tick_size)) + 1 if px <= best_bid else 0
    if side == "sell":
        return int(round((px - best_ask) / tick_size)) + 1 if px >= best_ask else 0
    raise ValueError(f"unknown side: {side}")


def price_at_level(depth, side: str, level: int, tick_size: float, max_scan_ticks: int = 10_000) -> tuple[float, int]:
    if level < 1:
        raise ValueError("level must be >= 1")
    if side == "buy":
        tick = int(depth.best_bid_tick)
        found = 0
        fallback_price = None
        for candidate_tick in range(tick, tick - max_scan_ticks, -1):
            if float(depth.bid_qty_at_tick(candidate_tick)) > 0:
                found += 1
                fallback_price = candidate_tick * tick_size
                if found == level:
                    return candidate_tick * tick_size, found
        if fallback_price is not None:
            return fallback_price, found
    if side == "sell":
        tick = int(depth.best_ask_tick)
        found = 0
        fallback_price = None
        for candidate_tick in range(tick, tick + max_scan_ticks):
            if float(depth.ask_qty_at_tick(candidate_tick)) > 0:
                found += 1
                fallback_price = candidate_tick * tick_size
                if found == level:
                    return candidate_tick * tick_size, found
        if fallback_price is not None:
            return fallback_price, found
    raise ValueError(f"cannot find {side}{level} within {max_scan_ticks} ticks")


def attach_level_metadata(
    row: dict[str, Any],
    side: str,
    level: int,
    actual_level: int,
    best_bid: float,
    best_ask: float,
    max_window_s: int,
) -> dict[str, Any]:
    row.update(
        {
            "level": level,
            "actual_level": actual_level,
            "send_best_bid": best_bid,
            "send_best_ask": best_ask,
            "max_window_s": max_window_s,
            "crossed_at_send": (side == "buy" and row["price"] >= best_ask)
            or (side == "sell" and row["price"] <= best_bid),
        }
    )
    return row


def run_passive_level_strategy(
    config: BacktestConfig,
    hbtpkg,
    event_data: np.ndarray,
    queue_model: str,
    side: str = "sell",
    level: int = 5,
    qty: float = 3.0,
    max_window_s: int = 6 * 60 * 60,
    step_ns: int = 1_000_000_000,
    response_timeout_ns: int = 10_000_000,
) -> pd.DataFrame:
    queue_config = config_with_queue_model(config, queue_model)

    def run(hbt):
        asset_no = 0
        rows = []
        wait_for_bbo(hbt, asset_no)

        depth = hbt.depth(asset_no)
        best_bid = float(depth.best_bid)
        best_ask = float(depth.best_ask)
        px, actual_level = price_at_level(depth, side, level, queue_config.tick_size)
        order_id = 40_000 + level

        rc = submit_limit_order(hbt, hbtpkg, asset_no, order_id, side, px, qty)
        response = hbt.wait_order_response(asset_no, order_id, response_timeout_ns)
        order = get_order(hbt, asset_no, order_id)
        if order is not None and float(order.exec_qty) > 0:
            raise RuntimeError(f"passive {side}{level} order filled on accept at px={px}")

        row = record_order_state(
            hbt,
            hbtpkg,
            asset_no,
            queue_config,
            "accepted",
            f"passive_{side}{level}",
            event_data=event_data,
            queue_model=queue_model,
            side=side,
            order_id=order_id,
            px=px,
            qty=qty,
            rc=rc,
            response=response,
            step=0,
        )
        rows.append(attach_level_metadata(row, side, level, actual_level, best_bid, best_ask, max_window_s))

        send_ts = int(order.local_timestamp)
        end_ts = send_ts + int(max_window_s * 1_000_000_000)
        step = 0
        while int(hbt.current_timestamp) < end_ts:
            order = get_order(hbt, asset_no, order_id)
            if order is not None and (float(order.exec_qty) > 0 or int(order.status) == hbtpkg.FILLED):
                row = record_order_state(
                    hbt,
                    hbtpkg,
                    asset_no,
                    queue_config,
                    "filled",
                    f"passive_{side}{level}",
                    event_data=event_data,
                    queue_model=queue_model,
                    side=side,
                    order_id=order_id,
                    px=px,
                    qty=qty,
                    step=step,
                    is_fill=True,
                )
                rows.append(attach_level_metadata(row, side, level, actual_level, best_bid, best_ask, max_window_s))
                break
            if hbt.elapse(step_ns) != 0:
                break
            step += 1

        row = record_order_state(
            hbt,
            hbtpkg,
            asset_no,
            queue_config,
            "final_state",
            f"passive_{side}{level}",
            event_data=event_data,
            queue_model=queue_model,
            side=side,
            order_id=order_id,
            px=px,
            qty=qty,
            step=step,
        )
        rows.append(attach_level_metadata(row, side, level, actual_level, best_bid, best_ask, max_window_s))
        return pd.DataFrame(rows)

    return with_backtest(queue_config, hbtpkg, run)


def summarize_level_output(output: pd.DataFrame) -> pd.DataFrame:
    accepted = output[output["label"].eq("accepted")].copy()
    fills = output[output["label"].eq("filled")].copy()
    if accepted.empty:
        return pd.DataFrame(columns=LEVEL_COMPARISON_COLUMNS)

    keys = ["queue_model", "side", "level", "actual_level", "order_id"]
    comparison = accepted[
        keys
        + [
            "price",
            "qty",
            "send_best_bid",
            "send_best_ask",
            "send_order_ts",
            "send_order_time",
        ]
    ].merge(
        fills[
            keys
            + [
                "fill_ts",
                "fill_time",
                "exec_qty",
                "exec_price",
                "step",
                "position",
                "balance",
                "equity",
            ]
        ].rename(columns={"step": "fill_step"}),
        on=keys,
        how="left",
    )
    comparison["was_filled"] = comparison["fill_ts"].notna()
    comparison["time_to_fill_ns"] = comparison["fill_ts"] - comparison["send_order_ts"]
    comparison["time_to_fill_s"] = comparison["time_to_fill_ns"] / 1_000_000_000
    comparison["queue_model_fill_delta_ns"] = pd.NA
    filled_mask = comparison["was_filled"]
    comparison.loc[filled_mask, "queue_model_fill_delta_ns"] = comparison[filled_mask].groupby(
        ["side", "level", "price"]
    )["fill_ts"].transform(lambda values: values - values.min())
    for column in LEVEL_COMPARISON_COLUMNS:
        if column not in comparison.columns:
            comparison[column] = pd.NA
    return comparison[LEVEL_COMPARISON_COLUMNS].sort_values(["side", "level", "queue_model"]).reset_index(drop=True)


def run_level_queue_model_comparison(
    config: BacktestConfig,
    hbtpkg,
    event_data: np.ndarray,
    queue_models: Sequence[str] = DEFAULT_QUEUE_MODELS,
    side: str = "sell",
    level: int = 5,
    qty: float = 3.0,
    max_window_s: int = 6 * 60 * 60,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    output = pd.concat(
        [
            run_passive_level_strategy(
                config,
                hbtpkg,
                event_data,
                queue_model,
                side=side,
                level=level,
                qty=qty,
                max_window_s=max_window_s,
            )
            for queue_model in queue_models
        ],
        ignore_index=True,
    )
    return output, summarize_level_output(output)


def attach_candidate_metadata(
    row: dict[str, Any],
    candidate: pd.Series,
    best_bid: float | None = None,
    best_ask: float | None = None,
    depth_level: int | None = None,
    crossed_at_send: bool = False,
    selected_qty: float | None = None,
    max_window_s: int | None = None,
) -> dict[str, Any]:
    row.update(
        {
            "candidate_id": int(candidate["candidate_id"]),
            "candidate_window_start": int(candidate["window_start"]),
            "candidate_window_end": int(candidate["window_end"]),
            "candidate_window_start_time": ns_to_datetime(int(candidate["window_start"])),
            "candidate_window_end_time": ns_to_datetime(int(candidate["window_end"])),
            "depth_reduce_events": int(candidate["depth_reduce_events"]),
            "depth_reduce_qty": float(candidate["depth_reduce_qty"]),
            "candidate_trade_events": int(candidate["trade_events"]),
            "candidate_trade_qty": float(candidate["trade_qty"]),
            "candidate_score": float(candidate["score"]),
            "send_best_bid": best_bid,
            "send_best_ask": best_ask,
            "depth_level": depth_level,
            "crossed_at_send": crossed_at_send,
            "selected_qty": selected_qty,
            "max_window_s": max_window_s,
        }
    )
    return row


def run_deep_passive_candidate(
    config: BacktestConfig,
    hbtpkg,
    event_data: np.ndarray,
    candidate: pd.Series,
    queue_model: str,
    qty: float = 3.0,
    min_depth_level: int = 2,
    max_window_s: int = 6 * 60 * 60,
    step_ns: int = 1_000_000_000,
    response_timeout_ns: int = 10_000_000,
) -> pd.DataFrame:
    queue_config = config_with_queue_model(config, queue_model)

    def run(hbt):
        asset_no = 0
        rows = []
        side = str(candidate["side"])
        px = float(candidate["px"])
        order_id = 30_000 + int(candidate["candidate_id"])
        step = 0

        wait_for_bbo(hbt, asset_no)
        target_ts = int(candidate["window_start"])
        if hbt.current_timestamp < target_ts and hbt.elapse(target_ts - int(hbt.current_timestamp)) != 0:
            row = record_order_state(
                hbt, hbtpkg, asset_no, queue_config, "skipped_start", "deep_passive", event_data=event_data, queue_model=queue_model
            )
            rows.append(attach_candidate_metadata(row, candidate, selected_qty=qty, max_window_s=max_window_s))
            return pd.DataFrame(rows)

        depth = hbt.depth(asset_no)
        best_bid = float(depth.best_bid)
        best_ask = float(depth.best_ask)
        depth_level = price_depth_level(side, px, best_bid, best_ask, queue_config.tick_size)
        crossed = (side == "buy" and px >= best_ask) or (side == "sell" and px <= best_bid)
        if crossed or depth_level < min_depth_level:
            label = "skipped_crossed" if crossed else "skipped_not_deep"
            row = record_order_state(
                hbt, hbtpkg, asset_no, queue_config, label, "deep_passive", event_data=event_data, queue_model=queue_model
            )
            rows.append(attach_candidate_metadata(row, candidate, best_bid, best_ask, depth_level, crossed, qty, max_window_s))
            return pd.DataFrame(rows)

        rc = submit_limit_order(hbt, hbtpkg, asset_no, order_id, side, px, qty)
        response = hbt.wait_order_response(asset_no, order_id, response_timeout_ns)
        order = get_order(hbt, asset_no, order_id)
        if order is not None and float(order.exec_qty) > 0:
            raise RuntimeError(f"deep passive {side} order filled on accept: candidate_id={candidate['candidate_id']}")

        row = record_order_state(
            hbt,
            hbtpkg,
            asset_no,
            queue_config,
            "accepted",
            "deep_passive",
            event_data=event_data,
            queue_model=queue_model,
            side=side,
            order_id=order_id,
            px=px,
            qty=qty,
            rc=rc,
            response=response,
            step=0,
        )
        rows.append(attach_candidate_metadata(row, candidate, best_bid, best_ask, depth_level, False, qty, max_window_s))

        send_ts = int(order.local_timestamp)
        end_ts = send_ts + int(max_window_s * 1_000_000_000)
        while int(hbt.current_timestamp) < end_ts:
            order = get_order(hbt, asset_no, order_id)
            if order is not None and (float(order.exec_qty) > 0 or int(order.status) == hbtpkg.FILLED):
                row = record_order_state(
                    hbt,
                    hbtpkg,
                    asset_no,
                    queue_config,
                    "filled",
                    "deep_passive",
                    event_data=event_data,
                    queue_model=queue_model,
                    side=side,
                    order_id=order_id,
                    px=px,
                    qty=qty,
                    step=step,
                    is_fill=True,
                )
                rows.append(attach_candidate_metadata(row, candidate, best_bid, best_ask, depth_level, False, qty, max_window_s))
                break
            if hbt.elapse(step_ns) != 0:
                break
            step += 1

        row = record_order_state(
            hbt,
            hbtpkg,
            asset_no,
            queue_config,
            "final_state",
            "deep_passive",
            event_data=event_data,
            queue_model=queue_model,
            side=side,
            order_id=order_id,
            px=px,
            qty=qty,
            step=step,
        )
        rows.append(attach_candidate_metadata(row, candidate, best_bid, best_ask, depth_level, False, qty, max_window_s))
        return pd.DataFrame(rows)

    return with_backtest(queue_config, hbtpkg, run)


def summarize_strategy3_output(output: pd.DataFrame) -> pd.DataFrame:
    if output.empty:
        return pd.DataFrame(columns=STRATEGY3_COMPARISON_COLUMNS)
    keys = ["candidate_id", "queue_model", "side", "order_id"]
    accepted = output[output["label"].eq("accepted")].copy()
    fills = output[output["label"].eq("filled")].copy()
    if accepted.empty:
        skipped = output[output["label"].str.startswith("skipped")].copy()
        for column in STRATEGY3_COMPARISON_COLUMNS:
            if column not in skipped.columns:
                skipped[column] = pd.NA
        return skipped[STRATEGY3_COMPARISON_COLUMNS]

    accepted_cols = keys + [
        "price",
        "qty",
        "send_order_ts",
        "send_order_time",
        "send_best_bid",
        "send_best_ask",
        "depth_level",
        "selected_qty",
        "max_window_s",
        "candidate_window_start_time",
        "depth_reduce_events",
        "depth_reduce_qty",
        "candidate_trade_events",
        "candidate_trade_qty",
        "candidate_score",
    ]
    fill_cols = keys + [
        "fill_ts",
        "fill_time",
        "exch_ts",
        "local_ts",
        "exec_qty",
        "exec_price",
        "step",
        "position",
        "balance",
        "equity",
    ]
    comparison = accepted[accepted_cols].merge(
        fills[fill_cols].rename(
            columns={
                "exch_ts": "fill_exch_ts",
                "local_ts": "fill_local_ts",
                "step": "fill_step",
                "position": "fill_position",
                "balance": "fill_balance",
                "equity": "fill_equity",
            }
        ),
        on=keys,
        how="left",
    )
    comparison["was_filled"] = comparison["fill_ts"].notna()
    comparison["time_to_fill_ns"] = comparison["fill_ts"] - comparison["send_order_ts"]
    comparison["time_to_fill_s"] = comparison["time_to_fill_ns"] / 1_000_000_000
    comparison["queue_model_fill_delta_ns"] = pd.NA
    filled_mask = comparison["was_filled"]
    comparison.loc[filled_mask, "queue_model_fill_delta_ns"] = comparison[filled_mask].groupby(
        ["candidate_id", "side", "price"]
    )["fill_ts"].transform(lambda values: values - values.min())
    comparison = comparison.sort_values(["candidate_id", "side", "queue_model"]).reset_index(drop=True)
    for column in STRATEGY3_COMPARISON_COLUMNS:
        if column not in comparison.columns:
            comparison[column] = pd.NA
    return comparison[STRATEGY3_COMPARISON_COLUMNS]


def run_strategy3_deep_queue_comparison(
    config: BacktestConfig,
    hbtpkg,
    event_data: np.ndarray,
    candidates: pd.DataFrame,
    queue_models: Sequence[str] = DEFAULT_QUEUE_MODELS,
    qty: float = 3.0,
    max_candidates: int = 3,
    min_depth_level: int = 2,
    max_window_s: int = 6 * 60 * 60,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    outputs = []
    selected = 0
    for _, candidate in candidates.iterrows():
        combined = pd.concat(
            [
                run_deep_passive_candidate(
                    config,
                    hbtpkg,
                    event_data,
                    candidate,
                    queue_model,
                    qty=qty,
                    min_depth_level=min_depth_level,
                    max_window_s=max_window_s,
                )
                for queue_model in queue_models
            ],
            ignore_index=True,
        )
        outputs.append(combined)
        if combined["label"].eq("accepted").any():
            selected += 1
        if selected >= max_candidates:
            break

    output = pd.concat(outputs, ignore_index=True) if outputs else pd.DataFrame()
    return output, summarize_strategy3_output(output)
