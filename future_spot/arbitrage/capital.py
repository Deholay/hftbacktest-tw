"""Capital-constrained replay for saved futures/spot HBT fills.

The pair HBT runs are independent candidate generators.  This module replays
their filled entry/exit rows in timestamp order and applies a shared capital
budget without rerunning market data conversion or HftBacktest.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .models import Signal


@dataclass(frozen=True)
class CapitalAllocationConfig:
    """Cash budget and collateral assumptions for one matched pair portfolio."""

    total_capital: float = 50_000_000.0
    futures_margin_rate: float = 0.20
    spot_equity_rate: float = 0.40
    leverage: bool = True
    carry_positions: bool = False

    def __post_init__(self) -> None:
        if self.total_capital <= 0:
            raise ValueError("total_capital must be > 0")
        if not 0 < self.futures_margin_rate <= 1:
            raise ValueError("futures_margin_rate must be in (0, 1]")
        if not 0 < self.spot_equity_rate <= 1:
            raise ValueError("spot_equity_rate must be in (0, 1]")

    @property
    def combined_capital_rate(self) -> float:
        return self.effective_futures_margin_rate + self.effective_spot_equity_rate

    @property
    def effective_futures_margin_rate(self) -> float:
        return self.futures_margin_rate if self.leverage else 1.0

    @property
    def effective_spot_equity_rate(self) -> float:
        return self.spot_equity_rate if self.leverage else 1.0

    @property
    def futures_capital_limit(self) -> float:
        return self.total_capital * self.effective_futures_margin_rate / self.combined_capital_rate

    @property
    def spot_capital_limit(self) -> float:
        return self.total_capital * self.effective_spot_equity_rate / self.combined_capital_rate

    @property
    def matched_notional_limit(self) -> float:
        return self.total_capital / self.combined_capital_rate


def build_capital_constraint_outputs(
    filled_trades: pd.DataFrame,
    config: CapitalAllocationConfig | None = None,
) -> dict[str, pd.DataFrame]:
    """Replay candidate fills under a shared capital limit.

    With ``carry_positions=True``, accepted lots and their occupied capital
    survive trade-date boundaries and next-day exits match by contract pair.
    Independent pair/day results retain the legacy daily reset by default.
    """

    cfg = config or CapitalAllocationConfig()
    if filled_trades.empty:
        return _empty_outputs(cfg)

    required = {
        "trade_date",
        "run_key",
        "pair_name",
        "spot_symbol",
        "future_symbol",
        "signal",
        "spot_exec_price",
        "future_exec_price",
        "spot_order_qty",
        "future_order_qty",
        "future_pnl_multiplier",
        "stock_commission_rate",
        "stock_commission_discount",
        "stock_transaction_tax_rate",
    }
    missing = sorted(required.difference(filled_trades.columns))
    if missing:
        raise ValueError(f"filled_trades is missing capital replay columns: {missing}")

    trades = filled_trades.copy()
    trades["trade_date"] = pd.to_datetime(trades["trade_date"], errors="raise").dt.normalize()
    for column in (
        "spot_exec_price",
        "future_exec_price",
        "spot_order_qty",
        "future_order_qty",
        "future_pnl_multiplier",
        "stock_commission_rate",
        "stock_commission_discount",
        "stock_transaction_tax_rate",
    ):
        trades[column] = pd.to_numeric(trades[column], errors="coerce")
    timestamp_column = "completion_timestamp" if "completion_timestamp" in trades.columns else "timestamp"
    trades["capital_event_timestamp"] = pd.to_numeric(trades[timestamp_column], errors="coerce")
    if "timestamp" in trades.columns:
        fallback = pd.to_numeric(trades["timestamp"], errors="coerce")
        trades["capital_event_timestamp"] = trades["capital_event_timestamp"].fillna(fallback)
    trades = trades.sort_values(
        ["trade_date", "capital_event_timestamp", "run_key"],
        kind="stable",
        na_position="last",
    )

    event_rows: list[dict[str, Any]] = []
    open_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    open_lots: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
    spot_used = 0.0
    futures_used = 0.0

    for trade_date, day_trades in trades.groupby("trade_date", sort=True):
        if not cfg.carry_positions:
            open_lots = defaultdict(deque)
            spot_used = 0.0
            futures_used = 0.0
        peak_spot_used = spot_used
        peak_futures_used = futures_used
        peak_total_used = spot_used + futures_used
        accepted_entries = 0
        rejected_entries = 0
        accepted_exits = 0
        unmatched_exits = 0
        realized_pnl = 0.0

        for row in day_trades.itertuples(index=False):
            signal = str(row.signal)
            action = "ignored"
            reason = "unsupported signal"
            released_spot = 0.0
            released_futures = 0.0
            event_realized_pnl = 0.0

            if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE.value:
                lot = _entry_lot(row, cfg)
                projected_spot = spot_used + lot["spot_equity_required"]
                projected_futures = futures_used + lot["futures_margin_required"]
                projected_total = projected_spot + projected_futures
                breaches: list[str] = []
                if projected_spot > cfg.spot_capital_limit + 1e-9:
                    breaches.append("spot equity limit")
                if projected_futures > cfg.futures_capital_limit + 1e-9:
                    breaches.append("futures margin limit")
                if projected_total > cfg.total_capital + 1e-9:
                    breaches.append("total capital limit")
                if breaches:
                    action = "rejected_entry"
                    reason = ", ".join(breaches)
                    rejected_entries += 1
                else:
                    action = "accepted_entry"
                    reason = "within capital limits"
                    accepted_entries += 1
                    spot_used = projected_spot
                    futures_used = projected_futures
                    open_lots[_position_key(row)].append(lot)

            elif signal == Signal.EXIT.value:
                queue = open_lots[_position_key(row)]
                if queue:
                    lot = queue.popleft()
                    released_spot = lot["spot_equity_required"]
                    released_futures = lot["futures_margin_required"]
                    spot_used = max(0.0, spot_used - released_spot)
                    futures_used = max(0.0, futures_used - released_futures)
                    event_realized_pnl = _realized_pair_pnl(lot, row)
                    realized_pnl += event_realized_pnl
                    accepted_exits += 1
                    action = "accepted_exit"
                    reason = "matched accepted entry"
                else:
                    unmatched_exits += 1
                    action = "ignored_exit"
                    reason = "no accepted entry lot"

            total_used = spot_used + futures_used
            peak_spot_used = max(peak_spot_used, spot_used)
            peak_futures_used = max(peak_futures_used, futures_used)
            peak_total_used = max(peak_total_used, total_used)
            event_rows.append(
                {
                    "trade_date": trade_date,
                    "capital_event_timestamp": getattr(row, "capital_event_timestamp", np.nan),
                    "run_key": row.run_key,
                    "pair_name": row.pair_name,
                    "spot_symbol": row.spot_symbol,
                    "future_symbol": row.future_symbol,
                    "signal": signal,
                    "capital_action": action,
                    "capital_reason": reason,
                    "spot_capital_used": spot_used,
                    "futures_capital_used": futures_used,
                    "total_capital_used": total_used,
                    "capital_headroom": cfg.total_capital - total_used,
                    "spot_capital_utilization": spot_used / cfg.spot_capital_limit,
                    "futures_capital_utilization": futures_used / cfg.futures_capital_limit,
                    "total_capital_utilization": total_used / cfg.total_capital,
                    "released_spot_capital": released_spot,
                    "released_futures_capital": released_futures,
                    "realized_pnl": event_realized_pnl,
                }
            )

        ending_open_lots = 0
        for queue in open_lots.values():
            for lot in queue:
                ending_open_lots += 1
                open_rows.append({"trade_date": trade_date, **lot})
        daily_rows.append(
            {
                "trade_date": trade_date,
                "accepted_entries": accepted_entries,
                "rejected_entries": rejected_entries,
                "accepted_exits": accepted_exits,
                "ignored_unmatched_exits": unmatched_exits,
                "ending_open_lots": ending_open_lots,
                "realized_pnl": realized_pnl,
                "ending_spot_capital": spot_used,
                "ending_futures_capital": futures_used,
                "ending_total_capital": spot_used + futures_used,
                "peak_spot_capital": peak_spot_used,
                "peak_futures_capital": peak_futures_used,
                "peak_total_capital": peak_total_used,
                "peak_capital_utilization": peak_total_used / cfg.total_capital,
            }
        )

    events = pd.DataFrame(event_rows)
    daily = pd.DataFrame(daily_rows)
    open_lots_frame = pd.DataFrame(open_rows)
    summary = _capital_summary(events, daily, cfg)
    return {
        "capital_constraint_summary": summary,
        "daily_capital_constraint": daily,
        "capital_constraint_events": events,
        "capital_constraint_open_lots": open_lots_frame,
    }


def _entry_lot(row: Any, cfg: CapitalAllocationConfig) -> dict[str, Any]:
    spot_notional = float(row.spot_exec_price) * float(row.spot_order_qty)
    future_multiplier = float(row.future_order_qty) * float(row.future_pnl_multiplier)
    futures_notional = float(row.future_exec_price) * future_multiplier
    commission_rate = float(row.stock_commission_rate) * float(row.stock_commission_discount)
    entry_stock_fee = spot_notional * commission_rate
    return {
        "run_key": row.run_key,
        "pair_name": row.pair_name,
        "spot_symbol": row.spot_symbol,
        "future_symbol": row.future_symbol,
        "entry_spot_price": float(row.spot_exec_price),
        "entry_future_price": float(row.future_exec_price),
        "spot_order_qty": float(row.spot_order_qty),
        "future_multiplier": future_multiplier,
        "commission_rate": commission_rate,
        "stock_transaction_tax_rate": float(row.stock_transaction_tax_rate),
        "spot_notional": spot_notional,
        "futures_notional": futures_notional,
        "entry_stock_fee": entry_stock_fee,
        "spot_equity_required": spot_notional * cfg.effective_spot_equity_rate + entry_stock_fee,
        "futures_margin_required": futures_notional * cfg.effective_futures_margin_rate,
    }


def _realized_pair_pnl(lot: dict[str, Any], exit_row: Any) -> float:
    exit_spot = float(exit_row.spot_exec_price)
    exit_future = float(exit_row.future_exec_price)
    spot_pnl = (exit_spot - lot["entry_spot_price"]) * lot["spot_order_qty"]
    future_pnl = (lot["entry_future_price"] - exit_future) * lot["future_multiplier"]
    exit_notional = exit_spot * lot["spot_order_qty"]
    stock_cost = lot["entry_stock_fee"] + exit_notional * (
        lot["commission_rate"] + lot["stock_transaction_tax_rate"]
    )
    return spot_pnl + future_pnl - stock_cost


def _capital_summary(
    events: pd.DataFrame,
    daily: pd.DataFrame,
    cfg: CapitalAllocationConfig,
) -> pd.DataFrame:
    accepted_entries = int(daily["accepted_entries"].sum()) if not daily.empty else 0
    rejected_entries = int(daily["rejected_entries"].sum()) if not daily.empty else 0
    candidate_entries = accepted_entries + rejected_entries
    return pd.DataFrame(
        [
            {
                "total_capital": cfg.total_capital,
                "futures_margin_rate": cfg.futures_margin_rate,
                "spot_equity_rate": cfg.spot_equity_rate,
                "effective_futures_margin_rate": cfg.effective_futures_margin_rate,
                "effective_spot_equity_rate": cfg.effective_spot_equity_rate,
                "leverage": cfg.leverage,
                "carry_positions": cfg.carry_positions,
                "futures_capital_limit": cfg.futures_capital_limit,
                "spot_capital_limit": cfg.spot_capital_limit,
                "matched_notional_limit": cfg.matched_notional_limit,
                "candidate_entries": candidate_entries,
                "accepted_entries": accepted_entries,
                "rejected_entries": rejected_entries,
                "entry_acceptance_rate": accepted_entries / candidate_entries if candidate_entries else np.nan,
                "accepted_exits": int(daily["accepted_exits"].sum()) if not daily.empty else 0,
                "ending_open_lot_days": int(daily["ending_open_lots"].sum()) if not daily.empty else 0,
                "capital_filtered_realized_pnl": float(daily["realized_pnl"].sum()) if not daily.empty else 0.0,
                "capital_filtered_realized_roi": (
                    float(daily["realized_pnl"].sum()) / cfg.total_capital if not daily.empty else 0.0
                ),
                "peak_spot_capital": float(daily["peak_spot_capital"].max()) if not daily.empty else 0.0,
                "peak_futures_capital": float(daily["peak_futures_capital"].max()) if not daily.empty else 0.0,
                "peak_total_capital": float(daily["peak_total_capital"].max()) if not daily.empty else 0.0,
                "peak_capital_utilization": float(daily["peak_capital_utilization"].max()) if not daily.empty else 0.0,
                "replay_scope": (
                    "continuous_position_candidate_replay"
                    if cfg.carry_positions
                    else "day_scoped_candidate_replay"
                ),
            }
        ]
    )


def _empty_outputs(cfg: CapitalAllocationConfig) -> dict[str, pd.DataFrame]:
    daily = pd.DataFrame(
        columns=[
            "trade_date",
            "accepted_entries",
            "rejected_entries",
            "accepted_exits",
            "ignored_unmatched_exits",
            "ending_open_lots",
            "realized_pnl",
            "ending_spot_capital",
            "ending_futures_capital",
            "ending_total_capital",
            "peak_spot_capital",
            "peak_futures_capital",
            "peak_total_capital",
            "peak_capital_utilization",
        ]
    )
    return {
        "capital_constraint_summary": _capital_summary(pd.DataFrame(), daily, cfg),
        "daily_capital_constraint": daily,
        "capital_constraint_events": pd.DataFrame(),
        "capital_constraint_open_lots": pd.DataFrame(),
    }


def capital_allocation_config_from_args(args: Any) -> CapitalAllocationConfig:
    """Create one consistent capital model from CLI or notebook arguments."""

    return CapitalAllocationConfig(
        total_capital=float(getattr(args, "total_capital", 50_000_000.0)),
        futures_margin_rate=float(getattr(args, "futures_margin_rate", 0.20)),
        spot_equity_rate=float(getattr(args, "spot_equity_rate", 0.40)),
        leverage=bool(getattr(args, "leverage", True)),
        carry_positions=bool(getattr(args, "carry_positions", False)),
    )


def _position_key(row: Any) -> tuple[str, str]:
    return str(row.spot_symbol), str(row.future_symbol)
