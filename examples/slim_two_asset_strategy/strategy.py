"""Clocked two-asset crossed-market probe using only the neutral slim API.

This example is intentionally not a futures/spot arbitrage implementation. It
uses the relationship between two BBOs as a signal, then sends one immediate
order to demonstrate that an independent strategy family can own its decision
logic and consume :class:`hftbacktest_slim.SlimEngine` directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from hftbacktest_slim import AssetConfig, OrderView, Side, SlimEngine, TimeInForce


@dataclass(frozen=True, slots=True)
class ProbeResult:
    decisions: int
    final_timestamp_ns: int | None
    order: OrderView | None


class CrossedMarketProbe:
    """Buy asset 0 when its ask is no higher than asset 1's bid."""

    def __init__(
        self,
        assets: Sequence[AssetConfig],
        *,
        decision_step_ns: int,
        response_timeout_ns: int = 1_000_000,
    ) -> None:
        if len(assets) != 2:
            raise ValueError("CrossedMarketProbe requires exactly two assets")
        if decision_step_ns <= 0:
            raise ValueError("decision_step_ns must be positive")
        self.assets = tuple(assets)
        self.decision_step_ns = int(decision_step_ns)
        self.response_timeout_ns = int(response_timeout_ns)

    def run(self, *, max_decisions: int) -> ProbeResult:
        decisions = 0
        last_timestamp: int | None = None
        visible_order: OrderView | None = None
        with SlimEngine(self.assets) as engine:
            while decisions < max_decisions and engine.advance(self.decision_step_ns):
                decisions += 1
                last_timestamp = engine.current_timestamp_ns
                left = engine.depth(0)
                right = engine.depth(1)
                if not left.valid or not right.valid or left.best_ask > right.best_bid:
                    continue
                order_id = decisions
                engine.submit_order(
                    asset_no=0,
                    order_id=order_id,
                    side=Side.BUY,
                    price=left.best_ask,
                    quantity=1.0,
                    time_in_force=TimeInForce.FOK,
                )
                engine.wait_order_response(0, order_id, self.response_timeout_ns)
                visible_order = engine.order(0, order_id)
                break
        return ProbeResult(decisions, last_timestamp, visible_order)


__all__ = ("CrossedMarketProbe", "ProbeResult")
