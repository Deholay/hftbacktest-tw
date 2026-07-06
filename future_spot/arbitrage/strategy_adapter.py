"""Futures/spot arbitrage implementation of the root strategy API."""

from __future__ import annotations

from dataclasses import dataclass

from scripts.strategy_api import StrategyContext, StrategyDecision

from .models import PairConfig, PairMarket, PairPosition, PairPricing, Signal
from .strategy import RiskManager, StopLossAwareSignalEngine


@dataclass(frozen=True)
class FutureSpotStrategyPayload:
    pair: PairConfig
    market: PairMarket
    pricing: PairPricing
    position: PairPosition
    enforce_risk_limits: bool = True


class FutureSpotPairStrategy:
    """Default futures/spot arbitrage strategy adapter.

    This preserves the previous behavior: evaluate the stop-loss-aware signal,
    then pass non-HOLD signals through the risk manager.
    """

    name = "future_spot.default_pair_arbitrage"

    def __init__(
        self,
        signal_engine: StopLossAwareSignalEngine | None = None,
        risk_manager: RiskManager | None = None,
    ) -> None:
        self.signal_engine = signal_engine or StopLossAwareSignalEngine()
        self.risk_manager = risk_manager or RiskManager()

    def decide(self, context: StrategyContext) -> StrategyDecision:
        payload = self._payload(context)
        signal = self.signal_engine.evaluate(payload.pair, payload.pricing, payload.position)
        if signal == Signal.HOLD:
            return StrategyDecision(action=signal.value, should_execute=False, reason="hold", metadata={"signal": signal})

        allowed, reason = self.risk_manager.check(
            payload.pair,
            payload.market,
            signal,
            payload.position,
            enforce_pair_max=payload.enforce_risk_limits,
        )
        return StrategyDecision(
            action=signal.value,
            should_execute=allowed,
            reason=reason,
            metadata={"signal": signal},
        )

    @staticmethod
    def _payload(context: StrategyContext) -> FutureSpotStrategyPayload:
        payload = context.payload
        if isinstance(payload, FutureSpotStrategyPayload):
            return payload
        return FutureSpotStrategyPayload(
            pair=payload["pair"],
            market=payload["market"],
            pricing=payload["pricing"],
            position=payload["position"],
            enforce_risk_limits=payload.get("enforce_risk_limits", True),
        )


def default_strategy() -> FutureSpotPairStrategy:
    return FutureSpotPairStrategy()
