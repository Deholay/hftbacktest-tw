from __future__ import annotations

from dataclasses import dataclass

from scripts.hbt_types import HbtAssetConfig, HbtLegFill
from .models import PairConfig


@dataclass(frozen=True)
class HbtPairBacktestConfig:
    pair: PairConfig
    spot: HbtAssetConfig
    future: HbtAssetConfig
    first_leg: str = "future"
    step_ns: int = 1_000_000_000
    response_timeout_ns: int = 50_000_000
    second_leg_delay_ns: int = 0
    post_first_feed_wait: str = "none"
    post_first_feed_timeout_ns: int = 0
    post_first_feed_poll_ns: int = 1_000_000
    max_steps: int | None = None
    max_trades: int | None = None
    enforce_risk_limits: bool = True
    flatten_on_second_leg_failure: bool = True
    second_leg_profit_check: bool = True
    record_market_every_steps: int | None = None
