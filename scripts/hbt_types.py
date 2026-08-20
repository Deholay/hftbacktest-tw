"""Project-wide HftBacktest dataclasses shared by strategy implementations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HbtAssetConfig:
    symbol: str
    data: Path
    instrument: str
    contract_size: float
    lot_size: float = 1.0
    maker_fee: float = 0.0
    taker_fee: float = 0.0
    tick_size: float | None = None
    trade_date: str | None = None
    order_entry_latency_ns: int = 0
    order_response_latency_ns: int = 0
    feed_latency_offset_ns: int = 0
    queue_model: str = "risk_adverse"
    queue_model_param: float = 3.0
    last_trades_capacity: int = 100


@dataclass(frozen=True)
class HbtLegFill:
    leg: str
    asset_no: int
    side: str
    order_id: int
    requested_price: float
    requested_qty: float
    response: int
    status: int | None
    filled: bool
    exec_price: float | None
    exec_qty: float
    local_timestamp: int | None
    exch_timestamp: int | None
    order_req_local_ts: int | None = None
    order_exch_ts: int | None = None
    order_resp_local_ts: int | None = None
