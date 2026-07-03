from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

class Mode(str, Enum):
    DRY_RUN = "DRY_RUN"
    PAPER = "PAPER"
    LIVE = "LIVE"
    SIM = "SIM"
    REPLAY = "REPLAY"


class Signal(str, Enum):
    HOLD = "HOLD"
    ENTER_LONG_SPOT_SHORT_FUTURE = "ENTER_LONG_SPOT_SHORT_FUTURE"
    ENTER_SHORT_SPOT_LONG_FUTURE = "ENTER_SHORT_SPOT_LONG_FUTURE"
    EXIT = "EXIT"


@dataclass(frozen=True)
class InitialPositionConfig:
    quantity: int = 0
    direction: Signal = Signal.HOLD
    entry_basis_pct: float | None = None
    entry_spot_price: float | None = None
    entry_future_price: float | None = None


@dataclass(frozen=True)
class PairConfig:
    name: str
    spot_symbol: str
    future_symbol: str
    spot_shares_per_pair: int
    future_shares_per_pair: int
    spot_order_qty: int
    future_order_qty: int
    future_pnl_multiplier: int
    entry_threshold_pct: float
    exit_threshold_pct: float
    stop_loss_pct: float
    exit_tick_multiple: float = 1.0
    exit_tick_rule: str = "lte"
    min_exit_realized_pnl: float | None = None
    min_effective_tick_multiple: float = 0.0
    min_entry_interval_sec: float = 0.0
    spot_tick_size: float | None = None
    future_tick_size: float | None = None
    stock_commission_rate: float = 0.001425
    stock_commission_discount: float = 0.28
    stock_transaction_tax_rate: float = 0.003
    max_pairs: int = 1
    allow_short_spot: bool = False
    futures_after_hours: bool = False
    min_bid_size: int = 1
    min_ask_size: int = 1
    stock_min_bid_size: int = 2
    stock_min_ask_size: int = 2
    future_min_bid_size: int = 1
    future_min_ask_size: int = 1
    first_leg_time_in_force: str = "ROD"
    second_leg_tick_offset: float = 0.0
    second_leg_time_in_force: str = "ROD"
    second_leg_failure_action: str = "none"
    min_second_leg_adjusted_basis_pct: float | None = None
    flatten_first_leg_tick_offset: float = 2.0
    flatten_first_leg_time_in_force: str = "IOC"
    cooldown_after_second_leg_failure_sec: float = 30.0
    fok_order_timeout_sec: float = 3.0
    initial_position: InitialPositionConfig = field(default_factory=InitialPositionConfig)


@dataclass(frozen=True)
class FubonLoginConfig:
    personal_id: str = ""
    password: str = ""
    cert_path: str = ""
    cert_pass: str = ""


@dataclass(frozen=True)
class FubonConfig:
    stock: FubonLoginConfig = field(default_factory=FubonLoginConfig)
    futures: FubonLoginConfig = field(default_factory=FubonLoginConfig)


@dataclass(frozen=True)
class HistoricalSourceConfig:
    path: str = ""
    timestamp_col: str = "timestamp"
    symbol_col: str = "symbol"
    bid_col: str = "bid"
    ask_col: str = "ask"
    bid_size_col: str | None = "bid_size"
    ask_size_col: str | None = "ask_size"
    last_col: str | None = None
    status_col: str | None = "status"
    filter_trial_status: bool = True
    session_start_time: str | None = None
    session_end_time: str | None = None
    default_size: int = 1


@dataclass(frozen=True)
class HistoricalReplayConfig:
    stock: HistoricalSourceConfig = field(default_factory=HistoricalSourceConfig)
    futures: HistoricalSourceConfig = field(default_factory=HistoricalSourceConfig)
    replay_interval_sec: float = 0.0
    replay_date: str | None = None


@dataclass(frozen=True)
class BacktestExecutionConfig:
    send_order_latency_ms: float = 0.0
    match_order_report_latency_ms: float = 0.0
    second_leg_profit_check: bool = True
    max_quote_age_sec: float | None = None


@dataclass(frozen=True)
class AppConfig:
    mode: Mode = Mode.DRY_RUN
    allow_live_order: bool = False
    poll_interval_sec: float = 2.0
    max_total_pairs: int | None = None
    max_total_spot_notional: float | None = None
    log_threshold_only: bool = False
    log_order_only: bool = False
    sync_system_time_on_start: bool = True
    trading_session_start_time: str | None = "09:00:00"
    trading_session_end_time: str | None = "13:25:00"
    fubon: FubonConfig = field(default_factory=FubonConfig)
    historical: HistoricalReplayConfig = field(default_factory=HistoricalReplayConfig)
    backtest_execution: BacktestExecutionConfig = field(default_factory=BacktestExecutionConfig)
    pairs: tuple[PairConfig, ...] = field(default_factory=tuple)

    def with_mode(self, mode: Mode) -> AppConfig:
        return AppConfig(
            mode=mode,
            allow_live_order=self.allow_live_order,
            poll_interval_sec=self.poll_interval_sec,
            max_total_pairs=self.max_total_pairs,
            max_total_spot_notional=self.max_total_spot_notional,
            log_threshold_only=self.log_threshold_only,
            log_order_only=self.log_order_only,
            sync_system_time_on_start=self.sync_system_time_on_start,
            trading_session_start_time=self.trading_session_start_time,
            trading_session_end_time=self.trading_session_end_time,
            fubon=self.fubon,
            historical=self.historical,
            backtest_execution=self.backtest_execution,
            pairs=self.pairs,
        )


@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: float
    ask: float
    bid_size: int | float = 0
    ask_size: int | float = 0
    last: float | None = None
    raw: dict[str, Any] | None = None

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


@dataclass(frozen=True)
class QuoteUpdate:
    source: str
    symbol: str


@dataclass(frozen=True)
class PairMarket:
    pair: PairConfig
    spot: Quote
    future: Quote
    trigger_source: str | None = None
    trigger_symbol: str | None = None


@dataclass(frozen=True)
class PairPricing:
    long_spot_short_future_pct: float
    short_spot_long_future_pct: float
    mid_basis_pct: float
    long_spot_short_future_ticks: float
    short_spot_long_future_ticks: float
    long_spot_short_future_exit_ticks: float
    short_spot_long_future_exit_ticks: float
    spot_tick_size: float
    future_tick_size: float
    spot_buy_notional: float
    spot_sell_notional: float
    future_sell_notional: float
    future_buy_notional: float


@dataclass
class PairPosition:
    pair_name: str
    quantity: float = 0
    direction: Signal = Signal.HOLD
    entry_basis_pct: float | None = None
    entry_spot_price: float | None = None
    entry_future_price: float | None = None
    stock_units: float = 0
    future_units: float = 0
    last_entry_time: Any = None

    @property
    def has_position(self) -> bool:
        return self.quantity != 0

    @property
    def has_leg_exposure(self) -> bool:
        return self.stock_units != 0 or self.future_units != 0


class MarketDataProvider(Protocol):
    def get_pair_market(self, pair: PairConfig) -> PairMarket:
        ...

    def wait_for_future_update(self, timeout: float = 1.0) -> QuoteUpdate | None:
        ...

    def is_finished(self) -> bool:
        ...

    def close(self) -> None:
        ...


class OrderRouter(Protocol):
    def place_pair_orders(
        self,
        signal: Signal,
        market: PairMarket,
        position: PairPosition,
    ) -> list[Any]:
        ...
