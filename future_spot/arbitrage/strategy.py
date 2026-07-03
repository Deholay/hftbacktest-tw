from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import BacktestExecutionConfig, Mode, OrderRouter, PairConfig, PairMarket, PairPosition, PairPricing, Quote, Signal
from .ticks import pair_leg_tick_size
from .utils import coerce_bool, exit_quantity_multiplier, pct


EFFECTIVE_TICK_COST_RATE = 0.004


def weighted_average(old_value: float | None, old_weight: float, new_value: float, new_weight: float) -> float:
    if old_value is None or old_weight <= 0:
        return new_value
    return ((old_value * old_weight) + (new_value * new_weight)) / (old_weight + new_weight)


def money_text(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def latency_raw_prefix(source: str, symbol: str, offset_ms: float) -> str:
    offset_text = f"{offset_ms:g}".replace(".", "p")
    return f"latency_{source}_{symbol}_{offset_text}ms"


@dataclass(frozen=True)
class TradeRecord:
    pair_name: str
    signal: Signal
    spot_symbol: str
    future_symbol: str
    spot_side: str
    future_side: str
    spot_qty: int
    future_qty: int
    spot_price: float
    future_price: float
    basis_pct: float
    reason: str = ""
    realized_pnl: float | None = None
    realized_pnl_pct: float | None = None
    gross_pnl: float | None = None
    stock_fee: float | None = None
    stock_tax: float | None = None
    stock_cost: float | None = None
    spot_pnl: float | None = None
    future_pnl: float | None = None
    spot_exchtime: Any = None
    future_exchtime: Any = None
    trigger_source: str | None = None
    trigger_symbol: str | None = None
    latency_first_leg: str | None = None
    latency_failure_reason: str | None = None
    latency_signal_time: float | None = None
    latency_first_fill_time: float | None = None
    latency_decision_time: float | None = None
    latency_second_fill_time: float | None = None
    latency_flatten_time: float | None = None
    latency_first_leg_entry_price: float | None = None
    latency_flatten_price: float | None = None


class PairPricer:
    def price(self, market: PairMarket) -> PairPricing:
        pair = market.pair
        spot = market.spot
        future = market.future

        spot_buy_notional = spot.ask * pair.spot_shares_per_pair
        spot_sell_notional = spot.bid * pair.spot_shares_per_pair
        future_sell_notional = future.bid * pair.future_shares_per_pair
        future_buy_notional = future.ask * pair.future_shares_per_pair
        long_spot_tick_size = pair_leg_tick_size(pair, "stock", spot.ask, spot.raw)
        long_future_tick_size = pair_leg_tick_size(pair, "future", future.bid, future.raw)
        short_spot_tick_size = pair_leg_tick_size(pair, "stock", spot.bid, spot.raw)
        short_future_tick_size = pair_leg_tick_size(pair, "future", future.ask, future.raw)

        long_spot_short_future_pct = (
            future_sell_notional - spot_buy_notional
        ) / spot_buy_notional
        short_spot_long_future_pct = (
            future_buy_notional - spot_sell_notional
        ) / spot_sell_notional
        mid_basis_pct = (future.mid - spot.mid) / spot.mid
        long_spot_short_future_ticks = (
            (future.bid - spot.ask) - (spot.ask * EFFECTIVE_TICK_COST_RATE)
        ) / (long_spot_tick_size + long_future_tick_size)
        short_spot_long_future_ticks = (
            (spot.bid - future.ask) - (spot.bid * EFFECTIVE_TICK_COST_RATE)
        ) / (short_spot_tick_size + short_future_tick_size)
        long_spot_short_future_exit_ticks = (
            future.ask - spot.bid
        ) / (short_spot_tick_size + short_future_tick_size)
        short_spot_long_future_exit_ticks = (
            spot.ask - future.bid
        ) / (long_spot_tick_size + long_future_tick_size)

        return PairPricing(
            long_spot_short_future_pct=long_spot_short_future_pct,
            short_spot_long_future_pct=short_spot_long_future_pct,
            mid_basis_pct=mid_basis_pct,
            long_spot_short_future_ticks=long_spot_short_future_ticks,
            short_spot_long_future_ticks=short_spot_long_future_ticks,
            long_spot_short_future_exit_ticks=long_spot_short_future_exit_ticks,
            short_spot_long_future_exit_ticks=short_spot_long_future_exit_ticks,
            spot_tick_size=long_spot_tick_size,
            future_tick_size=long_future_tick_size,
            spot_buy_notional=spot_buy_notional,
            spot_sell_notional=spot_sell_notional,
            future_sell_notional=future_sell_notional,
            future_buy_notional=future_buy_notional,
        )


class SignalEngine:
    def evaluate(
        self,
        pair: PairConfig,
        pricing: PairPricing,
        position: PairPosition,
    ) -> Signal:
        if position.has_position:
            if self._should_exit(position, pair, pricing):
                return Signal.EXIT
            if (
                position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE
                and pricing.long_spot_short_future_pct >= pair.entry_threshold_pct
                and self._effective_ticks_ok(pair, pricing.long_spot_short_future_ticks)
            ):
                return Signal.ENTER_LONG_SPOT_SHORT_FUTURE
            if (
                pair.allow_short_spot
                and position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE
                and pricing.short_spot_long_future_pct <= -pair.entry_threshold_pct
                and self._effective_ticks_ok(pair, pricing.short_spot_long_future_ticks)
            ):
                return Signal.ENTER_SHORT_SPOT_LONG_FUTURE
            return Signal.HOLD

        if (
            pricing.long_spot_short_future_pct >= pair.entry_threshold_pct
            and self._effective_ticks_ok(pair, pricing.long_spot_short_future_ticks)
        ):
            return Signal.ENTER_LONG_SPOT_SHORT_FUTURE

        if (
            pair.allow_short_spot
            and pricing.short_spot_long_future_pct <= -pair.entry_threshold_pct
            and self._effective_ticks_ok(pair, pricing.short_spot_long_future_ticks)
        ):
            return Signal.ENTER_SHORT_SPOT_LONG_FUTURE

        return Signal.HOLD

    @staticmethod
    def _effective_ticks_ok(pair: PairConfig, effective_ticks: float) -> bool:
        return effective_ticks > pair.min_effective_tick_multiple

    def _should_exit(
        self,
        position: PairPosition,
        pair: PairConfig,
        pricing: PairPricing,
    ) -> bool:
        if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return (
                self._exit_ticks_triggered(pair, pricing.long_spot_short_future_exit_ticks)
                or pricing.short_spot_long_future_pct <= pair.exit_threshold_pct
            )
        if position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return (
                self._exit_ticks_triggered(pair, pricing.short_spot_long_future_exit_ticks)
                or pricing.long_spot_short_future_pct >= -pair.exit_threshold_pct
            )
        return False

    @staticmethod
    def _exit_ticks_triggered(pair: PairConfig, exit_ticks: float) -> bool:
        if pair.exit_tick_rule == "gte":
            return exit_ticks >= pair.exit_tick_multiple
        return exit_ticks <= pair.exit_tick_multiple

class RiskManager:
    def check(
        self,
        pair: PairConfig,
        market: PairMarket,
        signal: Signal,
        position: PairPosition,
        total_open_pairs: float = 0,
        max_total_pairs: int | None = None,
        total_spot_notional: float = 0,
        max_total_spot_notional: float | None = None,
        enforce_pair_max: bool = True,
    ) -> tuple[bool, str]:
        if signal == Signal.HOLD:
            return True, "hold"

        if self._is_trial_match(market.spot.raw):
            return False, "spot quote is trial match; spot is not tradable"

        if signal == Signal.EXIT:
            if not position.has_position:
                return False, "no position to exit"
            if exit_quantity_multiplier(pair, market, position) <= 0:
                return False, "insufficient exit top-of-book size for one pair"
            exit_realized_pnl = self._estimated_exit_realized_pnl(pair, market, position)
            if pair.min_exit_realized_pnl is not None:
                if exit_realized_pnl is None:
                    return False, "cannot estimate exit realized pnl"
                if exit_realized_pnl <= pair.min_exit_realized_pnl:
                    return (
                        False,
                        "exit realized pnl below threshold "
                        f"({exit_realized_pnl:.2f}/{pair.min_exit_realized_pnl:.2f})",
                    )
            return True, "exit allowed"

        if position.has_leg_exposure and not position.has_position:
            return False, "partial leg exposure exists; wait for both legs to fill or handle manually"

        if (
            signal in (Signal.ENTER_LONG_SPOT_SHORT_FUTURE, Signal.ENTER_SHORT_SPOT_LONG_FUTURE)
            and pair.min_entry_interval_sec > 0
            and position.last_entry_time is not None
        ):
            current_time = market_event_time_seconds(market)
            if current_time is not None:
                elapsed = current_time - float(position.last_entry_time)
                if elapsed < pair.min_entry_interval_sec:
                    return (
                        False,
                        "entry interval not elapsed "
                        f"({elapsed:.3f}/{pair.min_entry_interval_sec:.3f}s)",
                    )

        if enforce_pair_max and position.quantity >= pair.max_pairs:
            return False, "max pair position reached"

        if (
            max_total_pairs is not None
            and signal in (Signal.ENTER_LONG_SPOT_SHORT_FUTURE, Signal.ENTER_SHORT_SPOT_LONG_FUTURE)
            and total_open_pairs >= max_total_pairs
        ):
            return False, f"max total pair position reached ({total_open_pairs:g}/{max_total_pairs})"

        if (
            max_total_spot_notional is not None
            and signal in (Signal.ENTER_LONG_SPOT_SHORT_FUTURE, Signal.ENTER_SHORT_SPOT_LONG_FUTURE)
        ):
            new_spot_notional = self._entry_spot_notional(pair, market, signal)
            projected_spot_notional = total_spot_notional + new_spot_notional
            if projected_spot_notional > max_total_spot_notional:
                return (
                    False,
                    "max total spot notional reached "
                    f"({projected_spot_notional:.0f}/{max_total_spot_notional:.0f})",
                )

        if (
            signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE
            and (
                market.spot.ask_size < pair.stock_min_ask_size
                or market.future.bid_size < pair.future_min_bid_size
            )
        ):
            return False, "insufficient long-spot/short-future top-of-book size"

        if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE and not pair.allow_short_spot:
            return False, "short spot is disabled"

        if (
            signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE
            and (
                market.spot.bid_size < pair.stock_min_bid_size
                or market.future.ask_size < pair.future_min_ask_size
            )
        ):
            return False, "insufficient short-spot/long-future top-of-book size"

        return True, "entry allowed"

    @staticmethod
    def _entry_spot_notional(pair: PairConfig, market: PairMarket, signal: Signal) -> float:
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return market.spot.ask * pair.spot_order_qty
        if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return market.spot.bid * pair.spot_order_qty
        return 0.0

    @staticmethod
    def _estimated_exit_realized_pnl(
        pair: PairConfig,
        market: PairMarket,
        position: PairPosition,
    ) -> float | None:
        if position.entry_spot_price is None or position.entry_future_price is None:
            return None

        quantity_multiplier = exit_quantity_multiplier(pair, market, position)
        if quantity_multiplier <= 0:
            return None
        spot_qty = pair.spot_order_qty * quantity_multiplier
        future_multiplier = pair.future_pnl_multiplier * pair.future_order_qty * quantity_multiplier
        commission_rate = pair.stock_commission_rate * pair.stock_commission_discount

        entry_stock_fee = position.entry_spot_price * spot_qty * commission_rate
        if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            exit_spot_price = market.spot.bid
            exit_future_price = market.future.ask
            spot_pnl = (exit_spot_price - position.entry_spot_price) * spot_qty
            future_pnl = (position.entry_future_price - exit_future_price) * future_multiplier
            stock_tax = exit_spot_price * spot_qty * pair.stock_transaction_tax_rate
        elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            exit_spot_price = market.spot.ask
            exit_future_price = market.future.bid
            spot_pnl = (position.entry_spot_price - exit_spot_price) * spot_qty
            future_pnl = (exit_future_price - position.entry_future_price) * future_multiplier
            stock_tax = position.entry_spot_price * spot_qty * pair.stock_transaction_tax_rate
        else:
            return None

        exit_stock_fee = exit_spot_price * spot_qty * commission_rate
        return spot_pnl + future_pnl - entry_stock_fee - exit_stock_fee - stock_tax

    @staticmethod
    def _is_trial_match(raw: dict[str, Any] | None) -> bool:
        if not raw:
            return False
        for key in ("status_trial_status_tag", "isTrial", "is_trial"):
            value = raw.get(key)
            bool_value = coerce_bool(value)
            if bool_value is not None:
                return bool_value
            try:
                if int(value) == 1:
                    return True
            except (TypeError, ValueError):
                continue
        return False


class ExecutionEngine:
    def __init__(
        self,
        mode: Mode,
        allow_live_order: bool,
        order_router: OrderRouter | None = None,
        backtest_execution: BacktestExecutionConfig | None = None,
    ) -> None:
        self.mode = mode
        self.allow_live_order = allow_live_order
        self.order_router = order_router
        self.backtest_execution = backtest_execution or BacktestExecutionConfig()
        self.trades: list[TradeRecord] = []
        self._paper_latency_cooldown_until: dict[str, float] = {}

    def execute(
        self,
        signal: Signal,
        market: PairMarket,
        pricing: PairPricing,
        position: PairPosition,
    ) -> None:
        pair = market.pair

        if signal == Signal.HOLD:
            return

        if self.mode == Mode.LIVE and not self.allow_live_order:
            raise RuntimeError("LIVE mode requires allow_live_order=True")

        if self.mode == Mode.LIVE:
            if self.order_router is None:
                raise RuntimeError("LIVE mode requires an order router")
            self.order_router.place_pair_orders(signal, market, position)
            return

        if self.mode == Mode.PAPER:
            delayed_market = self._paper_market_after_latency(signal, market, position)
            if delayed_market is None:
                return
            delayed_pricing = PairPricer().price(delayed_market)
            trade = self._build_trade_record(signal, delayed_market, delayed_pricing, position)
            self._log_paper_order(trade, delayed_pricing)
            self.trades.append(trade)
            self._update_paper_position(signal, delayed_market, delayed_pricing, position)

    def _paper_market_after_latency(
        self,
        signal: Signal,
        market: PairMarket,
        position: PairPosition,
    ) -> PairMarket | None:
        execution = self.backtest_execution
        if execution.send_order_latency_ms <= 0 and execution.match_order_report_latency_ms <= 0:
            return market

        signal_time = market_event_time_seconds(market)
        if signal_time is None:
            logging.warning("[%s] paper latency skipped because signal time is unavailable", market.pair.name)
            return market

        pair = market.pair
        trigger_quote = self._trigger_quote(market)
        if trigger_quote is None:
            logging.warning("[%s] paper latency skipped because trigger quote is unavailable", pair.name)
            return market
        cooldown_until = self._paper_latency_cooldown_until.get(pair.name)
        if cooldown_until is not None and signal_time < cooldown_until:
            logging.debug(
                "[%s] latency paper skip: cooldown active %.3fs",
                pair.name,
                cooldown_until - signal_time,
            )
            return None
        send_sec = execution.send_order_latency_ms / 1_000
        report_sec = execution.match_order_report_latency_ms / 1_000
        send_ms = execution.send_order_latency_ms
        decision_ms = execution.send_order_latency_ms + execution.match_order_report_latency_ms
        second_ms = 2 * execution.send_order_latency_ms + execution.match_order_report_latency_ms
        late_flatten_ms = 3 * execution.send_order_latency_ms + 2 * execution.match_order_report_latency_ms
        first_leg = "future" if market.trigger_source == "future" else "stock"
        second_leg = "stock" if first_leg == "future" else "future"
        first_fill_time = signal_time + send_sec
        decision_time = first_fill_time + report_sec
        second_fill_time = decision_time + send_sec
        first_side = self._leg_side(signal, position, first_leg)
        second_side = self._leg_side(signal, position, second_leg)
        if first_side is None or second_side is None:
            logging.info("[%s] latency paper skip: unsupported leg side signal=%s", pair.name, signal.value)
            return None
        first_limit_price = self._leg_order_price(signal, position, market, first_leg)
        if first_limit_price is None:
            logging.info("[%s] latency paper skip: cannot price first leg", pair.name)
            return None

        first_quote = self._latency_quote_from_trigger(trigger_quote, first_leg, pair, send_ms)
        if first_quote is None:
            logging.info("[%s] latency paper skip: no first-leg %s quote at %.6f", pair.name, first_leg, first_fill_time)
            return None
        if not self._quote_fresh_at(first_quote, first_fill_time):
            logging.info("[%s] latency paper skip: stale first-leg %s quote", pair.name, first_leg)
            return None
        if not self._leg_fillable(signal, position, pair, first_leg, first_side, first_limit_price, first_quote):
            logging.info("[%s] latency paper skip: first-leg %s top-of-book unavailable", pair.name, first_leg)
            return None

        decision_spot = self._latency_quote_from_trigger(trigger_quote, "stock", pair, decision_ms)
        decision_future = self._latency_quote_from_trigger(trigger_quote, "future", pair, decision_ms)
        if (
            decision_spot is None
            or decision_future is None
            or not self._quote_fresh_at(decision_spot, decision_time)
            or not self._quote_fresh_at(decision_future, decision_time)
        ):
            logging.info("[%s] latency paper skip: missing or stale decision-time quote", pair.name)
            self._record_latency_first_leg_flatten(
                signal,
                market,
                position,
                trigger_quote,
                first_leg,
                first_side,
                first_quote,
                first_fill_time,
                decision_time,
                second_fill_time,
                second_fill_time,
                second_ms,
                "MISSING_DECISION_QUOTE",
            )
            return None
        decision_market = self._market_from_leg_quotes(pair, first_leg, first_quote, decision_spot, decision_future, market)
        should_check_second_leg_profit = execution.second_leg_profit_check and signal != Signal.EXIT
        if should_check_second_leg_profit and not self._second_leg_profit_ok(signal, decision_market, position):
            logging.info("[%s] latency paper skip: second-leg profit check failed", pair.name)
            self._record_latency_first_leg_flatten(
                signal,
                market,
                position,
                trigger_quote,
                first_leg,
                first_side,
                first_quote,
                first_fill_time,
                decision_time,
                second_fill_time,
                second_fill_time,
                second_ms,
                "SECOND_LEG_PROFIT_CHECK_FAILED",
            )
            return None
        second_limit_price = self._second_leg_limit_price(signal, position, decision_market, second_leg)
        if second_limit_price is None:
            logging.info("[%s] latency paper skip: cannot price second leg", pair.name)
            self._record_latency_first_leg_flatten(
                signal,
                market,
                position,
                trigger_quote,
                first_leg,
                first_side,
                first_quote,
                first_fill_time,
                decision_time,
                second_fill_time,
                second_fill_time,
                second_ms,
                "CANNOT_PRICE_SECOND_LEG",
            )
            return None

        second_quote = self._latency_quote_from_trigger(trigger_quote, second_leg, pair, second_ms)
        if second_quote is None:
            logging.info("[%s] latency paper skip: no second-leg %s quote at %.6f", pair.name, second_leg, second_fill_time)
            self._record_latency_first_leg_flatten(
                signal,
                market,
                position,
                trigger_quote,
                first_leg,
                first_side,
                first_quote,
                first_fill_time,
                decision_time,
                second_fill_time,
                second_fill_time + report_sec + send_sec,
                late_flatten_ms,
                "MISSING_SECOND_LEG_QUOTE",
            )
            return None
        if not self._quote_fresh_at(second_quote, second_fill_time):
            logging.info("[%s] latency paper skip: stale second-leg %s quote", pair.name, second_leg)
            self._record_latency_first_leg_flatten(
                signal,
                market,
                position,
                trigger_quote,
                first_leg,
                first_side,
                first_quote,
                first_fill_time,
                decision_time,
                second_fill_time,
                second_fill_time + report_sec + send_sec,
                late_flatten_ms,
                "STALE_SECOND_LEG_QUOTE",
            )
            return None
        if not self._leg_fillable(signal, position, pair, second_leg, second_side, second_limit_price, second_quote):
            logging.info("[%s] latency paper skip: second-leg %s top-of-book unavailable", pair.name, second_leg)
            self._record_latency_first_leg_flatten(
                signal,
                market,
                position,
                trigger_quote,
                first_leg,
                first_side,
                first_quote,
                first_fill_time,
                decision_time,
                second_fill_time,
                second_fill_time + report_sec + send_sec,
                late_flatten_ms,
                "SECOND_LEG_NOT_FILLABLE",
            )
            return None

        delayed_market = self._market_from_leg_quotes(
            pair,
            first_leg,
            first_quote,
            second_quote if second_leg == "stock" else decision_spot,
            second_quote if second_leg == "future" else decision_future,
            market,
        )
        logging.info(
            "[%s] latency paper fill first_leg=%s signal_time=%.6f first_fill=%.6f decision=%.6f second_fill=%.6f",
            pair.name,
            first_leg,
            signal_time,
            first_fill_time,
            decision_time,
            second_fill_time,
        )
        return delayed_market

    @staticmethod
    def _trigger_quote(market: PairMarket) -> Quote | None:
        if market.trigger_source == "stock":
            return market.spot
        if market.trigger_source == "future":
            return market.future
        return None

    @staticmethod
    def _latency_quote_from_trigger(
        trigger_quote: Quote,
        source: str,
        pair: PairConfig,
        offset_ms: float,
    ) -> Quote | None:
        symbol = pair.spot_symbol if source == "stock" else pair.future_symbol
        raw = trigger_quote.raw or {}
        prefix = latency_raw_prefix(source, symbol, offset_ms)
        bid = raw.get(f"{prefix}_bid")
        ask = raw.get(f"{prefix}_ask")
        if bid in (None, "") or ask in (None, ""):
            return None
        try:
            bid_float = float(bid)
            ask_float = float(ask)
        except (TypeError, ValueError):
            return None
        if bid_float <= 0 or ask_float <= 0:
            return None
        quote_raw = {
            "exchtime_tw": raw.get(f"{prefix}_timestamp"),
            "timestamp_tw": raw.get(f"{prefix}_timestamp"),
            "latency_age_ms": raw.get(f"{prefix}_age_ms"),
            "latency_offset_ms": offset_ms,
            "latency_source": source,
            "latency_symbol": symbol,
        }
        return Quote(
            symbol=symbol,
            bid=bid_float,
            ask=ask_float,
            bid_size=raw.get(f"{prefix}_bid_size") or 0,
            ask_size=raw.get(f"{prefix}_ask_size") or 0,
            last=raw.get(f"{prefix}_last"),
            raw=quote_raw,
        )

    def _quote_fresh_at(self, quote: Quote, timestamp_seconds: float) -> bool:
        max_age = self.backtest_execution.max_quote_age_sec
        if max_age is None:
            return True
        quote_time = quote_raw_time_seconds(quote.raw)
        if quote_time is None:
            return False
        return 0 <= timestamp_seconds - quote_time <= max_age

    def _record_latency_first_leg_flatten(
        self,
        signal: Signal,
        market: PairMarket,
        position: PairPosition,
        trigger_quote: Quote,
        first_leg: str,
        first_side: str,
        first_quote: Quote,
        first_fill_time: float,
        decision_time: float,
        second_fill_time: float,
        flatten_time: float,
        flatten_offset_ms: float,
        reason: str,
    ) -> None:
        pair = market.pair
        flatten_quote = self._latency_quote_from_trigger(trigger_quote, first_leg, pair, flatten_offset_ms)
        entry_price = self._side_price(first_side, first_quote)
        flatten_side = "sell" if first_side == "buy" else "buy"
        if flatten_quote is None or not self._quote_fresh_at(flatten_quote, flatten_time):
            flatten_price = None
            gross_pnl = None
            stock_fee = None
            stock_tax = None
            stock_cost = None
            realized_pnl = None
            realized_pnl_pct = None
            logging.warning(
                "[%s] latency paper first-leg exposure could not be flattened: no fresh %s quote at %.6f",
                pair.name,
                first_leg,
                flatten_time,
            )
        else:
            flatten_price = self._side_price(flatten_side, flatten_quote)
            gross_pnl = self._first_leg_flatten_gross_pnl(pair, first_leg, first_side, entry_price, flatten_price)
            stock_fee = self._first_leg_stock_fee(pair, first_leg, entry_price, flatten_price)
            stock_tax = self._first_leg_stock_tax(pair, first_leg, first_side, entry_price, flatten_price)
            stock_cost = None if stock_fee is None or stock_tax is None else stock_fee + stock_tax
            realized_pnl = gross_pnl if stock_cost is None else gross_pnl - stock_cost
            notional = self._first_leg_notional(pair, first_leg, entry_price)
            realized_pnl_pct = None if realized_pnl is None or notional <= 0 else realized_pnl / notional

        if first_leg == "stock":
            spot_side = f"{first_side.upper()}->{flatten_side.upper()}"
            future_side = "NONE"
            spot_qty = pair.spot_order_qty
            future_qty = 0
            spot_price = entry_price
            future_price = 0.0
            spot_exchtime = (first_quote.raw or {}).get("exchtime_tw") or (first_quote.raw or {}).get("timestamp_tw")
            future_exchtime = None
        else:
            spot_side = "NONE"
            future_side = f"{first_side.upper()}->{flatten_side.upper()}"
            spot_qty = 0
            future_qty = pair.future_order_qty
            spot_price = 0.0
            future_price = entry_price
            spot_exchtime = None
            future_exchtime = (first_quote.raw or {}).get("exchtime_tw") or (first_quote.raw or {}).get("timestamp_tw")

        trade = TradeRecord(
            pair_name=pair.name,
            signal=signal,
            spot_symbol=pair.spot_symbol,
            future_symbol=pair.future_symbol,
            spot_side=spot_side,
            future_side=future_side,
            spot_qty=spot_qty,
            future_qty=future_qty,
            spot_price=spot_price,
            future_price=future_price,
            basis_pct=PairPricer().price(market).mid_basis_pct,
            reason=f"LATENCY_FIRST_LEG_FLATTEN:{reason}",
            realized_pnl=realized_pnl,
            realized_pnl_pct=realized_pnl_pct,
            gross_pnl=gross_pnl,
            stock_fee=stock_fee,
            stock_tax=stock_tax,
            stock_cost=stock_cost,
            spot_pnl=gross_pnl if first_leg == "stock" else None,
            future_pnl=gross_pnl if first_leg == "future" else None,
            spot_exchtime=spot_exchtime,
            future_exchtime=future_exchtime,
            trigger_source=market.trigger_source,
            trigger_symbol=market.trigger_symbol,
            latency_first_leg=first_leg,
            latency_failure_reason=reason,
            latency_signal_time=market_event_time_seconds(market),
            latency_first_fill_time=first_fill_time,
            latency_decision_time=decision_time,
            latency_second_fill_time=second_fill_time,
            latency_flatten_time=flatten_time,
            latency_first_leg_entry_price=entry_price,
            latency_flatten_price=flatten_price,
        )
        self.trades.append(trade)
        if pair.cooldown_after_second_leg_failure_sec > 0:
            self._paper_latency_cooldown_until[pair.name] = max(
                self._paper_latency_cooldown_until.get(pair.name, 0.0),
                flatten_time + pair.cooldown_after_second_leg_failure_sec,
            )
        logging.info(
            "[%s] latency paper first-leg flatten leg=%s reason=%s entry=%.2f flatten=%s realized_pnl=%s",
            pair.name,
            first_leg,
            reason,
            entry_price,
            "unavailable" if flatten_price is None else f"{flatten_price:.2f}",
            "unknown" if realized_pnl is None else f"{realized_pnl:.2f}",
        )

    @staticmethod
    def _side_price(side: str, quote: Quote) -> float:
        return quote.ask if side == "buy" else quote.bid

    @staticmethod
    def _first_leg_flatten_gross_pnl(
        pair: PairConfig,
        first_leg: str,
        first_side: str,
        entry_price: float,
        flatten_price: float,
    ) -> float:
        if first_leg == "stock":
            quantity = pair.spot_order_qty
            return (
                (flatten_price - entry_price) * quantity
                if first_side == "buy"
                else (entry_price - flatten_price) * quantity
            )
        multiplier = pair.future_pnl_multiplier * pair.future_order_qty
        return (
            (flatten_price - entry_price) * multiplier
            if first_side == "buy"
            else (entry_price - flatten_price) * multiplier
        )

    @staticmethod
    def _first_leg_stock_fee(
        pair: PairConfig,
        first_leg: str,
        entry_price: float,
        flatten_price: float,
    ) -> float | None:
        if first_leg != "stock":
            return None
        commission_rate = pair.stock_commission_rate * pair.stock_commission_discount
        quantity = pair.spot_order_qty
        return (entry_price + flatten_price) * quantity * commission_rate

    @staticmethod
    def _first_leg_stock_tax(
        pair: PairConfig,
        first_leg: str,
        first_side: str,
        entry_price: float,
        flatten_price: float,
    ) -> float | None:
        if first_leg != "stock":
            return None
        sell_price = flatten_price if first_side == "buy" else entry_price
        return sell_price * pair.spot_order_qty * pair.stock_transaction_tax_rate

    @staticmethod
    def _first_leg_notional(pair: PairConfig, first_leg: str, entry_price: float) -> float:
        if first_leg == "stock":
            return entry_price * pair.spot_order_qty
        return entry_price * pair.future_pnl_multiplier * pair.future_order_qty

    @staticmethod
    def _market_from_leg_quotes(
        pair: PairConfig,
        first_leg: str,
        first_quote: Quote,
        spot_quote: Quote,
        future_quote: Quote,
        original_market: PairMarket,
    ) -> PairMarket:
        if first_leg == "stock":
            spot_quote = first_quote
        else:
            future_quote = first_quote
        return PairMarket(
            pair=pair,
            spot=spot_quote,
            future=future_quote,
            trigger_source=original_market.trigger_source,
            trigger_symbol=original_market.trigger_symbol,
        )

    def _leg_fillable(
        self,
        signal: Signal,
        position: PairPosition,
        pair: PairConfig,
        leg: str,
        side: str,
        limit_price: float,
        quote: Quote,
    ) -> bool:
        if not self._leg_top_of_book_available(signal, position, pair, leg, quote):
            return False
        if side == "buy":
            return quote.ask <= limit_price
        if side == "sell":
            return quote.bid >= limit_price
        return False

    @staticmethod
    def _leg_top_of_book_available(
        signal: Signal,
        position: PairPosition,
        pair: PairConfig,
        leg: str,
        quote: Quote,
    ) -> bool:
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return quote.ask_size >= pair.stock_min_ask_size if leg == "stock" else quote.bid_size >= pair.future_min_bid_size
        if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return quote.bid_size >= pair.stock_min_bid_size if leg == "stock" else quote.ask_size >= pair.future_min_ask_size
        if signal == Signal.EXIT and position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return quote.bid_size >= pair.stock_min_bid_size if leg == "stock" else quote.ask_size >= pair.future_min_ask_size
        if signal == Signal.EXIT and position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return quote.ask_size >= pair.stock_min_ask_size if leg == "stock" else quote.bid_size >= pair.future_min_bid_size
        return False

    @staticmethod
    def _leg_side(signal: Signal, position: PairPosition, leg: str) -> str | None:
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return "buy" if leg == "stock" else "sell"
        if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return "sell" if leg == "stock" else "buy"
        if signal == Signal.EXIT and position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return "sell" if leg == "stock" else "buy"
        if signal == Signal.EXIT and position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return "buy" if leg == "stock" else "sell"
        return None

    def _leg_order_price(
        self,
        signal: Signal,
        position: PairPosition,
        market: PairMarket,
        leg: str,
    ) -> float | None:
        side = self._leg_side(signal, position, leg)
        if side is None:
            return None
        quote = market.spot if leg == "stock" else market.future
        return quote.ask if side == "buy" else quote.bid

    def _second_leg_limit_price(
        self,
        signal: Signal,
        position: PairPosition,
        market: PairMarket,
        leg: str,
    ) -> float | None:
        base_price = self._leg_order_price(signal, position, market, leg)
        if base_price is None:
            return None
        pair = market.pair
        if pair.second_leg_tick_offset <= 0:
            return base_price
        quote = market.spot if leg == "stock" else market.future
        tick_size = self._leg_tick_size(pair, leg, base_price, quote.raw)
        offset = tick_size * pair.second_leg_tick_offset
        side = self._leg_side(signal, position, leg)
        if side == "buy":
            return base_price + offset
        if side == "sell":
            return max(base_price - offset, tick_size)
        return None

    @staticmethod
    def _leg_tick_size(pair: PairConfig, leg: str, price: float, raw: dict[str, Any] | None = None) -> float:
        return pair_leg_tick_size(pair, leg, price, raw)

    def _second_leg_profit_ok(
        self,
        signal: Signal,
        market: PairMarket,
        position: PairPosition,
    ) -> bool:
        pair = market.pair
        pricing = PairPricer().price(market)
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            threshold = pair.min_second_leg_adjusted_basis_pct
            if threshold is None:
                threshold = pair.entry_threshold_pct
            return pricing.long_spot_short_future_pct >= threshold
        if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            threshold = pair.min_second_leg_adjusted_basis_pct
            if threshold is None:
                threshold = pair.entry_threshold_pct
            return -pricing.short_spot_long_future_pct >= threshold
        if signal == Signal.EXIT and pair.min_exit_realized_pnl is not None:
            quantity_multiplier = exit_quantity_multiplier(pair, market, position)
            if quantity_multiplier <= 0:
                return False
            realized_pnl = self._realized_pnl_for_market(signal, market, position, quantity_multiplier)
            return realized_pnl is not None and realized_pnl > pair.min_exit_realized_pnl
        return True

    def _realized_pnl_for_market(
        self,
        signal: Signal,
        market: PairMarket,
        position: PairPosition,
        quantity_multiplier: float,
    ) -> float | None:
        if signal == Signal.EXIT and position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return self._realized_pnl(signal, market.pair, position, market.spot.bid, market.future.ask, quantity_multiplier)
        if signal == Signal.EXIT and position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return self._realized_pnl(signal, market.pair, position, market.spot.ask, market.future.bid, quantity_multiplier)
        return None

    def _record_trade(
        self,
        signal: Signal,
        market: PairMarket,
        pricing: PairPricing,
        position: PairPosition,
    ) -> None:
        self.trades.append(self._build_trade_record(signal, market, pricing, position))

    def _build_trade_record(
        self,
        signal: Signal,
        market: PairMarket,
        pricing: PairPricing,
        position: PairPosition,
    ) -> TradeRecord:
        pair = market.pair
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            basis_pct = pricing.long_spot_short_future_pct
            spot_side = "BUY"
            future_side = "SELL"
            spot_price = market.spot.ask
            future_price = market.future.bid
        elif signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            basis_pct = -pricing.short_spot_long_future_pct
            spot_side = "SELL"
            future_side = "BUY"
            spot_price = market.spot.bid
            future_price = market.future.ask
        elif signal == Signal.EXIT and position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            basis_pct = pricing.short_spot_long_future_pct
            spot_side = "SELL"
            future_side = "BUY"
            spot_price = market.spot.bid
            future_price = market.future.ask
        elif signal == Signal.EXIT and position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            basis_pct = pricing.long_spot_short_future_pct
            spot_side = "BUY"
            future_side = "SELL"
            spot_price = market.spot.ask
            future_price = market.future.bid
        else:
            basis_pct = pricing.mid_basis_pct
            spot_side = "UNKNOWN"
            future_side = "UNKNOWN"
            spot_price = market.spot.mid
            future_price = market.future.mid
        quantity_multiplier = exit_quantity_multiplier(pair, market, position) if signal == Signal.EXIT else 1
        spot_raw = market.spot.raw or {}
        future_raw = market.future.raw or {}
        return TradeRecord(
            pair_name=pair.name,
            signal=signal,
            spot_symbol=pair.spot_symbol,
            future_symbol=pair.future_symbol,
            spot_side=spot_side,
            future_side=future_side,
            spot_qty=int(round(pair.spot_order_qty * quantity_multiplier)),
            future_qty=int(round(pair.future_order_qty * quantity_multiplier)),
            spot_price=spot_price,
            future_price=future_price,
            basis_pct=basis_pct,
            reason=self._trade_reason(signal, pair, pricing, position),
            realized_pnl=self._realized_pnl(signal, pair, position, spot_price, future_price, quantity_multiplier),
            realized_pnl_pct=self._realized_pnl_pct(signal, pair, position, spot_price, future_price, quantity_multiplier),
            gross_pnl=self._gross_pnl(signal, pair, position, spot_price, future_price, quantity_multiplier),
            stock_fee=self._stock_fee(signal, pair, position, spot_price, quantity_multiplier),
            stock_tax=self._stock_tax(signal, pair, position, spot_price, quantity_multiplier),
            stock_cost=self._stock_cost(signal, pair, position, spot_price, quantity_multiplier),
            spot_pnl=self._spot_pnl(signal, pair, position, spot_price, quantity_multiplier),
            future_pnl=self._future_pnl(signal, pair, position, future_price, quantity_multiplier),
            spot_exchtime=spot_raw.get("exchtime_tw") or spot_raw.get("timestamp_tw"),
            future_exchtime=future_raw.get("exchtime_tw") or future_raw.get("timestamp_tw"),
            trigger_source=market.trigger_source,
            trigger_symbol=market.trigger_symbol,
        )

    def _log_paper_order(self, trade: TradeRecord, pricing: PairPricing) -> None:
        time_lines = []
        if trade.spot_exchtime is not None:
            time_lines.append(f"    spot_exchtime={trade.spot_exchtime}")
        if trade.future_exchtime is not None:
            time_lines.append(f"    future_exchtime={trade.future_exchtime}")
        time_text = "\n" + "\n".join(time_lines) if time_lines else ""
        logging.info(
            (
                "\n[PAPER ORDER]\n"
                "  pair=%s\n"
                "  signal=%s\n"
                "  spot_order=%s %s qty=%s price=%.2f\n"
                "  future_order=%s %s qty=%s price=%.2f\n"
                "  basis=%s mid_basis=%s%s"
            ),
            trade.pair_name,
            trade.signal.value,
            trade.spot_side,
            trade.spot_symbol,
            trade.spot_qty,
            trade.spot_price,
            trade.future_side,
            trade.future_symbol,
            trade.future_qty,
            trade.future_price,
            pct(trade.basis_pct),
            pct(pricing.mid_basis_pct),
            time_text,
        )

    def _trade_reason(
        self,
        signal: Signal,
        pair: PairConfig,
        pricing: PairPricing,
        position: PairPosition,
    ) -> str:
        if signal != Signal.EXIT:
            return "ENTRY"
        if position.entry_basis_pct is None:
            return "EXIT_THRESHOLD"
        if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            pnl_pct = position.entry_basis_pct - pricing.short_spot_long_future_pct
            if pnl_pct <= pair.stop_loss_pct:
                return "STOP_LOSS"
        elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            pnl_pct = position.entry_basis_pct + pricing.long_spot_short_future_pct
            if pnl_pct <= pair.stop_loss_pct:
                return "STOP_LOSS"
        return "EXIT_THRESHOLD"

    def _gross_pnl(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_spot_price: float,
        exit_future_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        spot_pnl = self._spot_pnl(signal, pair, position, exit_spot_price, quantity_multiplier)
        future_pnl = self._future_pnl(signal, pair, position, exit_future_price, quantity_multiplier)
        if spot_pnl is None or future_pnl is None:
            return None
        return spot_pnl + future_pnl

    def _realized_pnl(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_spot_price: float,
        exit_future_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        gross_pnl = self._gross_pnl(signal, pair, position, exit_spot_price, exit_future_price, quantity_multiplier)
        stock_cost = self._stock_cost(signal, pair, position, exit_spot_price, quantity_multiplier)
        if gross_pnl is None or stock_cost is None:
            return None
        return gross_pnl - stock_cost

    def _realized_pnl_pct(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_spot_price: float,
        exit_future_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        pnl = self._realized_pnl(signal, pair, position, exit_spot_price, exit_future_price, quantity_multiplier)
        if pnl is None or position.entry_spot_price in (None, 0):
            return None
        notional = position.entry_spot_price * pair.spot_order_qty * quantity_multiplier
        return pnl / notional

    def _spot_pnl(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_spot_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        if signal != Signal.EXIT or position.entry_spot_price is None:
            return None
        multiplier = pair.spot_order_qty * quantity_multiplier
        if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return (exit_spot_price - position.entry_spot_price) * multiplier
        if position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return (position.entry_spot_price - exit_spot_price) * multiplier
        return None

    def _future_pnl(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_future_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        if signal != Signal.EXIT or position.entry_future_price is None:
            return None
        multiplier = pair.future_pnl_multiplier * pair.future_order_qty * quantity_multiplier
        if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return (position.entry_future_price - exit_future_price) * multiplier
        if position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return (exit_future_price - position.entry_future_price) * multiplier
        return None

    def _stock_fee(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_spot_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        if signal != Signal.EXIT or position.entry_spot_price is None:
            return None
        quantity = pair.spot_order_qty * quantity_multiplier
        commission_rate = pair.stock_commission_rate * pair.stock_commission_discount
        entry_fee = position.entry_spot_price * quantity * commission_rate
        exit_fee = exit_spot_price * quantity * commission_rate
        return entry_fee + exit_fee

    def _stock_tax(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_spot_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        if signal != Signal.EXIT or position.entry_spot_price is None:
            return None
        quantity = pair.spot_order_qty * quantity_multiplier
        if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            sell_notional = exit_spot_price * quantity
        elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            sell_notional = position.entry_spot_price * quantity
        else:
            return None
        return sell_notional * pair.stock_transaction_tax_rate

    def _stock_cost(
        self,
        signal: Signal,
        pair: PairConfig,
        position: PairPosition,
        exit_spot_price: float,
        quantity_multiplier: float,
    ) -> float | None:
        fee = self._stock_fee(signal, pair, position, exit_spot_price, quantity_multiplier)
        tax = self._stock_tax(signal, pair, position, exit_spot_price, quantity_multiplier)
        if fee is None or tax is None:
            return None
        return fee + tax

    def print_summary(self, positions: dict[str, PairPosition]) -> None:
        print("\n=== Trade Summary ===")
        print(f"mode={self.mode.value} total_trades={len(self.trades)}")
        if not self.trades:
            print("no simulated fills")
        else:
            counts: dict[str, int] = {}
            for trade in self.trades:
                counts[trade.pair_name] = counts.get(trade.pair_name, 0) + 1
            for pair_name in sorted(counts):
                position = positions[pair_name]
                print(
                    f"{pair_name}: trades={counts[pair_name]} final_quantity={position.quantity} "
                    f"direction={position.direction.value} stock_units={position.stock_units} "
                    f"future_units={position.future_units}"
                )
            self._print_closed_pnl_summary()
            print("--- fills ---")
            for idx, trade in enumerate(self.trades, start=1):
                pnl_text = ""
                if trade.realized_pnl is not None:
                    pnl_text = (
                        f" gross_pnl={money_text(trade.gross_pnl)}"
                        f" stock_cost={money_text(trade.stock_cost)}"
                        f" stock_fee={money_text(trade.stock_fee)}"
                        f" stock_tax={money_text(trade.stock_tax)}"
                        f" realized_pnl={money_text(trade.realized_pnl)}"
                        f" realized_pnl_pct={pct(trade.realized_pnl_pct or 0)}"
                        f" spot_pnl={money_text(trade.spot_pnl)}"
                        f" future_pnl={money_text(trade.future_pnl)}"
                    )
                time_text = ""
                if trade.spot_exchtime is not None:
                    time_text += f" spot_exchtime={trade.spot_exchtime}"
                if trade.future_exchtime is not None:
                    time_text += f" future_exchtime={trade.future_exchtime}"
                print(
                    f"{idx}. {trade.pair_name} {trade.signal.value} "
                    f"spot_order={trade.spot_side} {trade.spot_symbol} qty={trade.spot_qty} price={trade.spot_price:.2f} "
                    f"future_order={trade.future_side} {trade.future_symbol} qty={trade.future_qty} price={trade.future_price:.2f} "
                    f"basis={pct(trade.basis_pct)} reason={trade.reason} "
                    f"{pnl_text} "
                    f"{time_text}"
                )
        print("=== End Summary ===")

    def _print_closed_pnl_summary(self) -> None:
        closed_trades = [trade for trade in self.trades if trade.realized_pnl is not None]
        if not closed_trades:
            print("--- closed pnl ---")
            print("closed_trades=0")
            return

        total_gross_pnl = sum(trade.gross_pnl or 0 for trade in closed_trades)
        total_stock_cost = sum(trade.stock_cost or 0 for trade in closed_trades)
        total_stock_fee = sum(trade.stock_fee or 0 for trade in closed_trades)
        total_stock_tax = sum(trade.stock_tax or 0 for trade in closed_trades)
        total_realized_pnl = sum(trade.realized_pnl or 0 for trade in closed_trades)
        total_spot_pnl = sum(trade.spot_pnl or 0 for trade in closed_trades)
        total_future_pnl = sum(trade.future_pnl or 0 for trade in closed_trades)
        print("--- closed pnl ---")
        print(
            f"closed_trades={len(closed_trades)} "
            f"gross_pnl={total_gross_pnl:.2f} "
            f"stock_cost={total_stock_cost:.2f} "
            f"stock_fee={total_stock_fee:.2f} "
            f"stock_tax={total_stock_tax:.2f} "
            f"realized_pnl={total_realized_pnl:.2f} "
            f"spot_pnl={total_spot_pnl:.2f} "
            f"future_pnl={total_future_pnl:.2f}"
        )

    def _update_paper_position(
        self,
        signal: Signal,
        market: PairMarket,
        pricing: PairPricing,
        position: PairPosition,
    ) -> None:
        pair = market.pair
        if signal == Signal.EXIT:
            quantity_multiplier = exit_quantity_multiplier(pair, market, position)
            if quantity_multiplier <= 0:
                return
            if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
                position.stock_units = max(position.stock_units - quantity_multiplier, 0)
                position.future_units = max(position.future_units - quantity_multiplier, 0)
            elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
                position.stock_units = min(position.stock_units + quantity_multiplier, 0)
                position.future_units = min(position.future_units + quantity_multiplier, 0)
            position.quantity = max(abs(position.quantity) - quantity_multiplier, 0)
            if position.quantity <= 0 or position.stock_units == 0 or position.future_units == 0:
                position.quantity = 0
                position.direction = Signal.HOLD
                position.entry_basis_pct = None
                position.entry_spot_price = None
                position.entry_future_price = None
                position.stock_units = 0
                position.future_units = 0
            return

        old_quantity = abs(position.quantity)
        new_quantity = old_quantity + 1
        position.direction = signal
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            position.entry_basis_pct = weighted_average(
                position.entry_basis_pct,
                old_quantity,
                pricing.long_spot_short_future_pct,
                1,
            )
            position.entry_spot_price = weighted_average(
                position.entry_spot_price,
                old_quantity,
                market.spot.ask,
                1,
            )
            position.entry_future_price = weighted_average(
                position.entry_future_price,
                old_quantity,
                market.future.bid,
                1,
            )
            position.stock_units += 1
            position.future_units += 1
        elif signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            position.entry_basis_pct = weighted_average(
                position.entry_basis_pct,
                old_quantity,
                -pricing.short_spot_long_future_pct,
                1,
            )
            position.entry_spot_price = weighted_average(
                position.entry_spot_price,
                old_quantity,
                market.spot.bid,
                1,
            )
            position.entry_future_price = weighted_average(
                position.entry_future_price,
                old_quantity,
                market.future.ask,
                1,
            )
            position.stock_units -= 1
            position.future_units -= 1
        position.last_entry_time = market_event_time_seconds(market)
        position.quantity = new_quantity


class StopLossAwareSignalEngine(SignalEngine):
    def evaluate(
        self,
        pair: PairConfig,
        pricing: PairPricing,
        position: PairPosition,
    ) -> Signal:
        if position.has_position and self._hit_pair_stop_loss(pair, pricing, position):
            return Signal.EXIT
        return super().evaluate(pair, pricing, position)

    def _hit_pair_stop_loss(
        self,
        pair: PairConfig,
        pricing: PairPricing,
        position: PairPosition,
    ) -> bool:
        if position.entry_basis_pct is None:
            return False

        if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            pnl_pct = position.entry_basis_pct - pricing.short_spot_long_future_pct
        elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            pnl_pct = position.entry_basis_pct + pricing.long_spot_short_future_pct
        else:
            return False
        return pnl_pct <= pair.stop_loss_pct


def market_event_time_seconds(market: PairMarket) -> float | None:
    raws: list[dict[str, Any] | None]
    if market.trigger_source == "stock":
        raws = [market.spot.raw, market.future.raw]
    elif market.trigger_source == "future":
        raws = [market.future.raw, market.spot.raw]
    else:
        raws = [market.spot.raw, market.future.raw]

    for raw in raws:
        timestamp = quote_raw_time_seconds(raw)
        if timestamp is not None:
            return timestamp
    return None


def quote_raw_time_seconds(raw: dict[str, Any] | None) -> float | None:
    if not raw:
        return None
    for key in (
        "exchtime_tw",
        "timestamp_tw",
        "exchtime",
        "timestamp",
        "time",
        "date",
        "datetime",
    ):
        timestamp = coerce_time_seconds(raw.get(key))
        if timestamp is not None:
            return timestamp

    last_trade = raw.get("lastTrade")
    if isinstance(last_trade, dict):
        for key in ("time", "timestamp", "date", "datetime"):
            timestamp = coerce_time_seconds(last_trade.get(key))
            if timestamp is not None:
                return timestamp
    return None


def coerce_time_seconds(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)):
        # Historical raw exchange times are often nanoseconds since epoch.
        if value > 10_000_000_000_000:
            return float(value) / 1_000_000_000
        if value > 10_000_000_000:
            return float(value) / 1_000
        return float(value)
    to_pydatetime = getattr(value, "to_pydatetime", None)
    if callable(to_pydatetime):
        return to_pydatetime().timestamp()
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return None

