from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from scripts.tw_stock_hftbacktest import import_hftbacktest, workspace_root

from .config import build_initial_position
from .models import PairConfig, PairMarket, PairPosition, Quote, Signal
from .strategy import PairPricer, RiskManager, StopLossAwareSignalEngine, weighted_average
from .ticks import pair_leg_tick_size, tick_size_for_prices, trade_date_from_raw, tw_stock_future_tick_size
from .utils import STOCK_BOARD_LOT_SHARES, exit_quantity_multiplier


STOCK_ASSET_NO = 0
FUTURE_ASSET_NO = 1


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
    order_entry_latency_ns: int = 0
    order_response_latency_ns: int = 0
    feed_latency_offset_ns: int = 0
    queue_model: str = "risk_adverse"
    queue_model_param: float = 3.0
    last_trades_capacity: int = 100


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


class HbtPairBacktester:
    def __init__(self, config: HbtPairBacktestConfig, hbtpkg: Any | None = None) -> None:
        self.config = config
        self.hbtpkg = hbtpkg or import_hftbacktest(workspace_root(config.spot.data))
        self.pricer = PairPricer()
        self.signal_engine = StopLossAwareSignalEngine()
        self.risk = RiskManager()
        self.position = build_initial_position(config.pair)
        self.order_id = 1_000_000
        self.rows: list[dict[str, Any]] = []
        self.market_rows: list[dict[str, Any]] = []
        self.latency_rows: list[dict[str, Any]] = []
        self.resolved_tick_sizes: dict[str, float] = {}

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        hbt = self._build_backtest()
        try:
            step = 0
            while True:
                if self.config.max_steps is not None and step >= self.config.max_steps:
                    break
                if self.config.max_trades is not None and len(self.rows) >= self.config.max_trades:
                    break
                if hbt.elapse(self.config.step_ns) != 0:
                    break
                step += 1

                market = self._current_market(hbt)
                if market is None:
                    continue
                pricing = self.pricer.price(market)
                self._record_market_row(hbt, step, market, pricing)
                signal = self.signal_engine.evaluate(self.config.pair, pricing, self.position)
                if signal == Signal.HOLD:
                    continue
                ok, reason = self.risk.check(
                    self.config.pair,
                    market,
                    signal,
                    self.position,
                    enforce_pair_max=self.config.enforce_risk_limits,
                )
                if not ok:
                    self._append_skip_row(hbt, step, signal, market, pricing, reason)
                    continue
                self._execute_signal(hbt, step, signal, market, pricing)

            trades = pd.DataFrame(self.rows)
            summary = pd.DataFrame([self._summary_row(trades)])
            return trades, summary
        finally:
            close = getattr(hbt, "close", None)
            if close is not None:
                close()

    def _build_backtest(self):
        spot_asset, spot_tick = self._build_asset(self.config.spot)
        future_asset, future_tick = self._build_asset(self.config.future)
        self.resolved_tick_sizes = {"spot_tick_size": spot_tick, "future_tick_size": future_tick}
        return self.hbtpkg.HashMapMarketDepthBacktest([spot_asset, future_asset])

    def _build_asset(self, asset_config: HbtAssetConfig):
        tick_size = asset_config.tick_size or infer_hbt_asset_tick_size(
            asset_config.data,
            asset_config.instrument,
            fallback=1.0,
        )
        asset = (
            self.hbtpkg.BacktestAsset()
            .data(str(asset_config.data))
            .linear_asset(asset_config.contract_size)
            .constant_order_latency(
                asset_config.order_entry_latency_ns,
                asset_config.order_response_latency_ns,
            )
            .no_partial_fill_exchange()
            .trading_value_fee_model(asset_config.maker_fee, asset_config.taker_fee)
            .tick_size(tick_size)
            .lot_size(asset_config.lot_size)
            .last_trades_capacity(asset_config.last_trades_capacity)
        )
        if asset_config.feed_latency_offset_ns:
            asset = asset.latency_offset(asset_config.feed_latency_offset_ns)
        return apply_queue_model(asset, asset_config), tick_size

    def _current_market(self, hbt) -> PairMarket | None:
        spot = quote_from_depth(hbt.depth(STOCK_ASSET_NO), self.config.pair.spot_symbol, hbt.current_timestamp)
        future = quote_from_depth(hbt.depth(FUTURE_ASSET_NO), self.config.pair.future_symbol, hbt.current_timestamp)
        if spot is None or future is None:
            return None
        return PairMarket(
            pair=self.config.pair,
            spot=spot,
            future=future,
            trigger_source="hbt",
            trigger_symbol=f"{self.config.pair.spot_symbol}/{self.config.pair.future_symbol}",
        )

    def _execute_signal(
        self,
        hbt,
        step: int,
        signal: Signal,
        signal_market: PairMarket,
        signal_pricing: Any,
    ) -> None:
        signal_timestamp = int(hbt.current_timestamp)
        first_leg = self.config.first_leg
        second_leg = "stock" if first_leg == "future" else "future"
        self._record_latency_row(
            hbt,
            step,
            signal,
            "SIGNAL",
            "signal_market",
            signal_market,
            signal_pricing,
            local_ts=signal_timestamp,
        )
        first_fill = self._submit_leg(hbt, signal, first_leg, signal_market, is_second_leg=False)
        self._record_latency_row(
            hbt,
            step,
            signal,
            "FIRST_ORDER_RESPONSE",
            "first_order_entry",
            signal_market,
            signal_pricing,
            fill=first_fill,
        )
        if not first_fill.filled:
            self._append_execution_row(
                hbt,
                step,
                signal,
                "FIRST_LEG_UNFILLED",
                signal_market,
                signal_pricing,
                first_fill,
                None,
                realized_pnl=None,
                failure_reason="first leg did not fill",
                signal_timestamp=signal_timestamp,
            )
            return

        if self.config.second_leg_delay_ns > 0 and hbt.elapse(self.config.second_leg_delay_ns) != 0:
            self._append_execution_row(
                hbt,
                step,
                signal,
                "SECOND_LEG_DELAY_END_OF_DATA",
                signal_market,
                signal_pricing,
                first_fill,
                None,
                realized_pnl=None,
                failure_reason="end of data before second leg",
                signal_timestamp=signal_timestamp,
            )
            return

        post_first_feed_ok = self._wait_post_first_feed(hbt)
        decision_market = self._current_market(hbt) or signal_market
        decision_pricing = self.pricer.price(decision_market)
        self._record_latency_row(
            hbt,
            step,
            signal,
            "POST_FIRST_MARKET" if post_first_feed_ok else "POST_FIRST_FEED_TIMEOUT",
            "post_first_market",
            decision_market,
            decision_pricing,
        )
        if not post_first_feed_ok:
            flatten_fill = self._flatten_first_leg(hbt, signal, first_leg, decision_market)
            self._record_latency_row(
                hbt,
                step,
                signal,
                "FLATTEN_FIRST_ORDER",
                "flatten_first_order",
                decision_market,
                decision_pricing,
                fill=flatten_fill,
            )
            realized = self._first_leg_flatten_pnl(signal, first_fill, flatten_fill)
            self._append_execution_row(
                hbt,
                step,
                signal,
                "POST_FIRST_FEED_TIMEOUT",
                decision_market,
                decision_pricing,
                first_fill,
                None,
                flatten_fill=flatten_fill,
                realized_pnl=realized,
                failure_reason="post-first feed refresh timeout",
                signal_timestamp=signal_timestamp,
            )
            return

        if self.config.second_leg_profit_check and not self._second_leg_profit_ok(signal, decision_market):
            flatten_fill = self._flatten_first_leg(hbt, signal, first_leg, decision_market)
            self._record_latency_row(
                hbt,
                step,
                signal,
                "FLATTEN_FIRST_ORDER",
                "flatten_first_order",
                decision_market,
                decision_pricing,
                fill=flatten_fill,
            )
            realized = self._first_leg_flatten_pnl(signal, first_fill, flatten_fill)
            self._append_execution_row(
                hbt,
                step,
                signal,
                "SECOND_LEG_PROFIT_CHECK_FAILED",
                decision_market,
                decision_pricing,
                first_fill,
                flatten_fill,
                realized_pnl=realized,
                failure_reason="second leg profit check failed",
                signal_timestamp=signal_timestamp,
            )
            return

        second_fill = self._submit_leg(hbt, signal, second_leg, decision_market, is_second_leg=True)
        self._record_latency_row(
            hbt,
            step,
            signal,
            "SECOND_ORDER_RESPONSE",
            "second_order_entry",
            decision_market,
            decision_pricing,
            fill=second_fill,
        )
        if not second_fill.filled:
            flatten_fill = self._flatten_first_leg(hbt, signal, first_leg, decision_market)
            self._record_latency_row(
                hbt,
                step,
                signal,
                "FLATTEN_FIRST_ORDER",
                "flatten_first_order",
                decision_market,
                decision_pricing,
                fill=flatten_fill,
            )
            realized = self._first_leg_flatten_pnl(signal, first_fill, flatten_fill)
            self._append_execution_row(
                hbt,
                step,
                signal,
                "SECOND_LEG_UNFILLED",
                decision_market,
                decision_pricing,
                first_fill,
                second_fill,
                flatten_fill=flatten_fill,
                realized_pnl=realized,
                failure_reason="second leg did not fill",
                signal_timestamp=signal_timestamp,
            )
            return

        realized_pnl = self._realized_pnl(signal, first_fill, second_fill)
        self._update_position(signal, first_fill, second_fill, decision_pricing)
        self._append_execution_row(
            hbt,
            step,
            signal,
            "FILLED",
            decision_market,
            decision_pricing,
            first_fill,
            second_fill,
            realized_pnl=realized_pnl,
            failure_reason=None,
            signal_timestamp=signal_timestamp,
        )

    def _wait_post_first_feed(self, hbt) -> bool:
        mode = self.config.post_first_feed_wait
        if mode == "none":
            return True
        start_spot = hbt_feed_latency(hbt, STOCK_ASSET_NO)
        start_future = hbt_feed_latency(hbt, FUTURE_ASSET_NO)
        if self._post_first_feed_ready(mode, start_spot, start_future, hbt):
            return True

        deadline = int(hbt.current_timestamp) + max(0, self.config.post_first_feed_timeout_ns)
        poll_ns = max(1, self.config.post_first_feed_poll_ns)
        while int(hbt.current_timestamp) < deadline:
            step_ns = min(poll_ns, deadline - int(hbt.current_timestamp))
            if hbt.elapse(step_ns) != 0:
                return False
            if self._post_first_feed_ready(mode, start_spot, start_future, hbt):
                return True
        return self._post_first_feed_ready(mode, start_spot, start_future, hbt)

    @staticmethod
    def _post_first_feed_ready(
        mode: str,
        start_spot: tuple[int, int] | None,
        start_future: tuple[int, int] | None,
        hbt,
    ) -> bool:
        spot_ready = feed_refreshed(start_spot, hbt_feed_latency(hbt, STOCK_ASSET_NO))
        future_ready = feed_refreshed(start_future, hbt_feed_latency(hbt, FUTURE_ASSET_NO))
        if mode == "spot":
            return spot_ready
        if mode == "future":
            return future_ready
        if mode == "any":
            return spot_ready or future_ready
        if mode == "both":
            return spot_ready and future_ready
        raise ValueError(f"unknown post_first_feed_wait: {mode}")

    def _submit_leg(
        self,
        hbt,
        signal: Signal,
        leg: str,
        market: PairMarket,
        is_second_leg: bool,
    ) -> HbtLegFill:
        side = leg_side(signal, self.position, leg)
        if side is None:
            raise RuntimeError(f"unsupported signal/leg: {signal.value}/{leg}")
        quote = market.spot if leg == "stock" else market.future
        price = quote.ask if side == "buy" else quote.bid
        if is_second_leg:
            price = self._second_leg_limit_price(market, leg, side, price)
        return self._submit_raw_leg(
            hbt,
            leg=leg,
            side=side,
            price=price,
            qty=hbt_order_qty(self.config.pair, leg),
            time_in_force=time_in_force_for_leg(self.config.pair, leg, self.config.first_leg),
        )

    def _submit_raw_leg(
        self,
        hbt,
        leg: str,
        side: str,
        price: float,
        qty: float,
        time_in_force: str,
    ) -> HbtLegFill:
        asset_no = asset_no_for_leg(leg)
        order_id = self._next_order_id()
        tif = hbt_time_in_force(self.hbtpkg, time_in_force)
        if side == "buy":
            rc = hbt.submit_buy_order(asset_no, order_id, price, qty, tif, self.hbtpkg.LIMIT, False)
        elif side == "sell":
            rc = hbt.submit_sell_order(asset_no, order_id, price, qty, tif, self.hbtpkg.LIMIT, False)
        else:
            raise RuntimeError(f"unsupported side: {side}")
        response = hbt.wait_order_response(asset_no, order_id, self.config.response_timeout_ns)
        req_ts, order_exch_ts, resp_ts = hbt_order_latency(hbt, asset_no)
        order = get_order(hbt, asset_no, order_id)
        filled = order is not None and float(order.exec_qty) > 0
        status = None if order is None else int(order.status)
        fill = HbtLegFill(
            leg=leg,
            asset_no=asset_no,
            side=side,
            order_id=order_id,
            requested_price=price,
            requested_qty=qty,
            response=response if rc == 0 else rc,
            status=status,
            filled=filled,
            exec_price=None if order is None or float(order.exec_price) <= 0 else float(order.exec_price),
            exec_qty=0.0 if order is None else float(order.exec_qty),
            local_timestamp=None if order is None else int(order.local_timestamp),
            exch_timestamp=None if order is None else int(order.exch_timestamp),
            order_req_local_ts=req_ts,
            order_exch_ts=order_exch_ts,
            order_resp_local_ts=resp_ts,
        )
        if not filled and order_is_active(order, self.hbtpkg):
            hbt.cancel(asset_no, order_id, False)
            hbt.wait_order_response(asset_no, order_id, self.config.response_timeout_ns)
        hbt.clear_inactive_orders(asset_no)
        return fill

    def _second_leg_limit_price(self, market: PairMarket, leg: str, side: str, base_price: float) -> float:
        pair = self.config.pair
        if pair.second_leg_tick_offset <= 0:
            return base_price
        quote = market.spot if leg == "stock" else market.future
        tick_size = pair_leg_tick_size(pair, leg, base_price, quote.raw)
        offset = tick_size * pair.second_leg_tick_offset
        if side == "buy":
            return round_price_to_tick(base_price + offset, tick_size)
        if side == "sell":
            return round_price_to_tick(max(base_price - offset, tick_size), tick_size)
        raise RuntimeError(f"unsupported side: {side}")

    def _flatten_first_leg(
        self,
        hbt,
        original_signal: Signal,
        first_leg: str,
        market: PairMarket,
    ) -> HbtLegFill | None:
        if not self.config.flatten_on_second_leg_failure:
            return None
        side = opposite_side(leg_side(original_signal, self.position, first_leg))
        if side is None:
            return None
        quote = market.spot if first_leg == "stock" else market.future
        price = quote.ask if side == "buy" else quote.bid
        tick_size = pair_leg_tick_size(self.config.pair, first_leg, price, quote.raw)
        offset = tick_size * self.config.pair.flatten_first_leg_tick_offset
        if side == "buy":
            price = round_price_to_tick(price + offset, tick_size)
        else:
            price = round_price_to_tick(max(price - offset, tick_size), tick_size)
        return self._submit_raw_leg(
            hbt,
            leg=first_leg,
            side=side,
            price=price,
            qty=hbt_order_qty(self.config.pair, first_leg),
            time_in_force=self.config.pair.flatten_first_leg_time_in_force,
        )

    def _second_leg_profit_ok(self, signal: Signal, market: PairMarket) -> bool:
        pair = self.config.pair
        pricing = self.pricer.price(market)
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
            quantity_multiplier = exit_quantity_multiplier(pair, market, self.position)
            return quantity_multiplier > 0
        return True

    def _update_position(self, signal: Signal, first: HbtLegFill, second: HbtLegFill, pricing: Any) -> None:
        pair = self.config.pair
        fills = {first.leg: first, second.leg: second}
        spot_price = fills["stock"].exec_price
        future_price = fills["future"].exec_price
        if spot_price is None or future_price is None:
            return

        if signal == Signal.EXIT:
            self._reduce_position_after_exit()
            return

        old_quantity = abs(self.position.quantity)
        new_quantity = old_quantity + 1
        self.position.direction = signal
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            self.position.entry_basis_pct = weighted_average(
                self.position.entry_basis_pct,
                old_quantity,
                pricing.long_spot_short_future_pct,
                1,
            )
            self.position.stock_units += 1
            self.position.future_units += 1
        elif signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            self.position.entry_basis_pct = weighted_average(
                self.position.entry_basis_pct,
                old_quantity,
                -pricing.short_spot_long_future_pct,
                1,
            )
            self.position.stock_units -= 1
            self.position.future_units -= 1
        self.position.entry_spot_price = weighted_average(self.position.entry_spot_price, old_quantity, spot_price, 1)
        self.position.entry_future_price = weighted_average(self.position.entry_future_price, old_quantity, future_price, 1)
        self.position.last_entry_time = first.local_timestamp
        self.position.quantity = new_quantity

    def _reduce_position_after_exit(self) -> None:
        if self.position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            self.position.stock_units = max(self.position.stock_units - 1, 0)
            self.position.future_units = max(self.position.future_units - 1, 0)
        elif self.position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            self.position.stock_units = min(self.position.stock_units + 1, 0)
            self.position.future_units = min(self.position.future_units + 1, 0)
        self.position.quantity = max(abs(self.position.quantity) - 1, 0)
        if self.position.quantity <= 0:
            self.position.quantity = 0
            self.position.direction = Signal.HOLD
            self.position.entry_basis_pct = None
            self.position.entry_spot_price = None
            self.position.entry_future_price = None
            self.position.stock_units = 0
            self.position.future_units = 0

    def _realized_pnl(self, signal: Signal, first: HbtLegFill, second: HbtLegFill) -> float | None:
        if signal != Signal.EXIT:
            return None
        fills = {first.leg: first, second.leg: second}
        spot_price = fills["stock"].exec_price
        future_price = fills["future"].exec_price
        return realized_pair_pnl(self.config.pair, self.position, spot_price, future_price)

    def _first_leg_flatten_pnl(self, signal: Signal, first: HbtLegFill, flatten: HbtLegFill | None) -> float | None:
        if flatten is None or first.exec_price is None or flatten.exec_price is None:
            return None
        if first.leg == "stock":
            qty = self.config.pair.spot_order_qty
            if first.side == "buy":
                return (flatten.exec_price - first.exec_price) * qty
            return (first.exec_price - flatten.exec_price) * qty
        multiplier = self.config.pair.future_pnl_multiplier * self.config.pair.future_order_qty
        if first.side == "buy":
            return (flatten.exec_price - first.exec_price) * multiplier
        return (first.exec_price - flatten.exec_price) * multiplier

    def _append_skip_row(self, hbt, step: int, signal: Signal, market: PairMarket, pricing: Any, reason: str) -> None:
        self.rows.append(
            base_row(
                hbt=hbt,
                step=step,
                pair=self.config.pair,
                signal=signal,
                status="RISK_SKIP",
                market=market,
                pricing=pricing,
                position=self.position,
                resolved_tick_sizes=self.resolved_tick_sizes,
                failure_reason=reason,
            )
        )

    def _record_market_row(self, hbt, step: int, market: PairMarket, pricing: Any) -> None:
        interval = self.config.record_market_every_steps
        if interval is None or interval <= 0 or step % interval != 0:
            return
        self.market_rows.append(
            base_row(
                hbt=hbt,
                step=step,
                pair=self.config.pair,
                signal=Signal.HOLD,
                status="MARKET",
                market=market,
                pricing=pricing,
                position=self.position,
                resolved_tick_sizes=self.resolved_tick_sizes,
                failure_reason=None,
            )
        )

    def market_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.market_rows)

    def latency_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.latency_rows)

    def _record_latency_row(
        self,
        hbt,
        step: int,
        signal: Signal,
        status: str,
        event_type: str,
        market: PairMarket,
        pricing: Any,
        *,
        fill: HbtLegFill | None = None,
        local_ts: int | None = None,
    ) -> None:
        spot_feed = hbt_feed_latency(hbt, STOCK_ASSET_NO)
        future_feed = hbt_feed_latency(hbt, FUTURE_ASSET_NO)
        order_entry_latency_ns = None
        order_response_latency_ns = None
        if fill is not None and fill.order_req_local_ts is not None and fill.order_exch_ts is not None:
            order_entry_latency_ns = fill.order_exch_ts - fill.order_req_local_ts
        if fill is not None and fill.order_resp_local_ts is not None and fill.order_exch_ts is not None:
            order_response_latency_ns = fill.order_resp_local_ts - fill.order_exch_ts

        row = base_row(
            hbt=hbt,
            step=step,
            pair=self.config.pair,
            signal=signal,
            status=status,
            market=market,
            pricing=pricing,
            position=self.position,
            resolved_tick_sizes=self.resolved_tick_sizes,
            failure_reason=None,
        )
        row.update(
            {
                "event_type": event_type,
                "leg": None if fill is None else fill.leg,
                "side": None if fill is None else fill.side,
                "order_id": None if fill is None else fill.order_id,
                "local_ts": latency_event_local_ts(hbt, fill, local_ts),
                "spot_exch_ts": feed_exch_ts(spot_feed),
                "spot_feed_local_ts": feed_local_ts(spot_feed),
                "spot_feed_latency_ns": feed_latency_ns(spot_feed),
                "future_exch_ts": feed_exch_ts(future_feed),
                "future_feed_local_ts": feed_local_ts(future_feed),
                "future_feed_latency_ns": feed_latency_ns(future_feed),
                "order_req_local_ts": None if fill is None else fill.order_req_local_ts,
                "order_exch_ts": None if fill is None else fill.order_exch_ts,
                "order_resp_local_ts": None if fill is None else fill.order_resp_local_ts,
                "order_entry_latency_ns": order_entry_latency_ns,
                "order_response_latency_ns": order_response_latency_ns,
            }
        )
        self.latency_rows.append(row)

    def _append_execution_row(
        self,
        hbt,
        step: int,
        signal: Signal,
        status: str,
        market: PairMarket,
        pricing: Any,
        first_fill: HbtLegFill,
        second_fill: HbtLegFill | None,
        realized_pnl: float | None,
        failure_reason: str | None,
        flatten_fill: HbtLegFill | None = None,
        signal_timestamp: int | None = None,
    ) -> None:
        row = base_row(
            hbt=hbt,
            step=step,
            pair=self.config.pair,
            signal=signal,
            status=status,
            market=market,
            pricing=pricing,
            position=self.position,
            resolved_tick_sizes=self.resolved_tick_sizes,
            failure_reason=failure_reason,
        )
        row["signal_timestamp"] = signal_timestamp
        row["completion_timestamp"] = int(hbt.current_timestamp)
        row.update(fill_columns("first", first_fill))
        row.update(fill_columns("second", second_fill))
        row.update(fill_columns("flatten", flatten_fill))
        row["realized_pnl"] = realized_pnl
        self.rows.append(row)
        self._record_latency_row(
            hbt,
            step,
            signal,
            status,
            "completion",
            market,
            pricing,
            local_ts=int(hbt.current_timestamp),
        )

    def _summary_row(self, trades: pd.DataFrame) -> dict[str, Any]:
        if trades.empty:
            filled = failed_second = flatten_count = 0
            realized_pnl = 0.0
        else:
            filled = int(trades["status"].eq("FILLED").sum())
            failed_second = int(
                trades["status"]
                .isin(["SECOND_LEG_UNFILLED", "SECOND_LEG_PROFIT_CHECK_FAILED", "POST_FIRST_FEED_TIMEOUT"])
                .sum()
            )
            flatten_count = int(trades["flatten_filled"].fillna(False).sum()) if "flatten_filled" in trades else 0
            realized_pnl = float(trades["realized_pnl"].dropna().sum()) if "realized_pnl" in trades else 0.0
        return {
            "pair_name": self.config.pair.name,
            "spot_symbol": self.config.pair.spot_symbol,
            "future_symbol": self.config.pair.future_symbol,
            "rows": len(trades),
            "filled_pairs": filled,
            "second_leg_failures": failed_second,
            "flatten_count": flatten_count,
            "realized_pnl": realized_pnl,
            "final_quantity": self.position.quantity,
            "final_direction": self.position.direction.value,
            **self.resolved_tick_sizes,
            "first_leg": self.config.first_leg,
            "step_ns": self.config.step_ns,
            "second_leg_delay_ns": self.config.second_leg_delay_ns,
            "post_first_feed_wait": self.config.post_first_feed_wait,
            "post_first_feed_timeout_ns": self.config.post_first_feed_timeout_ns,
            "post_first_feed_poll_ns": self.config.post_first_feed_poll_ns,
            "spot_order_latency_ns": self.config.spot.order_entry_latency_ns,
            "future_order_latency_ns": self.config.future.order_entry_latency_ns,
        }

    def _next_order_id(self) -> int:
        self.order_id += 1
        return self.order_id


def infer_hbt_asset_tick_size(data: Path, instrument: str, fallback: float = 1.0) -> float:
    event_data = np.load(data)["data"]
    prices = event_data["px"][np.isfinite(event_data["px"]) & (event_data["px"] > 0)]
    if instrument not in {"future", "stock_future"}:
        return tick_size_for_prices(prices, instrument, fallback=fallback)

    ticks: list[float] = []
    for row in event_data:
        price = float(row["px"])
        if not math.isfinite(price) or price <= 0:
            continue
        trade_date = trade_date_from_raw({"exchtime": int(row["exch_ts"])})
        ticks.append(tw_stock_future_tick_size(price, trade_date))
    return min(ticks) if ticks else fallback


def apply_queue_model(asset: Any, config: HbtAssetConfig):
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


def quote_from_depth(depth: Any, symbol: str, timestamp: int) -> Quote | None:
    best_bid = float(depth.best_bid)
    best_ask = float(depth.best_ask)
    if not math.isfinite(best_bid) or not math.isfinite(best_ask):
        return None
    bid_tick = int(depth.best_bid_tick)
    ask_tick = int(depth.best_ask_tick)
    return Quote(
        symbol=symbol,
        bid=best_bid,
        ask=best_ask,
        bid_size=float(depth.bid_qty_at_tick(bid_tick)),
        ask_size=float(depth.ask_qty_at_tick(ask_tick)),
        raw={"exchtime": int(timestamp), "timestamp": int(timestamp), "source": "hbt"},
    )


def hbt_order_qty(pair: PairConfig, leg: str) -> float:
    if leg == "stock":
        return pair.spot_order_qty / STOCK_BOARD_LOT_SHARES
    if leg == "future":
        return float(pair.future_order_qty)
    raise ValueError(f"unknown leg: {leg}")


def asset_no_for_leg(leg: str) -> int:
    if leg == "stock":
        return STOCK_ASSET_NO
    if leg == "future":
        return FUTURE_ASSET_NO
    raise ValueError(f"unknown leg: {leg}")


def leg_side(signal: Signal, position: PairPosition, leg: str) -> str | None:
    if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        return "buy" if leg == "stock" else "sell"
    if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        return "sell" if leg == "stock" else "buy"
    if signal == Signal.EXIT and position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        return "sell" if leg == "stock" else "buy"
    if signal == Signal.EXIT and position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        return "buy" if leg == "stock" else "sell"
    return None


def opposite_side(side: str | None) -> str | None:
    if side == "buy":
        return "sell"
    if side == "sell":
        return "buy"
    return None


def time_in_force_for_leg(pair: PairConfig, leg: str, first_leg: str) -> str:
    if leg == first_leg:
        return pair.first_leg_time_in_force
    return pair.second_leg_time_in_force


def hbt_time_in_force(hbtpkg: Any, value: str) -> int:
    attr = getattr(hbtpkg, value.upper(), None)
    if attr is not None:
        return attr
    return hbtpkg.GTC


def get_order(hbt: Any, asset_no: int, order_id: int):
    try:
        return hbt.orders(asset_no).get(order_id)
    except Exception:
        return None


def order_is_active(order: Any, hbtpkg: Any) -> bool:
    return order is not None and int(order.status) == hbtpkg.NEW and float(order.leaves_qty) > 0


def round_price_to_tick(price: float, tick_size: float) -> float:
    return round(price / tick_size) * tick_size


def realized_pair_pnl(
    pair: PairConfig,
    position: PairPosition,
    exit_spot_price: float | None,
    exit_future_price: float | None,
) -> float | None:
    if exit_spot_price is None or exit_future_price is None:
        return None
    if position.entry_spot_price is None or position.entry_future_price is None:
        return None
    spot_qty = pair.spot_order_qty
    future_multiplier = pair.future_pnl_multiplier * pair.future_order_qty
    commission_rate = pair.stock_commission_rate * pair.stock_commission_discount
    entry_stock_fee = position.entry_spot_price * spot_qty * commission_rate
    exit_stock_fee = exit_spot_price * spot_qty * commission_rate

    if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        spot_pnl = (exit_spot_price - position.entry_spot_price) * spot_qty
        future_pnl = (position.entry_future_price - exit_future_price) * future_multiplier
        stock_tax = exit_spot_price * spot_qty * pair.stock_transaction_tax_rate
    elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        spot_pnl = (position.entry_spot_price - exit_spot_price) * spot_qty
        future_pnl = (exit_future_price - position.entry_future_price) * future_multiplier
        stock_tax = position.entry_spot_price * spot_qty * pair.stock_transaction_tax_rate
    else:
        return None
    return spot_pnl + future_pnl - entry_stock_fee - exit_stock_fee - stock_tax


def fill_columns(prefix: str, fill: HbtLegFill | None) -> dict[str, Any]:
    if fill is None:
        return {
            f"{prefix}_leg": None,
            f"{prefix}_side": None,
            f"{prefix}_order_id": None,
            f"{prefix}_requested_price": None,
            f"{prefix}_requested_qty": None,
            f"{prefix}_response": None,
            f"{prefix}_status": None,
            f"{prefix}_filled": False,
            f"{prefix}_exec_price": None,
            f"{prefix}_exec_qty": None,
            f"{prefix}_local_timestamp": None,
            f"{prefix}_exch_timestamp": None,
            f"{prefix}_order_req_local_ts": None,
            f"{prefix}_order_exch_ts": None,
            f"{prefix}_order_resp_local_ts": None,
            f"{prefix}_order_entry_latency_ns": None,
            f"{prefix}_order_response_latency_ns": None,
        }
    order_entry_latency_ns = None
    order_response_latency_ns = None
    if fill.order_req_local_ts is not None and fill.order_exch_ts is not None:
        order_entry_latency_ns = fill.order_exch_ts - fill.order_req_local_ts
    if fill.order_resp_local_ts is not None and fill.order_exch_ts is not None:
        order_response_latency_ns = fill.order_resp_local_ts - fill.order_exch_ts
    return {
        f"{prefix}_leg": fill.leg,
        f"{prefix}_side": fill.side,
        f"{prefix}_order_id": fill.order_id,
        f"{prefix}_requested_price": fill.requested_price,
        f"{prefix}_requested_qty": fill.requested_qty,
        f"{prefix}_response": fill.response,
        f"{prefix}_status": fill.status,
        f"{prefix}_filled": fill.filled,
        f"{prefix}_exec_price": fill.exec_price,
        f"{prefix}_exec_qty": fill.exec_qty,
        f"{prefix}_local_timestamp": fill.local_timestamp,
        f"{prefix}_exch_timestamp": fill.exch_timestamp,
        f"{prefix}_order_req_local_ts": fill.order_req_local_ts,
        f"{prefix}_order_exch_ts": fill.order_exch_ts,
        f"{prefix}_order_resp_local_ts": fill.order_resp_local_ts,
        f"{prefix}_order_entry_latency_ns": order_entry_latency_ns,
        f"{prefix}_order_response_latency_ns": order_response_latency_ns,
    }


def base_row(
    *,
    hbt: Any,
    step: int,
    pair: PairConfig,
    signal: Signal,
    status: str,
    market: PairMarket,
    pricing: Any,
    position: PairPosition,
    resolved_tick_sizes: dict[str, float],
    failure_reason: str | None,
) -> dict[str, Any]:
    return {
        "timestamp": int(hbt.current_timestamp),
        "step": step,
        "pair_name": pair.name,
        "spot_symbol": pair.spot_symbol,
        "future_symbol": pair.future_symbol,
        "signal": signal.value,
        "status": status,
        "failure_reason": failure_reason,
        "spot_bid": market.spot.bid,
        "spot_ask": market.spot.ask,
        "spot_bid_size": market.spot.bid_size,
        "spot_ask_size": market.spot.ask_size,
        "future_bid": market.future.bid,
        "future_ask": market.future.ask,
        "future_bid_size": market.future.bid_size,
        "future_ask_size": market.future.ask_size,
        "long_spot_short_future_pct": pricing.long_spot_short_future_pct,
        "short_spot_long_future_pct": pricing.short_spot_long_future_pct,
        "mid_basis_pct": pricing.mid_basis_pct,
        "long_spot_short_future_ticks": pricing.long_spot_short_future_ticks,
        "short_spot_long_future_ticks": pricing.short_spot_long_future_ticks,
        "pricing_spot_tick_size": pricing.spot_tick_size,
        "pricing_future_tick_size": pricing.future_tick_size,
        "position_quantity": position.quantity,
        "position_direction": position.direction.value,
        **resolved_tick_sizes,
    }


def hbt_feed_latency(hbt: Any, asset_no: int) -> tuple[int, int] | None:
    latency = hbt.feed_latency(asset_no)
    if latency is None:
        return None
    return int(latency[0]), int(latency[1])


def hbt_order_latency(hbt: Any, asset_no: int) -> tuple[int | None, int | None, int | None]:
    latency = hbt.order_latency(asset_no)
    if latency is None:
        return None, None, None
    return int(latency[0]), int(latency[1]), int(latency[2])


def feed_exch_ts(feed_latency: tuple[int, int] | None) -> int | None:
    return None if feed_latency is None else feed_latency[0]


def feed_local_ts(feed_latency: tuple[int, int] | None) -> int | None:
    return None if feed_latency is None else feed_latency[1]


def feed_latency_ns(feed_latency: tuple[int, int] | None) -> int | None:
    if feed_latency is None:
        return None
    return feed_latency[1] - feed_latency[0]


def feed_refreshed(before: tuple[int, int] | None, after: tuple[int, int] | None) -> bool:
    if after is None:
        return False
    if before is None:
        return True
    return after[1] > before[1]


def latency_event_local_ts(hbt: Any, fill: HbtLegFill | None, local_ts: int | None) -> int:
    if local_ts is not None:
        return int(local_ts)
    if fill is not None and fill.order_req_local_ts is not None:
        return int(fill.order_req_local_ts)
    return int(hbt.current_timestamp)
