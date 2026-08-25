from __future__ import annotations

from typing import Any

import pandas as pd

from scripts.strategy_api import Strategy, StrategyContext
from scripts.hbt_common import (
    apply_queue_model,
    feed_exch_ts,
    feed_latency_ns,
    feed_local_ts,
    feed_refreshed,
    fill_columns,
    get_order,
    hbt_feed_latency,
    hbt_order_latency,
    hbt_time_in_force,
    latency_event_local_ts,
    order_is_active,
    round_price_to_tick,
)
from scripts.tw_stock_hftbacktest import import_hftbacktest, workspace_root

from .config import build_initial_position
from .hbt_helpers import (
    FUTURE_ASSET_NO,
    STOCK_ASSET_NO,
    asset_no_for_leg,
    hbt_order_qty,
    infer_hbt_asset_tick_size,
    leg_side,
    opposite_side,
    quote_from_depth,
    realized_pair_pnl,
    time_in_force_for_leg,
)
from .hbt_rows import (
    base_row,
)
from .hbt_numba import (
    FUTURE_TICK_SCHEDULE_FROM_TIMESTAMP,
    SCAN_END_OF_DATA,
    SCAN_MAX_STEPS,
    SCAN_PERIODIC_RECORD,
    SCAN_SIGNAL,
    SIGNAL_EXIT,
    SIGNAL_HOLD,
    SIGNAL_LONG,
    SIGNAL_SHORT,
    scan_until_wakeup,
)
from .hbt_types import HbtAssetConfig, HbtLegFill, HbtPairBacktestConfig
from .models import PairMarket, Quote, Signal
from .strategy import PairPricer, weighted_average
from .strategy_adapter import FutureSpotPairStrategy, FutureSpotStrategyPayload, default_strategy
from .ticks import pair_leg_tick_size
from .utils import exit_quantity_multiplier


class HbtPairBacktester:
    def __init__(
        self,
        config: HbtPairBacktestConfig,
        hbtpkg: Any | None = None,
        strategy: Strategy | None = None,
    ) -> None:
        self.config = config
        self.hbtpkg = hbtpkg or import_hftbacktest(workspace_root(config.spot.data))
        self.pricer = PairPricer()
        self._custom_strategy = strategy is not None
        self.strategy = strategy or default_strategy()
        self.position = build_initial_position(config.pair)
        self.order_id = 1_000_000
        self.rows: list[dict[str, Any]] = []
        self.market_rows: list[dict[str, Any]] = []
        self.latency_rows: list[dict[str, Any]] = []
        self.resolved_tick_sizes: dict[str, float] = {}
        self.scan_calls = 0
        self.python_decisions = 0

    def run(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        hbt = self._build_backtest()
        try:
            engine = self.config.strategy_engine.strip().lower()
            if engine == "python":
                self._run_python(hbt)
            elif engine == "numba":
                self._run_numba(hbt)
            else:
                raise ValueError(f"strategy_engine must be 'python' or 'numba': {self.config.strategy_engine}")
            trades = pd.DataFrame(self.rows)
            summary = pd.DataFrame([self._summary_row(trades)])
            return trades, summary
        finally:
            close = getattr(hbt, "close", None)
            if close is not None:
                close()

    def _run_python(self, hbt) -> None:
        step = 0
        last_market: PairMarket | None = None
        last_pricing: Any | None = None
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
            last_market = market
            last_pricing = pricing
            self.python_decisions += 1
            decision = self._strategy_decision(hbt, market, pricing)
            signal = Signal(decision.action)
            self._record_market_row(hbt, step, market, pricing, signal)
            if signal == Signal.HOLD:
                continue
            if not decision.should_execute:
                self._append_skip_row(hbt, step, signal, market, pricing, decision.reason)
                continue
            self._execute_signal(hbt, step, signal, market, pricing)

        self._record_final_market(hbt, step, last_market, last_pricing)

    def _run_numba(self, hbt) -> None:
        self._validate_numba_strategy()
        step = 0
        last_market: PairMarket | None = None
        last_pricing: Any | None = None
        pair = self.config.pair
        interval = self.config.record_market_every_steps or 0
        position_codes = {
            Signal.HOLD: SIGNAL_HOLD,
            Signal.ENTER_LONG_SPOT_SHORT_FUTURE: SIGNAL_LONG,
            Signal.ENTER_SHORT_SPOT_LONG_FUTURE: SIGNAL_SHORT,
            Signal.EXIT: SIGNAL_EXIT,
        }

        while True:
            if self.config.max_steps is not None and step >= self.config.max_steps:
                break
            if self.config.max_trades is not None and len(self.rows) >= self.config.max_trades:
                break
            remaining_steps = -1
            if self.config.max_steps is not None:
                remaining_steps = self.config.max_steps - step

            self.scan_calls += 1
            scan_result = scan_until_wakeup(
                hbt,
                self.config.step_ns,
                step,
                remaining_steps,
                interval,
                float(pair.spot_shares_per_pair),
                float(pair.future_shares_per_pair),
                float(pair.spot_tick_size or 0.0),
                float(pair.future_tick_size or 0.0),
                self._future_tick_schedule_mode(),
                pair.entry_threshold_pct,
                pair.exit_threshold_pct,
                pair.stop_loss_pct,
                pair.exit_tick_multiple,
                pair.exit_tick_rule == "gte",
                pair.min_effective_tick_multiple,
                pair.allow_short_spot,
                float(self.position.quantity),
                position_codes.get(self.position.direction, SIGNAL_HOLD),
                float("nan") if self.position.entry_basis_pct is None else self.position.entry_basis_pct,
            )
            reason = int(scan_result[0])
            step = int(scan_result[1])
            compiled_signal = int(scan_result[2])

            if reason in (SCAN_END_OF_DATA, SCAN_MAX_STEPS):
                market = self._market_from_numba_snapshot(scan_result)
                if market is not None:
                    last_market = market
                    last_pricing = self.pricer.price(market)
                break
            if reason not in (SCAN_SIGNAL, SCAN_PERIODIC_RECORD):
                raise RuntimeError(f"unexpected Numba scanner reason: {reason}")

            market = self._current_market(hbt)
            if market is None:
                continue
            pricing = self.pricer.price(market)
            last_market = market
            last_pricing = pricing
            self.python_decisions += 1
            decision = self._strategy_decision(hbt, market, pricing)
            signal = Signal(decision.action)
            expected_signal = self._signal_from_numba_code(compiled_signal)
            if signal != expected_signal:
                raise RuntimeError(
                    "Numba/Python signal mismatch at "
                    f"step={step}: numba={expected_signal.value}, python={signal.value}"
                )
            self._record_market_row(hbt, step, market, pricing, signal)
            if signal == Signal.HOLD:
                continue
            if not decision.should_execute:
                self._append_skip_row(hbt, step, signal, market, pricing, decision.reason)
                continue
            self._execute_signal(hbt, step, signal, market, pricing)

        self._record_final_market(hbt, step, last_market, last_pricing)

    def _strategy_decision(self, hbt, market: PairMarket, pricing: Any):
        return self.strategy.decide(
            StrategyContext(
                strategy_name=getattr(self.strategy, "name", self.strategy.__class__.__name__),
                timestamp_ns=int(hbt.current_timestamp),
                payload=FutureSpotStrategyPayload(
                    pair=self.config.pair,
                    market=market,
                    pricing=pricing,
                    position=self.position,
                    enforce_risk_limits=self.config.enforce_risk_limits,
                ),
            )
        )

    def _record_final_market(
        self,
        hbt,
        step: int,
        last_market: PairMarket | None,
        last_pricing: Any | None,
    ) -> None:
        if last_market is None or last_pricing is None:
            return
        last_recorded_step = self.market_rows[-1].get("step") if self.market_rows else None
        if last_recorded_step != step:
            self._record_market_row(
                hbt,
                step,
                last_market,
                last_pricing,
                Signal.HOLD,
                force=True,
            )

    def _validate_numba_strategy(self) -> None:
        if self._custom_strategy or not isinstance(self.strategy, FutureSpotPairStrategy):
            raise ValueError("strategy_engine='numba' currently supports only the default future/spot strategy")

    def _future_tick_schedule_mode(self) -> int:
        # PairPricer derives the schedule from Quote.raw, whose HBT timestamp is
        # the current event time. Mirroring that source keeps both engines exact.
        return FUTURE_TICK_SCHEDULE_FROM_TIMESTAMP

    def _market_from_numba_snapshot(self, scan_result) -> PairMarket | None:
        timestamp = int(scan_result[3])
        if timestamp < 0:
            return None
        values = [float(value) for value in scan_result[4:12]]
        if not all(value == value for value in values):
            return None
        pair = self.config.pair
        raw = {"exchtime": timestamp, "timestamp": timestamp, "source": "hbt"}
        return PairMarket(
            pair=pair,
            spot=Quote(
                symbol=pair.spot_symbol,
                bid=values[0],
                ask=values[1],
                bid_size=values[2],
                ask_size=values[3],
                raw=raw,
            ),
            future=Quote(
                symbol=pair.future_symbol,
                bid=values[4],
                ask=values[5],
                bid_size=values[6],
                ask_size=values[7],
                raw=raw,
            ),
            trigger_source="hbt",
            trigger_symbol=f"{pair.spot_symbol}/{pair.future_symbol}",
        )

    @staticmethod
    def _signal_from_numba_code(code: int) -> Signal:
        mapping = {
            SIGNAL_HOLD: Signal.HOLD,
            SIGNAL_LONG: Signal.ENTER_LONG_SPOT_SHORT_FUTURE,
            SIGNAL_SHORT: Signal.ENTER_SHORT_SPOT_LONG_FUTURE,
            SIGNAL_EXIT: Signal.EXIT,
        }
        try:
            return mapping[int(code)]
        except KeyError as exc:
            raise RuntimeError(f"unknown Numba signal code: {code}") from exc

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
            trade_date=asset_config.trade_date,
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

    def _record_market_row(
        self,
        hbt,
        step: int,
        market: PairMarket,
        pricing: Any,
        signal: Signal,
        force: bool = False,
    ) -> None:
        interval = self.config.record_market_every_steps
        periodic = interval is not None and interval > 0 and step % interval == 0
        if not force and not periodic and signal == Signal.HOLD:
            return
        row = base_row(
            hbt=hbt,
            step=step,
            pair=self.config.pair,
            signal=signal,
            status="MARKET",
            market=market,
            pricing=pricing,
            position=self.position,
            resolved_tick_sizes=self.resolved_tick_sizes,
            failure_reason=None,
        )
        row["entry_signal"] = signal.value
        row["entry_signal_hit"] = signal != Signal.HOLD
        self.market_rows.append(row)

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
            "final_entry_basis_pct": self.position.entry_basis_pct,
            "final_entry_spot_price": self.position.entry_spot_price,
            "final_entry_future_price": self.position.entry_future_price,
            "final_stock_units": self.position.stock_units,
            "final_future_units": self.position.future_units,
            **self.resolved_tick_sizes,
            "first_leg": self.config.first_leg,
            "step_ns": self.config.step_ns,
            "second_leg_delay_ns": self.config.second_leg_delay_ns,
            "post_first_feed_wait": self.config.post_first_feed_wait,
            "post_first_feed_timeout_ns": self.config.post_first_feed_timeout_ns,
            "post_first_feed_poll_ns": self.config.post_first_feed_poll_ns,
            "spot_order_latency_ns": self.config.spot.order_entry_latency_ns,
            "future_order_latency_ns": self.config.future.order_entry_latency_ns,
            "strategy_engine": self.config.strategy_engine,
            "scan_calls": self.scan_calls,
            "python_decisions": self.python_decisions,
        }

    def _next_order_id(self) -> int:
        self.order_id += 1
        return self.order_id
