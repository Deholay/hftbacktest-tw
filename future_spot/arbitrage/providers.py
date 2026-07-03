from __future__ import annotations

import logging
import queue
import threading
import time
from bisect import bisect_right
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .models import (
    AppConfig,
    FubonLoginConfig,
    HistoricalSourceConfig,
    PairConfig,
    PairMarket,
    PairPosition,
    Quote,
    QuoteUpdate,
    Signal,
)
from .ticks import pair_leg_tick_size
from .utils import (
    account_type_aliases,
    coerce_bool,
    ensure_order_success,
    exit_quantity_multiplier,
    format_order_price,
    is_arbitrage_user_def,
    make_arbitrage_user_def_for_index,
    normalize_account_type,
    normalize_buy_sell,
    parse_books_quote,
    parse_ws_event,
    read_attr,
    read_float_attr,
    read_int_attr,
    sanitize_user_def,
)

@dataclass
class PendingPairOrder:
    signal: Signal
    target_stock_units: float
    target_future_units: float
    stock_side: str
    future_side: str
    quantity_multiplier: float
    first_leg: str = ""
    stock_order_sent: bool = False
    future_order_sent: bool = False
    stock_order_keys: set[str] = None
    second_leg_order_keys: set[str] = None
    second_leg_failure_handled: bool = False
    flatten_order_sent: bool = False
    created_at: float = 0.0
    last_order_sent_at: float = 0.0

    def __post_init__(self) -> None:
        if self.stock_order_keys is None:
            self.stock_order_keys = set()
        if self.second_leg_order_keys is None:
            self.second_leg_order_keys = set()
        if self.created_at <= 0:
            self.created_at = time.monotonic()


@dataclass
class LiveStockPosition:
    tradable_qty: int = 0
    cost_price: float | None = None


@dataclass
class LiveFuturePosition:
    tradable_lot: int = 0
    cost_price: float | None = None


class FubonMarketDataProvider:
    """Fubon Neo REST market data adapter.
    """

    def __init__(self, config: AppConfig) -> None:
        try:
            from fubon_neo.sdk import FubonSDK, FutOptOrder, Order
            from fubon_neo.constant import (
                BSAction,
                FutOptMarketType,
                FutOptOrderType,
                FutOptPriceType,
                MarketType,
                OrderType,
                PriceType,
                TimeInForce,
            )
        except ImportError as exc:
            raise RuntimeError(
                "fubon_neo SDK is not installed. Install Fubon Neo SDK first."
            ) from exc

        self.config = config
        self.Order = Order
        self.FutOptOrder = FutOptOrder
        self.BSAction = BSAction
        self.MarketType = MarketType
        self.PriceType = PriceType
        self.TimeInForce = TimeInForce
        self.OrderType = OrderType
        self.FutOptMarketType = FutOptMarketType
        self.FutOptPriceType = FutOptPriceType
        self.FutOptOrderType = FutOptOrderType
        self.stock_sdk = FubonSDK()
        self.futures_sdk = FubonSDK()
        self.stock_accounts = self._login(self.stock_sdk, "stock", config.fubon.stock)
        self.futures_accounts = self._login(self.futures_sdk, "futopt", config.fubon.futures)
        self.stock_account = self._first_account(self.stock_accounts, "stock")
        self.futures_account = self._first_account(self.futures_accounts, "futopt")
        self.stock_sdk.init_realtime()
        self.futures_sdk.init_realtime()
        self._lock = threading.RLock()
        self._stock_quotes: dict[str, Quote] = {}
        self._future_quotes: dict[str, Quote] = {}
        self._stock_trial_flags: dict[str, bool] = {}
        self._quote_updates: queue.Queue[QuoteUpdate] = queue.Queue()
        self._pending_quote_updates: set[tuple[str, str]] = set()
        self._active_quote_update: QuoteUpdate | None = None
        self._pending_entry_basis: dict[str, float] = {}
        self._pending_pair_orders: dict[str, PendingPairOrder] = {}
        self._pending_stock_order_keys: dict[str, str] = {}
        self._pending_futopt_order_keys: dict[str, str] = {}
        self._pair_entry_cooldown_until: dict[str, float] = {}
        self._pair_entry_locked_reason: dict[str, str] = {}
        self._new_entries_disabled_reason: str | None = None
        self._positions: dict[str, PairPosition] | None = None
        self._pairs_by_future: dict[str, list[PairConfig]] = {}
        self._pairs_by_stock: dict[str, list[PairConfig]] = {}
        self._user_defs_by_pair = self._build_pair_user_defs(config.pairs)
        self._pairs_by_user_def = {
            user_def: pair
            for pair in config.pairs
            for user_def in (self._user_defs_by_pair[pair.name],)
        }
        self.stock_ws = self.stock_sdk.marketdata.websocket_client.stock
        self.futopt_ws = self.futures_sdk.marketdata.websocket_client.futopt
        self._register_stock_filled_callback()
        self._register_stock_order_callback()
        self._register_futopt_filled_callback()
        self._register_futopt_order_callback()
        self._connect_websockets()
        self._subscribe_books()

    def _login(self, sdk: Any, account_type: str, login_config: FubonLoginConfig) -> Any:
        missing = [
            key
            for key, value in {
                f"fubon.{account_type}.personal_id": login_config.personal_id,
                f"fubon.{account_type}.password": login_config.password,
                f"fubon.{account_type}.cert_path": login_config.cert_path,
                f"fubon.{account_type}.cert_pass": login_config.cert_pass,
            }.items()
            if not value
        ]
        if missing:
            raise RuntimeError(f"Missing config values: {', '.join(missing)}")

        accounts = sdk.login(
            login_config.personal_id,
            login_config.password,
            login_config.cert_path,
            login_config.cert_pass,
        )
        self._assert_account_type(accounts, account_type)
        return accounts

    def _first_account(self, accounts: Any, account_type: str) -> Any:
        data = read_attr(accounts, "data")
        if isinstance(data, list):
            if not data:
                raise RuntimeError(f"Fubon login returned empty account list for {account_type}")
            expected = account_type_aliases(account_type)
            for account in data:
                actual = normalize_account_type(read_attr(account, "account_type", "accountType"))
                if actual in expected:
                    logging.info(
                        "selected %s account branch_no=%s account=%s account_type=%s",
                        account_type,
                        read_attr(account, "branch_no", "branchNo"),
                        read_attr(account, "account"),
                        read_attr(account, "account_type", "accountType"),
                    )
                    return account
            returned_types = [
                normalize_account_type(read_attr(account, "account_type", "accountType"))
                for account in data
            ]
            raise RuntimeError(
                f"Fubon login did not return a {account_type} account; "
                f"returned account types: {returned_types}"
            )
        if data is not None:
            actual = normalize_account_type(read_attr(data, "account_type", "accountType"))
            if actual in account_type_aliases(account_type):
                return data
            raise RuntimeError(
                f"Fubon login returned account_type={actual or '<missing>'}, "
                f"expected {account_type}"
            )
        raise RuntimeError(f"Fubon login returned no account data for {account_type}")

    def set_position_store(self, positions: dict[str, PairPosition]) -> None:
        self._positions = positions
        self._pairs_by_future = {}
        self._pairs_by_stock = {}
        for pair in self.config.pairs:
            self._pairs_by_future.setdefault(pair.future_symbol, []).append(pair)
            self._pairs_by_stock.setdefault(pair.spot_symbol, []).append(pair)

    def verify_startup_positions(self, positions: dict[str, PairPosition]) -> None:
        stock_positions = self._load_stock_inventory_by_symbol()
        future_positions = self._load_future_position_lots_by_symbol_and_side()
        self._apply_startup_position_costs(positions, stock_positions, future_positions)
        errors: list[str] = []
        expected_stock_qty_by_symbol: dict[str, int] = {}
        expected_future_sell_lot_by_symbol: dict[str, int] = {}

        for pair in self.config.pairs:
            position = positions[pair.name]
            if position.quantity > 0 and position.direction != Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
                errors.append(
                    f"{pair.name}: startup position check only supports "
                    f"{Signal.ENTER_LONG_SPOT_SHORT_FUTURE.value}; config direction={position.direction.value}"
                )
                continue

            expected_stock_qty_by_symbol[pair.spot_symbol] = (
                expected_stock_qty_by_symbol.get(pair.spot_symbol, 0)
                + int(round(position.stock_units * pair.spot_order_qty))
            )
            expected_future_sell_lot_by_symbol[pair.future_symbol] = (
                expected_future_sell_lot_by_symbol.get(pair.future_symbol, 0)
                + int(round(position.future_units * pair.future_order_qty))
            )

        for symbol in sorted(expected_stock_qty_by_symbol):
            expected_stock_qty = expected_stock_qty_by_symbol[symbol]
            actual_stock_qty = stock_positions.get(symbol, LiveStockPosition()).tradable_qty
            if actual_stock_qty != expected_stock_qty:
                errors.append(
                    f"spot {symbol} long tradable_qty mismatch "
                    f"expected={expected_stock_qty} actual={actual_stock_qty}"
                )

        for symbol in sorted(expected_future_sell_lot_by_symbol):
            expected_future_sell_lot = expected_future_sell_lot_by_symbol[symbol]
            actual_future_sell_lot = future_positions.get((symbol, "sell"), LiveFuturePosition()).tradable_lot
            if actual_future_sell_lot != expected_future_sell_lot:
                errors.append(
                    f"future {symbol} Sell tradable_lot mismatch "
                    f"expected={expected_future_sell_lot} actual={actual_future_sell_lot}"
                )

        if errors:
            raise RuntimeError("Startup position verification failed:\n" + "\n".join(f"- {error}" for error in errors))
        logging.info("startup position verification passed for %s configured pairs", len(self.config.pairs))

    def _apply_startup_position_costs(
        self,
        positions: dict[str, PairPosition],
        stock_positions: dict[str, LiveStockPosition],
        future_positions: dict[tuple[str, str], LiveFuturePosition],
    ) -> None:
        for pair in self.config.pairs:
            position = positions[pair.name]
            if position.direction != Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
                continue
            stock_position = stock_positions.get(pair.spot_symbol)
            future_position = future_positions.get((pair.future_symbol, "sell"))
            if position.entry_spot_price is None and stock_position is not None:
                position.entry_spot_price = stock_position.cost_price
            if position.entry_future_price is None and future_position is not None:
                position.entry_future_price = future_position.cost_price
            if (
                position.entry_basis_pct is None
                and position.entry_spot_price
                and position.entry_future_price
            ):
                position.entry_basis_pct = (
                    position.entry_future_price - position.entry_spot_price
                ) / position.entry_spot_price
            if position.has_position:
                logging.info(
                    "[%s] startup live costs entry_spot=%s entry_future=%s entry_basis=%s",
                    pair.name,
                    position.entry_spot_price,
                    position.entry_future_price,
                    position.entry_basis_pct,
                )

    def _load_stock_inventory_by_symbol(self) -> dict[str, LiveStockPosition]:
        accounting = getattr(self.stock_sdk, "accounting", None)
        unrealized = (
            getattr(accounting, "unrealized_gains_and_loses", None)
            or getattr(accounting, "unrealized_gains_and_losses", None)
        )
        if unrealized is None:
            raise RuntimeError("Fubon stock SDK does not expose accounting.unrealized_gains_and_loses")

        result = self._call_accounting_api(unrealized, self.stock_account, "stock unrealized gains and loses")
        rows = self._result_rows(result)
        positions: dict[str, LiveStockPosition] = {}
        for row in rows:
            symbol = str(read_attr(row, "stock_no", "stockNo", "symbol") or "")
            if not symbol:
                continue
            buy_sell_raw = read_attr(row, "buy_sell", "buySell", "side")
            buy_sell = normalize_buy_sell(buy_sell_raw)
            if buy_sell_raw not in (None, "") and buy_sell not in {"buy", "long"}:
                logging.debug("skip non-long stock inventory row buy_sell=%s row=%s", buy_sell_raw, row)
                continue
            tradable_qty = read_int_attr(row, "tradable_qty", "tradableQty")
            if tradable_qty <= 0:
                logging.debug("skip non-positive stock long position tradable_qty=%s row=%s", tradable_qty, row)
                continue
            cost_price = self._optional_positive_float(row, "cost_price", "costPrice", "avg_price", "avgPrice")
            self._add_stock_position(positions, symbol, tradable_qty, cost_price)
        logging.info("loaded %s stock unrealized rows for startup position verification", len(rows))
        return positions

    def _load_future_position_lots_by_symbol_and_side(self) -> dict[tuple[str, str], LiveFuturePosition]:
        accounting = getattr(self.futures_sdk, "futopt_accounting", None)
        query_hybrid_position = getattr(accounting, "query_hybrid_position", None)
        if query_hybrid_position is None:
            raise RuntimeError("Fubon futures SDK does not expose futopt_accounting.query_hybrid_position")

        result = self._call_accounting_api(query_hybrid_position, self.futures_account, "futures hybrid positions")
        rows = self._result_rows(result)
        positions: dict[tuple[str, str], LiveFuturePosition] = {}
        for row in rows:
            symbol = str(read_attr(row, "symbol", "stock_no", "stockNo") or "")
            if not symbol:
                continue
            mapped_symbol = self._mapped_futopt_symbol(symbol, row) or symbol
            buy_sell = normalize_buy_sell(read_attr(row, "buy_sell", "buySell"))
            tradable_lot = read_int_attr(row, "tradable_lot", "tradableLot", "lot")
            if buy_sell not in {"buy", "sell"}:
                logging.debug("skip futures position with unsupported buy_sell=%s row=%s", buy_sell, row)
                continue
            key = (mapped_symbol, buy_sell)
            cost_price = self._optional_positive_float(
                row,
                "cost_price",
                "costPrice",
                "avg_price",
                "avgPrice",
                "average_price",
                "averagePrice",
                "price",
            )
            self._add_future_position(positions, key, tradable_lot, cost_price)
        logging.info("loaded %s futures position rows for startup position verification", len(rows))
        return positions

    @staticmethod
    def _optional_positive_float(row: Any, *names: str) -> float | None:
        value = read_float_attr(row, *names)
        return value if value > 0 else None

    @staticmethod
    def _weighted_cost(old_cost: float | None, old_qty: int, new_cost: float | None, new_qty: int) -> float | None:
        if new_cost is None or new_qty <= 0:
            return old_cost
        if old_cost is None or old_qty <= 0:
            return new_cost
        return ((old_cost * old_qty) + (new_cost * new_qty)) / (old_qty + new_qty)

    @classmethod
    def _add_stock_position(
        cls,
        positions: dict[str, LiveStockPosition],
        symbol: str,
        tradable_qty: int,
        cost_price: float | None,
    ) -> None:
        current = positions.get(symbol, LiveStockPosition())
        current.cost_price = cls._weighted_cost(current.cost_price, current.tradable_qty, cost_price, tradable_qty)
        current.tradable_qty += tradable_qty
        positions[symbol] = current

    @classmethod
    def _add_future_position(
        cls,
        positions: dict[tuple[str, str], LiveFuturePosition],
        key: tuple[str, str],
        tradable_lot: int,
        cost_price: float | None,
    ) -> None:
        current = positions.get(key, LiveFuturePosition())
        current.cost_price = cls._weighted_cost(current.cost_price, current.tradable_lot, cost_price, tradable_lot)
        current.tradable_lot += tradable_lot
        positions[key] = current

    @staticmethod
    def _call_accounting_api(method: Any, account: Any, label: str) -> Any:
        try:
            return method(account)
        except TypeError as exc:
            logging.debug("%s query with account argument failed; retrying without account: %s", label, exc)
            return method()

    @staticmethod
    def _result_rows(result: Any) -> list[Any]:
        data = read_attr(result, "data")
        if data is None:
            data = result
        if isinstance(data, list):
            return data
        if isinstance(data, tuple):
            return list(data)
        if isinstance(data, dict):
            for key in ("data", "items", "inventories", "positions"):
                value = data.get(key)
                if isinstance(value, list):
                    return value
                if isinstance(value, tuple):
                    return list(value)
            return [data]
        return [data] if data is not None else []

    def _build_pair_user_defs(self, pairs: tuple[PairConfig, ...]) -> dict[str, str]:
        user_defs: dict[str, str] = {}
        used: set[str] = set()
        for index, pair in enumerate(pairs):
            user_def = make_arbitrage_user_def_for_index(index)
            if user_def in used:
                raise RuntimeError(f"duplicate arbitrage user_def generated: {user_def}")
            used.add(user_def)
            user_defs[pair.name] = user_def
            logging.info("assigned user_def=%s pair=%s", user_def, pair.name)
        return user_defs

    def _user_def_for_pair(self, pair: PairConfig) -> str:
        try:
            return self._user_defs_by_pair[pair.name]
        except KeyError as exc:
            raise RuntimeError(f"missing user_def mapping for pair={pair.name}") from exc

    def _pair_from_user_def(self, user_def: Any) -> PairConfig | None:
        if not is_arbitrage_user_def(user_def):
            return None
        return self._pairs_by_user_def.get(sanitize_user_def(str(user_def)))

    def _pair_from_futopt_content(self, content: Any) -> PairConfig | None:
        user_def = read_attr(content, "user_def", "userDef")
        pair = self._pair_from_user_def(user_def)
        if pair is not None:
            return pair

        symbol = str(read_attr(content, "symbol", "stock_no", "stockNo") or "")
        if not symbol:
            configured_pairs = list(self.config.pairs)
            if len(configured_pairs) == 1:
                return configured_pairs[0]
            logging.warning("futopt report missing symbol and user_def; cannot map to pair: %s", content)
            return None

        mapped_symbol = self._mapped_futopt_symbol(symbol, content)
        pairs = [
            pair
            for pair in self.config.pairs
            if pair.future_symbol in {symbol, mapped_symbol}
        ]
        if len(pairs) == 1:
            return pairs[0]
        if not pairs:
            logging.warning(
                "futopt report symbol is not configured; treating as not ours symbol=%s mapped_symbol=%s content=%s",
                symbol,
                mapped_symbol,
                content,
            )
            return None
        logging.warning("futopt report symbol maps to multiple pairs; cannot choose symbol=%s content=%s", symbol, content)
        return None

    def _pair_from_stock_order_content(self, content: Any) -> PairConfig | None:
        user_def = read_attr(content, "user_def", "userDef")
        pair = self._pair_from_user_def(user_def)
        if pair is not None:
            return pair
        return None

    def _pair_by_name(self, pair_name: str) -> PairConfig | None:
        for pair in self.config.pairs:
            if pair.name == pair_name:
                return pair
        return None

    @staticmethod
    def _text_attr(obj: Any, *names: str) -> str:
        value = read_attr(obj, *names)
        if value in (None, ""):
            return ""
        text = str(value).strip()
        if "." in text:
            text = text.rsplit(".", 1)[-1]
        return text

    def _is_fok_report(self, content: Any) -> bool:
        return self._text_attr(content, "time_in_force", "timeInForce").upper() == "FOK"

    def _is_fok_partial_abnormal(self, content: Any, asset_type: str) -> bool:
        if not self._is_fok_report(content):
            return False
        if asset_type == "stock":
            filled = read_int_attr(content, "filled_qty", "filledQty", "filled_quantity", "filledQuantity")
            remaining = read_int_attr(content, "after_qty", "afterQty", "after_quantity", "afterQuantity")
            total = read_int_attr(content, "qty", "quantity", "lot")
        else:
            filled = read_int_attr(content, "filled_lot", "filledLot")
            remaining = read_int_attr(content, "after_lot", "afterLot")
            total = read_int_attr(content, "lot")
        if filled <= 0:
            return False
        if remaining > 0:
            return True
        return total > 0 and filled < total

    def _stock_report_keys(self, content: Any) -> set[str]:
        keys = set()
        for name in ("order_no", "orderNo", "seq_no", "seqNo"):
            value = read_attr(content, name)
            if value not in (None, ""):
                keys.add(str(value))
        return keys

    def _futopt_report_keys(self, content: Any) -> set[str]:
        keys = set()
        for name in ("order_no", "orderNo", "seq_no", "seqNo"):
            value = read_attr(content, name)
            if value not in (None, ""):
                keys.add(str(value))
        return keys

    def _remember_pending_stock_order_keys(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        content: Any,
    ) -> None:
        for key in self._stock_report_keys(content):
            pending.stock_order_keys.add(key)
            self._pending_stock_order_keys[key] = pair.name

    def _forget_pending_stock_order_keys(self, pending: PendingPairOrder) -> None:
        for key in pending.stock_order_keys:
            self._pending_stock_order_keys.pop(key, None)
        pending.stock_order_keys.clear()

    def _remember_pending_futopt_order_keys(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        content: Any,
    ) -> None:
        for key in self._futopt_report_keys(content):
            pending.second_leg_order_keys.add(key)
            self._pending_futopt_order_keys[key] = pair.name

    def _forget_pending_futopt_order_keys(self, pending: PendingPairOrder) -> None:
        for key in pending.second_leg_order_keys:
            self._pending_futopt_order_keys.pop(key, None)
        pending.second_leg_order_keys.clear()

    @staticmethod
    def _mapped_futopt_symbol(symbol: str, content: Any) -> str | None:
        expiry_date = str(read_attr(content, "expiry_date", "expiryDate") or "")
        if len(symbol) < 3 or len(expiry_date) < 6:
            return None
        month_text = expiry_date[4:6]
        if not month_text.isdigit():
            return None
        month = int(month_text)
        if month < 1 or month > 12:
            return None
        month_code = chr(ord("A") + month - 1)
        year_code = expiry_date[3]
        return f"{symbol[-3:]}{month_code}{year_code}"

    def _register_stock_filled_callback(self) -> None:
        callback = getattr(self.stock_sdk, "set_on_filled", None)
        if callback is None:
            logging.warning("FubonSDK does not expose set_on_filled; stock leg position will not update from fills")
            return
        callback(self._handle_stock_filled)

    def _register_stock_order_callback(self) -> None:
        for callback_name in (
            "set_on_order",
            "set_on_order_changed",
            "set_on_order_report",
            "set_on_stock_order",
            "set_on_stock_order_changed",
            "set_on_stock_order_report",
        ):
            callback = getattr(self.stock_sdk, callback_name, None)
            if callback is None:
                continue
            callback(self._handle_stock_order)
            logging.info("registered stock order report callback: %s", callback_name)
            return
        logging.info("FubonSDK does not expose a known stock order report callback")

    def _register_futopt_filled_callback(self) -> None:
        callback = getattr(self.futures_sdk, "set_on_futopt_filled", None)
        if callback is None:
            logging.warning("FubonSDK does not expose set_on_futopt_filled; position will not update from fills")
            return
        callback(self._handle_futopt_filled)

    def _register_futopt_order_callback(self) -> None:
        for callback_name in (
            "set_on_futopt_order",
            "set_on_futopt_order_changed",
            "set_on_futopt_order_report",
        ):
            callback = getattr(self.futures_sdk, callback_name, None)
            if callback is None:
                continue
            callback(self._handle_futopt_order)
            logging.info("registered futures order report callback: %s", callback_name)
            return
        logging.info("FubonSDK does not expose a known futures order report callback")

    def _handle_stock_filled(self, code: Any, content: Any) -> None:
        user_def = read_attr(content, "user_def", "userDef")
        pair = self._pair_from_user_def(user_def)
        if pair is None:
            logging.debug("ignore stock fill without known arbitrage user_def=%s content=%s", user_def, content)
            return
        self._log_first_leg_fill_callback_if_pending(pair, "stock")

        print("==Stock Filled==")
        print(code)
        print(content)
        print("================")

        try:
            self._update_position_from_stock_fill(content)
        except Exception:
            logging.exception("failed to update position from stock fill: code=%s content=%s", code, content)

    def _handle_stock_order(self, code: Any, content: Any) -> None:
        rows = self._result_rows(content)
        if len(rows) != 1 or rows[0] is not content:
            for row in rows:
                self._handle_stock_order(code, row)
            return
        pair = self._pair_from_pending_stock_report(content) or self._pair_from_stock_order_content(content)
        if pair is None:
            return
        logging.info("[%s] stock order report code=%s content=%s", pair.name, code, content)
        self._handle_stock_order_report_state(pair, content)

    def _pair_from_pending_stock_report(self, content: Any) -> PairConfig | None:
        for key in self._stock_report_keys(content):
            pair_name = self._pending_stock_order_keys.get(key)
            if pair_name:
                return self._pair_by_name(pair_name)
        return None

    def _handle_stock_order_report_state(self, pair: PairConfig, content: Any) -> None:
        with self._lock:
            pending = self._pending_pair_orders.get(pair.name)
            if pending is None:
                return
            self._remember_pending_stock_order_keys(pair, pending, content)
            if not self._is_fok_report(content):
                return
            if self._is_fok_partial_abnormal(content, "stock"):
                leg_role = "first_leg" if pending.first_leg == "stock" else "second_leg"
                self._handle_fok_partial_abnormal(pair, pending, "stock", leg_role, content)
                return
            if pending.first_leg == "stock":
                self._handle_first_leg_stock_report_locked(pair, pending, content)
                return
            if pending.first_leg != "future":
                return
            if not pending.stock_order_sent or pending.second_leg_failure_handled:
                return

        status = read_int_attr(content, "status")
        error_message = read_attr(content, "error_message", "errorMessage")
        after_qty = read_int_attr(content, "after_qty", "afterQty", "after_quantity", "afterQuantity")
        filled_qty = read_int_attr(content, "filled_qty", "filledQty", "filled_quantity", "filledQuantity")
        if status == 30 and not error_message and after_qty == 0 and filled_qty == 0:
            self._handle_second_leg_fok_failure(pair, "stock FOK no-fill status=30")
        elif status == 90 or error_message:
            self._disable_new_entries(f"stock order rejected pair={pair.name} status={status} error={error_message}")
            self._handle_second_leg_fok_failure(pair, f"stock FOK rejected status={status} error={error_message}")

    def _handle_first_leg_stock_report_locked(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        content: Any,
    ) -> None:
        status = read_int_attr(content, "status")
        error_message = read_attr(content, "error_message", "errorMessage")
        after_qty = read_int_attr(content, "after_qty", "afterQty", "after_quantity", "afterQuantity")
        filled_qty = read_int_attr(content, "filled_qty", "filledQty", "filled_quantity", "filledQuantity")
        if status == 30 and not error_message and after_qty == 0 and filled_qty == 0:
            self._pending_pair_orders.pop(pair.name, None)
            self._forget_pending_stock_order_keys(pending)
            self._forget_pending_futopt_order_keys(pending)
            logging.warning("[%s] first stock leg FOK no-fill; pending order released", pair.name)
        elif status == 90 or error_message:
            self._pending_pair_orders.pop(pair.name, None)
            self._forget_pending_stock_order_keys(pending)
            self._forget_pending_futopt_order_keys(pending)
            self._disable_new_entries(f"first stock leg rejected pair={pair.name} status={status} error={error_message}")
            logging.critical("[%s] first stock leg rejected; new entries disabled", pair.name)

    def _handle_futopt_filled(self, code: Any, content: Any) -> None:
        pair = self._pair_from_futopt_content(content)
        if pair is None:
            return
        self._log_first_leg_fill_callback_if_pending(pair, "future")

        print("==FutOpt Filled==")
        print(code)
        print(content)
        print("=================")

        try:
            self._update_position_from_futopt_fill(content)
        except Exception:
            logging.exception("failed to update position from futopt fill: code=%s content=%s", code, content)

    def _handle_futopt_order(self, code: Any, content: Any) -> None:
        rows = self._result_rows(content)
        if len(rows) != 1 or rows[0] is not content:
            for row in rows:
                self._handle_futopt_order(code, row)
            return
        pair = self._pair_from_pending_futopt_report(content) or self._pair_from_futopt_content(content)
        if pair is None:
            return
        logging.info("[%s] futopt order report code=%s content=%s", pair.name, code, content)
        self._handle_futopt_order_report_state(pair, content)

    def _pair_from_pending_futopt_report(self, content: Any) -> PairConfig | None:
        for key in self._futopt_report_keys(content):
            pair_name = self._pending_futopt_order_keys.get(key)
            if pair_name:
                return self._pair_by_name(pair_name)
        return None

    def _handle_futopt_order_report_state(self, pair: PairConfig, content: Any) -> None:
        with self._lock:
            pending = self._pending_pair_orders.get(pair.name)
            if pending is None:
                return
            self._remember_pending_futopt_order_keys(pair, pending, content)
            if self._is_fok_partial_abnormal(content, "future"):
                leg_role = "first_leg" if pending.first_leg == "future" else "second_leg"
                self._handle_fok_partial_abnormal(pair, pending, "future", leg_role, content)
                return
            if pending.first_leg == "future":
                self._handle_first_leg_future_report_locked(pair, pending, content)
                return
            if pending.first_leg != "stock":
                return
            if not pending.future_order_sent or pending.second_leg_failure_handled:
                return
            if not self._is_fok_report(content):
                return

        status = read_int_attr(content, "status")
        error_message = read_attr(content, "error_message", "errorMessage")
        after_lot = read_int_attr(content, "after_lot", "afterLot")
        filled_lot = read_int_attr(content, "filled_lot", "filledLot")
        if status == 30 and not error_message and after_lot == 0 and filled_lot == 0:
            self._handle_second_leg_fok_failure(pair, "FOK no-fill status=30")
        elif status == 90 or error_message:
            self._disable_new_entries(f"futopt order rejected pair={pair.name} status={status} error={error_message}")
            self._handle_second_leg_fok_failure(pair, f"FOK rejected status={status} error={error_message}")

    def _handle_first_leg_future_report_locked(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        content: Any,
    ) -> None:
        if not self._is_fok_report(content):
            return
        status = read_int_attr(content, "status")
        error_message = read_attr(content, "error_message", "errorMessage")
        after_lot = read_int_attr(content, "after_lot", "afterLot")
        filled_lot = read_int_attr(content, "filled_lot", "filledLot")
        if status == 30 and not error_message and after_lot == 0 and filled_lot == 0:
            self._pending_pair_orders.pop(pair.name, None)
            self._forget_pending_stock_order_keys(pending)
            self._forget_pending_futopt_order_keys(pending)
            logging.warning("[%s] first future leg FOK no-fill; pending order released", pair.name)
        elif status == 90 or error_message:
            self._pending_pair_orders.pop(pair.name, None)
            self._forget_pending_stock_order_keys(pending)
            self._forget_pending_futopt_order_keys(pending)
            self._disable_new_entries(f"first future leg rejected pair={pair.name} status={status} error={error_message}")
            logging.critical("[%s] first future leg rejected; new entries disabled", pair.name)

    def _handle_fok_partial_abnormal(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        asset_type: str,
        leg_role: str,
        content: Any,
    ) -> None:
        reason = f"{asset_type} {leg_role} FOK partial abnormal content={content}"
        self._disable_new_entries(reason)
        self._lock_pair_entries(pair, reason)
        logging.critical(
            "[%s] %s %s FOK partial fill detected; pending order kept blocked for manual review content=%s",
            pair.name,
            asset_type,
            leg_role,
            content,
        )

    def _handle_second_leg_fok_failure(self, pair: PairConfig, reason: str) -> None:
        with self._lock:
            pending = self._pending_pair_orders.get(pair.name)
            if pending is None or pending.second_leg_failure_handled:
                return
            pending.second_leg_failure_handled = True
            self._cooldown_pair(pair, pair.cooldown_after_second_leg_failure_sec, reason)
            should_flatten = pair.second_leg_failure_action == "flatten_first_leg"

        logging.warning("[%s] second leg FOK failed: %s", pair.name, reason)
        if should_flatten:
            self._flatten_first_leg_after_second_leg_failure(pair, pending, reason)
        else:
            logging.critical(
                "[%s] second leg FOK failed and second_leg_failure_action=%s; manual review required",
                pair.name,
                pair.second_leg_failure_action,
            )

    def _flatten_first_leg_after_second_leg_failure(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        reason: str,
    ) -> None:
        with self._lock:
            if pending.flatten_order_sent:
                return
            pending.flatten_order_sent = True
        try:
            market = self.get_pair_market(pair)
            logging.critical("[%s] flattening first leg after second-leg failure: %s", pair.name, reason)
            if pending.first_leg == "stock":
                self._place_stock_flatten_order(market, pending)
            elif pending.first_leg == "future":
                self._place_future_flatten_order(market, pending)
            else:
                raise RuntimeError(f"Cannot flatten unknown first leg for {pair.name}: {pending.first_leg}")
        except Exception:
            with self._lock:
                pending.flatten_order_sent = False
            logging.exception("[%s] failed to flatten first leg after second-leg failure", pair.name)

    def _update_position_from_stock_fill(self, content: Any) -> None:
        if self._positions is None:
            logging.debug("received stock fill before position store was ready: %s", content)
            return

        user_def = read_attr(content, "user_def", "userDef")
        pair = self._pair_from_user_def(user_def)
        if pair is None:
            logging.debug("ignore stock fill without known arbitrage user_def=%s content=%s", user_def, content)
            return

        symbol = str(read_attr(content, "stock_no", "stockNo", "symbol") or "")
        if not symbol:
            logging.warning("stock fill missing symbol/stock_no: %s", content)
            return

        if symbol != pair.spot_symbol:
            logging.warning(
                "[%s] stock fill symbol mismatch user_def=%s expected=%s actual=%s content=%s",
                pair.name,
                user_def,
                pair.spot_symbol,
                symbol,
                content,
            )
            return

        buy_sell = normalize_buy_sell(read_attr(content, "buy_sell", "buySell"))
        filled_qty = read_int_attr(content, "filled_qty", "filledQty", "quantity", "qty")
        if filled_qty <= 0:
            logging.debug("stock fill has no positive filled qty: symbol=%s content=%s", symbol, content)
            return

        pair_units = filled_qty / pair.spot_order_qty
        signed_units = pair_units if buy_sell == "buy" else -pair_units if buy_sell == "sell" else 0
        if signed_units == 0:
            logging.warning("[%s] unsupported stock fill buy_sell=%s", pair.name, buy_sell)
            return
        with self._lock:
            position = self._positions[pair.name]
            old = (position.quantity, position.direction, position.stock_units, position.future_units)
            old_stock_units = position.stock_units
            filled_price = read_float_attr(content, "filled_price", "filledPrice", "price")
            if filled_price > 0 and self._is_increasing_leg(old_stock_units, signed_units):
                position.entry_spot_price = self._weighted_average(
                    position.entry_spot_price,
                    abs(old_stock_units),
                    filled_price,
                    abs(signed_units),
                )
            position.stock_units += signed_units
            self._reconcile_position_from_legs(pair, position)
            new = (
                position.quantity,
                position.direction.value,
                position.stock_units,
                position.future_units,
            )
        logging.info(
            "[%s] stock fill updated position %s -> quantity=%s direction=%s stock_units=%s future_units=%s",
            pair.name,
            old,
            new[0],
            new[1],
            new[2],
            new[3],
        )
        self._place_deferred_future_order_if_ready(pair)

    def _update_position_from_futopt_fill(self, content: Any) -> None:
        if self._positions is None:
            logging.debug("received futopt fill before position store was ready: %s", content)
            return

        user_def = read_attr(content, "user_def", "userDef")
        pair = self._pair_from_futopt_content(content)
        if pair is None:
            logging.debug("ignore futopt fill that cannot be mapped to a configured pair user_def=%s content=%s", user_def, content)
            return

        symbol = str(read_attr(content, "symbol", "stock_no", "stockNo") or "")
        if not symbol:
            symbol = pair.future_symbol
        mapped_symbol = self._mapped_futopt_symbol(symbol, content)

        if pair.future_symbol not in {symbol, mapped_symbol}:
            logging.warning(
                "[%s] futopt fill symbol mismatch user_def=%s expected=%s actual=%s mapped=%s content=%s",
                pair.name,
                user_def,
                pair.future_symbol,
                symbol,
                mapped_symbol,
                content,
            )
            return

        buy_sell = normalize_buy_sell(read_attr(content, "buy_sell", "buySell"))
        filled_lot = read_int_attr(content, "filled_lot", "filledLot", "lot", "filled_qty", "filledQty")
        if filled_lot <= 0:
            logging.debug("futopt fill has no positive filled lot: symbol=%s content=%s", symbol, content)
            return

        pair_units = filled_lot / pair.future_order_qty
        if not pair_units.is_integer():
            logging.warning(
                "[%s] filled_lot=%s is not divisible by future_order_qty=%s; using partial pair_units=%s",
                pair.name,
                filled_lot,
                pair.future_order_qty,
                pair_units,
            )
        signed_units = pair_units if buy_sell == "sell" else -pair_units if buy_sell == "buy" else 0
        if signed_units == 0:
            logging.warning("[%s] unsupported futopt fill buy_sell=%s", pair.name, buy_sell)
            return
        with self._lock:
            position = self._positions[pair.name]
            old = (position.quantity, position.direction, position.stock_units, position.future_units)
            old_future_units = position.future_units
            filled_price = read_float_attr(content, "filled_price", "filledPrice", "price")
            if filled_price > 0 and self._is_increasing_leg(old_future_units, signed_units):
                position.entry_future_price = self._weighted_average(
                    position.entry_future_price,
                    abs(old_future_units),
                    filled_price,
                    abs(signed_units),
                )
            position.future_units += signed_units
            self._reconcile_position_from_legs(pair, position)
            new = (
                position.quantity,
                position.direction.value,
                position.stock_units,
                position.future_units,
            )
        logging.info(
            "[%s] futopt fill updated position %s -> quantity=%s direction=%s stock_units=%s future_units=%s",
            pair.name,
            old,
            new[0],
            new[1],
            new[2],
            new[3],
        )
        self._place_deferred_stock_order_if_ready(pair)

    def _reconcile_position_from_legs(self, pair: PairConfig, position: PairPosition) -> None:
        old_had_position = position.has_position
        old_quantity = abs(position.quantity)
        if position.stock_units > 0 and position.future_units > 0:
            position.direction = Signal.ENTER_LONG_SPOT_SHORT_FUTURE
            position.quantity = min(position.stock_units, position.future_units)
        elif position.stock_units < 0 and position.future_units < 0:
            position.direction = Signal.ENTER_SHORT_SPOT_LONG_FUTURE
            position.quantity = min(abs(position.stock_units), abs(position.future_units))
        else:
            position.quantity = 0
            position.direction = Signal.HOLD

        if position.has_position and abs(position.quantity) > old_quantity:
            position.last_entry_time = time.time()

        if position.has_position and not old_had_position:
            pending_entry_basis = self._pending_entry_basis.get(position.pair_name)
            if pending_entry_basis is not None:
                position.entry_basis_pct = pending_entry_basis

        if not position.has_position and position.stock_units == 0 and position.future_units == 0:
            position.entry_basis_pct = None
            position.entry_spot_price = None
            position.entry_future_price = None
            position.last_entry_time = None

        self._release_pending_order_if_target_met(pair, position)

    def _release_pending_order_if_target_met(self, pair: PairConfig, position: PairPosition) -> None:
        with self._lock:
            pending = self._pending_pair_orders.get(pair.name)
            if pending is None:
                return
            if (
                pending.second_leg_failure_handled
                and position.stock_units == 0
                and position.future_units == 0
            ):
                self._pending_pair_orders.pop(pair.name, None)
                self._forget_pending_stock_order_keys(pending)
                self._forget_pending_futopt_order_keys(pending)
                logging.info("[%s] pending order released after first-leg flatten", pair.name)
                return
            if not (
                self._same_units(position.stock_units, pending.target_stock_units)
                and self._same_units(position.future_units, pending.target_future_units)
            ):
                return
            self._pending_pair_orders.pop(pair.name, None)
            self._forget_pending_stock_order_keys(pending)
            self._forget_pending_futopt_order_keys(pending)
        logging.info(
            "[%s] pending order released signal=%s target_stock_units=%s target_future_units=%s",
            pair.name,
            pending.signal.value,
            pending.target_stock_units,
            pending.target_future_units,
        )

    def _log_first_leg_fill_callback_if_pending(self, pair: PairConfig, leg: str) -> None:
        with self._lock:
            pending = self._pending_pair_orders.get(pair.name)
            if pending is None:
                return
            if leg == "stock":
                is_first_leg_fill = not pending.future_order_sent
            elif leg == "future":
                is_first_leg_fill = not pending.stock_order_sent
            else:
                return
        if is_first_leg_fill:
            self._log_order_timing(pair, "first_leg_fill_callback_received", leg, pending)

    @staticmethod
    def _same_units(left: float, right: float) -> bool:
        return abs(left - right) < 1e-9

    @staticmethod
    def _log_order_timing(
        pair: PairConfig,
        event: str,
        leg: str,
        pending: PendingPairOrder,
        elapsed_ms: float | None = None,
    ) -> None:
        logging.info(
            (
                "[%s] order_timing event=%s leg=%s signal=%s "
                "perf_ns=%s wall_ns=%s elapsed_ms=%s "
                "target_stock_units=%s target_future_units=%s stock_sent=%s future_sent=%s"
            ),
            pair.name,
            event,
            leg,
            pending.signal.value,
            time.perf_counter_ns(),
            time.time_ns(),
            "" if elapsed_ms is None else f"{elapsed_ms:.3f}",
            pending.target_stock_units,
            pending.target_future_units,
            pending.stock_order_sent,
            pending.future_order_sent,
        )

    def _pending_order_for_signal(
        self,
        signal: Signal,
        position: PairPosition,
        quantity_multiplier: float,
    ) -> PendingPairOrder:
        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            return PendingPairOrder(
                signal=signal,
                target_stock_units=position.stock_units + quantity_multiplier,
                target_future_units=position.future_units + quantity_multiplier,
                stock_side="buy",
                future_side="sell",
                quantity_multiplier=quantity_multiplier,
            )
        if signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            return PendingPairOrder(
                signal=signal,
                target_stock_units=position.stock_units - quantity_multiplier,
                target_future_units=position.future_units - quantity_multiplier,
                stock_side="sell",
                future_side="buy",
                quantity_multiplier=quantity_multiplier,
            )
        if signal == Signal.EXIT:
            if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
                stock_side = "sell"
                future_side = "buy"
                target_stock_units = max(position.stock_units - quantity_multiplier, 0)
                target_future_units = max(position.future_units - quantity_multiplier, 0)
            elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
                stock_side = "buy"
                future_side = "sell"
                target_stock_units = min(position.stock_units + quantity_multiplier, 0)
                target_future_units = min(position.future_units + quantity_multiplier, 0)
            else:
                raise RuntimeError(f"Unsupported exit position_direction={position.direction}")
            return PendingPairOrder(
                signal=signal,
                target_stock_units=target_stock_units,
                target_future_units=target_future_units,
                stock_side=stock_side,
                future_side=future_side,
                quantity_multiplier=quantity_multiplier,
            )
        raise RuntimeError(f"Unsupported pending order signal={signal}")

    @staticmethod
    def _is_increasing_leg(old_units: float, signed_units: float) -> bool:
        return old_units == 0 or old_units * signed_units > 0

    @staticmethod
    def _weighted_average(old_value: float | None, old_weight: float, new_value: float, new_weight: float) -> float:
        if old_value is None or old_weight <= 0:
            return new_value
        return ((old_value * old_weight) + (new_value * new_weight)) / (old_weight + new_weight)

    def _assert_account_type(self, accounts: Any, account_type: str) -> None:
        is_success = read_attr(accounts, "is_success", "isSuccess")
        if is_success is False:
            message = read_attr(accounts, "message") or "<no message>"
            raise RuntimeError(f"Fubon login failed for {account_type}: {message}")

        data = getattr(accounts, "data", None)
        if data is None and isinstance(accounts, dict):
            data = accounts.get("data")
        if not data:
            message = read_attr(accounts, "message") or "<no message>"
            raise RuntimeError(f"Fubon login returned no accounts for {account_type}: {message}")

        expected = account_type_aliases(account_type)
        types = {normalize_account_type(read_attr(account, "account_type", "accountType")) for account in data}
        if not types.intersection(expected):
            raise RuntimeError(f"Fubon login did not return a {account_type} account; returned account types: {sorted(types)}")

    def get_pair_market(self, pair: PairConfig) -> PairMarket:
        with self._lock:
            spot = self._stock_quotes.get(pair.spot_symbol)
            future = self._future_quotes.get(pair.future_symbol)

        missing = []
        if spot is None:
            missing.append(f"stock:{pair.spot_symbol}")
        if future is None:
            missing.append(f"future:{pair.future_symbol}")
        if missing:
            raise RuntimeError(f"Waiting for websocket books quote: {', '.join(missing)}")

        return PairMarket(
            pair=pair,
            spot=spot,
            future=future,
            trigger_source=None if self._active_quote_update is None else self._active_quote_update.source,
            trigger_symbol=None if self._active_quote_update is None else self._active_quote_update.symbol,
        )

    def _connect_websockets(self) -> None:
        self.stock_ws.on("message", self._handle_stock_message)
        self.futopt_ws.on("message", self._handle_future_message)
        self.stock_ws.on("connect", lambda *args: logging.info("stock websocket connected"))
        self.futopt_ws.on("connect", lambda *args: logging.info("futures websocket connected"))
        self.stock_ws.on("disconnect", lambda *args: logging.warning("stock websocket disconnected: %s", args))
        self.futopt_ws.on("disconnect", lambda *args: logging.warning("futures websocket disconnected: %s", args))
        self.stock_ws.on("error", lambda error: logging.error("stock websocket error: %s", error))
        self.futopt_ws.on("error", lambda error: logging.error("futures websocket error: %s", error))
        self.stock_ws.connect()
        self.futopt_ws.connect()

    def _subscribe_books(self) -> None:
        stock_symbols = sorted({pair.spot_symbol for pair in self.config.pairs})
        for symbol in stock_symbols:
            self.stock_ws.subscribe({"channel": "books", "symbol": symbol})
            self.stock_ws.subscribe({"channel": "trades", "symbol": symbol})
            logging.info("subscribed stock books: %s", symbol)
            logging.info("subscribed stock trades: %s", symbol)

        futures_subscriptions: dict[tuple[str, bool], None] = {}
        for pair in self.config.pairs:
            futures_subscriptions[(pair.future_symbol, pair.futures_after_hours)] = None

        for future_symbol, after_hours in sorted(futures_subscriptions):
            request: dict[str, Any] = {"channel": "books", "symbol": future_symbol}
            if after_hours:
                request["afterHours"] = True
            self.futopt_ws.subscribe(request)
            logging.info("subscribed futures books: %s after_hours=%s", future_symbol, after_hours)

    def _handle_stock_message(self, message: Any) -> None:
        try:
            event = parse_ws_event(message)
            if event.get("event") != "data":
                return
            channel = event.get("channel")
            if channel == "trades":
                self._handle_stock_trade_message(event)
                return
            if channel == "books":
                self._handle_books_message(event, self._stock_quotes, "stock", notify_update=True)
        except Exception:
            logging.exception("failed to parse stock websocket message: %r", message)

    def _handle_future_message(self, message: Any) -> None:
        self._handle_books_message(message, self._future_quotes, "future", notify_update=True)

    def _handle_stock_trade_message(self, event: dict[str, Any]) -> None:
        data = event.get("data") or {}
        symbol = str(data.get("symbol") or "")
        if not symbol:
            return
        is_trial = coerce_bool(data.get("isTrial"))
        if is_trial is None:
            is_trial = False
        with self._lock:
            self._stock_trial_flags[symbol] = is_trial
            quote = self._stock_quotes.get(symbol)
            if quote is not None:
                quote.raw["isTrial"] = is_trial
                quote.raw["status_trial_status_tag"] = 1 if is_trial else 0

    def _handle_books_message(
        self,
        message: Any,
        cache: dict[str, Quote],
        label: str,
        notify_update: bool,
    ) -> None:
        try:
            event = parse_ws_event(message)
            if event.get("event") != "data" or event.get("channel") != "books":
                return

            data = event.get("data") or {}
            quote = parse_books_quote(data)
            with self._lock:
                if label == "stock":
                    is_trial = coerce_bool(data.get("isTrial"))
                    if is_trial is None:
                        is_trial = self._stock_trial_flags.get(quote.symbol)
                    if is_trial is not None:
                        quote.raw["isTrial"] = is_trial
                        quote.raw["status_trial_status_tag"] = 1 if is_trial else 0
                old_quote = cache.get(quote.symbol)
                cache[quote.symbol] = quote
                quote_changed = (
                    old_quote is None
                    or old_quote.bid != quote.bid
                    or old_quote.ask != quote.ask
                    or old_quote.bid_size != quote.bid_size
                    or old_quote.ask_size != quote.ask_size
                )
                should_notify = (
                    notify_update
                    and quote_changed
                    and (label, quote.symbol) not in self._pending_quote_updates
                )
                if should_notify:
                    self._pending_quote_updates.add((label, quote.symbol))
            if should_notify:
                self._quote_updates.put_nowait(QuoteUpdate(source=label, symbol=quote.symbol))
        except ValueError as exc:
            event = parse_ws_event(message)
            data = event.get("data") or {}
            symbol = str(data.get("symbol") or "")
            if "missing bid/ask" in str(exc):
                if symbol:
                    with self._lock:
                        cache.pop(symbol, None)
                logging.debug(
                    "skip %s books quote with incomplete top-of-book symbol=%s reason=%s",
                    label,
                    symbol or "<missing>",
                    exc,
                )
                return
            logging.exception("failed to parse %s websocket message: %r", label, message)
        except Exception:
            logging.exception("failed to parse %s websocket message: %r", label, message)

    def wait_for_future_update(self, timeout: float = 1.0) -> QuoteUpdate | None:
        try:
            quote_update = self._quote_updates.get(timeout=timeout)
        except queue.Empty:
            self._check_pending_order_timeouts()
            return None
        with self._lock:
            self._pending_quote_updates.discard((quote_update.source, quote_update.symbol))
            self._active_quote_update = quote_update
        self._check_pending_order_timeouts()
        return quote_update

    def is_finished(self) -> bool:
        return False

    def place_pair_orders(
        self,
        signal: Signal,
        market: PairMarket,
        position: PairPosition,
    ) -> list[Any]:
        pair = market.pair
        self._check_pending_order_timeouts(pair)
        if signal != Signal.EXIT:
            blocked_reason = self._entry_block_reason(pair)
            if blocked_reason is not None:
                logging.critical("[%s] skip live entry because new entries are blocked: %s", pair.name, blocked_reason)
                return []
        if signal == Signal.EXIT:
            quantity_multiplier = exit_quantity_multiplier(pair, market, position)
            if quantity_multiplier <= 0:
                logging.info("[%s] skip live exit order because top-of-book cannot fill one pair", pair.name)
                return []
        else:
            quantity_multiplier = 1

        pending = self._pending_order_for_signal(signal, position, quantity_multiplier)
        with self._lock:
            existing_pending = self._pending_pair_orders.get(pair.name)
            if existing_pending is not None:
                logging.info(
                    "[%s] skip live order because pending order exists signal=%s "
                    "target_stock_units=%s target_future_units=%s stock_sent=%s future_sent=%s",
                    pair.name,
                    existing_pending.signal.value,
                    existing_pending.target_stock_units,
                    existing_pending.target_future_units,
                    existing_pending.stock_order_sent,
                    existing_pending.future_order_sent,
                )
                return []
            self._pending_pair_orders[pair.name] = pending

        if signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            self._pending_entry_basis[pair.name] = (market.future.bid - market.spot.ask) / market.spot.ask
        elif signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            self._pending_entry_basis[pair.name] = -((market.future.ask - market.spot.bid) / market.spot.bid)
        elif signal != Signal.EXIT:
            with self._lock:
                self._pending_pair_orders.pop(pair.name, None)
            raise RuntimeError(f"Unsupported live order signal={signal} position_direction={position.direction}")

        first_leg = "future" if market.trigger_source == "future" else "stock"
        pending.first_leg = first_leg
        logging.info(
            "[%s] live pair order first_leg=%s trigger=%s:%s signal=%s",
            pair.name,
            first_leg,
            market.trigger_source or "unknown",
            market.trigger_symbol or "unknown",
            signal.value,
        )

        orders: list[Any] = []
        try:
            if first_leg == "future":
                start_ns = time.perf_counter_ns()
                self._log_order_timing(pair, "first_leg_submit_start", "future", pending)
                orders.append(self._place_future_order_for_pending(market, pending))
                elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
                self._log_order_timing(pair, "first_leg_submit_done", "future", pending, elapsed_ms)
                pending.future_order_sent = True
            else:
                start_ns = time.perf_counter_ns()
                self._log_order_timing(pair, "first_leg_submit_start", "stock", pending)
                orders.append(self._place_stock_order_for_pending(market, pending))
                elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
                self._log_order_timing(pair, "first_leg_submit_done", "stock", pending, elapsed_ms)
                pending.stock_order_sent = True
            return orders
        except Exception:
            self._handle_pair_order_submit_failure(pair, signal, pending, orders)
            raise

    def _entry_block_reason(self, pair: PairConfig) -> str | None:
        now = time.time()
        with self._lock:
            if self._new_entries_disabled_reason:
                return self._new_entries_disabled_reason
            locked_reason = self._pair_entry_locked_reason.get(pair.name)
            if locked_reason:
                return locked_reason
            cooldown_until = self._pair_entry_cooldown_until.get(pair.name)
        if cooldown_until is not None and cooldown_until > now:
            return f"pair cooldown active for {cooldown_until - now:.3f}s"
        return None

    def _disable_new_entries(self, reason: str) -> None:
        with self._lock:
            if self._new_entries_disabled_reason is None:
                self._new_entries_disabled_reason = reason
        logging.critical("new live entries disabled: %s", reason)

    def _lock_pair_entries(self, pair: PairConfig, reason: str) -> None:
        with self._lock:
            self._pair_entry_locked_reason.setdefault(pair.name, reason)
        logging.critical("[%s] live entries locked: %s", pair.name, reason)

    def _cooldown_pair(self, pair: PairConfig, seconds: float, reason: str) -> None:
        if seconds <= 0:
            return
        until = time.time() + seconds
        with self._lock:
            self._pair_entry_cooldown_until[pair.name] = max(
                self._pair_entry_cooldown_until.get(pair.name, 0.0),
                until,
            )
        logging.warning("[%s] entry cooldown %.3fs: %s", pair.name, seconds, reason)

    def _check_pending_order_timeouts(self, pair: PairConfig | None = None) -> None:
        now = time.monotonic()
        timed_out: list[tuple[PairConfig, PendingPairOrder, str]] = []
        with self._lock:
            items = (
                [(pair.name, self._pending_pair_orders.get(pair.name))]
                if pair is not None
                else list(self._pending_pair_orders.items())
            )
            for pair_name, pending in items:
                if pending is None or pending.second_leg_failure_handled:
                    continue
                configured_pair = pair if pair is not None and pair.name == pair_name else self._pair_by_name(pair_name)
                if configured_pair is None or configured_pair.fok_order_timeout_sec <= 0:
                    continue
                sent_at = pending.last_order_sent_at or pending.created_at
                elapsed = now - sent_at
                if elapsed < configured_pair.fok_order_timeout_sec:
                    continue
                leg_role = self._pending_timeout_leg_role(pending)
                reason = f"{leg_role} FOK terminal timeout elapsed={elapsed:.3f}s"
                timed_out.append((configured_pair, pending, reason))

        for timeout_pair, pending, reason in timed_out:
            self._handle_pending_order_timeout(timeout_pair, pending, reason)

    @staticmethod
    def _pending_timeout_leg_role(pending: PendingPairOrder) -> str:
        if pending.first_leg == "stock":
            return "second_leg" if pending.future_order_sent else "first_leg"
        if pending.first_leg == "future":
            return "second_leg" if pending.stock_order_sent else "first_leg"
        return "unknown_leg"

    def _handle_pending_order_timeout(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        reason: str,
    ) -> None:
        leg_role = self._pending_timeout_leg_role(pending)
        self._lock_pair_entries(pair, reason)
        logging.critical(
            "[%s] pending order timeout: %s signal=%s first_leg=%s stock_sent=%s future_sent=%s",
            pair.name,
            reason,
            pending.signal.value,
            pending.first_leg,
            pending.stock_order_sent,
            pending.future_order_sent,
        )
        if leg_role == "second_leg":
            self._handle_second_leg_fok_failure(pair, reason)
            return

        with self._lock:
            current = self._pending_pair_orders.get(pair.name)
            if current is not pending:
                return
            self._pending_pair_orders.pop(pair.name, None)
            self._forget_pending_stock_order_keys(pending)
            self._forget_pending_futopt_order_keys(pending)
        self._disable_new_entries(f"first-leg {reason}; pending released for manual review")

    def _handle_pair_order_submit_failure(
        self,
        pair: PairConfig,
        signal: Signal,
        pending: PendingPairOrder,
        accepted_orders: list[Any],
    ) -> None:
        if not accepted_orders:
            with self._lock:
                self._pending_pair_orders.pop(pair.name, None)
                self._forget_pending_stock_order_keys(pending)
                self._forget_pending_futopt_order_keys(pending)
            logging.error("[%s] live pair order rejected before any leg was accepted signal=%s", pair.name, signal.value)
            return
        logging.critical(
            "[%s] live pair order partially submitted signal=%s accepted_legs=%s "
            "stock_sent=%s future_sent=%s; pending order kept to block duplicate orders. "
            "Manual review may be required.",
            pair.name,
            signal.value,
            len(accepted_orders),
            pending.stock_order_sent,
            pending.future_order_sent,
        )

    def _future_order_type_for_signal(self, signal: Signal) -> Any:
        if signal == Signal.EXIT:
            close_order_type = getattr(self.FutOptOrderType, "Close", None)
            if close_order_type is not None:
                return close_order_type
            auto_order_type = getattr(self.FutOptOrderType, "Auto", None)
            if auto_order_type is not None:
                return auto_order_type
            raise RuntimeError("FutOptOrderType.Close/Auto is required for future exit orders")
        return self.FutOptOrderType.New

    def _place_deferred_future_order_if_ready(self, pair: PairConfig) -> None:
        with self._lock:
            if self._positions is None:
                return
            pending = self._pending_pair_orders.get(pair.name)
            position = self._positions[pair.name]
            if pending is None or pending.future_order_sent:
                return
            if not self._same_units(position.stock_units, pending.target_stock_units):
                return
            pending.future_order_sent = True

        try:
            market = self.get_pair_market(pair)
            if not self._second_leg_adjusted_basis_ok(market, pending):
                self._handle_second_leg_fok_failure(pair, "adjusted second-leg basis below threshold")
                return
            start_ns = time.perf_counter_ns()
            self._log_order_timing(pair, "second_leg_submit_start", "future", pending)
            self._place_future_order_for_pending(market, pending)
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
            self._log_order_timing(pair, "second_leg_submit_done", "future", pending, elapsed_ms)
        except Exception:
            with self._lock:
                pending.future_order_sent = False
            logging.exception(
                "[%s] failed to submit deferred future order after stock fill signal=%s "
                "target_stock_units=%s target_future_units=%s",
                pair.name,
                pending.signal.value,
                pending.target_stock_units,
                pending.target_future_units,
            )

    def _place_deferred_stock_order_if_ready(self, pair: PairConfig) -> None:
        with self._lock:
            if self._positions is None:
                return
            pending = self._pending_pair_orders.get(pair.name)
            position = self._positions[pair.name]
            if pending is None or pending.stock_order_sent:
                return
            if not self._same_units(position.future_units, pending.target_future_units):
                return
            pending.stock_order_sent = True

        try:
            market = self.get_pair_market(pair)
            if not self._second_leg_adjusted_basis_ok(market, pending):
                self._handle_second_leg_fok_failure(pair, "adjusted second-leg basis below threshold")
                return
            start_ns = time.perf_counter_ns()
            self._log_order_timing(pair, "second_leg_submit_start", "stock", pending)
            self._place_stock_order_for_pending(market, pending)
            elapsed_ms = (time.perf_counter_ns() - start_ns) / 1_000_000
            self._log_order_timing(pair, "second_leg_submit_done", "stock", pending, elapsed_ms)
        except Exception:
            with self._lock:
                pending.stock_order_sent = False
            logging.exception(
                "[%s] failed to submit deferred stock order after future fill signal=%s "
                "target_stock_units=%s target_future_units=%s",
                pair.name,
                pending.signal.value,
                pending.target_stock_units,
                pending.target_future_units,
            )

    def _second_leg_adjusted_basis_ok(self, market: PairMarket, pending: PendingPairOrder) -> bool:
        pair = market.pair
        threshold = pair.min_second_leg_adjusted_basis_pct
        if threshold is None or threshold <= 0:
            return True
        if self._positions is None:
            return True
        position = self._positions[pair.name]
        adjusted_basis = self._second_leg_adjusted_basis_pct(market, pending, position)
        if adjusted_basis is None:
            logging.warning("[%s] cannot estimate adjusted second-leg basis; allowing second leg", pair.name)
            return True
        ok = adjusted_basis >= threshold
        logging.info(
            "[%s] second_leg_adjusted_basis basis=%s threshold=%s ok=%s signal=%s",
            pair.name,
            f"{adjusted_basis:.6f}",
            f"{threshold:.6f}",
            ok,
            pending.signal.value,
        )
        return ok

    def _second_leg_adjusted_basis_pct(
        self,
        market: PairMarket,
        pending: PendingPairOrder,
        position: PairPosition,
    ) -> float | None:
        pair = market.pair
        if pending.signal == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
            spot_price = self._entry_or_adjusted_spot_price(market, pending, position)
            future_price = self._entry_or_adjusted_future_price(market, pending, position)
            if spot_price is None or future_price is None or spot_price <= 0:
                return None
            return (future_price - spot_price) / spot_price
        if pending.signal == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
            spot_price = self._entry_or_adjusted_spot_price(market, pending, position)
            future_price = self._entry_or_adjusted_future_price(market, pending, position)
            if spot_price is None or future_price is None or spot_price <= 0:
                return None
            return (spot_price - future_price) / spot_price
        return None

    def _entry_or_adjusted_spot_price(
        self,
        market: PairMarket,
        pending: PendingPairOrder,
        position: PairPosition,
    ) -> float | None:
        pair = market.pair
        if pending.first_leg == "stock":
            return position.entry_spot_price or (market.spot.ask if pending.stock_side == "buy" else market.spot.bid)
        if pending.stock_side == "buy":
            base_price = market.spot.ask
        elif pending.stock_side == "sell":
            base_price = market.spot.bid
        else:
            return None
        return self._price_with_second_leg_offset(
            pair,
            pending,
            "stock",
            pending.stock_side,
            base_price,
            pair_leg_tick_size(pair, "stock", base_price, market.spot.raw),
        )

    def _entry_or_adjusted_future_price(
        self,
        market: PairMarket,
        pending: PendingPairOrder,
        position: PairPosition,
    ) -> float | None:
        pair = market.pair
        if pending.first_leg == "future":
            return position.entry_future_price or (market.future.ask if pending.future_side == "buy" else market.future.bid)
        if pending.future_side == "buy":
            base_price = market.future.ask
        elif pending.future_side == "sell":
            base_price = market.future.bid
        else:
            return None
        return self._price_with_second_leg_offset(
            pair,
            pending,
            "future",
            pending.future_side,
            base_price,
            pair_leg_tick_size(pair, "future", base_price, market.future.raw),
        )

    def _place_stock_order_for_pending(self, market: PairMarket, pending: PendingPairOrder) -> Any:
        pending.last_order_sent_at = time.monotonic()
        if pending.stock_side == "buy":
            buy_sell = self.BSAction.Buy
            price = market.spot.ask
        elif pending.stock_side == "sell":
            buy_sell = self.BSAction.Sell
            price = market.spot.bid
        else:
            raise RuntimeError(f"Unsupported stock side={pending.stock_side}")
        price = self._price_with_second_leg_offset(
            market.pair,
            pending,
            "stock",
            pending.stock_side,
            price,
            pair_leg_tick_size(market.pair, "stock", price, market.spot.raw),
        )
        result = self._place_stock_order(
            market,
            buy_sell,
            price,
            quantity_multiplier=pending.quantity_multiplier,
            time_in_force=self._time_in_force_for_pending_leg(market.pair, pending, "stock"),
        )
        self._remember_pending_stock_order_keys_from_result(market.pair, pending, result)
        return result

    def _place_future_order_for_pending(self, market: PairMarket, pending: PendingPairOrder) -> Any:
        pending.last_order_sent_at = time.monotonic()
        if pending.future_side == "buy":
            buy_sell = self.BSAction.Buy
            price = market.future.ask
        elif pending.future_side == "sell":
            buy_sell = self.BSAction.Sell
            price = market.future.bid
        else:
            raise RuntimeError(f"Unsupported future side={pending.future_side}")
        price = self._price_with_second_leg_offset(
            market.pair,
            pending,
            "future",
            pending.future_side,
            price,
            pair_leg_tick_size(market.pair, "future", price, market.future.raw),
        )
        result = self._place_future_order(
            market,
            buy_sell,
            price,
            quantity_multiplier=pending.quantity_multiplier,
            order_type=self._future_order_type_for_signal(pending.signal),
            time_in_force=self._time_in_force_for_pending_leg(market.pair, pending, "future"),
        )
        self._remember_pending_futopt_order_keys_from_result(market.pair, pending, result)
        return result

    def _remember_pending_futopt_order_keys_from_result(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        result: Any,
    ) -> None:
        rows = self._result_rows(result)
        with self._lock:
            for row in rows:
                self._remember_pending_futopt_order_keys(pair, pending, row)
        for row in rows:
            self._handle_futopt_order_report_state(pair, row)

    def _remember_pending_stock_order_keys_from_result(
        self,
        pair: PairConfig,
        pending: PendingPairOrder,
        result: Any,
    ) -> None:
        rows = self._result_rows(result)
        with self._lock:
            for row in rows:
                self._remember_pending_stock_order_keys(pair, pending, row)
        for row in rows:
            self._handle_stock_order_report_state(pair, row)

    def _place_stock_flatten_order(self, market: PairMarket, pending: PendingPairOrder) -> Any:
        pair = market.pair
        if pending.stock_side == "buy":
            buy_sell = self.BSAction.Sell
            price = self._price_with_side_offset(
                market.spot.bid,
                "sell",
                pair_leg_tick_size(pair, "stock", market.spot.bid, market.spot.raw),
                pair.flatten_first_leg_tick_offset,
            )
        elif pending.stock_side == "sell":
            buy_sell = self.BSAction.Buy
            price = self._price_with_side_offset(
                market.spot.ask,
                "buy",
                pair_leg_tick_size(pair, "stock", market.spot.ask, market.spot.raw),
                pair.flatten_first_leg_tick_offset,
            )
        else:
            raise RuntimeError(f"Unsupported stock side={pending.stock_side}")
        return self._place_stock_order(
            market,
            buy_sell,
            price,
            quantity_multiplier=pending.quantity_multiplier,
            time_in_force=self._time_in_force_from_text(pair.flatten_first_leg_time_in_force),
        )

    def _place_future_flatten_order(self, market: PairMarket, pending: PendingPairOrder) -> Any:
        pair = market.pair
        if pending.future_side == "buy":
            buy_sell = self.BSAction.Sell
            price = self._price_with_side_offset(
                market.future.bid,
                "sell",
                pair_leg_tick_size(pair, "future", market.future.bid, market.future.raw),
                pair.flatten_first_leg_tick_offset,
            )
        elif pending.future_side == "sell":
            buy_sell = self.BSAction.Buy
            price = self._price_with_side_offset(
                market.future.ask,
                "buy",
                pair_leg_tick_size(pair, "future", market.future.ask, market.future.raw),
                pair.flatten_first_leg_tick_offset,
            )
        else:
            raise RuntimeError(f"Unsupported future side={pending.future_side}")
        return self._place_future_order(
            market,
            buy_sell,
            price,
            quantity_multiplier=pending.quantity_multiplier,
            order_type=self._future_order_type_for_signal(Signal.EXIT),
            time_in_force=self._time_in_force_from_text(pair.flatten_first_leg_time_in_force),
        )

    def _time_in_force_for_pending_leg(self, pair: PairConfig, pending: PendingPairOrder, leg: str) -> Any:
        if pending.first_leg == leg:
            return self._time_in_force_from_text(pair.first_leg_time_in_force)
        return self._time_in_force_from_text(pair.second_leg_time_in_force)

    @staticmethod
    def _price_with_second_leg_offset(
        pair: PairConfig,
        pending: PendingPairOrder,
        leg: str,
        side: str,
        price: float,
        tick_size: float,
    ) -> float:
        if pending.first_leg in ("", leg) or pair.second_leg_tick_offset <= 0:
            return price
        offset = tick_size * pair.second_leg_tick_offset
        if side == "buy":
            return price + offset
        if side == "sell":
            return max(price - offset, tick_size)
        raise RuntimeError(f"Unsupported order side={side}")

    @staticmethod
    def _price_with_side_offset(price: float, side: str, tick_size: float, tick_offset: float) -> float:
        offset = tick_size * tick_offset
        if side == "buy":
            return price + offset
        if side == "sell":
            return max(price - offset, tick_size)
        raise RuntimeError(f"Unsupported order side={side}")

    def _time_in_force_from_text(self, value: str) -> Any:
        text = value.upper()
        time_in_force = getattr(self.TimeInForce, text, None)
        if time_in_force is None:
            raise RuntimeError(f"Fubon SDK TimeInForce does not expose {text}")
        return time_in_force

    def _place_stock_order(
        self,
        market: PairMarket,
        buy_sell: Any,
        price: float,
        quantity_multiplier: float,
        time_in_force: Any | None = None,
    ) -> Any:
        pair = market.pair
        quantity = int(round(pair.spot_order_qty * quantity_multiplier))
        if quantity <= 0:
            raise RuntimeError(f"Invalid stock order quantity for {pair.name}: {quantity}")
        order = self.Order(
            buy_sell=buy_sell,
            symbol=pair.spot_symbol,
            price=format_order_price(price),
            quantity=quantity,
            market_type=self.MarketType.Common,
            price_type=self.PriceType.Limit,
            time_in_force=self.TimeInForce.ROD if time_in_force is None else time_in_force,
            order_type=self.OrderType.Stock,
            user_def=self._user_def_for_pair(pair),
        )
        result = self.stock_sdk.stock.place_order(self.stock_account, order)
        ensure_order_success(result, f"stock {pair.spot_symbol}")
        logging.info("stock order accepted pair=%s result=%s", pair.name, result)
        return result

    def _place_future_order(
        self,
        market: PairMarket,
        buy_sell: Any,
        price: float,
        quantity_multiplier: float,
        order_type: Any | None = None,
        time_in_force: Any | None = None,
    ) -> Any:
        pair = market.pair
        lot = int(round(pair.future_order_qty * quantity_multiplier))
        if lot <= 0:
            raise RuntimeError(f"Invalid future order lot for {pair.name}: {lot}")
        order = self.FutOptOrder(
            buy_sell=buy_sell,
            symbol=pair.future_symbol,
            price=format_order_price(price),
            lot=lot,
            market_type=self.FutOptMarketType.Future,
            price_type=self.FutOptPriceType.Limit,
            time_in_force=self.TimeInForce.ROD if time_in_force is None else time_in_force,
            order_type=self.FutOptOrderType.New if order_type is None else order_type,
            user_def=self._user_def_for_pair(pair),
        )
        result = self.futures_sdk.futopt.place_order(self.futures_account, order)
        ensure_order_success(result, f"future {pair.future_symbol}")
        logging.info("future order accepted pair=%s result=%s", pair.name, result)
        return result

    def close(self) -> None:
        for name, websocket in (("stock", self.stock_ws), ("futures", self.futopt_ws)):
            disconnect = getattr(websocket, "disconnect", None)
            if disconnect is None:
                continue
            try:
                disconnect()
                logging.info("%s websocket disconnected", name)
            except Exception:
                logging.exception("failed to disconnect %s websocket", name)


class SimulatedMarketDataProvider:
    def __init__(self, config: AppConfig, stop_event: threading.Event) -> None:
        self.config = config
        self.stop_event = stop_event
        self._lock = threading.Lock()
        self._stock_quotes: dict[str, Quote] = {}
        self._future_quotes: dict[str, Quote] = {}
        self._quote_updates: queue.Queue[QuoteUpdate] = queue.Queue()
        self._pending_quote_updates: set[tuple[str, str]] = set()
        self._active_quote_update: QuoteUpdate | None = None
        self._tick = 0
        self._thread = threading.Thread(target=self._run_feed, name="simulated-market-feed", daemon=True)
        self._thread.start()

    def get_pair_market(self, pair: PairConfig) -> PairMarket:
        with self._lock:
            spot = self._stock_quotes.get(pair.spot_symbol)
            future = self._future_quotes.get(pair.future_symbol)

        missing = []
        if spot is None:
            missing.append(f"stock:{pair.spot_symbol}")
        if future is None:
            missing.append(f"future:{pair.future_symbol}")
        if missing:
            raise RuntimeError(f"Waiting for websocket books quote: {', '.join(missing)}")

        return PairMarket(
            pair=pair,
            spot=spot,
            future=future,
            trigger_source=None if self._active_quote_update is None else self._active_quote_update.source,
            trigger_symbol=None if self._active_quote_update is None else self._active_quote_update.symbol,
        )

    def wait_for_future_update(self, timeout: float = 1.0) -> QuoteUpdate | None:
        try:
            quote_update = self._quote_updates.get(timeout=timeout)
        except queue.Empty:
            return None
        with self._lock:
            self._pending_quote_updates.discard((quote_update.source, quote_update.symbol))
            self._active_quote_update = quote_update
        return quote_update

    def is_finished(self) -> bool:
        return False

    def close(self) -> None:
        self.stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run_feed(self) -> None:
        while not self.stop_event.is_set():
            self._tick += 1
            for idx, pair in enumerate(self.config.pairs):
                base = 1000 + idx * 50
                spot_mid = base + (self._tick % 7) * 0.5
                # Oscillate futures premium around common entry thresholds.
                premium = 0.006 + ((self._tick + idx) % 6) * 0.001
                future_mid = spot_mid * (1 + premium)

                spot_quote = Quote(
                    symbol=pair.spot_symbol,
                    bid=round(spot_mid - 0.5, 2),
                    ask=round(spot_mid + 0.5, 2),
                    bid_size=20,
                    ask_size=20,
                )
                future_quote = Quote(
                    symbol=pair.future_symbol,
                    bid=round(future_mid - 0.5, 2),
                    ask=round(future_mid + 0.5, 2),
                    bid_size=20,
                    ask_size=20,
                )
                self._update_quotes(spot_quote, future_quote)
            self.stop_event.wait(0.5)

    def _update_quotes(self, spot_quote: Quote, future_quote: Quote) -> None:
        with self._lock:
            old_spot = self._stock_quotes.get(spot_quote.symbol)
            self._stock_quotes[spot_quote.symbol] = spot_quote
            old_future = self._future_quotes.get(future_quote.symbol)
            self._future_quotes[future_quote.symbol] = future_quote
            spot_changed = (
                old_spot is None
                or old_spot.bid != spot_quote.bid
                or old_spot.ask != spot_quote.ask
            )
            future_changed = (
                old_future is None
                or old_future.bid != future_quote.bid
                or old_future.ask != future_quote.ask
            )
            queued_updates: list[QuoteUpdate] = []
            if spot_changed and ("stock", spot_quote.symbol) not in self._pending_quote_updates:
                self._pending_quote_updates.add(("stock", spot_quote.symbol))
                queued_updates.append(QuoteUpdate(source="stock", symbol=spot_quote.symbol))
            if future_changed and ("future", future_quote.symbol) not in self._pending_quote_updates:
                self._pending_quote_updates.add(("future", future_quote.symbol))
                queued_updates.append(QuoteUpdate(source="future", symbol=future_quote.symbol))
        for quote_update in queued_updates:
            self._quote_updates.put_nowait(quote_update)


@dataclass(frozen=True)
class HistoricalQuoteEvent:
    timestamp: Any
    source: str
    quote: Quote


def latency_raw_prefix(source: str, symbol: str, offset_ms: float) -> str:
    offset_text = f"{offset_ms:g}".replace(".", "p")
    return f"latency_{source}_{symbol}_{offset_text}ms"


def expand_twse_status_columns(df: Any, status_col: str = "status", prefix: str = "status_") -> Any:
    import numpy as np

    s = df[status_col].to_numpy(dtype=np.uint32, copy=False)

    return df.assign(
        **{
            f"{prefix}raw_status": s,
            f"{prefix}data_flag": (s & np.uint32(0xFF)).astype(np.uint8),
            f"{prefix}disclosure_tag": (s & np.uint32(0b00000001)).astype(np.uint8),
            f"{prefix}ask_level": ((s >> np.uint32(1)) & np.uint32(0b00000111)).astype(np.uint8),
            f"{prefix}bid_level": ((s >> np.uint32(4)) & np.uint32(0b00000111)).astype(np.uint8),
            f"{prefix}is_traded": ((s >> np.uint32(7)) & np.uint32(0b00000001)).astype(np.uint8),
            f"{prefix}limit_flag": ((s >> np.uint32(8)) & np.uint32(0xFF)).astype(np.uint8),
            f"{prefix}price_tag": ((s >> np.uint32(8)) & np.uint32(0b00000011)).astype(np.uint8),
            f"{prefix}best_ask": ((s >> np.uint32(10)) & np.uint32(0b00000011)).astype(np.uint8),
            f"{prefix}best_bid": ((s >> np.uint32(12)) & np.uint32(0b00000011)).astype(np.uint8),
            f"{prefix}limit_tag": ((s >> np.uint32(14)) & np.uint32(0b00000011)).astype(np.uint8),
            f"{prefix}data_status": ((s >> np.uint32(16)) & np.uint32(0xFF)).astype(np.uint8),
            f"{prefix}reserve": ((s >> np.uint32(16)) & np.uint32(0b00000011)).astype(np.uint8),
            f"{prefix}close_tag": ((s >> np.uint32(18)) & np.uint32(0b00000001)).astype(np.uint8),
            f"{prefix}open_tag": ((s >> np.uint32(19)) & np.uint32(0b00000001)).astype(np.uint8),
            f"{prefix}match_tag": ((s >> np.uint32(20)) & np.uint32(0b00000001)).astype(np.uint8),
            f"{prefix}close_delay_tag": ((s >> np.uint32(21)) & np.uint32(0b00000001)).astype(np.uint8),
            f"{prefix}open_delay_tag": ((s >> np.uint32(22)) & np.uint32(0b00000001)).astype(np.uint8),
            f"{prefix}trial_status_tag": ((s >> np.uint32(23)) & np.uint32(0b00000001)).astype(np.uint8),
        }
    )


class HistoricalReplayEventCache:
    def __init__(self) -> None:
        self._source_events: dict[tuple[Any, ...], list[tuple[frozenset[str], list[HistoricalQuoteEvent]]]] = {}

    def clear(self) -> None:
        self._source_events.clear()

    def preload_source_events(
        self,
        source: str,
        source_config: HistoricalSourceConfig,
        symbols: set[str],
    ) -> None:
        self.load_source_events(
            HistoricalParquetReplayProvider.new_loader(),
            source,
            source_config,
            symbols,
        )

    def load_source_events(
        self,
        provider: "HistoricalParquetReplayProvider",
        source: str,
        source_config: HistoricalSourceConfig,
        symbols: set[str],
    ) -> list[HistoricalQuoteEvent]:
        key = self._source_key(source, source_config)
        requested = frozenset(symbols)
        entries = self._source_events.setdefault(key, [])
        for cached_symbols, events in entries:
            if requested.issubset(cached_symbols):
                logging.info(
                    "reusing %s cached %s historical rows from %s for %s/%s symbols",
                    len(events),
                    source,
                    source_config.path,
                    len(requested),
                    len(cached_symbols),
                )
                if requested == cached_symbols:
                    return list(events)
                return [event for event in events if event.quote.symbol in requested]

        events = provider._load_source_events_uncached(source, source_config, symbols)
        entries.append((requested, events))
        return list(events)

    @staticmethod
    def _source_key(source: str, source_config: HistoricalSourceConfig) -> tuple[Any, ...]:
        return (
            source,
            tuple(sorted(asdict(source_config).items())),
        )


class HistoricalParquetReplayProvider:
    def __init__(
        self,
        config: AppConfig,
        stop_event: threading.Event,
        event_cache: HistoricalReplayEventCache | None = None,
    ) -> None:
        self.config = config
        self.stop_event = stop_event
        self.event_cache = event_cache
        self._lock = threading.Lock()
        self._stock_quotes: dict[str, Quote] = {}
        self._future_quotes: dict[str, Quote] = {}
        self._active_stock_quotes: dict[str, Quote] | None = None
        self._active_future_quotes: dict[str, Quote] | None = None
        self._quote_updates: queue.Queue[tuple[QuoteUpdate, dict[str, Quote], dict[str, Quote]]] = queue.Queue()
        self._active_quote_update: QuoteUpdate | None = None
        self._finished = False
        self._events = self._load_events()
        self._quote_timelines = self._build_quote_timelines(self._events)
        self._latency_trigger_pairs = self._build_latency_trigger_pairs()
        self._thread = threading.Thread(target=self._run_feed, name="historical-parquet-replay", daemon=True)
        self._thread.start()

    @classmethod
    def new_loader(cls) -> HistoricalParquetReplayProvider:
        return cls.__new__(cls)

    def get_pair_market(self, pair: PairConfig) -> PairMarket:
        with self._lock:
            stock_quotes = self._stock_quotes if self._active_stock_quotes is None else self._active_stock_quotes
            future_quotes = self._future_quotes if self._active_future_quotes is None else self._active_future_quotes
            spot = stock_quotes.get(pair.spot_symbol)
            future = future_quotes.get(pair.future_symbol)

        missing = []
        if spot is None:
            missing.append(f"stock:{pair.spot_symbol}")
        if future is None:
            missing.append(f"future:{pair.future_symbol}")
        if missing:
            raise RuntimeError(f"Waiting for websocket books quote: {', '.join(missing)}")

        return PairMarket(
            pair=pair,
            spot=spot,
            future=future,
            trigger_source=None if self._active_quote_update is None else self._active_quote_update.source,
            trigger_symbol=None if self._active_quote_update is None else self._active_quote_update.symbol,
        )

    def wait_for_future_update(self, timeout: float = 1.0) -> QuoteUpdate | None:
        try:
            quote_update, stock_snapshot, future_snapshot = self._quote_updates.get(timeout=timeout)
        except queue.Empty:
            return None
        with self._lock:
            self._active_stock_quotes = stock_snapshot
            self._active_future_quotes = future_snapshot
            self._active_quote_update = quote_update
        return quote_update

    def is_finished(self) -> bool:
        return self._finished and self._quote_updates.empty()

    def get_quote_at(self, source: str, symbol: str, timestamp: Any) -> Quote | None:
        timeline = self._quote_timelines.get((source, symbol))
        if timeline is None:
            return None
        timestamps, quotes = timeline
        index = bisect_right(timestamps, timestamp) - 1
        if index < 0:
            return None
        return quotes[index]

    def close(self) -> None:
        self.stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2)

    def _load_events(self) -> list[HistoricalQuoteEvent]:
        stock_symbols = {pair.spot_symbol for pair in self.config.pairs}
        future_symbols = {pair.future_symbol for pair in self.config.pairs}
        stock_events = self._load_source_events("stock", self.config.historical.stock, stock_symbols)
        future_events = self._load_source_events("future", self.config.historical.futures, future_symbols)
        events = stock_events + future_events
        events.sort(key=lambda event: event.timestamp)
        if not events:
            raise ValueError("historical replay has no rows after filtering configured pair symbols")
        logging.info("loaded %s historical quote events", len(events))
        return events

    @staticmethod
    def _build_quote_timelines(
        events: list[HistoricalQuoteEvent],
    ) -> dict[tuple[str, str], tuple[list[Any], list[Quote]]]:
        grouped: dict[tuple[str, str], tuple[list[Any], list[Quote]]] = {}
        for event in events:
            key = (event.source, event.quote.symbol)
            timestamps, quotes = grouped.setdefault(key, ([], []))
            timestamps.append(event.timestamp)
            quotes.append(event.quote)
        return grouped

    def _build_latency_trigger_pairs(self) -> dict[tuple[str, str], list[PairConfig]]:
        trigger_pairs: dict[tuple[str, str], list[PairConfig]] = {}
        for pair in self.config.pairs:
            trigger_pairs.setdefault(("stock", pair.spot_symbol), []).append(pair)
            trigger_pairs.setdefault(("future", pair.future_symbol), []).append(pair)
        return trigger_pairs

    def _enrich_latency_trigger_quote(self, source: str, symbol: str, quote: Quote) -> None:
        offsets_ms = self._latency_offsets_ms()
        pairs = self._latency_trigger_pairs.get((source, symbol), [])
        if not offsets_ms or not pairs or quote.raw is None:
            return
        base_timestamp = self._quote_raw_timestamp(quote)
        if base_timestamp is None:
            return
        for pair in pairs:
            for lookup_source, lookup_symbol in (("stock", pair.spot_symbol), ("future", pair.future_symbol)):
                for offset_ms in offsets_ms:
                    self._write_latency_quote_raw(
                        quote,
                        base_timestamp,
                        lookup_source,
                        lookup_symbol,
                        offset_ms,
                        self._quote_timelines,
                    )

    def _latency_offsets_ms(self) -> list[float]:
        execution = self.config.backtest_execution
        send = execution.send_order_latency_ms
        report = execution.match_order_report_latency_ms
        if send <= 0 and report <= 0:
            return []
        offsets = {
            send,
            send + report,
            2 * send + report,
            3 * send + 2 * report,
        }
        return sorted(offset for offset in offsets if offset >= 0)

    def _write_latency_quote_raw(
        self,
        trigger_quote: Quote,
        base_timestamp: Any,
        source: str,
        symbol: str,
        offset_ms: float,
        timelines: dict[tuple[str, str], tuple[list[Any], list[Quote]]],
    ) -> None:
        matched = self._quote_from_timelines_at(timelines, source, symbol, base_timestamp, offset_ms)
        if matched is None:
            return
        quote, quote_timestamp, age_ms = matched
        prefix = latency_raw_prefix(source, symbol, offset_ms)
        raw = trigger_quote.raw
        if raw is None:
            return
        raw[f"{prefix}_bid"] = quote.bid
        raw[f"{prefix}_ask"] = quote.ask
        raw[f"{prefix}_bid_size"] = quote.bid_size
        raw[f"{prefix}_ask_size"] = quote.ask_size
        raw[f"{prefix}_last"] = quote.last
        raw[f"{prefix}_timestamp"] = quote_timestamp
        raw[f"{prefix}_age_ms"] = age_ms

    @staticmethod
    def _quote_raw_timestamp(quote: Quote) -> Any:
        raw = quote.raw or {}
        for key in ("exchtime_tw", "timestamp_tw", "exchtime", "timestamp"):
            value = raw.get(key)
            if value not in (None, ""):
                return value
        return None

    @staticmethod
    def _quote_from_timelines_at(
        timelines: dict[tuple[str, str], tuple[list[Any], list[Quote]]],
        source: str,
        symbol: str,
        base_timestamp: Any,
        offset_ms: float,
    ) -> tuple[Quote, Any, float] | None:
        try:
            import pandas as pd

            target_timestamp = base_timestamp + pd.Timedelta(milliseconds=offset_ms)
        except Exception:
            return None
        timeline = timelines.get((source, symbol))
        if timeline is None:
            return None
        timestamps, quotes = timeline
        index = bisect_right(timestamps, target_timestamp) - 1
        if index < 0:
            return None
        quote_timestamp = timestamps[index]
        try:
            age_ms = (target_timestamp - quote_timestamp).total_seconds() * 1_000
        except Exception:
            age_ms = 0.0
        return quotes[index], quote_timestamp, age_ms

    def _load_source_events(
        self,
        source: str,
        source_config: HistoricalSourceConfig,
        symbols: set[str],
    ) -> list[HistoricalQuoteEvent]:
        if self.event_cache is not None:
            return self.event_cache.load_source_events(self, source, source_config, symbols)
        return self._load_source_events_uncached(source, source_config, symbols)

    def _load_source_events_uncached(
        self,
        source: str,
        source_config: HistoricalSourceConfig,
        symbols: set[str],
    ) -> list[HistoricalQuoteEvent]:
        if not source_config.path:
            raise ValueError(f"historical.{source}.path is required for REPLAY mode")

        try:
            import pandas as pd
        except ImportError as exc:
            raise RuntimeError("pandas is required for parquet replay") from exc

        path = Path(source_config.path)
        if not path.exists():
            raise FileNotFoundError(f"historical.{source}.path does not exist: {path}")

        required_cols = {
            source_config.timestamp_col,
            source_config.symbol_col,
            source_config.bid_col,
            source_config.ask_col,
        }
        optional_cols = {
            col
            for col in (
                source_config.bid_size_col,
                source_config.ask_size_col,
                source_config.last_col,
            )
            if col
        }
        if source_config.status_col:
            required_cols.add(source_config.status_col)

        try:
            import pyarrow.parquet as pq

            available_cols = set(pq.ParquetFile(path).schema_arrow.names)
        except Exception:
            available_cols = set(pd.read_parquet(path, columns=[]).columns)

        missing_required = sorted(required_cols - available_cols)
        if missing_required:
            raise ValueError(
                f"historical.{source}.path {path} missing required columns: {', '.join(missing_required)}"
            )
        missing = sorted(optional_cols - available_cols)
        if missing:
            logging.debug("historical.%s optional columns missing from %s: %s", source, path, ", ".join(missing))

        read_cols = sorted(required_cols | (optional_cols & available_cols))
        try:
            df = pd.read_parquet(
                path,
                columns=read_cols,
                filters=[(source_config.symbol_col, "in", sorted(symbols))],
            )
        except Exception:
            logging.debug("parquet filter pushdown failed for %s; falling back to pandas filtering", path)
            df = pd.read_parquet(path, columns=read_cols)
            df = df[df[source_config.symbol_col].astype(str).isin(symbols)]

        if source_config.status_col:
            df = expand_twse_status_columns(df, status_col=source_config.status_col, prefix="status_")
        if source_config.filter_trial_status and source_config.status_col:
            before = len(df)
            df = df[df["status_trial_status_tag"] == 0]
            logging.info(
                "filtered %s %s trial-status rows from %s",
                before - len(df),
                source,
                path,
            )

        timestamp_display_col = f"{source_config.timestamp_col}_tw"
        df = df.assign(
            **{
                timestamp_display_col: (
                    pd.to_datetime(df[source_config.timestamp_col], unit="ns")
                    + pd.Timedelta(hours=8)
                )
            }
        )

        if source_config.session_start_time or source_config.session_end_time:
            before = len(df)
            event_time = df[timestamp_display_col].dt.time
            if source_config.session_start_time:
                start_time = pd.to_datetime(source_config.session_start_time).time()
                df = df[event_time >= start_time]
                event_time = df[timestamp_display_col].dt.time
            if source_config.session_end_time:
                end_time = pd.to_datetime(source_config.session_end_time).time()
                df = df[event_time <= end_time]
            logging.info(
                "filtered %s %s rows outside session %s-%s from %s",
                before - len(df),
                source,
                source_config.session_start_time or "<open>",
                source_config.session_end_time or "<open>",
                path,
            )

        events: list[HistoricalQuoteEvent] = []
        for row_data in df.to_dict("records"):
            symbol = str(row_data[source_config.symbol_col])
            bid = float(row_data[source_config.bid_col])
            ask = float(row_data[source_config.ask_col])
            if bid <= 0 or ask <= 0:
                continue
            bid_size = self._row_value(row_data, source_config.bid_size_col, source_config.default_size)
            ask_size = self._row_value(row_data, source_config.ask_size_col, source_config.default_size)
            last = self._row_value(row_data, source_config.last_col, None)
            quote = Quote(
                symbol=symbol,
                bid=bid,
                ask=ask,
                bid_size=bid_size,
                ask_size=ask_size,
                last=None if last is None else float(last),
                raw={key: row_data[key] for key in row_data},
            )
            events.append(
                HistoricalQuoteEvent(
                    timestamp=row_data[timestamp_display_col],
                    source=source,
                    quote=quote,
                )
            )
        logging.info("loaded %s %s historical rows from %s", len(events), source, path)
        return events

    @staticmethod
    def _row_value(row_data: dict[str, Any], col: str | None, default: Any) -> Any:
        if not col or col not in row_data:
            return default
        value = row_data[col]
        try:
            import pandas as pd

            if pd.isna(value):
                return default
        except Exception:
            pass
        return value

    def _run_feed(self) -> None:
        interval = max(self.config.historical.replay_interval_sec, 0.0)
        current_timestamp: Any = None
        timestamp_events: list[HistoricalQuoteEvent] = []
        for event in self._events:
            if self.stop_event.is_set():
                break
            if current_timestamp is None:
                current_timestamp = event.timestamp
            if event.timestamp != current_timestamp:
                self._update_timestamp_group(timestamp_events)
                timestamp_events = []
                current_timestamp = event.timestamp
                if interval:
                    self.stop_event.wait(interval)
                    if self.stop_event.is_set():
                        break
            timestamp_events.append(event)
        if timestamp_events and not self.stop_event.is_set():
            self._update_timestamp_group(timestamp_events)
            if interval:
                self.stop_event.wait(interval)
        self._finished = True
        logging.info("historical parquet replay finished")

    def _update_timestamp_group(self, events: list[HistoricalQuoteEvent]) -> None:
        if not events:
            return
        queued_updates: list[tuple[QuoteUpdate, dict[str, Quote], dict[str, Quote]]] = []
        with self._lock:
            old_stocks: dict[str, Quote | None] = {}
            for event in events:
                if event.source == "stock":
                    old_stocks.setdefault(event.quote.symbol, self._stock_quotes.get(event.quote.symbol))
                    self._stock_quotes[event.quote.symbol] = event.quote

            old_futures: dict[str, Quote | None] = {}
            for event in events:
                if event.source != "future":
                    continue
                old_futures.setdefault(event.quote.symbol, self._future_quotes.get(event.quote.symbol))
                self._future_quotes[event.quote.symbol] = event.quote

            for stock_symbol, old_stock in old_stocks.items():
                new_stock = self._stock_quotes[stock_symbol]
                price_changed = (
                    old_stock is None
                    or old_stock.bid != new_stock.bid
                    or old_stock.ask != new_stock.ask
                )
                if price_changed:
                    self._enrich_latency_trigger_quote("stock", stock_symbol, new_stock)
                    queued_updates.append(
                        (
                            QuoteUpdate(source="stock", symbol=stock_symbol),
                            dict(self._stock_quotes),
                            dict(self._future_quotes),
                        )
                    )

            for future_symbol, old_future in old_futures.items():
                new_future = self._future_quotes[future_symbol]
                price_changed = (
                    old_future is None
                    or old_future.bid != new_future.bid
                    or old_future.ask != new_future.ask
                )
                if price_changed:
                    self._enrich_latency_trigger_quote("future", future_symbol, new_future)
                    queued_updates.append(
                        (
                            QuoteUpdate(source="future", symbol=future_symbol),
                            dict(self._stock_quotes),
                            dict(self._future_quotes),
                        )
                    )

        for queued_update in queued_updates:
            self._quote_updates.put_nowait(queued_update)
