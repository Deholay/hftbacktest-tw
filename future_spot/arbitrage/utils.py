from __future__ import annotations

import json
from typing import Any

from .models import PairConfig, PairMarket, PairPosition, Quote, Signal
from .ticks import tw_stock_tick_size

ARBITRAGE_USER_DEF_PREFIX = "FSARB"
USER_DEF_MAX_LENGTH = 10
STOCK_BOARD_LOT_SHARES = 1000


def exit_quantity_multiplier(pair: PairConfig, market: PairMarket, position: PairPosition) -> float:
    open_quantity = abs(position.quantity)
    if open_quantity <= 0:
        return 0.0

    if position.direction == Signal.ENTER_LONG_SPOT_SHORT_FUTURE:
        stock_capacity = _stock_book_pair_capacity(pair, market.spot.bid_size)
        future_capacity = _future_book_pair_capacity(pair, market.future.ask_size)
    elif position.direction == Signal.ENTER_SHORT_SPOT_LONG_FUTURE:
        stock_capacity = _stock_book_pair_capacity(pair, market.spot.ask_size)
        future_capacity = _future_book_pair_capacity(pair, market.future.bid_size)
    else:
        return 0.0

    return max(min(open_quantity, stock_capacity, future_capacity), 0.0)


def _stock_book_pair_capacity(pair: PairConfig, book_size: float) -> int:
    if pair.spot_order_qty <= 0:
        return 0
    return int((float(book_size) * STOCK_BOARD_LOT_SHARES) // pair.spot_order_qty)


def _future_book_pair_capacity(pair: PairConfig, book_size: float) -> int:
    if pair.future_order_qty <= 0:
        return 0
    return int(float(book_size) // pair.future_order_qty)


def parse_stock_quote(raw: dict[str, Any], symbol: str) -> Quote:
    bids = raw.get("bids") or []
    asks = raw.get("asks") or []
    bid = _first_book_price(bids)
    ask = _first_book_price(asks)
    bid_size = _first_book_size(bids)
    ask_size = _first_book_size(asks)

    last_trade = raw.get("lastTrade") or {}
    last = raw.get("lastPrice") or last_trade.get("price")

    if bid <= 0 or ask <= 0:
        bid = float(last_trade.get("bid") or bid or 0)
        ask = float(last_trade.get("ask") or ask or 0)

    if bid <= 0 or ask <= 0:
        raise ValueError(f"Invalid stock quote for {symbol}: missing bid/ask")

    return Quote(symbol=symbol, bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size, last=last, raw=raw)


def parse_future_quote(raw: dict[str, Any], symbol: str) -> Quote:
    last_trade = raw.get("lastTrade") or {}
    bid = float(last_trade.get("bid") or 0)
    ask = float(last_trade.get("ask") or 0)
    last = raw.get("lastPrice") or last_trade.get("price")
    size = float(last_trade.get("size") or 0)

    if bid <= 0 or ask <= 0:
        raise ValueError(f"Invalid future quote for {symbol}: missing lastTrade bid/ask")

    return Quote(symbol=symbol, bid=bid, ask=ask, bid_size=size, ask_size=size, last=last, raw=raw)


def parse_ws_event(message: Any) -> dict[str, Any]:
    if isinstance(message, str):
        return json.loads(message)
    if isinstance(message, bytes):
        return json.loads(message.decode("utf-8"))
    if isinstance(message, dict):
        return message
    raise TypeError(f"Unsupported websocket message type: {type(message).__name__}")


def read_attr(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def read_int_attr(obj: Any, *names: str) -> int:
    value = read_attr(obj, *names)
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def read_float_attr(obj: Any, *names: str) -> float:
    value = read_attr(obj, *names)
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_buy_sell(value: Any) -> str:
    text = str(value or "").strip().lower()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    if text in {"buy", "b", "1"}:
        return "buy"
    if text in {"sell", "s", "2"}:
        return "sell"
    return text


def coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "t", "yes", "y", "1"}:
        return True
    if text in {"false", "f", "no", "n", "0"}:
        return False
    return None


def ensure_order_success(result: Any, label: str) -> None:
    is_success = read_attr(result, "is_success", "isSuccess")
    if is_success is False:
        message = read_attr(result, "message") or "<no message>"
        raise RuntimeError(f"{label} order failed: {message}; result={result}")


def format_order_price(price: float) -> str:
    text = f"{price:.4f}".rstrip("0").rstrip(".")
    return text if text else "0"


def sanitize_user_def(value: str) -> str:
    cleaned = "".join(char for char in value.upper() if char.isalnum())
    return cleaned[:USER_DEF_MAX_LENGTH]


def make_arbitrage_user_def(value: str) -> str:
    suffix_length = USER_DEF_MAX_LENGTH - len(ARBITRAGE_USER_DEF_PREFIX)
    cleaned = sanitize_user_def(value)
    return f"{ARBITRAGE_USER_DEF_PREFIX}{cleaned[:suffix_length]}"


def make_arbitrage_user_def_for_index(index: int) -> str:
    if index < 0:
        raise ValueError("user_def index must be >= 0")
    suffix_length = USER_DEF_MAX_LENGTH - len(ARBITRAGE_USER_DEF_PREFIX)
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    value = index
    digits = []
    while value:
        value, remainder = divmod(value, len(alphabet))
        digits.append(alphabet[remainder])
    suffix = "".join(reversed(digits or ["0"]))
    if len(suffix) > suffix_length:
        raise ValueError(f"user_def index is too large for {USER_DEF_MAX_LENGTH} chars: {index}")
    return f"{ARBITRAGE_USER_DEF_PREFIX}{suffix.rjust(suffix_length, '0')}"


def is_arbitrage_user_def(value: Any) -> bool:
    if value in (None, ""):
        return False
    return sanitize_user_def(str(value)).startswith(ARBITRAGE_USER_DEF_PREFIX)


def normalize_account_type(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).lower()
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.replace("_", "").replace("-", "")


def account_type_aliases(account_type: str) -> set[str]:
    normalized = normalize_account_type(account_type)
    if normalized == "futopt":
        return {"futopt", "future", "futures", "futureoption", "futureoptions"}
    if normalized == "stock":
        return {"stock", "security", "securities"}
    return {normalized}


def parse_books_quote(data: dict[str, Any]) -> Quote:
    symbol = str(data.get("symbol") or "")
    bid = _first_book_price(data.get("bids") or [])
    ask = _first_book_price(data.get("asks") or [])
    bid_size = _first_book_size(data.get("bids") or [])
    ask_size = _first_book_size(data.get("asks") or [])
    if not symbol:
        raise ValueError("books message missing symbol")
    if bid <= 0 or ask <= 0:
        raise ValueError(f"books message for {symbol} missing bid/ask")
    return Quote(
        symbol=symbol,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        raw=data,
    )


def _first_book_price(book: list[dict[str, Any]]) -> float:
    if not book:
        return 0.0
    return float(book[0].get("price") or 0)


def _first_book_size(book: list[dict[str, Any]]) -> float:
    if not book:
        return 0.0
    return float(book[0].get("size") or 0)


def pct(value: float) -> str:
    return f"{value * 100:.4f}%"


def tw_price_tick_size(price: float) -> float:
    return tw_stock_tick_size(price)
