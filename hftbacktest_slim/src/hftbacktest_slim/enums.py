"""Neutral integer enums whose values match the current native ABI."""

from enum import IntEnum


class Side(IntEnum):
    """Order side values accepted by the native matcher."""

    BUY = 1
    SELL = -1


class TimeInForce(IntEnum):
    """Time-in-force values accepted by the current native matcher."""

    FOK = 2
    IOC = 3


class OrderStatus(IntEnum):
    """Order status values represented by the current native order view."""

    NEW = 1
    EXPIRED = 2
    FILLED = 3


class OrderType(IntEnum):
    """Order-type values retained by the current ABI-facing facade."""

    LIMIT = 0


__all__ = ("OrderStatus", "OrderType", "Side", "TimeInForce")
