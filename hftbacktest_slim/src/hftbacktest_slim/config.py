"""Neutral configuration types for the future slim runtime."""

from __future__ import annotations

import math
from dataclasses import dataclass
from os import PathLike
from pathlib import Path

from .errors import SlimConfigurationError


@dataclass(frozen=True, slots=True, init=False)
class AssetConfig:
    """Immutable inputs consumed by one slim compact-BBO asset.

    Reference-HftBacktest concerns such as instrument setup, queue models,
    fees, lot/contract accounting, and recorders intentionally do not belong
    here.
    """

    symbol: str
    data_path: Path
    tick_size: float
    feed_latency_offset_ns: int
    order_entry_latency_ns: int
    order_response_latency_ns: int

    def __init__(
        self,
        symbol: str,
        data_path: str | PathLike[str] | Path,
        tick_size: float,
        feed_latency_offset_ns: int = 0,
        order_entry_latency_ns: int = 0,
        order_response_latency_ns: int = 0,
    ) -> None:
        if not isinstance(symbol, str) or not symbol:
            raise SlimConfigurationError("symbol must be a non-empty string")
        try:
            path = Path(data_path)
        except TypeError as exc:
            raise SlimConfigurationError("data_path must be path-like") from exc
        try:
            normalized_tick_size = float(tick_size)
        except (TypeError, ValueError) as exc:
            raise SlimConfigurationError("tick_size must be a positive finite number") from exc
        if not math.isfinite(normalized_tick_size) or normalized_tick_size <= 0:
            raise SlimConfigurationError("tick_size must be a positive finite number")

        feed_offset = _integer_nanoseconds("feed_latency_offset_ns", feed_latency_offset_ns)
        entry_latency = _integer_nanoseconds("order_entry_latency_ns", order_entry_latency_ns)
        response_latency = _integer_nanoseconds(
            "order_response_latency_ns", order_response_latency_ns
        )
        if entry_latency < 0:
            raise SlimConfigurationError("order_entry_latency_ns must be non-negative")
        if response_latency < 0:
            raise SlimConfigurationError("order_response_latency_ns must be non-negative")

        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "data_path", path)
        object.__setattr__(self, "tick_size", normalized_tick_size)
        object.__setattr__(self, "feed_latency_offset_ns", feed_offset)
        object.__setattr__(self, "order_entry_latency_ns", entry_latency)
        object.__setattr__(self, "order_response_latency_ns", response_latency)


def _integer_nanoseconds(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SlimConfigurationError(f"{name} must be an integer number of nanoseconds")
    return value


__all__ = ("AssetConfig",)
