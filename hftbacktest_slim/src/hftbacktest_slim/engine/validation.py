"""Validation for the currently implemented two-asset immediate profile."""

from __future__ import annotations

from collections.abc import Sequence

from ..config import AssetConfig
from ..enums import Side, TimeInForce
from ..errors import SlimConfigurationError, UnsupportedCapabilityError


def validate_assets(assets: Sequence[AssetConfig]) -> tuple[AssetConfig, AssetConfig]:
    if len(assets) != 2:
        raise SlimConfigurationError("slim engine requires exactly two assets")
    if not all(isinstance(asset, AssetConfig) for asset in assets):
        raise SlimConfigurationError("neutral SlimEngine assets must be AssetConfig instances")
    return assets[0], assets[1]


def validate_asset_no(asset_no: int) -> int:
    if isinstance(asset_no, bool) or not isinstance(asset_no, int) or asset_no not in (0, 1):
        raise SlimConfigurationError("asset_no must be 0 or 1")
    return asset_no


def validate_nanoseconds(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SlimConfigurationError(f"{name} must be a non-negative integer number of nanoseconds")
    return value


def validate_order_id(order_id: int) -> int:
    if (
        isinstance(order_id, bool)
        or not isinstance(order_id, int)
        or order_id < 0
        or order_id > (2**64 - 1)
    ):
        raise SlimConfigurationError("order_id must be an unsigned 64-bit integer")
    return order_id


def supported_side(value: Side | int) -> Side:
    try:
        return Side(value)
    except (TypeError, ValueError) as exc:
        raise UnsupportedCapabilityError(f"slim engine supports only BUY or SELL side; got {value!r}") from exc


def supported_time_in_force(
    value: TimeInForce | int | str, *, strip: bool = True
) -> TimeInForce:
    if isinstance(value, str):
        key = value.strip().upper() if strip else value.upper()
        try:
            return TimeInForce[key]
        except KeyError as exc:
            raise UnsupportedCapabilityError(
                f"slim engine supports only immediate FOK/IOC; got {value!r}"
            ) from exc
    try:
        return TimeInForce(value)
    except (TypeError, ValueError) as exc:
        raise UnsupportedCapabilityError(
            f"slim engine supports only immediate FOK/IOC; got {value!r}"
        ) from exc


__all__ = (
    "supported_side",
    "supported_time_in_force",
    "validate_asset_no",
    "validate_assets",
    "validate_nanoseconds",
    "validate_order_id",
)
