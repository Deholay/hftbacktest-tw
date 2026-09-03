"""Provider-neutral Top-5 cleanup, aggregation, ordering, and BBO selection."""

from __future__ import annotations

import numpy as np
from numba import njit


@njit(cache=True)
def _aggregate_depth_side(
    prices: np.ndarray,
    quantities: np.ndarray,
    row: int,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    ascending: bool,
    work_prices: np.ndarray,
    work_quantities: np.ndarray,
) -> int:
    count = 0
    for level in range(prices.shape[1]):
        px = prices[row, level]
        qty = quantities[row, level]
        if (not np.isfinite(qty)) and use_price_only_depth_qty and np.isfinite(px) and px > 0.0:
            qty = price_only_depth_qty
        qty *= volume_scale
        if not (np.isfinite(px) and px > 0.0 and np.isfinite(qty) and qty > 0.0):
            continue

        found = -1
        for index in range(count):
            if work_prices[index] == px:
                found = index
                break
        if found >= 0:
            work_quantities[found] += qty
        else:
            work_prices[count] = px
            work_quantities[count] = qty
            count += 1

    # Top-5 is tiny; insertion sort avoids a temporary allocation for each row.
    for index in range(1, count):
        px = work_prices[index]
        qty = work_quantities[index]
        cursor = index - 1
        while cursor >= 0 and (
            (ascending and work_prices[cursor] > px)
            or ((not ascending) and work_prices[cursor] < px)
        ):
            work_prices[cursor + 1] = work_prices[cursor]
            work_quantities[cursor + 1] = work_quantities[cursor]
            cursor -= 1
        work_prices[cursor + 1] = px
        work_quantities[cursor + 1] = qty
    return count


@njit(cache=True)
def normalized_bbo_from_depth_columns(
    prices: np.ndarray,
    quantities: np.ndarray,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    bid: bool,
) -> tuple[np.ndarray, np.ndarray]:
    """Return normalized best prices and aggregate quantities.

    Inputs are expected to be contiguous two-dimensional float64 arrays. The
    output arrays remain float64 with NaN representing an empty side.
    """

    best_prices = np.full(prices.shape[0], np.nan, dtype=np.float64)
    best_quantities = np.full(prices.shape[0], np.nan, dtype=np.float64)
    work_prices = np.empty(prices.shape[1], dtype=np.float64)
    work_quantities = np.empty(prices.shape[1], dtype=np.float64)
    for row in range(prices.shape[0]):
        count = _aggregate_depth_side(
            prices,
            quantities,
            row,
            volume_scale,
            price_only_depth_qty,
            use_price_only_depth_qty,
            not bid,
            work_prices,
            work_quantities,
        )
        if count:
            best_prices[row] = work_prices[0]
            best_quantities[row] = work_quantities[0]
    return best_prices, best_quantities


__all__ = ("normalized_bbo_from_depth_columns",)
