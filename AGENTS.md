# Agent Notes

## Project

Taiwan stock top-5 L2 / market-by-price experiments using `hftbacktest`.

The main notebook is `notebooks/hftbacktest_TWStock.ipynb`. Keep it as a thin
runner: reusable logic belongs in `scripts/`.

## Key Files

- `notebooks/hftbacktest_TWStock.ipynb`
  Runnable experiment wrapper. Current sample uses `0050`, `2026-02-23`,
  `09:30:00` to `10:00:00`.

- `scripts/tw_stock_data_to_npz.py`
  Converts Taiwan stock top-5 rows into hftbacktest event `.npz` data.

- `scripts/tw_stock_hftbacktest.py`
  Shared `BacktestConfig`, hftbacktest asset setup, package import isolation,
  state helpers, and BBO helpers.

- `scripts/tw_stock_strategies.py`
  Strategy runners and DataFrame summary helpers.

- `README.md`
  Human-facing summary of current strategy work, event conversion caveats, and
  validation commands.

## Current Strategies

- Strategy 1: aggressive BBO fill sanity test.
- Strategy 2: passive bid1/ask1 queue model comparison.
- Strategy 3: simple fixed-level passive order, e.g. `side="sell"`, `level=5`,
  larger quantity, longer window.

Strategy output should be DataFrames. Prefer compact summary DataFrames in the
notebook and keep detailed raw output available as separate variables.

## HftBacktest Depth Rules

hftbacktest `HashMapMarketDepth` stores depth by integer tick index.

- `depth.best_ask` is a float price.
- `depth.best_ask_tick` is an integer tick.
- `depth.ask_qty_at_tick(tick)` expects an integer tick, not a float price.
- Convert price to tick with `round(price / tick_size)`.

For `tick_size = 0.05`:

```python
price = 77.10
tick = round(price / 0.05)
qty = depth.ask_qty_at_tick(tick)
```

Do not pass `77.10` directly into `ask_qty_at_tick`.

## TW Top-5 Data Rules

The source data is market-by-price, not market-by-order.

- Same-price top-5 rows must be aggregated before emitting depth snapshot
  events.
- The backtest cannot recover individual orders at the same price.
- Queue position at the same price must be modeled by hftbacktest queue models.
- `level=5` means the fifth non-empty price level, not the fifth order at the
  same price.

## Conversion Notes

`tw_stock_data_to_npz.py` emits event rows:

```text
ev, exch_ts, local_ts, px, qty, order_id, ival, fval
```

Each source top-5 row is converted as:

1. Clear visible price range.
2. Insert aggregated snapshot levels.
3. Optionally infer trade events from `total_volume` deltas.

Trade side is not explicit in the source; the converter infers it from previous
BBO by default.

## Validation

Use:

```powershell
python -m py_compile scripts/tw_stock_data_to_npz.py scripts/tw_stock_strategies.py
```

When debugging missing `ask5` / `bid5`, check both:

1. Source or converted events contain five distinct price levels.
2. `BacktestConfig.tick_size` matches the symbol's actual price grid.

Do not infer tick size from already-suspect or rounded event output.
