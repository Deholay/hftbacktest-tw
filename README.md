# TW Stock HftBacktest Notes

[繁體中文](README.zh-TW.md)

This repo contains Taiwan stock L2 / top-5 market-by-price experiments built on `hftbacktest`.

## Project Architecture

- `scripts/` owns reusable project-level interfaces and shared HBT helpers.
- `scripts/strategy_api.py` is the public strategy integration contract for new
  strategy families.
- `future_spot/` is one concrete strategy implementation: Taiwan stock-future /
  spot arbitrage.
- Future strategy folders should implement adapters against
  `scripts.strategy_api` instead of depending on `future_spot`.
- `future_spot/scripts/` contains thin CLI entrypoints for the `future_spot`
  strategy only. Reusable code should live in root `scripts/` or in the
  strategy package that owns the domain behavior.

See `STRATEGY_GUIDANCE.md` and `notebooks/hbt_strategy_interface_example.ipynb`
for the adapter contract and notebook/CLI usage.

## Setup

Install the shared Python dependencies from the repository root:

```bash
python3 -m pip install -r requirements.txt
```

`requirements.txt` installs `hftbacktest` from pip along with the core
numeric/notebook stack. Optional live-trading dependencies such as `fubon_neo`
are not pinned because the current retained workflow is offline HBT research.

The current working notebook is `notebooks/hftbacktest_TWStock.ipynb`. The notebook is intentionally thin: data conversion, hftbacktest setup, and strategy logic live in `scripts/` so notebook cells mostly run functions and display DataFrames.

## Key Files

- `scripts/tw_stock_data_to_npz.py`: converts Taiwan stock top-5 rows into hftbacktest event `.npz` files.
- `scripts/tw_stock_hftbacktest.py`: shared hftbacktest config, asset setup, BBO/state helpers, and package import isolation.
- `scripts/tw_stock_strategies.py`: strategy runners and DataFrame summary helpers.
- `scripts/strategy_api.py`: root strategy context/decision protocols and optional registry.
- `scripts/hbt_types.py`: HBT asset/fill dataclasses that are not tied to one strategy family.
- `scripts/hbt_common.py`: generic HBT queue/order/latency/fill helpers.
- `scripts/io_utils.py`: generic CSV/DataFrame/time conversion helpers.
- `future_spot/`: futures/spot arbitrage implementation of the root strategy interface.
- `notebooks/hbt_strategy_interface_example.ipynb`: root-level template for new strategy notebooks.
- `requirements.txt`: shared Python dependencies for notebooks and retained HBT runners.
- `notebooks/hftbacktest_TWStock.ipynb`: runnable experiment wrapper.
- `notebooks/hftbacktest_TWETF.ipynb`: ETF daily parquet runner.
- `notebooks/hftbacktest_TWOddLot.ipynb`: odd-lot daily parquet runner.
- `notebooks/hftbacktest_TWStockFuture.ipynb`: stock-future daily parquet runner.

## Additional Source Converters

The stock DataAPI path remains `data_platform_client/data_stock/api`. ETF, Odd Lot, and Stock Future use the daily parquet roots listed in `path.toml`, then share the same event builder, hftbacktest setup, and strategy code.

Column index check against the stock top-5 schema:

| Source | Same as stock? | Relevant column layout | Converter |
| --- | --- | --- | --- |
| Stock | Yes | `ask_price1`/`ask_volume1` start at stock schema indexes 16/17. | `convert_tw_stock_to_npz` |
| ETF | No | 23 columns; `ask_price1` index 7, `bid_price1` index 8; no per-level volume columns. | `convert_tw_etf_to_npz` |
| Odd Lot | No | Same 23-column price-only layout as ETF; `symbol` may be stored as an integer, so `0050` also matches `50`. | `convert_tw_odd_lot_to_npz` |
| Stock Future | No | 40 columns; `ask_price1`/`ask_volume1`/`bid_price1`/`bid_volume1` start at indexes 7/8/9/10. | `convert_tw_stock_future_to_npz` |

ETF and Odd Lot parquet data only contain top-5 prices, not top-5 quantities. Their converters use `price_only_depth_qty=1.0` by default so BBO/depth replay can run, but queue-size-sensitive strategy output should be read as a structural replay test rather than a true queue-volume model.

## Current Notebook Flow

1. Convert TW stock L2/top-5 data into hftbacktest event data.
2. Build `BacktestConfig`.
3. Run Strategy 1, Strategy 2, and Strategy 3.
4. Display compact DataFrame summaries.

Current sample config:

```python
SYMBOL = "0050"
START_DATE = "2026-02-23"
END_DATE = START_DATE
START_TIME = "09:30:00"
END_TIME = "10:00:00"
TICK_SIZE = 0.05
```

`TICK_SIZE` is explicit because hftbacktest depth is keyed by tick index. Do not infer it from already-converted event samples if the conversion is under review.

## Strategies

### Strategy 1: Aggressive BBO Fill

`run_aggressive_fill_strategy`

- Buys at best ask.
- Sells at best bid.
- Expected to fill immediately by design.
- Used as a sanity check for event replay, order submission, timestamps, and accounting fields.

Output includes order side, price, execution price/quantity, send order time, fill time, position, balance, equity, and trade counters.

### Strategy 2: Passive Bid1 / Ask1 Queue Model Comparison

`run_queue_model_comparison`

- Places passive buy at bid1 and passive sell at ask1.
- Runs multiple queue models, currently `risk_adverse` and `log_prob`.
- Compares fill timestamps and `queue_model_fill_delta_ns`.
- Records `send_order_time` and `fill_time` so zero fill deltas are easier to diagnose.

### Strategy 3: Simple Fixed Level Passive Order

`run_level_queue_model_comparison`

- Current simplified form targets one fixed book level, e.g. `side="sell"`, `level=5`.
- Uses a larger quantity and longer window than Strategy 2.
- Returns both requested `level` and observed `actual_level`.

`actual_level` matters because the visible book may not contain the requested fifth distinct price level. If fewer levels exist, the strategy falls back to the deepest visible level and records that actual level explicitly.

## HftBacktest Depth Model Notes

hftbacktest `HashMapMarketDepth` stores book depth by integer tick index.

Important API behavior:

```python
depth.best_ask          # float price
depth.best_ask_tick     # integer tick
depth.ask_qty_at_tick   # expects integer tick, not float price
```

For a stock with `tick_size = 0.05`:

```python
price = 77.10
tick = round(price / 0.05)  # 1542
qty = depth.ask_qty_at_tick(tick)
```

Do not call `depth.ask_qty_at_tick(77.10)`. That passes a price into an API that expects a tick index.

Strategy 3 scans from `best_ask_tick` or `best_bid_tick` and checks `ask_qty_at_tick` / `bid_qty_at_tick` for non-empty levels.

## TW Top-5 Data Caveats

The TW source data is top-5 market-by-price data, not market-by-order data.

Implications:

- It can model visible aggregate depth per price level.
- It cannot recover individual order queue positions inside the same price.
- Queue position must be estimated by hftbacktest queue models.
- Same-price top-5 rows must be aggregated before emitting hftbacktest depth snapshot events.

The converter now aggregates same-price visible rows:

```text
ask_price2 = 77.10, ask_volume2 = 20
ask_price3 = 77.10, ask_volume3 = 30

emitted depth at 77.10 = 50
```

This avoids multiple same-price snapshot events overwriting each other in hftbacktest depth.

## Event Conversion Notes

`tw_stock_data_to_npz.py` emits hftbacktest event rows with:

```text
ev, exch_ts, local_ts, px, qty, order_id, ival, fval
```

Each source row is treated as a top-5 book image:

1. Clear the visible top-5 price range for each side.
2. Insert aggregated snapshot levels.
3. Optionally infer trade events from `total_volume` deltas.

Trade side is not explicit in the source fields. The converter infers side from the previous BBO by default.

## Validation Commands

Compile the changed scripts:

```powershell
python -m py_compile scripts/tw_stock_data_to_npz.py scripts/tw_stock_strategies.py
```

Quick synthetic check for float price preservation and same-price aggregation:

```python
from scripts.tw_stock_data_to_npz import iter_depth_events, event_kind, DEPTH_SNAPSHOT_EVENT, SELL_EVENT

row = {
    "ask_price1": "77.05", "ask_volume1": "10",
    "ask_price2": "77.10", "ask_volume2": "20",
    "ask_price3": "77.10", "ask_volume3": "30",
}

events = list(iter_depth_events(row, 1, 1, 3, 1.0))
asks = [
    (px, qty)
    for ev, _, _, px, qty, *_ in events
    if ev & SELL_EVENT and event_kind(ev) == DEPTH_SNAPSHOT_EVENT
]
print(asks)
```

Expected output:

```text
[(77.05, 10.0), (77.1, 50.0)]
```
