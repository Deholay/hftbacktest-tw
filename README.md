# Taiwan Market Microstructure Backtesting

Research tooling for Taiwan stock, ETF, odd-lot, and stock-future market data,
built on [`hftbacktest`](https://github.com/nkaz001/hftbacktest). The repository
converts Taiwan top-5 market-by-price feeds into HftBacktest events, provides a
small cross-strategy API, and implements a full-market stock-future/spot
arbitrage workflow with latency, position carry, capital replay, and reports.

The main research path is `future_spot/`. The root notebooks remain useful for
conversion checks, queue-model experiments, and new strategy prototypes.

## Backtest result

The latest retained portfolio report covers the requested range
**2026-01-01 through 2026-07-31**. Dates with recognized PnL in the chart run
from **2026-01-08 through 2026-07-30** because PnL is recognized only when both
legs have a matched exit.

| Metric | Result |
| --- | ---: |
| Realized PnL | NT$7,069,581.38 |
| ROI on NT$50M starting capital | 14.14% |
| Peak capital in use | NT$49,779,287.42 (99.56%) |
| Candidate / accepted entries | 8,206 / 4,185 (51.00%) |
| Capital-rejected entries | 4,021 across 29 days |
| Accepted exits | 4,179 |

![Futures/spot realized portfolio overview](docs/assets/futures-spot-portfolio-overview-20260101-20260731.png)

This is a simulated research result, not live performance. It uses a shared
NT$50M capital replay with leverage disabled: spot and futures are each charged
at 100% of notional and limited to NT$25M. Open-position marks are excluded.
Seven incomplete-tick dates and eight expiry-residual pair runs are excluded;
the capital replay removes six additional expiry residual lots. The run also
records two missing-event-data errors on 2026-07-10. Exact inputs, exclusions,
latency settings, and code fingerprints are preserved in the run's
`backtest_manifest.json`.

The source output is:

```text
future_spot/output/hbt_daily_full_market_20260101_20260731_future_order_1ms_response_1ms_feed_0ms_spot_order_1ms_response_35ms_feed_0ms/
```

## Quick start

Use Python 3.11+ and install the shared research dependencies from the
repository root:

```bash
python3 -m pip install -r requirements.txt
```

Run a small serial smoke test before starting a full-market job:

```bash
python3 future_spot/test/run_full_backtest.py \
  --start-date 2026-05-21 \
  --end-date 2026-05-21 \
  --workers 1 \
  --max-pairs 5
```

The retained January-July run used the following result-defining execution and
capital settings in addition to the date range and data paths:

```bash
python3 future_spot/test/run_full_backtest.py \
  --start-date 2026-01-01 \
  --end-date 2026-07-31 \
  --future-order-latency-ms 1 \
  --future-response-latency-ms 1 \
  --future-feed-latency-offset-ms 0 \
  --spot-order-latency-ms 1 \
  --spot-response-latency-ms 35 \
  --spot-feed-latency-offset-ms 0 \
  --post-first-feed-wait spot \
  --post-first-feed-timeout-ms 5000 \
  --no-leverage
```

The default data paths target Linux/WSL mounts under `/mnt/z`. Supply the
`--futures-parquet-template`, daily TWSE/TPEX templates, and
`--data-platform-base` arguments when your storage layout differs. See
[`future_spot/README.md`](future_spot/README.md) for the complete command and
runner controls.

## How the repository is organized

| Path | Responsibility |
| --- | --- |
| `scripts/strategy_api.py` | Project-wide `StrategyContext`, `StrategyDecision`, `Strategy`, and runner contracts. |
| `scripts/hbt_types.py` | Strategy-neutral HBT asset and fill dataclasses. |
| `scripts/hbt_common.py` | Shared queue, order, latency, and fill helpers. |
| `scripts/io_utils.py` | Small DataFrame, CSV, and time helpers. |
| `scripts/tw_stock_data_to_npz.py` | Taiwan top-5 rows to HftBacktest event arrays or `.npz`. |
| `scripts/tw_stock_hftbacktest.py` | Shared stock backtest configuration, asset setup, state, and BBO helpers. |
| `scripts/tw_stock_strategies.py` | Stock notebook strategies and DataFrame summaries. |
| `future_spot/arbitrage/` | Futures/spot pricing, risk, execution, HBT, carry, capital, and reporting implementation. |
| `future_spot/scripts/` | Thin futures/spot command-line entrypoints. |
| `future_spot/test/run_full_backtest.py` | Complete backtest plus report-table and PNG generation. |
| `notebooks/` | Thin experiment runners and the cross-strategy integration example. |

Cross-strategy behavior belongs in root `scripts/`. A strategy folder owns only
its domain models, pricing, risk, execution, configuration, and output schema.
New strategy families should implement `scripts.strategy_api` rather than
importing reusable behavior from `future_spot`. See
[`STRATEGY_GUIDANCE.md`](STRATEGY_GUIDANCE.md) and
[`notebooks/hbt_strategy_interface_example.ipynb`](notebooks/hbt_strategy_interface_example.ipynb).

## Futures/spot workflow

The full-market runner performs one reproducible pipeline:

1. Select trading dates and construct the eligible stock-future/spot universe.
2. Convert source market data into per-symbol HftBacktest event files.
3. Replay every pair with independent feed, order, and response latency per leg.
4. Carry open positions across trading dates without silently rolling futures
   contracts.
5. Replay saved fills against one shared capital budget.
6. Write audit CSVs, report tables, and PNG figures.

The default strategy enters long spot / short future when the configured basis,
effective-tick, and visible-size rules pass. Futures are submitted first by
default. The second leg is re-evaluated after the first fill; the retained run
waits for a fresh spot feed before making that decision. If the second leg
fails, the configured risk action attempts to flatten the first leg.

The Numba engine is the optimized default. Use `--strategy-engine python` for a
reference/debug path or for a custom strategy implementation. Pair processes
run in parallel, while dates remain sequential when position carry is enabled.
`backtest_manifest.json` prevents reuse of cached HBT results when arguments,
daily configs, event files, or strategy/HBT source files have changed.

### Important outputs

Each full run writes these files under its output directory:

- `summary_all_daily_pairs.csv`: pair/day status, positions, and realized PnL.
- `trades_all_daily_pairs.csv`: order and execution events.
- `entry_exit_all_daily_pairs.csv`: compact signal/execution audit.
- `market_all_daily_pairs.csv`: sampled market and non-HOLD signal states.
- `latency_all_daily_pairs.csv`: local, spot-exchange, and futures-exchange
  timing events.
- `position_carry_status.csv`: initial/final positions, expiry, and next-day
  carry status.
- `run_errors.csv`: pair failures or missing inputs; inspect it for every run.
- `reports/`: capital, ROI, failure, latency, and profit tables.
- `figures/`: portfolio, performance, latency, and capital charts.

For a smaller report footprint, use `--report-mode summary` and
`--skip-entry-exit-by-pair`. Detailed tables default to Parquet. Use
`--report-mode full` only when per-event failure and capital diagnostics are
needed.

## Event conversion and market-data limits

Raw provider rows are not valid HftBacktest input. They must be converted to an
event array with fields:

```text
ev, exch_ts, local_ts, px, qty, order_id, ival, fval
```

Stock conversion must use `data_platform_client/data_stock/api`. The older
`data_platform/data_stock/api` path has previously coerced every column to
`Int64`, truncating decimal ETF prices. For `0050` at a `0.05` tick, healthy
events contain prices such as `77.90`, `77.95`, `78.00`, and `78.05`.

Taiwan top-5 data is market-by-price, not market-by-order:

- same-price levels must be aggregated before snapshot events are emitted;
- individual orders and exact queue positions cannot be reconstructed;
- queue position is therefore a model assumption;
- `level=5` means the fifth non-empty price level, not a fifth order at one
  price;
- trade side is inferred from the previous BBO when it is absent from source
  data.

ETF and odd-lot daily parquet feeds contain top-5 prices without top-5 sizes.
Their converter inserts `price_only_depth_qty=1.0` by default, so those replays
are structural BBO/depth checks rather than reliable queue-volume simulations.

HftBacktest depth is keyed by integer tick index. Convert prices before querying
quantity:

```python
price = 77.10
tick = round(price / 0.05)
qty = depth.ask_qty_at_tick(tick)
```

Passing `77.10` directly to `ask_qty_at_tick` is incorrect; the method expects
the integer tick (`1542` in this example).

## Notebook experiments

The notebooks are intentionally thin runners:

- `hftbacktest_TWStock.ipynb`: stock conversion and three queue/fill sanity
  strategies.
- `hftbacktest_TWETF.ipynb`: ETF daily-parquet replay.
- `hftbacktest_TWOddLot.ipynb`: odd-lot daily-parquet replay.
- `hftbacktest_TWStockFuture.ipynb`: stock-future daily-parquet replay.
- `hbt_strategy_interface_example.ipynb`: adapter template for a new strategy
  family.

Reusable logic belongs in Python modules. Notebook output should favor compact
summary DataFrames while retaining detailed frames in separate variables.

## Validation

Run the focused test suite and compile every retained Python entrypoint:

```bash
python3 -m pytest -q tests future_spot/test
python3 -m py_compile scripts/*.py future_spot/arbitrage/*.py future_spot/scripts/*.py future_spot/test/*.py
```

When debugging missing `ask5` or `bid5`, verify both the number of distinct
levels in source/events and the symbol's real tick size. Do not infer tick size
from output that is already suspected of rounding or conversion errors.
