# Agent Guide

## Mission

This repository supports Taiwan market-microstructure research with
`hftbacktest`. It has two related surfaces:

1. Root stock/ETF/odd-lot/futures converters and notebook experiments.
2. The `future_spot` full-market stock-future/spot arbitrage backtest, capital
   replay, and reporting pipeline.

Preserve reproducibility and market-data semantics. Do not make a backtest look
cleaner by silently dropping dates, pairs, errors, open positions, or capital
constraints.

## Architecture boundaries

- Root `scripts/` is the cross-strategy library layer.
- `scripts/strategy_api.py` owns project-wide strategy contracts:
  `StrategyContext`, `StrategyDecision`, `Strategy`, `StrategyRunner`, and the
  lightweight registry.
- `scripts/hbt_types.py`, `scripts/hbt_common.py`, and `scripts/io_utils.py` own
  strategy-neutral HBT types, execution helpers, and I/O helpers.
- Strategy folders own domain models, pricing, risk, execution, config parsing,
  output schemas, and adapters.
- `future_spot` implements the root strategy interface in
  `future_spot/arbitrage/strategy_adapter.py`.
- Never make a new strategy family import `future_spot` for reusable behavior.
  Promote clean, domain-neutral code to root `scripts/` first.
- Keep notebooks thin. Reusable conversion, execution, analytics, and plotting
  logic belongs in Python modules.
- Keep `future_spot/scripts/` as thin CLI entrypoints. Business logic belongs
  in `future_spot/arbitrage/`.

## Project map

### Root library and notebooks

- `scripts/tw_stock_data_to_npz.py`: source rows to HftBacktest event arrays or
  `.npz` files.
- `scripts/tw_stock_hftbacktest.py`: `BacktestConfig`, asset setup, import
  isolation, state, and BBO helpers.
- `scripts/tw_stock_strategies.py`: stock notebook strategies and DataFrame
  summary helpers.
- `notebooks/hftbacktest_TWStock.ipynb`: current stock sample (`0050`,
  `2026-02-23`, `09:30:00`-`10:00:00`).
- `notebooks/hftbacktest_TWETF.ipynb`,
  `notebooks/hftbacktest_TWOddLot.ipynb`, and
  `notebooks/hftbacktest_TWStockFuture.ipynb`: daily-parquet runners.
- `notebooks/hbt_strategy_interface_example.ipynb`: cross-strategy adapter
  example.

### Futures/spot implementation

- `future_spot/test/run_full_backtest.py`: supported one-command HBT + reports
  + PNG entrypoint.
- `future_spot/arbitrage/full_market_runner.py`: CLI definition and compatibility
  implementation.
- `future_spot/arbitrage/daily_pipeline.py`: date selection, daily universe,
  config, and path resolution.
- `future_spot/arbitrage/event_data.py`: source resolution, per-symbol event
  conversion, and reuse audit.
- `future_spot/arbitrage/hbt_pipeline.py`: pair configs, execution, cache, and
  result loading.
- `future_spot/arbitrage/hbt_backtest.py`: pair-level HBT and latency capture.
- `future_spot/arbitrage/hbt_numba.py`: optimized default strategy scanner.
- `future_spot/arbitrage/position_carry.py`: cross-date position restoration and
  expiry handling.
- `future_spot/arbitrage/capital.py`: shared-capital candidate replay.
- `future_spot/arbitrage/reporting.py` and `future_spot/test/report_tables.py`:
  persisted analytical tables.
- `future_spot/test/report_plots.py`: saved PNG reports.
- `future_spot/arbitrage/backtest_report.py` and
  `future_spot/arbitrage/result_replot.py`: filtered/durable report generation
  and result replotting.

## Non-negotiable data rules

### Data platform

Use `data_platform_client/data_stock/api` for stock conversion. Do not switch
back to `data_platform/data_stock/api`; that older path has cast all columns to
`Int64` and truncated decimal ETF prices.

For `0050` with `tick_size = 0.05`, healthy converted prices include `77.90`,
`77.95`, `78.00`, and `78.05`. Output containing only `77` and `78` indicates a
wrong or stale conversion import. Restart the notebook kernel or reload
`scripts.tw_stock_data_to_npz` before diagnosing downstream HBT behavior.

### HftBacktest input

HftBacktest accepts an `.npz` containing key `data` or an in-memory NumPy event
array with this dtype layout:

```text
ev, exch_ts, local_ts, px, qty, order_id, ival, fval
```

It does not accept raw provider rows. Each top-5 source row is converted by
clearing the visible range, inserting aggregated snapshot levels, and
optionally inferring trades from `total_volume` deltas. Source trade side is not
explicit and is inferred from the previous BBO by default.

### Top-5 market-by-price semantics

- The source is market-by-price, not market-by-order.
- Aggregate repeated prices across top-5 fields before emitting snapshots.
- Individual same-price orders and exact queue position are unrecoverable.
- Queue position must be represented by an explicit HftBacktest queue model.
- `level=5` means the fifth distinct non-empty price, never the fifth order at
  one price.
- ETF and odd-lot feeds have top-5 prices but no top-5 quantities. Their default
  `price_only_depth_qty=1.0` output is suitable for structural replay, not
  queue-size inference.

### Tick-index depth API

`HashMapMarketDepth` stores quantity by integer tick:

```python
price = 77.10
tick = round(price / tick_size)
qty = depth.ask_qty_at_tick(tick)
```

`depth.best_ask` is a float price, `depth.best_ask_tick` is an integer, and
`ask_qty_at_tick`/`bid_qty_at_tick` require the integer. Never pass a float price
to a quantity-at-tick method.

## Futures/spot behavior to preserve

- The default adapter is `FutureSpotPairStrategy`; a custom strategy must still
  implement the root `scripts.strategy_api` contract.
- The Numba engine is the optimized default. Keep the Python engine as the
  behavioral reference and custom-strategy fallback.
- Dates execute sequentially when carry is enabled; pairs within each date may
  execute in worker processes.
- Carry held contracts into the next trading day's universe. Never roll an old
  futures position into a new front month implicitly.
- A residual position on expiry must be explicit in carry/report output and
  treated according to the configured expiry policy.
- Futures are the default first leg. Re-check the second-leg condition after the
  first fill and apply the configured flatten action when it fails.
- Latency is independent per leg: feed offset, order entry, and response. Shared
  latency flags are fallbacks only.
- `--post-first-feed-wait spot` means the second-leg decision waits until the
  spot feed advances after the first future response. A timeout records
  `POST_FIRST_FEED_TIMEOUT` and triggers configured first-leg risk handling.
- The shared-capital replay must operate on saved fill candidates in timestamp
  order. Report candidate, accepted, and rejected entries separately.
- `--leverage` applies configured futures margin and spot own-funds ratios;
  `--no-leverage` charges 100% capital to both legs. Never describe one mode as
  the other.

Default strategy parameters come from
`future_spot/arbitrage_config_base.json`; command-line overrides take
precedence. Do not place credentials in this file. Optional live/provider SDKs
are intentionally absent from `requirements.txt`.

## Exclusions, errors, and reporting

- Known incomplete-tick dates and expiry-residual run keys are explicit defaults
  in `future_spot/arbitrage/full_market_runner.py`. An exclusion must affect
  config building, conversion, HBT, reports, plots, and capital replay
  consistently.
- Preserve an audit of every exclusion. Do not add a new exclusion simply to
  improve reported PnL.
- Always inspect `run_errors.csv`; a generated chart does not imply a clean run.
- Realized portfolio ROI is capital-constrained realized PnL divided by starting
  own capital. Do not include open-position marks unless the metric is clearly
  labeled as including open positions.
- Profit is recognized on matched exit dates. State the effective plotted date
  range when it differs from the requested backtest range.
- Report discarded/residual open lots alongside performance metrics.
- `backtest_manifest.json` fingerprints result-defining arguments, daily
  configs, event-file stats, and strategy/HBT sources. Keep cache validation
  conservative when adding result-affecting inputs.
- Low-memory summary reports read persisted CSVs in bounded chunks. Do not
  reintroduce multi-GB in-memory concatenation into the default report path.
- `future_spot/output/` is generated and git-ignored. When a result must appear
  in repository documentation, copy only the selected final artifact into a
  tracked documentation asset path and record its run assumptions.

## Output expectations

- Strategy and report tables should be DataFrames.
- Notebook cells should display compact summary DataFrames and retain detailed
  frames under separate variables.
- A complete full-market output includes summary, trade, entry/exit, market,
  latency, carry, settings, conversion, error, and manifest artifacts.
- Detailed reports default to Parquet; use CSV only when explicitly required.
- `--report-mode summary` is the bounded default. `full` is for diagnostic
  tables, not routine runs.

## Change workflow

1. Read the nearest implementation, tests, and manifest/report consumer before
   editing a result-defining path.
2. Keep changes inside the correct architectural layer.
3. Add or update focused tests for conversion, execution, position carry,
   capital replay, report streaming, cache invalidation, or exclusions as
   applicable.
4. Preserve unrelated user changes and generated outputs.
5. Run focused tests first, then compile every retained Python entrypoint.
6. Inspect `git diff --check` and the final diff for accidental output files,
   secrets, or undocumented changes to assumptions.

Validation baseline:

```bash
python3 -m pytest -q tests future_spot/test
python3 -m py_compile scripts/*.py future_spot/arbitrage/*.py future_spot/scripts/*.py future_spot/test/*.py
git diff --check
```

For `0050` conversion sanity checks:

```python
data = np.load(DATA_FILE)["data"]
unique_px = np.unique(data["px"][np.isfinite(data["px"]) & (data["px"] > 0)])
print(unique_px[:20])
```

If the prices are only `[77., 78.]` for a `0.05`-tick window, fix the conversion
path or stale import before changing strategy or depth logic.
