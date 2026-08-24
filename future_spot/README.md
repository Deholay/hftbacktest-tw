# Future/Spot HBT Summary

This folder contains Taiwan stock-future / spot arbitrage experiments and HBT
full-market reports.

## Main Outputs

Latest full-market run:

```text
future_spot/output/hbt_daily_full_market_20260521_20260526/
```

Key files:

- `summary_all_daily_pairs.csv`: one row per daily pair, including realized PnL.
- `trades_all_daily_pairs.csv`: execution rows.
- `entry_exit_all_daily_pairs.csv`: compact entry signal and execution rows.
- `entry_exit_index.csv`: per-pair entry/exit row counts.
- `market_all_daily_pairs.csv`: sampled market state and signal hits.
- `latency_all_daily_pairs.csv`: long-format local / spot exchange / future exchange latency events.
- `run_errors.csv`: run errors; should be empty for a clean run.

Notebook/test runner:

```text
future_spot/test/hbt_pair_backtest_visualization.ipynb
future_spot/test/run_full_backtest.py
../notebooks/hbt_strategy_interface_example.ipynb
```

The visualization notebook is intentionally a thin runner. Its configuration,
pipeline, report-table, and plotting functions are split into Python modules in
`future_spot/test/`. Both the notebook and `run_full_backtest.py` save:

- entry / exit rules;
- estimated profit by symbol and by pair;
- latency summaries for local / spot exchange / future exchange timelines;
- daily profit and execution charts;
- selected pair entry / execution drill-down;
- report CSVs under the selected output directory's `reports/` folder;
- five PNG charts under its `figures/` folder.

The root strategy interface example notebook demonstrates the
`scripts.strategy_api` contract plus the futures/spot adapter in
`arbitrage/strategy_adapter.py`. Keep cross-strategy examples in root
`notebooks/`; keep futures/spot-specific test/report runners in `future_spot/test/`.

## Run Command

The complete workflow, including report CSV and PNG generation, is:

```bash
python future_spot/test/run_full_backtest.py --start-date 2026-05-21 --end-date 2026-05-26
```

All options accepted by the existing full-market runner are accepted by this
entrypoint. The lower-level core-only command remains available below.

The optimized defaults use a `13:25:00` session end, six pair worker processes,
one periodic market sample per 60 strategy steps, and the Numba strategy scanner.
The compiled scanner advances HBT, reads both BBOs, calculates pair pricing, and
skips HOLD-only spans without constructing Python objects on every step. It
returns to Python for signals, periodic market samples, risk checks, execution,
and report rows. Every non-HOLD signal row is still retained. Backtest reuse is protected by `backtest_manifest.json`,
which fingerprints result-affecting arguments, daily configs, event-file stats,
and strategy/HBT implementation files.

Use `--strategy-engine python` as the reference/fallback path. Numba currently
supports only the default future/spot strategy; a custom `Strategy` must use the
Python engine. The first pair handled by each worker includes JIT compilation;
later pairs in the same worker reuse the compiled function.

For a lightweight run that keeps summary CSVs and PNG figures:

```bash
python future_spot/test/run_full_backtest.py \
  --skip-detailed-reports \
  --skip-entry-exit-by-pair
```

Large detailed report tables use Parquet by default. Use
`--detailed-report-format csv` only when CSV detail is required. Use
`--workers 1` for serial debugging and `--rebuild-hbt-results` to explicitly
bypass a valid cache.

Run from the Poetry project under `data_platform_client`:

```bash
cd /home/zoufuc/hftbacktest/data_platform_client

poetry run pip install -r /home/zoufuc/hftbacktest/requirements.txt

poetry run python /home/zoufuc/hftbacktest/future_spot/scripts/run_hbt_daily_full_market_backtest.py \
  --start-date 2026-05-21 \
  --end-date 2026-05-26 \
  --futures-parquet-template '/mnt/z/ticks_parquet_stock_future/{ldate}.parquet' \
  --twse-daytrade-template '/mnt/z/TWSE/每日個股狀況/{date_nodash}.csv' \
  --tpex-daytrade-template '/mnt/z/TPEX/每日個股狀況/{date_nodash}.csv' \
  --twse-daily-template '/mnt/z/TWSE/每日資料/{ldate_nodash}.ftr' \
  --tpex-daily-template '/mnt/z/TPEX/每日資料/{ldate_nodash}.ftr' \
  --data-platform-base '/mnt/z/數據平台' \
  --event-futures-parquet-dir '/mnt/z/ticks_parquet_stock_future' \
  --npz-compression uncompressed
```

`uncompressed` trades larger event files for faster conversion writes and
faster repeated HftBacktest loads. Omit the option to keep smaller compressed
NPZ files.

For latency observation, add non-zero order latency:

```bash
  --order-latency-ms 5 \
  --response-latency-ms 5
```

To force the second-leg decision to wait for a fresh post-first-leg feed, add:

```bash
  --post-first-feed-wait spot \
  --post-first-feed-timeout-ms 5000 \
  --rebuild-hbt-results
```

`spot` means the runner waits until the spot feed timestamp advances after the
first future order response. If no fresh spot feed arrives before the timeout,
the runner records `POST_FIRST_FEED_TIMEOUT` and attempts to flatten the first
leg.

The `/mnt/z/...` paths are needed when running from Linux/WSL. Windows-style
paths such as `Z:\ticks_parquet_stock_future\...` are treated as literal file
names by Linux Python and will not open correctly.

Spot tick conversion defaults to the `data_platform_client` parquet store at
`/mnt/z/數據平台`. The converter gets a per-symbol Polars LazyFrame, applies the
session filter before collection, and sends column arrays to the Numba event
builder without materializing Python dictionaries.

Daily CSV remains available as a fallback. To use a file such as:

```text
/mnt/z/FubunData/tick_csv/twstock_20260521.csv
```

pass this option:

```bash
  --spot-input-csv-template '/mnt/z/FubunData/tick_csv/twstock_{date_nodash}.csv'
```

The runner then splits each daily spot CSV into per-symbol files under:

```text
future_spot/output/.../spot_tick_csv_by_symbol/YYYYMMDD/
```

Then each per-symbol CSV is converted to HBT `.npz`. Omitting
`--spot-input-csv-template` keeps the faster DataAPI/Numba default.

If a local `(venv)` raises this error:

```text
Numba needs NumPy 2.4 or less. Got NumPy 2.5.
```

pin NumPy below the `hftbacktest` package limit:

```bash
python -m pip install --upgrade 'numpy>=2.0,<2.3'
```

The Poetry environment used above is expected to work with `numpy 2.2.x`.

## Strategy Rules

Entry signal:

- Long spot / short future when
  `long_spot_short_future_pct >= entry_threshold_pct`.
- The effective tick edge must satisfy
  `long_spot_short_future_ticks > min_effective_tick_multiple`.
- Spot ask size and future bid size must satisfy each pair's minimum size rule.
- Short spot / long future is only allowed when `allow_short_spot = True`.

Execution:

- Default first leg is `future`.
- Second leg is checked again after the first leg fills.
- If second-leg profit check fails, the runner records the failure and handles
  first-leg risk according to the pair/backtest config.

Exit:

- Existing positions exit when the configured exit tick or reverse-basis rule is
  triggered.
- Final PnL is recorded in `realized_pnl`.

## Code Map

Current HBT full-market path:

- `scripts/run_hbt_daily_full_market_backtest.py`: thin CLI/backward-compatible
  facade for the daily full-market HBT workflow.
- `scripts/build_arbitrage_config_from_date.py`: builds one arbitrage config per
  trade date. The full-market runner calls this before converting/running pairs.
- `arbitrage_config_base.json`: non-secret template config used by the daily
  config builder. Generated daily configs are written under the output
  directory.
- `arbitrage/hbt_backtest.py`: pair-level HBT strategy simulation and latency
  event capture. It accepts a root-interface-compatible strategy object.
- `arbitrage/strategy_adapter.py`: futures/spot implementation of
  `scripts.strategy_api`.
- `arbitrage/daily_pipeline.py`: date selection, path resolution, daily pair
  records, and pair-universe frames.
- `arbitrage/event_data.py`: spot/future event path resolution, CSV splitting,
  event conversion, and reuse status.
- `arbitrage/hbt_pipeline.py`: HBT settings audit, pair config construction,
  pair backtest execution, and cached CSV loading.
- `arbitrage/reporting.py`: entry/exit outputs, second-leg failure reports,
  cash/ROI outputs, and CSV helpers.
- `arbitrage/hbt_types.py`, `arbitrage/hbt_helpers.py`, `arbitrage/hbt_rows.py`:
  futures/spot pair backtest config, domain-specific HBT helpers, and pair
  output row builders.
- `arbitrage/config.py`, `arbitrage/models.py`, `arbitrage/strategy.py`,
  `arbitrage/ticks.py`, `arbitrage/utils.py`: shared config parsing, data
  models, strategy calculations, tick handling, and utility helpers.
- `arbitrage/full_market_runner.py`: compatibility implementation behind the
  split pipeline facades.

`future_spot/scripts/` is for futures/spot command-line entrypoints only. If a
module is expected to be reused by another strategy family, put it in root
`scripts/` instead of this folder.

Root shared modules currently used by `future_spot`:

- `scripts/strategy_api.py`: strategy context/decision protocol.
- `scripts/hbt_types.py`: generic HBT asset/fill dataclasses.
- `scripts/hbt_common.py`: generic queue model, order, latency, and fill helpers.
- `scripts/io_utils.py`: generic CSV/DataFrame/time conversion helpers.

`Calendar.csv` and `stockinfo.csv` are source inputs for the retained config
builder. `Calendar.csv` maps trade dates to `LDate`; `stockinfo.csv` maps
futures targets to spot metadata and unit constraints.

Generated files that are safe to remove:

- `future_spot/**/__pycache__/`
- `future_spot/**/*.pyc`

Generated outputs are under `future_spot/output/`. Keep or remove them based on
whether the corresponding CSV reports are still needed; they are not source
code.

## Strategy Interface

The project-level contract is `scripts/strategy_api.py`. `future_spot` implements
that contract with `FutureSpotPairStrategy`.

Default behavior is unchanged: if no custom strategy is supplied,
`HbtPairBacktester` uses the futures/spot adapter, which evaluates the current
stop-loss-aware signal engine and risk manager.

For a custom futures/spot strategy:

```python
from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
from future_spot.arbitrage.strategy_adapter import FutureSpotPairStrategy

backtester = HbtPairBacktester(run_config, strategy=FutureSpotPairStrategy())
trades, summary = backtester.run()
```

For non-futures/spot strategy families, create a separate implementation folder
and adapt to `scripts.strategy_api` instead of importing `future_spot`.

## Latest Run Snapshot

For `2026-05-21` through `2026-05-26`:

```text
dates=4
daily_pairs=195
ready_pairs=195
completed_pairs=195
errors=0
filled_pairs=1341
realized_pnl=513338.86455
```
