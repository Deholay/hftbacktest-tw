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

Notebook report:

```text
future_spot/notebooks/hbt_pair_backtest_visualization.ipynb
```

The notebook is intentionally a thin summary report. It reads the generated CSV
outputs and shows:

- entry / exit rules;
- estimated profit by symbol and by pair;
- latency summaries for local / spot exchange / future exchange timelines;
- daily profit and execution charts;
- selected pair entry / execution drill-down.

## Run Command

Run from the Poetry project under `data_platform_client`:

```bash
cd /home/zoufuc/hftbacktest/data_platform_client

poetry run pip install 'numpy>=2.0,<2.3' pandas pyarrow /home/zoufuc/hftbacktest/hftbacktest/py-hftbacktest

poetry run python /home/zoufuc/hftbacktest/future_spot/scripts/run_hbt_daily_full_market_backtest.py \
  --start-date 2026-05-21 \
  --end-date 2026-05-26 \
  --futures-parquet-template '/mnt/z/ticks_parquet_stock_future/{ldate}.parquet' \
  --twse-daytrade-template '/mnt/z/TWSE/每日個股狀況/{date_nodash}.csv' \
  --tpex-daytrade-template '/mnt/z/TPEX/每日個股狀況/{date_nodash}.csv' \
  --twse-daily-template '/mnt/z/TWSE/每日資料/{ldate_nodash}.ftr' \
  --tpex-daily-template '/mnt/z/TPEX/每日資料/{ldate_nodash}.ftr' \
  --spot-input-csv-template '/mnt/z/FubunData/tick_csv/twstock_{date_nodash}.csv' \
  --data-platform-base '/mnt/z/數據平台' \
  --event-futures-parquet-dir '/mnt/z/ticks_parquet_stock_future'
```

For latency observation, add non-zero order latency:

```bash
  --order-latency-ms 5 \
  --response-latency-ms 5
```

The `/mnt/z/...` paths are needed when running from Linux/WSL. Windows-style
paths such as `Z:\ticks_parquet_stock_future\...` are treated as literal file
names by Linux Python and will not open correctly.

Spot tick conversion currently uses daily CSV files such as:

```text
/mnt/z/FubunData/tick_csv/twstock_20260521.csv
```

This avoids the temporarily unavailable `data_platform_client` DataAPI path. If
`--spot-input-csv-template` is set to an empty string, the runner falls back to
the legacy DataAPI conversion path.

If a local `(venv)` raises this error:

```text
Numba needs NumPy 2.4 or less. Got NumPy 2.5.
```

pin NumPy below the local `hftbacktest` package limit:

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

- `scripts/run_hbt_daily_full_market_backtest.py`: daily full-market HBT runner,
  CSV output writer, cached result loader, latency output, and cash / ROI helper
  functions used by the notebook.
- `scripts/build_arbitrage_config_from_date.py`: builds one arbitrage config per
  trade date. The full-market runner calls this before converting/running pairs.
- `arbitrage_config_base.json`: non-secret template config used by the daily
  config builder. Generated daily configs are written under the output
  directory.
- `arbitrage/hbt_backtest.py`: pair-level HBT strategy simulation and latency
  event capture.
- `arbitrage/config.py`, `arbitrage/models.py`, `arbitrage/strategy.py`,
  `arbitrage/ticks.py`, `arbitrage/utils.py`: shared config parsing, data
  models, strategy calculations, tick handling, and utility helpers.
- `arbitrage/providers.py`: provider adapters used by config construction and
  the older replay/live stack.

Generated files that are safe to remove:

- `future_spot/**/__pycache__/`
- `future_spot/**/*.pyc`

Generated outputs are under `future_spot/output/`. Keep or remove them based on
whether the corresponding CSV reports are still needed; they are not source
code.

## Cleanup Candidates

These files are not part of the current notebook / full-market HBT path. Do not
delete them blindly if older replay, live monitoring, or manual debug workflows
are still in use.

Likely legacy config flow:

- `scripts/build_targets_and_config.py`
- `scripts/generate_arbitrage_config.py`

The current daily flow builds configs through
`scripts/build_arbitrage_config_from_date.py`, one file per trade date.

Standalone utilities:

- `scripts/run_hbt_pair_backtest.py`: single-pair HBT debug runner.
- `scripts/build_futures_daily_ohlcv.py`: futures OHLCV utility.
- `scripts/monitor_exit_future_ask.py`: live/manual exit monitor.
- `scripts/monitor_margin_equity.py`: margin/equity monitor.

Older replay/live stack:

- `arbitrage/app.py`
- `scripts/run_multi_day_backtest.py`
- `scripts/run_multi_config_backtest.py`

Potential dead helpers found by static scan:

- `arbitrage/utils.py`: `parse_stock_quote`, `parse_future_quote`,
  `tw_price_tick_size`.
- `arbitrage/config.py`: `parse_non_negative_int`.

Static scan can produce false positives for CLI entry points and manual tools,
so cleanup should start with generated files, then legacy config scripts, and
only then the older replay/live stack after confirming it is no longer used.

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
