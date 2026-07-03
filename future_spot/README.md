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
