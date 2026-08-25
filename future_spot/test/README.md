# Futures/Spot Backtest Test Runner

This folder contains the thin notebook and the reusable scripts behind it:

- `backtest_config.py`: notebook defaults and path normalization.
- `backtest_pipeline.py`: daily configs, event conversion, HBT, and core CSVs.
- `report_tables.py`: profit, failure, cash/ROI, and latency CSV reports.
- `report_plots.py`: five PNG report figures.
- `run_full_backtest.py`: one-command entrypoint for the complete workflow.
- `hbt_pair_backtest_visualization.ipynb`: step-by-step interactive runner.

Run the whole workflow from the repository root:

```bash
python future_spot/test/run_full_backtest.py \
  --start-date 2026-05-21 \
  --end-date 2026-05-26 \
  --output-dir output/hbt_daily_full_market_20260521_20260526
```

The main backtest CSVs are written directly under the output directory. Extra
analysis CSVs are saved under `reports/`, large detailed tables default to
Parquet, and charts are saved under `figures/`.

Fast defaults:

- all sessions end at `13:25:00`;
- six pair worker processes are used when enough pairs are available;
- periodic market state is sampled every 60 seconds while every non-HOLD
  signal row is retained;
- existing event NPZ files bypass daily CSV splitting;
- a parameter/input/code manifest validates cached HBT results before reuse.
- final positions are restored on the next trade day by default; held contracts
  stay in the universe until exit, with no automatic futures rollover.

Useful controls:

```bash
# Serial, small debugging run.
python future_spot/test/run_full_backtest.py --workers 1 --max-pairs 5

# Keep summary CSVs and figures, but skip large detailed tables and per-pair CSVs.
python future_spot/test/run_full_backtest.py \
  --skip-detailed-reports \
  --skip-entry-exit-by-pair

# Force a valid-cache bypass after intentional strategy/config changes.
python future_spot/test/run_full_backtest.py --rebuild-hbt-results

# Legacy independent pair/day HBT with no position carry.
python future_spot/test/run_full_backtest.py --no-carry-positions

# Disable futures/spot leverage in the capital replay (100% capital per leg).
python future_spot/test/run_full_backtest.py --no-leverage
```
