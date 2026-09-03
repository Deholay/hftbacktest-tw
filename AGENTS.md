# Agent Guide

## Mission

This repository supports Taiwan market-microstructure research with
`hftbacktest`. It has three related surfaces:

1. Root stock/ETF/odd-lot/futures converters and notebook experiments.
2. The `future_spot` full-market stock-future/spot arbitrage backtest, capital
   replay, and reporting pipeline.
3. The project-owned `hftbacktest_slim` BBO replay runtime and compact-data
   package shared by multiple strategy families.

The active performance direction is defined in
`HBT_ACCELERATION_STRATEGY.md`. Read that document before changing data
loading, compact-cache formats, matching behavior, multiprocessing, carry,
result persistence, or benchmark claims.

The completed extraction of reusable slim-engine components into
`hftbacktest_slim/` is governed by `HFTBACKTEST_SLIM_MIGRATION.md`. Read it
before moving, adding, or importing slim runtime, native matching, or compact
BBO code. For package location, ownership, and dependency direction, the
migration document supersedes historical locations described in the
acceleration strategy. The acceleration strategy remains authoritative for
market-data, scheduler, matching, latency, cache, parity, and benchmark
semantics.

Preserve reproducibility and market-data semantics. Never improve speed or make
a backtest look cleaner by silently dropping dates, pairs, errors, open
positions, capital constraints, latency, or audit output.

## Decision priorities

When goals conflict, use this order:

1. Market-data and execution correctness.
2. Reproducibility, conservative cache validation, and auditability.
3. Carry, capital, exclusion, and reporting consistency.
4. Bounded disk, memory, and file-descriptor usage for annual runs.
5. Runtime and throughput.

Performance estimates in the strategy document are targets, not measured
results. Update them only from reproducible benchmarks that disclose data range,
pair count, cache state, engine, semantic mode, output mode, hardware, wall
time, peak RSS, and disk growth.

## Architecture boundaries

- Root `scripts/` is the cross-strategy Python library layer.
- `hftbacktest_slim/` is the self-contained, cross-strategy slim runtime. It
  owns the native Rust scheduler/matcher, Python binding, slim-native types and
  public API, compact BBO schema, normalization, readers, cache builder,
  manifests, validation, and slim-specific command-line tools.
- New slim runtime code must not be added to root `scripts/`, root `crates/`,
  or a strategy package. The Phase 6 migration removed the transitional slim
  wrappers and old native location; do not recreate them.
- `hftbacktest_slim` must not import `future_spot`, another strategy package,
  `scripts.hbt_*`, `scripts.tw_stock_*`, or the installed `hftbacktest`
  package. Enforce this with an automated dependency-boundary test.
- Strategy packages may import `hftbacktest_slim`; the dependency must never
  point from `hftbacktest_slim` back into a strategy package.
- `scripts/strategy_api.py` owns project-wide strategy contracts:
  `StrategyContext`, `StrategyDecision`, `Strategy`, `StrategyRunner`,
  and the lightweight registry.
- `scripts/hbt_types.py`, `scripts/hbt_common.py`, and
  `scripts/io_utils.py` own strategy-neutral HBT types, execution helpers,
  and I/O helpers.
- Reference-HBT helpers and reference compact-to-event adapters remain outside
  `hftbacktest_slim`. Reusable slim compact-cache schema, manifest,
  normalization, validation, reader, and publication code belongs in
  `hftbacktest_slim`. Strategy-specific universe construction and source
  resolution remain in each strategy package.
- Strategy folders own domain models, pricing, risk, execution, config parsing,
  output schemas, and adapters.
- `future_spot` implements the root strategy interface in
  `future_spot/arbitrage/strategy_adapter.py`.
- Never make a new strategy family import `future_spot` for reusable
  behavior. Promote clean slim-runtime behavior to `hftbacktest_slim` and
  other domain-neutral behavior to root `scripts/` first.
- New strategies must use the neutral `hftbacktest_slim` public API. Removed
  HBT-shaped compatibility imports are not an extension surface.
- Keep notebooks thin. Reusable conversion, execution, analytics, and plotting
  logic belongs in Python modules.
- Keep `future_spot/scripts/` as thin CLI entrypoints. Business logic belongs
  in `future_spot/arbitrage/`.

## Engine policy

Maintain two explicit execution paths:

- **Reference engine:** installed HftBacktest plus the existing event converter,
  Python implementation, and Numba scanner. It is the regression oracle,
  queue/passive-order research engine, and custom-strategy fallback.
- **Slim engine:** the project-owned, cross-strategy matching runtime in
  `hftbacktest_slim/` for constrained BBO, immediate FOK/IOC,
  no-partial-fill paths. Strategy-specific decisions and portfolio behavior
  remain outside the runtime.

Do not replace the reference engine or make the slim engine the default until
the parity gates in `HBT_ACCELERATION_STRATEGY.md` pass. Every run manifest
must identify engine, implementation version, compact schema version, strategy
clock, time-in-force semantics, and result-affecting configuration.

Deleting apparently unused HBT code is not an acceleration strategy. Extract
only the semantics required by the supported slim mode, while retaining HBT for
behavioral comparison and unsupported modes.

## Project map

### Strategy and root library

- `HBT_ACCELERATION_STRATEGY.md`: authoritative acceleration architecture,
  hazards, phases, benchmarks, and acceptance gates.
- `HFTBACKTEST_SLIM_MIGRATION.md`: authoritative package-boundary, dependency,
  relocation, compatibility, versioning, and migration acceptance plan for
  `hftbacktest_slim`.
- `hftbacktest_slim/`: current home for the shared Python/Rust slim runtime,
  compact BBO data layer, and stable backend-facing API.
- `scripts/tw_stock_data_to_npz.py`: source rows to HftBacktest event arrays
  or `.npz` files for the reference path.
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
- `notebooks/hbt_pair_backtest_visualization.ipynb`: thin futures/spot report
  runner that uses the installed neutral slim package and delegates strategy
  behavior to `future_spot`.

### Futures/spot implementation

- `future_spot/test/run_full_backtest.py`: supported one-command HBT + reports
  + PNG entrypoint.
- `future_spot/arbitrage/full_market_runner.py`: CLI definition and
  compatibility implementation.
- `future_spot/arbitrage/daily_pipeline.py`: date selection, daily universe,
  config, and path resolution.
- `future_spot/arbitrage/event_data.py`: source resolution, per-symbol event
  conversion, and reuse audit.
- `future_spot/arbitrage/hbt_pipeline.py`: pair configs, execution, cache, and
  result loading.
- `future_spot/arbitrage/hbt_backtest.py`: pair-level HBT and latency capture.
- `future_spot/arbitrage/hbt_numba.py`: optimized default reference scanner.
- `future_spot/arbitrage/position_carry.py`: cross-date position restoration
  and expiry handling.
- `future_spot/arbitrage/capital.py`: shared-capital candidate replay.
- `future_spot/arbitrage/reporting.py` and
  `future_spot/test/report_tables.py`: persisted analytical tables.
- `future_spot/test/report_plots.py`: saved PNG reports.
- `future_spot/arbitrage/backtest_report.py` and
  `future_spot/arbitrage/result_replot.py`: filtered/durable report
  generation and result replotting.

## Non-negotiable data rules

### Data platform

Use `data_platform_client/data_stock/api` for stock conversion. Do not switch
back to `data_platform/data_stock/api`; that older path has cast all columns
to `Int64` and truncated decimal ETF prices.

For `0050` with `tick_size = 0.05`, healthy converted prices include
`77.90`, `77.95`, `78.00`, and `78.05`. Output containing only `77`
and `78` indicates a wrong or stale conversion import. Restart the notebook
kernel or reload `scripts.tw_stock_data_to_npz` before diagnosing downstream
behavior.

The stock API's symbol-partitioned history/update data and a daily all-symbol
Parquet are different materializations. Prove row, dtype, timestamp, duplicate,
symbol, and price parity before making the daily file canonical. A faster
source is not acceptable if its contents differ without an explicit,
documented semantic migration.

### Reference HftBacktest input

HftBacktest accepts an `.npz` containing key `data` or an in-memory NumPy
event array with this dtype:

```text
ev, exch_ts, local_ts, px, qty, order_id, ival, fval
```

It does not accept raw provider rows. Each Top-5 source row is converted by
clearing the visible range, inserting aggregated snapshot levels, and
optionally inferring trades from `total_volume` deltas. Source trade side is
not explicit and is inferred from the previous BBO by default.

### Top-5 and BBO semantics

- The source is market-by-price, not market-by-order.
- Aggregate repeated prices across Top-5 fields before selecting BBO or
  emitting depth snapshots.
- Remove invalid price/quantity levels and sort valid levels before choosing
  best bid and ask. Raw field position is not guaranteed to be price priority.
- Individual same-price orders and exact queue position are unrecoverable.
- Queue position must use an explicit HftBacktest queue model.
- `level=5` means the fifth distinct non-empty price, never the fifth order at
  one price.
- ETF and odd-lot feeds have Top-5 prices but no Top-5 quantities. Their
  `price_only_depth_qty=1.0` default is suitable for structural replay, not
  queue-size inference.
- A compact BBO cache is valid only for modes whose behavior depends on BBO,
  cumulative volume/trade inference, timestamps, and declared latency fields.
  Depth/queue modes require a separate lossless Top-5 schema or the reference
  path.

### Tick-index depth API

`HashMapMarketDepth` stores quantity by integer tick:

```python
price = 77.10
tick = round(price / tick_size)
qty = depth.ask_qty_at_tick(tick)
```

`depth.best_ask` is a float price, `depth.best_ask_tick` is an integer, and
`ask_qty_at_tick`/`bid_qty_at_tick` require the integer. Never pass a float
price to a quantity-at-tick method.

## Compact-cache contract

The first supported physical format is Arrow IPC File / Feather V2 with LZ4
compression, partitioned by trading date, source, and symbol. Use versioned
schemas. The initial BBO schema contains:

```text
source_seq, exch_ts, local_ts_raw,
bid_px, ask_px, bid_qty, ask_qty,
last_px, total_volume
```

Keep any corrected local timestamp, latency adjustment, and exchange/local
ordering sidecars needed to reproduce current behavior. `source_seq` is
mandatory for deterministic stable ordering.

### One-scan invariant

For a successful cold build of one trading date:

1. Scan the required stock daily source at most once.
2. Scan the required futures daily source at most once.
3. Project only required columns and stream Arrow record batches.
4. Route rows to every required symbol partition during those scans.
5. Do not call whole-day `.collect()` or materialize the complete day in RAM.
6. Do not perform a raw-source scan inside a symbol or pair loop.
7. Workers consume compact partitions only; they do not reopen raw daily
   Parquet.

Metrics, fingerprints, and validation must be derived during that pass or from
the completed compact files. Never hide a second raw scan as a checksum,
row-count, validation, or manifest step.

### Timestamp and order preservation

Current negative-latency correction is calculated per symbol. A daily scan must
maintain independent state per symbol and produce the same correction as
standalone symbol conversion. Preserve independent stable exchange-time and
local-time orderings when the selected engine requires them; do not replace
them with one global sort.

For equal timestamps, document and test the exact priority. The current HBT
reference ordering is:

```text
(asset_no, LocalData=0, LocalOrder=1, ExchData=2, ExchOrder=3)
```

### Publication and validation

- A compact date is reusable only if a versioned manifest validates source
  identity, source size/mtime or stronger fingerprint, schema version,
  converter version, projected fields, symbol universe, row counts, timestamp
  policy, aggregation policy, trade-inference policy, and completion state.
- Write batches to a temporary directory on the same filesystem, close and
  validate every file, write the manifest last, then atomically rename the
  directory.
- Interrupted or incomplete dates are never cache hits.
- Cache invalidation is conservative: unknown or missing result-defining
  metadata means rebuild.

### Disk and memory safety

Feather is a cache, not the only copy of research data. Before a build, estimate
worst-case space from source rows. Until measured otherwise, use:

```text
projected_bytes = source_rows * 96 * 1.20
```

Enforce both a configurable cache-size cap and a minimum-free-space reserve;
initial defaults are 200 GB each. Recheck free space after every batch and stop
cleanly before crossing a limit. Remove only the incomplete temporary
partition. Never automatically delete completed cache, raw data, or backtest
results.

Use bounded batch buffers and bounded open writers. Annual processing must not
require a year of raw rows, compact rows, HBT events, or result frames in RAM.

## Matching, time-in-force, and latency

- The current strategy decision clock is `step_ms` (default 1,000 ms), not
  every feed event. Preserve it unless a named semantic mode explicitly
  changes it.
- Continue processing feeds, order responses, waits, and end-of-data behavior
  according to the selected reference semantics.
- Latency is independent per leg: feed offset, order entry, and response.
  Shared latency flags are fallbacks only.
- Time in force must be strict. Never silently map unsupported FOK or IOC to
  GTC. Fail configuration or import the intended constant explicitly.
- Maintain separate regression baselines for legacy GTC-fallback results and
  intended FOK/IOC results. Do not claim parity across those two semantic
  modes.
- The slim engine initially supports the documented immediate, crossing,
  no-partial-fill BBO behavior only. Non-crossing FOK/IOC orders expire.
  Passive/GTC/GTX, queue, partial-fill, depth-sensitive, and arbitrary custom
  strategy modes remain on the reference engine until separately implemented
  and validated.

## Futures/spot behavior to preserve

- The default adapter is `FutureSpotPairStrategy`; a custom strategy must
  implement the root `scripts.strategy_api` contract.
- The Numba engine is the optimized default reference implementation. Keep the
  Python engine as behavioral reference and custom-strategy fallback.
- There may be many pairs per date; each pair currently has two legs, spot and
  future. Never collapse the universe to one pair or assume symbols are reused
  between pairs.
- Dates execute sequentially when carry is enabled. Pairs within one date may
  run in worker processes.
- Reuse a persistent worker pool across dates when practical, but enforce a
  date barrier: finish the date, update carry, persist results, and only then
  submit the next date.
- Do not count same-day symbol reuse as a speedup unless the actual universe
  proves it.
- Carry held contracts into the next trading day's universe. Never roll an old
  futures position into a new front month implicitly.
- A residual position on expiry must be explicit in carry/report output and
  treated according to the configured expiry policy.
- Futures are the default first leg. Re-check the second-leg condition after
  the first fill and apply the configured flatten action when it fails.
- `--post-first-feed-wait spot` means the second-leg decision waits until the
  spot feed advances after the first future response. A timeout records
  `POST_FIRST_FEED_TIMEOUT` and triggers configured first-leg risk handling.
- Shared-capital replay operates on saved fill candidates in timestamp order.
  Report candidate, accepted, and rejected entries separately.
- `--leverage` applies configured futures margin and spot own-funds ratios;
  `--no-leverage` charges 100% capital to both legs. Never describe one mode
  as the other.

Default strategy parameters come from
`future_spot/arbitrage_config_base.json`; command-line overrides take
precedence. Do not place credentials in this file. Optional live/provider SDKs
are intentionally absent from `requirements.txt`.

## Annual result and restart rules

- Persist each completed date before advancing. Date-partition summary, trades,
  market, latency, and entry/exit tables as Parquet.
- Build entry/exit output and update carry before releasing that date's detail
  frames.
- Retain only bounded cross-date state: carry, manifest state, aggregate
  counters, and data required for capital replay.
- Do not retain annual `all_pair_results` or lists of daily DataFrames for a
  final concatenation.
- Generate compatibility CSVs and reports by streaming persisted partitions in
  bounded chunks.
- `--report-mode summary` is the bounded default. `full` is diagnostic and
  must have an explicit resource budget.
- Resume only from a verified contiguous manifest prefix. The next date must
  receive the exact persisted carry state and result-defining configuration
  produced by the previous date. Never skip a failed or missing middle date.

## Exclusions, errors, capital, and reporting

- Known incomplete-tick dates and expiry-residual run keys are explicit
  defaults in `future_spot/arbitrage/full_market_runner.py`. An exclusion
  affects config building, conversion, matching, reports, plots, and capital
  replay consistently.
- Preserve an audit of every exclusion. Do not add exclusions simply to improve
  PnL or runtime.
- Always inspect `run_errors.csv`; a generated chart does not imply a clean
  run.
- Realized portfolio ROI is capital-constrained realized PnL divided by
  starting own capital. Do not include open-position marks unless the metric is
  clearly labeled as including open positions.
- Profit is recognized on matched exit dates. State the effective plotted date
  range when it differs from the requested range.
- Report discarded and residual open lots alongside performance metrics.
- `backtest_manifest.json` fingerprints result-defining arguments, daily
  configs, data/cache stats, engine and schema versions, and strategy/matching
  sources.
- `future_spot/output/` is generated and git-ignored. Copy only selected final
  artifacts into tracked documentation and record their run assumptions.

## Output expectations

- Strategy and report tables should be DataFrames at Python boundaries.
- Notebook cells display compact summaries and retain details separately.
- A complete full-market output includes summary, trade, entry/exit, market,
  latency, carry, settings, conversion, error, and manifest artifacts.
- Detailed reports default to Parquet; use CSV only when explicitly required.
- Compact-cache build reports include per-date/source input rows, output rows,
  bytes, elapsed time, cache hit/miss reason, and scan count.
- Benchmarks include cold build, warm reuse, matching, persistence, reporting,
  total wall time, peak RSS, and cache/result disk usage.

## Change workflow

1. Read the nearest implementation, tests, manifest consumer, carry consumer,
   and report consumer before editing a result-defining path.
2. Read `HBT_ACCELERATION_STRATEGY.md` and identify the affected phase,
   semantic baseline, and acceptance gate.
3. For any slim package API, native matcher, compact BBO, or package-boundary
   change, also read `HFTBACKTEST_SLIM_MIGRATION.md` and identify
   the migration phase and dependency boundary affected.
4. Keep changes inside the correct architectural layer. Do not combine a file
   relocation with an unversioned semantic change.
5. Label whether a change is performance-only, relocation-only, or semantic.
   Semantic changes require a new manifest identity and separate baseline.
6. Add focused tests for affected behavior. Depending on scope, cover daily vs
   symbol-source parity, Top-5 aggregation, single-scan enforcement,
   per-symbol timestamp correction, equal-time priority, FOK/IOC behavior,
   latency, carry, capital, disk interruption, cache invalidation, daily
   persistence, and restart parity.
7. For `hftbacktest_slim` changes, run package-isolation and external-import
   smoke tests in addition to behavioral parity tests.
8. Run focused tests first. Benchmark only after correctness gates pass.
9. Preserve unrelated user changes and generated outputs.
10. Compile every retained Python entrypoint, the `hftbacktest_slim` package
    once introduced, and any native workspace.
11. Inspect `git diff --check`, the final diff, generated files, manifests,
   secrets, and `run_errors.csv` where a run was executed.

Python validation baseline:

```bash
python3 -m pytest -q tests future_spot/test hftbacktest_slim/tests
python3 -m compileall -q scripts future_spot hftbacktest_slim/src/hftbacktest_slim
git diff --check
```

Rust validation:

```bash
cargo test --workspace
cargo fmt --check --all
cargo clippy --workspace --all-targets -- -D warnings
```

For `0050` conversion sanity:

```python
data = np.load(DATA_FILE)["data"]
unique_px = np.unique(data["px"][np.isfinite(data["px"]) & (data["px"] > 0)])
print(unique_px[:20])
```

If prices are only `[77., 78.]` for a `0.05`-tick window, fix the conversion
path or stale import before changing strategy or matching logic.

## Acceleration definition of done

The acceleration program is complete only when:

1. Daily-vs-symbol source parity is proven; if it fails, the authoritative
   daily materialization is repaired upstream and the parity gate is repeated.
2. One successful cold date performs no more than one stock and one futures raw
   daily scan.
3. Compact builds are bounded, atomic, resumable, and protected by disk limits.
4. Reference parity passes for the declared semantic mode and mismatches are
   zero or explicitly approved.
5. Multi-pair execution uses persistent workers without violating sequential
   date carry.
6. Annual runs persist and release daily details instead of accumulating them
   in RAM.
7. Interrupted-run restart matches an uninterrupted run.
8. Cold, warm, annual, disk, and RSS targets are measured and published with
   complete benchmark metadata.
