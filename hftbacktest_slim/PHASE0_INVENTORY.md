# Phase 0 slim migration inventory

## Scope and baseline identity

This inventory freezes the pre-migration slim and compact-cache boundary before
implementation relocation. It describes the tree inspected on 2026-09-02,
before the Phase 1 package files were introduced. The change classification for
Phase 0 and Phase 1 is **relocation-only/package-boundary preparation**. No
market-data, scheduler, matcher, latency, order, fill, cache, carry, capital,
reporting, exclusion, or persistence behavior is changed.

The controlling documents were read in full before the baseline:

- `AGENTS.md`
- `HBT_ACCELERATION_STRATEGY.md`
- `HFTBACKTEST_SLIM_MIGRATION.md`

The working tree already contained a modified `AGENTS.md` and an untracked
`HFTBACKTEST_SLIM_MIGRATION.md`. They are user-owned inputs and were not edited
or reverted by this work.

## Current dependency direction

The legacy implementation has these dependencies today:

```text
scripts.tw_stock_data_to_npz
    ^                     ^
    |                     `-- scripts.compact_hbt_adapter (reference bridge)
scripts.compact_cache
    ^              ^
    |              `-- compact cache CLIs and tests
    `-- future_spot.full_market_runner

scripts.hbt_types.HbtAssetConfig
    ^
scripts.slim_engine (ctypes + HBT-compatible facade)
    ^
future_spot.hbt_backtest
```

The target direction is the reverse for shared slim behavior: strategies and
reference adapters may import `hftbacktest_slim`, but `hftbacktest_slim` must
not import strategy packages, reference-HBT helpers, the installed
`hftbacktest` package, `scripts.hbt_*`, or `scripts.tw_stock_*`. Phase 1 uses an
even stricter automated boundary: package source may not import any
repository-sibling package. This automatically catches a newly added strategy
package as well as the currently known `future_spot` package.

## Component and symbol ownership

| Current component | Current ownership and principal symbols | Imports and consumers | Migration classification |
| --- | --- | --- | --- |
| `crates/hbt_slim/Cargo.toml` and `crates/hbt_slim/src/lib.rs` | Rust `cdylib`/`rlib` crate `hbt_slim` version `0.2.0`. Owns `BboRow`, `BboView`, `OrderView`, internal asset/depth/pending-event state, `SlimEngine`, deterministic event selection, immediate matching, and C exports `hbt_slim_version`, `hbt_slim_create`, `hbt_slim_free`, `hbt_slim_current_timestamp`, `hbt_slim_elapse`, `hbt_slim_depth`, `hbt_slim_feed_latency`, `hbt_slim_order_latency`, `hbt_slim_submit`, `hbt_slim_wait_order_response`, and `hbt_slim_order`. ABI constants include side `1/-1`, FOK `2`, IOC `3`, NEW `1`, EXPIRED `2`, and FILLED `3`; the existing Python facade also exposes GTC `0`, GTX `1`, CANCELED `4`, and LIMIT `0`. | Loaded only through `scripts.slim_engine` at runtime; unit tests are embedded in `lib.rs`. | **move later** (Phase 2). Do not split, relocate, or alter the ABI in Phase 0/1. |
| `scripts/slim_engine.py` | Owns `SLIM_ENGINE_VERSION = "rust-0.2.0"`, `SLIM_LIBRARY`, `SLIM_ROW_DTYPE`, ctypes views/signatures, Arrow reading, `SlimOrder`, `SlimDepth`, `_Orders`, `SlimBacktest`, `SlimHbtConstants`, and `validate_slim_pair_config`. It is both the direct binding and an HBT-shaped facade. | Imports NumPy, PyArrow, and `scripts.hbt_types.HbtAssetConfig`. Imported by `future_spot/arbitrage/hbt_backtest.py`, `future_spot/arbitrage/full_market_runner.py`, and `tests/test_slim_engine.py`. | **compatibility wrapper later**. Runtime/binding code moves in Phase 3; the legacy module becomes a thin wrapper only after consumers migrate. |
| `scripts/compact_cache.py` | Owns `bbo_v1`, builder version `1`, nine-field `BBO_SCHEMA`, projected Top-5 columns, `CompactSource`, `CompactBuildConfig`, `CompactCacheStore`, cache errors, one-pass batch routing, BBO normalization calls, per-symbol timestamp correction/order sidecars, resource checks, manifests, checksums, validation, and atomic publication. | Imports `normalized_bbo_from_depth_columns` from `scripts.tw_stock_data_to_npz`. Imported by both compact CLIs, the reference adapter, validator, full-market runner, slim tests, compact tests, and pair parity fixtures. | **move later** (Phase 4). The normalization dependency must be inverted then, not during Phase 1. |
| `scripts/build_compact_cache.py` | CLI parser plus settings-universe loading, cache configuration, build invocation, wall/CPU/RSS metrics, and JSON output. | Imports `CompactBuildConfig`, `CompactCacheStore`, and `CompactSource`. | **compatibility wrapper later** (Phase 4/6) after the CLI moves under the package. |
| `scripts/benchmark_compact_read.py` | Warm Arrow read benchmark with repetitions, bytes, rows, wall/CPU time, RSS, and checksum output. | Imports `CompactBuildConfig` and `CompactCacheStore`; reads PyArrow IPC files directly. | **compatibility wrapper later** (Phase 4/6). It is not run as a Phase 0 correctness baseline. |
| `scripts/compact_hbt_adapter.py` | Owns `ADAPTER_VERSION`, `compact_to_reference_events`, and atomic `write_reference_npz_from_compact`. Reconstructs BBO-only reference events for parity and reference execution. | Imports compact schema version plus `scripts.tw_stock_data_to_npz` conversion/stat/persistence helpers. Imported by full-market runner, compact tests, pair parity tests, and `scripts/validate_compact_reference_adapter.py`. | **remain outside** as the explicit reference-only compatibility/parity bridge. In Phase 4 it may import public slim schema/version APIs, never the reverse. |
| `scripts/source_parity.py` | Owns `ParityResult`, canonical provider dtype/timestamp/sequence normalization, stable hashes, and first-mismatch reporting. | Imports private converter helpers from `scripts.tw_stock_data_to_npz`; used by `scripts/validate_daily_symbol_parity.py` and `tests/test_source_parity.py`. | **remain outside** for provider/reference orchestration. Only demonstrably provider-neutral canonicalization may move later. |
| `scripts/validate_compact_reference_adapter.py` | CLI that compares compact reconstruction with direct canonical conversion and records row counts and hashes. | Imports the cache store, reference adapter, and converter internals. | **remain outside** as a cross-engine/reference validation tool. |
| `scripts/validate_daily_symbol_parity.py` | CLI that compares daily Parquet with DataAPI history and writes per-symbol parity evidence. | Imports `scripts.source_parity` and provider/converter loaders. | **remain outside** as provider/reference orchestration. |
| `future_spot/arbitrage/hbt_backtest.py` | `HbtPairBacktester` selects reference or slim. The slim branch substitutes `SlimHbtConstants`, forces the Python strategy loop, validates strict FOK/IOC, resolves compact tick sizes, constructs legacy `HbtAssetConfig` values, and returns `SlimBacktest`. Strategy pricing, positions, second-leg behavior, fills, and output rows remain here. | Imports `SlimBacktest`, `SlimHbtConstants`, and `validate_slim_pair_config` directly from `scripts.slim_engine`. | **remain outside**. Replace only its construction/import branch with a neutral package adapter in Phase 5. |
| `future_spot/arbitrage/full_market_runner.py` | Owns CLI selection, compact orchestration, strategy universe/path resolution, error/audit/settings rows, sequential-date carry, worker scheduling, persistence, reports, and run manifests. Generic candidates are `build_compact_event_data` and `compact_asset_audit`; strategy mapping remains here. | Imports compact config/store/source/errors/schema, reference adapter, and slim engine version. Supplies raw sources, cache paths, reference NPZ reconstruction, per-leg paths, settings/audits, and result fingerprints. | **remain outside**. Move only generic cache/audit behavior in Phase 4/5; strategy orchestration and outputs remain. |
| Root `Cargo.toml` and `Cargo.lock` | Workspace resolver `2`; sole member is `crates/hbt_slim`. | All current Cargo commands build/test that legacy member. | **move later** for the member path only (Phase 2). No Phase 0/1 Cargo edit. |
| Root `README.md` | Documents explicit `--engine slim`, current supported profile, legacy implementation locations, cache location, and build command. | User-facing build/run documentation. | **remain outside** and update only when later relocation makes paths change. Phase 1 has a separate package README that explicitly describes the skeleton. |

## Native build and library paths

- Root workspace member: `crates/hbt_slim`.
- Crate types: `cdylib` and `rlib`.
- Debug Cargo products are under `target/debug/`; unit test binaries are under
  `target/debug/deps/`.
- The documented release command is `cargo build --workspace --release`.
- The Linux shared library is produced at
  `target/release/libhbt_slim.so`.
- `scripts.slim_engine.SLIM_LIBRARY` hard-codes that release `.so` by resolving
  the repository root from `scripts/slim_engine.py`.
- The binding requires `hbt_slim_version() == 1`; crate/package version `0.2.0`,
  `SLIM_ENGINE_VERSION = "rust-0.2.0"`, and ABI version `1` are separate
  identities.

No workspace member, target path, native source, library discovery rule,
engine version, or ABI version changes in Phase 0/1.

## Hard-coded fingerprint and source paths

`scripts.compact_cache.CompactCacheStore._identity` currently fingerprints:

- the exact legacy builder file through `_file_sha256(Path(__file__))`;
- `scripts/tw_stock_data_to_npz.py` through
  `Path(__file__).with_name("tw_stock_data_to_npz.py")`;
- each raw source's resolved path, byte size, modification time, and row count.

`future_spot.arbitrage.full_market_runner.hbt_manifest_payload` currently hashes
this ordered implementation list:

- `future_spot/arbitrage/hbt_backtest.py`
- `future_spot/arbitrage/hbt_numba.py`
- `future_spot/arbitrage/hbt_helpers.py`
- `future_spot/arbitrage/strategy.py`
- `future_spot/arbitrage/strategy_adapter.py`
- `future_spot/arbitrage/position_carry.py`
- `scripts/compact_cache.py`
- `scripts/compact_hbt_adapter.py`
- `scripts/slim_engine.py`
- `crates/hbt_slim/src/lib.rs`
- `future_spot/arbitrage/full_market_runner.py`

Compact-mode event/source fingerprints are resolved from runner templates.
Current defaults include the stock daily template under
`/mnt/z/數據平台/ticker_store/daily_parquet/` and futures daily files under
`/mnt/z/ticks_parquet_stock_future/`. These paths are orchestration inputs and
must not be embedded into the shared package. Replacing the hard-coded legacy
implementation source list belongs to later phases; changing it now would
invalidate existing result manifests contrary to Phase 0/1 scope.

## CLI and utility entrypoints

- `python3 scripts/build_compact_cache.py`: build/validate one date from stock
  and/or futures raw files and explicit/settings-derived symbol universes.
- `python3 scripts/benchmark_compact_read.py`: benchmark validated warm reads.
- `python3 scripts/validate_compact_reference_adapter.py`: compare compact rows
  reconstructed as reference events with direct source conversion.
- `python3 scripts/validate_daily_symbol_parity.py`: compare daily stock
  Parquet with the authoritative symbol DataAPI path.
- `python3 future_spot/test/run_full_backtest.py --engine slim ...`: supported
  full-market entrypoint; it selects compact data automatically.
- `future_spot/test/compare_engine_outputs.py`: persisted reference/slim output
  comparator.
- `future_spot/test/monthly_runtime_benchmark.py`: monthly benchmark harness;
  retained but not executed for this relocation-only phase.

The root README's native build command is
`cargo build --workspace --release`. No benchmark was run and no performance
claim is made for Phase 0/1.

## Existing test coverage

| Test location | Behavior frozen by current coverage | Later disposition |
| --- | --- | --- |
| Rust tests in `crates/hbt_slim/src/lib.rs` | Crossing FOK/IOC no-partial fill, a non-crossing expiry, exchange-data/order equal-time priority, independent feed correction and order latency, locked-book clear bounds, and draining another asset's events at a response timestamp. | **move later** with the native crate in Phase 2. |
| `tests/test_slim_engine.py` | Direct ctypes/Arrow binding, feed/order latency, immediate fill fields/timestamps, close behavior, and valid empty asset partitions. | **compatibility wrapper later**; preserve as a legacy binding fixture through Phase 3. |
| `tests/test_compact_cache.py` | Cold one-scan/warm zero-scan, Top-5 same-price aggregation, decimal price preservation, missing/empty symbols, timestamp adjustment/order sidecars, corrupt file/sidecar rejection, budget and free-space failure cleanup, incomplete publication rejection, source-stat invalidation, and exact compact-to-reference events. | **move later** for package-owned cache tests in Phase 4; keep reference-bridge assertions outside. |
| `tests/test_source_parity.py` | Provider dtype/symbol canonicalization, stable hash equality, and first value mismatch evidence. | **remain outside** unless neutral pieces are promoted later. |
| `tests/test_slim_pair_parity.py` | One pair/date golden equality for reference/slim fill, status, price, quantity, signal/completion time, and order latency timestamps. | **remain outside** as a cross-engine golden test. |
| `tests/test_hbt_runner_execution.py` | Compact-build error audit preservation, caller-owned executor use, and row-weighted pair shards. | **remain outside** as runner integration coverage. |
| `tests/test_daily_result_pipeline.py`, `tests/test_backtest_pipeline_timings.py`, and `tests/test_compare_engine_outputs.py` | Daily result/manifest orchestration mocks, pipeline timing rows, and bounded persisted-output engine comparisons. | **remain outside** as result/runner/manifest integration coverage. |
| `future_spot/test/backtest_pipeline.py` | Production pipeline helper invokes run-manifest persistence after outputs. | **remain outside**; it is a runtime helper, not a collected unit test. |
| New `hftbacktest_slim/tests/` | Phase 1 exports/version/enums/config/errors plus AST dependency boundary and no-index/no-dependency external install/import smoke coverage. | Package-owned Phase 1 tests. |

## Missing tests and uncovered semantic hazards

The following are recorded gaps, not assertions weakened for migration:

- No direct binding fixture covers a missing native library or ABI mismatch;
  those belong with native-library loading in Phase 3.
- Current equal-time native coverage checks exchange data against an order and
  same-time draining across assets, but not a complete golden collision matrix
  for both assets and all local-data, local-order-response, exchange-data, and
  exchange-order-request event kinds.
- Direct native matching does not independently fixture every buy/sell ×
  crossing/non-crossing × FOK/IOC combination, invalid asset/side/TIF, duplicate
  order IDs, or timeout-before-response.
- Pair-level reference/slim parity has one successful two-leg fill case. It
  does not yet cover expiry, response timeout, post-first-feed timeout,
  end-of-data, second-leg recheck failure, flatten behavior, locked books,
  exclusions, or cross-date carry.
- The cache tests simulate preflight budget failures and corrupted/incomplete
  published state, but do not inject failures at every atomic-publication step
  or exercise a runtime disk-reserve crossing after several batches.
- Sidecar content is validated, while a dedicated reader-consumption parity
  test for every non-monotonic local/exchange ordering pattern is still needed
  when the reader moves.
- The one-scan fixture uses one physical source file. Multiple-file physical
  sources and file-descriptor/writer bounding need explicit migration tests.
- Daily-versus-symbol parity unit coverage is synthetic; representative
  external provider data still requires the data-dependent validation CLI and
  cannot be claimed from this repository-only baseline.
- No monthly, multi-month, or annual external-data run was executed for this
  relocation-only task. Therefore `run_errors.csv`, peak RSS, and large-run
  parity are not re-baselined here.

## Pre-package baseline commands and outcomes

These commands ran before any `hftbacktest_slim/` package code was created.

| Exact command | Outcome |
| --- | --- |
| `cargo test -p hbt_slim` | PASS: 5 Rust unit tests and 0 doc tests failed. |
| `cargo build -p hbt_slim --release` | PASS: existing release shared library built successfully. |
| `python3 -m pytest -q tests/test_slim_engine.py` | ENVIRONMENT FAILURE during collection: system `/usr/bin/python3` raised `ModuleNotFoundError: No module named 'numpy'`. No assertion ran and no environment was modified. |
| `.venv/bin/python -m pytest -q tests/test_slim_engine.py` | PASS: 2 tests. |
| `.venv/bin/python -m pytest -q tests/test_compact_cache.py tests/test_source_parity.py` | PASS: 9 tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_pair_parity.py` | PASS: 1 test. |
| `.venv/bin/python -m pytest -q tests/test_hbt_runner_execution.py tests/test_daily_result_pipeline.py tests/test_backtest_pipeline_timings.py tests/test_compare_engine_outputs.py` | PASS: 8 tests. |
| `.venv/bin/python -m pytest -q tests future_spot/test` | PASS: 70 tests. |
| `cargo test --workspace` | PASS: 5 Rust unit tests and 0 doc tests failed. |
| `cargo fmt --check --all` | PASS: no output. |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS: no warnings. |
| `.venv/bin/python -m py_compile scripts/*.py future_spot/arbitrage/*.py future_spot/scripts/*.py future_spot/test/*.py` | PASS: no output. |

The missing packages in the system interpreter are not a migration blocker:
the repository-managed `.venv` contains the declared test/runtime dependencies
and completes the baseline. The Phase 1 install smoke test deliberately uses a
temporary target and the base interpreter only as a build frontend; it does not
modify either global or virtual environments.

## Phase 0 gate result

The focused Rust, Python binding, compact-cache, source-parity, pair-parity,
runner/manifest, full Python, formatting, lint, and compilation baselines all
pass in the repository's existing environment. Existing runtime components,
imports, native paths, versions, ABI, schema, builder identity, Cargo workspace,
and consumers remain frozen for Phase 1.

## Post-Phase 1 validation

| Exact command | Outcome |
| --- | --- |
| `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider -q tests/test_slim_engine.py tests/test_compact_cache.py tests/test_source_parity.py tests/test_slim_pair_parity.py tests/test_hbt_runner_execution.py tests/test_daily_result_pipeline.py tests/test_backtest_pipeline_timings.py tests/test_compare_engine_outputs.py` | PASS: 20 focused legacy regression tests. |
| `cargo test --workspace` | PASS: 5 Rust unit tests and 0 doc-test failures. |
| `cargo fmt --check --all` | PASS: no output. |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS: no warnings. |
| `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider -q hftbacktest_slim/tests` | PASS: 12 package/type/boundary/install tests. |
| `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider -q tests future_spot/test` | PASS: 70 existing repository tests. |
| `PYTHONPYCACHEPREFIX=/tmp/hftbacktest_phase01_pycache .venv/bin/python -m py_compile scripts/*.py future_spot/arbitrage/*.py future_spot/scripts/*.py future_spot/test/*.py hftbacktest_slim/src/hftbacktest_slim/*.py hftbacktest_slim/tests/*.py` | PASS: all retained entrypoints, package modules, and package tests compiled. |
| `git diff --check` plus `git diff --no-index --check /dev/null <each-new-file>` | PASS: tracked and new files have no whitespace errors. |

The external install test uses `pip --no-index --no-deps --no-build-isolation
--target` against a temporary copy of the local package source, then imports
with isolated mode from a working directory outside the repository. It verifies
installed metadata version `0.3.0a0` and confirms that no `hftbacktest`,
`future_spot`, or `scripts` module was loaded. The temporary-copy approach also
prevents setuptools build and egg-info artifacts from polluting the package
source tree.

## Phase 2 pre-move baseline

The following commands ran against the untouched Phase 1 tree immediately
before relocating the native crate. The worktree was clean at baseline.

| Exact command | Outcome |
| --- | --- |
| `git status --short` | PASS: no tracked or untracked changes. |
| `cargo test --workspace` | PASS: 5 Rust unit tests and 0 doc-test failures. |
| `cargo fmt --check --all` | PASS: no output. |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS: no warnings. |
| `python3 -m pytest -q tests/test_slim_engine.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: system `/usr/bin/python3` raised `ModuleNotFoundError: No module named 'numpy'`; no assertion ran. |
| `.venv/bin/python -m pytest -q tests/test_slim_engine.py` | PASS: 2 tests. |
| `python3 -m pytest -q tests/test_slim_pair_parity.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: system `/usr/bin/python3` raised `ModuleNotFoundError: No module named 'numpy'`; no assertion ran. |
| `.venv/bin/python -m pytest -q tests/test_slim_pair_parity.py` | PASS: 1 test. |
| `cargo build --workspace --release` | PASS: `target/release/libhbt_slim.so` built at the existing root target path. |
| `python3 -c "import ctypes; lib=ctypes.CDLL('target/release/libhbt_slim.so'); lib.hbt_slim_version.restype=ctypes.c_uint32; print(lib.hbt_slim_version())"` | PASS: native ABI version `1`. |

The pre-move release library exported exactly these sorted `hbt_slim_*`
symbols, captured with
`nm -D --defined-only target/release/libhbt_slim.so`:

```text
hbt_slim_create
hbt_slim_current_timestamp
hbt_slim_depth
hbt_slim_elapse
hbt_slim_feed_latency
hbt_slim_free
hbt_slim_order
hbt_slim_order_latency
hbt_slim_submit
hbt_slim_version
hbt_slim_wait_order_response
```

The library was an ELF 64-bit x86-64 shared object named
`libhbt_slim.so`. The crate version was `0.2.0`, the native ABI version was
`1`, and the Python implementation identity remained `rust-0.2.0`.

## Phase 2 post-move result

Phase 2 is a relocation-only change. The sole root Cargo workspace member is
now `hftbacktest_slim/native`; the crate remains named `hbt_slim` at version
`0.2.0`, and release builds still publish
`target/release/libhbt_slim.so`. The old `crates/hbt_slim/` implementation was
removed without leaving a duplicate.

The former monolithic implementation is split under `native/src/` as follows:

- `types.rs`: ABI constants, C-layout rows/views, and native configuration;
- `book.rs`: BBO/depth snapshot state and locked/crossed handling;
- `scheduler.rs`: event kinds, pending events, priority keys, and selection;
- `matcher.rs`: pure immediate crossing, fill, and expiry decision;
- `engine.rs`: two-asset state, clock, latency, requests, and responses;
- `ffi.rs`: pointer validation and every retained C ABI export; and
- `lib.rs`: minimal declarations and crate-root compatibility re-exports.

The post-move release library exports the same sorted 11-symbol list recorded
above and returns ABI version `1`. The retained `scripts/slim_engine.py`
binding still loads the root release artifact without an import or path change.
Linux x86-64 layout tests freeze the existing `BboRow`, `BboView`, and
`OrderView` sizes, alignments, and field offsets used by that binding.

`future_spot.arbitrage.full_market_runner` now fingerprints
`hftbacktest_slim/native/Cargo.toml` and every `.rs` file below
`hftbacktest_slim/native/src/` in deterministic sorted source-path order. This
changes implementation identity, so existing result manifests may invalidate
conservatively. No unrelated manifest field, compact schema version, compact
builder version, native ABI version, matching semantic version, strategy clock,
or time-in-force semantic identity changed.

### Phase 2 post-move validation

| Exact command | Outcome |
| --- | --- |
| `cargo test -p hbt_slim` | PASS: 13 Rust unit tests and 0 doc-test failures. |
| `cargo test --workspace` | PASS: 13 Rust unit tests and 0 doc-test failures. |
| `cargo fmt --check --all` | PASS: no output. |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS: no warnings. |
| `cargo build --workspace --release` | PASS: relocated release library built at the unchanged root target path. |
| `.venv/bin/python -m pytest -q hftbacktest_slim/tests` | PASS: 12 Phase 1 package and dependency-boundary tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_engine.py` | PASS: 2 ctypes binding tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_pair_parity.py` | PASS: 1 reference/slim pair golden test. |
| `.venv/bin/python -m pytest -q tests/test_hbt_runner_execution.py` | PASS: 4 runner tests, including relocated native fingerprint selection. |
| `.venv/bin/python -m pytest -q tests/test_hbt_runner_execution.py tests/test_daily_result_pipeline.py tests/test_backtest_pipeline_timings.py tests/test_compare_engine_outputs.py` | PASS: 9 focused runner, manifest, persistence, timing, and comparison tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_engine.py tests/test_slim_pair_parity.py tests/test_hbt_runner_execution.py hftbacktest_slim/tests` | PASS: 19 focused binding, parity, runner, manifest, and package-boundary tests. |
| `.venv/bin/python -m pytest -q tests future_spot/test` | PASS: 71 repository tests. |
| `PYTHONPYCACHEPREFIX=/tmp/hftbacktest_phase2_pycache .venv/bin/python -m py_compile scripts/*.py future_spot/arbitrage/*.py future_spot/scripts/*.py future_spot/test/*.py hftbacktest_slim/src/hftbacktest_slim/*.py hftbacktest_slim/tests/*.py` | PASS: retained Python entrypoints, Phase 1 package modules, and tests compiled. |
| `git diff --check` plus `git diff --no-index --check /dev/null <each-new-native-file>` | PASS: tracked and relocated files have no whitespace errors. |

The system `python3` collection failures recorded in the pre-move table remain
an interpreter dependency issue only; the repository-managed `.venv` executes
the complete Python suite. No external market data or performance benchmark is
required or claimed for this relocation gate.

## Phase 3 pre-move baseline

The following commands ran against the clean, completed Phase 2 tree before
moving any Python runtime implementation. This Phase 3 change is classified as
**relocation-only/API-boundary work**: the Rust ABI, scheduler, matcher, compact
schema/builder, and futures/spot behavior are frozen.

| Exact command | Outcome |
| --- | --- |
| `git status --short` | PASS: no tracked or untracked changes. |
| `cargo test --workspace` | PASS: 13 Rust unit tests and 0 doc-test failures. |
| `python3 -m pytest -q hftbacktest_slim/tests` | PASS: 12 Phase 1/2 package and boundary tests. |
| `python3 -m pytest -q tests/test_slim_engine.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: system `/usr/bin/python3` raised `ModuleNotFoundError: No module named 'numpy'`; no assertion ran. |
| `python3 -m pytest -q tests/test_slim_pair_parity.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: system `/usr/bin/python3` raised `ModuleNotFoundError: No module named 'numpy'`; no assertion ran. |
| `python3 -m pytest -q tests/test_hbt_runner_execution.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: system `/usr/bin/python3` raised `ModuleNotFoundError: No module named 'pandas'`; no assertion ran. |
| `.venv/bin/python -m pytest -q hftbacktest_slim/tests` | PASS: 12 package and dependency-boundary tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_engine.py` | PASS: 2 exact legacy ctypes/Arrow binding tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_pair_parity.py` | PASS: 1 exact reference/slim pair golden test. |
| `.venv/bin/python -m pytest -q tests/test_hbt_runner_execution.py` | PASS: 4 runner and implementation-fingerprint tests. |
| `cargo build --workspace --release` | PASS: release shared library built at `target/release/libhbt_slim.so`. |

The pre-move release artifact reports native ABI version `1` and exports the
same sorted 11-symbol `hbt_slim_*` list recorded for Phase 2. The Rust crate is
version `0.2.0`, `SLIM_ENGINE_VERSION` is `rust-0.2.0`, the package is
`0.3.0a0`, compact schema is `bbo_v1`, and compact builder version is `1`.
The package root exports `AssetConfig`, neutral enums, the Phase 1 exception
hierarchy, and `__version__`; it intentionally has no engine before this move.
The legacy compatibility surface is `SlimBacktest`, `SlimHbtConstants`,
`SlimOrder`, `SlimDepth`, `validate_slim_pair_config`,
`SLIM_ENGINE_VERSION`, and `SLIM_LIBRARY`, with current HBT-shaped methods and
return codes frozen by the focused binding and pair-parity results above.

## Phase 3 post-move result

Phase 3 is complete as a relocation/API-boundary change. The package owns the
neutral `SlimEngine`, immutable depth/order/feed-latency/order-latency models,
ABI-v1 ctypes declarations and calls, deterministic native-library discovery,
the minimal runtime-side `bbo_v1` Arrow row contract, and the temporary HBT
compatibility facade. `scripts/slim_engine.py` is now an import-only wrapper
with a narrow repository `src/` bootstrap for current uninstalled consumers.

The package version is `0.3.0a1`. Rust crate `0.2.0`, engine identity
`rust-0.2.0`, native ABI `1`, compact schema `bbo_v1`, and compact builder
version `1` are unchanged. The release artifact remains
`target/release/libhbt_slim.so` with the same 11 exported symbols. No Rust
source, compact builder/schema, strategy consumer import, or matching behavior
changed.

The run-manifest implementation fingerprint now includes every sorted Python
source below `hftbacktest_slim/src/hftbacktest_slim/engine/`, neutral
config/enums/models/version, `compat/hbt.py`, the legacy wrapper, and the Phase
2 native sources. Existing result manifests may therefore invalidate
conservatively. Compact market-data cache identity is unchanged.

### Phase 3 post-move validation

| Exact command | Outcome |
| --- | --- |
| `cargo test --workspace` | PASS: 13 Rust unit tests and 0 doc-test failures. |
| `cargo fmt --check --all` | PASS: no output. |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS: no warnings. |
| `cargo build --workspace --release` | PASS: release artifact built at the unchanged root target path. |
| `.venv/bin/python -m pytest -q hftbacktest_slim/tests` | PASS: 42 neutral API, Arrow reader, native loading, compatibility, dependency-boundary, and external-install tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_engine.py` | PASS: 2 unchanged legacy binding tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_pair_parity.py` | PASS: 1 exact reference/slim pair golden test. |
| `.venv/bin/python -m pytest -q tests/test_hbt_runner_execution.py tests/test_daily_result_pipeline.py tests/test_backtest_pipeline_timings.py tests/test_compare_engine_outputs.py` | PASS: 10 runner, manifest, persistence, timing, and output-comparison tests. |
| `.venv/bin/python -m pytest -q tests future_spot/test` | PASS: 72 repository tests. |
| `PYTHONPYCACHEPREFIX=/tmp/hftbacktest_phase3_pycache .venv/bin/python -m compileall -q scripts future_spot/arbitrage future_spot/scripts future_spot/test hftbacktest_slim/src/hftbacktest_slim` | PASS: all retained Python entrypoints and package modules compiled. |
| `git diff --check` plus `git diff --no-index --check /dev/null <each-new-file>` | PASS: tracked and new Phase 3 files have no whitespace errors. |

The exact system-interpreter validation commands still fail during collection
because `/usr/bin/python3` has neither NumPy/PyArrow nor Pandas. The failures
are environment-only and occur before assertions; the repository `.venv`
passes every corresponding command and the full suite. The installed-package
test copies and installs the src-layout package into a temporary target, imports
it from an external working directory without loading forbidden packages or the
native library, then constructs the neutral engine there with the explicit
valid release-library path. Missing-library and ABI-mismatch construction
failures are typed. No external-data run or performance benchmark was executed,
so no `run_errors.csv` or performance claim is produced for Phase 3.

## Phase 4 pre-change baseline

The Phase 4 baseline was captured on 2026-09-03 against the completed Phase 3
tree before compact-cache ownership moved. The worktree contained only the
expected in-progress Phase 3 files listed by `git status --short`; they were
preserved and treated as user-owned migration work.

| Exact command | Outcome |
| --- | --- |
| `python3 -m pytest -q tests/test_compact_cache.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: `/usr/bin/python3` has no NumPy. |
| `python3 -m pytest -q tests/test_source_parity.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: `/usr/bin/python3` has no NumPy. |
| `python3 -m pytest -q tests/test_tw_stock_data_to_npz.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: `/usr/bin/python3` has no NumPy. |
| `python3 -m pytest -q tests/test_slim_engine.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: `/usr/bin/python3` has no NumPy. |
| `python3 -m pytest -q tests/test_slim_pair_parity.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: `/usr/bin/python3` has no NumPy. |
| `python3 -m pytest -q hftbacktest_slim/tests` | PRE-EXISTING ENVIRONMENT FAILURE during collection: `/usr/bin/python3` has no NumPy. |
| `python3 -m pytest -q tests/test_hbt_runner_execution.py tests/test_daily_result_pipeline.py tests/test_daily_result_store.py` | PRE-EXISTING ENVIRONMENT FAILURE during collection: `/usr/bin/python3` has no Pandas. |
| `.venv/bin/python -m pytest -q tests/test_compact_cache.py tests/test_source_parity.py tests/test_tw_stock_data_to_npz.py` | PASS: 15 cache, source-parity, and converter tests. |
| `.venv/bin/python -m pytest -q tests/test_slim_engine.py tests/test_slim_pair_parity.py` | PASS: 3 binding and exact pair-parity tests. |
| `.venv/bin/python -m pytest -q hftbacktest_slim/tests` | PASS: 42 package tests. |
| `.venv/bin/python -m pytest -q tests/test_hbt_runner_execution.py tests/test_daily_result_pipeline.py tests/test_daily_result_store.py future_spot/test/test_fast_pipeline.py` | PASS: 25 runner, manifest, persistence, budget, and pipeline tests. |
| `cargo test --workspace` | PASS: 13 Rust unit tests and 0 doc-test failures. |
| `.venv/bin/python scripts/build_compact_cache.py --help` | PASS: legacy build CLI arguments recorded. |
| `.venv/bin/python scripts/benchmark_compact_read.py --help` | PASS: legacy benchmark CLI arguments recorded. |

The frozen compact contract is `COMPACT_SCHEMA_VERSION = "bbo_v1"` and
`COMPACT_BUILDER_VERSION = 1`. `BBO_SCHEMA` has exactly nine nullable physical
fields in this order: `source_seq uint64`, `exch_ts int64`, `local_ts_raw
int64`, `bid_px float64`, `ask_px float64`, `bid_qty float64`, `ask_qty
float64`, `last_px float64`, and `total_volume int64`. The separately defined
Phase 3 `SLIM_ROW_DTYPE` has the same names in the same order, native-endian
8-byte scalar fields at offsets 0 through 64, aligned layout, and item size 72.

The projected source columns are `symbol`, `symbol_id`, `exchtime`,
`localtime`, `status`, `last_price`, `total_volume`, `sequence`, followed for
levels 1 through 5 by bid price/volume and ask price/volume. The identity keys
are `trade_date`, `schema_version`, `builder_version`, `builder_sha256`,
`top5_implementation_sha256`, `compression`, `profile`, `timezone`, session
bounds, `base_latency_ns`, `projected_columns`, and `sources`. Each source
identity records kind, ordered symbols, status filters, price-only quantity,
volume scale, and file identities; each file identity records resolved path,
bytes, nanosecond mtime, and Parquet/Arrow metadata row count. The baseline
legacy file fingerprints were builder SHA-256
`219fd49ef6366d50c193ff2df7cc250ff6353af1a26ff5e0e9f4a78feffca3e2` and
converter/normalization SHA-256
`9c16ffcf090896d6d1375c014823bc29fdf9dd49d6fc4f2293a12396418a758d`.

The cold synthetic build scans its single physical stock source once; a warm
validated build reports zero invocation scans while retaining the original
source manifest's scan count. Requested symbols with no retained rows publish
a valid empty `bbo_v1` Arrow partition rather than a missing status. Per-symbol
negative-latency correction is recorded in metadata, and independently
non-monotonic exchange/local orders publish deterministic LZ4 order sidecars.
Preflight cache-size or free-space failures publish no final date, and a build
failure cleans only its current `.tmp-*` date. An incomplete final date is not
a hit; without explicit rebuild it raises an incompatible-identity error.

The legacy build CLI accepts `--date`, `--cache-root`, stock/future paths,
explicit spot/future symbols, settings Parquet, `none|lz4|zstd` compression,
batch rows, cache/free-space GB limits, rebuild, and output. Its JSON result has
top-level `date`, `cache_root`, `compression`, `cache_state`, wall/CPU seconds,
peak RSS, and the full manifest. The warm-read CLI accepts cache root, date,
repetitions, and output; its JSON has date/root, file and byte counts, peak RSS,
per-run wall/CPU/rows/throughput/checksum records, and median wall/throughput.

## Phase 4 post-move result

Phase 4 is complete as a compact-builder implementation-ownership change. The
package now owns the canonical `bbo_v1` schema and aligned dtype, neutral Top-5
normalization, per-symbol timestamp correction and order sidecars, generic
partition audit, cache configuration/builder/store, deterministic manifest
identity, validation, resource checks, recoverable atomic publication, and the
build/read CLIs. The reference converter imports the package normalizer. The
legacy compact module and command scripts are thin re-exports/delegates, while
the reference-HBT adapter remains outside the package.

The package is `0.3.0a2`, compact builder version is `2`, and compact schema
remains `bbo_v1`. Rust crate `0.2.0`, engine identity `rust-0.2.0`, native ABI
`1`, matching, latency, FOK/IOC, strategy, carry, capital, and reporting
semantics are unchanged. Builder-version-1 caches and old implementation
fingerprints invalidate conservatively. The final package implementation SHA-256
is `d2d775a64e0d7ad86d895e55c6c76959c05240db8151d6483d695b0e2b5072c0`;
the package normalization SHA-256 is
`1dc8b7507bd94f5764b198c6926a5be7384721125938d9c81879f0a2a562b019`.
Both are content identities, not performance claims.

The canonical Arrow schema retains the exact nine nullable fields and types
recorded in the pre-change section. `SLIM_ROW_DTYPE` has the same names, native
8-byte formats and offsets `[0, 8, 16, 24, 32, 40, 48, 56, 64]`, aligned item
size 72. Its reader, builder, and compatibility imports all source this one
definition from `market_data/schema.py`.

### Phase 4 post-move validation

| Exact command | Outcome |
| --- | --- |
| `.venv/bin/python -m pytest -q hftbacktest_slim/tests` | PASS: 70 schema, normalization, cache, ordering, publication, resource, CLI, native, boundary, and external-install tests. |
| `.venv/bin/python -m pytest -q hftbacktest_slim/tests tests/test_compact_cache.py tests/test_source_parity.py tests/test_tw_stock_data_to_npz.py tests/test_slim_engine.py tests/test_slim_pair_parity.py tests/test_hbt_runner_execution.py tests/test_daily_result_pipeline.py tests/test_daily_result_store.py future_spot/test/test_fast_pipeline.py` | PASS: 115 focused package, compatibility, converter, parity, runner, manifest, persistence, and pipeline tests. |
| `.venv/bin/python -m pytest -q tests future_spot/test` | PASS: 74 repository tests. |
| `cargo test --workspace` | PASS: 13 Rust unit tests and 0 doc-test failures. |
| `cargo fmt --check --all` | PASS: no output. |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS: no warnings. |
| `cargo build --workspace --release` | PASS: unchanged release artifact built successfully. |
| `PYTHONPYCACHEPREFIX=/tmp/hftbacktest_phase4_pycache .venv/bin/python -m compileall -q scripts future_spot/arbitrage future_spot/scripts future_spot/test hftbacktest_slim/src/hftbacktest_slim hftbacktest_slim/tests tests` | PASS: all retained entrypoints, package modules, and tests compiled. |
| `.venv/bin/python -c "import numpy as np; ... data/tw_stock_events/0050_20260223_093000_100000.npz ..."` | PASS: available fixture preserves `77.90`, `77.95`, `78.00`, and `78.05` among its first valid prices. |
| `git diff --check` plus whitespace checks for new files | PASS: no whitespace errors. |

Cold multi-symbol tests observe one payload scan for each physical source; warm
validated reuse observes zero. Two `future_spot` pairs are combined into one
stock and one futures source universe before the package builder is invoked.
Manifest/source identity derives row counts and Parquet footer identity without
a payload rescan. Tests also freeze empty partitions, independent per-symbol
correction, equal-time stable ordering, both sidecars, all three compression
modes, last-manifest publication, builder/source/implementation invalidation,
corruption rejection, preflight and during-build budget failures, narrow temp
cleanup, and preservation of completed caches.

The system `/usr/bin/python3` failures from the pre-change table remain a
concrete interpreter dependency limitation: NumPy/PyArrow and Pandas are not
installed there, so collection stops before assertions. The repository-managed
`.venv` passes every corresponding focused and full command. The external-install
package test performs a no-index local install and imports/constructs cache and
engine APIs from a working directory outside the repository while proving no
`scripts`, `future_spot`, or installed `hftbacktest` dependency. No full-market
data run or performance benchmark was executed, so Phase 4 creates no
`run_errors.csv` and makes no throughput claim.
