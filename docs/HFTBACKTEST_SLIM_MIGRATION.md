# HftBacktest Slim Package Migration

## Status and decision

Implementation status (2026-09-03): **Phases 0–6 complete**. The reusable slim
runtime has been extracted from its historical root and strategy integration
locations into the self-contained root-level package:

```text
ROOT/hftbacktest_slim/
```

This is an architectural extraction, not a matching or strategy semantic
change. The migration must preserve existing market-data, scheduler, latency,
order, fill, carry, capital, exclusion, persistence, and reporting behavior.

For package location, code ownership, public API, and dependency direction,
this document supersedes older extraction locations in
`HBT_ACCELERATION_STRATEGY.md`. That strategy document and `AGENTS.md` remain
authoritative for execution semantics, correctness priorities, compact-cache
contracts, resource limits, parity gates, and benchmark claims.

## Goals

- Give every strategy one neutral slim runtime instead of making new strategies
  import `future_spot` or HBT compatibility code.
- Let a backend build/read compact data and run the slim engine by importing
  only `hftbacktest_slim`.
- Keep the native matcher, Python binding, compact BBO data contract, cache
  publication, and version metadata together.
- Preserve the reference HftBacktest path as the regression oracle and fallback
  for unsupported modes.
- Make package ownership and allowed dependency direction mechanically
  testable.

## Non-goals

- Moving futures/spot pricing, signals, risk, carry, expiry, capital replay, or
  reports into the shared runtime.
- Making slim the default engine as part of the relocation.
- Expanding slim to passive GTC/GTX, queue models, partial fills, arbitrary
  depth-sensitive strategies, or live trading.
- Changing the `bbo_v1` physical schema without a separate semantic design and
  versioned migration.
- Removing the reference engine or compact-to-reference parity bridge.

## Required dependency direction

```text
hftbacktest_slim
    ^
    |-- future_spot strategy adapter
    |-- another strategy adapter
    |-- backend or strategy runner
    `-- reference compact adapter

reference HftBacktest <--- parity/compatibility tests ---> hftbacktest_slim
```

The `hftbacktest_slim` package must not import:

```text
future_spot.*
another strategy package
scripts.hbt_*
scripts.tw_stock_*
hftbacktest
```

Standard-library modules and declared package dependencies such as NumPy and
PyArrow are allowed. A package-boundary test must inspect imports and fail on a
forbidden dependency. A subprocess smoke test must also prove that importing
and opening the public API does not import the installed `hftbacktest` package.

## Final package layout

The exact module split may evolve, but ownership must follow this structure:

```text
hftbacktest_slim/
├── pyproject.toml
├── README.md
├── src/
│   └── hftbacktest_slim/
│       ├── __init__.py
│       ├── api.py
│       ├── config.py
│       ├── enums.py
│       ├── errors.py
│       ├── models.py
│       ├── version.py
│       ├── cli/
│       │   ├── build_cache.py
│       │   └── benchmark_read.py
│       ├── engine/
│       │   ├── binding.py
│       │   ├── replay.py
│       │   ├── arrow_reader.py
│       │   └── validation.py
│       ├── market_data/
│       │   ├── schema.py
│       │   ├── normalize.py
│       │   ├── ordering.py
│       │   └── audit.py
│       └── cache/
│           ├── config.py
│           ├── builder.py
│           ├── store.py
│           ├── manifest.py
│           └── publication.py
├── native/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── types.rs
│       ├── book.rs
│       ├── scheduler.rs
│       ├── matcher.rs
│       ├── engine.rs
│       └── ffi.rs
└── tests/
```

Do not split modules merely to satisfy this example tree. Split by stable
responsibility while keeping the public API small. Internal implementation
modules must not be treated as backend contracts.

## Ownership boundary

### Belongs in `hftbacktest_slim`

- Rust BBO state, event scheduler, immediate matcher, order state, latency
  state, and C ABI.
- Native-library loading and ABI validation.
- Slim-native asset, order, depth, latency, side, time-in-force, and status
  types.
- Arrow BBO schema, native row dtype, partition reader, timestamp adjustment,
  and deterministic local/exchange ordering.
- Top-5 normalization needed to construct the shared BBO representation.
- Compact-cache config, source descriptors, streaming builder, manifest,
  validation, sidecars, resource budgets, atomic publication, and reader.
- Slim capability validation and stable public API.
- Slim-specific cache build and read benchmark command-line entrypoints.

### Remains outside `hftbacktest_slim`

- Futures/spot universe construction and raw-source path resolution.
- Strategy domain models, pricing, signal decisions, position accounting,
  second-leg checks, flatten policy, risk, carry, expiry, and capital replay.
- Strategy output schemas, persisted reports, plots, and full-market CLI.
- HftBacktest event conversion, queue models, Numba reference scanner, and
  reference asset construction.
- Compact-to-reference event reconstruction. It may import the slim compact
  schema, but slim must not import it.
- Cross-engine parity tools and fixtures that intentionally import both sides.

## Historical old-to-final relocation map

The left column records pre-migration locations only; none is supported after
Phase 6.

| Historical location | Final location or action |
| --- | --- |
| `crates/hbt_slim/Cargo.toml` | Move to `hftbacktest_slim/native/Cargo.toml`; update the root Cargo workspace member. |
| `crates/hbt_slim/src/lib.rs` | Move and split under `hftbacktest_slim/native/src/` without changing behavior. |
| `scripts/slim_engine.py` | Split into slim-native config/models, Arrow reader, binding, replay API, and temporary HBT compatibility adapter. |
| `scripts/compact_cache.py` | Split into `market_data/` and `cache/`; preserve one-scan, bounded-memory, atomic-publication, and validation contracts. |
| `scripts/build_compact_cache.py` | Move to a slim CLI entrypoint with a thin legacy wrapper during migration. |
| `scripts/benchmark_compact_read.py` | Move to a slim CLI entrypoint with a thin legacy wrapper during migration. |
| `scripts.tw_stock_data_to_npz.normalized_bbo_from_depth_columns` | Promote the neutral normalization implementation into `hftbacktest_slim.market_data`; make the reference converter import it, never the reverse. |
| `future_spot.arbitrage.full_market_runner.compact_asset_audit` | Move generic compact audit behavior into slim; retain strategy-specific settings-row mapping in `future_spot`. |
| `future_spot.arbitrage.full_market_runner.build_compact_event_data` | Split generic cache build/read operations from futures/spot universe and path orchestration. Only the generic part moves. |
| `future_spot.arbitrage.hbt_backtest` slim construction branch | Replace with a small adapter that constructs the neutral slim engine. Strategy execution remains in `future_spot`. |
| `scripts/compact_hbt_adapter.py` | Keep outside slim as a reference parity bridge; update it to import slim schema/version APIs. |
| `scripts/source_parity.py` | Keep provider/reference orchestration outside; promote only provider-neutral canonicalization if required by slim cache validation. |

## Public API requirements

New strategies and backends must import from `hftbacktest_slim` or documented
public subpackages, not internal binding or compatibility modules. The initial
API should expose neutral concepts such as:

```python
from hftbacktest_slim import (
    AssetConfig,
    CompactBuildConfig,
    CompactCacheStore,
    SlimEngine,
    TimeInForce,
)
```

The neutral engine API should provide current time, bounded clock advancement,
BBO/depth views, feed and order latency, immediate order submission, response
waiting, order lookup, and explicit close/context-manager behavior.

HftBacktest names are not part of the final contract. During Phases 1–5,
`SlimBacktest`, `SlimHbtConstants`, `submit_buy_order`, and similar HBT-shaped
APIs existed temporarily under an explicit compatibility namespace; Phase 6
removed them. New strategy code must use neutral enums and
`submit_order`-style methods.

The public API must document the supported profile:

- exactly two assets per current engine instance;
- compact BBO input;
- explicit strategy clock controlled by the caller;
- immediate crossing FOK/IOC limit orders;
- no partial fill and no displayed-size cap;
- independent feed, entry, and response latency;
- no passive queue or arbitrary depth-sensitive behavior.

If another strategy requires Top-5 depth, introduce a separately versioned
`top5_v1` profile. Do not broaden `bbo_v1` silently.

## Migration phases and gates

### Phase 0: freeze behavior and inventory

- Record every current slim import, source fingerprint path, CLI entrypoint,
  native build path, and test.
- Preserve Rust and Python fixtures for clock, equal-time ordering, crossing and
  non-crossing FOK/IOC, latency, response waits, empty partitions, and locked
  books.
- Preserve pair-level reference/slim golden comparisons and compact-cache
  one-scan, interruption, disk, and invalidation tests.

Gate: the current focused tests and full baseline pass before relocation.

### Phase 1: create the package boundary

- Add package metadata, public exports, versions, errors, neutral enums, and
  neutral configuration models.
- Add forbidden-import and external-import smoke tests.
- Do not move matching behavior yet.

Gate: the package imports from outside the repository working directory and
does not load `hftbacktest`, `future_spot`, or root HBT helpers.

### Phase 2: move and split the native core

Implementation status (2026-09-02): **complete**. The crate is owned by
`hftbacktest_slim/native/`, its C ABI and root Cargo target path are unchanged,
and the Phase 2 Rust, ctypes-binding, pair-parity, manifest-fingerprint, and
package-boundary gates pass. Later phases are now complete.

- Relocate the Rust crate under `hftbacktest_slim/native/`.
- Split types, book, scheduler, matcher, engine, and FFI by responsibility.
- Keep symbol names and ABI layout stable unless an intentional ABI bump is
  documented.
- Update Cargo workspace and development library discovery.

Gate: Rust tests and Python binding fixtures match the pre-move behavior
exactly.

### Phase 3: move the Python engine runtime

Implementation status (2026-09-02): **complete**. The neutral Python engine,
ctypes ABI binding, compact Arrow row reader, immutable runtime models, native
library discovery, then-transitional HBT facade, and then-legacy wrapper were
package-owned at this checkpoint. Native ABI/matching behavior and compact-cache
ownership were unchanged; later phases are now complete.

- Move ctypes structs, signatures, library loading, Arrow row reading, depth
  and order views, and engine lifecycle.
- Replace the dependency on `scripts.hbt_types.HbtAssetConfig` with the neutral
  slim `AssetConfig`.
- Introduce the neutral order API and retain a thin compatibility adapter.

Gate: direct binding tests and pair-level golden output comparisons pass for
fills, statuses, prices, quantities, timestamps, and latency fields.

### Phase 4: move compact BBO and cache ownership

Implementation status (2026-09-03): **complete**. The package owns the one
canonical `bbo_v1` Arrow schema and aligned native dtype, neutral Top-5
normalization, timestamp correction/order sidecars, generic compact audit,
builder version 2 streaming cache, deterministic manifests, validation,
resource controls, atomic publication, reader, and installable build/read
commands. Legacy compact modules are thin wrappers/delegates and the reference
converter imports the package normalization implementation. Builder-version-1
caches and the relocated implementation fingerprint invalidate
conservatively. Physical schema, native ABI, Rust/engine versions, matching,
strategy, carry, capital, and reporting semantics are unchanged. Phases 5 and
6 are now complete.

- Move schema, normalization, ordering, builder, reader, manifest, sidecars,
  disk checks, and atomic publication.
- Reverse the normalization dependency so reference conversion imports the
  neutral implementation.
- Keep raw-source scans outside symbol and pair loops.

Gate: decimal ETF, Top-5 aggregation, per-symbol timestamp correction,
equal-order sidecars, cold one-scan, warm zero-scan, interrupted publication,
disk-budget, and cache-invalidation tests pass.

### Phase 5: migrate strategy consumers

Implementation status (2026-09-03): **complete**. `future_spot` now owns a minimal execution port plus
separate reference and neutral slim adapters. Its primary slim path imports the
documented root API and no compatibility facade. The reference Python/Numba
paths remain intact, and `examples/slim_two_asset_strategy/` independently
exercises a clocked FOK order with synthetic Arrow input. Synthetic one-pair and
five-pair parity gates pass. A 42-pair complete date and the full 22-trading-day
March 2026 month (638 daily pairs) also pass exact semantic-table parity with
carry enabled and zero errors. Result implementation
manifests invalidate because adapter sources changed, while compact identities,
versions, matching, and output schemas do not. Phase 6 is now complete.

- Make `future_spot` the first adapter using the neutral public API.
- Move only generic compact audit/build behavior out of the full-market runner.
- Keep pricing, risk, execution policy, carry, capital, reports, and strategy
  outputs in `future_spot`.
- Exercise the API with at least one additional strategy before declaring the
  extension boundary stable.

Gate: reference/slim golden comparisons pass for one pair/date, several pairs,
one complete date, one month, and carry across dates under the selected
semantic baseline.

### Phase 6: remove transitional locations

Implementation status (2026-09-03): **complete**. All active consumers and
tests use the neutral package API. The root engine/cache wrappers, root compact
CLI delegates, package HBT compatibility namespace, compatibility-only tests,
and old native location are absent. Reference compact reconstruction remains in
`scripts/compact_hbt_adapter.py`. Root setup installs the src-layout package,
and package-owned module/console CLIs are the supported compact commands.

The Python package is stable version `0.3.0`. Native ABI `1`, Rust crate
`0.2.0`, engine `rust-0.2.0`, compact schema `bbo_v1`, builder `2`, strict TIF,
strategy clock, and persisted schemas are unchanged. Result manifests
intentionally invalidate because deleted paths were removed from active source
selection; compact identities do not invalidate solely due to wrapper removal.

No external-data gate remains pending: Phase 5 already passed a complete
42-pair date and the 22-trading-day March 2026, 638-pair parity rollout with
carry and zero errors. Phase 6 reuses that semantic evidence and adds clean
package/install/build/search gates; it does not claim a new performance result.

Gate: repository search finds no runtime import of old slim locations, and a
clean build/test run succeeds without them.

## Historical compatibility and removal policy

- During Phases 1–5, wrappers delegated to one implementation and never forked
  logic.
- Phase 6 removed those deprecated imports after every active consumer moved.
- Do not recreate compatibility aliases for new strategy code.
- The reference compact adapter remains supported and is not considered a
  transitional slim wrapper.

## Versioning, cache, and manifest policy

- A package relocation with unchanged physical BBO fields does not by itself
  require a compact schema bump.
- Bump the compact builder version because implementation identity and source
  fingerprint ownership change. Reuse only when the new validator can prove
  full compatibility; unknown metadata means rebuild.
- Bump the slim implementation/package version for the new public API.
- Keep the native ABI version only if C layouts, exported functions, constants,
  ownership, and return semantics are unchanged. Otherwise bump it and fail
  clearly on mismatches.
- Run manifests must fingerprint the result-defining slim package sources and
  native sources deterministically. Replace hard-coded old source paths with a
  sorted package implementation fingerprint.
- Do not relabel old results with a new engine or builder version.
- Do not combine relocation with a matching-semantic change. If unavoidable,
  version and baseline the semantic change separately.

## Required validation

Focused validation must include:

```bash
cargo test --workspace
cargo fmt --check --all
cargo clippy --workspace --all-targets -- -D warnings
python3 -m pytest -q tests future_spot/test hftbacktest_slim/tests
python3 -m compileall -q scripts future_spot hftbacktest_slim/src/hftbacktest_slim
git diff --check
```

Once the package exists, also compile and test all retained Python modules under
`hftbacktest_slim/`. Add explicit checks for:

- forbidden imports and dependency direction;
- import from a working directory outside the repository;
- native-library not-built and ABI-mismatch errors;
- deterministic source and implementation fingerprints;
- exact reference/slim output parity under the declared semantics;
- no new or silently removed `run_errors.csv` rows where a run is executed.

Benchmark only after correctness gates pass. A file relocation is not a speed
improvement and must not be reported as one.

## Definition of done

The migration is complete only when:

1. All reusable slim Python and Rust runtime code lives under
   `hftbacktest_slim/`.
2. `hftbacktest_slim` has no forbidden imports and can be installed/imported by
   a backend without the reference HftBacktest package or strategy packages.
3. `future_spot` and at least one other strategy use the neutral public API and
   do not import each other.
4. The compact-cache one-scan, bounded-resource, atomic-publication, versioning,
   and validation contracts still pass.
5. Scheduler, matching, latency, order, fill, and selected cross-engine parity
   gates pass with no unexplained mismatches.
6. Manifests identify the new package, native ABI, engine implementation,
   compact schema, builder version, strategy clock, and result-defining source
   fingerprint.
7. Transitional wrappers and the old native crate location are removed, with
   reference-only adapters retained explicitly.
8. Documentation and build/test commands describe only the supported final
   locations and dependency direction.
