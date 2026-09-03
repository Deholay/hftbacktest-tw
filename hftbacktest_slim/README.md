# hftbacktest-slim

`hftbacktest-slim` is the project-owned, strategy-neutral compact-BBO data and
replay runtime. Version `0.3.0a2` includes the Phase 5 consumer boundary: the canonical
schema/native dtype, Top-5 normalization, timestamp ordering, audit, streaming
cache builder, manifest validation, sidecars, resource controls, publication,
reader, compact CLIs, and neutral engine API live in this standalone package,
and `future_spot` now consumes that API through strategy-owned adapters. The native crate
remains version `0.2.0`, engine identity remains `rust-0.2.0`, and C ABI version
remains `1`.

The supported profile is deliberately constrained:

- exactly two compact-BBO assets per engine;
- an explicit caller-controlled strategy clock;
- immediate crossing FOK or IOC limit orders;
- no partial fills and no displayed-size cap;
- independent feed, order-entry, and order-response latency; and
- no passive queue, cancel/modify, market-order, or arbitrary depth behavior.

## Neutral API

Only root-package imports are the primary runtime contract:

```python
from hftbacktest_slim import AssetConfig, Side, SlimEngine, TimeInForce

left = AssetConfig(
    symbol="0050",
    data_path="/data/compact/0050.arrow",
    tick_size=0.05,
    order_entry_latency_ns=1_000_000,
    order_response_latency_ns=1_000_000,
)
right = AssetConfig(
    symbol="NYF",
    data_path="/data/compact/NYF.arrow",
    tick_size=1.0,
)

with SlimEngine([left, right]) as engine:
    if engine.advance(1_000_000_000):
        depth = engine.depth(0)
        engine.submit_order(
            asset_no=0,
            order_id=1,
            side=Side.BUY,
            price=depth.best_ask,
            quantity=10,
            time_in_force=TimeInForce.FOK,
        )
        visible = engine.wait_order_response(0, 1, 50_000_000)
        order = engine.order(0, 1) if visible else None
```

`advance()` returns `False` when the requested clock step extends beyond the
remaining native events. `wait_order_response()` returns `False` on timeout.
`submit_order()` returns `None` or raises a typed submission/configuration
error. `depth()` returns `DepthView`; `feed_latency()` and `order_latency()`
return `FeedLatency | None` and `OrderLatency | None`; `order()` returns the
response-visible `OrderView | None`. These views are immutable and retain raw
nanosecond timestamps and unrounded native prices/quantities. `close()` is
explicit and idempotent; context-manager exit closes the engine, and later
operations raise `EngineClosedError`.

## Compact cache API and physical contract

Backends and new strategies use the root-package cache API:

```python
from hftbacktest_slim import (
    BBO_SCHEMA,
    COMPACT_BUILDER_VERSION,
    COMPACT_SCHEMA_VERSION,
    CompactBuildConfig,
    CompactCacheBudgetError,
    CompactCacheError,
    CompactCacheStore,
    CompactSource,
)
```

`COMPACT_SCHEMA_VERSION` remains `bbo_v1`. Its physical Arrow IPC File/Feather
V2 fields are fixed and nullable in this exact order:

```text
source_seq    uint64
exch_ts       int64
local_ts_raw  int64
bid_px        float64
ask_px        float64
bid_qty       float64
ask_qty       float64
last_px       float64
total_volume  int64
```

`source_seq` is mandatory. The aligned native `SLIM_ROW_DTYPE` is derived from
the same package schema module and remains a 72-byte structure. File metadata
records the schema, symbol/source/date, local-timestamp adjustment, and exact
exchange/local ordering. Empty symbols are valid files with the same schema.

Builder version `2` deliberately invalidates builder-version-1 cache
identities because implementation ownership and deterministic fingerprints
moved. The physical fields and matching behavior did not change. A cold date
streams projected Arrow record batches and scans each physical stock/futures
source once while routing every requested symbol; validated warm reuse performs
zero payload scans. Source and implementation identity validation uses source
stats, Parquet footer metadata, and completed compact files, never a hidden
second raw-data scan.

The default LZ4 compression also supports `none` and `zstd`. Before writing,
the builder estimates `source_rows * 96 * 1.20`, enforces the configured cache
cap and free-space reserve, and checks both again after every batch. It writes a
same-filesystem temporary date, validates closed files and deterministic
sidecars, writes the date manifest last, then atomically publishes. Failures
clean only that incomplete temporary date; completed cache, raw inputs, and
results are never automatically deleted.

Installable package commands retain the legacy arguments and JSON shapes:

```bash
hftbacktest-slim-build-cache \
  --date 2026-03-02 \
  --cache-root data/tw_compact_v1 \
  --stock-path /data/twstock_20260302.parquet \
  --spot-symbols 0050 2330

hftbacktest-slim-benchmark-read \
  --date 2026-03-02 \
  --cache-root data/tw_compact_v1 \
  --repetitions 3
```

They are also available as `python -m hftbacktest_slim.cli.build_cache` and
`python -m hftbacktest_slim.cli.benchmark_read`.

## Native library discovery

The library is resolved deterministically without a system-basename search:

1. `library_path=` passed to `SlimEngine`;
2. `HFTBACKTEST_SLIM_LIBRARY`;
3. a packaged `_native/`, `native/`, or package-root artifact;
4. the repository development artifact under root `target/release`; or
5. `NativeLibraryNotFoundError` listing the checked paths.

The library is loaded only when an engine is constructed. Importing
`hftbacktest_slim` does not load the shared object. `engine.library_path`
records the resolved diagnostic path. ABI values other than `1` raise
`AbiMismatchError` before engine construction.

Build the development artifact from the repository root:

```bash
cargo build --workspace --release
```

On Linux x86-64 this produces `target/release/libhbt_slim.so`.

## Compatibility and remaining migration work

The primary futures/spot slim path no longer imports the legacy
`scripts.slim_engine` path or `hftbacktest_slim.compat.hbt`. It constructs
neutral `AssetConfig` values and uses `SlimEngine`, `Side`, `TimeInForce`, and
`OrderStatus` through `future_spot`'s execution adapter. An independent example
under `examples/slim_two_asset_strategy/` also uses only the neutral public API.

`scripts.slim_engine` remains an import-only wrapper around
`hftbacktest_slim.compat.hbt`, which preserves HBT-shaped methods, numeric
constants, integer return codes, order mapping behavior, and historical no-op
cancel/clear calls for compatibility tests and callers not yet migrated. It is
not an extension surface; new strategies must use the neutral API.

The wrapper contains a temporary repository-local `src/` path bootstrap so the
existing uninstalled checkout continues to run. Phase 6 owns removal after all
remaining callers migrate.

`scripts.compact_cache` is now an import-only re-export and
`scripts/build_compact_cache.py` plus `scripts/benchmark_compact_read.py` are
thin CLI delegates. They emit no worker deprecation warnings. The explicit
reference-HftBacktest bridge remains outside the package at
`scripts/compact_hbt_adapter.py`; it imports the package schema but the package
never imports it. Strategy pricing, execution policy, carry, capital, and
reporting remain outside this package. Phase 5 changes result implementation
fingerprints and therefore invalidates old result manifests; compact-cache
identity is unchanged. Full-date, complete-month, and multi-date carry parity
were validated on mounted March 2026 market data and are recorded in the
migration inventory; this integration refactor makes no performance claim.

## Installation

The package uses a conventional `src` layout and declares Numba, NumPy, and
PyArrow:

```bash
python3 -m pip install /path/to/hftbacktest_slim
```
