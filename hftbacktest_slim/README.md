# hftbacktest-slim

`hftbacktest-slim` is the project-owned, strategy-neutral compact-BBO replay
runtime. Version `0.3.0a1` completes migration Phase 3: the Python ctypes
binding, Arrow row reader, neutral engine lifecycle, models, and temporary HBT
compatibility facade now live in this standalone package. The native crate
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

Current futures/spot consumers continue to import the legacy
`scripts.slim_engine` path. That file is now an import-only wrapper around
`hftbacktest_slim.compat.hbt`, which preserves HBT-shaped methods, numeric
constants, integer return codes, order mapping behavior, and historical no-op
cancel/clear calls. New strategies must use the neutral API, not this
compatibility namespace.

The wrapper contains a temporary repository-local `src/` path bootstrap so the
existing uninstalled checkout continues to run. Phase 5/6 removes that shim
after consumers migrate.

Phase 4 still owns the move of compact schema, normalization, builder,
manifests, publication, and sidecars out of `scripts.compact_cache`; none moved
in Phase 3. Phase 5 still owns futures/spot migration to the neutral API.

## Installation

The package uses a conventional `src` layout and declares NumPy and PyArrow:

```bash
python3 -m pip install /path/to/hftbacktest_slim
```
