# hftbacktest-slim

`hftbacktest-slim` is the future shared package for the project-owned slim
market replay runtime. Version `0.3.0a0` is the Phase 1 package-boundary
skeleton: it provides only neutral configuration, enums, exceptions, and
version metadata. It does not yet provide a usable engine or compact-cache
implementation.

The native engine remains in `crates/hbt_slim/`, and the current Python binding
and compact-cache implementation remain in `scripts/slim_engine.py` and
`scripts/compact_cache.py`. They will move in later migration phases. Existing
runtime imports and consumers therefore continue to use their legacy locations
during Phase 1.

The future supported slim profile is deliberately constrained:

- compact BBO input;
- an explicit caller-controlled strategy clock;
- immediate crossing FOK or IOC limit orders;
- no partial fills and no displayed-size cap;
- independent feed, order-entry, and order-response latency; and
- no passive queue or arbitrary depth-sensitive behavior.

New strategies must use the neutral public API exposed from
`hftbacktest_slim`, once the engine API is implemented. They must not build on
the transitional HftBacktest-compatible facade. Futures/spot pricing, signals,
risk, execution policy, carry, expiry, capital replay, persistence, and
reporting remain owned by `future_spot` and are not part of this shared runtime.

## Phase 1 public API

Only root-package imports are supported as public contracts:

```python
from hftbacktest_slim import AssetConfig, Side, TimeInForce

asset = AssetConfig(
    symbol="0050",
    data_path="/data/compact/0050.arrow",
    tick_size=0.05,
    feed_latency_offset_ns=0,
    order_entry_latency_ns=1_000_000,
    order_response_latency_ns=1_000_000,
)
```

There is intentionally no `SlimEngine` or compact-cache export in Phase 1.
Importing internal modules is unsupported and must not be treated as a stable
backend contract.

## Installation

The skeleton has no runtime dependencies and uses a conventional `src`
layout. It can be installed from this directory without relying on the
repository as the current working directory:

```bash
python3 -m pip install --no-deps /path/to/hftbacktest_slim
```
