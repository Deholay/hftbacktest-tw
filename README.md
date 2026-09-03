# hftbacktest-slim

`hftbacktest-slim` is a strategy-neutral compact-BBO replay runtime. It combines
a Python API and Arrow cache layer with a Rust scheduler and immediate matcher.

Version `0.3.0` supports a deliberately narrow execution profile:

- exactly two assets per engine;
- caller-controlled strategy clocks;
- immediate crossing FOK and IOC limit orders;
- no partial fills or displayed-size cap;
- independent feed, order-entry, and order-response latency; and
- compact `bbo_v1` Arrow input.

Passive orders, GTC/GTX, queue models, partial fills, arbitrary depth strategies,
and live trading are outside this package's supported contract.

## Install

Python 3.10 or newer and a Rust toolchain with Cargo are required when installing
from source.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
```

The build compiles the Rust library and installs it inside the Python package.
Importing `hftbacktest_slim` is lazy and does not load the native library until
an engine is created.

For editable development:

```bash
python -m pip install -e '.[dev]'
cargo build --workspace --release
```

## Replay two assets

Each asset points to an Arrow IPC file using the compact schema described below.

```python
from hftbacktest_slim import AssetConfig, Side, SlimEngine, TimeInForce

assets = [
    AssetConfig(
        symbol="0050",
        data_path="/data/compact/0050.arrow",
        tick_size=0.05,
        order_entry_latency_ns=1_000_000,
        order_response_latency_ns=1_000_000,
    ),
    AssetConfig(
        symbol="NYF",
        data_path="/data/compact/NYF.arrow",
        tick_size=1.0,
    ),
]

with SlimEngine(assets) as engine:
    while engine.advance(1_000_000_000):
        depth = engine.depth(0)
        if depth.best_ask <= 0:
            continue

        engine.submit_order(
            asset_no=0,
            order_id=1,
            side=Side.BUY,
            price=depth.best_ask,
            quantity=10,
            time_in_force=TimeInForce.FOK,
        )
        if engine.wait_order_response(0, 1, 50_000_000):
            order = engine.order(0, 1)
            print(order)
        break
```

`advance()` returns `False` when the requested step extends beyond the remaining
events. `wait_order_response()` returns `False` on timeout. Views returned by
`depth()`, `order()`, `feed_latency()`, and `order_latency()` are immutable.

## Build a compact cache

The cache builder reads projected Parquet batches, normalizes Top-5 depth into
BBO, and publishes symbol-partitioned Arrow files with a versioned manifest.

```bash
hftbacktest-slim-build-cache \
  --date 2026-03-02 \
  --cache-root /data/tw_compact_v1 \
  --stock-path /data/twstock_20260302.parquet \
  --spot-symbols 0050 2330 \
  --max-gb 200 \
  --min-free-gb 200
```

Add futures in the same date build when needed:

```bash
hftbacktest-slim-build-cache \
  --date 2026-03-02 \
  --cache-root /data/tw_compact_v1 \
  --stock-path /data/twstock_20260302.parquet \
  --future-path /data/twfuture_20260302.parquet \
  --spot-symbols 0050 \
  --future-symbols NYF
```

Inspect every available option with:

```bash
hftbacktest-slim-build-cache --help
hftbacktest-slim-benchmark-read --help
```

The builder defaults to LZ4 compression, bounded record batches, a 200 GB cache
cap, and a 200 GB free-space reserve. It writes into a temporary directory,
validates completed files, writes the manifest last, and atomically publishes
the date. Failed builds remove only their incomplete temporary partition.

## Compact data contract

`COMPACT_SCHEMA_VERSION` is `bbo_v1`. Arrow fields are nullable and ordered as:

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

`source_seq` is mandatory for deterministic equal-time ordering. The file
metadata records schema version, symbol, source, date, timestamp correction,
and exchange/local ordering. Repeated Top-5 prices are aggregated before BBO
selection.

The public cache API is available from the package root:

```python
from hftbacktest_slim import (
    BBO_SCHEMA,
    COMPACT_BUILDER_VERSION,
    COMPACT_SCHEMA_VERSION,
    CompactBuildConfig,
    CompactCacheStore,
    CompactSource,
)
```

## Native library resolution

`SlimEngine` resolves the native library in this order:

1. its explicit `library_path=` argument;
2. `HFTBACKTEST_SLIM_LIBRARY`;
3. the native artifact installed inside the Python package; and
4. `target/release` in a source checkout.

To build the development artifact manually:

```bash
cargo build --workspace --release
export HFTBACKTEST_SLIM_LIBRARY="$PWD/target/release/libhbt_slim.so"
```

An ABI mismatch or missing library raises a typed error instead of falling back
to a system-wide basename search.

## Development and release checks

```bash
cargo test --workspace --locked
cargo fmt --check --all
cargo clippy --workspace --all-targets --locked -- -D warnings
python -m pytest -q
python -m compileall -q src/hftbacktest_slim
python -m build
python -m twine check dist/*
```

The Python package version, Rust crate version, native ABI, engine identity,
compact schema, and cache builder are versioned independently. A repository
relocation alone does not change matching behavior or the `bbo_v1` schema.
