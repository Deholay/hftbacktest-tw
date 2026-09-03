# hftbacktest-slim

[English](README_en.md)

`hftbacktest-slim` 是一套與策略無關的 compact-BBO 重播 runtime，結合 Python
API、Arrow cache layer，以及以 Rust 實作的 scheduler 與立即成交 matcher。

版本 `0.3.0` 刻意限制在以下執行情境：

- 每個 engine 固定使用兩項資產；
- 由呼叫端控制策略時鐘；
- 僅支援立即穿價的 FOK 與 IOC 限價單；
- 不允許 partial fill，也不以顯示數量限制成交量；
- feed、order-entry 與 order-response latency 各自獨立；
- 使用 compact `bbo_v1` Arrow 輸入。

被動委託、GTC/GTX、queue model、partial fill、任意深度策略與 live trading 均不在
本套件支援範圍內。

## 安裝

從原始碼安裝需要 Python 3.10 以上版本，以及包含 Cargo 的 Rust toolchain。

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
```

安裝流程會編譯 Rust library，並將 native artifact 安裝到 Python package 裡。
匯入 `hftbacktest_slim` 時不會立即載入 native library；建立 engine 時才會載入。

Editable 開發環境：

```bash
python -m pip install -e '.[dev]'
cargo build --workspace --release
```

## 重播兩項資產

每項資產都要指向符合下方 compact schema 的 Arrow IPC 檔案。

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

當要求的時間步長超過剩餘 events 時，`advance()` 會回傳 `False`；等待逾時時，
`wait_order_response()` 會回傳 `False`。`depth()`、`order()`、
`feed_latency()` 與 `order_latency()` 回傳的 view 都是 immutable。

## 建立 compact cache

Cache builder 會串流讀取 Parquet projected batches，將 Top-5 depth 正規化為 BBO，
再發布依商品分割的 Arrow 檔案與具版本控制的 manifest。

```bash
hftbacktest-slim-build-cache \
  --date 2026-03-02 \
  --cache-root /data/tw_compact_v1 \
  --stock-path /data/twstock_20260302.parquet \
  --spot-symbols 0050 2330 \
  --max-gb 200 \
  --min-free-gb 200
```

需要在同一日期建置期貨時，可一併指定：

```bash
hftbacktest-slim-build-cache \
  --date 2026-03-02 \
  --cache-root /data/tw_compact_v1 \
  --stock-path /data/twstock_20260302.parquet \
  --future-path /data/twfuture_20260302.parquet \
  --spot-symbols 0050 \
  --future-symbols NYF
```

查看完整參數：

```bash
hftbacktest-slim-build-cache --help
hftbacktest-slim-benchmark-read --help
```

Builder 預設使用 LZ4 壓縮、有限大小的 record batches、200 GB cache 上限，以及
200 GB 最低可用空間保留。它會先寫入 temporary directory，驗證已關閉的檔案，最後
寫入 manifest，再以 atomic rename 發布日期 partition。失敗時只會刪除該次未完成的
temporary partition。

## Compact data contract

`COMPACT_SCHEMA_VERSION` 為 `bbo_v1`。Arrow fields 可為 null，且順序固定如下：

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

`source_seq` 是確保相同時間戳排序可重現的必要欄位。檔案 metadata 會記錄 schema
version、商品、來源、日期、timestamp correction，以及 exchange/local ordering。
選取 BBO 前會先彙總 Top-5 中重複價格的數量。

Package root 提供以下公開 cache API：

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

## Native library 載入順序

`SlimEngine` 依下列順序尋找 native library：

1. 明確傳入的 `library_path=`；
2. `HFTBACKTEST_SLIM_LIBRARY`；
3. 安裝在 Python package 內的 native artifact；
4. 原始碼 checkout 中的 `target/release`。

手動建置開發用 artifact：

```bash
cargo build --workspace --release
export HFTBACKTEST_SLIM_LIBRARY="$PWD/target/release/libhbt_slim.so"
```

ABI 不相符或找不到 library 時會拋出具體的 typed error，不會退回搜尋系統層級的
library basename。

## 開發與發行檢查

```bash
cargo test --workspace --locked
cargo fmt --check --all
cargo clippy --workspace --all-targets --locked -- -D warnings
python -m pytest -q
python -m compileall -q src/hftbacktest_slim
python -m build
python -m twine check dist/*
```

Python package version、Rust crate version、native ABI、engine identity、compact schema
與 cache builder 會各自獨立版控。單純搬移 repository 不會改變 matching 行為或
`bbo_v1` schema。
