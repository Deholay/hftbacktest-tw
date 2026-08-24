# TW Stock HftBacktest 筆記

[English](README.md)

這個 repo 主要拿來做台股 L2 / top-5 market-by-price 資料的 `hftbacktest` 實驗。現在的整理方向是：root `scripts/` 放跨策略會共用的東西，各策略資料夾只放自己的 domain 實作。

## 專案架構

- `scripts/` 是專案層共用區，放策略接口、HBT helper、I/O helper。
- `scripts/strategy_api.py` 是之後所有策略要接的 root strategy interface。
- `future_spot/` 是其中一個策略實作：台股期貨 / 現貨套利。
- 之後如果要加新的策略資料夾，請接 `scripts.strategy_api`，不要反過來依賴 `future_spot`。
- `future_spot/scripts/` 只放期現套利自己的 CLI wrapper。只要是其他策略也可能用到的邏輯，就應該往 root `scripts/` 放。

策略怎麼接、notebook / CLI 怎麼用，可以看：

- `STRATEGY_GUIDANCE.md`
- `notebooks/hbt_strategy_interface_example.ipynb`

## 安裝

在 repo root 安裝共用 dependency：

```bash
python3 -m pip install -r requirements.txt
```

`requirements.txt` 會從 pip 安裝 `hftbacktest`，也會安裝這個 repo 的 notebook / 資料處理會用到的基本套件。`fubon_neo` 這類 live-trading 相關套件沒有放進去，因為目前保留的主要 workflow 是離線 HBT research。

主要 notebook 是 `notebooks/hftbacktest_TWStock.ipynb`。Notebook 盡量只當 runner：呼叫 function、顯示 DataFrame、做少量視覺化；可重用的轉檔、HBT setup、策略邏輯都放在 `scripts/`。

## 重要檔案

- `scripts/tw_stock_data_to_npz.py`：把台股 top-5 row 轉成 hftbacktest event `.npz`。
- `scripts/tw_stock_hftbacktest.py`：共用的 hftbacktest config、asset setup、BBO / state helper、import isolation。
- `scripts/tw_stock_strategies.py`：策略 runner 與 DataFrame summary helper。
- `scripts/strategy_api.py`：root strategy context / decision protocol，以及 optional registry。
- `scripts/hbt_types.py`：跟特定策略無關的 HBT asset / fill dataclass。
- `scripts/hbt_common.py`：通用的 HBT queue、order、latency、fill helper。
- `scripts/io_utils.py`：通用 CSV、DataFrame、時間轉換 helper。
- `future_spot/`：期貨 / 現貨套利策略，實作 root strategy interface。
- `notebooks/hbt_strategy_interface_example.ipynb`：新策略 notebook 可以參考的 root-level 範例。
- `requirements.txt`：notebook 和目前保留 runner 會用到的共用 Python dependency。
- `notebooks/hftbacktest_TWStock.ipynb`：目前主要的台股實驗 notebook。
- `notebooks/hftbacktest_TWETF.ipynb`：ETF daily parquet runner。
- `notebooks/hftbacktest_TWOddLot.ipynb`：零股 daily parquet runner。
- `notebooks/hftbacktest_TWStockFuture.ipynb`：股票期貨 daily parquet runner。

## 其他資料來源轉換

股票轉檔透過 `data_platform_client/data_stock/api` 讀 parquet store，先用 Polars LazyFrame 投影及過濾必要欄位，再把欄式 NumPy array 直接送入 Numba event builder。ETF、零股、股票期貨則吃 `path.toml` 裡設定的 daily parquet root，後面共用同一套 event builder、hftbacktest setup 和策略程式。

股票 top-5 schema 對照：

| Source | 跟股票 schema 一樣嗎？ | 欄位重點 | Converter |
| --- | --- | --- | --- |
| Stock | Yes | `ask_price1` / `ask_volume1` 從 index 16 / 17 開始。 | `convert_tw_stock_to_npz` |
| ETF | No | 23 欄；`ask_price1` 是 index 7，`bid_price1` 是 index 8；沒有各 level volume。 | `convert_tw_etf_to_npz` |
| Odd Lot | No | 跟 ETF 一樣是 23 欄 price-only layout；`symbol` 可能被存成整數，所以 `0050` 也要能對到 `50`。 | `convert_tw_odd_lot_to_npz` |
| Stock Future | No | 40 欄；`ask_price1` / `ask_volume1` / `bid_price1` / `bid_volume1` 從 index 7 / 8 / 9 / 10 開始。 | `convert_tw_stock_future_to_npz` |

ETF 和零股 parquet 只有 top-5 price，沒有 top-5 quantity。converter 預設用 `price_only_depth_qty=1.0`，讓 BBO / depth replay 可以跑起來；但只要策略結果跟 queue size 很有關，就要把它當成結構 replay 測試，不要當成真實 queue-volume model。

DataAPI 與 daily parquet 轉檔現在都直接用欄式 NumPy array 配合 Numba 建立 event，不會先把每列變成 Python dict。轉檔摘要會分別印出讀檔、event 建立、時間線整理與 NPZ 寫入時間。NPZ 預設仍維持壓縮；如果磁碟空間足夠、比較重視寫入及後續載入速度，可以用 `--npz-compression uncompressed`，Python 呼叫則傳入 `npz_compression="uncompressed"`。

期現套利 full-market runner 預設也使用 `--strategy-engine numba`。Numba
會在 compiled loop 裡推進 HBT、讀兩腿 BBO、計價並掃過連續的 `HOLD`；
只有遇到訊號、定期市場採樣或資料結束才回 Python。風控、下單、成交與
報表仍沿用原邏輯。需要逐步除錯或使用自訂 Strategy 時，改用
`--strategy-engine python`。

## 目前 Notebook Flow

1. 把台股 L2 / top-5 data 轉成 hftbacktest event data。
2. 建 `BacktestConfig`。
3. 跑 Strategy 1、Strategy 2、Strategy 3。
4. 顯示精簡版 DataFrame summary。

目前 sample config：

```python
SYMBOL = "0050"
START_DATE = "2026-02-23"
END_DATE = START_DATE
START_TIME = "09:30:00"
END_TIME = "10:00:00"
TICK_SIZE = 0.05
```

`TICK_SIZE` 一定要明確給，因為 hftbacktest 的 depth 是用 tick index 當 key。如果你正在懷疑轉檔結果，不要再從那份可疑的 event sample 反推 tick size。

## 策略

### Strategy 1: Aggressive BBO Fill

`run_aggressive_fill_strategy`

- 用 best ask 買。
- 用 best bid 賣。
- 設計上就是要立刻成交。
- 主要拿來檢查 event replay、order submission、timestamp、accounting 欄位有沒有通。

輸出會包含 order side、price、execution price / quantity、send order time、fill time、position、balance、equity、trade counter。

### Strategy 2: Passive Bid1 / Ask1 Queue Model Comparison

`run_queue_model_comparison`

- 在 bid1 掛 passive buy，在 ask1 掛 passive sell。
- 目前會跑多個 queue model，例如 `risk_adverse` 和 `log_prob`。
- 用來比較 fill timestamp 和 `queue_model_fill_delta_ns`。
- 會記錄 `send_order_time` 和 `fill_time`，比較好查 zero fill delta。

### Strategy 3: Simple Fixed Level Passive Order

`run_level_queue_model_comparison`

- 簡化版會固定打一個 book level，例如 `side="sell"`、`level=5`。
- quantity 比 Strategy 2 大，window 也比較長。
- 回傳時會同時給 requested `level` 和實際看到的 `actual_level`。

`actual_level` 很重要，因為可見 order book 不一定真的有第五個 distinct price level。如果資料裡 level 不夠，策略會退到最深的可見 level，並把實際使用的 level 記下來。

## HftBacktest Depth Model 注意事項

hftbacktest 的 `HashMapMarketDepth` 是用 integer tick index 存 book depth。

幾個容易踩雷的 API：

```python
depth.best_ask          # float price
depth.best_ask_tick     # integer tick
depth.ask_qty_at_tick   # expects integer tick, not float price
```

以 `tick_size = 0.05` 為例：

```python
price = 77.10
tick = round(price / 0.05)  # 1542
qty = depth.ask_qty_at_tick(tick)
```

不要寫 `depth.ask_qty_at_tick(77.10)`。這個 API 要的是 tick index，不是價格。

Strategy 3 會從 `best_ask_tick` 或 `best_bid_tick` 開始掃，透過 `ask_qty_at_tick` / `bid_qty_at_tick` 找非空 level。

## 台股 Top-5 Data 的限制

台股來源資料是 top-5 market-by-price，不是 market-by-order。

所以：

- 可以表示每個 price level 的可見 aggregate depth。
- 不能還原同價格裡每一張單的 queue position。
- 同價格內的 queue position 只能交給 hftbacktest queue model 去估。
- 同價格出現在 top-5 多個欄位時，必須先 aggregate，再輸出成 hftbacktest depth snapshot event。

converter 現在會把同價格的可見量加總：

```text
ask_price2 = 77.10, ask_volume2 = 20
ask_price3 = 77.10, ask_volume3 = 30

emitted depth at 77.10 = 50
```

這樣可以避免多筆 same-price snapshot event 在 hftbacktest depth 裡互相覆蓋。

## Event Conversion 注意事項

`tw_stock_data_to_npz.py` 會輸出 hftbacktest event row：

```text
ev, exch_ts, local_ts, px, qty, order_id, ival, fval
```

每一筆來源 row 都會被當成一張 top-5 book image：

1. 清掉該 side 可見 top-5 price range。
2. 寫入 aggregate 後的 snapshot level。
3. 視需要用 `total_volume` delta 推估 trade event。

來源資料沒有明確 trade side。converter 預設會用前一筆 BBO 來推估。

## 驗證指令

Compile 變更過的 script：

```powershell
python -m py_compile scripts/tw_stock_data_to_npz.py scripts/tw_stock_strategies.py
```

快速檢查 float price 有沒有被保留，以及 same-price aggregation 是否正確：

```python
from scripts.tw_stock_data_to_npz import iter_depth_events, event_kind, DEPTH_SNAPSHOT_EVENT, SELL_EVENT

row = {
    "ask_price1": "77.05", "ask_volume1": "10",
    "ask_price2": "77.10", "ask_volume2": "20",
    "ask_price3": "77.10", "ask_volume3": "30",
}

events = list(iter_depth_events(row, 1, 1, 3, 1.0))
asks = [
    (px, qty)
    for ev, _, _, px, qty, *_ in events
    if ev & SELL_EVENT and event_kind(ev) == DEPTH_SNAPSHOT_EVENT
]
print(asks)
```

預期輸出：

```text
[(77.05, 10.0), (77.1, 50.0)]
```
