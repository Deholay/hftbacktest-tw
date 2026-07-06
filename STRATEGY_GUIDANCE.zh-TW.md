# 策略接入指南

[English](STRATEGY_GUIDANCE.md)

專案層的策略接口放在 `scripts/strategy_api.py`。所有具體策略資料夾，包含 `future_spot/`，都應該寫 adapter 去接這個 root contract，不要各自長出一套 public interface。

Root `scripts/` 也負責放跨策略會共用的 HBT 工具：

- `scripts/hbt_types.py`：通用 HBT asset / fill dataclass。
- `scripts/hbt_common.py`：通用 queue model、order、latency、fill helper。
- `scripts/io_utils.py`：CSV / DataFrame / 時間轉換 helper。

策略資料夾只放自己 domain 真的需要的東西，例如 model、pricing、risk、execution、config parsing、output schema。

## Root Contract

策略 class 只要實作這個形狀：

```python
from scripts.strategy_api import StrategyContext, StrategyDecision


class MyStrategy:
    name = "my_strategy"

    def decide(self, context: StrategyContext) -> StrategyDecision:
        ...
```

`StrategyContext` 刻意做得很薄：

- `strategy_name`：顯示或 debug 用的策略名稱。
- `timestamp_ns`：可選的 decision timestamp。
- `payload`：策略自己的資料。payload 長什麼樣子，由該策略 package 自己定義、自己文件化。

`StrategyDecision` 是策略回給 runner 的結果：

- `action`：runner 看得懂的 string signal。
- `should_execute`：`False` 代表這次 signal 要被擋掉或跳過。
- `reason`：簡短說明為什麼 hold / skip，debug 時會用到。
- `metadata`：進階情境才需要放的 implementation object。

## future_spot 怎麼接

`future_spot` 的 adapter 在 `future_spot/arbitrage/strategy_adapter.py`。

預設 adapter 是 `FutureSpotPairStrategy`。它吃的 payload 是 `FutureSpotStrategyPayload`，裡面包含：

- `pair`
- `market`
- `pricing`
- `position`
- `enforce_risk_limits`

`HbtPairBacktester` 可以直接吃 root-compatible strategy：

```python
from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
from future_spot.arbitrage.strategy_adapter import FutureSpotPairStrategy

backtester = HbtPairBacktester(run_config, strategy=FutureSpotPairStrategy())
trades, summary = backtester.run()
```

如果沒有傳 strategy，`future_spot` 會用預設 adapter，行為維持原本的期貨 / 現貨套利邏輯。

## 新增另一個策略家族

1. 建一個新的策略資料夾。
2. Domain model 和 execution logic 放在那個策略資料夾裡。
3. 寫一個 adapter，吃 `scripts.strategy_api.StrategyContext`。
4. 把 adapter payload schema 寫清楚。
5. Notebook 和 CLI import root contract，再 import 具體策略 adapter。

驗證：

```bash
python3 -m py_compile scripts/*.py future_spot/arbitrage/*.py future_spot/scripts/*.py
```
