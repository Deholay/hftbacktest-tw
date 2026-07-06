# Strategy Integration Guidance

[繁體中文](STRATEGY_GUIDANCE.zh-TW.md)

The project-level strategy interface lives in `scripts/strategy_api.py`.
Concrete strategy folders, including `future_spot/`, should implement adapters
against this root contract instead of defining their own public interface.

Root `scripts/` is also where cross-strategy HBT utilities belong:

- `scripts/hbt_types.py`: generic HBT asset/fill dataclasses.
- `scripts/hbt_common.py`: generic queue model, order, latency, and fill helpers.
- `scripts/io_utils.py`: CSV/DataFrame/time conversion helpers.

Strategy folders should only keep domain-specific models, pricing, risk,
execution, config parsing, and output schemas.

## Root Contract

Implement a class with:

```python
from scripts.strategy_api import StrategyContext, StrategyDecision


class MyStrategy:
    name = "my_strategy"

    def decide(self, context: StrategyContext) -> StrategyDecision:
        ...
```

`StrategyContext` is intentionally generic:

- `strategy_name`: display/debug name.
- `timestamp_ns`: optional decision timestamp.
- `payload`: strategy-specific data. The strategy package owns and documents
  the payload schema.

`StrategyDecision` returns:

- `action`: a string signal understood by the strategy runner.
- `should_execute`: `False` means the signal is blocked or skipped.
- `reason`: concise reason for hold/skip/debug output.
- `metadata`: optional implementation objects for advanced consumers.

## future_spot Implementation

`future_spot` adapts the root API in
`future_spot/arbitrage/strategy_adapter.py`.

The default adapter is `FutureSpotPairStrategy`. It expects a
`FutureSpotStrategyPayload` containing:

- `pair`
- `market`
- `pricing`
- `position`
- `enforce_risk_limits`

`HbtPairBacktester` accepts a custom root-compatible strategy:

```python
from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
from future_spot.arbitrage.strategy_adapter import FutureSpotPairStrategy

backtester = HbtPairBacktester(run_config, strategy=FutureSpotPairStrategy())
trades, summary = backtester.run()
```

If no strategy is provided, `future_spot` uses the default adapter and preserves
the previous futures/spot arbitrage behavior.

## Adding Another Strategy Family

1. Create a new folder for the strategy implementation.
2. Keep domain models and execution logic inside that folder.
3. Implement an adapter that consumes `scripts.strategy_api.StrategyContext`.
4. Document the adapter payload schema.
5. Keep notebooks and CLIs importing the root contract plus the concrete
   strategy adapter.

Validation:

```bash
python3 -m py_compile scripts/*.py future_spot/arbitrage/*.py future_spot/scripts/*.py
```
