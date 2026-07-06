"""Project-wide strategy integration contracts.

Strategy implementations should depend on these small protocols instead of
depending on another concrete strategy package. Domain-specific packages can
place rich objects in ``StrategyContext.payload`` and document their schema.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol


@dataclass(frozen=True)
class StrategyContext:
    """Implementation-neutral context passed to a strategy decision."""

    strategy_name: str
    timestamp_ns: int | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyDecision:
    """Implementation-neutral strategy decision.

    ``action`` is intentionally a string so strategy packages can expose their
    own signal vocabulary without forcing a root-level enum.
    """

    action: str
    should_execute: bool = True
    reason: str = "ok"
    metadata: Mapping[str, Any] = field(default_factory=dict)


class Strategy(Protocol):
    """Decision interface implemented by concrete strategies."""

    def decide(self, context: StrategyContext) -> StrategyDecision:
        ...


class StrategyRunner(Protocol):
    """Runnable strategy/backtest interface for notebooks and CLIs."""

    def run(self) -> Any:
        ...


StrategyFactory = Callable[..., Strategy]

_REGISTRY: dict[str, StrategyFactory] = {}


def register_strategy(name: str, factory: StrategyFactory, *, replace: bool = False) -> None:
    """Register a strategy factory for lightweight notebook/CLI discovery."""

    if not replace and name in _REGISTRY:
        raise ValueError(f"strategy already registered: {name}")
    _REGISTRY[name] = factory


def get_strategy_factory(name: str) -> StrategyFactory:
    try:
        return _REGISTRY[name]
    except KeyError as exc:
        raise KeyError(f"unknown strategy: {name}") from exc


def create_strategy(name: str, *args: Any, **kwargs: Any) -> Strategy:
    return get_strategy_factory(name)(*args, **kwargs)


def registered_strategies() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))
