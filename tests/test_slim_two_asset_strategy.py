from __future__ import annotations

import ast
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

PACKAGE_SOURCE = Path(__file__).resolve().parents[1] / "hftbacktest_slim" / "src"
if str(PACKAGE_SOURCE) not in sys.path:
    sys.path.insert(0, str(PACKAGE_SOURCE))

from examples.slim_two_asset_strategy import CrossedMarketProbe
from hftbacktest_slim import AssetConfig, OrderStatus
from hftbacktest_slim.market_data import BBO_SCHEMA


def _write_partition(path: Path, rows: list[tuple]) -> Path:
    table = pa.Table.from_pylist(
        [dict(zip(BBO_SCHEMA.names, row)) for row in rows], schema=BBO_SCHEMA
    ).replace_schema_metadata(
        {b"schema_version": b"bbo_v1", b"local_timestamp_adjustment_ns": b"0"}
    )
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return path


def test_second_strategy_uses_its_own_clock_and_neutral_order(tmp_path: Path) -> None:
    left = _write_partition(
        tmp_path / "left.arrow",
        [
            (0, 100, 100, 99.0, 100.0, 0.1, 0.1, 100.0, 1),
            (1, 110, 110, 99.0, 100.0, 0.1, 0.1, 100.0, 1),
            (2, 120, 120, 99.0, 100.0, 0.1, 0.1, 100.0, 1),
        ],
    )
    right = _write_partition(
        tmp_path / "right.arrow",
        [
            (0, 100, 100, 99.0, 100.0, 10.0, 10.0, 100.0, 1),
            (1, 110, 110, 101.0, 102.0, 10.0, 10.0, 101.0, 1),
            (2, 120, 120, 101.0, 102.0, 10.0, 10.0, 101.0, 1),
        ],
    )
    strategy = CrossedMarketProbe(
        (AssetConfig("LEFT", left, 1.0), AssetConfig("RIGHT", right, 1.0)),
        decision_step_ns=10,
        response_timeout_ns=10,
    )

    result = strategy.run(max_decisions=3)

    assert result.decisions == 1
    assert result.final_timestamp_ns == 110
    assert result.order is not None
    assert result.order.status is OrderStatus.FILLED
    assert result.order.execution_quantity == 1.0
    # The native no-partial-fill profile ignores displayed size as designed.
    assert result.order.execution_quantity > 0.1


def test_second_strategy_import_boundary() -> None:
    root = Path(__file__).resolve().parents[1] / "examples" / "slim_two_asset_strategy"
    forbidden = ("future_spot", "scripts.slim_engine", "hftbacktest_slim.compat")
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            for module in modules:
                if any(module == name or module.startswith(f"{name}.") for name in forbidden):
                    violations.append(f"{path.relative_to(root)}: {module}")
    assert violations == []
