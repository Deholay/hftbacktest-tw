from __future__ import annotations

import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PACKAGE_ROOT / "src" / "hftbacktest_slim"


def _absolute_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imports.add(node.module)
            imports.update(f"{node.module}.{alias.name}" for alias in node.names)
    return imports


def test_package_has_no_forbidden_reverse_dependencies() -> None:
    violations: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for module in sorted(_absolute_imports(path)):
            root = module.partition(".")[0]
            explicitly_forbidden = (
                root in {"future_spot", "hftbacktest"}
                or module.startswith("scripts.hbt_")
                or module.startswith("scripts.tw_stock_")
            )
            if explicitly_forbidden:
                violations.append(f"{path.relative_to(PACKAGE_ROOT)} imports {module}")

    assert violations == [], "forbidden package dependencies:\n" + "\n".join(violations)
