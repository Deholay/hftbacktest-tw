from __future__ import annotations

import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT.parent
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


def _repository_sibling_roots() -> set[str]:
    """Treat every repository-owned sibling as a reverse dependency.

    This is intentionally stronger than naming only the current `future_spot`
    strategy: a newly added strategy package is rejected automatically.
    """

    return {
        path.name
        for path in REPOSITORY_ROOT.iterdir()
        if path.is_dir() and not path.name.startswith(".") and path.name != PACKAGE_ROOT.name
    }


def test_package_has_no_forbidden_reverse_dependencies() -> None:
    repository_roots = _repository_sibling_roots()
    violations: list[str] = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        for module in sorted(_absolute_imports(path)):
            root = module.partition(".")[0]
            explicitly_forbidden = (
                root in {"future_spot", "hftbacktest"}
                or module.startswith("scripts.hbt_")
                or module.startswith("scripts.tw_stock_")
            )
            if explicitly_forbidden or root in repository_roots:
                violations.append(f"{path.relative_to(PACKAGE_ROOT)} imports {module}")

    assert violations == [], "forbidden package dependencies:\n" + "\n".join(violations)
