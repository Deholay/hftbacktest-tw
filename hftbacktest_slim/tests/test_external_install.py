from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT.parent


def test_local_install_and_import_work_outside_repository(tmp_path: Path) -> None:
    target = tmp_path / "target"
    outside = tmp_path / "outside"
    source = tmp_path / "local-source"
    outside.mkdir()
    shutil.copytree(
        PACKAGE_ROOT,
        source,
        ignore=shutil.ignore_patterns(
            ".pytest_cache", "__pycache__", "*.egg-info", "build", "dist"
        ),
    )
    base_python = Path(getattr(sys, "_base_executable", sys.executable)).resolve()
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment["PYTHONNOUSERSITE"] = "1"

    installed = subprocess.run(
        [
            str(base_python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-index",
            "--no-deps",
            "--no-build-isolation",
            "--target",
            str(target),
            str(source),
        ],
        cwd=outside,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert installed.returncode == 0, installed.stdout + installed.stderr

    code = f"""
import importlib.metadata
import pathlib
import sys

sys.path.insert(0, {str(target)!r})
import hftbacktest_slim

repository = pathlib.Path({str(REPOSITORY_ROOT)!r}).resolve()
cwd = pathlib.Path.cwd().resolve()
assert cwd != repository and repository not in cwd.parents
assert hftbacktest_slim.__version__ == "0.3.0a0"
assert importlib.metadata.version("hftbacktest-slim") == "0.3.0a0"
assert hftbacktest_slim.AssetConfig("0050", "0050.arrow", 0.05).symbol == "0050"
forbidden = sorted(
    name
    for name in sys.modules
    if name == "hftbacktest"
    or name.startswith("hftbacktest.")
    or name == "future_spot"
    or name.startswith("future_spot.")
    or name == "scripts"
    or name.startswith("scripts.")
)
assert forbidden == [], forbidden
"""
    imported = subprocess.run(
        [str(base_python), "-I", "-c", code],
        cwd=outside,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert imported.returncode == 0, imported.stdout + imported.stderr
