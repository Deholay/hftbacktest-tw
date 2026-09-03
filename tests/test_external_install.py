from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = PACKAGE_ROOT


def test_local_install_and_import_work_outside_repository(
    tmp_path: Path, write_partition
) -> None:
    target = tmp_path / "target"
    outside = tmp_path / "outside"
    source = tmp_path / "local-source"
    outside.mkdir()
    source.mkdir()
    shutil.copytree(
        PACKAGE_ROOT / "src",
        source / "src",
        ignore=shutil.ignore_patterns(
            "__pycache__", "*.egg-info", "*.so", "*.dylib", "*.dll", "*.pyd"
        ),
    )
    shutil.copytree(PACKAGE_ROOT / "native", source / "native")
    for name in (
        "Cargo.lock",
        "Cargo.toml",
        "MANIFEST.in",
        "README.md",
        "README_en.md",
        "pyproject.toml",
    ):
        shutil.copy2(PACKAGE_ROOT / name, source / name)
    build_python = Path(sys.executable)
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment["PYTHONNOUSERSITE"] = "1"

    installed = subprocess.run(
        [
            str(build_python),
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
assert hftbacktest_slim.__version__ == "0.3.0"
assert importlib.metadata.version("hftbacktest-slim") == "0.3.0"
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
        [str(build_python), "-I", "-c", code],
        cwd=outside,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert imported.returncode == 0, imported.stdout + imported.stderr

    left = write_partition(
        tmp_path / "installed-left.arrow",
        [(0, 100, 110, 99.0, 101.0, 1.0, 1.0, 100.0, 1)],
    )
    right = write_partition(
        tmp_path / "installed-right.arrow",
        [(0, 100, 110, 199.0, 201.0, 1.0, 1.0, 200.0, 1)],
    )
    engine_code = f"""
import sys
sys.path.insert(0, {str(target)!r})
from hftbacktest_slim import (
    AssetConfig,
    BBO_SCHEMA,
    COMPACT_BUILDER_VERSION,
    CompactBuildConfig,
    CompactCacheStore,
    SlimEngine,
)
assert BBO_SCHEMA.names[0] == 'source_seq'
assert COMPACT_BUILDER_VERSION == 2
assert CompactCacheStore(CompactBuildConfig(
    cache_root={str(tmp_path / 'installed-cache')!r},
    max_cache_bytes=0,
    min_free_bytes=0,
)).root.name == 'installed-cache'
assets = [
    AssetConfig('A', {str(left)!r}, 1.0),
    AssetConfig('B', {str(right)!r}, 1.0),
]
with SlimEngine(assets) as engine:
    assert engine.library_path.parent.name == '_native'
    assert engine.advance(10)
    assert engine.depth(0).best_ask == 101.0
"""
    engine_opened = subprocess.run(
        [sys.executable, "-I", "-c", engine_code],
        cwd=outside,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert engine_opened.returncode == 0, engine_opened.stdout + engine_opened.stderr
