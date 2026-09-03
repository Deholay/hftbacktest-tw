from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from hftbacktest_slim import AbiMismatchError, NativeLibraryNotFoundError
from hftbacktest_slim.engine import binding


def test_explicit_path_has_priority_over_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    explicit = tmp_path / "explicit.so"
    environment = tmp_path / "environment.so"
    explicit.touch()
    environment.touch()
    monkeypatch.setenv(binding.LIBRARY_ENVIRONMENT_VARIABLE, str(environment))
    assert binding.resolve_library_path(explicit) == explicit.resolve()


def test_environment_path_has_priority_over_packaged_and_development(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    environment = tmp_path / "environment.so"
    environment.touch()
    monkeypatch.setenv(binding.LIBRARY_ENVIRONMENT_VARIABLE, str(environment))
    assert binding.resolve_library_path() == environment.resolve()


def test_development_fallback_is_used_after_missing_packaged_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    development = tmp_path / "target" / "release" / binding.LIBRARY_FILENAME
    development.parent.mkdir(parents=True)
    development.touch()
    monkeypatch.delenv(binding.LIBRARY_ENVIRONMENT_VARIABLE, raising=False)
    monkeypatch.setattr(
        binding,
        "package_library_candidates",
        lambda: (tmp_path / "missing-packaged.so",),
    )
    monkeypatch.setattr(binding, "development_library_path", lambda: development)
    assert binding.resolve_library_path() == development.resolve()


def test_packaged_location_has_priority_over_development(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packaged = tmp_path / "package" / binding.LIBRARY_FILENAME
    development = tmp_path / "target" / "release" / binding.LIBRARY_FILENAME
    packaged.parent.mkdir(parents=True)
    development.parent.mkdir(parents=True)
    packaged.touch()
    development.touch()
    monkeypatch.delenv(binding.LIBRARY_ENVIRONMENT_VARIABLE, raising=False)
    monkeypatch.setattr(binding, "package_library_candidates", lambda: (packaged,))
    monkeypatch.setattr(binding, "development_library_path", lambda: development)
    assert binding.resolve_library_path() == packaged.resolve()


def test_missing_library_error_is_typed_and_lists_resolution_help(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(binding.LIBRARY_ENVIRONMENT_VARIABLE, raising=False)
    monkeypatch.setattr(
        binding,
        "package_library_candidates",
        lambda: (tmp_path / "missing-packaged.so",),
    )
    monkeypatch.setattr(
        binding, "development_library_path", lambda: tmp_path / "missing-development.so"
    )
    with pytest.raises(NativeLibraryNotFoundError, match="cargo build.*HFTBACKTEST_SLIM_LIBRARY"):
        binding.resolve_library_path()


class _FakeFunction:
    def __init__(self, result=0):
        self.result = result
        self.argtypes = None
        self.restype = None

    def __call__(self, *_args):
        return self.result


class _WrongAbiLibrary:
    def __init__(self) -> None:
        for name in (
            "hbt_slim_create",
            "hbt_slim_free",
            "hbt_slim_current_timestamp",
            "hbt_slim_elapse",
            "hbt_slim_depth",
            "hbt_slim_feed_latency",
            "hbt_slim_order_latency",
            "hbt_slim_submit",
            "hbt_slim_wait_order_response",
            "hbt_slim_order",
        ):
            setattr(self, name, _FakeFunction())
        self.hbt_slim_version = _FakeFunction(99)


def test_abi_mismatch_is_typed_and_reports_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "wrong-abi.so"
    path.touch()
    monkeypatch.setattr(binding.ctypes, "CDLL", lambda _path: _WrongAbiLibrary())
    with pytest.raises(AbiMismatchError, match=r"expected 1, got 99") as caught:
        binding.NativeBinding(path)
    assert str(path.resolve()) in str(caught.value)


def test_root_import_is_lazy_and_does_not_import_forbidden_packages() -> None:
    source_root = Path(__file__).resolve().parents[1] / "src"
    code = """
import ctypes
import sys
def fail(*args, **kwargs):
    raise AssertionError('ctypes.CDLL called during package import')
ctypes.CDLL = fail
import hftbacktest_slim
forbidden = [
    name for name in sys.modules
    if name == 'hftbacktest' or name.startswith('hftbacktest.')
    or name == 'future_spot' or name.startswith('future_spot.')
    or name == 'scripts' or name.startswith('scripts.')
]
assert forbidden == [], forbidden
assert hftbacktest_slim.SlimEngine is not None
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(source_root)
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=source_root.parent,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
