"""Small project-wide IO and dataframe helpers."""

from __future__ import annotations

import logging
import math
import numbers
import re
from pathlib import Path

import pandas as pd


def ms_to_ns(value: float) -> int:
    return int(round(value * 1_000_000))


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    return pd.concat(non_empty, ignore_index=True, sort=False) if non_empty else pd.DataFrame()


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    logging.info("wrote %s rows to %s", len(frame), path)


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Write a frame without routing nullable Python integers through float64.

    DataFrames assembled from heterogeneous execution rows can leave optional
    nanosecond timestamps as ``object`` columns containing Python ``int`` and
    missing values. PyArrow otherwise attempts to infer a double for these
    columns, which cannot exactly represent epoch nanoseconds.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    parquet_frame = frame.copy()
    for name in parquet_frame.select_dtypes(include="object").columns:
        values = parquet_frame[name]
        present = values[values.notna()]
        if not present.empty and present.map(_is_integer_compatible).all():
            parquet_frame[name] = pd.array(values, dtype="Int64")
    parquet_frame.to_parquet(path, index=False)
    logging.info("wrote %s rows to %s", len(frame), path)


def _is_integer_compatible(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        return False
    if isinstance(value, numbers.Integral):
        return True
    return math.isfinite(float(value)) and float(value).is_integer()


def safe_filename(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z._-]+", "_", value)
