"""Small project-wide IO and dataframe helpers."""

from __future__ import annotations

import logging
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


def safe_filename(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z._-]+", "_", value)
