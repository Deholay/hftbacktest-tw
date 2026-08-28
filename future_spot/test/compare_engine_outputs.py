#!/usr/bin/env python3
"""Exact persisted-output parity check for reference and slim engine runs."""

from __future__ import annotations

import argparse
from itertools import zip_longest
import json
from pathlib import Path

import pandas as pd


TABLES = (
    "summary",
    "trades",
    "market",
    "latency",
    "position_carry",
    "run_errors",
    "entry_exit",
    "entry_exit_index",
)
ALLOWED_ENGINE_DIAGNOSTICS = {
    "summary": {"strategy_engine", "scan_calls", "python_decisions"},
}
LEGACY_FILES = {
    "summary": "summary_all_daily_pairs.csv",
    "trades": "trades_all_daily_pairs.csv",
    "market": "market_all_daily_pairs.csv",
    "latency": "latency_all_daily_pairs.csv",
    "position_carry": "position_carry_status.csv",
    "run_errors": "run_errors.csv",
    "entry_exit": "entry_exit_all_daily_pairs.csv",
    "entry_exit_index": "entry_exit_index.csv",
}


def _read_dates(root: Path) -> list[str]:
    return sorted(path.name.split("=", 1)[1] for path in (root / "core" / "dates").glob("trade_date=*") if path.is_dir())


def _compare_frame(left: pd.DataFrame, right: pd.DataFrame, ignored: set[str]) -> dict:
    left = left.drop(columns=sorted(ignored & set(left.columns)))
    right = right.drop(columns=sorted(ignored & set(right.columns)))
    if left.shape != right.shape or list(left.columns) != list(right.columns):
        return {
            "equal": False,
            "left_shape": left.shape,
            "right_shape": right.shape,
            "column_order_equal": list(left.columns) == list(right.columns),
        }
    mismatched = []
    for column in left.columns:
        equal = left[column].eq(right[column]) | (left[column].isna() & right[column].isna())
        if not equal.all():
            index = int(equal[~equal].index[0])
            mismatched.append(
                {
                    "column": column,
                    "count": int((~equal).sum()),
                    "first_row": index,
                    "left": _scalar(left.loc[index, column]),
                    "right": _scalar(right.loc[index, column]),
                }
            )
    return {"equal": not mismatched, "rows": len(left), "mismatches": mismatched}


def _scalar(value):
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else str(value) if hasattr(value, "isoformat") else value


def _csv_dates(path: Path, chunk_rows: int) -> list[str]:
    dates: set[str] = set()
    try:
        for frame in pd.read_csv(path, usecols=["trade_date"], chunksize=chunk_rows, low_memory=False):
            dates.update(frame["trade_date"].dropna().astype(str))
    except (pd.errors.EmptyDataError, ValueError):
        return []
    return sorted(dates)


def _compare_csv(left_path: Path, right_path: Path, ignored: set[str], chunk_rows: int) -> dict:
    try:
        left_chunks = pd.read_csv(left_path, chunksize=chunk_rows, low_memory=False)
    except pd.errors.EmptyDataError:
        left_chunks = iter(())
    try:
        right_chunks = pd.read_csv(right_path, chunksize=chunk_rows, low_memory=False)
    except pd.errors.EmptyDataError:
        right_chunks = iter(())
    mismatches: dict[str, dict] = {}
    rows = 0
    for chunk_no, pair in enumerate(zip_longest(left_chunks, right_chunks), start=1):
        left, right = pair
        if left is None or right is None:
            return {
                "equal": False,
                "rows": rows,
                "chunk": chunk_no,
                "reason": "chunk_count_mismatch",
            }
        left = left.drop(columns=sorted(ignored & set(left.columns))).reset_index(drop=True)
        right = right.drop(columns=sorted(ignored & set(right.columns))).reset_index(drop=True)
        if left.shape != right.shape or list(left.columns) != list(right.columns):
            return {
                "equal": False,
                "rows": rows,
                "chunk": chunk_no,
                "left_shape": left.shape,
                "right_shape": right.shape,
                "column_order_equal": list(left.columns) == list(right.columns),
            }
        for column in left.columns:
            equal = left[column].eq(right[column]) | (left[column].isna() & right[column].isna())
            if equal.all():
                continue
            count = int((~equal).sum())
            first = int(equal[~equal].index[0])
            if column not in mismatches:
                mismatches[column] = {
                    "column": column,
                    "count": 0,
                    "first_row": rows + first,
                    "left": _scalar(left.loc[first, column]),
                    "right": _scalar(right.loc[first, column]),
                }
            mismatches[column]["count"] += count
        rows += len(left)
    return {"equal": not mismatches, "rows": rows, "mismatches": list(mismatches.values())}


def _compare_legacy_roots(reference: Path, slim: Path, chunk_rows: int) -> dict:
    reference_dates = _csv_dates(reference / LEGACY_FILES["summary"], chunk_rows)
    slim_dates = _csv_dates(slim / LEGACY_FILES["summary"], chunk_rows)
    tables = {}
    all_equal = reference_dates == slim_dates
    for table, filename in LEGACY_FILES.items():
        comparison = _compare_csv(
            reference / filename,
            slim / filename,
            ALLOWED_ENGINE_DIAGNOSTICS.get(table, set()),
            chunk_rows,
        )
        tables[table] = comparison
        all_equal &= comparison["equal"]
    return {
        "equal": all_equal,
        "mode": "legacy_streaming_csv",
        "reference": str(reference.resolve()),
        "slim": str(slim.resolve()),
        "reference_dates": reference_dates,
        "slim_dates": slim_dates,
        "chunk_rows": chunk_rows,
        "allowed_engine_diagnostics": {
            key: sorted(value) for key, value in ALLOWED_ENGINE_DIAGNOSTICS.items()
        },
        "tables": tables,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--slim", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--chunk-rows", type=int, default=50_000)
    args = parser.parse_args()
    if args.chunk_rows <= 0:
        parser.error("--chunk-rows must be positive")
    reference_dates = _read_dates(args.reference)
    slim_dates = _read_dates(args.slim)
    if not reference_dates and all((args.reference / name).is_file() for name in LEGACY_FILES.values()):
        payload = _compare_legacy_roots(args.reference, args.slim, args.chunk_rows)
        rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        print(rendered, end="")
        return 0 if payload["equal"] else 1
    results = {}
    all_equal = reference_dates == slim_dates
    for trade_date in sorted(set(reference_dates) & set(slim_dates)):
        date_results = {}
        for table in TABLES:
            left = pd.read_parquet(args.reference / "core" / "dates" / f"trade_date={trade_date}" / f"{table}.parquet")
            right = pd.read_parquet(args.slim / "core" / "dates" / f"trade_date={trade_date}" / f"{table}.parquet")
            comparison = _compare_frame(left, right, ALLOWED_ENGINE_DIAGNOSTICS.get(table, set()))
            date_results[table] = comparison
            all_equal &= comparison["equal"]
        results[trade_date] = date_results
    payload = {
        "equal": all_equal,
        "reference": str(args.reference.resolve()),
        "slim": str(args.slim.resolve()),
        "reference_dates": reference_dates,
        "slim_dates": slim_dates,
        "allowed_engine_diagnostics": {key: sorted(value) for key, value in ALLOWED_ENGINE_DIAGNOSTICS.items()},
        "dates": results,
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if all_equal else 1


if __name__ == "__main__":
    raise SystemExit(main())
