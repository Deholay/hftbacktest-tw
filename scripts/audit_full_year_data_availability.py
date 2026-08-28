#!/usr/bin/env python3
"""Audit whether a requested full-year futures/spot benchmark is runnable."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _ranges(values: list[str]) -> list[dict[str, object]]:
    if not values:
        return []
    dates = [pd.Timestamp(value) for value in sorted(values)]
    groups: list[list[pd.Timestamp]] = [[dates[0]]]
    for value in dates[1:]:
        if (value - groups[-1][-1]).days <= 4:
            groups[-1].append(value)
        else:
            groups.append([value])
    return [
        {"start": str(group[0].date()), "end": str(group[-1].date()), "dates": len(group)}
        for group in groups
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calendar", type=Path, default=Path("future_spot/Calendar.csv"))
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--stock-template",
        default="/mnt/z/數據平台/ticker_store/daily_parquet/twstock_{date_nodash}.parquet",
    )
    parser.add_argument(
        "--futures-dir", type=Path, default=Path("/mnt/z/ticks_parquet_stock_future")
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    calendar = pd.read_csv(args.calendar, dtype=str)
    calendar = calendar.loc[
        calendar["trade_dates"].between(args.start_date, args.end_date, inclusive="both")
    ].copy()
    rows = []
    for row in calendar.itertuples(index=False):
        trade_date = str(row.trade_dates)
        previous_date = None if pd.isna(row.LDate) else str(row.LDate)
        stock_path = Path(
            args.stock_template.format(
                date=trade_date,
                date_dash=trade_date,
                date_nodash=trade_date.replace("-", ""),
            )
        )
        future_path = args.futures_dir / f"{trade_date}.parquet"
        prior_future_path = (
            args.futures_dir / f"{previous_date}.parquet" if previous_date else None
        )
        rows.append(
            {
                "trade_date": trade_date,
                "stock": stock_path.is_file(),
                "future": future_path.is_file(),
                "prior_future_for_universe": bool(
                    prior_future_path is not None and prior_future_path.is_file()
                ),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise ValueError("calendar contains no dates in the requested range")
    frame["runnable"] = frame[["stock", "future", "prior_future_for_universe"]].all(axis=1)
    missing = frame.loc[~frame["runnable"], "trade_date"].tolist()
    runnable = frame.loc[frame["runnable"], "trade_date"].tolist()
    payload = {
        "requested_start": args.start_date,
        "requested_end": args.end_date,
        "calendar_trade_dates": len(frame),
        "runnable_trade_dates": len(runnable),
        "first_runnable_date": runnable[0] if runnable else None,
        "last_runnable_date": runnable[-1] if runnable else None,
        "full_range_available": not missing,
        "missing_trade_dates": len(missing),
        "missing_ranges": _ranges(missing),
        "missing_by_input": {
            column: int((~frame[column]).sum())
            for column in ("stock", "future", "prior_future_for_universe")
        },
        "calendar": str(args.calendar.resolve()),
        "stock_template": args.stock_template,
        "futures_dir": str(args.futures_dir.resolve()),
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if payload["full_range_available"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
