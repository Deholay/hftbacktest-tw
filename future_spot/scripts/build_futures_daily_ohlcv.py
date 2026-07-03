from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


DEFAULT_COLUMNS = [
    "symbol",
    "exchtime",
    "last_price",
    "open",
    "high",
    "low",
    "trade_volume",
    "total_volume",
    "status",
]


@dataclass
class SymbolAgg:
    symbol: str
    date: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: int = 0
    first_exchtime: pd.Timestamp | None = None
    last_exchtime: pd.Timestamp | None = None
    ticks: int = 0

    def update(self, row: pd.Series) -> None:
        first_price = _to_float(row.get("first_price"))
        max_price = _to_float(row.get("max_price"))
        min_price = _to_float(row.get("min_price"))
        last_price = _to_float(row.get("last_price"))
        max_volume = _to_int(row.get("max_volume"))
        first_exchtime = row.get("first_exchtime")
        last_exchtime = row.get("last_exchtime")

        if self.open is None and first_price is not None:
            self.open = first_price
            self.first_exchtime = first_exchtime
        if max_price is not None:
            self.high = max_price if self.high is None else max(self.high, max_price)
        if min_price is not None:
            self.low = min_price if self.low is None else min(self.low, min_price)
        if last_price is not None:
            self.close = last_price
            self.last_exchtime = last_exchtime
        self.volume = max(self.volume, max_volume)
        self.ticks += _to_int(row.get("ticks"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build per-symbol daily OHLCV from futures tick parquet files."
    )
    parser.add_argument(
        "--input-dir",
        default=r"Z:\ticks_parquet_stock_future",
        help="Directory containing daily futures parquet files.",
    )
    parser.add_argument(
        "--output",
        default="futures_daily_ohlcv.parquet",
        help="Output path. Use .parquet or .csv.",
    )
    parser.add_argument(
        "--pattern",
        default="*.parquet",
        help="Input filename glob pattern.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Only process files with date >= YYYY-MM-DD. Date is parsed from filename stem.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Only process files with date <= YYYY-MM-DD. Date is parsed from filename stem.",
    )
    parser.add_argument(
        "--session-start",
        default="09:00:00",
        help="Asia/Taipei session start time. Use empty string to disable time filtering.",
    )
    parser.add_argument(
        "--session-end",
        default="13:25:00",
        help="Asia/Taipei session end time. Use empty string to disable time filtering.",
    )
    parser.add_argument(
        "--include-trial",
        action="store_true",
        help="Keep trial-match rows. Default filters status trial_status_tag == 1 when status exists.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500_000,
        help="Parquet batch size.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    files = list(iter_input_files(Path(args.input_dir), args.pattern, args.start_date, args.end_date))
    if not files:
        raise SystemExit(f"No parquet files found under {args.input_dir!r}")

    records: list[dict[str, object]] = []
    for index, path in enumerate(files, start=1):
        logging.info("processing %s/%s %s", index, len(files), path)
        daily = process_file(
            path=path,
            batch_size=args.batch_size,
            session_start=args.session_start or None,
            session_end=args.session_end or None,
            filter_trial=not args.include_trial,
        )
        logging.info("finished %s rows=%s symbols=%s", path.name, daily["ticks"].sum(), len(daily))
        records.extend(daily.to_dict("records"))

    result = pd.DataFrame(records)
    if result.empty:
        raise SystemExit("No OHLCV rows were produced.")

    result = result.sort_values(["date", "symbol"]).reset_index(drop=True)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".csv":
        result.to_csv(output, index=False, encoding="utf-8-sig")
    else:
        result.to_parquet(output, index=False)
    logging.info("wrote %s rows to %s", len(result), output)


def iter_input_files(
    input_dir: Path,
    pattern: str,
    start_date: str | None,
    end_date: str | None,
) -> Iterable[Path]:
    for path in sorted(input_dir.glob(pattern)):
        if not path.is_file():
            continue
        date = path.stem
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue
        yield path


def process_file(
    path: Path,
    batch_size: int,
    session_start: str | None,
    session_end: str | None,
    filter_trial: bool,
) -> pd.DataFrame:
    parquet_file = pq.ParquetFile(path)
    columns = [column for column in DEFAULT_COLUMNS if column in parquet_file.schema.names]
    required = {"symbol", "exchtime", "last_price"}
    missing = required - set(columns)
    if missing:
        raise RuntimeError(f"{path} missing required columns: {sorted(missing)}")

    aggregations: dict[str, SymbolAgg] = {}
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns, use_threads=True):
        frame = batch.to_pandas()
        if frame.empty:
            continue
        frame = filter_frame(frame, session_start, session_end, filter_trial)
        if frame.empty:
            continue
        batch_agg = aggregate_batch(frame, date=path.stem)
        for _, row in batch_agg.iterrows():
            symbol = str(row["symbol"])
            if symbol not in aggregations:
                aggregations[symbol] = SymbolAgg(symbol=symbol, date=path.stem)
            aggregations[symbol].update(row)

    rows = [
        {
            "date": agg.date,
            "symbol": agg.symbol,
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": agg.volume,
            "first_exchtime": agg.first_exchtime,
            "last_exchtime": agg.last_exchtime,
            "ticks": agg.ticks,
        }
        for agg in aggregations.values()
        if agg.open is not None and agg.close is not None
    ]
    return pd.DataFrame(rows)


def filter_frame(
    frame: pd.DataFrame,
    session_start: str | None,
    session_end: str | None,
    filter_trial: bool,
) -> pd.DataFrame:
    frame = frame[frame["last_price"].fillna(0) > 0].copy()
    if frame.empty:
        return frame

    frame["exchtime_tw"] = pd.to_datetime(frame["exchtime"], unit="ns") + pd.Timedelta(hours=8)

    if session_start:
        frame = frame[frame["exchtime_tw"].dt.time >= pd.Timestamp(session_start).time()]
    if session_end:
        frame = frame[frame["exchtime_tw"].dt.time <= pd.Timestamp(session_end).time()]

    if filter_trial and "status" in frame.columns:
        status = frame["status"].fillna(0).to_numpy(dtype=np.uint32, copy=False)
        trial_status_tag = (status >> np.uint32(23)) & np.uint32(1)
        frame = frame[trial_status_tag == 0]

    return frame


def aggregate_batch(frame: pd.DataFrame, date: str) -> pd.DataFrame:
    frame = frame.sort_values(["symbol", "exchtime"])
    volume_col = "total_volume" if "total_volume" in frame.columns else "trade_volume"

    grouped = frame.groupby("symbol", sort=False, observed=True)
    result = grouped.agg(
        first_price=("last_price", "first"),
        max_price=("last_price", "max"),
        min_price=("last_price", "min"),
        last_price=("last_price", "last"),
        max_volume=(volume_col, "max"),
        first_exchtime=("exchtime_tw", "first"),
        last_exchtime=("exchtime_tw", "last"),
        ticks=("last_price", "size"),
    ).reset_index()
    result.insert(0, "date", date)
    return result


def _to_float(value: object) -> float | None:
    if pd.isna(value):
        return None
    value = float(value)
    return value if value > 0 else None


def _to_int(value: object) -> int:
    if pd.isna(value):
        return 0
    return int(value)


if __name__ == "__main__":
    main()
