#!/usr/bin/env python3
"""
Convert Taiwan stock top-5 L2 CSV rows into HftBacktest Event data.

Input assumption:
    Each CSV row is a full top-5 book image for one symbol, with columns like:

    symbol,symbol_id,exchtime,localtime,status,last_price,previous_close,open,
    high_limit,low_limit,total_trade,total_volume,total_value,
    average_ask_price,average_bid_price,total_ask_volume,total_bid_volume,
    ask_price1,ask_volume1,bid_price1,bid_volume1,...,ask_price5,ask_volume5,
    bid_price5,bid_volume5,sequence

Output:
    A compressed npz file with key "data", using HftBacktest event_dtype:
    ev, exch_ts, local_ts, px, qty, order_id, ival, fval

Important limitations:
    - Top-5 L2 is aggregate market-by-price data. Queue position still has to be
      modeled by the backtester.
    - Trade side is not explicit in the provided columns. This script infers it
      from the previous row's BBO by default.
    - total_volume delta is used as trade quantity. Use --volume-scale if your
      raw field is in board lots and your strategy expects shares, or vice versa.
    - Every row is emitted as a top-5 snapshot: clear visible top-5 range, then
      insert snapshot levels. This is robust for BBO replay but does not recover
      depth outside the visible range.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator
from zoneinfo import ZoneInfo

import numpy as np


DEPTH_EVENT = 1
TRADE_EVENT = 2
DEPTH_CLEAR_EVENT = 3
DEPTH_SNAPSHOT_EVENT = 4

EXCH_EVENT = 1 << 31
LOCAL_EVENT = 1 << 30
BUY_EVENT = 1 << 29
SELL_EVENT = 1 << 28
EVENT_FLAG_MASK = EXCH_EVENT | LOCAL_EVENT | BUY_EVENT | SELL_EVENT

EVENT_DTYPE = np.dtype(
    [
        ("ev", "u8"),
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("px", "f8"),
        ("qty", "f8"),
        ("order_id", "u8"),
        ("ival", "i8"),
        ("fval", "f8"),
    ],
    align=True,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Taiwan stock top-5 L2 data into HftBacktest .npz events."
    )
    parser.add_argument("output", type=Path, help="Output .npz file.")
    parser.add_argument(
        "--input-csv",
        type=Path,
        help="Input CSV file. Mutually exclusive with --data-api.",
    )
    parser.add_argument(
        "--data-api",
        action="store_true",
        help="Load rows through data_platform DataAPI.get_data_single_symbol.",
    )
    parser.add_argument(
        "--start-date",
        default="2025-09-09",
        help="DataAPI start date in YYYY-MM-DD. Default: 2025-09-09.",
    )
    parser.add_argument(
        "--end-date",
        default="2025-09-09",
        help="DataAPI end date in YYYY-MM-DD. Default: 2025-09-09.",
    )
    parser.add_argument(
        "--data-platform-base",
        default=r"\\DC_TW\taiwan_stock\?豢?撟喳",
        help=r"DataAPI base_dir. Default: \\DC_TW\taiwan_stock\?豢?撟喳.",
    )
    parser.add_argument(
        "--index-backend",
        default="duckdb",
        choices=("duckdb", "none"),
        help="DataAPI index backend. Default: duckdb.",
    )
    parser.add_argument(
        "--data-api-module-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data_platform" / "data_stock" / "api",
        help="Directory containing api_parquet.py. Default: ./data_platform/data_stock/api.",
    )
    parser.add_argument(
        "--symbol",
        help="Symbol to convert. Filters CSV rows when present. DataAPI default: 2330.",
    )
    parser.add_argument(
        "--date",
        help=(
            "Trading date, YYYY-MM-DD. Required when exchtime/localtime are "
            "time-of-day values such as 09:00:00.123456 or 090000123456."
        ),
    )
    parser.add_argument(
        "--timezone",
        default="Asia/Taipei",
        help="Timezone for naive date/time parsing. Default: Asia/Taipei.",
    )
    parser.add_argument(
        "--timestamp-unit",
        choices=("auto", "ns", "us", "ms", "s"),
        default="auto",
        help="Unit for numeric epoch timestamps. Default: auto.",
    )
    parser.add_argument(
        "--base-latency-ns",
        type=int,
        default=0,
        help="Added when local_ts must be shifted to avoid negative feed latency.",
    )
    parser.add_argument(
        "--volume-scale",
        type=float,
        default=1.0,
        help="Multiply all volume fields by this value. Default: 1.0.",
    )
    parser.add_argument(
        "--levels",
        type=int,
        default=5,
        help="Number of ask/bid levels in the CSV. Default: 5.",
    )
    parser.add_argument(
        "--status-allow",
        action="append",
        help=(
            "Only convert rows whose status matches this value. Can be passed "
            "multiple times. Default: convert all statuses."
        ),
    )
    parser.add_argument(
        "--trade-side",
        choices=("infer", "buy", "sell", "none"),
        default="infer",
        help=(
            "Trade side handling for total_volume deltas. 'infer' uses previous "
            "BBO. 'none' skips trade events. Default: infer."
        ),
    )
    parser.add_argument(
        "--no-depth",
        action="store_true",
        help="Do not emit depth snapshot events.",
    )
    parser.add_argument(
        "--no-trades",
        action="store_true",
        help="Do not emit trade events from total_volume deltas.",
    )
    parser.add_argument(
        "--start-exch-ts",
        type=int,
        help="Only convert rows with exchtime >= this epoch-ns timestamp.",
    )
    parser.add_argument(
        "--end-exch-ts",
        type=int,
        help="Only convert rows with exchtime <= this epoch-ns timestamp.",
    )
    parser.add_argument(
        "--qa-sample-rows",
        type=int,
        default=1000,
        help="Number of converted rows to use for replay QA. Default: 1000.",
    )
    args = parser.parse_args()
    if args.data_api == bool(args.input_csv):
        parser.error("choose exactly one input mode: --data-api or --input-csv PATH")
    if args.data_api and args.symbol is None:
        args.symbol = "2330"
    return args


def to_float(value: object, default: float = math.nan) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if text == "":
        return default
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return default


def to_int(value: object, default: int = 0) -> int:
    number = to_float(value, math.nan)
    if not math.isfinite(number):
        return default
    return int(number)


def numeric_epoch_to_ns(value: float, unit: str) -> int:
    if unit == "ns":
        return int(value)
    if unit == "us":
        return int(value * 1_000)
    if unit == "ms":
        return int(value * 1_000_000)
    if unit == "s":
        return int(value * 1_000_000_000)

    abs_value = abs(value)
    if abs_value >= 1e17:
        return int(value)
    if abs_value >= 1e14:
        return int(value * 1_000)
    if abs_value >= 1e11:
        return int(value * 1_000_000)
    return int(value * 1_000_000_000)


def integer_epoch_to_ns(value: int, unit: str) -> int:
    if unit == "ns":
        return value
    if unit == "us":
        return value * 1_000
    if unit == "ms":
        return value * 1_000_000
    if unit == "s":
        return value * 1_000_000_000

    abs_value = abs(value)
    if abs_value >= 100_000_000_000_000_000:
        return value
    if abs_value >= 100_000_000_000_000:
        return value * 1_000
    if abs_value >= 100_000_000_000:
        return value * 1_000_000
    return value * 1_000_000_000


def parse_time_of_day_to_ns(text: str, date: str, tz: ZoneInfo) -> int:
    compact = text.replace(":", "").replace(".", "").replace("-", "")
    if not compact.isdigit() or len(compact) < 6:
        raise ValueError(f"Cannot parse time-of-day timestamp: {text!r}")

    hour = int(compact[0:2])
    minute = int(compact[2:4])
    second = int(compact[4:6])
    frac = compact[6:]
    nanosecond = int((frac + "0" * 9)[:9]) if frac else 0
    microsecond = nanosecond // 1_000
    dt = datetime.fromisoformat(date).replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=microsecond,
        tzinfo=tz,
    )
    return datetime_to_ns(dt) + nanosecond % 1_000


def datetime_to_ns(dt: datetime) -> int:
    utc_dt = dt.astimezone(timezone.utc)
    seconds = calendar.timegm(utc_dt.timetuple())
    return seconds * 1_000_000_000 + utc_dt.microsecond * 1_000


def parse_timestamp(value: object, unit: str, date: str | None, tz: ZoneInfo) -> int:
    text = str(value).strip()
    if text == "":
        raise ValueError("empty timestamp")

    if any(ch in text for ch in ("-", "/", ":", "T")):
        normalized = text.replace("/", "-")
        if date and normalized[0:2].isdigit() and ":" in normalized and "-" not in normalized[:8]:
            return parse_time_of_day_to_ns(normalized, date, tz)
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return datetime_to_ns(dt)

    integer_text = text.split(".", maxsplit=1)[0]
    if unit == "auto" and date and integer_text.isdigit() and len(integer_text) <= 12:
        return parse_time_of_day_to_ns(text, date, tz)

    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return integer_epoch_to_ns(int(text), unit)

    try:
        number = float(text)
    except ValueError as exc:
        raise ValueError(f"Cannot parse timestamp: {text!r}") from exc

    return numeric_epoch_to_ns(number, unit)


def make_event(ev: int, exch_ts: int, local_ts: int, px: float, qty: float) -> tuple:
    return (ev, exch_ts, local_ts, px, qty, 0, 0, 0.0)


def event_kind(ev: int) -> int:
    return ev & ~EVENT_FLAG_MASK


def valid_price_qty(price: float, qty: float) -> bool:
    return math.isfinite(price) and price > 0 and math.isfinite(qty) and qty > 0


def add_depth_level(depth: dict[float, float], price: float, qty: float) -> None:
    depth[price] = depth.get(price, 0.0) + qty


def iter_depth_events(
    row: dict[str, str],
    exch_ts: int,
    local_ts: int,
    levels: int,
    volume_scale: float,
) -> Iterable[tuple]:
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}

    for level in range(1, levels + 1):
        ask_px = to_float(row.get(f"ask_price{level}"))
        ask_qty = to_float(row.get(f"ask_volume{level}")) * volume_scale
        bid_px = to_float(row.get(f"bid_price{level}"))
        bid_qty = to_float(row.get(f"bid_volume{level}")) * volume_scale

        if valid_price_qty(ask_px, ask_qty):
            add_depth_level(asks, ask_px, ask_qty)
        if valid_price_qty(bid_px, bid_qty):
            add_depth_level(bids, bid_px, bid_qty)

    if bids:
        worst_bid = min(bids)
        yield make_event(DEPTH_CLEAR_EVENT | BUY_EVENT, exch_ts, local_ts, worst_bid, 0.0)
        for px, qty in sorted(bids.items(), reverse=True):
            yield make_event(DEPTH_SNAPSHOT_EVENT | BUY_EVENT, exch_ts, local_ts, px, qty)

    if asks:
        worst_ask = max(asks)
        yield make_event(DEPTH_CLEAR_EVENT | SELL_EVENT, exch_ts, local_ts, worst_ask, 0.0)
        for px, qty in sorted(asks.items()):
            yield make_event(DEPTH_SNAPSHOT_EVENT | SELL_EVENT, exch_ts, local_ts, px, qty)


def infer_trade_flag(
    last_price: float,
    previous_bid: float,
    previous_ask: float,
    previous_last_price: float,
) -> int:
    if math.isfinite(previous_ask) and last_price >= previous_ask:
        return BUY_EVENT
    if math.isfinite(previous_bid) and last_price <= previous_bid:
        return SELL_EVENT
    if math.isfinite(previous_last_price):
        if last_price > previous_last_price:
            return BUY_EVENT
        if last_price < previous_last_price:
            return SELL_EVENT
    return BUY_EVENT


def correct_local_timestamp(data: np.ndarray, base_latency_ns: int) -> np.ndarray:
    if len(data) == 0:
        return data
    min_latency = int(np.min(data["local_ts"] - data["exch_ts"]))
    if min_latency < 0:
        data["local_ts"] += -min_latency + base_latency_ns
    return data


def correct_event_order(data: np.ndarray) -> np.ndarray:
    if len(data) == 0:
        return data

    sorted_exch_index = np.argsort(data["exch_ts"], kind="mergesort")
    sorted_local_index = np.argsort(data["local_ts"], kind="mergesort")
    out = np.zeros(len(data) * 2, dtype=EVENT_DTYPE)

    out_rn = 0
    exch_rn = 0
    local_rn = 0

    while exch_rn < len(data) or local_rn < len(data):
        sorted_exch = data[sorted_exch_index[exch_rn]] if exch_rn < len(data) else None
        sorted_local = data[sorted_local_index[local_rn]] if local_rn < len(data) else None

        if sorted_exch is not None and sorted_local is not None:
            same_event = (
                sorted_exch["exch_ts"] == sorted_local["exch_ts"]
                and sorted_exch["local_ts"] == sorted_local["local_ts"]
                and sorted_exch["ev"] == sorted_local["ev"]
                and sorted_exch["px"] == sorted_local["px"]
                and sorted_exch["qty"] == sorted_local["qty"]
            )
            if same_event:
                out[out_rn] = sorted_exch
                out[out_rn]["ev"] |= EXCH_EVENT | LOCAL_EVENT
                out_rn += 1
                exch_rn += 1
                local_rn += 1
                continue

            if sorted_exch["exch_ts"] <= sorted_local["exch_ts"]:
                out[out_rn] = sorted_exch
                out[out_rn]["ev"] |= EXCH_EVENT
                out_rn += 1
                exch_rn += 1
                continue

            out[out_rn] = sorted_local
            out[out_rn]["ev"] |= LOCAL_EVENT
            out_rn += 1
            local_rn += 1
            continue

        if sorted_exch is not None:
            out[out_rn] = sorted_exch
            out[out_rn]["ev"] |= EXCH_EVENT
            out_rn += 1
            exch_rn += 1
        else:
            out[out_rn] = sorted_local
            out[out_rn]["ev"] |= LOCAL_EVENT
            out_rn += 1
            local_rn += 1

    return out[:out_rn]


def validate_event_order(data: np.ndarray) -> None:
    exch_mask = data["ev"] & EXCH_EVENT == EXCH_EVENT
    local_mask = data["ev"] & LOCAL_EVENT == LOCAL_EVENT
    if np.any(np.diff(data["exch_ts"][exch_mask]) < 0):
        raise ValueError("exchange events are out of order")
    if np.any(np.diff(data["local_ts"][local_mask]) < 0):
        raise ValueError("local events are out of order")


@dataclass
class ReplayState:
    best_bid: float = math.nan
    best_ask: float = math.nan
    bid_depth: dict[float, float] | None = None
    ask_depth: dict[float, float] | None = None

    def __post_init__(self) -> None:
        self.bid_depth = {}
        self.ask_depth = {}

    def apply(self, event: tuple) -> None:
        ev, _, _, px, qty, _, _, _ = event
        kind = event_kind(ev)
        if ev & BUY_EVENT == BUY_EVENT:
            depth = self.bid_depth
            if kind == DEPTH_CLEAR_EVENT:
                for price in list(depth):
                    if price >= px:
                        del depth[price]
            elif kind in (DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT):
                if qty > 0:
                    depth[px] = qty
                else:
                    depth.pop(px, None)
            self.best_bid = max(depth) if depth else math.nan
        elif ev & SELL_EVENT == SELL_EVENT:
            depth = self.ask_depth
            if kind == DEPTH_CLEAR_EVENT:
                for price in list(depth):
                    if price <= px:
                        del depth[price]
            elif kind in (DEPTH_EVENT, DEPTH_SNAPSHOT_EVENT):
                if qty > 0:
                    depth[px] = qty
                else:
                    depth.pop(px, None)
            self.best_ask = min(depth) if depth else math.nan


@dataclass
class ConversionStats:
    input_rows: int = 0
    converted_rows: int = 0
    skipped_symbol_rows: int = 0
    skipped_status_rows: int = 0
    skipped_time_rows: int = 0
    raw_events: int = 0
    output_events: int = 0
    depth_events: int = 0
    trade_events: int = 0
    opening_jump_qty: float = 0.0
    first_exch_ts: int | None = None
    last_exch_ts: int | None = None
    min_feed_latency: int | None = None
    max_feed_latency: int | None = None
    best_bid_mismatches: int = 0
    best_ask_mismatches: int = 0
    trade_qty_mismatches: int = 0
    qa_rows_checked: int = 0

    def observe_time(self, exch_ts: int, local_ts: int) -> None:
        latency = local_ts - exch_ts
        self.first_exch_ts = exch_ts if self.first_exch_ts is None else min(self.first_exch_ts, exch_ts)
        self.last_exch_ts = exch_ts if self.last_exch_ts is None else max(self.last_exch_ts, exch_ts)
        self.min_feed_latency = latency if self.min_feed_latency is None else min(self.min_feed_latency, latency)
        self.max_feed_latency = latency if self.max_feed_latency is None else max(self.max_feed_latency, latency)


def same_price(left: float, right: float) -> bool:
    if not math.isfinite(left) and not math.isfinite(right):
        return True
    return math.isclose(left, right, rel_tol=0.0, abs_tol=1e-9)


def should_skip_time(exch_ts: int, args: argparse.Namespace) -> bool:
    if args.start_exch_ts is not None and exch_ts < args.start_exch_ts:
        return True
    if args.end_exch_ts is not None and exch_ts > args.end_exch_ts:
        return True
    return False


def row_iter_from_csv(args: argparse.Namespace) -> Iterator[dict[str, object]]:
    with args.input_csv.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        yield from reader


def row_iter_from_data_api(args: argparse.Namespace) -> Iterator[dict[str, object]]:
    module_dir = args.data_api_module_dir.resolve()
    sys.path.insert(0, str(module_dir))
    try:
        from api_parquet import DataAPI
    except Exception as exc:
        raise RuntimeError(f"cannot import DataAPI from {module_dir}") from exc

    api = DataAPI(base_dir=args.data_platform_base, index_backend=args.index_backend)
    df = api.get_data_single_symbol(str(args.symbol), args.start_date, args.end_date)
    yield from df.to_dicts()


def build_events_from_rows(
    rows: Iterable[dict[str, object]],
    args: argparse.Namespace,
) -> tuple[np.ndarray, ConversionStats]:
    tz = ZoneInfo(args.timezone)
    events: list[tuple] = []
    stats = ConversionStats()
    replay = ReplayState()

    previous_total_volume: int | None = None
    previous_bid = math.nan
    previous_ask = math.nan
    previous_last_price = math.nan

    for row in rows:
        stats.input_rows += 1

        if args.input_csv and args.symbol and str(row.get("symbol")) != str(args.symbol):
            stats.skipped_symbol_rows += 1
            continue
        if args.status_allow and str(row.get("status")) not in args.status_allow:
            stats.skipped_status_rows += 1
            continue

        exch_ts = parse_timestamp(row["exchtime"], args.timestamp_unit, args.date, tz)
        if should_skip_time(exch_ts, args):
            stats.skipped_time_rows += 1
            continue
        local_source = row.get("localtime") or row["exchtime"]
        local_ts = parse_timestamp(local_source, args.timestamp_unit, args.date, tz)
        stats.observe_time(exch_ts, local_ts)

        total_volume = to_int(row.get("total_volume"))
        last_price = to_float(row.get("last_price"))

        row_events: list[tuple] = []
        expected_trade_qty = 0.0
        if not args.no_trades and args.trade_side != "none" and previous_total_volume is not None:
            delta_volume = total_volume - previous_total_volume
            if delta_volume > 0 and math.isfinite(last_price) and last_price > 0:
                expected_trade_qty = delta_volume * args.volume_scale
                if previous_total_volume == 0:
                    stats.opening_jump_qty += expected_trade_qty
                if args.trade_side == "buy":
                    side = BUY_EVENT
                elif args.trade_side == "sell":
                    side = SELL_EVENT
                else:
                    side = infer_trade_flag(
                        last_price,
                        previous_bid,
                        previous_ask,
                        previous_last_price,
                    )
                row_events.append(
                    make_event(
                        TRADE_EVENT | side,
                        exch_ts,
                        local_ts,
                        last_price,
                        expected_trade_qty,
                    )
                )

        if not args.no_depth:
            row_events.extend(
                iter_depth_events(
                    row,
                    exch_ts,
                    local_ts,
                    args.levels,
                    args.volume_scale,
                )
            )

        if expected_trade_qty > 0:
            actual_trade_qty = sum(
                ev[4] for ev in row_events if event_kind(ev[0]) == TRADE_EVENT
            )
            if not math.isclose(actual_trade_qty, expected_trade_qty, rel_tol=0.0, abs_tol=1e-9):
                stats.trade_qty_mismatches += 1

        if stats.qa_rows_checked < args.qa_sample_rows:
            for event in row_events:
                replay.apply(event)
            expected_bid = to_float(row.get("bid_price1"))
            expected_ask = to_float(row.get("ask_price1"))
            if math.isfinite(expected_bid) and expected_bid > 0 and not same_price(replay.best_bid, expected_bid):
                stats.best_bid_mismatches += 1
            if math.isfinite(expected_ask) and expected_ask > 0 and not same_price(replay.best_ask, expected_ask):
                stats.best_ask_mismatches += 1
            stats.qa_rows_checked += 1

        for event in row_events:
            kind = event_kind(event[0])
            if kind == TRADE_EVENT:
                stats.trade_events += 1
            if kind in (DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT):
                stats.depth_events += 1
        events.extend(row_events)

        previous_total_volume = total_volume
        previous_bid = to_float(row.get("bid_price1"))
        previous_ask = to_float(row.get("ask_price1"))
        previous_last_price = last_price
        stats.converted_rows += 1

    stats.raw_events = len(events)
    data = np.array(events, dtype=EVENT_DTYPE)
    data = correct_local_timestamp(data, args.base_latency_ns)
    data = correct_event_order(data)
    validate_event_order(data)
    stats.output_events = len(data)
    return data, stats


def print_summary(stats: ConversionStats, output: Path) -> None:
    print(f"input_rows={stats.input_rows}")
    print(f"converted_rows={stats.converted_rows}")
    print(f"skipped_symbol_rows={stats.skipped_symbol_rows}")
    print(f"skipped_status_rows={stats.skipped_status_rows}")
    print(f"skipped_time_rows={stats.skipped_time_rows}")
    print(f"raw_events={stats.raw_events}")
    print(f"output_events={stats.output_events}")
    print(f"depth_events={stats.depth_events}")
    print(f"trade_events={stats.trade_events}")
    print(f"opening_jump_qty={stats.opening_jump_qty}")
    print(f"first_exch_ts={stats.first_exch_ts}")
    print(f"last_exch_ts={stats.last_exch_ts}")
    print(f"min_feed_latency={stats.min_feed_latency}")
    print(f"max_feed_latency={stats.max_feed_latency}")
    print(f"qa_rows_checked={stats.qa_rows_checked}")
    print(f"best_bid_mismatches={stats.best_bid_mismatches}")
    print(f"best_ask_mismatches={stats.best_ask_mismatches}")
    print(f"trade_qty_mismatches={stats.trade_qty_mismatches}")
    print(f"output={output}")


def convert(args: argparse.Namespace) -> np.ndarray:
    if args.data_api:
        rows = row_iter_from_data_api(args)
    else:
        rows = row_iter_from_csv(args)

    data, stats = build_events_from_rows(rows, args)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(args.output, data=data)
    print_summary(stats, args.output)
    return data


def default_output_path(
    workspace_root: Path,
    symbol: str,
    start_date: str,
    end_date: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> Path:
    date_part = start_date.replace("-", "")
    if end_date and end_date != start_date:
        date_part = f"{date_part}_{end_date.replace('-', '')}"
    time_part = ""
    if start_time or end_time:
        start_label = (start_time or "start").replace(":", "").replace(".", "")
        end_label = (end_time or "end").replace(":", "").replace(".", "")
        time_part = f"_{start_label}_{end_label}"
    return workspace_root / "data" / "tw_stock_events" / f"{symbol}_{date_part}{time_part}.npz"


def convert_tw_stock_to_npz(
    *,
    symbol: str,
    start_date: str,
    end_date: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    output: Path | None = None,
    workspace_root: Path | None = None,
    input_csv: Path | None = None,
    data_api: bool | None = None,
    data_platform_base: str = r"\\DC_TW\taiwan_stock\數據平台",
    index_backend: str = "duckdb",
    data_api_module_dir: Path | None = None,
    timezone_name: str = "Asia/Taipei",
    timestamp_unit: str = "auto",
    base_latency_ns: int = 0,
    volume_scale: float = 1.0,
    levels: int = 5,
    status_allow: list[str] | None = None,
    trade_side: str = "infer",
    no_depth: bool = False,
    no_trades: bool = False,
    qa_sample_rows: int = 1000,
) -> tuple[Path, np.ndarray]:
    """Convert one Taiwan stock symbol/time window to an HftBacktest npz file."""
    root = Path.cwd() if workspace_root is None else workspace_root
    root = root.resolve()
    end_date = start_date if end_date is None else end_date
    use_data_api = input_csv is None if data_api is None else data_api
    if use_data_api == bool(input_csv):
        raise ValueError("choose exactly one input mode: data_api=True or input_csv=Path(...)")

    tz = ZoneInfo(timezone_name)
    start_exch_ts = parse_timestamp(start_time, timestamp_unit, start_date, tz) if start_time else None
    end_exch_ts = parse_timestamp(end_time, timestamp_unit, end_date, tz) if end_time else None
    output_path = output or default_output_path(root, symbol, start_date, end_date, start_time, end_time)

    args = argparse.Namespace(
        output=output_path,
        input_csv=input_csv,
        data_api=use_data_api,
        start_date=start_date,
        end_date=end_date,
        data_platform_base=data_platform_base,
        index_backend=index_backend,
        data_api_module_dir=data_api_module_dir
        or root / "data_platform" / "data_stock" / "api",
        symbol=symbol,
        date=start_date,
        timezone=timezone_name,
        timestamp_unit=timestamp_unit,
        base_latency_ns=base_latency_ns,
        volume_scale=volume_scale,
        levels=levels,
        status_allow=status_allow,
        trade_side=trade_side,
        no_depth=no_depth,
        no_trades=no_trades,
        start_exch_ts=start_exch_ts,
        end_exch_ts=end_exch_ts,
        qa_sample_rows=qa_sample_rows,
    )
    data = convert(args)
    return output_path, data


def main() -> int:
    args = parse_args()
    try:
        convert(args)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0



def parse_hit_state_report_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tw_stock_data_to_npz.py hit-state-report",
        description=(
            "Cross the spread with GTC limit orders and print state after each "
            "step to verify fills, position, balance, fee, equity, and volume."
        ),
    )
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("data/tw_stock_events/2330_20250909.npz"),
        help="HftBacktest .npz event file.",
    )
    parser.add_argument("--contract-size", type=float, default=1000.0, help="Shares per board lot.")
    parser.add_argument("--tick-size", type=float, default=5.0)
    parser.add_argument("--lot-size", type=float, default=1.0)
    parser.add_argument("--qty", type=float, default=1.0, help="Order qty in board lots.")
    parser.add_argument("--round-trips", type=int, default=1, help="Number of buy-hit/sell-hit pairs.")
    parser.add_argument("--warmup-ns", type=int, default=1_000_000_000, help="Initial elapse time before first order.")
    parser.add_argument("--response-timeout-ns", type=int, default=10_000_000, help="Order response wait timeout.")
    return parser.parse_args(argv)


def submit_and_report(
    hbt,
    hbtpkg,
    asset_no: int,
    order_id: int,
    side: str,
    px: float,
    qty: float,
    response_timeout_ns: int,
    contract_size: float,
) -> None:
    try:
        from .tw_stock_hftbacktest import print_state, state_snapshot, submit_limit_order
    except ImportError:
        from tw_stock_hftbacktest import print_state, state_snapshot, submit_limit_order

    before = state_snapshot(hbt, asset_no, contract_size)
    print_state(f"before_{side}", order_id, before)

    rc = submit_limit_order(hbt, hbtpkg, asset_no, order_id, side, px, qty)
    print(f"submit_{side:<11} order_id={order_id:<6} px={px:.2f} qty={qty:.4f} rc={rc}")

    response = hbt.wait_order_response(asset_no, order_id, response_timeout_ns)
    after = state_snapshot(hbt, asset_no, contract_size)
    print(f"response_{side:<9} order_id={order_id:<6} response={response}")
    print_state(f"after_{side}", order_id, after)

    hbt.clear_inactive_orders(asset_no)
    print_state(f"clear_{side}", order_id, state_snapshot(hbt, asset_no, contract_size))


def hit_state_report_main(argv: list[str] | None = None) -> int:
    try:
        from .tw_stock_hftbacktest import (
            BacktestConfig,
            build_backtest,
            close_backtest,
            import_hftbacktest,
            print_state,
            state_snapshot,
            wait_for_bbo,
        )
    except ImportError:
        from tw_stock_hftbacktest import (
            BacktestConfig,
            build_backtest,
            close_backtest,
            import_hftbacktest,
            print_state,
            state_snapshot,
            wait_for_bbo,
        )

    args = parse_hit_state_report_args(argv)
    workspace_root = Path(__file__).resolve().parents[1]
    args.data = args.data if args.data.is_absolute() else workspace_root / args.data
    if not args.data.exists():
        raise FileNotFoundError(args.data)

    hbtpkg = import_hftbacktest(workspace_root)
    print(f"hftbacktest={getattr(hbtpkg, '__version__', 'unknown')} file={getattr(hbtpkg, '__file__', None)}")
    print(f"data={args.data}")
    print("fee model: maker=0, taker=0; order latency: 0 ns; qty unit: board lots")

    config = BacktestConfig(
        data=args.data,
        contract_size=args.contract_size,
        tick_size=args.tick_size,
        lot_size=args.lot_size,
        maker_fee=0.0,
        taker_fee=0.0,
        order_latency_ns=0,
    )
    hbt = build_backtest(config, hbtpkg)
    asset_no = 0
    try:
        wait_for_bbo(hbt, asset_no, args.warmup_ns)
        print_state("initial_bbo", None, state_snapshot(hbt, asset_no, args.contract_size))

        next_order_id = 10_001
        for round_no in range(1, args.round_trips + 1):
            depth = hbt.depth(asset_no)
            print(f"\nround={round_no} aggressive buy at best ask")
            submit_and_report(
                hbt,
                hbtpkg,
                asset_no,
                next_order_id,
                "buy",
                float(depth.best_ask),
                args.qty,
                args.response_timeout_ns,
                args.contract_size,
            )
            next_order_id += 1

            depth = hbt.depth(asset_no)
            print(f"\nround={round_no} aggressive sell at best bid")
            submit_and_report(
                hbt,
                hbtpkg,
                asset_no,
                next_order_id,
                "sell",
                float(depth.best_bid),
                args.qty,
                args.response_timeout_ns,
                args.contract_size,
            )
            next_order_id += 1

        print("\nfinal")
        print_state("final_state", None, state_snapshot(hbt, asset_no, args.contract_size))
    finally:
        close_backtest(hbt)
    return 0


def cli_main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    if args and args[0] in {"hit-state-report", "state-report", "report"}:
        return hit_state_report_main(args[1:])
    return main()


if __name__ == "__main__":
    raise SystemExit(cli_main())
