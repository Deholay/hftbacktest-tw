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
    An npz file with key "data", using HftBacktest event_dtype:
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
import importlib
import math
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator
from zoneinfo import ZoneInfo

import numpy as np
from numba import njit


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

SOURCE_KIND_ALIASES = {
    "stock": "stock",
    "tw_stock": "stock",
    "odd lot": "odd_lot",
    "odd_lot": "odd_lot",
    "odd-lot": "odd_lot",
    "tw_odd_lot": "odd_lot",
    "etf": "etf",
    "tw_etf": "etf",
    "stock future": "stock_future",
    "stock_future": "stock_future",
    "stock-future": "stock_future",
    "tw_stock_future": "stock_future",
}

SOURCE_KIND_LABELS = {
    "stock": "Stock",
    "odd_lot": "Odd Lot",
    "etf": "ETF",
    "stock_future": "Stock Future",
}

DEFAULT_DAILY_PARQUET_DIRS = {
    "odd_lot": Path(r"\\DC_TW\taiwan_stock\ticks_parquet_odd_lot"),
    "etf": Path(r"\\DC_TW\taiwan_stock\ticks_parquet_etf"),
    "stock_future": Path(r"\\DC_TW\taiwan_stock\ticks_parquet_stock_future"),
}

PRICE_ONLY_DEPTH_SOURCE_KINDS = {"odd_lot", "etf"}


def default_data_api_module_dir(root: Path) -> Path:
    """Return the data_platform_client API path used by default."""
    return root / "data_platform_client" / "data_stock" / "api"


def normalize_source_kind(source_kind: str) -> str:
    normalized = SOURCE_KIND_ALIASES.get(source_kind.strip().lower().replace("_", " "))
    if normalized is not None:
        return normalized
    normalized = SOURCE_KIND_ALIASES.get(source_kind.strip().lower())
    if normalized is not None:
        return normalized
    valid = ", ".join(sorted(SOURCE_KIND_LABELS))
    raise ValueError(f"unknown source_kind={source_kind!r}; valid values: {valid}")


def read_path_config(path: Path) -> dict[str, Path]:
    """Read the local path.toml section/path format used by this workspace."""
    if not path.exists():
        return {}

    paths: dict[str, Path] = {}
    section: str | None = None
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if section is None:
            continue
        try:
            source_kind = normalize_source_kind(section)
        except ValueError:
            continue
        paths[source_kind] = Path(line)
    return paths


def default_daily_parquet_dir(
    workspace_root: Path,
    source_kind: str,
    path_config: Path | None = None,
) -> Path:
    source_kind = normalize_source_kind(source_kind)
    config_path = path_config or workspace_root / "path.toml"
    configured = read_path_config(config_path).get(source_kind)
    if configured is not None:
        return configured
    try:
        return DEFAULT_DAILY_PARQUET_DIRS[source_kind]
    except KeyError as exc:
        raise ValueError(f"{source_kind!r} does not have a daily parquet directory") from exc


def output_folder_for_source(source_kind: str) -> str:
    source_kind = normalize_source_kind(source_kind)
    return {
        "stock": "tw_stock_events",
        "odd_lot": "tw_odd_lot_events",
        "etf": "tw_etf_events",
        "stock_future": "tw_stock_future_events",
    }[source_kind]


def module_loaded_from(module, root: Path) -> bool:
    module_file = getattr(module, "__file__", None)
    if module_file is None:
        return False
    try:
        Path(module_file).resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def import_data_api_class(module_dir: Path):
    """Import DataAPI from either the packaged client API or an explicit module dir."""
    module_dir = module_dir.resolve()
    errors: list[str] = []

    if module_dir.name == "api" and module_dir.parent.name == "data_stock":
        package_root = module_dir.parent.parent
        if (module_dir.parent / "__init__.py").exists():
            sys.path.insert(0, str(package_root))
            try:
                module = importlib.import_module("data_stock.api.api_parquet")
                if not module_loaded_from(module, module_dir):
                    errors.append(
                        f"package import resolved outside {module_dir}: "
                        f"{getattr(module, '__file__', None)!r}"
                    )
                else:
                    return module.DataAPI
            except Exception as exc:
                errors.append(f"package import data_stock.api.api_parquet failed: {exc!r}")

    sys.path.insert(0, str(module_dir))
    try:
        module = importlib.import_module("api_parquet")
        if not module_loaded_from(module, module_dir):
            errors.append(
                f"top-level import resolved outside {module_dir}: "
                f"{getattr(module, '__file__', None)!r}"
            )
        else:
            return module.DataAPI
    except Exception as exc:
        errors.append(f"top-level import api_parquet failed: {exc!r}")

    raise RuntimeError(f"cannot import DataAPI from {module_dir}; " + "; ".join(errors))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Taiwan stock top-5 L2 data into HftBacktest .npz events."
    )
    parser.add_argument("output", type=Path, help="Output .npz file.")
    parser.add_argument(
        "--source-kind",
        default="stock",
        help="Market data source kind. Default: stock.",
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        help="Input CSV file. Mutually exclusive with --data-api and --daily-parquet.",
    )
    parser.add_argument(
        "--data-api",
        action="store_true",
        help="Load rows through data_platform_client DataAPI.get_data_single_symbol.",
    )
    parser.add_argument(
        "--daily-parquet",
        action="store_true",
        help="Load rows from daily parquet files such as ETF, Odd Lot, or Stock Future sources.",
    )
    parser.add_argument(
        "--daily-parquet-dir",
        type=Path,
        help="Directory containing daily parquet files named YYYY-MM-DD.parquet.",
    )
    parser.add_argument(
        "--path-config",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "path.toml",
        help="Workspace path config. Default: ./path.toml.",
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
        choices=("duckdb", "parquet"),
        help="data_platform_client index backend. Default: duckdb.",
    )
    parser.add_argument(
        "--data-api-module-dir",
        type=Path,
        default=default_data_api_module_dir(Path(__file__).resolve().parents[1]),
        help=(
            "Directory containing api_parquet.py. Defaults to "
            "./data_platform_client/data_stock/api."
        ),
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
        "--price-only-depth-qty",
        type=float,
        help=(
            "Depth quantity to use when the source has top-5 prices but no "
            "level volume columns. Defaults to 1.0 for ETF and Odd Lot."
        ),
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
    parser.add_argument(
        "--npz-compression",
        choices=("compressed", "uncompressed"),
        default="compressed",
        help=(
            "NPZ storage mode. 'compressed' preserves the existing smaller output; "
            "'uncompressed' writes and reloads faster but uses more disk. Default: compressed."
        ),
    )
    args = parser.parse_args()
    args.source_kind = normalize_source_kind(args.source_kind)
    input_modes = [args.data_api, bool(args.input_csv), args.daily_parquet]
    if sum(bool(mode) for mode in input_modes) != 1:
        parser.error("choose exactly one input mode: --data-api, --daily-parquet, or --input-csv PATH")
    if args.data_api and args.symbol is None:
        args.symbol = "2330"
    if args.daily_parquet and args.symbol is None:
        parser.error("--symbol is required with --daily-parquet")
    if args.daily_parquet and args.daily_parquet_dir is None:
        args.daily_parquet_dir = default_daily_parquet_dir(
            Path(__file__).resolve().parents[1],
            args.source_kind,
            args.path_config,
        )
    if args.price_only_depth_qty is None and args.source_kind in PRICE_ONLY_DEPTH_SOURCE_KINDS:
        args.price_only_depth_qty = 1.0
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


def depth_qty_from_row(
    row: dict[str, object],
    field_name: str,
    price: float,
    volume_scale: float,
    price_only_depth_qty: float | None,
) -> float:
    value = row.get(field_name)
    if value is not None and str(value).strip() != "":
        return to_float(value) * volume_scale
    if price_only_depth_qty is not None and math.isfinite(price) and price > 0:
        return price_only_depth_qty * volume_scale
    return math.nan


def iter_depth_events(
    row: dict[str, object],
    exch_ts: int,
    local_ts: int,
    levels: int,
    volume_scale: float,
    price_only_depth_qty: float | None = None,
) -> Iterable[tuple]:
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}

    for level in range(1, levels + 1):
        ask_px = to_float(row.get(f"ask_price{level}"))
        bid_px = to_float(row.get(f"bid_price{level}"))
        ask_qty = depth_qty_from_row(
            row,
            f"ask_volume{level}",
            ask_px,
            volume_scale,
            price_only_depth_qty,
        )
        bid_qty = depth_qty_from_row(
            row,
            f"bid_volume{level}",
            bid_px,
            volume_scale,
            price_only_depth_qty,
        )

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

    exch_ordered = bool(np.all(data["exch_ts"][1:] >= data["exch_ts"][:-1]))
    local_ordered = bool(np.all(data["local_ts"][1:] >= data["local_ts"][:-1]))
    if exch_ordered and local_ordered:
        out = data.copy()
        out["ev"] |= np.uint64(EXCH_EVENT | LOCAL_EVENT)
        return out

    sorted_exch_index = np.argsort(data["exch_ts"], kind="stable")
    sorted_local_index = np.argsort(data["local_ts"], kind="stable")
    out = np.empty(len(data) * 2, dtype=EVENT_DTYPE)
    out_rn = _merge_event_order(data, sorted_exch_index, sorted_local_index, out)
    return out[:out_rn]


@njit(cache=True)
def _merge_event_order(
    data: np.ndarray,
    sorted_exch_index: np.ndarray,
    sorted_local_index: np.ndarray,
    out: np.ndarray,
) -> int:
    """Merge exchange/local event streams in compiled code for the uncommon reordered case."""
    out_rn = 0
    exch_rn = 0
    local_rn = 0
    data_len = len(data)

    while exch_rn < data_len or local_rn < data_len:
        if exch_rn < data_len and local_rn < data_len:
            sorted_exch = data[sorted_exch_index[exch_rn]]
            sorted_local = data[sorted_local_index[local_rn]]
            px_equal = sorted_exch["px"] == sorted_local["px"] or (
                np.isnan(sorted_exch["px"]) and np.isnan(sorted_local["px"])
            )
            same_event = (
                sorted_exch["exch_ts"] == sorted_local["exch_ts"]
                and sorted_exch["local_ts"] == sorted_local["local_ts"]
                and sorted_exch["ev"] == sorted_local["ev"]
                and px_equal
                and sorted_exch["qty"] == sorted_local["qty"]
            )
            if same_event:
                out[out_rn] = sorted_exch
                out[out_rn]["ev"] |= EXCH_EVENT | LOCAL_EVENT
                out_rn += 1
                exch_rn += 1
                local_rn += 1
            elif sorted_exch["exch_ts"] <= sorted_local["exch_ts"]:
                out[out_rn] = sorted_exch
                out[out_rn]["ev"] |= EXCH_EVENT
                out_rn += 1
                exch_rn += 1
            else:
                out[out_rn] = sorted_local
                out[out_rn]["ev"] |= LOCAL_EVENT
                out_rn += 1
                local_rn += 1
        elif exch_rn < data_len:
            out[out_rn] = data[sorted_exch_index[exch_rn]]
            out[out_rn]["ev"] |= EXCH_EVENT
            out_rn += 1
            exch_rn += 1
        else:
            out[out_rn] = data[sorted_local_index[local_rn]]
            out[out_rn]["ev"] |= LOCAL_EVENT
            out_rn += 1
            local_rn += 1

    return out_rn


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
    load_seconds: float = 0.0
    event_build_seconds: float = 0.0
    normalize_seconds: float = 0.0
    write_seconds: float = 0.0

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
    DataAPI = import_data_api_class(module_dir)

    api = DataAPI(base_dir=args.data_platform_base, index_backend=args.index_backend)
    df = api.get_data_single_symbol(str(args.symbol), args.start_date, args.end_date)
    yield from df.to_dicts()


def iter_date_strings(start_date: str, end_date: str) -> Iterator[str]:
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def daily_parquet_files(data_dir: Path, start_date: str, end_date: str) -> list[Path]:
    files: list[Path] = []
    for date_text in iter_date_strings(start_date, end_date):
        path = data_dir / f"{date_text}.parquet"
        if path.exists():
            files.append(path)
    return files


def symbol_filter_values(symbol: str) -> list[str]:
    values = [str(symbol)]
    stripped = str(symbol).lstrip("0")
    if stripped and stripped not in values:
        values.append(stripped)
    return values


def daily_parquet_columns(levels: int) -> list[str]:
    columns = [
        "symbol",
        "symbol_id",
        "exchtime",
        "localtime",
        "status",
        "last_price",
        "total_volume",
        "sequence",
    ]
    for level in range(1, levels + 1):
        columns.extend(
            [
                f"ask_price{level}",
                f"ask_volume{level}",
                f"bid_price{level}",
                f"bid_volume{level}",
            ]
        )
    return columns


def load_daily_parquet_frame(args: argparse.Namespace):
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("daily parquet input requires polars") from exc

    data_dir = Path(args.daily_parquet_dir).resolve()
    files = daily_parquet_files(data_dir, args.start_date, args.end_date)
    if not files:
        raise FileNotFoundError(
            f"no daily parquet files found in {data_dir} for {args.start_date} to {args.end_date}"
        )

    lf = pl.scan_parquet([str(path) for path in files])
    schema_names = set(lf.collect_schema().names())
    required = {"symbol", "exchtime", "last_price", "total_volume"}
    for level in range(1, args.levels + 1):
        required.add(f"ask_price{level}")
        required.add(f"bid_price{level}")
    missing = sorted(required - schema_names)
    if missing:
        raise ValueError(f"daily parquet source missing required columns: {missing}")

    selected_columns = [column for column in daily_parquet_columns(args.levels) if column in schema_names]
    lf = lf.select(selected_columns)

    lf = lf.filter(pl.col("symbol").cast(pl.Utf8).is_in(symbol_filter_values(str(args.symbol))))
    if args.status_allow and "status" in schema_names:
        lf = lf.filter(pl.col("status").cast(pl.Utf8).is_in([str(value) for value in args.status_allow]))
    if args.start_exch_ts is not None:
        lf = lf.filter(pl.col("exchtime").cast(pl.Int64) >= int(args.start_exch_ts))
    if args.end_exch_ts is not None:
        lf = lf.filter(pl.col("exchtime").cast(pl.Int64) <= int(args.end_exch_ts))

    df = lf.collect()
    sort_columns = [column for column in ("exchtime", "localtime", "sequence") if column in df.columns]
    if sort_columns:
        df = df.sort(sort_columns)
    return df


def row_iter_from_daily_parquet(args: argparse.Namespace) -> Iterator[dict[str, object]]:
    """Compatibility row iterator; the converter itself uses the columnar fast path."""
    yield from load_daily_parquet_frame(args).to_dicts()


def _float_column(df, name: str, default: float = math.nan) -> np.ndarray:
    """Return a contiguous float64 column without materializing Python row dictionaries."""
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("daily parquet input requires polars") from exc

    if name not in df.columns:
        return np.full(df.height, default, dtype=np.float64)
    series = df.get_column(name)
    if series.dtype == pl.String:
        series = series.str.replace_all(",", "")
    series = series.cast(pl.Float64, strict=False).fill_null(default)
    # Polars may expose a read-only Arrow-backed view; the converter normalizes
    # missing values in a few columns, so return an owned writable array.
    return np.array(series.to_numpy(), dtype=np.float64, copy=True, order="C")


def _float_matrix(df, names: list[str]) -> np.ndarray:
    if not names:
        return np.empty((df.height, 0), dtype=np.float64)
    return np.ascontiguousarray(np.column_stack([_float_column(df, name) for name in names]))


def _numeric_timestamp_array_to_ns(values: np.ndarray, unit: str) -> np.ndarray:
    """Vectorized equivalent of numeric timestamp parsing used by the row converter."""
    values = np.asarray(values)
    if np.issubdtype(values.dtype, np.integer):
        source = values.astype(np.int64, copy=False)
        out = source.copy()
        if unit == "ns":
            return out
        if unit == "us":
            return out * 1_000
        if unit == "ms":
            return out * 1_000_000
        if unit == "s":
            return out * 1_000_000_000
        abs_values = np.abs(source)
        out[abs_values < 100_000_000_000] *= 1_000_000_000
        us_mask = (abs_values >= 100_000_000_000_000) & (abs_values < 100_000_000_000_000_000)
        out[us_mask] *= 1_000
        ms_mask = (abs_values >= 100_000_000_000) & (abs_values < 100_000_000_000_000)
        out[ms_mask] *= 1_000_000
        return out

    source = values.astype(np.float64, copy=False)
    if unit == "ns":
        scale = np.ones(len(source), dtype=np.float64)
    elif unit == "us":
        scale = np.full(len(source), 1_000.0)
    elif unit == "ms":
        scale = np.full(len(source), 1_000_000.0)
    elif unit == "s":
        scale = np.full(len(source), 1_000_000_000.0)
    else:
        abs_values = np.abs(source)
        scale = np.ones(len(source), dtype=np.float64)
        scale[abs_values < 1e11] = 1_000_000_000.0
        scale[(abs_values >= 1e11) & (abs_values < 1e14)] = 1_000_000.0
        scale[(abs_values >= 1e14) & (abs_values < 1e17)] = 1_000.0
    return (source * scale).astype(np.int64)


def _timestamp_column_to_ns(
    df,
    name: str,
    args: argparse.Namespace,
    fallback: np.ndarray | None = None,
) -> np.ndarray:
    """Convert a Polars timestamp column while keeping numeric data vectorized."""
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("daily parquet input requires polars") from exc

    if name not in df.columns:
        if fallback is None:
            raise ValueError(f"daily parquet source missing timestamp column: {name}")
        return fallback.copy()

    series = df.get_column(name)
    missing = series.is_null().to_numpy()
    dtype = series.dtype
    if dtype.is_integer():
        values = series.fill_null(0).to_numpy()
        result = _numeric_timestamp_array_to_ns(values, args.timestamp_unit)
    elif dtype.is_float():
        values = series.fill_null(0.0).to_numpy()
        result = _numeric_timestamp_array_to_ns(values, args.timestamp_unit)
    elif dtype.is_temporal():
        if dtype == pl.Date:
            result = series.cast(pl.Datetime("ns")).cast(pl.Int64).fill_null(0).to_numpy()
        else:
            result = series.dt.cast_time_unit("ns").cast(pl.Int64).fill_null(0).to_numpy()
        result = np.asarray(result, dtype=np.int64)
    else:
        tz = ZoneInfo(args.timezone)
        parsed: list[int] = []
        missing_list: list[bool] = []
        for value in series.to_list():
            is_missing = value is None or str(value).strip() == ""
            missing_list.append(is_missing)
            parsed.append(0 if is_missing else parse_timestamp(value, args.timestamp_unit, args.date, tz))
        result = np.asarray(parsed, dtype=np.int64)
        missing = np.asarray(missing_list, dtype=bool)

    if fallback is not None:
        # The legacy row path falls back for None, empty strings, and numeric zero.
        missing = missing | (result == 0)
        result[missing] = fallback[missing]
    elif np.any(missing):
        raise ValueError(f"daily parquet timestamp column {name!r} contains null values")
    return np.ascontiguousarray(result, dtype=np.int64)


@njit(cache=True)
def _write_event(
    out: np.ndarray,
    out_rn: int,
    ev: int,
    exch_ts: int,
    local_ts: int,
    px: float,
    qty: float,
) -> int:
    out[out_rn]["ev"] = ev
    out[out_rn]["exch_ts"] = exch_ts
    out[out_rn]["local_ts"] = local_ts
    out[out_rn]["px"] = px
    out[out_rn]["qty"] = qty
    out[out_rn]["order_id"] = 0
    out[out_rn]["ival"] = 0
    out[out_rn]["fval"] = 0.0
    return out_rn + 1


@njit(cache=True)
def _aggregate_depth_side(
    prices: np.ndarray,
    quantities: np.ndarray,
    row: int,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    ascending: bool,
    work_prices: np.ndarray,
    work_quantities: np.ndarray,
) -> int:
    count = 0
    for level in range(prices.shape[1]):
        px = prices[row, level]
        qty = quantities[row, level]
        if (not np.isfinite(qty)) and use_price_only_depth_qty and np.isfinite(px) and px > 0.0:
            qty = price_only_depth_qty
        qty *= volume_scale
        if not (np.isfinite(px) and px > 0.0 and np.isfinite(qty) and qty > 0.0):
            continue

        found = -1
        for index in range(count):
            if work_prices[index] == px:
                found = index
                break
        if found >= 0:
            work_quantities[found] += qty
        else:
            work_prices[count] = px
            work_quantities[count] = qty
            count += 1

    # Top-5 is tiny; insertion sort avoids allocating a temporary array for every source row.
    for index in range(1, count):
        px = work_prices[index]
        qty = work_quantities[index]
        cursor = index - 1
        while cursor >= 0 and (
            (ascending and work_prices[cursor] > px)
            or ((not ascending) and work_prices[cursor] < px)
        ):
            work_prices[cursor + 1] = work_prices[cursor]
            work_quantities[cursor + 1] = work_quantities[cursor]
            cursor -= 1
        work_prices[cursor + 1] = px
        work_quantities[cursor + 1] = qty
    return count


@njit(cache=True)
def _fill_events_from_columns(
    out: np.ndarray,
    exch_ts: np.ndarray,
    local_ts: np.ndarray,
    total_volume: np.ndarray,
    last_price: np.ndarray,
    bid_prices: np.ndarray,
    bid_quantities: np.ndarray,
    ask_prices: np.ndarray,
    ask_quantities: np.ndarray,
    volume_scale: float,
    price_only_depth_qty: float,
    use_price_only_depth_qty: bool,
    emit_trades: bool,
    trade_side_code: int,
    emit_depth: bool,
    qa_sample_rows: int,
) -> tuple[int, int, int, float, int, int, int]:
    out_rn = 0
    trade_events = 0
    depth_events = 0
    opening_jump_qty = 0.0
    best_bid_mismatches = 0
    best_ask_mismatches = 0
    qa_rows_checked = 0
    previous_total_volume = 0
    has_previous_volume = False
    previous_bid = np.nan
    previous_ask = np.nan
    previous_last_price = np.nan
    replay_best_bid = np.nan
    replay_best_ask = np.nan

    levels = bid_prices.shape[1]
    work_prices = np.empty(levels, dtype=np.float64)
    work_quantities = np.empty(levels, dtype=np.float64)

    for row in range(len(exch_ts)):
        if emit_trades and has_previous_volume:
            delta_volume = total_volume[row] - previous_total_volume
            px = last_price[row]
            if delta_volume > 0 and np.isfinite(px) and px > 0.0:
                qty = delta_volume * volume_scale
                if previous_total_volume == 0:
                    opening_jump_qty += qty
                if trade_side_code == 1:
                    side = BUY_EVENT
                elif trade_side_code == -1:
                    side = SELL_EVENT
                elif np.isfinite(previous_ask) and px >= previous_ask:
                    side = BUY_EVENT
                elif np.isfinite(previous_bid) and px <= previous_bid:
                    side = SELL_EVENT
                elif np.isfinite(previous_last_price) and px < previous_last_price:
                    side = SELL_EVENT
                else:
                    side = BUY_EVENT
                out_rn = _write_event(
                    out, out_rn, TRADE_EVENT | side, exch_ts[row], local_ts[row], px, qty
                )
                trade_events += 1

        if emit_depth:
            bid_count = _aggregate_depth_side(
                bid_prices,
                bid_quantities,
                row,
                volume_scale,
                price_only_depth_qty,
                use_price_only_depth_qty,
                False,
                work_prices,
                work_quantities,
            )
            if bid_count > 0:
                out_rn = _write_event(
                    out,
                    out_rn,
                    DEPTH_CLEAR_EVENT | BUY_EVENT,
                    exch_ts[row],
                    local_ts[row],
                    work_prices[bid_count - 1],
                    0.0,
                )
                depth_events += 1
                for index in range(bid_count):
                    out_rn = _write_event(
                        out,
                        out_rn,
                        DEPTH_SNAPSHOT_EVENT | BUY_EVENT,
                        exch_ts[row],
                        local_ts[row],
                        work_prices[index],
                        work_quantities[index],
                    )
                    depth_events += 1
                replay_best_bid = work_prices[0]

            ask_count = _aggregate_depth_side(
                ask_prices,
                ask_quantities,
                row,
                volume_scale,
                price_only_depth_qty,
                use_price_only_depth_qty,
                True,
                work_prices,
                work_quantities,
            )
            if ask_count > 0:
                out_rn = _write_event(
                    out,
                    out_rn,
                    DEPTH_CLEAR_EVENT | SELL_EVENT,
                    exch_ts[row],
                    local_ts[row],
                    work_prices[ask_count - 1],
                    0.0,
                )
                depth_events += 1
                for index in range(ask_count):
                    out_rn = _write_event(
                        out,
                        out_rn,
                        DEPTH_SNAPSHOT_EVENT | SELL_EVENT,
                        exch_ts[row],
                        local_ts[row],
                        work_prices[index],
                        work_quantities[index],
                    )
                    depth_events += 1
                replay_best_ask = work_prices[0]

        if qa_rows_checked < qa_sample_rows:
            expected_bid = bid_prices[row, 0]
            expected_ask = ask_prices[row, 0]
            if (
                np.isfinite(expected_bid)
                and expected_bid > 0.0
                and (not np.isfinite(replay_best_bid) or abs(replay_best_bid - expected_bid) > 1e-9)
            ):
                best_bid_mismatches += 1
            if (
                np.isfinite(expected_ask)
                and expected_ask > 0.0
                and (not np.isfinite(replay_best_ask) or abs(replay_best_ask - expected_ask) > 1e-9)
            ):
                best_ask_mismatches += 1
            qa_rows_checked += 1

        previous_total_volume = total_volume[row]
        has_previous_volume = True
        previous_bid = bid_prices[row, 0]
        previous_ask = ask_prices[row, 0]
        previous_last_price = last_price[row]

    return (
        out_rn,
        trade_events,
        depth_events,
        opening_jump_qty,
        best_bid_mismatches,
        best_ask_mismatches,
        qa_rows_checked,
    )


def build_events_from_parquet_frame(
    df,
    args: argparse.Namespace,
) -> tuple[np.ndarray, ConversionStats]:
    """Build events from column arrays, avoiding per-row dictionaries and event tuples."""
    stats = ConversionStats(input_rows=df.height, converted_rows=df.height)
    if df.height == 0:
        return np.empty(0, dtype=EVENT_DTYPE), stats

    started = time.perf_counter()
    exch_ts = _timestamp_column_to_ns(df, "exchtime", args)
    local_ts = _timestamp_column_to_ns(df, "localtime", args, fallback=exch_ts)
    total_volume_float = _float_column(df, "total_volume", default=0.0)
    total_volume_float[~np.isfinite(total_volume_float)] = 0.0
    total_volume = np.ascontiguousarray(total_volume_float.astype(np.int64))
    last_price = _float_column(df, "last_price")
    bid_prices = _float_matrix(df, [f"bid_price{level}" for level in range(1, args.levels + 1)])
    ask_prices = _float_matrix(df, [f"ask_price{level}" for level in range(1, args.levels + 1)])
    bid_quantities = _float_matrix(df, [f"bid_volume{level}" for level in range(1, args.levels + 1)])
    ask_quantities = _float_matrix(df, [f"ask_volume{level}" for level in range(1, args.levels + 1)])

    max_events_per_row = 2 * args.levels + 3
    raw = np.empty(df.height * max_events_per_row, dtype=EVENT_DTYPE)
    trade_side_code = {"buy": 1, "sell": -1, "infer": 0, "none": 0}[args.trade_side]
    price_only_depth_qty = 0.0 if args.price_only_depth_qty is None else args.price_only_depth_qty
    (
        output_rows,
        stats.trade_events,
        stats.depth_events,
        stats.opening_jump_qty,
        stats.best_bid_mismatches,
        stats.best_ask_mismatches,
        stats.qa_rows_checked,
    ) = _fill_events_from_columns(
        raw,
        exch_ts,
        local_ts,
        total_volume,
        last_price,
        bid_prices,
        bid_quantities,
        ask_prices,
        ask_quantities,
        args.volume_scale,
        price_only_depth_qty,
        args.price_only_depth_qty is not None,
        not args.no_trades and args.trade_side != "none",
        trade_side_code,
        not args.no_depth,
        args.qa_sample_rows,
    )
    stats.event_build_seconds = time.perf_counter() - started
    stats.raw_events = output_rows

    stats.first_exch_ts = int(np.min(exch_ts))
    stats.last_exch_ts = int(np.max(exch_ts))
    latency = local_ts - exch_ts
    stats.min_feed_latency = int(np.min(latency))
    stats.max_feed_latency = int(np.max(latency))

    started = time.perf_counter()
    data = correct_local_timestamp(raw[:output_rows], args.base_latency_ns)
    data = correct_event_order(data)
    validate_event_order(data)
    stats.normalize_seconds = time.perf_counter() - started
    stats.output_events = len(data)
    return data, stats


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
                    args.price_only_depth_qty,
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
    print(f"load_seconds={stats.load_seconds:.6f}")
    print(f"event_build_seconds={stats.event_build_seconds:.6f}")
    print(f"normalize_seconds={stats.normalize_seconds:.6f}")
    print(f"write_seconds={stats.write_seconds:.6f}")
    print(f"output={output}")


def convert(args: argparse.Namespace) -> np.ndarray:
    load_started = time.perf_counter()
    if args.data_api:
        rows = row_iter_from_data_api(args)
        load_seconds = 0.0  # The lazy row iterator performs its load during event building.
        build_started = time.perf_counter()
        data, stats = build_events_from_rows(rows, args)
        stats.event_build_seconds = time.perf_counter() - build_started
    elif args.daily_parquet:
        frame = load_daily_parquet_frame(args)
        load_seconds = time.perf_counter() - load_started
        data, stats = build_events_from_parquet_frame(frame, args)
    else:
        rows = row_iter_from_csv(args)
        load_seconds = 0.0  # CSV reading is lazy and included in event_build_seconds.
        build_started = time.perf_counter()
        data, stats = build_events_from_rows(rows, args)
        stats.event_build_seconds = time.perf_counter() - build_started
    stats.load_seconds = load_seconds

    args.output.parent.mkdir(parents=True, exist_ok=True)
    prices = data["px"]
    valid_prices = prices[np.isfinite(prices) & (prices > 0)]
    min_price = float(np.min(valid_prices)) if len(valid_prices) else math.nan
    max_price = float(np.max(valid_prices)) if len(valid_prices) else math.nan
    event_kind_mask = np.uint64(~EVENT_FLAG_MASK & np.iinfo(np.uint64).max)
    kinds = data["ev"].astype(np.uint64, copy=False) & event_kind_mask
    latency = data["local_ts"] - data["exch_ts"]
    save_npz = np.savez if getattr(args, "npz_compression", "compressed") == "uncompressed" else np.savez_compressed
    write_started = time.perf_counter()
    save_npz(
        args.output,
        data=data,
        min_price=np.asarray([min_price], dtype=np.float64),
        max_price=np.asarray([max_price], dtype=np.float64),
        event_rows=np.asarray([len(data)], dtype=np.int64),
        first_exch_ts=np.asarray([int(data["exch_ts"][0]) if len(data) else -1], dtype=np.int64),
        last_exch_ts=np.asarray([int(data["exch_ts"][-1]) if len(data) else -1], dtype=np.int64),
        min_latency_ns=np.asarray([int(np.min(latency)) if len(data) else -1], dtype=np.int64),
        max_latency_ns=np.asarray([int(np.max(latency)) if len(data) else -1], dtype=np.int64),
        depth_events=np.asarray(
            [int(np.sum(np.isin(kinds, [DEPTH_EVENT, DEPTH_CLEAR_EVENT, DEPTH_SNAPSHOT_EVENT])))],
            dtype=np.int64,
        ),
        trade_events=np.asarray([int(np.sum(kinds == TRADE_EVENT))], dtype=np.int64),
    )
    stats.write_seconds = time.perf_counter() - write_started
    print_summary(stats, args.output)
    return data


def default_output_path(
    workspace_root: Path,
    symbol: str,
    start_date: str,
    end_date: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    source_kind: str = "stock",
) -> Path:
    date_part = start_date.replace("-", "")
    if end_date and end_date != start_date:
        date_part = f"{date_part}_{end_date.replace('-', '')}"
    time_part = ""
    if start_time or end_time:
        start_label = (start_time or "start").replace(":", "").replace(".", "")
        end_label = (end_time or "end").replace(":", "").replace(".", "")
        time_part = f"_{start_label}_{end_label}"
    return workspace_root / "data" / output_folder_for_source(source_kind) / f"{symbol}_{date_part}{time_part}.npz"


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
    daily_parquet: bool | None = None,
    daily_parquet_dir: Path | None = None,
    path_config: Path | None = None,
    source_kind: str = "stock",
    data_platform_base: str = r"\\DC_TW\taiwan_stock\數據平台",
    index_backend: str = "duckdb",
    data_api_module_dir: Path | None = None,
    timezone_name: str = "Asia/Taipei",
    timestamp_unit: str = "auto",
    base_latency_ns: int = 0,
    volume_scale: float = 1.0,
    price_only_depth_qty: float | None = None,
    levels: int = 5,
    status_allow: list[str] | None = None,
    trade_side: str = "infer",
    no_depth: bool = False,
    no_trades: bool = False,
    qa_sample_rows: int = 1000,
    npz_compression: str = "compressed",
) -> tuple[Path, np.ndarray]:
    """Convert one Taiwan top-5 symbol/time window to an HftBacktest npz file."""
    root = Path.cwd() if workspace_root is None else workspace_root
    root = root.resolve()
    end_date = start_date if end_date is None else end_date
    source_kind = normalize_source_kind(source_kind)
    use_daily_parquet = False if daily_parquet is None else daily_parquet
    use_data_api = input_csv is None and not use_daily_parquet if data_api is None else data_api
    input_modes = [use_data_api, bool(input_csv), use_daily_parquet]
    if sum(bool(mode) for mode in input_modes) != 1:
        raise ValueError("choose exactly one input mode: data_api=True, daily_parquet=True, or input_csv=Path(...)")
    if use_daily_parquet:
        daily_parquet_dir = daily_parquet_dir or default_daily_parquet_dir(root, source_kind, path_config)
    if price_only_depth_qty is None and source_kind in PRICE_ONLY_DEPTH_SOURCE_KINDS:
        price_only_depth_qty = 1.0
    if npz_compression not in {"compressed", "uncompressed"}:
        raise ValueError("npz_compression must be 'compressed' or 'uncompressed'")

    tz = ZoneInfo(timezone_name)
    start_exch_ts = parse_timestamp(start_time, timestamp_unit, start_date, tz) if start_time else None
    end_exch_ts = parse_timestamp(end_time, timestamp_unit, end_date, tz) if end_time else None
    output_path = output or default_output_path(
        root,
        symbol,
        start_date,
        end_date,
        start_time,
        end_time,
        source_kind,
    )

    args = argparse.Namespace(
        output=output_path,
        input_csv=input_csv,
        data_api=use_data_api,
        daily_parquet=use_daily_parquet,
        daily_parquet_dir=daily_parquet_dir,
        path_config=path_config or root / "path.toml",
        source_kind=source_kind,
        start_date=start_date,
        end_date=end_date,
        data_platform_base=data_platform_base,
        index_backend=index_backend,
        data_api_module_dir=data_api_module_dir
        or default_data_api_module_dir(root),
        symbol=symbol,
        date=start_date,
        timezone=timezone_name,
        timestamp_unit=timestamp_unit,
        base_latency_ns=base_latency_ns,
        volume_scale=volume_scale,
        price_only_depth_qty=price_only_depth_qty,
        levels=levels,
        status_allow=status_allow,
        trade_side=trade_side,
        no_depth=no_depth,
        no_trades=no_trades,
        start_exch_ts=start_exch_ts,
        end_exch_ts=end_exch_ts,
        qa_sample_rows=qa_sample_rows,
        npz_compression=npz_compression,
    )
    data = convert(args)
    return output_path, data


def convert_tw_etf_to_npz(**kwargs) -> tuple[Path, np.ndarray]:
    """Convert Taiwan ETF daily parquet top-5 prices into HftBacktest events."""
    return convert_tw_stock_to_npz(
        source_kind="etf",
        data_api=False,
        daily_parquet=True,
        **kwargs,
    )


def convert_tw_odd_lot_to_npz(**kwargs) -> tuple[Path, np.ndarray]:
    """Convert Taiwan odd-lot daily parquet top-5 prices into HftBacktest events."""
    return convert_tw_stock_to_npz(
        source_kind="odd_lot",
        data_api=False,
        daily_parquet=True,
        **kwargs,
    )


def convert_tw_stock_future_to_npz(**kwargs) -> tuple[Path, np.ndarray]:
    """Convert Taiwan stock-future daily parquet top-5 depth into HftBacktest events."""
    return convert_tw_stock_to_npz(
        source_kind="stock_future",
        data_api=False,
        daily_parquet=True,
        **kwargs,
    )


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
