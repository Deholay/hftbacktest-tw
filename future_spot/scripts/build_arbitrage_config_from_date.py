from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage.config import load_config  # noqa: E402
from arbitrage.providers import FubonMarketDataProvider  # noqa: E402
from arbitrage.utils import (  # noqa: E402
    account_type_aliases,
    normalize_account_type,
    normalize_buy_sell,
    read_attr,
    read_float_attr,
    read_int_attr,
)


PRODUCT_KEYS = {"name", "spot_symbol", "future_symbol"}
MONTH_CODES = {code: index for index, code in enumerate("ABCDEFGHIJKL", start=1)}


@dataclass
class LiveStockPosition:
    tradable_qty: int = 0
    cost_price: float | None = None


@dataclass
class LiveFuturePosition:
    tradable_lot: int = 0
    cost_price: float | None = None


@dataclass(frozen=True)
class BuildArbitrageConfigResult:
    trade_date: str
    ldate: str
    output: Path
    target_output: Path | None
    pair_count: int
    target_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build target stock futures from a trade date and generate arbitrage_config.json."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Trade date used by daily.ipynb, e.g. 2026-05-22. The script uses its LDate for futures ticks.",
    )
    parser.add_argument(
        "--base-config",
        default="arbitrage_unified_config.example.json",
        help="Shared config containing pair_defaults.",
    )
    parser.add_argument(
        "--calendar",
        default="Calendar.csv",
        help="Calendar CSV with trade_dates and LDate columns.",
    )
    parser.add_argument(
        "--stockinfo",
        default="stockinfo.csv",
        help="Stock future mapping CSV.",
    )
    parser.add_argument(
        "--futures-parquet-template",
        default=r"Z:\ticks_parquet_stock_future\{ldate}.parquet",
        help="Futures parquet path template. Default uses LDate in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--twse-daytrade-template",
        default=r"Z:\TWSE\每日個股狀況\{date_nodash}.csv",
        help="TWSE day-trade status CSV path template. {date_nodash} is input date.",
    )
    parser.add_argument(
        "--tpex-daytrade-template",
        default=r"Z:\TPEX\每日個股狀況\{date_nodash}.csv",
        help="TPEX day-trade status CSV path template. {date_nodash} is input date.",
    )
    parser.add_argument(
        "--twse-daily-template",
        default=r"Z:\TWSE\每日資料\{ldate_nodash}.ftr",
        help="TWSE previous trading day daily file template.",
    )
    parser.add_argument(
        "--tpex-daily-template",
        default=r"Z:\TPEX\每日資料\{ldate_nodash}.ftr",
        help="TPEX previous trading day daily file template.",
    )
    parser.add_argument(
        "--session-start",
        default="08:45:00",
        help="Futures session start time for target volume calculation.",
    )
    parser.add_argument(
        "--session-end",
        default="13:45:00",
        help="Futures session end time for target volume calculation.",
    )
    parser.add_argument(
        "--min-future-volume",
        type=int,
        default=1000,
        help="Minimum futures total_volume.",
    )
    parser.add_argument(
        "--min-stock-volume",
        type=int,
        default=20_000_000,
        help="Minimum underlying stock volume on LDate.",
    )
    parser.add_argument(
        "--required-unit",
        type=int,
        default=2000,
        help="Required stock futures contract unit. Default keeps regular 2000-share contracts.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Generated arbitrage config output path. Default: output/arbitrage_config_YYYYMMDD.json.",
    )
    parser.add_argument(
        "--target-output",
        default=r"output\target_futures.csv",
        help="Optional CSV output for the filtered target futures.",
    )
    parser.add_argument(
        "--name-template",
        default="{spot_symbol}_{future_symbol}",
        help="Pair name template using {spot_symbol}, {future_symbol}, {future_prefix}.",
    )
    parser.add_argument(
        "--min-effective-tick-multiple",
        type=float,
        default=None,
        help="Override pair_defaults.min_effective_tick_multiple for generated pairs.",
    )
    parser.add_argument(
        "--exit-tick-multiple",
        type=float,
        default=None,
        help="Override pair_defaults.exit_tick_multiple for generated pairs.",
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
    build_arbitrage_config_from_date(
        args.date,
        base_config=args.base_config,
        calendar=args.calendar,
        stockinfo=args.stockinfo,
        futures_parquet_template=args.futures_parquet_template,
        twse_daytrade_template=args.twse_daytrade_template,
        tpex_daytrade_template=args.tpex_daytrade_template,
        twse_daily_template=args.twse_daily_template,
        tpex_daily_template=args.tpex_daily_template,
        session_start=args.session_start,
        session_end=args.session_end,
        min_future_volume=args.min_future_volume,
        min_stock_volume=args.min_stock_volume,
        required_unit=args.required_unit,
        output=args.output,
        target_output=args.target_output,
        name_template=args.name_template,
        min_effective_tick_multiple=args.min_effective_tick_multiple,
        exit_tick_multiple=args.exit_tick_multiple,
    )


def build_arbitrage_config_from_date(
    trade_date: str,
    *,
    base_config: str | Path = "arbitrage_unified_config.example.json",
    calendar: str | Path = "Calendar.csv",
    stockinfo: str | Path = "stockinfo.csv",
    futures_parquet_template: str = r"Z:\ticks_parquet_stock_future\{ldate}.parquet",
    twse_daytrade_template: str = r"Z:\TWSE\瘥??瘜{date_nodash}.csv",
    tpex_daytrade_template: str = r"Z:\TPEX\瘥??瘜{date_nodash}.csv",
    twse_daily_template: str = r"Z:\TWSE\瘥鞈?\{ldate_nodash}.ftr",
    tpex_daily_template: str = r"Z:\TPEX\瘥鞈?\{ldate_nodash}.ftr",
    session_start: str = "08:45:00",
    session_end: str = "13:45:00",
    min_future_volume: int = 1000,
    min_stock_volume: int = 20_000_000,
    required_unit: int = 2000,
    output: str | Path | None = None,
    target_output: str | Path | None = r"output\target_futures.csv",
    name_template: str = "{spot_symbol}_{future_symbol}",
    min_effective_tick_multiple: float | None = None,
    exit_tick_multiple: float | None = None,
) -> BuildArbitrageConfigResult:
    base_config_path = Path(base_config)

    trade_date = normalize_date(trade_date)
    ldate = get_ldate(Path(calendar), trade_date)
    logging.info("trade_date=%s ldate=%s", trade_date, ldate)

    base = read_json(base_config_path)

    futures_ohlcv = build_futures_session_ohlcv(
        path=Path(format_template(futures_parquet_template, trade_date, ldate)),
        session_start=session_start,
        session_end=session_end,
    )
    stock_info = read_stockinfo_frame(Path(stockinfo))
    stock_daily = read_stock_daily(
        trade_date=trade_date,
        ldate=ldate,
        twse_daytrade_template=twse_daytrade_template,
        tpex_daytrade_template=tpex_daytrade_template,
        twse_daily_template=twse_daily_template,
        tpex_daily_template=tpex_daily_template,
    )
    targets = build_targets(
        futures_ohlcv=futures_ohlcv,
        stock_info=stock_info,
        stock_daily=stock_daily,
        min_future_volume=min_future_volume,
        min_stock_volume=min_stock_volume,
        required_unit=required_unit,
    )
    if targets.empty:
        raise SystemExit("No target futures passed filters.")
    if front_month_only_enabled(base):
        targets = filter_front_month_targets(targets, trade_date=trade_date)

    target_output_path = None if target_output is None else Path(target_output)
    if target_output_path is not None:
        write_targets(targets, target_output_path)

    pair_defaults = extract_pair_defaults(base, base_config_path)
    if min_effective_tick_multiple is not None:
        pair_defaults["min_effective_tick_multiple"] = min_effective_tick_multiple
    if exit_tick_multiple is not None:
        pair_defaults["exit_tick_multiple"] = exit_tick_multiple
    pairs = build_pairs(targets, pair_defaults, name_template)
    output_config = {key: value for key, value in base.items() if key not in {"pair_defaults", "pairs"}}
    remove_replay_date(output_config)
    rebase_fubon_cert_paths(output_config, base_config_path.parent)
    if is_today(trade_date):
        pairs = apply_live_initial_positions(
            pairs=pairs,
            pair_defaults=pair_defaults,
            stock_info=stock_info,
            output_config=output_config,
            name_template=name_template,
        )
    output_config["pairs"] = pairs

    output_path = Path(output) if output else default_config_output_path(trade_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    load_config(output_path)
    logging.info("wrote %s pairs to %s", len(pairs), output_path)
    return BuildArbitrageConfigResult(
        trade_date=trade_date,
        ldate=ldate,
        output=output_path,
        target_output=target_output_path,
        pair_count=len(pairs),
        target_count=len(targets),
    )


def normalize_date(value: str) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def is_today(trade_date: str) -> bool:
    return trade_date == date.today().isoformat()


def default_config_output_path(trade_date: str) -> Path:
    return Path("output") / f"arbitrage_config_{trade_date.replace('-', '')}.json"


def get_ldate(calendar_path: Path, trade_date: str) -> str:
    calendar = pd.read_csv(calendar_path, dtype=str)
    matched = calendar.loc[calendar["trade_dates"] == trade_date, "LDate"]
    if matched.empty or pd.isna(matched.iloc[0]) or not str(matched.iloc[0]).strip():
        raise ValueError(f"{calendar_path} has no LDate for trade_date={trade_date}")
    return normalize_date(str(matched.iloc[0]))


def format_template(template: str, trade_date: str, ldate: str) -> str:
    date_nodash = trade_date.replace("-", "")
    ldate_nodash = ldate.replace("-", "")
    return template.format(
        date=trade_date,
        date_nodash=date_nodash,
        ldate=ldate,
        ldate_nodash=ldate_nodash,
    )


def build_futures_session_ohlcv(path: Path, session_start: str, session_end: str) -> pd.DataFrame:
    columns = ["symbol", "exchtime", "last_price", "trade_volume", "total_volume", "status"]
    logging.info("reading futures ticks: %s", path)
    ticks = pd.read_parquet(path, columns=columns)
    ticks["localtime"] = pd.to_datetime(ticks["exchtime"], unit="ns") + pd.Timedelta(hours=8)
    ticks["future_symbol"] = ticks["symbol"].astype(str).str.slice(0, 2)
    ticks = add_future_status_columns(ticks)
    return build_future_ohlcv(ticks, start_time=session_start, end_time=session_end)


def add_future_status_columns(df: pd.DataFrame) -> pd.DataFrame:
    status = df["status"].fillna(0).to_numpy(dtype=np.uint32, copy=False)
    return df.assign(
        build_type=(status & np.uint32(0xFF)).astype(np.uint8),
        match_flag=((status >> np.uint32(8)) & np.uint32(0xFF)).astype(np.uint8),
        orderbook_action=((status >> np.uint32(16)) & np.uint32(0xFF)).astype(np.uint8),
        continuous_flag=((status >> np.uint32(24)) & np.uint32(0xFF)).astype(np.uint8),
    )


def build_future_ohlcv(ticks: pd.DataFrame, start_time: str, end_time: str) -> pd.DataFrame:
    data = ticks[
        (ticks["match_flag"] == ord("0"))
        & (ticks["trade_volume"] > 0)
        & (ticks["last_price"] > 0)
    ].copy()
    data = data[data["localtime"].dt.time >= pd.to_datetime(start_time).time()]
    data = data[data["localtime"].dt.time <= pd.to_datetime(end_time).time()]
    data["month"] = data["symbol"].astype(str).str.slice(3, 4)
    data = data.sort_values(["symbol", "localtime"])

    result = (
        data.groupby("symbol", as_index=False)
        .agg(
            future_symbol=("future_symbol", "first"),
            open=("last_price", "first"),
            high=("last_price", "max"),
            low=("last_price", "min"),
            close=("last_price", "last"),
            volume=("total_volume", "max"),
            trades=("last_price", "size"),
            first_time=("localtime", "first"),
            last_time=("localtime", "last"),
            month=("month", "first"),
        )
        .sort_values("symbol")
        .reset_index(drop=True)
    )
    logging.info("built futures OHLCV rows=%s", len(result))
    return result


def read_stockinfo_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        encoding="cp950",
        header=None,
        usecols=[0, 2, 3, 11],
        names=["future_symbol", "underlying_symbol", "stock_name", "unit"],
        dtype={"future_symbol": str, "underlying_symbol": str, "stock_name": str},
    )
    frame["future_symbol"] = frame["future_symbol"].str.strip()
    frame["underlying_symbol"] = frame["underlying_symbol"].str.strip()
    frame["unit"] = frame["unit"].astype(str).str.replace(",", "", regex=False).str.strip().astype(int)
    return frame


def read_stock_daily(
    trade_date: str,
    ldate: str,
    twse_daytrade_template: str,
    tpex_daytrade_template: str,
    twse_daily_template: str,
    tpex_daily_template: str,
) -> pd.DataFrame:
    daytrade = read_daytrade_status(
        Path(format_template(twse_daytrade_template, trade_date, ldate)),
        Path(format_template(tpex_daytrade_template, trade_date, ldate)),
    )
    ldaily = read_ldaily(
        Path(format_template(twse_daily_template, trade_date, ldate)),
        Path(format_template(tpex_daily_template, trade_date, ldate)),
    )
    daily = daytrade.merge(
        ldaily[["symbol", "volume", "trades", "amount", "Open", "Close", "High", "Low", "Change"]],
        on="symbol",
        how="left",
    )
    return daily


def read_daytrade_status(twse_path: Path, tpex_path: Path) -> pd.DataFrame:
    frames = []
    for path in (twse_path, tpex_path):
        frame = pd.read_csv(path, dtype={"證券代號": str})
        frame = frame.rename(
            {"證券代號": "symbol", "暫停現股賣出後現款買進當沖註記": "StopDayTrade"},
            axis=1,
        )
        frame["symbol"] = frame["symbol"].astype(str).str.strip()
        frames.append(frame[["symbol", "StopDayTrade"]])
    return pd.concat(frames, ignore_index=True)


def read_ldaily(twse_path: Path, tpex_path: Path) -> pd.DataFrame:
    rename_dict = {
        "成交股數": "volume",
        "成交筆數": "trades",
        "成交金額": "amount",
        "開盤價": "Open",
        "收盤價": "Close",
        "最高價": "High",
        "最低價": "Low",
        "漲跌價差": "Change",
        "asset": "symbol",
    }
    frames = [pd.read_feather(path) for path in (tpex_path, twse_path)]
    frame = pd.concat(frames, axis=0, ignore_index=True).rename(rename_dict, axis=1)
    frame["symbol"] = frame["symbol"].astype(str).str.strip()
    return frame


def build_targets(
    futures_ohlcv: pd.DataFrame,
    stock_info: pd.DataFrame,
    stock_daily: pd.DataFrame,
    min_future_volume: int,
    min_stock_volume: int,
    required_unit: int,
) -> pd.DataFrame:
    target = futures_ohlcv.loc[futures_ohlcv["volume"] > min_future_volume].copy()
    target = target.merge(stock_info, on="future_symbol", how="left")
    target = target.merge(
        stock_daily,
        left_on="underlying_symbol",
        right_on="symbol",
        how="left",
        suffixes=("", "_StockDaily"),
    )
    target["underlying_symbol"] = target["underlying_symbol"].astype(str).str.strip()
    target = target.loc[
        (target["volume"] > min_future_volume)
        & (target["volume_StockDaily"] > min_stock_volume)
        & (target["StopDayTrade"] == "X")
        & (target["underlying_symbol"].between("1001", "9999"))
        & (target["unit"] == required_unit)
        & (target["underlying_symbol"] != "2303")
        & (target["underlying_symbol"] != "2330")
        # & (target["underlying_symbol"].isin(["2313","5347"]))
    ].reset_index(drop=True)
    logging.info("filtered target rows=%s", len(target))
    return target


def front_month_only_enabled(config: dict[str, Any]) -> bool:
    return bool(config.get("front_month_only", False))


def filter_front_month_targets(targets: pd.DataFrame, trade_date: str | None = None) -> pd.DataFrame:
    if targets.empty:
        return targets
    symbol_col = pick_existing_column(targets, ("symbol", "future_symbol_full", "future_contract", "future"))
    prefix_col = pick_existing_column(
        targets,
        ("future_symbol", "future_prefix", "underlying_future_symbol"),
        required=False,
    )
    work = targets.copy()
    if prefix_col is None:
        work["_front_month_prefix"] = work[symbol_col].astype(str).str.slice(0, 2)
        prefix_col = "_front_month_prefix"
    work["_front_month_key"] = work[symbol_col].map(lambda value: contract_month_sort_key(value, trade_date))
    before = len(work)
    filtered = (
        work.sort_values([prefix_col, "_front_month_key", symbol_col])
        .groupby(prefix_col, as_index=False, sort=False)
        .head(1)
        .drop(columns=["_front_month_key", "_front_month_prefix"], errors="ignore")
        .reset_index(drop=True)
    )
    logging.info("front_month_only filtered target rows=%s -> %s", before, len(filtered))
    return filtered


def pick_existing_column(frame: pd.DataFrame, names: tuple[str, ...], required: bool = True) -> str | None:
    normalized = {str(column).lower(): str(column) for column in frame.columns}
    for name in names:
        column = normalized.get(name.lower())
        if column is not None:
            return column
    if required:
        raise ValueError(f"target file missing one of columns: {', '.join(names)}")
    return None


def contract_month_sort_key(symbol: Any, trade_date: str | None = None) -> tuple[int, int, str]:
    text = clean_symbol(symbol)
    month_code = text[3:4].upper() if len(text) >= 4 else ""
    month = MONTH_CODES.get(month_code, 99)
    year_digit_text = text[4:5] if len(text) >= 5 else ""
    year_digit = int(year_digit_text) if year_digit_text.isdigit() else 99
    if trade_date is None or month == 99 or year_digit == 99:
        return year_digit, month, text

    trade = pd.Timestamp(trade_date)
    for year in range(trade.year - 1, trade.year + 11):
        if year % 10 == year_digit and (year, month) >= (trade.year, trade.month):
            return year, month, text
    return trade.year + 100, month, text


def write_targets(targets: pd.DataFrame, path: Path) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    targets.to_csv(path, index=False, encoding="utf-8-sig")
    logging.info("wrote targets to %s", path)


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def remove_replay_date(config: dict[str, Any]) -> None:
    historical = config.get("historical")
    if isinstance(historical, dict):
        historical.pop("replay_date", None)


def rebase_fubon_cert_paths(config: dict[str, Any], base_dir: Path) -> None:
    fubon = config.get("fubon")
    if not isinstance(fubon, dict):
        return
    for section in ("stock", "futures"):
        login = fubon.get(section)
        if not isinstance(login, dict):
            continue
        cert_path = login.get("cert_path")
        if not cert_path:
            continue
        path = Path(str(cert_path))
        if not path.is_absolute():
            login["cert_path"] = str((base_dir / path).resolve())


def extract_pair_defaults(base: dict[str, Any], path: Path) -> dict[str, Any]:
    if isinstance(base.get("pair_defaults"), dict):
        defaults = dict(base["pair_defaults"])
    else:
        existing_pairs = base.get("pairs") or []
        if not existing_pairs:
            raise ValueError(f"{path} must contain pair_defaults or at least one pair as template")
        defaults = {key: value for key, value in existing_pairs[0].items() if key not in PRODUCT_KEYS}
        inconsistent = []
        for pair in existing_pairs[1:]:
            candidate = {key: value for key, value in pair.items() if key not in PRODUCT_KEYS}
            if candidate != defaults:
                inconsistent.append(pair.get("name", pair.get("future_symbol", "<unknown>")))
        if inconsistent:
            raise ValueError(
                f"{path} has no pair_defaults and non-product fields differ across pairs; "
                f"first inconsistent pairs={inconsistent[:10]}"
            )
    missing = [
        key
        for key in (
            "spot_shares_per_pair",
            "future_shares_per_pair",
            "spot_order_qty",
            "future_order_qty",
            "entry_threshold_pct",
            "stop_loss_pct",
        )
        if key not in defaults
    ]
    if missing:
        raise ValueError(f"{path} pair defaults missing required keys: {', '.join(missing)}")
    defaults.setdefault("exit_threshold_pct", 0.0)
    defaults.setdefault("exit_tick_multiple", 1.0)
    defaults.setdefault("min_effective_tick_multiple", 0.0)
    return defaults


def build_pairs(targets: pd.DataFrame, pair_defaults: dict[str, Any], name_template: str) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for _, row in targets.iterrows():
        future_symbol = clean_symbol(row["symbol"])
        future_prefix = clean_symbol(row["future_symbol"])
        spot_symbol = clean_symbol(row["underlying_symbol"])
        if not future_symbol or not spot_symbol or future_symbol in seen:
            continue
        pair = dict(pair_defaults)
        pair.update(
            {
                "name": name_template.format(
                    spot_symbol=spot_symbol,
                    future_symbol=future_symbol,
                    future_prefix=future_prefix,
                ),
                "spot_symbol": spot_symbol,
                "future_symbol": future_symbol,
            }
        )
        pairs.append(pair)
        seen.add(future_symbol)
    if not pairs:
        raise ValueError("No pairs generated from targets")
    return pairs


def apply_live_initial_positions(
    pairs: list[dict[str, Any]],
    pair_defaults: dict[str, Any],
    stock_info: pd.DataFrame,
    output_config: dict[str, Any],
    name_template: str,
) -> list[dict[str, Any]]:
    stock_positions, future_positions = load_live_positions(output_config)
    positions_by_future = build_initial_positions_from_live(
        pairs=pairs,
        pair_defaults=pair_defaults,
        stock_info=stock_info,
        stock_positions=stock_positions,
        future_positions=future_positions,
        name_template=name_template,
    )
    if not positions_by_future:
        logging.info("no live long-spot/short-future positions found for generated config")
        return pairs

    pairs_by_future = {pair["future_symbol"]: pair for pair in pairs}
    for future_symbol, pair in positions_by_future.items():
        if future_symbol not in pairs_by_future:
            pairs.append(pair)
            pairs_by_future[future_symbol] = pair
            logging.info("added live position pair not in target: %s", future_symbol)
        else:
            pairs_by_future[future_symbol]["initial_position"] = pair["initial_position"]
        logging.info(
            "loaded live initial_position pair=%s quantity=%s",
            pairs_by_future[future_symbol]["name"],
            pairs_by_future[future_symbol]["initial_position"]["quantity"],
        )
    return pairs


def load_live_positions(
    output_config: dict[str, Any],
) -> tuple[dict[str, LiveStockPosition], dict[tuple[str, str], LiveFuturePosition]]:
    try:
        from fubon_neo.sdk import FubonSDK
    except ImportError as exc:
        raise RuntimeError("fubon_neo SDK is required to load live positions for today's config") from exc

    fubon = output_config.get("fubon")
    if not isinstance(fubon, dict):
        raise ValueError("config missing fubon settings; cannot load live positions")

    stock_sdk = FubonSDK()
    futures_sdk = FubonSDK()
    try:
        stock_account = login_first_account(stock_sdk, "stock", fubon.get("stock") or {})
        futures_account = login_first_account(futures_sdk, "futopt", fubon.get("futures") or {})
        return (
            load_stock_inventory_by_symbol(stock_sdk, stock_account),
            load_future_position_lots_by_symbol_and_side(futures_sdk, futures_account),
        )
    finally:
        logout_sdk(stock_sdk)
        logout_sdk(futures_sdk)


def login_first_account(sdk: Any, account_type: str, login_config: dict[str, Any]) -> Any:
    missing = [
        key
        for key, value in {
            f"fubon.{account_type}.personal_id": login_config.get("personal_id"),
            f"fubon.{account_type}.password": login_config.get("password"),
            f"fubon.{account_type}.cert_path": login_config.get("cert_path"),
            f"fubon.{account_type}.cert_pass": login_config.get("cert_pass"),
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing config values: {', '.join(missing)}")

    accounts = sdk.login(
        str(login_config["personal_id"]),
        str(login_config["password"]),
        str(login_config["cert_path"]),
        str(login_config["cert_pass"]),
    )
    return first_account(accounts, account_type)


def first_account(accounts: Any, account_type: str) -> Any:
    data = read_attr(accounts, "data")
    if isinstance(data, list):
        if not data:
            raise RuntimeError(f"Fubon login returned empty account list for {account_type}")
        expected = account_type_aliases(account_type)
        for account in data:
            actual = normalize_account_type(read_attr(account, "account_type", "accountType"))
            if actual in expected:
                return account
        returned_types = [
            normalize_account_type(read_attr(account, "account_type", "accountType"))
            for account in data
        ]
        raise RuntimeError(
            f"Fubon login did not return a {account_type} account; returned account types: {returned_types}"
        )
    if data is not None:
        actual = normalize_account_type(read_attr(data, "account_type", "accountType"))
        if actual in account_type_aliases(account_type):
            return data
    raise RuntimeError(f"Fubon login returned no account data for {account_type}")


def logout_sdk(sdk: Any) -> None:
    logout = getattr(sdk, "logout", None)
    if logout is None:
        return
    try:
        logout()
    except TypeError:
        pass
    except Exception:
        logging.debug("Fubon logout failed", exc_info=True)


def load_stock_inventory_by_symbol(sdk: Any, account: Any) -> dict[str, LiveStockPosition]:
    accounting = getattr(sdk, "accounting", None)
    unrealized = (
        getattr(accounting, "unrealized_gains_and_loses", None)
        or getattr(accounting, "unrealized_gains_and_losses", None)
    )
    if unrealized is None:
        raise RuntimeError("Fubon stock SDK does not expose accounting.unrealized_gains_and_loses")

    result = call_accounting_api(unrealized, account, "stock unrealized gains and loses")
    positions: dict[str, LiveStockPosition] = {}
    for row in result_rows(result):
        symbol = str(read_attr(row, "stock_no", "stockNo", "symbol") or "")
        if not symbol:
            continue
        buy_sell_raw = read_attr(row, "buy_sell", "buySell", "side")
        buy_sell = normalize_buy_sell(buy_sell_raw)
        if buy_sell_raw not in (None, "") and buy_sell not in {"buy", "long"}:
            continue
        tradable_qty = read_int_attr(row, "tradable_qty", "tradableQty")
        if tradable_qty <= 0:
            logging.debug("skip non-positive stock long position tradable_qty=%s row=%s", tradable_qty, row)
            continue
        cost_price = optional_positive_float(row, "cost_price", "costPrice", "avg_price", "avgPrice")
        add_stock_position(positions, symbol, tradable_qty, cost_price)
    logging.info("loaded live stock unrealized position symbols=%s", len(positions))
    return positions


def load_future_position_lots_by_symbol_and_side(
    sdk: Any,
    account: Any,
) -> dict[tuple[str, str], LiveFuturePosition]:
    accounting = getattr(sdk, "futopt_accounting", None)
    query_hybrid_position = getattr(accounting, "query_hybrid_position", None)
    if query_hybrid_position is None:
        raise RuntimeError("Fubon futures SDK does not expose futopt_accounting.query_hybrid_position")

    result = call_accounting_api(query_hybrid_position, account, "futures hybrid positions")
    positions: dict[tuple[str, str], LiveFuturePosition] = {}
    for row in result_rows(result):
        symbol = str(read_attr(row, "symbol", "stock_no", "stockNo") or "")
        if not symbol:
            continue
        mapped_symbol = FubonMarketDataProvider._mapped_futopt_symbol(symbol, row) or symbol
        buy_sell = normalize_buy_sell(read_attr(row, "buy_sell", "buySell"))
        tradable_lot = read_int_attr(row, "tradable_lot", "tradableLot", "lot")
        if buy_sell not in {"buy", "sell"}:
            continue
        cost_price = optional_positive_float(
            row,
            "cost_price",
            "costPrice",
            "avg_price",
            "avgPrice",
            "average_price",
            "averagePrice",
            "price",
        )
        key = (mapped_symbol, buy_sell)
        add_future_position(positions, key, tradable_lot, cost_price)
    logging.info("loaded live futures position rows=%s", len(positions))
    return positions


def optional_positive_float(row: Any, *names: str) -> float | None:
    value = read_float_attr(row, *names)
    return value if value > 0 else None


def weighted_cost(old_cost: float | None, old_qty: int, new_cost: float | None, new_qty: int) -> float | None:
    if new_cost is None or new_qty <= 0:
        return old_cost
    if old_cost is None or old_qty <= 0:
        return new_cost
    return ((old_cost * old_qty) + (new_cost * new_qty)) / (old_qty + new_qty)


def add_stock_position(
    positions: dict[str, LiveStockPosition],
    symbol: str,
    tradable_qty: int,
    cost_price: float | None,
) -> None:
    current = positions.get(symbol, LiveStockPosition())
    current.cost_price = weighted_cost(current.cost_price, current.tradable_qty, cost_price, tradable_qty)
    current.tradable_qty += tradable_qty
    positions[symbol] = current


def add_future_position(
    positions: dict[tuple[str, str], LiveFuturePosition],
    key: tuple[str, str],
    tradable_lot: int,
    cost_price: float | None,
) -> None:
    current = positions.get(key, LiveFuturePosition())
    current.cost_price = weighted_cost(current.cost_price, current.tradable_lot, cost_price, tradable_lot)
    current.tradable_lot += tradable_lot
    positions[key] = current


def call_accounting_api(method: Any, account: Any, label: str) -> Any:
    try:
        return method(account)
    except TypeError as exc:
        logging.debug("%s query with account argument failed; retrying without account: %s", label, exc)
        return method()


def result_rows(result: Any) -> list[Any]:
    data = read_attr(result, "data")
    if data is None:
        data = result
    if isinstance(data, list):
        return data
    if isinstance(data, tuple):
        return list(data)
    if isinstance(data, dict):
        for key in ("data", "items", "inventories", "positions"):
            value = data.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, tuple):
                return list(value)
        return [data]
    return [data] if data is not None else []


def build_initial_positions_from_live(
    pairs: list[dict[str, Any]],
    pair_defaults: dict[str, Any],
    stock_info: pd.DataFrame,
    stock_positions: dict[str, LiveStockPosition],
    future_positions: dict[tuple[str, str], LiveFuturePosition],
    name_template: str,
) -> dict[str, dict[str, Any]]:
    pairs_by_future = {pair["future_symbol"]: pair for pair in pairs}
    spot_by_future_prefix = {
        clean_symbol(row["future_symbol"]): clean_symbol(row["underlying_symbol"])
        for _, row in stock_info.iterrows()
    }
    live_pairs: dict[str, dict[str, Any]] = {}
    for (future_symbol, side), future_position in future_positions.items():
        future_lots = future_position.tradable_lot
        if side != "sell" or future_lots <= 0:
            continue
        base_pair = pairs_by_future.get(future_symbol)
        if base_pair is None:
            future_prefix = future_symbol[:2]
            spot_symbol = spot_by_future_prefix.get(future_prefix, "")
            if not spot_symbol:
                logging.warning("skip live future position %s: no stockinfo mapping", future_symbol)
                continue
            base_pair = dict(pair_defaults)
            base_pair.update(
                {
                    "name": name_template.format(
                        spot_symbol=spot_symbol,
                        future_symbol=future_symbol,
                        future_prefix=future_prefix,
                    ),
                    "spot_symbol": spot_symbol,
                    "future_symbol": future_symbol,
                }
            )
        stock_position = stock_positions.get(str(base_pair["spot_symbol"]), LiveStockPosition())
        stock_qty = stock_position.tradable_qty
        stock_units = stock_qty // int(base_pair["spot_order_qty"])
        future_units = future_lots // int(base_pair["future_order_qty"])
        quantity = min(stock_units, future_units)
        if quantity <= 0:
            logging.warning(
                "skip live future position %s: stock_qty=%s future_lots=%s cannot form a pair",
                future_symbol,
                stock_qty,
                future_lots,
            )
            continue
        pair = dict(base_pair)
        entry_spot_price = stock_position.cost_price
        entry_future_price = future_position.cost_price
        entry_basis_pct = None
        if entry_spot_price and entry_future_price:
            entry_basis_pct = (entry_future_price - entry_spot_price) / entry_spot_price
        pair["initial_position"] = {
            "quantity": int(quantity),
            "direction": "ENTER_LONG_SPOT_SHORT_FUTURE",
            "entry_basis_pct": entry_basis_pct,
            "entry_spot_price": entry_spot_price,
            "entry_future_price": entry_future_price,
        }
        live_pairs[future_symbol] = pair
    return live_pairs


def clean_symbol(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


if __name__ == "__main__":
    main()
