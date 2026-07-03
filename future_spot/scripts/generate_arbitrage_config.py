from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage.config import load_config
from scripts.build_arbitrage_config_from_date import filter_front_month_targets, front_month_only_enabled


PRODUCT_KEYS = {"name", "spot_symbol", "future_symbol"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate arbitrage_config.json pairs from target futures and shared defaults."
    )
    parser.add_argument(
        "--base-config",
        default="arbitrage_unified_config.example.json",
        help="Shared config containing pair_defaults, or an existing config with one pair as template.",
    )
    parser.add_argument(
        "--targets",
        default=r"output\target_futures.csv",
        help="CSV/Parquet target futures file exported from daily.ipynb.",
    )
    parser.add_argument(
        "--stockinfo",
        default="stockinfo.csv",
        help="Stock future mapping csv. Column 0 is future prefix, column 2 is underlying stock.",
    )
    parser.add_argument(
        "--output",
        default="arbitrage_config.json",
        help="Generated arbitrage config output path.",
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
        "--encoding",
        default="utf-8-sig",
        help="Target CSV encoding.",
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

    base = read_json(Path(args.base_config))
    pair_defaults = extract_pair_defaults(base, Path(args.base_config))
    if args.min_effective_tick_multiple is not None:
        pair_defaults["min_effective_tick_multiple"] = args.min_effective_tick_multiple
    if args.exit_tick_multiple is not None:
        pair_defaults["exit_tick_multiple"] = args.exit_tick_multiple
    targets = read_table(Path(args.targets), encoding=args.encoding)
    if front_month_only_enabled(base):
        targets = filter_front_month_targets(targets)
    mapping = read_stockinfo(Path(args.stockinfo))
    pairs = build_pairs(
        targets=targets,
        mapping=mapping,
        pair_defaults=pair_defaults,
        name_template=args.name_template,
    )

    output_config = {key: value for key, value in base.items() if key not in {"pair_defaults", "pairs"}}
    remove_replay_date(output_config)
    rebase_fubon_cert_paths(output_config, Path(args.base_config).parent)
    output_config["pairs"] = pairs

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(output_config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    load_config(output)
    logging.info("wrote %s pairs to %s", len(pairs), output)


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
        if len(existing_pairs) != 1:
            raise ValueError(
                f"{path} must contain pair_defaults, or exactly one item in pairs to use as template"
            )
        defaults = {key: value for key, value in existing_pairs[0].items() if key not in PRODUCT_KEYS}

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


def read_table(path: Path, encoding: str) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        frame = pd.read_parquet(path)
    elif suffix == ".json":
        frame = pd.read_json(path)
    else:
        frame = pd.read_csv(path, encoding=encoding)
    if frame.empty:
        raise ValueError(f"{path} has no rows")
    return frame


def read_stockinfo(path: Path) -> dict[str, str]:
    last_error: Exception | None = None
    for encoding in ("cp950", "utf-8-sig", "big5", "latin1"):
        try:
            frame = pd.read_csv(path, header=None, encoding=encoding, usecols=[0, 2])
            frame.columns = ["future_prefix", "spot_symbol"]
            frame["future_prefix"] = frame["future_prefix"].astype(str).str.strip()
            frame["spot_symbol"] = frame["spot_symbol"].astype(str).str.strip()
            return dict(zip(frame["future_prefix"], frame["spot_symbol"], strict=False))
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"failed to read stockinfo mapping from {path}: {last_error}") from last_error


def build_pairs(
    targets: pd.DataFrame,
    mapping: dict[str, str],
    pair_defaults: dict[str, Any],
    name_template: str,
) -> list[dict[str, Any]]:
    future_col = pick_column(targets, ("symbol", "future_symbol_full", "future_contract", "future"))
    prefix_col = pick_column(targets, ("future_symbol", "future_prefix", "underlying_future_symbol"), required=False)
    spot_col = pick_column(targets, ("underlying_symbol", "spot_symbol", "stock_symbol"), required=False)

    pairs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for _, row in targets.iterrows():
        future_symbol = clean_symbol(row.get(future_col))
        if not future_symbol or future_symbol in seen:
            continue
        future_prefix = clean_symbol(row.get(prefix_col)) if prefix_col else future_symbol[:2]
        spot_symbol = clean_symbol(row.get(spot_col)) if spot_col else ""
        if not spot_symbol:
            spot_symbol = mapping.get(future_prefix, "")
        if not spot_symbol:
            logging.warning("skip %s: no spot mapping for future_prefix=%s", future_symbol, future_prefix)
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
        raise ValueError("No pairs were generated from target file")
    return pairs


def pick_column(frame: pd.DataFrame, names: tuple[str, ...], required: bool = True) -> str | None:
    normalized = {str(column).lower(): str(column) for column in frame.columns}
    for name in names:
        if name.lower() in normalized:
            return normalized[name.lower()]
    if required:
        raise ValueError(f"target file missing one of columns: {', '.join(names)}")
    return None


def clean_symbol(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


if __name__ == "__main__":
    main()
