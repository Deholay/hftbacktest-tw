from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from arbitrage.utils import account_type_aliases, normalize_account_type, read_attr


def configure_output_encoding() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor Fubon futures margin equity.")
    parser.add_argument(
        "--config",
        default="output/arbitrage_config_20260618.json",
        help="Path to a JSON config containing fubon.futures login settings.",
    )
    parser.add_argument("--interval-sec", type=float, default=60.0, help="Seconds between queries.")
    parser.add_argument("--iterations", type=int, default=None, help="Stop after N queries. Default runs forever.")
    parser.add_argument(
        "--output",
        default=None,
        help="JSONL output path. Default: output/margin_equity/margin_equity_YYYYMMDD_HHMMSS.jsonl.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def load_futures_login_config(config_path: str | Path) -> dict[str, str]:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as file:
        raw = json.load(file)

    fubon = raw.get("fubon")
    if not isinstance(fubon, dict):
        raise ValueError(f"{path} missing fubon settings")

    futures = fubon.get("futures")
    if not isinstance(futures, dict):
        raise ValueError(f"{path} missing fubon.futures settings")

    login_config = {
        "personal_id": str(futures.get("personal_id", "")),
        "password": str(futures.get("password", "")),
        "cert_path": str(futures.get("cert_path", "")),
        "cert_pass": str(futures.get("cert_pass", "")),
    }
    cert_path = login_config["cert_path"]
    if cert_path and not Path(cert_path).is_absolute():
        login_config["cert_path"] = str((path.parent / cert_path).resolve())
    return login_config


def login_first_account(sdk: Any, login_config: dict[str, str]) -> Any:
    missing = [f"fubon.futures.{key}" for key, value in login_config.items() if not value]
    if missing:
        raise RuntimeError(f"Missing config values: {', '.join(missing)}")

    accounts = sdk.login(
        login_config["personal_id"],
        login_config["password"],
        login_config["cert_path"],
        login_config["cert_pass"],
    )
    return first_account(accounts, "futopt")


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


def query_margin_equity(sdk: Any, account: Any) -> Any:
    accounting = getattr(sdk, "futopt_accounting", None)
    query = getattr(accounting, "query_margin_equity", None)
    if query is None:
        raise RuntimeError("Fubon futures SDK does not expose futopt_accounting.query_margin_equity")

    try:
        return query(account)
    except TypeError as exc:
        logging.debug("query_margin_equity(account) failed; retrying without account: %s", exc)
        return query()


def to_plain(value: Any, depth: int = 0) -> Any:
    if depth > 5:
        return repr(value)
    if value in (None, "") or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): to_plain(item, depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_plain(item, depth + 1) for item in value]
    if hasattr(value, "__dict__"):
        return {
            str(key): to_plain(item, depth + 1)
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }

    attrs: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if callable(item):
            continue
        if isinstance(item, (str, int, float, bool, type(None), list, tuple, dict)):
            attrs[name] = to_plain(item, depth + 1)
    return attrs or repr(value)


def extract_data(result: Any) -> Any:
    plain = to_plain(result)
    if isinstance(plain, dict) and "data" in plain:
        return plain["data"]
    return plain


def extract_rows(result: Any) -> list[Any]:
    data = read_attr(result, "data")
    if data is None:
        data = result
    if isinstance(data, list):
        return data
    if isinstance(data, tuple):
        return list(data)
    return [data] if data is not None else []


def calc_margin_risk(equity: Any) -> dict[str, float | str]:
    today_equity = read_required_float(equity, "today_equity", "todayEquity")
    initial_margin = read_required_float(equity, "initial_margin", "initialMargin")
    maintenance_margin = read_required_float(equity, "maintenance_margin", "maintenanceMargin")
    excess_margin = read_required_float(equity, "excess_margin", "excessMargin")
    available_margin = read_required_float(equity, "available_margin", "availableMargin")
    disgorgement = read_required_float(equity, "disgorgement")
    fut_unrealized_pnl = read_required_float(equity, "fut_unrealized_pnl", "futUnrealizedPnl")

    risk_initial = today_equity / initial_margin if initial_margin > 0 else float("inf")
    risk_maintenance = today_equity / maintenance_margin if maintenance_margin > 0 else float("inf")

    need_to_120 = max(0, initial_margin * 1.2 - today_equity)
    need_to_130 = max(0, initial_margin * 1.3 - today_equity)
    buffer_to_initial = today_equity - initial_margin
    buffer_to_maintenance = today_equity - maintenance_margin

    if disgorgement > 0 or risk_maintenance < 1.05:
        level = "立即處理"
    elif risk_initial < 1.10 or excess_margin < 20000:
        level = "高度危險"
    elif risk_initial < 1.20:
        level = "準備補保證金"
    elif risk_initial < 1.30:
        level = "偏緊"
    else:
        level = "正常"

    return {
        "today_equity": today_equity,
        "initial_margin": initial_margin,
        "maintenance_margin": maintenance_margin,
        "risk_initial_pct": risk_initial * 100,
        "risk_maintenance_pct": risk_maintenance * 100,
        "need_to_120": need_to_120,
        "need_to_130": need_to_130,
        "buffer_to_initial": buffer_to_initial,
        "buffer_to_maintenance": buffer_to_maintenance,
        "excess_margin": excess_margin,
        "available_margin": available_margin,
        "disgorgement": disgorgement,
        "fut_unrealized_pnl": fut_unrealized_pnl,
        "level": level,
    }


def read_required_float(obj: Any, *names: str) -> float:
    value = read_attr(obj, *names)
    if value in (None, ""):
        raise ValueError(f"margin equity result missing field: {'/'.join(names)}")
    return float(value)


def flatten(data: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(data, dict):
        flattened: dict[str, Any] = {}
        for key, value in data.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flatten(value, child_prefix))
        return flattened
    if isinstance(data, list):
        return {prefix or "value": json.dumps(data, ensure_ascii=False)}
    return {prefix or "value": data}


def display_snapshot(timestamp: str, data: Any) -> None:
    rows = data if isinstance(data, list) else [data]
    if not rows:
        print(f"{timestamp} no margin equity data")
        return

    for index, row in enumerate(rows, start=1):
        flat = flatten(row)
        selected = select_display_fields(flat)
        suffix = f" row={index}" if len(rows) > 1 else ""
        if selected:
            details = " ".join(f"{key}={value}" for key, value in selected.items())
        else:
            details = json.dumps(row, ensure_ascii=False, default=str)
        print(f"{timestamp}{suffix} {details}")


def display_margin_risk(timestamp: str, rows: list[Any]) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    if not rows:
        print(f"{timestamp} no margin equity data")
        return snapshots

    for index, equity in enumerate(rows, start=1):
        plain_equity = to_plain(equity)
        try:
            risk = calc_margin_risk(equity)
        except Exception as exc:
            print(f"{timestamp} row={index} failed to calculate margin risk: {exc}; raw={plain_equity}")
            snapshots.append({"row": index, "raw": plain_equity, "error": str(exc)})
            continue

        suffix = f" row={index}" if len(rows) > 1 else ""
        print(f"{timestamp}{suffix} {format_margin_risk(risk)}")
        snapshots.append({"row": index, "raw": plain_equity, "risk": risk})
    return snapshots


def format_margin_risk(risk: dict[str, Any]) -> str:
    return " ".join(
        (
            f"level={risk['level']}",
            f"today_equity={format_number(risk['today_equity'])}",
            f"initial_margin={format_number(risk['initial_margin'])}",
            f"maintenance_margin={format_number(risk['maintenance_margin'])}",
            f"risk_initial={format_pct(risk['risk_initial_pct'])}",
            f"risk_maintenance={format_pct(risk['risk_maintenance_pct'])}",
            f"need_to_120={format_number(risk['need_to_120'])}",
            f"need_to_130={format_number(risk['need_to_130'])}",
            f"buffer_to_initial={format_number(risk['buffer_to_initial'])}",
            f"buffer_to_maintenance={format_number(risk['buffer_to_maintenance'])}",
            f"excess_margin={format_number(risk['excess_margin'])}",
            f"available_margin={format_number(risk['available_margin'])}",
            f"disgorgement={format_number(risk['disgorgement'])}",
            f"fut_unrealized_pnl={format_number(risk['fut_unrealized_pnl'])}",
        )
    )


def format_number(value: Any) -> str:
    try:
        return f"{float(value):,.0f}"
    except (TypeError, ValueError):
        return str(value)


def format_pct(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return str(value)


def select_display_fields(flat: dict[str, Any]) -> dict[str, Any]:
    candidates = (
        "currency",
        "account",
        "branch_no",
        "branchNo",
        "equity",
        "margin_equity",
        "marginEquity",
        "available_margin",
        "availableMargin",
        "available_balance",
        "availableBalance",
        "initial_margin",
        "initialMargin",
        "maintenance_margin",
        "maintenanceMargin",
        "today_balance",
        "todayBalance",
        "unrealized_pnl",
        "unrealizedPnl",
        "floating_profit_loss",
        "floatingProfitLoss",
        "risk_indicator",
        "riskIndicator",
    )
    selected: dict[str, Any] = {}
    lower_to_key = {key.lower(): key for key in flat}
    for candidate in candidates:
        key = lower_to_key.get(candidate.lower())
        if key is not None and flat[key] not in (None, ""):
            selected[key] = flat[key]
    return selected


def default_output_path() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("output") / "margin_equity" / f"margin_equity_{timestamp}.jsonl"


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


def configure_logging(log_level: str) -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s", force=True)


def main() -> None:
    configure_output_encoding()
    args = parse_args()
    configure_logging(args.log_level)

    try:
        from fubon_neo.sdk import FubonSDK
    except ImportError as exc:
        raise RuntimeError("fubon_neo SDK is not installed. Install Fubon Neo SDK first.") from exc

    output_path = Path(args.output) if args.output else default_output_path()
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    login_config = load_futures_login_config(args.config)
    sdk = FubonSDK()
    try:
        account = login_first_account(sdk, login_config)
        logging.info(
            "selected futopt account branch_no=%s account=%s account_type=%s",
            read_attr(account, "branch_no", "branchNo"),
            read_attr(account, "account"),
            read_attr(account, "account_type", "accountType"),
        )
        logging.info("margin equity snapshots will be written to %s", output_path)

        count = 0
        with output_path.open("a", encoding="utf-8") as file:
            while args.iterations is None or count < args.iterations:
                timestamp = datetime.now().isoformat(timespec="seconds")
                result = query_margin_equity(sdk, account)
                plain_result = to_plain(result)
                snapshots = display_margin_risk(timestamp, extract_rows(result))
                file.write(
                    json.dumps(
                        {
                            "timestamp": timestamp,
                            "result": plain_result,
                            "snapshots": snapshots,
                        },
                        ensure_ascii=False,
                        default=str,
                    )
                    + "\n"
                )
                file.flush()
                count += 1
                if args.iterations is not None and count >= args.iterations:
                    break
                time.sleep(max(args.interval_sec, 0.0))
    except KeyboardInterrupt:
        logging.info("received Ctrl+C, shutting down")
    finally:
        logout_sdk(sdk)


if __name__ == "__main__":
    main()
