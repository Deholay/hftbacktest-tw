"""Runtime configuration shared by the notebook and full backtest CLI."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
for _path in (PROJECT_ROOT, WORKSPACE_ROOT, PROJECT_ROOT / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from arbitrage.full_market_runner import parse_args, resolve_output_dir, resolve_project_path  # noqa: E402


def default_notebook_args(**overrides: Any) -> Namespace:
    """Return CLI-compatible defaults with the notebook's latency settings."""
    args = parse_args([])
    defaults: dict[str, Any] = {
        "start_date": "2026-05-21",
        "end_date": "2026-05-26",
        # Let resolve_output_dir derive the folder from the effective dates and
        # latency settings. A caller can still pass output_dir explicitly.
        "output_dir": None,
        "futures_parquet_template": "/mnt/z/ticks_parquet_stock_future/{ldate}.parquet",
        "twse_daytrade_template": "/mnt/z/TWSE/每日個股狀況/{date_nodash}.csv",
        "tpex_daytrade_template": "/mnt/z/TPEX/每日個股狀況/{date_nodash}.csv",
        "twse_daily_template": "/mnt/z/TWSE/每日資料/{ldate_nodash}.ftr",
        "tpex_daily_template": "/mnt/z/TPEX/每日資料/{ldate_nodash}.ftr",
        "spot_input_csv_template": "",
        "data_platform_base": "/mnt/z/數據平台",
        "event_futures_parquet_dir": Path("/mnt/z/ticks_parquet_stock_future"),
        "session_end": "13:25:00",
        "order_latency_ms": 10.0,
        "response_latency_ms": 10.0,
        "feed_latency_offset_ms": 10.0,
        "post_first_feed_wait": "spot",
        "post_first_feed_timeout_ms": 5000.0,
        "post_first_feed_poll_ms": 10.0,
        "record_market_every_steps": 60,
        "rebuild_hbt_results": False,
    }
    defaults.update(overrides)
    for name, value in defaults.items():
        if not hasattr(args, name):
            raise TypeError(f"Unknown backtest argument: {name}")
        setattr(args, name, value)
    return prepare_args(args)


def prepare_args(args: Namespace) -> Namespace:
    """Resolve project-relative paths and create the configured output folder."""
    args.base_config = resolve_project_path(Path(args.base_config))
    args.calendar = resolve_project_path(Path(args.calendar))
    args.stockinfo = resolve_project_path(Path(args.stockinfo))
    args.output_dir = resolve_output_dir(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    return args
