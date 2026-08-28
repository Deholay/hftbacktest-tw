#!/usr/bin/env python3
"""Benchmark one month of futures/spot event conversion and HBT execution.

The HBT phase uses the normal full-market runner, but avoids report generation
and records its dynamic (position-carry-aware) universe for the conversion
phase.  The conversion phase writes each test NPZ to an isolated directory and
unlinks it after measuring its size, so the shared event cache is untouched.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import gc
import json
import logging
import os
import platform
import resource
import socket
import sys
import time
from pathlib import Path
from typing import Any, Callable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
for path in (PROJECT_ROOT, WORKSPACE_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from arbitrage import full_market_runner as runner  # noqa: E402
from scripts.tw_stock_data_to_npz import (  # noqa: E402
    convert_tw_stock_future_to_npz,
    convert_tw_stock_to_npz,
)


BASELINE_ARGS = (
    "--future-order-latency-ms", "1",
    "--future-response-latency-ms", "1",
    "--future-feed-latency-offset-ms", "0",
    "--spot-order-latency-ms", "1",
    "--spot-response-latency-ms", "35",
    "--spot-feed-latency-offset-ms", "0",
    "--post-first-feed-wait", "spot",
    "--post-first-feed-timeout-ms", "5000",
    "--workers", "6",
    "--strategy-engine", "numba",
    "--record-market-every-steps", "60",
    "--continue-on-error",
)


def parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("hbt", "convert"))
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--benchmark-dir", type=Path, required=True)
    parser.add_argument("--npz-compression", choices=("compressed", "uncompressed"), default="compressed")
    return parser.parse_args()


def host_info() -> dict[str, Any]:
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": sys.version,
        "cpu_count": os.cpu_count(),
    }


def cpu_seconds() -> float:
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime + own.ru_stime + children.ru_utime + children.ru_stime


def peak_rss_bytes() -> int:
    own = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    children = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return int(max(own, children) * 1024)


def directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def partition_rows(output_dir: Path, table: str) -> int:
    rows = 0
    for path in (output_dir / "core" / "dates").glob("trade_date=*/manifest.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows += int(payload.get("tables", {}).get(table, {}).get("rows", 0))
    return rows


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def runner_args(cli: argparse.Namespace) -> argparse.Namespace:
    argv = [
        "--start-date", cli.start_date,
        "--end-date", cli.end_date,
        "--output-dir", str(cli.benchmark_dir / "runner_state"),
        *BASELINE_ARGS,
    ]
    args = runner.parse_args(argv)
    args.base_config = runner.resolve_project_path(args.base_config)
    args.calendar = runner.resolve_project_path(args.calendar)
    args.stockinfo = runner.resolve_project_path(args.stockinfo)
    args.output_dir = runner.resolve_output_dir(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    return args


def timed_wrapper(
    original: Callable[..., Any],
    totals: dict[str, float],
    name: str,
) -> Callable[..., Any]:
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            totals[name] = totals.get(name, 0.0) + time.perf_counter() - started

    return wrapped


def run_hbt(cli: argparse.Namespace) -> None:
    args = runner_args(cli)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config_started = time.perf_counter()
    trade_dates = runner.select_trade_dates(
        args.calendar,
        args.start_date,
        args.end_date,
        excluded_dates=args.excluded_dates,
    )
    base_records, build_status = runner.build_daily_pair_records(args, trade_dates)
    config_seconds = time.perf_counter() - config_started
    build_status.to_csv(cli.benchmark_dir / "daily_config_build_status.csv", index=False)

    timings: dict[str, float] = {}
    originals = {
        "build_event_data": runner.build_event_data,
        "hbt_settings_frame": runner.hbt_settings_frame,
        "run_backtests": runner.run_backtests,
    }
    runner.build_event_data = timed_wrapper(originals["build_event_data"], timings, "event_lookup_seconds")
    runner.hbt_settings_frame = timed_wrapper(originals["hbt_settings_frame"], timings, "npz_audit_seconds")
    runner.run_backtests = timed_wrapper(originals["run_backtests"], timings, "pair_backtest_seconds")

    result_bytes_before = directory_bytes(args.output_dir)
    wall_started = time.perf_counter()
    cpu_started = cpu_seconds()
    try:
        outputs = runner.execute_hbt_runs(args, base_records, trade_dates)
    finally:
        runner.build_event_data = originals["build_event_data"]
        runner.hbt_settings_frame = originals["hbt_settings_frame"]
        runner.run_backtests = originals["run_backtests"]
    wall_seconds = time.perf_counter() - wall_started
    cpu_used = cpu_seconds() - cpu_started
    result_bytes_after = directory_bytes(args.output_dir)

    records = runner.pair_universe_frame(outputs.records)
    records.to_csv(cli.benchmark_dir / "records.csv", index=False)
    outputs.summary.to_csv(cli.benchmark_dir / "summary.csv", index=False)
    outputs.run_errors.to_csv(cli.benchmark_dir / "run_errors.csv", index=False)

    unique_assets = outputs.settings.drop_duplicates("data") if not outputs.settings.empty else outputs.settings
    event_file_bytes = int(sum(Path(value).stat().st_size for value in unique_assets.get("data", [])))
    result = {
        "phase": "hbt",
        "start_date": cli.start_date,
        "end_date": cli.end_date,
        "trade_dates": trade_dates,
        "trade_date_count": len(trade_dates),
        "base_pair_count": len(base_records),
        "executed_pair_count": len(outputs.records),
        "completed_pair_count": len(outputs.summary),
        "run_error_count": len(outputs.run_errors),
        "unique_event_asset_count": int(unique_assets["data"].nunique()) if not unique_assets.empty else 0,
        "event_rows": int(unique_assets["rows"].sum()) if not unique_assets.empty else 0,
        "depth_events": int(unique_assets["depth_events"].sum()) if not unique_assets.empty else 0,
        "trade_events": int(unique_assets["trade_events"].sum()) if not unique_assets.empty else 0,
        "event_file_bytes": event_file_bytes,
        "summary_rows": len(outputs.summary),
        "trade_rows": partition_rows(args.output_dir, "trades") if outputs.daily_partitions else len(outputs.trades),
        "market_rows": partition_rows(args.output_dir, "market") if outputs.daily_partitions else len(outputs.market),
        "latency_rows": partition_rows(args.output_dir, "latency") if outputs.daily_partitions else len(outputs.latency),
        "config_build_seconds": config_seconds,
        "hbt_total_wall_seconds": wall_seconds,
        "hbt_total_cpu_seconds": cpu_used,
        "peak_rss_bytes": peak_rss_bytes(),
        "result_disk_bytes": result_bytes_after,
        "result_disk_growth_bytes": result_bytes_after - result_bytes_before,
        "cache_state": (
            "warm_legacy"
            if outputs.cache_hit
            else "warm_daily"
            if outputs.daily_dates_reused and not outputs.daily_dates_executed
            else "partial_daily_resume"
            if outputs.daily_dates_reused
            else "cold"
        ),
        "daily_dates_reused": outputs.daily_dates_reused,
        "daily_dates_executed": outputs.daily_dates_executed,
        "engine": "reference",
        "engine_version": runner.REFERENCE_ENGINE_VERSION,
        "time_in_force_semantics": runner.HBT_TIME_IN_FORCE_SEMANTICS,
        "compact_schema_version": None,
        "daily_result_schema_version": runner.DAILY_RESULT_SCHEMA_VERSION,
        "worker_pool_creations": int(
            not outputs.cache_hit and args.carry_positions and args.workers > 1
        ),
        "worker_process_count": len(getattr(args, "hbt_worker_pids", set())),
        "last_date_shard_weights": list(getattr(args, "hbt_shard_weights", [])),
        **timings,
        "settings": {
            "workers": args.workers,
            "strategy_engine": args.strategy_engine,
            "session_start": args.session_start,
            "session_end": args.session_end,
            "npz_compression": cli.npz_compression,
            "excluded_dates": args.excluded_dates,
            "excluded_run_keys": args.excluded_run_keys,
            "future_latency_ms": runner.leg_latency_ms(args, "future"),
            "spot_latency_ms": runner.leg_latency_ms(args, "spot"),
            "post_first_feed_wait": args.post_first_feed_wait,
            "report_mode": args.report_mode,
            "strategy_clock_step_ms": args.step_ms,
        },
        "host": host_info(),
    }
    write_json(cli.benchmark_dir / "hbt_result.json", result)
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))


def conversion_tasks(records: pd.DataFrame) -> list[tuple[str, str, str]]:
    seen: set[tuple[str, str, str]] = set()
    tasks: list[tuple[str, str, str]] = []
    for row in records.itertuples(index=False):
        for leg, symbol in (("stock", str(row.spot_symbol)), ("stock_future", str(row.future_symbol))):
            key = (str(row.trade_date), leg, symbol)
            if key not in seen:
                seen.add(key)
                tasks.append(key)
    return tasks


def run_conversion(cli: argparse.Namespace) -> None:
    records_path = cli.benchmark_dir / "records.csv"
    if not records_path.exists():
        raise FileNotFoundError(f"run the hbt phase first; missing {records_path}")
    records = pd.read_csv(records_path, dtype={"trade_date": str, "spot_symbol": str, "future_symbol": str})
    tasks = conversion_tasks(records)
    temp_dir = cli.benchmark_dir / "temporary_npz"
    temp_dir.mkdir(parents=True, exist_ok=True)
    log_path = cli.benchmark_dir / "conversion_details.log"
    detail_path = cli.benchmark_dir / "conversion_timings.csv"

    wall_started = time.perf_counter()
    cpu_started = cpu_seconds()
    output_bytes = 0
    event_rows = 0
    stock_seconds = 0.0
    future_seconds = 0.0
    successes = 0
    with detail_path.open("w", newline="", encoding="utf-8") as detail_file, log_path.open(
        "w", encoding="utf-8"
    ) as log_file:
        writer = csv.DictWriter(
            detail_file,
            fieldnames=("index", "trade_date", "source_kind", "symbol", "seconds", "events", "output_bytes"),
        )
        writer.writeheader()
        for index, (trade_date, source_kind, symbol) in enumerate(tasks, start=1):
            output = temp_dir / f"{source_kind}_{symbol}_{trade_date.replace('-', '')}.npz"
            started = time.perf_counter()
            with contextlib.redirect_stdout(log_file):
                if source_kind == "stock":
                    _, data = convert_tw_stock_to_npz(
                        symbol=symbol,
                        start_date=trade_date,
                        end_date=trade_date,
                        start_time="09:00:00",
                        end_time="13:25:00",
                        output=output,
                        workspace_root=WORKSPACE_ROOT,
                        data_api=True,
                        daily_parquet=False,
                        data_platform_base=runner.DEFAULT_DATA_PLATFORM_BASE,
                        levels=5,
                        qa_sample_rows=1000,
                        npz_compression=cli.npz_compression,
                    )
                else:
                    _, data = convert_tw_stock_future_to_npz(
                        symbol=symbol,
                        start_date=trade_date,
                        end_date=trade_date,
                        start_time="09:00:00",
                        end_time="13:25:00",
                        output=output,
                        workspace_root=WORKSPACE_ROOT,
                        path_config=WORKSPACE_ROOT / "path.toml",
                        daily_parquet_dir=Path("/mnt/z/ticks_parquet_stock_future"),
                        levels=5,
                        qa_sample_rows=1000,
                        npz_compression=cli.npz_compression,
                    )
            seconds = time.perf_counter() - started
            size = output.stat().st_size
            rows = len(data)
            output_bytes += size
            event_rows += rows
            successes += 1
            if source_kind == "stock":
                stock_seconds += seconds
            else:
                future_seconds += seconds
            writer.writerow(
                {
                    "index": index,
                    "trade_date": trade_date,
                    "source_kind": source_kind,
                    "symbol": symbol,
                    "seconds": f"{seconds:.9f}",
                    "events": rows,
                    "output_bytes": size,
                }
            )
            detail_file.flush()
            log_file.flush()
            output.unlink()
            del data
            if index % 25 == 0 or index == len(tasks):
                elapsed = time.perf_counter() - wall_started
                logging.info("conversion progress=%s/%s elapsed=%.1fs", index, len(tasks), elapsed)
            if index % 100 == 0:
                gc.collect()

    wall_seconds = time.perf_counter() - wall_started
    cpu_used = cpu_seconds() - cpu_started
    result = {
        "phase": "convert",
        "start_date": cli.start_date,
        "end_date": cli.end_date,
        "asset_count": len(tasks),
        "success_count": successes,
        "stock_asset_count": sum(source == "stock" for _, source, _ in tasks),
        "future_asset_count": sum(source == "stock_future" for _, source, _ in tasks),
        "event_rows": event_rows,
        "output_bytes_if_retained": output_bytes,
        "conversion_total_wall_seconds": wall_seconds,
        "conversion_total_cpu_seconds": cpu_used,
        "peak_rss_bytes": peak_rss_bytes(),
        "stock_conversion_seconds": stock_seconds,
        "future_conversion_seconds": future_seconds,
        "npz_compression": cli.npz_compression,
        "temporary_npz_retained": 0,
        "host": host_info(),
    }
    write_json(cli.benchmark_dir / "conversion_result.json", result)
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))


def main() -> None:
    cli = parse_cli()
    cli.benchmark_dir = cli.benchmark_dir.resolve()
    cli.benchmark_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if cli.phase == "hbt":
        run_hbt(cli)
    else:
        run_conversion(cli)


if __name__ == "__main__":
    main()
