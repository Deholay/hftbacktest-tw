from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc

from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
from future_spot.arbitrage.hbt_types import HbtPairBacktestConfig
from future_spot.arbitrage.models import PairConfig
from scripts.compact_cache import BBO_SCHEMA
from scripts.compact_hbt_adapter import compact_to_reference_events
from scripts.hbt_types import HbtAssetConfig
from scripts.tw_stock_hftbacktest import import_hftbacktest


def _compact(path: Path, rows: list[tuple]) -> pa.Table:
    table = pa.Table.from_pylist(
        [dict(zip(BBO_SCHEMA.names, row)) for row in rows], schema=BBO_SCHEMA
    ).replace_schema_metadata({b"schema_version": b"bbo_v1", b"local_timestamp_adjustment_ns": b"0"})
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return table


def _pair() -> PairConfig:
    return PairConfig(
        name="A_AF",
        spot_symbol="A",
        future_symbol="AF",
        spot_shares_per_pair=1000,
        future_shares_per_pair=1000,
        spot_order_qty=1000,
        future_order_qty=1,
        future_pnl_multiplier=1000,
        entry_threshold_pct=0.01,
        exit_threshold_pct=0.0,
        stop_loss_pct=-1.0,
        min_effective_tick_multiple=0.0,
        spot_tick_size=1.0,
        future_tick_size=1.0,
        stock_min_bid_size=1,
        stock_min_ask_size=1,
        first_leg_time_in_force="FOK",
        second_leg_time_in_force="FOK",
        flatten_first_leg_time_in_force="IOC",
    )


def test_reference_and_slim_pair_fill_golden_match(tmp_path: Path) -> None:
    spot_arrow = tmp_path / "A.arrow"
    future_arrow = tmp_path / "AF.arrow"
    rows_spot = [
        (0, 100, 110, 99.0, 100.0, 10.0, 10.0, 100.0, 1),
        (1, 200, 210, 99.0, 100.0, 10.0, 10.0, 100.0, 1),
    ]
    rows_future = [
        (0, 100, 110, 110.0, 111.0, 1.0, 1.0, 110.0, 1),
        (1, 200, 210, 110.0, 111.0, 1.0, 1.0, 110.0, 1),
    ]
    spot_table = _compact(spot_arrow, rows_spot)
    future_table = _compact(future_arrow, rows_future)
    spot_events, _ = compact_to_reference_events(spot_table, trade_date="2026-03-02")
    future_events, _ = compact_to_reference_events(future_table, trade_date="2026-03-02")
    spot_npz = tmp_path / "A.npz"
    future_npz = tmp_path / "AF.npz"
    np.savez(spot_npz, data=spot_events)
    np.savez(future_npz, data=future_events)

    pair = _pair()
    common = dict(
        pair=pair,
        first_leg="future",
        step_ns=10,
        response_timeout_ns=20,
        max_steps=2,
        max_trades=1,
        second_leg_profit_check=True,
        record_market_every_steps=None,
        strategy_engine="python",
    )
    reference = HbtPairBacktestConfig(
        **common,
        spot=HbtAssetConfig(
            "A",
            spot_npz,
            "stock",
            1000.0,
            tick_size=1.0,
            order_entry_latency_ns=3,
            order_response_latency_ns=4,
        ),
        future=HbtAssetConfig(
            "AF",
            future_npz,
            "future",
            1000.0,
            tick_size=1.0,
            order_entry_latency_ns=2,
            order_response_latency_ns=3,
        ),
        execution_engine="reference",
    )
    slim = replace(
        reference,
        spot=replace(reference.spot, data=spot_arrow),
        future=replace(reference.future, data=future_arrow),
        execution_engine="slim",
    )
    reference_trades, _ = HbtPairBacktester(
        reference, hbtpkg=import_hftbacktest(Path(__file__).resolve().parents[1])
    ).run()
    slim_trades, _ = HbtPairBacktester(slim).run()
    columns = [
        "signal",
        "status",
        "first_leg",
        "first_side",
        "first_status",
        "first_filled",
        "first_exec_price",
        "first_exec_qty",
        "second_leg",
        "second_side",
        "second_status",
        "second_filled",
        "second_exec_price",
        "second_exec_qty",
        "signal_timestamp",
        "completion_timestamp",
        "first_local_timestamp",
        "first_exch_timestamp",
        "first_order_req_local_ts",
        "first_order_exch_ts",
        "first_order_resp_local_ts",
        "second_local_timestamp",
        "second_exch_timestamp",
        "second_order_req_local_ts",
        "second_order_exch_ts",
        "second_order_resp_local_ts",
    ]
    assert not reference_trades.empty
    assert reference_trades[columns].to_dict("records") == slim_trades[columns].to_dict("records")
