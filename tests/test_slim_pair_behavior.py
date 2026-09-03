from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from future_spot.arbitrage.hbt_backtest import HbtPairBacktester
from future_spot.arbitrage.hbt_types import HbtPairBacktestConfig
from future_spot.arbitrage.models import PairConfig
from hftbacktest_slim import BBO_SCHEMA
from scripts.hbt_types import HbtAssetConfig


def _write(path: Path, rows: list[tuple]) -> Path:
    table = pa.Table.from_pylist(
        [dict(zip(BBO_SCHEMA.names, row)) for row in rows], schema=BBO_SCHEMA
    ).replace_schema_metadata(
        {b"schema_version": b"bbo_v1", b"local_timestamp_adjustment_ns": b"0"}
    )
    with path.open("wb") as sink, ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return path


def _pair(**updates) -> PairConfig:
    values = dict(
        name="S_F",
        spot_symbol="S",
        future_symbol="F",
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
    values.update(updates)
    return PairConfig(**values)


def _run(
    tmp_path: Path,
    spot_rows: list[tuple],
    future_rows: list[tuple],
    *,
    pair: PairConfig | None = None,
    spot_entry_latency: int = 0,
    future_entry_latency: int = 0,
    spot_response_latency: int = 0,
    future_response_latency: int = 0,
    **updates,
):
    spot = _write(tmp_path / "spot.arrow", spot_rows)
    future = _write(tmp_path / "future.arrow", future_rows)
    config_values = dict(
        pair=pair or _pair(),
        spot=HbtAssetConfig(
            "S",
            spot,
            "stock",
            1000.0,
            tick_size=1.0,
            order_entry_latency_ns=spot_entry_latency,
            order_response_latency_ns=spot_response_latency,
        ),
        future=HbtAssetConfig(
            "F",
            future,
            "future",
            1000.0,
            tick_size=1.0,
            order_entry_latency_ns=future_entry_latency,
            order_response_latency_ns=future_response_latency,
        ),
        execution_engine="slim",
        strategy_engine="python",
        first_leg="future",
        step_ns=10,
        response_timeout_ns=50,
        max_steps=5,
        max_trades=1,
    )
    config_values.update(updates)
    config = HbtPairBacktestConfig(**config_values)
    backtester = HbtPairBacktester(config)
    trades, summary = backtester.run()
    return backtester, trades, summary


INITIAL_SPOT = [(0, 100, 110, 99.0, 100.0, 10.0, 10.0, 100.0, 1)]
INITIAL_FUTURE = [(0, 100, 110, 110.0, 111.0, 10.0, 10.0, 110.0, 1)]


def test_crossing_legs_preserve_sequence_position_and_audit_rows(tmp_path: Path) -> None:
    backtester, trades, summary = _run(tmp_path, INITIAL_SPOT, INITIAL_FUTURE)

    row = trades.iloc[0]
    assert row["status"] == "FILLED"
    assert row["first_leg"] == "future"
    assert row["first_side"] == "sell"
    assert row["second_leg"] == "stock"
    assert row["second_side"] == "buy"
    assert row["signal_timestamp"] == 110
    assert row["completion_timestamp"] == 110
    assert summary.loc[0, "final_quantity"] == 1
    assert summary.loc[0, "final_stock_units"] == 1
    assert summary.loc[0, "final_future_units"] == 1
    assert backtester.latency_frame()["event_type"].tolist() == [
        "signal_market",
        "first_order_entry",
        "post_first_market",
        "second_order_entry",
        "completion",
    ]
    assert not backtester.market_frame().empty


def test_first_leg_non_fill_after_feed_moves_during_entry_latency(tmp_path: Path) -> None:
    future_rows = [
        *INITIAL_FUTURE,
        (1, 102, 112, 109.0, 110.0, 10.0, 10.0, 109.0, 2),
    ]
    backtester, trades, summary = _run(
        tmp_path, INITIAL_SPOT, future_rows, future_entry_latency=5
    )

    assert trades.loc[0, "status"] == "FIRST_LEG_UNFILLED"
    assert not bool(trades.loc[0, "first_filled"])
    assert trades.loc[0, "first_order_req_local_ts"] == 110
    assert trades.loc[0, "first_order_exch_ts"] == 115
    assert summary.loc[0, "final_quantity"] == 0


def test_feed_moves_during_first_order_response_latency(tmp_path: Path) -> None:
    future_rows = [
        *INITIAL_FUTURE,
        (1, 102, 112, 109.0, 110.0, 10.0, 10.0, 109.0, 2),
    ]
    backtester, trades, summary = _run(
        tmp_path,
        INITIAL_SPOT,
        future_rows,
        future_response_latency=5,
    )

    row = trades.iloc[0]
    assert row["status"] == "FIRST_LEG_UNFILLED"
    assert not row["first_filled"]
    assert row["first_order_req_local_ts"] == 110
    assert row["first_order_exch_ts"] == 110
    assert row["first_order_resp_local_ts"] == 115
    first_response = backtester.latency_frame().query("event_type == 'first_order_entry'").iloc[0]
    assert first_response["future_feed_local_ts"] == 112
    assert summary.loc[0, "final_quantity"] == 0


def test_second_leg_non_fill_flattens_first_leg(tmp_path: Path) -> None:
    spot_rows = [
        *INITIAL_SPOT,
        (1, 102, 112, 100.0, 101.0, 10.0, 10.0, 101.0, 2),
    ]
    _, trades, summary = _run(
        tmp_path, spot_rows, INITIAL_FUTURE, spot_entry_latency=5
    )

    row = trades.iloc[0]
    assert row["status"] == "SECOND_LEG_UNFILLED"
    assert bool(row["first_filled"])
    assert not bool(row["second_filled"])
    assert bool(row["flatten_filled"])
    assert summary.loc[0, "flatten_count"] == 1
    assert summary.loc[0, "final_quantity"] == 0


def test_post_first_feed_refresh_rechecks_profit_and_flattens(tmp_path: Path) -> None:
    spot_rows = [
        *INITIAL_SPOT,
        (1, 110, 120, 109.0, 110.0, 10.0, 10.0, 110.0, 2),
    ]
    future_rows = [
        *INITIAL_FUTURE,
        (1, 120, 130, 110.0, 111.0, 10.0, 10.0, 110.0, 2),
    ]
    backtester, trades, summary = _run(
        tmp_path,
        spot_rows,
        future_rows,
        post_first_feed_wait="spot",
        post_first_feed_timeout_ns=20,
        post_first_feed_poll_ns=1,
    )

    row = trades.iloc[0]
    assert row["status"] == "SECOND_LEG_PROFIT_CHECK_FAILED"
    # The existing output contract records this branch's flatten in the
    # second-fill columns; the latency audit independently proves submission.
    assert bool(row["second_filled"])
    assert "flatten_first_order" in backtester.latency_frame()["event_type"].tolist()
    assert summary.loc[0, "final_quantity"] == 0


def test_post_first_feed_timeout_is_audited(tmp_path: Path) -> None:
    _, trades, _ = _run(
        tmp_path,
        INITIAL_SPOT,
        INITIAL_FUTURE,
        post_first_feed_wait="spot",
        post_first_feed_timeout_ns=10,
        post_first_feed_poll_ns=1,
    )

    assert trades.loc[0, "status"] == "POST_FIRST_FEED_TIMEOUT"
    assert "post-first feed refresh timeout" in trades.loc[0, "failure_reason"]


@pytest.mark.parametrize(
    ("mode", "expected"),
    [("spot", True), ("future", False), ("any", True), ("both", False)],
)
def test_post_first_feed_wait_modes(mode: str, expected: bool) -> None:
    class FeedState:
        def feed_latency(self, asset_no: int):
            return (100, 120) if asset_no == 0 else (100, 110)

    assert HbtPairBacktester._post_first_feed_ready(
        mode, (100, 110), (100, 110), FeedState()
    ) is expected


def test_strategy_clock_is_step_based_and_honors_max_steps(tmp_path: Path) -> None:
    rows_spot = [
        (index, 100 + index, 100 + index, 99.0, 100.0, 10.0, 10.0, 100.0, index)
        for index in range(31)
    ]
    rows_future = [
        (index, 100 + index, 100 + index, 99.0, 100.0, 10.0, 10.0, 100.0, index)
        for index in range(31)
    ]
    backtester, trades, summary = _run(
        tmp_path,
        rows_spot,
        rows_future,
        pair=_pair(entry_threshold_pct=1.0),
        max_steps=2,
        max_trades=None,
    )

    assert trades.empty
    assert backtester.python_decisions == 2
    assert summary.loc[0, "python_decisions"] == 2
    assert len(backtester.market_frame()) == 1  # final market only
