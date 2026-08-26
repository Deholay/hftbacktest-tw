from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

import pandas as pd

from future_spot.test.backtest_pipeline import BacktestArtifacts
from future_spot.test.report_tables import build_report_tables


def _filled_trade(timestamp: int, signal: str) -> dict[str, object]:
    is_entry = signal == "ENTER_LONG_SPOT_SHORT_FUTURE"
    return {
        "trade_date": "2026-01-02",
        "run_key": "2026-01-02::2330_CFA6",
        "pair_name": "2330_CFA6",
        "spot_symbol": "2330",
        "future_symbol": "CFA6",
        "timestamp": timestamp,
        "completion_timestamp": timestamp + 10,
        "step": timestamp,
        "signal": signal,
        "status": "FILLED",
        "failure_reason": "",
        "first_leg": "future",
        "first_side": "sell" if is_entry else "buy",
        "first_exec_price": 102.0 if is_entry else 101.0,
        "first_exec_qty": 1,
        "second_leg": "stock",
        "second_side": "buy" if is_entry else "sell",
        "second_exec_price": 100.0 if is_entry else 101.0,
        "second_exec_qty": 2_000,
        "realized_pnl": None if is_entry else 1_000.0,
        "position_quantity": 1 if is_entry else 0,
    }


class StreamingReportTablesTest(unittest.TestCase):
    def test_summary_report_streams_large_inputs_and_releases_frames(self) -> None:
        run_key = "2026-01-02::2330_CFA6"
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            trades = pd.DataFrame(
                [
                    _filled_trade(1, "ENTER_LONG_SPOT_SHORT_FUTURE"),
                    {
                        **_filled_trade(2, "ENTER_LONG_SPOT_SHORT_FUTURE"),
                        "status": "SECOND_LEG_UNFILLED",
                        "failure_reason": "test failure",
                    },
                    _filled_trade(3, "EXIT"),
                ]
            )
            market = pd.DataFrame(
                [
                    {"run_key": run_key, "timestamp": 1, "spot_bid": 99.0, "spot_ask": 100.0, "future_bid": 102.0, "future_ask": 103.0},
                    {"run_key": run_key, "timestamp": 4, "spot_bid": 101.0, "spot_ask": 102.0, "future_bid": 100.0, "future_ask": 101.0},
                ]
            )
            latency = pd.DataFrame(
                [
                    {"pair_name": "2330_CFA6", "event_type": "signal", "leg": "future", "side": "sell", "spot_feed_latency_ns": 1_000_000, "future_feed_latency_ns": 2_000_000, "order_entry_latency_ns": 1_000_000, "order_response_latency_ns": 1_000_000, "local_ts": 1, "spot_exch_ts": 1, "future_exch_ts": 1},
                    {"pair_name": "2330_CFA6", "event_type": "signal", "leg": "stock", "side": "buy", "spot_feed_latency_ns": 3_000_000, "future_feed_latency_ns": 4_000_000, "order_entry_latency_ns": 1_000_000, "order_response_latency_ns": 35_000_000, "local_ts": 2, "spot_exch_ts": 2, "future_exch_ts": 2},
                ]
            )
            trades.to_csv(output_dir / "trades_all_daily_pairs.csv", index=False)
            market.to_csv(output_dir / "market_all_daily_pairs.csv", index=False)
            latency.to_csv(output_dir / "latency_all_daily_pairs.csv", index=False)

            summary = pd.DataFrame(
                [{
                    "trade_date": "2026-01-02",
                    "run_key": run_key,
                    "pair_name": "2330_CFA6",
                    "spot_symbol": "2330",
                    "future_symbol": "CFA6",
                    "realized_pnl": 1_000.0,
                    "filled_pairs": 2,
                    "second_leg_failures": 1,
                    "flatten_count": 0,
                    "final_quantity": 0,
                }]
            )
            pair_universe = pd.DataFrame([{"run_key": run_key}])
            pair = SimpleNamespace(
                spot_order_qty=2_000,
                future_order_qty=1,
                future_pnl_multiplier=2_000,
                stock_commission_rate=0.001425,
                stock_commission_discount=0.28,
                stock_transaction_tax_rate=0.003,
            )
            args = Namespace(
                output_dir=output_dir,
                low_memory_reports=True,
                report_mode="summary",
                report_chunk_rows=1,
                skip_detailed_reports=False,
                detailed_report_format="parquet",
                total_capital=50_000_000.0,
                futures_margin_rate=0.20,
                spot_equity_rate=0.40,
                leverage=True,
                carry_positions=True,
            )
            artifacts = BacktestArtifacts(
                args=args,
                trade_dates=["2026-01-02"],
                records=[SimpleNamespace(run_key=run_key, pair=pair)],
                event_paths={},
                pair_results={run_key: {"market": market}},
                frames={
                    "summary": summary,
                    "pair_universe": pair_universe,
                    "trades": trades.copy(),
                    "market": market.copy(),
                    "latency": latency.copy(),
                    "entry_exit_all": trades.copy(),
                },
            )

            reports = build_report_tables(artifacts)

            self.assertTrue(artifacts.frame("trades").empty)
            self.assertTrue(artifacts.frame("market").empty)
            self.assertTrue(artifacts.frame("latency").empty)
            self.assertTrue(artifacts.frame("entry_exit_all").empty)
            self.assertFalse(artifacts.pair_results)
            self.assertTrue(bool(reports.frame("report_build_info").iloc[0]["low_memory"]))
            self.assertEqual(reports.frame("failure_overview").iloc[0]["failure_trade_rows"], 1)
            self.assertNotIn("filled_trades", reports.frames)
            self.assertNotIn("capital_constraint_events", reports.frames)
            latency_row = reports.frame("latency_summary").iloc[0]
            self.assertAlmostEqual(latency_row["spot_feed_latency_ms"], 2.0)
            self.assertAlmostEqual(latency_row["order_response_latency_ms"], 18.0)
            self.assertEqual(len(reports.frame("latency_plot_sample")), 2)


if __name__ == "__main__":
    unittest.main()
