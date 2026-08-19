"""One-command full futures/spot HBT run with CSV and PNG outputs."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

TEST_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = TEST_ROOT.parent
WORKSPACE_ROOT = PROJECT_ROOT.parent
for path in (TEST_ROOT, PROJECT_ROOT, WORKSPACE_ROOT, PROJECT_ROOT / "scripts"):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from arbitrage.full_market_runner import parse_args  # noqa: E402
try:  # Package import (future_spot.test) and direct script execution.
    from .backtest_config import prepare_args  # noqa: E402
    from .backtest_pipeline import run_backtest_pipeline  # noqa: E402
    from .report_plots import save_report_plots  # noqa: E402
    from .report_tables import build_report_tables  # noqa: E402
except ImportError:  # pragma: no cover - exercised by direct script execution
    from backtest_config import prepare_args  # noqa: E402
    from backtest_pipeline import run_backtest_pipeline  # noqa: E402
    from report_plots import save_report_plots  # noqa: E402
    from report_tables import build_report_tables  # noqa: E402


def main() -> int:
    args = prepare_args(parse_args())
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    artifacts = run_backtest_pipeline(args)
    reports = build_report_tables(artifacts)
    figures = {} if args.no_plots else save_report_plots(artifacts, reports)
    logging.info(
        "done dates=%s pairs=%s errors=%s report_csv=%s figures=%s",
        len(artifacts.trade_dates),
        len(artifacts.records),
        len(artifacts.frame("run_errors")),
        reports.output_dir,
        artifacts.output_dir / "figures",
    )
    for path in figures.values():
        logging.info("saved figure %s", path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
