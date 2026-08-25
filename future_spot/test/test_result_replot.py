from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import pytest

from future_spot.arbitrage.result_replot import (
    PlotInterval,
    build_performance_figure,
    filter_summary,
    load_precomputed_summary,
    save_interval_figures,
)


def _summary() -> pd.DataFrame:
    rows = []
    for date, values in (("2026-04-01", (100.0, -20.0)), ("2026-04-02", (-9999.0, 0.0)), ("2026-04-03", (50.0, 25.0))):
        for index, pnl in enumerate(values):
            rows.append(
                {
                    "trade_date": pd.Timestamp(date),
                    "run_key": f"{date}-{index}",
                    "pair_name": f"pair-{index}",
                    "spot_symbol": str(1000 + index),
                    "realized_pnl": pnl,
                    "filled_pairs": index + 1,
                    "second_leg_failures": index,
                }
            )
    return pd.DataFrame(rows)


def test_filter_summary_excludes_configured_date() -> None:
    filtered, excluded = filter_summary(
        _summary(), start_date="2026-04-01", end_date="2026-04-03", excluded_dates=["2026-04-02"]
    )
    assert filtered["trade_date"].dt.strftime("%Y-%m-%d").unique().tolist() == ["2026-04-01", "2026-04-03"]
    assert excluded.loc[0, "realized_pnl"] == -9999.0


def test_load_precomputed_summary_reads_csv_only(tmp_path: Path) -> None:
    output_dir = tmp_path / "computed"
    output_dir.mkdir()
    frame = _summary()
    frame.to_csv(output_dir / "summary_all_daily_pairs.csv", index=False)
    loaded = load_precomputed_summary([output_dir])
    assert len(loaded) == len(frame)
    assert loaded["trade_date"].dtype.kind == "M"


def test_save_interval_figures_writes_png_and_audit(tmp_path: Path) -> None:
    interval = PlotInterval("April", "2026-04-01", "2026-04-03", ("2026-04-02",))
    figure = build_performance_figure(_summary(), interval)
    assert len(figure.axes) == 4
    plt.close(figure)

    audit = save_interval_figures(_summary(), [interval], tmp_path)
    assert Path(audit.loc[0, "figure_path"]).exists()
    assert (tmp_path / "filtered_plot_audit.csv").exists()
    assert audit.loc[0, "included_dates"] == 2


def test_save_interval_figures_allows_excluded_date_not_in_data(tmp_path: Path) -> None:
    interval = PlotInterval("April", "2026-04-01", "2026-04-03", ("2026-04-30",))
    audit = save_interval_figures(_summary(), [interval], tmp_path)

    assert Path(audit.loc[0, "figure_path"]).exists()
    assert audit.loc[0, "excluded_dates_found"] == ""
    assert audit.loc[0, "included_dates"] == 3


def test_filter_summary_rejects_reversed_interval() -> None:
    with pytest.raises(ValueError, match="start_date"):
        filter_summary(_summary(), start_date="2026-04-03", end_date="2026-04-01")
