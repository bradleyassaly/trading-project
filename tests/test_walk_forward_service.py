from __future__ import annotations

import pandas as pd

from trading_platform.config.models import WalkForwardConfig
from trading_platform.services.walk_forward_service import (
    build_walk_forward_windows,
    run_walk_forward_evaluation,
)


def test_build_walk_forward_windows_returns_expected_windows() -> None:
    timestamps = list(pd.date_range("2024-01-01", periods=20, freq="D"))

    windows = build_walk_forward_windows(
        timestamps=timestamps,
        train_window_bars=10,
        test_window_bars=3,
        step_bars=3,
    )

    assert len(windows) > 0
    assert windows[0]["train_start"] == pd.Timestamp("2024-01-01")
    assert windows[0]["train_end"] == pd.Timestamp("2024-01-10")
    assert windows[0]["test_start"] == pd.Timestamp("2024-01-11")
    assert windows[0]["test_end"] == pd.Timestamp("2024-01-13")

def test_run_walk_forward_evaluation_returns_summary(monkeypatch, tmp_path) -> None:
    feature_path = tmp_path / "AAPL.parquet"
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=40, freq="D"),
            "symbol": ["AAPL"] * 40,
            "close": range(40),
        }
    )
    df.to_parquet(feature_path, index=False)

    call_count = {"n": 0}

    def fake_run_research_workflow(*, config, provider=None):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return {
                "features_path": feature_path,
                "experiment_id": "prep-exp",
                "stats": {
                    "Return [%]": 1.0,
                    "Sharpe Ratio": 0.5,
                    "Max. Drawdown [%]": -2.0,
                },
            }
        return {
            "features_path": feature_path,
            "experiment_id": f"wf-exp-{call_count['n']}",
            "stats": {
                "Return [%]": 10.0,
                "Sharpe Ratio": 1.0,
                "Max. Drawdown [%]": -5.0,
            },
        }

    monkeypatch.setattr(
        "trading_platform.services.walk_forward_service.run_research_workflow",
        fake_run_research_workflow,
    )

    config = WalkForwardConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast=10,
        slow=20,
        train_window_bars=20,
        test_window_bars=5,
        step_bars=5,
        min_required_bars=20,
    )

    out = run_walk_forward_evaluation(config)

    assert out["prep_experiment_id"] == "prep-exp"
    assert not out["results_df"].empty
    assert out["summary"]["window_count"] > 0