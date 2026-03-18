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

    def fake_run_research_prep_pipeline(*, ingest_config, feature_config, provider=None):
        return {
            "normalized_path": tmp_path / "AAPL_normalized.parquet",
            "features_path": feature_path,
        }

    def fake_run_research_workflow(*, config, provider=None):
        return {
            "features_path": feature_path,
            "experiment_id": "wf-exp",
            "stats": {
                "Return [%]": 10.0,
                "Sharpe Ratio": 1.0,
                "Max. Drawdown [%]": -5.0,
            },
        }

    monkeypatch.setattr(
        "trading_platform.services.walk_forward_service.run_research_prep_pipeline",
        fake_run_research_prep_pipeline,
    )
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
        walk_forward_mode="fixed",
    )

    out = run_walk_forward_evaluation(config)

    assert out["prep_experiment_id"] is None
    assert not out["results_df"].empty
    assert out["summary"]["window_count"] > 0


def test_run_walk_forward_evaluation_optimize_mode_selects_best_params(
    monkeypatch,
    tmp_path,
) -> None:
    feature_path = tmp_path / "AAPL.parquet"
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=50, freq="D"),
            "symbol": ["AAPL"] * 50,
            "close": range(50),
        }
    )
    df.to_parquet(feature_path, index=False)

    captured_test_configs: list[object] = []

    def fake_run_research_prep_pipeline(*, ingest_config, feature_config, provider=None):
        return {
            "normalized_path": tmp_path / "AAPL_normalized.parquet",
            "features_path": feature_path,
        }

    def fake_run_research_workflow(*, config, provider=None):
        captured_test_configs.append(config)
        return {
            "features_path": feature_path,
            "experiment_id": "test-exp",
            "stats": {
                "Return [%]": 8.0,
                "Sharpe Ratio": 1.1,
                "Max. Drawdown [%]": -4.0,
            },
        }

    def fake_run_parameter_sweep(*, config, provider=None, continue_on_error=True):
        return {
            "config": {},
            "results": [],
            "errors": [],
            "leaderboard": pd.DataFrame(
                [
                    {
                        "fast": 20,
                        "slow": 50,
                        "lookback": None,
                        "return_pct": 12.0,
                        "sharpe_ratio": 1.2,
                        "max_drawdown_pct": -8.0,
                    },
                    {
                        "fast": 10,
                        "slow": 100,
                        "lookback": None,
                        "return_pct": 9.0,
                        "sharpe_ratio": 1.0,
                        "max_drawdown_pct": -6.0,
                    },
                ]
            ),
        }

    monkeypatch.setattr(
        "trading_platform.services.walk_forward_service.run_research_prep_pipeline",
        fake_run_research_prep_pipeline,
    )
    monkeypatch.setattr(
        "trading_platform.services.walk_forward_service.run_research_workflow",
        fake_run_research_workflow,
    )
    monkeypatch.setattr(
        "trading_platform.services.walk_forward_service.run_parameter_sweep",
        fake_run_parameter_sweep,
    )

    config = WalkForwardConfig(
        symbol="AAPL",
        strategy="sma_cross",
        walk_forward_mode="optimize",
        fast_values=[10, 20],
        slow_values=[50, 100],
        train_window_bars=20,
        test_window_bars=5,
        step_bars=5,
        min_required_bars=20,
    )

    out = run_walk_forward_evaluation(config)

    assert out["prep_experiment_id"] is None
    assert not out["results_df"].empty
    assert out["results_df"].iloc[0]["selected_fast"] == 20
    assert out["results_df"].iloc[0]["selected_slow"] == 50

    assert captured_test_configs
    assert captured_test_configs[0].fast == 20
    assert captured_test_configs[0].slow == 50