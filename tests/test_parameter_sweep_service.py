from __future__ import annotations

from trading_platform.config.models import ParameterSweepConfig
from trading_platform.services.parameter_sweep_service import (
    build_sweep_workflow_configs,
    run_parameter_sweep,
)


def test_build_sweep_workflow_configs_for_sma_cross_filters_invalid_pairs() -> None:
    config = ParameterSweepConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast_values=[10, 20, 50],
        slow_values=[20, 50],
    )

    configs = build_sweep_workflow_configs(config)

    pairs = {(c.fast, c.slow) for c in configs}
    assert (10, 20) in pairs
    assert (10, 50) in pairs
    assert (20, 50) in pairs
    assert (20, 20) not in pairs
    assert (50, 20) not in pairs
    assert (50, 50) not in pairs


def test_run_parameter_sweep_builds_ranked_results(monkeypatch) -> None:
    def fake_run_research_workflow(*, config, provider=None):
        score = float(config.fast + config.slow)
        return {
            "normalized_path": f"/tmp/{config.symbol}_norm.parquet",
            "features_path": f"/tmp/{config.symbol}_feat.parquet",
            "experiment_id": f"exp-{config.fast}-{config.slow}",
            "stats": {
                "Return [%]": score,
                "Sharpe Ratio": score / 100.0,
                "Max. Drawdown [%]": -score / 10.0,
            },
        }

    monkeypatch.setattr(
        "trading_platform.services.parameter_sweep_service.run_research_workflow",
        fake_run_research_workflow,
    )

    config = ParameterSweepConfig(
        symbol="AAPL",
        strategy="sma_cross",
        fast_values=[10, 20],
        slow_values=[50, 100],
        rank_metric="Return [%]",
    )

    out = run_parameter_sweep(config)

    leaderboard = out["leaderboard"]
    assert not leaderboard.empty
    assert leaderboard.iloc[0]["return_pct"] >= leaderboard.iloc[1]["return_pct"]
    assert len(out["errors"]) == 0