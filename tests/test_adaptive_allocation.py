from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.adaptive_allocation_build import cmd_adaptive_allocation_build
from trading_platform.cli.commands.adaptive_allocation_export_run_config import (
    cmd_adaptive_allocation_export_run_config,
)
from trading_platform.cli.commands.adaptive_allocation_show import cmd_adaptive_allocation_show
from trading_platform.config.loader import (
    load_multi_strategy_portfolio_config,
    load_pipeline_run_config,
)
from trading_platform.portfolio.adaptive_allocation import (
    AdaptiveAllocationPolicyConfig,
    build_adaptive_allocation,
    export_adaptive_allocation_run_config,
    load_adaptive_allocation,
)


def _write_strategy_portfolio(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "strategy_portfolio.json").write_text(
        json.dumps(
            {
                "summary": {"total_selected_strategies": 2, "total_active_weight": 1.0, "warning_count": 0},
                "selected_strategies": [
                    {
                        "preset_name": "generated_momentum_a",
                        "source_run_id": "run-a",
                        "signal_family": "momentum",
                        "universe": "nasdaq100",
                        "promotion_status": "active",
                        "allocation_weight": 0.6,
                        "target_capital_fraction": 0.6,
                        "generated_preset_path": str(root / "generated_momentum_a.json"),
                    },
                    {
                        "preset_name": "generated_value_b",
                        "source_run_id": "run-b",
                        "signal_family": "value",
                        "universe": "sp500",
                        "promotion_status": "active",
                        "allocation_weight": 0.4,
                        "target_capital_fraction": 0.4,
                        "generated_preset_path": str(root / "generated_value_b.json"),
                    },
                ],
                "warnings": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return root


def _write_strategy_monitoring(root: Path, *, generated_at: str = "2026-03-22T00:00:00+00:00") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "strategy_monitoring.json").write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "summary": {
                    "selected_strategy_count": 2,
                    "warning_strategy_count": 1,
                    "deactivation_candidate_count": 0,
                    "observation_count": 10,
                },
                "strategies": [
                    {
                        "preset_name": "generated_momentum_a",
                        "realized_return": 0.08,
                        "realized_sharpe": 1.1,
                        "drawdown": 0.03,
                        "paper_observation_count": 10,
                        "recommendation": "keep",
                        "attribution_confidence": "high",
                    },
                    {
                        "preset_name": "generated_value_b",
                        "realized_return": -0.05,
                        "realized_sharpe": -0.3,
                        "drawdown": 0.11,
                        "paper_observation_count": 10,
                        "recommendation": "reduce",
                        "attribution_confidence": "medium",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return root


def test_adaptive_allocation_build_adjusts_weights_and_exports(tmp_path: Path) -> None:
    portfolio_dir = _write_strategy_portfolio(tmp_path / "strategy_portfolio")
    monitoring_dir = _write_strategy_monitoring(tmp_path / "monitoring")

    result = build_adaptive_allocation(
        strategy_portfolio_path=portfolio_dir,
        strategy_monitoring_path=monitoring_dir,
        output_dir=tmp_path / "adaptive",
        policy=AdaptiveAllocationPolicyConfig(
            weighting_mode="performance_tilted",
            max_upweight_per_cycle=0.08,
            max_downweight_per_cycle=0.08,
            max_weight_per_strategy=0.7,
            min_weight_per_strategy=0.1,
            rebalance_smoothing=1.0,
            family_diversification_penalty=1.0,
            universe_diversification_penalty=1.0,
            require_min_observations=5,
        ),
    )

    payload = load_adaptive_allocation(tmp_path / "adaptive")
    rows = {row["preset_name"]: row for row in payload["strategies"]}

    assert result["selected_count"] == 2
    assert abs(sum(row["adjusted_weight"] for row in payload["strategies"]) - 1.0) < 1e-9
    assert rows["generated_momentum_a"]["adjusted_weight"] > rows["generated_momentum_a"]["prior_weight"]
    assert rows["generated_value_b"]["adjusted_weight"] < rows["generated_value_b"]["prior_weight"]

    export = export_adaptive_allocation_run_config(
        adaptive_allocation_path=tmp_path / "adaptive",
        output_dir=tmp_path / "adaptive_run",
    )
    multi_strategy = load_multi_strategy_portfolio_config(export["multi_strategy_config_path"])
    pipeline = load_pipeline_run_config(export["pipeline_config_path"])

    assert len(multi_strategy.sleeves) == 2
    assert pipeline.multi_strategy_input_path is not None


def test_adaptive_allocation_freezes_stale_monitoring(tmp_path: Path) -> None:
    portfolio_dir = _write_strategy_portfolio(tmp_path / "strategy_portfolio")
    monitoring_dir = _write_strategy_monitoring(tmp_path / "monitoring", generated_at="2026-03-01T00:00:00+00:00")

    build_adaptive_allocation(
        strategy_portfolio_path=portfolio_dir,
        strategy_monitoring_path=monitoring_dir,
        output_dir=tmp_path / "adaptive",
        policy=AdaptiveAllocationPolicyConfig(
            max_monitoring_age_days=1,
            freeze_on_stale_monitoring=True,
            require_min_observations=5,
        ),
    )

    payload = load_adaptive_allocation(tmp_path / "adaptive")
    rows = {row["preset_name"]: row for row in payload["strategies"]}

    assert rows["generated_momentum_a"]["adjusted_weight"] == rows["generated_momentum_a"]["prior_weight"]
    assert any("stale_monitoring" in warning for warning in payload["warnings"])


def test_adaptive_allocation_respects_lifecycle_state_caps(tmp_path: Path) -> None:
    portfolio_dir = _write_strategy_portfolio(tmp_path / "strategy_portfolio")
    monitoring_dir = _write_strategy_monitoring(tmp_path / "monitoring")
    lifecycle_dir = tmp_path / "lifecycle"
    lifecycle_dir.mkdir(parents=True, exist_ok=True)
    (lifecycle_dir / "strategy_lifecycle.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy_id": "generated_value_b",
                        "preset_name": "generated_value_b",
                        "current_state": "demoted",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    build_adaptive_allocation(
        strategy_portfolio_path=portfolio_dir,
        strategy_monitoring_path=monitoring_dir,
        strategy_lifecycle_path=lifecycle_dir,
        output_dir=tmp_path / "adaptive",
        policy=AdaptiveAllocationPolicyConfig(
            weighting_mode="performance_tilted",
            max_upweight_per_cycle=0.7,
            max_weight_per_strategy=1.0,
            rebalance_smoothing=1.0,
            require_min_observations=5,
        ),
    )

    payload = load_adaptive_allocation(tmp_path / "adaptive")
    rows = {row["preset_name"]: row for row in payload["strategies"]}

    assert rows["generated_value_b"]["lifecycle_state"] == "demoted"
    assert rows["generated_value_b"]["adjusted_weight"] == 0.0


def test_adaptive_allocation_cli_commands_write_outputs(tmp_path: Path, capsys) -> None:
    portfolio_dir = _write_strategy_portfolio(tmp_path / "strategy_portfolio")
    monitoring_dir = _write_strategy_monitoring(tmp_path / "monitoring")

    cmd_adaptive_allocation_build(
        Namespace(
            portfolio=str(portfolio_dir),
            monitoring=str(monitoring_dir),
            lifecycle=None,
            policy_config=None,
            output_dir=str(tmp_path / "adaptive"),
            dry_run=False,
        )
    )
    cmd_adaptive_allocation_show(Namespace(allocation=str(tmp_path / "adaptive")))
    cmd_adaptive_allocation_export_run_config(
        Namespace(
            allocation=str(tmp_path / "adaptive"),
            output_dir=str(tmp_path / "adaptive_run"),
        )
    )

    captured = capsys.readouterr().out
    assert "Adaptive allocation JSON" in captured
    assert "Selected strategies:" in captured
    assert "Multi-strategy config:" in captured
