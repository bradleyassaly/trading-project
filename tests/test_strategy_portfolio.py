from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.strategy_portfolio_build import cmd_strategy_portfolio_build
from trading_platform.cli.commands.strategy_portfolio_export_run_config import (
    cmd_strategy_portfolio_export_run_config,
)
from trading_platform.cli.commands.strategy_portfolio_show import cmd_strategy_portfolio_show
from trading_platform.config.loader import (
    load_multi_strategy_portfolio_config,
    load_pipeline_run_config,
)
from trading_platform.portfolio.strategy_portfolio import (
    StrategyPortfolioPolicyConfig,
    build_strategy_portfolio,
    export_strategy_portfolio_run_config,
    load_strategy_portfolio,
)


def _write_promoted_strategies(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "strategies": [
            {
                "preset_name": "generated_momentum_a",
                "source_run_id": "run-a",
                "signal_family": "momentum",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.2,
                "promotion_timestamp": "2026-03-22T00:00:00+00:00",
                "generated_preset_path": str(root / "generated_momentum_a.json"),
                "generated_registry_path": str(root / "generated_momentum_a_registry.json"),
                "generated_pipeline_config_path": str(root / "generated_momentum_a_pipeline.yaml"),
            },
            {
                "preset_name": "generated_momentum_b",
                "source_run_id": "run-b",
                "signal_family": "momentum",
                "universe": "sp500",
                "status": "inactive",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.1,
                "promotion_timestamp": "2026-03-21T00:00:00+00:00",
                "generated_preset_path": str(root / "generated_momentum_b.json"),
                "generated_registry_path": str(root / "generated_momentum_b_registry.json"),
                "generated_pipeline_config_path": str(root / "generated_momentum_b_pipeline.yaml"),
            },
            {
                "preset_name": "generated_value_a",
                "source_run_id": "run-b",
                "signal_family": "value",
                "universe": "sp500",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 0.9,
                "promotion_timestamp": "2026-03-20T00:00:00+00:00",
                "generated_preset_path": str(root / "generated_value_a.json"),
                "generated_registry_path": str(root / "generated_value_a_registry.json"),
                "generated_pipeline_config_path": str(root / "generated_value_a_pipeline.yaml"),
            },
            {
                "preset_name": "generated_reversal_dup",
                "source_run_id": "run-a",
                "signal_family": "reversal",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 0.7,
                "promotion_timestamp": "2026-03-19T00:00:00+00:00",
                "generated_preset_path": str(root / "generated_reversal_dup.json"),
                "generated_registry_path": str(root / "generated_reversal_dup_registry.json"),
                "generated_pipeline_config_path": str(root / "generated_reversal_dup_pipeline.yaml"),
            },
        ]
    }
    (root / "promoted_strategies.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return root


def test_strategy_portfolio_build_applies_caps_and_deduplicates(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")
    result = build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "strategy_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=3,
            max_strategies_per_signal_family=1,
            max_weight_per_strategy=0.7,
            min_weight_per_strategy=0.1,
        ),
    )

    payload = load_strategy_portfolio(tmp_path / "strategy_portfolio")

    assert result["selected_count"] == 2
    assert payload["selected_strategies"][0]["preset_name"] == "generated_momentum_a"
    assert any(row["reason"] == "signal_family_cap" for row in payload["excluded_candidates"])
    assert any(row["reason"] == "duplicate_source_run" for row in payload["excluded_candidates"])


def test_strategy_portfolio_weighting_and_export_run_config(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")
    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "strategy_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=2,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.6,
            weighting_mode="metric_proportional",
        ),
    )

    export = export_strategy_portfolio_run_config(
        strategy_portfolio_path=tmp_path / "strategy_portfolio",
        output_dir=tmp_path / "run_bundle",
    )

    multi_strategy = load_multi_strategy_portfolio_config(export["multi_strategy_config_path"])
    pipeline = load_pipeline_run_config(export["pipeline_config_path"])

    assert len(multi_strategy.sleeves) == 2
    assert pipeline.multi_strategy_input_path is not None
    assert pipeline.stages.paper_trading is True


def test_strategy_portfolio_excludes_demoted_lifecycle_entries(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")
    lifecycle_dir = tmp_path / "lifecycle"
    lifecycle_dir.mkdir()
    (lifecycle_dir / "strategy_lifecycle.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy_id": "generated_momentum_a",
                        "preset_name": "generated_momentum_a",
                        "current_state": "demoted",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        lifecycle_path=lifecycle_dir,
        output_dir=tmp_path / "strategy_portfolio",
        policy=StrategyPortfolioPolicyConfig(max_strategies=3),
    )
    payload = load_strategy_portfolio(tmp_path / "strategy_portfolio")

    assert all(row["preset_name"] != "generated_momentum_a" for row in payload["selected_strategies"])
    assert any(row["reason"] == "lifecycle_demoted" for row in payload["excluded_candidates"])


def test_strategy_portfolio_cli_commands_write_outputs(tmp_path: Path, capsys) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")
    output_dir = tmp_path / "strategy_portfolio"

    cmd_strategy_portfolio_build(
        Namespace(
            promoted_dir=str(promoted_dir),
            policy_config=None,
            lifecycle=None,
            output_dir=str(output_dir),
        )
    )
    cmd_strategy_portfolio_show(Namespace(portfolio=str(output_dir)))
    cmd_strategy_portfolio_export_run_config(
        Namespace(
            portfolio=str(output_dir),
            output_dir=str(tmp_path / "run_bundle"),
        )
    )

    captured = capsys.readouterr().out
    assert "Strategy portfolio JSON" in captured
    assert "Selected strategies:" in captured
    assert "Multi-strategy config:" in captured
