from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

from trading_platform.cli.commands.paper_run_scheduled import cmd_paper_run_scheduled
from trading_platform.cli.commands.research_promote import cmd_research_promote
from trading_platform.cli.grouped_parser import build_parser
from trading_platform.cli.presets import resolve_cli_preset
from trading_platform.config.loader import load_pipeline_run_config
from trading_platform.research.promotion_pipeline import (
    PromotionPolicyConfig,
    apply_research_promotions,
)
from trading_platform.research.registry import build_promotion_candidates, build_research_registry
from trading_platform.research.strategy_validation import (
    StrategyValidationPolicyConfig,
    build_strategy_validation,
)


def _write_research_run(
    root: Path,
    *,
    run_name: str,
    signal_family: str,
    universe: str,
    mean_spearman_ic: float,
    portfolio_sharpe: float,
    promoted_signal_count: int,
    folds_tested: int = 4,
) -> Path:
    run_dir = root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    leaderboard_path = run_dir / "leaderboard.csv"
    fold_results_path = run_dir / "fold_results.csv"
    promoted_path = run_dir / "promoted_signals.csv"
    portfolio_metrics_path = run_dir / "portfolio_metrics.csv"
    implementability_path = run_dir / "implementability_report.csv"
    diagnostics_path = run_dir / "signal_diagnostics.json"
    approved_dir = run_dir / "approved"
    approved_dir.mkdir(exist_ok=True)
    approved_model_state_path = approved_dir / "approved_model_state.json"

    pd.DataFrame(
        [
            {
                "signal_family": signal_family,
                "lookback": 20,
                "horizon": 5,
                "mean_spearman_ic": mean_spearman_ic,
                "mean_hit_rate": 0.55,
                "mean_turnover": 0.12,
                "promotion_status": "promote",
                "rejection_reason": "",
            }
        ]
    ).to_csv(leaderboard_path, index=False)
    pd.DataFrame(
        [
            {
                "fold_id": idx + 1,
                "test_start": f"2025-0{idx + 1}-01",
                "test_end": f"2025-0{idx + 1}-28",
            }
            for idx in range(folds_tested)
        ]
    ).to_csv(fold_results_path, index=False)
    pd.DataFrame([{"signal_family": signal_family}] * promoted_signal_count).to_csv(promoted_path, index=False)
    pd.DataFrame([{"sharpe": portfolio_sharpe, "total_return": 0.18, "max_drawdown": -0.08}]).to_csv(
        portfolio_metrics_path,
        index=False,
    )
    pd.DataFrame([{"return_drag": 0.05}]).to_csv(implementability_path, index=False)
    diagnostics_path.write_text(json.dumps({"evaluation_mode": "cross_sectional_long_short"}, indent=2), encoding="utf-8")
    approved_model_state_path.write_text(
        json.dumps({"source_artifact_dir": str(run_dir), "promoted_signals": []}, indent=2),
        encoding="utf-8",
    )

    from trading_platform.research.registry import write_research_run_manifest

    return write_research_run_manifest(
        output_dir=run_dir,
        workflow_type="alpha_research",
        command="test",
        feature_dir=root / "features",
        signal_family=signal_family,
        universe=universe,
        symbols_requested=["AAPL", "MSFT", "NVDA"],
        lookbacks=[20],
        horizons=[5],
        min_rows=250,
        train_size=756,
        test_size=63,
        step_size=63,
        min_train_size=252,
        artifact_paths={
            "leaderboard_path": leaderboard_path,
            "fold_results_path": fold_results_path,
            "promoted_signals_path": promoted_path,
            "portfolio_metrics_path": portfolio_metrics_path,
            "implementability_report_path": implementability_path,
            "signal_diagnostics_path": diagnostics_path,
            "approved_model_state_deployment_path": approved_model_state_path,
            "approved_model_state_path": approved_model_state_path,
        },
    )


def _seed_registry(tmp_path: Path) -> Path:
    _write_research_run(
        tmp_path,
        run_name="run_a",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    _write_research_run(
        tmp_path,
        run_name="run_b",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.03,
        portfolio_sharpe=0.9,
        promoted_signal_count=1,
    )
    registry_dir = tmp_path / "research_registry"
    build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)
    return registry_dir


def test_promotion_artifact_generation_and_policy_filtering(tmp_path: Path) -> None:
    registry_dir = _seed_registry(tmp_path)
    output_dir = tmp_path / "generated_strategies"
    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=1, max_strategies_per_group=1),
    )

    assert result["selected_count"] == 1
    promoted_index = json.loads((output_dir / "promoted_strategies.json").read_text(encoding="utf-8"))
    assert promoted_index["registry_dir"] == str(registry_dir)
    assert promoted_index["promotion_candidates_path"] == str(registry_dir / "promotion_candidates.json")
    row = promoted_index["strategies"][0]
    assert row["source_run_id"] == "run_a"
    assert Path(row["generated_preset_path"]).exists()
    assert Path(row["generated_registry_path"]).exists()
    assert Path(row["generated_pipeline_config_path"]).exists()


def test_promotion_dry_run_and_duplicate_protection(tmp_path: Path) -> None:
    registry_dir = _seed_registry(tmp_path)
    output_dir = tmp_path / "generated_strategies"
    dry_run = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=1),
        dry_run=True,
    )

    assert dry_run["selected_count"] == 1
    assert not (output_dir / "promoted_strategies.json").exists()

    apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=1),
    )
    second = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=1),
    )
    third = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=1),
    )

    promoted_index = json.loads((output_dir / "promoted_strategies.json").read_text(encoding="utf-8"))
    source_run_ids = [row["source_run_id"] for row in promoted_index["strategies"]]
    assert second["selected_count"] == 1
    assert third["selected_count"] == 0
    assert len(source_run_ids) == len(set(source_run_ids))


def test_promotion_requires_validation_pass_unless_overridden(tmp_path: Path) -> None:
    registry_dir = _seed_registry(tmp_path)
    validation_dir = tmp_path / "validation"
    build_strategy_validation(
        artifacts_root=tmp_path,
        output_dir=validation_dir,
        policy=StrategyValidationPolicyConfig(
            min_folds=5,
            min_out_of_sample_sharpe=1.5,
            min_mean_spearman_ic=0.05,
            min_positive_fold_ratio=0.8,
            min_proxy_confidence_score=0.9,
        ),
    )
    output_dir = tmp_path / "generated_strategies"

    blocked = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        validation_path=validation_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=2, require_validation_pass=True),
    )
    allowed = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        validation_path=validation_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=1, require_validation_pass=True),
        override_validation=True,
    )

    assert blocked["selected_count"] == 0
    assert allowed["selected_count"] == 1
    assert allowed["promoted_rows"][0]["validation_status"] in {"weak", "fail"}


def test_generated_configs_are_valid_for_pipeline_and_paper(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    registry_dir = _seed_registry(tmp_path)
    output_dir = tmp_path / "generated_strategies"
    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=output_dir,
        policy=PromotionPolicyConfig(max_strategies_total=1),
    )
    row = result["promoted_rows"][0]
    preset_name = row["preset_name"]

    from trading_platform import cli as cli_pkg  # noqa: F401
    from trading_platform.cli import presets as preset_module

    monkeypatch.setattr(preset_module, "GENERATED_PRESET_DIRECTORIES", [output_dir])
    preset = resolve_cli_preset(preset_name)
    assert preset.params["signal_source"] == "composite"

    parser = build_parser()
    args = parser.parse_args(
        [
            "paper",
            "run-preset-scheduled",
            "--preset",
            preset_name,
        ]
    )
    called: dict[str, str] = {}

    def fake_cmd_paper_run(parsed_args) -> None:
        called["preset"] = parsed_args.preset

    monkeypatch.setattr("trading_platform.cli.commands.paper_run_scheduled.cmd_paper_run", fake_cmd_paper_run)
    cmd_paper_run_scheduled(args)
    assert called["preset"] == preset_name

    pipeline_config = load_pipeline_run_config(row["generated_pipeline_config_path"])
    assert pipeline_config.registry_path is not None
    assert pipeline_config.stages.paper_trading is True


def test_research_promote_cli_writes_outputs(tmp_path: Path, capsys) -> None:
    _seed_registry(tmp_path)
    output_dir = tmp_path / "generated_strategies"

    cmd_research_promote(
        Namespace(
            artifacts_root=str(tmp_path),
            registry_dir=None,
            output_dir=str(output_dir),
            policy_config=None,
            validation=None,
            top_n=1,
            allow_overwrite=False,
            dry_run=False,
            inactive=True,
            override_validation=False,
        )
    )

    captured = capsys.readouterr().out
    assert "Research registry:" in captured
    assert "Promotion candidates:" in captured
    assert "Selected promotions: 1" in captured
    assert (output_dir / "promoted_strategies.json").exists()


def test_canonical_promotion_output_feeds_strategy_portfolio_build(tmp_path: Path) -> None:
    _seed_registry(tmp_path)
    promoted_dir = tmp_path / "generated_strategies"
    strategy_portfolio_dir = tmp_path / "strategy_portfolio"

    cmd_research_promote(
        Namespace(
            artifacts_root=str(tmp_path),
            registry_dir=None,
            output_dir=str(promoted_dir),
            policy_config=None,
            validation=None,
            top_n=2,
            allow_overwrite=False,
            dry_run=False,
            inactive=False,
            override_validation=False,
        )
    )

    from trading_platform.cli.commands.strategy_portfolio_build import cmd_strategy_portfolio_build
    from trading_platform.portfolio.strategy_portfolio import load_strategy_portfolio

    cmd_strategy_portfolio_build(
        Namespace(
            promoted_dir=str(promoted_dir),
            policy_config=None,
            lifecycle=None,
            output_dir=str(strategy_portfolio_dir),
        )
    )

    payload = load_strategy_portfolio(strategy_portfolio_dir)
    assert payload["summary"]["total_selected_strategies"] >= 1
    assert payload["selected_strategies"][0]["preset_name"].startswith("generated_")
