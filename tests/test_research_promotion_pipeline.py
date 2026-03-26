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


def _write_stale_global_registry(root: Path, *, stale_run_id: str = "run_stale") -> Path:
    registry_dir = root / "research_registry"
    registry_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": "2026-03-25T00:00:00+00:00",
        "summary": {"run_count": 1, "signal_families": ["synthetic"]},
        "runs": [
            {
                "run_id": stale_run_id,
                "timestamp": "2024-01-01T00:00:00+00:00",
                "signal_family": "synthetic",
                "promotion_recommendation": {"recommendation": "promotion_candidate", "eligible": True, "reasons": []},
                "top_metrics": {"mean_spearman_ic": 0.99, "portfolio_sharpe": 9.99, "implementability_return_drag": 0.0},
                "promoted_signal_count": 5,
                "folds_tested": 6,
                "candidate_count": 10,
                "artifact_dir": str(root / stale_run_id),
            }
        ],
    }
    (registry_dir / "research_registry.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    pd.DataFrame(
        [
            {
                "run_id": stale_run_id,
                "timestamp": "2024-01-01T00:00:00+00:00",
                "signal_family": "synthetic",
                "promotion_recommendation": "promotion_candidate",
                "mean_spearman_ic": 0.99,
                "portfolio_sharpe": 9.99,
                "eligible": True,
            }
        ]
    ).to_csv(registry_dir / "research_registry.csv", index=False)
    (registry_dir / "promotion_candidates.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-25T00:00:00+00:00",
                "rows": [
                    {
                        "run_id": stale_run_id,
                        "timestamp": "2024-01-01T00:00:00+00:00",
                        "signal_family": "synthetic",
                        "eligible": True,
                        "promotion_recommendation": "promotion_candidate",
                        "reason_count": 0,
                        "reasons": "",
                        "mean_spearman_ic": 0.99,
                        "portfolio_sharpe": 9.99,
                        "promoted_signal_count": 5,
                        "folds_tested": 6,
                        "candidate_count": 10,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "run_id": stale_run_id,
                "timestamp": "2024-01-01T00:00:00+00:00",
                "signal_family": "synthetic",
                "eligible": True,
                "promotion_recommendation": "promotion_candidate",
                "reason_count": 0,
                "reasons": "",
                "mean_spearman_ic": 0.99,
                "portfolio_sharpe": 9.99,
                "promoted_signal_count": 5,
                "folds_tested": 6,
                "candidate_count": 10,
            }
        ]
    ).to_csv(registry_dir / "promotion_candidates.csv", index=False)
    return registry_dir


def _attach_conditional_promotion_candidate(
    manifest_path: Path,
    *,
    condition_id: str = "regime_risk_on",
    condition_type: str = "regime",
    sample_size: int = 50,
    improvement_vs_baseline: float = 0.2,
    metric_name: str = "mean_spearman_ic",
    metric_value: float = 0.24,
    baseline_metric_value: float = 0.04,
) -> None:
    _attach_conditional_promotion_candidates(
        manifest_path,
        [
            {
                "eligible": True,
                "condition_id": condition_id,
                "condition_type": condition_type,
                "sample_size": sample_size,
                "metric_name": metric_name,
                "metric_value": metric_value,
                "baseline_metric_value": baseline_metric_value,
                "improvement_vs_baseline": improvement_vs_baseline,
                "activation_condition": {
                    "condition_id": condition_id,
                    "condition_type": condition_type,
                },
                "promotion_summary": "conditional variant eligible",
                "reason": "conditional variant eligible",
            }
        ],
    )


def _attach_conditional_promotion_candidates(
    manifest_path: Path,
    rows: list[dict[str, object]],
) -> None:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["conditional_research"] = {
        "enabled": True,
        "promotion_candidates": rows,
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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


def test_promotion_min_families_if_available_preserves_family_diversity(tmp_path: Path) -> None:
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
        universe="sp500",
        mean_spearman_ic=0.03,
        portfolio_sharpe=1.1,
        promoted_signal_count=2,
    )
    _write_research_run(
        tmp_path,
        run_name="run_c",
        signal_family="value",
        universe="sp500",
        mean_spearman_ic=0.025,
        portfolio_sharpe=1.0,
        promoted_signal_count=2,
    )
    registry_dir = tmp_path / "research_registry"
    build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)

    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "generated_strategies",
        policy=PromotionPolicyConfig(
            max_strategies_total=2,
            max_strategies_per_group=2,
            max_strategies_per_family=2,
            min_families_if_available=2,
        ),
    )

    families = {row["signal_family"] for row in result["promoted_rows"]}
    assert result["selected_count"] == 2
    assert families == {"momentum", "value"}


def test_promotion_candidates_built_from_multi_family_manifest_preserve_registry_family_summary(tmp_path: Path) -> None:
    manifest_path = _write_research_run(
        tmp_path,
        run_name="run_multi_family",
        signal_family="multi_family",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    leaderboard_path = tmp_path / "run_multi_family" / "leaderboard.csv"
    pd.DataFrame(
        [
            {
                "candidate_id": "momentum_candidate",
                "candidate_name": "momentum_candidate",
                "signal_family": "momentum",
                "signal_variant": "baseline",
                "lookback": 20,
                "horizon": 5,
                "mean_spearman_ic": 0.04,
                "mean_hit_rate": 0.55,
                "mean_turnover": 0.12,
                "promotion_status": "promote",
                "rejection_reason": "",
            },
            {
                "candidate_id": "fundamental_value_candidate",
                "candidate_name": "fundamental_value_candidate",
                "signal_family": "fundamental_value",
                "signal_variant": "baseline",
                "lookback": 20,
                "horizon": 5,
                "mean_spearman_ic": 0.035,
                "mean_hit_rate": 0.54,
                "mean_turnover": 0.08,
                "promotion_status": "promote",
                "rejection_reason": "",
            },
        ]
    ).to_csv(leaderboard_path, index=False)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["signal_families"] = ["momentum", "fundamental_value"]
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    registry_dir = tmp_path / "research_registry"
    registry_result = build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    candidate_result = build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)
    registry_payload = json.loads(Path(registry_result["registry_json_path"]).read_text(encoding="utf-8"))

    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "generated_strategies",
        policy=PromotionPolicyConfig(
            max_strategies_total=2,
            max_strategies_per_group=2,
            max_strategies_per_family=1,
            min_families_if_available=2,
        ),
    )

    assert registry_payload["summary"]["signal_families"] == ["fundamental_value", "momentum"]
    assert candidate_result["eligible_count"] == 1
    assert result["selected_count"] == 1
    assert result["promoted_rows"][0]["signal_family"] == "multi_family"


def test_bootstrap_promotion_from_fresh_run_without_prior_history(tmp_path: Path) -> None:
    registry_dir = _seed_registry(tmp_path)

    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "generated_strategies",
        policy=PromotionPolicyConfig(
            max_strategies_total=1,
            bootstrap_mode=True,
            allow_first_promotion_without_history=True,
        ),
    )

    assert result["selected_count"] == 1
    assert result["promoted_rows"][0]["source_run_id"] == "run_a"


def test_bootstrap_mode_uses_candidate_composition_without_circular_history_block(tmp_path: Path) -> None:
    manifest_path = _write_research_run(
        tmp_path,
        run_name="run_bootstrap",
        signal_family="multi_family",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.25,
        promoted_signal_count=2,
    )
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["promoted_signal_count"] = None
    payload.setdefault("diagnostics_snapshot", {})["composite_portfolio"] = {
        "selected_signals": [{"signal_family": "momentum"}, {"signal_family": "value"}]
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    registry_dir = tmp_path / "research_registry"
    build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)

    blocked = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "blocked",
        policy=PromotionPolicyConfig(
            max_strategies_total=1,
            min_promoted_signals=2,
        ),
    )
    allowed = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "allowed",
        policy=PromotionPolicyConfig(
            max_strategies_total=1,
            min_promoted_signals=2,
            bootstrap_mode=True,
            allow_first_promotion_without_history=True,
        ),
    )

    assert blocked["selected_count"] == 0
    assert allowed["selected_count"] == 1
    assert allowed["promoted_rows"][0]["bootstrap_applied"] is True


def test_conditional_promotion_rejects_small_samples_and_no_material_improvement(tmp_path: Path) -> None:
    manifest_path = _write_research_run(
        tmp_path,
        run_name="run_conditional_filters",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    _attach_conditional_promotion_candidates(
        manifest_path,
        [
            {
                "eligible": True,
                "condition_id": "regime_small_sample",
                "condition_type": "regime",
                "sample_size": 10,
                "metric_name": "mean_spearman_ic",
                "metric_value": 0.2,
                "baseline_metric_value": 0.04,
                "improvement_vs_baseline": 0.16,
                "reason": "too small",
            },
            {
                "eligible": True,
                "condition_id": "benchmark_no_improvement",
                "condition_type": "benchmark_context",
                "sample_size": 60,
                "metric_name": "mean_spearman_ic",
                "metric_value": 0.041,
                "baseline_metric_value": 0.04,
                "improvement_vs_baseline": 0.001,
                "reason": "not materially better",
            },
            {
                "eligible": True,
                "condition_id": "benchmark_good",
                "condition_type": "benchmark_context",
                "sample_size": 75,
                "metric_name": "mean_spearman_ic",
                "metric_value": 0.09,
                "baseline_metric_value": 0.04,
                "improvement_vs_baseline": 0.05,
                "reason": "material improvement",
            },
        ],
    )
    registry_dir = tmp_path / "research_registry"
    build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)

    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "generated_strategies",
        policy=PromotionPolicyConfig(
            max_strategies_total=1,
            enable_conditional_variants=True,
            min_condition_sample_size=30,
            min_condition_improvement=0.01,
        ),
    )

    assert result["selected_count"] == 1
    assert result["promoted_rows"][0]["promotion_variant"] == "conditional"
    assert result["promoted_rows"][0]["condition_id"] == "benchmark_good"


def test_promotion_can_emit_conditional_variant_alongside_unconditional_baseline(tmp_path: Path) -> None:
    manifest_path = _write_research_run(
        tmp_path,
        run_name="run_conditional",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.04,
        portfolio_sharpe=1.2,
        promoted_signal_count=2,
    )
    _attach_conditional_promotion_candidate(manifest_path)
    registry_dir = tmp_path / "research_registry"
    build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)

    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "generated_strategies",
        policy=PromotionPolicyConfig(
            max_strategies_total=1,
            enable_conditional_variants=True,
            emit_conditional_variants_alongside_baseline=True,
            conditional_variant_allowance=1,
        ),
    )

    variants = {row["promotion_variant"] for row in result["promoted_rows"]}
    assert result["selected_count"] == 2
    assert variants == {"unconditional", "conditional"}
    promoted_index = json.loads((tmp_path / "generated_strategies" / "promoted_strategies.json").read_text(encoding="utf-8"))
    assert promoted_index["summary"]["baseline_count"] == 1
    assert promoted_index["summary"]["conditional_count"] == 1
    assert Path(promoted_index["promoted_condition_summary_path"]).exists()


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
    assert third["selected_count"] == 1
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


def test_research_promote_cli_defaults_to_run_local_registry(tmp_path: Path) -> None:
    manifest_path = _write_research_run(
        tmp_path,
        run_name="run_current",
        signal_family="momentum",
        universe="nasdaq100",
        mean_spearman_ic=0.05,
        portfolio_sharpe=1.3,
        promoted_signal_count=2,
    )
    _write_stale_global_registry(tmp_path)
    output_dir = tmp_path / "generated_strategies"

    cmd_research_promote(
        Namespace(
            artifacts_root=str(tmp_path),
            run_dir=str(manifest_path.parent),
            registry_scope="run_local",
            use_global_registry=False,
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

    promoted_index = json.loads((output_dir / "promoted_strategies.json").read_text(encoding="utf-8"))
    assert promoted_index["strategies"][0]["source_run_id"] == "run_current"
    assert promoted_index["registry_dir"] == str(manifest_path.parent / "research_registry")


def test_research_promote_cli_global_scope_uses_shared_registry(tmp_path: Path) -> None:
    _seed_registry(tmp_path)
    output_dir = tmp_path / "generated_strategies"

    cmd_research_promote(
        Namespace(
            artifacts_root=str(tmp_path),
            run_dir=None,
            registry_scope="global",
            use_global_registry=False,
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

    promoted_index = json.loads((output_dir / "promoted_strategies.json").read_text(encoding="utf-8"))
    assert promoted_index["registry_dir"] == str(tmp_path / "research_registry")
    assert promoted_index["strategies"][0]["source_run_id"] == "run_a"


def test_research_promote_cli_explicit_global_registry_dir_preserves_legacy_behavior(tmp_path: Path) -> None:
    registry_dir = _seed_registry(tmp_path)
    output_dir = tmp_path / "generated_strategies"

    cmd_research_promote(
        Namespace(
            artifacts_root=str(tmp_path),
            run_dir=None,
            registry_scope="global",
            use_global_registry=False,
            registry_dir=str(registry_dir),
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

    promoted_index = json.loads((output_dir / "promoted_strategies.json").read_text(encoding="utf-8"))
    assert promoted_index["registry_dir"] == str(registry_dir)


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
