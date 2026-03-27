from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from trading_platform.cli.commands.strategy_portfolio_build import cmd_strategy_portfolio_build
from trading_platform.cli.commands.strategy_portfolio_activate import cmd_strategy_portfolio_activate
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
                "rationale": "baseline promotion",
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
                "rationale": "secondary baseline promotion",
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
                "rationale": "value promotion",
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
                "rationale": "reversal promotion",
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


def test_strategy_portfolio_export_prefers_activated_active_rows(tmp_path: Path) -> None:
    portfolio_dir = tmp_path / "strategy_portfolio"
    activated_dir = portfolio_dir / "activated"
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    activated_dir.mkdir(parents=True, exist_ok=True)
    (portfolio_dir / "strategy_portfolio.json").write_text(
        json.dumps(
            {
                "selected_strategies": [
                    {
                        "preset_name": "generated_base",
                        "target_capital_fraction": 0.5,
                        "universe": "nasdaq100",
                        "promotion_variant": "unconditional",
                    },
                    {
                        "preset_name": "generated_conditional",
                        "target_capital_fraction": 0.5,
                        "universe": "nasdaq100",
                        "promotion_variant": "conditional",
                        "condition_id": "regime::risk_on",
                    },
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (activated_dir / "activated_strategy_portfolio.json").write_text(
        json.dumps(
            {
                "active_strategies": [
                    {
                        "preset_name": "generated_base",
                        "target_capital_fraction": 1.0,
                        "universe": "nasdaq100",
                        "promotion_variant": "unconditional",
                        "activation_state": "active",
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    export = export_strategy_portfolio_run_config(
        strategy_portfolio_path=portfolio_dir,
        output_dir=tmp_path / "run_bundle",
    )

    bundle_payload = json.loads(Path(export["run_bundle_path"]).read_text(encoding="utf-8"))
    assert bundle_payload["selected_preset_names"] == ["generated_base"]
    assert bundle_payload["activation_applied"] is True
    assert bundle_payload["source_artifact_path"].endswith("activated_strategy_portfolio.json")


def test_strategy_portfolio_metric_weighted_concentrates_more_than_capped_metric_weighted(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "metric_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=3,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.8,
            weighting_mode="metric_weighted",
        ),
    )
    metric_payload = load_strategy_portfolio(tmp_path / "metric_portfolio")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "capped_metric_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=3,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.8,
            weighting_mode="capped_metric_weighted",
            metric_weight_cap_multiple=1.0,
        ),
    )
    capped_payload = load_strategy_portfolio(tmp_path / "capped_metric_portfolio")

    assert metric_payload["summary"]["max_strategy_weight"] > capped_payload["summary"]["max_strategy_weight"]


def test_strategy_portfolio_inverse_count_by_signal_family_balances_family_weights(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "family_balanced_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=3,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.8,
            weighting_mode="inverse_count_by_signal_family",
            deduplicate_source_runs=False,
        ),
    )
    payload = load_strategy_portfolio(tmp_path / "family_balanced_portfolio")

    family_weights = payload["summary"]["signal_family_weights"]
    assert family_weights["momentum"] == family_weights["value"]
    assert payload["summary"]["effective_family_count"] >= 2.0


def test_strategy_portfolio_min_families_if_available_prefers_family_diversity(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "diverse_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=2,
            max_strategies_per_signal_family=2,
            deduplicate_source_runs=False,
            min_families_if_available=2,
        ),
    )
    payload = load_strategy_portfolio(tmp_path / "diverse_portfolio")

    families = {row["signal_family"] for row in payload["selected_strategies"]}
    assert len(payload["selected_strategies"]) == 2
    assert families == {"momentum", "value"}


def test_strategy_portfolio_score_then_cap_prefers_higher_ranked_strategies(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "score_then_cap_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=3,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.6,
            weighting_mode="score_then_cap",
            deduplicate_source_runs=False,
        ),
    )
    payload = load_strategy_portfolio(tmp_path / "score_then_cap_portfolio")
    rows = {row["preset_name"]: row for row in payload["selected_strategies"]}

    assert rows["generated_momentum_a"]["allocation_weight"] > rows["generated_value_a"]["allocation_weight"]
    assert payload["summary"]["weighting_mode_resolved"] == "score_then_cap"


def test_strategy_portfolio_accepts_strategy_weighting_aliases(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "aliased_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_active_strategies=2,
            strategy_weighting_mode="metric_weighted",
            strategy_weight_metric="ranking_value",
            min_strategy_weight=0.2,
            max_strategy_weight=0.8,
            max_strategies_per_signal_family=2,
        ),
    )
    payload = load_strategy_portfolio(tmp_path / "aliased_portfolio")

    assert payload["summary"]["max_active_strategies"] == 2
    assert payload["summary"]["strategy_weight_metric"] == "ranking_value"
    assert payload["summary"]["weighting_mode_resolved"] == "metric_weighted"


def test_strategy_portfolio_risk_adjusted_weighting_penalizes_drawdown(tmp_path: Path) -> None:
    promoted_dir = tmp_path / "promoted"
    promoted_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "strategies": [
            {
                "preset_name": "lower_risk",
                "source_run_id": "run-a",
                "signal_family": "momentum",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.0,
                "max_drawdown": 0.05,
            },
            {
                "preset_name": "higher_risk",
                "source_run_id": "run-b",
                "signal_family": "value",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.0,
                "max_drawdown": 0.45,
            },
        ]
    }
    (promoted_dir / "promoted_strategies.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "risk_adjusted",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=2,
            max_strategies_per_signal_family=2,
            weighting_mode="risk_adjusted",
            max_weight_per_strategy=1.0,
        ),
    )
    result = load_strategy_portfolio(tmp_path / "risk_adjusted")
    rows = {row["preset_name"]: row for row in result["selected_strategies"]}

    assert rows["lower_risk"]["allocation_weight"] > rows["higher_risk"]["allocation_weight"]


def test_strategy_portfolio_can_keep_conditional_sibling_for_same_run_when_enabled(tmp_path: Path) -> None:
    promoted_dir = tmp_path / "promoted"
    promoted_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "strategies": [
            {
                "preset_name": "generated_momentum_base",
                "source_run_id": "run-a",
                "signal_family": "momentum",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.2,
                "promotion_variant": "unconditional",
                "promotion_timestamp": "2026-03-22T00:00:00+00:00",
                "generated_preset_path": str(promoted_dir / "generated_momentum_base.json"),
                "generated_registry_path": str(promoted_dir / "generated_momentum_base_registry.json"),
                "generated_pipeline_config_path": str(promoted_dir / "generated_momentum_base_pipeline.yaml"),
            },
            {
                "preset_name": "generated_momentum_conditional",
                "source_run_id": "run-a",
                "signal_family": "momentum",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.15,
                "promotion_variant": "conditional",
                "condition_id": "regime_risk_on",
                "condition_type": "regime",
                "activation_conditions": [{"condition_id": "regime_risk_on", "condition_type": "regime"}],
                "rationale": "regime edge",
                "promotion_timestamp": "2026-03-22T00:00:01+00:00",
                "generated_preset_path": str(promoted_dir / "generated_momentum_conditional.json"),
                "generated_registry_path": str(promoted_dir / "generated_momentum_conditional_registry.json"),
                "generated_pipeline_config_path": str(promoted_dir / "generated_momentum_conditional_pipeline.yaml"),
            },
        ]
    }
    (promoted_dir / "promoted_strategies.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "conditional_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=2,
            max_strategies_per_signal_family=2,
            allow_conditional_variant_siblings=True,
            conditional_variant_score_bonus=0.05,
        ),
    )
    portfolio_payload = load_strategy_portfolio(tmp_path / "conditional_portfolio")

    assert len(portfolio_payload["selected_strategies"]) == 2
    assert portfolio_payload["summary"]["selected_conditional_variant_count"] == 1


def test_strategy_portfolio_can_disable_conditional_strategies(tmp_path: Path) -> None:
    promoted_dir = tmp_path / "promoted"
    promoted_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "strategies": [
            {
                "preset_name": "generated_base",
                "source_run_id": "run-a",
                "signal_family": "momentum",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.2,
                "promotion_variant": "unconditional",
                "promotion_timestamp": "2026-03-22T00:00:00+00:00",
                "generated_preset_path": str(promoted_dir / "generated_base.json"),
            },
            {
                "preset_name": "generated_conditional",
                "source_run_id": "run-b",
                "signal_family": "momentum",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.15,
                "promotion_variant": "conditional",
                "condition_id": "regime::risk_on",
                "condition_type": "regime",
                "activation_conditions": [{"condition_id": "regime::risk_on", "condition_type": "regime"}],
                "rationale": "conditional edge",
                "promotion_timestamp": "2026-03-22T00:00:01+00:00",
                "generated_preset_path": str(promoted_dir / "generated_conditional.json"),
            },
        ]
    }
    (promoted_dir / "promoted_strategies.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=2,
            include_conditional_strategies=False,
        ),
    )
    portfolio_payload = load_strategy_portfolio(tmp_path / "portfolio")

    assert len(portfolio_payload["selected_strategies"]) == 1
    assert portfolio_payload["selected_strategies"][0]["promotion_variant"] == "unconditional"
    assert any(row["reason"] == "conditional_disabled" for row in portfolio_payload["excluded_candidates"])


def test_strategy_portfolio_preserves_conditional_activation_metadata_and_shadow_mode(tmp_path: Path) -> None:
    promoted_dir = tmp_path / "promoted"
    promoted_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "strategies": [
            {
                "preset_name": "generated_base",
                "source_run_id": "run-a",
                "signal_family": "momentum",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "portfolio_sharpe",
                "ranking_value": 1.2,
                "promotion_variant": "unconditional",
                "promotion_timestamp": "2026-03-22T00:00:00+00:00",
                "generated_preset_path": str(promoted_dir / "generated_base.json"),
            },
            {
                "preset_name": "generated_conditional",
                "source_run_id": "run-a",
                "signal_family": "momentum",
                "universe": "nasdaq100",
                "status": "active",
                "ranking_metric": "mean_spearman_ic",
                "ranking_value": 0.15,
                "promotion_variant": "conditional",
                "condition_id": "regime::risk_on",
                "condition_type": "regime",
                "activation_conditions": [{"condition_id": "regime::risk_on", "condition_type": "regime"}],
                "rationale": "conditional edge",
                "promotion_timestamp": "2026-03-22T00:00:01+00:00",
                "generated_preset_path": str(promoted_dir / "generated_conditional.json"),
            },
        ]
    }
    (promoted_dir / "promoted_strategies.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=2,
            allow_conditional_variant_siblings=True,
            conditional_selection_mode="shadow_only",
        ),
    )
    portfolio_payload = load_strategy_portfolio(tmp_path / "portfolio")

    assert len(portfolio_payload["selected_strategies"]) == 1
    assert portfolio_payload["summary"]["shadow_conditional_variant_count"] == 1
    assert portfolio_payload["shadow_strategies"][0]["activation_state"] == "shadow_only"
    assert portfolio_payload["shadow_strategies"][0]["activation_conditions"]
    assert Path(tmp_path / "portfolio" / "strategy_portfolio_condition_summary.csv").exists()


def test_strategy_portfolio_weight_smoothing_reduces_concentration(tmp_path: Path) -> None:
    promoted_dir = _write_promoted_strategies(tmp_path / "promoted")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "unsmoothed_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=3,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.8,
            weighting_mode="metric_weighted",
            deduplicate_source_runs=False,
            weighting_smoothing_power=1.0,
        ),
    )
    unsmoothed_payload = load_strategy_portfolio(tmp_path / "unsmoothed_portfolio")

    build_strategy_portfolio(
        promoted_dir=promoted_dir,
        output_dir=tmp_path / "smoothed_portfolio",
        policy=StrategyPortfolioPolicyConfig(
            max_strategies=3,
            max_strategies_per_signal_family=2,
            max_weight_per_strategy=0.8,
            weighting_mode="metric_weighted",
            deduplicate_source_runs=False,
            weighting_smoothing_power=0.5,
        ),
    )
    smoothed_payload = load_strategy_portfolio(tmp_path / "smoothed_portfolio")

    assert smoothed_payload["summary"]["max_strategy_weight"] < unsmoothed_payload["summary"]["max_strategy_weight"]


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


def test_strategy_portfolio_activate_cli_command_writes_outputs(tmp_path: Path, capsys) -> None:
    portfolio_dir = tmp_path / "portfolio"
    activated_dir = tmp_path / "activated"
    regime_dir = tmp_path / "regime"
    metadata_dir = tmp_path / "metadata"
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    regime_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    (portfolio_dir / "strategy_portfolio.json").write_text(
        json.dumps(
            {
                "policy": {
                    "evaluate_conditional_activation": True,
                    "activation_context_sources": ["regime"],
                    "include_inactive_conditionals_in_output": True,
                },
                "selected_strategies": [
                    {
                        "preset_name": "generated_base",
                        "promotion_variant": "unconditional",
                        "activation_conditions": [],
                        "activation_state": "always_on",
                    },
                    {
                        "preset_name": "generated_regime",
                        "promotion_variant": "conditional",
                        "condition_id": "regime::high_vol|uptrend|low_dispersion",
                        "condition_type": "regime",
                        "activation_conditions": [
                            {"condition_id": "regime::high_vol|uptrend|low_dispersion", "condition_type": "regime"}
                        ],
                        "activation_state": "inactive_until_condition_match",
                    },
                ],
                "shadow_strategies": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (regime_dir / "regime_labels_by_date.csv").write_text(
        "timestamp,regime_key,volatility_regime,trend_regime,dispersion_regime\n"
        "2026-03-26,high_vol|uptrend|low_dispersion,high_vol,uptrend,low_dispersion\n",
        encoding="utf-8",
    )
    (metadata_dir / "universe_enrichment.csv").write_text(
        "symbol,benchmark_context_label,relative_strength_20\nAAPL,risk_on_outperform_broad,0.03\n",
        encoding="utf-8",
    )

    cmd_strategy_portfolio_activate(
        Namespace(
            portfolio=str(portfolio_dir),
            output_dir=str(activated_dir),
            market_regime=None,
            regime_labels=str(regime_dir),
            metadata_dir=str(metadata_dir),
            activation_context_sources=["regime"],
            include_inactive_conditionals_in_output=True,
        )
    )

    captured = capsys.readouterr().out
    assert "Active strategies:" in captured
    assert "Activated portfolio JSON:" in captured
    assert (activated_dir / "activated_strategy_portfolio.json").exists()
