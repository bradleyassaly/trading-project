from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from trading_platform.research.promotion_pipeline import PromotionPolicyConfig, apply_research_promotions
from trading_platform.research.registry import (
    build_promotion_candidates,
    build_research_registry,
    write_research_run_manifest,
)


def _write_conditional_research_run(root: Path, *, include_conditional: bool = True) -> Path:
    run_dir = root / "run_conditional"
    run_dir.mkdir(parents=True, exist_ok=True)
    leaderboard_path = run_dir / "leaderboard.csv"
    fold_results_path = run_dir / "fold_results.csv"
    promoted_path = run_dir / "promoted_signals.csv"
    portfolio_metrics_path = run_dir / "portfolio_metrics.csv"
    implementability_path = run_dir / "implementability_report.csv"
    diagnostics_path = run_dir / "signal_diagnostics.json"

    pd.DataFrame(
        [
            {
                "signal_family": "momentum",
                "lookback": 20,
                "horizon": 5,
                "mean_spearman_ic": 0.04,
                "mean_hit_rate": 0.56,
                "mean_turnover": 0.1,
                "promotion_status": "promote",
                "rejection_reason": "",
            }
        ]
    ).to_csv(leaderboard_path, index=False)
    pd.DataFrame(
        [
            {"fold_id": 1, "test_start": "2025-01-01", "test_end": "2025-01-31"},
            {"fold_id": 2, "test_start": "2025-02-01", "test_end": "2025-02-28"},
            {"fold_id": 3, "test_start": "2025-03-01", "test_end": "2025-03-31"},
            {"fold_id": 4, "test_start": "2025-04-01", "test_end": "2025-04-30"},
        ]
    ).to_csv(fold_results_path, index=False)
    pd.DataFrame([{"signal_family": "momentum"}] * 2).to_csv(promoted_path, index=False)
    pd.DataFrame([{"portfolio_sharpe": 1.15, "portfolio_total_return": 0.22, "portfolio_max_drawdown": -0.07}]).to_csv(
        portfolio_metrics_path,
        index=False,
    )
    pd.DataFrame([{"return_drag": 0.04}]).to_csv(implementability_path, index=False)
    diagnostics_path.write_text(json.dumps({"evaluation_mode": "cross_sectional_long_short"}, indent=2), encoding="utf-8")

    artifact_paths = {
        "leaderboard_path": leaderboard_path,
        "fold_results_path": fold_results_path,
        "promoted_signals_path": promoted_path,
        "portfolio_metrics_path": portfolio_metrics_path,
        "implementability_report_path": implementability_path,
        "signal_diagnostics_path": diagnostics_path,
    }
    if include_conditional:
        regime_path = run_dir / "signal_performance_by_regime.csv"
        benchmark_path = run_dir / "signal_performance_by_benchmark_context.csv"
        sub_universe_path = run_dir / "signal_performance_by_sub_universe.csv"
        pd.DataFrame(
            [
                {
                    "candidate_id": "momentum|20|5",
                    "signal_family": "momentum",
                    "lookback": 20,
                    "horizon": 5,
                    "regime_key": "trend|normal_vol|normal_dispersion",
                    "volatility_regime": "normal_vol",
                    "trend_regime": "trend",
                    "dispersion_regime": "normal_dispersion",
                    "dates_evaluated": 48,
                    "mean_spearman_ic": 0.08,
                    "mean_long_short_spread": 0.02,
                },
                {
                    "candidate_id": "momentum|20|5",
                    "signal_family": "momentum",
                    "lookback": 20,
                    "horizon": 5,
                    "regime_key": "high_vol|flat|wide_dispersion",
                    "volatility_regime": "high_vol",
                    "trend_regime": "flat",
                    "dispersion_regime": "wide_dispersion",
                    "dates_evaluated": 8,
                    "mean_spearman_ic": 0.01,
                    "mean_long_short_spread": 0.0,
                },
            ]
        ).to_csv(regime_path, index=False)
        pd.DataFrame(
            [
                {
                    "candidate_id": "momentum|20|5",
                    "benchmark_context_label": "risk_on",
                    "sample_size": 35,
                    "mean_spearman_ic": 0.065,
                }
            ]
        ).to_csv(benchmark_path, index=False)
        pd.DataFrame(
            [
                {
                    "candidate_id": "momentum|20|5",
                    "sub_universe_id": "liquid_trend_candidates",
                    "sample_size": 42,
                    "mean_spearman_ic": 0.07,
                }
            ]
        ).to_csv(sub_universe_path, index=False)
        artifact_paths["signal_performance_by_regime_path"] = regime_path
        artifact_paths["signal_performance_by_benchmark_context_path"] = benchmark_path
        artifact_paths["signal_performance_by_sub_universe_path"] = sub_universe_path

    return write_research_run_manifest(
        output_dir=run_dir,
        workflow_type="alpha_research",
        command="test",
        feature_dir=root / "features",
        signal_family="momentum",
        universe="nasdaq100",
        symbols_requested=["AAPL", "MSFT", "NVDA"],
        lookbacks=[20],
        horizons=[5],
        min_rows=250,
        train_size=756,
        test_size=63,
        step_size=63,
        min_train_size=252,
        artifact_paths=artifact_paths,
    )


def test_manifest_generates_conditional_research_artifacts(tmp_path: Path) -> None:
    manifest_path = _write_conditional_research_run(tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    conditional = manifest["conditional_research"]
    assert conditional["summary"]["row_count"] == 4
    assert conditional["summary"]["eligible_condition_count"] == 3
    assert conditional["promotion_candidates"][0]["eligible"] is True
    assert Path(manifest["artifact_paths"]["conditional_signal_performance_path"]).exists()
    assert Path(manifest["artifact_paths"]["conditional_research_summary_path"]).exists()


def test_promotion_candidates_include_conditional_rows(tmp_path: Path) -> None:
    _write_conditional_research_run(tmp_path)
    result = build_promotion_candidates(artifacts_root=tmp_path, output_dir=tmp_path / "registry")
    payload = json.loads(Path(result["promotion_candidates_json_path"]).read_text(encoding="utf-8"))

    assert payload["conditional_rows"]
    assert payload["conditional_rows"][0]["condition_type"] in {"regime", "sub_universe", "benchmark_context"}
    assert Path(result["conditional_promotion_candidates_csv_path"]).exists()


def test_conditional_promotion_emits_activation_metadata(tmp_path: Path) -> None:
    _write_conditional_research_run(tmp_path)
    registry_dir = tmp_path / "registry"
    build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)

    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "promoted",
        policy=PromotionPolicyConfig(
            max_strategies_total=1,
            enable_conditional_variants=True,
            min_condition_sample_size=20,
            min_condition_improvement=0.01,
        ),
    )

    row = result["promoted_rows"][0]
    assert row["promotion_variant"] == "conditional"
    assert row["condition_id"]
    assert row["activation_conditions"]
    preset_payload = json.loads(Path(row["generated_preset_path"]).read_text(encoding="utf-8"))
    assert preset_payload["decision_context"]["promotion_variant"] == "conditional"
    assert preset_payload["params"]["activation_conditions"]


def test_unconditional_promotion_remains_default_when_conditional_mode_off(tmp_path: Path) -> None:
    _write_conditional_research_run(tmp_path)
    registry_dir = tmp_path / "registry"
    build_research_registry(artifacts_root=tmp_path, output_dir=registry_dir)
    build_promotion_candidates(artifacts_root=tmp_path, output_dir=registry_dir)

    result = apply_research_promotions(
        artifacts_root=tmp_path,
        registry_dir=registry_dir,
        output_dir=tmp_path / "promoted",
        policy=PromotionPolicyConfig(max_strategies_total=1, enable_conditional_variants=False),
    )

    assert result["promoted_rows"][0]["promotion_variant"] == "unconditional"
