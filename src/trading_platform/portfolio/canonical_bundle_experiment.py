from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.loader import (
    load_multi_strategy_portfolio_config,
    load_pipeline_run_config,
    load_promotion_policy_config,
    load_strategy_portfolio_policy_config,
)
from trading_platform.config.workflow_models import (
    CanonicalBundleExperimentMatrixCaseConfig,
    CanonicalBundleExperimentMatrixWorkflowConfig,
    CanonicalBundleExperimentVariantConfig,
    CanonicalBundleExperimentWorkflowConfig,
)
from trading_platform.orchestration.models import OrchestrationStageToggles, PipelineRunConfig
from trading_platform.portfolio.strategy_portfolio import (
    StrategyPortfolioPolicyConfig,
    build_strategy_portfolio,
    export_strategy_portfolio_run_config,
    load_strategy_portfolio,
)
from trading_platform.research.promotion_pipeline import PromotionPolicyConfig, apply_research_promotions
from trading_platform.research.registry import refresh_research_registry_bundle


_PRESET_SET_NAME = "policy_sensitivity_v1"


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    return path


def _write_variant_config_manifest(
    *,
    variant_dir: Path,
    stage_configs: dict[str, Path],
) -> Path:
    manifest_dir = variant_dir / "config_manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, str]] = []
    for stage_name, source_path in stage_configs.items():
        snapshot_path = manifest_dir / source_path.name
        shutil.copy2(source_path, snapshot_path)
        entries.append(
            {
                "stage": stage_name,
                "source_path": str(source_path),
                "snapshot_path": str(snapshot_path),
                "content_hash": hashlib.sha256(snapshot_path.read_bytes()).hexdigest()[:12],
            }
        )
    manifest_path = manifest_dir / "canonical_config_manifest.json"
    _write_json(
        manifest_path,
        {
            "manifest_type": "canonical_config_manifest",
            "stage_count": len(entries),
            "stages": entries,
        },
    )
    return manifest_path


def _resolve_run_bundle_path(bundle_path_or_dir: str | Path) -> Path:
    path = Path(bundle_path_or_dir)
    if path.is_file():
        return path
    if not path.exists():
        raise FileNotFoundError(f"Bundle path not found: {path}")
    candidates = sorted(path.glob("*_run_bundle.json"))
    if not candidates:
        raise FileNotFoundError(f"No *_run_bundle.json found under {path}")
    return candidates[0]


def _resolve_promotion_policy(
    *,
    base_policy_config_path: str | None,
    overrides: dict[str, Any],
) -> PromotionPolicyConfig:
    base_policy = (
        load_promotion_policy_config(base_policy_config_path)
        if base_policy_config_path
        else PromotionPolicyConfig()
    )
    payload = asdict(base_policy)
    payload.update(overrides)
    return PromotionPolicyConfig(**payload)


def _resolve_strategy_portfolio_policy(
    *,
    base_policy_config_path: str | None,
    overrides: dict[str, Any],
) -> StrategyPortfolioPolicyConfig:
    base_policy = (
        load_strategy_portfolio_policy_config(base_policy_config_path)
        if base_policy_config_path
        else StrategyPortfolioPolicyConfig()
    )
    payload = asdict(base_policy)
    payload.update(overrides)
    return StrategyPortfolioPolicyConfig(**payload)


def _count_activation_conditions(rows: list[dict[str, Any]]) -> int:
    return sum(len(row.get("activation_conditions", []) or []) for row in rows)


def _count_conditional_variants(rows: list[dict[str, Any]]) -> int:
    return sum(
        1
        for row in rows
        if row.get("condition_id") or row.get("condition_type") or row.get("promotion_variant") == "conditional"
    )


def _allocation_weight_map(rows: list[dict[str, Any]]) -> dict[str, float]:
    return {
        str(row.get("preset_name") or ""): float(
            row.get("allocation_weight", row.get("target_capital_fraction", 0.0)) or 0.0
        )
        for row in rows
        if row.get("preset_name")
    }


def _resolve_variants(
    config: CanonicalBundleExperimentWorkflowConfig,
) -> list[CanonicalBundleExperimentVariantConfig]:
    if config.variants:
        return list(config.variants)
    if config.preset_set != _PRESET_SET_NAME:
        raise ValueError(f"Unsupported canonical bundle experiment preset_set: {config.preset_set}")
    return [
        CanonicalBundleExperimentVariantConfig(name="baseline"),
        CanonicalBundleExperimentVariantConfig(
            name="strict_promotion",
            promotion_policy_overrides={
                "enable_conditional_variants": True,
                "min_condition_sample_size": 30,
                "min_condition_improvement": 0.1,
                "compare_condition_to_unconditional": True,
                "max_strategies_total": 2,
                "max_strategies_per_group": 1,
            },
        ),
        CanonicalBundleExperimentVariantConfig(
            name="loose_promotion",
            promotion_policy_overrides={
                "enable_conditional_variants": True,
                "min_condition_sample_size": 0,
                "min_condition_improvement": 0.0,
                "compare_condition_to_unconditional": False,
                "max_strategies_total": 5,
                "max_strategies_per_group": 3,
            },
        ),
        CanonicalBundleExperimentVariantConfig(
            name="alternate_weighting",
            strategy_portfolio_policy_overrides={
                "weighting_mode": "metric_weighted",
                "max_strategies": 3,
            },
        ),
        CanonicalBundleExperimentVariantConfig(
            name="combined_strict_weighting",
            promotion_policy_overrides={
                "enable_conditional_variants": True,
                "min_condition_sample_size": 30,
                "min_condition_improvement": 0.1,
                "compare_condition_to_unconditional": True,
                "max_strategies_total": 2,
                "max_strategies_per_group": 1,
            },
            strategy_portfolio_policy_overrides={
                "weighting_mode": "score_then_cap",
                "max_strategies": 2,
            },
        ),
    ]


def _write_daily_pipeline_config(
    *,
    multi_strategy_config_path: Path,
    output_dir: Path,
    variant_name: str,
) -> Path:
    config = PipelineRunConfig(
        run_name=f"{variant_name}_daily",
        schedule_type="daily",
        universes=["canonical_bundle_experiment"],
        multi_strategy_input_path=str(multi_strategy_config_path),
        paper_state_path=str(output_dir / f"{variant_name}_paper_state.json"),
        output_root_dir=str(output_dir),
        continue_on_stage_error=True,
        stages=OrchestrationStageToggles(
            portfolio_allocation=True,
            paper_trading=True,
            live_dry_run=True,
            reporting=True,
        ),
    )
    return _write_json(output_dir / "daily_pipeline_config.json", config.to_dict())


def _validate_multi_strategy_readiness(
    *,
    multi_strategy_config_path: Path,
    pipeline_config_path: Path,
    daily_pipeline_config_path: Path,
) -> tuple[bool, bool]:
    portfolio_config = load_multi_strategy_portfolio_config(multi_strategy_config_path)
    pipeline_config = load_pipeline_run_config(pipeline_config_path)
    daily_pipeline_config = load_pipeline_run_config(daily_pipeline_config_path)
    preset_paths_ready = all(
        Path(str(sleeve.preset_path)).exists()
        for sleeve in portfolio_config.sleeves
        if sleeve.preset_path
    )
    paper_ready = bool(
        portfolio_config.sleeves
        and preset_paths_ready
        and pipeline_config.stages.paper_trading
        and daily_pipeline_config.stages.paper_trading
    )
    live_ready = bool(
        portfolio_config.sleeves
        and preset_paths_ready
        and daily_pipeline_config.stages.live_dry_run
    )
    return paper_ready, live_ready


def _prepare_promoted_dir(
    *,
    config: CanonicalBundleExperimentWorkflowConfig,
    variant: CanonicalBundleExperimentVariantConfig,
    variant_dir: Path,
    effective_promotion_policy: PromotionPolicyConfig,
) -> tuple[Path, bool, dict[str, Any] | None]:
    needs_promotion_rerun = bool(config.base_promotion_policy_config or variant.promotion_policy_overrides)
    if not needs_promotion_rerun:
        return Path(config.promoted_dir), False, None
    if not config.artifacts_root:
        raise ValueError(
            "artifacts_root is required when promotion_policy_overrides or base_promotion_policy_config are provided"
        )
    promoted_dir = variant_dir / "promoted"
    registry_dir = variant_dir / "research_registry"
    registry_bundle = refresh_research_registry_bundle(
        artifacts_root=Path(config.artifacts_root),
        output_dir=registry_dir,
    )
    apply_research_promotions(
        artifacts_root=Path(config.artifacts_root),
        registry_dir=registry_dir,
        output_dir=promoted_dir,
        policy=effective_promotion_policy,
        top_n=None,
        allow_overwrite=True,
        dry_run=False,
        inactive=False,
        validation_path=None,
        override_validation=False,
    )
    return promoted_dir, True, registry_bundle


def run_canonical_bundle_experiment(
    config: CanonicalBundleExperimentWorkflowConfig,
) -> dict[str, Any]:
    baseline_bundle_path = _resolve_run_bundle_path(config.bundle_dir)
    baseline_bundle_payload = _safe_read_json(baseline_bundle_path)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    variants = _resolve_variants(config)

    rows: list[dict[str, Any]] = []
    baseline_row: dict[str, Any] | None = None
    baseline_allocation_map: dict[str, float] | None = None

    for variant in variants:
        variant_dir = output_dir / variant.name
        variant_dir.mkdir(parents=True, exist_ok=True)

        effective_promotion_policy = _resolve_promotion_policy(
            base_policy_config_path=config.base_promotion_policy_config,
            overrides=variant.promotion_policy_overrides,
        )
        effective_strategy_portfolio_policy = _resolve_strategy_portfolio_policy(
            base_policy_config_path=config.base_strategy_portfolio_policy_config,
            overrides=variant.strategy_portfolio_policy_overrides,
        )

        effective_promotion_policy_path = _write_json(
            variant_dir / "effective_promotion_policy.json",
            asdict(effective_promotion_policy),
        )
        effective_strategy_portfolio_policy_path = _write_json(
            variant_dir / "effective_strategy_portfolio_policy.json",
            asdict(effective_strategy_portfolio_policy),
        )

        promoted_dir, promotion_rerun, registry_bundle = _prepare_promoted_dir(
            config=config,
            variant=variant,
            variant_dir=variant_dir,
            effective_promotion_policy=effective_promotion_policy,
        )
        promoted_payload = _safe_read_json(promoted_dir / "promoted_strategies.json")
        promoted_rows = list(promoted_payload.get("strategies", []))

        strategy_portfolio_dir = variant_dir / "strategy_portfolio"
        build_strategy_portfolio(
            promoted_dir=promoted_dir,
            output_dir=strategy_portfolio_dir,
            policy=effective_strategy_portfolio_policy,
            lifecycle_path=Path(config.lifecycle) if config.lifecycle else None,
        )
        strategy_portfolio_payload = load_strategy_portfolio(strategy_portfolio_dir)

        run_bundle_dir = variant_dir / "run_bundle"
        export_paths = export_strategy_portfolio_run_config(
            strategy_portfolio_path=strategy_portfolio_dir,
            output_dir=run_bundle_dir,
        )
        multi_strategy_config_path = Path(export_paths["multi_strategy_config_path"])
        pipeline_config_path = Path(export_paths["pipeline_config_path"])
        run_bundle_path = Path(export_paths["run_bundle_path"])
        daily_pipeline_config_path = _write_daily_pipeline_config(
            multi_strategy_config_path=multi_strategy_config_path,
            output_dir=run_bundle_dir,
            variant_name=variant.name,
        )
        paper_ready, live_ready = _validate_multi_strategy_readiness(
            multi_strategy_config_path=multi_strategy_config_path,
            pipeline_config_path=pipeline_config_path,
            daily_pipeline_config_path=daily_pipeline_config_path,
        )
        config_manifest_path = _write_variant_config_manifest(
            variant_dir=variant_dir,
            stage_configs={
                "promotion_policy": effective_promotion_policy_path,
                "strategy_portfolio_policy": effective_strategy_portfolio_policy_path,
                "daily_pipeline_config": daily_pipeline_config_path,
            },
        )

        selected_rows = list(strategy_portfolio_payload.get("selected_strategies", []))
        summary = strategy_portfolio_payload.get("summary", {})
        family_weights = summary.get("signal_family_weights", {}) or {}
        allocation_map = _allocation_weight_map(selected_rows)
        row = {
            "variant_name": variant.name,
            "is_baseline": variant.name == config.baseline_variant_name,
            "promotion_rerun": promotion_rerun,
            "promoted_strategy_count": len(promoted_rows),
            "selected_strategy_count": len(selected_rows),
            "conditional_variant_count": _count_conditional_variants(promoted_rows),
            "activation_condition_count": _count_activation_conditions(promoted_rows),
            "portfolio_weighting_mode": summary.get("weighting_mode_resolved"),
            "portfolio_max_strategies": effective_strategy_portfolio_policy.max_strategies,
            "signal_family_count": len(family_weights),
            "signal_family_weight_summary": json.dumps(family_weights, sort_keys=True),
            "selected_preset_names": json.dumps(sorted(allocation_map.keys())),
            "total_active_weight": float(summary.get("total_active_weight", 0.0) or 0.0),
            "max_strategy_weight": float(summary.get("max_strategy_weight", 0.0) or 0.0),
            "max_family_weight": float(summary.get("max_family_weight", 0.0) or 0.0),
            "effective_strategy_count": float(summary.get("effective_strategy_count", 0.0) or 0.0),
            "effective_family_count": float(summary.get("effective_family_count", 0.0) or 0.0),
            "promotion_enable_conditional_variants": effective_promotion_policy.enable_conditional_variants,
            "promotion_min_condition_sample_size": effective_promotion_policy.min_condition_sample_size,
            "promotion_min_condition_improvement": effective_promotion_policy.min_condition_improvement,
            "promotion_compare_condition_to_unconditional": effective_promotion_policy.compare_condition_to_unconditional,
            "promotion_max_strategies_total": effective_promotion_policy.max_strategies_total,
            "promotion_max_strategies_per_group": effective_promotion_policy.max_strategies_per_group,
            "paper_ready": paper_ready,
            "live_ready": live_ready,
            "promoted_dir": str(promoted_dir),
            "strategy_portfolio_json_path": str(strategy_portfolio_dir / "strategy_portfolio.json"),
            "multi_strategy_config_path": str(multi_strategy_config_path),
            "pipeline_config_path": str(pipeline_config_path),
            "daily_pipeline_config_path": str(daily_pipeline_config_path),
            "run_bundle_path": str(run_bundle_path),
            "effective_promotion_policy_path": str(effective_promotion_policy_path),
            "effective_strategy_portfolio_policy_path": str(effective_strategy_portfolio_policy_path),
            "config_manifest_path": str(config_manifest_path),
            "promotion_candidates_path": (
                str(registry_bundle.get("promotion_candidates_json_path")) if registry_bundle else None
            ),
            "baseline_bundle_path": str(baseline_bundle_path),
        }
        if row["is_baseline"]:
            baseline_row = row
            baseline_allocation_map = allocation_map
        rows.append(row)

    if baseline_row is not None:
        for row in rows:
            row["promoted_strategy_count_delta"] = row["promoted_strategy_count"] - baseline_row["promoted_strategy_count"]
            row["selected_strategy_count_delta"] = row["selected_strategy_count"] - baseline_row["selected_strategy_count"]
            row["conditional_variant_count_delta"] = row["conditional_variant_count"] - baseline_row["conditional_variant_count"]
            row["activation_condition_count_delta"] = (
                row["activation_condition_count"] - baseline_row["activation_condition_count"]
            )
            row["max_strategy_weight_delta"] = row["max_strategy_weight"] - baseline_row["max_strategy_weight"]
            row["effective_strategy_count_delta"] = row["effective_strategy_count"] - baseline_row["effective_strategy_count"]
            row["signal_family_count_delta"] = row["signal_family_count"] - baseline_row["signal_family_count"]
            current_allocation_map = json.loads(row["selected_preset_names"])
            allocation_names = set(current_allocation_map)
            baseline_names = set(baseline_allocation_map or {})
            union_names = allocation_names | baseline_names
            current_portfolio_payload = load_strategy_portfolio(Path(row["strategy_portfolio_json_path"]).parent)
            current_map = _allocation_weight_map(list(current_portfolio_payload.get("selected_strategies", [])))
            baseline_map = baseline_allocation_map or {}
            row["allocation_l1_delta_vs_baseline"] = float(
                sum(abs(current_map.get(name, 0.0) - baseline_map.get(name, 0.0)) for name in union_names)
            )
    else:
        for row in rows:
            row["promoted_strategy_count_delta"] = 0
            row["selected_strategy_count_delta"] = 0
            row["conditional_variant_count_delta"] = 0
            row["activation_condition_count_delta"] = 0
            row["max_strategy_weight_delta"] = 0.0
            row["effective_strategy_count_delta"] = 0.0
            row["signal_family_count_delta"] = 0
            row["allocation_l1_delta_vs_baseline"] = 0.0

    rows_json_path = output_dir / "experiment_variant_results.json"
    rows_csv_path = output_dir / "experiment_variant_results.csv"
    comparison_csv_path = output_dir / "experiment_policy_comparison.csv"
    summary_json_path = output_dir / "experiment_summary.json"
    _write_json(rows_json_path, {"variants": rows})
    pd.DataFrame(rows).to_csv(rows_csv_path, index=False)
    comparison_columns = [
        "variant_name",
        "is_baseline",
        "promoted_strategy_count",
        "promoted_strategy_count_delta",
        "conditional_variant_count",
        "conditional_variant_count_delta",
        "activation_condition_count",
        "activation_condition_count_delta",
        "signal_family_count",
        "signal_family_count_delta",
        "selected_strategy_count",
        "selected_strategy_count_delta",
        "portfolio_weighting_mode",
        "allocation_l1_delta_vs_baseline",
        "max_strategy_weight",
        "max_strategy_weight_delta",
        "effective_strategy_count",
        "effective_strategy_count_delta",
        "promotion_enable_conditional_variants",
        "promotion_min_condition_sample_size",
        "promotion_min_condition_improvement",
        "promotion_compare_condition_to_unconditional",
        "paper_ready",
        "live_ready",
        "run_bundle_path",
    ]
    pd.DataFrame(rows)[comparison_columns].to_csv(comparison_csv_path, index=False)
    _write_json(
        summary_json_path,
        {
            "generated_at": _now_utc(),
            "baseline_variant_name": config.baseline_variant_name,
            "preset_set": config.preset_set,
            "baseline_bundle_path": str(baseline_bundle_path),
            "baseline_bundle_payload": baseline_bundle_payload,
            "variant_count": len(rows),
            "variants": rows,
            "paths": {
                "experiment_variant_results_json_path": str(rows_json_path),
                "experiment_variant_results_csv_path": str(rows_csv_path),
                "experiment_policy_comparison_csv_path": str(comparison_csv_path),
                "experiment_summary_json_path": str(summary_json_path),
            },
        },
    )
    return {
        "baseline_bundle_path": str(baseline_bundle_path),
        "output_dir": str(output_dir),
        "variant_rows": rows,
        "experiment_variant_results_json_path": str(rows_json_path),
        "experiment_variant_results_csv_path": str(rows_csv_path),
        "experiment_policy_comparison_csv_path": str(comparison_csv_path),
        "experiment_summary_json_path": str(summary_json_path),
    }


def _build_case_experiment_config(
    *,
    matrix_config: CanonicalBundleExperimentMatrixWorkflowConfig,
    case: CanonicalBundleExperimentMatrixCaseConfig,
    output_dir: Path,
) -> CanonicalBundleExperimentWorkflowConfig:
    return CanonicalBundleExperimentWorkflowConfig(
        bundle_dir=case.bundle_dir,
        promoted_dir=case.promoted_dir,
        output_dir=str(output_dir),
        artifacts_root=case.artifacts_root,
        lifecycle=case.lifecycle,
        base_promotion_policy_config=matrix_config.base_promotion_policy_config,
        base_strategy_portfolio_policy_config=matrix_config.base_strategy_portfolio_policy_config,
        baseline_variant_name=matrix_config.baseline_variant_name,
        preset_set=matrix_config.preset_set,
    )


def _float_series(values: list[Any]) -> pd.Series:
    return pd.to_numeric(pd.Series(values, dtype="object"), errors="coerce").fillna(0.0)


def run_canonical_bundle_experiment_matrix(
    config: CanonicalBundleExperimentMatrixWorkflowConfig,
) -> dict[str, Any]:
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    case_rows: list[dict[str, Any]] = []
    case_results: list[dict[str, Any]] = []
    for case in config.cases:
        case_output_dir = output_dir / case.case_id
        result = run_canonical_bundle_experiment(
            _build_case_experiment_config(
                matrix_config=config,
                case=case,
                output_dir=case_output_dir,
            )
        )
        case_results.append(
            {
                "case_id": case.case_id,
                "label": case.label or case.case_id,
                "output_dir": str(case_output_dir),
                "summary_json_path": result["experiment_summary_json_path"],
                "policy_comparison_csv_path": result["experiment_policy_comparison_csv_path"],
            }
        )
        for row in result["variant_rows"]:
            case_rows.append(
                {
                    "case_id": case.case_id,
                    "case_label": case.label or case.case_id,
                    **row,
                }
            )

    case_results_json_path = output_dir / "bundle_case_results.json"
    stability_csv_path = output_dir / "experiment_time_stability.csv"
    stability_json_path = output_dir / "experiment_time_stability.json"
    stability_summary_json_path = output_dir / "bundle_policy_stability_summary.json"

    case_df = pd.DataFrame(case_rows)
    case_df.to_csv(stability_csv_path, index=False)
    _write_json(stability_json_path, {"rows": case_rows})

    stability_rows: list[dict[str, Any]] = []
    for variant_name, group in case_df.groupby("variant_name", dropna=False):
        promoted_series = _float_series(group["promoted_strategy_count"].tolist())
        conditional_series = _float_series(group["conditional_variant_count"].tolist())
        selected_series = _float_series(group["selected_strategy_count"].tolist())
        effective_series = _float_series(group["effective_strategy_count"].tolist())
        weight_series = _float_series(group["max_strategy_weight"].tolist())
        allocation_series = _float_series(group["allocation_l1_delta_vs_baseline"].tolist())
        readiness_paper = int(group["paper_ready"].astype(bool).sum())
        readiness_live = int(group["live_ready"].astype(bool).sum())
        stability_rows.append(
            {
                "variant_name": variant_name,
                "case_count": int(len(group.index)),
                "paper_ready_pass_count": readiness_paper,
                "live_ready_pass_count": readiness_live,
                "promoted_strategy_count_mean": float(promoted_series.mean()),
                "promoted_strategy_count_min": float(promoted_series.min()),
                "promoted_strategy_count_max": float(promoted_series.max()),
                "promoted_strategy_count_range": float(promoted_series.max() - promoted_series.min()),
                "conditional_variant_count_mean": float(conditional_series.mean()),
                "conditional_variant_count_range": float(conditional_series.max() - conditional_series.min()),
                "selected_strategy_count_mean": float(selected_series.mean()),
                "selected_strategy_count_range": float(selected_series.max() - selected_series.min()),
                "effective_strategy_count_mean": float(effective_series.mean()),
                "effective_strategy_count_range": float(effective_series.max() - effective_series.min()),
                "max_strategy_weight_mean": float(weight_series.mean()),
                "max_strategy_weight_range": float(weight_series.max() - weight_series.min()),
                "allocation_l1_delta_vs_baseline_mean": float(allocation_series.mean()),
                "allocation_l1_delta_vs_baseline_range": float(allocation_series.max() - allocation_series.min()),
                "material_divergence_case_count": int((allocation_series > 0.25).sum()),
            }
        )

    _write_json(case_results_json_path, {"cases": case_results})
    _write_json(stability_json_path, {"rows": case_rows})
    pd.DataFrame(case_rows).to_csv(stability_csv_path, index=False)
    _write_json(
        stability_summary_json_path,
        {
            "generated_at": _now_utc(),
            "experiment_name": config.experiment_name,
            "preset_set": config.preset_set,
            "case_count": len(config.cases),
            "variant_count": len(stability_rows),
            "cases": case_results,
            "variant_stability": stability_rows,
            "paths": {
                "bundle_case_results_json_path": str(case_results_json_path),
                "experiment_time_stability_csv_path": str(stability_csv_path),
                "experiment_time_stability_json_path": str(stability_json_path),
                "bundle_policy_stability_summary_json_path": str(stability_summary_json_path),
            },
        },
    )
    return {
        "output_dir": str(output_dir),
        "case_rows": case_rows,
        "variant_stability": stability_rows,
        "bundle_case_results_json_path": str(case_results_json_path),
        "experiment_time_stability_csv_path": str(stability_csv_path),
        "experiment_time_stability_json_path": str(stability_json_path),
        "bundle_policy_stability_summary_json_path": str(stability_summary_json_path),
    }
