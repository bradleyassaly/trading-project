from __future__ import annotations

import json
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

    rows: list[dict[str, Any]] = []
    baseline_row: dict[str, Any] | None = None

    for variant in config.variants:
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

        selected_rows = list(strategy_portfolio_payload.get("selected_strategies", []))
        row = {
            "variant_name": variant.name,
            "is_baseline": variant.name == config.baseline_variant_name,
            "promotion_rerun": promotion_rerun,
            "promoted_strategy_count": len(promoted_rows),
            "selected_strategy_count": len(selected_rows),
            "activation_condition_count": _count_activation_conditions(promoted_rows),
            "portfolio_weighting_mode": strategy_portfolio_payload.get("summary", {}).get("weighting_mode_resolved"),
            "portfolio_max_strategies": effective_strategy_portfolio_policy.max_strategies,
            "promotion_enable_conditional_variants": effective_promotion_policy.enable_conditional_variants,
            "promotion_min_condition_sample_size": effective_promotion_policy.min_condition_sample_size,
            "promotion_min_condition_improvement": effective_promotion_policy.min_condition_improvement,
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
            "promotion_candidates_path": (
                str(registry_bundle.get("promotion_candidates_json_path")) if registry_bundle else None
            ),
            "baseline_bundle_path": str(baseline_bundle_path),
        }
        if row["is_baseline"]:
            baseline_row = row
        rows.append(row)

    if baseline_row is not None:
        for row in rows:
            row["promoted_strategy_count_delta"] = row["promoted_strategy_count"] - baseline_row["promoted_strategy_count"]
            row["selected_strategy_count_delta"] = row["selected_strategy_count"] - baseline_row["selected_strategy_count"]
            row["activation_condition_count_delta"] = (
                row["activation_condition_count"] - baseline_row["activation_condition_count"]
            )
    else:
        for row in rows:
            row["promoted_strategy_count_delta"] = 0
            row["selected_strategy_count_delta"] = 0
            row["activation_condition_count_delta"] = 0

    rows_json_path = output_dir / "experiment_variant_results.json"
    rows_csv_path = output_dir / "experiment_variant_results.csv"
    summary_json_path = output_dir / "experiment_summary.json"
    _write_json(rows_json_path, {"variants": rows})
    pd.DataFrame(rows).to_csv(rows_csv_path, index=False)
    _write_json(
        summary_json_path,
        {
            "generated_at": _now_utc(),
            "baseline_variant_name": config.baseline_variant_name,
            "baseline_bundle_path": str(baseline_bundle_path),
            "baseline_bundle_payload": baseline_bundle_payload,
            "variant_count": len(rows),
            "variants": rows,
            "paths": {
                "experiment_variant_results_json_path": str(rows_json_path),
                "experiment_variant_results_csv_path": str(rows_csv_path),
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
        "experiment_summary_json_path": str(summary_json_path),
    }
