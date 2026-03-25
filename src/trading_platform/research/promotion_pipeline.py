from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from trading_platform.governance.models import (
    StrategyRegistry,
    StrategyRegistryAuditEvent,
    StrategyRegistryEntry,
)
from trading_platform.governance.persistence import save_strategy_registry
from trading_platform.orchestration.models import OrchestrationStageToggles, PipelineRunConfig
from trading_platform.regime.service import infer_strategy_regime_compatibility
from trading_platform.research.registry import load_research_manifests

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


PROMOTION_POLICY_SCHEMA_VERSION = 1
PROMOTED_STRATEGIES_INDEX_NAME = "promoted_strategies.json"


@dataclass(frozen=True)
class PromotionPolicyConfig:
    schema_version: int = PROMOTION_POLICY_SCHEMA_VERSION
    metric_name: str = "portfolio_sharpe"
    min_metric_threshold: float = 0.5
    min_folds_tested: int = 3
    min_promoted_signals: int = 1
    require_validation_pass: bool = True
    allow_weak_validation: bool = False
    max_strategies_total: int | None = 5
    max_strategies_per_group: int | None = 1
    max_strategies_per_family: int | None = None
    min_families_if_available: int = 0
    group_by: str = "signal_family"
    require_eligible_candidates: bool = True
    default_status: str = "inactive"
    pipeline_monitoring_config_path: str | None = "configs/monitoring.yaml"
    pipeline_execution_config_path: str | None = "configs/execution.yaml"
    enable_conditional_variants: bool = False
    emit_conditional_variants_alongside_baseline: bool = False
    conditional_variant_allowance: int = 0
    conditional_variant_score_bonus: float = 0.0
    allowed_condition_types: list[str] = field(default_factory=lambda: ["regime", "sub_universe", "benchmark_context"])
    min_condition_sample_size: int = 20
    min_condition_improvement: float = 0.0
    compare_condition_to_unconditional: bool = True
    notes: str | None = None
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.schema_version != PROMOTION_POLICY_SCHEMA_VERSION:
            raise ValueError(f"Unsupported promotion policy schema_version: {self.schema_version}")
        if self.min_folds_tested < 0:
            raise ValueError("min_folds_tested must be >= 0")
        if self.min_promoted_signals < 0:
            raise ValueError("min_promoted_signals must be >= 0")
        if self.max_strategies_total is not None and self.max_strategies_total <= 0:
            raise ValueError("max_strategies_total must be > 0 when provided")
        if self.max_strategies_per_group is not None and self.max_strategies_per_group <= 0:
            raise ValueError("max_strategies_per_group must be > 0 when provided")
        if self.max_strategies_per_family is not None and self.max_strategies_per_family <= 0:
            raise ValueError("max_strategies_per_family must be > 0 when provided")
        if self.min_families_if_available < 0:
            raise ValueError("min_families_if_available must be >= 0")
        if self.conditional_variant_allowance < 0:
            raise ValueError("conditional_variant_allowance must be >= 0")
        if self.min_condition_sample_size < 0:
            raise ValueError("min_condition_sample_size must be >= 0")
        if self.group_by not in {"signal_family", "universe", "workflow_type"}:
            raise ValueError("group_by must be one of: signal_family, universe, workflow_type")
        if self.default_status not in {"active", "inactive"}:
            raise ValueError("default_status must be one of: active, inactive")


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    number = _safe_float(value)
    return int(number) if number is not None else None


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists() or file_path.is_dir():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _write_payload(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".json":
        path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
        return path
    if suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML files. Install with `pip install pyyaml`.")
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return path
    raise ValueError(f"Unsupported file type: {suffix}")


def load_promotion_policy_config(path: str | Path) -> PromotionPolicyConfig:
    file_path = Path(path)
    suffix = file_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    elif suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise ImportError("PyYAML is required for YAML files. Install with `pip install pyyaml`.")
        payload = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
    else:
        raise ValueError(f"Unsupported file type: {suffix}")
    return PromotionPolicyConfig(**payload)


def _sanitize_name(text: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", text.lower()).strip("_")


def _build_generated_preset_name(manifest: dict[str, Any], conditional_variant: dict[str, Any] | None = None) -> str:
    signal_family = _sanitize_name(str(manifest.get("signal_family") or "signal"))
    universe = _sanitize_name(str(manifest.get("universe") or "custom"))
    run_id = _sanitize_name(str(manifest.get("run_id") or "run"))
    if conditional_variant is None:
        return f"generated_{signal_family}_{universe}_{run_id}_paper"
    suffix = _sanitize_name(str(conditional_variant.get("condition_id") or conditional_variant.get("condition_type") or "condition"))
    return f"generated_{signal_family}_{universe}_{run_id}_{suffix}_paper"


def _load_promoted_index(output_dir: Path) -> dict[str, Any]:
    path = output_dir / PROMOTED_STRATEGIES_INDEX_NAME
    payload = _safe_read_json(path)
    if not payload:
        return {"schema_version": 1, "generated_at": None, "strategies": []}
    payload.setdefault("schema_version", 1)
    payload.setdefault("strategies", [])
    return payload


def _build_warning_list(manifest: dict[str, Any], policy: PromotionPolicyConfig) -> list[str]:
    warnings: list[str] = []
    folds = _safe_int(manifest.get("folds_tested"))
    if folds is None or folds <= policy.min_folds_tested:
        warnings.append("low_sample_size_or_missing_folds")
    metric_value = _safe_float(manifest.get("top_metrics", {}).get(policy.metric_name))
    if metric_value is None:
        warnings.append(f"missing_metric_{policy.metric_name}")
    turnover = _safe_float(manifest.get("top_metrics", {}).get("mean_turnover"))
    if turnover is not None and turnover > 0.5:
        warnings.append("high_turnover_proxy")
    if manifest.get("top_metrics", {}).get("rejection_reason"):
        warnings.append("candidate_has_rejection_reason")
    return warnings


def _candidate_passes_policy(candidate: dict[str, Any], manifest: dict[str, Any], policy: PromotionPolicyConfig) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    metric_value = _safe_float(manifest.get("top_metrics", {}).get(policy.metric_name))
    if policy.require_eligible_candidates and not bool(candidate.get("eligible")):
        reasons.append("candidate_not_marked_eligible")
    if metric_value is None or metric_value < policy.min_metric_threshold:
        reasons.append(
            f"{policy.metric_name} {metric_value if metric_value is not None else 'missing'} < {policy.min_metric_threshold}"
        )
    folds = _safe_int(manifest.get("folds_tested"))
    if folds is None or folds < policy.min_folds_tested:
        reasons.append(f"folds_tested {folds if folds is not None else 'missing'} < {policy.min_folds_tested}")
    promoted_signals = _safe_int(manifest.get("promoted_signal_count"))
    if promoted_signals is None or promoted_signals < policy.min_promoted_signals:
        reasons.append(
            f"promoted_signal_count {promoted_signals if promoted_signals is not None else 'missing'} < {policy.min_promoted_signals}"
        )
    return (not reasons), reasons


def _load_validation_rows(path: str | Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    file_path = Path(path)
    if file_path.is_dir():
        file_path = file_path / "strategy_validation.json"
    payload = _safe_read_json(file_path)
    return {
        str(row.get("run_id")): row
        for row in payload.get("rows", [])
        if row.get("run_id")
    }


def _build_generated_preset_payload(
    *,
    preset_name: str,
    manifest: dict[str, Any],
    candidate: dict[str, Any],
    status: str,
    warnings: list[str],
    metric_name: str,
    conditional_variant: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_paths = manifest.get("artifact_paths", {})
    approved_model_state = artifact_paths.get("approved_model_state_deployment_path") or artifact_paths.get("approved_model_state_path")
    composite_settings = manifest.get("diagnostics_snapshot", {}).get("composite_portfolio", {})
    regime_compatibility = infer_strategy_regime_compatibility(
        signal_family=str(manifest.get("signal_family") or ""),
        strategy_name="composite_alpha",
    )
    horizon = manifest.get("top_candidate", {}).get("horizon") or (
        (manifest.get("evaluation_periods", {}).get("horizons") or [1])[0]
    )
    params = {
        "preset_name": preset_name,
        "signal_source": "composite",
        "strategy": "sma_cross",
        "symbols": manifest.get("symbols_requested"),
        "approved_model_state": approved_model_state,
        "composite_artifact_dir": manifest.get("artifact_dir"),
        "composite_horizon": int(horizon) if horizon is not None else 1,
        "composite_weighting_scheme": "equal",
        "composite_portfolio_mode": "long_only_top_n",
        "rebalance_frequency": "daily",
        "min_price": composite_settings.get("min_price"),
        "min_volume": composite_settings.get("min_volume"),
        "min_avg_dollar_volume": composite_settings.get("min_avg_dollar_volume"),
        "max_adv_participation": composite_settings.get("max_adv_participation", 0.05),
        "max_position_pct_of_adv": composite_settings.get("max_position_pct_of_adv", 0.1),
        "max_notional_per_name": composite_settings.get("max_notional_per_name"),
        "activation_conditions": [conditional_variant.get("activation_condition")] if conditional_variant else [],
    }
    params = {key: value for key, value in params.items() if value is not None}
    return {
        "schema_version": 1,
        "preset_type": "generated_strategy_preset",
        "name": preset_name,
        "description": f"Generated composite paper preset promoted from research run {manifest.get('run_id')}.",
        "params": params,
        "decision_context": {
            "source_run_id": manifest.get("run_id"),
            "signal_family": manifest.get("signal_family"),
            "universe": manifest.get("universe"),
            "status": status,
            "ranking_metric": metric_name,
            "ranking_value": manifest.get("top_metrics", {}).get(metric_name),
            "promotion_recommendation": candidate.get("promotion_recommendation"),
            "reasons": [part.strip() for part in str(candidate.get("reasons") or "").split(";") if part.strip()],
            "warnings": warnings,
            "regime_compatibility": regime_compatibility,
            "promotion_variant": "conditional" if conditional_variant else "unconditional",
            "activation_condition": conditional_variant.get("activation_condition") if conditional_variant else None,
            "conditional_promotion_summary": conditional_variant.get("promotion_summary") if conditional_variant else None,
            "artifact_dir": manifest.get("artifact_dir"),
            "approved_model_state_path": approved_model_state,
        },
    }


def _build_registry_entry(
    *,
    preset_name: str,
    manifest: dict[str, Any],
    status: str,
    promoted_strategy_path: Path,
    conditional_variant: dict[str, Any] | None = None,
) -> StrategyRegistryEntry:
    pipeline_stage = "paper" if status == "active" else "candidate"
    return StrategyRegistryEntry(
        strategy_id=preset_name,
        strategy_name=preset_name,
        family=str(manifest.get("signal_family") or "generated"),
        version=str(manifest.get("timestamp") or _now_utc())[:10].replace("-", ""),
        preset_name=preset_name,
        research_artifact_paths=[str(manifest.get("artifact_dir"))],
        created_at=_now_utc(),
        status="paper",
        owner="research_promotion",
        source="research_promote_cli",
        current_deployment_stage=pipeline_stage,
        notes=f"Promoted from research run {manifest.get('run_id')}",
        tags=["generated", "research_promotion"],
        universe=manifest.get("universe"),
        signal_type="composite_alpha",
        rebalance_frequency="daily",
        benchmark="equal_weight",
        risk_profile="paper_candidate",
        metadata={
            "source_run_id": manifest.get("run_id"),
            "generated_strategy_path": str(promoted_strategy_path),
            "activation_condition": conditional_variant.get("activation_condition") if conditional_variant else None,
            "promotion_variant": "conditional" if conditional_variant else "unconditional",
        },
    )


def _build_pipeline_config(
    *,
    preset_name: str,
    manifest: dict[str, Any],
    output_dir: Path,
    registry_path: Path,
    policy: PromotionPolicyConfig,
) -> PipelineRunConfig:
    universe = manifest.get("universe") or "nasdaq100"
    return PipelineRunConfig(
        run_name=f"paper_promotion_{preset_name}",
        schedule_type="ad_hoc",
        universes=[str(universe)],
        preset_filters=[preset_name],
        registry_path=str(registry_path),
        monitoring_config_path=policy.pipeline_monitoring_config_path,
        execution_config_path=policy.pipeline_execution_config_path,
        multi_strategy_output_path=str(output_dir / f"{preset_name}_multi_strategy.json"),
        paper_state_path=str(Path("artifacts/paper/promoted") / f"{preset_name}_state.json"),
        output_root_dir="artifacts/orchestration",
        continue_on_stage_error=True,
        registry_include_paper_strategies=True,
        registry_selection_weighting_scheme="equal",
        registry_max_strategies=1,
        stages=OrchestrationStageToggles(
            multi_strategy_config_generation=True,
            portfolio_allocation=True,
            paper_trading=True,
            reporting=True,
            monitoring=bool(policy.pipeline_monitoring_config_path),
        ),
    )


def _serialize_pipeline_config(config: PipelineRunConfig) -> dict[str, Any]:
    return config.to_dict()


def _select_conditional_variant(manifest: dict[str, Any], policy: PromotionPolicyConfig) -> dict[str, Any] | None:
    if not policy.enable_conditional_variants:
        return None
    allowed_types = set(policy.allowed_condition_types or [])
    candidates = list(manifest.get("conditional_research", {}).get("promotion_candidates", []))
    filtered = [
        row
        for row in candidates
        if row.get("eligible")
        and (not allowed_types or str(row.get("condition_type") or "") in allowed_types)
        and (_safe_int(row.get("sample_size")) or 0) >= policy.min_condition_sample_size
        and (
            not policy.compare_condition_to_unconditional
            or (_safe_float(row.get("improvement_vs_baseline")) or float("-inf")) >= policy.min_condition_improvement
        )
    ]
    if not filtered:
        return None
    return sorted(
        filtered,
        key=lambda row: (
            _safe_float(row.get("improvement_vs_baseline")) or float("-inf"),
            _safe_int(row.get("sample_size")) or -1,
            str(row.get("condition_id") or ""),
        ),
        reverse=True,
    )[0]


def _manifest_metric_value(manifest: dict[str, Any], policy: PromotionPolicyConfig) -> float:
    metric_value = _safe_float(manifest.get("top_metrics", {}).get(policy.metric_name))
    return metric_value if metric_value is not None else float("-inf")


def _promotion_group_value(manifest: dict[str, Any], policy: PromotionPolicyConfig) -> str:
    return str(manifest.get(policy.group_by) or "ungrouped")


def _promotion_family_value(manifest: dict[str, Any]) -> str:
    return str(manifest.get("signal_family") or "ungrouped")


def _load_candidate_rows(registry_dir: Path) -> list[dict[str, Any]]:
    payload = _safe_read_json(registry_dir / "promotion_candidates.json")
    return list(payload.get("rows", []))


def apply_research_promotions(
    *,
    artifacts_root: str | Path,
    registry_dir: str | Path,
    output_dir: str | Path,
    policy: PromotionPolicyConfig,
    validation_path: str | Path | None = None,
    top_n: int | None = None,
    allow_overwrite: bool = False,
    dry_run: bool = False,
    inactive: bool = False,
    override_validation: bool = False,
) -> dict[str, Any]:
    artifacts_root_path = Path(artifacts_root)
    registry_dir_path = Path(registry_dir)
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    manifests = {manifest.get("run_id"): manifest for manifest in load_research_manifests(artifacts_root_path)}
    candidates = _load_candidate_rows(registry_dir_path)
    explicit_validation_requested = validation_path is not None
    validation_lookup = _load_validation_rows(validation_path or (registry_dir_path / "strategy_validation.json"))
    existing_index = _load_promoted_index(output_dir_path)
    existing_run_ids = {row.get("source_run_id") for row in existing_index.get("strategies", [])}
    existing_preset_names = {row.get("preset_name") for row in existing_index.get("strategies", [])}
    selected: list[dict[str, Any]] = []
    group_counts: dict[str, int] = {}
    family_counts: dict[str, int] = {}

    prepared_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        run_id = str(candidate.get("run_id") or "")
        manifest = manifests.get(run_id)
        if not manifest:
            continue
        passes, reasons = _candidate_passes_policy(candidate, manifest, policy)
        if not passes:
            continue
        validation_row = validation_lookup.get(run_id)
        validation_status = str(validation_row.get("validation_status") or "") if validation_row else ""
        if policy.require_validation_pass and not override_validation and (validation_lookup or explicit_validation_requested):
            if not validation_row:
                continue
            if validation_status != "pass" and not (policy.allow_weak_validation and validation_status == "weak"):
                continue
        if run_id in existing_run_ids and not allow_overwrite:
            continue
        conditional_variant = _select_conditional_variant(manifest, policy)
        effective_metric = _manifest_metric_value(manifest, policy) + (
            policy.conditional_variant_score_bonus if conditional_variant is not None else 0.0
        )
        prepared_candidates.append(
            {
                "candidate": candidate,
                "manifest": manifest,
                "reasons": reasons,
                "validation_row": validation_row,
                "conditional_variant": conditional_variant,
                "effective_metric": effective_metric,
                "group_value": _promotion_group_value(manifest, policy),
                "family_value": _promotion_family_value(manifest),
            }
        )

    sorted_candidates = sorted(
        prepared_candidates,
        key=lambda row: (
            float(row["effective_metric"]),
            1 if row["conditional_variant"] is not None else 0,
            str(row["candidate"].get("timestamp") or ""),
            str(row["manifest"].get("run_id") or ""),
        ),
        reverse=True,
    )

    total_limit = top_n if top_n is not None else policy.max_strategies_total
    distinct_families_available = {
        str(row["family_value"])
        for row in sorted_candidates
        if str(row["family_value"] or "").strip()
    }
    target_min_families = min(
        policy.min_families_if_available,
        len(distinct_families_available),
        total_limit if total_limit is not None else len(distinct_families_available),
    )

    selected_run_ids: set[str] = set()

    def can_select(prepared: dict[str, Any]) -> bool:
        run_id = str(prepared["manifest"].get("run_id") or "")
        if run_id in selected_run_ids:
            return False
        group_value = str(prepared["group_value"])
        family_value = str(prepared["family_value"])
        if policy.max_strategies_per_group is not None and group_counts.get(group_value, 0) >= policy.max_strategies_per_group:
            return False
        if (
            policy.max_strategies_per_family is not None
            and family_value
            and family_counts.get(family_value, 0) >= policy.max_strategies_per_family
        ):
            return False
        return True

    def add_selected(prepared: dict[str, Any]) -> None:
        selected.append(prepared)
        run_id = str(prepared["manifest"].get("run_id") or "")
        if run_id:
            selected_run_ids.add(run_id)
        group_value = str(prepared["group_value"])
        family_value = str(prepared["family_value"])
        group_counts[group_value] = group_counts.get(group_value, 0) + 1
        if family_value:
            family_counts[family_value] = family_counts.get(family_value, 0) + 1

    if target_min_families > 0:
        chosen_families: set[str] = set()
        for prepared in sorted_candidates:
            if total_limit is not None and len(selected) >= total_limit:
                break
            family_value = str(prepared["family_value"])
            if not family_value or family_value in chosen_families:
                continue
            if not can_select(prepared):
                continue
            add_selected(prepared)
            chosen_families.add(family_value)
            if len(chosen_families) >= target_min_families:
                break

    for prepared in sorted_candidates:
        if total_limit is not None and len(selected) >= total_limit:
            break
        if not can_select(prepared):
            continue
        add_selected(prepared)

    promoted_rows: list[dict[str, Any]] = []
    generated_paths: dict[str, str] = {}
    existing_rows = [row for row in existing_index.get("strategies", [])]

    def emit_promoted_artifacts(
        *,
        candidate: dict[str, Any],
        manifest: dict[str, Any],
        validation_row: dict[str, Any] | None,
        conditional_variant: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        preset_name = _build_generated_preset_name(manifest, conditional_variant)
        status = "inactive" if inactive else policy.default_status
        warnings = _build_warning_list(manifest, policy)
        preset_payload = _build_generated_preset_payload(
            preset_name=preset_name,
            manifest=manifest,
            candidate=candidate,
            status=status,
            warnings=warnings,
            metric_name=policy.metric_name,
            conditional_variant=conditional_variant,
        )
        promoted_strategy_path = output_dir_path / f"{preset_name}.json"
        registry_path = output_dir_path / f"{preset_name}_registry.json"
        pipeline_config_path = output_dir_path / f"{preset_name}_pipeline.yaml"
        if (promoted_strategy_path.exists() or registry_path.exists() or pipeline_config_path.exists()) and not allow_overwrite:
            if preset_name in existing_preset_names:
                return None
            raise FileExistsError(
                f"Generated strategy artifacts already exist for preset {preset_name}. Use --allow-overwrite to replace them."
            )

        registry = StrategyRegistry(
            updated_at=_now_utc(),
            entries=[_build_registry_entry(
                preset_name=preset_name,
                manifest=manifest,
                status=status,
                promoted_strategy_path=promoted_strategy_path,
                conditional_variant=conditional_variant,
            )],
            audit_log=[
                StrategyRegistryAuditEvent(
                    timestamp=_now_utc(),
                    strategy_id=preset_name,
                    action="generated_promotion",
                    to_status="paper",
                    note=f"Promoted from research run {manifest.get('run_id')}",
                )
            ],
        )
        pipeline_config = _build_pipeline_config(
            preset_name=preset_name,
            manifest=manifest,
            output_dir=output_dir_path,
            registry_path=registry_path,
            policy=policy,
        )
        promoted_row = {
            "preset_name": preset_name,
            "source_run_id": manifest.get("run_id"),
            "signal_family": manifest.get("signal_family"),
            "strategy_name": "composite_alpha",
            "universe": manifest.get("universe"),
            "regime_compatibility": infer_strategy_regime_compatibility(
                signal_family=str(manifest.get("signal_family") or ""),
                strategy_name="composite_alpha",
            ),
            "ranking_metric": policy.metric_name,
            "ranking_value": manifest.get("top_metrics", {}).get(policy.metric_name),
            "validation_status": validation_row.get("validation_status") if validation_row else None,
            "validation_reason": validation_row.get("validation_reason") if validation_row else None,
            "promotion_timestamp": _now_utc(),
            "status": status,
            "promotion_variant": "conditional" if conditional_variant else "unconditional",
            "condition_id": conditional_variant.get("condition_id") if conditional_variant else None,
            "condition_type": conditional_variant.get("condition_type") if conditional_variant else None,
            "conditional_promotion_summary": conditional_variant.get("promotion_summary") if conditional_variant else None,
            "activation_conditions": [conditional_variant.get("activation_condition")] if conditional_variant else [],
            "rationale": candidate.get("reasons"),
            "warnings": warnings,
            "generated_preset_path": str(promoted_strategy_path),
            "generated_registry_path": str(registry_path),
            "generated_pipeline_config_path": str(pipeline_config_path),
        }
        promoted_rows.append(promoted_row)
        if dry_run:
            return promoted_row
        _write_payload(promoted_strategy_path, preset_payload)
        save_strategy_registry(registry, registry_path)
        _write_payload(pipeline_config_path, _serialize_pipeline_config(pipeline_config))
        generated_paths[preset_name] = str(promoted_strategy_path)
        return promoted_row

    conditional_allowance_remaining = int(policy.conditional_variant_allowance)
    for prepared in selected:
        candidate = dict(prepared["candidate"])
        manifest = dict(prepared["manifest"])
        validation_row = prepared["validation_row"]
        conditional_variant = prepared["conditional_variant"]
        base_variant = (
            None
            if policy.emit_conditional_variants_alongside_baseline
            else conditional_variant
        )
        emit_promoted_artifacts(
            candidate=candidate,
            manifest=manifest,
            validation_row=validation_row,
            conditional_variant=base_variant,
        )
        if (
            policy.emit_conditional_variants_alongside_baseline
            and conditional_variant is not None
            and conditional_allowance_remaining > 0
        ):
            emitted = emit_promoted_artifacts(
                candidate=candidate,
                manifest=manifest,
                validation_row=validation_row,
                conditional_variant=conditional_variant,
            )
            if emitted is not None:
                conditional_allowance_remaining -= 1

    if not dry_run:
        retained = [
            row
            for row in existing_rows
            if row.get("source_run_id") not in {item["source_run_id"] for item in promoted_rows}
        ]
        index_payload = {
            "schema_version": 1,
            "generated_at": _now_utc(),
            "artifacts_root": str(artifacts_root_path),
            "registry_dir": str(registry_dir_path),
            "promotion_candidates_path": str(registry_dir_path / "promotion_candidates.json"),
            "validation_path": str(validation_path) if validation_path is not None else None,
            "policy": asdict(policy),
            "strategies": sorted(retained + promoted_rows, key=lambda row: str(row.get("preset_name"))),
        }
        _write_payload(output_dir_path / PROMOTED_STRATEGIES_INDEX_NAME, index_payload)

    return {
        "selected_count": len(promoted_rows),
        "dry_run": dry_run,
        "promoted_rows": promoted_rows,
        "generated_paths": generated_paths,
        "promoted_index_path": str(output_dir_path / PROMOTED_STRATEGIES_INDEX_NAME),
    }
