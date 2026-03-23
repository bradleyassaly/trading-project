from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

import pandas as pd

from trading_platform.monitoring.notification_service import send_notifications
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.persistence import persist_paper_run_outputs
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle_for_targets,
    write_paper_trading_artifacts,
)
from trading_platform.portfolio.multi_strategy import (
    allocate_multi_strategy_portfolio,
    write_multi_strategy_artifacts,
)
from trading_platform.portfolio.adaptive_allocation import (
    build_adaptive_allocation,
    export_adaptive_allocation_run_config,
)
from trading_platform.portfolio.strategy_monitoring import (
    build_strategy_monitoring_snapshot,
    recommend_kill_switch_actions,
)
from trading_platform.portfolio.strategy_portfolio import (
    build_strategy_portfolio,
    export_strategy_portfolio_run_config,
    load_strategy_portfolio,
)
from trading_platform.governance.strategy_lifecycle import apply_strategy_governance
from trading_platform.research.experiment_tracking import (
    build_paper_experiment_record,
    register_experiment,
)
from trading_platform.regime.service import detect_market_regime
from trading_platform.research.promotion_pipeline import apply_research_promotions
from trading_platform.research.registry import (
    build_promotion_candidates,
    build_research_leaderboard,
    build_research_registry,
    load_research_manifests,
)
from trading_platform.research.strategy_validation import build_strategy_validation


ORCHESTRATION_STAGE_NAMES = [
    "research",
    "registry",
    "validation",
    "promotion",
    "portfolio",
    "allocation",
    "paper",
    "monitoring",
    "regime",
    "adaptive_allocation",
    "governance",
    "kill_switch",
]
ORCHESTRATION_STAGE_STATUSES = {"pending", "skipped", "running", "succeeded", "failed"}
ORCHESTRATION_STAGE_DEPENDENCIES: dict[str, list[str]] = {
    "registry": ["research"],
    "validation": ["research"],
    "promotion": ["registry", "validation"],
    "portfolio": ["promotion"],
    "allocation": ["portfolio"],
    "paper": ["portfolio"],
    "monitoring": ["paper"],
    "regime": ["monitoring"],
    "adaptive_allocation": ["monitoring", "regime"],
    "governance": ["monitoring"],
    "kill_switch": ["monitoring"],
}


@dataclass(frozen=True)
class AutomatedOrchestrationStageToggles:
    research: bool = True
    registry: bool = True
    validation: bool = True
    promotion: bool = True
    portfolio: bool = True
    allocation: bool = True
    paper: bool = True
    monitoring: bool = True
    regime: bool = False
    adaptive_allocation: bool = False
    governance: bool = True
    kill_switch: bool = True


@dataclass(frozen=True)
class AutomatedOrchestrationConfig:
    run_name: str
    schedule_frequency: str
    research_artifacts_root: str
    experiment_name: str | None = None
    variant_name: str | None = None
    experiment_run_id: str | None = None
    feature_flags: dict[str, Any] = field(default_factory=dict)
    run_label_metadata: dict[str, Any] = field(default_factory=dict)
    output_root_dir: str = "artifacts/orchestration_runs"
    registry_output_dir: str | None = None
    validation_output_dir: str | None = None
    promotion_output_dir: str | None = None
    strategy_portfolio_output_dir: str | None = None
    strategy_portfolio_run_output_dir: str | None = None
    allocation_output_dir: str | None = None
    paper_output_dir: str | None = None
    paper_state_path: str | None = None
    monitoring_output_dir: str | None = None
    regime_output_dir: str | None = None
    adaptive_allocation_output_dir: str | None = None
    adaptive_allocation_run_output_dir: str | None = None
    governance_output_dir: str | None = None
    kill_switch_output_dir: str | None = None
    promotion_policy_config_path: str | None = None
    strategy_validation_policy_config_path: str | None = None
    strategy_portfolio_policy_config_path: str | None = None
    strategy_monitoring_policy_config_path: str | None = None
    market_regime_policy_config_path: str | None = None
    adaptive_allocation_policy_config_path: str | None = None
    strategy_governance_policy_config_path: str | None = None
    strategy_lifecycle_path: str | None = None
    execution_config_path: str | None = None
    monitoring_notification_config_path: str | None = None
    market_regime_input_path: str | None = None
    fail_fast: bool = True
    continue_on_stage_error: bool = False
    max_promotions_per_run: int | None = None
    min_selected_strategies_warning: int = 1
    promotion_dry_run: bool = False
    promotion_inactive: bool = False
    adaptive_allocation_dry_run: bool = False
    kill_switch_include_review: bool = False
    loop_sleep_seconds: int | None = None
    stage_order: list[str] = field(default_factory=lambda: list(ORCHESTRATION_STAGE_NAMES))
    stages: AutomatedOrchestrationStageToggles = field(default_factory=AutomatedOrchestrationStageToggles)
    tags: list[str] = field(default_factory=list)
    notes: str | None = None

    def __post_init__(self) -> None:
        if not self.run_name or not self.run_name.strip():
            raise ValueError("run_name must be a non-empty string")
        if self.schedule_frequency not in {"daily", "weekly", "manual"}:
            raise ValueError("schedule_frequency must be one of: daily, weekly, manual")
        if not self.research_artifacts_root or not self.research_artifacts_root.strip():
            raise ValueError("research_artifacts_root must be a non-empty string")
        if self.max_promotions_per_run is not None and self.max_promotions_per_run <= 0:
            raise ValueError("max_promotions_per_run must be > 0 when provided")
        if self.min_selected_strategies_warning < 0:
            raise ValueError("min_selected_strategies_warning must be >= 0")
        if self.loop_sleep_seconds is not None and self.loop_sleep_seconds <= 0:
            raise ValueError("loop_sleep_seconds must be > 0 when provided")
        self._validate_stage_order()
        self._validate_stage_requirements()

    def _validate_stage_order(self) -> None:
        if len(self.stage_order) != len(set(self.stage_order)):
            raise ValueError("stage_order must not contain duplicate stage names")
        unknown = [name for name in self.stage_order if name not in ORCHESTRATION_STAGE_NAMES]
        if unknown:
            raise ValueError(f"stage_order contains unknown stage names: {unknown}")
        index_map = {name: index for index, name in enumerate(self.stage_order)}
        for stage_name, dependencies in ORCHESTRATION_STAGE_DEPENDENCIES.items():
            for dependency in dependencies:
                if dependency in index_map and stage_name in index_map and index_map[dependency] > index_map[stage_name]:
                    raise ValueError(f"Invalid stage ordering: {dependency} must come before {stage_name}")

    def _validate_stage_requirements(self) -> None:
        if self.stages.promotion and not self.promotion_policy_config_path:
            raise ValueError("promotion_policy_config_path is required when promotion is enabled")
        if self.stages.validation and not self.strategy_validation_policy_config_path:
            raise ValueError("strategy_validation_policy_config_path is required when validation is enabled")
        if self.stages.portfolio and not self.strategy_portfolio_policy_config_path:
            raise ValueError("strategy_portfolio_policy_config_path is required when portfolio is enabled")
        if self.stages.paper and not self.paper_state_path:
            raise ValueError("paper_state_path is required when paper is enabled")
        if self.stages.monitoring and not self.strategy_monitoring_policy_config_path:
            raise ValueError("strategy_monitoring_policy_config_path is required when monitoring is enabled")
        if self.stages.regime and not self.market_regime_policy_config_path:
            raise ValueError("market_regime_policy_config_path is required when regime is enabled")
        if self.stages.adaptive_allocation and not self.adaptive_allocation_policy_config_path:
            raise ValueError("adaptive_allocation_policy_config_path is required when adaptive_allocation is enabled")
        if self.stages.governance and not self.strategy_governance_policy_config_path:
            raise ValueError("strategy_governance_policy_config_path is required when governance is enabled")

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stages"] = asdict(self.stages)
        return payload


@dataclass
class AutomatedStageRecord:
    stage_name: str
    status: str = "pending"
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None
    inputs: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    error_message: str | None = None

    def __post_init__(self) -> None:
        if self.stage_name not in ORCHESTRATION_STAGE_NAMES:
            raise ValueError(f"Unknown stage_name: {self.stage_name}")
        if self.status not in ORCHESTRATION_STAGE_STATUSES:
            raise ValueError(f"Unknown stage status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AutomatedOrchestrationResult:
    run_id: str
    run_name: str
    schedule_frequency: str
    experiment_name: str | None
    variant_name: str | None
    experiment_run_id: str | None
    feature_flags: dict[str, Any]
    run_label_metadata: dict[str, Any]
    started_at: str
    ended_at: str
    status: str
    run_dir: str
    stage_records: list[AutomatedStageRecord]
    warnings: list[str]
    errors: list[dict[str, Any]]
    outputs: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_name": self.run_name,
            "schedule_frequency": self.schedule_frequency,
            "experiment_name": self.experiment_name,
            "variant_name": self.variant_name,
            "experiment_run_id": self.experiment_run_id,
            "feature_flags": self.feature_flags,
            "run_label_metadata": self.run_label_metadata,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "status": self.status,
            "run_dir": self.run_dir,
            "stage_records": [record.to_dict() for record in self.stage_records],
            "warnings": self.warnings,
            "errors": self.errors,
            "outputs": self.outputs,
        }


class OrchestrationStageError(RuntimeError):
    pass


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _sanitize_timestamp(timestamp: str) -> str:
    return timestamp.replace(":", "-")


def _ensure_stage_output_dir(run_dir: Path, configured_path: str | None, stage_name: str) -> Path:
    if configured_path:
        path = Path(configured_path)
        if not path.is_absolute():
            return path
        return path
    return run_dir / stage_name


def _build_multi_strategy_paper_config(result, reserve_cash_pct: float) -> PaperTradingConfig:
    symbols = sorted(result.combined_target_weights)
    return PaperTradingConfig(
        symbols=symbols,
        preset_name="multi_strategy",
        universe_name=f"{result.summary['enabled_sleeve_count']}_sleeves",
        strategy="multi_strategy",
        signal_source="legacy",
        reserve_cash_pct=reserve_cash_pct,
    )


def _build_multi_strategy_target_diagnostics(allocation_result) -> dict[str, Any]:
    return {
        "portfolio_construction_mode": "multi_strategy",
        "rebalance_timestamp": allocation_result.as_of,
        "selected_symbols": ",".join(sorted(set(row["symbol"] for row in allocation_result.sleeve_rows))),
        "target_selected_symbols": ",".join(sorted(allocation_result.combined_target_weights)),
        "realized_holdings_count": len(allocation_result.combined_target_weights),
        "realized_holdings_minus_top_n": 0,
        "average_gross_exposure": allocation_result.summary["gross_exposure_after_constraints"],
        "liquidity_excluded_count": sum(
            int(bundle.diagnostics.get("liquidity_excluded_count") or 0)
            for bundle in allocation_result.sleeve_bundles
        ),
        "sector_cap_excluded_count": sum(
            1
            for row in allocation_result.summary["symbols_removed_or_clipped"]
            if row["constraint_name"] == "sector_cap"
        ),
        "turnover_cap_binding_count": int(allocation_result.summary["turnover_cap_binding"]),
        "semantic_warning": "portfolio_constraints_applied"
        if allocation_result.summary["symbols_removed_or_clipped"]
        else "",
        "target_selected_count": len(allocation_result.combined_target_weights),
        "summary": {"mean_turnover": allocation_result.summary["turnover_estimate"]},
        "multi_strategy_allocation": allocation_result.summary,
    }


def _render_markdown_summary(result: AutomatedOrchestrationResult) -> str:
    lines = [
        f"# Automated Orchestration Run: {result.run_name}",
        "",
        f"- Run id: `{result.run_id}`",
        f"- Schedule: `{result.schedule_frequency}`",
        f"- Experiment: `{result.experiment_name or 'n/a'}`",
        f"- Variant: `{result.variant_name or 'n/a'}`",
        f"- Experiment run id: `{result.experiment_run_id or 'n/a'}`",
        f"- Started: `{result.started_at}`",
        f"- Ended: `{result.ended_at}`",
        f"- Status: `{result.status}`",
        "",
        "## Stages",
    ]
    for record in result.stage_records:
        lines.append(
            f"- `{record.stage_name}`: status=`{record.status}` warnings=`{len(record.warnings)}` error=`{record.error_message or ''}`"
        )
    if result.warnings:
        lines.extend(["", "## Run Warnings"])
        lines.extend([f"- {warning}" for warning in result.warnings])
    if result.errors:
        lines.extend(["", "## Errors"])
        lines.extend([f"- `{item['stage_name']}`: {item['error_message']}" for item in result.errors])
    outputs = result.outputs
    if outputs.get("selected_strategy_count") is not None:
        lines.extend(
            [
                "",
                "## Key Outputs",
                f"- Validated strategies: `{outputs.get('validated_pass_count', 0)}`",
                f"- Promoted strategies: `{outputs.get('promoted_strategy_count', 0)}`",
                f"- Selected strategies: `{outputs.get('selected_strategy_count', 0)}`",
                f"- Allocation positions: `{outputs.get('allocation_position_count', 0)}`",
                f"- Monitoring warnings: `{outputs.get('warning_strategy_count', 0)}`",
                f"- Current regime: `{outputs.get('current_regime_label', 'n/a')}`",
                f"- Adaptive strategies: `{outputs.get('adaptive_selected_strategy_count', 0)}`",
                f"- Under review: `{outputs.get('under_review_count', 0)}`",
                f"- Demoted: `{outputs.get('demoted_count', 0)}`",
                f"- Kill-switch recommendations: `{outputs.get('kill_switch_recommendation_count', 0)}`",
            ]
        )
    if result.feature_flags:
        lines.extend(["", "## Feature Flags"])
        lines.extend([f"- `{key}`: `{value}`" for key, value in sorted(result.feature_flags.items())])
    return "\n".join(lines) + "\n"


def _write_run_artifacts(config: AutomatedOrchestrationConfig, result: AutomatedOrchestrationResult, run_dir: Path) -> dict[str, Path]:
    run_dir.mkdir(parents=True, exist_ok=True)
    run_json_path = run_dir / "orchestration_run.json"
    run_md_path = run_dir / "orchestration_run.md"
    stage_csv_path = run_dir / "stage_status.csv"
    config_path = run_dir / "orchestration_config_snapshot.json"
    errors_path = run_dir / "errors.json"

    payload = result.to_dict()
    run_json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    run_md_path.write_text(_render_markdown_summary(result), encoding="utf-8")
    pd.DataFrame([record.to_dict() for record in result.stage_records]).to_csv(stage_csv_path, index=False)
    config_path.write_text(json.dumps(config.to_dict(), indent=2, default=str), encoding="utf-8")
    if result.errors:
        errors_path.write_text(json.dumps(result.errors, indent=2, default=str), encoding="utf-8")
    from trading_platform.system_evaluation.service import evaluate_orchestration_run

    system_eval_payload = evaluate_orchestration_run(run_dir=run_dir, output_dir=run_dir)
    return {
        "orchestration_run_json_path": run_json_path,
        "orchestration_run_md_path": run_md_path,
        "stage_status_path": stage_csv_path,
        "orchestration_config_snapshot_path": config_path,
        "system_evaluation_json_path": Path(system_eval_payload["system_evaluation_json_path"]),
        "system_evaluation_csv_path": Path(system_eval_payload["system_evaluation_csv_path"]),
        **({"errors_path": errors_path} if result.errors else {}),
    }


def _stage_research(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    manifests = load_research_manifests(config.research_artifacts_root)
    if not manifests:
        raise OrchestrationStageError(f"No research manifests found under {config.research_artifacts_root}")
    output_dir = run_dir / "research"
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = output_dir / "research_stage_snapshot.json"
    snapshot_payload = {
        "generated_at": _now_utc(),
        "research_artifacts_root": config.research_artifacts_root,
        "run_count": len(manifests),
        "run_ids": [manifest.get("run_id") for manifest in manifests[:50]],
    }
    snapshot_path.write_text(json.dumps(snapshot_payload, indent=2), encoding="utf-8")
    context["research_manifest_count"] = len(manifests)
    return {"research_manifest_count": len(manifests), "research_stage_snapshot_path": str(snapshot_path)}


def _stage_registry(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    if int(context.get("research_manifest_count", 0)) <= 0:
        raise OrchestrationStageError("registry stage requires research manifests")
    output_dir = _ensure_stage_output_dir(run_dir, config.registry_output_dir, "registry")
    output_dir.mkdir(parents=True, exist_ok=True)
    registry_result = build_research_registry(artifacts_root=config.research_artifacts_root, output_dir=output_dir)
    leaderboard_result = build_research_leaderboard(artifacts_root=config.research_artifacts_root, output_dir=output_dir)
    candidates_result = build_promotion_candidates(artifacts_root=config.research_artifacts_root, output_dir=output_dir)
    if registry_result["run_count"] <= 0:
        raise OrchestrationStageError("research registry is empty")
    context["registry_dir"] = str(output_dir)
    context["promotion_candidate_count"] = int(candidates_result["eligible_count"])
    return {**registry_result, **leaderboard_result, **candidates_result}


def _stage_validation(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    if int(context.get("research_manifest_count", 0)) <= 0:
        raise OrchestrationStageError("validation stage requires research manifests")
    from trading_platform.config.loader import load_strategy_validation_policy_config

    policy = load_strategy_validation_policy_config(config.strategy_validation_policy_config_path)
    output_dir = _ensure_stage_output_dir(run_dir, config.validation_output_dir, "validation")
    output_dir.mkdir(parents=True, exist_ok=True)
    result = build_strategy_validation(
        artifacts_root=config.research_artifacts_root,
        output_dir=output_dir,
        policy=policy,
    )
    if (result["pass_count"] + result["weak_count"] + result["fail_count"]) <= 0:
        raise OrchestrationStageError("validation stage produced no rows")
    context["strategy_validation_dir"] = str(output_dir)
    return result


def _stage_promotion(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    registry_dir = context.get("registry_dir")
    if not registry_dir:
        raise OrchestrationStageError("promotion stage requires registry outputs")
    candidate_payload = json.loads((Path(registry_dir) / "promotion_candidates.json").read_text(encoding="utf-8"))
    if not candidate_payload.get("rows"):
        raise OrchestrationStageError("promotion stage requires promotion candidates")
    from trading_platform.config.loader import load_promotion_policy_config

    policy = load_promotion_policy_config(config.promotion_policy_config_path)
    output_dir = _ensure_stage_output_dir(run_dir, config.promotion_output_dir, "promotion")
    output_dir.mkdir(parents=True, exist_ok=True)
    result = apply_research_promotions(
        artifacts_root=config.research_artifacts_root,
        registry_dir=registry_dir,
        output_dir=output_dir,
        policy=policy,
        top_n=config.max_promotions_per_run,
        dry_run=config.promotion_dry_run,
        inactive=config.promotion_inactive,
        validation_path=context.get("strategy_validation_dir"),
    )
    if result["selected_count"] <= 0:
        raise OrchestrationStageError("no strategies were promoted")
    context["promoted_dir"] = str(output_dir)
    context["promoted_rows"] = result["promoted_rows"]
    return {
        "promoted_strategy_count": result["selected_count"],
        "promoted_index_path": result["promoted_index_path"],
        "promotion_dry_run": result["dry_run"],
        "promoted_preset_names": [row["preset_name"] for row in result["promoted_rows"]],
    }


def _stage_portfolio(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    promoted_dir = context.get("promoted_dir")
    if not promoted_dir:
        raise OrchestrationStageError("portfolio stage requires promoted strategies")
    from trading_platform.config.loader import load_strategy_portfolio_policy_config

    policy = load_strategy_portfolio_policy_config(config.strategy_portfolio_policy_config_path)
    portfolio_dir = _ensure_stage_output_dir(run_dir, config.strategy_portfolio_output_dir, "strategy_portfolio")
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    build_result = build_strategy_portfolio(
        promoted_dir=promoted_dir,
        lifecycle_path=config.strategy_lifecycle_path,
        output_dir=portfolio_dir,
        policy=policy,
    )
    payload = load_strategy_portfolio(portfolio_dir)
    selected_count = int(payload.get("summary", {}).get("total_selected_strategies", 0))
    if selected_count <= 0:
        raise OrchestrationStageError("strategy portfolio is empty")
    export_dir = _ensure_stage_output_dir(run_dir, config.strategy_portfolio_run_output_dir, "strategy_portfolio_run")
    export_dir.mkdir(parents=True, exist_ok=True)
    export_result = export_strategy_portfolio_run_config(strategy_portfolio_path=portfolio_dir, output_dir=export_dir)
    warnings: list[str] = []
    if selected_count < config.min_selected_strategies_warning:
        warnings.append("selected_strategy_count_below_warning_threshold")
    context["strategy_portfolio_dir"] = str(portfolio_dir)
    context["strategy_portfolio_run_dir"] = str(export_dir)
    context["multi_strategy_config_path"] = export_result["multi_strategy_config_path"]
    context["selected_strategy_count"] = selected_count
    return {
        **build_result,
        **export_result,
        "selected_strategy_count": selected_count,
        "portfolio_warning_count": len(payload.get("warnings", [])),
        "warnings": warnings,
    }


def _stage_allocation(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    config_path = context.get("multi_strategy_config_path")
    if not config_path:
        raise OrchestrationStageError("allocation stage requires exported multi-strategy config")
    from trading_platform.config.loader import load_multi_strategy_portfolio_config

    portfolio_config = load_multi_strategy_portfolio_config(config_path)
    allocation_result = allocate_multi_strategy_portfolio(portfolio_config)
    if not allocation_result.combined_target_weights:
        raise OrchestrationStageError("allocation produced an empty combined portfolio")
    output_dir = _ensure_stage_output_dir(run_dir, config.allocation_output_dir, "allocation")
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths = write_multi_strategy_artifacts(allocation_result, output_dir)
    context["allocation_result"] = allocation_result
    context["allocation_dir"] = str(output_dir)
    return {
        **{name: str(path) for name, path in artifact_paths.items()},
        "allocation_position_count": len(allocation_result.combined_target_weights),
        "gross_exposure": allocation_result.summary["gross_exposure_after_constraints"],
    }


def _stage_paper(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    allocation_result = context.get("allocation_result")
    if allocation_result is None:
        raise OrchestrationStageError("paper stage requires allocation results")
    from trading_platform.config.loader import load_multi_strategy_portfolio_config

    portfolio_config = load_multi_strategy_portfolio_config(context["multi_strategy_config_path"])
    output_dir = _ensure_stage_output_dir(run_dir, config.paper_output_dir, "paper")
    output_dir.mkdir(parents=True, exist_ok=True)
    from trading_platform.config.loader import load_execution_config

    execution_config = load_execution_config(config.execution_config_path) if config.execution_config_path else None
    paper_config = _build_multi_strategy_paper_config(allocation_result, reserve_cash_pct=portfolio_config.cash_reserve_pct)
    state_path = Path(config.paper_state_path)
    state_preexisting = state_path.exists()
    state_store = JsonPaperStateStore(state_path)
    result = run_paper_trading_cycle_for_targets(
        config=paper_config,
        state_store=state_store,
        as_of=allocation_result.as_of,
        latest_prices=allocation_result.latest_prices,
        latest_scores={},
        latest_scheduled_weights=allocation_result.combined_target_weights,
        latest_effective_weights=allocation_result.combined_target_weights,
        target_diagnostics=_build_multi_strategy_target_diagnostics(allocation_result),
        skipped_symbols=[],
        extra_diagnostics={"multi_strategy_allocation": allocation_result.summary},
        execution_config=execution_config,
        auto_apply_fills=False,
    )
    paper_paths = write_paper_trading_artifacts(result=result, output_dir=output_dir)
    persistence_paths, health_checks, latest_summary = persist_paper_run_outputs(
        result=result,
        config=paper_config,
        output_dir=output_dir,
        state_file_preexisting=state_preexisting,
    )
    tracker_dir = run_dir / "experiment_tracking"
    registry_paths = register_experiment(build_paper_experiment_record(output_dir), tracker_dir=tracker_dir)
    context["paper_dir"] = str(output_dir)
    context["paper_summary"] = latest_summary
    return {
        **{name: str(path) for name, path in paper_paths.items()},
        **{name: str(path) for name, path in persistence_paths.items()},
        "experiment_registry_path": registry_paths["experiment_registry_path"],
        "paper_order_count": len(result.orders),
        "paper_equity": latest_summary.get("current_equity", latest_summary.get("equity")),
        "health_check_count": len(health_checks),
    }


def _stage_monitoring(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    strategy_portfolio_dir = context.get("strategy_portfolio_dir")
    paper_dir = context.get("paper_dir")
    if not strategy_portfolio_dir or not paper_dir:
        raise OrchestrationStageError("monitoring stage requires strategy portfolio and paper artifacts")
    from trading_platform.config.loader import load_strategy_monitoring_policy_config

    policy = load_strategy_monitoring_policy_config(config.strategy_monitoring_policy_config_path)
    output_dir = _ensure_stage_output_dir(run_dir, config.monitoring_output_dir, "monitoring")
    output_dir.mkdir(parents=True, exist_ok=True)
    result = build_strategy_monitoring_snapshot(
        strategy_portfolio_path=strategy_portfolio_dir,
        paper_dir=str(paper_dir),
        execution_dir=str(paper_dir),
        allocation_dir=context.get("allocation_dir"),
        output_dir=output_dir,
        policy=policy,
    )
    context["strategy_monitoring_dir"] = str(output_dir)
    if config.monitoring_notification_config_path:
        from trading_platform.config.loader import load_notification_config
        from trading_platform.monitoring.models import Alert

        notification_config = load_notification_config(config.monitoring_notification_config_path)
        monitoring_payload = json.loads((output_dir / "strategy_monitoring.json").read_text(encoding="utf-8"))
        alerts = [
            Alert(
                code=f"strategy_monitor_{row['recommendation']}",
                severity="critical" if row.get("recommendation") == "deactivate" else "warning",
                message=f"{row['preset_name']} recommendation={row['recommendation']}",
                timestamp=_now_utc(),
                entity_type="strategy_portfolio",
                entity_id=str(row["preset_name"]),
            )
            for row in monitoring_payload.get("strategies", [])
            if row.get("recommendation") in {"review", "reduce", "deactivate"}
        ]
        notification_result = send_notifications(alerts=alerts, config=notification_config)
        (output_dir / "notification_summary.json").write_text(json.dumps(notification_result, indent=2, default=str), encoding="utf-8")
        result["notification_sent"] = bool(notification_result.get("sent"))
    return result


def _stage_regime(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    from trading_platform.config.loader import load_market_regime_policy_config

    output_dir = _ensure_stage_output_dir(run_dir, config.regime_output_dir, "regime")
    output_dir.mkdir(parents=True, exist_ok=True)
    input_path = config.market_regime_input_path
    if not input_path:
        paper_dir = context.get("paper_dir")
        if paper_dir and (Path(paper_dir) / "paper_equity_curve.csv").exists():
            input_path = str(Path(paper_dir) / "paper_equity_curve.csv")
    if not input_path:
        raise OrchestrationStageError("regime stage requires market_regime_input_path or paper_equity_curve.csv")
    policy = load_market_regime_policy_config(config.market_regime_policy_config_path)
    result = detect_market_regime(
        input_path=input_path,
        output_dir=output_dir,
        policy=policy,
    )
    latest = result.get("latest", {})
    context["market_regime_dir"] = str(output_dir)
    return {
        **result,
        "current_regime_label": latest.get("regime_label"),
        "regime_confidence_score": latest.get("confidence_score"),
    }


def _stage_kill_switch(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    monitoring_dir = context.get("strategy_monitoring_dir")
    if not monitoring_dir:
        raise OrchestrationStageError("kill_switch stage requires monitoring outputs")
    output_dir = _ensure_stage_output_dir(run_dir, config.kill_switch_output_dir, "kill_switch")
    output_dir.mkdir(parents=True, exist_ok=True)
    result = recommend_kill_switch_actions(
        strategy_monitoring_path=monitoring_dir,
        output_dir=output_dir,
        include_review=config.kill_switch_include_review,
    )
    if result["recommendation_count"] <= 0:
        raise OrchestrationStageError("kill_switch stage produced no actionable recommendations")
    return {**result, "kill_switch_recommendation_count": result["recommendation_count"]}


def _stage_adaptive_allocation(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    strategy_portfolio_dir = context.get("strategy_portfolio_dir")
    monitoring_dir = context.get("strategy_monitoring_dir")
    if not strategy_portfolio_dir or not monitoring_dir:
        raise OrchestrationStageError("adaptive_allocation stage requires strategy portfolio and monitoring outputs")
    from trading_platform.config.loader import load_adaptive_allocation_policy_config

    policy = load_adaptive_allocation_policy_config(config.adaptive_allocation_policy_config_path)
    if config.adaptive_allocation_dry_run:
        policy = type(policy)(**{**policy.__dict__, "dry_run": True})
    output_dir = _ensure_stage_output_dir(run_dir, config.adaptive_allocation_output_dir, "adaptive_allocation")
    output_dir.mkdir(parents=True, exist_ok=True)
    build_result = build_adaptive_allocation(
        strategy_portfolio_path=strategy_portfolio_dir,
        strategy_monitoring_path=monitoring_dir,
        strategy_lifecycle_path=config.strategy_lifecycle_path,
        market_regime_path=context.get("market_regime_dir"),
        output_dir=output_dir,
        policy=policy,
    )
    payload = json.loads((output_dir / "adaptive_allocation.json").read_text(encoding="utf-8"))
    selected_count = int(payload.get("summary", {}).get("total_selected_strategies", 0))
    if selected_count <= 0:
        raise OrchestrationStageError("adaptive allocation is empty")
    export_dir = _ensure_stage_output_dir(
        run_dir,
        config.adaptive_allocation_run_output_dir,
        "adaptive_allocation_run",
    )
    export_dir.mkdir(parents=True, exist_ok=True)
    export_result = export_adaptive_allocation_run_config(
        adaptive_allocation_path=output_dir,
        output_dir=export_dir,
    )
    context["adaptive_allocation_dir"] = str(output_dir)
    context["adaptive_allocation_run_dir"] = str(export_dir)
    context["next_cycle_multi_strategy_config_path"] = export_result["multi_strategy_config_path"]
    return {
        **build_result,
        **export_result,
        "adaptive_selected_strategy_count": selected_count,
        "adaptive_warning_count": int(payload.get("summary", {}).get("warning_count", 0)),
        "current_regime_label": payload.get("summary", {}).get("current_regime_label"),
    }


def _stage_governance(config: AutomatedOrchestrationConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    promoted_dir = context.get("promoted_dir")
    monitoring_dir = context.get("strategy_monitoring_dir")
    if not promoted_dir or not monitoring_dir:
        raise OrchestrationStageError("governance stage requires promoted strategies and monitoring outputs")
    from trading_platform.config.loader import load_strategy_governance_policy_config

    policy = load_strategy_governance_policy_config(config.strategy_governance_policy_config_path)
    output_dir = _ensure_stage_output_dir(run_dir, config.governance_output_dir, "governance")
    output_dir.mkdir(parents=True, exist_ok=True)
    result = apply_strategy_governance(
        promoted_dir=promoted_dir,
        strategy_validation_path=context.get("strategy_validation_dir"),
        strategy_monitoring_path=monitoring_dir,
        adaptive_allocation_path=context.get("adaptive_allocation_dir"),
        lifecycle_path=config.strategy_lifecycle_path,
        output_dir=output_dir,
        policy=policy,
        dry_run=False,
    )
    context["strategy_governance_dir"] = str(output_dir)
    return result


RUNNER_STAGE_HANDLERS: dict[str, Callable[[AutomatedOrchestrationConfig, Path, dict[str, Any]], dict[str, Any]]] = {
    "research": _stage_research,
    "registry": _stage_registry,
    "validation": _stage_validation,
    "promotion": _stage_promotion,
    "portfolio": _stage_portfolio,
    "allocation": _stage_allocation,
    "paper": _stage_paper,
    "monitoring": _stage_monitoring,
    "regime": _stage_regime,
    "adaptive_allocation": _stage_adaptive_allocation,
    "governance": _stage_governance,
    "kill_switch": _stage_kill_switch,
}


def run_automated_orchestration(config: AutomatedOrchestrationConfig) -> tuple[AutomatedOrchestrationResult, dict[str, Path]]:
    started_at = _now_utc()
    run_id = _sanitize_timestamp(started_at)
    run_dir = Path(config.output_root_dir) / config.run_name / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    stage_records = [AutomatedStageRecord(stage_name=name) for name in config.stage_order]
    context: dict[str, Any] = {"run_id": run_id, "run_started_at": started_at}
    warnings: list[str] = []
    errors: list[dict[str, Any]] = []
    outputs: dict[str, Any] = {}

    for record in stage_records:
        enabled = bool(getattr(config.stages, record.stage_name))
        if not enabled:
            record.status = "skipped"
            continue

        record.status = "running"
        record.started_at = _now_utc()
        record.inputs = {"run_dir": str(run_dir)}
        stage_start = perf_counter()
        try:
            stage_outputs = RUNNER_STAGE_HANDLERS[record.stage_name](config, run_dir, context)
            stage_warnings = stage_outputs.pop("warnings", []) if isinstance(stage_outputs, dict) else []
            record.outputs = stage_outputs
            record.warnings.extend(stage_warnings)
            warnings.extend(f"{record.stage_name}:{warning}" for warning in stage_warnings)
            record.status = "succeeded"
            outputs[record.stage_name] = record.outputs
            outputs.update(
                {
                    key: value
                    for key, value in record.outputs.items()
                    if key in {
                        "promoted_strategy_count",
                        "promoted_preset_names",
                        "pass_count",
                        "weak_count",
                        "fail_count",
                        "selected_strategy_count",
                        "allocation_position_count",
                        "paper_equity",
                        "warning_strategy_count",
                        "deactivation_candidate_count",
                        "notification_sent",
                        "current_regime_label",
                        "adaptive_selected_strategy_count",
                        "adaptive_warning_count",
                        "under_review_count",
                        "degraded_count",
                        "demoted_count",
                        "kill_switch_recommendation_count",
                    }
                    and value is not None
                }
            )
            if record.stage_name == "validation":
                outputs["validated_pass_count"] = record.outputs.get("pass_count", 0)
        except Exception as exc:
            record.status = "failed"
            record.error_message = f"{type(exc).__name__}: {exc}"
            errors.append({"stage_name": record.stage_name, "error_message": record.error_message})
            if config.fail_fast or not config.continue_on_stage_error:
                record.ended_at = _now_utc()
                record.duration_seconds = round(perf_counter() - stage_start, 6)
                break
        finally:
            if record.ended_at is None:
                record.ended_at = _now_utc()
            record.duration_seconds = round(perf_counter() - stage_start, 6)

        if record.status == "failed" and (config.fail_fast or not config.continue_on_stage_error):
            break

    ended_at = _now_utc()
    status = "failed" if errors else "succeeded"
    result = AutomatedOrchestrationResult(
        run_id=run_id,
        run_name=config.run_name,
        schedule_frequency=config.schedule_frequency,
        experiment_name=config.experiment_name,
        variant_name=config.variant_name,
        experiment_run_id=config.experiment_run_id,
        feature_flags=dict(sorted(config.feature_flags.items())),
        run_label_metadata=dict(sorted(config.run_label_metadata.items())),
        started_at=started_at,
        ended_at=ended_at,
        status=status,
        run_dir=str(run_dir),
        stage_records=stage_records,
        warnings=warnings,
        errors=errors,
        outputs=outputs,
    )
    artifact_paths = _write_run_artifacts(config, result, run_dir)
    return result, artifact_paths


def show_orchestration_run(path_or_dir: str | Path) -> dict[str, Any]:
    path = Path(path_or_dir)
    if path.is_dir():
        path = path / "orchestration_run.json"
    return json.loads(path.read_text(encoding="utf-8"))


def orchestration_loop(
    *,
    config: AutomatedOrchestrationConfig,
    max_iterations: int | None = None,
) -> list[dict[str, Any]]:
    iterations: list[dict[str, Any]] = []
    iteration = 0
    sleep_seconds = config.loop_sleep_seconds or (
        86400 if config.schedule_frequency == "daily" else 604800 if config.schedule_frequency == "weekly" else 60
    )
    while max_iterations is None or iteration < max_iterations:
        result, artifact_paths = run_automated_orchestration(config)
        iterations.append(
            {
                "run_id": result.run_id,
                "status": result.status,
                "run_dir": result.run_dir,
                "artifact_paths": {name: str(path) for name, path in artifact_paths.items()},
            }
        )
        iteration += 1
        if max_iterations is not None and iteration >= max_iterations:
            break
        time.sleep(sleep_seconds)
    return iterations


def find_latest_automated_orchestration_run(root: str | Path) -> Path:
    candidates = list(Path(root).rglob("orchestration_run.json"))
    if not candidates:
        raise FileNotFoundError(f"No orchestration_run.json files found under {root}")
    return max(candidates, key=lambda path: path.stat().st_mtime).parent
