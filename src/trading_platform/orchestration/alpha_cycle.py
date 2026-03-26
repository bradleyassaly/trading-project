from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.cli.common import UNIVERSES
from trading_platform.config.loader import (
    load_alpha_research_workflow_config,
    load_promotion_policy_config,
    load_research_input_refresh_workflow_config,
    load_strategy_portfolio_policy_config,
)
from trading_platform.config.workflow_models import (
    AlphaCycleWorkflowConfig,
    AlphaResearchWorkflowConfig,
    ResearchInputRefreshWorkflowConfig,
)
from trading_platform.db.services import DatabaseLineageService, build_research_memory_service
from trading_platform.portfolio.strategy_portfolio import (
    build_strategy_portfolio,
    export_strategy_portfolio_run_config,
    load_strategy_portfolio,
)
from trading_platform.portfolio.conditional_activation import (
    ConditionalActivationConfig,
    activate_strategy_portfolio,
    load_activated_strategy_portfolio,
)
from trading_platform.research.alpha_lab.runner import run_alpha_research
from trading_platform.research.promotion_pipeline import apply_research_promotions
from trading_platform.research.registry import refresh_research_registry_bundle
from trading_platform.services.research_input_refresh_service import (
    ResearchInputRefreshRequest,
    refresh_research_inputs,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class AlphaCycleStageRecord:
    stage_name: str
    status: str = "pending"
    started_at: str | None = None
    ended_at: str | None = None
    duration_seconds: float | None = None
    outputs: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AlphaCycleResult:
    run_name: str
    run_id: str | None
    run_dir: str
    started_at: str
    ended_at: str
    duration_seconds: float
    status: str
    stage_records: list[AlphaCycleStageRecord]
    warnings: list[str]
    errors: list[str]
    summary_json_path: str
    summary_md_path: str
    key_artifacts: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_name": self.run_name,
            "run_id": self.run_id,
            "run_dir": self.run_dir,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "stage_records": [record.to_dict() for record in self.stage_records],
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "summary_json_path": self.summary_json_path,
            "summary_md_path": self.summary_md_path,
            "key_artifacts": dict(self.key_artifacts),
        }


def _resolve_symbols(*, symbols: list[str] | None, universe: str | None) -> list[str]:
    if symbols:
        return list(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    if universe:
        if universe not in UNIVERSES:
            raise ValueError(f"Unknown universe: {universe}")
        return list(dict.fromkeys(str(symbol).upper() for symbol in UNIVERSES[universe]))
    raise ValueError("A symbol selection is required")


def _build_refresh_request(config: ResearchInputRefreshWorkflowConfig) -> ResearchInputRefreshRequest:
    return ResearchInputRefreshRequest(
        symbols=_resolve_symbols(symbols=config.symbols, universe=config.universe),
        feature_groups=config.feature_groups,
        universe_name=config.universe,
        sub_universe_id=config.sub_universe_id,
        reference_data_root=config.reference_data_root,
        universe_membership_path=config.universe_membership_path,
        taxonomy_snapshot_path=config.taxonomy_snapshot_path,
        benchmark_mapping_path=config.benchmark_mapping_path,
        market_regime_path=config.market_regime_path,
        group_map_path=config.group_map_path,
        benchmark_id=config.benchmark,
        feature_dir=Path(config.feature_dir),
        metadata_dir=Path(config.metadata_dir),
        normalized_dir=Path(config.normalized_dir),
        failure_policy=config.failure_policy,
        fundamentals_enabled=config.fundamentals_enabled,
        fundamentals_artifact_root=Path(config.fundamentals_artifact_root) if config.fundamentals_artifact_root else None,
        fundamentals_providers=list(config.fundamentals_providers or []),
        fundamentals_sec_companyfacts_root=config.fundamentals_sec_companyfacts_root,
        fundamentals_sec_submissions_root=config.fundamentals_sec_submissions_root,
        fundamentals_vendor_file_path=config.fundamentals_vendor_file_path,
        fundamentals_vendor_api_key=config.fundamentals_vendor_api_key,
    )


def _research_kwargs(
    config: AlphaResearchWorkflowConfig,
    *,
    feature_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    return {
        "symbols": _resolve_symbols(symbols=config.symbols, universe=config.universe),
        "universe": None,
        "feature_dir": feature_dir,
        "signal_family": config.signal_family,
        "signal_families": list(config.signal_families or [config.signal_family]),
        "lookbacks": config.lookbacks,
        "horizons": config.horizons,
        "min_rows": config.min_rows,
        "top_quantile": config.top_quantile,
        "bottom_quantile": config.bottom_quantile,
        "candidate_grid_preset": config.candidate_grid_preset,
        "signal_composition_preset": config.signal_composition_preset,
        "max_variants_per_family": config.max_variants_per_family,
        "output_dir": output_dir,
        "train_size": config.train_size,
        "test_size": config.test_size,
        "step_size": config.step_size,
        "min_train_size": config.min_train_size,
        "portfolio_top_n": config.portfolio_top_n,
        "portfolio_long_quantile": config.portfolio_long_quantile,
        "portfolio_short_quantile": config.portfolio_short_quantile,
        "commission": config.commission,
        "min_price": config.min_price,
        "min_volume": config.min_volume,
        "min_avg_dollar_volume": config.min_avg_dollar_volume,
        "max_adv_participation": config.max_adv_participation,
        "max_position_pct_of_adv": config.max_position_pct_of_adv,
        "max_notional_per_name": config.max_notional_per_name,
        "slippage_bps_per_turnover": config.slippage_bps_per_turnover,
        "slippage_bps_per_adv": config.slippage_bps_per_adv,
        "dynamic_recent_quality_window": config.dynamic_recent_quality_window,
        "dynamic_min_history": config.dynamic_min_history,
        "dynamic_downweight_mean_rank_ic": config.dynamic_downweight_mean_rank_ic,
        "dynamic_deactivate_mean_rank_ic": config.dynamic_deactivate_mean_rank_ic,
        "regime_aware_enabled": config.regime_aware_enabled,
        "regime_min_history": config.regime_min_history,
        "regime_underweight_mean_rank_ic": config.regime_underweight_mean_rank_ic,
        "regime_exclude_mean_rank_ic": config.regime_exclude_mean_rank_ic,
        "equity_context_enabled": config.equity_context_enabled,
        "equity_context_include_volume": config.equity_context_include_volume,
        "fundamentals_enabled": config.fundamentals_enabled,
        "fundamentals_daily_features_path": Path(config.fundamentals_daily_features_path) if config.fundamentals_daily_features_path else None,
        "enable_context_confirmations": config.enable_context_confirmations,
        "enable_relative_features": config.enable_relative_features,
        "enable_flow_confirmations": config.enable_flow_confirmations,
        "ensemble_enabled": config.enable_ensemble,
        "ensemble_mode": config.ensemble_mode,
        "ensemble_weight_method": config.ensemble_weight_method,
        "ensemble_normalize_scores": config.ensemble_normalize_scores,
        "ensemble_max_members": config.ensemble_max_members,
        "ensemble_require_promoted_only": True,
        "ensemble_max_members_per_family": config.ensemble_max_members_per_family,
        "ensemble_minimum_member_observations": config.ensemble_minimum_member_observations,
        "ensemble_minimum_member_metric": config.ensemble_minimum_member_metric,
    }


def _stage_path(path_value: str | None, default_path: Path) -> Path:
    return Path(path_value) if path_value else default_path


def _summarize_promotions(promoted_dir: Path) -> dict[str, Any]:
    index_path = promoted_dir / "promoted_strategies.json"
    if not index_path.exists():
        return {
            "promoted_strategy_count": 0,
            "promoted_family_count": 0,
            "promoted_family_names": [],
        }
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    strategies = list(payload.get("strategies", []))
    families = sorted({str(row.get("signal_family") or "") for row in strategies if str(row.get("signal_family") or "").strip()})
    unconditional_count = sum(1 for row in strategies if str(row.get("promotion_variant") or "unconditional") != "conditional")
    conditional_count = sum(1 for row in strategies if str(row.get("promotion_variant") or "") == "conditional")
    return {
        "promoted_strategy_count": len(strategies),
        "promoted_family_count": len(families),
        "promoted_family_names": families,
        "promoted_unconditional_count": unconditional_count,
        "promoted_conditional_count": conditional_count,
    }


def _summarize_portfolio(portfolio_dir: Path) -> dict[str, Any]:
    try:
        payload = load_strategy_portfolio(portfolio_dir)
    except FileNotFoundError:
        return {
            "selected_portfolio_strategy_count": 0,
            "selected_portfolio_weights": {},
        }
    selected_rows = list(payload.get("selected_strategies", []))
    return {
        "selected_portfolio_strategy_count": len(selected_rows),
        "selected_conditional_portfolio_count": sum(
            1 for row in selected_rows if str(row.get("promotion_variant") or "") == "conditional"
        ),
        "selected_portfolio_weights": {
            str(row.get("preset_name")): float(row.get("allocation_weight") or 0.0)
            for row in selected_rows
        },
        "portfolio_summary": payload.get("summary", {}),
    }


def _summarize_activated_portfolio(activated_dir: Path | None) -> dict[str, Any]:
    if activated_dir is None:
        return {
            "activated_unconditional_count": 0,
            "activated_conditional_count": 0,
            "inactive_conditional_count": 0,
        }
    try:
        payload = load_activated_strategy_portfolio(activated_dir)
    except FileNotFoundError:
        return {
            "activated_unconditional_count": 0,
            "activated_conditional_count": 0,
            "inactive_conditional_count": 0,
        }
    summary = dict(payload.get("summary") or {})
    return {
        "activated_unconditional_count": int(summary.get("activated_unconditional_count") or 0),
        "activated_conditional_count": int(summary.get("activated_conditional_count") or 0),
        "inactive_conditional_count": int(summary.get("inactive_conditional_count") or 0),
    }


def _portfolio_run_notes(payload: dict[str, Any]) -> str:
    summary = payload.get("summary", {})
    return (
        "selected_count="
        f"{int(summary.get('total_selected_strategies') or 0)}"
        f"; selected_conditional_count={int(summary.get('selected_conditional_variant_count') or 0)}"
        f"; shadow_conditional_count={int(summary.get('shadow_conditional_variant_count') or 0)}"
    )


def _top_metrics_from_research(research_result: dict[str, Any]) -> dict[str, Any]:
    manifest_path = research_result.get("research_manifest_path")
    if not manifest_path or not Path(manifest_path).exists():
        return {}
    payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    return dict(payload.get("top_metrics", {}))


def _write_summary_artifacts(
    *,
    config: AlphaCycleWorkflowConfig,
    run_dir: Path,
    started_at: str,
    ended_at: str,
    duration_seconds: float,
    stage_records: list[AlphaCycleStageRecord],
    warnings: list[str],
    errors: list[str],
    key_artifacts: dict[str, str],
    promotion_summary: dict[str, Any],
    portfolio_summary: dict[str, Any],
    activated_summary: dict[str, Any],
    top_metrics: dict[str, Any],
) -> tuple[Path, Path, str]:
    failed_count = sum(1 for record in stage_records if record.status == "failed")
    status = "succeeded"
    if failed_count and any(record.status == "succeeded" for record in stage_records):
        status = "partial_failed"
    elif failed_count:
        status = "failed"

    summary_payload = {
        "workflow_type": "alpha_cycle",
        "run_name": config.run_name,
        "run_id": config.run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "status": status,
        "strict_mode": config.strict_mode,
        "best_effort_mode": config.best_effort_mode,
        "run_dir": str(run_dir),
        "stage_records": [record.to_dict() for record in stage_records],
        "key_artifacts": key_artifacts,
        "promoted_strategy_count": promotion_summary.get("promoted_strategy_count", 0),
        "promoted_unconditional_count": promotion_summary.get("promoted_unconditional_count", 0),
        "promoted_conditional_count": promotion_summary.get("promoted_conditional_count", 0),
        "promoted_family_count": promotion_summary.get("promoted_family_count", 0),
        "promoted_family_names": promotion_summary.get("promoted_family_names", []),
        "selected_portfolio_strategy_count": portfolio_summary.get("selected_portfolio_strategy_count", 0),
        "selected_conditional_portfolio_count": portfolio_summary.get("selected_conditional_portfolio_count", 0),
        "activated_unconditional_count": activated_summary.get("activated_unconditional_count", 0),
        "activated_conditional_count": activated_summary.get("activated_conditional_count", 0),
        "inactive_conditional_count": activated_summary.get("inactive_conditional_count", 0),
        "selected_portfolio_weights": portfolio_summary.get("selected_portfolio_weights", {}),
        "top_metrics": top_metrics,
        "warnings": warnings,
        "errors": errors,
        "config": {
            "refresh_config": config.refresh_config,
            "research_config": config.research_config,
            "promotion_policy_config": config.promotion_policy_config,
            "strategy_portfolio_policy_config": config.strategy_portfolio_policy_config,
            "database_enabled": config.enable_database_metadata,
            "database_schema": config.database_schema,
        },
    }

    json_path = run_dir / "alpha_cycle_summary.json"
    md_path = run_dir / "alpha_cycle_summary.md"
    json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")

    lines = [
        "# Alpha Cycle Summary",
        "",
        f"- run_name: `{config.run_name}`",
        f"- run_id: `{config.run_id}`" if config.run_id else "- run_id: `none`",
        f"- status: `{status}`",
        f"- started_at: `{started_at}`",
        f"- ended_at: `{ended_at}`",
        f"- duration_seconds: `{duration_seconds:.3f}`",
        f"- promoted_strategy_count: `{promotion_summary.get('promoted_strategy_count', 0)}`",
        f"- promoted_unconditional_count: `{promotion_summary.get('promoted_unconditional_count', 0)}`",
        f"- promoted_conditional_count: `{promotion_summary.get('promoted_conditional_count', 0)}`",
        f"- promoted_family_count: `{promotion_summary.get('promoted_family_count', 0)}`",
        f"- promoted_family_names: `{', '.join(promotion_summary.get('promoted_family_names', [])) or 'none'}`",
        f"- selected_portfolio_strategy_count: `{portfolio_summary.get('selected_portfolio_strategy_count', 0)}`",
        f"- selected_conditional_portfolio_count: `{portfolio_summary.get('selected_conditional_portfolio_count', 0)}`",
        f"- activated_unconditional_count: `{activated_summary.get('activated_unconditional_count', 0)}`",
        f"- activated_conditional_count: `{activated_summary.get('activated_conditional_count', 0)}`",
        f"- inactive_conditional_count: `{activated_summary.get('inactive_conditional_count', 0)}`",
        "",
        "## Stages",
        "",
        "| stage | status | duration_seconds | error |",
        "| --- | --- | ---: | --- |",
    ]
    for record in stage_records:
        duration_text = f"{record.duration_seconds:.3f}" if record.duration_seconds is not None else "n/a"
        lines.append(
            f"| {record.stage_name} | {record.status} | {duration_text} | {record.error_message or ''} |"
        )
    if top_metrics:
        lines.extend(
            [
                "",
                "## Top Metrics",
                "",
            ]
        )
        for key, value in sorted(top_metrics.items()):
            lines.append(f"- {key}: `{value}`")
    if key_artifacts:
        lines.extend(
            [
                "",
                "## Artifacts",
                "",
            ]
        )
        for key, value in sorted(key_artifacts.items()):
            lines.append(f"- {key}: `{value}`")
    if warnings:
        lines.extend(["", "## Warnings", ""])
        for warning in warnings:
            lines.append(f"- {warning}")
    if errors:
        lines.extend(["", "## Errors", ""])
        for error in errors:
            lines.append(f"- {error}")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, md_path, status


def run_alpha_cycle(config: AlphaCycleWorkflowConfig) -> AlphaCycleResult:
    started_at = _now_utc()
    started_clock = time.monotonic()
    run_dir_name = str(config.run_name).strip()
    if config.run_id:
        run_dir_name = f"{run_dir_name}_{config.run_id}"
    run_dir = Path(config.output_root) / run_dir_name
    run_dir.mkdir(parents=True, exist_ok=True)

    research_output_dir = _stage_path(config.research_output_dir, run_dir / "research")
    registry_dir = _stage_path(config.registry_dir, run_dir / "research_registry")
    promoted_dir = _stage_path(config.promoted_dir, run_dir / "promoted_strategies")
    portfolio_dir = _stage_path(config.portfolio_dir, run_dir / "strategy_portfolio")
    export_dir = _stage_path(config.export_dir, run_dir / "export_bundle")

    warnings: list[str] = []
    errors: list[str] = []
    key_artifacts: dict[str, str] = {}
    stage_records: list[AlphaCycleStageRecord] = []
    db_lineage = DatabaseLineageService.from_config(
        enable_database_metadata=config.enable_database_metadata,
        database_url=config.database_url,
        database_schema=config.database_schema,
    )
    research_memory = build_research_memory_service(
        enable_database_metadata=config.enable_database_metadata,
        database_url=config.database_url,
        database_schema=config.database_schema,
        write_candidates=config.tracking_write_candidates,
        write_metrics=config.tracking_write_metrics,
        write_promotions=config.tracking_write_promotions,
    )
    research_memory.init_schema(schema_name=config.database_schema)

    refresh_result: Any | None = None
    research_result: dict[str, Any] | None = None
    promotion_result: dict[str, Any] | None = None
    portfolio_result: dict[str, Any] | None = None
    export_result: dict[str, Any] | None = None
    activated_portfolio_dir: Path | None = None
    research_run_db_id = None

    def run_stage(stage_name: str, enabled: bool, action) -> None:
        nonlocal refresh_result, research_result, promotion_result, portfolio_result, export_result
        record = AlphaCycleStageRecord(stage_name=stage_name)
        stage_records.append(record)
        if not enabled:
            record.status = "skipped"
            return
        record.started_at = _now_utc()
        stage_start = time.monotonic()
        try:
            output = action(record)
            if isinstance(output, dict):
                record.outputs = output
            if record.status == "pending":
                record.status = "succeeded"
        except Exception as exc:
            record.status = "failed"
            record.error_message = f"{type(exc).__name__}: {exc}"
            errors.append(f"{stage_name}: {type(exc).__name__}: {exc}")
            if config.strict_mode and not config.best_effort_mode:
                raise
        finally:
            record.ended_at = _now_utc()
            record.duration_seconds = time.monotonic() - stage_start

    refresh_config = (
        load_research_input_refresh_workflow_config(config.refresh_config)
        if config.refresh_config
        else None
    )
    research_config = (
        load_alpha_research_workflow_config(config.research_config)
        if config.research_config
        else None
    )

    try:
        def do_refresh(record: AlphaCycleStageRecord) -> dict[str, Any]:
            nonlocal refresh_result
            if refresh_config is None:
                raise ValueError("refresh_config is required for refresh stage")
            refresh_result = refresh_research_inputs(request=_build_refresh_request(refresh_config))
            outputs = {key: str(path) for key, path in refresh_result.paths.items()}
            outputs["feature_dir"] = str(refresh_result.feature_dir)
            outputs["metadata_dir"] = str(refresh_result.metadata_dir)
            key_artifacts.update({
                "refresh_summary_path": str(refresh_result.paths.get("research_input_refresh_summary_json", "")),
            })
            return outputs

        run_stage("refresh", config.stages.refresh, do_refresh)

        def do_research(record: AlphaCycleStageRecord) -> dict[str, Any]:
            nonlocal research_result, research_run_db_id
            if research_config is None:
                raise ValueError("research_config is required for research stage")
            feature_dir = Path(research_config.feature_dir)
            if refresh_result is not None:
                feature_dir = refresh_result.feature_dir
            research_output_dir.mkdir(parents=True, exist_ok=True)
            research_run_key = research_output_dir.name
            research_run_db_id = db_lineage.create_research_run(
                run_key=research_run_key,
                run_type="alpha_research",
                config_payload={
                    **_research_kwargs(
                        research_config,
                        feature_dir=feature_dir,
                        output_dir=research_output_dir,
                    ),
                    "feature_dir": str(feature_dir),
                    "output_dir": str(research_output_dir),
                },
                notes="alpha_cycle research stage",
            )
            try:
                research_result = run_alpha_research(
                    **_research_kwargs(
                        research_config,
                        feature_dir=feature_dir,
                        output_dir=research_output_dir,
                    )
                )
                key_artifacts.update(
                    {
                        "research_manifest_path": str(research_result.get("research_manifest_path", "")),
                        "research_leaderboard_path": str(research_result.get("leaderboard_path", "")),
                        "promoted_signals_path": str(research_result.get("promoted_signals_path", "")),
                    }
                )
                leaderboard_path = research_result.get("leaderboard_path")
                if leaderboard_path and Path(leaderboard_path).exists():
                    research_memory.persist_alpha_research_outputs(
                        run_id=research_run_db_id,
                        leaderboard_df=pd.read_csv(leaderboard_path),
                    )
                research_memory.attach_run_metadata(
                    run_id=research_run_db_id,
                    artifacts_root=str(research_output_dir.parent),
                    output_dir=str(research_output_dir),
                    universe=research_config.universe,
                    config_path=config.research_config,
                )
                db_lineage.complete_research_run(research_run_db_id, notes="alpha_cycle research stage completed")
            except Exception:
                db_lineage.fail_research_run(research_run_db_id, notes="alpha_cycle research stage failed")
                raise
            return research_result

        run_stage("research", config.stages.research, do_research)

        def do_promotion(record: AlphaCycleStageRecord) -> dict[str, Any]:
            nonlocal promotion_result
            policy = load_promotion_policy_config(config.promotion_policy_config)
            artifacts_root = research_output_dir.parent
            registry_bundle = refresh_research_registry_bundle(
                artifacts_root=artifacts_root,
                output_dir=registry_dir,
            )
            promotion_result = apply_research_promotions(
                artifacts_root=artifacts_root,
                registry_dir=registry_dir,
                output_dir=promoted_dir,
                policy=policy,
                top_n=config.promotion_top_n,
                allow_overwrite=config.allow_overwrite,
                dry_run=False,
                inactive=config.inactive,
                validation_path=Path(config.validation_path) if config.validation_path else None,
                override_validation=config.override_validation,
            )
            if int(promotion_result.get("selected_count") or 0) == 0:
                record.warnings.append("zero_promotions")
                warnings.append("Promotion produced zero strategies; portfolio and export stages may be skipped.")
            enriched_promoted_rows: list[dict[str, Any]] = []
            for row in promotion_result.get("promoted_rows", []):
                strategy_definition_id = db_lineage.upsert_strategy_definition(
                    name=str(row["preset_name"]),
                    version=str(row.get("promotion_timestamp") or row.get("source_run_id") or "v1"),
                    config_payload=row,
                    code_hash=None,
                    is_active=row.get("status") == "active",
                )
                promotion_decision_id = db_lineage.record_promotion_decision(
                    strategy_definition_id=strategy_definition_id,
                    source_research_run_id=db_lineage.find_research_run_id(str(row.get("source_run_id") or "")),
                    decision=str(row.get("status") or "promoted"),
                    reason=str(row.get("rationale") or ""),
                    metrics_json={
                        "ranking_metric": row.get("ranking_metric"),
                        "ranking_value": row.get("ranking_value"),
                        "promotion_variant": row.get("promotion_variant"),
                        "condition_id": row.get("condition_id"),
                        "condition_type": row.get("condition_type"),
                    },
                )
                promoted_strategy_id = db_lineage.record_promoted_strategy(
                    strategy_definition_id=strategy_definition_id,
                    promotion_decision_id=promotion_decision_id,
                    status=str(row.get("status") or "inactive"),
                )
                enriched_promoted_rows.append(
                    {
                        **row,
                        "strategy_definition_id": strategy_definition_id,
                        "promoted_strategy_id": promoted_strategy_id,
                    }
                )
            research_memory.persist_promotions(
                run_id=db_lineage.find_research_run_id(research_output_dir.name),
                promoted_rows=enriched_promoted_rows,
            )
            key_artifacts.update(
                {
                    "research_registry_path": str(registry_bundle.get("registry_json_path", "")),
                    "promotion_candidates_path": str(registry_bundle.get("promotion_candidates_json_path", "")),
                    "promoted_index_path": str(promotion_result.get("promoted_index_path", "")),
                }
            )
            return {
                **{key: str(value) for key, value in registry_bundle.items()},
                **{key: str(value) if isinstance(value, Path) else value for key, value in promotion_result.items() if key != "promoted_rows"},
                "promoted_row_count": int(len(promotion_result.get("promoted_rows", []))),
            }

        run_stage("promotion", config.stages.promotion, do_promotion)

        def do_portfolio(record: AlphaCycleStageRecord) -> dict[str, Any]:
            nonlocal portfolio_result, activated_portfolio_dir
            promotion_summary = _summarize_promotions(promoted_dir)
            if promotion_summary["promoted_strategy_count"] == 0:
                record.status = "skipped"
                record.warnings.append("zero_promotions")
                warnings.append("Portfolio stage skipped because no promoted strategies were available.")
                return {}
            policy = load_strategy_portfolio_policy_config(config.strategy_portfolio_policy_config)
            portfolio_result = build_strategy_portfolio(
                promoted_dir=promoted_dir,
                output_dir=portfolio_dir,
                policy=policy,
                lifecycle_path=Path(config.lifecycle_path) if config.lifecycle_path else None,
            )
            if policy.evaluate_conditional_activation:
                activated_portfolio_dir = portfolio_dir / "activated"
                activation_result = activate_strategy_portfolio(
                    portfolio_path=portfolio_dir,
                    output_dir=activated_portfolio_dir,
                    config=ConditionalActivationConfig(
                        evaluate_conditional_activation=True,
                        activation_context_sources=list(policy.activation_context_sources),
                        include_inactive_conditionals_in_output=policy.include_inactive_conditionals_in_output,
                    ),
                    market_regime_path=(refresh_config.market_regime_path if refresh_config is not None else None),
                    regime_labels_path=research_output_dir,
                    metadata_dir=(str(refresh_result.metadata_dir) if refresh_result is not None else None),
                )
            else:
                activation_result = {}
            portfolio_run_id = db_lineage.create_portfolio_run(
                run_key=f"{run_dir_name}_strategy_portfolio",
                mode="strategy_portfolio",
                config_payload={
                    "policy": asdict(policy),
                    "summary": load_strategy_portfolio(portfolio_dir).get("summary", {}),
                    "promoted_dir": str(promoted_dir),
                    "portfolio_dir": str(portfolio_dir),
                },
                notes="alpha_cycle portfolio stage",
            )
            db_lineage.complete_portfolio_run(
                portfolio_run_id,
                notes=_portfolio_run_notes(load_strategy_portfolio(portfolio_dir)),
            )
            key_artifacts.update(
                {
                    "strategy_portfolio_json_path": str(portfolio_result.get("strategy_portfolio_json_path", "")),
                    "strategy_portfolio_csv_path": str(portfolio_result.get("strategy_portfolio_csv_path", "")),
                    "strategy_portfolio_condition_summary_path": str(
                        portfolio_result.get("strategy_portfolio_condition_summary_path", "")
                    ),
                    "activated_strategy_portfolio_json_path": str(
                        activation_result.get("activated_strategy_portfolio_json_path", "")
                    ),
                    "activated_strategy_portfolio_csv_path": str(
                        activation_result.get("activated_strategy_portfolio_csv_path", "")
                    ),
                }
            )
            return {**portfolio_result, **activation_result}

        run_stage("portfolio", config.stages.portfolio, do_portfolio)

        def do_export(record: AlphaCycleStageRecord) -> dict[str, Any]:
            nonlocal export_result
            portfolio_json_path = portfolio_dir / "strategy_portfolio.json"
            if not portfolio_json_path.exists():
                record.status = "skipped"
                record.warnings.append("missing_strategy_portfolio")
                warnings.append("Export stage skipped because no strategy portfolio artifact was available.")
                return {}
            selected_count = int(_summarize_portfolio(portfolio_dir).get("selected_portfolio_strategy_count") or 0)
            if selected_count == 0:
                record.status = "skipped"
                record.warnings.append("empty_strategy_portfolio")
                warnings.append("Export stage skipped because the strategy portfolio selected zero strategies.")
                return {}
            export_source: Path = portfolio_dir
            if activated_portfolio_dir is not None:
                activated_json_path = activated_portfolio_dir / "activated_strategy_portfolio.json"
                if activated_json_path.exists():
                    active_rows = list(load_activated_strategy_portfolio(activated_portfolio_dir).get("active_strategies", []))
                    if not active_rows:
                        record.status = "skipped"
                        record.warnings.append("no_active_activated_strategies")
                        warnings.append("Export stage skipped because the activated strategy portfolio had zero active strategies.")
                        return {}
                    export_source = activated_portfolio_dir
            export_result = export_strategy_portfolio_run_config(
                strategy_portfolio_path=export_source,
                output_dir=export_dir,
            )
            key_artifacts.update({key: str(value) for key, value in export_result.items()})
            return export_result

        run_stage("export_bundle", config.stages.export_bundle, do_export)

        report_record = AlphaCycleStageRecord(stage_name="report")
        report_record.started_at = _now_utc()
        stage_records.append(report_record)
    except Exception:
        report_record = AlphaCycleStageRecord(stage_name="report")
        report_record.started_at = _now_utc()
        stage_records.append(report_record)

    promotion_summary = _summarize_promotions(promoted_dir)
    portfolio_summary = _summarize_portfolio(portfolio_dir)
    activated_summary = _summarize_activated_portfolio(activated_portfolio_dir)
    top_metrics = _top_metrics_from_research(research_result or {})
    ended_at = _now_utc()
    duration_seconds = time.monotonic() - started_clock
    report_json_path, report_md_path, status = _write_summary_artifacts(
        config=config,
        run_dir=run_dir,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        stage_records=stage_records,
        warnings=warnings,
        errors=errors,
        key_artifacts=key_artifacts,
        promotion_summary=promotion_summary,
        portfolio_summary=portfolio_summary,
        activated_summary=activated_summary,
        top_metrics=top_metrics,
    )
    report_record.status = "succeeded"
    report_record.ended_at = _now_utc()
    report_record.duration_seconds = 0.0
    report_record.outputs = {
        "alpha_cycle_summary_json_path": str(report_json_path),
        "alpha_cycle_summary_md_path": str(report_md_path),
    }
    key_artifacts["alpha_cycle_summary_json_path"] = str(report_json_path)
    key_artifacts["alpha_cycle_summary_md_path"] = str(report_md_path)
    key_artifacts = {
        key: value
        for key, value in key_artifacts.items()
        if str(value or "").strip()
    }

    return AlphaCycleResult(
        run_name=config.run_name,
        run_id=config.run_id,
        run_dir=str(run_dir),
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        status=status,
        stage_records=stage_records,
        warnings=warnings,
        errors=errors,
        summary_json_path=str(report_json_path),
        summary_md_path=str(report_md_path),
        key_artifacts=key_artifacts,
    )
