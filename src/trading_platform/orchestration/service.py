from __future__ import annotations

import json
from dataclasses import asdict, replace
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

import pandas as pd

from trading_platform.artifacts.summary_utils import add_standard_summary_fields
from trading_platform.config.loader import (
    load_execution_config,
    load_monitoring_config,
    load_multi_strategy_portfolio_config,
    load_notification_config,
)
from trading_platform.config.models import FeatureConfig, IngestConfig, ResearchWorkflowConfig
from trading_platform.governance.models import RegistrySelectionOptions
from trading_platform.governance.persistence import (
    get_registry_entry,
    load_governance_criteria_config,
    load_strategy_registry,
    save_strategy_registry,
)
from trading_platform.governance.service import (
    build_multi_strategy_config_from_registry,
    evaluate_promotion,
    promote_registry_entry,
    write_decision_artifacts,
    write_registry_backed_multi_strategy_artifacts,
)
from trading_platform.live.preview import (
    LivePreviewConfig,
    run_live_dry_run_preview_for_targets,
    write_live_dry_run_artifacts,
)
from trading_platform.monitoring.notification_service import send_notifications
from trading_platform.monitoring.service import evaluate_run_health_snapshot
from trading_platform.orchestration.models import (
    PIPELINE_STAGE_NAMES,
    PipelineRunConfig,
    PipelineRunResult,
    PipelineStageRecord,
)
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
from trading_platform.portfolio.strategy_execution_handoff import (
    StrategyExecutionHandoffConfig,
    resolve_strategy_execution_handoff,
    write_strategy_execution_handoff_summary,
)
from trading_platform.research.experiment_tracking import (
    build_paper_experiment_record,
    register_experiment,
)
from trading_platform.services.feature_service import run_feature_build
from trading_platform.services.ingest_service import run_ingest
from trading_platform.services.universe_research_service import run_universe_research_workflow
from trading_platform.universes.registry import get_universe_symbols


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _sanitize_timestamp(timestamp: str) -> str:
    return timestamp.replace(":", "-")


class StageExecutionError(RuntimeError):
    pass


def _union_symbols(universes: list[str]) -> list[str]:
    symbols: list[str] = []
    for universe in universes:
        for symbol in get_universe_symbols(universe):
            if symbol not in symbols:
                symbols.append(symbol)
    return symbols


def _ensure_path(base_dir: Path, maybe_path: str | None, fallback_name: str) -> Path:
    if maybe_path:
        path = Path(maybe_path)
        return path if path.is_absolute() else base_dir / path
    return base_dir / fallback_name


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
        "requested_active_strategy_count": allocation_result.summary.get("requested_active_strategy_count"),
        "requested_symbol_count": allocation_result.summary.get("requested_symbol_count"),
        "pre_validation_target_symbol_count": allocation_result.summary.get("pre_validation_target_symbol_count"),
        "post_validation_target_symbol_count": len(allocation_result.combined_target_weights),
        "usable_symbol_count": allocation_result.summary.get("usable_symbol_count"),
        "skipped_symbol_count": allocation_result.summary.get("skipped_symbol_count"),
        "target_drop_stage": allocation_result.summary.get("target_drop_stage"),
        "zero_target_reason": allocation_result.summary.get("zero_target_reason"),
        "target_drop_reason": allocation_result.summary.get("target_drop_reason"),
        "latest_price_source_summary": allocation_result.summary.get("latest_price_source_summary", {}),
        "generated_preset_path": allocation_result.summary.get("generated_preset_path"),
        "signal_artifact_path": allocation_result.summary.get("signal_artifact_path"),
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
        "turnover_buffer_blocked_replacements": sum(
            int(bundle.diagnostics.get("turnover_buffer_blocked_replacements") or 0)
            for bundle in allocation_result.sleeve_bundles
        ),
        "semantic_warning": "portfolio_constraints_applied"
        if allocation_result.summary["symbols_removed_or_clipped"]
        else "",
        "target_selected_count": len(allocation_result.combined_target_weights),
        "summary": {"mean_turnover": allocation_result.summary["turnover_estimate"]},
        "multi_strategy_allocation": allocation_result.summary,
    }


def _apply_execution_validation_policy(portfolio_config, config: PipelineRunConfig):
    if hasattr(portfolio_config, "__dataclass_fields__"):
        return replace(
            portfolio_config,
            fail_if_no_usable_symbols=config.fail_if_no_usable_symbols,
            fail_if_zero_targets_after_validation=config.fail_if_zero_targets_after_validation,
            allow_latest_close_fallback=config.allow_latest_close_fallback,
            min_usable_symbol_fraction=config.min_usable_symbol_fraction,
        )
    for name, value in {
        "fail_if_no_usable_symbols": config.fail_if_no_usable_symbols,
        "fail_if_zero_targets_after_validation": config.fail_if_zero_targets_after_validation,
        "allow_latest_close_fallback": config.allow_latest_close_fallback,
        "min_usable_symbol_fraction": config.min_usable_symbol_fraction,
    }.items():
        setattr(portfolio_config, name, value)
    return portfolio_config


def _render_run_summary_markdown(result: PipelineRunResult) -> str:
    lines = [
        f"# Pipeline Run Summary: {result.run_name}",
        "",
        f"- Schedule type: `{result.schedule_type}`",
        f"- Started: `{result.started_at}`",
        f"- Ended: `{result.ended_at}`",
        f"- Status: `{result.status}`",
        "",
        "## Stages",
    ]
    for record in result.stage_records:
        lines.append(
            f"- `{record.stage_name}`: status=`{record.status}` duration=`{record.duration_seconds}` error=`{record.error_message or ''}`"
        )
    promoted = result.outputs.get("promoted_strategy_ids", [])
    if promoted:
        lines.extend(["", "## Promoted Strategies"])
        for strategy_id in promoted:
            lines.append(f"- `{strategy_id}`")
    selected = result.outputs.get("multi_strategy_selected_strategies", [])
    if selected:
        lines.extend(["", "## Multi-Strategy Portfolio"])
        for strategy_id in selected:
            lines.append(f"- `{strategy_id}`")
    paper_summary = result.outputs.get("paper_summary", {})
    if paper_summary:
        lines.extend(
            [
                "",
                "## Paper Summary",
                f"- Equity: `{paper_summary.get('current_equity', paper_summary.get('equity'))}`",
                f"- Gross exposure: `{paper_summary.get('gross_exposure')}`",
                f"- Turnover estimate: `{paper_summary.get('turnover_estimate')}`",
            ]
        )
    live_summary = result.outputs.get("live_summary", {})
    if live_summary:
        lines.extend(
            [
                "",
                "## Live Dry-Run Summary",
                f"- Readiness: `{live_summary.get('readiness', live_summary.get('adjusted_order_count'))}`",
                f"- Proposed orders: `{live_summary.get('proposed_order_count', live_summary.get('adjusted_order_count'))}`",
                f"- Gross exposure: `{live_summary.get('gross_exposure')}`",
            ]
        )
    monitoring_health = result.outputs.get("monitoring_health_status")
    if monitoring_health:
        alert_counts = result.outputs.get("monitoring_alert_counts", {})
        critical_alerts = result.outputs.get("monitoring_critical_alerts", [])
        lines.extend(
            [
                "",
                "## Monitoring",
                f"- Health status: `{monitoring_health}`",
                f"- Alert counts: `info={alert_counts.get('info', 0)} warning={alert_counts.get('warning', 0)} critical={alert_counts.get('critical', 0)}`",
                f"- Notification sent: `{result.outputs.get('notification_sent', False)}`",
            ]
        )
        if critical_alerts:
            lines.append("- Critical alerts:")
            for message in critical_alerts:
                lines.append(f"  - {message}")
    return "\n".join(lines) + "\n"


def _write_pipeline_artifacts(
    *,
    config: PipelineRunConfig,
    result: PipelineRunResult,
    run_dir: Path,
) -> dict[str, Path]:
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_json = run_dir / "run_summary.json"
    summary_md = run_dir / "run_summary.md"
    stage_status_csv = run_dir / "stage_status.csv"
    config_snapshot = run_dir / "pipeline_config_snapshot.json"
    errors_json = run_dir / "errors.json"

    summary_payload = add_standard_summary_fields(
        result.to_dict(),
        summary_type="pipeline_run",
        timestamp=result.ended_at,
        status=result.status,
        key_counts={
            "stage_count": len(result.stage_records),
            "failed_stage_count": sum(1 for record in result.stage_records if record.status == "failed"),
            "error_count": len(result.errors),
        },
        key_metrics={
            "duration_seconds": sum(record.duration_seconds or 0.0 for record in result.stage_records),
        },
        warnings=[],
        errors=[str(item.get("error_message", "")) for item in result.errors if item.get("error_message")],
    )
    summary_json.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    summary_md.write_text(_render_run_summary_markdown(result), encoding="utf-8")
    pd.DataFrame([record.to_dict() for record in result.stage_records]).to_csv(stage_status_csv, index=False)
    config_snapshot.write_text(json.dumps(config.to_dict(), indent=2, default=str), encoding="utf-8")
    if result.errors:
        errors_json.write_text(json.dumps(result.errors, indent=2, default=str), encoding="utf-8")

    paths = {
        "run_summary_json_path": summary_json,
        "run_summary_md_path": summary_md,
        "stage_status_path": stage_status_csv,
        "pipeline_config_snapshot_path": config_snapshot,
    }
    if result.errors:
        paths["errors_path"] = errors_json
    summary_payload["artifact_paths"] = {name: str(path) for name, path in paths.items()}
    summary_json.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    return paths


def _run_data_refresh_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    symbols = context["symbols"]
    output_dir = run_dir / "data_refresh"
    output_dir.mkdir(parents=True, exist_ok=True)
    normalized_paths: dict[str, str] = {}
    for symbol in symbols:
        path = run_ingest(
            IngestConfig(
                symbol=symbol,
                start=config.data_start,
                end=config.data_end,
                interval=config.data_interval,
            )
        )
        normalized_paths[symbol] = str(path)
    return {"normalized_paths": normalized_paths, "symbol_count": len(symbols)}


def _run_feature_generation_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    symbols = context["symbols"]
    output_dir = run_dir / "feature_generation"
    output_dir.mkdir(parents=True, exist_ok=True)
    feature_paths: dict[str, str] = {}
    for symbol in symbols:
        path = run_feature_build(
            FeatureConfig(
                symbol=symbol,
                feature_groups=config.feature_groups,
            )
        )
        feature_paths[symbol] = str(path)
    return {"feature_paths": feature_paths, "symbol_count": len(symbols)}


def _run_research_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    output_dir = run_dir / "research"
    output_dir.mkdir(parents=True, exist_ok=True)
    if config.research_strategy == "xsec_momentum_topn":
        placeholder_path = output_dir / "research_placeholder.json"
        payload = {
            "implemented": False,
            "reason": "xsec_momentum_topn orchestration research stage is not yet wired to a direct service-layer runner",
            "strategy": config.research_strategy,
            "symbols": context["symbols"],
        }
        placeholder_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {
            "implementation_mode": "placeholder",
            "placeholder_artifact_path": str(placeholder_path),
        }

    base_config = ResearchWorkflowConfig(
        symbol="PLACEHOLDER",
        start=config.data_start,
        end=config.data_end,
        interval=config.data_interval,
        feature_groups=config.feature_groups,
        strategy=config.research_strategy,
        fast=config.research_fast,
        slow=config.research_slow,
        lookback=config.research_lookback,
        cash=config.research_cash,
        commission=config.research_commission,
    )
    result = run_universe_research_workflow(
        symbols=context["symbols"],
        base_config=base_config,
        continue_on_error=True,
    )
    snapshot_path = output_dir / "research_stage_summary.json"
    snapshot_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    return {
        "implementation_mode": "service",
        "results_count": len(result["results"]),
        "errors_count": len(result["errors"]),
        "research_summary_path": str(snapshot_path),
    }


def _run_promotion_evaluation_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    registry = load_strategy_registry(config.registry_path)
    criteria = load_governance_criteria_config(config.governance_config_path).promotion
    output_dir = run_dir / "promotion_evaluation"
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_entries = [
        entry
        for entry in registry.entries
        if entry.status == "candidate"
        and (not config.preset_filters or entry.preset_name in config.preset_filters)
    ]

    batch_rows: list[dict[str, Any]] = []
    promoted_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    passing_strategy_ids: list[str] = []
    decision_paths: dict[str, dict[str, str]] = {}
    for entry in candidate_entries:
        report, snapshot = evaluate_promotion(entry=entry, criteria=criteria)
        strategy_dir = output_dir / entry.strategy_id
        paths = write_decision_artifacts(
            report=report,
            snapshot=snapshot,
            output_dir=strategy_dir,
            prefix="promotion_decision",
        )
        decision_paths[entry.strategy_id] = {name: str(path) for name, path in paths.items()}
        row = {
            "strategy_id": entry.strategy_id,
            "preset_name": entry.preset_name,
            "passed": report.passed,
            "recommendation": report.recommendation,
            "failed_criteria": "|".join(report.failed_criteria),
        }
        batch_rows.append(row)
        if report.passed:
            promoted_rows.append(row)
            passing_strategy_ids.append(entry.strategy_id)
        else:
            rejected_rows.append(row)

    summary_csv = output_dir / "promotion_batch_summary.csv"
    promoted_csv = output_dir / "promoted_strategies.csv"
    rejected_csv = output_dir / "rejected_strategies.csv"
    pd.DataFrame(batch_rows).to_csv(summary_csv, index=False)
    pd.DataFrame(promoted_rows).to_csv(promoted_csv, index=False)
    pd.DataFrame(rejected_rows).to_csv(rejected_csv, index=False)

    context["promotion_candidates_passed"] = passing_strategy_ids
    return {
        "candidate_count": len(candidate_entries),
        "promoted_count": len(promoted_rows),
        "rejected_count": len(rejected_rows),
        "promotion_batch_summary_path": str(summary_csv),
        "promoted_strategies_path": str(promoted_csv),
        "rejected_strategies_path": str(rejected_csv),
        "decision_paths": decision_paths,
    }


def _run_registry_mutation_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    registry = load_strategy_registry(config.registry_path)
    promoted_strategy_ids: list[str] = []
    if config.auto_promote_qualifying_candidates:
        for strategy_id in context.get("promotion_candidates_passed", []):
            registry = promote_registry_entry(
                registry=registry,
                strategy_id=strategy_id,
                note="orchestration auto promotion",
            )
            promoted_strategy_ids.append(strategy_id)
        save_strategy_registry(registry, config.registry_path)
    audit_path = run_dir / "registry_mutation" / "registry_snapshot.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps(registry.to_dict(), indent=2, default=str), encoding="utf-8")
    context["mutated_registry"] = registry
    context["promoted_strategy_ids"] = promoted_strategy_ids
    return {
        "mutated_registry_path": str(audit_path),
        "promoted_strategy_ids": promoted_strategy_ids,
    }


def _run_multi_strategy_config_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    registry = context.get("mutated_registry") or load_strategy_registry(config.registry_path)
    output_path = _ensure_path(run_dir, config.multi_strategy_output_path, "generated_multi_strategy.json")
    options = RegistrySelectionOptions(
        include_statuses=["approved", "paper"]
        if config.registry_include_paper_strategies
        else ["approved"],
        max_strategies=config.registry_max_strategies,
        weighting_scheme=config.registry_selection_weighting_scheme,
    )
    multi_strategy_config, family_rows = build_multi_strategy_config_from_registry(
        registry=registry,
        options=options,
    )
    artifact_paths = write_registry_backed_multi_strategy_artifacts(
        config=multi_strategy_config,
        family_rows=family_rows,
        output_path=output_path,
    )
    context["multi_strategy_config_path"] = str(output_path)
    context["multi_strategy_selected_strategies"] = [
        sleeve.sleeve_name for sleeve in multi_strategy_config.sleeves
    ]
    return {name: str(path) for name, path in artifact_paths.items()}


def _run_portfolio_allocation_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    output_dir = run_dir / "portfolio_allocation"
    handoff = resolve_strategy_execution_handoff(
        context["multi_strategy_config_path"],
        config=StrategyExecutionHandoffConfig(
            use_activated_portfolio_for_paper=config.use_activated_portfolio_for_paper,
            fail_if_no_active_strategies=config.fail_if_no_active_strategies,
            include_inactive_conditionals_in_reports=config.include_inactive_conditionals_in_reports,
        ),
    )
    handoff_summary_path = write_strategy_execution_handoff_summary(
        handoff=handoff,
        output_dir=output_dir,
        artifact_name="execution_active_strategy_summary.json",
    )
    portfolio_config = handoff.portfolio_config
    if portfolio_config is None:
        if config.fail_if_no_active_strategies:
            raise StageExecutionError("No active strategies available for portfolio allocation")
        return {
            "execution_active_strategy_summary_path": str(handoff_summary_path),
            "enabled_sleeve_count": 0,
            "active_strategy_count": 0,
            "skip_reason": "no_active_strategies",
        }
    portfolio_config = _apply_execution_validation_policy(portfolio_config, config)
    allocation_result = allocate_multi_strategy_portfolio(portfolio_config)
    artifact_paths = write_multi_strategy_artifacts(
        allocation_result,
        output_dir,
    )
    context["allocation_result"] = allocation_result
    context["allocation_paths"] = artifact_paths
    return {
        **{name: str(path) for name, path in artifact_paths.items()},
        "enabled_sleeve_count": allocation_result.summary["enabled_sleeve_count"],
        "execution_active_strategy_summary_path": str(handoff_summary_path),
        "active_strategy_count": handoff.summary.get("active_strategy_count", allocation_result.summary["enabled_sleeve_count"]),
    }


def _run_paper_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    handoff = resolve_strategy_execution_handoff(
        context["multi_strategy_config_path"],
        config=StrategyExecutionHandoffConfig(
            use_activated_portfolio_for_paper=config.use_activated_portfolio_for_paper,
            fail_if_no_active_strategies=config.fail_if_no_active_strategies,
            include_inactive_conditionals_in_reports=config.include_inactive_conditionals_in_reports,
        ),
    )
    paper_output_dir = run_dir / "paper_trading"
    paper_output_dir.mkdir(parents=True, exist_ok=True)
    handoff_summary_path = write_strategy_execution_handoff_summary(
        handoff=handoff,
        output_dir=paper_output_dir,
        artifact_name="paper_active_strategy_summary.json",
    )
    portfolio_config = handoff.portfolio_config
    if portfolio_config is None:
        if config.fail_if_no_active_strategies:
            raise StageExecutionError("No active strategies available for paper trading")
        return {
            "paper_active_strategy_summary_path": str(handoff_summary_path),
            "paper_summary": {"skip_reason": "no_active_strategies", **handoff.summary},
        }
    portfolio_config = _apply_execution_validation_policy(portfolio_config, config)
    allocation_result = context.get("allocation_result") or allocate_multi_strategy_portfolio(portfolio_config)
    allocation_paths = context.get("allocation_paths") or write_multi_strategy_artifacts(
        allocation_result,
        run_dir / "portfolio_allocation",
    )
    execution_config = load_execution_config(config.execution_config_path) if config.execution_config_path else None
    paper_config = _build_multi_strategy_paper_config(
        allocation_result,
        reserve_cash_pct=portfolio_config.cash_reserve_pct,
    )
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
        target_diagnostics=_build_multi_strategy_target_diagnostics(allocation_result)
        | {"strategy_execution_handoff": handoff.summary},
        skipped_symbols=sorted(
            {
                str(row["symbol"])
                for row in getattr(allocation_result, "execution_symbol_coverage_rows", [])
                if str(row.get("skip_reason") or "")
            }
        ),
        extra_diagnostics={
            "multi_strategy_allocation": allocation_result.summary,
            "strategy_execution_handoff": handoff.summary,
        },
        execution_config=execution_config,
        auto_apply_fills=False,
    )
    paper_paths = write_paper_trading_artifacts(result=result, output_dir=paper_output_dir)
    persistence_paths, health_checks, latest_summary = persist_paper_run_outputs(
        result=result,
        config=paper_config,
        output_dir=paper_output_dir,
        state_file_preexisting=state_preexisting,
    )
    tracker_dir = run_dir / "experiment_tracking"
    registry_paths = register_experiment(
        build_paper_experiment_record(paper_output_dir),
        tracker_dir=tracker_dir,
    )
    context["paper_summary"] = latest_summary
    return {
        **{name: str(path) for name, path in allocation_paths.items()},
        **{name: str(path) for name, path in paper_paths.items()},
        **{name: str(path) for name, path in persistence_paths.items()},
        "paper_active_strategy_summary_path": str(handoff_summary_path),
        "experiment_registry_path": registry_paths["experiment_registry_path"],
        "paper_summary": latest_summary,
        "health_check_count": len(health_checks),
    }


def _run_live_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    live_output_dir = run_dir / "live_dry_run"
    live_output_dir.mkdir(parents=True, exist_ok=True)
    handoff = resolve_strategy_execution_handoff(
        context["multi_strategy_config_path"],
        config=StrategyExecutionHandoffConfig(
            use_activated_portfolio_for_paper=config.use_activated_portfolio_for_paper,
            fail_if_no_active_strategies=config.fail_if_no_active_strategies,
            include_inactive_conditionals_in_reports=config.include_inactive_conditionals_in_reports,
        ),
    )
    handoff_summary_path = write_strategy_execution_handoff_summary(
        handoff=handoff,
        output_dir=live_output_dir,
        artifact_name="live_active_strategy_summary.json",
    )
    portfolio_config = handoff.portfolio_config
    if portfolio_config is None:
        if config.fail_if_no_active_strategies:
            raise StageExecutionError("No active strategies available for live dry-run")
        return {
            "live_active_strategy_summary_path": str(handoff_summary_path),
            "live_summary": {"skip_reason": "no_active_strategies", **handoff.summary},
        }
    portfolio_config = _apply_execution_validation_policy(portfolio_config, config)
    allocation_result = context.get("allocation_result") or allocate_multi_strategy_portfolio(portfolio_config)
    allocation_paths = context.get("allocation_paths") or write_multi_strategy_artifacts(
        allocation_result,
        run_dir / "portfolio_allocation",
    )
    execution_config = load_execution_config(config.execution_config_path) if config.execution_config_path else None
    target_diagnostics = _build_multi_strategy_target_diagnostics(allocation_result) | {
        "strategy_execution_handoff": handoff.summary,
    }
    preview_config = LivePreviewConfig(
        symbols=sorted(allocation_result.combined_target_weights),
        preset_name="multi_strategy",
        universe_name=f"{allocation_result.summary['enabled_sleeve_count']}_sleeves",
        strategy="multi_strategy",
        reserve_cash_pct=portfolio_config.cash_reserve_pct,
        broker=config.live_broker,
        output_dir=live_output_dir,
    )
    result = run_live_dry_run_preview_for_targets(
        config=preview_config,
        as_of=allocation_result.as_of,
        target_weights=allocation_result.combined_target_weights,
        latest_prices=allocation_result.latest_prices,
        target_diagnostics=target_diagnostics,
        execution_config=execution_config,
    )
    live_paths = write_live_dry_run_artifacts(result)
    summary_payload = json.loads(
        (preview_config.output_dir / "live_dry_run_summary.json").read_text(encoding="utf-8")
    )
    context["live_summary"] = summary_payload
    return {
        **{name: str(path) for name, path in allocation_paths.items()},
        **{name: str(path) for name, path in live_paths.items()},
        "live_active_strategy_summary_path": str(handoff_summary_path),
        "live_summary": summary_payload,
    }


def _run_reporting_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    report_path = run_dir / "reporting" / "final_report_context.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "promoted_strategy_ids": context.get("promoted_strategy_ids", []),
        "multi_strategy_selected_strategies": context.get("multi_strategy_selected_strategies", []),
        "paper_summary": context.get("paper_summary", {}),
        "live_summary": context.get("live_summary", {}),
    }
    report_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return {"reporting_context_path": str(report_path)}


def _run_monitoring_stage(config: PipelineRunConfig, run_dir: Path, context: dict[str, Any]) -> dict[str, Any]:
    monitoring_config = load_monitoring_config(config.monitoring_config_path)
    stage_snapshot = [
        record.to_dict()
        for record in context.get("stage_records_snapshot", [])
        if record.stage_name != "monitoring"
    ]
    run_payload = {
        "run_name": config.run_name,
        "schedule_type": config.schedule_type,
        "started_at": context.get("run_started_at"),
        "ended_at": _now_utc(),
        "status": "failed" if context.get("pipeline_errors") else "succeeded",
        "run_dir": str(run_dir),
        "stage_records": stage_snapshot,
        "errors": context.get("pipeline_errors", []),
        "outputs": {
            **context.get("pipeline_outputs", {}),
            "promoted_strategy_ids": context.get("promoted_strategy_ids", []),
            "multi_strategy_selected_strategies": context.get("multi_strategy_selected_strategies", []),
            "paper_summary": context.get("paper_summary", {}),
            "live_summary": context.get("live_summary", {}),
        },
    }
    report, paths = evaluate_run_health_snapshot(
        run_dir=run_dir,
        run_payload=run_payload,
        config=monitoring_config,
        output_dir=run_dir / "monitoring",
    )
    context["monitoring_health_status"] = report.status
    context["monitoring_alert_counts"] = report.alert_counts
    context["monitoring_critical_alerts"] = [
        alert.message for alert in report.alerts if alert.severity == "critical"
    ]
    outputs = {
        **{name: str(path) for name, path in paths.items()},
        "monitoring_health_status": report.status,
        "monitoring_alert_counts": report.alert_counts,
        "monitoring_critical_alerts": context["monitoring_critical_alerts"],
    }
    if config.notification_config_path:
        notification_config = load_notification_config(config.notification_config_path)
        notification_result = send_notifications(
            alerts=report.alerts,
            config=notification_config,
        )
        notification_summary_path = run_dir / "monitoring" / "notification_summary.json"
        notification_summary_path.write_text(
            json.dumps(notification_result, indent=2, default=str),
            encoding="utf-8",
        )
        context["notification_sent"] = bool(notification_result.get("sent"))
        outputs["notification_summary_path"] = str(notification_summary_path)
        outputs["notification_sent"] = context["notification_sent"]
    return outputs


STAGE_HANDLERS: dict[str, Callable[[PipelineRunConfig, Path, dict[str, Any]], dict[str, Any]]] = {
    "data_refresh": _run_data_refresh_stage,
    "feature_generation": _run_feature_generation_stage,
    "research": _run_research_stage,
    "promotion_evaluation": _run_promotion_evaluation_stage,
    "registry_mutation": _run_registry_mutation_stage,
    "multi_strategy_config_generation": _run_multi_strategy_config_stage,
    "portfolio_allocation": _run_portfolio_allocation_stage,
    "paper_trading": _run_paper_stage,
    "live_dry_run": _run_live_stage,
    "reporting": _run_reporting_stage,
    "monitoring": _run_monitoring_stage,
}


def run_orchestration_pipeline(config: PipelineRunConfig) -> tuple[PipelineRunResult, dict[str, Path]]:
    started_at = _now_utc()
    run_dir = Path(config.output_root_dir) / config.run_name / _sanitize_timestamp(started_at)
    run_dir.mkdir(parents=True, exist_ok=True)
    context: dict[str, Any] = {
        "symbols": _union_symbols(config.universes),
        "run_started_at": started_at,
    }
    if config.multi_strategy_input_path:
        context["multi_strategy_config_path"] = str(config.multi_strategy_input_path)
    stage_records = [PipelineStageRecord(stage_name=stage_name) for stage_name in config.stage_order]
    errors: list[dict[str, Any]] = []
    outputs: dict[str, Any] = {}

    for record in stage_records:
        enabled = bool(getattr(config.stages, record.stage_name))
        if not enabled:
            record.status = "skipped"
            continue

        handler = STAGE_HANDLERS[record.stage_name]
        record.status = "running"
        record.started_at = _now_utc()
        context["stage_records_snapshot"] = stage_records
        context["pipeline_errors"] = errors
        context["pipeline_outputs"] = outputs
        record.inputs = {
            "run_dir": str(run_dir),
            "symbols": context.get("symbols", []),
        }
        start_clock = perf_counter()
        try:
            stage_outputs = handler(config, run_dir, context)
            record.outputs = stage_outputs
            outputs[record.stage_name] = stage_outputs
            if isinstance(stage_outputs, dict):
                outputs.update(
                    {
                        key: value
                        for key, value in stage_outputs.items()
                        if key
                        in {
                            "promoted_strategy_ids",
                            "paper_summary",
                            "live_summary",
                            "monitoring_health_status",
                            "monitoring_alert_counts",
                            "monitoring_critical_alerts",
                            "notification_sent",
                        }
                    }
                )
                if "promoted_strategy_ids" in stage_outputs:
                    context["promoted_strategy_ids"] = stage_outputs["promoted_strategy_ids"]
            record.status = "succeeded"
        except Exception as exc:
            record.status = "failed"
            record.error_message = f"{type(exc).__name__}: {exc}"
            errors.append(
                {
                    "stage_name": record.stage_name,
                    "error_message": record.error_message,
                }
            )
            if config.fail_fast or not config.continue_on_stage_error:
                record.ended_at = _now_utc()
                record.duration_seconds = round(perf_counter() - start_clock, 6)
                break
        finally:
            if record.ended_at is None:
                record.ended_at = _now_utc()
            record.duration_seconds = round(perf_counter() - start_clock, 6)

        if record.status == "failed" and (config.fail_fast or not config.continue_on_stage_error):
            break

    ended_at = _now_utc()
    status = "failed" if errors else "succeeded"
    result = PipelineRunResult(
        run_name=config.run_name,
        schedule_type=config.schedule_type,
        started_at=started_at,
        ended_at=ended_at,
        status=status,
        run_dir=str(run_dir),
        stage_records=stage_records,
        errors=errors,
        outputs={
            **outputs,
            "promoted_strategy_ids": context.get("promoted_strategy_ids", []),
            "multi_strategy_selected_strategies": context.get("multi_strategy_selected_strategies", []),
            "paper_summary": context.get("paper_summary", {}),
            "live_summary": context.get("live_summary", {}),
            "monitoring_health_status": context.get("monitoring_health_status"),
            "monitoring_alert_counts": context.get("monitoring_alert_counts", {}),
            "monitoring_critical_alerts": context.get("monitoring_critical_alerts", []),
            "notification_sent": context.get("notification_sent", False),
        },
    )
    artifact_paths = _write_pipeline_artifacts(config=config, result=result, run_dir=run_dir)
    return result, artifact_paths
