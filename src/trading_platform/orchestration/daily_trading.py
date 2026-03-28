from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.cli.common import UNIVERSES
from trading_platform.config.loader import (
    load_alpha_research_workflow_config,
    load_execution_config,
    load_promotion_policy_config,
    load_research_input_refresh_workflow_config,
    load_strategy_portfolio_policy_config,
)
from trading_platform.config.workflow_models import (
    AlphaResearchWorkflowConfig,
    DailyTradingWorkflowConfig,
    ResearchInputRefreshWorkflowConfig,
)
from trading_platform.dashboard.server import build_dashboard_static_data
from trading_platform.db.services import DatabaseLineageService, build_research_memory_service
from trading_platform.decision_journal.models import DecisionJournalBundle
from trading_platform.paper.models import PaperTradingConfig
from trading_platform.paper.persistence import persist_paper_run_outputs
from trading_platform.paper.service import (
    JsonPaperStateStore,
    run_paper_trading_cycle_for_targets,
    write_paper_trading_artifacts,
)
from trading_platform.portfolio.conditional_activation import (
    ConditionalActivationConfig,
    activate_strategy_portfolio,
    load_activated_strategy_portfolio,
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
from trading_platform.portfolio.strategy_portfolio import (
    build_strategy_portfolio,
    export_strategy_portfolio_run_config,
    load_strategy_portfolio,
)
from trading_platform.reporting.paper_account_report import (
    build_paper_account_report,
    write_paper_account_report,
)
from trading_platform.reporting.pnl_attribution import build_symbol_strategy_provenance
from trading_platform.reporting.strategy_quality_report import (
    build_strategy_quality_report,
    write_strategy_quality_report,
)
from trading_platform.research.alpha_lab.runner import refresh_alpha_research_artifacts, run_alpha_research
from trading_platform.research.promotion_pipeline import apply_research_promotions
from trading_platform.research.registry import refresh_research_registry_bundle
from trading_platform.services.research_input_refresh_service import (
    ResearchInputRefreshRequest,
    refresh_research_inputs,
)


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class DailyTradingStageRecord:
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
class DailyTradingResult:
    run_name: str
    run_id: str | None
    run_dir: str
    started_at: str
    ended_at: str
    duration_seconds: float
    status: str
    stage_records: list[DailyTradingStageRecord]
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
        fundamentals_artifact_root=(
            Path(config.fundamentals_artifact_root) if config.fundamentals_artifact_root else None
        ),
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
        "fundamentals_daily_features_path": (
            Path(config.fundamentals_daily_features_path) if config.fundamentals_daily_features_path else None
        ),
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
        "require_runtime_computability_for_approval": config.require_runtime_computability_for_approval,
        "min_runtime_computable_symbols_for_approval": config.min_runtime_computable_symbols_for_approval,
        "allow_research_only_noncomputable_candidates": config.allow_research_only_noncomputable_candidates,
        "runtime_computability_penalty_on_ranking": config.runtime_computability_penalty_on_ranking,
        "runtime_computability_check_mode": config.runtime_computability_check_mode,
        "require_composite_runtime_computability_for_approval": config.require_composite_runtime_computability_for_approval,
        "min_composite_runtime_computable_symbols_for_approval": config.min_composite_runtime_computable_symbols_for_approval,
        "allow_research_only_noncomputable_composites": config.allow_research_only_noncomputable_composites,
        "composite_runtime_computability_check_mode": config.composite_runtime_computability_check_mode,
        "composite_runtime_computability_penalty_on_ranking": config.composite_runtime_computability_penalty_on_ranking,
        "fast_refresh_mode": config.fast_refresh_mode,
        "skip_heavy_diagnostics": config.skip_heavy_diagnostics,
        "reuse_existing_fold_results": config.reuse_existing_fold_results,
        "restrict_to_existing_candidates": config.restrict_to_existing_candidates,
        "max_families_for_refresh": config.max_families_for_refresh,
        "max_candidates_for_refresh": config.max_candidates_for_refresh,
    }


def _stage_path(path_value: str | None, default_path: Path) -> Path:
    return Path(path_value) if path_value else default_path


def _summarize_promotions(promoted_dir: Path) -> dict[str, Any]:
    index_path = promoted_dir / "promoted_strategies.json"
    if not index_path.exists():
        return {
            "promoted_strategy_count": 0,
            "promoted_unconditional_count": 0,
            "promoted_conditional_count": 0,
        }
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    strategies = list(payload.get("strategies", []))
    return {
        "promoted_strategy_count": len(strategies),
        "promoted_unconditional_count": sum(
            1 for row in strategies if str(row.get("promotion_variant") or "unconditional") != "conditional"
        ),
        "promoted_conditional_count": sum(
            1 for row in strategies if str(row.get("promotion_variant") or "") == "conditional"
        ),
    }


def _resolve_strategy_weighting_metrics_path(config: DailyTradingWorkflowConfig) -> Path | None:
    if config.strategy_weighting_metrics_path:
        candidate = Path(config.strategy_weighting_metrics_path)
        return candidate if candidate.exists() else None
    candidate = Path(config.paper_output_dir) / "strategy_pnl_attribution.csv"
    return candidate if candidate.exists() else None


def _summarize_portfolio(portfolio_dir: Path) -> dict[str, Any]:
    try:
        payload = load_strategy_portfolio(portfolio_dir)
    except FileNotFoundError:
        return {"selected_portfolio_strategy_count": 0}
    selected_rows = list(payload.get("selected_strategies", []))
    summary = dict(payload.get("summary") or {})
    return {
        "selected_portfolio_strategy_count": len(selected_rows),
        "selected_conditional_portfolio_count": int(summary.get("selected_conditional_variant_count") or 0),
    }


def _summarize_activated_portfolio(activated_dir: Path | None) -> dict[str, Any]:
    if activated_dir is None:
        return {
            "active_strategy_count": 0,
            "activated_unconditional_count": 0,
            "activated_conditional_count": 0,
            "inactive_conditional_count": 0,
        }
    try:
        payload = load_activated_strategy_portfolio(activated_dir)
    except FileNotFoundError:
        return {
            "active_strategy_count": 0,
            "activated_unconditional_count": 0,
            "activated_conditional_count": 0,
            "inactive_conditional_count": 0,
        }
    summary = dict(payload.get("summary") or {})
    return {
        "active_strategy_count": int(summary.get("active_row_count") or 0),
        "activated_unconditional_count": int(summary.get("activated_unconditional_count") or 0),
        "activated_conditional_count": int(summary.get("activated_conditional_count") or 0),
        "inactive_conditional_count": int(summary.get("inactive_conditional_count") or 0),
    }


def _summarize_paper_run(paper_output_dir: Path) -> dict[str, Any]:
    summary_path = paper_output_dir / "paper_run_summary_latest.json"
    attribution_summary_path = paper_output_dir / "pnl_attribution_summary.json"
    if not summary_path.exists():
        return {
            "requested_symbol_count": 0,
            "usable_symbol_count": 0,
            "pre_validation_target_symbol_count": 0,
            "post_validation_target_symbol_count": 0,
            "executable_order_count": 0,
            "skipped_trades_count": 0,
            "skipped_turnover": 0.0,
            "effective_turnover_reduction": 0.0,
            "blocked_entries_count": 0,
            "held_in_hold_zone_count": 0,
            "forced_exit_count": 0,
            "skipped_due_to_entry_band_count": 0,
            "skipped_due_to_hold_zone_count": 0,
            "score_band_enabled": False,
            "entry_threshold_used": None,
            "exit_threshold_used": None,
            "fill_count": 0,
            "zero_target_reason": "",
            "attribution_summary": {},
        }
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_payload = dict(payload.get("summary") or payload)
    attribution_payload = (
        json.loads(attribution_summary_path.read_text(encoding="utf-8")) if attribution_summary_path.exists() else {}
    )
    return {
        "requested_symbol_count": int(summary_payload.get("requested_symbol_count") or 0),
        "usable_symbol_count": int(summary_payload.get("usable_symbol_count") or 0),
        "pre_validation_target_symbol_count": int(summary_payload.get("pre_validation_target_symbol_count") or 0),
        "post_validation_target_symbol_count": int(summary_payload.get("post_validation_target_symbol_count") or 0),
        "executable_order_count": int(summary_payload.get("executable_order_count") or 0),
        "skipped_trades_count": int(summary_payload.get("skipped_trades_count") or 0),
        "skipped_turnover": float(summary_payload.get("skipped_turnover") or 0.0),
        "effective_turnover_reduction": float(summary_payload.get("effective_turnover_reduction") or 0.0),
        "blocked_entries_count": int(summary_payload.get("blocked_entries_count") or 0),
        "held_in_hold_zone_count": int(summary_payload.get("held_in_hold_zone_count") or 0),
        "forced_exit_count": int(summary_payload.get("forced_exit_count") or 0),
        "skipped_due_to_entry_band_count": int(summary_payload.get("skipped_due_to_entry_band_count") or 0),
        "skipped_due_to_hold_zone_count": int(summary_payload.get("skipped_due_to_hold_zone_count") or 0),
        "score_band_enabled": bool(summary_payload.get("score_band_enabled", False)),
        "entry_threshold_used": summary_payload.get("entry_threshold_used"),
        "exit_threshold_used": summary_payload.get("exit_threshold_used"),
        "fill_count": int(summary_payload.get("fill_count") or 0),
        "zero_target_reason": str(summary_payload.get("zero_target_reason") or ""),
        "paper_summary_path": str(summary_path),
        "source_portfolio_path": str(summary_payload.get("source_portfolio_path") or ""),
        "attribution_summary_path": str(attribution_summary_path) if attribution_summary_path.exists() else "",
        "attribution_summary": attribution_payload,
    }


def _build_strategy_report_summary(report_dir: Path) -> dict[str, Any]:
    comparison_path = report_dir / "strategy_comparison_summary.csv"
    account_report_path = report_dir / "paper_account_report.json"
    top_selected: list[dict[str, Any]] = []
    portfolio_composition: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    if comparison_path.exists():
        try:
            frame = pd.read_csv(comparison_path)
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            frame = pd.DataFrame()
        if not frame.empty:
            frame = frame.astype(object).where(pd.notna(frame), None)
            ordered = frame.sort_values(
                ["is_active", "normalized_capital_weight", "ranking_value"],
                ascending=[False, False, False],
                kind="stable",
            )
            top_selected = ordered.head(5).to_dict(orient="records")
            grouped = (
                ordered.groupby("signal_family", dropna=False)
                .agg(
                    selected_count=("strategy_id", "count"),
                    active_count=("is_active", lambda values: int(sum(bool(value) for value in values))),
                    total_weight=("normalized_capital_weight", "sum"),
                )
                .reset_index()
            )
            grouped = grouped.astype(object).where(pd.notna(grouped), None)
            portfolio_composition = grouped.to_dict(orient="records")
            summary = {
                "strategy_count": int(len(ordered)),
                "active_strategy_count": int(sum(bool(value) for value in ordered["is_active"].tolist())),
                "runtime_computable_count": int(
                    sum(bool(value) for value in ordered["runtime_computability_pass"].tolist())
                ),
            }
    performance_stats = (
        json.loads(account_report_path.read_text(encoding="utf-8")) if account_report_path.exists() else {}
    )
    return {
        "summary": summary,
        "top_selected_strategies": top_selected,
        "portfolio_composition": portfolio_composition,
        "performance_stats": performance_stats,
    }


def _write_summary_artifacts(
    *,
    config: DailyTradingWorkflowConfig,
    run_dir: Path,
    effective_as_of_date: str | None,
    started_at: str,
    ended_at: str,
    duration_seconds: float,
    stage_records: list[DailyTradingStageRecord],
    warnings: list[str],
    errors: list[str],
    key_artifacts: dict[str, str],
    promotion_summary: dict[str, Any],
    portfolio_summary: dict[str, Any],
    activated_summary: dict[str, Any],
    paper_summary: dict[str, Any],
    strategy_report_summary: dict[str, Any],
) -> tuple[Path, Path, str]:
    failed_count = sum(1 for record in stage_records if record.status == "failed")
    warning_count = sum(1 for record in stage_records if record.status == "warning")
    status = "succeeded"
    if failed_count and any(record.status in {"succeeded", "warning"} for record in stage_records):
        status = "partial_failed"
    elif failed_count:
        status = "failed"
    elif warning_count:
        status = "warning"

    summary_payload = {
        "workflow_type": "daily_trading",
        "run_name": config.run_name,
        "run_id": config.run_id,
        "timestamp": ended_at,
        "effective_as_of_date": effective_as_of_date,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "status": status,
        "strict_mode": config.strict_mode,
        "best_effort_mode": config.best_effort_mode,
        "run_dir": str(run_dir),
        "stage_records": [record.to_dict() for record in stage_records],
        "stage_statuses": {record.stage_name: record.status for record in stage_records},
        "promoted_strategy_count": promotion_summary.get("promoted_strategy_count", 0),
        "promoted_unconditional_count": promotion_summary.get("promoted_unconditional_count", 0),
        "promoted_conditional_count": promotion_summary.get("promoted_conditional_count", 0),
        "selected_portfolio_strategy_count": portfolio_summary.get("selected_portfolio_strategy_count", 0),
        "selected_conditional_portfolio_count": portfolio_summary.get("selected_conditional_portfolio_count", 0),
        "active_strategy_count": activated_summary.get("active_strategy_count", 0),
        "activated_unconditional_count": activated_summary.get("activated_unconditional_count", 0),
        "activated_conditional_count": activated_summary.get("activated_conditional_count", 0),
        "inactive_conditional_count": activated_summary.get("inactive_conditional_count", 0),
        "requested_symbol_count": paper_summary.get("requested_symbol_count", 0),
        "usable_symbol_count": paper_summary.get("usable_symbol_count", 0),
        "pre_validation_target_symbol_count": paper_summary.get("pre_validation_target_symbol_count", 0),
        "post_validation_target_symbol_count": paper_summary.get("post_validation_target_symbol_count", 0),
        "executable_order_count": paper_summary.get("executable_order_count", 0),
        "fill_count": paper_summary.get("fill_count", 0),
        "zero_target_reason": paper_summary.get("zero_target_reason", ""),
        "blocked_entries_count": paper_summary.get("blocked_entries_count", 0),
        "held_in_hold_zone_count": paper_summary.get("held_in_hold_zone_count", 0),
        "forced_exit_count": paper_summary.get("forced_exit_count", 0),
        "skipped_due_to_entry_band_count": paper_summary.get("skipped_due_to_entry_band_count", 0),
        "skipped_due_to_hold_zone_count": paper_summary.get("skipped_due_to_hold_zone_count", 0),
        "score_band_enabled": paper_summary.get("score_band_enabled", False),
        "entry_threshold_used": paper_summary.get("entry_threshold_used"),
        "exit_threshold_used": paper_summary.get("exit_threshold_used"),
        "pnl_attribution_summary": paper_summary.get("attribution_summary", {}),
        "top_selected_strategies": strategy_report_summary.get("top_selected_strategies", []),
        "portfolio_composition": strategy_report_summary.get("portfolio_composition", []),
        "strategy_quality_summary": strategy_report_summary.get("summary", {}),
        "performance_stats": strategy_report_summary.get("performance_stats", {}),
        "source_artifact_paths": key_artifacts,
        "warnings": warnings,
        "errors": errors,
    }
    json_path = run_dir / "daily_trading_summary.json"
    md_path = run_dir / "daily_trading_summary.md"
    json_path.write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")

    lines = [
        "# Daily Trading Summary",
        "",
        f"- run_name: `{config.run_name}`",
        f"- run_id: `{config.run_id}`" if config.run_id else "- run_id: `none`",
        f"- status: `{status}`",
        f"- timestamp: `{ended_at}`",
        f"- effective_as_of_date: `{effective_as_of_date}`"
        if effective_as_of_date
        else "- effective_as_of_date: `live/default`",
        f"- promoted_strategy_count: `{promotion_summary.get('promoted_strategy_count', 0)}`",
        f"- promoted_unconditional_count: `{promotion_summary.get('promoted_unconditional_count', 0)}`",
        f"- promoted_conditional_count: `{promotion_summary.get('promoted_conditional_count', 0)}`",
        f"- selected_portfolio_strategy_count: `{portfolio_summary.get('selected_portfolio_strategy_count', 0)}`",
        f"- selected_conditional_portfolio_count: `{portfolio_summary.get('selected_conditional_portfolio_count', 0)}`",
        f"- active_strategy_count: `{activated_summary.get('active_strategy_count', 0)}`",
        f"- activated_unconditional_count: `{activated_summary.get('activated_unconditional_count', 0)}`",
        f"- activated_conditional_count: `{activated_summary.get('activated_conditional_count', 0)}`",
        f"- inactive_conditional_count: `{activated_summary.get('inactive_conditional_count', 0)}`",
        f"- requested_symbol_count: `{paper_summary.get('requested_symbol_count', 0)}`",
        f"- usable_symbol_count: `{paper_summary.get('usable_symbol_count', 0)}`",
        f"- pre_validation_target_symbol_count: `{paper_summary.get('pre_validation_target_symbol_count', 0)}`",
        f"- post_validation_target_symbol_count: `{paper_summary.get('post_validation_target_symbol_count', 0)}`",
        f"- executable_order_count: `{paper_summary.get('executable_order_count', 0)}`",
        f"- fill_count: `{paper_summary.get('fill_count', 0)}`",
        f"- zero_target_reason: `{paper_summary.get('zero_target_reason', '') or 'none'}`",
        f"- score_band_enabled: `{paper_summary.get('score_band_enabled', False)}`",
        f"- entry_threshold_used: `{paper_summary.get('entry_threshold_used')}`",
        f"- exit_threshold_used: `{paper_summary.get('exit_threshold_used')}`",
        f"- blocked_entries_count: `{paper_summary.get('blocked_entries_count', 0)}`",
        f"- held_in_hold_zone_count: `{paper_summary.get('held_in_hold_zone_count', 0)}`",
        f"- forced_exit_count: `{paper_summary.get('forced_exit_count', 0)}`",
        "",
        "## Top Strategies",
        "",
    ]
    top_strategies = list(strategy_report_summary.get("top_selected_strategies", []))
    if top_strategies:
        for row in top_strategies:
            lines.append(
                "- "
                f"`{row.get('strategy_id')}` "
                f"family=`{row.get('signal_family')}` "
                f"active=`{row.get('is_active')}` "
                f"weight=`{row.get('normalized_capital_weight')}` "
                f"metric=`{row.get('ranking_metric')}` "
                f"value=`{row.get('ranking_value')}`"
            )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Portfolio Composition",
            "",
        ]
    )
    portfolio_composition = list(strategy_report_summary.get("portfolio_composition", []))
    if portfolio_composition:
        for row in portfolio_composition:
            lines.append(
                "- "
                f"`{row.get('signal_family')}` "
                f"selected=`{row.get('selected_count')}` "
                f"active=`{row.get('active_count')}` "
                f"weight=`{row.get('total_weight')}`"
            )
    else:
        lines.append("- none")
    performance_stats = dict(strategy_report_summary.get("performance_stats") or {})
    attribution_summary = dict(paper_summary.get("attribution_summary") or {})
    lines.extend(
        [
            "",
            "## Performance",
            "",
            f"- latest_equity: `{performance_stats.get('latest_equity', 0)}`",
            f"- cumulative_return: `{performance_stats.get('cumulative_return', 0)}`",
            f"- max_drawdown: `{performance_stats.get('max_drawdown', 0)}`",
            f"- sharpe_ratio: `{performance_stats.get('sharpe_ratio', 0)}`",
            "",
            "## Attribution",
            "",
            f"- total_gross_realized_pnl: `{attribution_summary.get('total_gross_realized_pnl', 0)}`",
            f"- total_net_realized_pnl: `{attribution_summary.get('total_net_realized_pnl', 0)}`",
            f"- total_realized_pnl: `{attribution_summary.get('total_realized_pnl', 0)}`",
            f"- total_gross_unrealized_pnl: `{attribution_summary.get('total_gross_unrealized_pnl', 0)}`",
            f"- total_net_unrealized_pnl: `{attribution_summary.get('total_net_unrealized_pnl', 0)}`",
            f"- total_unrealized_pnl: `{attribution_summary.get('total_unrealized_pnl', 0)}`",
            f"- total_gross_pnl: `{attribution_summary.get('total_gross_pnl', 0)}`",
            f"- total_net_pnl: `{attribution_summary.get('total_net_pnl', 0)}`",
            f"- total_pnl: `{attribution_summary.get('total_pnl', 0)}`",
            f"- total_execution_cost: `{attribution_summary.get('total_execution_cost', 0)}`",
            "",
            "## Stages",
            "",
            "| stage | status | duration_seconds | error |",
            "| --- | --- | ---: | --- |",
        ]
    )
    top_strategy_pnl = list(attribution_summary.get("top_strategies_by_total_pnl", []))
    if top_strategy_pnl:
        lines.insert(
            -5,
            f"- top_strategy_contributor: `{top_strategy_pnl[0].get('strategy_id')}` pnl=`{top_strategy_pnl[0].get('total_pnl')}`",
        )
    bottom_strategy_pnl = list(attribution_summary.get("bottom_strategies_by_total_pnl", []))
    if bottom_strategy_pnl:
        lines.insert(
            -5,
            f"- top_strategy_detractor: `{bottom_strategy_pnl[0].get('strategy_id')}` pnl=`{bottom_strategy_pnl[0].get('total_pnl')}`",
        )
    top_symbol_pnl = list(attribution_summary.get("top_symbols_by_total_pnl", []))
    if top_symbol_pnl:
        lines.insert(
            -5,
            f"- top_symbol_contributor: `{top_symbol_pnl[0].get('symbol')}` pnl=`{top_symbol_pnl[0].get('total_pnl')}`",
        )
    bottom_symbol_pnl = list(attribution_summary.get("bottom_symbols_by_total_pnl", []))
    if bottom_symbol_pnl:
        lines.insert(
            -5,
            f"- top_symbol_detractor: `{bottom_symbol_pnl[0].get('symbol')}` pnl=`{bottom_symbol_pnl[0].get('total_pnl')}`",
        )
    for record in stage_records:
        duration_text = f"{record.duration_seconds:.3f}" if record.duration_seconds is not None else "n/a"
        lines.append(f"| {record.stage_name} | {record.status} | {duration_text} | {record.error_message or ''} |")
    if key_artifacts:
        lines.extend(["", "## Artifacts", ""])
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


def _build_multi_strategy_paper_config(result, reserve_cash_pct: float, workflow_config) -> PaperTradingConfig:
    symbols = sorted(result.combined_target_weights)
    return PaperTradingConfig(
        symbols=symbols,
        preset_name="multi_strategy",
        universe_name=f"{result.summary['enabled_sleeve_count']}_sleeves",
        strategy="multi_strategy",
        signal_source="legacy",
        reserve_cash_pct=reserve_cash_pct,
        slippage_model=str(getattr(workflow_config, "slippage_model", "none") or "none"),
        slippage_buy_bps=float(getattr(workflow_config, "slippage_buy_bps", 0.0) or 0.0),
        slippage_sell_bps=float(getattr(workflow_config, "slippage_sell_bps", 0.0) or 0.0),
        enable_cost_model=bool(getattr(workflow_config, "enable_cost_model", False)),
        commission_bps=float(getattr(workflow_config, "commission_bps", 0.0) or 0.0),
        minimum_commission=float(getattr(workflow_config, "minimum_commission", 0.0) or 0.0),
        spread_bps=float(getattr(workflow_config, "spread_bps", 0.0) or 0.0),
        min_weight_change_to_trade=float(getattr(workflow_config, "min_weight_change_to_trade", 0.0) or 0.0),
        entry_score_threshold=getattr(workflow_config, "entry_score_threshold", None),
        exit_score_threshold=getattr(workflow_config, "exit_score_threshold", None),
        hold_score_band=bool(getattr(workflow_config, "hold_score_band", True)),
        use_percentile_thresholds=bool(getattr(workflow_config, "use_percentile_thresholds", False)),
        entry_score_percentile=getattr(workflow_config, "entry_score_percentile", None),
        exit_score_percentile=getattr(workflow_config, "exit_score_percentile", None),
        apply_bands_to_new_entries=bool(getattr(workflow_config, "apply_bands_to_new_entries", True)),
        apply_bands_to_reductions=bool(getattr(workflow_config, "apply_bands_to_reductions", True)),
        apply_bands_to_full_exits=bool(getattr(workflow_config, "apply_bands_to_full_exits", True)),
    )


def _apply_replay_testing_overrides(
    portfolio_config,
    *,
    replay_settings: dict[str, Any] | None,
    output_dir: Path,
) -> tuple[Any, list[str]]:
    if not replay_settings:
        return portfolio_config, []

    override_min_signal = replay_settings.get("override_min_signal_strength")
    override_max_weight = replay_settings.get("override_max_weight_per_strategy")
    relax_thresholds = bool(replay_settings.get("relax_thresholds_for_testing", False))
    if override_min_signal is None and override_max_weight is None and not relax_thresholds:
        return portfolio_config, []

    override_dir = output_dir / "replay_testing_overrides"
    override_dir.mkdir(parents=True, exist_ok=True)
    updated_sleeves = []
    notes: list[str] = []
    for sleeve in portfolio_config.sleeves:
        preset_path = Path(str(sleeve.preset_path)) if sleeve.preset_path else None
        if preset_path is None or not preset_path.exists():
            updated_sleeves.append(sleeve)
            continue
        payload = json.loads(preset_path.read_text(encoding="utf-8"))
        params = dict(payload.get("params") or {})
        changed = False
        if override_min_signal is not None:
            params["min_score"] = float(override_min_signal)
            changed = True
        elif relax_thresholds and "min_score" in params:
            params.pop("min_score", None)
            changed = True
        if override_max_weight is not None:
            params["max_weight"] = float(override_max_weight)
            params["max_position_weight"] = float(override_max_weight)
            changed = True
        if relax_thresholds:
            if float(params.get("turnover_buffer_bps", 0.0) or 0.0) != 0.0:
                params["turnover_buffer_bps"] = 0.0
                changed = True
        if not changed:
            updated_sleeves.append(sleeve)
            continue
        payload["params"] = params
        override_path = override_dir / f"{sleeve.sleeve_name}.json"
        override_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        updated_sleeves.append(replace(sleeve, preset_path=str(override_path)))
        notes.append(f"{sleeve.sleeve_name}:applied_replay_testing_overrides")
    return replace(portfolio_config, sleeves=updated_sleeves), notes


def _decision_row_action(*, current_position: int | None, target_position: int | None) -> str:
    current_qty = int(current_position or 0)
    target_qty = int(target_position or 0)
    if current_qty == 0 and target_qty > 0:
        return "buy"
    if current_qty > 0 and target_qty == 0:
        return "sell"
    if current_qty != target_qty:
        return "rebalance"
    return "hold"


def _decision_row_reason(
    *,
    action: str,
    current_position: int | None,
    target_position: int | None,
    current_weight: float | None,
    target_weight: float | None,
    explicit_reason: str | None,
) -> str:
    if explicit_reason:
        lowered = explicit_reason.lower()
        if "limit" in lowered:
            return "blocked_by_limits"
        if "validation" in lowered:
            return "blocked_by_validation"
        return explicit_reason
    current_qty = int(current_position or 0)
    target_qty = int(target_position or 0)
    if action == "buy" and current_qty == 0 and target_qty > 0:
        return "enter_new_position"
    if action == "sell" and current_qty > 0 and target_qty == 0:
        return "exit_position"
    if action == "rebalance":
        return "rebalance_weight_change"
    if (
        current_weight is not None
        and target_weight is not None
        and abs(float(target_weight) - float(current_weight)) <= 1e-9
    ):
        return "hold_within_tolerance"
    return "hold_within_tolerance"


def _build_trade_decision_log_rows(
    *,
    as_of: str,
    signal_source: str,
    decision_bundle: Any | None,
    order_generation_diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    candidate_by_symbol = {}
    selection_by_symbol = {}
    sizing_by_symbol = {}
    trade_by_symbol = {}
    execution_by_symbol = {}
    if decision_bundle is not None:
        candidate_by_symbol = {row.symbol: row for row in getattr(decision_bundle, "candidate_evaluations", [])}
        selection_by_symbol = {row.symbol: row for row in getattr(decision_bundle, "selection_decisions", [])}
        sizing_by_symbol = {row.symbol: row for row in getattr(decision_bundle, "sizing_decisions", [])}
        trade_by_symbol = {row.symbol: row for row in getattr(decision_bundle, "trade_decisions", [])}
        execution_by_symbol = {row.symbol: row for row in getattr(decision_bundle, "execution_decisions", [])}
    order_generation_diagnostics = dict(order_generation_diagnostics or {})
    band_lookup = {
        str(row.get("symbol")): dict(row)
        for row in list(order_generation_diagnostics.get("band_decision_rows") or [])
        if row.get("symbol")
    }
    skipped_lookup = {
        str(row.get("symbol")): dict(row)
        for row in list(order_generation_diagnostics.get("skipped_trade_rows") or [])
        if row.get("symbol")
    }
    symbols = sorted(
        set(candidate_by_symbol)
        | set(selection_by_symbol)
        | set(sizing_by_symbol)
        | set(trade_by_symbol)
        | set(execution_by_symbol)
        | set(band_lookup)
        | set(skipped_lookup)
    )
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        candidate = candidate_by_symbol.get(symbol)
        selection = selection_by_symbol.get(symbol)
        sizing = sizing_by_symbol.get(symbol)
        trade = trade_by_symbol.get(symbol)
        execution = execution_by_symbol.get(symbol)
        band_row = band_lookup.get(symbol, {})
        skipped_row = skipped_lookup.get(symbol, {})
        current_weight = None
        if trade is not None:
            current_weight = (trade.metadata or {}).get("current_weight")
        if current_weight is None and execution is not None:
            current_weight = execution.current_weight
        if current_weight is None:
            current_weight = band_row.get("current_weight")
        target_weight = None
        if trade is not None:
            target_weight = trade.target_weight_post_constraint
        if target_weight is None and sizing is not None:
            target_weight = sizing.target_weight_post_constraint
        if target_weight is None and execution is not None:
            target_weight = execution.target_weight
        if target_weight is None:
            target_weight = band_row.get("adjusted_target_weight", skipped_row.get("target_weight"))
        current_position = (
            trade.current_quantity if trade is not None else (sizing.current_quantity if sizing is not None else None)
        )
        target_position = (
            trade.target_quantity if trade is not None else (sizing.target_quantity if sizing is not None else None)
        )
        action = _decision_row_action(current_position=current_position, target_position=target_position)
        explicit_reason = None
        if execution is not None and execution.order_status == "rejected":
            explicit_reason = execution.rejection_reason or execution.rationale_summary
        if explicit_reason is None and trade is not None:
            explicit_reason = trade.entry_reason_summary or trade.rejection_reason
        if explicit_reason is None and selection is not None:
            explicit_reason = selection.rejection_reason or selection.rationale_summary
        if explicit_reason is None:
            explicit_reason = str(skipped_row.get("action_reason") or band_row.get("action_reason") or "") or None
        signal_score = (
            (trade.final_signal_score if trade is not None else None)
            if trade is not None and trade.final_signal_score is not None
            else (
                (selection.final_signal_score if selection is not None else None)
                if selection is not None and selection.final_signal_score is not None
                else (candidate.final_signal_score if candidate is not None else None)
            )
        )
        if signal_score is None:
            signal_score = band_row.get("score_value")
        rank_value = (
            (selection.rank if selection is not None else None)
            if selection is not None and selection.rank is not None
            else (candidate.rank if candidate is not None else None)
        )
        if rank_value is None:
            rank_value = band_row.get("score_rank")
        if skipped_row:
            action = "hold"
        rows.append(
            {
                "date": str(pd.Timestamp(as_of).date()),
                "symbol": symbol,
                "strategy_id": (
                    (trade.strategy_id if trade is not None else None)
                    or (selection.strategy_id if selection is not None else None)
                    or (candidate.strategy_id if candidate is not None else None)
                ),
                "signal_source": signal_source,
                "signal_score": signal_score,
                "rank": rank_value,
                "score_value": band_row.get("score_value"),
                "score_rank": band_row.get("score_rank"),
                "score_percentile": band_row.get("score_percentile"),
                "entry_threshold": band_row.get("entry_threshold"),
                "exit_threshold": band_row.get("exit_threshold"),
                "band_decision": band_row.get("band_decision"),
                "current_weight": current_weight,
                "target_weight": target_weight,
                "weight_delta": (
                    (float(target_weight) - float(current_weight))
                    if current_weight is not None and target_weight is not None
                    else None
                ),
                "current_position": current_position,
                "target_position": target_position,
                "action": action,
                "action_reason": _decision_row_reason(
                    action=action,
                    current_position=current_position,
                    target_position=target_position,
                    current_weight=current_weight,
                    target_weight=target_weight,
                    explicit_reason=explicit_reason,
                ),
            }
        )
    return rows


def _build_multi_strategy_decision_bundle(allocation_result: Any) -> DecisionJournalBundle:
    strategy_metadata: dict[str, dict[str, Any]] = {}
    symbol_contributions: dict[str, dict[str, float]] = {}
    for bundle in getattr(allocation_result, "sleeve_bundles", []):
        strategy_id = str(bundle.sleeve.preset_name)
        strategy_metadata[strategy_id] = {
            "sleeve_name": bundle.sleeve.sleeve_name,
            "preset_name": bundle.sleeve.preset_name,
            "signal_source": getattr(bundle.paper_config, "signal_source", "multi_strategy"),
            "signal_family": getattr(bundle.sleeve, "signal_family", None),
        }
    for row in getattr(allocation_result, "sleeve_rows", []):
        strategy_id = str(row.get("preset_name") or row.get("sleeve_name") or "")
        symbol = str(row.get("symbol") or "")
        if not strategy_id or not symbol:
            continue
        symbol_contributions.setdefault(symbol, {})[strategy_id] = float(row.get("scaled_target_weight") or 0.0)
    provenance_by_symbol = build_symbol_strategy_provenance(
        final_target_weights=dict(getattr(allocation_result, "combined_target_weights", {}) or {}),
        symbol_contributions=symbol_contributions,
        strategy_metadata=strategy_metadata,
    )
    return DecisionJournalBundle(provenance_by_symbol=provenance_by_symbol)


def _write_trade_decision_log(*, output_dir: Path, rows: list[dict[str, Any]]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "trade_decision_log.json"
    csv_path = output_dir / "trade_decision_log.csv"
    json_path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    pd.DataFrame(
        rows,
        columns=[
            "date",
            "symbol",
            "strategy_id",
            "signal_source",
            "signal_score",
            "rank",
            "score_value",
            "score_rank",
            "score_percentile",
            "entry_threshold",
            "exit_threshold",
            "band_decision",
            "current_weight",
            "target_weight",
            "weight_delta",
            "current_position",
            "target_position",
            "action",
            "action_reason",
        ],
    ).to_csv(csv_path, index=False)
    return {
        "trade_decision_log_json_path": str(json_path),
        "trade_decision_log_csv_path": str(csv_path),
    }


def run_daily_trading_pipeline(
    config: DailyTradingWorkflowConfig,
    *,
    replay_as_of_date: str | None = None,
    replay_settings: dict[str, Any] | None = None,
    refresh_dashboard_static_data: bool | None = None,
) -> DailyTradingResult:
    started_at = _now_utc()
    started_clock = time.monotonic()
    run_dir_name = str(config.run_name).strip()
    if config.run_id:
        run_dir_name = f"{run_dir_name}_{config.run_id}"
    run_dir = Path(config.output_root) / run_dir_name
    run_dir.mkdir(parents=True, exist_ok=True)

    research_output_dir = _stage_path(config.research_output_dir, run_dir / "research")
    registry_dir = _stage_path(config.registry_dir, research_output_dir / "research_registry")
    promoted_dir = _stage_path(config.promoted_dir, run_dir / "promoted")
    portfolio_dir = _stage_path(config.portfolio_dir, run_dir / "strategy_portfolio")
    activated_dir = _stage_path(config.activated_dir, portfolio_dir / "activated")
    export_dir = _stage_path(config.export_dir, run_dir / "run_bundle")
    paper_output_dir = _stage_path(config.paper_output_dir, run_dir / "paper")
    paper_state_path = Path(config.paper_state_path)
    report_dir = (
        _stage_path(config.report_dir, paper_output_dir / "report")
        if config.report_dir
        else (paper_output_dir / "report")
    )
    dashboard_output_dir = (
        _stage_path(config.dashboard_output_dir, run_dir / "dashboard")
        if config.dashboard_output_dir
        else (run_dir / "dashboard")
    )

    warnings: list[str] = []
    errors: list[str] = []
    key_artifacts: dict[str, str] = {}
    stage_records: list[DailyTradingStageRecord] = []
    refresh_result = None

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
    portfolio_run_id = db_lineage.create_portfolio_run(
        run_key=run_dir_name,
        mode="daily_trading",
        config_payload=config.to_cli_defaults(),
        notes="daily_trading pipeline",
    )

    def run_stage(stage_name: str, enabled: bool, action) -> None:
        record = DailyTradingStageRecord(stage_name=stage_name)
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
        load_research_input_refresh_workflow_config(config.refresh_config) if config.refresh_config else None
    )
    research_config = load_alpha_research_workflow_config(config.research_config) if config.research_config else None

    try:

        def do_refresh(record: DailyTradingStageRecord) -> dict[str, Any]:
            nonlocal refresh_result
            if refresh_config is None:
                raise ValueError("refresh_config is required for refresh_inputs stage")
            refresh_result = refresh_research_inputs(request=_build_refresh_request(refresh_config))
            key_artifacts["refresh_summary_path"] = str(
                refresh_result.paths.get("research_input_refresh_summary_json", "")
            )
            return {
                "feature_dir": str(refresh_result.feature_dir),
                "metadata_dir": str(refresh_result.metadata_dir),
                **{key: str(value) for key, value in refresh_result.paths.items()},
            }

        run_stage("refresh_inputs", config.stages.refresh_inputs, do_refresh)

        def do_research(record: DailyTradingStageRecord) -> dict[str, Any]:
            if config.research_mode == "skip":
                record.status = "skipped"
                record.warnings.append("research_mode_skip")
                return {}
            if research_config is None:
                raise ValueError("research_config is required for research stage")
            feature_dir = (
                refresh_result.feature_dir if refresh_result is not None else Path(research_config.feature_dir)
            )
            research_output_dir.mkdir(parents=True, exist_ok=True)
            runner = run_alpha_research if config.research_mode == "full" else refresh_alpha_research_artifacts
            research_result = runner(
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
            return {key: str(value) if isinstance(value, Path) else value for key, value in research_result.items()}

        run_stage("research", config.stages.research, do_research)

        def do_promote(record: DailyTradingStageRecord) -> dict[str, Any]:
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
                override_validation=config.override_validation,
            )
            key_artifacts.update(
                {
                    "research_registry_path": str(registry_bundle.get("registry_json_path", "")),
                    "promotion_candidates_path": str(registry_bundle.get("promotion_candidates_json_path", "")),
                    "promoted_index_path": str(promotion_result.get("promoted_index_path", "")),
                }
            )
            if int(registry_bundle.get("run_count") or 0) == 0:
                record.warnings.append("empty_research_registry")
                warnings.append(f"promotion stage found zero research runs under artifacts_root={artifacts_root}")
            if int(promotion_result.get("selected_count") or 0) == 0:
                record.status = "warning"
                record.warnings.append("zero_promotions")
                warnings.append("promotion stage produced zero promoted strategies")
            return {
                "artifacts_root": str(artifacts_root),
                **{key: str(value) if isinstance(value, Path) else value for key, value in registry_bundle.items()},
                **{
                    key: str(value) if isinstance(value, Path) else value
                    for key, value in promotion_result.items()
                    if key != "promoted_rows"
                },
                "promoted_row_count": len(promotion_result.get("promoted_rows", [])),
            }

        run_stage("promote", config.stages.promote, do_promote)

        def do_build_portfolio(record: DailyTradingStageRecord) -> dict[str, Any]:
            if int(_summarize_promotions(promoted_dir).get("promoted_strategy_count") or 0) == 0:
                record.status = "skipped"
                record.warnings.append("zero_promotions")
                return {}
            policy = load_strategy_portfolio_policy_config(config.strategy_portfolio_policy_config)
            strategy_weighting_metrics_path = _resolve_strategy_weighting_metrics_path(config)
            if (
                str(getattr(policy, "weighting_mode", "") or "").strip() == "cost_adjusted"
                and strategy_weighting_metrics_path is None
            ):
                record.warnings.append("missing_strategy_weighting_metrics")
            portfolio_result = build_strategy_portfolio(
                promoted_dir=promoted_dir,
                output_dir=portfolio_dir,
                policy=policy,
                lifecycle_path=Path(config.lifecycle_path) if config.lifecycle_path else None,
                strategy_weighting_metrics_path=strategy_weighting_metrics_path,
            )
            key_artifacts.update(
                {
                    "strategy_portfolio_json_path": str(portfolio_result.get("strategy_portfolio_json_path", "")),
                    "strategy_portfolio_csv_path": str(portfolio_result.get("strategy_portfolio_csv_path", "")),
                    "strategy_portfolio_condition_summary_path": str(
                        portfolio_result.get("strategy_portfolio_condition_summary_path", "")
                    ),
                    "strategy_weighting_diagnostics_json_path": str(
                        portfolio_result.get("strategy_weighting_diagnostics_json_path", "")
                    ),
                    "strategy_weighting_diagnostics_csv_path": str(
                        portfolio_result.get("strategy_weighting_diagnostics_csv_path", "")
                    ),
                }
            )
            return portfolio_result

        run_stage("build_portfolio", config.stages.build_portfolio, do_build_portfolio)

        def do_activate_portfolio(record: DailyTradingStageRecord) -> dict[str, Any]:
            portfolio_json_path = portfolio_dir / "strategy_portfolio.json"
            if not portfolio_json_path.exists():
                record.status = "skipped"
                record.warnings.append("missing_strategy_portfolio")
                return {}
            policy = load_strategy_portfolio_policy_config(config.strategy_portfolio_policy_config)
            evaluate = (
                config.evaluate_conditional_activation
                if config.evaluate_conditional_activation is not None
                else policy.evaluate_conditional_activation
            )
            activation_config = ConditionalActivationConfig(
                evaluate_conditional_activation=bool(evaluate),
                activation_context_sources=list(
                    config.activation_context_sources
                    if config.activation_context_sources is not None
                    else policy.activation_context_sources
                ),
                include_inactive_conditionals_in_output=bool(
                    config.include_inactive_conditionals_in_output
                    if config.include_inactive_conditionals_in_output is not None
                    else policy.include_inactive_conditionals_in_output
                ),
            )
            activation_result = activate_strategy_portfolio(
                portfolio_path=portfolio_dir,
                output_dir=activated_dir,
                config=activation_config,
                market_regime_path=(refresh_config.market_regime_path if refresh_config is not None else None),
                regime_labels_path=research_output_dir,
                metadata_dir=(str(refresh_result.metadata_dir) if refresh_result is not None else None),
            )
            key_artifacts.update(
                {
                    "activated_strategy_portfolio_json_path": str(
                        activation_result.get("activated_strategy_portfolio_json_path", "")
                    ),
                    "activated_strategy_portfolio_csv_path": str(
                        activation_result.get("activated_strategy_portfolio_csv_path", "")
                    ),
                }
            )
            return activation_result

        run_stage("activate_portfolio", config.stages.activate_portfolio, do_activate_portfolio)

        def do_export_bundle(record: DailyTradingStageRecord) -> dict[str, Any]:
            export_source = (
                activated_dir if (activated_dir / "activated_strategy_portfolio.json").exists() else portfolio_dir
            )
            if export_source == activated_dir:
                active_rows = list(load_activated_strategy_portfolio(activated_dir).get("active_strategies", []))
                if not active_rows:
                    if config.fail_if_no_active_strategies:
                        raise ValueError("activated strategy portfolio contains zero active strategies")
                    record.status = "warning"
                    record.warnings.append("no_active_strategies")
                    warnings.append("export stage found zero active strategies; active-only bundle not written")
                    return {}
            elif not (portfolio_dir / "strategy_portfolio.json").exists():
                record.status = "skipped"
                record.warnings.append("missing_strategy_portfolio")
                return {}
            export_result = export_strategy_portfolio_run_config(
                strategy_portfolio_path=export_source,
                output_dir=export_dir,
            )
            key_artifacts.update({key: str(value) for key, value in export_result.items()})
            return export_result

        run_stage("export_bundle", config.stages.export_bundle, do_export_bundle)

        def do_paper_run(record: DailyTradingStageRecord) -> dict[str, Any]:
            empty_trade_log_paths = _write_trade_decision_log(output_dir=run_dir, rows=[])
            key_artifacts.update(empty_trade_log_paths)
            canonical_input = activated_dir / "activated_strategy_portfolio.json"
            if not canonical_input.exists() or not config.use_activated_portfolio_for_paper:
                canonical_input = portfolio_dir / "strategy_portfolio.json"
            if not canonical_input.exists():
                record.status = "skipped"
                record.warnings.append("missing_portfolio_input")
                return dict(empty_trade_log_paths)

            handoff = resolve_strategy_execution_handoff(
                canonical_input,
                config=StrategyExecutionHandoffConfig(
                    fail_if_no_active_strategies=config.fail_if_no_active_strategies,
                    include_inactive_conditionals_in_reports=config.include_inactive_conditionals_in_reports,
                ),
            )
            handoff_summary_path = write_strategy_execution_handoff_summary(
                handoff=handoff,
                output_dir=paper_output_dir,
                artifact_name="paper_active_strategy_summary.json",
            )
            key_artifacts["paper_active_strategy_summary_path"] = str(handoff_summary_path)

            if handoff.portfolio_config is None:
                if config.fail_if_no_active_strategies:
                    raise ValueError(f"No active strategies available for paper trading: {canonical_input}")
                record.status = "warning"
                record.warnings.append("no_active_strategies")
                warnings.append("paper stage skipped because no active strategies were available")
                return {
                    "paper_active_strategy_summary_path": str(handoff_summary_path),
                    **empty_trade_log_paths,
                }

            portfolio_config = handoff.portfolio_config
            portfolio_config, replay_override_notes = _apply_replay_testing_overrides(
                portfolio_config,
                replay_settings=replay_settings,
                output_dir=paper_output_dir,
            )
            if replay_override_notes:
                record.warnings.extend(replay_override_notes)
            execution_config = load_execution_config(config.execution_config) if config.execution_config else None
            if replay_as_of_date:
                allocation_result = allocate_multi_strategy_portfolio(portfolio_config, as_of_date=replay_as_of_date)
            else:
                allocation_result = allocate_multi_strategy_portfolio(portfolio_config)
            allocation_paths = write_multi_strategy_artifacts(allocation_result, paper_output_dir)
            post_validation_target_count = int(
                allocation_result.summary.get(
                    "post_validation_target_symbol_count",
                    len(allocation_result.combined_target_weights),
                )
                or len(allocation_result.combined_target_weights)
            )

            if config.fail_if_zero_targets_after_validation and post_validation_target_count == 0:
                raise ValueError(
                    f"zero targets after validation: {allocation_result.summary.get('zero_target_reason') or 'unknown'}"
                )
            if post_validation_target_count == 0:
                record.status = "warning"
                record.warnings.append(str(allocation_result.summary.get("zero_target_reason") or "zero_targets"))

            paper_config = _build_multi_strategy_paper_config(
                allocation_result,
                reserve_cash_pct=portfolio_config.cash_reserve_pct,
                workflow_config=config,
            )
            if replay_as_of_date:
                paper_config = replace(paper_config, replay_as_of_date=replay_as_of_date)
            multi_strategy_decision_bundle = _build_multi_strategy_decision_bundle(allocation_result)
            state_store = JsonPaperStateStore(paper_state_path)
            state_file_preexisting = paper_state_path.exists()
            target_diagnostics = {
                "portfolio_construction_mode": "multi_strategy",
                "rebalance_timestamp": allocation_result.as_of,
                "selected_symbols": ",".join(sorted(set(row["symbol"] for row in allocation_result.sleeve_rows))),
                "target_selected_symbols": ",".join(sorted(allocation_result.combined_target_weights)),
                "requested_active_strategy_count": allocation_result.summary.get("requested_active_strategy_count"),
                "requested_symbol_count": allocation_result.summary.get("requested_symbol_count"),
                "pre_validation_target_symbol_count": allocation_result.summary.get(
                    "pre_validation_target_symbol_count"
                ),
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
                "semantic_warning": (
                    "portfolio_constraints_applied" if allocation_result.summary["symbols_removed_or_clipped"] else ""
                ),
                "target_selected_count": len(allocation_result.combined_target_weights),
                "summary": {"mean_turnover": allocation_result.summary["turnover_estimate"]},
                "multi_strategy_allocation": allocation_result.summary,
                "strategy_execution_handoff": handoff.summary,
            }
            paper_cycle_result = run_paper_trading_cycle_for_targets(
                config=paper_config,
                state_store=state_store,
                as_of=allocation_result.as_of,
                latest_prices=allocation_result.latest_prices,
                latest_scores=getattr(allocation_result, "latest_scores", {}),
                latest_scheduled_weights=allocation_result.combined_target_weights,
                latest_effective_weights=allocation_result.combined_target_weights,
                target_diagnostics=target_diagnostics,
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
                decision_bundle=multi_strategy_decision_bundle,
                execution_config=execution_config,
                auto_apply_fills=config.auto_apply_fills,
            )
            paper_paths = write_paper_trading_artifacts(result=paper_cycle_result, output_dir=paper_output_dir)
            persistence_paths, _health_checks, latest_summary = persist_paper_run_outputs(
                result=paper_cycle_result,
                config=paper_config,
                output_dir=paper_output_dir,
                state_file_preexisting=state_file_preexisting,
            )
            trade_decision_log_paths = _write_trade_decision_log(
                output_dir=run_dir,
                rows=_build_trade_decision_log_rows(
                    as_of=replay_as_of_date or paper_cycle_result.as_of,
                    signal_source=str(paper_config.signal_source or "multi_strategy"),
                    decision_bundle=paper_cycle_result.decision_bundle,
                    order_generation_diagnostics=paper_cycle_result.diagnostics.get("order_generation", {}),
                ),
            )
            key_artifacts.update(
                {
                    **{key: str(value) for key, value in allocation_paths.items()},
                    **{key: str(value) for key, value in paper_paths.items()},
                    **{key: str(value) for key, value in persistence_paths.items()},
                    **trade_decision_log_paths,
                    "paper_state_path": str(paper_state_path),
                }
            )
            return {
                "handoff_summary_path": str(handoff_summary_path),
                "allocation_summary": allocation_result.summary,
                "paper_summary": latest_summary,
                **trade_decision_log_paths,
                **{key: str(value) for key, value in allocation_paths.items()},
                **{key: str(value) for key, value in paper_paths.items()},
                **{key: str(value) for key, value in persistence_paths.items()},
            }

        run_stage("paper_run", config.stages.paper_run, do_paper_run)

        def do_report(record: DailyTradingStageRecord) -> dict[str, Any]:
            if not paper_output_dir.exists():
                record.status = "skipped"
                record.warnings.append("missing_paper_output")
                return {}
            report = build_paper_account_report(paper_output_dir)
            report_paths = write_paper_account_report(report=report, output_dir=report_dir)
            output_payload: dict[str, Any] = {key: str(value) for key, value in report_paths.items()}
            if config.enable_strategy_diagnostics:
                strategy_quality_report = build_strategy_quality_report(
                    promoted_dir=promoted_dir,
                    portfolio_dir=portfolio_dir,
                    activated_dir=activated_dir,
                    paper_output_dir=paper_output_dir,
                    output_root=Path(config.output_root),
                    run_name=config.run_name,
                )
                strategy_quality_paths = write_strategy_quality_report(
                    report=strategy_quality_report,
                    output_dir=report_dir,
                )
                output_payload.update({key: str(value) for key, value in strategy_quality_paths.items()})
                key_artifacts.update({key: str(value) for key, value in strategy_quality_paths.items()})
            should_refresh_dashboard = (
                config.refresh_dashboard_static_data
                if refresh_dashboard_static_data is None
                else bool(refresh_dashboard_static_data)
            )
            if should_refresh_dashboard:
                dashboard_paths = build_dashboard_static_data(
                    artifacts_root=Path("artifacts"),
                    output_dir=dashboard_output_dir,
                )
                output_payload.update({f"dashboard_{key}": str(value) for key, value in dashboard_paths.items()})
                key_artifacts["dashboard_output_dir"] = str(dashboard_output_dir)
            key_artifacts.update(
                {
                    "paper_account_report_json_path": str(report_paths["json_path"]),
                    "paper_account_report_csv_path": str(report_paths["csv_path"]),
                }
            )
            return output_payload

        run_stage("report", config.stages.report, do_report)
    except Exception:
        ended_at = _now_utc()
        duration_seconds = time.monotonic() - started_clock
        promotion_summary = _summarize_promotions(promoted_dir)
        portfolio_summary = _summarize_portfolio(portfolio_dir)
        activated_summary = _summarize_activated_portfolio(activated_dir)
        paper_summary = _summarize_paper_run(paper_output_dir)
        strategy_report_summary = {
            "summary": {},
            "top_selected_strategies": [],
            "portfolio_composition": [],
            "performance_stats": {},
        }
        summary_json_path, summary_md_path, status = _write_summary_artifacts(
            config=config,
            run_dir=run_dir,
            effective_as_of_date=replay_as_of_date,
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
            paper_summary=paper_summary,
            strategy_report_summary=strategy_report_summary,
        )
        db_lineage.fail_portfolio_run(portfolio_run_id, notes=f"status={status}; summary={summary_json_path}")
        raise

    ended_at = _now_utc()
    duration_seconds = time.monotonic() - started_clock
    promotion_summary = _summarize_promotions(promoted_dir)
    portfolio_summary = _summarize_portfolio(portfolio_dir)
    activated_summary = _summarize_activated_portfolio(activated_dir)
    paper_summary = _summarize_paper_run(paper_output_dir)
    strategy_report_summary = _build_strategy_report_summary(report_dir)
    summary_json_path, summary_md_path, status = _write_summary_artifacts(
        config=config,
        run_dir=run_dir,
        effective_as_of_date=replay_as_of_date,
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
        paper_summary=paper_summary,
        strategy_report_summary=strategy_report_summary,
    )
    db_lineage.complete_portfolio_run(portfolio_run_id, notes=f"status={status}; summary={summary_json_path}")
    return DailyTradingResult(
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
        summary_json_path=str(summary_json_path),
        summary_md_path=str(summary_md_path),
        key_artifacts=key_artifacts,
    )
