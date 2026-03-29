from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.workflow_models import DailyReplayWorkflowConfig, DailyTradingWorkflowConfig
from trading_platform.dashboard.server import build_dashboard_static_data
from trading_platform.orchestration.daily_trading import DailyTradingResult, run_daily_trading_pipeline
from trading_platform.portfolio.strategy_execution_handoff import resolve_strategy_execution_handoff
from trading_platform.research.trade_ev import evaluate_replay_trade_ev_predictions
from trading_platform.research.trade_ev_regression import run_replay_trade_ev_regression
from trading_platform.reporting.ev_lifecycle import (
    aggregate_replay_ev_lifecycle,
    write_replay_ev_lifecycle_artifacts,
)
from trading_platform.reporting.pnl_attribution import (
    aggregate_replay_attribution,
    write_replay_pnl_attribution_artifacts,
)


@dataclass(frozen=True)
class DailyReplayDayResult:
    requested_date: str
    run_dir: str
    status: str
    error_message: str | None
    summary_json_path: str | None
    trade_decision_log_path: str | None
    input_summary_path: str | None
    state_before_path: str | None
    state_after_path: str | None


@dataclass(frozen=True)
class DailyReplayResult:
    output_dir: str
    state_path: str
    requested_dates: list[str]
    processed_dates: list[str]
    status: str
    day_results: list[DailyReplayDayResult]
    summary_json_path: str
    summary_md_path: str
    artifact_paths: dict[str, str]
    summary: dict[str, Any]


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _read_dates_file(path: str | Path) -> list[str]:
    values: list[str] = []
    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        values.extend(part.strip() for part in line.split(",") if part.strip())
    return [str(pd.Timestamp(value).date()) for value in values]


def build_daily_replay_dates(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    dates_file: str | None = None,
    max_days: int | None = None,
) -> list[str]:
    if dates_file:
        ordered = _read_dates_file(dates_file)
    elif start_date and end_date:
        ordered = [str(ts.date()) for ts in pd.bdate_range(start=start_date, end=end_date)]
    else:
        raise ValueError("daily replay requires either dates_file or both start_date and end_date")
    deduped = list(dict.fromkeys(ordered))
    if max_days is not None:
        return deduped[: max(int(max_days), 0)]
    return deduped


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)
    return path


def _stage_duration_map(result: DailyTradingResult | None) -> dict[str, float]:
    if result is None:
        return {}
    return {
        str(record.stage_name): float(record.duration_seconds or 0.0)
        for record in result.stage_records
    }


def _write_replay_timing_artifacts(
    *,
    replay_root: Path,
    day_rows: list[dict[str, Any]],
    summary: dict[str, Any],
) -> dict[str, str]:
    timing_csv_path = _write_csv(
        replay_root / "replay_timing_by_day.csv",
        day_rows,
        [
            "date",
            "status",
            "setup_s",
            "pipeline_s",
            "research_s",
            "promote_s",
            "build_portfolio_s",
            "activate_portfolio_s",
            "export_bundle_s",
            "paper_run_s",
            "report_s",
            "input_summary_s",
            "total_s",
        ],
    )
    summary_json_path = replay_root / "replay_timing_summary.json"
    summary_json_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    return {
        "replay_timing_by_day_csv_path": str(timing_csv_path),
        "replay_timing_summary_json_path": str(summary_json_path),
    }


def _build_day_config(
    base_config: DailyTradingWorkflowConfig,
    *,
    replay_root: Path,
    requested_date: str,
    state_path: Path,
    strategy_weighting_metrics_path: Path | None = None,
) -> DailyTradingWorkflowConfig:
    payload = base_config.to_cli_defaults()
    payload["stages"] = base_config.stages
    run_dir = replay_root / requested_date
    research_stage_writes = bool(base_config.stages.research and base_config.research_mode in {"full", "fast_refresh"})
    promote_stage_writes = bool(base_config.stages.promote)
    portfolio_stage_writes = bool(base_config.stages.build_portfolio)
    activate_stage_writes = bool(base_config.stages.activate_portfolio)
    export_stage_writes = bool(base_config.stages.export_bundle)
    payload.update(
        {
            "output_root": str(replay_root),
            "run_name": requested_date,
            "run_id": None,
            "research_output_dir": str(run_dir / "research")
            if research_stage_writes
            else base_config.research_output_dir,
            "registry_dir": str(run_dir / "research" / "research_registry")
            if promote_stage_writes
            else base_config.registry_dir,
            "promoted_dir": str(run_dir / "promoted") if promote_stage_writes else base_config.promoted_dir,
            "portfolio_dir": str(run_dir / "strategy_portfolio")
            if portfolio_stage_writes
            else base_config.portfolio_dir,
            "activated_dir": (
                str(run_dir / "strategy_portfolio" / "activated")
                if activate_stage_writes
                else base_config.activated_dir
            ),
            "export_dir": str(run_dir / "run_bundle") if export_stage_writes else base_config.export_dir,
            "paper_output_dir": str(run_dir / "paper"),
            "paper_state_path": str(state_path),
            "strategy_weighting_metrics_path": (
                str(strategy_weighting_metrics_path)
                if strategy_weighting_metrics_path is not None
                else base_config.strategy_weighting_metrics_path
            ),
            "ev_gate_training_root": (
                str(replay_root)
                if bool(base_config.ev_gate_enabled) and not base_config.ev_gate_training_root
                else base_config.ev_gate_training_root
            ),
            "report_dir": str(run_dir / "report"),
            "dashboard_output_dir": str(run_dir / "dashboard"),
        }
    )
    return DailyTradingWorkflowConfig(**payload)


def _count_rows(path: Path, *, key: str) -> int:
    payload = _read_json(path)
    return int(len(payload.get(key, []))) if payload else 0


def _count_selected_strategy_rows(path: Path) -> int:
    payload = _read_json(path)
    return int(len(payload.get("selected_strategies", []))) if payload else 0


def _count_active_strategy_rows(path: Path) -> int:
    payload = _read_json(path)
    if not payload:
        return 0
    return int(len(payload.get("active_strategies", []))) or int(
        payload.get("summary", {}).get("active_row_count", 0) or 0
    )


def _resolve_replay_universe_inputs(day_config: DailyTradingWorkflowConfig) -> tuple[list[str], list[str], list[str]]:
    canonical_input = Path(day_config.activated_dir) / "activated_strategy_portfolio.json"
    if not canonical_input.exists() or not day_config.use_activated_portfolio_for_paper:
        canonical_input = Path(day_config.portfolio_dir) / "strategy_portfolio.json"
    if not canonical_input.exists():
        return [], [], []
    try:
        handoff = resolve_strategy_execution_handoff(canonical_input)
    except Exception:
        return [], [], []
    if handoff.portfolio_config is None:
        return [], [], []
    symbols: set[str] = set()
    universe_paths: list[str] = []
    preset_paths: list[str] = []
    for sleeve in handoff.portfolio_config.sleeves:
        if not sleeve.preset_path:
            continue
        preset_path = Path(str(sleeve.preset_path))
        preset_paths.append(str(preset_path))
        if not preset_path.exists():
            continue
        payload = _read_json(preset_path)
        params = dict(payload.get("params") or {})
        for symbol in params.get("symbols", []) or []:
            symbols.add(str(symbol))
        for maybe_path_key in ("universe_membership_path", "group_map_path", "reference_data_root"):
            value = params.get(maybe_path_key)
            if value:
                universe_paths.append(str(value))
    return sorted(symbols), sorted(dict.fromkeys(universe_paths)), sorted(dict.fromkeys(preset_paths))


def _build_validation_drop_reason_counts(day_dir: Path) -> dict[str, int]:
    coverage_path = day_dir / "paper" / "execution_symbol_coverage.csv"
    if not coverage_path.exists():
        return {}
    try:
        frame = pd.read_csv(coverage_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return {}
    if frame.empty or "skip_reason" not in frame.columns:
        return {}
    filtered = frame["skip_reason"].fillna("").astype(str)
    filtered = filtered[filtered != ""]
    if filtered.empty:
        return {}
    return {str(index): int(value) for index, value in filtered.value_counts().items()}


def _count_csv_rows(path: str | None) -> int:
    if not path or not Path(path).exists():
        return 0
    try:
        frame = pd.read_csv(path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return 0
    return int(len(frame.index))


def _write_replay_day_input_summary(
    *,
    replay_root: Path,
    requested_date: str,
    day_config: DailyTradingWorkflowConfig,
    day_result: DailyReplayDayResult,
) -> str:
    day_dir = replay_root / requested_date
    research_output_dir = Path(day_config.research_output_dir)
    registry_dir = Path(day_config.registry_dir)
    promoted_dir = Path(day_config.promoted_dir)
    portfolio_dir = Path(day_config.portfolio_dir)
    activated_dir = Path(day_config.activated_dir)
    paper_dir = Path(day_config.paper_output_dir)
    daily_summary = _load_daily_summary(day_dir)
    paper_summary = _load_paper_summary(day_dir)
    research_registry_path = registry_dir / "research_registry.json"
    promoted_index_path = promoted_dir / "promoted_strategies.json"
    portfolio_json_path = portfolio_dir / "strategy_portfolio.json"
    activated_json_path = activated_dir / "activated_strategy_portfolio.json"
    universe_symbols, universe_artifact_paths, strategy_preset_paths = _resolve_replay_universe_inputs(day_config)
    research_registry = _read_json(research_registry_path)
    missing_input_warnings: list[str] = []
    if day_config.stages.promote and not (research_output_dir.exists() and research_registry_path.exists()):
        missing_input_warnings.append("missing_research_artifacts_for_promotion")
    if day_config.stages.promote and int((research_registry.get("summary") or {}).get("run_count", 0) or 0) == 0:
        missing_input_warnings.append("empty_research_registry_for_promotion")
    if day_config.stages.build_portfolio and not promoted_index_path.exists():
        missing_input_warnings.append("missing_promoted_artifact")
    if day_config.stages.build_portfolio and _count_rows(promoted_index_path, key="strategies") == 0:
        missing_input_warnings.append("zero_promoted_strategies")
    if day_config.stages.activate_portfolio and not portfolio_json_path.exists():
        missing_input_warnings.append("missing_strategy_portfolio_artifact")
    if day_config.stages.paper_run and not activated_json_path.exists() and not portfolio_json_path.exists():
        missing_input_warnings.append("missing_portfolio_input_for_paper")
    payload = {
        "replay_date": requested_date,
        "daily_status": day_result.status,
        "execution_config": {
            "slippage_model": str(day_config.slippage_model or "none"),
            "slippage_buy_bps": float(day_config.slippage_buy_bps or 0.0),
            "slippage_sell_bps": float(day_config.slippage_sell_bps or 0.0),
            "cost_model_enabled": bool(day_config.enable_cost_model),
            "commission_bps": float(day_config.commission_bps or 0.0),
            "minimum_commission": float(day_config.minimum_commission or 0.0),
            "spread_bps": float(day_config.spread_bps or 0.0),
            "min_weight_change_to_trade": float(day_config.min_weight_change_to_trade or 0.0),
            "score_band_enabled": any(
                value is not None
                for value in (
                    day_config.entry_score_threshold,
                    day_config.exit_score_threshold,
                    day_config.entry_score_percentile,
                    day_config.exit_score_percentile,
                )
            ),
            "entry_score_threshold": day_config.entry_score_threshold,
            "exit_score_threshold": day_config.exit_score_threshold,
            "hold_score_band": bool(day_config.hold_score_band),
            "use_percentile_thresholds": bool(day_config.use_percentile_thresholds),
            "entry_score_percentile": day_config.entry_score_percentile,
            "exit_score_percentile": day_config.exit_score_percentile,
            "ev_gate_enabled": bool(day_config.ev_gate_enabled),
            "ev_gate_model_type": str(day_config.ev_gate_model_type or "bucketed_mean"),
            "ev_gate_mode": str(day_config.ev_gate_mode or "hard"),
            "ev_gate_target_type": str(day_config.ev_gate_target_type or "market_proxy"),
            "ev_gate_hybrid_alpha": float(day_config.ev_gate_hybrid_alpha or 0.8),
            "ev_gate_training_source": str(day_config.ev_gate_training_source or "executed_trades"),
            "ev_gate_normalization_method": str(day_config.ev_gate_normalization_method or "zscore"),
            "ev_gate_normalize_within": str(day_config.ev_gate_normalize_within or "all_candidates"),
            "ev_gate_use_normalized_score_for_weighting": bool(
                day_config.ev_gate_use_normalized_score_for_weighting
            ),
            "ev_gate_weight_multiplier": bool(day_config.ev_gate_weight_multiplier),
            "ev_gate_weight_scale": float(day_config.ev_gate_weight_scale or 0.0),
            "ev_gate_use_confidence_weighting": bool(day_config.ev_gate_use_confidence_weighting),
            "ev_gate_confidence_method": str(day_config.ev_gate_confidence_method or "residual_std"),
            "ev_gate_confidence_scale": float(day_config.ev_gate_confidence_scale),
            "ev_gate_confidence_clip_min": float(day_config.ev_gate_confidence_clip_min),
            "ev_gate_confidence_clip_max": float(day_config.ev_gate_confidence_clip_max),
            "ev_gate_confidence_min_samples_per_bucket": int(
                day_config.ev_gate_confidence_min_samples_per_bucket
            ),
            "ev_gate_confidence_shrinkage_enabled": bool(day_config.ev_gate_confidence_shrinkage_enabled),
            "ev_gate_confidence_component_residual_std_weight": float(
                day_config.ev_gate_confidence_component_residual_std_weight
            ),
            "ev_gate_confidence_component_magnitude_weight": float(
                day_config.ev_gate_confidence_component_magnitude_weight
            ),
            "ev_gate_confidence_component_model_performance_weight": float(
                day_config.ev_gate_confidence_component_model_performance_weight
            ),
            "ev_gate_use_confidence_filter": bool(day_config.ev_gate_use_confidence_filter),
            "ev_gate_confidence_threshold": float(day_config.ev_gate_confidence_threshold),
            "ev_gate_horizon_days": int(day_config.ev_gate_horizon_days or 5),
            "ev_gate_min_expected_net_return": float(day_config.ev_gate_min_expected_net_return or 0.0),
        },
        "artifact_paths_used": {
            "research_output_dir": str(research_output_dir),
            "research_registry_path": str(research_registry_path),
            "promoted_dir": str(promoted_dir),
            "promoted_index_path": str(promoted_index_path),
            "portfolio_dir": str(portfolio_dir),
            "portfolio_json_path": str(portfolio_json_path),
            "activated_dir": str(activated_dir),
            "activated_json_path": str(activated_json_path),
            "paper_output_dir": str(paper_dir),
            "strategy_weighting_metrics_path": day_config.strategy_weighting_metrics_path,
            "trade_decision_log_path": day_result.trade_decision_log_path,
        },
        "artifact_exists": {
            "research_output_dir": research_output_dir.exists(),
            "research_registry_path": research_registry_path.exists(),
            "promoted_index_path": promoted_index_path.exists(),
            "portfolio_json_path": portfolio_json_path.exists(),
            "activated_json_path": activated_json_path.exists(),
            "paper_output_dir": paper_dir.exists(),
        },
        "universe_artifact_paths_used": universe_artifact_paths,
        "universe_symbol_count": len(universe_symbols),
        "strategy_preset_paths_used": strategy_preset_paths,
        "research_run_count": int((research_registry.get("summary") or {}).get("run_count", 0) or 0),
        "promoted_strategy_count": _count_rows(promoted_index_path, key="strategies"),
        "selected_strategy_count": _count_selected_strategy_rows(portfolio_json_path),
        "active_strategy_count": _count_active_strategy_rows(activated_json_path),
        "requested_symbol_count": int(paper_summary.get("requested_symbol_count", 0) or 0),
        "usable_symbol_count": int(paper_summary.get("usable_symbol_count", 0) or 0),
        "target_construction_ran": (paper_dir / "allocation_summary.json").exists(),
        "validation_removed_all_symbols": bool(paper_summary.get("requested_symbol_count", 0))
        and int(paper_summary.get("usable_symbol_count", 0) or 0) == 0,
        "validation_drop_reason_counts": _build_validation_drop_reason_counts(day_dir),
        "decision_log_rows_written": _count_csv_rows(day_result.trade_decision_log_path),
        "missing_input_warnings": sorted(dict.fromkeys(missing_input_warnings)),
        "stage_statuses": dict((daily_summary or {}).get("stage_statuses") or {}),
    }
    output_path = day_dir / "replay_day_input_summary.json"
    output_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return str(output_path)


def _day_status_failed(status: str) -> bool:
    return status in {"failed", "partial_failed"}


def _safe_copy(src: Path, dst: Path) -> str | None:
    if not src.exists():
        return None
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    return str(dst)


def _load_paper_summary(day_dir: Path) -> dict[str, Any]:
    payload = _read_json(day_dir / "paper" / "paper_run_summary_latest.json")
    return dict(payload.get("summary") or payload)


def _load_daily_summary(day_dir: Path) -> dict[str, Any]:
    return _read_json(day_dir / "daily_trading_summary.json")


def _collect_trade_log_rows(day_dir: Path, requested_date: str) -> list[dict[str, Any]]:
    fills_path = day_dir / "paper" / "paper_fills.csv"
    if not fills_path.exists():
        return []
    try:
        frame = pd.read_csv(fills_path)
    except pd.errors.EmptyDataError:
        return []
    if frame.empty:
        return []
    frame["date"] = requested_date
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _collect_strategy_activity_rows(day_dir: Path, requested_date: str) -> list[dict[str, Any]]:
    path = day_dir / "report" / "strategy_comparison_summary.csv"
    if not path.exists():
        return []
    frame = pd.read_csv(path)
    if frame.empty:
        return []
    frame["date"] = requested_date
    return frame.astype(object).where(pd.notna(frame), None).to_dict(orient="records")


def _aggregate_candidate_ev_dataset(replay_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    coverage_rows: list[dict[str, Any]] = []
    for day_dir in sorted(path for path in replay_root.iterdir() if path.is_dir()):
        try:
            requested_date = str(pd.Timestamp(day_dir.name).date())
        except (TypeError, ValueError):
            continue
        candidate_path = day_dir / "paper" / "trade_candidate_dataset.csv"
        training_summary_path = day_dir / "paper" / "trade_ev_training_summary.json"
        candidate_frame = _read_csv(candidate_path)
        training_summary = _read_json(training_summary_path)
        coverage_rows.append(
            {
                "date": requested_date,
                "candidate_row_count": int(len(candidate_frame.index)) if not candidate_frame.empty else 0,
                "executed_row_count": int(
                    (candidate_frame.get("candidate_outcome", pd.Series(dtype=object)).astype(str) == "executed").sum()
                )
                if not candidate_frame.empty
                else 0,
                "skipped_row_count": int(
                    (candidate_frame.get("candidate_status", pd.Series(dtype=object)).astype(str) == "skipped").sum()
                )
                if not candidate_frame.empty
                else 0,
                "training_source_used": str(training_summary.get("training_source", "executed_trades") or "executed_trades"),
                "target_type": str(training_summary.get("target_type", "market_proxy") or "market_proxy"),
                "hybrid_alpha": float(training_summary.get("hybrid_alpha", 0.8) or 0.8),
                "requested_training_source": str(
                    training_summary.get("requested_training_source", training_summary.get("training_source", "executed_trades"))
                    or "executed_trades"
                ),
                "training_sample_count": int(training_summary.get("training_sample_count", 0) or 0),
                "labeled_row_count": int(training_summary.get("labeled_row_count", 0) or 0),
                "positive_label_rate": float(training_summary.get("positive_label_rate", 0.0) or 0.0),
                "fallback_reason": str(training_summary.get("fallback_reason", "") or ""),
            }
        )
    summary = {
        "candidate_row_count": int(sum(int(row.get("candidate_row_count", 0) or 0) for row in coverage_rows)),
        "executed_row_count": int(sum(int(row.get("executed_row_count", 0) or 0) for row in coverage_rows)),
        "skipped_row_count": int(sum(int(row.get("skipped_row_count", 0) or 0) for row in coverage_rows)),
        "labeled_row_count": int(sum(int(row.get("labeled_row_count", 0) or 0) for row in coverage_rows)),
        "avg_positive_label_rate": (
            float(pd.DataFrame(coverage_rows)["positive_label_rate"].mean())
            if coverage_rows
            else 0.0
        ),
        "target_type": str(coverage_rows[-1]["target_type"]) if coverage_rows else "market_proxy",
        "hybrid_alpha": float(coverage_rows[-1]["hybrid_alpha"]) if coverage_rows else 0.8,
        "training_source_used": str(coverage_rows[-1]["training_source_used"]) if coverage_rows else "executed_trades",
    }
    return coverage_rows, summary


def _compute_replay_summary(
    *,
    config: DailyReplayWorkflowConfig,
    replay_root: Path,
    day_results: list[DailyReplayDayResult],
    requested_dates: list[str],
    daily_metric_rows: list[dict[str, Any]],
    trade_log_rows: list[dict[str, Any]],
    strategy_activity_rows: list[dict[str, Any]],
    replay_day_input_summaries: list[dict[str, Any]],
    state_transition_consistent: bool,
    holdings_changed: bool,
    aborted: bool,
) -> tuple[dict[str, Any], list[str], dict[str, bool]]:
    metrics_frame = pd.DataFrame(daily_metric_rows)
    activity_frame = pd.DataFrame(strategy_activity_rows)
    trade_frame = pd.DataFrame(trade_log_rows)
    input_frame = pd.DataFrame(replay_day_input_summaries)

    total_order_count = (
        int(metrics_frame["executable_order_count"].sum()) if "executable_order_count" in metrics_frame else 0
    )
    total_fill_count = int(metrics_frame["fill_count"].sum()) if "fill_count" in metrics_frame else 0
    failed_day_count = int(sum(_day_status_failed(row.status) or bool(row.error_message) for row in day_results))
    successful_day_count = int(
        sum(not (_day_status_failed(row.status) or bool(row.error_message)) for row in day_results)
    )
    trade_day_count = int(sum(int(row.get("fill_count", 0) or 0) > 0 for row in daily_metric_rows))
    no_op_day_count = int(sum(int(row.get("executable_order_count", 0) or 0) == 0 for row in daily_metric_rows))
    avg_daily_turnover = (
        float(metrics_frame["turnover_estimate"].mean())
        if "turnover_estimate" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_active_position_count = (
        float(metrics_frame["position_count"].mean())
        if "position_count" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_requested_symbol_count = (
        float(metrics_frame["requested_symbol_count"].mean())
        if "requested_symbol_count" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_usable_symbol_count = (
        float(metrics_frame["usable_symbol_count"].mean())
        if "usable_symbol_count" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    blocked_entries_count = (
        int(metrics_frame["blocked_entries_count"].sum())
        if "blocked_entries_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    held_in_hold_zone_count = (
        int(metrics_frame["held_in_hold_zone_count"].sum())
        if "held_in_hold_zone_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    forced_exit_count = (
        int(metrics_frame["forced_exit_count"].sum())
        if "forced_exit_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    ev_gate_blocked_count = (
        int(metrics_frame["ev_gate_blocked_count"].sum())
        if "ev_gate_blocked_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    confidence_filtered_count = (
        int(metrics_frame["confidence_filtered_count"].sum())
        if "confidence_filtered_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    avg_expected_net_return_traded = (
        float(metrics_frame["avg_expected_net_return_traded"].mean())
        if "avg_expected_net_return_traded" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_expected_net_return_blocked = (
        float(metrics_frame["avg_expected_net_return_blocked"].mean())
        if "avg_expected_net_return_blocked" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_ev_executed_trades = (
        float(metrics_frame["avg_ev_executed_trades"].mean())
        if "avg_ev_executed_trades" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_raw_ev_executed_trades = (
        float(metrics_frame["avg_raw_ev_executed_trades"].mean())
        if "avg_raw_ev_executed_trades" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_normalized_ev_executed_trades = (
        float(metrics_frame["avg_normalized_ev_executed_trades"].mean())
        if "avg_normalized_ev_executed_trades" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_ev_weighting_score = (
        float(metrics_frame["avg_ev_weighting_score"].mean())
        if "avg_ev_weighting_score" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_ev_confidence = (
        float(metrics_frame["avg_ev_confidence"].mean())
        if "avg_ev_confidence" in metrics_frame and not metrics_frame.empty
        else 1.0
    )
    avg_ev_confidence_multiplier = (
        float(metrics_frame["avg_ev_confidence_multiplier"].mean())
        if "avg_ev_confidence_multiplier" in metrics_frame and not metrics_frame.empty
        else 1.0
    )
    avg_ev_score_before_confidence = (
        float(metrics_frame["avg_ev_score_before_confidence"].mean())
        if "avg_ev_score_before_confidence" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_ev_score_after_confidence = (
        float(metrics_frame["avg_ev_score_after_confidence"].mean())
        if "avg_ev_score_after_confidence" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    ev_weighted_exposure = (
        float(metrics_frame["ev_weighted_exposure"].sum())
        if "ev_weighted_exposure" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_ev_weight_multiplier = (
        float(metrics_frame["avg_ev_weight_multiplier"].mean())
        if "avg_ev_weight_multiplier" in metrics_frame and not metrics_frame.empty
        else 1.0
    )
    regression_prediction_available_count = (
        int(metrics_frame["regression_prediction_available_count"].sum())
        if "regression_prediction_available_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    regression_prediction_missing_count = (
        int(metrics_frame["regression_prediction_missing_count"].sum())
        if "regression_prediction_missing_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    avg_regression_ev_executed_trades = (
        float(metrics_frame["avg_regression_ev_executed_trades"].mean())
        if "avg_regression_ev_executed_trades" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    avg_regression_ev_weighting_score = (
        float(metrics_frame["avg_regression_ev_weighting_score"].mean())
        if "avg_regression_ev_weighting_score" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    regression_ev_weighted_exposure = (
        float(metrics_frame["regression_ev_weighted_exposure"].sum())
        if "regression_ev_weighted_exposure" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    candidate_dataset_row_count = (
        int(metrics_frame["candidate_dataset_row_count"].sum())
        if "candidate_dataset_row_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    candidate_executed_count = (
        int(metrics_frame["candidate_executed_count"].sum())
        if "candidate_executed_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    candidate_skipped_count = (
        int(metrics_frame["candidate_skipped_count"].sum())
        if "candidate_skipped_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    ev_model_type = (
        str(metrics_frame["ev_gate_model_type"].iloc[-1])
        if "ev_gate_model_type" in metrics_frame and not metrics_frame.empty
        else "bucketed_mean"
    )
    ev_model_type_requested = (
        str(metrics_frame["ev_model_type_requested"].iloc[-1])
        if "ev_model_type_requested" in metrics_frame and not metrics_frame.empty
        else ev_model_type
    )
    ev_model_type_used = (
        str(metrics_frame["ev_model_type_used"].iloc[-1])
        if "ev_model_type_used" in metrics_frame and not metrics_frame.empty
        else ev_model_type
    )
    ev_model_fallback_reason = (
        str(metrics_frame["ev_model_fallback_reason"].iloc[-1])
        if "ev_model_fallback_reason" in metrics_frame and not metrics_frame.empty
        else ""
    )
    ev_gate_enabled = bool(metrics_frame["ev_gate_enabled"].any()) if "ev_gate_enabled" in metrics_frame and not metrics_frame.empty else False
    ev_gate_mode = (
        str(metrics_frame["ev_gate_mode"].iloc[-1])
        if "ev_gate_mode" in metrics_frame and not metrics_frame.empty
        else "hard"
    )
    ev_gate_target_type = (
        str(metrics_frame["ev_gate_target_type"].iloc[-1])
        if "ev_gate_target_type" in metrics_frame and not metrics_frame.empty
        else "market_proxy"
    )
    ev_gate_hybrid_alpha = (
        float(metrics_frame["ev_gate_hybrid_alpha"].iloc[-1])
        if "ev_gate_hybrid_alpha" in metrics_frame and not metrics_frame.empty
        else 0.8
    )
    ev_gate_training_source = (
        str(metrics_frame["ev_gate_training_source"].iloc[-1])
        if "ev_gate_training_source" in metrics_frame and not metrics_frame.empty
        else "executed_trades"
    )
    ev_gate_normalization_method = (
        str(metrics_frame["ev_gate_normalization_method"].iloc[-1])
        if "ev_gate_normalization_method" in metrics_frame and not metrics_frame.empty
        else "zscore"
    )
    ev_gate_normalize_within = (
        str(metrics_frame["ev_gate_normalize_within"].iloc[-1])
        if "ev_gate_normalize_within" in metrics_frame and not metrics_frame.empty
        else "all_candidates"
    )
    ev_gate_use_normalized_score_for_weighting = (
        bool(metrics_frame["ev_gate_use_normalized_score_for_weighting"].iloc[-1])
        if "ev_gate_use_normalized_score_for_weighting" in metrics_frame and not metrics_frame.empty
        else True
    )
    ev_model_sample_count = (
        int(metrics_frame["ev_model_sample_count"].iloc[-1])
        if "ev_model_sample_count" in metrics_frame and not metrics_frame.empty
        else 0
    )
    final_equity = (
        float(metrics_frame["current_equity"].iloc[-1])
        if "current_equity" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    final_gross_equity = (
        float(metrics_frame["final_gross_equity"].iloc[-1])
        if "final_gross_equity" in metrics_frame and not metrics_frame.empty
        else final_equity
    )
    cumulative_realized_pnl = (
        float(metrics_frame["cumulative_realized_pnl"].iloc[-1])
        if "cumulative_realized_pnl" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    cumulative_unrealized_pnl = (
        float(metrics_frame["unrealized_pnl"].iloc[-1])
        if "unrealized_pnl" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    gross_total_pnl = (
        float(metrics_frame["gross_total_pnl"].iloc[-1])
        if "gross_total_pnl" in metrics_frame and not metrics_frame.empty
        else float(cumulative_realized_pnl + cumulative_unrealized_pnl)
    )
    net_total_pnl = (
        float(metrics_frame["net_total_pnl"].iloc[-1])
        if "net_total_pnl" in metrics_frame and not metrics_frame.empty
        else final_equity
    )
    total_execution_cost = (
        float(metrics_frame["total_execution_cost"].iloc[-1])
        if "total_execution_cost" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    total_slippage_cost = (
        float(metrics_frame["total_slippage_cost"].iloc[-1])
        if "total_slippage_cost" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    total_commission_cost = (
        float(metrics_frame["total_commission_cost"].iloc[-1])
        if "total_commission_cost" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    total_spread_cost = (
        float(metrics_frame["total_spread_cost"].iloc[-1])
        if "total_spread_cost" in metrics_frame and not metrics_frame.empty
        else 0.0
    )
    if "current_equity" in metrics_frame and not metrics_frame.empty:
        equity_series = metrics_frame["current_equity"].astype(float)
        rolling_max = equity_series.cummax().replace(0.0, pd.NA)
        raw_drawdowns = (equity_series / rolling_max) - 1.0
        drawdowns = raw_drawdowns.where(pd.notna(raw_drawdowns), 0.0)
        max_drawdown = float(drawdowns.min())
    else:
        max_drawdown = 0.0
    if "final_gross_equity" in metrics_frame and not metrics_frame.empty:
        gross_equity_series = metrics_frame["final_gross_equity"].astype(float)
        gross_rolling_max = gross_equity_series.cummax().replace(0.0, pd.NA)
        gross_drawdowns = ((gross_equity_series / gross_rolling_max) - 1.0).where(pd.notna(gross_rolling_max), 0.0)
        max_drawdown_gross = float(gross_drawdowns.min())
    else:
        max_drawdown_gross = max_drawdown

    top_strategies_by_days_active: list[dict[str, Any]] = []
    top_strategies_by_average_weight: list[dict[str, Any]] = []
    if not activity_frame.empty and "strategy_id" in activity_frame:
        if "is_active" in activity_frame:
            active_days = (
                activity_frame.assign(is_active=activity_frame["is_active"].astype(bool))
                .groupby("strategy_id", dropna=False)["is_active"]
                .sum()
                .sort_values(ascending=False)
            )
            top_strategies_by_days_active = [
                {"strategy_id": str(index), "days_active": int(value)} for index, value in active_days.head(10).items()
            ]
        if "normalized_capital_weight" in activity_frame:
            avg_weights = (
                pd.to_numeric(activity_frame["normalized_capital_weight"], errors="coerce")
                .groupby(activity_frame["strategy_id"])
                .mean()
                .sort_values(ascending=False)
            )
            top_strategies_by_average_weight = [
                {"strategy_id": str(index), "average_weight": float(value)}
                for index, value in avg_weights.head(10).items()
            ]

    top_symbols_by_trade_count: list[dict[str, Any]] = []
    if not trade_frame.empty and "symbol" in trade_frame:
        trade_counts = trade_frame.groupby("symbol").size().sort_values(ascending=False)
        top_symbols_by_trade_count = [
            {"symbol": str(index), "trade_count": int(value)} for index, value in trade_counts.head(10).items()
        ]

    diagnostics_complete = all(
        Path(row.summary_json_path or "").exists() and Path(row.trade_decision_log_path or "").exists()
        for row in day_results
    )
    if config.daily_trading.refresh_dashboard_static_data:
        diagnostics_complete = diagnostics_complete and (replay_root / "dashboard").exists()

    readiness_flags = {
        "pipeline_stable": failed_day_count == 0 and not aborted,
        "generated_trades": total_order_count > 0 or total_fill_count > 0,
        "state_persistence_consistent": state_transition_consistent,
        "multi_strategy_active": bool(
            top_strategies_by_days_active
            and any(int(row.get("days_active", 0) or 0) > 0 for row in top_strategies_by_days_active[:2])
        )
        and any(int(row.get("active_strategy_count", 0) or 0) > 1 for row in daily_metric_rows),
        "diagnostics_complete": diagnostics_complete,
    }

    warnings: list[str] = []
    if config.replay.warn_if_all_days_no_op and daily_metric_rows and no_op_day_count == len(daily_metric_rows):
        warnings.append("all days were no-op")
    if (
        top_strategies_by_days_active
        and len([row for row in top_strategies_by_days_active if int(row["days_active"]) > 0]) <= 1
    ):
        warnings.append("only one strategy was ever active")
    if not holdings_changed and daily_metric_rows:
        warnings.append("holdings never changed")
    if failed_day_count > max(1, len(day_results) // 4):
        warnings.append("too many execution failures")
    if not diagnostics_complete:
        warnings.append("missing dashboard/report artifacts")
    if not state_transition_consistent:
        warnings.append("inconsistent state transitions")
    if not input_frame.empty and any(
        bool(rows) for rows in input_frame.get("missing_input_warnings", pd.Series(dtype=object)).tolist()
    ):
        warnings.append("missing replay upstream inputs")
    if config.replay.min_expected_trade_days is not None and trade_day_count < config.replay.min_expected_trade_days:
        warnings.append("trade day count below configured expectation")
    if (
        config.replay.min_expected_total_trades is not None
        and total_fill_count < config.replay.min_expected_total_trades
    ):
        warnings.append("total trades below configured expectation")
    if (
        config.replay.warn_if_turnover_too_low is not None
        and avg_daily_turnover < config.replay.warn_if_turnover_too_low
    ):
        warnings.append("turnover below configured threshold")

    summary = {
        "workflow_type": "daily_replay",
        "start_date": requested_dates[0] if requested_dates else None,
        "end_date": requested_dates[-1] if requested_dates else None,
        "trading_day_count": len(day_results),
        "successful_day_count": successful_day_count,
        "failed_day_count": failed_day_count,
        "requested_dates": requested_dates,
        "processed_dates": [row.requested_date for row in day_results],
        "total_order_count": total_order_count,
        "total_fill_count": total_fill_count,
        "trade_day_count": trade_day_count,
        "no_op_day_count": no_op_day_count,
        "avg_daily_turnover": avg_daily_turnover,
        "avg_active_position_count": avg_active_position_count,
        "avg_requested_symbol_count": avg_requested_symbol_count,
        "avg_usable_symbol_count": avg_usable_symbol_count,
        "blocked_entries_count": blocked_entries_count,
        "held_in_hold_zone_count": held_in_hold_zone_count,
        "forced_exit_count": forced_exit_count,
        "ev_gate_blocked_count": ev_gate_blocked_count,
        "confidence_filtered_count": confidence_filtered_count,
        "ev_gate_enabled": ev_gate_enabled,
        "ev_gate_mode": ev_gate_mode,
        "ev_gate_target_type": ev_gate_target_type,
        "ev_gate_hybrid_alpha": ev_gate_hybrid_alpha,
        "ev_gate_training_source": ev_gate_training_source,
        "ev_gate_normalization_method": ev_gate_normalization_method,
        "ev_gate_normalize_within": ev_gate_normalize_within,
        "ev_gate_use_normalized_score_for_weighting": ev_gate_use_normalized_score_for_weighting,
        "ev_model_type": ev_model_type,
        "ev_model_type_requested": ev_model_type_requested,
        "ev_model_type_used": ev_model_type_used,
        "ev_model_fallback_reason": ev_model_fallback_reason,
        "avg_expected_net_return_traded": avg_expected_net_return_traded,
        "avg_expected_net_return_blocked": avg_expected_net_return_blocked,
        "avg_ev_executed_trades": avg_ev_executed_trades,
        "avg_raw_ev_executed_trades": avg_raw_ev_executed_trades,
        "avg_normalized_ev_executed_trades": avg_normalized_ev_executed_trades,
        "avg_ev_weighting_score": avg_ev_weighting_score,
        "avg_ev_confidence": avg_ev_confidence,
        "avg_ev_confidence_multiplier": avg_ev_confidence_multiplier,
        "avg_ev_score_before_confidence": avg_ev_score_before_confidence,
        "avg_ev_score_after_confidence": avg_ev_score_after_confidence,
        "ev_weighted_exposure": ev_weighted_exposure,
        "avg_ev_weight_multiplier": avg_ev_weight_multiplier,
        "regression_prediction_available_count": regression_prediction_available_count,
        "regression_prediction_missing_count": regression_prediction_missing_count,
        "avg_regression_ev_executed_trades": avg_regression_ev_executed_trades,
        "avg_regression_ev_weighting_score": avg_regression_ev_weighting_score,
        "regression_ev_weighted_exposure": regression_ev_weighted_exposure,
        "ev_model_sample_count": ev_model_sample_count,
        "candidate_dataset_row_count": candidate_dataset_row_count,
        "candidate_executed_count": candidate_executed_count,
        "candidate_skipped_count": candidate_skipped_count,
        "cumulative_realized_pnl": cumulative_realized_pnl,
        "cumulative_unrealized_pnl": cumulative_unrealized_pnl,
        "gross_total_pnl": gross_total_pnl,
        "net_total_pnl": net_total_pnl,
        "final_equity": final_equity,
        "final_gross_equity": final_gross_equity,
        "final_net_equity": final_equity,
        "max_drawdown": max_drawdown,
        "max_drawdown_gross": max_drawdown_gross,
        "max_drawdown_net": max_drawdown,
        "total_execution_cost": total_execution_cost,
        "total_slippage_cost": total_slippage_cost,
        "total_commission_cost": total_commission_cost,
        "total_spread_cost": total_spread_cost,
        "avg_daily_execution_cost": (
            float(metrics_frame["execution_cost_delta"].mean())
            if "execution_cost_delta" in metrics_frame and not metrics_frame.empty
            else 0.0
        ),
        "cost_drag_pct": (total_execution_cost / gross_total_pnl) if gross_total_pnl not in (0.0, -0.0) else 0.0,
        "top_strategies_by_days_active": top_strategies_by_days_active,
        "top_strategies_by_average_weight": top_strategies_by_average_weight,
        "top_symbols_by_trade_count": top_symbols_by_trade_count,
        "readiness_flags": readiness_flags,
        "warnings": warnings,
        "aborted": aborted,
        "missing_input_days": [
            {
                "date": str(row.get("replay_date")),
                "warnings": list(row.get("missing_input_warnings") or []),
            }
            for row in replay_day_input_summaries
            if row.get("missing_input_warnings")
        ],
    }
    return summary, warnings, readiness_flags


def _write_replay_summary_artifacts(
    *,
    replay_root: Path,
    summary: dict[str, Any],
    daily_metric_rows: list[dict[str, Any]],
    trade_log_rows: list[dict[str, Any]],
    strategy_activity_rows: list[dict[str, Any]],
) -> dict[str, str]:
    summary_json_path = replay_root / "replay_summary.json"
    summary_md_path = replay_root / "replay_summary.md"
    summary_json_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    summary_md_path.write_text(
        "\n".join(
            [
                "# Daily Replay Summary",
                "",
                f"- start_date: `{summary.get('start_date')}`",
                f"- end_date: `{summary.get('end_date')}`",
                f"- trading_day_count: `{summary.get('trading_day_count', 0)}`",
                f"- successful_day_count: `{summary.get('successful_day_count', 0)}`",
                f"- failed_day_count: `{summary.get('failed_day_count', 0)}`",
                f"- total_order_count: `{summary.get('total_order_count', 0)}`",
                f"- total_fill_count: `{summary.get('total_fill_count', 0)}`",
                f"- trade_day_count: `{summary.get('trade_day_count', 0)}`",
                f"- no_op_day_count: `{summary.get('no_op_day_count', 0)}`",
                f"- avg_daily_turnover: `{summary.get('avg_daily_turnover', 0.0)}`",
                f"- blocked_entries_count: `{summary.get('blocked_entries_count', 0)}`",
                f"- held_in_hold_zone_count: `{summary.get('held_in_hold_zone_count', 0)}`",
                f"- forced_exit_count: `{summary.get('forced_exit_count', 0)}`",
                f"- ev_gate_enabled: `{summary.get('ev_gate_enabled', False)}`",
                f"- ev_gate_mode: `{summary.get('ev_gate_mode', 'hard')}`",
                f"- ev_gate_target_type: `{summary.get('ev_gate_target_type', 'market_proxy')}`",
                f"- ev_gate_hybrid_alpha: `{summary.get('ev_gate_hybrid_alpha', 0.8)}`",
                f"- ev_gate_training_source: `{summary.get('ev_gate_training_source', 'executed_trades')}`",
                f"- ev_gate_normalization_method: `{summary.get('ev_gate_normalization_method', 'zscore')}`",
                f"- ev_gate_normalize_within: `{summary.get('ev_gate_normalize_within', 'all_candidates')}`",
                f"- ev_gate_use_normalized_score_for_weighting: `{summary.get('ev_gate_use_normalized_score_for_weighting', True)}`",
                f"- ev_model_type: `{summary.get('ev_model_type', 'bucketed_mean')}`",
                f"- ev_gate_blocked_count: `{summary.get('ev_gate_blocked_count', 0)}`",
                f"- confidence_filtered_count: `{summary.get('confidence_filtered_count', 0)}`",
                f"- avg_ev_executed_trades: `{summary.get('avg_ev_executed_trades', 0.0)}`",
                f"- avg_raw_ev_executed_trades: `{summary.get('avg_raw_ev_executed_trades', 0.0)}`",
                f"- avg_normalized_ev_executed_trades: `{summary.get('avg_normalized_ev_executed_trades', 0.0)}`",
                f"- avg_ev_weighting_score: `{summary.get('avg_ev_weighting_score', 0.0)}`",
                f"- avg_ev_confidence: `{summary.get('avg_ev_confidence', 1.0)}`",
                f"- avg_ev_confidence_multiplier: `{summary.get('avg_ev_confidence_multiplier', 1.0)}`",
                f"- avg_ev_score_before_confidence: `{summary.get('avg_ev_score_before_confidence', 0.0)}`",
                f"- avg_ev_score_after_confidence: `{summary.get('avg_ev_score_after_confidence', 0.0)}`",
                f"- ev_weighted_exposure: `{summary.get('ev_weighted_exposure', 0.0)}`",
                f"- ev_model_type_requested: `{summary.get('ev_model_type_requested', 'bucketed_mean')}`",
                f"- ev_model_type_used: `{summary.get('ev_model_type_used', 'bucketed_mean')}`",
                f"- regression_prediction_available_count: `{summary.get('regression_prediction_available_count', 0)}`",
                f"- avg_regression_ev_executed_trades: `{summary.get('avg_regression_ev_executed_trades', 0.0)}`",
                f"- avg_regression_ev_weighting_score: `{summary.get('avg_regression_ev_weighting_score', 0.0)}`",
                f"- regression_ev_weighted_exposure: `{summary.get('regression_ev_weighted_exposure', 0.0)}`",
                f"- regression_ev_rank_correlation: `{summary.get('regression_ev_rank_correlation', 0.0)}`",
                f"- candidate_dataset_row_count: `{summary.get('candidate_dataset_row_count', 0)}`",
                f"- candidate_executed_count: `{summary.get('candidate_executed_count', 0)}`",
                f"- candidate_skipped_count: `{summary.get('candidate_skipped_count', 0)}`",
                f"- ev_rank_correlation: `{summary.get('ev_rank_correlation', 0.0)}`",
                f"- ev_top_vs_bottom_bucket_spread: `{summary.get('ev_top_vs_bottom_bucket_spread', 0.0)}`",
                f"- avg_EV_entry: `{summary.get('avg_EV_entry', 0.0)}`",
                f"- avg_EV_exit: `{summary.get('avg_EV_exit', 0.0)}`",
                f"- exit_efficiency: `{summary.get('exit_efficiency', 0.0)}`",
                f"- EV_alignment_rate: `{summary.get('EV_alignment_rate', 0.0)}`",
                f"- pct_trades_EV_entry_positive: `{summary.get('pct_trades_EV_entry_positive', 0.0)}`",
                f"- pct_exits_EV_exit_negative: `{summary.get('pct_exits_EV_exit_negative', 0.0)}`",
                f"- EV_entry_realized_return_correlation: `{summary.get('EV_entry_realized_return_correlation', 0.0)}`",
                f"- MFE_vs_EV_entry_correlation: `{summary.get('MFE_vs_EV_entry_correlation', 0.0)}`",
                f"- avg_daily_execution_cost: `{summary.get('avg_daily_execution_cost', 0.0)}`",
                f"- gross_total_pnl: `{summary.get('gross_total_pnl', 0.0)}`",
                f"- net_total_pnl: `{summary.get('net_total_pnl', 0.0)}`",
                f"- total_execution_cost: `{summary.get('total_execution_cost', 0.0)}`",
                f"- final_equity: `{summary.get('final_equity', 0.0)}`",
                f"- final_gross_equity: `{summary.get('final_gross_equity', 0.0)}`",
                f"- max_drawdown: `{summary.get('max_drawdown', 0.0)}`",
                f"- max_drawdown_gross: `{summary.get('max_drawdown_gross', 0.0)}`",
                "",
                "## Readiness",
                "",
            ]
            + [f"- {key}: `{value}`" for key, value in sorted((summary.get("readiness_flags") or {}).items())]
            + (
                ["", "## Warnings", ""] + [f"- {warning}" for warning in summary.get("warnings", [])]
                if summary.get("warnings")
                else []
            )
        ),
        encoding="utf-8",
    )
    metrics_path = _write_csv(
        replay_root / "replay_daily_metrics.csv",
        daily_metric_rows,
        [
            "date",
            "status",
            "active_strategy_count",
            "requested_symbol_count",
            "usable_symbol_count",
            "executable_order_count",
            "skipped_trades_count",
            "skipped_turnover",
            "effective_turnover_reduction",
            "blocked_entries_count",
            "held_in_hold_zone_count",
            "forced_exit_count",
            "ev_gate_blocked_count",
            "confidence_filtered_count",
            "avg_expected_net_return_traded",
            "avg_expected_net_return_blocked",
            "avg_ev_executed_trades",
            "avg_raw_ev_executed_trades",
            "avg_normalized_ev_executed_trades",
            "avg_ev_weighting_score",
            "avg_ev_confidence",
            "avg_ev_confidence_multiplier",
            "avg_ev_score_before_confidence",
            "avg_ev_score_after_confidence",
            "ev_weighted_exposure",
            "avg_ev_weight_multiplier",
            "regression_prediction_available_count",
            "regression_prediction_missing_count",
            "avg_regression_ev_executed_trades",
            "avg_regression_ev_weighting_score",
            "regression_ev_weighted_exposure",
            "candidate_dataset_row_count",
            "candidate_executed_count",
            "candidate_skipped_count",
            "score_band_enabled",
            "entry_threshold_used",
            "exit_threshold_used",
            "ev_gate_enabled",
            "ev_gate_mode",
            "ev_gate_training_source",
            "ev_gate_normalization_method",
            "ev_gate_normalize_within",
            "ev_gate_use_normalized_score_for_weighting",
            "ev_gate_model_type",
            "ev_model_type_requested",
            "ev_model_type_used",
            "ev_model_fallback_reason",
            "ev_model_sample_count",
            "fill_count",
            "turnover_estimate",
            "position_count",
            "current_equity",
            "final_gross_equity",
            "cumulative_realized_pnl",
            "unrealized_pnl",
            "gross_total_pnl",
            "net_total_pnl",
            "execution_cost_delta",
            "total_execution_cost",
            "total_slippage_cost",
            "total_commission_cost",
            "total_spread_cost",
            "zero_target_reason",
        ],
    )
    trade_log_path = _write_csv(
        replay_root / "replay_trade_log.csv",
        trade_log_rows,
        sorted({key for row in trade_log_rows for key in row})
        if trade_log_rows
        else ["date", "symbol", "side", "quantity"],
    )
    strategy_activity_path = _write_csv(
        replay_root / "replay_strategy_activity.csv",
        strategy_activity_rows,
        sorted({key for row in strategy_activity_rows for key in row})
        if strategy_activity_rows
        else ["date", "strategy_id", "is_active", "normalized_capital_weight"],
    )
    return {
        "replay_summary_json_path": str(summary_json_path),
        "replay_summary_md_path": str(summary_md_path),
        "replay_daily_metrics_csv_path": str(metrics_path),
        "replay_trade_log_csv_path": str(trade_log_path),
        "replay_strategy_activity_csv_path": str(strategy_activity_path),
    }


def run_daily_replay(config: DailyReplayWorkflowConfig) -> DailyReplayResult:
    replay_root = Path(config.output_dir)
    replay_root.mkdir(parents=True, exist_ok=True)
    replay_started = time.monotonic()
    requested_dates = build_daily_replay_dates(
        start_date=config.start_date,
        end_date=config.end_date,
        dates_file=config.dates_file,
        max_days=config.max_days,
    )
    state_path = replay_root / "replay_state.json"
    if config.initial_state_path and not state_path.exists():
        shutil.copyfile(config.initial_state_path, state_path)

    day_results: list[DailyReplayDayResult] = []
    daily_metric_rows: list[dict[str, Any]] = []
    trade_log_rows: list[dict[str, Any]] = []
    strategy_activity_rows: list[dict[str, Any]] = []
    replay_day_input_summaries: list[dict[str, Any]] = []
    timing_rows: list[dict[str, Any]] = []
    previous_state_after_payload: str | None = None
    previous_strategy_weighting_metrics_path: Path | None = None
    state_transition_consistent = True
    position_signatures: list[str] = []
    aborted = False

    for requested_date in requested_dates:
        day_started = time.monotonic()
        day_dir = replay_root / requested_date
        setup_started = time.monotonic()
        day_config = _build_day_config(
            config.daily_trading,
            replay_root=replay_root,
            requested_date=requested_date,
            state_path=state_path,
            strategy_weighting_metrics_path=previous_strategy_weighting_metrics_path,
        )
        setup_seconds = time.monotonic() - setup_started
        state_before_path = _safe_copy(state_path, day_dir / "paper_state_before.json")
        if previous_state_after_payload is not None and state_before_path is not None:
            current_before_payload = Path(state_before_path).read_text(encoding="utf-8")
            state_transition_consistent = state_transition_consistent and (
                current_before_payload == previous_state_after_payload
            )

        day_error_message: str | None = None
        result: DailyTradingResult | None = None
        pipeline_started = time.monotonic()
        try:
            result = run_daily_trading_pipeline(
                day_config,
                replay_as_of_date=requested_date,
                replay_settings=config.replay.__dict__,
                refresh_dashboard_static_data=False,
            )
        except Exception as exc:
            day_error_message = f"{type(exc).__name__}: {exc}"
            if config.stop_on_error and not config.continue_on_error:
                aborted = True
        pipeline_seconds = time.monotonic() - pipeline_started

        state_after_path = _safe_copy(state_path, day_dir / "paper_state_after.json")
        if state_after_path:
            previous_state_after_payload = Path(state_after_path).read_text(encoding="utf-8")
            state_payload = _read_json(Path(state_after_path))
            position_signatures.append(json.dumps(state_payload.get("positions", {}), sort_keys=True))
        current_strategy_metrics_path = day_dir / "paper" / "strategy_pnl_attribution.csv"
        if current_strategy_metrics_path.exists():
            previous_strategy_weighting_metrics_path = current_strategy_metrics_path

        daily_summary = _load_daily_summary(day_dir)
        paper_summary = _load_paper_summary(day_dir)
        daily_metric_rows.append(
            {
                "date": requested_date,
                "status": str(
                    (daily_summary or {}).get("status") or (result.status if result is not None else "failed")
                ),
                "active_strategy_count": int((daily_summary or {}).get("active_strategy_count", 0) or 0),
                "requested_symbol_count": int(paper_summary.get("requested_symbol_count", 0) or 0),
                "usable_symbol_count": int(paper_summary.get("usable_symbol_count", 0) or 0),
                "executable_order_count": int(paper_summary.get("executable_order_count", 0) or 0),
                "skipped_trades_count": int(paper_summary.get("skipped_trades_count", 0) or 0),
                "skipped_turnover": float(paper_summary.get("skipped_turnover", 0.0) or 0.0),
                "effective_turnover_reduction": float(
                    paper_summary.get("effective_turnover_reduction", 0.0) or 0.0
                ),
                "blocked_entries_count": int(paper_summary.get("blocked_entries_count", 0) or 0),
                "held_in_hold_zone_count": int(paper_summary.get("held_in_hold_zone_count", 0) or 0),
                "forced_exit_count": int(paper_summary.get("forced_exit_count", 0) or 0),
                "ev_gate_blocked_count": int(paper_summary.get("ev_gate_blocked_count", 0) or 0),
                "confidence_filtered_count": int(paper_summary.get("confidence_filtered_count", 0) or 0),
                "avg_expected_net_return_traded": float(
                    paper_summary.get("avg_expected_net_return_traded", 0.0) or 0.0
                ),
                "avg_expected_net_return_blocked": float(
                    paper_summary.get("avg_expected_net_return_blocked", 0.0) or 0.0
                ),
                "avg_ev_executed_trades": float(paper_summary.get("avg_ev_executed_trades", 0.0) or 0.0),
                "avg_raw_ev_executed_trades": float(
                    paper_summary.get("avg_raw_ev_executed_trades", 0.0) or 0.0
                ),
                "avg_normalized_ev_executed_trades": float(
                    paper_summary.get("avg_normalized_ev_executed_trades", 0.0) or 0.0
                ),
                "avg_ev_weighting_score": float(paper_summary.get("avg_ev_weighting_score", 0.0) or 0.0),
                "avg_ev_confidence": float(paper_summary.get("avg_ev_confidence", 1.0)),
                "avg_ev_confidence_multiplier": float(
                    paper_summary.get("avg_ev_confidence_multiplier", 1.0)
                ),
                "avg_ev_score_before_confidence": float(
                    paper_summary.get("avg_ev_score_before_confidence", 0.0) or 0.0
                ),
                "avg_ev_score_after_confidence": float(
                    paper_summary.get("avg_ev_score_after_confidence", 0.0) or 0.0
                ),
                "ev_weighted_exposure": float(paper_summary.get("ev_weighted_exposure", 0.0) or 0.0),
                "avg_ev_weight_multiplier": float(paper_summary.get("avg_ev_weight_multiplier", 1.0) or 1.0),
                "candidate_dataset_row_count": int(paper_summary.get("candidate_dataset_row_count", 0) or 0),
                "candidate_executed_count": int(paper_summary.get("candidate_executed_count", 0) or 0),
                "candidate_skipped_count": int(paper_summary.get("candidate_skipped_count", 0) or 0),
                "score_band_enabled": bool(paper_summary.get("score_band_enabled", False)),
                "entry_threshold_used": paper_summary.get("entry_threshold_used"),
                "exit_threshold_used": paper_summary.get("exit_threshold_used"),
                "ev_gate_enabled": bool(paper_summary.get("ev_gate_enabled", False)),
                "ev_gate_mode": str(paper_summary.get("ev_gate_mode", "hard") or "hard"),
                "ev_gate_target_type": str(paper_summary.get("ev_gate_target_type", "market_proxy") or "market_proxy"),
                "ev_gate_hybrid_alpha": float(paper_summary.get("ev_gate_hybrid_alpha", 0.8) or 0.8),
                "ev_gate_training_source": str(
                    paper_summary.get("ev_gate_training_source", "executed_trades") or "executed_trades"
                ),
                "ev_gate_normalization_method": str(
                    paper_summary.get("ev_gate_normalization_method", "zscore") or "zscore"
                ),
                "ev_gate_normalize_within": str(
                    paper_summary.get("ev_gate_normalize_within", "all_candidates") or "all_candidates"
                ),
                "ev_gate_use_normalized_score_for_weighting": bool(
                    paper_summary.get("ev_gate_use_normalized_score_for_weighting", True)
                ),
                "ev_gate_model_type": str(paper_summary.get("ev_gate_model_type", "bucketed_mean") or "bucketed_mean"),
                "ev_model_type_requested": str(
                    paper_summary.get("ev_model_type_requested", paper_summary.get("ev_gate_model_type", "bucketed_mean"))
                    or "bucketed_mean"
                ),
                "ev_model_type_used": str(
                    paper_summary.get("ev_model_type_used", paper_summary.get("ev_gate_model_type", "bucketed_mean"))
                    or "bucketed_mean"
                ),
                "ev_model_fallback_reason": str(paper_summary.get("ev_model_fallback_reason", "") or ""),
                "ev_model_sample_count": int(paper_summary.get("ev_model_sample_count", 0) or 0),
                "ev_labeled_row_count": int(paper_summary.get("ev_labeled_row_count", 0) or 0),
                "ev_excluded_unlabeled_row_count": int(
                    paper_summary.get("ev_excluded_unlabeled_row_count", 0) or 0
                ),
                "ev_average_target_value": float(paper_summary.get("ev_average_target_value", 0.0) or 0.0),
                "ev_positive_label_rate": float(paper_summary.get("ev_positive_label_rate", 0.0) or 0.0),
                "regression_prediction_available_count": int(
                    paper_summary.get("regression_prediction_available_count", 0) or 0
                ),
                "regression_prediction_missing_count": int(
                    paper_summary.get("regression_prediction_missing_count", 0) or 0
                ),
                "avg_regression_ev_executed_trades": float(
                    paper_summary.get("avg_regression_ev_executed_trades", 0.0) or 0.0
                ),
                "avg_regression_ev_weighting_score": float(
                    paper_summary.get("avg_regression_ev_weighting_score", 0.0) or 0.0
                ),
                "regression_ev_weighted_exposure": float(
                    paper_summary.get("regression_ev_weighted_exposure", 0.0) or 0.0
                ),
                "fill_count": int(paper_summary.get("fill_count", 0) or 0),
                "turnover_estimate": float(paper_summary.get("turnover_estimate", 0.0) or 0.0),
                "position_count": int(paper_summary.get("realized_holdings_count", 0) or 0),
                "current_equity": float(paper_summary.get("current_equity", 0.0) or 0.0),
                "final_gross_equity": float(paper_summary.get("final_gross_equity", paper_summary.get("current_equity", 0.0)) or 0.0),
                "cumulative_realized_pnl": float(paper_summary.get("cumulative_realized_pnl", 0.0) or 0.0),
                "unrealized_pnl": float(paper_summary.get("unrealized_pnl", 0.0) or 0.0),
                "gross_total_pnl": float(paper_summary.get("gross_total_pnl", 0.0) or 0.0),
                "net_total_pnl": float(paper_summary.get("net_total_pnl", paper_summary.get("total_pnl", 0.0)) or 0.0),
                "execution_cost_delta": float(paper_summary.get("execution_cost_delta", 0.0) or 0.0),
                "total_execution_cost": float(paper_summary.get("total_execution_cost", 0.0) or 0.0),
                "total_slippage_cost": float(paper_summary.get("total_slippage_cost", 0.0) or 0.0),
                "total_commission_cost": float(paper_summary.get("total_commission_cost", 0.0) or 0.0),
                "total_spread_cost": float(paper_summary.get("total_spread_cost", 0.0) or 0.0),
                "zero_target_reason": str(paper_summary.get("zero_target_reason", "") or ""),
            }
        )
        trade_log_rows.extend(_collect_trade_log_rows(day_dir, requested_date))
        strategy_activity_rows.extend(_collect_strategy_activity_rows(day_dir, requested_date))
        day_results.append(
            DailyReplayDayResult(
                requested_date=requested_date,
                run_dir=str(day_dir),
                status=str((daily_summary or {}).get("status") or (result.status if result is not None else "failed")),
                error_message=day_error_message,
                summary_json_path=str(day_dir / "daily_trading_summary.json")
                if (day_dir / "daily_trading_summary.json").exists()
                else None,
                trade_decision_log_path=str(day_dir / "trade_decision_log.csv")
                if (day_dir / "trade_decision_log.csv").exists()
                else None,
                input_summary_path=None,
                state_before_path=state_before_path,
                state_after_path=state_after_path,
            )
        )
        input_summary_started = time.monotonic()
        replay_day_input_summary_path = _write_replay_day_input_summary(
            replay_root=replay_root,
            requested_date=requested_date,
            day_config=day_config,
            day_result=day_results[-1],
        )
        input_summary_seconds = time.monotonic() - input_summary_started
        day_results[-1] = DailyReplayDayResult(
            requested_date=day_results[-1].requested_date,
            run_dir=day_results[-1].run_dir,
            status=day_results[-1].status,
            error_message=day_results[-1].error_message,
            summary_json_path=day_results[-1].summary_json_path,
            trade_decision_log_path=day_results[-1].trade_decision_log_path,
            input_summary_path=replay_day_input_summary_path,
            state_before_path=day_results[-1].state_before_path,
            state_after_path=day_results[-1].state_after_path,
        )
        replay_day_input_summaries.append(_read_json(Path(replay_day_input_summary_path)))
        stage_durations = _stage_duration_map(result)
        timing_rows.append(
            {
                "date": requested_date,
                "status": str((daily_summary or {}).get("status") or (result.status if result is not None else "failed")),
                "setup_s": setup_seconds,
                "pipeline_s": pipeline_seconds,
                "research_s": _safe_float(stage_durations.get("research")),
                "promote_s": _safe_float(stage_durations.get("promote")),
                "build_portfolio_s": _safe_float(stage_durations.get("build_portfolio")),
                "activate_portfolio_s": _safe_float(stage_durations.get("activate_portfolio")),
                "export_bundle_s": _safe_float(stage_durations.get("export_bundle")),
                "paper_run_s": _safe_float(stage_durations.get("paper_run")),
                "report_s": _safe_float(stage_durations.get("report")),
                "input_summary_s": input_summary_seconds,
                "total_s": time.monotonic() - day_started,
            }
        )
        if (
            (day_error_message or (result is not None and _day_status_failed(result.status)))
            and config.stop_on_error
            and not config.continue_on_error
        ):
            aborted = True
            break

    summary_started = time.monotonic()
    summary, _warnings, _flags = _compute_replay_summary(
        config=config,
        replay_root=replay_root,
        day_results=day_results,
        requested_dates=requested_dates,
        daily_metric_rows=daily_metric_rows,
        trade_log_rows=trade_log_rows,
        strategy_activity_rows=strategy_activity_rows,
        replay_day_input_summaries=replay_day_input_summaries,
        state_transition_consistent=state_transition_consistent,
        holdings_changed=len(set(position_signatures)) > 1,
        aborted=aborted,
    )
    replay_summary_seconds = time.monotonic() - summary_started
    attribution_started = time.monotonic()
    replay_attribution = aggregate_replay_attribution(replay_root=replay_root)
    replay_attribution_seconds = time.monotonic() - attribution_started
    attribution_summary = dict(replay_attribution.get("summary") or {})
    ev_lifecycle_rows, ev_lifecycle_summary = aggregate_replay_ev_lifecycle(replay_root=replay_root)
    ev_realized_rows, ev_bucket_rows, ev_calibration_summary = evaluate_replay_trade_ev_predictions(
        replay_root=replay_root,
        horizon_days=int(config.daily_trading.ev_gate_horizon_days or 5),
        target_type=str(config.daily_trading.ev_gate_target_type or "market_proxy"),
        hybrid_alpha=float(config.daily_trading.ev_gate_hybrid_alpha or 0.8),
    )
    candidate_coverage_rows, candidate_dataset_summary = _aggregate_candidate_ev_dataset(replay_root)
    regression_summary: dict[str, Any] = {}
    regression_artifact_paths: dict[str, str] = {}
    if ev_lifecycle_rows:
        regression_result = run_replay_trade_ev_regression(
            replay_root=replay_root,
            model_output_path=Path("artifacts") / "ev_model" / "ev_regression_model.pkl",
            expected_horizon_days=int(config.daily_trading.ev_gate_horizon_days or 5),
            min_training_samples=max(5, int(config.daily_trading.ev_gate_min_training_samples or 20)),
            use_confidence_weighting=bool(config.daily_trading.ev_gate_use_confidence_weighting),
            confidence_scale=float(config.daily_trading.ev_gate_confidence_scale or 1.0),
            confidence_clip_min=float(config.daily_trading.ev_gate_confidence_clip_min or 0.5),
            confidence_clip_max=float(config.daily_trading.ev_gate_confidence_clip_max or 1.5),
            confidence_min_samples_per_bucket=int(
                config.daily_trading.ev_gate_confidence_min_samples_per_bucket or 20
            ),
            confidence_shrinkage_enabled=bool(
                config.daily_trading.ev_gate_confidence_shrinkage_enabled
            ),
            confidence_component_residual_std_weight=float(
                config.daily_trading.ev_gate_confidence_component_residual_std_weight or 0.0
            ),
            confidence_component_magnitude_weight=float(
                config.daily_trading.ev_gate_confidence_component_magnitude_weight or 0.0
            ),
            confidence_component_model_performance_weight=float(
                config.daily_trading.ev_gate_confidence_component_model_performance_weight or 0.0
            ),
        )
        regression_summary = dict(regression_result.get("summary") or {})
        regression_artifact_paths = {
            key: str(value) for key, value in dict(regression_result.get("artifact_paths") or {}).items()
        }
    if ev_calibration_summary:
        summary["replay_ev_calibration_summary"] = ev_calibration_summary
        summary["ev_gate_target_type"] = str(
            ev_calibration_summary.get("target_type", config.daily_trading.ev_gate_target_type or "market_proxy")
        )
        summary["ev_gate_hybrid_alpha"] = float(
            ev_calibration_summary.get("hybrid_alpha", config.daily_trading.ev_gate_hybrid_alpha or 0.8) or 0.8
        )
        summary["realized_component_available_ratio"] = float(
            ev_calibration_summary.get("realized_component_available_ratio", 0.0) or 0.0
        )
        summary["ev_top_bucket_realized_net_return"] = ev_calibration_summary.get("top_bucket_realized_net_return", 0.0)
        summary["ev_bottom_bucket_realized_net_return"] = ev_calibration_summary.get(
            "bottom_bucket_realized_net_return",
            0.0,
        )
        summary["ev_rank_correlation"] = ev_calibration_summary.get("rank_correlation", 0.0)
        summary["ev_bucket_monotonicity"] = ev_calibration_summary.get("bucket_monotonicity", False)
        summary["ev_top_vs_bottom_bucket_spread"] = ev_calibration_summary.get("top_vs_bottom_bucket_spread", 0.0)
    if attribution_summary:
        summary["best_strategy_by_total_pnl"] = next(
            iter(attribution_summary.get("top_strategies_by_total_pnl", [])),
            {},
        )
        summary["worst_strategy_by_total_pnl"] = next(
            iter(attribution_summary.get("bottom_strategies_by_total_pnl", [])),
            {},
        )
        summary["best_symbol_by_total_pnl"] = next(
            iter(attribution_summary.get("top_symbols_by_total_pnl", [])),
            {},
        )
        summary["worst_symbol_by_total_pnl"] = next(
            iter(attribution_summary.get("bottom_symbols_by_total_pnl", [])),
            {},
        )
        summary["strategy_pnl_concentration"] = attribution_summary.get("strategy_concentration_metrics", {})
        summary["replay_pnl_attribution_summary"] = attribution_summary
    if ev_lifecycle_summary:
        summary["avg_EV_entry"] = float(ev_lifecycle_summary.get("avg_EV_entry", 0.0) or 0.0)
        summary["avg_EV_exit"] = float(ev_lifecycle_summary.get("avg_EV_exit", 0.0) or 0.0)
        summary["exit_efficiency"] = float(ev_lifecycle_summary.get("avg_exit_efficiency", 0.0) or 0.0)
        summary["EV_alignment_rate"] = float(ev_lifecycle_summary.get("EV_alignment_rate", 0.0) or 0.0)
        summary["EV_decay_stats"] = dict(ev_lifecycle_summary.get("EV_decay_stats") or {})
        summary["pct_trades_EV_entry_positive"] = float(
            ev_lifecycle_summary.get("pct_trades_EV_entry_positive", 0.0) or 0.0
        )
        summary["pct_exits_EV_exit_negative"] = float(
            ev_lifecycle_summary.get("pct_exits_EV_exit_negative", 0.0) or 0.0
        )
        summary["EV_entry_realized_return_correlation"] = float(
            ev_lifecycle_summary.get("EV_entry_realized_return_correlation", 0.0) or 0.0
        )
        summary["MFE_vs_EV_entry_correlation"] = float(
            ev_lifecycle_summary.get("MFE_vs_EV_entry_correlation", 0.0) or 0.0
        )
        summary["replay_ev_lifecycle_summary"] = ev_lifecycle_summary
    if candidate_dataset_summary:
        summary["replay_candidate_ev_dataset_summary"] = candidate_dataset_summary
    if regression_summary:
        summary["regression_ev_correlation"] = float(regression_summary.get("correlation", 0.0) or 0.0)
        summary["regression_ev_rank_correlation"] = float(regression_summary.get("rank_correlation", 0.0) or 0.0)
        summary["regression_ev_bucket_spread"] = float(regression_summary.get("bucket_spread", 0.0) or 0.0)
        if max(
            int(regression_summary.get("confidence_row_count", 0) or 0),
            int(regression_summary.get("prediction_count", 0) or 0),
        ) > 0:
            summary["avg_ev_confidence"] = float(
                regression_summary.get("avg_ev_confidence", summary.get("avg_ev_confidence", 1.0))
            )
            summary["avg_ev_confidence_multiplier"] = float(
                regression_summary.get(
                    "avg_ev_confidence_multiplier",
                    summary.get("avg_ev_confidence_multiplier", 1.0),
                )
            )
            summary["confidence_absolute_error_correlation"] = float(
                regression_summary.get("confidence_absolute_error_correlation", 0.0) or 0.0
            )
            summary["confidence_realized_return_correlation"] = float(
                regression_summary.get("confidence_realized_return_correlation", 0.0) or 0.0
            )
            summary["confidence_top_vs_bottom_bucket_spread"] = float(
                regression_summary.get("top_vs_bottom_realized_return_spread", 0.0) or 0.0
            )
        summary["replay_ev_regression_summary"] = regression_summary
    status = "succeeded"
    failed_day_count = int(summary.get("failed_day_count", 0) or 0)
    if failed_day_count and int(summary.get("successful_day_count", 0) or 0):
        status = "partial_failed"
    elif failed_day_count or aborted:
        status = "failed"
    elif summary.get("warnings"):
        status = "warning"
    summary["status"] = status

    replay_dashboard_seconds = 0.0
    if config.daily_trading.refresh_dashboard_static_data:
        dashboard_started = time.monotonic()
        dashboard_paths = build_dashboard_static_data(
            artifacts_root=replay_root,
            output_dir=replay_root / "dashboard",
        )
        replay_dashboard_seconds = time.monotonic() - dashboard_started
        core_diagnostics_complete = all(
            Path(row.summary_json_path or "").exists() and Path(row.trade_decision_log_path or "").exists()
            for row in day_results
        )
        diagnostics_complete = core_diagnostics_complete and (replay_root / "dashboard").exists()
        summary.setdefault("readiness_flags", {})["diagnostics_complete"] = diagnostics_complete
        warnings = [warning for warning in list(summary.get("warnings") or []) if warning != "missing dashboard/report artifacts"]
        if not diagnostics_complete:
            warnings.append("missing dashboard/report artifacts")
        summary["warnings"] = warnings
    else:
        dashboard_paths = {}

    artifact_paths = _write_replay_summary_artifacts(
        replay_root=replay_root,
        summary=summary,
        daily_metric_rows=daily_metric_rows,
        trade_log_rows=trade_log_rows,
        strategy_activity_rows=strategy_activity_rows,
    )
    artifact_paths.update(
        {
            key: str(value)
            for key, value in write_replay_pnl_attribution_artifacts(
                replay_root=replay_root,
                replay_payload=replay_attribution,
            ).items()
        }
    )
    artifact_paths.update(
        {
            key: str(value)
            for key, value in write_replay_ev_lifecycle_artifacts(
                replay_root=replay_root,
                lifecycle_rows=ev_lifecycle_rows,
                summary=ev_lifecycle_summary,
            ).items()
        }
    )
    ev_bucket_path = _write_csv(
        replay_root / "replay_ev_bucket_summary.csv",
        ev_bucket_rows,
        [
            "bucket",
            "trade_count",
            "avg_predicted_gross_return",
            "avg_predicted_net_return",
            "avg_realized_gross_return",
            "avg_realized_net_return",
            "realized_hit_rate",
            "avg_execution_cost",
            "avg_weight_multiplier",
        ],
    )
    ev_calibration_summary_path = replay_root / "replay_ev_calibration_summary.json"
    ev_calibration_summary_path.write_text(
        json.dumps(ev_calibration_summary, indent=2, default=str),
        encoding="utf-8",
    )
    candidate_coverage_path = _write_csv(
        replay_root / "replay_candidate_ev_coverage.csv",
        candidate_coverage_rows,
        [
            "date",
            "candidate_row_count",
            "executed_row_count",
            "skipped_row_count",
            "training_source_used",
            "requested_training_source",
            "training_sample_count",
            "labeled_row_count",
            "positive_label_rate",
            "fallback_reason",
        ],
    )
    candidate_summary_path = replay_root / "replay_candidate_ev_dataset_summary.json"
    candidate_summary_path.write_text(
        json.dumps(candidate_dataset_summary, indent=2, default=str),
        encoding="utf-8",
    )
    artifact_paths["replay_ev_bucket_summary_csv_path"] = str(ev_bucket_path)
    artifact_paths["replay_ev_calibration_summary_json_path"] = str(ev_calibration_summary_path)
    artifact_paths["replay_candidate_ev_coverage_csv_path"] = str(candidate_coverage_path)
    artifact_paths["replay_candidate_ev_dataset_summary_json_path"] = str(candidate_summary_path)
    artifact_paths.update(regression_artifact_paths)
    artifact_paths.update({f"dashboard_{key}": str(value) for key, value in dashboard_paths.items()})
    if config.replay.profile_timings:
        total_replay_seconds = time.monotonic() - replay_started
        timing_summary = {
            "total_replay_seconds": total_replay_seconds,
            "total_setup_seconds": float(sum(_safe_float(row.get("setup_s")) for row in timing_rows)),
            "total_daily_pipeline_seconds": float(sum(_safe_float(row.get("pipeline_s")) for row in timing_rows)),
            "total_input_summary_seconds": float(sum(_safe_float(row.get("input_summary_s")) for row in timing_rows)),
            "total_replay_summary_seconds": replay_summary_seconds,
            "total_replay_aggregation_seconds": replay_attribution_seconds,
            "total_dashboard_refresh_seconds": replay_dashboard_seconds,
            "day_count": len(timing_rows),
            "slowest_days": sorted(
                (
                    {
                        "date": str(row.get("date")),
                        "total_s": _safe_float(row.get("total_s")),
                        "pipeline_s": _safe_float(row.get("pipeline_s")),
                        "report_s": _safe_float(row.get("report_s")),
                    }
                    for row in timing_rows
                ),
                key=lambda row: float(row["total_s"]),
                reverse=True,
            )[:10],
        }
        artifact_paths.update(
            _write_replay_timing_artifacts(
                replay_root=replay_root,
                day_rows=timing_rows,
                summary=timing_summary,
            )
        )
    return DailyReplayResult(
        output_dir=str(replay_root),
        state_path=str(state_path),
        requested_dates=requested_dates,
        processed_dates=[row.requested_date for row in day_results],
        status=status,
        day_results=day_results,
        summary_json_path=artifact_paths["replay_summary_json_path"],
        summary_md_path=artifact_paths["replay_summary_md_path"],
        artifact_paths=artifact_paths,
        summary=summary,
    )
