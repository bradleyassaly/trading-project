from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.config.workflow_models import DailyReplayWorkflowConfig, DailyTradingWorkflowConfig
from trading_platform.orchestration.daily_trading import DailyTradingResult, run_daily_trading_pipeline
from trading_platform.portfolio.strategy_execution_handoff import resolve_strategy_execution_handoff


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


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


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


def _build_day_config(
    base_config: DailyTradingWorkflowConfig,
    *,
    replay_root: Path,
    requested_date: str,
    state_path: Path,
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
    final_equity = (
        float(metrics_frame["current_equity"].iloc[-1])
        if "current_equity" in metrics_frame and not metrics_frame.empty
        else 0.0
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
    if "current_equity" in metrics_frame and not metrics_frame.empty:
        equity_series = metrics_frame["current_equity"].astype(float)
        rolling_max = equity_series.cummax().replace(0.0, pd.NA)
        raw_drawdowns = (equity_series / rolling_max) - 1.0
        drawdowns = raw_drawdowns.where(pd.notna(raw_drawdowns), 0.0)
        max_drawdown = float(drawdowns.min())
    else:
        max_drawdown = 0.0

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
        diagnostics_complete = diagnostics_complete and all(
            (replay_root / row.requested_date / "dashboard").exists() for row in day_results
        )

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
        "cumulative_realized_pnl": cumulative_realized_pnl,
        "cumulative_unrealized_pnl": cumulative_unrealized_pnl,
        "final_equity": final_equity,
        "max_drawdown": max_drawdown,
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
                f"- final_equity: `{summary.get('final_equity', 0.0)}`",
                f"- max_drawdown: `{summary.get('max_drawdown', 0.0)}`",
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
            "fill_count",
            "turnover_estimate",
            "position_count",
            "current_equity",
            "cumulative_realized_pnl",
            "unrealized_pnl",
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
    previous_state_after_payload: str | None = None
    state_transition_consistent = True
    position_signatures: list[str] = []
    aborted = False

    for requested_date in requested_dates:
        day_dir = replay_root / requested_date
        day_config = _build_day_config(
            config.daily_trading,
            replay_root=replay_root,
            requested_date=requested_date,
            state_path=state_path,
        )
        state_before_path = _safe_copy(state_path, day_dir / "paper_state_before.json")
        if previous_state_after_payload is not None and state_before_path is not None:
            current_before_payload = Path(state_before_path).read_text(encoding="utf-8")
            state_transition_consistent = state_transition_consistent and (
                current_before_payload == previous_state_after_payload
            )

        day_error_message: str | None = None
        result: DailyTradingResult | None = None
        try:
            result = run_daily_trading_pipeline(
                day_config,
                replay_as_of_date=requested_date,
                replay_settings=config.replay.__dict__,
            )
        except Exception as exc:
            day_error_message = f"{type(exc).__name__}: {exc}"
            if config.stop_on_error and not config.continue_on_error:
                aborted = True

        state_after_path = _safe_copy(state_path, day_dir / "paper_state_after.json")
        if state_after_path:
            previous_state_after_payload = Path(state_after_path).read_text(encoding="utf-8")
            state_payload = _read_json(Path(state_after_path))
            position_signatures.append(json.dumps(state_payload.get("positions", {}), sort_keys=True))

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
                "fill_count": int(paper_summary.get("fill_count", 0) or 0),
                "turnover_estimate": float(paper_summary.get("turnover_estimate", 0.0) or 0.0),
                "position_count": int(paper_summary.get("realized_holdings_count", 0) or 0),
                "current_equity": float(paper_summary.get("current_equity", 0.0) or 0.0),
                "cumulative_realized_pnl": float(paper_summary.get("cumulative_realized_pnl", 0.0) or 0.0),
                "unrealized_pnl": float(paper_summary.get("unrealized_pnl", 0.0) or 0.0),
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
        replay_day_input_summary_path = _write_replay_day_input_summary(
            replay_root=replay_root,
            requested_date=requested_date,
            day_config=day_config,
            day_result=day_results[-1],
        )
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
        if (
            (day_error_message or (result is not None and _day_status_failed(result.status)))
            and config.stop_on_error
            and not config.continue_on_error
        ):
            aborted = True
            break

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
    status = "succeeded"
    failed_day_count = int(summary.get("failed_day_count", 0) or 0)
    if failed_day_count and int(summary.get("successful_day_count", 0) or 0):
        status = "partial_failed"
    elif failed_day_count or aborted:
        status = "failed"
    elif summary.get("warnings"):
        status = "warning"
    summary["status"] = status

    artifact_paths = _write_replay_summary_artifacts(
        replay_root=replay_root,
        summary=summary,
        daily_metric_rows=daily_metric_rows,
        trade_log_rows=trade_log_rows,
        strategy_activity_rows=strategy_activity_rows,
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
