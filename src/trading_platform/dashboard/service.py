from __future__ import annotations

import json
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.dashboard.chart_service import build_chart_payload, build_signals_payload, build_trades_payload
from trading_platform.dashboard.portfolio_service import (
    build_discovery_payload,
    build_execution_diagnostics_payload,
    build_portfolio_overview_payload,
    build_strategy_detail_payload,
    build_trade_blotter_payload,
    build_trade_detail_payload,
)
from trading_platform.governance.persistence import load_strategy_registry
from trading_platform.settings import FEATURES_DIR


def _now_utc() -> str:
    return datetime.now(UTC).isoformat()


def _safe_read_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _safe_read_csv(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    file_path = Path(path)
    if not file_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(file_path)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _frame_to_records(frame: pd.DataFrame, *, head: int | None = None, tail: int | None = None) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    normalized = frame.astype(object).where(pd.notna(frame), None)
    if head is not None:
        normalized = normalized.head(head)
    if tail is not None:
        normalized = normalized.tail(tail)
    return normalized.to_dict(orient="records")


def _newest_path(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


def _candidate_files(root: Path, names: list[str]) -> list[Path]:
    found: list[Path] = []
    for name in names:
        direct = root / name
        if direct.exists():
            found.append(direct)
        found.extend(root.rglob(name))
    seen: set[str] = set()
    unique: list[Path] = []
    for path in found:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _latest_matching_file(root: Path, names: list[str]) -> Path | None:
    return _newest_path(_candidate_files(root, names))


def _status_counts(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _row_mapping(row: object) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    if hasattr(row, "_asdict"):
        try:
            value = row._asdict()
        except TypeError:
            value = None
        if isinstance(value, Mapping):
            return dict(value)
    if hasattr(row, "to_dict"):
        try:
            value = row.to_dict()
        except TypeError:
            value = None
        if isinstance(value, Mapping):
            return dict(value)
    if hasattr(row, "__dict__"):
        try:
            value = vars(row)
        except TypeError:
            value = None
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _json_scalar(value: object) -> Any:
    if value is None:
        return None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat() if not pd.isna(value) else None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if pd.isna(value):
        return None
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _json_scalar(item())
        except (TypeError, ValueError):
            pass
    return str(value)


def _json_safe(value: object) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return _json_scalar(value)


def _row_contract(row: object, keys: list[str]) -> dict[str, Any]:
    mapping = _row_mapping(row)
    return {key: _json_safe(mapping.get(key)) for key in keys}


def _row_list_contract(rows: object, keys: list[str]) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    return [_row_contract(row, keys) for row in rows]


def _normalize_context_rows(rows: object) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    return [dict(_json_safe(_row_mapping(row))) for row in rows]


def _normalize_chart_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["bars"] = _row_list_contract(
        payload.get("bars"),
        ["ts", "open", "high", "low", "close", "volume"],
    )
    indicators = normalized.get("indicators")
    if not isinstance(indicators, Mapping):
        normalized["indicators"] = {}
    else:
        normalized["indicators"] = {
            str(name): _row_list_contract(payload.get("indicators", {}).get(name), ["ts", "value"])
            for name, rows in indicators.items()
        }
    normalized["signals"] = _row_list_contract(payload.get("signals"), ["ts", "type", "price", "label", "score"])
    normalized["orders"] = _row_list_contract(
        payload.get("orders"), ["ts", "symbol", "side", "qty", "price", "order_id", "status", "reason", "source_type"]
    )
    normalized["fills"] = _row_list_contract(
        payload.get("fills"), ["ts", "symbol", "side", "qty", "price", "order_id", "status", "reason", "source_type"]
    )
    normalized["trades"] = _row_list_contract(
        payload.get("trades"),
        [
            "trade_id",
            "symbol",
            "side",
            "qty",
            "entry_ts",
            "entry_price",
            "exit_ts",
            "exit_price",
            "realized_pnl",
            "status",
            "strategy_id",
            "source",
            "run_id",
            "mode",
            "trade_source",
            "trade_source_mode",
            "hold_duration_hours",
        ],
    )
    normalized["provenance"] = _row_list_contract(
        payload.get("provenance"),
        [
            "ts",
            "symbol",
            "trade_id",
            "strategy_id",
            "run_id",
            "source",
            "mode",
            "signal_type",
            "signal_value",
            "ranking_score",
            "universe_rank",
            "selection_included",
            "selection_status",
            "exclusion_reason",
            "target_weight",
            "sizing_rationale",
            "constraint_hits",
            "order_intent_summary",
            "label",
            "regime_context",
            "artifact_path",
            "metadata_path",
        ],
    )
    normalized["position"] = dict(_json_safe(normalized.get("position") or {}))
    normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
    return normalized


def _normalize_portfolio_overview_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["summary"] = dict(_json_safe(normalized.get("summary") or {}))
    normalized["equity_curve"] = _row_list_contract(payload.get("equity_curve"), ["ts", "equity"])
    normalized["drawdown_curve"] = _row_list_contract(payload.get("drawdown_curve"), ["ts", "drawdown"])
    normalized["positions"] = _row_list_contract(
        payload.get("positions"), ["symbol", "qty", "avg_price", "market_value", "side"]
    )
    normalized["exposure"] = _row_list_contract(
        payload.get("exposure"), ["symbol", "side", "market_value", "weight_proxy"]
    )
    normalized["recent_activity"] = _row_list_contract(
        payload.get("recent_activity"), ["kind", "ts", "symbol", "side", "qty", "price", "status"]
    )
    normalized["pnl_by_symbol"] = _row_list_contract(
        payload.get("pnl_by_symbol"),
        ["symbol", "trade_count", "closed_trade_count", "cumulative_realized_pnl", "win_rate"],
    )
    normalized["recent_realized_pnl"] = _row_list_contract(
        payload.get("recent_realized_pnl"), ["period", "realized_pnl"]
    )
    normalized["best_trades"] = _row_list_contract(
        payload.get("best_trades"), ["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"]
    )
    normalized["worst_trades"] = _row_list_contract(
        payload.get("worst_trades"),
        ["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"],
    )
    normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
    return normalized


def _normalize_strategy_detail_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["summary"] = dict(_json_safe(normalized.get("summary") or {}))
    normalized["trades"] = _row_list_contract(
        payload.get("trades"),
        [
            "trade_id",
            "symbol",
            "side",
            "qty",
            "entry_ts",
            "entry_price",
            "exit_ts",
            "exit_price",
            "realized_pnl",
            "status",
            "strategy_id",
            "source",
            "run_id",
            "mode",
        ],
    )
    normalized["pnl_by_symbol"] = _row_list_contract(
        payload.get("pnl_by_symbol"),
        ["symbol", "trade_count", "closed_trade_count", "cumulative_realized_pnl", "win_rate"],
    )
    normalized["recent_realized_pnl"] = _row_list_contract(
        payload.get("recent_realized_pnl"), ["period", "realized_pnl"]
    )
    normalized["best_trades"] = _row_list_contract(
        payload.get("best_trades"), ["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"]
    )
    normalized["worst_trades"] = _row_list_contract(
        payload.get("worst_trades"),
        ["trade_id", "symbol", "side", "realized_pnl", "entry_ts", "exit_ts", "strategy_id"],
    )
    normalized["comparisons"] = _row_list_contract(
        payload.get("comparisons"),
        [
            "source",
            "run_id",
            "mode",
            "trade_count",
            "closed_trade_count",
            "open_trade_count",
            "cumulative_realized_pnl",
            "win_rate",
        ],
    )
    normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
    normalized["meta"]["sources"] = _normalize_context_rows(normalized["meta"].get("sources"))
    return normalized


def _normalize_trade_detail_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["trade"] = _row_contract(
        payload.get("trade"),
        [
            "trade_id",
            "symbol",
            "side",
            "qty",
            "entry_ts",
            "entry_price",
            "exit_ts",
            "exit_price",
            "realized_pnl",
            "status",
            "strategy_id",
            "source",
            "run_id",
            "mode",
            "trade_source",
            "trade_source_mode",
            "hold_duration_hours",
        ],
    )
    normalized["chart"] = _normalize_chart_payload(dict(payload.get("chart") or {}))
    normalized["signals"] = _row_list_contract(payload.get("signals"), ["ts", "type", "price", "label", "score"])
    normalized["fills"] = _row_list_contract(
        payload.get("fills"), ["ts", "symbol", "side", "qty", "price", "order_id", "status", "reason", "source_type"]
    )
    normalized["orders"] = _row_list_contract(
        payload.get("orders"), ["ts", "symbol", "side", "qty", "price", "order_id", "status", "reason", "source_type"]
    )
    normalized["trade_summary"] = dict(_json_safe(normalized.get("trade_summary") or {}))
    normalized["portfolio_context"] = dict(_json_safe(normalized.get("portfolio_context") or {}))
    normalized["execution_review"] = dict(_json_safe(normalized.get("execution_review") or {}))
    normalized["outcome_review"] = dict(_json_safe(normalized.get("outcome_review") or {}))
    normalized["related_metadata"] = dict(_json_safe(normalized.get("related_metadata") or {}))
    normalized["provenance"] = dict(_json_safe(normalized.get("provenance") or {}))
    normalized["provenance"]["latest"] = _row_contract(
        (payload.get("provenance") or {}).get("latest"),
        [
            "ts",
            "symbol",
            "trade_id",
            "strategy_id",
            "run_id",
            "source",
            "mode",
            "signal_type",
            "signal_value",
            "ranking_score",
            "universe_rank",
            "selection_included",
            "selection_status",
            "exclusion_reason",
            "target_weight",
            "sizing_rationale",
            "constraint_hits",
            "order_intent_summary",
            "label",
            "regime_context",
            "artifact_path",
            "metadata_path",
        ],
    )
    normalized["provenance"]["rows"] = _row_list_contract(
        (payload.get("provenance") or {}).get("rows"),
        [
            "ts",
            "symbol",
            "trade_id",
            "strategy_id",
            "run_id",
            "source",
            "mode",
            "signal_type",
            "signal_value",
            "ranking_score",
            "universe_rank",
            "selection_included",
            "selection_status",
            "exclusion_reason",
            "target_weight",
            "sizing_rationale",
            "constraint_hits",
            "order_intent_summary",
            "label",
            "regime_context",
            "artifact_path",
            "metadata_path",
        ],
    )
    normalized["lifecycle"] = _row_list_contract(payload.get("lifecycle"), ["ts", "kind", "label", "detail", "status"])
    comparison = dict(_json_safe(normalized.get("comparison") or {}))
    comparison["related_trades"] = _row_list_contract(
        (payload.get("comparison") or {}).get("related_trades"),
        ["trade_id", "symbol", "side", "qty", "entry_ts", "exit_ts", "realized_pnl", "status", "strategy_id"],
    )
    comparison["available_chart_sources"] = _normalize_context_rows(
        (payload.get("comparison") or {}).get("available_chart_sources")
    )
    comparison["available_provenance_sources"] = _normalize_context_rows(
        (payload.get("comparison") or {}).get("available_provenance_sources")
    )
    normalized["comparison"] = comparison
    normalized["explain"] = dict(_json_safe(normalized.get("explain") or {}))
    normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
    return normalized


def _normalize_trade_blotter_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["summary"] = dict(_json_safe(normalized.get("summary") or {}))
    normalized["trades"] = _row_list_contract(
        payload.get("trades"),
        [
            "trade_id",
            "timestamp",
            "symbol",
            "side",
            "qty",
            "target_weight",
            "strategy_id",
            "signal_score",
            "ranking_score",
            "universe_rank",
            "expected_edge",
            "order_status",
            "status",
            "entry_ts",
            "exit_ts",
            "entry_price",
            "exit_price",
            "realized_pnl",
            "unrealized_pnl",
            "portfolio_qty",
            "portfolio_market_value",
            "source",
            "run_id",
            "mode",
        ],
    )
    normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
    return normalized


def _normalize_execution_diagnostics_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["summary"] = dict(_json_safe(normalized.get("summary") or {}))
    normalized["rows"] = _row_list_contract(
        payload.get("rows"),
        ["symbol", "signal_ts", "fill_ts", "latency_seconds", "signal_price", "fill_price", "slippage_bps"],
    )
    normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
    return normalized


def _normalize_discovery_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["summary"] = dict(_json_safe(normalized.get("summary") or {}))
    normalized["recent_symbols"] = _row_list_contract(
        payload.get("recent_symbols"),
        [
            "symbol",
            "trade_count",
            "latest_trade_id",
            "latest_entry_ts",
            "latest_strategy_id",
            "latest_source",
            "latest_run_id",
            "status",
        ],
    )
    normalized["recent_trades"] = _row_list_contract(
        payload.get("recent_trades"),
        [
            "trade_id",
            "symbol",
            "strategy_id",
            "side",
            "qty",
            "entry_ts",
            "exit_ts",
            "entry_price",
            "exit_price",
            "realized_pnl",
            "status",
            "source",
            "run_id",
            "mode",
        ],
    )
    normalized["recent_strategies"] = _row_list_contract(
        payload.get("recent_strategies"),
        [
            "strategy_id",
            "trade_count",
            "closed_trade_count",
            "latest_symbol",
            "latest_entry_ts",
            "latest_source",
            "latest_run_id",
        ],
    )
    normalized["recent_run_contexts"] = _row_list_contract(
        payload.get("recent_run_contexts"),
        ["source", "run_id", "mode", "trade_count", "strategy_count", "symbol_count", "latest_entry_ts"],
    )
    return normalized


def _normalize_portfolio_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["summary"] = dict(_json_safe(normalized.get("summary") or {}))
    normalized["combined_positions"] = _normalize_context_rows(normalized.get("combined_positions"))
    normalized["sleeve_weights"] = _normalize_context_rows(normalized.get("sleeve_weights"))
    normalized["top_positions"] = _normalize_context_rows(normalized.get("top_positions"))
    normalized["overlap"] = _normalize_context_rows(normalized.get("overlap"))
    normalized["clipped_symbols"] = _normalize_context_rows(normalized.get("clipped_symbols"))
    normalized["adaptive_allocation"] = dict(_json_safe(normalized.get("adaptive_allocation") or {}))
    normalized["adaptive_allocation"]["top_changes"] = _normalize_context_rows(
        normalized["adaptive_allocation"].get("top_changes")
    )
    normalized["adaptive_allocation"]["strategies"] = _normalize_context_rows(
        normalized["adaptive_allocation"].get("strategies")
    )
    normalized["market_regime"] = dict(_json_safe(normalized.get("market_regime") or {}))
    normalized["market_regime"]["summary"] = dict(_json_safe(normalized["market_regime"].get("summary") or {}))
    normalized["market_regime"]["history"] = _normalize_context_rows(normalized["market_regime"].get("history"))
    normalized["strategy_quality"] = dict(_json_safe(normalized.get("strategy_quality") or {}))
    normalized["strategy_quality"]["summary"] = dict(_json_safe(normalized["strategy_quality"].get("summary") or {}))
    normalized["strategy_quality"]["strategy_comparison"] = _normalize_context_rows(
        normalized["strategy_quality"].get("strategy_comparison")
    )
    normalized["strategy_quality"]["rolling_sharpe"] = _normalize_context_rows(
        normalized["strategy_quality"].get("rolling_sharpe")
    )
    normalized["strategy_quality"]["rolling_ic"] = _normalize_context_rows(
        normalized["strategy_quality"].get("rolling_ic")
    )
    return normalized


def _normalize_execution_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["summary"] = dict(_json_safe(normalized.get("summary") or {}))
    normalized["requested_orders"] = _normalize_context_rows(normalized.get("requested_orders"))
    normalized["executable_orders"] = _normalize_context_rows(normalized.get("executable_orders"))
    normalized["rejected_orders"] = _normalize_context_rows(normalized.get("rejected_orders"))
    normalized["liquidity_diagnostics"] = _normalize_context_rows(normalized.get("liquidity_diagnostics"))
    normalized["turnover_summary"] = _normalize_context_rows(normalized.get("turnover_summary"))
    return normalized


def _normalize_live_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(_json_safe(payload))
    normalized["dry_run_summary"] = dict(_json_safe(normalized.get("dry_run_summary") or {}))
    normalized["submission_summary"] = dict(_json_safe(normalized.get("submission_summary") or {}))
    normalized["risk_checks"] = _normalize_context_rows(normalized.get("risk_checks"))
    normalized["blocked_checks"] = _normalize_context_rows(normalized.get("blocked_checks"))
    normalized["duplicate_events"] = _normalize_context_rows(normalized.get("duplicate_events"))
    normalized["broker_health"] = dict(_json_safe(normalized.get("broker_health") or {}))
    return normalized


class DashboardDataService:
    def __init__(self, artifacts_root: str | Path, *, feature_dir: str | Path | None = None) -> None:
        self.artifacts_root = Path(artifacts_root)
        self.feature_dir = Path(feature_dir) if feature_dir is not None else FEATURES_DIR

    def find_latest_run_dir(self) -> Path | None:
        summary = _latest_matching_file(self.artifacts_root, ["run_summary.json"])
        return summary.parent if summary is not None else None

    def recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for summary_path in sorted(self.artifacts_root.rglob("run_summary.json")):
            payload = _safe_read_json(summary_path)
            run_dir = summary_path.parent
            run_health = _safe_read_json(run_dir / "monitoring" / "run_health.json")
            stage_df = _safe_read_csv(run_dir / "stage_status.csv")
            rows.append(
                {
                    "run_name": payload.get("run_name", run_dir.name),
                    "run_dir": str(run_dir),
                    "started_at": payload.get("started_at"),
                    "ended_at": payload.get("ended_at"),
                    "status": payload.get("status", "unknown"),
                    "schedule_type": payload.get("schedule_type"),
                    "health_status": run_health.get("status"),
                    "critical_alert_count": int(run_health.get("alert_counts", {}).get("critical", 0) or 0),
                    "warning_alert_count": int(run_health.get("alert_counts", {}).get("warning", 0) or 0),
                    "stage_count": int(len(stage_df.index)),
                    "failed_stage_count": (
                        int((stage_df.get("status", pd.Series(dtype=object)) == "failed").sum())
                        if not stage_df.empty
                        else 0
                    ),
                    "artifact_dir": str(run_dir),
                }
            )
        rows.sort(key=lambda row: str(row.get("started_at") or row["run_dir"]), reverse=True)
        return rows[:limit]

    def recent_orchestration_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for summary_path in sorted(self.artifacts_root.rglob("orchestration_run.json")):
            payload = _safe_read_json(summary_path)
            run_dir = summary_path.parent
            stage_records = payload.get("stage_records", [])
            outputs = payload.get("outputs", {})
            system_eval = _safe_read_json(run_dir / "system_evaluation.json")
            system_row = system_eval.get("row", {})
            rows.append(
                {
                    "run_id": payload.get("run_id"),
                    "run_name": payload.get("run_name", run_dir.name),
                    "run_dir": str(run_dir),
                    "started_at": payload.get("started_at"),
                    "ended_at": payload.get("ended_at"),
                    "status": payload.get("status", "unknown"),
                    "schedule_frequency": payload.get("schedule_frequency"),
                    "experiment_name": payload.get("experiment_name"),
                    "variant_name": payload.get("variant_name"),
                    "experiment_run_id": payload.get("experiment_run_id"),
                    "feature_flags": payload.get("feature_flags", {}),
                    "failed_stage_count": sum(1 for row in stage_records if row.get("status") == "failed"),
                    "selected_strategy_count": outputs.get("selected_strategy_count", 0),
                    "total_return": system_row.get("total_return"),
                    "sharpe": system_row.get("sharpe"),
                    "warning_strategy_count": outputs.get("warning_strategy_count", 0),
                    "kill_switch_recommendation_count": outputs.get("kill_switch_recommendation_count", 0),
                }
            )
        rows.sort(key=lambda row: str(row.get("started_at") or row["run_dir"]), reverse=True)
        return rows[:limit]

    def latest_run_payload(self) -> dict[str, Any]:
        run_dir = self.find_latest_run_dir()
        if run_dir is None:
            return {"run_dir": None, "summary": {}, "health": {}, "stages": []}
        return {
            "run_dir": str(run_dir),
            "summary": _safe_read_json(run_dir / "run_summary.json"),
            "health": _safe_read_json(run_dir / "monitoring" / "run_health.json"),
            "stages": _safe_read_csv(run_dir / "stage_status.csv").to_dict(orient="records"),
        }

    def registry_payload(self) -> dict[str, Any]:
        registry_path = _latest_matching_file(
            self.artifacts_root,
            ["strategy_registry.json", "strategy_registry.yaml", "strategy_registry.yml"],
        )
        if registry_path is None:
            return {
                "registry_path": None,
                "updated_at": None,
                "strategies": [],
                "status_counts": {},
                "family_counts": {},
                "champion_challenger": [],
            }
        registry = load_strategy_registry(registry_path)
        comparison_path = _latest_matching_file(self.artifacts_root, ["family_comparison.csv"])
        comparison_rows = _safe_read_csv(comparison_path).to_dict(orient="records") if comparison_path else []
        strategies: list[dict[str, Any]] = []
        for entry in registry.entries:
            promotion = _safe_read_json(entry.latest_promotion_decision_path)
            degradation = _safe_read_json(entry.latest_degradation_report_path)
            strategies.append(
                {
                    "strategy_id": entry.strategy_id,
                    "strategy_name": entry.strategy_name,
                    "family": entry.family,
                    "version": entry.version,
                    "preset_name": entry.preset_name,
                    "status": entry.status,
                    "current_deployment_stage": entry.current_deployment_stage,
                    "universe": entry.universe,
                    "signal_type": entry.signal_type,
                    "rebalance_frequency": entry.rebalance_frequency,
                    "benchmark": entry.benchmark,
                    "risk_profile": entry.risk_profile,
                    "owner": entry.owner,
                    "tags": entry.tags,
                    "promotion_passed": promotion.get("passed") if promotion else None,
                    "promotion_summary": promotion.get("summary_metrics", {}) if promotion else {},
                    "degradation_status": degradation.get("status") if degradation else None,
                    "degradation_summary": (
                        degradation.get("summary_metrics", degradation.get("metrics", {})) if degradation else {}
                    ),
                    "paper_artifact_path": entry.paper_artifact_path,
                    "live_artifact_path": entry.live_artifact_path,
                }
            )
        return {
            "registry_path": str(registry_path),
            "updated_at": registry.updated_at,
            "strategies": strategies,
            "status_counts": _status_counts([row["status"] for row in strategies]),
            "family_counts": _status_counts([str(row["family"]) for row in strategies]),
            "champion_challenger": comparison_rows,
        }

    def latest_portfolio_payload(self) -> dict[str, Any]:
        run_dir = self.find_latest_run_dir()
        candidates: list[Path] = []
        if run_dir is not None:
            candidates.append(run_dir / "portfolio_allocation" / "allocation_summary.json")
        latest_summary_path = _newest_path(
            candidates + _candidate_files(self.artifacts_root, ["allocation_summary.json"])
        )
        if latest_summary_path is None:
            return {
                "summary": {},
                "combined_positions": [],
                "sleeve_weights": [],
                "top_positions": [],
                "overlap": [],
                "clipped_symbols": [],
                "artifact_dir": None,
            }
        allocation_dir = latest_summary_path.parent
        summary_payload = _safe_read_json(latest_summary_path)
        summary = summary_payload.get("summary", summary_payload)
        combined_df = _safe_read_csv(allocation_dir / "combined_target_weights.csv")
        sleeve_df = _safe_read_csv(allocation_dir / "sleeve_target_weights.csv")
        overlap_df = _safe_read_csv(allocation_dir / "symbol_overlap_report.csv")
        sleeve_weights = []
        if not sleeve_df.empty and "sleeve_name" in sleeve_df.columns:
            weight_col = "scaled_target_weight" if "scaled_target_weight" in sleeve_df.columns else "target_weight"
            aggregated = (
                sleeve_df.groupby("sleeve_name", as_index=False)[weight_col]
                .sum()
                .sort_values(weight_col, ascending=False)
            )
            sleeve_weights = aggregated.to_dict(orient="records")
        top_positions = []
        if not combined_df.empty and "target_weight" in combined_df.columns:
            sorted_df = combined_df.assign(abs_weight=combined_df["target_weight"].abs()).sort_values(
                "abs_weight", ascending=False
            )
            top_positions = sorted_df.drop(columns=["abs_weight"]).head(10).to_dict(orient="records")
        return {
            "summary": summary,
            "combined_positions": combined_df.to_dict(orient="records"),
            "sleeve_weights": sleeve_weights,
            "top_positions": top_positions,
            "overlap": overlap_df.to_dict(orient="records"),
            "clipped_symbols": summary.get("symbols_removed_or_clipped", []),
            "artifact_dir": str(allocation_dir),
        }

    def latest_execution_payload(self) -> dict[str, Any]:
        run_dir = self.find_latest_run_dir()
        preferred_dirs: list[Path] = []
        if run_dir is not None:
            preferred_dirs.extend([run_dir / "live_dry_run", run_dir / "paper_trading"])
        summary_path = _newest_path(
            [path / "execution_summary.json" for path in preferred_dirs if path.exists()]
            + _candidate_files(self.artifacts_root, ["execution_summary.json"])
        )
        if summary_path is None:
            return {
                "summary": {},
                "requested_orders": [],
                "executable_orders": [],
                "rejected_orders": [],
                "liquidity_diagnostics": [],
                "turnover_summary": [],
                "artifact_dir": None,
            }
        execution_dir = summary_path.parent
        return {
            "summary": _safe_read_json(summary_path),
            "requested_orders": _safe_read_csv(execution_dir / "requested_orders.csv").to_dict(orient="records"),
            "executable_orders": _safe_read_csv(execution_dir / "executable_orders.csv").to_dict(orient="records"),
            "rejected_orders": _safe_read_csv(execution_dir / "rejected_orders.csv").to_dict(orient="records"),
            "liquidity_diagnostics": _safe_read_csv(execution_dir / "liquidity_constraints_report.csv").to_dict(
                orient="records"
            ),
            "turnover_summary": _safe_read_csv(execution_dir / "turnover_summary.csv").to_dict(orient="records"),
            "artifact_dir": str(execution_dir),
        }

    def latest_live_payload(self) -> dict[str, Any]:
        dry_run_path = _latest_matching_file(self.artifacts_root, ["live_dry_run_summary.json"])
        submit_path = _latest_matching_file(self.artifacts_root, ["live_submission_summary.json"])
        dry_run = _safe_read_json(dry_run_path)
        submit = _safe_read_json(submit_path)
        risk_checks = submit.get("risk_checks", [])
        broker_health = None
        for item in risk_checks:
            if item.get("check_name") == "broker_health":
                broker_health = item
                break
        if broker_health is None:
            for item in dry_run.get("health_checks", []):
                if item.get("check_name") == "broker_connectivity":
                    broker_health = item
                    break
        duplicate_events = []
        if submit_path is not None:
            duplicate_events = [
                row
                for row in _safe_read_csv(Path(submit_path).parent / "broker_order_results.csv").to_dict(
                    orient="records"
                )
                if row.get("status") == "skipped"
            ]
        return {
            "dry_run_summary": dry_run,
            "submission_summary": submit,
            "risk_checks": risk_checks,
            "blocked_checks": [item for item in risk_checks if not bool(item.get("passed", False))],
            "duplicate_events": duplicate_events,
            "broker_health": broker_health or {},
            "dry_run_artifact_dir": str(dry_run_path.parent) if dry_run_path is not None else None,
            "submission_artifact_dir": str(submit_path.parent) if submit_path is not None else None,
        }

    def latest_daily_trading_payload(self) -> dict[str, Any]:
        summary_path = _latest_matching_file(self.artifacts_root, ["daily_trading_summary.json"])
        payload = _safe_read_json(summary_path)
        return {
            "generated_at": _now_utc(),
            "summary": payload,
            "artifact_path": str(summary_path) if summary_path is not None else None,
        }

    def strategy_quality_payload(self) -> dict[str, Any]:
        comparison_path = _latest_matching_file(self.artifacts_root, ["strategy_comparison_summary.csv"])
        history_path = _latest_matching_file(self.artifacts_root, ["strategy_performance_history.csv"])
        sharpe_path = _latest_matching_file(self.artifacts_root, ["rolling_sharpe_by_strategy.csv"])
        ic_path = _latest_matching_file(self.artifacts_root, ["rolling_ic_by_signal.csv"])
        drawdown_path = _latest_matching_file(self.artifacts_root, ["drawdown_by_strategy.csv"])
        summary_path = _latest_matching_file(self.artifacts_root, ["strategy_quality_summary.json"])
        comparison_rows = _frame_to_records(_safe_read_csv(comparison_path), head=50) if comparison_path else []
        history_rows = _frame_to_records(_safe_read_csv(history_path), tail=200) if history_path else []
        sharpe_rows = _frame_to_records(_safe_read_csv(sharpe_path), tail=200) if sharpe_path else []
        ic_rows = _frame_to_records(_safe_read_csv(ic_path), tail=200) if ic_path else []
        drawdown_rows = _frame_to_records(_safe_read_csv(drawdown_path), head=50) if drawdown_path else []
        summary = _safe_read_json(summary_path).get("summary", {})
        return {
            "generated_at": _now_utc(),
            "summary": summary,
            "strategy_comparison": comparison_rows,
            "strategy_history": history_rows,
            "rolling_sharpe": sharpe_rows,
            "rolling_ic": ic_rows,
            "drawdown": drawdown_rows,
            "artifact_paths": {
                "strategy_comparison_summary_path": str(comparison_path) if comparison_path is not None else None,
                "strategy_performance_history_path": str(history_path) if history_path is not None else None,
                "rolling_sharpe_by_strategy_path": str(sharpe_path) if sharpe_path is not None else None,
                "rolling_ic_by_signal_path": str(ic_path) if ic_path is not None else None,
                "drawdown_by_strategy_path": str(drawdown_path) if drawdown_path is not None else None,
                "strategy_quality_summary_path": str(summary_path) if summary_path is not None else None,
            },
        }

    def latest_alerts_payload(self) -> dict[str, Any]:
        run_payload = self.latest_run_payload()
        run_dir = Path(run_payload["run_dir"]) if run_payload.get("run_dir") else None
        alert_path = (
            run_dir / "monitoring" / "alerts.json"
            if run_dir is not None and (run_dir / "monitoring" / "alerts.json").exists()
            else _latest_matching_file(self.artifacts_root, ["alerts.json"])
        )
        alerts = []
        if alert_path is not None and alert_path.exists():
            try:
                alerts = json.loads(alert_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                alerts = []
        return {
            "alerts": alerts,
            "severity_counts": _status_counts([str(item.get("severity", "info")) for item in alerts]),
            "artifact_path": str(alert_path) if alert_path is not None else None,
        }

    def research_payload(self) -> dict[str, Any]:
        registry_path = _latest_matching_file(self.artifacts_root, ["research_registry.json"])
        leaderboard_path = _latest_matching_file(self.artifacts_root, ["research_leaderboard.json"])
        candidates_path = _latest_matching_file(self.artifacts_root, ["promotion_candidates.json"])
        promoted_path = _latest_matching_file(self.artifacts_root, ["promoted_strategies.json"])
        validation_path = _latest_matching_file(self.artifacts_root, ["strategy_validation.json"])
        lifecycle_path = _latest_matching_file(self.artifacts_root, ["strategy_lifecycle.json"])
        registry_payload = _safe_read_json(registry_path)
        leaderboard_payload = _safe_read_json(leaderboard_path)
        candidates_payload = _safe_read_json(candidates_path)
        promoted_payload = _safe_read_json(promoted_path)
        validation_payload = _safe_read_json(validation_path)
        lifecycle_payload = _safe_read_json(lifecycle_path)
        runs = registry_payload.get("runs", [])
        leaderboard_rows = leaderboard_payload.get("rows", [])
        candidate_rows = candidates_payload.get("rows", [])
        promoted_rows = promoted_payload.get("strategies", [])
        validation_rows = validation_payload.get("rows", [])
        lifecycle_rows = lifecycle_payload.get("strategies", [])
        strategy_portfolio_path = _latest_matching_file(self.artifacts_root, ["strategy_portfolio.json"])
        strategy_portfolio_payload = _safe_read_json(strategy_portfolio_path)
        return {
            "generated_at": _now_utc(),
            "registry_path": str(registry_path) if registry_path is not None else None,
            "leaderboard_path": str(leaderboard_path) if leaderboard_path is not None else None,
            "promotion_candidates_path": str(candidates_path) if candidates_path is not None else None,
            "promoted_strategies_path": str(promoted_path) if promoted_path is not None else None,
            "strategy_validation_path": str(validation_path) if validation_path is not None else None,
            "strategy_lifecycle_path": str(lifecycle_path) if lifecycle_path is not None else None,
            "strategy_portfolio_path": str(strategy_portfolio_path) if strategy_portfolio_path is not None else None,
            "summary": {
                "run_count": len(runs),
                "signal_family_counts": _status_counts(
                    [str(row.get("signal_family")) for row in runs if row.get("signal_family")]
                ),
                "universe_counts": _status_counts([str(row.get("universe")) for row in runs if row.get("universe")]),
                "eligible_candidate_count": len([row for row in candidate_rows if bool(row.get("eligible"))]),
                "promoted_strategy_count": len(promoted_rows),
                "validated_pass_count": len([row for row in validation_rows if row.get("validation_status") == "pass"]),
                "validated_weak_count": len([row for row in validation_rows if row.get("validation_status") == "weak"]),
                "degraded_strategy_count": len(
                    [row for row in lifecycle_rows if row.get("current_state") == "degraded"]
                ),
                "demoted_strategy_count": len([row for row in lifecycle_rows if row.get("current_state") == "demoted"]),
                "strategy_portfolio_selected_count": len(strategy_portfolio_payload.get("selected_strategies", [])),
            },
            "recent_runs": runs[:10],
            "leaderboard": leaderboard_rows[:10],
            "promotion_candidates": candidate_rows[:10],
            "promoted_strategies": promoted_rows[:10],
            "strategy_validation": validation_rows[:10],
            "strategy_lifecycle": lifecycle_rows[:10],
            "strategy_portfolio": strategy_portfolio_payload,
        }

    def strategy_monitoring_payload(self) -> dict[str, Any]:
        monitoring_path = _latest_matching_file(self.artifacts_root, ["strategy_monitoring.json"])
        recommendations_path = _latest_matching_file(self.artifacts_root, ["kill_switch_recommendations.json"])
        monitoring_payload = _safe_read_json(monitoring_path)
        recommendations_payload = _safe_read_json(recommendations_path)
        return {
            "generated_at": _now_utc(),
            "strategy_monitoring_path": str(monitoring_path) if monitoring_path is not None else None,
            "kill_switch_recommendations_path": str(recommendations_path) if recommendations_path is not None else None,
            "summary": monitoring_payload.get("summary", {}),
            "strategies": monitoring_payload.get("strategies", []),
            "attribution_summary": monitoring_payload.get("attribution_summary", {}),
            "recommendations": recommendations_payload.get(
                "recommendations",
                monitoring_payload.get("kill_switch_recommendations", []),
            ),
        }

    def adaptive_allocation_payload(self) -> dict[str, Any]:
        adaptive_path = _latest_matching_file(self.artifacts_root, ["adaptive_allocation.json"])
        adaptive_payload = _safe_read_json(adaptive_path)
        return {
            "generated_at": _now_utc(),
            "adaptive_allocation_path": str(adaptive_path) if adaptive_path is not None else None,
            "summary": adaptive_payload.get("summary", {}),
            "strategies": adaptive_payload.get("strategies", []),
            "top_changes": adaptive_payload.get("top_changes", []),
            "warnings": adaptive_payload.get("warnings", []),
            "policy": adaptive_payload.get("policy", {}),
        }

    def market_regime_payload(self) -> dict[str, Any]:
        regime_path = _latest_matching_file(self.artifacts_root, ["market_regime.json"])
        regime_payload = _safe_read_json(regime_path)
        history_rows: list[dict[str, Any]] = []
        for path in sorted(self.artifacts_root.rglob("market_regime.json")):
            payload = _safe_read_json(path)
            latest = payload.get("latest", {})
            if latest:
                history_rows.append(
                    {
                        "artifact_path": str(path),
                        "timestamp": latest.get("timestamp") or payload.get("generated_at"),
                        "regime_label": latest.get("regime_label"),
                        "confidence_score": latest.get("confidence_score"),
                        "realized_volatility": latest.get("realized_volatility"),
                        "long_return": latest.get("long_return"),
                    }
                )
        history_rows.sort(key=lambda row: str(row.get("timestamp") or ""), reverse=True)
        return {
            "generated_at": _now_utc(),
            "market_regime_path": str(regime_path) if regime_path is not None else None,
            "summary": regime_payload.get("latest", {}),
            "history": history_rows[:20],
            "policy": regime_payload.get("policy", {}),
        }

    def strategy_validation_payload(self) -> dict[str, Any]:
        validation_path = _latest_matching_file(self.artifacts_root, ["strategy_validation.json"])
        validation_payload = _safe_read_json(validation_path)
        return {
            "generated_at": _now_utc(),
            "strategy_validation_path": str(validation_path) if validation_path is not None else None,
            "summary": validation_payload.get("summary", {}),
            "rows": validation_payload.get("rows", []),
            "policy": validation_payload.get("policy", {}),
        }

    def strategy_lifecycle_payload(self) -> dict[str, Any]:
        lifecycle_path = _latest_matching_file(self.artifacts_root, ["strategy_lifecycle.json"])
        lifecycle_payload = _safe_read_json(lifecycle_path)
        governance_path = _latest_matching_file(self.artifacts_root, ["strategy_governance_summary.json"])
        governance_payload = _safe_read_json(governance_path)
        return {
            "generated_at": _now_utc(),
            "strategy_lifecycle_path": str(lifecycle_path) if lifecycle_path is not None else None,
            "strategy_governance_summary_path": str(governance_path) if governance_path is not None else None,
            "summary": lifecycle_payload.get("summary", {}),
            "strategies": lifecycle_payload.get("strategies", []),
            "governance_summary": governance_payload,
        }

    def latest_automated_orchestration_payload(self) -> dict[str, Any]:
        latest = _latest_matching_file(self.artifacts_root, ["orchestration_run.json"])
        if latest is None:
            return {"run_dir": None, "summary": {}, "stage_records": []}
        payload = _safe_read_json(latest)
        return {
            "run_dir": str(latest.parent),
            "summary": payload,
            "stage_records": payload.get("stage_records", []),
        }

    def system_evaluation_payload(self) -> dict[str, Any]:
        latest = _latest_matching_file(self.artifacts_root, ["system_evaluation.json"])
        payload = _safe_read_json(latest)
        return {
            "generated_at": _now_utc(),
            "system_evaluation_path": str(latest) if latest is not None else None,
            "row": payload.get("row", {}),
            "metrics": payload.get("metrics", {}),
        }

    def system_evaluation_history_payload(self) -> dict[str, Any]:
        latest = _latest_matching_file(self.artifacts_root, ["system_evaluation_history.json"])
        payload = _safe_read_json(latest)
        return {
            "generated_at": _now_utc(),
            "system_evaluation_history_path": str(latest) if latest is not None else None,
            "summary": payload.get("summary", {}),
            "rows": payload.get("rows", [])[:50],
        }

    def experiments_payload(self) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        for path in sorted(self.artifacts_root.rglob("experiment_run.json")):
            payload = _safe_read_json(path)
            summary = payload.get("summary", {})
            rows.append(
                {
                    "experiment_name": payload.get("experiment_name"),
                    "experiment_run_id": payload.get("experiment_run_id"),
                    "status": payload.get("status"),
                    "started_at": payload.get("started_at"),
                    "ended_at": payload.get("ended_at"),
                    "variant_count": summary.get("variant_count", 0),
                    "variant_run_count": summary.get("variant_run_count", 0),
                    "succeeded_count": summary.get("succeeded_count", 0),
                    "failed_count": summary.get("failed_count", 0),
                    "run_dir": str(path.parent),
                    "variants": payload.get("variants", []),
                    "system_evaluation": payload.get("system_evaluation", {}),
                }
            )
        rows.sort(key=lambda row: str(row.get("started_at") or row.get("run_dir")), reverse=True)
        latest = rows[0] if rows else {}
        return {
            "generated_at": _now_utc(),
            "summary": {
                "experiment_count": len(rows),
                "latest_experiment_name": latest.get("experiment_name"),
                "latest_variant_count": latest.get("variant_count", 0),
            },
            "latest": latest,
            "rows": rows[:20],
        }

    def overview_payload(self) -> dict[str, Any]:
        latest_run = self.latest_run_payload()
        registry = self.registry_payload()
        portfolio = self.latest_portfolio_payload()
        execution = self.latest_execution_payload()
        live = self.latest_live_payload()
        alerts = self.latest_alerts_payload()
        research = self.research_payload()
        strategy_monitoring = self.strategy_monitoring_payload()
        adaptive_allocation = self.adaptive_allocation_payload()
        market_regime = self.market_regime_payload()
        orchestration = self.latest_automated_orchestration_payload()
        system_eval = self.system_evaluation_payload()
        system_eval_history = self.system_evaluation_history_payload()
        experiments = self.experiments_payload()
        validation = self.strategy_validation_payload()
        lifecycle = self.strategy_lifecycle_payload()
        daily_trading = self.latest_daily_trading_payload()
        strategy_quality = self.strategy_quality_payload()
        latest_run_summary = latest_run.get("summary", {})
        latest_run_health = latest_run.get("health", {})
        portfolio_summary = portfolio.get("summary", {})
        execution_summary = execution.get("summary", {})
        broker_health = live.get("broker_health", {})
        quick_links = [
            {"label": "latest_run_dir", "path": latest_run.get("run_dir")},
            {"label": "registry", "path": registry.get("registry_path")},
            {"label": "portfolio", "path": portfolio.get("artifact_dir")},
            {"label": "adaptive_allocation", "path": adaptive_allocation.get("adaptive_allocation_path")},
            {"label": "experiment_run", "path": experiments.get("latest", {}).get("run_dir")},
            {"label": "system_evaluation", "path": system_eval.get("system_evaluation_path")},
            {"label": "execution", "path": execution.get("artifact_dir")},
            {"label": "live_submit", "path": live.get("submission_artifact_dir")},
        ]
        return {
            "generated_at": _now_utc(),
            "latest_run": {
                "run_name": latest_run_summary.get("run_name"),
                "status": latest_run_summary.get("status"),
                "schedule_type": latest_run_summary.get("schedule_type"),
                "started_at": latest_run_summary.get("started_at"),
                "health_status": latest_run_health.get("status"),
                "run_dir": latest_run.get("run_dir"),
            },
            "monitoring": {
                "status": latest_run_health.get("status"),
                "alert_counts": latest_run_health.get("alert_counts", alerts.get("severity_counts", {})),
            },
            "registry": {
                "approved_strategy_count": int(registry.get("status_counts", {}).get("approved", 0)),
                "strategy_count": len(registry.get("strategies", [])),
            },
            "research": {
                "run_count": research.get("summary", {}).get("run_count", 0),
                "eligible_candidate_count": research.get("summary", {}).get("eligible_candidate_count", 0),
                "promoted_strategy_count": research.get("summary", {}).get("promoted_strategy_count", 0),
                "validated_pass_count": validation.get("summary", {}).get("pass_count", 0),
                "strategy_portfolio_selected_count": research.get("summary", {}).get(
                    "strategy_portfolio_selected_count", 0
                ),
                "top_leaderboard_entry": research.get("leaderboard", [{}])[0] if research.get("leaderboard") else {},
            },
            "strategy_monitoring": {
                "warning_strategy_count": strategy_monitoring.get("summary", {}).get("warning_strategy_count", 0),
                "deactivation_candidate_count": strategy_monitoring.get("summary", {}).get(
                    "deactivation_candidate_count", 0
                ),
                "aggregate_return": strategy_monitoring.get("summary", {}).get("aggregate_return"),
            },
            "strategy_lifecycle": {
                "under_review_count": lifecycle.get("summary", {}).get("under_review_count", 0),
                "degraded_count": lifecycle.get("summary", {}).get("degraded_count", 0),
                "demoted_count": lifecycle.get("summary", {}).get("demoted_count", 0),
            },
            "adaptive_allocation": {
                "selected_strategy_count": adaptive_allocation.get("summary", {}).get("total_selected_strategies", 0),
                "absolute_weight_change": adaptive_allocation.get("summary", {}).get("absolute_weight_change"),
                "warning_count": adaptive_allocation.get("summary", {}).get("warning_count", 0),
            },
            "market_regime": {
                "regime_label": market_regime.get("summary", {}).get("regime_label"),
                "confidence_score": market_regime.get("summary", {}).get("confidence_score"),
                "realized_volatility": market_regime.get("summary", {}).get("realized_volatility"),
            },
            "orchestration": {
                "run_id": orchestration.get("summary", {}).get("run_id"),
                "status": orchestration.get("summary", {}).get("status"),
                "selected_strategy_count": orchestration.get("summary", {})
                .get("outputs", {})
                .get("selected_strategy_count", 0),
            },
            "daily_trading": {
                "status": daily_trading.get("summary", {}).get("status"),
                "active_strategy_count": daily_trading.get("summary", {}).get("active_strategy_count"),
                "fill_count": daily_trading.get("summary", {}).get("fill_count"),
                "executable_order_count": daily_trading.get("summary", {}).get("executable_order_count"),
            },
            "experiments": {
                "experiment_count": experiments.get("summary", {}).get("experiment_count", 0),
                "latest_experiment_name": experiments.get("summary", {}).get("latest_experiment_name"),
                "latest_variant_count": experiments.get("summary", {}).get("latest_variant_count", 0),
            },
            "system_evaluation": {
                "total_return": system_eval.get("row", {}).get("total_return"),
                "sharpe": system_eval.get("row", {}).get("sharpe"),
                "max_drawdown": system_eval.get("row", {}).get("max_drawdown"),
                "best_run_id": system_eval_history.get("summary", {}).get("best_run_id"),
            },
            "portfolio": {
                "generated_position_count": len(portfolio.get("combined_positions", [])),
                "gross_exposure": portfolio_summary.get("gross_exposure_after_constraints"),
                "net_exposure": portfolio_summary.get("net_exposure_after_constraints"),
            },
            "strategy_quality": {
                "strategy_count": strategy_quality.get("summary", {}).get("strategy_count", 0),
                "active_strategy_count": strategy_quality.get("summary", {}).get("active_strategy_count", 0),
            },
            "execution": {
                "executable_order_count": execution_summary.get("executable_order_count"),
                "rejected_order_count": execution_summary.get("rejected_order_count"),
                "expected_total_cost": execution_summary.get("expected_total_cost"),
            },
            "broker_health": {
                "status": broker_health.get("status") or ("pass" if broker_health.get("passed") else None),
                "message": broker_health.get("message"),
            },
            "quick_links": [item for item in quick_links if item.get("path")],
        }

    def strategies_payload(self) -> dict[str, Any]:
        registry = self.registry_payload()
        lifecycle = self.strategy_lifecycle_payload()
        strategy_quality = self.strategy_quality_payload()
        tags = sorted({tag for row in registry["strategies"] for tag in row.get("tags", [])})
        return {
            "generated_at": _now_utc(),
            "registry_path": registry["registry_path"],
            "summary": {
                "status_counts": registry["status_counts"],
                "family_counts": registry["family_counts"],
                "lifecycle_counts": lifecycle.get("summary", {}).get("state_counts", {}),
                "strategy_quality_count": strategy_quality.get("summary", {}).get("strategy_count", 0),
            },
            "filters": {
                "statuses": sorted(registry["status_counts"]),
                "families": sorted(registry["family_counts"]),
                "tags": tags,
            },
            "strategies": registry["strategies"],
            "champion_challenger": registry["champion_challenger"],
            "strategy_lifecycle": lifecycle.get("strategies", []),
            "strategy_comparison": strategy_quality.get("strategy_comparison", []),
            "rolling_sharpe": strategy_quality.get("rolling_sharpe", []),
            "rolling_ic": strategy_quality.get("rolling_ic", []),
        }

    def runs_payload(self) -> dict[str, Any]:
        return {
            "generated_at": _now_utc(),
            "runs": self.recent_runs(),
            "orchestration_runs": self.recent_orchestration_runs(),
            "experiments": self.experiments_payload(),
            "system_evaluation": self.system_evaluation_history_payload(),
        }

    def latest_run_detail_payload(self) -> dict[str, Any]:
        latest_run = self.latest_run_payload()
        return {
            "generated_at": _now_utc(),
            "run_dir": latest_run.get("run_dir"),
            "summary": latest_run.get("summary", {}),
            "health": latest_run.get("health", {}),
            "stages": latest_run.get("stages", []),
        }

    def portfolio_payload(self) -> dict[str, Any]:
        payload = self.latest_portfolio_payload()
        payload["adaptive_allocation"] = self.adaptive_allocation_payload()
        payload["market_regime"] = self.market_regime_payload()
        payload["strategy_quality"] = self.strategy_quality_payload()
        payload["generated_at"] = _now_utc()
        return _normalize_portfolio_payload(payload)

    def execution_payload(self) -> dict[str, Any]:
        payload = self.latest_execution_payload()
        payload["generated_at"] = _now_utc()
        return _normalize_execution_payload(payload)

    def live_payload(self) -> dict[str, Any]:
        payload = self.latest_live_payload()
        payload["generated_at"] = _now_utc()
        return _normalize_live_payload(payload)

    def research_latest_payload(self) -> dict[str, Any]:
        payload = self.research_payload()
        payload["adaptive_allocation"] = self.adaptive_allocation_payload()
        payload["market_regime"] = self.market_regime_payload()
        payload["strategy_validation"] = self.strategy_validation_payload()
        payload["strategy_lifecycle"] = self.strategy_lifecycle_payload()
        return payload

    def portfolio_overview_payload(self) -> dict[str, Any]:
        payload = build_portfolio_overview_payload(artifacts_root=self.artifacts_root)
        payload["generated_at"] = _now_utc()
        return _normalize_portfolio_overview_payload(payload)

    def strategy_pnl_latest_payload(self) -> dict[str, Any]:
        csv_path = _latest_matching_file(
            self.artifacts_root, ["strategy_pnl_attribution.csv", "replay_strategy_pnl.csv"]
        )
        summary_path = _latest_matching_file(
            self.artifacts_root, ["pnl_attribution_summary.json", "replay_pnl_attribution_summary.json"]
        )
        frame = _safe_read_csv(csv_path)
        payload = _safe_read_json(summary_path)
        return {
            "generated_at": _now_utc(),
            "summary": payload,
            "rows": _frame_to_records(frame),
            "meta": {
                "csv_path": str(csv_path) if csv_path is not None else None,
                "summary_path": str(summary_path) if summary_path is not None else None,
            },
        }

    def symbol_pnl_latest_payload(self) -> dict[str, Any]:
        csv_path = _latest_matching_file(self.artifacts_root, ["symbol_pnl_attribution.csv", "replay_symbol_pnl.csv"])
        frame = _safe_read_csv(csv_path)
        return {
            "generated_at": _now_utc(),
            "rows": _frame_to_records(frame),
            "meta": {"csv_path": str(csv_path) if csv_path is not None else None},
        }

    def attribution_latest_payload(self) -> dict[str, Any]:
        summary_path = _latest_matching_file(
            self.artifacts_root, ["pnl_attribution_summary.json", "replay_pnl_attribution_summary.json"]
        )
        payload = _safe_read_json(summary_path)
        return {
            "generated_at": _now_utc(),
            "summary": payload,
            "meta": {"summary_path": str(summary_path) if summary_path is not None else None},
        }

    def execution_costs_latest_payload(self) -> dict[str, Any]:
        summary_path = _latest_matching_file(
            self.artifacts_root, ["paper_run_summary_latest.json", "replay_summary.json"]
        )
        payload = _safe_read_json(summary_path)
        summary = dict(payload.get("summary") or payload)
        return {
            "generated_at": _now_utc(),
            "summary": {
                "gross_total_pnl": summary.get("gross_total_pnl"),
                "net_total_pnl": summary.get("net_total_pnl", summary.get("total_pnl")),
                "total_execution_cost": summary.get("total_execution_cost"),
                "total_slippage_cost": summary.get("total_slippage_cost"),
                "total_commission_cost": summary.get("total_commission_cost"),
                "total_spread_cost": summary.get("total_spread_cost"),
                "cost_drag_pct": summary.get("cost_drag_pct"),
            },
            "meta": {"summary_path": str(summary_path) if summary_path is not None else None},
        }

    def strategy_costs_latest_payload(self) -> dict[str, Any]:
        payload = self.strategy_pnl_latest_payload()
        return {
            "generated_at": payload.get("generated_at"),
            "rows": payload.get("rows", []),
            "meta": payload.get("meta", {}),
        }

    def cost_drag_latest_payload(self) -> dict[str, Any]:
        attribution = self.attribution_latest_payload()
        strategy_payload = self.strategy_pnl_latest_payload()
        return {
            "generated_at": _now_utc(),
            "summary": attribution.get("summary", {}),
            "strategies": strategy_payload.get("rows", []),
            "meta": {
                "summary_path": attribution.get("meta", {}).get("summary_path"),
                "csv_path": strategy_payload.get("meta", {}).get("csv_path"),
            },
        }

    def execution_diagnostics_payload(self) -> dict[str, Any]:
        payload = build_execution_diagnostics_payload(artifacts_root=self.artifacts_root)
        payload["generated_at"] = _now_utc()
        return _normalize_execution_diagnostics_payload(payload)

    def strategy_detail_payload(self, strategy_id: str) -> dict[str, Any]:
        payload = build_strategy_detail_payload(artifacts_root=self.artifacts_root, strategy_id=strategy_id)
        payload["generated_at"] = _now_utc()
        return _normalize_strategy_detail_payload(payload)

    def trade_detail_payload(self, trade_id: str) -> dict[str, Any]:
        payload = build_trade_detail_payload(
            artifacts_root=self.artifacts_root,
            feature_dir=self.feature_dir,
            trade_id=trade_id,
        )
        payload["generated_at"] = _now_utc()
        return _normalize_trade_detail_payload(payload)

    def trade_blotter_payload(self) -> dict[str, Any]:
        payload = build_trade_blotter_payload(artifacts_root=self.artifacts_root)
        payload["generated_at"] = _now_utc()
        return _normalize_trade_blotter_payload(payload)

    def ops_payload(self) -> dict[str, Any]:
        latest_run = self.latest_run_payload()
        latest_run_summary = latest_run.get("summary", {})
        latest_run_health = latest_run.get("health", {})
        live = self.live_payload()
        alerts = self.latest_alerts_payload()
        execution_diag = self.execution_diagnostics_payload()
        orchestration = self.latest_automated_orchestration_payload()
        runs = self.runs_payload()
        return {
            "generated_at": _now_utc(),
            "summary": {
                "latest_run_name": latest_run_summary.get("run_name"),
                "latest_run_status": latest_run_summary.get("status"),
                "health_status": latest_run_health.get("status"),
                "critical_alert_count": int(latest_run_health.get("alert_counts", {}).get("critical", 0) or 0),
                "warning_alert_count": int(latest_run_health.get("alert_counts", {}).get("warning", 0) or 0),
                "blocked_check_count": len(live.get("blocked_checks", [])),
                "missing_fill_count": execution_diag.get("summary", {}).get("missing_fill_count"),
            },
            "latest_run": latest_run,
            "alerts": alerts,
            "live": live,
            "execution_diagnostics": execution_diag,
            "orchestration": orchestration,
            "runs": runs.get("runs", []),
            "orchestration_runs": runs.get("orchestration_runs", []),
        }

    def discovery_payload(self) -> dict[str, Any]:
        payload = build_discovery_payload(artifacts_root=self.artifacts_root)
        payload["generated_at"] = _now_utc()
        return _normalize_discovery_payload(payload)

    def chart_payload(
        self,
        symbol: str,
        *,
        timeframe: str = "1d",
        lookback: int | None = 200,
        run_id: str | None = None,
        source: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        return _normalize_chart_payload(
            build_chart_payload(
                artifacts_root=self.artifacts_root,
                feature_dir=self.feature_dir,
                symbol=symbol,
                timeframe=timeframe,
                lookback=lookback,
                run_id=run_id,
                source=source,
                mode=mode,
            )
        )

    def trades_payload(
        self,
        symbol: str,
        *,
        run_id: str | None = None,
        source: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        payload = build_trades_payload(
            artifacts_root=self.artifacts_root,
            symbol=symbol,
            run_id=run_id,
            source=source,
            mode=mode,
        )
        normalized = dict(_json_safe(payload))
        normalized["trades"] = _row_list_contract(
            normalized.get("trades"),
            [
                "trade_id",
                "symbol",
                "side",
                "qty",
                "entry_ts",
                "entry_price",
                "exit_ts",
                "exit_price",
                "realized_pnl",
                "status",
                "strategy_id",
                "source",
                "run_id",
                "mode",
            ],
        )
        normalized["fills"] = _row_list_contract(
            normalized.get("fills"),
            ["ts", "symbol", "side", "qty", "price", "order_id", "status", "reason", "source_type"],
        )
        normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
        return normalized

    def signals_payload(
        self,
        symbol: str,
        *,
        lookback: int | None = 200,
        run_id: str | None = None,
        source: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        payload = build_signals_payload(
            artifacts_root=self.artifacts_root,
            symbol=symbol,
            lookback=lookback,
            run_id=run_id,
            source=source,
            mode=mode,
        )
        normalized = dict(_json_safe(payload))
        normalized["signals"] = _row_list_contract(normalized.get("signals"), ["ts", "type", "price", "label", "score"])
        normalized["meta"] = dict(_json_safe(normalized.get("meta") or {}))
        return normalized
