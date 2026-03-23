from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from trading_platform.governance.persistence import load_strategy_registry


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


class DashboardDataService:
    def __init__(self, artifacts_root: str | Path) -> None:
        self.artifacts_root = Path(artifacts_root)

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
                    "failed_stage_count": int((stage_df.get("status", pd.Series(dtype=object)) == "failed").sum())
                    if not stage_df.empty
                    else 0,
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
            rows.append(
                {
                    "run_id": payload.get("run_id"),
                    "run_name": payload.get("run_name", run_dir.name),
                    "run_dir": str(run_dir),
                    "started_at": payload.get("started_at"),
                    "ended_at": payload.get("ended_at"),
                    "status": payload.get("status", "unknown"),
                    "schedule_frequency": payload.get("schedule_frequency"),
                    "failed_stage_count": sum(1 for row in stage_records if row.get("status") == "failed"),
                    "selected_strategy_count": outputs.get("selected_strategy_count", 0),
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
                    "degradation_summary": degradation.get("summary_metrics", degradation.get("metrics", {})) if degradation else {},
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
        latest_summary_path = _newest_path(candidates + _candidate_files(self.artifacts_root, ["allocation_summary.json"]))
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
            aggregated = sleeve_df.groupby("sleeve_name", as_index=False)[weight_col].sum().sort_values(weight_col, ascending=False)
            sleeve_weights = aggregated.to_dict(orient="records")
        top_positions = []
        if not combined_df.empty and "target_weight" in combined_df.columns:
            sorted_df = combined_df.assign(abs_weight=combined_df["target_weight"].abs()).sort_values("abs_weight", ascending=False)
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
            "liquidity_diagnostics": _safe_read_csv(execution_dir / "liquidity_constraints_report.csv").to_dict(orient="records"),
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
                row for row in _safe_read_csv(Path(submit_path).parent / "broker_order_results.csv").to_dict(orient="records")
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

    def latest_alerts_payload(self) -> dict[str, Any]:
        run_payload = self.latest_run_payload()
        run_dir = Path(run_payload["run_dir"]) if run_payload.get("run_dir") else None
        alert_path = run_dir / "monitoring" / "alerts.json" if run_dir is not None and (run_dir / "monitoring" / "alerts.json").exists() else _latest_matching_file(self.artifacts_root, ["alerts.json"])
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
                "signal_family_counts": _status_counts([str(row.get("signal_family")) for row in runs if row.get("signal_family")]),
                "universe_counts": _status_counts([str(row.get("universe")) for row in runs if row.get("universe")]),
                "eligible_candidate_count": len([row for row in candidate_rows if bool(row.get("eligible"))]),
                "promoted_strategy_count": len(promoted_rows),
                "validated_pass_count": len([row for row in validation_rows if row.get("validation_status") == "pass"]),
                "validated_weak_count": len([row for row in validation_rows if row.get("validation_status") == "weak"]),
                "degraded_strategy_count": len([row for row in lifecycle_rows if row.get("current_state") == "degraded"]),
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
        orchestration = self.latest_automated_orchestration_payload()
        validation = self.strategy_validation_payload()
        lifecycle = self.strategy_lifecycle_payload()
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
                "strategy_portfolio_selected_count": research.get("summary", {}).get("strategy_portfolio_selected_count", 0),
                "top_leaderboard_entry": research.get("leaderboard", [{}])[0] if research.get("leaderboard") else {},
            },
            "strategy_monitoring": {
                "warning_strategy_count": strategy_monitoring.get("summary", {}).get("warning_strategy_count", 0),
                "deactivation_candidate_count": strategy_monitoring.get("summary", {}).get("deactivation_candidate_count", 0),
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
            "orchestration": {
                "run_id": orchestration.get("summary", {}).get("run_id"),
                "status": orchestration.get("summary", {}).get("status"),
                "selected_strategy_count": orchestration.get("summary", {}).get("outputs", {}).get("selected_strategy_count", 0),
            },
            "portfolio": {
                "generated_position_count": len(portfolio.get("combined_positions", [])),
                "gross_exposure": portfolio_summary.get("gross_exposure_after_constraints"),
                "net_exposure": portfolio_summary.get("net_exposure_after_constraints"),
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
        tags = sorted({tag for row in registry["strategies"] for tag in row.get("tags", [])})
        return {
            "generated_at": _now_utc(),
            "registry_path": registry["registry_path"],
            "summary": {
                "status_counts": registry["status_counts"],
                "family_counts": registry["family_counts"],
                "lifecycle_counts": lifecycle.get("summary", {}).get("state_counts", {}),
            },
            "filters": {"statuses": sorted(registry["status_counts"]), "families": sorted(registry["family_counts"]), "tags": tags},
            "strategies": registry["strategies"],
            "champion_challenger": registry["champion_challenger"],
            "strategy_lifecycle": lifecycle.get("strategies", []),
        }

    def runs_payload(self) -> dict[str, Any]:
        return {
            "generated_at": _now_utc(),
            "runs": self.recent_runs(),
            "orchestration_runs": self.recent_orchestration_runs(),
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
        payload["generated_at"] = _now_utc()
        return payload

    def execution_payload(self) -> dict[str, Any]:
        payload = self.latest_execution_payload()
        payload["generated_at"] = _now_utc()
        return payload

    def live_payload(self) -> dict[str, Any]:
        payload = self.latest_live_payload()
        payload["generated_at"] = _now_utc()
        return payload

    def research_latest_payload(self) -> dict[str, Any]:
        payload = self.research_payload()
        payload["adaptive_allocation"] = self.adaptive_allocation_payload()
        payload["strategy_validation"] = self.strategy_validation_payload()
        payload["strategy_lifecycle"] = self.strategy_lifecycle_payload()
        return payload
