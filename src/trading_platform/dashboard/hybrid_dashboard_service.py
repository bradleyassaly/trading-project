from __future__ import annotations

from collections import Counter
from typing import Any

from trading_platform.dashboard.service import DashboardDataService, _now_utc
from trading_platform.db.session import create_session_factory
from trading_platform.db.services import DecisionQueryService, OpsQueryService, RunQueryService
from trading_platform.db.settings import resolve_database_settings


def _bool_status(value: object) -> str:
    text = str(value or "").lower()
    if text in {"completed", "succeeded", "pass", "healthy"}:
        return "healthy"
    if text in {"failed", "critical"}:
        return "critical"
    if text:
        return text
    return "unknown"


class HybridDashboardDataService(DashboardDataService):
    def __init__(
        self,
        artifacts_root,
        *,
        feature_dir=None,
        enable_database_metadata: bool | None = None,
        database_url: str | None = None,
        database_schema: str | None = None,
    ) -> None:
        super().__init__(artifacts_root, feature_dir=feature_dir)
        settings = resolve_database_settings(
            enable_database_metadata=enable_database_metadata,
            database_url=database_url,
            database_schema=database_schema,
        )
        self.db_session_factory = create_session_factory(settings)
        self.run_queries = RunQueryService(self.db_session_factory)
        self.decision_queries = DecisionQueryService(self.db_session_factory)
        self.ops_queries = OpsQueryService(self.db_session_factory)

    @property
    def db_enabled(self) -> bool:
        return self.db_session_factory is not None

    def _db_runs_payload(self) -> dict[str, Any] | None:
        if not self.db_enabled:
            return None
        portfolio_runs = [row.to_dict() for row in self.run_queries.list_recent_portfolio_runs(limit=20)]
        if not portfolio_runs:
            return None
        research_runs = [row.to_dict() for row in self.run_queries.list_recent_research_runs(limit=20)]
        return {
            "generated_at": _now_utc(),
            "runs": portfolio_runs,
            "research_runs": research_runs,
            "orchestration_runs": self.recent_orchestration_runs(),
            "experiments": self.experiments_payload(),
            "system_evaluation": self.system_evaluation_history_payload(),
            "source": "hybrid",
        }

    def runs_payload(self) -> dict[str, Any]:
        db_payload = self._db_runs_payload()
        if db_payload is not None:
            return db_payload
        payload = super().runs_payload()
        payload["source"] = "artifact"
        return payload

    def latest_run_detail_payload(self) -> dict[str, Any]:
        if self.db_enabled:
            recent = self.run_queries.list_recent_portfolio_runs(limit=1)
            if recent:
                detail = self.run_queries.get_run_detail(recent[0].run_id, run_kind="portfolio")
                if detail is not None:
                    payload = detail.to_dict()
                    return {
                        "generated_at": _now_utc(),
                        "run_dir": payload["summary"].get("artifact_dir"),
                        "summary": payload["summary"],
                        "health": {"status": _bool_status(payload["summary"].get("status"))},
                        "stages": [],
                        "artifacts": payload.get("artifacts", []),
                        "source": "db",
                    }
        payload = super().latest_run_detail_payload()
        payload["source"] = "artifact"
        return payload

    def trade_blotter_payload(self) -> dict[str, Any]:
        if self.db_enabled:
            trades = [row.to_dict() for row in self.decision_queries.list_recent_trade_decisions(limit=250)]
            if trades:
                status_counts = Counter(str(row.get("status") or "unknown") for row in trades)
                return {
                    "summary": {
                        "trade_count": len(trades),
                        "open_trade_count": sum(1 for row in trades if str(row.get("status") or "") not in {"closed", "rejected"}),
                        "closed_trade_count": sum(1 for row in trades if str(row.get("status") or "") == "closed"),
                        "winning_trade_count": 0,
                        "total_realized_pnl": None,
                        "status_counts": dict(status_counts),
                    },
                    "trades": [
                        {
                            "trade_id": row.get("trade_id"),
                            "timestamp": row.get("timestamp"),
                            "symbol": row.get("symbol"),
                            "side": row.get("side"),
                            "qty": row.get("quantity"),
                            "target_weight": row.get("target_weight"),
                            "strategy_id": row.get("strategy_id"),
                            "signal_score": row.get("signal_score"),
                            "ranking_score": row.get("rank_score"),
                            "universe_rank": None,
                            "expected_edge": row.get("expected_edge"),
                            "order_status": row.get("order_status"),
                            "status": row.get("status"),
                            "entry_ts": row.get("timestamp"),
                            "exit_ts": None,
                            "entry_price": None,
                            "exit_price": None,
                            "realized_pnl": None,
                            "unrealized_pnl": None,
                            "portfolio_qty": row.get("quantity"),
                            "portfolio_market_value": None,
                            "source": row.get("source"),
                            "run_id": row.get("run_name"),
                            "mode": row.get("mode"),
                        }
                        for row in trades
                    ],
                    "meta": {"source": "db"},
                    "source": "db",
                }
        payload = super().trade_blotter_payload()
        payload["source"] = "artifact"
        payload.setdefault("meta", {})["source"] = "artifact"
        return payload

    def trade_detail_payload(self, trade_id: str) -> dict[str, Any]:
        db_detail = self.decision_queries.get_trade_decision_detail(trade_id) if self.db_enabled else None
        if db_detail is None:
            payload = super().trade_detail_payload(trade_id)
            payload["source"] = "artifact"
            payload.setdefault("meta", {})["source"] = "artifact"
            return payload

        decision = dict(db_detail.get("decision") or {})
        chart = self.chart_payload(decision.get("symbol")) if decision.get("symbol") else {}
        candidate_rows = list(db_detail.get("candidate_evaluations") or [])
        filter_rows = list(db_detail.get("filter_results") or [])
        execution = list(db_detail.get("execution") or [])
        fills = [fill for row in execution for fill in row.get("fills", [])]
        events = [event for row in execution for event in row.get("events", [])]
        lifecycle = [
            {
                "ts": decision.get("timestamp"),
                "kind": "decision",
                "label": decision.get("status") or "decision",
                "detail": decision.get("entry_reason_summary") or decision.get("rejection_reason") or "db-backed portfolio decision",
                "status": decision.get("status"),
            },
            *[
                {
                    "ts": event.get("event_ts"),
                    "kind": "order_event",
                    "label": event.get("event_type"),
                    "detail": str(event.get("payload") or {}),
                    "status": event.get("event_type"),
                }
                for event in events
            ],
            *[
                {
                    "ts": fill.get("fill_ts"),
                    "kind": "fill",
                    "label": "fill",
                    "detail": f"qty={fill.get('quantity')} price={fill.get('price')}",
                    "status": "filled",
                }
                for fill in fills
            ],
        ]
        return {
            "trade": {
                "trade_id": decision.get("trade_id"),
                "symbol": decision.get("symbol"),
                "side": decision.get("side"),
                "qty": decision.get("quantity"),
                "entry_ts": decision.get("timestamp"),
                "entry_price": None,
                "exit_ts": None,
                "exit_price": None,
                "realized_pnl": None,
                "status": decision.get("status"),
                "strategy_id": decision.get("strategy_id"),
                "source": "db",
                "run_id": decision.get("run_name"),
                "mode": decision.get("mode"),
                "trade_source": "db_control_plane",
                "trade_source_mode": "db_control_plane",
                "hold_duration_hours": None,
            },
            "chart": chart,
            "signals": chart.get("signals", []),
            "fills": fills,
            "orders": execution,
            "trade_summary": {
                "symbol": decision.get("symbol"),
                "side": decision.get("side"),
                "status": decision.get("status"),
                "strategy_id": decision.get("strategy_id"),
                "entry_ts": decision.get("timestamp"),
                "qty": decision.get("quantity"),
                "realized_pnl": None,
            },
            "portfolio_context": {
                "selection_status": decision.get("status"),
                "target_weight": decision.get("target_weight"),
                "candidate_count": len(candidate_rows),
                "base_universe_id": decision.get("base_universe_id"),
                "sub_universe_id": decision.get("sub_universe_id"),
            },
            "execution_review": {
                "order_count": len(execution),
                "fill_count": len(fills),
                "latest_order_status": decision.get("order_status"),
            },
            "outcome_review": {
                "trade_status": decision.get("status"),
                "realized_pnl": None,
                "unrealized_pnl": None,
            },
            "related_metadata": {
                "run": db_detail.get("run", {}),
                "artifacts": db_detail.get("artifacts", []),
            },
            "provenance": {
                "latest": candidate_rows[0] if candidate_rows else {},
                "rows": candidate_rows[:8],
                "filter_results": filter_rows,
            },
            "lifecycle": sorted(lifecycle, key=lambda row: str(row.get("ts") or "")),
            "comparison": {
                "related_trades": [],
                "available_chart_sources": chart.get("meta", {}).get("available_chart_sources", []),
                "available_provenance_sources": [],
            },
            "explain": {
                "signal": None,
                "indicator_snapshot": {},
                "regime": {},
                "sizing_context": {
                    "qty": decision.get("quantity"),
                    "target_weight": decision.get("target_weight"),
                },
                "candidate_evaluations": candidate_rows,
                "filter_results": filter_rows,
                "signal_contributions": db_detail.get("signal_contributions", []),
            },
            "meta": {
                "source": "hybrid" if chart else "db",
                "strategy_id": decision.get("strategy_id"),
                "run_id": decision.get("run_name"),
                "mode": decision.get("mode"),
                "trade_source": "db_control_plane",
                "trade_source_mode": "db_control_plane",
            },
            "generated_at": _now_utc(),
            "source": "hybrid" if chart else "db",
        }

    def ops_payload(self) -> dict[str, Any]:
        db_payload = self.ops_queries.get_ops_health_summary() if self.db_enabled else {}
        if db_payload and db_payload.get("portfolio_runs"):
            latest_run = self.latest_run_detail_payload()
            live = self.live_payload()
            execution_diag = self.execution_diagnostics_payload()
            orchestration = self.latest_automated_orchestration_payload()
            runs = list(db_payload.get("portfolio_runs", []))
            return {
                "generated_at": _now_utc(),
                "summary": {
                    "latest_run_name": db_payload.get("summary", {}).get("latest_run_name"),
                    "latest_run_status": db_payload.get("summary", {}).get("latest_run_status"),
                    "health_status": _bool_status(db_payload.get("summary", {}).get("latest_run_status")),
                    "critical_alert_count": db_payload.get("summary", {}).get("recent_failure_count", 0),
                    "warning_alert_count": 0,
                    "blocked_check_count": len(live.get("blocked_checks", [])),
                    "missing_fill_count": execution_diag.get("summary", {}).get("missing_fill_count"),
                },
                "latest_run": latest_run,
                "alerts": {"rows": db_payload.get("recent_failures", []), "severity_counts": {"critical": db_payload.get("summary", {}).get("recent_failure_count", 0)}},
                "live": live,
                "execution_diagnostics": execution_diag,
                "orchestration": orchestration,
                "runs": [
                    {
                        "run_name": row.get("run_name"),
                        "status": row.get("status"),
                        "health_status": _bool_status(row.get("status")),
                        "schedule_type": row.get("mode") or row.get("run_type"),
                        "started_at": row.get("started_at"),
                        "failed_stage_count": 1 if row.get("status") == "failed" else 0,
                        "artifact_dir": row.get("artifact_dir"),
                        "source": row.get("source"),
                    }
                    for row in runs
                ],
                "orchestration_runs": self.recent_orchestration_runs(),
                "db_activity": db_payload,
                "source": "hybrid",
            }
        payload = super().ops_payload()
        payload["source"] = "artifact"
        return payload
