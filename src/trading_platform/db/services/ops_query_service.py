from __future__ import annotations

from trading_platform.db.services.decision_query_service import DecisionQueryService
from trading_platform.db.services.execution_query_service import ExecutionQueryService
from trading_platform.db.services.read_models import OpsHealthReadModel
from trading_platform.db.services.run_query_service import RunQueryService
from trading_platform.db.services.strategy_query_service import StrategyQueryService


class OpsQueryService:
    def __init__(self, session_factory) -> None:
        self.session_factory = session_factory
        self.run_queries = RunQueryService(session_factory)
        self.strategy_queries = StrategyQueryService(session_factory)
        self.execution_queries = ExecutionQueryService(session_factory)
        self.decision_queries = DecisionQueryService(session_factory)

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def get_ops_health_summary(self, *, run_limit: int = 10) -> dict:
        if not self.enabled:
            return {}
        research_runs = self.run_queries.list_recent_research_runs(limit=run_limit)
        portfolio_runs = self.run_queries.list_recent_portfolio_runs(limit=run_limit)
        combined = sorted(
            [*research_runs, *portfolio_runs],
            key=lambda row: (str(row.started_at or ""), 1 if row.run_kind == "portfolio" else 0),
            reverse=True,
        )
        latest = combined[0] if combined else None
        failures = [row.to_dict() for row in combined if row.status == "failed"][:10]
        promotions = self.strategy_queries.list_recent_promotions(limit=10)
        execution = self.execution_queries.list_recent_execution_activity(limit=10)
        activity = self.decision_queries.list_recent_trade_decisions(limit=10)
        summary = OpsHealthReadModel(
            latest_run_name=latest.run_name if latest else None,
            latest_run_status=latest.status if latest else None,
            latest_run_kind=latest.run_kind if latest else None,
            latest_run_started_at=latest.started_at if latest else None,
            latest_run_completed_at=latest.completed_at if latest else None,
            recent_failure_count=len([row for row in combined if row.status == "failed"]),
            research_run_count=len(research_runs),
            portfolio_run_count=len(portfolio_runs),
            recent_promotion_count=len(promotions),
            recent_execution_event_count=sum(row.event_count for row in execution),
        )
        return {
            "summary": summary.to_dict(),
            "research_runs": [row.to_dict() for row in research_runs],
            "portfolio_runs": [row.to_dict() for row in portfolio_runs],
            "recent_failures": failures,
            "recent_promotions": [row.to_dict() for row in promotions],
            "recent_execution_activity": [row.to_dict() for row in execution],
            "recent_trade_activity": [row.to_dict() for row in activity],
            "source": "db",
        }

    def get_recent_failures(self, limit: int = 20) -> list[dict]:
        payload = self.get_ops_health_summary(run_limit=limit)
        return list(payload.get("recent_failures", []))[:limit]

    def get_recent_db_backed_activity(self, limit: int = 20) -> dict:
        payload = self.get_ops_health_summary(run_limit=limit)
        return {
            "recent_trade_activity": list(payload.get("recent_trade_activity", []))[:limit],
            "recent_execution_activity": list(payload.get("recent_execution_activity", []))[:limit],
            "recent_promotions": list(payload.get("recent_promotions", []))[:limit],
            "source": "db",
        }
