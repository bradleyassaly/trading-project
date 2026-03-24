from __future__ import annotations

from trading_platform.db.services.decision_query_service import DecisionQueryService
from trading_platform.db.services.execution_query_service import ExecutionQueryService
from trading_platform.db.services.read_models import DecisionQueryFilters, OpsActivityFilters, OpsHealthReadModel, RunQueryFilters, StrategyHistoryFilters
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

    def get_ops_health_summary(self, *, filters: OpsActivityFilters | None = None, run_limit: int | None = None) -> dict:
        filters = filters or OpsActivityFilters(limit=run_limit or 20)
        if not self.enabled:
            return {}
        research_runs = self.run_queries.list_research_runs(
            RunQueryFilters(status=filters.status, date_from=filters.date_from, date_to=filters.date_to, limit=filters.limit, offset=filters.offset)
        )
        portfolio_runs = self.run_queries.list_portfolio_runs(
            RunQueryFilters(status=filters.status, date_from=filters.date_from, date_to=filters.date_to, limit=filters.limit, offset=filters.offset)
        )
        combined = sorted(
            [*research_runs.items, *portfolio_runs.items],
            key=lambda row: (str(row.started_at or ""), 1 if row.run_kind == "portfolio" else 0),
            reverse=True,
        )
        latest = combined[0] if combined else None
        failures = [row.to_dict() for row in combined if row.status == "failed"][:10]
        promotions = self.strategy_queries.list_promotions(
            StrategyHistoryFilters(status=filters.status, date_from=filters.date_from, date_to=filters.date_to, limit=filters.limit, offset=filters.offset)
        )
        execution = self.execution_queries.list_recent_execution_activity(limit=filters.limit)
        activity = self.decision_queries.list_trade_decisions(
            DecisionQueryFilters(decision_status=filters.status, date_from=filters.date_from, date_to=filters.date_to, limit=filters.limit, offset=filters.offset)
        )
        activity_type = (filters.activity_type or "all").lower()
        activity_feed: list[dict] = []
        if activity_type in {"all", "trades"}:
            activity_feed.extend([{**row.to_dict(), "activity_type": "trade"} for row in activity.items])
        if activity_type in {"all", "promotions"}:
            activity_feed.extend([{**row.to_dict(), "activity_type": "promotion"} for row in promotions.items])
        if activity_type in {"all", "execution"}:
            activity_feed.extend([{**row.to_dict(), "activity_type": "execution"} for row in execution])
        activity_feed.sort(key=lambda row: str(row.get("timestamp") or row.get("created_at") or row.get("submitted_at") or ""), reverse=True)
        activity_feed = activity_feed[filters.offset : filters.offset + filters.limit]
        summary = OpsHealthReadModel(
            latest_run_name=latest.run_name if latest else None,
            latest_run_status=latest.status if latest else None,
            latest_run_kind=latest.run_kind if latest else None,
            latest_run_started_at=latest.started_at if latest else None,
            latest_run_completed_at=latest.completed_at if latest else None,
            recent_failure_count=len([row for row in combined if row.status == "failed"]),
            research_run_count=research_runs.total_count,
            portfolio_run_count=portfolio_runs.total_count,
            recent_promotion_count=promotions.total_count,
            recent_execution_event_count=sum(row.event_count for row in execution),
        )
        return {
            "summary": summary.to_dict(),
            "research_runs": research_runs.to_dict(),
            "portfolio_runs": portfolio_runs.to_dict(),
            "recent_failures": failures,
            "recent_promotions": promotions.to_dict(),
            "recent_execution_activity": [row.to_dict() for row in execution],
            "recent_trade_activity": activity.to_dict(),
            "activity_feed": {
                "items": activity_feed,
                "limit": filters.limit,
                "offset": filters.offset,
                "has_more": len(activity_feed) == filters.limit,
                "source": "db",
            },
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
