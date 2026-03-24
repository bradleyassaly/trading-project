from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.models.artifact import RunArtifactLink
from trading_platform.db.models.runs import PortfolioRun, ResearchRun
from trading_platform.db.services.artifact_query_service import ArtifactQueryService
from trading_platform.db.services.read_models import PagedResultReadModel, RunDetailReadModel, RunQueryFilters, RunSummaryReadModel


def _iso(value: object) -> str | None:
    if value is None:
        return None
    text = getattr(value, "isoformat", None)
    return text() if callable(text) else str(value)


def _uuid_value(value: object) -> object:
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            return value
    return value


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        text = value.replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except ValueError:
        return None


class RunQueryService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory
        self.artifact_queries = ArtifactQueryService(session_factory)

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def list_research_runs(self, filters: RunQueryFilters | None = None) -> PagedResultReadModel:
        filters = filters or RunQueryFilters()
        if not self.enabled:
            return PagedResultReadModel(items=[], total_count=0, limit=filters.limit, offset=filters.offset, source="db")
        with self.session_factory() as session:
            artifact_counts = (
                select(RunArtifactLink.research_run_id, func.count(RunArtifactLink.id).label("artifact_count"))
                .where(RunArtifactLink.research_run_id.is_not(None))
                .group_by(RunArtifactLink.research_run_id)
                .subquery()
            )
            statement = (
                select(ResearchRun, func.coalesce(artifact_counts.c.artifact_count, 0))
                .outerjoin(artifact_counts, artifact_counts.c.research_run_id == ResearchRun.id)
            )
            count_statement = select(func.count(ResearchRun.id))
            if filters.status:
                statement = statement.where(ResearchRun.status == filters.status)
                count_statement = count_statement.where(ResearchRun.status == filters.status)
            if filters.run_type:
                statement = statement.where(ResearchRun.run_type == filters.run_type)
                count_statement = count_statement.where(ResearchRun.run_type == filters.run_type)
            date_from = _parse_dt(filters.date_from)
            if date_from is not None:
                statement = statement.where(ResearchRun.started_at >= date_from)
                count_statement = count_statement.where(ResearchRun.started_at >= date_from)
            date_to = _parse_dt(filters.date_to)
            if date_to is not None:
                statement = statement.where(ResearchRun.started_at <= date_to)
                count_statement = count_statement.where(ResearchRun.started_at <= date_to)
            order_column = ResearchRun.started_at.desc() if filters.sort_desc else ResearchRun.started_at.asc()
            rows = session.execute(statement.order_by(order_column).offset(filters.offset).limit(filters.limit)).all()
            total_count = int(session.scalar(count_statement) or 0)
        return PagedResultReadModel(
            items=[
                RunSummaryReadModel(
                    run_id=str(run.id),
                    run_kind="research",
                    run_name=run.run_key,
                    status=run.status,
                    started_at=_iso(run.started_at),
                    completed_at=_iso(run.completed_at),
                    run_type=run.run_type,
                    config_hash=run.config_hash,
                    git_commit=run.git_commit,
                    notes=run.notes,
                    artifact_count=int(artifact_count or 0),
                    artifact_dir=self.artifact_queries.latest_run_artifact_dir(research_run_id=run.id),
                )
                for run, artifact_count in rows
            ],
            total_count=total_count,
            limit=filters.limit,
            offset=filters.offset,
            source="db",
        )

    def list_recent_research_runs(self, limit: int = 20) -> list[RunSummaryReadModel]:
        return list(self.list_research_runs(RunQueryFilters(limit=limit)).items)

    def list_portfolio_runs(self, filters: RunQueryFilters | None = None) -> PagedResultReadModel:
        filters = filters or RunQueryFilters()
        if not self.enabled:
            return PagedResultReadModel(items=[], total_count=0, limit=filters.limit, offset=filters.offset, source="db")
        with self.session_factory() as session:
            artifact_counts = (
                select(RunArtifactLink.portfolio_run_id, func.count(RunArtifactLink.id).label("artifact_count"))
                .where(RunArtifactLink.portfolio_run_id.is_not(None))
                .group_by(RunArtifactLink.portfolio_run_id)
                .subquery()
            )
            statement = (
                select(PortfolioRun, func.coalesce(artifact_counts.c.artifact_count, 0))
                .outerjoin(artifact_counts, artifact_counts.c.portfolio_run_id == PortfolioRun.id)
            )
            count_statement = select(func.count(PortfolioRun.id))
            if filters.status:
                statement = statement.where(PortfolioRun.status == filters.status)
                count_statement = count_statement.where(PortfolioRun.status == filters.status)
            if filters.mode:
                statement = statement.where(PortfolioRun.mode == filters.mode)
                count_statement = count_statement.where(PortfolioRun.mode == filters.mode)
            if filters.strategy:
                statement = statement.where(PortfolioRun.run_key.ilike(f"%{filters.strategy}%"))
                count_statement = count_statement.where(PortfolioRun.run_key.ilike(f"%{filters.strategy}%"))
            date_from = _parse_dt(filters.date_from)
            if date_from is not None:
                statement = statement.where(PortfolioRun.started_at >= date_from)
                count_statement = count_statement.where(PortfolioRun.started_at >= date_from)
            date_to = _parse_dt(filters.date_to)
            if date_to is not None:
                statement = statement.where(PortfolioRun.started_at <= date_to)
                count_statement = count_statement.where(PortfolioRun.started_at <= date_to)
            order_column = PortfolioRun.started_at.desc() if filters.sort_desc else PortfolioRun.started_at.asc()
            rows = session.execute(statement.order_by(order_column).offset(filters.offset).limit(filters.limit)).all()
            total_count = int(session.scalar(count_statement) or 0)
        return PagedResultReadModel(
            items=[
                RunSummaryReadModel(
                    run_id=str(run.id),
                    run_kind="portfolio",
                    run_name=run.run_key,
                    status=run.status,
                    started_at=_iso(run.started_at),
                    completed_at=_iso(run.completed_at),
                    mode=run.mode,
                    config_hash=run.config_hash,
                    git_commit=run.git_commit,
                    notes=run.notes,
                    artifact_count=int(artifact_count or 0),
                    artifact_dir=self.artifact_queries.latest_run_artifact_dir(portfolio_run_id=run.id),
                )
                for run, artifact_count in rows
            ],
            total_count=total_count,
            limit=filters.limit,
            offset=filters.offset,
            source="db",
        )

    def list_recent_portfolio_runs(self, limit: int = 20) -> list[RunSummaryReadModel]:
        return list(self.list_portfolio_runs(RunQueryFilters(limit=limit)).items)

    def get_run_detail(
        self,
        run_id: str,
        *,
        run_kind: Literal["research", "portfolio"] | None = None,
    ) -> RunDetailReadModel | None:
        if not self.enabled:
            return None
        with self.session_factory() as session:
            row = None
            resolved_kind = run_kind
            if resolved_kind in (None, "portfolio"):
                row = session.get(PortfolioRun, _uuid_value(run_id))
                if row is not None:
                    resolved_kind = "portfolio"
            if row is None and resolved_kind in (None, "research"):
                row = session.get(ResearchRun, _uuid_value(run_id))
                if row is not None:
                    resolved_kind = "research"
        if row is None or resolved_kind is None:
            return None
        artifacts = self.artifact_queries.list_run_artifacts(
            research_run_id=run_id if resolved_kind == "research" else None,
            portfolio_run_id=run_id if resolved_kind == "portfolio" else None,
        )
        summary = RunSummaryReadModel(
            run_id=str(row.id),
            run_kind=resolved_kind,
            run_name=row.run_key,
            status=row.status,
            started_at=_iso(row.started_at),
            completed_at=_iso(row.completed_at),
            mode=getattr(row, "mode", None),
            run_type=getattr(row, "run_type", None),
            config_hash=row.config_hash,
            git_commit=row.git_commit,
            notes=row.notes,
            artifact_count=len(artifacts),
            artifact_dir=self.artifact_queries.latest_run_artifact_dir(
                research_run_id=run_id if resolved_kind == "research" else None,
                portfolio_run_id=run_id if resolved_kind == "portfolio" else None,
            ),
        )
        detail = RunDetailReadModel(summary=summary, config_json=dict(row.config_json or {}), artifacts=artifacts)
        payload = detail.to_dict()
        if resolved_kind == "portfolio":
            from trading_platform.db.services.decision_query_service import DecisionQueryService

            decision_queries = DecisionQueryService(self.session_factory)
            decisions = decision_queries.list_trade_decisions(
                filters=decision_queries.default_filters(run_id=str(row.id), limit=20)
            )
            candidate_page = decision_queries.list_candidate_evaluations_for_run(
                str(row.id),
                limit=20,
                offset=0,
            )
            payload["linked_decisions"] = decisions.to_dict()
            payload["candidate_evaluations"] = candidate_page.to_dict()
            payload["decision_summary"] = {
                "decision_count": decisions.total_count,
                "candidate_count": candidate_page.total_count,
                "selected_count": len([item for item in decisions.items if item.status == "selected"]),
            }
            return payload
        from trading_platform.db.services.strategy_query_service import StrategyQueryService

        promotions = StrategyQueryService(self.session_factory).list_promotions_for_research_run(str(row.id), limit=20, offset=0)
        payload["linked_promotions"] = promotions.to_dict()
        payload["decision_summary"] = {"promotion_count": promotions.total_count}
        return payload
