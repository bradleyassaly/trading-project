from __future__ import annotations

from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.models.artifact import RunArtifactLink
from trading_platform.db.models.runs import PortfolioRun, ResearchRun
from trading_platform.db.services.artifact_query_service import ArtifactQueryService
from trading_platform.db.services.read_models import RunDetailReadModel, RunSummaryReadModel


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


class RunQueryService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory
        self.artifact_queries = ArtifactQueryService(session_factory)

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def list_recent_research_runs(self, limit: int = 20) -> list[RunSummaryReadModel]:
        if not self.enabled:
            return []
        with self.session_factory() as session:
            artifact_counts = (
                select(RunArtifactLink.research_run_id, func.count(RunArtifactLink.id).label("artifact_count"))
                .where(RunArtifactLink.research_run_id.is_not(None))
                .group_by(RunArtifactLink.research_run_id)
                .subquery()
            )
            rows = session.execute(
                select(ResearchRun, func.coalesce(artifact_counts.c.artifact_count, 0))
                .outerjoin(artifact_counts, artifact_counts.c.research_run_id == ResearchRun.id)
                .order_by(ResearchRun.started_at.desc())
                .limit(limit)
            ).all()
        return [
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
        ]

    def list_recent_portfolio_runs(self, limit: int = 20) -> list[RunSummaryReadModel]:
        if not self.enabled:
            return []
        with self.session_factory() as session:
            artifact_counts = (
                select(RunArtifactLink.portfolio_run_id, func.count(RunArtifactLink.id).label("artifact_count"))
                .where(RunArtifactLink.portfolio_run_id.is_not(None))
                .group_by(RunArtifactLink.portfolio_run_id)
                .subquery()
            )
            rows = session.execute(
                select(PortfolioRun, func.coalesce(artifact_counts.c.artifact_count, 0))
                .outerjoin(artifact_counts, artifact_counts.c.portfolio_run_id == PortfolioRun.id)
                .order_by(PortfolioRun.started_at.desc())
                .limit(limit)
            ).all()
        return [
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
        ]

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
        return RunDetailReadModel(summary=summary, config_json=dict(row.config_json or {}), artifacts=artifacts)
