from __future__ import annotations

from pathlib import Path
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trading_platform.db.models.artifact import Artifact, RunArtifactLink
from trading_platform.db.services.read_models import ArtifactReadModel


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


class ArtifactQueryService:
    def __init__(self, session_factory: sessionmaker[Session] | None) -> None:
        self.session_factory = session_factory

    @property
    def enabled(self) -> bool:
        return self.session_factory is not None

    def list_run_artifacts(
        self,
        *,
        research_run_id: str | None = None,
        portfolio_run_id: str | None = None,
    ) -> list[ArtifactReadModel]:
        if not self.enabled or (research_run_id is None and portfolio_run_id is None):
            return []
        with self.session_factory() as session:
            statement = (
                select(Artifact, RunArtifactLink)
                .join(RunArtifactLink, RunArtifactLink.artifact_id == Artifact.id)
                .order_by(Artifact.created_at.desc())
            )
            if research_run_id is not None:
                statement = statement.where(RunArtifactLink.research_run_id == _uuid_value(research_run_id))
            if portfolio_run_id is not None:
                statement = statement.where(RunArtifactLink.portfolio_run_id == _uuid_value(portfolio_run_id))
            rows = session.execute(statement).all()
        return [
            ArtifactReadModel(
                artifact_id=str(artifact.id),
                artifact_type=artifact.artifact_type,
                path=artifact.path,
                format=artifact.format,
                content_hash=artifact.content_hash,
                schema_version=artifact.schema_version,
                row_count=artifact.row_count,
                role=link.role,
                metadata=dict(artifact.metadata_json or {}),
                created_at=_iso(artifact.created_at),
            )
            for artifact, link in rows
        ]

    def latest_run_artifact_dir(
        self,
        *,
        research_run_id: str | None = None,
        portfolio_run_id: str | None = None,
    ) -> str | None:
        artifacts = self.list_run_artifacts(research_run_id=research_run_id, portfolio_run_id=portfolio_run_id)
        if not artifacts:
            return None
        try:
            return str(Path(artifacts[0].path).parent)
        except (TypeError, ValueError):
            return None
