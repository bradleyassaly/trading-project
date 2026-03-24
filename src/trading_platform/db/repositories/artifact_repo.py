from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from trading_platform.db.models.artifact import Artifact, RunArtifactLink


class ArtifactRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def register_artifact(self, *, artifact_type: str, path: str, format: str | None = None, content_hash: str | None = None, schema_version: str | None = None, row_count: int | None = None, metadata_json: dict[str, Any] | None = None) -> Artifact:
        row = self.session.scalar(select(Artifact).where(Artifact.path == path))
        if row is None:
            row = Artifact(artifact_type=artifact_type, path=path, format=format, content_hash=content_hash, schema_version=schema_version, row_count=row_count, metadata_json=dict(metadata_json or {}))
            self.session.add(row)
        else:
            row.artifact_type = artifact_type
            row.format = format
            row.content_hash = content_hash
            row.schema_version = schema_version
            row.row_count = row_count
            row.metadata_json = dict(metadata_json or row.metadata_json)
        self.session.flush()
        return row

    def link_artifact_to_run(self, *, artifact_id, research_run_id=None, portfolio_run_id=None, role: str | None = None) -> RunArtifactLink:
        row = RunArtifactLink(artifact_id=artifact_id, research_run_id=research_run_id, portfolio_run_id=portfolio_run_id, role=role)
        self.session.add(row)
        self.session.flush()
        return row
