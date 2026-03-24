from __future__ import annotations

from typing import Any

from sqlalchemy import BigInteger, ForeignKey, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Artifact(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "artifacts"
    __table_args__ = (UniqueConstraint("path", name="uq_artifacts_path"),)

    artifact_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    path: Mapped[str] = mapped_column(String(1024), nullable=False)
    format: Mapped[str | None] = mapped_column(String(32), index=True)
    content_hash: Mapped[str | None] = mapped_column(String(128), index=True)
    schema_version: Mapped[str | None] = mapped_column(String(64))
    row_count: Mapped[int | None] = mapped_column(BigInteger)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class RunArtifactLink(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "run_artifact_links"

    artifact_id: Mapped[Any] = mapped_column(ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False)
    research_run_id: Mapped[Any | None] = mapped_column(ForeignKey("research_runs.id", ondelete="CASCADE"))
    portfolio_run_id: Mapped[Any | None] = mapped_column(ForeignKey("portfolio_runs.id", ondelete="CASCADE"))
    role: Mapped[str | None] = mapped_column(String(128), index=True)

    artifact = relationship("Artifact")
    research_run = relationship("ResearchRun")
    portfolio_run = relationship("PortfolioRun")
