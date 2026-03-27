from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class ResearchRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "research_runs"
    __table_args__ = (UniqueConstraint("run_key", name="uq_research_runs_run_key"),)

    run_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    run_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    config_hash: Mapped[str | None] = mapped_column(String(128), index=True)
    git_commit: Mapped[str | None] = mapped_column(String(64), index=True)
    artifacts_root: Mapped[str | None] = mapped_column(Text)
    output_dir: Mapped[str | None] = mapped_column(Text)
    universe: Mapped[str | None] = mapped_column(String(255), index=True)
    config_path: Mapped[str | None] = mapped_column(Text)
    composite_runtime_computability_pass: Mapped[bool | None] = mapped_column(Boolean)
    composite_runtime_computability_reason: Mapped[str | None] = mapped_column(String(255))
    composite_runtime_computable_symbol_count: Mapped[int | None] = mapped_column(Integer)
    notes: Mapped[str | None] = mapped_column(Text)


class PortfolioRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "portfolio_runs"
    __table_args__ = (UniqueConstraint("run_key", name="uq_portfolio_runs_run_key"),)

    run_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    mode: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    config_hash: Mapped[str | None] = mapped_column(String(128), index=True)
    git_commit: Mapped[str | None] = mapped_column(String(64), index=True)
    notes: Mapped[str | None] = mapped_column(Text)
