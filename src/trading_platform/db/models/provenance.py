from __future__ import annotations

from typing import Any

from sqlalchemy import Float, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class CandidateEvaluation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "candidate_evaluations"

    portfolio_run_id: Mapped[Any] = mapped_column(ForeignKey("portfolio_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    symbol_id: Mapped[Any] = mapped_column(ForeignKey("symbols.id", ondelete="CASCADE"), nullable=False, index=True)
    base_universe_id: Mapped[str | None] = mapped_column(String(255), index=True)
    sub_universe_id: Mapped[str | None] = mapped_column(String(255), index=True)
    score: Mapped[float | None] = mapped_column(Float, index=True)
    rank: Mapped[int | None] = mapped_column(Integer, index=True)
    candidate_status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    rejection_reason: Mapped[str | None] = mapped_column(String(255))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    portfolio_run = relationship("PortfolioRun")
    symbol = relationship("Symbol")


class UniverseFilterResult(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "universe_filter_results"

    portfolio_run_id: Mapped[Any | None] = mapped_column(ForeignKey("portfolio_runs.id", ondelete="SET NULL"), index=True)
    symbol_id: Mapped[Any] = mapped_column(ForeignKey("symbols.id", ondelete="CASCADE"), nullable=False, index=True)
    filter_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    pass_fail: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    observed_value: Mapped[str | None] = mapped_column(String(255))
    reason: Mapped[str | None] = mapped_column(String(255))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    portfolio_run = relationship("PortfolioRun")
    symbol = relationship("Symbol")
