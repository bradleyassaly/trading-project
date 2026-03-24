from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class PortfolioDecision(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "portfolio_decisions"

    portfolio_run_id: Mapped[Any] = mapped_column(ForeignKey("portfolio_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    symbol_id: Mapped[Any] = mapped_column(ForeignKey("symbols.id", ondelete="CASCADE"), nullable=False, index=True)
    side: Mapped[str | None] = mapped_column(String(16), index=True)
    target_weight: Mapped[float | None] = mapped_column(Float)
    target_shares: Mapped[int | None] = mapped_column(Integer)
    rank_score: Mapped[float | None] = mapped_column(Float, index=True)
    decision_status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    explanation_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    portfolio_run = relationship("PortfolioRun")
    symbol = relationship("Symbol")


class DecisionSignalContribution(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "decision_signal_contributions"

    portfolio_decision_id: Mapped[Any] = mapped_column(ForeignKey("portfolio_decisions.id", ondelete="CASCADE"), nullable=False, index=True)
    signal_name: Mapped[str | None] = mapped_column(String(255), index=True)
    contribution_value: Mapped[float | None] = mapped_column(Float)
    contribution_rank: Mapped[int | None] = mapped_column(Integer)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    portfolio_decision = relationship("PortfolioDecision")


class PositionSnapshot(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "position_snapshots"

    symbol_id: Mapped[Any] = mapped_column(ForeignKey("symbols.id", ondelete="CASCADE"), nullable=False, index=True)
    account: Mapped[str | None] = mapped_column(String(255), index=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    avg_price: Mapped[float | None] = mapped_column(Float)
    market_value: Mapped[float | None] = mapped_column(Float)
    as_of_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    source: Mapped[str | None] = mapped_column(String(128), index=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    symbol = relationship("Symbol")
