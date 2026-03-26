from __future__ import annotations

from typing import Any

from sqlalchemy import Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class SignalCandidate(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "signal_candidates"
    __table_args__ = (UniqueConstraint("research_run_id", "candidate_id", name="uq_signal_candidates_run_candidate"),)

    research_run_id: Mapped[Any] = mapped_column(ForeignKey("research_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    candidate_name: Mapped[str] = mapped_column(String(255), nullable=False)
    signal_family: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    signal_variant: Mapped[str | None] = mapped_column(String(128), index=True)
    lookback: Mapped[int | None] = mapped_column(Integer)
    horizon: Mapped[int | None] = mapped_column(Integer)
    variant_parameters_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    research_run = relationship("ResearchRun")


class SignalMetric(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "signal_metrics"
    __table_args__ = (UniqueConstraint("research_run_id", "candidate_id", name="uq_signal_metrics_run_candidate"),)

    research_run_id: Mapped[Any] = mapped_column(ForeignKey("research_runs.id", ondelete="CASCADE"), nullable=False, index=True)
    candidate_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    folds_tested: Mapped[int | None] = mapped_column(Integer)
    symbols_tested: Mapped[int | None] = mapped_column(Integer)
    mean_dates_evaluated: Mapped[float | None] = mapped_column(Float)
    mean_pearson_ic: Mapped[float | None] = mapped_column(Float)
    mean_spearman_ic: Mapped[float | None] = mapped_column(Float)
    mean_hit_rate: Mapped[float | None] = mapped_column(Float)
    mean_long_short_spread: Mapped[float | None] = mapped_column(Float)
    mean_quantile_spread: Mapped[float | None] = mapped_column(Float)
    mean_turnover: Mapped[float | None] = mapped_column(Float)
    worst_fold_spearman_ic: Mapped[float | None] = mapped_column(Float)
    total_obs: Mapped[int | None] = mapped_column(Integer)
    rejection_reason: Mapped[str | None] = mapped_column(Text)
    promotion_status: Mapped[str | None] = mapped_column(String(64), index=True)

    research_run = relationship("ResearchRun")
