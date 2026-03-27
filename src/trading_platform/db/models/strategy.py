from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class StrategyDefinition(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "strategy_definitions"
    __table_args__ = (UniqueConstraint("name", "version", name="uq_strategy_definitions_name_version"),)

    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    version: Mapped[str] = mapped_column(String(128), nullable=False)
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    code_hash: Mapped[str | None] = mapped_column(String(128), index=True)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)


class PromotionDecision(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "promotion_decisions"

    strategy_definition_id: Mapped[Any] = mapped_column(ForeignKey("strategy_definitions.id", ondelete="CASCADE"), nullable=False)
    source_research_run_id: Mapped[Any | None] = mapped_column(ForeignKey("research_runs.id", ondelete="SET NULL"))
    decision: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    reason: Mapped[str | None] = mapped_column(Text)
    metrics_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    strategy_definition = relationship("StrategyDefinition")
    source_research_run = relationship("ResearchRun")


class PromotedStrategy(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "promoted_strategies"

    strategy_definition_id: Mapped[Any] = mapped_column(ForeignKey("strategy_definitions.id", ondelete="CASCADE"), nullable=False)
    promotion_decision_id: Mapped[Any | None] = mapped_column(ForeignKey("promotion_decisions.id", ondelete="SET NULL"))
    research_run_id: Mapped[Any | None] = mapped_column(ForeignKey("research_runs.id", ondelete="SET NULL"), index=True)
    candidate_id: Mapped[str | None] = mapped_column(String(255), index=True)
    preset_name: Mapped[str | None] = mapped_column(String(255), index=True)
    signal_family: Mapped[str | None] = mapped_column(String(128), index=True)
    strategy_name: Mapped[str | None] = mapped_column(String(255))
    ranking_metric: Mapped[str | None] = mapped_column(String(128))
    ranking_value: Mapped[float | None] = mapped_column(Float)
    validation_status: Mapped[str | None] = mapped_column(String(64))
    promotion_variant: Mapped[str | None] = mapped_column(String(64))
    condition_id: Mapped[str | None] = mapped_column(String(255), index=True)
    condition_type: Mapped[str | None] = mapped_column(String(128), index=True)
    rationale: Mapped[str | None] = mapped_column(Text)
    runtime_score_validation_pass: Mapped[bool | None] = mapped_column(Boolean)
    runtime_score_validation_reason: Mapped[str | None] = mapped_column(String(255))
    runtime_computable_symbol_count: Mapped[int | None] = mapped_column(Integer)
    promotion_timestamp_text: Mapped[str | None] = mapped_column(String(64))
    generated_preset_path: Mapped[str | None] = mapped_column(Text)
    generated_registry_path: Mapped[str | None] = mapped_column(Text)
    generated_pipeline_config_path: Mapped[str | None] = mapped_column(Text)
    active_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    active_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    strategy_definition = relationship("StrategyDefinition")
    promotion_decision = relationship("PromotionDecision")
    research_run = relationship("ResearchRun")
