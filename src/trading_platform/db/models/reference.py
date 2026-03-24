from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import Boolean, Date, ForeignKey, Index, JSON, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Symbol(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "symbols"
    __table_args__ = (UniqueConstraint("symbol", name="uq_symbols_symbol"),)

    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String(255))
    asset_type: Mapped[str | None] = mapped_column(String(64))
    exchange: Mapped[str | None] = mapped_column(String(64))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Universe(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "universes"
    __table_args__ = (UniqueConstraint("universe_id", name="uq_universes_universe_id"),)

    universe_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(String(500))


class UniverseMembership(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "universe_memberships"
    __table_args__ = (
        UniqueConstraint("universe_id", "symbol_id", "start_date", "end_date", name="uq_universe_membership_window"),
        Index("ix_universe_memberships_lookup", "universe_id", "symbol_id", "start_date", "end_date"),
    )

    universe_id: Mapped[Any] = mapped_column(ForeignKey("universes.id", ondelete="CASCADE"), nullable=False)
    symbol_id: Mapped[Any] = mapped_column(ForeignKey("symbols.id", ondelete="CASCADE"), nullable=False)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    membership_status: Mapped[str] = mapped_column(String(64), nullable=False, default="member")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    universe = relationship("Universe")
    symbol = relationship("Symbol")
