from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Float, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Order(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "orders"

    portfolio_decision_id: Mapped[Any | None] = mapped_column(ForeignKey("portfolio_decisions.id", ondelete="SET NULL"), index=True)
    broker: Mapped[str | None] = mapped_column(String(128), index=True)
    broker_order_id: Mapped[str | None] = mapped_column(String(255), index=True)
    symbol_id: Mapped[Any] = mapped_column(ForeignKey("symbols.id", ondelete="CASCADE"), nullable=False, index=True)
    side: Mapped[str | None] = mapped_column(String(16), index=True)
    order_type: Mapped[str | None] = mapped_column(String(32))
    tif: Mapped[str | None] = mapped_column(String(32))
    quantity: Mapped[float | None] = mapped_column(Float)
    limit_price: Mapped[float | None] = mapped_column(Float)
    stop_price: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at_event: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notes: Mapped[str | None] = mapped_column(Text)

    portfolio_decision = relationship("PortfolioDecision")
    symbol = relationship("Symbol")


class OrderEvent(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "order_events"

    order_id: Mapped[Any] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    order = relationship("Order")


class Fill(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "fills"

    order_id: Mapped[Any] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    fill_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float | None] = mapped_column(Float)
    fees: Mapped[float | None] = mapped_column(Float)
    liquidity_flag: Mapped[str | None] = mapped_column(String(64))
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    order = relationship("Order")
