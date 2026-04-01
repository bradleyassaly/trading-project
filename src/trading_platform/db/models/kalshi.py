"""
SQLAlchemy ORM models for Kalshi prediction market data.

After adding these models, generate an Alembic migration:
    alembic revision --autogenerate -m "add kalshi tables"
    alembic upgrade head
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from trading_platform.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class KalshiMarketRecord(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """
    Snapshot of a Kalshi market's metadata and latest pricing.
    Upserted on each ingestion run.
    """
    __tablename__ = "kalshi_markets"

    ticker: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    title: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str | None] = mapped_column(String(32), index=True)
    event_ticker: Mapped[str | None] = mapped_column(String(128), index=True)
    series_ticker: Mapped[str | None] = mapped_column(String(128), index=True)
    category: Mapped[str | None] = mapped_column(String(128))
    close_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    volume: Mapped[int | None] = mapped_column(Integer)
    open_interest: Mapped[int | None] = mapped_column(Integer)
    yes_bid: Mapped[float | None] = mapped_column(Float)   # best YES bid as float (0–1)
    yes_ask: Mapped[float | None] = mapped_column(Float)   # best YES ask as float (0–1)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class KalshiTradeRecord(UUIDPrimaryKeyMixin, Base):
    """
    Individual trade (fill) that occurred on a Kalshi market.
    Immutable — only inserted, never updated.
    """
    __tablename__ = "kalshi_trades"

    trade_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)  # yes | no (taker side)
    yes_price: Mapped[float | None] = mapped_column(Float)        # 0.0–1.0
    no_price: Mapped[float | None] = mapped_column(Float)         # 0.0–1.0
    count: Mapped[int] = mapped_column(Integer, nullable=False)
    traded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)

    __table_args__ = (
        Index("ix_kalshi_trades_ticker_traded_at", "ticker", "traded_at"),
    )


class KalshiOrderBookSnapshot(UUIDPrimaryKeyMixin, Base):
    """
    Point-in-time snapshot of a market's order book.
    Appended on every polling cycle for time-series analysis.
    """
    __tablename__ = "kalshi_orderbook_snapshots"

    ticker: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    # Serialized as "price:qty,price:qty,..." (CSV of levels, best first)
    yes_bids_raw: Mapped[str | None] = mapped_column(Text)
    no_bids_raw: Mapped[str | None] = mapped_column(Text)
    best_yes_bid: Mapped[float | None] = mapped_column(Float)
    best_no_bid: Mapped[float | None] = mapped_column(Float)
    mid_price: Mapped[float | None] = mapped_column(Float)

    __table_args__ = (
        Index("ix_kalshi_ob_ticker_ts", "ticker", "snapshot_at"),
    )
