"""Tests for KalshiIngestionPipeline — database operations use SQLite in-memory."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from trading_platform.db.base import Base
from trading_platform.db.models.kalshi import KalshiMarketRecord, KalshiOrderBookSnapshot, KalshiTradeRecord
from trading_platform.kalshi.ingestion import KalshiIngestionPipeline
from trading_platform.kalshi.models import (
    KalshiMarket,
    KalshiOrderBook,
    KalshiOrderBookLevel,
    KalshiTrade,
)


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        yield s


@pytest.fixture()
def mock_client():
    return MagicMock()


@pytest.fixture()
def pipeline(mock_client, session):
    return KalshiIngestionPipeline(client=mock_client, session=session)


# ── ingest_markets ────────────────────────────────────────────────────────────

def test_ingest_markets_inserts_new(pipeline, mock_client, session):
    mock_client.get_all_markets.return_value = [
        KalshiMarket(
            ticker="FOO-24", title="Foo", subtitle=None, status="open",
            yes_bid="0.6500", yes_ask="0.6700", no_bid="0.3300", no_ask="0.3500",
            volume=500, open_interest=200, close_time=None,
            event_ticker="EVT-24", series_ticker="SER-24",
            category="politics", liquidity="100.00",
        )
    ]

    count = pipeline.ingest_markets(status="open")

    assert count == 1
    record = session.query(KalshiMarketRecord).filter_by(ticker="FOO-24").first()
    assert record is not None
    assert record.yes_bid == pytest.approx(0.65)
    assert record.status == "open"


def test_ingest_markets_upserts_existing(pipeline, mock_client, session):
    # First run
    mock_client.get_all_markets.return_value = [
        KalshiMarket(
            ticker="FOO-24", title="Foo", subtitle=None, status="open",
            yes_bid="0.6500", yes_ask=None, no_bid=None, no_ask=None,
            volume=100, open_interest=50, close_time=None,
            event_ticker=None, series_ticker=None, category=None, liquidity=None,
        )
    ]
    pipeline.ingest_markets()

    # Second run with updated volume
    mock_client.get_all_markets.return_value = [
        KalshiMarket(
            ticker="FOO-24", title="Foo", subtitle=None, status="open",
            yes_bid="0.7000", yes_ask=None, no_bid=None, no_ask=None,
            volume=999, open_interest=50, close_time=None,
            event_ticker=None, series_ticker=None, category=None, liquidity=None,
        )
    ]
    count = pipeline.ingest_markets()

    assert count == 1
    records = session.query(KalshiMarketRecord).filter_by(ticker="FOO-24").all()
    assert len(records) == 1  # still only one record
    assert records[0].volume == 999
    assert records[0].yes_bid == pytest.approx(0.70)


# ── ingest_trades ─────────────────────────────────────────────────────────────

def test_ingest_trades_inserts_new(pipeline, mock_client, session):
    mock_client.get_all_trades.return_value = [
        KalshiTrade(trade_id="t1", ticker="FOO-24", side="yes",
                    yes_price="0.6500", no_price="0.3500", count=10,
                    created_time="2024-01-15T12:00:00Z"),
        KalshiTrade(trade_id="t2", ticker="FOO-24", side="no",
                    yes_price="0.6200", no_price="0.3800", count=5,
                    created_time="2024-01-15T12:01:00Z"),
    ]

    count = pipeline.ingest_trades("FOO-24")

    assert count == 2
    trades = session.query(KalshiTradeRecord).filter_by(ticker="FOO-24").all()
    assert len(trades) == 2
    assert {t.trade_id for t in trades} == {"t1", "t2"}


def test_ingest_trades_deduplicates(pipeline, mock_client, session):
    mock_client.get_all_trades.return_value = [
        KalshiTrade(trade_id="t1", ticker="FOO-24", side="yes",
                    yes_price="0.65", no_price="0.35", count=10,
                    created_time="2024-01-15T12:00:00Z"),
    ]
    pipeline.ingest_trades("FOO-24")

    # Second call with same trade
    count = pipeline.ingest_trades("FOO-24")
    assert count == 0  # no new inserts

    total = session.query(KalshiTradeRecord).count()
    assert total == 1


# ── ingest_orderbook_snapshot ─────────────────────────────────────────────────

def test_ingest_orderbook_snapshot(pipeline, mock_client, session):
    mock_client.get_orderbook.return_value = KalshiOrderBook(
        ticker="FOO-24",
        yes_bids=[KalshiOrderBookLevel("0.6500", 50), KalshiOrderBookLevel("0.6400", 100)],
        no_bids=[KalshiOrderBookLevel("0.3400", 80)],
    )

    snapshot = pipeline.ingest_orderbook_snapshot("FOO-24", depth=5)

    assert snapshot.ticker == "FOO-24"
    assert snapshot.best_yes_bid == pytest.approx(0.65)
    assert snapshot.best_no_bid == pytest.approx(0.34)
    assert snapshot.mid_price is not None

    db_snap = session.query(KalshiOrderBookSnapshot).first()
    assert db_snap is not None
    assert "0.6500:50" in db_snap.yes_bids_raw
