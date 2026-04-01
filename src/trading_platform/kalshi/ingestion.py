"""
Kalshi data ingestion pipeline.

Fetches market data from the Kalshi REST API and persists it to the
local database (SQLAlchemy session).

Usage::

    from trading_platform.db.session import get_session
    from trading_platform.kalshi.auth import KalshiConfig
    from trading_platform.kalshi.client import KalshiClient
    from trading_platform.kalshi.ingestion import KalshiIngestionPipeline

    config = KalshiConfig.from_env()
    client = KalshiClient(config)

    with get_session() as session:
        pipeline = KalshiIngestionPipeline(client, session)
        pipeline.ingest_markets(status="open")
        pipeline.ingest_trades("SOME-TICKER-23DEC29")
        pipeline.ingest_orderbook_snapshot("SOME-TICKER-23DEC29")
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from trading_platform.db.models.kalshi import (
    KalshiMarketRecord,
    KalshiOrderBookSnapshot,
    KalshiTradeRecord,
)
from trading_platform.kalshi.client import KalshiClient

logger = logging.getLogger(__name__)


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _serialize_levels(levels: list[Any]) -> str:
    return ",".join(f"{lvl.price}:{lvl.quantity}" for lvl in levels)


class KalshiIngestionPipeline:
    """
    Ingests Kalshi market data into the local database.

    All methods are idempotent — safe to run repeatedly as cron jobs.
    """

    def __init__(self, client: KalshiClient, session: Session) -> None:
        self.client = client
        self.session = session

    # ── Markets ───────────────────────────────────────────────────────────────

    def ingest_markets(self, status: str | None = "open") -> int:
        """
        Fetch all markets (filtered by status) and upsert into kalshi_markets.

        :param status: "open", "closed", "settled", or None for all.
        :returns: Number of records upserted.
        """
        logger.info("Ingesting Kalshi markets (status=%s)...", status)
        markets = self.client.get_all_markets(status=status)
        now = datetime.now(UTC)
        upserted = 0

        for market in markets:
            existing = (
                self.session.query(KalshiMarketRecord)
                .filter_by(ticker=market.ticker)
                .first()
            )
            if existing is None:
                record = KalshiMarketRecord(
                    ticker=market.ticker,
                    title=market.title,
                    status=market.status,
                    event_ticker=market.event_ticker,
                    series_ticker=market.series_ticker,
                    category=market.category,
                    volume=market.volume,
                    open_interest=market.open_interest,
                    close_time=_parse_iso(market.close_time),
                    yes_bid=_to_float(market.yes_bid),
                    yes_ask=_to_float(market.yes_ask),
                    last_refreshed_at=now,
                )
                self.session.add(record)
            else:
                existing.status = market.status
                existing.volume = market.volume
                existing.open_interest = market.open_interest
                existing.yes_bid = _to_float(market.yes_bid)
                existing.yes_ask = _to_float(market.yes_ask)
                existing.last_refreshed_at = now
            upserted += 1

        self.session.commit()
        logger.info("Upserted %d market records.", upserted)
        return upserted

    # ── Trades ────────────────────────────────────────────────────────────────

    def ingest_trades(
        self,
        ticker: str,
        min_ts: int | None = None,
        max_ts: int | None = None,
    ) -> int:
        """
        Fetch all historical trades for a ticker and insert new records.
        Existing trade_ids are skipped (deduplication).

        :param ticker: Market ticker.
        :param min_ts: Optional Unix timestamp (seconds) lower bound.
        :param max_ts: Optional Unix timestamp (seconds) upper bound.
        :returns: Number of new records inserted.
        """
        logger.info("Ingesting trades for %s (min_ts=%s, max_ts=%s)...", ticker, min_ts, max_ts)
        trades = self.client.get_all_trades(ticker=ticker, min_ts=min_ts, max_ts=max_ts)

        existing_ids: set[str] = {
            row[0]
            for row in self.session.query(KalshiTradeRecord.trade_id)
            .filter(KalshiTradeRecord.ticker == ticker)
            .all()
        }

        new_records: list[KalshiTradeRecord] = []
        for trade in trades:
            if trade.trade_id in existing_ids:
                continue
            new_records.append(
                KalshiTradeRecord(
                    trade_id=trade.trade_id,
                    ticker=trade.ticker,
                    side=trade.side,
                    yes_price=_to_float(trade.yes_price),
                    no_price=_to_float(trade.no_price),
                    count=trade.count,
                    traded_at=_parse_iso(trade.created_time),
                )
            )

        if new_records:
            self.session.add_all(new_records)
            self.session.commit()

        logger.info("Inserted %d new trades for %s.", len(new_records), ticker)
        return len(new_records)

    # ── Order Book Snapshots ──────────────────────────────────────────────────

    def ingest_orderbook_snapshot(self, ticker: str, depth: int = 10) -> KalshiOrderBookSnapshot:
        """
        Fetch and persist a single orderbook snapshot for a ticker.

        :param ticker: Market ticker.
        :param depth:  Number of price levels to request (0–100).
        :returns: The persisted KalshiOrderBookSnapshot record.
        """
        ob = self.client.get_orderbook(ticker=ticker, depth=depth)
        now = datetime.now(UTC)

        best_yes = ob.best_yes_bid
        best_no = ob.best_no_bid
        mid = ob.mid_price

        snapshot = KalshiOrderBookSnapshot(
            ticker=ticker,
            snapshot_at=now,
            yes_bids_raw=_serialize_levels(ob.yes_bids),
            no_bids_raw=_serialize_levels(ob.no_bids),
            best_yes_bid=best_yes,
            best_no_bid=best_no,
            mid_price=mid,
        )
        self.session.add(snapshot)
        self.session.commit()
        logger.debug("Snapshot for %s: yes=%.4f no=%.4f mid=%.4f", ticker, best_yes or 0, best_no or 0, mid or 0)
        return snapshot
