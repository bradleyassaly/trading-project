"""
Wallet Trade Poller — the missing link between wallet intelligence and live signals.

The CLOB WebSocket only streams market-level price/book data.  It does NOT
report which wallet placed a trade.  This poller bridges the gap by
periodically checking the Polymarket Data API for recent trades by our
copyable wallets, then feeding them through the existing signal engine
pipeline (alpha gate → hypothesis → paper trade → Telegram alert).

Usage::

    # CLI
    trading-cli data polymarket poll-wallet-trades

    # Scheduler (every 5 minutes)
    Task(name="wallet_trade_poller", cmd="trading-cli data polymarket poll-wallet-trades", ...)

    # Programmatic
    from trading_platform.polymarket.wallet_trade_poller import WalletTradePoller
    poller = WalletTradePoller()
    poller.run()

Rate limiting: 56 wallets × 0.5s delay ≈ 30s per cycle.  At every 5 minutes
that's 56 API calls per 5 min — well within Polymarket's limits.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db import connect_wallet_db

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = str(_PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db")

# ── DB schema for poll state persistence ────────────────────────────────────

_POLL_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_poll_state (
    wallet TEXT PRIMARY KEY,
    last_checked_ts INTEGER NOT NULL DEFAULT 0,
    last_trade_hash TEXT,
    trades_found INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS whale_exit_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER,
    wallet TEXT,
    condition_id TEXT,
    exit_price REAL,
    exit_side TEXT,
    exit_ts INTEGER,
    our_entry_price REAL,
    unrealized_at_exit REAL,
    action_taken TEXT DEFAULT 'HOLD',
    detected_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_wes_trade ON whale_exit_signals(trade_id);
CREATE TABLE IF NOT EXISTS stale_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT,
    condition_id TEXT,
    market_title TEXT,
    side TEXT,
    price REAL,
    size REAL,
    trade_ts INTEGER,
    detected_at INTEGER,
    age_seconds INTEGER,
    reason TEXT,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS dry_run_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_wallet TEXT,
    signal_type TEXT,
    token_id TEXT,
    market_title TEXT,
    category TEXT,
    side TEXT,
    intended_price REAL,
    intended_size REAL,
    intended_stake REAL,
    alpha_score REAL,
    resolution_price REAL,
    would_have_pnl REAL,
    spread_at_signal REAL,
    created_at INTEGER
);
"""

DATA_API_BASE = "https://data-api.polymarket.com"
REQUEST_DELAY = 0.5  # seconds between API calls
STALE_SIGNAL_THRESHOLD = 600  # 10 minutes — don't trade signals older than this


@dataclass
class PolledTrade:
    """A trade fetched from the Data API, normalised to our field names."""
    wallet: str
    condition_id: str
    side: str           # BUY or SELL
    price: float
    size: float
    outcome: str        # YES or NO
    title: str
    category: str
    timestamp: int
    tx_hash: str
    slug: str


class WalletTradePoller:
    """Polls Polymarket Data API for recent trades by copyable wallets."""

    def __init__(self, db_path: str | None = None, dry_run: bool = False) -> None:
        self.db_path = db_path or _DEFAULT_DB
        self.dry_run = dry_run
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        try:
            conn = connect_wallet_db(self.db_path)
            try:
                conn.executescript(_POLL_STATE_SCHEMA)
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("wallet_poll_state schema creation failed: %s", exc)

    # ── Load copyable wallets ───────────────────────────────────────────────

    def _get_copyable_wallets(self) -> list[dict[str, Any]]:
        """Get wallets that are copyable in at least one category."""
        try:
            conn = connect_wallet_db(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    """SELECT DISTINCT wallet,
                              GROUP_CONCAT(category) AS categories
                       FROM wallet_alpha_scores
                       WHERE is_copyable = 1
                       GROUP BY wallet"""
                ).fetchall()
                return [dict(r) for r in rows]
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("Failed to load copyable wallets: %s", exc)
            return []

    def _get_last_checked(self) -> dict[str, int]:
        """Load last-checked timestamps from DB."""
        try:
            conn = connect_wallet_db(self.db_path)
            try:
                rows = conn.execute(
                    "SELECT wallet, last_checked_ts FROM wallet_poll_state"
                ).fetchall()
                return {r[0]: r[1] for r in rows}
            finally:
                conn.close()
        except Exception:
            return {}

    def _update_last_checked(self, wallet: str, ts: int, trades_found: int,
                             last_hash: str = "") -> None:
        """Persist the last-checked timestamp for a wallet."""
        try:
            conn = connect_wallet_db(self.db_path)
            try:
                conn.execute(
                    """INSERT INTO wallet_poll_state
                       (wallet, last_checked_ts, last_trade_hash, trades_found, updated_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(wallet) DO UPDATE SET
                           last_checked_ts = excluded.last_checked_ts,
                           last_trade_hash = excluded.last_trade_hash,
                           trades_found = wallet_poll_state.trades_found + excluded.trades_found,
                           updated_at = excluded.updated_at""",
                    (wallet, ts, last_hash, trades_found, int(time.time())),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Failed to update poll state for %s: %s", wallet[:10], exc)

    # ── Fetch trades from Data API ──────────────────────────────────────────

    def _fetch_wallet_trades(self, wallet: str, limit: int = 10) -> list[dict]:
        """Fetch recent trades for a wallet from the Data API.

        Uses the existing PolymarketDataApiFetcher which has a proper
        requests.Session with correct headers (the Data API returns 403
        for Python's default urllib user-agent).
        """
        try:
            from trading_platform.polymarket.data_api_fetcher import PolymarketDataApiFetcher
            if not hasattr(self, '_fetcher'):
                self._fetcher = PolymarketDataApiFetcher()
            # Data API uses "user" parameter (not "maker")
            return self._fetcher._fetch_page({"user": wallet, "limit": limit})
        except Exception as exc:
            logger.debug("Data API fetch failed for %s: %s", wallet[:10], exc)
            return []

    # ── Map Data API fields to our format ───────────────────────────────────

    @staticmethod
    def _map_trade(wallet: str, raw: dict, copyable_categories: set[str]) -> PolledTrade | None:
        """Map a Data API trade response to our normalised format.

        Data API fields: proxyWallet, side, asset, conditionId, size, price,
        timestamp, title, slug, eventSlug, outcome, outcomeIndex, transactionHash
        """
        condition_id = raw.get("conditionId") or raw.get("condition_id") or ""
        if not condition_id:
            return None

        # Parse timestamp
        ts = raw.get("timestamp") or raw.get("createdAt") or 0
        if isinstance(ts, str):
            try:
                from datetime import datetime, timezone
                ts = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
            except Exception:
                ts = 0
        try:
            ts = int(float(ts))
        except (TypeError, ValueError):
            ts = 0
        if ts <= 0:
            return None

        # Map side
        side_raw = str(raw.get("side") or "BUY").upper()
        side = side_raw if side_raw in ("BUY", "SELL") else "BUY"

        # Price and size
        try:
            price = float(raw.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            size = float(raw.get("size") or raw.get("amount") or 0)
        except (TypeError, ValueError):
            size = 0.0

        # Outcome / title / slug
        outcome = str(raw.get("outcome") or "Yes").upper()
        if outcome not in ("YES", "NO"):
            outcome = "YES" if raw.get("outcomeIndex") == "0" else "NO"
        title = raw.get("title") or ""
        slug = raw.get("slug") or raw.get("eventSlug") or ""
        tx_hash = raw.get("transactionHash") or ""

        # Infer category from title/slug (best effort)
        category = _infer_category(title, slug)

        return PolledTrade(
            wallet=wallet,
            condition_id=condition_id,
            side=side,
            price=price,
            size=size,
            outcome=outcome,
            title=title,
            category=category,
            timestamp=ts,
            tx_hash=tx_hash,
            slug=slug,
        )

    # ── Build WhaleTrade from polled trade ──────────────────────────────────

    def _to_whale_trade(self, pt: PolledTrade) -> Any:
        """Convert a PolledTrade to a WhaleTrade for the signal engine."""
        from trading_platform.polymarket.whale_tripwire import WhaleTrade, WhaleTripwire

        # Look up wallet profile from leaderboard for WR/conviction/tier
        try:
            tripwire = WhaleTripwire(db_path=self.db_path)
            profile = tripwire.leaderboard.get(pt.wallet.lower(), {})
            tier = tripwire.tier(pt.wallet) or "tier2"
        except Exception:
            profile = {}
            tier = "tier2"

        return WhaleTrade(
            wallet=pt.wallet,
            condition_id=pt.condition_id,
            side=pt.side,
            price=pt.price,
            size=pt.size,
            outcome=pt.outcome,
            question=pt.title,
            category=pt.category,
            timestamp=pt.timestamp,
            wallet_tier=tier,
            directional_win_rate=profile.get("directional_win_rate") or 0,
            conviction_score=profile.get("conviction_score") or 0,
            total_volume_usdc=profile.get("total_volume_usdc") or 0,
        )

    # ── Main run loop ───────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """Run one poll cycle. Returns summary stats."""
        wallets = self._get_copyable_wallets()
        if not wallets:
            print("[POLLER] No copyable wallets found. Run: trading-cli data polymarket alpha-scores")
            return {"wallets": 0, "trades_found": 0, "signals_fired": 0}

        last_checked = self._get_last_checked()

        # Load signal engine + paper executor (lazy, single instance)
        signal_engine = None
        if not self.dry_run:
            try:
                from trading_platform.polymarket.wallet_db import WalletDB
                from trading_platform.polymarket.whale_signal_engine import WhaleSignalEngine
                db = WalletDB(self.db_path)
                signal_engine = WhaleSignalEngine(db=db)
            except Exception as exc:
                print(f"[POLLER] WARNING: Could not init signal engine: {exc}")
                print("[POLLER] Running in detection-only mode (no signals/trades)")

        # Cache WhaleTripwire for tier lookups (one load, not per-trade)
        tripwire = None
        try:
            from trading_platform.polymarket.whale_tripwire import WhaleTripwire
            tripwire = WhaleTripwire(db_path=self.db_path)
        except Exception as exc:
            logger.debug("Could not init tripwire for tier lookups: %s", exc)

        total_trades = 0
        total_signals = 0
        wallets_polled = 0

        print(f"[POLLER] Polling {len(wallets)} copyable wallets...")

        for w_info in wallets:
            wallet = w_info["wallet"]
            categories = set((w_info.get("categories") or "").split(","))
            cutoff_ts = last_checked.get(wallet, 0)

            # Fetch recent trades
            raw_trades = self._fetch_wallet_trades(wallet)
            wallets_polled += 1

            if not raw_trades:
                time.sleep(REQUEST_DELAY)
                continue

            new_trades = []
            max_ts = cutoff_ts

            for raw in raw_trades:
                pt = self._map_trade(wallet, raw, categories)
                if pt is None:
                    continue
                if pt.timestamp <= cutoff_ts:
                    continue

                new_trades.append(pt)
                if pt.timestamp > max_ts:
                    max_ts = pt.timestamp

            if new_trades:
                print(
                    f"  [{wallet[:10]}...] {len(new_trades)} new trades "
                    f"(newest: {max_ts})"
                )

                for pt in new_trades:
                    total_trades += 1

                    if self.dry_run:
                        title_safe = pt.title[:50].encode("ascii", "replace").decode()
                        print(
                            f"    [DRY] {pt.side} {pt.outcome} @ {pt.price:.3f} "
                            f"${pt.size:.0f} -- {title_safe}"
                        )
                        continue

                    # Stale signal check: don't trade if the whale
                    # traded more than 10 minutes ago (gap catchup).
                    age = int(time.time()) - pt.timestamp
                    if age > STALE_SIGNAL_THRESHOLD:
                        self._record_stale(pt, age, "gap_catchup")
                        continue

                    # Skip sports markets entirely — saves API calls and signal processing
                    from trading_platform.polymarket.whale_signal_engine import _is_sports_market
                    if _is_sports_market(pt.category, pt.title):
                        continue

                    if signal_engine:
                        try:
                            # Build WhaleTrade with profile data
                            whale = self._build_whale_trade(pt, tripwire)
                            signal = signal_engine.on_whale_trade(whale)
                            if signal:
                                total_signals += 1
                                print(
                                    f"    -> SIGNAL: {signal.get('signal_type')} "
                                    f"conf={signal.get('confidence', 0):.2f} "
                                    f"alpha={signal.get('alpha_score', 0):.3f}"
                                )
                        except Exception as exc:
                            logger.debug("Signal engine error for %s: %s",
                                         wallet[:10], exc)

                    # Whale exit detection: if this wallet is SELLING,
                    # check if we have an open paper trade that was
                    # triggered by this wallet on this market.
                    if pt.side == "SELL":
                        self._check_whale_exit(pt)

                # Update last_checked
                last_hash = new_trades[-1].tx_hash if new_trades else ""
                self._update_last_checked(wallet, max_ts, len(new_trades), last_hash)

            time.sleep(REQUEST_DELAY)

        summary = {
            "wallets": len(wallets),
            "wallets_polled": wallets_polled,
            "trades_found": total_trades,
            "signals_fired": total_signals,
            "dry_run": self.dry_run,
        }
        print(
            f"[POLLER] Done: {wallets_polled} wallets polled, "
            f"{total_trades} new trades, {total_signals} signals fired"
        )

        # Write heartbeat for monitoring
        try:
            import json
            hb_path = _PROJECT_ROOT / "data" / "poller_heartbeat.json"
            hb_path.parent.mkdir(parents=True, exist_ok=True)
            hb_path.write_text(json.dumps({
                "last_run": int(time.time()),
                "wallets_checked": wallets_polled,
                "trades_detected": total_trades,
                "signals_fired": total_signals,
            }))
        except Exception:
            pass

        return summary

    def _build_whale_trade(self, pt: PolledTrade, tripwire: Any) -> Any:
        """Convert a PolledTrade to a WhaleTrade using cached tripwire."""
        from trading_platform.polymarket.whale_tripwire import WhaleTrade

        profile = {}
        tier = "tier2"
        if tripwire:
            profile = tripwire.leaderboard.get(pt.wallet.lower(), {})
            tier = tripwire.tier(pt.wallet) or "tier2"

        return WhaleTrade(
            wallet=pt.wallet,
            condition_id=pt.condition_id,
            side=pt.side,
            price=pt.price,
            size=pt.size,
            outcome=pt.outcome,
            question=pt.title,
            category=pt.category,
            timestamp=pt.timestamp,
            wallet_tier=tier,
            directional_win_rate=profile.get("directional_win_rate") or 0,
            conviction_score=profile.get("conviction_score") or 0,
            total_volume_usdc=profile.get("total_volume_usdc") or 0,
        )


    def _record_stale(self, pt: PolledTrade, age: int, reason: str) -> None:
        """Record a stale signal that was detected but not traded."""
        try:
            conn = connect_wallet_db(self.db_path)
            try:
                conn.execute(
                    """INSERT INTO stale_signals
                       (wallet, condition_id, market_title, side, price, size,
                        trade_ts, detected_at, age_seconds, reason, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (pt.wallet, pt.condition_id, pt.title, pt.side,
                     pt.price, pt.size, pt.timestamp, int(time.time()),
                     age, reason, int(time.time())),
                )
                conn.commit()
            finally:
                conn.close()
            logger.info(
                "[STALE] %s trade %ds old (max %ds) — logged, not traded",
                pt.wallet[:10], age, STALE_SIGNAL_THRESHOLD,
            )
        except Exception as exc:
            logger.debug("Stale signal record failed: %s", exc)

    def _check_whale_exit(self, pt: PolledTrade) -> None:
        """Detect if a whale selling matches an open paper trade we hold.

        Paper mode: log + record only (HOLD). Live mode would trigger exit.
        """
        try:
            conn = connect_wallet_db(self.db_path)
            try:
                # Find our open trades sourced from this wallet on this market
                our_trades = conn.execute(
                    """SELECT id, entry_price, size_usd, condition_id
                       FROM polymarket_paper_trades
                       WHERE exit_ts IS NULL AND archived = 0
                         AND condition_id = ?
                         AND (LOWER(wallet) = LOWER(?) OR LOWER(source_wallet) = LOWER(?))""",
                    (pt.condition_id, pt.wallet, pt.wallet),
                ).fetchall()

                for trade_id, entry_price, size_usd, cid in our_trades:
                    entry = float(entry_price or 0)
                    unrealized = (pt.price - entry) * (size_usd / max(entry, 0.01))

                    conn.execute(
                        """INSERT INTO whale_exit_signals
                           (trade_id, wallet, condition_id, exit_price, exit_side,
                            exit_ts, our_entry_price, unrealized_at_exit,
                            action_taken, detected_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'HOLD', ?)""",
                        (trade_id, pt.wallet, pt.condition_id, pt.price,
                         pt.side, pt.timestamp, entry, round(unrealized, 2),
                         int(time.time())),
                    )
                    conn.commit()

                    print(
                        f"    [WHALE_EXIT] {pt.wallet[:10]}.. selling {pt.condition_id[:16]}.. "
                        f"— we hold trade #{trade_id} (unrealized: ${unrealized:+.0f})"
                    )

                    # Alert via Telegram (non-blocking)
                    try:
                        from trading_platform.polymarket.alert_manager import get_alert_manager
                        title_safe = pt.title[:40].encode("ascii", "replace").decode()
                        get_alert_manager().send(
                            f"WHALE EXIT -- Trade #{trade_id}\n"
                            f"Wallet {pt.wallet[:14]}.. SOLD position in \"{title_safe}\"\n"
                            f"Exit price: {pt.price:.3f} | Our entry: {entry:.3f}\n"
                            f"Unrealized: ${unrealized:+.0f}\n"
                            f"Action: HOLD (paper mode)",
                            tier="TRADE",
                        )
                    except Exception:
                        pass
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Whale exit check failed: %s", exc)


def _infer_category(title: str, slug: str) -> str:
    """Best-effort category inference from trade title/slug."""
    text = (title + " " + slug).lower()
    if any(k in text for k in ("trump", "biden", "election", "president", "congress",
                                 "senate", "governor", "political", "democrat", "republican")):
        return "politics"
    if any(k in text for k in ("bitcoin", "ethereum", "btc", "eth", "crypto", "sol",
                                 "token", "defi")):
        return "crypto"
    if any(k in text for k in ("nfl", "nba", "mlb", "soccer", "football", "basketball",
                                 "tennis", "sports", "game", "match")):
        return "sports"
    if any(k in text for k in ("fed", "rate", "inflation", "gdp", "jobs", "unemployment",
                                 "cpi", "fomc", "economic")):
        return "economics"
    if any(k in text for k in ("ai", "gpt", "openai", "tech", "apple", "google",
                                 "microsoft", "nvidia")):
        return "science"
    if any(k in text for k in ("culture", "celebrity", "movie", "oscar", "grammy",
                                 "entertainment", "viral", "music", "album",
                                 "musk", "tweet", "elon")):
        return "entertainment"
    return "other"


# ── CLI entry point ─────────────────────────────────────────────────────────

def main(db_path: str | None = None, dry_run: bool = False) -> dict[str, Any]:
    """Entry point for CLI and scheduler."""
    poller = WalletTradePoller(db_path=db_path, dry_run=dry_run)
    return poller.run()
