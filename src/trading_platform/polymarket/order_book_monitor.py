"""
Polymarket order book monitor.

Periodically pulls order book snapshots from the CLOB ``/book`` endpoint
and detects insider-style anomalies:

  * **order_book_imbalance** — bid/ask depth ratio crosses 70% / 80%
  * **bid_wall** — a single bid level holds >$30K, suggesting a hidden
    whale absorbing supply
  * **sustained_imbalance** — imbalance >70% maintained across multiple
    snapshots (much more meaningful than a single point in time)
  * **ask_removal** — ask depth drops >40% snapshot-over-snapshot
    (sellers pulling liquidity right before a move)

Designed to run as a daemon thread inside the live collector. Pure
read + compute against the public CLOB API; this module never writes
to the database directly. Callers (e.g. the live collector) are
responsible for persisting detected anomalies via the
``market_anomalies`` table writer.

Usage::

    from trading_platform.polymarket.order_book_monitor import OrderBookMonitor
    mon = OrderBookMonitor(db_path)
    snap = mon.fetch_snapshot(condition_id, token_id)
    signals = mon.analyze_book(condition_id, token_id, question, category)
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

CLOB_BOOK_URL = "https://clob.polymarket.com/book"
DATA_API_TRADES = "https://data-api.polymarket.com/trades"


@dataclass
class OrderBookSnapshot:
    condition_id: str
    token_id: str
    timestamp: int
    bids: list[dict[str, Any]] = field(default_factory=list)
    asks: list[dict[str, Any]] = field(default_factory=list)
    mid_price: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 1.0
    spread: float = 1.0
    bid_depth_usd: float = 0.0
    ask_depth_usd: float = 0.0
    imbalance_ratio: float = 0.5
    top_bid_usd: float = 0.0
    top_ask_usd: float = 0.0


class OrderBookMonitor:
    """Fetch CLOB order books and detect insider-style anomalies."""

    # Anomaly thresholds — tuned for live use, not academic precision
    IMBALANCE_HIGH = 0.80          # >80% buy-side = critical
    IMBALANCE_MEDIUM = 0.70        # >70% buy-side = high
    BID_WALL_USD = 30_000          # single bid level threshold
    ASK_REMOVAL_DROP = 0.40        # >40% drop in ask depth = critical
    SUSTAINED_IMBALANCE_MINS = 15  # imbalance held >15 min = critical
    LARGE_TRADE_USD = 10_000       # standalone large trade
    DEPTH_LEVELS = 20              # top N levels for depth aggregation
    MIN_TOTAL_USD = 500            # ignore markets with <$500 total depth
    HISTORY_RETENTION_SEC = 7200   # keep 2h of snapshots per market

    def __init__(self, db_path: str | None = None) -> None:
        self._db_path = db_path
        # cid -> [(ts, imbalance, bid_usd, ask_usd)]
        self._history: dict[str, list[tuple[int, float, float, float]]] = {}
        self._snapshot_schema_ready = False

    def _ensure_snapshot_schema(self, conn) -> None:
        """Lazy-create order_book_snapshots table.

        Wired 2026-04-30 to support strategies (e.g. btc_5min_strategy)
        that read OB depth at trade-time. Previously the monitor kept
        history only in process memory — strategies couldn't query it.
        """
        if self._snapshot_schema_ready:
            return
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS order_book_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    token_id TEXT,
                    ts BIGINT NOT NULL,
                    yes_bid_depth_usd DOUBLE PRECISION,
                    yes_ask_depth_usd DOUBLE PRECISION,
                    mid_price DOUBLE PRECISION,
                    imbalance_ratio DOUBLE PRECISION
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_obs_cid_ts "
                "ON order_book_snapshots(condition_id, ts DESC)"
            )
            conn.commit()
            self._snapshot_schema_ready = True
        except Exception as exc:
            logger.debug("OB snapshot schema ensure failed: %s", exc)

    def _persist_snapshot(self, snap: "OrderBookSnapshot") -> None:
        """Write one snapshot row. Best-effort; never raises."""
        if not self._db_path:
            return
        try:
            from trading_platform.polymarket.db_connection import get_connection
            conn = get_connection(self._db_path)
            try:
                self._ensure_snapshot_schema(conn)
                conn.execute(
                    """INSERT INTO order_book_snapshots
                       (condition_id, token_id, ts, yes_bid_depth_usd,
                        yes_ask_depth_usd, mid_price, imbalance_ratio)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        snap.condition_id, snap.token_id, snap.timestamp,
                        float(snap.bid_depth_usd or 0),
                        float(snap.ask_depth_usd or 0),
                        float(snap.mid_price or 0),
                        float(snap.imbalance_ratio or 0),
                    ),
                )
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.debug("OB snapshot persist failed: %s", exc)

    # ── Snapshot fetch ───────────────────────────────────────────────────────

    def fetch_snapshot(self, condition_id: str, token_id: str) -> OrderBookSnapshot | None:
        """Fetch the current CLOB order book for one token."""
        try:
            r = requests.get(CLOB_BOOK_URL, params={"token_id": token_id}, timeout=5)
            if r.status_code != 200:
                return None
            book = r.json()
        except Exception as exc:
            logger.debug("book fetch failed for %s: %s", token_id[:16], exc)
            return None

        bids = book.get("bids") or []
        asks = book.get("asks") or []

        def _depth(levels: list[dict]) -> tuple[float, float]:
            """Return (total_usd, top_level_usd) over top DEPTH_LEVELS levels."""
            if not levels:
                return 0.0, 0.0
            usd = 0.0
            top_usd = 0.0
            for lvl in levels[: self.DEPTH_LEVELS]:
                try:
                    p = float(lvl.get("price") or 0)
                    s = float(lvl.get("size") or 0)
                except (TypeError, ValueError):
                    continue
                # CLOB price is already a probability (0..1), not cents
                level_usd = p * s
                usd += level_usd
                if level_usd > top_usd:
                    top_usd = level_usd
            return usd, top_usd

        bid_usd, top_bid_usd = _depth(bids)
        ask_usd, top_ask_usd = _depth(asks)
        total_usd = bid_usd + ask_usd
        imbalance = bid_usd / total_usd if total_usd >= self.MIN_TOTAL_USD else 0.5

        # min/max, not [0] — raw book sides arrive sorted descending, so
        # asks[0] is the worst quote (0.99 dust) and mid/spread skew high
        try:
            best_bid = max((float(b.get("price") or 0) for b in bids), default=0.0)
        except (TypeError, ValueError):
            best_bid = 0.0
        try:
            best_ask = min((float(a.get("price") or 1) for a in asks), default=1.0)
        except (TypeError, ValueError):
            best_ask = 1.0

        return OrderBookSnapshot(
            condition_id=condition_id,
            token_id=token_id,
            timestamp=int(time.time()),
            bids=bids[:30],
            asks=asks[:30],
            mid_price=(best_bid + best_ask) / 2,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=max(0.0, best_ask - best_bid),
            bid_depth_usd=round(bid_usd, 2),
            ask_depth_usd=round(ask_usd, 2),
            imbalance_ratio=round(imbalance, 4),
            top_bid_usd=round(top_bid_usd, 2),
            top_ask_usd=round(top_ask_usd, 2),
        )

    # ── Anomaly detection ────────────────────────────────────────────────────

    def detect_anomalies(
        self,
        condition_id: str,
        current: OrderBookSnapshot,
        previous: OrderBookSnapshot | None,
    ) -> list[dict[str, Any]]:
        """Compare current vs previous snapshot to detect insider patterns."""
        anomalies: list[dict[str, Any]] = []

        # 1. Point-in-time imbalance
        if current.imbalance_ratio >= self.IMBALANCE_HIGH:
            severity = "critical"
        elif current.imbalance_ratio >= self.IMBALANCE_MEDIUM:
            severity = "high"
        else:
            severity = None
        if severity:
            anomalies.append({
                "type": "order_book_imbalance",
                "severity": severity,
                "detail": (
                    f"{current.imbalance_ratio:.0%} buy-side | "
                    f"${current.bid_depth_usd:,.0f} bids vs ${current.ask_depth_usd:,.0f} asks"
                ),
                "imbalance": current.imbalance_ratio,
                "bid_usd": current.bid_depth_usd,
                "ask_usd": current.ask_depth_usd,
            })

        # 2. Bid wall — single hidden whale order
        if current.top_bid_usd >= self.BID_WALL_USD:
            anomalies.append({
                "type": "bid_wall",
                "severity": "high",
                "detail": f"${current.top_bid_usd:,.0f} bid wall at {current.best_bid:.3f}",
                "imbalance": current.imbalance_ratio,
                "bid_usd": current.bid_depth_usd,
                "ask_usd": current.ask_depth_usd,
            })

        # 3. Ask removal — sellers pulling liquidity
        if previous and previous.ask_depth_usd > 5_000:
            drop = (previous.ask_depth_usd - current.ask_depth_usd) / previous.ask_depth_usd
            if drop > self.ASK_REMOVAL_DROP:
                anomalies.append({
                    "type": "ask_removal",
                    "severity": "critical",
                    "detail": (
                        f"Ask depth dropped {drop:.0%}: "
                        f"${previous.ask_depth_usd:,.0f} → ${current.ask_depth_usd:,.0f}"
                    ),
                    "imbalance": current.imbalance_ratio,
                    "bid_usd": current.bid_depth_usd,
                    "ask_usd": current.ask_depth_usd,
                })

        # 4. Spread compression
        if previous and previous.spread > 0 and current.spread < previous.spread * 0.5:
            anomalies.append({
                "type": "spread_compression",
                "severity": "medium",
                "detail": f"Spread compressed {previous.spread:.3f} → {current.spread:.3f}",
                "imbalance": current.imbalance_ratio,
                "bid_usd": current.bid_depth_usd,
                "ask_usd": current.ask_depth_usd,
            })

        # 5. Sustained imbalance — multi-snapshot agreement
        history = self._history.get(condition_id, [])
        if len(history) >= 3:
            recent = [i for _, i, _, _ in history[-3:]]
            if all(i >= self.IMBALANCE_MEDIUM for i in recent):
                minutes = (history[-1][0] - history[-3][0]) / 60
                if minutes >= self.SUSTAINED_IMBALANCE_MINS:
                    anomalies.append({
                        "type": "sustained_imbalance",
                        "severity": "critical",
                        "detail": (
                            f"Imbalance sustained {minutes:.0f}min: "
                            f"avg {sum(recent)/3:.0%} buy-side"
                        ),
                        "imbalance": round(sum(recent) / 3, 4),
                        "bid_usd": current.bid_depth_usd,
                        "ask_usd": current.ask_depth_usd,
                    })

        return anomalies

    def analyze_book(
        self,
        condition_id: str,
        token_id: str,
        question: str = "",
        category: str = "",
    ) -> list[dict[str, Any]]:
        """One-shot fetch + analyze. Returns anomaly dicts with market context.

        Records the snapshot in the history buffer so subsequent calls can
        detect sustained-imbalance / ask-removal patterns.
        """
        current = self.fetch_snapshot(condition_id, token_id)
        if not current:
            return []

        # 2026-04-30: persist snapshot so trade-time strategies (e.g.
        # btc_5min) can read recent depth without keeping the monitor
        # process-shared. Best-effort; failure never blocks anomaly path.
        self._persist_snapshot(current)

        history = self._history.setdefault(condition_id, [])
        previous = history[-1] if history else None
        # Convert previous tuple back to a minimal snapshot for diff
        prev_snap = None
        if previous:
            prev_snap = OrderBookSnapshot(
                condition_id=condition_id,
                token_id=token_id,
                timestamp=previous[0],
                imbalance_ratio=previous[1],
                bid_depth_usd=previous[2],
                ask_depth_usd=previous[3],
            )

        anomalies = self.detect_anomalies(condition_id, current, prev_snap)

        # Decorate
        for a in anomalies:
            a["condition_id"] = condition_id
            a["token_id"] = token_id
            a["question"] = question
            a["category"] = category
            a["mid_price"] = current.mid_price
            a["timestamp"] = current.timestamp

        # Append history (compact tuple form)
        history.append((current.timestamp, current.imbalance_ratio,
                        current.bid_depth_usd, current.ask_depth_usd))
        # Trim to retention window
        cutoff = current.timestamp - self.HISTORY_RETENTION_SEC
        self._history[condition_id] = [t for t in history if t[0] >= cutoff]

        return anomalies

    # ── Large trade detection ────────────────────────────────────────────────

    def check_large_trades(self, condition_id: str, minutes_back: int = 60) -> list[dict[str, Any]]:
        """Find unusually large trades for a market in the last N minutes."""
        since = int(time.time()) - minutes_back * 60
        try:
            r = requests.get(
                DATA_API_TRADES,
                params={"market": condition_id, "limit": 100},
                timeout=10,
            )
            data = r.json()
            trades = data if isinstance(data, list) else data.get("data", [])
        except Exception as exc:
            logger.debug("large trades fetch failed: %s", exc)
            return []

        large = []
        for t in trades:
            ts = t.get("timestamp") or 0
            if ts < since:
                continue
            try:
                price = float(t.get("price") or 0)
                size = float(t.get("size") or 0)
            except (TypeError, ValueError):
                continue
            usd = price * size
            if usd >= self.LARGE_TRADE_USD:
                large.append({
                    "timestamp": ts,
                    "side": t.get("side"),
                    "usd": round(usd, 2),
                    "price": price,
                    "size": size,
                    "wallet": (t.get("proxyWallet") or t.get("maker") or "").lower(),
                })
        large.sort(key=lambda x: x["usd"], reverse=True)
        return large

    # ── Bulk scan ────────────────────────────────────────────────────────────

    def run_scan(
        self,
        markets: list[dict[str, Any]],
        sleep_between: float = 0.15,
        persist: bool = True,
    ) -> list[dict[str, Any]]:
        """Scan a list of markets and return all detected anomalies.

        ``markets`` is a list of dicts each with at least condition_id and
        token_id; question/category optional. When ``persist=True`` and
        ``self._db_path`` is set, anomalies are written to the
        ``market_anomalies`` table for later querying.
        """
        all_signals: list[dict[str, Any]] = []
        for m in markets:
            cid = m.get("condition_id") or ""
            token_id = m.get("token_id") or ""
            if not cid or not token_id:
                continue
            try:
                anomalies = self.analyze_book(
                    cid, token_id,
                    question=m.get("question", ""),
                    category=m.get("category", ""),
                )
            except Exception as exc:
                logger.debug("analyze_book failed for %s: %s", cid[:20], exc)
                continue
            for a in anomalies:
                logger.warning(
                    "[OB] %s [%s] %s — %s",
                    a["type"], a["severity"], a["question"][:35], a["detail"],
                )
            all_signals.extend(anomalies)
            if sleep_between > 0:
                time.sleep(sleep_between)

        if persist and self._db_path and all_signals:
            self._persist_anomalies(all_signals)

        # SILENCED on Telegram per signal_analysis_clean.md follow-up:
        # order-book anomalies are technical scanner output with no
        # wallet alpha basis. The historical flood was largely from
        # this and the velocity scanner. Skips are accumulated in the
        # AlertManager daily digest counter instead.
        try:
            from trading_platform.polymarket.alert_manager import get_alert_manager
            am = get_alert_manager()
            for s in all_signals:
                if (s.get("severity") or "") in ("critical", "high"):
                    am.record_signal_skipped("order_book_anomaly_no_wallet")
        except Exception:
            pass

        return all_signals

    def _persist_anomalies(self, anomalies: list[dict[str, Any]]) -> None:
        """Insert anomalies into ``market_anomalies``."""
        import sqlite3
        try:
            conn = sqlite3.connect(self._db_path)
            try:
                conn.executemany(
                    """INSERT INTO market_anomalies
                        (detected_at, condition_id, token_id, question, category,
                         anomaly_type, severity, detail, imbalance, bid_usd, ask_usd)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    [
                        (
                            a.get("timestamp") or int(time.time()),
                            a.get("condition_id") or "",
                            a.get("token_id"),
                            a.get("question") or "",
                            a.get("category") or "",
                            a.get("type") or "",
                            a.get("severity") or "",
                            a.get("detail") or "",
                            a.get("imbalance"),
                            a.get("bid_usd"),
                            a.get("ask_usd"),
                        )
                        for a in anomalies
                    ],
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("anomaly persistence failed: %s", exc)
