"""
Wallet archetype classifier.

Classifies wallets into behavioral archetypes based on observable
trading patterns — NOT outcome data — to avoid circular reasoning.
Archetypes determine whether a wallet is safe to copy-trade.

The classifier was validated on fully corrected data (both resolution
bugs fixed): excluding NON_COPYABLE archetypes shifts whale_entry
from break-even (+0.006 EV) to statistically significant positive
(+0.153 EV, p=0.003). See reports/copy_trading_clean_data_2026-04-12.md.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"

NON_COPYABLE = frozenset({"arb_bot", "market_maker", "hft_algo", "penny_collector"})
COPYABLE = frozenset({"specialist", "conviction", "research", "diversified", "whale"})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS wallet_archetypes (
    wallet TEXT PRIMARY KEY,
    archetype TEXT NOT NULL,
    copyable INTEGER NOT NULL,
    total_trades INTEGER,
    fills_per_market REAL,
    trades_per_day REAL,
    buy_ratio REAL,
    extreme_price_ratio REAL,
    uncertain_price_ratio REAL,
    n_categories INTEGER,
    avg_trade_size REAL,
    total_volume REAL,
    classified_at INTEGER DEFAULT (unixepoch('now'))
);
"""


def classify_wallet(stats: dict[str, Any]) -> str:
    fills = stats.get("fills_per_market") or 0
    tpd = stats.get("trades_per_day") or 0
    extreme = stats.get("extreme_price_ratio") or 0
    buy_ratio = stats.get("buy_ratio") or 0.5
    interval = stats.get("avg_interval_sec") or 1e9
    total = stats.get("total_trades") or 0
    uncertain = stats.get("uncertain_price_ratio") or 0
    n_cats = stats.get("n_categories") or 1
    volume = stats.get("total_volume") or 0

    if fills > 15 and extreme > 0.3 and tpd > 20:
        return "arb_bot"
    if fills > 10 and 0.35 < buy_ratio < 0.65 and tpd > 10:
        return "market_maker"
    if interval < 120 and total > 500 and tpd > 50:
        return "hft_algo"
    if extreme > 0.6 and uncertain < 0.2:
        return "penny_collector"
    if n_cats <= 2 and fills <= 8 and tpd < 20:
        return "specialist"
    if (buy_ratio > 0.70 or buy_ratio < 0.30) and uncertain > 0.4 and fills <= 10:
        return "conviction"
    if tpd < 5 and fills <= 5 and uncertain > 0.5:
        return "research"
    if n_cats >= 3 and tpd < 20 and fills <= 10:
        return "diversified"
    # total_volume is USDC notional (size*price), so this is $500k traded.
    if volume > 500_000 and fills <= 15:
        return "whale"
    return "unclassified"


class WalletArchetypeClassifier:

    def __init__(self, db_path: str | Path | None = None) -> None:
        self._db_path = str(db_path or _DEFAULT_DB)
        self._cache: dict[str, tuple[str, bool]] = {}
        self._ensure_table()

    def _connect(self):
        # get_connection routes to Postgres in production (db_path only
        # matters under the DB_BACKEND=sqlite test lane).
        return get_connection(self._db_path)

    def _ensure_table(self) -> None:
        try:
            conn = self._connect()
            conn.execute(_SCHEMA)
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.debug("archetype table ensure failed: %s", exc)

    def classify_all(self) -> dict[str, int]:
        conn = self._connect()
        # wallet_trades.size is SHARES; monetary stats use size*price (USDC).
        rows = conn.execute("""
            SELECT wallet,
                COUNT(*) AS total_trades,
                COUNT(DISTINCT condition_id) AS unique_markets,
                CAST(COUNT(*) AS REAL)/NULLIF(COUNT(DISTINCT condition_id),0) AS fills_per_market,
                AVG(size * price) AS avg_trade_size,
                SUM(size * price) AS total_volume,
                SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END)*1.0/COUNT(*) AS buy_ratio,
                COUNT(DISTINCT category) AS n_categories,
                (MAX(timestamp)-MIN(timestamp))*1.0/NULLIF(COUNT(*)-1,0) AS avg_interval_sec,
                (MAX(timestamp)-MIN(timestamp))/86400.0 AS span_days,
                SUM(CASE WHEN price<0.10 OR price>0.90 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS extreme_price_ratio,
                SUM(CASE WHEN price>=0.15 AND price<=0.85 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS uncertain_price_ratio
            FROM wallet_trades
            GROUP BY wallet
            HAVING COUNT(*) >= 10
        """).fetchall()

        cols = [
            "wallet", "total_trades", "unique_markets", "fills_per_market",
            "avg_trade_size", "total_volume", "buy_ratio", "n_categories",
            "avg_interval_sec", "span_days", "extreme_price_ratio", "uncertain_price_ratio",
        ]
        counts: dict[str, int] = {}
        batch = []
        for row in rows:
            s = dict(zip(cols, row))
            s["trades_per_day"] = s["total_trades"] / max(s["span_days"] or 1, 1)
            arch = classify_wallet(s)
            copyable = 0 if arch in NON_COPYABLE else 1
            counts[arch] = counts.get(arch, 0) + 1
            self._cache[s["wallet"]] = (arch, bool(copyable))
            batch.append((
                s["wallet"], arch, copyable, s["total_trades"],
                s["fills_per_market"], s["trades_per_day"], s["buy_ratio"],
                s["extreme_price_ratio"], s["uncertain_price_ratio"],
                s["n_categories"], s["avg_trade_size"], s["total_volume"],
            ))

        now_ts = int(time.time())
        conn.executemany(
            """INSERT OR REPLACE INTO wallet_archetypes
               (wallet, archetype, copyable, total_trades, fills_per_market,
                trades_per_day, buy_ratio, extreme_price_ratio,
                uncertain_price_ratio, n_categories, avg_trade_size,
                total_volume, classified_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [b + (now_ts,) for b in batch],
        )
        conn.commit()
        conn.close()
        logger.info("classified %d wallets: %s", len(batch), counts)
        return counts

    def _load_cache(self) -> None:
        if self._cache:
            return
        try:
            conn = self._connect()
            for w, a, c in conn.execute("SELECT wallet, archetype, copyable FROM wallet_archetypes"):
                self._cache[w] = (a, bool(c))
            conn.close()
        except Exception:
            pass

    def is_copyable(self, wallet: str) -> bool:
        self._load_cache()
        entry = self._cache.get(wallet)
        if entry is None:
            return True
        return entry[1]

    def get_archetype(self, wallet: str) -> str | None:
        self._load_cache()
        entry = self._cache.get(wallet)
        return entry[0] if entry else None
