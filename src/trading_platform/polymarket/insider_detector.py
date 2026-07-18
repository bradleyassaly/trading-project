"""
Insider wallet detector.

Identifies wallets with demonstrated information advantage by measuring
uncertain-zone trading accuracy across resolved markets. Wallets that
exceed 60% accuracy on 10+ uncertain-zone trades are flagged as insiders.

Backtested at +0.283 EV on 38 out-of-sample trades (p < 0.001) with
zero overlap to accumulation wallets. See
reports/insider_detection_analysis_2026-04-12.md.
"""
from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_DB = _PROJECT_ROOT / "data" / "polymarket" / "wallet_intelligence.db"
_GAMMA_CSV = _PROJECT_ROOT / "data" / "polymarket" / "gamma_resolution.csv"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS insider_wallets (
    wallet TEXT PRIMARY KEY,
    insider_score REAL NOT NULL,
    uncertain_accuracy REAL NOT NULL,
    uncertain_trades INTEGER NOT NULL,
    total_trades INTEGER,
    avg_entry_percentile REAL,
    primary_category TEXT,
    category_purity REAL,
    total_pnl REAL,
    avg_trade_size REAL,
    detection_method TEXT DEFAULT 'accuracy_60pct',
    classified_at INTEGER DEFAULT (unixepoch('now'))
);
"""

MIN_UNCERTAIN_TRADES = 10
MIN_ACCURACY = 0.60
UNCERTAIN_LO = 0.15
UNCERTAIN_HI = 0.85


class InsiderDetector:

    def __init__(self, db_path: str | Path | None = None) -> None:
        self._db_path = str(db_path or _DEFAULT_DB)
        self._cache: dict[str, dict] = {}
        self._ensure_table()

    def _ensure_table(self) -> None:
        try:
            conn = sqlite3.connect(self._db_path, timeout=30)
            conn.executescript(_SCHEMA)
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.debug("insider_wallets table ensure failed: %s", exc)

    def rebuild(self) -> dict[str, int]:
        csv_path = Path(_GAMMA_CSV)
        if not csv_path.exists():
            return {"error": "gamma_resolution.csv not found", "qualified": 0}

        res_map: dict[str, bool] = {}
        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            seen: set[str] = set()
            for row in csv.DictReader(fh):
                cid = (row.get("condition_id") or "").strip().lower()
                if cid in seen:
                    continue
                seen.add(cid)
                ry = (row.get("resolves_yes") or "").strip().lower()
                if ry in ("true", "false"):
                    res_map[cid] = ry == "true"

        conn = sqlite3.connect(self._db_path, timeout=60)
        rows = conn.execute(
            "SELECT wallet, condition_id, side, price, size, timestamp, category "
            "FROM wallet_trades WHERE market_outcome IS NOT NULL"
        ).fetchall()
        cols = ["wallet", "condition_id", "side", "price", "size", "timestamp", "category"]
        trades = [dict(zip(cols, r)) for r in rows]

        for t in trades:
            yw = res_map.get(t["condition_id"].lower() if t["condition_id"] else "")
            t["yes_won"] = yw
            t["correct"] = (
                (t["side"] == "BUY" and yw is True)
                or (t["side"] == "SELL" and yw is False)
            ) if yw is not None else None
            p = t["price"] or 0
            t["uncertain"] = UNCERTAIN_LO <= p <= UNCERTAIN_HI

        # Market timing
        mkt_first: dict[str, int] = {}
        mkt_last: dict[str, int] = {}
        for t in trades:
            cid = t["condition_id"]
            ts = t["timestamp"] or 0
            if cid not in mkt_first or ts < mkt_first[cid]:
                mkt_first[cid] = ts
            if cid not in mkt_last or ts > mkt_last[cid]:
                mkt_last[cid] = ts
        for t in trades:
            cid = t["condition_id"]
            span = max((mkt_last.get(cid, 0) - mkt_first.get(cid, 0)), 1)
            t["entry_pct"] = ((t["timestamp"] or 0) - mkt_first.get(cid, 0)) / span

        # Per-wallet aggregation
        from collections import defaultdict
        by_wallet: dict[str, list] = defaultdict(list)
        for t in trades:
            if t["correct"] is not None:
                by_wallet[t["wallet"]].append(t)

        conn.execute("DELETE FROM insider_wallets")
        qualified = 0
        self._cache.clear()

        for wallet, wt in by_wallet.items():
            unc = [t for t in wt if t["uncertain"]]
            n_unc = len(unc)
            if n_unc < MIN_UNCERTAIN_TRADES:
                continue
            acc = sum(1 for t in unc if t["correct"]) / n_unc
            if acc < MIN_ACCURACY:
                continue

            cats = [t["category"] for t in wt if t["category"]]
            from collections import Counter
            cat_counts = Counter(cats)
            primary_cat = cat_counts.most_common(1)[0][0] if cat_counts else "unknown"
            purity = cat_counts.most_common(1)[0][1] / len(wt) if cat_counts and wt else 0

            avg_ep = sum(t["entry_pct"] for t in wt) / len(wt) if wt else 0.5
            # size is SHARES — avg_trade_size is USDC notional (size*price),
            # matching the whale engine's trade_usdc comparison downstream.
            avg_size = (
                sum((t["size"] or 0) * (t["price"] or 0) for t in wt) / len(wt)
                if wt else 0
            )

            score = (
                acc * 0.35
                + (1 - avg_ep) * 0.20
                + purity * 0.15
                + min(n_unc / 20, 1.0) * 0.15
                + min(avg_size / 1000, 1.0) * 0.15
            )

            total_pnl = sum(
                (t["size"] or 0) * ((1.0 if t["yes_won"] else 0.0) - (t["price"] or 0))
                if t["side"] == "BUY"
                else (t["size"] or 0) * ((t["price"] or 0) - (1.0 if t["yes_won"] else 0.0))
                for t in wt if t["yes_won"] is not None
            )

            conn.execute(
                """INSERT OR REPLACE INTO insider_wallets
                   (wallet, insider_score, uncertain_accuracy, uncertain_trades,
                    total_trades, avg_entry_percentile, primary_category,
                    category_purity, total_pnl, avg_trade_size, detection_method,
                    classified_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,'accuracy_60pct',unixepoch('now'))""",
                (wallet, round(score, 4), round(acc, 4), n_unc, len(wt),
                 round(avg_ep, 4), primary_cat, round(purity, 4),
                 round(total_pnl, 2), round(avg_size, 2)),
            )
            self._cache[wallet] = {"insider_score": score, "uncertain_accuracy": acc,
                                   "uncertain_trades": n_unc, "primary_category": primary_cat}
            qualified += 1

        conn.commit()
        conn.close()
        logger.info("insider detector: %d wallets qualified", qualified)
        return {"qualified": qualified}

    def _load_cache(self) -> None:
        if self._cache:
            return
        try:
            conn = sqlite3.connect(self._db_path, timeout=30)
            for r in conn.execute("SELECT wallet, insider_score, uncertain_accuracy, uncertain_trades, primary_category FROM insider_wallets"):
                self._cache[r[0]] = {"insider_score": r[1], "uncertain_accuracy": r[2],
                                     "uncertain_trades": r[3], "primary_category": r[4]}
            conn.close()
        except Exception:
            pass

    def is_insider(self, wallet: str) -> bool:
        self._load_cache()
        return wallet in self._cache

    def get_insider_profile(self, wallet: str) -> dict | None:
        self._load_cache()
        return self._cache.get(wallet)

    def get_all_insiders(self) -> list[dict[str, Any]]:
        try:
            conn = sqlite3.connect(self._db_path, timeout=30)
            cols = [c[1] for c in conn.execute("PRAGMA table_info(insider_wallets)").fetchall()]
            rows = conn.execute("SELECT * FROM insider_wallets ORDER BY insider_score DESC").fetchall()
            conn.close()
            return [dict(zip(cols, r)) for r in rows]
        except Exception:
            return []
