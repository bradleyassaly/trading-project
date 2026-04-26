"""Cross-platform Kalshi ↔ Polymarket divergence detector (skeleton).

Phase D of the alpha-discovery onramp. The hypothesis: when both
Kalshi and Polymarket list a market for the same underlying event,
significant price divergence (≥5pp) is mispricing — buy the cheap
side, sell the expensive side. Edge is independent of whale flow
and of any single-platform structural bias.

Status: SKELETON. Enabled by env flag `PHASE_D_DIVERGENCE_ENABLED=1`.
Ships disabled by default. Full implementation requires:
  1. Kalshi market ingestion into a queryable form (currently SQLite)
  2. Cross-platform market matching by event signature (slug,
     resolution date, question semantics — likely embedding-based)
  3. Bidirectional execution adapter (PM CLOB + Kalshi REST)
  4. Synchronized exit logic (one side resolves, must close the other)

Until those land, this module emits informational divergence rows to
a new `cross_platform_divergence` table for human review. No trades
fire. The point is to start collecting the data NOW so backtest
evidence is ready when the executor is.

Workflow when enabled:
  - Pull active PM markets resolving in <14d
  - For each, attempt fuzzy match against Kalshi market list
  - When matched, compare YES-side mid prices
  - If |Δ| ≥ 5pp AND both have liquidity, log a divergence row
  - Schedule: every 30 min
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


ENV_FLAG = "PHASE_D_DIVERGENCE_ENABLED"
KALSHI_DB_PATH = os.environ.get(
    "KALSHI_DB_PATH", "/app/data/kalshi/paper_trades.db",
)
DIVERGENCE_THRESHOLD_PCT = 0.05  # 5pp


_SCHEMA = """
CREATE TABLE IF NOT EXISTS cross_platform_divergence (
    id              BIGSERIAL PRIMARY KEY,
    detected_at     BIGINT NOT NULL,
    pm_condition_id TEXT NOT NULL,
    pm_question     TEXT,
    pm_yes_price    DOUBLE PRECISION,
    kalshi_ticker   TEXT NOT NULL,
    kalshi_question TEXT,
    kalshi_yes_price DOUBLE PRECISION,
    divergence_pct  DOUBLE PRECISION,
    cheap_side      TEXT,
    match_score     DOUBLE PRECISION,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_cpd_detected ON cross_platform_divergence(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_cpd_div_pct ON cross_platform_divergence(divergence_pct DESC);
"""


def _ensure_schema(conn) -> None:
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            try:
                conn.execute(s)
            except Exception:
                pass


def _kalshi_active_markets() -> list[dict[str, Any]]:
    """Pull active Kalshi markets from the local SQLite store. Returns
    [] if the file isn't present (Kalshi rails not deployed yet)."""
    if not os.path.exists(KALSHI_DB_PATH):
        return []
    try:
        conn = sqlite3.connect(KALSHI_DB_PATH, timeout=5.0)
        try:
            rows = conn.execute(
                """SELECT ticker, question, yes_bid, yes_ask, status,
                          close_date
                     FROM markets
                    WHERE status = 'active'
                       OR (close_date IS NOT NULL AND close_date > datetime('now'))
                    LIMIT 500"""
            ).fetchall()
        finally:
            conn.close()
        out = []
        for r in rows:
            ticker, q, bid, ask, status, close_date = r
            mid = None
            try:
                if bid is not None and ask is not None:
                    mid = (float(bid) + float(ask)) / 2.0
            except (TypeError, ValueError):
                mid = None
            out.append({
                "ticker": ticker, "question": q or "",
                "yes_price": mid, "close_date": close_date,
            })
        return out
    except Exception as exc:
        logger.debug("kalshi market pull failed: %s", exc)
        return []


def _pm_active_markets(conn) -> list[dict[str, Any]]:
    """Pull active PM markets resolving in <14d with positive liquidity."""
    try:
        rows = conn.execute(
            """SELECT condition_id, slug, question, end_date_iso,
                      outcome_prices, volume_24h
                 FROM markets
                WHERE end_date_iso IS NOT NULL
                  AND end_date_iso > to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS')
                  AND end_date_iso < to_char(
                        NOW() + interval '14 days', 'YYYY-MM-DD"T"HH24:MI:SS')
                  AND yes_token_id IS NOT NULL
                  AND COALESCE(volume_24h, 0) >= 1000
                LIMIT 500"""
        ).fetchall()
    except Exception:
        return []
    out = []
    import json as _j
    for r in rows:
        cid, slug, q, end_iso, prices_raw, vol = r
        yes_price = None
        try:
            prices = _j.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            if isinstance(prices, list) and prices:
                yes_price = float(prices[0])
        except Exception:
            yes_price = None
        if yes_price is None:
            continue
        out.append({
            "condition_id": cid, "slug": slug or "", "question": q or "",
            "end_date_iso": end_iso, "yes_price": yes_price,
            "volume_24h": float(vol or 0),
        })
    return out


def _question_match_score(q1: str, q2: str) -> float:
    """Crude Jaccard similarity on lowercased word tokens. Real
    implementation should embedding-match; this is a placeholder for
    the experiment."""
    if not q1 or not q2:
        return 0.0
    s1 = {w for w in q1.lower().split() if len(w) > 3}
    s2 = {w for w in q2.lower().split() if len(w) > 3}
    if not s1 or not s2:
        return 0.0
    inter = s1 & s2
    union = s1 | s2
    return len(inter) / len(union) if union else 0.0


def run_pipeline() -> dict[str, Any]:
    if os.environ.get(ENV_FLAG, "").lower() not in ("1", "true", "yes"):
        return {"skipped": f"{ENV_FLAG} not set", "matches": 0, "divergences": 0}
    t0 = time.time()
    kalshi = _kalshi_active_markets()
    if not kalshi:
        return {
            "skipped": "no kalshi markets (rails not deployed yet)",
            "elapsed_seconds": round(time.time() - t0, 1),
            "matches": 0, "divergences": 0,
        }
    conn = get_connection()
    try:
        _ensure_schema(conn)
        pm = _pm_active_markets(conn)
        n_matches = 0
        n_divergences = 0
        now = int(time.time())
        for pm_m in pm:
            best_match = None
            best_score = 0.0
            for k_m in kalshi:
                score = _question_match_score(pm_m["question"], k_m["question"])
                if score > best_score:
                    best_score = score
                    best_match = k_m
            if best_score < 0.4 or best_match is None:
                continue
            n_matches += 1
            pm_p = pm_m["yes_price"]
            k_p = best_match["yes_price"]
            if pm_p is None or k_p is None:
                continue
            div = abs(pm_p - k_p)
            if div < DIVERGENCE_THRESHOLD_PCT:
                continue
            cheap_side = "polymarket" if pm_p < k_p else "kalshi"
            try:
                conn.execute(
                    """INSERT INTO cross_platform_divergence
                         (detected_at, pm_condition_id, pm_question,
                          pm_yes_price, kalshi_ticker, kalshi_question,
                          kalshi_yes_price, divergence_pct, cheap_side,
                          match_score, notes)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        now, pm_m["condition_id"], pm_m["question"][:200],
                        pm_p, best_match["ticker"], best_match["question"][:200],
                        k_p, round(div, 4), cheap_side, round(best_score, 3),
                        "skeleton; informational only",
                    ),
                )
                n_divergences += 1
            except Exception as exc:
                logger.debug("divergence row insert failed: %s", exc)
        conn.commit()
        return {
            "elapsed_seconds": round(time.time() - t0, 1),
            "kalshi_active": len(kalshi),
            "pm_active": len(pm),
            "matches": n_matches,
            "divergences": n_divergences,
        }
    finally:
        try: conn.close()
        except Exception: pass


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(run_pipeline())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
