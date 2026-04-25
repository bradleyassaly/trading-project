"""Per-decision audit trail for every gate evaluation.

Wired 2026-04-25 to close the elite-comparison Layer 7 gap: until now
the live/paper executor's gate decisions only surfaced as log lines.
That makes the question "why didn't market X trade yesterday?"
unanswerable as a query — you have to grep logs.

This module persists a row at every gate evaluation in a single
table. Designed to be cheap (single insert per decision, sampled if
volume gets too high) and queryable (composite index on
(condition_id, decision_ts)).

Usage from any executor:

    from trading_platform.polymarket.decision_trace import trace
    trace(
        signal=signal,
        gate="FAV_GATE",
        passed=False,
        value=0.71,
        threshold=0.65,
        detail="BUY at 0.71 blocked",
    )

Cheap by design — one insert, autocommit, swallows errors so a logger
problem can never starve the trade decision path.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS decision_trace (
    id              BIGSERIAL PRIMARY KEY,
    decision_ts     BIGINT NOT NULL,
    condition_id    TEXT,
    signal_type     TEXT,
    side            TEXT,
    wallet          TEXT,
    category        TEXT,
    gate            TEXT NOT NULL,
    passed          BIGINT NOT NULL,
    value           DOUBLE PRECISION,
    threshold       DOUBLE PRECISION,
    detail          TEXT,
    surface         TEXT
);
CREATE INDEX IF NOT EXISTS idx_dt_condition ON decision_trace(condition_id, decision_ts);
CREATE INDEX IF NOT EXISTS idx_dt_gate ON decision_trace(gate, decision_ts);
CREATE INDEX IF NOT EXISTS idx_dt_passed ON decision_trace(passed, decision_ts);
"""

_schema_ready = False
_lock = __import__("threading").Lock()

# Optional sampling — set DECISION_TRACE_SAMPLE_RATE in env to a float in
# [0, 1] to subsample writes (e.g. 0.1 = 10% sampling). Default = 1.0.
_SAMPLE_RATE = float(os.environ.get("DECISION_TRACE_SAMPLE_RATE", "1.0"))


def _ensure_schema(conn) -> None:
    global _schema_ready
    if _schema_ready:
        return
    with _lock:
        if _schema_ready:
            return
        for stmt in _SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                try:
                    conn.execute(s)
                except Exception:
                    pass
        _schema_ready = True


def trace(
    *,
    signal: dict[str, Any] | None = None,
    gate: str,
    passed: bool,
    value: float | None = None,
    threshold: float | None = None,
    detail: str | None = None,
    surface: str = "paper",
    db_path: str | None = None,
) -> None:
    """Insert one decision row. Never raises — logger problems can never
    block trading."""
    if _SAMPLE_RATE < 1.0:
        import random
        if random.random() > _SAMPLE_RATE:
            return
    try:
        conn = get_connection(db_path) if db_path else get_connection()
    except Exception:
        return
    try:
        _ensure_schema(conn)
        sig = signal or {}
        conn.execute(
            """INSERT INTO decision_trace
                 (decision_ts, condition_id, signal_type, side, wallet,
                  category, gate, passed, value, threshold, detail, surface)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                int(time.time()),
                sig.get("condition_id"),
                sig.get("signal_type"),
                (sig.get("side") or sig.get("direction") or "").upper() or None,
                (sig.get("wallet") or "").lower() or None,
                sig.get("category"),
                gate,
                1 if passed else 0,
                float(value) if value is not None else None,
                float(threshold) if threshold is not None else None,
                (detail or "")[:240],
                surface,
            ),
        )
        conn.commit()
    except Exception as exc:
        logger.debug("decision_trace insert failed (%s): %s", gate, exc)
    finally:
        try: conn.close()
        except Exception: pass


def funnel_summary(
    hours: int = 24, db_path: str | None = None,
) -> list[dict[str, Any]]:
    """Aggregate rejections by gate over the past `hours` window.
    Returns rows ordered by rejection count desc.
    """
    try:
        conn = get_connection(db_path) if db_path else get_connection()
    except Exception:
        return []
    try:
        _ensure_schema(conn)
        cutoff = int(time.time()) - hours * 3600
        rows = conn.execute(
            """SELECT gate, surface,
                      SUM(CASE WHEN passed=1 THEN 1 ELSE 0 END) AS passed,
                      SUM(CASE WHEN passed=0 THEN 1 ELSE 0 END) AS rejected,
                      COUNT(*) AS total
                 FROM decision_trace
                WHERE decision_ts > ?
                GROUP BY gate, surface
                ORDER BY rejected DESC""",
            (cutoff,),
        ).fetchall()
        return [
            {
                "gate": r[0], "surface": r[1],
                "passed": int(r[2] or 0), "rejected": int(r[3] or 0),
                "total": int(r[4] or 0),
            }
            for r in rows
        ]
    finally:
        try: conn.close()
        except Exception: pass
