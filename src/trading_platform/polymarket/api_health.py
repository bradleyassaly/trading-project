"""External-API health tracking.

Every external dependency (Gamma, CLOB, Data API, Goldsky, etc.) records
its last success and last error here. The daily review surfaces any API
that's been silent for >1h.

2026-05-23 motivation: Gamma started returning 422 on offset>=10000 and
live-collect spent 24h in a tight error loop emitting no signals. Volume
dropped 9→2 SELL attempts/day. We noticed only because Polymarket UI
showed today's PnL was lower than expected. The history of similar
silent failures (stale balance API hid $57 of equity for 5d on 2026-05-18;
phantom resolved_zero_balance hid $10 of bad accounting for 2 weeks)
shows: any external dependency that stops working silently is a future
bug. This module centralizes that watch.

Usage:
    from trading_platform.polymarket.api_health import record_success, record_error
    try:
        resp = requests.get(...)
        resp.raise_for_status()
        record_success("gamma_markets")
    except Exception as exc:
        record_error("gamma_markets", str(exc))
        raise

Cheap: writes a single row per call via INSERT...ON CONFLICT.
Read pattern: daily review SELECTs all rows and flags last_success_ts
older than threshold.
"""
from __future__ import annotations

import logging
import os
import time

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)


_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS api_health (
    api_name TEXT PRIMARY KEY,
    last_success_ts BIGINT,
    last_error_ts BIGINT,
    last_error_msg TEXT,
    success_count_24h BIGINT DEFAULT 0,
    error_count_24h BIGINT DEFAULT 0,
    updated_at BIGINT
)
"""

_table_ensured = False


def _ensure_table() -> None:
    """Idempotent table creation. Called once per process."""
    global _table_ensured
    if _table_ensured:
        return
    try:
        conn = get_connection()
        try:
            conn.execute(_ENSURE_TABLE_SQL)
            conn.commit()
            _table_ensured = True
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as exc:
        logger.debug("api_health ensure_table failed: %s", exc)


def record_success(api_name: str) -> None:
    """Mark an API call as successful. Fast — used in hot path."""
    if os.environ.get("DISABLE_API_HEALTH"):
        return
    _ensure_table()
    now = int(time.time())
    try:
        conn = get_connection()
        try:
            # Postgres-compatible UPSERT
            conn.execute(
                """INSERT INTO api_health (api_name, last_success_ts, updated_at, success_count_24h)
                   VALUES (?, ?, ?, 1)
                   ON CONFLICT (api_name) DO UPDATE SET
                     last_success_ts = EXCLUDED.last_success_ts,
                     updated_at = EXCLUDED.updated_at,
                     success_count_24h = api_health.success_count_24h + 1""",
                (api_name, now, now),
            )
            conn.commit()
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as exc:
        logger.debug("api_health.record_success(%s) failed: %s", api_name, exc)


def record_error(api_name: str, error_msg: str) -> None:
    """Mark an API call as failed."""
    if os.environ.get("DISABLE_API_HEALTH"):
        return
    _ensure_table()
    now = int(time.time())
    truncated = (error_msg or "")[:500]
    try:
        conn = get_connection()
        try:
            conn.execute(
                """INSERT INTO api_health (api_name, last_error_ts, last_error_msg, updated_at, error_count_24h)
                   VALUES (?, ?, ?, ?, 1)
                   ON CONFLICT (api_name) DO UPDATE SET
                     last_error_ts = EXCLUDED.last_error_ts,
                     last_error_msg = EXCLUDED.last_error_msg,
                     updated_at = EXCLUDED.updated_at,
                     error_count_24h = api_health.error_count_24h + 1""",
                (api_name, now, truncated, now),
            )
            conn.commit()
        finally:
            try: conn.close()
            except Exception: pass
    except Exception as exc:
        logger.debug("api_health.record_error(%s) failed: %s", api_name, exc)


def get_status() -> list[dict]:
    """Return current api_health rows for reporting.

    Each row: {api_name, last_success_age_sec, last_error_age_sec,
               last_error_msg, success_24h, error_24h}

    Note: staleness is judged by callers against their own per-API
    thresholds (daily_system_review.THRESHOLDS / monitor_alerts.API_THRESHOLDS),
    so no single "healthy" flag is exposed here — a hardcoded window would
    contradict the per-API thresholds (e.g. gamma tolerates 8h).
    """
    _ensure_table()
    now = int(time.time())
    try:
        conn = get_connection()
        try:
            rows = conn.execute(
                """SELECT api_name, last_success_ts, last_error_ts, last_error_msg,
                          success_count_24h, error_count_24h
                     FROM api_health ORDER BY api_name"""
            ).fetchall()
        finally:
            try: conn.close()
            except Exception: pass
    except Exception:
        return []
    out = []
    for r in rows:
        last_success_ts = int(r[1] or 0)
        last_error_ts = int(r[2] or 0)
        success_age = (now - last_success_ts) if last_success_ts else None
        error_age = (now - last_error_ts) if last_error_ts else None
        out.append({
            "api_name": r[0],
            "last_success_age_sec": success_age,
            "last_error_age_sec": error_age,
            "last_error_msg": r[3],
            "success_24h": int(r[4] or 0),
            "error_24h": int(r[5] or 0),
        })
    return out
