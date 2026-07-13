"""Fine-grained mark logger for OPEN live positions near resolution.

Why this exists
---------------
2026-07-12 eval: resolution_decay's realized loss is concentrated in sports
longshots that sit near fair value, then gap straight to $0 at the final
whistle. The exit-at-peak counterfactual showed the held-to-resolution
losers COULD have been exited profitably (+$16 vs actual -$32) if a
trailing/TP had fired during the intra-event swing — but the 5-minute exit
monitor samples too coarsely to catch the seconds-long window before the gap.

To test whether FASTER exit polling (e.g. 60s in the final hours) would
recover that value, we need the intra-hold price PATH at sub-5-minute
granularity. It does not exist historically:
  * ``market_ticks`` does not cover these thin sports tokens (0 ticks for
    every resolution_decay position except one fight that was in the tick
    watchlist).
  * ``position_snapshots`` is written by position_monitor.mark_to_market
    against ``polymarket_paper_trades`` ONLY — there is no live-position
    mark time-series at all; live rows only get peak_yes_price /
    last_mark_price overwritten in place.

This logger closes that gap. It is MEASUREMENT-ONLY: it reads the CLOB book
and writes marks to ``live_position_marks``. It NEVER places, cancels, or
books an order, and it does not touch ``live_trades``. It changes nothing
about when we actually exit — it only records the path so a backtest
(scripts/backtest_faster_poll.py) can later replay exit rules at 60s vs the
live 5-minute cadence and quantify the delta before any policy change.

As a side benefit, polling the CLOB book here keeps the ``clob`` api_health
row fresh (get_order_book records clob health), covering the real read path.

Run once per pass via ``capture_marks_once()`` (wired into task_scheduler at
60s), or ``run_forever(interval=60)`` as a standalone loop.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

# Only log fine marks while a position is within this many hours of its
# resolution_date — that is the window where the intra-event swing (and the
# eventual gap) happens, and where faster polling could matter. Positions
# with no resolution_date are always logged (we cannot tell how close they
# are, and there are only a handful open at once). Keep this generous:
# resolution_decay enters ~24-48h out, so 12h captures the run-in cheaply.
DEFAULT_NEAR_HOURS = 12.0

_SCHEMA = """
CREATE TABLE IF NOT EXISTS live_position_marks (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id           INTEGER NOT NULL,
    token_id           TEXT,
    condition_id       TEXT,
    signal_type        TEXT,
    direction          TEXT,
    entry_price        REAL,
    mid                REAL,
    best_bid           REAL,
    best_ask           REAL,
    spread             REAL,
    secs_to_resolution INTEGER,
    snapshot_at        INTEGER NOT NULL
)
"""
_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_lpm_trade ON live_position_marks(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_lpm_time ON live_position_marks(snapshot_at)",
)

_schema_ready = False


def ensure_schema(conn) -> None:
    """Idempotent table + index creation."""
    global _schema_ready
    if _schema_ready:
        return
    conn.execute(_SCHEMA)
    for stmt in _INDEXES:
        conn.execute(stmt)
    conn.commit()
    _schema_ready = True


def _best_levels(book: dict) -> tuple[float | None, float | None]:
    """(best_bid, best_ask) from a normalized order book, or (None, None).

    ClobClient.get_order_book already sorts asks ascending / bids descending,
    so index 0 is the best level on each side.
    """
    if not isinstance(book, dict) or book.get("error"):
        return None, None
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bb = ba = None
    try:
        if bids:
            bb = float(bids[0]["price"])
        if asks:
            ba = float(asks[0]["price"])
    except (TypeError, ValueError, KeyError):
        return None, None
    return bb, ba


def _open_live_positions(conn) -> list[dict]:
    rows = conn.execute(
        """SELECT id, token_id, condition_id, signal_type, direction,
                  entry_price, resolution_date
             FROM live_trades
            WHERE dry_run = 0 AND exit_ts IS NULL
              AND status IN ('submitted', 'live', 'matched')
              AND token_id IS NOT NULL"""
    ).fetchall()
    out = []
    for r in rows:
        out.append({
            "id": r[0], "token_id": r[1], "condition_id": r[2],
            "signal_type": r[3], "direction": r[4],
            "entry_price": r[5], "resolution_date": r[6],
        })
    return out


def capture_marks_once(
    conn=None,
    *,
    near_hours: float = DEFAULT_NEAR_HOURS,
    client=None,
) -> dict[str, Any]:
    """Record one CLOB mark for every open live position near resolution.

    Measurement-only: reads the CLOB book, writes to live_position_marks.
    Never places/cancels/books orders; never mutates live_trades. Safe by
    default — a per-position failure is skipped, never raised.

    Returns a summary dict for the scheduler log.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        ensure_schema(conn)
        now = int(time.time())
        positions = _open_live_positions(conn)
        if not positions:
            return {"open": 0, "eligible": 0, "marks": 0}

        if client is None:
            # Lazy import so tests can inject a fake client without CLOB creds.
            from trading_platform.polymarket.clob_client import ClobClient
            client = ClobClient()

        eligible = marks = errors = 0
        for p in positions:
            res_date = p.get("resolution_date")
            secs_to_res = None
            if res_date:
                secs_to_res = int(res_date) - now
                # Skip positions that are still far from resolution — the
                # intra-event window we care about hasn't started. Keep
                # already-past-resolution rows (secs<=0): a position awaiting
                # settlement is exactly when the gap happens.
                if secs_to_res > near_hours * 3600:
                    continue
            eligible += 1
            try:
                book = client.get_order_book(p["token_id"])
                bb, ba = _best_levels(book)
                if bb is None and ba is None:
                    errors += 1
                    continue
                if bb is not None and ba is not None:
                    mid = (bb + ba) / 2.0
                    spread = ba - bb
                elif ba is not None:
                    mid, spread = ba, None
                else:
                    mid, spread = bb, None
                conn.execute(
                    """INSERT INTO live_position_marks
                       (trade_id, token_id, condition_id, signal_type,
                        direction, entry_price, mid, best_bid, best_ask,
                        spread, secs_to_resolution, snapshot_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (p["id"], p["token_id"], p["condition_id"],
                     p["signal_type"], p["direction"], p["entry_price"],
                     mid, bb, ba, spread, secs_to_res, now),
                )
                marks += 1
            except Exception as exc:  # never let one position kill the pass
                errors += 1
                logger.debug("[mark_logger] trade #%s failed: %s", p["id"], exc)
        conn.commit()
        return {"open": len(positions), "eligible": eligible,
                "marks": marks, "errors": errors}
    finally:
        if own_conn:
            try:
                conn.close()
            except Exception:
                pass


def run_forever(interval: int = 60, *, near_hours: float = DEFAULT_NEAR_HOURS) -> None:
    """Standalone loop: capture marks every ``interval`` seconds.

    For running as a dedicated process. The task_scheduler path calls
    capture_marks_once() once per 60s tick instead.
    """
    logger.info("[mark_logger] starting loop interval=%ds near_hours=%.1f",
                interval, near_hours)
    while True:
        t0 = time.time()
        try:
            r = capture_marks_once(near_hours=near_hours)
            logger.info("[mark_logger] %s", r)
        except Exception as exc:
            logger.warning("[mark_logger] pass failed: %s", exc)
        # Steady cadence regardless of pass duration.
        dt = time.time() - t0
        time.sleep(max(1.0, interval - dt))


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ap = argparse.ArgumentParser(description="Live-position fine mark logger")
    ap.add_argument("--once", action="store_true", help="single pass then exit")
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--near-hours", type=float, default=DEFAULT_NEAR_HOURS)
    a = ap.parse_args()
    if a.once:
        print(capture_marks_once(near_hours=a.near_hours))
    else:
        run_forever(a.interval, near_hours=a.near_hours)
