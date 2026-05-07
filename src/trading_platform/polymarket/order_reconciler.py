"""Order status reconciler.

Polls the Polymarket CLOB for any live_trades rows that have an order_id but
whose status is still 'live' or 'submitted' — i.e., a GTC order that was
accepted but never confirmed as filled or cancelled (e.g., the process
restarted mid-poll).

For each such row:
  - Query CLOB for the order status
  - If matched: update fill_price, status, filled_at, slippage, fill_time_ms
  - If cancelled/expired: update status to 'cancelled'
  - If still open and > MAX_OPEN_MINUTES: cancel it and update status

Run from the scheduler every 10 min.
"""
from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

MAX_OPEN_MINUTES = 10


def reconcile_open_orders() -> dict[str, int]:
    """One pass: resolve any orders stuck in 'live'/'submitted' status."""
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.clob_client import ClobClient

    conn = get_connection()
    try:
        cutoff = int(time.time()) - MAX_OPEN_MINUTES * 60
        rows = conn.execute(
            """SELECT id, order_id, entry_price, size_usd, submitted_at
               FROM live_trades
               WHERE dry_run = 0
                 AND status IN ('live', 'submitted', 'delayed')
                 AND order_id IS NOT NULL
                 AND exit_ts IS NULL""",
        ).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    if not rows:
        return {"checked": 0, "filled": 0, "cancelled": 0, "still_open": 0}

    clob = ClobClient()
    if not clob.is_configured:
        logger.debug("[reconciler] CLOB not configured, skipping")
        return {"checked": 0, "filled": 0, "cancelled": 0, "still_open": 0}

    filled = cancelled = still_open = 0

    try:
        from py_clob_client_v2 import ClobClient as PyClobClient, ApiCreds
        from py_clob_client_v2.constants import POLYGON
        import os as _os
        funder = _os.environ.get("POLYMARKET_FUNDER_ADDRESS") or ""
        client_kwargs = dict(
            host="https://clob.polymarket.com", chain_id=POLYGON,
            key=clob._private_key,
            creds=ApiCreds(
                api_key=clob._api_key,
                api_secret=clob._api_secret,
                api_passphrase=clob._passphrase,
            ),
        )
        if funder:
            client_kwargs["signature_type"] = int(_os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1"))
            client_kwargs["funder"] = funder
        py_client = PyClobClient(**client_kwargs)
    except Exception as exc:
        logger.debug("[reconciler] py_clob_client unavailable: %s", exc)
        return {"checked": len(rows), "filled": 0, "cancelled": 0, "still_open": len(rows)}

    for lid, order_id, entry_price, size_usd, submitted_at in rows:
        try:
            check = py_client.get_order(order_id)
            st = check.get("status", "")
            now_ts = int(time.time())

            if st == "matched":
                fp_raw = check.get("avgFilledPrice")
                fp = float(fp_raw) if fp_raw else (float(entry_price) if entry_price else None)
                slippage = round(fp - float(entry_price), 4) if fp and entry_price else None
                fill_time_ms = int((now_ts - int(submitted_at or now_ts)) * 1000)
                conn = get_connection()
                try:
                    conn.execute(
                        """UPDATE live_trades
                           SET status='matched', fill_price=?, filled_at=?,
                               slippage=?, fill_time_ms=?
                           WHERE id=?""",
                        (fp, now_ts, slippage, fill_time_ms, lid),
                    )
                    conn.commit()
                finally:
                    try: conn.close()
                    except Exception: pass
                filled += 1
                logger.info("[reconciler] order %s → matched @ %.3f", order_id[:16], fp or 0)

            elif st in ("cancelled", "expired"):
                conn = get_connection()
                try:
                    conn.execute(
                        "UPDATE live_trades SET status=? WHERE id=?",
                        (st, lid),
                    )
                    conn.commit()
                finally:
                    try: conn.close()
                    except Exception: pass
                cancelled += 1

            else:
                # Still open — cancel if past max age
                age_min = (now_ts - int(submitted_at or now_ts)) / 60
                if age_min > MAX_OPEN_MINUTES:
                    try:
                        py_client.cancel_order(order_id)
                        conn = get_connection()
                        try:
                            conn.execute(
                                "UPDATE live_trades SET status='cancelled', error_msg=? WHERE id=?",
                                (f"reconciler cancelled after {age_min:.0f}m", lid),
                            )
                            conn.commit()
                        finally:
                            try: conn.close()
                            except Exception: pass
                        cancelled += 1
                        logger.info("[reconciler] cancelled stale order %s (%.0fm old)", order_id[:16], age_min)
                    except Exception as exc:
                        logger.debug("[reconciler] cancel failed for %s: %s", order_id[:16], exc)
                        still_open += 1
                else:
                    still_open += 1

        except Exception as exc:
            logger.debug("[reconciler] order %s check failed: %s", order_id[:16], exc)
            still_open += 1

    return {"checked": len(rows), "filled": filled, "cancelled": cancelled, "still_open": still_open}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = reconcile_open_orders()
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
