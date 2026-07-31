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


def backfill_actual_fills(lookback_hours: int = 24) -> int:
    """Correct fill_price/shares on recent OPEN matched entries from the
    wallet's actual data-api fills.

    place_market_order records fill_price = the limit TARGET
    (best_ask + tick) when the CLOB's synchronous response lacks
    avgFilledPrice — systematically ~1 tick worse than the real fill
    (observed 2026-07-06: recorded 0.13/0.19/0.22 vs actual
    0.12/0.18/0.21), overstating cost basis and understating PnL.
    data-api /trades is the fill-level truth; entries are BUYs of the
    held token for both directions. Only open positions are touched —
    rewriting basis under an already-booked realized_pnl would corrupt
    the ledger.

    Returns count of corrected rows.
    """
    import os
    from trading_platform.polymarket.db_connection import get_connection

    wallet = (os.environ.get("POLYMARKET_FUNDER_ADDRESS")
              or os.environ.get("POLYMARKET_WALLET_ADDRESS"))
    if not wallet:
        return 0

    from trading_platform.polymarket.price_space import to_yes_space

    conn = get_connection()
    try:
        cutoff = int(time.time()) - lookback_hours * 3600
        rows = conn.execute(
            """SELECT id, token_id, attempted_at, fill_price, shares, size_usd,
                      direction
                 FROM live_trades
                WHERE dry_run = 0 AND status = 'matched'
                  AND exit_ts IS NULL AND token_id IS NOT NULL
                  AND attempted_at > ?""",
            (cutoff,),
        ).fetchall()
    finally:
        try: conn.close()
        except Exception: pass
    if not rows:
        return 0

    try:
        import requests
        r = requests.get(
            "https://data-api.polymarket.com/trades",
            params={"user": wallet, "limit": 500}, timeout=15,
        )
        fills = r.json() if r.ok else []
    except Exception as exc:
        logger.debug("[reconciler] fill backfill fetch failed: %s", exc)
        return 0
    if not isinstance(fills, list) or not fills:
        return 0

    corrected = 0
    for lid, token_id, attempted_at, fp_db, sh_db, size_usd, direction in rows:
        # Entry fills: BUYs of the held token within [attempt-2m, attempt+2h]
        tot_sz = 0.0
        tot_cost = 0.0
        for t in fills:
            if str(t.get("asset")) != str(token_id):
                continue
            if (t.get("side") or "").upper() != "BUY":
                continue
            ts = int(t.get("timestamp") or 0)
            if not (int(attempted_at) - 120 <= ts <= int(attempted_at) + 7200):
                continue
            sz = float(t.get("size") or 0)
            px = float(t.get("price") or 0)
            tot_sz += sz
            tot_cost += sz * px
        if tot_sz < 1.0:
            continue
        # data-api prices are in the HELD token's own space (the NO token for
        # a SELL). fill_price is stored YES-space by convention, so convert.
        # 2026-07-07: the original version wrote the raw NO-space vwap here,
        # inflating SELL cost basis ~10x on every corrected SELL row.
        held_vwap = tot_cost / tot_sz
        vwap = round(to_yes_space(direction, held_vwap), 4)
        fp_old = float(fp_db) if fp_db is not None else None
        sh_old = float(sh_db) if sh_db is not None else None
        if (fp_old is not None and abs(vwap - fp_old) < 0.0005) and \
                (sh_old is not None and abs(tot_sz - sh_old) < 0.5):
            continue
        conn = get_connection()
        try:
            # 2026-07-31: recompute the execution-cost metrics from the
            # CORRECTED fill. This UPDATE used to fix fill_price and leave
            # slippage_c holding the value computed at insert time from the
            # PROVISIONAL price (the posted limit, best_ask + tick), so
            # every reconciled row carried a constant +1c (one tick) of
            # phantom execution cost: stored 1.371c vs a true 0.662c on
            # n=67, i.e. the metric read as a FULL spread when the real
            # cost is the HALF spread. cost_model.py discounts paper P&L
            # with these numbers, so every paper strategy was penalised a
            # full extra spread. Derive them here or they stay stale.
            conn.execute(
                """UPDATE live_trades
                      SET fill_price = ?, shares = ?,
                          slippage_c = CASE
                              WHEN mid_at_decision IS NOT NULL
                              THEN ROUND(((? - mid_at_decision) * 100)::numeric, 4)
                              ELSE slippage_c END,
                          slippage = CASE
                              WHEN entry_price IS NOT NULL AND entry_price > 0
                              THEN ROUND((ABS(? - entry_price) / entry_price)::numeric, 6)
                              ELSE slippage END,
                          slippage_signed = CASE
                              WHEN entry_price IS NOT NULL AND entry_price > 0
                              THEN ROUND((
                                  CASE WHEN UPPER(COALESCE(direction,'BUY')) = 'SELL'
                                       THEN -(? - entry_price)
                                       ELSE (? - entry_price) END
                                  / entry_price)::numeric, 6)
                              ELSE slippage_signed END,
                          slippage_cost_usd = CASE
                              WHEN entry_price IS NOT NULL
                              THEN ROUND((ABS(? - entry_price) * ?)::numeric, 4)
                              ELSE slippage_cost_usd END
                    WHERE id = ? AND exit_ts IS NULL""",
                (vwap, tot_sz, vwap, vwap, vwap, vwap, vwap, tot_sz, lid),
            )
            conn.commit()
        finally:
            try: conn.close()
            except Exception: pass
        corrected += 1
        logger.info(
            "[reconciler] #%d fill corrected: price %s→%.4f shares %s→%.1f",
            lid, f"{fp_old:.4f}" if fp_old is not None else "?", vwap,
            f"{sh_old:.1f}" if sh_old is not None else "?", tot_sz,
        )
    return corrected


def reconcile_open_orders() -> dict[str, int]:
    """One pass: resolve any orders stuck in 'live'/'submitted' status."""
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.clob_client import ClobClient

    try:
        fills_corrected = backfill_actual_fills()
    except Exception as exc:
        logger.debug("[reconciler] fill backfill failed: %s", exc)
        fills_corrected = 0

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
        return {"checked": 0, "filled": 0, "cancelled": 0, "still_open": 0, "fills_corrected": fills_corrected}

    clob = ClobClient()
    if not clob.is_configured:
        logger.debug("[reconciler] CLOB not configured, skipping")
        return {"checked": 0, "filled": 0, "cancelled": 0, "still_open": 0, "fills_corrected": fills_corrected}

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
        return {"checked": len(rows), "filled": 0, "cancelled": 0, "still_open": len(rows), "fills_corrected": fills_corrected}

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
                    # slippage_c must follow fill_price here too — same
                    # staleness bug as correct_fill_prices() above.
                    conn.execute(
                        """UPDATE live_trades
                           SET status='matched', fill_price=?, filled_at=?,
                               slippage=?, fill_time_ms=?,
                               slippage_c = CASE
                                   WHEN mid_at_decision IS NOT NULL AND ? IS NOT NULL
                                   THEN ROUND(((? - mid_at_decision) * 100)::numeric, 4)
                                   ELSE slippage_c END
                           WHERE id=?""",
                        (fp, now_ts, slippage, fill_time_ms, fp, fp, lid),
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
                        from py_clob_client_v2.clob_types import OrderPayload as _OP
                        py_client.cancel_order(_OP(orderID=order_id))
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

    return {"checked": len(rows), "filled": filled, "cancelled": cancelled, "still_open": still_open, "fills_corrected": fills_corrected}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = reconcile_open_orders()
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
