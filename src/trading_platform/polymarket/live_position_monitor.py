"""Live position auto-exit monitor.

Mirrors paper_executor.check_exits() for live_trades. For every open
live position:

  1. Fetch current price for the token we hold
  2. Compute unrealized return + update MAE/MFE
  3. Apply per-signal exit profile (SL / TP / trailing / time / market-life)
  4. Apply whale-mirror exit (source wallet sold the same market)
  5. On exit trigger: submit a SELL via py_clob_client + record exit_*

Runs from the scheduler every 5 min. Same exit profiles as paper, so
behavior is identical between paper and live for a given signal type —
this matters because once a signal graduates to live, the exit logic
should produce the same shape of trades as the paper-validated ones.

Safe-by-default: if py_clob_client is missing or sell fails, the
position stays open and we log the error rather than crash.
"""
from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


def _exit_profile(signal_type: str) -> dict[str, float]:
    """Per-signal exit thresholds — must mirror paper_executor's profiles
    so live behavior matches what the paper data calibrated.
    """
    from trading_platform.polymarket.polymarket_paper_executor import (
        PolymarketPaperExecutor,
    )
    profiles = getattr(PolymarketPaperExecutor, "_EXIT_PROFILES", {})
    default = {
        "sl": getattr(PolymarketPaperExecutor, "STOP_LOSS", -0.25),
        "tp": getattr(PolymarketPaperExecutor, "TAKE_PROFIT", 0.40),
        "trail_act": getattr(PolymarketPaperExecutor, "TRAILING_STOP_ACTIVATE", 0.20),
        "trail_back": getattr(PolymarketPaperExecutor, "TRAILING_STOP_DRAWBACK", 0.10),
        "time_days": getattr(PolymarketPaperExecutor, "TIME_DECAY_DAYS", 30),
    }
    return {**default, **profiles.get(signal_type, {})}


def _decide_exit(
    *,
    side: str,
    entry: float,
    current: float,
    mfe_pct: float,
    age_days: float,
    profile: dict,
) -> str | None:
    """Pure-function exit decision. Returns reason or None."""
    if side in ("YES", "BUY"):
        unrealized = (current - entry) / max(entry, 0.01)
    else:
        unrealized = (entry - current) / max(1 - entry, 0.01)

    if mfe_pct >= profile["trail_act"] and unrealized <= (mfe_pct - profile["trail_back"]):
        return "trailing_stop"
    if unrealized <= profile["sl"]:
        return "stop_loss"
    if unrealized >= profile["tp"]:
        return "take_profit"
    if age_days > profile["time_days"] and abs(unrealized) < 0.05:
        return "time_decay"
    return None


def check_live_exits() -> dict[str, int]:
    """One pass: review every open live position; exit if conditions met."""
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.clob_client import ClobClient

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, condition_id, token_id, side, fill_price, size_usd,
                   shares, signal_type, signal_wallet, submitted_at,
                   COALESCE(mfe, 0) AS mfe, last_mark_price,
                   entry_price
            FROM live_trades
            WHERE dry_run = 0
              AND exit_ts IS NULL AND status NOT IN ('error', 'blocked')
        """).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    if not rows:
        return {"checked": 0, "exited": 0, "reasons": {}}

    clob = ClobClient()
    exited = 0
    reasons: dict[str, int] = {}

    for row in rows:
        (lid, cid, token_id, side, fill_price, size_usd, shares,
         sig_type, src_wallet, submitted_at, mfe_dollars, last_mark,
         entry_price) = row
        if not cid or not token_id:
            continue
        # Use fill_price when available; fall back to entry_price for orders
        # where avgFilledPrice was 0 (FOK match without price echo).
        effective_fill = fill_price if fill_price else entry_price
        if not effective_fill:
            continue

        # Fetch current mid price for the token we HOLD
        current = clob.get_mid_price(token_id)
        if current is None:
            continue

        # Compute unrealized pct on stake.
        # For SELL (long NO) positions, use actual CONDITIONAL token balance to
        # compute dollar PnL — DB size_usd reflects intended allocation while
        # GTC partial fills may leave only 1–N tokens in the wallet.
        entry = float(effective_fill)
        if side in ("YES", "BUY"):
            unrealized_pct = (current - entry) / max(entry, 0.01)
            unrealized_dollars = unrealized_pct * float(size_usd or 0)
        else:
            unrealized_pct = (entry - current) / max(1 - entry, 0.01)
            actual_tok = clob.get_conditional_balance(token_id) if token_id else None
            if actual_tok is not None:
                no_price_change = max(0.01, 1.0 - current) - max(0.01, 1.0 - entry)
                unrealized_dollars = actual_tok * no_price_change
            else:
                unrealized_dollars = unrealized_pct * float(size_usd or 0)

        prof = _exit_profile(sig_type or "")

        # Update mark + MAE/MFE in DB
        try:
            conn = get_connection()
            try:
                conn.execute(
                    """UPDATE live_trades
                       SET last_mark_price = ?, last_mark_ts = ?, unrealized_pnl = ?,
                           mae = CASE WHEN mae IS NULL OR ? < mae THEN ? ELSE mae END,
                           mfe = CASE WHEN mfe IS NULL OR ? > mfe THEN ? ELSE mfe END
                       WHERE id = ?""",
                    (current, int(time.time()), unrealized_dollars,
                     unrealized_dollars, unrealized_dollars,
                     unrealized_dollars, unrealized_dollars, lid),
                )
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.debug("mark update failed for live #%d: %s", lid, exc)

        # Whale-mirror exit: source wallet sold this market since we entered
        whale_mirror_exit = False
        if src_wallet and submitted_at:
            try:
                conn = get_connection()
                try:
                    sell_row = conn.execute(
                        """SELECT 1 FROM wallet_trades
                           WHERE wallet = ? AND condition_id = ?
                             AND side = 'SELL' AND timestamp > ? LIMIT 1""",
                        ((src_wallet or "").lower(), cid, int(submitted_at)),
                    ).fetchone()
                    if sell_row:
                        whale_mirror_exit = True
                finally:
                    try: conn.close()
                    except Exception: pass
            except Exception:
                pass

        age_days = (time.time() - float(submitted_at or time.time())) / 86400
        mfe_pct = float(mfe_dollars or 0) / max(float(size_usd or 1), 1)
        exit_reason = "whale_mirror_exit" if whale_mirror_exit else _decide_exit(
            side=side, entry=entry, current=current,
            mfe_pct=mfe_pct, age_days=age_days, profile=prof,
        )

        if not exit_reason:
            continue

        # Place SELL order on the token we HOLD.
        # For SELL direction (long NO): use actual CONDITIONAL token balance — DB
        # size_usd records the intended USDC allocation, but GTC orders on thin
        # near-resolution NO books often partially fill (e.g. 1 token vs. 467
        # intended). Selling the computed size_usd/no_price quantity then hits
        # "not enough balance/allowance". Fall back to size_usd/no_price only if
        # the balance query fails.
        if side not in ("YES", "BUY"):
            actual_tokens = clob.get_conditional_balance(token_id) if token_id else None
            if actual_tokens is not None and actual_tokens < 1.0:
                # Below CLOB minimum sell size (1 token). Market may have resolved
                # or position was a dust partial fill. Mark closed at $0 rather than
                # looping forever on a doomed sell attempt.
                logger.warning(
                    "[live-exit] #%d %s has dust CONDITIONAL balance %.4f — marking resolved_zero_balance",
                    lid, sig_type, actual_tokens if actual_tokens is not None else 0,
                )
                try:
                    conn = get_connection()
                    try:
                        conn.execute(
                            """UPDATE live_trades
                               SET exit_ts=?, exit_reason='resolved_zero_balance',
                                   outcome='loss', realized_pnl=0
                               WHERE id=?""",
                            (int(time.time()), lid),
                        )
                        conn.commit()
                    finally:
                        try: conn.close()
                        except Exception: pass
                except Exception as exc:
                    logger.error("zero-balance close failed for #%d: %s", lid, exc)
                exited += 1
                reasons["resolved_zero_balance"] = reasons.get("resolved_zero_balance", 0) + 1
                continue
            if actual_tokens is not None and actual_tokens > 0:
                shares_held = actual_tokens
            else:
                no_entry = max(1.0 - entry, 0.01)
                shares_held = float(size_usd or 0) / no_entry
            # current is the YES-space price (get_mid_price returns YES
            # regardless of token). Flip to get the actual NO token price
            # for the exit order fallback — otherwise an empty NO book uses
            # YES price as the sell target and the order never fills.
            exit_price_hint = max(0.01, 1.0 - current)
        else:
            # For YES positions, check actual balance to catch:
            #   (a) dust positions (< 1 token) that can't meet CLOB minimum
            #   (b) partial fills where DB `shares` > tokens actually received
            actual_tokens = clob.get_conditional_balance(token_id) if token_id else None
            if actual_tokens is not None and actual_tokens < 1.0:
                logger.warning(
                    "[live-exit] #%d %s has dust YES balance %.4f — marking resolved_zero_balance",
                    lid, sig_type, actual_tokens,
                )
                try:
                    conn = get_connection()
                    try:
                        conn.execute(
                            """UPDATE live_trades
                               SET exit_ts=?, exit_reason='resolved_zero_balance',
                                   outcome='loss', realized_pnl=0
                               WHERE id=?""",
                            (int(time.time()), lid),
                        )
                        conn.commit()
                    finally:
                        try: conn.close()
                        except Exception: pass
                except Exception as exc:
                    logger.error("dust-close failed for #%d: %s", lid, exc)
                exited += 1
                reasons["resolved_zero_balance"] = reasons.get("resolved_zero_balance", 0) + 1
                continue
            db_shares = float(shares or 0) or (float(size_usd or 0) / max(entry, 0.01))
            # Cap at actual wallet balance — DB shares may be stale for partial fills
            if actual_tokens is not None and 0 < actual_tokens < db_shares:
                shares_held = actual_tokens
            else:
                shares_held = db_shares
            exit_price_hint = current
        # shares_held is guaranteed >= 1.0 at this point (dust handled above)
        exact_sell_shares = int(shares_held)
        order_result = clob.place_market_order(
            token_id=token_id, side="SELL",
            size_usdc=float(size_usd or 0),
            exact_shares=exact_sell_shares,
            max_slippage=0.05,
            price_hint=exit_price_hint,
        )
        if not order_result.success:
            logger.warning(
                "[live-exit] %s exit blocked for #%d (%s) — sell failed: %s",
                exit_reason, lid, sig_type, order_result.error_msg,
            )
            continue

        fill = order_result.filled_price or current
        # Use shares_held (actual token count) instead of deriving from size_usd.
        # size_usd can be stale/wrong for partially-filled GTC orders.
        # SELL: entry=NO_entry, fill=NO_exit; BUY: entry=YES_entry, fill=YES_exit.
        realized_pnl = float(shares_held) * (fill - entry) \
            if side in ("YES", "BUY") else \
            float(shares_held) * (entry - fill)
        outcome = "win" if realized_pnl > 0 else "loss"

        try:
            conn = get_connection()
            try:
                conn.execute(
                    """UPDATE live_trades
                       SET exit_price = ?, exit_ts = ?, outcome = ?,
                           realized_pnl = ?, exit_reason = ?
                       WHERE id = ?""",
                    (fill, int(time.time()), outcome,
                     round(realized_pnl, 2), exit_reason, lid),
                )
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.error("live-exit DB update failed for #%d: %s", lid, exc)

        exited += 1
        reasons[exit_reason] = reasons.get(exit_reason, 0) + 1

        # Loud Telegram alert for live exits
        try:
            from trading_platform.polymarket.telegram_alerts import get_alerter
            alerter = get_alerter()
            sign = "+" if realized_pnl >= 0 else ""
            alerter._send(
                f"\U0001f6a8 <b>LIVE EXIT</b> \U0001f6a8\n\n"
                f"<b>{sig_type}</b> exited via <b>{exit_reason}</b>\n"
                f"Entry: {entry:.3f} → Exit: {fill:.3f}\n"
                f"PnL: <b>{sign}${realized_pnl:.2f}</b> ({outcome})\n"
                f"Held {age_days:.1f} days",
                disable_notification=False,
            )
        except Exception:
            pass

        logger.info(
            "[live-exit] #%d %s @ %.3f → %s | pnl=%+.2f",
            lid, sig_type, fill, exit_reason, realized_pnl,
        )

    return {"checked": len(rows), "exited": exited, "reasons": reasons}


def main() -> int:
    try:
        from trading_platform.polymarket.logging_config import setup_logging
        setup_logging(service="live-monitor")
    except Exception:
        logging.basicConfig(level=logging.INFO)
    result = check_live_exits()
    logger.info(
        "[live-monitor] checked=%d exited=%d reasons=%s",
        result["checked"], result["exited"], result["reasons"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
