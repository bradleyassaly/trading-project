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
import os
import time
from pathlib import Path
from typing import Any

from trading_platform.polymarket import price_space

logger = logging.getLogger(__name__)

_GAMMA_CSV = Path(__file__).resolve().parents[3] / "data" / "polymarket" / "gamma_resolution.csv"

# 2026-07-05: positions whose remaining market value is below this floor are
# not worth exiting — the recoverable USDC is pennies and the far side of the
# book is usually empty, so sell attempts rest unfilled forever. Abandon the
# exit (exit_stuck=1) and let resolution/book_resolved_positions book the
# outcome. Before this floor existed, dust SELLs produced an 11k-warning
# cancel/re-place loop against the CLOB (one cycle every 5 minutes per
# position since 2026-06-03).
LIVE_EXIT_MIN_VALUE_USD = float(os.environ.get("LIVE_EXIT_MIN_VALUE_USD", "0.50"))


def _settle_dust_close(
    *,
    token_id: str | None,
    condition_id: str | None,
    direction: str,
    trade_status: str | None,
    fill_price: float | None,
    db_shares: float,
    size_usd: float = 0.0,
) -> tuple[float, str | None] | None:
    """Determine realized P&L for a position whose on-chain balance hit dust.

    A zero CONDITIONAL balance is AMBIGUOUS: it means either the market
    resolved against us (tokens worthless) OR we WON and Polymarket
    auto-redeemed the tokens into USDC. Until 2026-07-02 both dust
    branches assumed loss, so auto-redeemed WINS were booked as losses —
    the root cause of "Polymarket's PnL is higher than ours".

    Truth sources, in order:
      1. data-api /positions ``cashPnl`` — Polymarket's own number
         (matches scripts/book_resolved_positions.py semantics).
      2. gamma_resolution.csv via ResolutionResolver — market outcome
         vs our side.
      3. Neither available → return None. Caller must LEAVE THE TRADE
         OPEN so the scheduled book_resolved_positions task can settle
         it from data-api later; guessing "loss" corrupts the ledger.

    Returns (realized_pnl, outcome) or None when truth is unavailable.
    """
    # Never matched on-chain → no capital was committed.
    if trade_status != "matched" or fill_price is None:
        return 0.0, None
    # 2026-07-06: db_shares can be 0 on a REAL matched position — the
    # hourly on-chain share sync used to overwrite shares with the
    # post-resolution zero balance (fixed, but rows may still arrive
    # zeroed). If capital was committed, derive the entry quantity from
    # size_usd instead of booking (0, None) and silently erasing the
    # outcome. Six resolved wins were booked at pnl=0 this way on 7/6.
    if db_shares <= 0:
        fp0 = float(fill_price)
        per_share = fp0 if (direction or "BUY").upper() == "BUY" \
            else max(1.0 - fp0, 0.01)
        if size_usd and size_usd > 0 and per_share > 0:
            db_shares = float(size_usd) / per_share
        else:
            return 0.0, None

    fp = float(fill_price)
    want_yes = (direction or "BUY").upper() == "BUY"
    # Cost basis: BUY holds YES at fp/share; SELL holds NO at (1-fp)/share.
    cost = db_shares * (fp if want_yes else (1.0 - fp))

    # 1. Polymarket's own cashPnl from the data-api positions endpoint.
    wallet = (os.environ.get("POLYMARKET_FUNDER_ADDRESS")
              or os.environ.get("POLYMARKET_WALLET_ADDRESS"))
    if wallet and token_id:
        try:
            from trading_platform.polymarket.position_fetcher import PositionFetcher
            for p in PositionFetcher().fetch_wallet_positions(wallet):
                if str(p.get("asset")) == str(token_id):
                    cash = p.get("cashPnl")
                    if cash is not None:
                        realized = float(cash)
                        return realized, ("win" if realized > 0 else "loss")
        except Exception as exc:
            logger.debug("dust-settle: data-api lookup failed: %s", exc)

    # 1.5. Our own SELL fills — a zero balance can also mean WE SOLD the
    # tokens (exit order filled but the booking raced the fill and the
    # trade was left "open"; happened to six positions on 2026-07-06).
    # Booking such a position at RESOLUTION payout would be wrong — e.g.
    # sold at 0.40, market later resolved 1.00. The wallet's data-api
    # trade history is the ground truth for what we actually received.
    if wallet and token_id:
        try:
            import requests as _rq
            _tr = _rq.get(
                "https://data-api.polymarket.com/trades",
                params={"user": wallet, "limit": 500}, timeout=15,
            )
            if _tr.ok:
                proceeds = 0.0
                sold = 0.0
                for t in _tr.json():
                    if str(t.get("asset")) == str(token_id) and \
                            (t.get("side") or "").upper() == "SELL":
                        sz = float(t.get("size") or 0)
                        px = float(t.get("price") or 0)
                        proceeds += sz * px
                        sold += sz
                if sold >= 1.0:
                    realized = round(proceeds - cost, 4)
                    return realized, ("win" if realized > 0 else "loss")
        except Exception as exc:
            logger.debug("dust-settle: sell-fill lookup failed: %s", exc)

    # 2. Market outcome from the resolution file.
    try:
        from trading_platform.polymarket.resolution_resolver import ResolutionResolver
        resolver = ResolutionResolver(_GAMMA_CSV)
        price = resolver.resolve(token_id or "", condition_id=condition_id)
        if price is not None:
            resolved_yes = price >= 99.0
            we_won = resolved_yes if want_yes else not resolved_yes
            if we_won:
                # Payout $1/share on the side we hold.
                realized = db_shares - cost
                return round(realized, 4), "win"
            return round(-cost, 4), "loss"
    except Exception as exc:
        logger.debug("dust-settle: resolution lookup failed: %s", exc)

    # 3. Unknown — do not guess.
    return None


def _exit_profile(signal_type: str, direction: str = "BUY") -> dict[str, Any]:
    """Per-(signal, direction) exit thresholds.

    Mirrors paper_executor's profiles so live behavior matches what the paper
    data calibrated. Direction-specific overrides apply on top.

    2026-06-02: added direction-specific override for wallet_reversal SELL.
    Audit (90d) showed wallet_reversal SELL captured only ~37% of MFE vs
    84-99% on other (signal, direction) cells. Root cause: single trail tier
    (act=0.30, back=0.15) is too loose for the volatile NO-side moves
    these positions ride. Tighter ladder + earlier TP gets us closer to
    the per-MFE peak.
    """
    from trading_platform.polymarket.polymarket_paper_executor import (
        PolymarketPaperExecutor,
    )
    profiles = getattr(PolymarketPaperExecutor, "_EXIT_PROFILES", {})
    default: dict[str, Any] = {
        "sl": getattr(PolymarketPaperExecutor, "STOP_LOSS", -0.25),
        "tp": getattr(PolymarketPaperExecutor, "TAKE_PROFIT", 0.40),
        "trail_act": getattr(PolymarketPaperExecutor, "TRAILING_STOP_ACTIVATE", 0.20),
        "trail_back": getattr(PolymarketPaperExecutor, "TRAILING_STOP_DRAWBACK", 0.10),
        "time_days": getattr(PolymarketPaperExecutor, "TIME_DECAY_DAYS", 30),
    }
    merged: dict[str, Any] = {**default, **profiles.get(signal_type, {})}

    # Direction-specific override: wallet_reversal SELL needs tighter capture.
    # Tiered trail: as MFE grows, tighten trail_back to lock in more of the move.
    if signal_type == "wallet_reversal" and direction == "SELL":
        merged["tp"] = 0.40  # was 0.60 — fire earlier (was leaking giant winners)
        merged["trail_ladder"] = [
            # (mfe_threshold, trail_back)  — first match wins, evaluate top→bottom
            (0.50, 0.03),  # MFE >= 50% → exit on 3pp pullback (very tight)
            (0.30, 0.06),  # MFE >= 30% → 6pp
            (0.15, 0.10),  # MFE >= 15% → 10pp (early lock-in tier)
        ]
    return merged


def _decide_exit(
    *,
    side: str,
    entry: float,
    current: float,
    mfe_pct: float,
    age_days: float,
    profile: dict,
) -> str | None:
    """Pure-function exit decision. Returns reason or None.

    2026-06-02: stop_loss is now SKIPPED for SELL entries at fp>=0.90.
    Audit showed 71 stop_loss exits for -$57.20 total — biggest single
    loss category. For "lottery ticket" SELLs at fp=0.99 (NO at 1¢),
    the position is already near max-loss at entry: there's effectively
    nothing to stop out from. The stop_loss gate fires on noise as YES
    ticks up briefly, then YES often retreats. Whipsaw.
    Trailing_stop + take_profit still apply (those exit on real moves).
    For SELL at fp < 0.90 (mid-band), stop_loss still fires as designed.
    """
    if side in ("YES", "BUY"):
        unrealized = (current - entry) / max(entry, 0.01)
    else:
        unrealized = (entry - current) / max(1 - entry, 0.01)

    # Tiered trail (multi-level TP ladder): profile["trail_ladder"] = [(mfe, back), ...]
    # Top-down, first match wins. Tighter trail at higher MFE locks in winners.
    ladder = profile.get("trail_ladder")
    if ladder:
        for mfe_threshold, back in ladder:
            if mfe_pct >= mfe_threshold and unrealized <= (mfe_pct - back):
                return "trailing_stop"
    elif mfe_pct >= profile["trail_act"] and unrealized <= (mfe_pct - profile["trail_back"]):
        return "trailing_stop"
    # Skip stop_loss for high-fp SELLs (lottery tickets — can't stop)
    skip_sl = (side not in ("YES", "BUY")) and entry is not None and entry >= 0.90
    if not skip_sl and unrealized <= profile["sl"]:
        return "stop_loss"
    if unrealized >= profile["tp"]:
        return "take_profit"
    if age_days > profile["time_days"] and abs(unrealized) < 0.05:
        return "time_decay"
    return None


# Arbitrary constant key for the exit-monitor advisory lock. Postgres
# advisory locks are global per-key; this serializes check_live_exits across
# the scheduler AND the wallet-stream chain-direct trigger (both call it).
_EXIT_LOCK_KEY = 748213001


def check_live_exits() -> dict[str, int]:
    """One pass: review every open live position; exit if conditions met.

    2026-07-07 (audit A4/F4): guarded by a Postgres advisory lock. This
    function runs from BOTH the 5-min scheduler and the wallet-stream
    chain-direct trigger; concurrent runs both SELECT the same open rows and
    place SELL orders, and the second run's "not enough balance / active
    orders" handler cancels the FIRST run's live order mid-fill. The lock
    makes overlapping runs a no-op instead of a self-cancelling race.
    """
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.clob_client import ClobClient

    _lock_conn = get_connection()
    try:
        _got = _lock_conn.execute(
            "SELECT pg_try_advisory_lock(%s)", (_EXIT_LOCK_KEY,)
        ).fetchone()
        _have_lock = bool(_got and _got[0])
    except Exception:
        # Non-PG backend (sqlite test lane) has no advisory locks — proceed
        # unlocked; concurrency is a production-only concern.
        _have_lock = True
        _lock_conn = None
    if not _have_lock:
        logger.info("[live-exit] another exit pass holds the lock — skipping")
        try:
            if _lock_conn is not None:
                _lock_conn.close()
        except Exception:
            pass
        return {"checked": 0, "exited": 0, "reasons": {}, "skipped_locked": 1}

    try:
        return _check_live_exits_locked()
    finally:
        try:
            if _lock_conn is not None:
                _lock_conn.execute("SELECT pg_advisory_unlock(%s)", (_EXIT_LOCK_KEY,))
                _lock_conn.close()
        except Exception:
            pass


def _check_live_exits_locked() -> dict[str, int]:
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.clob_client import ClobClient

    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT id, condition_id, token_id, side, fill_price, size_usd,
                   shares, signal_type, signal_wallet, submitted_at,
                   COALESCE(mfe, 0) AS mfe, last_mark_price,
                   entry_price, status, resolution_date,
                   COALESCE(exit_attempts, 0) AS exit_attempts_so_far
            FROM live_trades
            WHERE dry_run = 0
              AND exit_ts IS NULL AND status NOT IN ('error', 'blocked')
              AND COALESCE(exit_stuck, 0) = 0
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
         entry_price, trade_status, resolution_date, exit_attempts_so_far) = row
        if not cid or not token_id:
            continue
        # 2026-06-04: late-stage exit policy.
        # Near-resolution SELL positions have no intermediate-price takers
        # (the book is empty 1-2¢ inside the eventual outcome). Trying to
        # exit them just churns CLOB rate budget and Telegram alerts.
        # Once we've failed >= 5 times AND we're within 2 days of resolution,
        # stop attempting. Let it resolve naturally. This bounds the
        # observed pattern of 7+ positions all retrying every cycle.
        if resolution_date and exit_attempts_so_far >= 5:
            days_to_resolve = (float(resolution_date) - time.time()) / 86400.0
            if 0 < days_to_resolve <= 2.0:
                logger.info(
                    "[live-exit] holding #%d (%s) to resolution: %.1fd left, %d attempts already",
                    lid, sig_type, days_to_resolve, exit_attempts_so_far,
                )
                reasons["holding_to_resolution"] = reasons.get("holding_to_resolution", 0) + 1
                continue
        # Use fill_price when available; fall back to entry_price for orders
        # where avgFilledPrice was 0 (FOK match without price echo).
        effective_fill = fill_price if fill_price else entry_price
        if not effective_fill:
            continue

        # Current YES-space price for the token we HOLD. 2026-07-06:
        # prefer the EXECUTABLE side over the mid — thin/dead books make
        # mids fictitious (empty post-game book → mid 0.5 on worthless
        # tokens), which fires phantom take-profits and pollutes MFE/mark
        # stats. For a long-YES position the exit hits the YES bid; for
        # long-NO it effectively hits (1 - YES ask), so YES ask is the
        # relevant YES-space price. Mid remains the fallback when the
        # needed book side is empty.
        current = None
        try:
            _book = clob.get_order_book(token_id)
            _bids = _book.get("bids") or []
            _asks = _book.get("asks") or []
            if side in ("YES", "BUY"):
                current = float(_bids[0]["price"]) if _bids else None
            else:
                current = float(_asks[0]["price"]) if _asks else None
        except Exception:
            current = None
        if current is None:
            current = clob.get_mid_price(token_id)
        if current is None:
            continue

        # Compute unrealized pct on stake.
        # For SELL (long NO) positions, use actual CONDITIONAL token balance to
        # compute dollar PnL — DB size_usd reflects intended allocation while
        # GTC partial fills may leave only 1–N tokens in the wallet.
        # entry and current are both YES-space. unrealized_pct is return on
        # the held-token cost; unrealized_dollars uses actual on-chain tokens
        # when available (GTC partials leave fewer tokens than size_usd implies).
        entry = float(effective_fill)
        entry_held = price_space.held_token_price(side, entry)
        cur_held = price_space.held_token_price(side, current)
        unrealized_pct = (cur_held - entry_held) / max(entry_held, 0.01)
        if side in ("YES", "BUY"):
            unrealized_dollars = unrealized_pct * float(size_usd or 0)
        else:
            actual_tok = clob.get_conditional_balance(token_id) if token_id else None
            if actual_tok is not None:
                unrealized_dollars = actual_tok * (cur_held - entry_held)
            else:
                unrealized_dollars = unrealized_pct * float(size_usd or 0)

        # Direction-specific profile (e.g., wallet_reversal SELL has tighter trail ladder)
        direction = "BUY" if side in ("YES", "BUY") else "SELL"
        prof = _exit_profile(sig_type or "", direction)

        # Update mark + MAE/MFE in DB.
        # `current` is YES-space price (clob.get_mid_price always returns YES).
        # peak_yes_price/peak_no_price store the most favorable price reached
        # for each side — lets us quantify trailing-stop leakage post-exit
        # (peak_no − exit_no on SELL = $ left on the table per share).
        _no_now = max(0.0, 1.0 - float(current))
        try:
            conn = get_connection()
            try:
                conn.execute(
                    """UPDATE live_trades
                       SET last_mark_price = ?, last_mark_ts = ?, unrealized_pnl = ?,
                           mae = CASE WHEN mae IS NULL OR ? < mae THEN ? ELSE mae END,
                           mfe = CASE WHEN mfe IS NULL OR ? > mfe THEN ? ELSE mfe END,
                           peak_yes_price = CASE WHEN peak_yes_price IS NULL OR ? > peak_yes_price THEN ? ELSE peak_yes_price END,
                           peak_no_price = CASE WHEN peak_no_price IS NULL OR ? > peak_no_price THEN ? ELSE peak_no_price END
                       WHERE id = ?""",
                    (current, int(time.time()), unrealized_dollars,
                     unrealized_dollars, unrealized_dollars,
                     unrealized_dollars, unrealized_dollars,
                     current, current,
                     _no_now, _no_now,
                     lid),
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
                # 2026-07-02: settlement is delegated to _settle_dust_close.
                # Zero balance no longer implies loss — auto-redeemed WINS
                # also zero out. Truth comes from data-api cashPnl, then the
                # resolution file; if neither knows, leave the trade OPEN for
                # the scheduled book_resolved_positions task.
                db_shares = float(shares or 0)
                settled = _settle_dust_close(
                    token_id=token_id, condition_id=cid,
                    direction="SELL", trade_status=trade_status,
                    fill_price=fill_price, db_shares=db_shares,
                    size_usd=float(size_usd or 0),
                )
                if settled is None:
                    logger.warning(
                        "[live-exit] #%d %s dust balance but resolution unknown — "
                        "leaving open for book_resolved_positions", lid, sig_type,
                    )
                    continue
                realized, outcome_val = settled
                try:
                    conn = get_connection()
                    try:
                        conn.execute(
                            """UPDATE live_trades
                               SET exit_ts=?, exit_reason='resolved_zero_balance',
                                   outcome=?, realized_pnl=?
                               WHERE id=? AND exit_ts IS NULL""",
                            (int(time.time()), outcome_val, round(realized, 4), lid),
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
                # Same settlement as the SELL branch: zero balance is
                # ambiguous (loss OR auto-redeemed win) — resolve from truth
                # sources, never assume.
                db_shares = float(shares or 0)
                settled = _settle_dust_close(
                    token_id=token_id, condition_id=cid,
                    direction="BUY", trade_status=trade_status,
                    fill_price=fill_price, db_shares=db_shares,
                    size_usd=float(size_usd or 0),
                )
                if settled is None:
                    logger.warning(
                        "[live-exit] #%d %s dust YES balance but resolution unknown — "
                        "leaving open for book_resolved_positions", lid, sig_type,
                    )
                    continue
                realized, outcome_val = settled
                try:
                    conn = get_connection()
                    try:
                        conn.execute(
                            """UPDATE live_trades
                               SET exit_ts=?, exit_reason='resolved_zero_balance',
                                   outcome=?, realized_pnl=?
                               WHERE id=? AND exit_ts IS NULL""",
                            (int(time.time()), outcome_val, round(realized, 4), lid),
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
        # Dust-VALUE abandon: >=1 token but the whole position is worth
        # pennies (e.g. 24 NO tokens at 0.0025). exit_price_hint is already
        # in the held token's price space for both branches.
        position_value = float(shares_held) * float(exit_price_hint)
        if position_value < LIVE_EXIT_MIN_VALUE_USD:
            logger.warning(
                "[live-exit] #%d (%s) remaining value $%.2f < $%.2f floor — "
                "abandoning exit attempts; resolution booking will close it",
                lid, sig_type, position_value, LIVE_EXIT_MIN_VALUE_USD,
            )
            try:
                conn = get_connection()
                try:
                    conn.execute(
                        """UPDATE live_trades
                              SET exit_stuck = 1,
                                  exit_first_attempt_at = COALESCE(exit_first_attempt_at, %s),
                                  exit_last_error = %s
                            WHERE id = %s""",
                        (int(time.time()),
                         f"abandoned_dust_value ${position_value:.2f}", lid),
                    )
                    conn.commit()
                finally:
                    try: conn.close()
                    except Exception: pass
            except Exception as _exc:
                logger.debug("[live-exit] dust-abandon update failed for #%d: %s", lid, _exc)
            reasons["abandoned_dust_value"] = reasons.get("abandoned_dust_value", 0) + 1
            continue
        exact_sell_shares = int(shares_held)
        order_result = clob.place_market_order(
            token_id=token_id, side="SELL",
            size_usdc=float(size_usd or 0),
            exact_shares=exact_sell_shares,
            max_slippage=0.05,
            price_hint=exit_price_hint,
        )
        if not order_result.success:
            # Track retry state. After 5 attempts: telegram alert.
            # After 20 attempts: mark stuck and stop trying.
            try:
                conn = get_connection()
                try:
                    conn.execute(
                        """UPDATE live_trades
                              SET exit_attempts = exit_attempts + 1,
                                  exit_first_attempt_at = COALESCE(exit_first_attempt_at, %s),
                                  exit_last_error = %s,
                                  exit_stuck = CASE WHEN exit_attempts + 1 >= 20 THEN 1 ELSE 0 END
                            WHERE id = %s""",
                        (int(time.time()), (order_result.error_msg or "")[:500], lid),
                    )
                    new_attempts_row = conn.execute(
                        "SELECT exit_attempts, exit_stuck FROM live_trades WHERE id = %s", (lid,)
                    ).fetchone()
                    conn.commit()
                finally:
                    try: conn.close()
                    except Exception: pass
            except Exception as _exc:
                logger.debug("[live-exit] retry-state update failed for #%d: %s", lid, _exc)
                new_attempts_row = None

            attempts = int(new_attempts_row[0]) if new_attempts_row else 0
            stuck = int(new_attempts_row[1]) if new_attempts_row else 0
            logger.warning(
                "[live-exit] %s exit blocked for #%d (%s) attempts=%d stuck=%d — sell failed: %s",
                exit_reason, lid, sig_type, attempts, stuck,
                (order_result.error_msg or "")[:120],
            )
            # Escalate via Telegram at attempts 5 and 20 (entry into stuck state).
            if attempts in (5, 20):
                try:
                    from trading_platform.polymarket.telegram_alerts import get_alerter
                    alerter = get_alerter()
                    severity = "STUCK" if stuck else "RETRYING"
                    alerter._send(
                        f"⚠️ <b>EXIT {severity}</b>\n"
                        f"#{lid} {sig_type} {exit_reason}\n"
                        f"attempts={attempts}, size=${size_usd}\n"
                        f"last: {(order_result.error_msg or '')[:200]}",
                        disable_notification=(attempts < 20),
                    )
                except Exception:
                    pass
            continue

        # 2026-05-28 BUG FIX: verify the on-chain balance actually dropped
        # before booking realized_pnl. Previously, place_market_order
        # returned success=True for status='live' (order resting in book,
        # not filled). We then recorded realized_pnl as if the position
        # had been sold — but the shares were still in the wallet.
        # On 2026-05-27 this hid ~$18 of fictitious profit (#32224, #30316
        # both marked closed at +$9.38 / +$8.82, on-chain still held 10/11
        # shares each). Polymarket dashboard showed -$8.61 reality vs my
        # +$16.26 fictional realized.
        #
        # Fix: re-query CONDITIONAL balance post-order. If significant
        # balance remains, the order didn't fully fill — leave the trade
        # open and let next cycle retry. Only record closed when balance
        # drops below threshold (1 share for dust).
        try:
            post_balance = clob.get_conditional_balance(token_id) if token_id else None
        except Exception:
            post_balance = None
        # 2026-07-06: the instant re-query RACES the GTC fill. All six
        # 17:05 exits filled within seconds of placement, but the
        # balance read ~0.3s later still showed the full holding, so
        # every one was declared DID-NOT-FILL and left open — the fills
        # then landed with no booking (ledger lost +$7.24 of realized
        # exits until reconciled by hand). Give the fill a moment and
        # re-check once before concluding the order is resting.
        if post_balance is not None and post_balance >= 1.0:
            time.sleep(10)
            try:
                _recheck = clob.get_conditional_balance(token_id) if token_id else None
                if _recheck is not None:
                    post_balance = _recheck
            except Exception:
                pass
        if post_balance is not None and post_balance >= 1.0:
            logger.warning(
                "[live-exit] %s exit DID NOT FILL for #%d (%s) — "
                "on-chain balance still %.2f (was %.2f). "
                "Leaving position open; will retry next cycle.",
                exit_reason, lid, sig_type, post_balance, shares_held,
            )
            # 2026-07-05: a resting-unfilled SELL is a failed exit attempt.
            # Not counting it here meant the 20-attempt exit_stuck cap never
            # engaged for empty-book positions — each cycle cancelled the
            # previous resting order and re-placed forever (the 11k-warning
            # livelock). With this, hopeless exits stop after 20 cycles.
            try:
                conn = get_connection()
                try:
                    conn.execute(
                        """UPDATE live_trades
                              SET exit_attempts = exit_attempts + 1,
                                  exit_first_attempt_at = COALESCE(exit_first_attempt_at, %s),
                                  exit_last_error = 'resting_unfilled',
                                  exit_stuck = CASE WHEN exit_attempts + 1 >= 20 THEN 1 ELSE 0 END
                            WHERE id = %s""",
                        (int(time.time()), lid),
                    )
                    conn.commit()
                finally:
                    try: conn.close()
                    except Exception: pass
            except Exception as _exc:
                logger.debug("[live-exit] unfilled-attempt update failed for #%d: %s", lid, _exc)
            continue

        # 2026-07-07: compute PnL in HELD-token space through the canonical
        # price_space helper. order_result.filled_price is the exit price of
        # the token we actually sold (NO-space for a SELL exit, YES-space for
        # a BUY exit); the fallback `current` is YES-space, so it must be
        # converted to held space for SELL. entry (fill_price) is YES-space.
        # The prior `(entry - fill)` SELL formula mixed YES-entry with
        # NO-exit and mis-signed/mis-scaled every NO exit.
        if order_result.filled_price is not None:
            exit_held = float(order_result.filled_price)
        else:
            exit_held = float(current) if side in ("YES", "BUY") \
                else max(0.001, 1.0 - float(current))
        entry_held = price_space.held_token_price(side, entry)  # entry is YES-space
        realized_pnl = float(shares_held) * (exit_held - entry_held)
        outcome = "win" if realized_pnl > 0 else "loss"
        exit_price_yes = round(price_space.to_yes_space(side, exit_held), 4)

        try:
            conn = get_connection()
            try:
                conn.execute(
                    """UPDATE live_trades
                       SET exit_price = ?, exit_ts = ?, outcome = ?,
                           realized_pnl = ?, exit_reason = ?
                       WHERE id = ? AND exit_ts IS NULL""",
                    (exit_price_yes, int(time.time()), outcome,
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
                f"Entry: {entry:.3f} → Exit: {exit_price_yes:.3f}\n"
                f"PnL: <b>{sign}${realized_pnl:.2f}</b> ({outcome})\n"
                f"Held {age_days:.1f} days",
                disable_notification=False,
            )
        except Exception:
            pass

        logger.info(
            "[live-exit] #%d %s @ %.3f → %s | pnl=%+.2f",
            lid, sig_type, exit_price_yes, exit_reason, realized_pnl,
        )

    return {"checked": len(rows), "exited": exited, "reasons": reasons}


def _resolve_token_id_for_paper(condition_id: str, want_yes: bool) -> str | None:
    """Resolve YES/NO token_id from condition_id via DB markets table then Gamma API."""
    if not condition_id:
        return None
    # 1. Try the local markets table first (fastest path, no network).
    try:
        from trading_platform.polymarket.db_connection import get_connection
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT yes_token_id, no_token_id FROM markets WHERE condition_id = %s",
                (condition_id,),
            ).fetchone()
        finally:
            try: conn.close()
            except Exception: pass
        if row:
            tid = row[0] if want_yes else row[1]
            return str(tid) if tid else None
    except Exception:
        pass
    # 2. Fall back to Gamma API.
    try:
        import requests as _req
        import json as _json
        r = _req.get(
            "https://gamma-api.polymarket.com/markets",
            params={"condition_ids": condition_id},
            timeout=6,
        )
        data = r.json()
        m = data[0] if isinstance(data, list) and data else None
        if m:
            tids_raw = m.get("clobTokenIds") or "[]"
            tids = _json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
            if isinstance(tids, list) and len(tids) >= 2:
                return str(tids[0]) if want_yes else str(tids[1])
    except Exception:
        pass
    return None


def check_paper_exits() -> dict[str, int]:
    """Simulate exits for open paper positions (dry_run=1, status='dry_run').

    Applies the same exit profiles as check_live_exits() but without
    placing any SELL order — just marks exit_ts/exit_price/realized_pnl
    in live_trades so paper performance is trackable.

    Resolves token_id from condition_id to get current market prices.
    Positions with unresolvable token_ids are skipped (not failed).
    """
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.clob_client import ClobClient

    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT id, condition_id, token_id, side, fill_price, size_usd,
                      shares, signal_type, signal_wallet, submitted_at,
                      COALESCE(mfe, 0) AS mfe, last_mark_price,
                      entry_price, direction
               FROM live_trades
               WHERE dry_run = 1 AND status = %s
                 AND exit_ts IS NULL
                 AND (fill_price IS NOT NULL OR entry_price IS NOT NULL)""",
            ("dry_run",),
        ).fetchall()
    finally:
        try: conn.close()
        except Exception: pass

    if not rows:
        return {"checked": 0, "exited": 0, "reasons": {}}

    clob = ClobClient()
    exited = 0
    reasons: dict[str, int] = {}
    # Cache token_id resolutions within this pass to avoid duplicate API calls.
    _tok_cache: dict[tuple[str, bool], str | None] = {}

    for row in rows:
        (lid, cid, token_id, side, fill_price, size_usd, shares,
         sig_type, src_wallet, submitted_at, mfe_dollars, last_mark,
         entry_price, direction) = row

        # Use fill_price if set, else entry_price (paper trades are logged
        # with fill_price=None and entry_price=current_price_at_signal_time).
        effective_fill = fill_price if fill_price else entry_price
        if not effective_fill:
            continue
        entry = float(effective_fill)

        # Resolve token_id if not stored.
        want_yes = (direction or side or "BUY").upper() in ("BUY", "YES")
        if not token_id:
            if cid:
                cache_key = (cid, want_yes)
                if cache_key not in _tok_cache:
                    _tok_cache[cache_key] = _resolve_token_id_for_paper(cid, want_yes)
                token_id = _tok_cache[cache_key]
        if not token_id:
            continue

        current = clob.get_mid_price(token_id)
        if current is None:
            continue
        # get_mid_price always returns YES-space price. Flip for NO side.
        if not want_yes:
            current = max(0.01, 1.0 - current)

        # Compute unrealized PnL.
        if want_yes:
            unrealized_pct = (current - entry) / max(entry, 0.01)
            paper_shares = float(shares or 0) or (float(size_usd or 0) / max(entry, 0.01))
            unrealized_dollars = unrealized_pct * float(size_usd or 0)
        else:
            unrealized_pct = (entry - current) / max(1 - entry, 0.01)
            paper_shares = float(shares or 0) or (float(size_usd or 0) / max(1.0 - entry, 0.01))
            unrealized_dollars = unrealized_pct * float(size_usd or 0)

        # Direction-specific profile (e.g., wallet_reversal SELL has tighter trail ladder)
        direction = "BUY" if want_yes else "SELL"
        prof = _exit_profile(sig_type or "", direction)

        # Update mark in DB.
        try:
            conn = get_connection()
            try:
                conn.execute(
                    """UPDATE live_trades
                       SET last_mark_price = %s, last_mark_ts = %s, unrealized_pnl = %s,
                           mae = CASE WHEN mae IS NULL OR %s < mae THEN %s ELSE mae END,
                           mfe = CASE WHEN mfe IS NULL OR %s > mfe THEN %s ELSE mfe END
                       WHERE id = %s""",
                    (current, int(time.time()), unrealized_dollars,
                     unrealized_dollars, unrealized_dollars,
                     unrealized_dollars, unrealized_dollars, lid),
                )
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.debug("paper mark update failed for #%d: %s", lid, exc)

        age_days = (time.time() - float(submitted_at or time.time())) / 86400
        mfe_pct = float(mfe_dollars or 0) / max(float(size_usd or 1), 1)
        side_key = "YES" if want_yes else "NO"
        exit_reason = _decide_exit(
            side=side_key, entry=entry, current=current,
            mfe_pct=mfe_pct, age_days=age_days, profile=prof,
        )

        if not exit_reason:
            continue

        # Simulated fill at current price (no actual order placed).
        fill = current
        realized_pnl = paper_shares * (fill - entry) if want_yes else paper_shares * (entry - fill)
        outcome = "win" if realized_pnl > 0 else "loss"

        try:
            conn = get_connection()
            try:
                conn.execute(
                    """UPDATE live_trades
                       SET exit_price = %s, exit_ts = %s, outcome = %s,
                           realized_pnl = %s, exit_reason = %s
                       WHERE id = %s""",
                    (fill, int(time.time()), outcome,
                     round(realized_pnl, 2), exit_reason, lid),
                )
                conn.commit()
            finally:
                try: conn.close()
                except Exception: pass
        except Exception as exc:
            logger.error("paper-exit DB update failed for #%d: %s", lid, exc)
            continue

        exited += 1
        reasons[exit_reason] = reasons.get(exit_reason, 0) + 1
        logger.info(
            "[paper-exit] #%d %s @ %.3f → %s | pnl=%+.2f",
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
    paper = check_paper_exits()
    logger.info(
        "[paper-monitor] checked=%d exited=%d reasons=%s",
        paper["checked"], paper["exited"], paper["reasons"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
