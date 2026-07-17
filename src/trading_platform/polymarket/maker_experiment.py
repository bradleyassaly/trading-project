"""Maker experiment: be the RESTING counterparty to near-resolution dumb flow.

WHY (2026-07-16, three independent derivations — see memory 2026-07-16-fade-losers-test):
  1. Our own taker losses were exactly the spread we paid (5.3% entry cost).
  2. The paper lane's fake alpha was unfillable spike-exits — taker fantasy.
  3. Persistent loser-wallet flow is real (lb-api-verified -$157k/-$245k 30d
     wallets) but its losses accrue to the RESTING order it crosses into, at
     fill time. Follow-fading captures nothing; being the book captures it.

WHAT: post small resting BUY orders on the COMPLEMENT (NO side) of longshot
sports markets near resolution — the exact segment where loser flow
concentrates (they buy YES longshots at 0.05-0.40 that resolve NO ~86%).
On Polymarket's CLOB a resting NO-bid at (1-p) IS the counterparty to a taker
buying YES at p (complete-set minting). We earn their crossing.

MEASUREMENT-FIRST. This is an instrument, not a strategy: every quote records
the book at post time; every fill records marks at +5m/+30m (adverse-selection
/ pickoff detection) and the final resolution outcome.

── PRE-REGISTERED EVALUATION (locked 2026-07-16, before first fill) ──────────
  Window: 14 days or 50 fills, whichever first. Run --evaluate for the report.
  Metrics: fill rate, time-to-fill, net EV per $ staked, pickoff rate
           (mark_5m <= 80% of fill price on our token).
  KILL if: pickoff_rate > 0.50 (n>=20 fills)
        or net EV/$ <= -0.10 (n>=30 fills)
        or cumulative realized loss >= $15.
  PROMOTE-CONSIDERATION if: net EV/$ >= +0.05 with n>=50 fills.
  No parameter tuning inside the window; changes = new experiment.
──────────────────────────────────────────────────────────────────────────────

SAFETY: dry-run by default (records would-be quotes; fills simulated from
market_ticks prints — OPTIMISTIC re queue priority, labeled live=0).
Live orders ONLY when MAKER_EXPERIMENT_LIVE=1 (operator-armed). Hard bounds:
MAX_OPEN_ORDERS, MAX_OUTSTANDING_USDC, per-order 5 shares (CLOB minimum),
kill-switch respected, quotes cancelled when stale / book moves / near
resolution. The experiment cannot exceed ~$25 outstanding by construction.

Run: python -m trading_platform.polymarket.maker_experiment [--status|--evaluate|--cancel-all]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from trading_platform.polymarket.db_connection import get_connection

logger = logging.getLogger(__name__)

LIVE = os.environ.get("MAKER_EXPERIMENT_LIVE", "0").strip() in ("1", "true", "yes")

# ── Bounds (env-overridable, defaults are the pre-registered values) ─────────
MAX_OPEN_ORDERS = int(os.environ.get("MAKER_MAX_OPEN_ORDERS", "5"))
MAX_OUTSTANDING_USDC = float(os.environ.get("MAKER_MAX_OUTSTANDING", "25"))
EXPERIMENT_STOP_LOSS = float(os.environ.get("MAKER_STOP_LOSS", "15"))
SHARES_PER_ORDER = 5.0            # CLOB minimum
MIN_HOURS_TO_RESOLVE = 1.0        # inside this = pickoff window, do not quote
MAX_HOURS_TO_RESOLVE = 24.0
CANCEL_UNDER_HOURS = 0.33         # cancel resting quotes <20min to resolution
MAX_QUOTE_AGE_S = 45 * 60         # stale-quote cancel
MAX_MID_DRIFT = 0.10              # book moved -> we're stale -> cancel
YES_LONGSHOT_LO, YES_LONGSHOT_HI = 0.05, 0.40   # the dumb-flow band
MIN_SPREAD = 0.02                 # only make when there is spread to earn
MIN_VOLUME_24H = 500.0
NO_QUOTE_LO, NO_QUOTE_HI = 0.55, 0.94


def _conn():
    return get_connection()


def ensure_table() -> None:
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS maker_experiment_orders (
                id BIGSERIAL PRIMARY KEY,
                posted_at BIGINT NOT NULL,
                live BIGINT NOT NULL DEFAULT 0,
                condition_id TEXT NOT NULL,
                event_slug TEXT,
                question TEXT,
                yes_token_id TEXT,
                no_token_id TEXT,
                quote_price DOUBLE PRECISION,      -- NO-space price we bid
                implied_yes DOUBLE PRECISION,      -- 1 - quote_price
                shares DOUBLE PRECISION,
                notional DOUBLE PRECISION,
                yes_bid DOUBLE PRECISION,
                yes_ask DOUBLE PRECISION,
                hours_to_resolve DOUBLE PRECISION,
                order_id TEXT,
                status TEXT NOT NULL DEFAULT 'open',  -- open|cancelled|filled|resolved|error
                cancel_reason TEXT,
                filled_at BIGINT,
                fill_price DOUBLE PRECISION,
                mark_5m DOUBLE PRECISION,
                mark_30m DOUBLE PRECISION,
                resolved_at BIGINT,
                resolves_yes BIGINT,
                realized_pnl DOUBLE PRECISION,
                pickoff BIGINT
            )
        """)
        c.commit()
    finally:
        c.close()


def _now() -> int:
    return int(time.time())


def _hours_to_resolve(end_iso: str) -> float | None:
    try:
        clean = end_iso.replace("Z", "+00:00") if end_iso.endswith("Z") else end_iso
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt - datetime.now(timezone.utc)).total_seconds() / 3600
    except Exception:
        return None


def _latest_yes_tick(c, condition_id: str, since: int | None = None) -> float | None:
    q = "SELECT price FROM market_ticks WHERE condition_id = ?"
    args: list[Any] = [condition_id]
    if since:
        q += " AND timestamp >= ?"
        args.append(since)
    q += " ORDER BY timestamp DESC LIMIT 1"
    row = c.execute(q, args).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _max_yes_tick_since(c, condition_id: str, since: int) -> float | None:
    row = c.execute(
        "SELECT MAX(price) FROM market_ticks WHERE condition_id = ? AND timestamp >= ?",
        (condition_id, since),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


import re as _re

# Hard-exclude: weather + price-target (crypto/stock/index) markets — both are
# informed/forecast-driven near resolution (pickoff traps), not dumb sports flow.
_EXCLUDE_RX = ("temperat", "weather", "rain", "snow", "degrees", "fahrenheit",
               "celsius", "wind-speed", "hit-high", "hit-low", "-hit-", "price-above",
               "price-below", "market-cap", "all-time-high", "-usd-", "bitcoin",
               "ethereum", "-coin-", "nasdaq", "s-p-500", "stock")
# Compound sports phrases — safe as substrings (unambiguous).
_SPORTS_PHRASE = ("spread", "o/u", "corners", "btts", "moneyline", "first-half",
                  "second-half", "to-win", "to-score", "total-goals", "-vs-",
                  " vs ", "vs.", "-draw", "-utd", "-fc-")
# Short/ambiguous codes — match only as whole hyphen/space tokens ("nba" must
# not match inside "coinbase"). 'draw' also here to avoid 'withdrawal' etc.
_SPORTS_TOKEN = {"fc", "cf", "atp", "wta", "itf", "nba", "nfl", "nhl", "mlb",
                 "ufc", "mls", "epl", "draw", "match", "fifa", "uefa",
                 # "[team] to win/beat" soccer+tennis markets carry no other
                 # sports token; safe here since weather+price already excluded.
                 "win", "wins", "beat", "beats", "advance", "advances",
                 "qualify", "qualifies"}


def _is_sports_market(subcategory: str | None, slug: str, question: str) -> bool:
    """Positively classify sports; hard-exclude weather + price-target markets.
    Enriched 'sports/%' is authoritative; for un-enriched NULL/'' rows require a
    sports slug/question pattern and no excluded keyword. (categorize_slug is
    useless — it labels these sports slugs 'other'.)"""
    sub = (subcategory or "").lower()
    if sub.startswith("sports/"):
        return True
    if sub:
        return False  # enriched as something else (science/politics/crypto/...)
    text = f"{slug or ''} {question or ''}".lower()
    if any(w in text for w in _EXCLUDE_RX):
        return False
    if any(p in text for p in _SPORTS_PHRASE):
        return True
    tokens = set(_re.split(r"[^a-z0-9]+", text))
    return bool(tokens & _SPORTS_TOKEN)


def _kill_switch_active() -> bool:
    try:
        from trading_platform.polymarket.kill_switch import KillSwitch
        ks = KillSwitch(os.environ.get(
            "WALLET_DB_PATH", "data/polymarket/wallet_intelligence.db"))
        active, _ = ks.is_emergency_stopped()
        return bool(active)
    except Exception:
        return True  # fail closed: no kill-switch visibility -> no new quotes


# ── Lifecycle: manage existing orders ────────────────────────────────────────

def manage_open_orders(c, clob) -> None:
    now = _now()
    rows = c.execute(
        """SELECT id, live, condition_id, no_token_id, yes_token_id, quote_price,
                  implied_yes, shares, posted_at, order_id, yes_bid, yes_ask
           FROM maker_experiment_orders WHERE status = 'open'"""
    ).fetchall()
    for (oid, live, cid, no_tid, yes_tid, q, impl_yes, shares, posted_at,
         order_id, yes_bid0, yes_ask0) in rows:
        # 1. Fill check
        filled_price = None
        if live and order_id:
            st = clob.get_order_status(order_id)
            if st["status"] == "matched" or st["size_matched"] >= float(shares):
                filled_price = st["filled_price"] or q
        elif not live:
            # dry-run fill proxy: a YES print at/above our implied ask means a
            # taker paid our level (their NO-mint counterparty would be us).
            # market_ticks is EMPTY for these thin near-resolution markets
            # (verified 2026-07-16: 0/50 quotes had tick coverage), so poll the
            # CLOB last-trade price on the YES token directly, with market_ticks
            # as a fallback for WS-subscribed markets. Per-cycle sampling misses
            # intra-cycle peaks => dry fills are now a LOWER bound (was: always
            # 0). Queue priority still ignored => optimistic on fills it DOES see.
            hi = clob.get_last_price(yes_tid) if yes_tid else None
            tick_hi = _max_yes_tick_since(c, cid, int(posted_at))
            hi = max([h for h in (hi, tick_hi) if h is not None], default=None)
            if hi is not None and hi >= float(impl_yes):
                filled_price = q
        if filled_price is not None:
            c.execute(
                "UPDATE maker_experiment_orders SET status='filled', filled_at=?, "
                "fill_price=? WHERE id=?",
                (now, float(filled_price), oid))
            c.commit()
            logger.info("[maker] order %s FILLED @ %.2f (live=%s)", oid, filled_price, live)
            continue

        # 2. Cancel conditions
        reason = None
        end_row = c.execute(
            "SELECT end_date_iso FROM markets WHERE condition_id=? LIMIT 1", (cid,)
        ).fetchone()
        htr = _hours_to_resolve(end_row[0]) if end_row and end_row[0] else None
        if htr is not None and htr < CANCEL_UNDER_HOURS:
            reason = "near_resolution"
        elif now - int(posted_at) > MAX_QUOTE_AGE_S:
            reason = "stale_quote"
        else:
            yes_now = _latest_yes_tick(c, cid)
            mid0 = (float(yes_bid0 or 0) + float(yes_ask0 or 0)) / 2
            if yes_now is not None and mid0 and abs(yes_now - mid0) > MAX_MID_DRIFT:
                reason = "book_moved"
        if reason:
            ok = True
            if live and order_id:
                ok = clob.cancel_order_by_id(order_id)
            if ok:
                c.execute(
                    "UPDATE maker_experiment_orders SET status='cancelled', "
                    "cancel_reason=? WHERE id=?", (reason, oid))
                c.commit()
                logger.info("[maker] order %s cancelled (%s)", oid, reason)


def update_fill_marks_and_resolutions(c) -> None:
    """Post-fill measurement: +5m/+30m marks (pickoff) and resolution booking."""
    now = _now()
    rows = c.execute(
        """SELECT id, condition_id, fill_price, filled_at, mark_5m, mark_30m, shares, quote_price
           FROM maker_experiment_orders WHERE status = 'filled'"""
    ).fetchall()
    for oid, cid, fp, filled_at, m5, m30, shares, q in rows:
        # marks in OUR token's space (NO) = 1 - yes_tick
        if m5 is None and now - int(filled_at) >= 300:
            y = _latest_yes_tick(c, cid)
            if y is not None:
                mark = 1.0 - y
                pickoff = 1 if mark <= float(fp) * 0.80 else 0
                c.execute("UPDATE maker_experiment_orders SET mark_5m=?, pickoff=? WHERE id=?",
                          (mark, pickoff, oid))
                c.commit()
        if m30 is None and now - int(filled_at) >= 1800:
            y = _latest_yes_tick(c, cid)
            if y is not None:
                c.execute("UPDATE maker_experiment_orders SET mark_30m=? WHERE id=?",
                          (1.0 - y, oid))
                c.commit()
        # resolution booking
        res = c.execute(
            "SELECT resolves_yes FROM market_resolutions WHERE condition_id=? LIMIT 1",
            (cid,)).fetchone()
        if res is not None and res[0] is not None:
            resolves_yes = int(res[0])
            # we hold NO: worth 1 if resolves NO, 0 if YES
            pnl = float(shares) * ((1.0 - float(fp)) if resolves_yes == 0 else (-float(fp)))
            c.execute(
                "UPDATE maker_experiment_orders SET status='resolved', resolved_at=?, "
                "resolves_yes=?, realized_pnl=? WHERE id=?",
                (now, resolves_yes, round(pnl, 4), oid))
            c.commit()
            logger.info("[maker] order %s RESOLVED %s pnl=%.2f", oid,
                        "YES" if resolves_yes else "NO", pnl)


# ── Quoting: post new resting orders ─────────────────────────────────────────

def _experiment_totals(c) -> dict:
    row = c.execute(
        """SELECT COUNT(*) FILTER (WHERE status='open'),
                  COALESCE(SUM(notional) FILTER (WHERE status IN ('open','filled')), 0),
                  COALESCE(SUM(realized_pnl) FILTER (WHERE status != 'voided'), 0)
           FROM maker_experiment_orders"""
    ).fetchone()
    return {"open": int(row[0] or 0), "outstanding": float(row[1] or 0),
            "realized": float(row[2] or 0)}


def post_new_quotes(c, clob) -> int:
    tot = _experiment_totals(c)
    if tot["realized"] <= -EXPERIMENT_STOP_LOSS:
        logger.warning("[maker] experiment stop-loss hit ($%.2f) — no new quotes",
                       tot["realized"])
        return 0
    slots = MAX_OPEN_ORDERS - tot["open"]
    if slots <= 0 or tot["outstanding"] >= MAX_OUTSTANDING_USDC:
        return 0

    # Universe: sports ONLY. The NULL-subcategory bucket is NOT a sports
    # firehose (verified 2026-07-16: 525 other / 124 weather / 49 sports in a
    # 24h window) — the first cut quoted weather temp markets, the exact
    # forecast-informed pickoff trap this experiment excludes. So require
    # sports POSITIVELY: enriched sports/% OR a NULL/'' subcategory whose slug
    # matches a sports pattern AND is not weather (see _is_sports_slug).
    rows = c.execute(
        """SELECT condition_id, slug, question, end_date_iso, yes_token_id,
                  no_token_id, outcome_prices, volume_24h, subcategory
           FROM markets
           WHERE end_date_iso IS NOT NULL
             AND end_date_iso > to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS')
             AND (subcategory LIKE 'sports/%' OR subcategory IS NULL
                  OR subcategory = '')
           ORDER BY end_date_iso ASC LIMIT 600"""
    ).fetchall()
    posted = 0
    for cid, slug, question, end_iso, yes_tid, no_tid, prices_raw, vol, subcat in rows:
        if posted >= slots:
            break
        if not yes_tid or not no_tid:
            continue
        if not _is_sports_market(subcat, slug, question):
            continue
        if vol is not None and float(vol) < MIN_VOLUME_24H:
            continue
        htr = _hours_to_resolve(end_iso or "")
        if htr is None or not (MIN_HOURS_TO_RESOLVE <= htr <= MAX_HOURS_TO_RESOLVE):
            continue
        try:
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            yes_price = float(prices[0]) if isinstance(prices, list) and prices else None
        except Exception:
            yes_price = None
        if yes_price is None or not (YES_LONGSHOT_LO <= yes_price <= YES_LONGSHOT_HI):
            continue
        # one order per condition, ever (measurement independence)
        dup = c.execute(
            "SELECT 1 FROM maker_experiment_orders "
            "WHERE condition_id=? AND status != 'voided' LIMIT 1",
            (cid,)).fetchone()
        if dup:
            continue

        book = clob.get_order_book(yes_tid)
        bids, asks = book.get("bids") or [], book.get("asks") or []
        if not bids or not asks:
            continue
        yes_bid, yes_ask = float(bids[0]["price"]), float(asks[0]["price"])
        if yes_ask - yes_bid < MIN_SPREAD:
            continue
        tick = 0.01
        implied_yes = round(yes_ask - tick, 2)      # improve the ask by a tick
        if implied_yes <= yes_bid:                   # never cross
            implied_yes = round(yes_ask, 2)
        q_no = round(1.0 - implied_yes, 2)
        if not (NO_QUOTE_LO <= q_no <= NO_QUOTE_HI):
            continue
        notional = round(SHARES_PER_ORDER * q_no, 2)
        if tot["outstanding"] + notional > MAX_OUTSTANDING_USDC:
            continue

        order_id = None
        if LIVE:
            res = clob.place_resting_limit(no_tid, "BUY", q_no, SHARES_PER_ORDER)
            if not res.success:
                logger.warning("[maker] post failed for %s: %s", cid[:14], res.error_msg)
                continue
            order_id = res.order_id
        c.execute(
            """INSERT INTO maker_experiment_orders
               (posted_at, live, condition_id, event_slug, question, yes_token_id,
                no_token_id, quote_price, implied_yes, shares, notional, yes_bid,
                yes_ask, hours_to_resolve, order_id, status)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'open')""",
            (_now(), 1 if LIVE else 0, cid, slug, (question or "")[:200], yes_tid,
             no_tid, q_no, implied_yes, SHARES_PER_ORDER, notional, yes_bid,
             yes_ask, round(htr, 2), order_id))
        c.commit()
        tot["outstanding"] += notional
        posted += 1
        logger.info("[maker] %s quote NO %.2f (implied YES ask %.2f, book %.2f/%.2f, "
                    "%.1fh to res) on %s", "LIVE" if LIVE else "DRY",
                    q_no, implied_yes, yes_bid, yes_ask, htr, (question or "")[:50])
    return posted


# ── Cycle + CLI ──────────────────────────────────────────────────────────────

def run_cycle() -> dict:
    ensure_table()
    from trading_platform.polymarket.clob_client import ClobClient
    clob = ClobClient()
    c = _conn()
    try:
        if _kill_switch_active():
            # manage/cancel existing, never post new while halted
            manage_open_orders(c, clob)
            update_fill_marks_and_resolutions(c)
            logger.warning("[maker] kill switch active — managed existing, no new quotes")
            return {"posted": 0, "halted": True}
        manage_open_orders(c, clob)
        update_fill_marks_and_resolutions(c)
        posted = post_new_quotes(c, clob)
        t = _experiment_totals(c)
        print(f"[maker] cycle done: posted={posted} open={t['open']} "
              f"outstanding=${t['outstanding']:.2f} realized=${t['realized']:.2f} "
              f"mode={'LIVE' if LIVE else 'DRY-RUN'}")
        return {"posted": posted, **t}
    finally:
        c.close()


def cancel_all() -> None:
    ensure_table()
    from trading_platform.polymarket.clob_client import ClobClient
    clob = ClobClient()
    c = _conn()
    try:
        rows = c.execute(
            "SELECT id, live, order_id FROM maker_experiment_orders WHERE status='open'"
        ).fetchall()
        for oid, live, order_id in rows:
            if live and order_id:
                clob.cancel_order_by_id(order_id)
            c.execute("UPDATE maker_experiment_orders SET status='cancelled', "
                      "cancel_reason='operator' WHERE id=?", (oid,))
        c.commit()
        print(f"cancelled {len(rows)} open orders")
    finally:
        c.close()


def evaluate() -> None:
    """The pre-registered evaluation report."""
    ensure_table()
    c = _conn()
    try:
        row = c.execute(
            """SELECT COUNT(*),
                      COUNT(*) FILTER (WHERE status IN ('filled','resolved')),
                      COUNT(*) FILTER (WHERE status='resolved'),
                      COALESCE(SUM(realized_pnl),0),
                      COALESCE(SUM(notional) FILTER (WHERE status='resolved'),0),
                      COUNT(*) FILTER (WHERE pickoff=1),
                      COUNT(*) FILTER (WHERE pickoff IS NOT NULL),
                      AVG(filled_at - posted_at) FILTER (WHERE filled_at IS NOT NULL),
                      MIN(posted_at)
               FROM maker_experiment_orders WHERE status != 'voided'"""
        ).fetchone()
        (n_quotes, n_fills, n_resolved, pnl, resolved_notional,
         n_pickoff, n_marked, avg_ttf, first_post) = row
        days = ((_now() - int(first_post)) / 86400) if first_post else 0
        print(f"\nMAKER EXPERIMENT — pre-registered evaluation "
              f"(day {days:.1f} of 14, {n_fills or 0}/50 fills)")
        print(f"  quotes: {n_quotes}  fills: {n_fills}  resolved: {n_resolved}")
        if n_quotes:
            print(f"  fill rate: {(n_fills or 0)/n_quotes*100:.0f}%"
                  + (f"   avg time-to-fill: {(avg_ttf or 0)/60:.0f}m" if avg_ttf else ""))
        if resolved_notional:
            print(f"  net EV/$ (resolved): {pnl/resolved_notional:+.3f}   realized: ${pnl:+.2f}")
        if n_marked:
            pr = (n_pickoff or 0) / n_marked
            print(f"  pickoff rate: {pr:.0%} ({n_pickoff}/{n_marked} marked fills)")
        print("\n  KILL if pickoff>0.50 (n>=20) | EV/$<=-0.10 (n>=30) | loss>=$15")
        print("  PROMOTE-consider if EV/$>=+0.05 (n>=50)")
        # split live vs dry
        for live in (1, 0):
            r = c.execute(
                """SELECT COUNT(*) FILTER (WHERE status IN ('filled','resolved')),
                          COALESCE(SUM(realized_pnl),0)
                   FROM maker_experiment_orders
                   WHERE live=? AND status != 'voided'""", (live,)).fetchone()
            if r and r[0]:
                print(f"  [{'LIVE' if live else 'DRY (optimistic fills)'}] "
                      f"fills={r[0]} realized=${r[1]:+.2f}")
    finally:
        c.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--evaluate", action="store_true")
    ap.add_argument("--cancel-all", action="store_true")
    args = ap.parse_args()
    if args.cancel_all:
        cancel_all()
    elif args.status or args.evaluate:
        evaluate()
    else:
        run_cycle()


if __name__ == "__main__":
    main()
