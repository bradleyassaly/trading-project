"""Auto-redeem reconciler — book live_trades rows Polymarket settled for us.

Polymarket AUTO-REDEEMS resolved winning positions: the winning conditional
tokens are burned and the USDC payout lands in the wallet WITHOUT a SELL fill
(verified 2026-07-22: data-api ``/activity?type=REDEEM`` shows 66 redemptions
for our funder). Our three existing live_trades closers all miss this:

  * ``order_reconciler`` only resolves resting/partial ENTRY orders.
  * ``book_resolved_positions`` books from ``/positions`` — but an auto-redeemed
    token is ABSENT from ``/positions`` entirely, so it SKIPs the row
    ("not in data-api — verify manually").
  * ``live_position_monitor`` dust settlement books ``proceeds - cost`` from
    SELL fills only; it never sees the REDEEM payout, so a redeemed winner
    reads as a loss (or stays open).

So a row whose position closed via auto-redemption sits ``status='matched'`` /
``exit_ts IS NULL`` forever: the win is never booked (``realized_pnl_cumulative``
understates), and it pollutes open-position counts. The hourly equity snapshot
already zero-values these truth-absent tokens (commit 074c1bc), so equity is
right — this reconciler moves that already-excluded value from "phantom open
position" into realized-PnL history. Cash is untouched; equity is conserved.

Truth model — a live_trades row is settled from the funder wallet's OWN
on-chain cash flow, matched by token (BUY/SELL of ``asset``) and condition
(REDEEM carries ``conditionId`` + ``usdcSize`` payout, its ``asset`` is empty):

    realized_pnl = (Σ SELL usdcSize + Σ REDEEM usdcSize) − Σ BUY usdcSize

This subsumes every partial-lifecycle shape we actually see. The real ghosts
(2026-07-23) had bought 5 shares, partially SOLD 4 (an unbooked exit), then
auto-redeemed the last 1 — booking only the redeem leg against the 5-share
basis would be wrong; the full cash-flow is correct by construction. Win/loss
falls out of the sign (a position that sold 4 at a loss then redeemed 1 is a
net LOSS even though its last share "won").

Booking rules (only for rows ABSENT from a SUCCESSFUL ``/positions`` fetch — a
present token is still held and belongs to the other closers):

  * redeem payout > 0                    → exit_reason='reconciled_redeem',
                                           exit_price = payout/shares (≈1.0)
  * no redeem, sell proceeds > 0         → exit_reason='reconciled_dataapi_sell'
                                           (full sell-out that was never booked)
  * no redeem, no sells, market ENDED    → LOSS, exit_price=0.0 (token expired
                                           worthless), exit_reason='reconciled_expired'
  * otherwise (can't tell)               → None: leave OPEN, never guess.

DRY-RUN BY DEFAULT — prints proposed bookings; ``--apply`` commits. Idempotent
(only touches ``exit_ts IS NULL`` rows).

Usage:
    python -m trading_platform.polymarket.redeem_reconciler            # dry-run
    python -m trading_platform.polymarket.redeem_reconciler --apply    # commit
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
_HDRS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
# Below this on-chain residual we treat a position as fully closed (Polymarket
# /positions hides sub-0.01 dust; the CLOB minimum sell size is 1 token).
_RESIDUAL_DUST_SHARES = 0.5


class RedeemBooking(NamedTuple):
    """A proposed close for one open live_trades row."""
    realized_pnl: float
    exit_price: float
    outcome: str          # 'win' | 'loss'
    exit_reason: str      # reconciled_redeem | reconciled_dataapi_sell | reconciled_expired
    buy_cost: float
    sell_proceeds: float
    redeem_payout: float


def _sum_usdc(rows: list[dict]) -> float:
    """Σ actual USDC across activity rows (usdcSize is Polymarket's own cash
    figure; fall back to size×price only when it is missing)."""
    total = 0.0
    for r in rows:
        u = r.get("usdcSize")
        if u is not None:
            total += float(u)
        else:
            total += float(r.get("size") or 0) * float(r.get("price") or 0)
    return total


def _sum_size(rows: list[dict]) -> float:
    return sum(float(r.get("size") or 0) for r in rows)


def _reconstruct_cost(
    direction: str, price: float | None, shares: float | None, size_usd: float,
) -> float | None:
    """Fallback entry cost when BUY activity is unavailable (pagination / old
    fill). Held-token basis: BUY holds YES at price/share, SELL holds NO at
    (1-price)/share. Prefers DB shares, else derives from committed capital."""
    if price is None:
        return None
    per_share = float(price) if (direction or "BUY").upper() == "BUY" \
        else max(1.0 - float(price), 0.01)
    if per_share <= 0:
        return None
    qty = float(shares) if (shares and float(shares) > 0) else None
    if qty is None and size_usd and size_usd > 0:
        qty = float(size_usd) / per_share
    if not qty or qty <= 0:
        return None
    return qty * per_share


def settle_redeem_from_activity(
    *,
    direction: str,
    fill_price: float | None,
    entry_price: float | None,
    shares: float | None,
    size_usd: float,
    in_positions: bool,
    market_ended: bool,
    buys: list[dict],
    sells: list[dict],
    redeems: list[dict],
) -> RedeemBooking | None:
    """Decide how to close one open live_trades row from wallet cash-flow truth.

    Pure function — all I/O is done by the caller and passed in. Returns a
    :class:`RedeemBooking` or ``None`` when the row must be LEFT OPEN (still
    held per Polymarket, or outcome genuinely unknown — never guess).
    """
    # A token still in /positions is still held on-chain: the /positions-based
    # closer (book_resolved_positions) or the live monitor owns it, not us.
    if in_positions:
        return None

    buy_cost = _sum_usdc(buys)
    sell_proceeds = _sum_usdc(sells)
    redeem_payout = _sum_usdc(redeems)
    has_redeem = redeem_payout > 0
    has_sells = sell_proceeds > 0

    # Absent from /positions but no positive cash event AND the market has not
    # ended → we cannot tell win/loss/still-settling. Leave it open; a later
    # pass (redeem lands, or the market ends) will settle it.
    if not has_redeem and not has_sells and not market_ended:
        return None

    # Cost basis: actual on-chain BUY spend is truth (matches the USDC that
    # left the wallet); reconstruct from the DB row only if activity lacks it.
    cost = buy_cost
    if cost <= 0:
        rebuilt = _reconstruct_cost(direction, fill_price or entry_price, shares, size_usd)
        if rebuilt is None:
            return None  # can't price the entry — don't guess
        cost = rebuilt

    proceeds = sell_proceeds + redeem_payout
    realized = round(proceeds - cost, 4)
    outcome = "win" if realized > 0 else "loss"

    if has_redeem:
        redeem_shares = _sum_size(redeems)
        exit_price = round(redeem_payout / redeem_shares, 4) if redeem_shares > 0 else 1.0
        exit_reason = "reconciled_redeem"
    elif has_sells:
        # Full sell-out that was never booked (the declared-but-unwritten
        # reason in exit_counterfactual.PRE_RES_EXITS).
        exit_price = 0.0
        exit_reason = "reconciled_dataapi_sell"
    else:
        # Market ended, token gone, no payout → expired worthless.
        exit_price = 0.0
        exit_reason = "reconciled_expired"

    return RedeemBooking(
        realized_pnl=realized, exit_price=exit_price, outcome=outcome,
        exit_reason=exit_reason, buy_cost=round(buy_cost, 4),
        sell_proceeds=round(sell_proceeds, 4), redeem_payout=round(redeem_payout, 4),
    )


# ── I/O orchestration ───────────────────────────────────────────────────────

def _fetch_activity(wallet: str, kind: str, max_pages: int = 12) -> list[dict]:
    """Page the funder wallet's /activity feed for one event type.

    Needs a browser User-Agent (bare urllib gets 403). Returns [] on any
    failure — the reconciler must fail SAFE (book nothing) rather than crash.
    """
    import urllib.request
    import json as _json
    out: list[dict] = []
    try:
        for page in range(max_pages):
            url = (f"{DATA_API}/activity?user={wallet}"
                   f"&limit=500&offset={page * 500}&type={kind}")
            req = urllib.request.Request(url, headers=_HDRS)
            with urllib.request.urlopen(req, timeout=20) as r:
                chunk = _json.loads(r.read().decode())
            out.extend(chunk)
            if len(chunk) < 500:
                break
    except Exception as exc:
        logger.warning("[redeem-reconcile] /activity %s fetch failed: %s", kind, exc)
    return out


def reconcile_redeemed_positions(apply: bool = False) -> dict[str, Any]:
    """One pass: close open live_trades rows Polymarket auto-redeemed/settled.

    Returns a structured report. Writes nothing unless ``apply=True``.
    """
    from trading_platform.polymarket.db_connection import get_connection
    from trading_platform.polymarket.position_fetcher import PositionFetcher

    report: dict[str, Any] = {
        "ts": int(time.time()), "checked": 0, "booked": 0,
        "left_open": 0, "still_held": 0, "realized_delta": 0.0,
        "bookings": [], "error": None,
    }

    wallet = (os.environ.get("POLYMARKET_FUNDER_ADDRESS")
              or os.environ.get("POLYMARKET_WALLET_ADDRESS"))
    if not wallet:
        report["error"] = "no POLYMARKET_FUNDER_ADDRESS / POLYMARKET_WALLET_ADDRESS"
        return report

    # Ground truth: what Polymarket says we STILL hold. An empty/failed fetch
    # must NOT be read as "everything is gone" — that would mass-book every
    # open row as settled. Require a non-empty response before trusting absence.
    positions = PositionFetcher().fetch_wallet_positions(wallet)
    if not positions:
        report["error"] = "empty /positions response — skipping (fail-safe)"
        return report
    held_tokens = {str(p.get("asset")) for p in positions}
    held_cids = {str(p.get("conditionId")) for p in positions}

    # Wallet cash-flow: BUY/SELL by token, REDEEM by condition.
    trade_acts = _fetch_activity(wallet, "TRADE")
    redeem_acts = _fetch_activity(wallet, "REDEEM")
    buys_by_tok: dict[str, list[dict]] = {}
    sells_by_tok: dict[str, list[dict]] = {}
    for t in trade_acts:
        side = (t.get("side") or "").upper()
        tok = str(t.get("asset"))
        if side == "BUY":
            buys_by_tok.setdefault(tok, []).append(t)
        elif side == "SELL":
            sells_by_tok.setdefault(tok, []).append(t)
    redeems_by_cid: dict[str, list[dict]] = {}
    for a in redeem_acts:
        redeems_by_cid.setdefault(str(a.get("conditionId")), []).append(a)

    conn = get_connection()
    now = int(time.time())
    to_book: list[tuple] = []
    try:
        rows = conn.execute(
            """SELECT id, token_id, condition_id, direction, side, fill_price,
                      entry_price, shares, size_usd, resolution_date,
                      LEFT(question, 40) AS q
                 FROM live_trades
                WHERE dry_run = 0 AND exit_ts IS NULL
                  AND status IN ('submitted', 'live', 'matched')
                  AND token_id IS NOT NULL
                ORDER BY id"""
        ).fetchall()
        report["checked"] = len(rows)

        for (lid, tok, cid, direction, side, fp, ep, sh, sz, resdate, q) in rows:
            tok = str(tok)
            cid = str(cid)
            in_positions = tok in held_tokens or cid in held_cids
            # A row is settled only if its ON-CHAIN residual is gone; guard
            # redeem attribution against double-claim across rows sharing a cid.
            redeems = redeems_by_cid.get(cid, [])
            booking = settle_redeem_from_activity(
                direction=direction or side or "BUY",
                fill_price=float(fp) if fp is not None else None,
                entry_price=float(ep) if ep is not None else None,
                shares=float(sh) if sh is not None else None,
                size_usd=float(sz or 0),
                in_positions=in_positions,
                market_ended=bool(resdate and float(resdate) < now),
                buys=buys_by_tok.get(tok, []),
                sells=sells_by_tok.get(tok, []),
                redeems=redeems,
            )
            if booking is None:
                if in_positions:
                    report["still_held"] += 1
                else:
                    report["left_open"] += 1
                continue
            # Consume the redeem rows so a second open row on the same
            # condition can't double-count the same payout.
            if booking.redeem_payout > 0:
                redeems_by_cid[cid] = []
            # Economic resolution date: keep the existing one; else redeem ts /
            # market end / now (kill-switch loss windows key on this).
            res_ts = int(resdate) if resdate else None
            if res_ts is None and redeems:
                res_ts = int(min(int(r.get("timestamp") or now) for r in redeems))
            to_book.append((lid, booking, res_ts or now, q))
            report["realized_delta"] += booking.realized_pnl
            report["bookings"].append({
                "id": int(lid), "outcome": booking.outcome,
                "realized_pnl": booking.realized_pnl,
                "exit_price": booking.exit_price, "exit_reason": booking.exit_reason,
                "buy_cost": booking.buy_cost, "sell_proceeds": booking.sell_proceeds,
                "redeem_payout": booking.redeem_payout, "question": q,
            })

        report["booked"] = len(to_book)
        report["realized_delta"] = round(report["realized_delta"], 4)

        if apply and to_book:
            for lid, booking, res_ts, _q in to_book:
                conn.execute(
                    """UPDATE live_trades
                          SET exit_ts = %s, exit_price = %s, outcome = %s,
                              realized_pnl = %s, exit_reason = %s,
                              resolution_date = COALESCE(resolution_date, %s)
                        WHERE id = %s AND exit_ts IS NULL""",
                    (now, booking.exit_price, booking.outcome,
                     round(booking.realized_pnl, 4), booking.exit_reason,
                     res_ts, lid),
                )
            conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return report


def _print_report(report: dict[str, Any], apply: bool) -> None:
    if report.get("error"):
        print(f"[redeem-reconcile] SKIP: {report['error']}")
        return
    print(f"[redeem-reconcile] checked={report['checked']} "
          f"still_held={report['still_held']} left_open={report['left_open']} "
          f"→ book {report['booked']} rows, realized Δ ${report['realized_delta']:+.4f}")
    if report["bookings"]:
        print(f"  {'#id':<8}{'outcome':<7}{'reason':<24}"
              f"{'buy$':<9}{'sell$':<9}{'red$':<8}{'realized':<10}q")
        for b in report["bookings"]:
            print(f"  {b['id']:<7} {b['outcome']:<7}{b['exit_reason']:<24}"
                  f"{b['buy_cost']:<9.4f}{b['sell_proceeds']:<9.4f}"
                  f"{b['redeem_payout']:<8.4f}{b['realized_pnl']:<+10.4f}{b['question']}")
    if not apply:
        print("\nDRY-RUN — no writes. Re-run with --apply to commit.")
    elif report["booked"]:
        print(f"\nCOMMITTED {report['booked']} bookings.")


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Commit the bookings (default: dry-run, no writes)")
    args = ap.parse_args()
    try:
        from trading_platform.polymarket.logging_config import setup_logging
        setup_logging(service="redeem-reconcile")
    except Exception:
        logging.basicConfig(level=logging.INFO)
    report = reconcile_redeemed_positions(apply=args.apply)
    _print_report(report, args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
