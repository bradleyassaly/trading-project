"""Data-layer truth test: do OUR per-wallet numbers match Polymarket's public API?

Competitor tools (PolySmartWallet, Predicts.guru, Polycopy...) are built on
Polymarket's public data — the leaderboard API (lb-api) and data-api. If our
wallet analytics diverge from those, our data layer is worse than competitors'
and every wallet-level decision is suspect.

Two tests, read-only:
  1. Window-aligned aggregates per wallet: lb-api profit/volume (week|month)
     vs our wallet_trades aggregates over the same rolling window.
       - VOLUME mismatch  -> ingestion completeness problem (missing trades).
       - PROFIT mismatch  -> booking/methodology problem (pnl calc, resolution
         attribution, unrealized handling).
  2. (--activity-diff WALLET) Per-trade diff vs data-api /activity for the last
     N days: exact count + notional of trades they report vs rows we hold.

Usage:
  python scripts/wallet_truth_compare.py                # aggregate test, top wallets
  python scripts/wallet_truth_compare.py --activity-diff 0xabc... --days 7
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request

from trading_platform.polymarket.db_connection import get_connection

HDRS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126",
    "Accept": "application/json",
}
LB = "https://lb-api.polymarket.com"
DATA = "https://data-api.polymarket.com"


def _get(url: str, retries: int = 3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HDRS)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:  # transient 429/5xx — back off and retry
            last = e
            time.sleep(1.5 * (i + 1))
    raise last


def lb_amount(kind: str, window: str, wallet: str) -> float | None:
    """kind: profit|volume; window: day|week|month|all."""
    try:
        rows = _get(f"{LB}/{kind}?window={window}&limit=1&address={wallet}")
        if isinstance(rows, list) and rows:
            return float(rows[0].get("amount") or 0)
    except Exception as e:
        print(f"    [warn] lb-api {kind}/{window} failed for {wallet[:10]}: {str(e)[:60]}")
    return None


def our_stats(conn, wallet: str, days: int):
    lo = int(time.time()) - days * 86400
    # volume = USDC notional (size is SHARES; usdc = shares*price).
    # pnl: FIFO realized (realized_pnl_closed, matches leaderboard methodology)
    # with legacy resolution-only pnl as fallback where FIFO hasn't run.
    row = conn.execute(
        """SELECT COUNT(*),
                  COALESCE(SUM(ABS(size * price)), 0),
                  COALESCE(SUM(COALESCE(realized_pnl_closed,
                               CASE WHEN pnl_reliable=1 AND market_resolved=1
                                    THEN pnl END)), 0)
           FROM wallet_trades WHERE wallet = ? AND timestamp >= ?""",
        (wallet, lo),
    ).fetchone()
    return {"n": int(row[0] or 0), "volume": float(row[1] or 0), "pnl": float(row[2] or 0)}


def aggregate_test(conn, wallets: list[str]):
    print(f"\n{'wallet':<14} {'win':>5} | {'PM volume':>12} {'our volume':>12} {'vol Δ%':>7} | "
          f"{'PM profit':>11} {'our pnl':>11}")
    print("-" * 92)
    for w in wallets:
        for window, days in (("7d", 7), ("30d", 30)):
            pm_vol = lb_amount("volume", window, w)
            pm_pft = lb_amount("profit", window, w)
            ours = our_stats(conn, w, days)
            if pm_vol is None:
                continue
            dv = (ours["volume"] - pm_vol) / pm_vol * 100 if pm_vol else (0.0 if ours["volume"] == 0 else 999.0)
            print(f"{w[:12]:<14} {window:>5} | {pm_vol:>12,.0f} {ours['volume']:>12,.0f} {dv:>+6.0f}% | "
                  f"{(pm_pft if pm_pft is not None else float('nan')):>11,.0f} {ours['pnl']:>11,.0f}")
        print()


def activity_diff(conn, wallet: str, days: int, max_pages: int = 4):
    """Per-trade completeness diff vs data-api /activity (TRADE events only).

    High-frequency wallets do 30k+ trades/day, so fetching a full N-day history
    is impractical. Instead we SAMPLE: fetch the most recent `max_pages`x500
    trades, take their exact [oldest, newest] span, and compare our rows over
    the SAME span — a fair completeness estimate without deep pagination.
    `days` still caps the span for slow wallets."""
    lo_cap = int(time.time()) - days * 86400
    theirs, offset = [], 0
    for _ in range(max_pages):
        rows = _get(f"{DATA}/activity?user={wallet}&limit=500&offset={offset}&type=TRADE")
        if not rows:
            break
        theirs.extend(rows)
        offset += 500
        time.sleep(0.6)  # be polite; avoid transient 429s
        if len(rows) < 500 or int(rows[-1].get("timestamp") or 0) < lo_cap:
            break
    theirs = [t for t in theirs if int(t.get("timestamp") or 0) >= lo_cap]
    if not theirs:
        print(f"\nActivity diff — {wallet}: data-api returned NO trades "
              f"(EOA-direct trader not indexed by proxy-wallet APIs, or inactive)")
        return
    span_lo = min(int(t["timestamp"]) for t in theirs)
    span_hi = max(int(t["timestamp"]) for t in theirs)
    t_n = len(theirs)
    t_usdc = sum(float(t.get("usdcSize") or 0) for t in theirs)

    row = conn.execute(
        """SELECT COUNT(*), COALESCE(SUM(ABS(size)),0),
                  COALESCE(SUM(ABS(size*price)),0)
           FROM wallet_trades
           WHERE wallet = ? AND timestamp >= ? AND timestamp <= ?""",
        (wallet, span_lo, span_hi),
    ).fetchone()
    o_n, o_size, o_usdc = int(row[0] or 0), float(row[1] or 0), float(row[2] or 0)

    span_h = (span_hi - span_lo) / 3600
    print(f"\nActivity diff — {wallet}")
    print(f"  sampled span: {time.strftime('%m-%d %H:%M', time.gmtime(span_lo))} → "
          f"{time.strftime('%m-%d %H:%M', time.gmtime(span_hi))} UTC ({span_h:.1f}h)")
    print(f"  Polymarket /activity : {t_n:>6} trades  ${t_usdc:>12,.0f} usdc notional")
    print(f"  our wallet_trades    : {o_n:>6} trades  ${o_usdc:>12,.0f} (size*price)  "
          f"[sum|size|={o_size:,.0f}]")
    print(f"  coverage: {o_n/t_n*100:.0f}% of trades, "
          f"{(o_usdc/t_usdc*100 if t_usdc else 0):.0f}% of usdc notional")

    # sample of their most recent 5 trades with our match status (by tx hash)
    print("\n  their 5 most recent, matched against our rows (by tx hash):")
    for t in theirs[:5]:
        h = (t.get("transactionHash") or "").lower()
        ts = int(t.get("timestamp") or 0)
        m = conn.execute(
            "SELECT COUNT(*) FROM wallet_trades WHERE wallet=? AND transaction_hash=?",
            (wallet, h),
        ).fetchone()
        have = "HAVE" if m and m[0] else "MISSING"
        print(f"    {time.strftime('%m-%d %H:%M', time.gmtime(ts))} "
              f"${float(t.get('usdcSize') or 0):>8,.2f} {t.get('side','?'):<4} "
              f"tx={h[:16]}...  [{have}]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", nargs="*", default=None)
    ap.add_argument("--top", type=int, default=6, help="auto-pick top-N active tracked wallets")
    ap.add_argument("--activity-diff", default=None)
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()

    conn = get_connection()
    if args.activity_diff:
        activity_diff(conn, args.activity_diff.lower(), args.days)
        return

    wallets = args.wallets
    if not wallets:
        lo = int(time.time()) - 30 * 86400
        rows = conn.execute(
            """SELECT wallet, COUNT(*) n FROM wallet_trades
               WHERE timestamp >= ? GROUP BY wallet
               ORDER BY n DESC LIMIT ?""",
            (lo, args.top),
        ).fetchall()
        wallets = [r[0] for r in rows]
        print("Auto-picked most-active tracked wallets (30d):", *[w[:12] for w in wallets])
    aggregate_test(conn, wallets)
    conn.close()


if __name__ == "__main__":
    main()
