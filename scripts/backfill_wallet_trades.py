"""Backfill wallet_trades from Polymarket data-api /activity.

The 2026-07-16 truth-comparison (scripts/wallet_truth_compare.py) found we held
only 23% of an active wallet's trades: the poller fetched 10 rows/cycle, the
sync roster was an unordered lottery, and upsert_trade collapsed multi-fill
transactions to one row. Those are fixed forward; this script repairs HISTORY
so window-based analytics (7d/30d PnL, volume, win-rate, persistence tests)
are computed on complete data.

Wallet set: --wallets, or auto = the top --top wallets by our own wallet_trades
activity in the last --days (they're the ones feeding analytics) plus any
is_copyable=1 wallets from wallet_alpha_scores.

Per wallet: page /activity (type=TRADE) back --days (cap --max-pages), upsert
via WalletDB.upsert_trade (composite-fill-key dedup — safe to re-run).
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.request

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.wallet_db import WalletDB, categorize_slug, _now_ts

HDRS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126",
        "Accept": "application/json"}
DATA = "https://data-api.polymarket.com"


def _get(url: str, retries: int = 6):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HDRS)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:  # data-api throws transient 400/429/5xx under burst
            last = e
            time.sleep(2.0 * (i + 1))
    raise last


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def backfill_wallet(db: WalletDB, wallet: str, days: int, max_pages: int) -> tuple[int, int]:
    """Timestamp-cursor pagination: the data-api rejects offset > 3000
    ("max historical activity offset"), so deep history is walked by
    repeatedly fetching the newest 500 rows with end=<oldest seen so far>.
    """
    lo = int(time.time()) - days * 86400
    new = dup = 0
    end_cursor: int | None = None
    for _ in range(max_pages):
        url = f"{DATA}/activity?user={wallet}&limit=500&type=TRADE&start={lo}"
        if end_cursor is not None:
            url += f"&end={end_cursor}"
        rows = _get(url)
        if not rows:
            break
        for t in rows:
            ts = int(_safe_float(t.get("timestamp")))
            if ts < lo:
                continue
            tx = t.get("transactionHash") or ""
            if not tx:
                continue
            slug = t.get("slug") or ""
            inserted = db.upsert_trade(
                wallet=wallet,
                proxy_wallet=t.get("proxyWallet"),
                asset=t.get("asset"),
                condition_id=t.get("conditionId"),
                side=(t.get("side") or "").upper(),
                size=_safe_float(t.get("size")),
                price=_safe_float(t.get("price")),
                timestamp=ts,
                title=t.get("title", ""),
                slug=slug,
                outcome=t.get("outcome", ""),
                event_slug=t.get("eventSlug", ""),
                transaction_hash=tx,
                category=categorize_slug(slug),
                synced_at=_now_ts(),
            )
            if inserted:
                new += 1
            else:
                dup += 1
        oldest = min(int(_safe_float(r.get("timestamp"))) for r in rows)
        if len(rows) < 500 or oldest <= lo:
            break
        if end_cursor is not None and oldest >= end_cursor:
            # >500 fills in one second — cannot advance by timestamp; step past it.
            end_cursor -= 1
        else:
            end_cursor = oldest
        time.sleep(0.6)
    return new, dup


def auto_wallets(conn, days: int, top: int) -> list[str]:
    lo = int(time.time()) - days * 86400
    rows = conn.execute(
        """SELECT wallet, COUNT(*) n FROM wallet_trades
           WHERE timestamp >= ? GROUP BY wallet ORDER BY n DESC LIMIT ?""",
        (lo, top),
    ).fetchall()
    wallets = [r[0] for r in rows]
    try:
        extra = conn.execute(
            "SELECT DISTINCT wallet FROM wallet_alpha_scores WHERE is_copyable = 1"
        ).fetchall()
        for (w,) in extra:
            if w not in wallets:
                wallets.append(w)
    except Exception:
        pass
    return wallets


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", nargs="*", default=None)
    ap.add_argument("--top", type=int, default=50)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--max-pages", type=int, default=40,
                    help="page cap per wallet (40x500 = 20k trades)")
    args = ap.parse_args()

    conn = get_connection()
    wallets = [w.lower() for w in args.wallets] if args.wallets else auto_wallets(conn, args.days, args.top)
    conn.close()
    db = WalletDB()

    print(f"Backfilling {len(wallets)} wallets, {args.days}d, cap {args.max_pages} pages each")
    t0 = time.time()
    tot_new = 0
    for i, w in enumerate(wallets, 1):
        try:
            new, dup = backfill_wallet(db, w, args.days, args.max_pages)
        except Exception as e:
            print(f"  [{i}/{len(wallets)}] {w[:12]} ERROR {str(e)[:60]}")
            continue
        tot_new += new
        print(f"  [{i}/{len(wallets)}] {w[:12]} +{new} new ({dup} already had)")
    print(f"\nDone: +{tot_new} fills in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
