"""Re-apply Fix 1/3/4/7 + new A/B/C in one transactional pass.

Run while ALL other DB writers are stopped. Touches
wallet_intelligence.db via db_connection only.
"""
from __future__ import annotations

import io
import contextlib

from trading_platform.polymarket.db_connection import db, get_connection


def _banner(t: str) -> None:
    print("\n" + "=" * 60)
    print(t)
    print("=" * 60)


# ── FIX 1: purge phantom paper trades ─────────────────────────────────
_banner("Fix 1 — purge phantom paper trades")
with db() as c:
    pre = c.execute("SELECT COUNT(*), SUM(realized_pnl) FROM polymarket_paper_trades").fetchone()
    n = c.execute(
        "DELETE FROM polymarket_paper_trades "
        "WHERE entry_price IS NULL OR entry_price < 0.05 OR entry_price > 0.95"
    ).rowcount
    post = c.execute("SELECT COUNT(*), SUM(realized_pnl) FROM polymarket_paper_trades").fetchone()
print(f"  deleted {n} / before n={pre[0]} pnl=${pre[1] or 0:,.2f}")
print(f"  after  n={post[0]} pnl=${post[1] or 0:,.2f}")

# ── FIX 3: classify_wallets (populates wallet_bucket) ─────────────────
_banner("Fix 3 — classify_wallets (wallet_bucket)")
from trading_platform.polymarket.wallet_buckets import classify_wallets
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    r3 = classify_wallets()
print(f"  {r3}")

# ── FIX 7: reclassify signal_outcomes + market_signals (NOT wallet_trades) ─
_banner("Fix 7 — reclassify signal_outcomes + market_signals")
from trading_platform.polymarket.market_categorizer import classify_keywords
for table in ("signal_outcomes", "market_signals"):
    with db() as c:
        rows = c.execute(f"SELECT rowid, question, category FROM {table} WHERE question IS NOT NULL").fetchall()
        updates = []
        for rid, text, old in rows:
            new, _ = classify_keywords(None, text)
            if new != (old or ""):
                updates.append((new, rid))
        if updates:
            c.executemany(f"UPDATE {table} SET category=? WHERE rowid=?", updates)
    print(f"  {table}: {len(updates)}/{len(rows)} reclassified")

# ── FIX 4: SignalResolver (CSV + Gamma fallback) ──────────────────────
_banner("Fix 4 — SignalResolver (CSV + Gamma fallback)")
from trading_platform.polymarket.signal_resolver import SignalResolver
r4 = SignalResolver(max_gamma_calls=300).run()
print(f"  {r4}")

# ── NEW A: split pol_geo in wallet_category_profiles ──────────────────
#
# Backs-away from a legacy "pol_geo" combined tier category: that string
# never matches trade.category ('politics' or 'geopolitics') in
# _check_tier_entry, which is why `tier_entry` signal has 0 fires in 7d.
# Strategy: duplicate each pol_geo row as TWO rows, one per sub-category,
# so either sub-category matches. The wallet is a proven S/A-tier in
# both geopolitics and politics historically.
_banner("A — split pol_geo rows in wallet_category_profiles")
with db() as c:
    combined = c.execute(
        "SELECT * FROM wallet_category_profiles WHERE category='pol_geo'"
    ).fetchall()
    cols = [d[0] for d in c.execute("SELECT * FROM wallet_category_profiles LIMIT 0").description]
    print(f"  found {len(combined)} pol_geo rows to split")
    if combined:
        col_idx = {name: i for i, name in enumerate(cols)}
        inserted = 0
        for row in combined:
            row = list(row)
            for sub in ("politics", "geopolitics"):
                row[col_idx["category"]] = sub
                # Upsert: delete existing (wallet, sub) then insert
                c.execute(
                    "DELETE FROM wallet_category_profiles WHERE wallet=? AND category=?",
                    (row[col_idx["wallet"]], sub),
                )
                placeholders = ", ".join("?" for _ in cols)
                c.execute(
                    f"INSERT INTO wallet_category_profiles ({','.join(cols)}) VALUES ({placeholders})",
                    row,
                )
                inserted += 1
        c.execute("DELETE FROM wallet_category_profiles WHERE category='pol_geo'")
        print(f"  split into {inserted} per-sub-category rows, purged 'pol_geo'")
    final_sa = c.execute(
        "SELECT category, COUNT(*) FROM wallet_category_profiles "
        "WHERE tier IN ('S','A') GROUP BY category ORDER BY 2 DESC"
    ).fetchall()
    print("  S/A tier by category (after split):")
    for cat, n in final_sa:
        print(f"    {cat}: {n}")

# ── NEW B: alpha_score_recompute (direct, bypasses API) ──────────────
_banner("B — alpha_score_recompute")
from trading_platform.polymarket.alpha_scores import compute_alpha_scores
r_b = compute_alpha_scores("data/polymarket/wallet_intelligence.db")
print(f"  {r_b}")

# ── sync_wallet_trades catch-up (backlog from 31h poller gap) ─────────
_banner("sync_wallet_trades catch-up (network-bound, may take 1-2 min)")
from trading_platform.polymarket.wallet_trade_sync import sync_wallet_trades
r_sync = sync_wallet_trades(top_n=200, max_age_hours=2.0)
print(f"  {r_sync}")

# ── final verification ──────────────────────────────────────────────
_banner("FINAL STATE")
with db() as c:
    stats = {
        "integrity": c.execute("PRAGMA integrity_check").fetchone()[0],
        "journal": c.execute("PRAGMA journal_mode").fetchone()[0],
        "wallet_trades": c.execute("SELECT COUNT(*) FROM wallet_trades").fetchone()[0],
        "paper_trades": c.execute("SELECT COUNT(*) FROM polymarket_paper_trades").fetchone()[0],
        "outcomes_total": c.execute("SELECT COUNT(*) FROM signal_outcomes").fetchone()[0],
        "outcomes_resolved": c.execute(
            "SELECT COUNT(*) FROM signal_outcomes WHERE resolved_at IS NOT NULL"
        ).fetchone()[0],
        "wallet_buckets": c.execute(
            "SELECT COUNT(*) FROM wallet_profiles WHERE wallet_bucket IS NOT NULL"
        ).fetchone()[0],
        "alpha_copyable": c.execute(
            "SELECT COUNT(*) FROM wallet_alpha_scores WHERE is_copyable=1"
        ).fetchone()[0],
        "wallet_cat_profiles_sa": c.execute(
            "SELECT COUNT(*) FROM wallet_category_profiles WHERE tier IN ('S','A')"
        ).fetchone()[0],
    }
for k, v in stats.items():
    print(f"  {k:22s} {v}")
