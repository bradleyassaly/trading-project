"""Replay recent wallet_trades through the signal engine + paper-executor
gates to see what WOULD fire with the current post-A/B/C config.

Does NOT mutate the DB: signal engine writes are monkey-patched to no-ops.
Paper-executor gates are evaluated directly (not via execute_signal) to
avoid any side effects.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from trading_platform.polymarket.db_connection import get_connection
from trading_platform.polymarket.whale_tripwire import WhaleTrade
from trading_platform.polymarket.whale_signal_engine import (
    WhaleSignalEngine,
    PROBATION_SIGNAL_TYPES,
)
from trading_platform.polymarket.polymarket_paper_executor import (
    MIN_ENTRY_PRICE,
    MAX_ENTRY_PRICE,
    PAPER_TRADE_CATEGORIES,
    TIER1_ONLY_CATEGORIES,
    EXCLUDE_CATEGORIES,
    EXCLUDE_SIGNAL_TYPES,
    LIVE_TRADE_CATEGORIES,
    MIN_CONFIDENCE,
    MIN_CONFIDENCE_BY_TYPE,
    SIGNAL_BANKROLL,
)
from trading_platform.polymarket.alpha_scores import get_wallet_alpha


# ── Prep: tier/leaderboard lookup ────────────────────────────────────────
conn = get_connection()

tier_map: dict[str, str] = {}
for w, t in conn.execute("SELECT wallet, tier FROM leaderboard WHERE tier IS NOT NULL").fetchall():
    tier_map[w] = t

wallet_meta: dict[str, dict] = {}
for row in conn.execute(
    "SELECT wallet, directional_win_rate, conviction_score, total_volume_usdc "
    "FROM wallet_profiles"
).fetchall():
    wallet_meta[row[0]] = {
        "directional_win_rate": row[1] or 0.0,
        "conviction_score": row[2] or 0.0,
        "total_volume_usdc": row[3] or 0.0,
    }

print(f"tiered wallets loaded: {len(tier_map)}  (tier1={sum(1 for t in tier_map.values() if t=='tier1')}, tier2={sum(1 for t in tier_map.values() if t=='tier2')}, tier1h={sum(1 for t in tier_map.values() if t=='tier1h')})")

# ── Pull last 30d of trades by tiered wallets ──────────────────────────
cutoff = int((datetime.now() - timedelta(days=30)).timestamp())
trades_df = pd.read_sql(
    """
    SELECT wt.wallet, wt.condition_id, wt.side, wt.price, wt.size, wt.outcome,
           wt.title, wt.category, wt.timestamp, l.tier
    FROM wallet_trades wt
    JOIN leaderboard l ON wt.wallet = l.wallet
    WHERE wt.timestamp >= ?
      AND wt.size >= 25
      AND wt.price IS NOT NULL
    ORDER BY wt.timestamp ASC
    """,
    conn,
    params=(cutoff,),
)
print(f"loaded {len(trades_df):,} trades (>=$25, last 30d, tiered wallets)")
conn.close()

# ── Monkey-patch the signal engine DB writes ───────────────────────────
# The engine calls self.db.insert_signal / insert_alert /
# increment_category_signal, plus self._record_signal_outcome and the
# convergence branch calls self.db.update_signal_confidence. Stub all.

from trading_platform.polymarket.wallet_db import WalletDB

class _SilentDB(WalletDB):
    # override every write path
    def insert_signal(self, **kw): pass
    def insert_alert(self, **kw): pass
    def increment_category_signal(self, *a, **kw): pass
    def update_signal_confidence(self, *a, **kw): pass
    def upsert_trade(self, **kw): return False
    def upsert_profile(self, *a, **kw): pass

silent_db = _SilentDB()
engine = WhaleSignalEngine(db=silent_db)
# Also stub the outcome recorder
engine._record_signal_outcome = lambda *a, **kw: None

# ── Run the replay ─────────────────────────────────────────────────────
signal_counts_by_type: Counter = Counter()
signal_counts_by_cat_type: Counter = Counter()
signal_counts_by_tier_type: Counter = Counter()
paper_passes_by_cat_type: Counter = Counter()
paper_blocked_reasons: Counter = Counter()
conviction_logs: list[dict] = []

def _paper_gate_check(sig: dict, db_path: str) -> tuple[bool, str]:
    """Replicate `execute_signal` gates WITHOUT side-effects.
    Returns (passes, reason_if_blocked)."""
    sig_type = sig.get("signal_type", "")
    conf = sig.get("confidence") or 0

    floor = MIN_CONFIDENCE_BY_TYPE.get(sig_type, MIN_CONFIDENCE)
    if conf < floor:
        return False, f"below_confidence ({conf:.2f}<{floor})"
    if sig_type not in SIGNAL_BANKROLL:
        return False, "not_in_SIGNAL_BANKROLL"
    if sig_type in EXCLUDE_SIGNAL_TYPES:
        return False, "excluded_signal_type"

    cat_raw = sig.get("category") or ""
    cat = cat_raw.lower() if isinstance(cat_raw, str) else ""
    if cat in EXCLUDE_CATEGORIES:
        return False, f"excluded_category:{cat}"
    if cat and cat not in PAPER_TRADE_CATEGORIES:
        return False, f"unproven_category:{cat}"

    if cat in TIER1_ONLY_CATEGORIES:
        tier = sig.get("wallet_tier") or ""
        if tier not in ("tier1", "tier1h"):
            return False, f"tier_gate_sports_non_tier1:{tier or 'NULL'}"

    direction = (sig.get("direction") or "").upper()
    if direction not in ("BUY", "SELL"):
        return False, "bad_direction"

    try:
        ep = float(sig.get("price") or 0)
    except (TypeError, ValueError):
        ep = 0
    if ep <= 0:
        return False, "missing_price"
    if ep < MIN_ENTRY_PRICE or ep > MAX_ENTRY_PRICE:
        return False, f"price_outside_band:{ep:.3f}"

    # Alpha gate (with tier1/tier1h bypass)
    wallet = sig.get("wallet")
    if wallet and wallet not in ("velocity_detector", "order_book_monitor"):
        alpha = get_wallet_alpha(db_path, wallet, cat or "other")
        tier = sig.get("wallet_tier") or ""
        if alpha <= 0 and tier not in ("tier1", "tier1h"):
            return False, "alpha_gate"

    return True, ""


DB_PATH = "data/polymarket/wallet_intelligence.db"
processed = 0
for _, r in trades_df.iterrows():
    # Note: use r['size'] not r.size — the latter returns pandas element count
    wallet_val = r["wallet"]
    wmeta = wallet_meta.get(wallet_val, {})
    tier = tier_map.get(wallet_val) or ""
    wt = WhaleTrade(
        wallet=wallet_val,
        condition_id=r["condition_id"] or "",
        side=(r["side"] or "").upper(),
        price=float(r["price"] or 0),
        size=float(r["size"] or 0),
        outcome=(r["outcome"] or "").upper(),
        question=r["title"] or "",
        category=r["category"] or "other",
        timestamp=int(r["timestamp"]),
        wallet_tier=tier,
        directional_win_rate=wmeta.get("directional_win_rate", 0.0),
        conviction_score=wmeta.get("conviction_score", 0.0),
        total_volume_usdc=wmeta.get("total_volume_usdc", 0.0),
    )
    try:
        sig = engine.on_whale_trade(wt)
    except Exception as exc:
        if processed < 3:
            import traceback; traceback.print_exc()
        continue
    processed += 1
    if processed <= 3:
        print(f"  sample #{processed}: wallet={wt.wallet[:14]} tier={wt.wallet_tier} "
              f"size={wt.size:.0f} cat={wt.category} sig={sig}")
    if not sig:
        continue

    stype = sig.get("signal_type", "unknown")
    cat = (sig.get("category") or "other").lower()
    signal_counts_by_type[stype] += 1
    signal_counts_by_cat_type[(cat, stype)] += 1
    signal_counts_by_tier_type[(tier or "none", stype)] += 1

    passes, reason = _paper_gate_check(sig, DB_PATH)
    if passes:
        paper_passes_by_cat_type[(cat, stype)] += 1
    else:
        paper_blocked_reasons[reason] += 1

    if stype in ("tier_entry", "late_conviction", "convergence", "cascade"):
        conviction_logs.append({
            "type": stype,
            "cat": cat,
            "tier": tier,
            "wallet": wallet_val[:12],
            "price": float(sig.get("price") or 0),
            "conf": float(sig.get("confidence") or 0),
            "question": (r["title"] or "")[:60],
        })

print(f"\nprocessed: {processed} tradeable-sized trades")

# ── Report ──────────────────────────────────────────────────────────────
def hdr(t): print("\n" + "=" * 64 + f"\n{t}\n" + "=" * 64)

hdr("Signals fired by type (last 30d replay)")
for st, n in signal_counts_by_type.most_common():
    print(f"  {st:25s} {n}")

hdr("Signals fired by category × type")
by_cat = defaultdict(list)
for (c, st), n in signal_counts_by_cat_type.most_common():
    by_cat[c].append((st, n))
for cat in sorted(by_cat):
    print(f"  [{cat}]")
    for st, n in by_cat[cat]:
        print(f"    {st:25s} {n}")

hdr("Signals fired by tier × type")
by_tier = defaultdict(list)
for (t, st), n in signal_counts_by_tier_type.most_common():
    by_tier[t].append((st, n))
for tier in sorted(by_tier):
    print(f"  [{tier or 'untiered'}]")
    for st, n in by_tier[tier]:
        print(f"    {st:25s} {n}")

hdr("Paper-executor gate outcomes")
total_fired = sum(signal_counts_by_type.values())
total_pass = sum(paper_passes_by_cat_type.values())
total_block = sum(paper_blocked_reasons.values())
print(f"  fired:   {total_fired:,}")
print(f"  passes:  {total_pass:,}  ({total_pass/max(total_fired,1)*100:.1f}%)")
print(f"  blocked: {total_block:,}")
print("\n  pass-through by category × type:")
for (c, st), n in sorted(paper_passes_by_cat_type.items(), key=lambda x: -x[1]):
    print(f"    [{c:14s}] {st:22s} {n}")
print("\n  block reasons:")
for reason, n in paper_blocked_reasons.most_common():
    print(f"    {reason:35s} {n}")

hdr("NEW SIGNAL PATHS — tier_entry / late_conviction / convergence / cascade")
print(f"  {len(conviction_logs)} conviction signals generated in replay")
for row in conviction_logs[:20]:
    print(f"    {row['type']:18s} {row['cat']:12s} tier={row['tier']:6s} "
          f"p={row['price']:.2f} c={row['conf']:.2f}  {row['question']}")
if len(conviction_logs) > 20:
    print(f"    ... and {len(conviction_logs)-20} more")

hdr("LIVE-GATE CANDIDATES (what would have gone LIVE if writers were on)")
live_count = 0
live_by_type = Counter()
for (c, st), n in paper_passes_by_cat_type.items():
    if c in LIVE_TRADE_CATEGORIES:
        live_count += n
        live_by_type[st] += n
print(f"  total would-be-live paper-trade candidates: {live_count}")
print(f"  by type:")
for st, n in live_by_type.most_common():
    print(f"    {st:25s} {n}")
print(f"\n  LIVE_TRADE_CATEGORIES = {LIVE_TRADE_CATEGORIES}")
print(f"  PAPER_TRADE_CATEGORIES = {PAPER_TRADE_CATEGORIES}")
