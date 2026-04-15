"""Data layer audit: coverage, freshness, completeness."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from trading_platform.polymarket.db_connection import get_connection

conn = get_connection()
now = int(datetime.now().timestamp())


def ts_fmt(ts):
    if ts is None:
        return "never"
    return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M")


def hdr(t):
    print("\n" + "=" * 72)
    print(t)
    print("=" * 72)


hdr("DATA LAYER AUDIT")

# ── wallet_trades ─────────────────────────────────────────────────────
print("\n### wallet_trades — core trade ledger ###")
r = conn.execute(
    "SELECT COUNT(*), COUNT(DISTINCT wallet), COUNT(DISTINCT condition_id), "
    "  MIN(timestamp), MAX(timestamp), "
    "  SUM(CASE WHEN market_resolved=1 THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN pnl IS NOT NULL THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN category='other' OR category IS NULL THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN title IS NULL OR title='' THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN slug IS NULL OR slug='' THEN 1 ELSE 0 END) "
    "FROM wallet_trades"
).fetchone()
print(f"  rows:            {r[0]:,}")
print(f"  distinct wallets:{r[1]:,}")
print(f"  distinct markets:{r[2]:,}")
print(f"  time span:       {ts_fmt(r[3])} -> {ts_fmt(r[4])}   ({(r[4]-r[3])/86400:.0f}d)")
print(f"  market_resolved: {r[5]:,} ({r[5]/r[0]*100:.0f}%)")
print(f"  pnl populated:   {r[6]:,} ({r[6]/r[0]*100:.0f}%)")
print(f"  category=other/NULL: {r[7]:,} ({r[7]/r[0]*100:.0f}%)")
print(f"  title missing:   {r[8]:,}")
print(f"  slug missing:    {r[9]:,}")
fresh = conn.execute(
    "SELECT COUNT(*) FROM wallet_trades WHERE timestamp > strftime('%s','now','-24 hours')"
).fetchone()[0]
print(f"  last 24h:        {fresh:,}")

# Age of newest trade
newest_age = (now - int(r[4])) / 3600
print(f"  newest trade:    {newest_age:.1f}h ago")

# ── wallet_profiles ───────────────────────────────────────────────────
print("\n### wallet_profiles ###")
r = conn.execute(
    "SELECT COUNT(*), "
    "  SUM(CASE WHEN resolved_trades >= 5 THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN resolved_trades >= 20 THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN wallet_bucket IS NOT NULL THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN win_rate IS NOT NULL THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN net_pnl_usdc > 0 THEN 1 ELSE 0 END), "
    "  MAX(last_synced_ts), MIN(last_synced_ts) "
    "FROM wallet_profiles"
).fetchone()
print(f"  rows:            {r[0]:,}")
print(f"  5+ resolved:     {r[1]:,} ({r[1]/r[0]*100:.0f}%)")
print(f"  20+ resolved:    {r[2]:,} ({r[2]/r[0]*100:.0f}%)")
print(f"  wallet_bucket:   {r[3]:,} ({r[3]/r[0]*100:.0f}%)")
print(f"  win_rate set:    {r[4]:,} ({r[4]/r[0]*100:.0f}%)")
print(f"  profitable:      {r[5]:,} ({r[5]/r[0]*100:.0f}%)")
print(f"  last_synced:     newest={ts_fmt(r[6])}  oldest={ts_fmt(r[7])}")

# ── leaderboard ───────────────────────────────────────────────────────
print("\n### leaderboard ###")
for t, n, wr, pnl, upd in conn.execute(
    "SELECT tier, COUNT(*), AVG(directional_win_rate), AVG(net_pnl_usdc), MAX(updated_at) "
    "FROM leaderboard GROUP BY tier"
).fetchall():
    print(f"  {t:8s}: n={n:>3d} avg_WR={wr:.2f} avg_pnl=${pnl:,.0f}  updated={ts_fmt(upd)}")

missing = pd.read_sql(
    "SELECT wp.wallet, wp.net_pnl_usdc, wp.directional_win_rate, wp.resolved_trades "
    "FROM wallet_profiles wp "
    "LEFT JOIN leaderboard l ON wp.wallet = l.wallet "
    "WHERE wp.net_pnl_usdc > 10000 AND wp.resolved_trades >= 25 AND l.tier IS NULL "
    "ORDER BY wp.net_pnl_usdc DESC LIMIT 10",
    conn,
)
if not missing.empty:
    print(f"\n  WINNERS WITH $10K+ PnL & 25+ resolved NOT on leaderboard: {len(missing)}")
    for _, w in missing.iterrows():
        print(
            f"    {w.wallet[:16]} pnl=${w.net_pnl_usdc:,.0f} "
            f"wr={w.directional_win_rate:.2f} n={w.resolved_trades}"
        )
else:
    print("  All high-PnL wallets are on the leaderboard.")

# ── alpha_scores ─────────────────────────────────────────────────────
print("\n### wallet_alpha_scores ###")
r = conn.execute(
    "SELECT COUNT(*), COUNT(DISTINCT wallet), SUM(is_copyable), "
    "  MAX(computed_at), COUNT(DISTINCT category) "
    "FROM wallet_alpha_scores"
).fetchone()
print(f"  rows:             {r[0]}")
print(f"  distinct wallets: {r[1]}")
print(f"  is_copyable=1:    {r[2]}")
print(f"  categories:       {r[4]}")
print(f"  latest computed:  {ts_fmt(r[3])}")

q = conn.execute(
    "SELECT COUNT(DISTINCT l.wallet), "
    "  COUNT(DISTINCT CASE WHEN a.wallet IS NOT NULL THEN l.wallet END) "
    "FROM leaderboard l LEFT JOIN wallet_alpha_scores a ON l.wallet = a.wallet"
).fetchone()
print(f"  leaderboard wallets with alpha row: {q[1]}/{q[0]}")

# Per-category coverage
print("\n  alpha coverage per category (copyable=1 only):")
for cat, n in conn.execute(
    "SELECT category, COUNT(*) FROM wallet_alpha_scores "
    "WHERE is_copyable=1 GROUP BY category ORDER BY 2 DESC"
).fetchall():
    print(f"    {cat:14s} {n}")

# ── signal_outcomes ──────────────────────────────────────────────────
print("\n### signal_outcomes ###")
r = conn.execute(
    "SELECT COUNT(*), "
    "  SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN paper_trade_id IS NOT NULL THEN 1 ELSE 0 END), "
    "  MIN(fired_at), MAX(fired_at) "
    "FROM signal_outcomes"
).fetchone()
print(f"  rows:           {r[0]:,}")
print(f"  resolved:       {r[1]:,} ({r[1]/r[0]*100:.0f}%)")
print(f"  paper-traded:   {r[2]:,} ({r[2]/r[0]*100:.1f}%)")
print(f"  time span:      {ts_fmt(r[3])} -> {ts_fmt(r[4])}")

# ── wallet_category_profiles ─────────────────────────────────────────
print("\n### wallet_category_profiles (S/A/B/C/D per-category tier) ###")
for t, n, wr, rt in conn.execute(
    "SELECT tier, COUNT(*), AVG(win_rate), AVG(resolved_trades) "
    "FROM wallet_category_profiles GROUP BY tier ORDER BY "
    " CASE tier WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3 "
    " WHEN 'C' THEN 4 WHEN 'D' THEN 5 ELSE 9 END"
).fetchall():
    print(f"  {str(t):6s}: n={n:>3d} avg_WR={wr:.2f} avg_resolved={rt:.1f}")

# SA tier per category
print("\n  S/A tier by category:")
for cat, n in conn.execute(
    "SELECT category, COUNT(*) FROM wallet_category_profiles "
    "WHERE tier IN ('S','A') GROUP BY category ORDER BY 2 DESC"
).fetchall():
    print(f"    {cat:14s} {n}")

# ── market_signals ────────────────────────────────────────────────────
print("\n### market_signals ###")
r = conn.execute(
    "SELECT COUNT(*), COUNT(DISTINCT condition_id), COUNT(DISTINCT wallet), "
    "  MIN(fired_at), MAX(fired_at), "
    "  SUM(CASE WHEN category='other' OR category IS NULL THEN 1 ELSE 0 END) "
    "FROM market_signals"
).fetchone()
print(f"  rows:             {r[0]:,}")
print(f"  distinct markets: {r[1]:,}")
print(f"  distinct wallets: {r[2]:,}")
print(f"  time span:        {ts_fmt(r[3])} -> {ts_fmt(r[4])}")
print(f"  other/NULL cat:   {r[5]:,} ({r[5]/r[0]*100:.0f}%)")

print("\n  signals fired 24h by type:")
for s, n in conn.execute(
    "SELECT signal_type, COUNT(*) FROM market_signals "
    "WHERE fired_at > strftime('%s','now','-24 hours') "
    "GROUP BY signal_type ORDER BY 2 DESC"
).fetchall():
    print(f"    {s:25s} {n}")

# ── market price streams ─────────────────────────────────────────────
print("\n### market price streams ###")
for table in ["market_ticks", "market_price_history"]:
    try:
        r = conn.execute(
            f"SELECT COUNT(*), MAX(timestamp), COUNT(DISTINCT condition_id) FROM {table}"
        ).fetchone()
        age = (now - int(r[1])) / 60 if r[1] else None
        age_s = f"{age:.0f}min ago" if age else "never"
        print(f"  {table}: rows={r[0]:,} markets={r[2]:,} latest={age_s}")
    except Exception as e:
        print(f"  {table}: {e}")

# ── paper trades ─────────────────────────────────────────────────────
print("\n### polymarket_paper_trades ###")
r = conn.execute(
    "SELECT COUNT(*), "
    "  SUM(CASE WHEN exit_ts IS NULL AND archived=0 THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN outcome='expired' THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN last_mark_ts IS NOT NULL THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN last_mark_ts > strftime('%s','now','-2 hours') THEN 1 ELSE 0 END) "
    "FROM polymarket_paper_trades"
).fetchone()
print(f"  total:      {r[0]}")
print(f"  open:       {r[1]}")
print(f"  wins:       {r[2]}   losses: {r[3]}   expired: {r[4]}")
print(f"  ever marked-to-market:  {r[5]}/{r[0]}")
print(f"  marked in last 2h:      {r[7]}/{r[1] or 1} open positions")
print(f"  category NULL:          {r[6]}")

# ── wallet_alerts ─────────────────────────────────────────────────────
print("\n### wallet_alerts (signal firings log) ###")
r = conn.execute(
    "SELECT COUNT(*), MAX(detected_at), "
    "  SUM(CASE WHEN signal_fired=1 THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN detected_at > strftime('%s','now','-24 hours') THEN 1 ELSE 0 END) "
    "FROM wallet_alerts"
).fetchone()
print(f"  rows:     {r[0]:,}")
print(f"  fired:    {r[2]:,}")
print(f"  last 24h: {r[3]:,}")
print(f"  latest:   {ts_fmt(r[1])}")

# ── market_categories ────────────────────────────────────────────────
print("\n### market_categories ###")
r = conn.execute(
    "SELECT COUNT(*), COUNT(DISTINCT condition_id), "
    "  SUM(CASE WHEN category IN ('politics','geopolitics','sports','crypto','economics','entertainment','science') THEN 1 ELSE 0 END), "
    "  SUM(CASE WHEN category='other' OR category IS NULL THEN 1 ELSE 0 END) "
    "FROM market_categories"
).fetchone()
print(f"  rows:       {r[0]:,}")
print(f"  markets:    {r[1]:,}")
print(f"  categorized: {r[2]:,}")
print(f"  other/NULL: {r[3]:,}")

conn.close()
print("\nDONE.")
