"""Depth/breadth gap audit."""
from __future__ import annotations

import time
from datetime import datetime

from trading_platform.polymarket.db_connection import get_connection

c = get_connection()
now = int(time.time())


def hdr(t):
    print("\n" + "=" * 68 + f"\n  {t}\n" + "=" * 68)


# 1. Market metadata coverage
hdr("1. MARKET METADATA COVERAGE")
r = c.execute("SELECT COUNT(DISTINCT condition_id) FROM wallet_trades").fetchone()[0]
u = c.execute("SELECT COUNT(*) FROM market_categories").fetchone()[0]
print(f"  markets in wallet_trades:              {r:,}")
print(f"  rows in market_categories:             {u:,}")
cov = c.execute(
    "SELECT COUNT(DISTINCT wt.condition_id) FROM wallet_trades wt "
    "JOIN market_categories mc ON wt.condition_id=mc.condition_id"
).fetchone()[0]
print(f"  wallet_trades markets with category:   {cov:,} ({cov/max(r,1)*100:.0f}%)")
oc = c.execute(
    "SELECT COUNT(DISTINCT pt.condition_id) FROM polymarket_paper_trades pt "
    "LEFT JOIN market_categories mc ON pt.condition_id=mc.condition_id "
    "WHERE pt.exit_ts IS NULL AND mc.condition_id IS NULL"
).fetchone()[0]
print(f"  OPEN paper-trade markets NOT in market_categories: {oc}")
cols = [r[1] for r in c.execute("PRAGMA table_info(market_categories)").fetchall()]
print(f"  market_categories has: {cols}")
print("  → NOTHING stores endDate, volume, outcomes, tags per condition_id persistently")

# 2. Wallet activity staleness
hdr("2. WALLET ACTIVITY STALENESS")
r = c.execute(
    "SELECT "
    " SUM(CASE WHEN (?-last_trade_ts) < 7*86400 THEN 1 ELSE 0 END), "
    " SUM(CASE WHEN (?-last_trade_ts) BETWEEN 7*86400 AND 30*86400 THEN 1 ELSE 0 END), "
    " SUM(CASE WHEN (?-last_trade_ts) BETWEEN 30*86400 AND 90*86400 THEN 1 ELSE 0 END), "
    " SUM(CASE WHEN (?-last_trade_ts) > 90*86400 THEN 1 ELSE 0 END), "
    " SUM(CASE WHEN last_trade_ts IS NULL THEN 1 ELSE 0 END) "
    "FROM leaderboard",
    (now, now, now, now),
).fetchone()
print(f"  tracked wallets (leaderboard n=56):")
print(f"    active <7d:          {r[0]}")
print(f"    stale 7-30d:         {r[1]}")
print(f"    stale 30-90d:        {r[2]}")
print(f"    dead >90d:           {r[3]}")
print(f"    no trade data:       {r[4]}")
print("  → Tier thresholds don't consider recency; dead wallets stay on board")

# 3. Order book / execution context
hdr("3. ORDER BOOK / EXECUTION CONTEXT AT SIGNAL FIRE")
for t in ["order_book_snapshots", "orderbook_snapshots", "book_snapshots"]:
    try:
        n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {n}")
    except Exception:
        pass
r = c.execute(
    "SELECT COUNT(*), "
    " SUM(CASE WHEN spread_cost IS NOT NULL THEN 1 ELSE 0 END), "
    " AVG(spread_cost), AVG(slippage_cost) "
    "FROM polymarket_paper_trades"
).fetchone()
print(f"  paper_trades with spread_cost recorded: {r[1]}/{r[0]}")
print(f"  avg spread_cost: {r[2]}  avg slippage_cost: {r[3]}")
print("  → No order book snapshot saved at signal-fire time")
print("  → Paper trades compute spread on-the-fly but don't persist book state")

# 4. Price history depth
hdr("4. PRICE HISTORY DEPTH")
r = c.execute(
    "SELECT COUNT(*), COUNT(DISTINCT condition_id), MIN(timestamp), MAX(timestamp) FROM market_ticks"
).fetchone()
print(f"  market_ticks: {r[0]:,} rows, {r[1]:,} markets")
print(f"    span: {datetime.fromtimestamp(r[2])} → {datetime.fromtimestamp(r[3])}")
thin = c.execute(
    "SELECT COUNT(*) FROM (SELECT condition_id, COUNT(*) n FROM market_ticks GROUP BY condition_id) WHERE n < 10"
).fetchone()[0]
print(f"  markets with <10 ticks: {thin}")
try:
    cols = [r[1] for r in c.execute("PRAGMA table_info(market_price_history)").fetchall()]
    r = c.execute("SELECT COUNT(*), COUNT(DISTINCT condition_id) FROM market_price_history").fetchone()
    print(f"  market_price_history: {r[0]:,} rows, {r[1]:,} markets")
    print(f"    cols: {cols}")
except Exception:
    print("  market_price_history: NOT PRESENT")
print("  → No per-market OHLC candles — tick data only, irregular frequency")

# 5. Cross-venue data
hdr("5. CROSS-VENUE / ALTERNATE DATA")
for t in ["insider_wallets", "wallet_archetypes", "trade_hypotheses"]:
    try:
        n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {n}")
    except Exception:
        print(f"  {t}: not present")
# Kalshi / Manifold data
import os
from pathlib import Path
for p in ["data/kalshi", "data/manifold"]:
    if Path(p).exists():
        files = list(Path(p).rglob("*.csv"))
        print(f"  {p}: {len(files)} CSVs (offline data, not live)")
    else:
        print(f"  {p}: not present")
print("  → Kalshi/Manifold CSV files exist but aren't joined live for cross-venue signal")

# 6. Failed trade attempts
hdr("6. LIVE-EXECUTION TELEMETRY")
for t in ["dry_run_trades", "live_trades", "trade_fills"]:
    try:
        n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {n}")
    except Exception:
        pass
# Latest live attempt?
try:
    r = c.execute("SELECT MAX(created_at) FROM live_trades").fetchone()
    if r[0]:
        print(f"  latest live_trade: {datetime.fromtimestamp(r[0])}")
except Exception:
    pass
print("  → Limited telemetry on live-executor decisions pre-launch")

# 7. Wallet clustering / copy detection
hdr("7. WALLET CLUSTERING / COPY DETECTION")
r = c.execute(
    "SELECT COUNT(*) FROM ("
    " SELECT wt.condition_id, wt.side, COUNT(DISTINCT wt.wallet) AS n "
    " FROM wallet_trades wt JOIN leaderboard l ON wt.wallet=l.wallet "
    " WHERE wt.timestamp > strftime('%s','now','-30 days') "
    " GROUP BY wt.condition_id, wt.side HAVING n >= 3)"
).fetchone()[0]
print(f"  markets with 3+ leaderboard wallets same-side in 30d: {r}")
print("  → Have raw data but no persistent cluster / correlation analysis")

# 8. Pending signals without market metadata
hdr("8. PENDING SIGNAL MARKET METADATA COVERAGE")
r = c.execute(
    "SELECT "
    " COUNT(DISTINCT so.condition_id), "
    " COUNT(DISTINCT CASE WHEN mc.condition_id IS NULL THEN so.condition_id END) "
    "FROM signal_outcomes so LEFT JOIN market_categories mc ON so.condition_id=mc.condition_id "
    "WHERE so.resolved_at IS NULL AND so.signal_type != 'price_velocity' "
    "AND LOWER(so.category) IN ('politics','geopolitics')"
).fetchone()
print(f"  pending politics+geo markets: {r[0]}  missing metadata: {r[1]}")

# 9. Signal outcome MAE/MFE completeness
hdr("9. MAE / MFE TRACKING")
r = c.execute(
    "SELECT COUNT(*), "
    " SUM(CASE WHEN mae IS NOT NULL THEN 1 ELSE 0 END) "
    "FROM signal_outcomes"
).fetchone()
print(f"  signal_outcomes with mae recorded: {r[1]}/{r[0]} ({r[1]/max(r[0],1)*100:.0f}%)")
print("  → MAE (max adverse excursion) mostly unpopulated — can't compute risk-adjusted EV per signal type")

c.close()
