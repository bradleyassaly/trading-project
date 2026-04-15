"""One-off wallet intelligence deep dive. Reads from wallet_intelligence.db."""
from __future__ import annotations

import os
import sys

import pandas as pd
import numpy as np

from trading_platform.polymarket.db_connection import get_connection

pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 80)

TMP = "data/polymarket/_deep_dive"
os.makedirs(TMP, exist_ok=True)

conn = get_connection()


# ── SECTION 1: Ranking ─────────────────────────────────────────────────────
print("=" * 70)
print("SECTION 1 — RANK WALLETS")
print("=" * 70)

wallets = pd.read_sql(
    """
    SELECT
      wp.wallet,
      l.tier,
      wp.wallet_bucket,
      wp.wallet_type,
      wp.win_rate                    AS profile_wr,
      wp.directional_win_rate        AS dir_wr,
      wp.net_pnl_usdc                AS profile_pnl,
      wp.resolved_trades             AS resolved_markets,
      wp.total_volume_usdc,
      wp.avg_position_size_usdc      AS avg_size,
      wp.conviction_score,
      wp.profit_factor,
      COUNT(wt.id)                   AS total_trades,
      COUNT(DISTINCT wt.condition_id) AS unique_markets,
      SUM(wt.size)                   AS total_volume,
      AVG(wt.size)                   AS avg_trade_size,
      MAX(wt.size)                   AS max_trade_size,
      SUM(CASE WHEN LOWER(wt.category)='politics' THEN 1 ELSE 0 END) AS politics_trades,
      SUM(CASE WHEN LOWER(wt.category)='geopolitics' THEN 1 ELSE 0 END) AS geo_trades,
      SUM(CASE WHEN LOWER(wt.category)='crypto' THEN 1 ELSE 0 END)   AS crypto_trades,
      SUM(CASE WHEN LOWER(wt.category)='sports' THEN 1 ELSE 0 END)   AS sports_trades,
      SUM(CASE WHEN LOWER(wt.category)='economics' THEN 1 ELSE 0 END) AS econ_trades,
      SUM(CASE WHEN wt.side='BUY' THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS buy_ratio,
      AVG(wt.price) AS avg_entry_price,
      SUM(CASE WHEN wt.price<0.20 THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS low_price_ratio,
      SUM(CASE WHEN wt.price>0.80 THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS high_price_ratio,
      SUM(CASE WHEN wt.price BETWEEN 0.35 AND 0.65 THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS uncertain_ratio,
      SUM(CASE WHEN wt.market_resolved=1 THEN 1 ELSE 0 END) AS resolved_count,
      SUM(CASE WHEN wt.pnl>0 THEN 1 ELSE 0 END) AS trade_wins,
      SUM(CASE WHEN wt.pnl<0 THEN 1 ELSE 0 END) AS trade_losses,
      SUM(wt.pnl)                    AS realized_pnl,
      AVG(wt.pnl)                    AS avg_pnl_per_trade,
      (MAX(wt.timestamp)-MIN(wt.timestamp))/86400.0 AS span_days
    FROM wallet_profiles wp
    LEFT JOIN wallet_trades wt ON wp.wallet=wt.wallet
    LEFT JOIN leaderboard l ON wp.wallet=l.wallet
    GROUP BY wp.wallet
    HAVING total_trades >= 10
    """,
    conn,
)

wallets["trades_per_day"] = wallets["total_trades"] / wallets["span_days"].clip(lower=1)
wallets["resolved_wr"] = wallets["trade_wins"] / wallets["resolved_count"].clip(lower=1)
wallets["concentration"] = wallets["total_trades"] / wallets["unique_markets"].clip(lower=1)

print(f"Total wallets with 10+ trades: {len(wallets)}")
print(f"With resolved trades: {(wallets.resolved_count >= 1).sum()}")
print(f"With 5+ resolved: {(wallets.resolved_count >= 5).sum()}\n")

print("── TOP 10 BY REALIZED PnL ──")
top_realized = wallets[wallets.realized_pnl.notna()].nlargest(10, "realized_pnl")
for _, r in top_realized.iterrows():
    print(
        f"  {r.wallet[:14]} tier={str(r.tier):6s} realized=${r.realized_pnl:>12,.0f} "
        f"res={r.resolved_count:>3.0f} WR={r.resolved_wr:.0%} trades={r.total_trades:>5.0f} "
        f"mkts={r.unique_markets:>4.0f} avg=${r.avg_trade_size:>7,.0f} bkt={r.wallet_bucket}"
    )

print("\n── TOP 10 BY WIN RATE (10+ resolved) ──")
enough = wallets[wallets.resolved_count >= 10]
top_wr = enough.nlargest(10, "resolved_wr")
for _, r in top_wr.iterrows():
    print(
        f"  {r.wallet[:14]} tier={str(r.tier):6s} WR={r.resolved_wr:.0%} "
        f"res={r.resolved_count:>3.0f} realized=${r.realized_pnl:>10,.0f} "
        f"avg=${r.avg_trade_size:>7,.0f} bkt={r.wallet_bucket}"
    )

print("\n── TOP 10 BY VOLUME ──")
top_vol = wallets.nlargest(10, "total_volume")
for _, r in top_vol.iterrows():
    print(
        f"  {r.wallet[:14]} vol=${r.total_volume:>12,.0f} "
        f"trades={r.total_trades:>5.0f} avg=${r.avg_trade_size:>7,.0f} "
        f"WR={r.resolved_wr:.0%} realized=${r.realized_pnl or 0:>10,.0f}"
    )

top_set = set(top_realized.wallet.tolist()[:10] + top_wr.wallet.tolist()[:10])
with open(f"{TMP}/top_wallets.txt", "w") as f:
    f.write("\n".join(top_set))
wallets.to_parquet(f"{TMP}/wallets.parquet", index=False)
print(f"\nSelected {len(top_set)} wallets for deep dive")


# ── SECTION 2: Per-wallet deep dive (compact) ──────────────────────────────
print("\n" + "=" * 70)
print("SECTION 2 — WALLET STRATEGIES")
print("=" * 70)

for wallet in sorted(top_set):
    t = pd.read_sql(
        "SELECT timestamp, side, size, price, title, condition_id, category, "
        "market_resolved, pnl FROM wallet_trades WHERE wallet=? ORDER BY timestamp",
        conn,
        params=(wallet,),
    )
    if t.empty:
        continue
    t["dt"] = pd.to_datetime(t["timestamp"], unit="s")

    n = len(t)
    nmkt = t["condition_id"].nunique()
    span = max((t["dt"].max() - t["dt"].min()).days, 1)
    buy_ratio = (t["side"] == "BUY").mean()
    avg_price = t["price"].mean()
    low_p = (t["price"] < 0.20).mean()
    high_p = (t["price"] > 0.80).mean()
    mid_p = t["price"].between(0.35, 0.65).mean()
    size_cv = t["size"].std() / max(t["size"].mean(), 1)
    conc = n / max(nmkt, 1)
    cat_counts = t["category"].value_counts()
    top_cat = cat_counts.index[0] if len(cat_counts) else "?"
    top_cat_pct = cat_counts.iloc[0] / n if len(cat_counts) else 0

    resolved = t[t["pnl"].notna()]
    wins = (resolved["pnl"] > 0).sum() if len(resolved) else 0
    losses = (resolved["pnl"] < 0).sum() if len(resolved) else 0
    r_pnl = resolved["pnl"].sum() if len(resolved) else 0
    r_wr = wins / max(len(resolved), 1)

    tags = []
    if 0.4 <= buy_ratio <= 0.6 and size_cv < 0.5 and t["size"].mean() > 500:
        tags.append("MM/ARB")
    if buy_ratio > 0.7 and size_cv > 0.5 and conc > 2:
        tags.append("CONVICTION_DIR")
    if nmkt < 20 and t["size"].max() > 5000 and conc > 3:
        tags.append("SNIPER")
    if nmkt > 50 and conc < 2:
        tags.append("SPRAY")
    if top_cat_pct > 0.7:
        tags.append(f"{top_cat}_SPECIALIST")
    if mid_p > 0.4:
        tags.append("UNCERTAIN_ZONE")
    if high_p > 0.4:
        tags.append("LATE_MOMENTUM")
    if low_p > 0.3:
        tags.append("LONGSHOT")
    if n / span > 10:
        tags.append("HFT")
    if conc > 4 and size_cv > 0.5:
        tags.append("ACCUMULATOR")
    if not tags:
        tags.append("MIXED")

    print(
        f"\n{wallet[:16]} | n={n} mkts={nmkt} span={span}d | buy={buy_ratio:.0%} "
        f"avgP={avg_price:.2f} midP={mid_p:.0%} | CV={size_cv:.2f} conc={conc:.1f} "
        f"| cat={top_cat}({top_cat_pct:.0%})"
    )
    if len(resolved):
        print(
            f"  resolved={len(resolved)} WR={r_wr:.0%} pnl=${r_pnl:,.0f} "
            f"W=${resolved[resolved.pnl>0].pnl.mean() if wins else 0:,.0f} "
            f"L=${resolved[resolved.pnl<0].pnl.mean() if losses else 0:,.0f}"
        )
    else:
        print("  resolved=0 (all trades pending)")
    # category PnL breakdown
    if len(resolved):
        cat_pnl = resolved.groupby("category")["pnl"].agg(["count", "sum", lambda s: (s>0).sum()])
        cat_pnl.columns = ["n", "pnl", "w"]
        cat_pnl = cat_pnl.sort_values("pnl", ascending=False).head(4)
        parts = [f"{c}:n={r.n},pnl=${r.pnl:,.0f},WR={r.w/r.n:.0%}" for c, r in cat_pnl.iterrows()]
        print(f"  by_cat: {' | '.join(parts)}")
    print(f"  tags: {', '.join(tags)}")


# ── SECTION 3: Winners vs Losers ───────────────────────────────────────────
print("\n" + "=" * 70)
print("SECTION 3 — WINNERS vs LOSERS")
print("=" * 70)

# Prefer realized_pnl where available; fallback to profile_pnl
has_real = wallets[wallets.realized_pnl.notna() & (wallets.resolved_count >= 5)]
if len(has_real) >= 10:
    pnl_col = "realized_pnl"
    df = has_real
else:
    pnl_col = "profile_pnl"
    df = wallets[wallets.profile_pnl.notna() & (wallets.total_trades >= 10)]

w = df[df[pnl_col] > 0]
l = df[df[pnl_col] <= 0]
print(f"Using {pnl_col}: winners={len(w)} losers={len(l)}\n")

cmp_cols = [
    ("avg_trade_size", "Avg trade $"),
    ("total_volume", "Total volume $"),
    ("concentration", "Trades/market"),
    ("buy_ratio", "Buy ratio"),
    ("avg_entry_price", "Avg entry price"),
    ("low_price_ratio", "% price<0.20"),
    ("high_price_ratio", "% price>0.80"),
    ("uncertain_ratio", "% price 0.35-0.65"),
    ("trades_per_day", "Trades/day"),
    ("unique_markets", "Unique markets"),
    ("resolved_count", "Resolved trades"),
]

print(f"{'Metric':28s} {'Winners':>14s} {'Losers':>14s} {'Δ%':>10s}  Signal")
for c, label in cmp_cols:
    wm, lm = w[c].mean(), l[c].mean()
    d = (wm - lm) / (abs(lm) + 1e-9) * 100
    star = "***" if abs(d) > 50 else "**" if abs(d) > 25 else "*" if abs(d) > 10 else ""
    print(f"  {label:26s} {wm:>14,.3f} {lm:>14,.3f} {d:>+9.0f}%  {star}")

print("\nCategory share (winners vs losers):")
for c in ["politics_trades", "geo_trades", "crypto_trades", "sports_trades", "econ_trades"]:
    wp = (w[c] / w["total_trades"].clip(lower=1)).mean()
    lp = (l[c] / l["total_trades"].clip(lower=1)).mean()
    print(f"  {c.replace('_trades',''):12s} W={wp:.0%} L={lp:.0%}  Δ={wp-lp:+.0%}")

print("\nTier distribution:")
for tier in sorted(set(df["tier"].dropna().tolist())):
    wp = (w.tier == tier).mean()
    lp = (l.tier == tier).mean()
    print(f"  {str(tier):8s} W={wp:.0%} L={lp:.0%}")

print("\nBucket distribution:")
for bk in sorted(set(df.wallet_bucket.dropna().tolist())):
    wp = (w.wallet_bucket == bk).mean()
    lp = (l.wallet_bucket == bk).mean()
    print(f"  {str(bk):25s} W={wp:.0%} L={lp:.0%}")


# ── SECTION 4: Trade-level patterns ────────────────────────────────────────
print("\n" + "=" * 70)
print("SECTION 4 — TRADE-LEVEL PATTERNS (across top wallets)")
print("=" * 70)

qmarks = ",".join("?" for _ in top_set)
trades = pd.read_sql(
    f"SELECT wallet, side, size, price, category, pnl, timestamp, condition_id "
    f"FROM wallet_trades WHERE wallet IN ({qmarks})",
    conn,
    params=list(top_set),
)
resolved = trades[trades.pnl.notna()].copy()
resolved["win"] = (resolved.pnl > 0).astype(int)
resolved["dt"] = pd.to_datetime(resolved.timestamp, unit="s")
print(f"Total trades: {len(trades)}  resolved: {len(resolved)}")

if len(resolved):
    print("\n── Entry price buckets vs WR / PnL ──")
    bins = [0, 0.10, 0.20, 0.35, 0.50, 0.65, 0.80, 0.90, 1.01]
    resolved["pbin"] = pd.cut(resolved.price, bins=bins)
    pbin = resolved.groupby("pbin", observed=True).agg(n=("win","count"), w=("win","sum"), pnl=("pnl","sum"))
    pbin["wr"] = pbin.w / pbin.n
    for idx, r in pbin.iterrows():
        if r.n < 3: continue
        print(f"  {str(idx):15s} n={r.n:>5.0f} WR={r.wr:.0%} pnl=${r.pnl:>10,.0f}")

    print("\n── Trade size vs WR ──")
    sbins = [0, 50, 200, 500, 1000, 5000, 50000, 1e12]
    slab = ["<50", "50-200", "200-500", "500-1k", "1k-5k", "5k-50k", "50k+"]
    resolved["sbin"] = pd.cut(resolved["size"], bins=sbins, labels=slab)
    sbin = resolved.groupby("sbin", observed=True).agg(n=("win","count"), w=("win","sum"), pnl=("pnl","mean"))
    sbin["wr"] = sbin.w / sbin.n
    for idx, r in sbin.iterrows():
        if r.n < 3: continue
        print(f"  {str(idx):10s} n={r.n:>5.0f} WR={r.wr:.0%} avg_pnl=${r.pnl:>8,.0f}")

    print("\n── Side ──")
    side = resolved.groupby("side").agg(n=("win","count"), w=("win","sum"), pnl=("pnl","sum"))
    for idx, r in side.iterrows():
        print(f"  {idx} n={r.n:.0f} WR={r.w/r.n:.0%} pnl=${r.pnl:,.0f}")

    print("\n── Category ──")
    cat = resolved.groupby("category").agg(n=("win","count"), w=("win","sum"), pnl=("pnl","sum"))
    cat["wr"] = cat.w / cat.n
    cat = cat.sort_values("pnl", ascending=False)
    for idx, r in cat.iterrows():
        if r.n < 3: continue
        print(f"  {str(idx):14s} n={r.n:>5.0f} WR={r.wr:.0%} pnl=${r.pnl:>10,.0f}")

    # position-building patterns
    print("\n── Position-building patterns ──")
    groups = trades.groupby(["wallet", "condition_id"]).filter(lambda x: len(x) >= 3)
    pat = {"inc_size":0, "dec_size":0, "flat_size":0, "mixed_side":0, "single_side":0, "front":0, "back":0}
    gg = groups.groupby(["wallet", "condition_id"])
    for _, g in gg:
        sizes = g["size"].tolist(); sides = g.side.tolist()
        if len(sizes) >= 3:
            if sizes[-1] > sizes[0]*1.5: pat["inc_size"] += 1
            elif sizes[-1] < sizes[0]*0.5: pat["dec_size"] += 1
            else: pat["flat_size"] += 1
        if len(set(sides)) > 1: pat["mixed_side"] += 1
        else: pat["single_side"] += 1
        tot = sum(sizes); fh = sum(sizes[:len(sizes)//2])
        if tot and fh > tot*0.65: pat["front"] += 1
        elif tot and fh < tot*0.35: pat["back"] += 1
    n_g = len(gg)
    print(f"  Multi-trade markets: {n_g}")
    for k, v in pat.items():
        print(f"    {k:13s} {v:>5d} ({v/max(n_g,1)*100:.0f}%)")


# ── SECTION 5: Convergence ─────────────────────────────────────────────────
print("\n" + "=" * 70)
print("SECTION 5 — CONVERGENCE")
print("=" * 70)

conv = pd.read_sql(
    """
    SELECT wt.condition_id, MAX(wt.title) AS title, MAX(wt.category) AS category,
           COUNT(DISTINCT wt.wallet) AS n_wallets,
           SUM(CASE WHEN wt.side='BUY' THEN 1 ELSE 0 END)  AS buys,
           SUM(CASE WHEN wt.side='SELL' THEN 1 ELSE 0 END) AS sells,
           SUM(wt.size) AS vol,
           SUM(CASE WHEN wt.pnl IS NOT NULL THEN 1 ELSE 0 END) AS resolved_t,
           SUM(CASE WHEN wt.pnl>0 THEN 1 ELSE 0 END) AS wins,
           SUM(wt.pnl) AS pnl
    FROM wallet_trades wt
    JOIN leaderboard l ON wt.wallet=l.wallet
    WHERE l.tier IN ('tier1h','tier1')
    GROUP BY wt.condition_id
    """,
    conn,
)
print(f"Markets with 1+ top-tier wallet: {len(conv)}")
for min_w in [1, 2, 3, 4, 5]:
    s = conv[conv.n_wallets >= min_w]
    rs = s[s.resolved_t > 0]
    if len(rs) == 0:
        print(f"  {min_w}+ wallets: markets={len(s)}  no resolved data")
        continue
    wr = rs.wins.sum() / rs.resolved_t.sum() if rs.resolved_t.sum() else 0
    pnl = rs.pnl.sum()
    print(f"  {min_w}+ wallets: markets={len(s):>4d}  resolved={len(rs):>4d}  WR={wr:.0%}  PnL=${pnl:>12,.0f}")

print("\nSample 3+ wallet convergence markets:")
top_conv = conv[conv.n_wallets >= 3].nlargest(10, "vol")
for _, r in top_conv.iterrows():
    direction = "BUY" if r.buys > r.sells * 2 else "SELL" if r.sells > r.buys * 2 else "MIXED"
    pnl = f"${r.pnl:,.0f}" if r.resolved_t else "pending"
    print(f"  [{r.n_wallets}w {direction:5s}] vol=${r.vol:>9,.0f} pnl={pnl:>12s} {str(r.title)[:55]}")

conn.close()
print("\nDONE")
