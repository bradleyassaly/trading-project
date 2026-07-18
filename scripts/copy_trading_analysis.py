"""
Per-wallet whale_entry copy-trading analysis.

Slices the existing 87 resolved whale_entry signals by individual wallet,
wallet archetype, and (wallet x category) cells. Outputs:
  - /tmp/per_wallet_perf.parquet
  - /tmp/wallet_archetypes.parquet
  - /tmp/wallet_category_matrix.parquet
  - data/polymarket/copy_trading_whitelist.json (only if winners exist)

Read-only against wallet_intelligence.db. Re-runnable.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd
from scipy import stats

DB = "data/polymarket/wallet_intelligence.db"
OUT = Path(os.environ.get("TMPDIR", "/tmp"))
OUT.mkdir(parents=True, exist_ok=True)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)


def banner(title: str) -> None:
    bar = "=" * 72
    print(f"\n{bar}")
    print(f"  {title}")
    print(bar)


def drop_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    cats = [c for c in df.columns if str(df[c].dtype).startswith("category")]
    return df.drop(columns=cats) if cats else df


# ──────────────────────────────────────────────────────────────────────
# Part 1 — per-wallet copy trading EV
# ──────────────────────────────────────────────────────────────────────

banner("PART 1 — PER-WALLET COPY TRADING EV")

conn = sqlite3.connect(DB, timeout=60)

df = pd.read_sql(
    """
    SELECT so.id, so.fired_at, so.condition_id, so.token_id,
           so.question, so.category, so.direction, so.confidence,
           so.entry_price, so.wallet,
           so.resolution_price, so.outcome_delta, so.is_win, so.hold_days,
           so.paper_trade_id, so.paper_pnl,
           wcp.tier              AS cat_tier,
           wcp.win_rate          AS cat_wr,
           wcp.monthly_pnl       AS cat_monthly_pnl,
           wcp.tier_score        AS cat_tier_score,
           wcp.consistency_score AS cat_consistency,
           wcp.resolved_trades   AS cat_resolved,
           wp.pnl_30d            AS wp_pnl_30d,
           wp.total_volume_usdc  AS wp_total_volume,
           wp.resolved_trades    AS wp_resolved,
           wp.wallet_type        AS wp_type,
           wp.wallet_bucket      AS wp_bucket,
           wp.volume_tier        AS wp_volume_tier,
           wp.directional_win_rate AS wp_directional_wr,
           wp.conviction_score   AS wp_conviction
    FROM signal_outcomes so
    LEFT JOIN wallet_category_profiles wcp
      ON so.wallet = wcp.wallet AND so.category = wcp.category
    LEFT JOIN wallet_profiles wp
      ON so.wallet = wp.wallet
    WHERE so.signal_type = 'whale_entry'
      AND so.resolution_price IS NOT NULL
    """,
    conn,
)
print(f"resolved whale_entry signals: {len(df)}")
print(f"unique wallets: {df['wallet'].nunique()}")
print(f"unique (wallet, category) pairs: {df.groupby(['wallet','category']).ngroups}")

wp_rows = []
for wallet, group in df.groupby("wallet"):
    n = len(group)
    if n < 2:
        continue
    wr = group["is_win"].mean()
    ev = group["outcome_delta"].mean()
    if n >= 5:
        t, p = stats.ttest_1samp(group["outcome_delta"], 0)
        p = p / 2 if ev > 0 else 1.0 - p / 2
    else:
        p = 1.0
    row = group.iloc[0]
    wp_rows.append({
        "wallet_short": wallet[:12] + "...",
        "wallet": wallet,
        "n": n,
        "wr": round(wr, 3),
        "ev": round(ev, 4),
        "total_ev": round(group["outcome_delta"].sum(), 4),
        "p_value": round(p, 4),
        "tier": row.get("cat_tier"),
        "tier_score": row.get("cat_tier_score"),
        "tier_wr": row.get("cat_wr"),
        "consistency": row.get("cat_consistency"),
        "wp_type": row.get("wp_type"),
        "wp_bucket": row.get("wp_bucket"),
        "wp_volume_tier": row.get("wp_volume_tier"),
        "categories": ", ".join(sorted(group["category"].dropna().unique())),
    })

wp_df = pd.DataFrame(wp_rows).sort_values("ev", ascending=False)
print(f"\nwallets with >=2 signals: {len(wp_df)}")
print("\n-- per-wallet performance (n>=2) --")
display_cols = ["wallet_short", "n", "wr", "ev", "total_ev", "p_value", "tier", "wp_type", "categories"]
print(wp_df[display_cols].to_string(index=False))

positive = wp_df[wp_df["ev"] > 0]
negative = wp_df[wp_df["ev"] <= 0]
print(f"\n-- bimodality check --")
print(f"positive-EV wallets: {len(positive)}/{len(wp_df)}  ({len(positive)/max(len(wp_df),1):.1%})")
print(f"  total_ev contributed: {positive['total_ev'].sum():+.2f}")
print(f"  avg ev across these wallets: {positive['ev'].mean():+.4f}")
print(f"negative-EV wallets: {len(negative)}/{len(wp_df)}  ({len(negative)/max(len(wp_df),1):.1%})")
print(f"  total_ev contributed: {negative['total_ev'].sum():+.2f}")
print(f"  avg ev across these wallets: {negative['ev'].mean():+.4f}")

if len(positive) > 0:
    pos_wallets = set(positive["wallet"])
    pos_signals = df[df["wallet"].isin(pos_wallets)]
    print(f"\n-- if we ONLY copied positive-EV wallets --")
    print(f"  n={len(pos_signals)}  WR={pos_signals['is_win'].mean():.1%}  EV={pos_signals['outcome_delta'].mean():+.4f}")
    if len(pos_signals) >= 5:
        t, p = stats.ttest_1samp(pos_signals["outcome_delta"], 0)
        p_one = p / 2 if pos_signals["outcome_delta"].mean() > 0 else 1.0 - p / 2
        print(f"  one-tailed p: {p_one:.4f}")

drop_categoricals(wp_df).to_parquet(OUT / "per_wallet_perf.parquet", index=False)

# ──────────────────────────────────────────────────────────────────────
# Part 2 — feature comparison / predictive rules
# ──────────────────────────────────────────────────────────────────────

banner("PART 2 — COPYABLE WALLET PROFILE")

if len(wp_df) > 0:
    wp_df = wp_df.copy()
    wp_df["copyable"] = (wp_df["ev"] > 0).astype(int)

    print("-- feature means: copyable vs non-copyable --")
    feats = ["n", "tier_score", "tier_wr", "consistency"]
    print(f"{'feature':>15} {'copyable':>12} {'non_copy':>12} {'diff':>10}")
    for f in feats:
        if f in wp_df.columns and wp_df[f].notna().any():
            cm = wp_df.loc[wp_df["copyable"] == 1, f].mean()
            nm = wp_df.loc[wp_df["copyable"] == 0, f].mean()
            diff = (cm or 0) - (nm or 0) if pd.notna(cm) and pd.notna(nm) else float("nan")
            print(f"{f:>15} {(cm or float('nan')):>12.4f} {(nm or float('nan')):>12.4f} {diff:>+10.4f}")

    print("\n-- tier composition --")
    for label, g in [("copyable", wp_df[wp_df["copyable"] == 1]),
                     ("non_copy", wp_df[wp_df["copyable"] == 0])]:
        tc = g["tier"].fillna("untiered").value_counts()
        print(f"  {label} ({len(g)} wallets): {dict(tc)}")

    print("\n-- wp_type composition --")
    for label, g in [("copyable", wp_df[wp_df["copyable"] == 1]),
                     ("non_copy", wp_df[wp_df["copyable"] == 0])]:
        tc = g["wp_type"].fillna("unknown").value_counts()
        print(f"  {label}: {dict(tc)}")

    print("\n-- wp_bucket composition --")
    for label, g in [("copyable", wp_df[wp_df["copyable"] == 1]),
                     ("non_copy", wp_df[wp_df["copyable"] == 0])]:
        tc = g["wp_bucket"].fillna("unknown").value_counts()
        print(f"  {label}: {dict(tc)}")

    print("\n-- predictive rule scan --")
    # Tier-based
    for tset in [["S"], ["S", "A"], ["S", "A", "B"]]:
        sub = wp_df[wp_df["tier"].isin(tset)]
        if len(sub) > 0:
            print(f"  tier in {tset}: copyable_rate={sub['copyable'].mean():.1%}  "
                  f"n_wallets={len(sub)}  avg_ev={sub['ev'].mean():+.4f}")
    # wp_type-based
    for wtype in wp_df["wp_type"].dropna().unique():
        sub = wp_df[wp_df["wp_type"] == wtype]
        if len(sub) >= 2:
            print(f"  wp_type='{wtype}': copyable_rate={sub['copyable'].mean():.1%}  "
                  f"n_wallets={len(sub)}  avg_ev={sub['ev'].mean():+.4f}")
    # wp_bucket-based
    for b in wp_df["wp_bucket"].dropna().unique():
        sub = wp_df[wp_df["wp_bucket"] == b]
        if len(sub) >= 2:
            print(f"  wp_bucket='{b}': copyable_rate={sub['copyable'].mean():.1%}  "
                  f"n_wallets={len(sub)}  avg_ev={sub['ev'].mean():+.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 3 — wallet archetype classification (use existing wallet_type +
# behavioral fallback)
# ──────────────────────────────────────────────────────────────────────

banner("PART 3 — WALLET ARCHETYPE")

print("-- wallet_profiles.wallet_type distribution --")
wt_dist = pd.read_sql(
    "SELECT COALESCE(wallet_type,'NULL') AS wallet_type, COUNT(*) AS n "
    "FROM wallet_profiles GROUP BY wallet_type ORDER BY n DESC",
    conn,
)
print(wt_dist.to_string(index=False))

print("\n-- wallet_profiles.wallet_bucket distribution --")
wb_dist = pd.read_sql(
    "SELECT COALESCE(wallet_bucket,'NULL') AS wallet_bucket, COUNT(*) AS n "
    "FROM wallet_profiles GROUP BY wallet_bucket ORDER BY n DESC LIMIT 15",
    conn,
)
print(wb_dist.to_string(index=False))

# Behavioral classification (fallback for wallets that aren't in wallet_profiles)
print("\n-- computing behavioral archetypes --")
beh = pd.read_sql(
    """
    SELECT wallet,
           COUNT(*) AS total_trades,
           COUNT(DISTINCT condition_id) AS unique_markets,
           CAST(COUNT(*) AS REAL) / NULLIF(COUNT(DISTINCT condition_id), 0) AS fills_per_market,
           AVG(size * price) AS avg_trade_size_usd,
           SUM(size * price) AS total_volume_usd,
           SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS buy_ratio,
           COUNT(DISTINCT category) AS n_categories,
           (MAX(timestamp) - MIN(timestamp)) * 1.0 / NULLIF(COUNT(*) - 1, 0) AS avg_trade_interval,
           SUM(CASE WHEN price < 0.10 OR price > 0.90 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS extreme_price_ratio
    FROM wallet_trades
    GROUP BY wallet
    HAVING total_trades >= 10
    """,
    conn,
)


def classify(row: pd.Series) -> str:
    if (row["fills_per_market"] or 0) > 20 and (row["extreme_price_ratio"] or 0) > 0.3:
        return "arb_bot"
    if (row["fills_per_market"] or 0) > 15 and 0.35 < (row["buy_ratio"] or 0) < 0.65:
        return "market_maker"
    if (row["avg_trade_interval"] or 1e9) < 60 and (row["total_trades"] or 0) > 500:
        return "algo_bot"
    if (row["n_categories"] or 0) <= 2 and (row["fills_per_market"] or 0) <= 8:
        return "specialist"
    if (row["buy_ratio"] or 0) > 0.75 or (row["buy_ratio"] or 0) < 0.25:
        return "directional"
    if (row["n_categories"] or 0) >= 4:
        return "diversified"
    return "unclassified"


beh["beh_archetype"] = beh.apply(classify, axis=1)
print(f"\nbehavioral archetype distribution ({len(beh)} wallets in wallet_trades):")
print(beh["beh_archetype"].value_counts().to_string())

beh[["wallet", "beh_archetype", "fills_per_market", "buy_ratio", "n_categories",
     "avg_trade_interval", "extreme_price_ratio"]].to_parquet(
    OUT / "wallet_archetypes.parquet", index=False,
)

# Pull whale_entry signals + merge both classifications
sig = pd.read_sql(
    """
    SELECT so.wallet, so.outcome_delta, so.is_win, so.category,
           so.entry_price, so.confidence,
           wp.wallet_type AS wp_type
    FROM signal_outcomes so
    LEFT JOIN wallet_profiles wp ON so.wallet = wp.wallet
    WHERE so.signal_type = 'whale_entry'
      AND so.resolution_price IS NOT NULL
    """,
    conn,
)
arch_join = sig.merge(beh[["wallet", "beh_archetype"]], on="wallet", how="left")
arch_join["beh_archetype"] = arch_join["beh_archetype"].fillna("not_in_wallet_trades")
arch_join["wp_type"] = arch_join["wp_type"].fillna("unknown")

print("\n-- copy trading EV by wallet_profiles.wallet_type --")
print(f"{'wallet_type':>20} {'n':>5} {'WR':>7} {'avg_EV':>9} {'total_EV':>10} {'p':>7}")
for wt, g in arch_join.groupby("wp_type"):
    n = len(g)
    if n < 3:
        continue
    ev = g["outcome_delta"].mean()
    if n >= 5:
        _, p = stats.ttest_1samp(g["outcome_delta"], 0)
        p = p / 2 if ev > 0 else 1.0 - p / 2
    else:
        p = 1.0
    flag = "  GOOD" if ev > 0 and p < 0.15 else ("  pos " if ev > 0 else "")
    print(f"{wt:>20} {n:>5} {g['is_win'].mean():>7.1%} {ev:>+9.4f} {g['outcome_delta'].sum():>+10.4f} {p:>7.3f}{flag}")

print("\n-- copy trading EV by behavioral archetype --")
print(f"{'archetype':>22} {'n':>5} {'WR':>7} {'avg_EV':>9} {'total_EV':>10} {'p':>7}")
for arch, g in arch_join.groupby("beh_archetype"):
    n = len(g)
    if n < 3:
        continue
    ev = g["outcome_delta"].mean()
    if n >= 5:
        _, p = stats.ttest_1samp(g["outcome_delta"], 0)
        p = p / 2 if ev > 0 else 1.0 - p / 2
    else:
        p = 1.0
    flag = "  GOOD" if ev > 0 and p < 0.15 else ("  pos " if ev > 0 else "")
    print(f"{arch:>22} {n:>5} {g['is_win'].mean():>7.1%} {ev:>+9.4f} {g['outcome_delta'].sum():>+10.4f} {p:>7.3f}{flag}")

# Cross-tabulate wp_type x category for the positive types
for wt in arch_join.groupby("wp_type")["outcome_delta"].mean().sort_values(ascending=False).index[:3]:
    sub = arch_join[arch_join["wp_type"] == wt]
    if len(sub) < 5:
        continue
    by_cat = sub.groupby("category").agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).round(4)
    print(f"\n  wp_type='{wt}' x category:")
    print(by_cat.to_string())

# ──────────────────────────────────────────────────────────────────────
# Part 4 — wallet × category matrix
# ──────────────────────────────────────────────────────────────────────

banner("PART 4 — WALLET x CATEGORY MATRIX")

mx_rows = []
for (wallet, cat), g in df.groupby(["wallet", "category"]):
    n = len(g)
    if n < 2:
        continue
    mx_rows.append({
        "wallet_short": wallet[:12] + "...",
        "wallet": wallet,
        "category": cat,
        "n": n,
        "wr": round(g["is_win"].mean(), 3),
        "ev": round(g["outcome_delta"].mean(), 4),
        "total_ev": round(g["outcome_delta"].sum(), 4),
        "tier": g["cat_tier"].iloc[0],
    })

mx = pd.DataFrame(mx_rows).sort_values("ev", ascending=False)
print(f"unique (wallet, category) cells with n>=2: {len(mx)}")
print("\n-- all wallet x category cells --")
print(mx[["wallet_short", "category", "n", "wr", "ev", "total_ev", "tier"]].to_string(index=False))

positive_cells = mx[mx["ev"] > 0]
negative_cells = mx[mx["ev"] <= 0]
print(f"\npositive-EV cells: {len(positive_cells)}/{len(mx)}")
print(f"negative-EV cells: {len(negative_cells)}/{len(mx)}")

both_pos_and_neg = set(positive_cells["wallet"]) & set(negative_cells["wallet"])
print(f"\nwallets that are positive in some categories but negative in others: {len(both_pos_and_neg)}")
for w in list(both_pos_and_neg)[:5]:
    g = mx[mx["wallet"] == w]
    print(f"  {w[:14]}...:")
    for _, r in g.iterrows():
        flag = "+" if r["ev"] > 0 else "-"
        print(f"    [{flag}] {r['category']:>15}: n={r['n']}, ev={r['ev']:+.4f}")

print("\n-- POLITICAL/GEO cells --")
pol = mx[mx["category"].isin(["politics", "geopolitics"])]
if len(pol) > 0:
    print(pol.to_string(index=False))
    pos_pol = pol[pol["ev"] > 0]
    print(f"copyable political cells: {len(pos_pol)}/{len(pol)}")
else:
    print("no political/geo cells with n>=2 — sample too thin")

mx.to_parquet(OUT / "wallet_category_matrix.parquet", index=False)

# ──────────────────────────────────────────────────────────────────────
# Part 5 — whitelist construction
# ──────────────────────────────────────────────────────────────────────

banner("PART 5 — WHITELIST CONSTRUCTION")

# Merge archetype info into matrix
mx_with_arch = mx.merge(
    beh[["wallet", "beh_archetype"]],
    on="wallet", how="left",
)
mx_with_arch["beh_archetype"] = mx_with_arch["beh_archetype"].fillna("not_in_wallet_trades")

print("-- candidate filter sweep --")
print(f"{'min_n':>6} {'min_wr':>7} {'excl_bots':>10} {'cells':>6} {'agg_n':>6} {'agg_wr':>8} {'agg_ev':>9} {'p':>7}")
sweeps = []
for min_n in (2, 3, 5):
    for min_wr in (0.0, 0.5, 0.6):
        for excl in (True, False):
            cand = mx_with_arch[(mx_with_arch["ev"] > 0)
                                & (mx_with_arch["n"] >= min_n)
                                & (mx_with_arch["wr"] >= min_wr)]
            if excl:
                cand = cand[~cand["beh_archetype"].isin(["arb_bot", "market_maker", "algo_bot"])]
            if len(cand) == 0:
                continue
            pairs = set(zip(cand["wallet"], cand["category"]))
            agg = df[df.apply(lambda r: (r["wallet"], r["category"]) in pairs, axis=1)]
            n = len(agg)
            wr = agg["is_win"].mean() if n else 0
            ev = agg["outcome_delta"].mean() if n else 0
            if n >= 5:
                _, p = stats.ttest_1samp(agg["outcome_delta"], 0)
                p_one = p / 2 if ev > 0 else 1.0 - p / 2
            else:
                p_one = 1.0
            sweeps.append({
                "min_n": min_n, "min_wr": min_wr, "excl_bots": excl,
                "cells": len(cand), "agg_n": n, "agg_wr": round(wr, 4),
                "agg_ev": round(ev, 4), "p_value": round(p_one, 4),
            })
            print(f"{min_n:>6} {min_wr:>7.0%} {str(excl):>10} {len(cand):>6} {n:>6} {wr:>8.1%} {ev:>+9.4f} {p_one:>7.4f}")

# Pick the best statistically significant whitelist
sweeps_df = pd.DataFrame(sweeps)
viable = sweeps_df[(sweeps_df["agg_n"] >= 10) & (sweeps_df["agg_ev"] > 0)
                   & (sweeps_df["p_value"] < 0.15)]
print(f"\nstat-sig whitelists (agg_n>=10, ev>0, p<0.15): {len(viable)}")

if len(viable) > 0:
    best = viable.sort_values("agg_ev", ascending=False).iloc[0]
    print("BEST:", dict(best))
    cand = mx_with_arch[(mx_with_arch["ev"] > 0)
                        & (mx_with_arch["n"] >= best["min_n"])
                        & (mx_with_arch["wr"] >= best["min_wr"])]
    if best["excl_bots"]:
        cand = cand[~cand["beh_archetype"].isin(["arb_bot", "market_maker", "algo_bot"])]

    print(f"\nwhitelist cells ({len(cand)}):")
    print(cand[["wallet_short", "category", "n", "wr", "ev", "total_ev", "tier", "beh_archetype"]].to_string(index=False))

    wl = []
    for _, row in cand.iterrows():
        wl.append({
            "wallet": row["wallet"],
            "category": row["category"],
            "ev": float(row["ev"]),
            "win_rate": float(row["wr"]),
            "sample_size": int(row["n"]),
            "tier": row["tier"],
            "archetype": row["beh_archetype"],
        })
    out_path = Path("data/polymarket/copy_trading_whitelist.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(wl, indent=2))
    print(f"\nwrote {out_path} ({len(wl)} entries)")
else:
    print("\nNO viable whitelist found.")
    print("Per-wallet copy-trading does not have a recoverable edge in this sample.")
    print("Recommendation: retire whale_entry, focus on accumulation + market_maker_flip.")

conn.close()
