"""
Whale entry experiment harness.

Runs Parts 1-11 of the whale-entry experiment plan as a single script so
the intermediate parquet files stay hot and schema fixes apply once.
Read-only against wallet_intelligence.db. Emits results to
/tmp/whale_entry_* and prints a section-by-section report to stdout.
"""
from __future__ import annotations

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


# ──────────────────────────────────────────────────────────────────────
# Part 1 — baseline dataset
# ──────────────────────────────────────────────────────────────────────

banner("PART 1 — BASELINE DATASET")

conn = sqlite3.connect(DB, timeout=60)
df = pd.read_sql(
    """
    SELECT so.*,
           wcp.tier              AS wallet_tier_in_category,
           wcp.win_rate          AS wallet_category_wr,
           wcp.resolved_trades   AS wallet_category_resolved,
           wcp.monthly_pnl       AS wallet_category_monthly_pnl,
           wcp.consistency_score AS wallet_consistency,
           wp.pnl_30d            AS wallet_pnl_30d,
           wp.total_volume_usdc  AS wallet_total_volume,
           wp.resolved_trades    AS wallet_overall_resolved
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
print(f"rows: {len(df)}")
print(f"baseline WR: {df['is_win'].mean():.1%}")
print(f"baseline avg EV: {df['outcome_delta'].mean():+.4f}")
print(f"baseline median EV: {df['outcome_delta'].median():+.4f}")

print("\n-- category --")
print(
    df.groupby("category").agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).sort_values("n", ascending=False).round(4).to_string()
)

print("\n-- confidence bucket --")
df["conf_bucket"] = pd.cut(df["confidence"], bins=[0, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0])
print(
    df.groupby("conf_bucket", observed=True).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).round(4).to_string()
)

print("\n-- entry price bucket --")
df["entry_bucket"] = pd.cut(df["entry_price"], bins=[0, 0.2, 0.35, 0.5, 0.65, 0.8, 1.0])
print(
    df.groupby("entry_bucket", observed=True).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).round(4).to_string()
)

if df["hold_days"].notna().any():
    print("\n-- hold days --")
    df["hold_bucket"] = pd.cut(df["hold_days"], bins=[0, 1, 3, 7, 14, 30, 365])
    print(
        df.groupby("hold_bucket", observed=True).agg(
            n=("is_win", "count"),
            wr=("is_win", "mean"),
            ev=("outcome_delta", "mean"),
        ).round(4).to_string()
    )

df_save = df.drop(columns=[c for c in df.columns if str(df[c].dtype).startswith("category")])
df_save.to_parquet(OUT / "whale_entry_experiments.parquet", index=False)

# ──────────────────────────────────────────────────────────────────────
# Part 2 — latency analysis + zero-lag simulation
# ──────────────────────────────────────────────────────────────────────

banner("PART 2 — LATENCY ANALYSIS")

lat = pd.read_sql(
    """
    SELECT so.id, so.fired_at, so.entry_price, so.wallet,
           so.condition_id, so.direction,
           so.outcome_delta, so.is_win,
           (SELECT MIN(wt.timestamp)
              FROM wallet_trades wt
              WHERE wt.wallet = so.wallet
                AND wt.condition_id = so.condition_id
                AND wt.timestamp <= so.fired_at
                AND wt.timestamp > so.fired_at - 86400)          AS whale_trade_ts,
           (SELECT wt.price
              FROM wallet_trades wt
              WHERE wt.wallet = so.wallet
                AND wt.condition_id = so.condition_id
                AND wt.timestamp <= so.fired_at
              ORDER BY wt.timestamp DESC LIMIT 1)                AS whale_entry_price
    FROM signal_outcomes so
    WHERE so.signal_type = 'whale_entry'
      AND so.resolution_price IS NOT NULL
    """,
    conn,
)
lat["lag_seconds"] = lat["fired_at"] - lat["whale_trade_ts"]
lat["lag_minutes"] = lat["lag_seconds"] / 60
lat["price_slippage"] = lat["entry_price"] - lat["whale_entry_price"]
lat["price_slippage_pct"] = lat["price_slippage"] / lat["whale_entry_price"].clip(lower=0.01)

has_lat = lat["lag_minutes"].notna()
print(f"signals with matched whale trade: {has_lat.sum()}/{len(lat)}")
if has_lat.any():
    print(f"lag mean   (min): {lat.loc[has_lat,'lag_minutes'].mean():.1f}")
    print(f"lag median (min): {lat.loc[has_lat,'lag_minutes'].median():.1f}")
    print(f"lag p25    (min): {lat.loc[has_lat,'lag_minutes'].quantile(0.25):.1f}")
    print(f"lag p75    (min): {lat.loc[has_lat,'lag_minutes'].quantile(0.75):.1f}")
    print(f"lag p95    (min): {lat.loc[has_lat,'lag_minutes'].quantile(0.95):.1f}")

has_slip = lat["price_slippage"].notna()
if has_slip.any():
    print(f"\nslippage mean:   {lat.loc[has_slip,'price_slippage'].mean():+.4f}"
          f" ({lat.loc[has_slip,'price_slippage_pct'].mean():+.1%})")
    print(f"slippage median: {lat.loc[has_slip,'price_slippage'].median():+.4f}")

lat["lag_bucket"] = pd.cut(lat["lag_minutes"], bins=[0, 5, 15, 30, 60, 120, 1440])
print("\n-- EV by detection lag --")
print(
    lat.groupby("lag_bucket", observed=True).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
        slip=("price_slippage_pct", "mean"),
    ).round(4).to_string()
)

lat["slip_bucket"] = pd.cut(
    lat["price_slippage_pct"],
    bins=[-5, -0.05, 0, 0.02, 0.05, 0.10, 0.50, 5.0],
)
print("\n-- EV by price slippage --")
print(
    lat.groupby("slip_bucket", observed=True).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).round(4).to_string()
)

# Zero-lag simulation
lat["hyp_delta"] = lat["outcome_delta"] + lat["price_slippage"]
lat["hyp_win"] = (lat["hyp_delta"] > 0).astype(int)
have = lat["price_slippage"].notna()
if have.any():
    print("\n-- zero-lag simulation --")
    print(f"actual      : WR={lat.loc[have,'is_win'].mean():.1%}, EV={lat.loc[have,'outcome_delta'].mean():+.4f}")
    print(f"at whale px : WR={lat.loc[have,'hyp_win'].mean():.1%}, EV={lat.loc[have,'hyp_delta'].mean():+.4f}")
    recovered = lat.loc[have, "hyp_delta"].mean() - lat.loc[have, "outcome_delta"].mean()
    print(f"recovered   : {recovered:+.4f}")
    if lat.loc[have, "hyp_delta"].mean() > 0:
        print("VERDICT: zero lag WOULD make this profitable -- latency is the fix")
    else:
        print("VERDICT: even zero lag is NOT enough -- deeper selection problem")

lat_save = lat.drop(columns=[c for c in lat.columns if str(lat[c].dtype).startswith("category")])
lat_save.to_parquet(OUT / "whale_latency.parquet", index=False)

# ──────────────────────────────────────────────────────────────────────
# Part 3 — wallet tier filter
# ──────────────────────────────────────────────────────────────────────

banner("PART 3 — WALLET TIER FILTER")

print("-- EV by wallet tier in category --")
print(
    df.groupby(df["wallet_tier_in_category"].fillna("_null")).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
        total_ev=("outcome_delta", "sum"),
    ).sort_values("ev", ascending=False).round(4).to_string()
)

sa = df[df["wallet_tier_in_category"].isin(["S", "A"])]
print(f"\nS+A only: n={len(sa)}  WR={sa['is_win'].mean():.1%}  EV={sa['outcome_delta'].mean():+.4f}")
if len(sa) > 0:
    print(
        sa.groupby("category").agg(
            n=("is_win", "count"),
            wr=("is_win", "mean"),
            ev=("outcome_delta", "mean"),
        ).round(4).to_string()
    )

sab = df[df["wallet_tier_in_category"].isin(["S", "A", "B"])]
print(f"\nS+A+B: n={len(sab)}  WR={sab['is_win'].mean():.1%}  EV={sab['outcome_delta'].mean():+.4f}")

untiered = df[df["wallet_tier_in_category"].isna()]
print(f"untiered (no category profile): n={len(untiered)}  WR={untiered['is_win'].mean():.1%}  EV={untiered['outcome_delta'].mean():+.4f}")

print("\n-- EV by wallet category WR --")
df["wr_bucket"] = pd.cut(df["wallet_category_wr"], bins=[0, 0.4, 0.5, 0.55, 0.6, 0.7, 1.0])
print(
    df.groupby("wr_bucket", observed=True).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).round(4).to_string()
)

# ──────────────────────────────────────────────────────────────────────
# Part 4 — price-lag (slippage) filter
# ──────────────────────────────────────────────────────────────────────

banner("PART 4 — PRICE-LAG FILTER")

merged = df.merge(
    lat[["id", "lag_minutes", "price_slippage_pct", "whale_entry_price"]],
    on="id",
    how="left",
)

print("-- EV by max allowed absolute slippage --")
print(f"{'thresh':>10} {'n':>6} {'WR':>7} {'avg_EV':>9} {'total_EV':>10}")
for t in [0.01, 0.02, 0.03, 0.05, 0.07, 0.10, 0.15, 0.20]:
    sub = merged[merged["price_slippage_pct"].abs() <= t]
    if len(sub) > 0:
        print(f"{t:>10.0%} {len(sub):>6} {sub['is_win'].mean():>7.1%} "
              f"{sub['outcome_delta'].mean():>+9.4f} {sub['outcome_delta'].sum():>+10.4f}")

for t in [0.03, 0.05, 0.10]:
    sub = merged[
        (merged["price_slippage_pct"].abs() <= t)
        & (merged["wallet_tier_in_category"].isin(["S", "A"]))
    ]
    if len(sub) > 0:
        print(
            f"\nS/A + slip<={t:.0%}: n={len(sub)}  "
            f"WR={sub['is_win'].mean():.1%}  "
            f"EV={sub['outcome_delta'].mean():+.4f}"
        )

# ──────────────────────────────────────────────────────────────────────
# Part 5 — entry price sweet spot
# ──────────────────────────────────────────────────────────────────────

banner("PART 5 — ENTRY PRICE SWEET SPOT")

zones = [
    ("near_certain_low",  0.00, 0.15),
    ("uncertain_low",     0.15, 0.35),
    ("uncertain_mid",     0.35, 0.65),
    ("uncertain_high",    0.65, 0.85),
    ("near_certain_high", 0.85, 1.00),
]
for name, lo, hi in zones:
    sub = df[(df["entry_price"] >= lo) & (df["entry_price"] < hi)]
    if len(sub) > 0:
        print(f"{name:>20}: n={len(sub):>4}  WR={sub['is_win'].mean():.1%}  EV={sub['outcome_delta'].mean():+.4f}")

uncertain = df[(df["entry_price"] >= 0.15) & (df["entry_price"] <= 0.85)]
print(f"\nuncertain (0.15-0.85): n={len(uncertain)}  WR={uncertain['is_win'].mean():.1%}  EV={uncertain['outcome_delta'].mean():+.4f}")
uncertain_sa = uncertain[uncertain["wallet_tier_in_category"].isin(["S", "A"])]
print(f"uncertain + S/A: n={len(uncertain_sa)}  WR={uncertain_sa['is_win'].mean():.1%}  EV={uncertain_sa['outcome_delta'].mean():+.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 6 — convergence filter (concurrent tracked wallets)
# ──────────────────────────────────────────────────────────────────────

banner("PART 6 — CONVERGENCE FILTER")

# Batch the counting — one SQL per window for all signals, then pivot
conv_rows: list[dict] = []
cur = conn.cursor()
sig_rows = df[["id", "condition_id", "wallet", "direction", "fired_at", "outcome_delta", "is_win"]].to_dict("records")
for row in sig_rows:
    side = "BUY" if "BUY" in str(row["direction"] or "").upper() else "SELL"
    for wh in (6, 12, 24, 48):
        win = wh * 3600
        n = cur.execute(
            """SELECT COUNT(DISTINCT wallet) FROM wallet_trades
               WHERE condition_id = ?
                 AND wallet != ?
                 AND side = ?
                 AND timestamp BETWEEN ? AND ?""",
            (row["condition_id"], row["wallet"], side,
             row["fired_at"] - win, row["fired_at"] + win),
        ).fetchone()[0]
        conv_rows.append({
            "id": row["id"], "window_hours": wh, "n_concurrent": n,
            "outcome_delta": row["outcome_delta"], "is_win": row["is_win"],
        })
conv = pd.DataFrame(conv_rows)

for wh in (6, 12, 24, 48):
    w = conv[conv["window_hours"] == wh]
    print(f"\n-- {wh}h window --")
    for mn in (0, 1, 2, 3, 5):
        sub = w[w["n_concurrent"] >= mn]
        if len(sub) >= 3:
            print(f"  >={mn} concurrent: n={len(sub):>4}  WR={sub['is_win'].mean():.1%}  EV={sub['outcome_delta'].mean():+.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 7 — whale exit + hold duration
# ──────────────────────────────────────────────────────────────────────

banner("PART 7 — EXIT STRATEGY / HOLD DURATION")

ex = pd.read_sql(
    """
    SELECT so.id, so.wallet, so.condition_id, so.fired_at,
           so.entry_price, so.outcome_delta, so.is_win,
           so.hold_days, so.resolution_price,
           (SELECT MIN(wt.timestamp) FROM wallet_trades wt
            WHERE wt.wallet = so.wallet AND wt.condition_id = so.condition_id
              AND wt.side='SELL' AND wt.timestamp > so.fired_at) AS whale_exit_ts,
           (SELECT wt.price FROM wallet_trades wt
            WHERE wt.wallet = so.wallet AND wt.condition_id = so.condition_id
              AND wt.side='SELL' AND wt.timestamp > so.fired_at
            ORDER BY wt.timestamp ASC LIMIT 1)                    AS whale_exit_price
    FROM signal_outcomes so
    WHERE so.signal_type = 'whale_entry'
      AND so.resolution_price IS NOT NULL
    """,
    conn,
)
ex["whale_exited"] = ex["whale_exit_ts"].notna()
ex["whale_hold_hours"] = (ex["whale_exit_ts"] - ex["fired_at"]) / 3600

print(f"whales exiting before resolution: {ex['whale_exited'].sum()}/{len(ex)} ({ex['whale_exited'].mean():.1%})")
exited = ex[ex["whale_exited"]].copy()
if len(exited) > 0:
    exited["exit_at_whale_delta"] = exited["whale_exit_price"] - exited["entry_price"]
    exited["exit_at_whale_win"] = (exited["exit_at_whale_delta"] > 0).astype(int)
    print("if we exit when whale exits:")
    print(f"  n={len(exited)}  WR={exited['exit_at_whale_win'].mean():.1%}  EV={exited['exit_at_whale_delta'].mean():+.4f}")
    print("vs hold to resolution:")
    print(f"  n={len(exited)}  WR={exited['is_win'].mean():.1%}  EV={exited['outcome_delta'].mean():+.4f}")
    print(f"whale hold hours: mean={exited['whale_hold_hours'].mean():.1f}, median={exited['whale_hold_hours'].median():.1f}")

if df["hold_days"].notna().any():
    print("\n-- EV by max hold days --")
    for md in (1, 3, 7, 14, 30):
        sub = df[df["hold_days"] <= md]
        if len(sub) > 0:
            print(f"  <={md:>2}d: n={len(sub):>4}  WR={sub['is_win'].mean():.1%}  EV={sub['outcome_delta'].mean():+.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 8 — market age proxy (via hold_days)
# ──────────────────────────────────────────────────────────────────────

banner("PART 8 — MARKET AGE PROXY (hold_days min floor)")

for md in (1, 3, 7, 14, 30):
    sub = df[df["hold_days"] >= md]
    if len(sub) > 0:
        print(f"  >={md:>2}d before close: n={len(sub):>4}  WR={sub['is_win'].mean():.1%}  EV={sub['outcome_delta'].mean():+.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 9 — whale size + conviction
# ──────────────────────────────────────────────────────────────────────

banner("PART 9 — WHALE SIZE / CONVICTION")

size = pd.read_sql(
    """
    SELECT so.id, so.wallet, so.condition_id, so.fired_at,
           so.outcome_delta, so.is_win, so.category,
           (SELECT SUM(wt.size) FROM wallet_trades wt
            WHERE wt.wallet = so.wallet AND wt.condition_id = so.condition_id
              AND wt.side='BUY'
              AND wt.timestamp BETWEEN so.fired_at - 86400 AND so.fired_at) AS trade_size,
           (SELECT AVG(wt.size) FROM wallet_trades wt
            WHERE wt.wallet = so.wallet)                                    AS whale_avg_size,
           (SELECT SUM(wt.size) FROM wallet_trades wt
            WHERE wt.wallet = so.wallet)                                    AS whale_total_volume
    FROM signal_outcomes so
    WHERE so.signal_type = 'whale_entry'
      AND so.resolution_price IS NOT NULL
    """,
    conn,
)
size["conviction_multiple"] = size["trade_size"] / size["whale_avg_size"].clip(lower=1)
size["size_bucket"] = pd.cut(
    size["trade_size"],
    bins=[0, 100, 500, 1000, 5000, 10000, 1e9],
    labels=["<100", "100-500", "500-1k", "1k-5k", "5k-10k", ">10k"],
)
size["conv_bucket"] = pd.cut(
    size["conviction_multiple"],
    bins=[0, 0.5, 1, 2, 5, 10, 1000],
    labels=["<0.5x", "0.5-1x", "1-2x", "2-5x", "5-10x", ">10x"],
)

print("-- EV by absolute trade size --")
print(
    size.groupby("size_bucket", observed=True).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).round(4).to_string()
)
print("\n-- EV by conviction multiple --")
print(
    size.groupby("conv_bucket", observed=True).agg(
        n=("is_win", "count"),
        wr=("is_win", "mean"),
        ev=("outcome_delta", "mean"),
    ).round(4).to_string()
)

size_merged = size.merge(df[["id", "wallet_tier_in_category"]], on="id", how="left")
high = size_merged[
    (size_merged["conviction_multiple"] >= 2)
    & (size_merged["wallet_tier_in_category"].isin(["S", "A"]))
]
if len(high) > 0:
    print(f"\nS/A + conv>=2x: n={len(high)}  WR={high['is_win'].mean():.1%}  EV={high['outcome_delta'].mean():+.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 10 — category specific
# ──────────────────────────────────────────────────────────────────────

banner("PART 10 — CATEGORY SPECIFIC WHALE_ENTRY EV")

for cat, g in df.groupby("category"):
    n = len(g)
    if n < 3:
        continue
    ev = g["outcome_delta"].mean()
    if n >= 5:
        t, p = stats.ttest_1samp(g["outcome_delta"], 0)
        p = p / 2
    else:
        p = 1.0
    flag = "  GOOD" if ev > 0 and p < 0.15 else ""
    print(f"  {cat:>15}: n={n:>4}  WR={g['is_win'].mean():.1%}  EV={ev:+.4f}  p={p:.3f}{flag}")

for cat, g in df.groupby("category"):
    if g["outcome_delta"].mean() > 0 and len(g) >= 5:
        sa = g[g["wallet_tier_in_category"].isin(["S", "A"])]
        if len(sa) > 0:
            print(f"  {cat} + S/A: n={len(sa)}  WR={sa['is_win'].mean():.1%}  EV={sa['outcome_delta'].mean():+.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 11 — grid search
# ──────────────────────────────────────────────────────────────────────

banner("PART 11 — GRID SEARCH OVER FILTER COMBOS")

grid_src = merged.copy()  # has tier, slippage already

filters = {
    "tier": {
        "none":  lambda d: d,
        "S_A":   lambda d: d[d["wallet_tier_in_category"].isin(["S", "A"])],
        "S_A_B": lambda d: d[d["wallet_tier_in_category"].isin(["S", "A", "B"])],
    },
    "slippage": {
        "none":      lambda d: d,
        "lte_3pct":  lambda d: d[d["price_slippage_pct"].abs() <= 0.03],
        "lte_5pct":  lambda d: d[d["price_slippage_pct"].abs() <= 0.05],
        "lte_10pct": lambda d: d[d["price_slippage_pct"].abs() <= 0.10],
    },
    "entry_price": {
        "none":           lambda d: d,
        "uncertain":      lambda d: d[(d["entry_price"] >= 0.15) & (d["entry_price"] <= 0.85)],
        "mid_uncertain":  lambda d: d[(d["entry_price"] >= 0.25) & (d["entry_price"] <= 0.75)],
    },
    "confidence": {
        "none":   lambda d: d,
        "gte_50": lambda d: d[d["confidence"] >= 0.50],
        "gte_60": lambda d: d[d["confidence"] >= 0.60],
    },
    "category": {
        "all":                 lambda d: d,
        "politics_geo":        lambda d: d[d["category"].isin(["politics", "geopolitics"])],
        "excl_sports":         lambda d: d[d["category"] != "sports"],
        "geopolitics_only":    lambda d: d[d["category"] == "geopolitics"],
    },
}

results = []
for t_name, t_fn in filters["tier"].items():
    for s_name, s_fn in filters["slippage"].items():
        for p_name, p_fn in filters["entry_price"].items():
            for c_name, c_fn in filters["confidence"].items():
                for cat_name, cat_fn in filters["category"].items():
                    sub = cat_fn(c_fn(p_fn(s_fn(t_fn(grid_src)))))
                    n = len(sub)
                    if n < 5:
                        continue
                    wr = sub["is_win"].mean()
                    ev = sub["outcome_delta"].mean()
                    t_stat, p_val = stats.ttest_1samp(sub["outcome_delta"], 0)
                    p_val = p_val / 2 if ev > 0 else 1.0 - p_val / 2
                    results.append({
                        "tier": t_name, "slippage": s_name, "entry_price": p_name,
                        "confidence": c_name, "category": cat_name,
                        "n": n, "wr": round(wr, 4), "ev": round(ev, 4),
                        "p_value": round(p_val, 4),
                        "total_ev": round(sub["outcome_delta"].sum(), 4),
                    })

rd = pd.DataFrame(results)
rd.to_csv(OUT / "whale_entry_grid.csv", index=False)
print(f"total combos tested: {len(rd)}")

viable = rd[rd["n"] >= 10].sort_values("ev", ascending=False)
print("\n-- top 15 combos by EV (n>=10) --")
print(viable.head(15).to_string(index=False))

sig = viable[(viable["ev"] > 0) & (viable["p_value"] < 0.15)].sort_values("ev", ascending=False)
print(f"\nstat-sig (p<0.15, EV>0, n>=10): {len(sig)} combos")
if len(sig) > 0:
    print(sig.head(10).to_string(index=False))
    best = sig.iloc[0]
    print("\nBEST CONFIG:")
    for k in ("tier", "slippage", "entry_price", "confidence", "category"):
        print(f"  {k}: {best[k]}")
    print(f"  n={best['n']}  WR={best['wr']:.1%}  EV={best['ev']:+.4f}  p={best['p_value']:.3f}")
else:
    print("NO statistically significant positive-EV configuration found.")

conn.close()
