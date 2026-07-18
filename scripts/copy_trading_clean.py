"""
Per-wallet copy trading analysis on FULLY CORRECTED data.

Both resolution bugs fixed:
  1. gamma_resolution_fetcher resolves_yes per-market (not per-row)
  2. enrich_resolution market_outcome checks token name (not just payout)

Read-only. Single-connection. No parquet intermediate (avoids pyarrow bugs).
"""
from __future__ import annotations
import sqlite3, sys, math
from collections import defaultdict
import pandas as pd
from scipy import stats

DB = "data/polymarket/wallet_intelligence.db"
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)

NON_COPYABLE = {"arb_bot", "market_maker", "hft_algo", "penny_collector"}

def p1(t, p_raw, ev):
    return p_raw / 2 if ev > 0 else 1 - p_raw / 2

def banner(t):
    print(f"\n{'='*72}\n  {t}\n{'='*72}")

conn = sqlite3.connect(DB, timeout=120)

# ────────────────────────────────────────────────────────────
# PART 1: Map the whale_entry universe
# ────────────────────────────────────────────────────────────
banner("PART 1 -- WHALE_ENTRY UNIVERSE")

df = pd.read_sql("""
  SELECT so.id, so.fired_at, so.condition_id, so.category, so.direction,
         so.confidence, so.entry_price, so.wallet, so.resolution_price,
         so.outcome_delta, so.is_win, so.hold_days,
         wcp.tier, wcp.win_rate AS tier_wr, wcp.consistency_score
  FROM signal_outcomes so
  LEFT JOIN wallet_category_profiles wcp
    ON so.wallet = wcp.wallet AND so.category = wcp.category
  WHERE so.signal_type = 'whale_entry'
    AND so.resolution_price IS NOT NULL
    AND so.entry_price > 0 AND so.entry_price < 1
""", conn)

print(f"resolved: {len(df)}, wallets: {df['wallet'].nunique()}")
print(f"aggregate WR={df['is_win'].mean():.1%}  EV={df['outcome_delta'].mean():+.4f}")
print(f"\ndirection: {dict(df['direction'].value_counts())}")
print(f"category:\n{df['category'].value_counts().to_string()}")
print(f"res_price: {dict(df['resolution_price'].value_counts())}")

# ────────────────────────────────────────────────────────────
# PART 2: Classify wallet archetypes from behavior
# ────────────────────────────────────────────────────────────
banner("PART 2 -- WALLET ARCHETYPE CLASSIFICATION")

wallets = df["wallet"].unique().tolist()
beh = pd.read_sql(f"""
  SELECT wallet,
    COUNT(*) AS total_trades,
    COUNT(DISTINCT condition_id) AS unique_markets,
    CAST(COUNT(*) AS REAL)/NULLIF(COUNT(DISTINCT condition_id),0) AS fills_per_market,
    AVG(size * price) AS avg_trade_size_usd,
    SUM(size * price) AS total_volume_usd,
    SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END)*1.0/COUNT(*) AS buy_ratio,
    COUNT(DISTINCT category) AS n_categories,
    (MAX(timestamp)-MIN(timestamp))*1.0/NULLIF(COUNT(*)-1,0) AS avg_interval_sec,
    SUM(CASE WHEN price<0.10 OR price>0.90 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS extreme_price_ratio,
    SUM(CASE WHEN price>=0.15 AND price<=0.85 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS uncertain_price_ratio,
    SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) AS wins,
    COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) AS resolved_trades,
    SUM(pnl) AS actual_pnl,
    (MAX(timestamp)-MIN(timestamp))/86400.0 AS span_days
  FROM wallet_trades
  WHERE wallet IN ({','.join(f"'{w}'" for w in wallets)})
  GROUP BY wallet
""", conn)

beh["trades_per_day"] = beh["total_trades"] / beh["span_days"].clip(lower=1)
beh["win_rate"] = beh["wins"] / beh["resolved_trades"].clip(lower=1)

def classify(r):
    fills = r.get("fills_per_market") or 0
    tpd = r.get("trades_per_day") or 0
    ext = r.get("extreme_price_ratio") or 0
    br = r.get("buy_ratio") or 0.5
    intv = r.get("avg_interval_sec") or 1e9
    nc = r.get("n_categories") or 0
    unc = r.get("uncertain_price_ratio") or 0
    tt = r.get("total_trades") or 0
    if fills > 15 and ext > 0.3 and tpd > 20: return "arb_bot"
    if fills > 10 and 0.35 < br < 0.65 and tpd > 10: return "market_maker"
    if intv < 120 and tt > 500 and tpd > 50: return "hft_algo"
    if ext > 0.6 and unc < 0.2: return "penny_collector"
    if nc <= 2 and fills <= 8 and tpd < 20: return "specialist"
    if (br > 0.70 or br < 0.30) and unc > 0.4 and fills <= 10: return "conviction"
    if tpd < 5 and fills <= 5 and unc > 0.5: return "research"
    if nc >= 3 and tpd < 20 and fills <= 10: return "diversified"
    return "unclassified"

beh["archetype"] = beh.apply(classify, axis=1)
arch_map = beh.set_index("wallet")["archetype"].to_dict()
df["archetype"] = df["wallet"].map(arch_map).fillna("unknown")

for arch in sorted(df["archetype"].unique()):
    g = df[df["archetype"] == arch]
    tag = "NON-COPY" if arch in NON_COPYABLE else "COPYABLE"
    print(f"  {arch:>18} [{tag:>8}]: n={len(g):>4}  WR={g['is_win'].mean():.1%}  EV={g['outcome_delta'].mean():+.4f}")

# ────────────────────────────────────────────────────────────
# PART 3: Copy trading EV by archetype — THE KEY SPLIT
# ────────────────────────────────────────────────────────────
banner("PART 3 -- THE KEY SPLIT: COPYABLE vs NON-COPYABLE")

for label, mask in [("ALL", df["archetype"].notna()),
                    ("NON-COPYABLE", df["archetype"].isin(NON_COPYABLE)),
                    ("COPYABLE", ~df["archetype"].isin(NON_COPYABLE))]:
    g = df[mask]
    if len(g) < 2: continue
    ev = g["outcome_delta"].mean()
    wr = g["is_win"].mean()
    _, p_raw = stats.ttest_1samp(g["outcome_delta"], 0) if len(g) >= 5 else (0, 2)
    p = p1(0, p_raw, ev)
    print(f"  {label:>15}: N={len(g):>4}  WR={wr:.1%}  EV={ev:+.4f}  p={p:.4f}")

# ────────────────────────────────────────────────────────────
# PART 4: Per-wallet copy trading EV
# ────────────────────────────────────────────────────────────
banner("PART 4 -- PER-WALLET PERFORMANCE")

rows = []
for w, g in df.groupby("wallet"):
    if len(g) < 2: continue
    ev = g["outcome_delta"].mean()
    _, p_raw = stats.ttest_1samp(g["outcome_delta"], 0) if len(g) >= 5 else (0, 2)
    rows.append({"wallet": w, "short": w[:14]+"...", "arch": arch_map.get(w,"?"),
                 "n": len(g), "wr": round(g["is_win"].mean(),3),
                 "ev": round(ev,4), "total_ev": round(g["outcome_delta"].sum(),2),
                 "p": round(p1(0,p_raw,ev),4),
                 "cats": ",".join(sorted(g["category"].dropna().unique()))})
wp = pd.DataFrame(rows).sort_values("ev", ascending=False)
print(wp[["short","arch","n","wr","ev","total_ev","p","cats"]].to_string(index=False))

pos = wp[wp["ev"]>0]; neg = wp[wp["ev"]<=0]
print(f"\npositive-EV wallets: {len(pos)}/{len(wp)} (total_ev={pos['total_ev'].sum():+.2f})")
print(f"negative-EV wallets: {len(neg)}/{len(wp)} (total_ev={neg['total_ev'].sum():+.2f})")

# ────────────────────────────────────────────────────────────
# PART 5: Copyable archetypes x category
# ────────────────────────────────────────────────────────────
banner("PART 5 -- COPYABLE ARCHETYPE x CATEGORY")

copyable = df[~df["archetype"].isin(NON_COPYABLE)]
print(f"{'archetype':>18} {'category':>15} {'n':>5} {'WR':>7} {'EV':>8} {'p':>7}")
for (arch, cat), g in copyable.groupby(["archetype","category"]):
    if len(g) < 3: continue
    ev = g["outcome_delta"].mean()
    _, pr = stats.ttest_1samp(g["outcome_delta"], 0) if len(g) >= 5 else (0, 2)
    p = p1(0, pr, ev)
    flag = " ***" if ev > 0 and p < 0.15 else (" +" if ev > 0 else "")
    print(f"{arch:>18} {cat:>15} {len(g):>5} {g['is_win'].mean():>7.1%} {ev:>+8.4f} {p:>7.3f}{flag}")

# ────────────────────────────────────────────────────────────
# PART 6: Conviction analysis
# ────────────────────────────────────────────────────────────
banner("PART 6 -- CONVICTION (trade size / avg size)")

enr = []
for _, s in df.iterrows():
    side = "BUY" if s.get("direction","") == "BUY" else "SELL"
    r = conn.execute(
        "SELECT SUM(size * price) FROM wallet_trades WHERE wallet=? AND condition_id=? AND side=? AND timestamp BETWEEN ? AND ?",
        (s["wallet"], s["condition_id"], side, s["fired_at"]-86400, s["fired_at"]+3600)).fetchone()
    avg = conn.execute("SELECT AVG(size * price) FROM wallet_trades WHERE wallet=?", (s["wallet"],)).fetchone()
    enr.append({"id": s["id"], "trade_size": r[0] or 0, "avg_size": avg[0] or 1})
enr_df = pd.DataFrame(enr)
df = df.merge(enr_df, on="id", how="left")
df["conviction"] = df["trade_size"] / df["avg_size"].clip(lower=1)

copyable = df[~df["archetype"].isin(NON_COPYABLE)]
for label, lo, hi in [("<0.5x",0,0.5),("0.5-1x",0.5,1),("1-2x",1,2),("2-5x",2,5),(">5x",5,1000)]:
    g = copyable[(copyable["conviction"]>=lo)&(copyable["conviction"]<hi)]
    if len(g) < 3: continue
    print(f"  {label:>8}: N={len(g):>4}  WR={g['is_win'].mean():.1%}  EV={g['outcome_delta'].mean():+.4f}")

# ────────────────────────────────────────────────────────────
# PART 7: Entry price zone x archetype
# ────────────────────────────────────────────────────────────
banner("PART 7 -- ENTRY PRICE ZONE (copyable only)")

for label, lo, hi in [("near_zero",0,0.15),("low_unc",0.15,0.35),("mid_unc",0.35,0.65),
                       ("high_unc",0.65,0.85),("near_one",0.85,1.0)]:
    g = copyable[(copyable["entry_price"]>=lo)&(copyable["entry_price"]<hi)]
    if len(g) < 3: continue
    print(f"  {label:>10}: N={len(g):>4}  WR={g['is_win'].mean():.1%}  EV={g['outcome_delta'].mean():+.4f}")

uncertain = copyable[(copyable["entry_price"]>=0.15)&(copyable["entry_price"]<=0.85)]
if len(uncertain) >= 5:
    _, pr = stats.ttest_1samp(uncertain["outcome_delta"], 0)
    print(f"\n  uncertain zone aggregate: N={len(uncertain)}  WR={uncertain['is_win'].mean():.1%}  EV={uncertain['outcome_delta'].mean():+.4f}  p={pr/2:.4f}")

# ────────────────────────────────────────────────────────────
# PART 8: Wallet profitability -> copy EV
# ────────────────────────────────────────────────────────────
banner("PART 8 -- WALLET PROFITABILITY -> COPY EV")

pnl_map = beh.set_index("wallet")[["actual_pnl","win_rate"]].to_dict("index")
df["wallet_pnl"] = df["wallet"].map(lambda w: (pnl_map.get(w) or {}).get("actual_pnl"))
df["wallet_wr"] = df["wallet"].map(lambda w: (pnl_map.get(w) or {}).get("win_rate"))

copyable = df[~df["archetype"].isin(NON_COPYABLE)]
for label, mask in [("profitable (pnl>0)", copyable["wallet_pnl"]>0),
                    ("unprofitable", (copyable["wallet_pnl"]<=0) & copyable["wallet_pnl"].notna()),
                    ("no pnl data", copyable["wallet_pnl"].isna())]:
    g = copyable[mask]
    if len(g) < 3: continue
    ev = g["outcome_delta"].mean()
    print(f"  {label:>25}: N={len(g):>4}  WR={g['is_win'].mean():.1%}  EV={ev:+.4f}")

# ────────────────────────────────────────────────────────────
# PART 10: Combined grid search
# ────────────────────────────────────────────────────────────
banner("PART 10 -- GRID SEARCH")

base = df[~df["archetype"].isin(NON_COPYABLE)].copy()
filters = {
    "arch": {"all": lambda d: d,
             "specialist": lambda d: d[d["archetype"]=="specialist"],
             "conviction": lambda d: d[d["archetype"]=="conviction"],
             "spec+conv": lambda d: d[d["archetype"].isin(["specialist","conviction"])]},
    "price": {"all": lambda d: d,
              "uncertain": lambda d: d[(d["entry_price"]>=0.15)&(d["entry_price"]<=0.85)]},
    "conv": {"all": lambda d: d, "gte2x": lambda d: d[d["conviction"]>=2]},
    "prof": {"all": lambda d: d, "profitable": lambda d: d[d["wallet_pnl"]>0]},
    "cat": {"all": lambda d: d, "pol+geo": lambda d: d[d["category"].isin(["politics","geopolitics"])],
            "geo": lambda d: d[d["category"]=="geopolitics"],
            "politics": lambda d: d[d["category"]=="politics"]},
}

results = []
for a_n, a_fn in filters["arch"].items():
    for p_n, p_fn in filters["price"].items():
        for c_n, c_fn in filters["conv"].items():
            for pr_n, pr_fn in filters["prof"].items():
                for ct_n, ct_fn in filters["cat"].items():
                    s = ct_fn(pr_fn(c_fn(p_fn(a_fn(base)))))
                    if len(s) < 5: continue
                    ev = s["outcome_delta"].mean()
                    _, pr = stats.ttest_1samp(s["outcome_delta"], 0)
                    p = p1(0, pr, ev)
                    results.append({"arch":a_n,"price":p_n,"conv":c_n,"prof":pr_n,"cat":ct_n,
                                    "n":len(s),"wr":round(s["is_win"].mean(),4),
                                    "ev":round(ev,4),"p":round(p,4),
                                    "total_ev":round(s["outcome_delta"].sum(),2)})

rd = pd.DataFrame(results)
sig = rd[(rd["ev"]>0)&(rd["p"]<0.15)].sort_values("ev", ascending=False)
print(f"total combos: {len(rd)}")
print(f"stat-sig positive (p<0.15): {len(sig)}")
if len(sig) > 0:
    print(sig.head(15).to_string(index=False))
else:
    print("NONE")

marg = rd[(rd["ev"]>0)&(rd["p"]<0.30)&(rd["n"]>=8)].sort_values("ev", ascending=False)
print(f"\ndirectionally positive (p<0.30, n>=8): {len(marg)}")
if len(marg) > 0:
    print(marg.head(10).to_string(index=False))

best10 = rd[rd["n"]>=10].sort_values("ev", ascending=False)
print(f"\ntop 10 by EV (n>=10):")
print(best10.head(10).to_string(index=False))

# ────────────────────────────────────────────────────────────
# PART 11: Compare to accumulation
# ────────────────────────────────────────────────────────────
banner("PART 11 -- COPY TRADING vs STRUCTURAL SIGNALS")

for st in ["accumulation","whale_entry","market_maker_flip","oversized_bet"]:
    sdf = pd.read_sql(f"SELECT outcome_delta, is_win FROM signal_outcomes WHERE signal_type=? AND resolution_price IS NOT NULL AND entry_price>0 AND entry_price<1",
                       conn, params=[st])
    if len(sdf) == 0: continue
    ev = sdf["outcome_delta"].mean()
    _, pr = stats.ttest_1samp(sdf["outcome_delta"], 0) if len(sdf)>=5 else (0,2)
    print(f"  {st:>20}: N={len(sdf)}  WR={sdf['is_win'].mean():.1%}  EV={ev:+.4f}  p={p1(0,pr,ev):.4f}")

best_copy_ev = sig.iloc[0]["ev"] if len(sig) > 0 else (marg.iloc[0]["ev"] if len(marg) > 0 else 0)
print(f"\n  accumulation EV:     +0.2797")
print(f"  best copy trading:   {best_copy_ev:+.4f}")
if best_copy_ev > 0:
    ratio = 0.2797 / max(best_copy_ev, 0.001)
    print(f"  accumulation is {ratio:.1f}x better" if ratio > 1 else f"  copy trading is {1/ratio:.1f}x better")
else:
    print(f"  no viable copy-trading config found")

conn.close()
print("\ndone.")
