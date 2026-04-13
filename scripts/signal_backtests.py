"""Signal candidate backtests — all 6 signals in one script."""
import sqlite3, os
import pandas as pd, numpy as np
from scipy import stats

DB = "data/polymarket/wallet_intelligence.db"
CSV = "data/polymarket/gamma_resolution.csv"
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

def banner(t):
    print(f"\n{'='*72}\n  {t}\n{'='*72}")

def oos_test(df, label=""):
    """Temporal 50/50 split OOS test."""
    if len(df) < 6:
        return None
    df = df.sort_values("timestamp")
    test = df.iloc[len(df)//2:]
    if len(test) < 3:
        return None
    ev = test["od"].mean()
    wr = test["win"].mean()
    _, p = stats.ttest_1samp(test["od"], 0) if len(test) >= 5 else (0, 2)
    p = p/2 if ev > 0 else 1-p/2
    return {"label": label, "n": len(test), "wr": round(wr, 4), "ev": round(ev, 4), "p": round(p, 4)}

conn = sqlite3.connect(DB, timeout=60)

# ── Load base dataset ───────────────────────────────────────
banner("LOADING DATA")
res = pd.read_csv(CSV)
cid_col = [c for c in res.columns if "condition" in c.lower()][0]
yes_col = [c for c in res.columns if "resolves_yes" in c.lower()][0]
rmap = res.drop_duplicates(subset=[cid_col]).set_index(cid_col)[yes_col].to_dict()

trades = pd.read_sql("""
    SELECT wt.wallet, wt.condition_id, wt.side, wt.price, wt.size, wt.timestamp,
           wt.category, wt.market_outcome,
           wa.archetype, wa.copyable, wcp.tier
    FROM wallet_trades wt
    LEFT JOIN wallet_archetypes wa ON wt.wallet = wa.wallet
    LEFT JOIN wallet_category_profiles wcp ON wt.wallet = wcp.wallet AND wt.category = wcp.category
    WHERE wt.market_outcome IS NOT NULL
""", conn)
trades["yes_won"] = trades["condition_id"].map(rmap)
trades = trades[trades["yes_won"].notna()].copy()
trades["correct"] = (((trades["side"]=="BUY")&(trades["yes_won"]==True))|((trades["side"]=="SELL")&(trades["yes_won"]==False))).astype(int)
trades["uncertain"] = ((trades["price"]>=0.15)&(trades["price"]<=0.85)).astype(int)
trades["od"] = np.where(trades["side"]=="BUY", trades["yes_won"].astype(float)-trades["price"], trades["price"]-trades["yes_won"].astype(float))
trades["win"] = (trades["od"]>0).astype(int)

# Timing
mt = trades.groupby("condition_id").agg(ft=("timestamp","min"),lt=("timestamp","max")).reset_index()
trades = trades.merge(mt, on="condition_id")
trades["life_pct"] = np.where(trades["lt"]>trades["ft"], (trades["timestamp"]-trades["ft"])/(trades["lt"]-trades["ft"]), 0.5)

# Consensus
mc = trades.groupby("condition_id").agg(bc=("side", lambda x: (x=="BUY").sum()), sc=("side", lambda x: (x=="SELL").sum())).reset_index()
mc["consensus"] = np.where(mc["bc"]>mc["sc"], "BUY", "SELL")
trades = trades.merge(mc[["condition_id","consensus"]], on="condition_id")
trades["is_contrarian"] = (trades["side"]!=trades["consensus"]).astype(int)

unc = trades[trades["uncertain"]==1].copy()
print(f"uncertain trades: {len(unc)}, wallets: {unc['wallet'].nunique()}")

NON_COPY = {"arb_bot","market_maker","hft_algo","penny_collector"}
all_results = []

# ── SIGNAL 1: Tier Divergence ───────────────────────────────
banner("SIGNAL 1: TIER DIVERGENCE")
for name, tiers in [("S", ["S"]), ("S_A", ["S","A"]), ("S_A_B", ["S","A","B"])]:
    sub = unc[unc["tier"].isin(tiers)]
    r = oos_test(sub, f"tier_{name}")
    if r:
        all_results.append({**r, "signal": "tier_divergence", "config": name})
        print(f"  {name}: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")
    # With bot exclusion
    sub2 = sub[~sub["archetype"].isin(NON_COPY)]
    r2 = oos_test(sub2, f"tier_{name}_no_bots")
    if r2:
        all_results.append({**r2, "signal": "tier_divergence", "config": f"{name}_no_bots"})
        print(f"  {name}+no_bots: OOS N={r2['n']} WR={r2['wr']:.1%} EV={r2['ev']:+.4f} p={r2['p']:.4f}")
    # Geopolitics only
    sub3 = sub[sub["category"]=="geopolitics"]
    r3 = oos_test(sub3, f"tier_{name}_geo")
    if r3:
        all_results.append({**r3, "signal": "tier_divergence", "config": f"{name}_geo"})
        print(f"  {name}+geo: OOS N={r3['n']} WR={r3['wr']:.1%} EV={r3['ev']:+.4f} p={r3['p']:.4f}")

# ── SIGNAL 2: Specialist Entry ──────────────────────────────
banner("SIGNAL 2: SPECIALIST ENTRY")
spec = unc[unc["archetype"]=="specialist"]
r = oos_test(spec, "specialist_all")
if r:
    all_results.append({**r, "signal": "specialist_entry", "config": "all"})
    print(f"  all: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")
for cat in spec["category"].unique():
    sub = spec[spec["category"]==cat]
    r = oos_test(sub, f"specialist_{cat}")
    if r and r["n"] >= 3:
        all_results.append({**r, "signal": "specialist_entry", "config": cat})
        print(f"  {cat}: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")

# ── SIGNAL 3: Late Informed Entry ───────────────────────────
banner("SIGNAL 3: LATE INFORMED ENTRY")
for label, lo in [("late_75", 0.75), ("late_90", 0.90)]:
    late = unc[unc["life_pct"]>lo]
    r = oos_test(late, label)
    if r:
        all_results.append({**r, "signal": "late_informed", "config": label})
        print(f"  {label}: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")
    # + large size
    big = late[late["size"]>=unc["size"].quantile(0.75)]
    r2 = oos_test(big, f"{label}_large")
    if r2:
        all_results.append({**r2, "signal": "late_informed", "config": f"{label}_large"})
        print(f"  {label}+large: OOS N={r2['n']} WR={r2['wr']:.1%} EV={r2['ev']:+.4f} p={r2['p']:.4f}")
    # + S/A tier
    sa_late = late[late["tier"].isin(["S","A"])]
    r3 = oos_test(sa_late, f"{label}_SA")
    if r3:
        all_results.append({**r3, "signal": "late_informed", "config": f"{label}_SA"})
        print(f"  {label}+S/A: OOS N={r3['n']} WR={r3['wr']:.1%} EV={r3['ev']:+.4f} p={r3['p']:.4f}")

# ── SIGNAL 4: Contrarian Entry ──────────────────────────────
banner("SIGNAL 4: CONTRARIAN ENTRY")
contra = unc[unc["is_contrarian"]==1]
r = oos_test(contra, "contrarian_all")
if r:
    all_results.append({**r, "signal": "contrarian", "config": "all"})
    print(f"  all: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")
# Smart contrarian (S/A tier)
sc = contra[contra["tier"].isin(["S","A"])]
r = oos_test(sc, "contrarian_SA")
if r:
    all_results.append({**r, "signal": "contrarian", "config": "SA"})
    print(f"  S/A: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")
# + large size
big_c = contra[contra["size"]>=unc["size"].quantile(0.75)]
r = oos_test(big_c, "contrarian_large")
if r:
    all_results.append({**r, "signal": "contrarian", "config": "large"})
    print(f"  large: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")

# ── SIGNAL 5: Accumulation variants ─────────────────────────
banner("SIGNAL 5: ACCUMULATION VARIANTS")
acc = pd.read_sql("""
    SELECT so.outcome_delta as od, so.is_win as win, so.category, so.confidence,
           so.fired_at as timestamp, wcp.tier, wa.archetype
    FROM signal_outcomes so
    LEFT JOIN wallet_category_profiles wcp ON so.wallet = wcp.wallet AND so.category = wcp.category
    LEFT JOIN wallet_archetypes wa ON so.wallet = wa.wallet
    WHERE so.signal_type='accumulation' AND so.resolution_price IS NOT NULL
      AND so.entry_price>0 AND so.entry_price<1
""", conn)
print(f"accumulation resolved: {len(acc)}")
for name, sub in [("base", acc), ("geo_only", acc[acc["category"]=="geopolitics"]),
                   ("SA_tier", acc[acc["tier"].isin(["S","A"])]),
                   ("no_bots", acc[~acc["archetype"].isin(NON_COPY)])]:
    r = oos_test(sub, f"acc_{name}")
    if r:
        all_results.append({**r, "signal": "accumulation_variant", "config": name})
        print(f"  {name}: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")

# ── SIGNAL 6: Multi-factor ──────────────────────────────────
banner("SIGNAL 6: MULTI-FACTOR SCORING")
size_med = unc["size"].median()
unc["score"] = (
    unc["tier"].isin(["S","A"]).astype(int) * 2 +
    (unc["life_pct"]>0.60).astype(int) +
    (unc["size"]>=size_med).astype(int) +
    (unc["archetype"]=="specialist").astype(int) +
    (unc["category"]=="geopolitics").astype(int) * 1.5 +
    unc["is_contrarian"] * 0.5
)
for ms in [1, 2, 3, 4, 5]:
    sub = unc[unc["score"]>=ms]
    r = oos_test(sub, f"score_gte_{ms}")
    if r:
        all_results.append({**r, "signal": "multi_factor", "config": f"score>={ms}"})
        print(f"  score>={ms}: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")

# Single factors
for fname, mask in [("tier_SA", unc["tier"].isin(["S","A"])),
                     ("late_60", unc["life_pct"]>0.60),
                     ("large", unc["size"]>=size_med),
                     ("specialist", unc["archetype"]=="specialist"),
                     ("geopolitics", unc["category"]=="geopolitics"),
                     ("contrarian", unc["is_contrarian"]==1)]:
    sub = unc[mask]
    r = oos_test(sub, f"factor_{fname}")
    if r:
        print(f"  factor {fname}: OOS N={r['n']} WR={r['wr']:.1%} EV={r['ev']:+.4f} p={r['p']:.4f}")

# ── FINAL SUMMARY ───────────────────────────────────────────
banner("FINAL SUMMARY")
rd = pd.DataFrame(all_results)
if len(rd) > 0:
    sig = rd[(rd["ev"]>0)&(rd["p"]<0.15)].sort_values("ev", ascending=False)
    print(f"stat-sig positive (p<0.15): {len(sig)}")
    if len(sig) > 0:
        print(sig.to_string(index=False))
    print(f"\ntop 15 by EV (n>=5):")
    top = rd[rd["n"]>=5].sort_values("ev", ascending=False)
    print(top.head(15).to_string(index=False))

# Overlap with accumulation
acc_w = set(pd.read_sql("SELECT DISTINCT wallet FROM signal_outcomes WHERE signal_type='accumulation'", conn)["wallet"])
sa_w = set(unc[unc["tier"].isin(["S","A"])]["wallet"])
spec_w = set(unc[unc["archetype"]=="specialist"]["wallet"])
print(f"\n-- overlap with accumulation wallets --")
print(f"  tier S/A: {len(sa_w & acc_w)}/{len(sa_w)} ({len(sa_w & acc_w)/max(len(sa_w),1)*100:.0f}%)")
print(f"  specialist: {len(spec_w & acc_w)}/{len(spec_w)} ({len(spec_w & acc_w)/max(len(spec_w),1)*100:.0f}%)")

conn.close()
print("\ndone.")
