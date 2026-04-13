"""
Insider detection analysis — identify wallets with information advantage
and backtest copy-trading EV. All on corrected resolution data.

Read-only against wallet_intelligence.db. Saves intermediates to /tmp/.
"""
from __future__ import annotations
import sqlite3, json, os
from pathlib import Path
import pandas as pd, numpy as np
from scipy import stats

DB = "data/polymarket/wallet_intelligence.db"
CSV = "data/polymarket/gamma_resolution.csv"
OUT = Path(os.environ.get("TMPDIR", "/tmp"))
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

def banner(t):
    print(f"\n{'='*72}\n  {t}\n{'='*72}")

conn = sqlite3.connect(DB, timeout=60)

# ── PART 1: Data overview ───────────────────────────────────────────
banner("PART 1 -- DATA OVERVIEW")
print(pd.read_sql("""
    SELECT COUNT(*) total, COUNT(DISTINCT wallet) wallets,
           COUNT(DISTINCT condition_id) markets,
           SUM(CASE WHEN market_outcome IS NOT NULL THEN 1 ELSE 0 END) resolved
    FROM wallet_trades
""", conn).to_string(index=False))

# ── Load resolutions ────────────────────────────────────────────────
res = pd.read_csv(CSV)
cid_col = [c for c in res.columns if "condition" in c.lower()][0]
yes_col = [c for c in res.columns if "resolves_yes" in c.lower()][0]
resolution_map = res.drop_duplicates(subset=[cid_col]).set_index(cid_col)[yes_col].to_dict()

trades = pd.read_sql("""
    SELECT wallet, condition_id, side, price, size, timestamp,
           category, title as question, market_outcome, pnl
    FROM wallet_trades WHERE market_outcome IS NOT NULL
""", conn)
trades["yes_won"] = trades["condition_id"].map(resolution_map)
trades = trades[trades["yes_won"].notna()].copy()
trades["correct"] = (
    ((trades["side"] == "BUY") & (trades["yes_won"] == True)) |
    ((trades["side"] == "SELL") & (trades["yes_won"] == False))
).astype(int)
trades["uncertain_entry"] = ((trades["price"] >= 0.15) & (trades["price"] <= 0.85)).astype(int)

# Entry timing
mkt_timing = trades.groupby("condition_id").agg(first_t=("timestamp","min"), last_t=("timestamp","max")).reset_index()
trades = trades.merge(mkt_timing, on="condition_id")
trades["entry_pct"] = np.where(trades["last_t"] > trades["first_t"],
    (trades["timestamp"] - trades["first_t"]) / (trades["last_t"] - trades["first_t"]), 0.5)

print(f"resolved trades: {len(trades)}, wallets: {trades['wallet'].nunique()}")

# ── PART 2: Per-wallet insider scores ───────────────────────────────
banner("PART 2 -- INSIDER SCORES")

rows = []
for w, g in trades.groupby("wallet"):
    unc = g[g["uncertain_entry"] == 1]
    n_unc = len(unc)
    if n_unc < 3: continue
    rows.append({
        "wallet": w,
        "total_trades": len(g),
        "uncertain_trades": n_unc,
        "uncertain_accuracy": unc["correct"].mean(),
        "overall_accuracy": g["correct"].mean(),
        "avg_entry_pct": g["entry_pct"].mean(),
        "total_pnl": g["pnl"].sum() if g["pnl"].notna().any() else 0,
        "primary_category": g["category"].mode().iloc[0] if len(g["category"].mode()) else "?",
        "category_purity": g["category"].value_counts().iloc[0] / len(g),
        "n_categories": g["category"].nunique(),
        "avg_trade_size": g["size"].mean(),
        "unique_markets": g["condition_id"].nunique(),
    })
wm = pd.DataFrame(rows)
wm["insider_score"] = (
    wm["uncertain_accuracy"] * 0.35
    + (1 - wm["avg_entry_pct"]) * 0.20
    + wm["category_purity"] * 0.15
    + np.minimum(wm["uncertain_trades"] / 20, 1.0) * 0.15
    + np.minimum(wm["avg_trade_size"] / 1000, 1.0) * 0.15
)
print(f"wallets scored: {len(wm)}")
print(f"insider_score p75={wm['insider_score'].quantile(0.75):.3f} p90={wm['insider_score'].quantile(0.90):.3f}")

top = wm[wm["uncertain_trades"] >= 5].nlargest(15, "insider_score")
print(f"\ntop 15 (5+ uncertain trades):")
print(top[["wallet","insider_score","uncertain_accuracy","uncertain_trades",
           "avg_entry_pct","total_pnl","primary_category"]].to_string(index=False,
    formatters={"wallet": lambda x: x[:14]+"...", "insider_score": "{:.3f}".format,
                "uncertain_accuracy": "{:.1%}".format, "avg_entry_pct": "{:.2f}".format,
                "total_pnl": "${:,.0f}".format}))

# ── PART 4: Define insider methods ──────────────────────────────────
banner("PART 4 -- INSIDER DETECTION METHODS")

archetypes = pd.read_sql("SELECT wallet, archetype, copyable FROM wallet_archetypes", conn)
wm_a = wm.merge(archetypes, on="wallet", how="left")
NON_COPYABLE = {"arb_bot","market_maker","hft_algo","penny_collector"}

methods = {}
def add(name, mask):
    methods[name] = set(wm_a[mask]["wallet"])
    print(f"  {name}: {len(methods[name])} wallets")

add("accuracy_60", (wm_a["uncertain_accuracy"] > 0.60) & (wm_a["uncertain_trades"] >= 10))
p90 = wm_a[wm_a["uncertain_trades"]>=5]["insider_score"].quantile(0.90)
add("score_top10", (wm_a["insider_score"] >= p90) & (wm_a["uncertain_trades"] >= 5))
add("early_accurate", (wm_a["avg_entry_pct"] < 0.25) & (wm_a["uncertain_accuracy"] > 0.55) & (wm_a["uncertain_trades"] >= 5))
add("specialist", (wm_a["uncertain_accuracy"] > 0.65) & (wm_a["category_purity"] > 0.60) & (wm_a["uncertain_trades"] >= 5))
add("combined_no_bots", (wm_a["uncertain_accuracy"] > 0.58) & (wm_a["uncertain_trades"] >= 5)
    & (wm_a["avg_entry_pct"] < 0.40) & (~wm_a["archetype"].isin(NON_COPYABLE)))

# ── PART 5: Backtest (out-of-sample) ────────────────────────────────
banner("PART 5 -- OUT-OF-SAMPLE BACKTEST")
print("temporal split: first 50% identification, last 50% copy EV\n")

for name, wallets in methods.items():
    oos_list = []
    for w in wallets:
        wt = trades[(trades["wallet"] == w) & (trades["uncertain_entry"] == 1)].sort_values("timestamp")
        if len(wt) < 6: continue
        oos = wt.iloc[len(wt)//2:].copy()
        oos["od"] = np.where(oos["side"]=="BUY", oos["yes_won"].astype(float)-oos["price"],
                              oos["price"]-oos["yes_won"].astype(float))
        oos_list.append(oos)
    if not oos_list: print(f"  {name}: no OOS data"); continue
    oos = pd.concat(oos_list)
    n = len(oos); ev = oos["od"].mean(); wr = (oos["od"]>0).mean()
    _, p = stats.ttest_1samp(oos["od"], 0) if n >= 5 else (0, 2)
    p = p/2 if ev > 0 else 1-p/2
    flag = " <<<" if ev > 0 and p < 0.15 else ""
    print(f"  {name:>20}: N={n:>4} WR={wr:.1%} EV={ev:+.4f} p={p:.4f}{flag}")
    for cat, cg in oos.groupby("category"):
        if len(cg) < 3: continue
        print(f"    {cat:>15}: N={len(cg)} WR={(cg['od']>0).mean():.1%} EV={cg['od'].mean():+.4f}")

# ── PART 6: Political/Geo insider deep dive ─────────────────────────
banner("PART 6 -- POLITICAL/GEO INSIDERS")

pg = trades[trades["category"].isin(["politics","geopolitics"])].copy()
pg_unc = pg[pg["uncertain_entry"] == 1]
print(f"pol/geo uncertain trades: {len(pg_unc)}, wallets: {pg_unc['wallet'].nunique()}")

pg_met = []
for w, g in pg_unc.groupby("wallet"):
    if len(g) < 3: continue
    pg_met.append({"wallet": w, "n": len(g), "acc": g["correct"].mean(),
                    "pnl": g["pnl"].sum() if g["pnl"].notna().any() else 0})
pgm = pd.DataFrame(pg_met)

t1 = pgm[(pgm["acc"]>0.70) & (pgm["n"]>=5)]
t2 = pgm[(pgm["acc"]>0.60) & (pgm["n"]>=5) & ~pgm["wallet"].isin(t1["wallet"])]
print(f"T1 (>70%, 5+): {len(t1)}  T2 (>60%, 5+): {len(t2)}")

if len(t1) > 0:
    print(t1.sort_values("acc",ascending=False).to_string(index=False,
        formatters={"wallet": lambda x: x[:14]+"...","acc":"{:.1%}".format,"pnl":"${:,.0f}".format}))

# Backtest T1+T2 OOS
for label, tw in [("T1", t1), ("T1+T2", pd.concat([t1,t2]))]:
    oos = []
    for _, r in tw.iterrows():
        wt = pg_unc[pg_unc["wallet"]==r["wallet"]].sort_values("timestamp")
        if len(wt) < 4: continue
        o = wt.iloc[len(wt)//2:].copy()
        o["od"] = np.where(o["side"]=="BUY", o["yes_won"].astype(float)-o["price"],
                            o["price"]-o["yes_won"].astype(float))
        oos.append(o)
    if not oos: print(f"  {label}: no OOS"); continue
    oos = pd.concat(oos)
    n = len(oos); ev = oos["od"].mean(); wr = (oos["od"]>0).mean()
    _, p = stats.ttest_1samp(oos["od"], 0) if n >= 5 else (0, 2)
    print(f"  {label} OOS: N={n} WR={wr:.1%} EV={ev:+.4f} p={p/2:.4f}")

# ── PART 7: Market type analysis ────────────────────────────────────
banner("PART 7 -- MARKET TYPE ANALYSIS")

def mtype(q):
    q = str(q).lower()
    if any(k in q for k in ["election","vote","primary","nominee"]): return "election"
    if any(k in q for k in ["tariff","sanction","embargo","trade war"]): return "trade_policy"
    if any(k in q for k in ["war","invasion","ceasefire","military","strike","attack"]): return "military"
    if any(k in q for k in ["deal","agreement","treaty","negotiat"]): return "diplomatic"
    if any(k in q for k in ["fed","rate","cpi","inflation","gdp"]): return "econ_data"
    return "other"

trades["mtype"] = trades["question"].apply(mtype)
top_insiders = set(wm[wm["uncertain_trades"]>=5].nlargest(int(len(wm[wm["uncertain_trades"]>=5])*0.20),"insider_score")["wallet"])

for mt, g in trades[trades["uncertain_entry"]==1].groupby("mtype"):
    if len(g) < 10: continue
    ins = g[g["wallet"].isin(top_insiders)]
    base = g["correct"].mean()
    ins_acc = ins["correct"].mean() if len(ins) else 0
    edge = ins_acc - base
    print(f"  {mt:>15}: base={base:.1%} insiders={ins_acc:.1%} ({len(ins)}) edge={edge:+.1%}{'  *' if edge>0.10 else ''}")

# ── PART 8: Comparison ──���───────────────────────────────────────────
banner("PART 8 -- COMPARISON TO EXISTING SIGNALS")

for st in ["accumulation","whale_entry"]:
    df = pd.read_sql(f"SELECT outcome_delta, is_win FROM signal_outcomes WHERE signal_type='{st}' AND resolution_price IS NOT NULL AND entry_price>0 AND entry_price<1", conn)
    if len(df) < 5: continue
    _, p = stats.ttest_1samp(df["outcome_delta"], 0)
    print(f"  {st:>25}: N={len(df)} WR={df['is_win'].mean():.1%} EV={df['outcome_delta'].mean():+.4f} p={p/2:.4f}")

# Overlap with accumulation
acc_w = set(pd.read_sql("SELECT DISTINCT wallet FROM signal_outcomes WHERE signal_type='accumulation'", conn)["wallet"])
for name, wallets in methods.items():
    ov = wallets & acc_w
    print(f"  {name}: {len(ov)}/{len(wallets)} overlap accumulation ({len(ov)/max(len(wallets),1)*100:.0f}%)")

conn.close()
print("\ndone.")
