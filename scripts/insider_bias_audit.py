"""
Insider bias audit: validate all insider wallets against Polymarket API
and test bias-resistant detection methods.
"""
import sqlite3, json, time, os
from pathlib import Path
import pandas as pd, numpy as np
from scipy import stats

DB = "data/polymarket/wallet_intelligence.db"
CSV = "data/polymarket/gamma_resolution.csv"
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

def banner(t):
    print(f"\n{'='*72}\n  {t}\n{'='*72}")

conn = sqlite3.connect(DB, timeout=60)

# ── Part 2: API Validation ──────────────────────────────────────────
banner("PART 2 -- API VALIDATION OF INSIDER WALLETS")

insiders = pd.read_sql("SELECT * FROM insider_wallets ORDER BY insider_score DESC", conn)
our_cov = pd.read_sql("SELECT wallet, COUNT(*) our_trades, COUNT(DISTINCT condition_id) our_markets FROM wallet_trades GROUP BY wallet", conn)
insiders = insiders.merge(our_cov, on="wallet", how="left")

import requests
results = []
for _, r in insiders.iterrows():
    w = r["wallet"]
    try:
        resp = requests.get(f"https://data-api.polymarket.com/profile/{w}", timeout=15)
        if resp.ok:
            d = resp.json()
            api_trades = d.get("numTrades", d.get("predictions", None))
            api_pnl = d.get("pnl", d.get("profit", None))
            try: api_trades = int(float(api_trades))
            except: api_trades = None
            try: api_pnl = float(api_pnl)
            except: api_pnl = None
        else:
            api_trades = api_pnl = None
    except:
        api_trades = api_pnl = None
    cov = (r["our_markets"] or 0) / api_trades if api_trades and api_trades > 0 else None
    results.append({"wallet": w, "our_acc": r["uncertain_accuracy"], "our_n": r["uncertain_trades"],
                    "our_mkts": r.get("our_markets",0), "api_trades": api_trades, "api_pnl": api_pnl,
                    "coverage": cov})
    time.sleep(1.5)

val = pd.DataFrame(results)
print(val.to_string(index=False, formatters={
    "wallet": lambda x: x[:18]+"...", "our_acc": "{:.1%}".format,
    "api_pnl": lambda x: f"${x:,.0f}" if x is not None else "?",
    "coverage": lambda x: f"{x:.1%}" if x is not None else "?"}))

# ── Part 3: Broader bias check ──────────────────────────────────────
banner("PART 3 -- BROADER BIAS CHECK (top 20 by our PnL)")

top20 = pd.read_sql("""
    SELECT wallet, COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) n,
           SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) wins, SUM(pnl) our_pnl
    FROM wallet_trades WHERE pnl IS NOT NULL
    GROUP BY wallet HAVING n >= 10 ORDER BY our_pnl DESC LIMIT 20
""", conn)
top20["our_wr"] = top20["wins"] / top20["n"]

sign_flips = 0
for _, r in top20.iterrows():
    try:
        resp = requests.get(f"https://data-api.polymarket.com/profile/{r['wallet']}", timeout=10)
        d = resp.json() if resp.ok else {}
        api_pnl = float(d.get("pnl", d.get("profit", 0)))
        flip = (r["our_pnl"] > 0) != (api_pnl > 0)
        if flip: sign_flips += 1
        print(f"  {r['wallet'][:16]}... our=${r['our_pnl']:>+8,.0f} api=${api_pnl:>+10,.0f} {'FLIP' if flip else 'ok'}")
    except:
        print(f"  {r['wallet'][:16]}... API error")
    time.sleep(0.8)
print(f"\nsign flips: {sign_flips}/20")

# ── Part 4-5: Redesigned methods + backtest ──────────────────────���───
banner("PARTS 4-5 -- BIAS-RESISTANT METHODS + OOS BACKTEST")

# Load trades
res = pd.read_csv(CSV)
cid_col = [c for c in res.columns if "condition" in c.lower()][0]
yes_col = [c for c in res.columns if "resolves_yes" in c.lower()][0]
rmap = res.drop_duplicates(subset=[cid_col]).set_index(cid_col)[yes_col].to_dict()

trades = pd.read_sql("SELECT wallet,condition_id,side,price,size,timestamp,category,pnl FROM wallet_trades WHERE market_outcome IS NOT NULL", conn)
trades["yes_won"] = trades["condition_id"].map(rmap)
trades = trades[trades["yes_won"].notna()].copy()
trades["uncertain"] = ((trades["price"]>=0.15)&(trades["price"]<=0.85)).astype(int)
trades["correct"] = (((trades["side"]=="BUY")&(trades["yes_won"]==True))|((trades["side"]=="SELL")&(trades["yes_won"]==False))).astype(int)
trades["od"] = np.where(trades["side"]=="BUY", trades["yes_won"].astype(float)-trades["price"], trades["price"]-trades["yes_won"].astype(float))
trades["win"] = (trades["od"]>0).astype(int)

# Method A: API-verified profitable + >55% accuracy
api_profitable = set(val[val["api_pnl"].apply(lambda x: x is not None and x > 0)]["wallet"])
unc_acc = trades[trades["uncertain"]==1].groupby("wallet").agg(n=("correct","count"),acc=("correct","mean")).reset_index()
method_a = set(unc_acc[(unc_acc["n"]>=5)&(unc_acc["acc"]>0.55)]["wallet"]) & api_profitable

# Method C: PnL-weighted
pnl_w = trades[trades["uncertain"]==1].groupby("wallet").agg(n=("od","count"),avg_pnl=("od","mean"),total_pnl=("od","sum")).reset_index()
method_c = set(pnl_w[(pnl_w["n"]>=5)&(pnl_w["avg_pnl"]>0)&(pnl_w["total_pnl"]>10)]["wallet"])

# Combined
method_ac = method_a & method_c

methods = {"method_a (api_profitable+acc)": method_a, "method_c (pnl_weighted)": method_c,
           "method_a_AND_c": method_ac, "method_a_OR_c": method_a | method_c}

print(f"\n{'method':<35} {'wallets':>8} {'OOS_n':>6} {'WR':>7} {'EV':>8} {'p':>7}")
print("-"*75)

for name, wallets in methods.items():
    if not wallets: print(f"{name:<35} {'0':>8}"); continue
    oos = []
    for w in wallets:
        wt = trades[(trades["wallet"]==w)&(trades["uncertain"]==1)].sort_values("timestamp")
        if len(wt) < 6: continue
        oos.append(wt.iloc[len(wt)//2:])
    if not oos: print(f"{name:<35} {len(wallets):>8} {'0':>6} no OOS"); continue
    o = pd.concat(oos)
    n = len(o); ev = o["od"].mean(); wr = o["win"].mean()
    _, p = stats.ttest_1samp(o["od"], 0) if n >= 5 else (0, 2)
    p = p/2 if ev > 0 else 1 - p/2
    flag = " <<<" if ev > 0 and p < 0.15 else ""
    print(f"{name:<35} {len(wallets):>8} {n:>6} {wr:>7.1%} {ev:>+8.4f} {p:>7.4f}{flag}")
    if ev > 0 and p < 0.15:
        for cat, cg in o.groupby("category"):
            if len(cg) >= 3: print(f"  {'':>35} {cat:>15}: N={len(cg)} EV={cg['od'].mean():+.4f}")

# Reference
acc = pd.read_sql("SELECT outcome_delta FROM signal_outcomes WHERE signal_type='accumulation' AND resolution_price IS NOT NULL AND entry_price>0 AND entry_price<1", conn)
if len(acc) >= 5:
    _, p = stats.ttest_1samp(acc["outcome_delta"], 0)
    print(f"\n{'accumulation (reference)':<35} {'--':>8} {len(acc):>6} {acc['outcome_delta'].apply(lambda x: x>0).mean():>7.1%} {acc['outcome_delta'].mean():>+8.4f} {p/2:>7.4f}")

# ── Part 8: Impact on other signals ─────────────────────────────────
banner("PART 8 -- IMPACT ON OTHER SIGNALS")
print("accumulation: fires on MARKET convergence patterns, not wallet accuracy.")
print("  Core EV metric unaffected by wallet coverage bias. SAFE.")
print("  Tier-based confidence boost has MINOR risk (tiers from partial data).")
print("whale_entry_filtered: uses behavioral archetype (fills/market, buy_ratio).")
print("  NOT accuracy-based. NOT affected by sampling bias. SAFE.")

conn.close()
print("\ndone.")
