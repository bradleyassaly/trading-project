"""Category grouping analysis: politics vs geopolitics vs merged.

NOTE: reads the legacy sqlite snapshot, which is FROZEN since the
2026-04-14 Postgres cutover — treat results as historical. Live data
lives in Postgres via db_connection.get_connection().
"""
import sqlite3
import pandas as pd
from scipy import stats

DB = "data/polymarket/wallet_intelligence.db"
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

conn = sqlite3.connect(DB, timeout=60)

def banner(t):
    print(f"\n{'='*72}\n  {t}\n{'='*72}")

# ── Part 1: Baseline ────────────────────────────────────────────────
banner("PART 1 -- CATEGORY DISTRIBUTION")

print("-- wallet_trades --")
print(pd.read_sql("""
    SELECT category, COUNT(*) trades, COUNT(DISTINCT wallet) wallets,
           COUNT(DISTINCT condition_id) markets,
           ROUND(SUM(size * price),0) volume_usdc
    FROM wallet_trades GROUP BY category ORDER BY trades DESC
""", conn).to_string(index=False))

print("\n-- signal_outcomes by category x signal_type (resolved only) --")
print(pd.read_sql("""
    SELECT category, signal_type, COUNT(*) n,
           SUM(CASE WHEN resolution_price IS NOT NULL THEN 1 ELSE 0 END) resolved
    FROM signal_outcomes GROUP BY category, signal_type
    HAVING resolved > 0 ORDER BY resolved DESC
""", conn).to_string(index=False))

# ── Part 2: Signal performance pol vs geo vs merged ─────────────────
banner("PART 2 -- SIGNAL PERFORMANCE: POL vs GEO vs MERGED")

for st in ["accumulation","whale_entry","market_maker_flip","oversized_bet","price_velocity"]:
    df = pd.read_sql("""
        SELECT outcome_delta, is_win, category FROM signal_outcomes
        WHERE signal_type=? AND resolution_price IS NOT NULL
          AND entry_price>0 AND entry_price<1
    """, conn, params=[st])
    if len(df) < 3: continue

    pol = df[df["category"]=="politics"]
    geo = df[df["category"]=="geopolitics"]
    pg = df[df["category"].isin(["politics","geopolitics"])]
    oth = df[~df["category"].isin(["politics","geopolitics"])]

    def row(label, d):
        if len(d) < 2: return f"  {label:>14}: N={len(d)} (too few)"
        ev = d["outcome_delta"].mean()
        wr = d["is_win"].mean()
        _, p = stats.ttest_1samp(d["outcome_delta"], 0) if len(d) >= 5 else (0, 2)
        p = p/2 if ev > 0 else 1-p/2
        return f"  {label:>14}: N={len(d):>4}  WR={wr:.1%}  EV={ev:+.4f}  p={p:.4f}"

    print(f"\n{st} (N={len(df)})")
    print(row("overall", df))
    print(row("politics", pol))
    print(row("geopolitics", geo))
    print(row("pol+geo", pg))
    print(row("other cats", oth))

    if st == "accumulation" and len(geo) >= 3 and len(pg) >= 3:
        gev = geo["outcome_delta"].mean()
        pev = pg["outcome_delta"].mean()
        print(f"\n  ** merging {'DILUTES' if pev < gev else 'MAINTAINS'} EV: "
              f"geo={gev:+.4f} -> merged={pev:+.4f} (delta={pev-gev:+.4f})")

# ── Part 3: Wallet overlap ──────────────────────────────────────────
banner("PART 3 -- WALLET OVERLAP")

pol_w = set(r[0] for r in conn.execute("SELECT DISTINCT wallet FROM wallet_trades WHERE category='politics'"))
geo_w = set(r[0] for r in conn.execute("SELECT DISTINCT wallet FROM wallet_trades WHERE category='geopolitics'"))
both = pol_w & geo_w
print(f"politics only: {len(pol_w-geo_w)}")
print(f"geopolitics only: {len(geo_w-pol_w)}")
print(f"both: {len(both)}")
print(f"overlap: {len(both)/len(pol_w|geo_w):.1%}")

if both:
    ov = pd.read_sql(f"""
        SELECT wallet, category,
            COUNT(CASE WHEN pnl IS NOT NULL THEN 1 END) resolved,
            SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) wins,
            SUM(pnl) total_pnl
        FROM wallet_trades
        WHERE wallet IN ({",".join("?" for _ in both)})
          AND category IN ('politics','geopolitics') AND pnl IS NOT NULL
        GROUP BY wallet, category
    """, conn, params=list(both))
    if len(ov) > 0:
        ov["wr"] = ov["wins"] / ov["resolved"].clip(lower=1)
        piv = ov.pivot_table(index="wallet", columns="category", values="wr", aggfunc="first").dropna()
        if len(piv) >= 5 and "politics" in piv.columns and "geopolitics" in piv.columns:
            from scipy.stats import pearsonr
            r, p = pearsonr(piv["politics"], piv["geopolitics"])
            print(f"\nWR correlation (pol vs geo): r={r:.3f} p={p:.4f}")
            if r > 0.5: print("  -> strong: expertise transfers -> merging reasonable")
            elif r < 0.2: print("  -> weak: different skills -> keep separate")
        else:
            print(f"\ninsufficient overlap wallets with both categories resolved ({len(piv)})")

# ── Part 5: Boundary markets ────────────────────────────────────────
banner("PART 5 -- BOUNDARY MARKETS")

pol_m = pd.read_sql("SELECT DISTINCT condition_id, question FROM wallet_trades WHERE category='politics'", conn)
geo_m = pd.read_sql("SELECT DISTINCT condition_id, question FROM wallet_trades WHERE category='geopolitics'", conn)
geo_kw = ["iran","israel","ukraine","russia","china","taiwan","nato","sanction","military",
           "war","invasion","missile","nuclear","ceasefire","troops","hamas","hezbollah",
           "putin","zelensky","netanyahu","khamenei"]
boundary = 0
for _, r in pol_m.iterrows():
    q = r["question"].lower()
    if any(k in q for k in geo_kw):
        boundary += 1
miscat = boundary / max(len(pol_m)+len(geo_m), 1) * 100
print(f"politics markets with geo keywords: {boundary}/{len(pol_m)}")
print(f"miscategorization rate: {miscat:.1f}%")
if miscat > 10: print("  -> high: boundary is fuzzy -> merge")
elif miscat < 3: print("  -> low: categories clean -> keep separate")

# ── Part 7: Historical simulation ────────────────────────────────────
banner("PART 7 -- SIMULATION: TOTAL PnL BY STRATEGY")

acc = pd.read_sql("""
    SELECT outcome_delta, is_win, category FROM signal_outcomes
    WHERE signal_type='accumulation' AND resolution_price IS NOT NULL
      AND entry_price>0 AND entry_price<1
""", conn)

trade_size = 25
configs = {
    "A: all categories": acc,
    "B: geopolitics only": acc[acc["category"]=="geopolitics"],
    "C: politics only": acc[acc["category"]=="politics"],
    "D: pol+geo merged": acc[acc["category"].isin(["politics","geopolitics"])],
    "E: excl crypto": acc[acc["category"]!="crypto"],
}
print(f"\n{'config':<30} {'N':>5} {'WR':>7} {'EV':>8} {'PnL@$25':>10}")
for name, d in configs.items():
    if len(d) < 2: continue
    ev = d["outcome_delta"].mean()
    pnl = ev * trade_size * len(d)
    _, p = stats.ttest_1samp(d["outcome_delta"], 0) if len(d)>=5 else (0,2)
    p = p/2 if ev>0 else 1-p/2
    print(f"{name:<30} {len(d):>5} {d['is_win'].mean():>7.1%} {ev:>+8.4f} {pnl:>+10.1f}")

# ── Part 9: All-categories analysis ─────────────────────────────────
banner("PART 9 -- ALL CATEGORIES EV (all signal types combined)")

all_sig = pd.read_sql("""
    SELECT category, outcome_delta, is_win FROM signal_outcomes
    WHERE resolution_price IS NOT NULL AND entry_price>0 AND entry_price<1
""", conn)
for cat, g in all_sig.groupby("category"):
    if len(g) < 5: continue
    ev = g["outcome_delta"].mean()
    _, p = stats.ttest_1samp(g["outcome_delta"], 0)
    flag = "POS" if ev > 0 and p/2 < 0.15 else ("pos" if ev > 0 else "neg")
    print(f"  {cat:>15}: N={len(g):>4} WR={g['is_win'].mean():.1%} EV={ev:+.4f} p={p/2:.4f} [{flag}]")

conn.close()
print("\ndone.")
