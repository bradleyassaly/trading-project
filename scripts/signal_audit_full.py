"""Full signal audit on corrected data — all types, all categories."""
import sqlite3
import pandas as pd
from scipy import stats

DB = "data/polymarket/wallet_intelligence.db"
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

conn = sqlite3.connect(DB, timeout=60)

# All signal types
all_types = pd.read_sql("""
    SELECT signal_type,
           COUNT(*) AS total,
           SUM(CASE WHEN resolution_price IS NOT NULL THEN 1 ELSE 0 END) AS resolved,
           SUM(CASE WHEN resolution_price IS NOT NULL AND entry_price>0 AND entry_price<1 THEN 1 ELSE 0 END) AS valid
    FROM signal_outcomes GROUP BY signal_type ORDER BY valid DESC
""", conn)
print("=== ALL SIGNAL TYPES ===")
print(all_types.to_string(index=False))

# Check market_signals for types NOT in signal_outcomes
ms_only = pd.read_sql("""
    SELECT signal_type, COUNT(*) n FROM market_signals
    WHERE signal_type NOT IN (SELECT DISTINCT signal_type FROM signal_outcomes)
    GROUP BY signal_type ORDER BY n DESC
""", conn)
if len(ms_only):
    print(f"\nSignal types in market_signals ONLY (not in signal_outcomes):")
    print(ms_only.to_string(index=False))

print(f"\n{'='*80}")
print(f"FULL SIGNAL PERFORMANCE (CLEAN DATA)")
print(f"{'='*80}")

for stype in all_types["signal_type"]:
    df = pd.read_sql("""
        SELECT outcome_delta, is_win, category, confidence, entry_price, direction, wallet
        FROM signal_outcomes
        WHERE signal_type = ? AND resolution_price IS NOT NULL
          AND entry_price > 0 AND entry_price < 1
    """, conn, params=[stype])
    if len(df) < 3:
        print(f"\n{stype}: {len(df)} valid resolved (skip)")
        continue

    n = len(df)
    wr = df["is_win"].mean()
    ev = df["outcome_delta"].mean()
    total_ev = df["outcome_delta"].sum()
    _, p_raw = stats.ttest_1samp(df["outcome_delta"], 0) if n >= 5 else (0, 2)
    p = p_raw / 2 if ev > 0 else 1 - p_raw / 2

    n_pos_cats = sum(1 for _, g in df.groupby("category") if g["outcome_delta"].mean() > 0 and len(g) >= 3)
    gates = sum([n >= 20, ev > 0, p < 0.15, wr > 0.52, n_pos_cats >= 2])
    tag = " <<< LIVE CANDIDATE" if ev > 0 and p < 0.15 and n >= 10 else ""

    print(f"\n{stype}{tag}")
    print(f"  N={n}  WR={wr:.1%}  EV={ev:+.4f}  p={p:.4f}  total=${total_ev:+.1f}  gates={gates}/5")
    for cat, cg in df.groupby("category"):
        if len(cg) >= 3:
            print(f"    {cat:>15}: N={len(cg)}  WR={cg['is_win'].mean():.1%}  EV={cg['outcome_delta'].mean():+.4f}")

conn.close()
