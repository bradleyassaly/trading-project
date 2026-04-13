"""
Signal outcomes data integrity audit.

Critical checks before trusting any EV computation:
  1. Direction inversion (BUY_YES + resolved YES should be POSITIVE delta)
  2. NULL / zero entry prices contaminating averages
  3. outcome_delta formula correctness for every signal type
  4. is_win consistency with outcome_delta sign
  5. Resolution price sanity (must be 0.0 or 1.0 for binary markets)
  6. The 80% wallet_trades gap diagnosis

Read-only. Saves nothing.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd

DB = "data/polymarket/wallet_intelligence.db"
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)


def banner(t: str) -> None:
    bar = "=" * 72
    print(f"\n{bar}\n  {t}\n{bar}")


conn = sqlite3.connect(DB, timeout=60)

# ──────────────────────────────────────────────────────────────────────
# Part 2A — entry / direction / resolution sanity
# ──────────────────────────────────────────────────────────────────────

banner("PART 2A — ENTRY PRICE / DIRECTION / RESOLUTION SANITY")

df = pd.read_sql(
    """
    SELECT id, signal_type, entry_price, direction, outcome_delta,
           is_win, resolution_price, category, confidence, wallet,
           condition_id, question
    FROM signal_outcomes
    WHERE resolution_price IS NOT NULL
    """,
    conn,
)
print(f"total resolved signals (all types): {len(df)}")

# Per signal type
for st in sorted(df["signal_type"].unique()):
    sub = df[df["signal_type"] == st]
    null_ep = sub["entry_price"].isna().sum()
    zero_ep = (sub["entry_price"] == 0).sum()
    bad_ep = sub["entry_price"].fillna(0).apply(lambda x: x < 0 or x > 1).sum()
    print(f"\n  {st}: n={len(sub)}")
    print(f"    entry_price NULL  : {null_ep}")
    print(f"    entry_price zero  : {zero_ep}")
    print(f"    entry_price out-of-range : {bad_ep}")
    print(f"    direction values  : {dict(sub['direction'].fillna('NULL').value_counts())}")
    rp_vals = sub["resolution_price"].value_counts().to_dict()
    print(f"    resolution_price  : {rp_vals}")

# ──────────────────────────────────────────────────────────────────────
# Part 2B — outcome_delta formula correctness, all signal types
# ──────────────────────────────────────────────────────────────────────

banner("PART 2B — OUTCOME_DELTA FORMULA VALIDATION")

print(f"{'signal_type':>20} {'n':>6} {'matches_yes':>12} {'matches_no':>12} {'neither':>10} {'mean_ep':>10} {'mean_od':>10}")

eps = 0.0011
formula_summary = {}
for st in sorted(df["signal_type"].unique()):
    sub = df[df["signal_type"] == st].copy()
    have_ep = sub["entry_price"].notna() & (sub["entry_price"] != 0)
    sub = sub[have_ep]
    if len(sub) == 0:
        continue
    sub["expected_yes"] = sub["resolution_price"] - sub["entry_price"]
    sub["expected_no"] = sub["entry_price"] - sub["resolution_price"]
    yes_match = ((sub["outcome_delta"] - sub["expected_yes"]).abs() < eps).sum()
    no_match = ((sub["outcome_delta"] - sub["expected_no"]).abs() < eps).sum()
    neither = len(sub) - yes_match - no_match
    formula_summary[st] = {
        "n": len(sub), "yes": int(yes_match), "no": int(no_match), "neither": int(neither),
        "mean_ep": sub["entry_price"].mean(), "mean_od": sub["outcome_delta"].mean(),
    }
    print(f"{st:>20} {len(sub):>6} {yes_match:>12} {no_match:>12} {neither:>10} "
          f"{sub['entry_price'].mean():>10.4f} {sub['outcome_delta'].mean():>+10.4f}")

# ──────────────────────────────────────────────────────────────────────
# Part 2C — direction inversion: BUY_YES + resolved YES MUST be positive
# ──────────────────────────────────────────────────────────────────────

banner("PART 2C — DIRECTION INVERSION CHECK")

# In our data, direction is 'BUY' or 'SELL' (not BUY_YES/BUY_NO).
# By convention BUY = buy YES, SELL = buy NO. Verify that.
for st in ("whale_entry", "accumulation", "market_maker_flip", "oversized_bet"):
    sub = df[df["signal_type"] == st]
    if len(sub) == 0:
        continue
    print(f"\n{st}:")
    for dir_val in sorted(sub["direction"].fillna("NULL").unique()):
        dsub = sub[sub["direction"] == dir_val] if dir_val != "NULL" else sub[sub["direction"].isna()]
        if len(dsub) == 0:
            continue
        for rp in sorted(dsub["resolution_price"].unique()):
            cell = dsub[dsub["resolution_price"] == rp]
            if len(cell) == 0:
                continue
            mean_od = cell["outcome_delta"].mean()
            mean_win = cell["is_win"].mean()
            print(f"  direction={dir_val:>6}  res={rp:.2f}  n={len(cell):>4}  "
                  f"mean_delta={mean_od:>+.4f}  mean_win={mean_win:>.2f}")

# ──────────────────────────────────────────────────────────────────────
# Part 2D — entry_price contamination impact on EV
# ──────────────────────────────────────────────────────────────────────

banner("PART 2D — ENTRY_PRICE NULL/ZERO IMPACT ON EV")

for st in ("whale_entry", "accumulation", "market_maker_flip", "oversized_bet"):
    sub = df[df["signal_type"] == st]
    if len(sub) == 0:
        continue
    bad = sub[(sub["entry_price"].isna()) | (sub["entry_price"] == 0)]
    good = sub[(sub["entry_price"].notna()) & (sub["entry_price"] != 0)]
    if len(sub) == 0:
        continue
    mean_all = sub["outcome_delta"].mean()
    mean_good = good["outcome_delta"].mean() if len(good) else float("nan")
    mean_bad = bad["outcome_delta"].mean() if len(bad) else float("nan")
    wr_all = sub["is_win"].mean()
    wr_good = good["is_win"].mean() if len(good) else float("nan")
    print(f"\n{st}: total={len(sub)}, bad_ep={len(bad)}, good_ep={len(good)}")
    print(f"   mean delta — all   : {mean_all:+.4f}  WR={wr_all:.1%}")
    print(f"   mean delta — good  : {mean_good:+.4f}  WR={wr_good:.1%}")
    print(f"   mean delta — bad   : {mean_bad:+.4f}  WR={(bad['is_win'].mean() if len(bad) else float('nan')):.2f}")
    if len(bad) > 0:
        bad_dist = bad["outcome_delta"].describe()
        print(f"   bad-ep delta min/max: {bad_dist['min']:+.4f} / {bad_dist['max']:+.4f}")
        # Are they all using the formula 'resolution_price - 0' = resolution_price?
        identical = (bad["outcome_delta"] == bad["resolution_price"]).sum()
        opposite  = (bad["outcome_delta"] == (1.0 - bad["resolution_price"])).sum()
        print(f"   bad-ep delta == resolution_price: {identical}/{len(bad)}")
        print(f"   bad-ep delta == (1 - resolution_price): {opposite}/{len(bad)}")

# ──────────────────────────────────────────────────────────────────────
# Part 2E — is_win sign consistency
# ──────────────────────────────────────────────────────────────────────

banner("PART 2E — is_win VS outcome_delta SIGN CONSISTENCY")

for st in sorted(df["signal_type"].unique()):
    sub = df[df["signal_type"] == st]
    sub = sub[sub["outcome_delta"].notna() & sub["is_win"].notna()]
    if len(sub) == 0:
        continue
    expected = (sub["outcome_delta"] > 0).astype(int)
    mismatch = (sub["is_win"].astype(int) != expected).sum()
    print(f"  {st:>20}: n={len(sub)}, is_win/delta mismatch = {mismatch}")

# ──────────────────────────────────────────────────────────────────────
# Part 3 — the 80% wallet_trades gap
# ──────────────────────────────────────────────────────────────────────

banner("PART 3 — DIAGNOSING THE 80% WALLET_TRADES GAP")

we_signals = pd.read_sql(
    """SELECT id, wallet, condition_id, fired_at, direction, entry_price, category
       FROM signal_outcomes WHERE signal_type='whale_entry'""",
    conn,
)
print(f"total whale_entry signals: {len(we_signals)}")
print(f"unique wallets: {we_signals['wallet'].nunique()}")
print(f"unique condition_ids: {we_signals['condition_id'].nunique()}")

trade_wallets = set(
    r[0] for r in conn.execute("SELECT DISTINCT wallet FROM wallet_trades").fetchall()
)
sig_wallets = set(we_signals["wallet"].dropna().unique())
overlap = sig_wallets & trade_wallets
sig_only = sig_wallets - trade_wallets
print(f"\nwallets in wallet_trades: {len(trade_wallets)}")
print(f"signal wallets in wallet_trades: {len(overlap)}/{len(sig_wallets)}")
print(f"signal wallets NOT in wallet_trades: {len(sig_only)}")

if sig_only:
    print("\nsample signal-only wallets (and where they live):")
    for w in list(sig_only)[:10]:
        homes = []
        for tbl in ("wallet_profiles", "wallet_alpha_scores",
                    "wallet_category_profiles", "wallet_alerts", "leaderboard"):
            try:
                n = conn.execute(
                    f'SELECT COUNT(*) FROM "{tbl}" WHERE wallet = ?', (w,)
                ).fetchone()[0]
                if n:
                    homes.append(f"{tbl}({n})")
            except sqlite3.OperationalError:
                pass
        print(f"  {w[:18]}... -> {', '.join(homes) or 'NOWHERE'}")

# Per-signal join state
have_pair = 0
have_wallet_only = 0
have_market_only = 0
have_neither = 0
for _, r in we_signals.iterrows():
    pair = conn.execute(
        "SELECT 1 FROM wallet_trades WHERE wallet=? AND condition_id=? LIMIT 1",
        (r["wallet"], r["condition_id"]),
    ).fetchone()
    if pair:
        have_pair += 1
        continue
    w_only = conn.execute(
        "SELECT 1 FROM wallet_trades WHERE wallet=? LIMIT 1", (r["wallet"],)
    ).fetchone()
    m_only = conn.execute(
        "SELECT 1 FROM wallet_trades WHERE condition_id=? LIMIT 1", (r["condition_id"],)
    ).fetchone()
    if w_only and not m_only:
        have_wallet_only += 1
    elif m_only and not w_only:
        have_market_only += 1
    else:
        have_neither += 1

print(f"\nper-signal join state:")
print(f"  matched (wallet + market): {have_pair}/{len(we_signals)}")
print(f"  wallet exists, market doesn't: {have_wallet_only}")
print(f"  market exists, wallet doesn't: {have_market_only}")
print(f"  neither exists in wallet_trades: {have_neither}")

# Distinct condition_ids that don't appear in wallet_trades at all
cids = set(we_signals["condition_id"].dropna().unique())
cids_in_trades = set(
    r[0] for r in conn.execute(
        f"SELECT DISTINCT condition_id FROM wallet_trades WHERE condition_id IN "
        f"({','.join('?' * len(cids))})",
        list(cids),
    ).fetchall()
)
print(f"\ndistinct signal condition_ids: {len(cids)}")
print(f"  present in wallet_trades:    {len(cids_in_trades)}")
print(f"  absent from wallet_trades:   {len(cids - cids_in_trades)}")

# ──────────────────────────────────────────────────────────────────────
# Where does whale_entry's wallet field come from?
# ──────────────────────────────────────────────────────────────────────

banner("PART 3B — WHO FIRES whale_entry?")

# Frequency of each wallet
print("top 10 wallets by whale_entry signal count:")
top = (we_signals.groupby("wallet").size().sort_values(ascending=False).head(10))
for w, n in top.items():
    in_trades = "yes" if w in trade_wallets else "NO"
    print(f"  {w[:14]}... n={n:>4}  in wallet_trades: {in_trades}")

conn.close()
print("\ndone.")
