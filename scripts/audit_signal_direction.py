"""
Signal direction validation.

Two questions:
  Q1. Does signal_outcomes.direction match the whale's actual trade
      side in wallet_trades for the same (wallet, condition_id)?
      Direction inversion bug would show up here.

  Q2. Why is resolution_price almost always 1.0? Is it sample bias
      from the markets that have resolved so far, or is the
      SignalResolver attaching the wrong field?
"""
from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

from trading_platform.polymarket.db_connection import get_connection

CSV = Path("data/polymarket/gamma_resolution.csv")

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)


def banner(t: str) -> None:
    bar = "=" * 72
    print(f"\n{bar}\n  {t}\n{bar}")


conn = get_connection()

# ─────────────────────────────────────────────────────────────────────
# Q1: signal direction vs whale's actual trade side
# ─────────────────────────────────────────────────────────────────────

banner("Q1 — SIGNAL DIRECTION vs WHALE'S ACTUAL TRADE SIDE")

# Pull every (wallet, condition_id, fired_at, direction) for resolved signals
sigs = pd.read_sql(
    """
    SELECT signal_type, wallet, condition_id, direction, fired_at,
           entry_price, resolution_price, outcome_delta, is_win
    FROM signal_outcomes
    WHERE resolution_price IS NOT NULL
      AND signal_type IN ('whale_entry','accumulation','market_maker_flip','oversized_bet')
    """,
    conn,
)
print(f"resolved signals to check: {len(sigs)}")

# For each, find the whale's most recent BUY or SELL on this market
# *before* the signal fired. We check both directions.
matches = []
for _, r in sigs.iterrows():
    # last whale trade on this market by this wallet, before fire time
    row = conn.execute(
        """SELECT side, price, timestamp FROM wallet_trades
           WHERE wallet = ? AND condition_id = ?
             AND timestamp <= ?
           ORDER BY timestamp DESC LIMIT 1""",
        (r["wallet"], r["condition_id"], r["fired_at"]),
    ).fetchone()
    matches.append({
        "signal_type": r["signal_type"],
        "wallet": r["wallet"],
        "condition_id": r["condition_id"],
        "sig_dir": r["direction"],
        "sig_entry": r["entry_price"],
        "sig_delta": r["outcome_delta"],
        "sig_win": r["is_win"],
        "res": r["resolution_price"],
        "whale_side": row[0] if row else None,
        "whale_price": row[1] if row else None,
        "whale_ts": row[2] if row else None,
        "lag_min": (r["fired_at"] - row[2]) / 60 if row else None,
    })

m = pd.DataFrame(matches)
print(f"with matched whale trade (any age): {m['whale_side'].notna().sum()}/{len(m)}")

# Cross-tab signal direction vs whale side
have = m[m["whale_side"].notna()]
print("\n-- per signal_type, sig_dir x whale_side --")
for st in ("whale_entry", "accumulation", "market_maker_flip", "oversized_bet"):
    sub = have[have["signal_type"] == st]
    if len(sub) == 0:
        continue
    print(f"\n  {st} (n={len(sub)} matched):")
    ct = pd.crosstab(sub["sig_dir"].fillna("NULL"), sub["whale_side"].fillna("NULL"))
    print(ct.to_string())
    # Mean EV by alignment
    aligned = sub[sub["sig_dir"] == sub["whale_side"]]
    inverted = sub[sub["sig_dir"] != sub["whale_side"]]
    if len(aligned):
        print(f"    aligned   (sig_dir == whale_side): n={len(aligned)} "
              f"WR={aligned['sig_win'].mean():.2f} delta={aligned['sig_delta'].mean():+.4f}")
    if len(inverted):
        print(f"    inverted  (sig_dir != whale_side): n={len(inverted)} "
              f"WR={inverted['sig_win'].mean():.2f} delta={inverted['sig_delta'].mean():+.4f}")

# Same but only signals where the whale trade is recent (within 6h)
print("\n-- whale trade within 6h before signal fire --")
recent = have[have["lag_min"] <= 360]
print(f"recent matches: {len(recent)}")
for st in ("whale_entry", "accumulation", "market_maker_flip", "oversized_bet"):
    sub = recent[recent["signal_type"] == st]
    if len(sub) == 0:
        continue
    aligned = sub[sub["sig_dir"] == sub["whale_side"]]
    inverted = sub[sub["sig_dir"] != sub["whale_side"]]
    print(f"  {st}: total={len(sub)}  aligned={len(aligned)}  inverted={len(inverted)}")

# ─────────────────────────────────────────────────────────────────────
# Q2: why is resolution_price ~always 1.0?
# ─────────────────────────────────────────────────────────────────────

banner("Q2 — WHY ARE RESOLUTIONS YES-BIASED?")

# Resolution distribution per signal type
print("-- per signal_type --")
for st in sorted(sigs["signal_type"].unique()):
    sub = sigs[sigs["signal_type"] == st]
    rd = sub["resolution_price"].value_counts().to_dict()
    yes_pct = sub[sub["resolution_price"] == 1.0].shape[0] / len(sub) if len(sub) else 0
    print(f"  {st:>20}: n={len(sub)}  yes_pct={yes_pct:.0%}  dist={rd}")

# Now: is the gamma CSV itself biased? Sample 10000 cids
banner("Q2B — DOES gamma_resolution.csv ITSELF SKEW YES?")
yes_count = 0
no_count = 0
mixed = 0
seen_cid: dict[str, dict] = {}
total_rows = 0
with CSV.open(newline="", encoding="utf-8-sig") as fh:
    for row in csv.DictReader(fh):
        total_rows += 1
        cid = (row.get("condition_id") or "").strip().lower()
        rp = (row.get("resolution_price") or "").strip()
        ry = (row.get("resolves_yes") or "").strip().lower() == "true"
        if not cid or not rp:
            continue
        try:
            price = float(rp)
        except ValueError:
            continue
        if cid not in seen_cid:
            seen_cid[cid] = {}
        seen_cid[cid][("yes" if ry else "no")] = price
print(f"csv rows: {total_rows}, distinct condition_ids: {len(seen_cid)}")

# For each cid, look at the YES-token row's price (1.0 = YES won, 0.0 = NO won)
yes_won_count = 0
no_won_count = 0
malformed = 0
for cid, sides in seen_cid.items():
    if "yes" in sides:
        if sides["yes"] >= 50:
            yes_won_count += 1
        else:
            no_won_count += 1
    elif "no" in sides:
        # Only NO row present — invert it
        if sides["no"] < 50:
            yes_won_count += 1
        else:
            no_won_count += 1
    else:
        malformed += 1
print(f"distinct markets resolved YES: {yes_won_count}")
print(f"distinct markets resolved NO:  {no_won_count}")
print(f"yes_pct overall: {yes_won_count/(yes_won_count+no_won_count):.1%}")

# Now: of OUR resolved signals' condition_ids, how many actually resolved NO?
banner("Q2C — DO OUR SIGNAL CONDITION_IDs INCLUDE NO-WINS?")

cids = sigs["condition_id"].dropna().unique()
print(f"distinct signal condition_ids (resolved): {len(cids)}")

# Build the same dict but only for our cids
our_resolved = {}
for cid in cids:
    sides = seen_cid.get(cid.lower())
    if sides is None:
        continue
    yes_price = sides.get("yes")
    if yes_price is None and "no" in sides:
        yes_price = 100.0 - sides["no"]
    if yes_price is None:
        continue
    our_resolved[cid.lower()] = yes_price / 100.0  # 0..1

print(f"signal cids found in CSV: {len(our_resolved)}")
counts = pd.Series(our_resolved.values()).value_counts().to_dict()
print(f"YES-token price distribution among our cids: {counts}")
ours_yes = sum(1 for v in our_resolved.values() if v >= 0.5)
ours_no = sum(1 for v in our_resolved.values() if v < 0.5)
print(f"yes-resolved: {ours_yes}, no-resolved: {ours_no}")

# Compare against what's actually stored in signal_outcomes
banner("Q2D — DO STORED resolution_price VALUES MATCH THE CSV?")

mismatches = 0
checked = 0
mismatch_examples = []
for cid in cids:
    csv_yes = our_resolved.get(cid.lower())
    if csv_yes is None:
        continue
    rows = sigs[sigs["condition_id"] == cid]["resolution_price"].unique()
    for stored in rows:
        checked += 1
        if abs(stored - csv_yes) > 0.01:
            mismatches += 1
            if len(mismatch_examples) < 8:
                mismatch_examples.append({
                    "cid": cid[:18] + "...",
                    "csv_yes_token_price": csv_yes,
                    "stored_in_signal_outcomes": stored,
                })

print(f"checked: {checked}, mismatches: {mismatches}")
if mismatch_examples:
    print(pd.DataFrame(mismatch_examples).to_string(index=False))

conn.close()
print("\ndone.")
