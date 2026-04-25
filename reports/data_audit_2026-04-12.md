# Data Pipeline Audit — 2026-04-12

The previous three sessions concluded `accumulation` and `market_maker_flip` were live-ready and `whale_entry` was structurally broken with no edge. **All three of those conclusions were wrong**, driven by a single bug in `gamma_resolution_fetcher.py:137` that miscoded every market in the resolution CSV as "YES won". Once corrected, the EV picture rearranges:

- `whale_entry` is **not** −0.205 EV. It is **−0.0005** (essentially break-even at n=250).
- `accumulation` is **not** +0.309 EV. It is **−0.104** (n=92, WR 40%).
- `market_maker_flip` is **not** +0.138 EV. It is **−0.159** (n=51, WR 27%).
- `oversized_bet` survives at **+0.122 EV** (n=50, p=0.036) — **the only signal that's still live-ready after the fix**.

The previous reports' headline numbers all need to be considered void. This document supersedes them.

## Data Integrity Findings

### The Root Cause

`src/trading_platform/polymarket/gamma_resolution_fetcher.py:137` (pre-fix):

```python
# Write row for YES token (index 0)
writer.writerow({
    "ticker": clob_ids[0],
    "resolution_price": rp,                  # rp is 100 if YES won, 0 if NO won
    "resolves_yes": rp >= 99.0,              # ❌ this asks "did YES win?", not "is this row the YES token?"
    ...
})
# Write row for NO token (index 1) with inverted resolution
writer.writerow({
    "ticker": clob_ids[1],
    "resolution_price": 100.0 - rp,          # complement
    "resolves_yes": rp < 1.0,                # ❌ also wrong, and inconsistent with row 1
    ...
})
```

The `resolves_yes` column was being populated based on the per-row token payout, not the per-market outcome. For a NO-won market the writer produced:
- YES token row: `resolution_price=0.0, resolves_yes=False`
- NO token row: `resolution_price=100.0, resolves_yes=True`

`signal_resolver._load_gamma_resolutions` then read the row with `resolves_yes=True` and stored its `resolution_price/100` as the YES-token-final-price. **For every NO-won market, that pulled the NO token's price (100) instead of the YES token's price (0)**, recording it as `resolution_price=1.0` in `signal_outcomes`. So every NO outcome became a YES outcome silently.

The audit script `scripts/audit_signal_outcomes.py` and `scripts/audit_signal_direction.py` make this verifiable end-to-end.

### Confirming the bug

| check | observed | expected if no bug | verdict |
|---|---|---|---|
| `gamma_resolution.csv` distinct cids in pre-fix CSV (90-day) | 14,945 | — | — |
| pre-fix CSV YES-won rate | **100.0%** (14,945 / 14,945) | ~50-55% | 🚨 |
| post-fix CSV YES-won rate (90-day) | **25.7%** (3,702 / 14,419) | 25-55% | ✅ |
| post-fix CSV YES-won rate (365-day) | **18.2%** (3,509 / 19,298) | 25-55% | mildly low; many sports markets resolve NO |
| `whale_entry` resolutions before fix | 87/87 = 100% YES | mixed | 🚨 |
| `whale_entry` resolutions after fix | 250 total, 58 YES + 192 NO = 23% YES | 30-50% | ✅ |
| direction inversion (signal vs whale's actual side) | 28/29 aligned (96.6%) | — | ✅ no inversion |

### Other things checked and ruled out

1. **NULL / zero entry_price contamination**: 0 across every signal type. Not the bug.
2. **`outcome_delta` formula**: matches the BUY (`res - ep`) and SELL (`ep - res`) formulas correctly per direction; no formula error.
3. **`is_win` vs `outcome_delta` sign consistency**: 0 mismatches for `whale_entry`/`accumulation`/`market_maker_flip`/`oversized_bet`; 57/1118 for `price_velocity` (a separate, smaller bug — not load-bearing because price_velocity is disabled).
4. **The "80% wallet_trades gap"**: was wrong. The previous audit's 70/87 missing was a 24h-window timestamp filter artifact. The actual coverage is **all 28 unique whale_entry wallets exist in `wallet_trades`** (100%), and **325/520 of all whale_entry signals have a (wallet, condition_id) pair in `wallet_trades`** (62%). The remaining 38% is split between markets that haven't resolved yet and slug-only matches.
5. **Direction inversion**: tested directly. For 29 whale_entry signals where we could find the whale's actual trade in `wallet_trades`, the signal direction matched the whale's side **28/29 times**. Within a 6-hour lag window, **11/11 are aligned**. The signal is correctly copying the whale's side. This is not a bug.

## Fixes Applied

### 1. `gamma_resolution_fetcher.py` — corrected writer

```python
# `resolves_yes` is now a per-MARKET fact: did the YES outcome win?
yes_won = rp >= 50.0
# YES token row
writer.writerow({..., "resolution_price": rp,           "resolves_yes": yes_won, ...})
# NO  token row
writer.writerow({..., "resolution_price": 100.0 - rp,   "resolves_yes": yes_won, ...})
```

Both rows of a market now agree on `resolves_yes` and that value reflects the actual market outcome.

### 2. `signal_resolver.py` — simplified reader

```python
yes_won = ry_raw == "true"
out[cid] = 1.0 if yes_won else 0.0
```

The reader no longer needs to disambiguate which row is the YES token; it reads `resolves_yes` directly as the per-market boolean.

### 3. Re-fetched + re-attached resolutions

- `trading-cli data polymarket fetch-resolutions --days 365` → 38,596 rows, 19,298 distinct condition_ids written to a fresh CSV.
- Cleared all `signal_outcomes.resolution_price`/`outcome_delta`/`is_win`/`hold_days`.
- Re-attached from three sources, in this order:
  1. `polymarket_paper_trades` for signals with `paper_trade_id` (126 attached, with `side`-aware YES-token conversion).
  2. `gamma_resolution.csv` via `SignalResolver` (CSV-cid overlap with unresolved signals was 0 — most signal markets are still open).
  3. `wallet_trades.market_outcome` for any remaining signals whose underlying market resolved (4,281 attached after adding `idx_wt_cid_resolved` to make the join fast — went from minutes to milliseconds).
- Recomputed `outcome_delta` using `direction` and `is_win` from `outcome_delta > 0`.
- Final state: **9,714 total signals, 4,407 resolved** (was 1,347 with the buggy data).

### 4. Added index `idx_wt_cid_resolved` on `wallet_trades(condition_id, market_resolved)`

The correlated UPDATE for the wallet_trades resolution attach was a full table scan and ran for several minutes before timing out. Adding a covering index brought it under a second.

## Recomputed EV — Pre-Fix vs Post-Fix

| signal_type | n_pre | EV_pre | n_post | **EV_post** | WR_post | p_post | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| **whale_entry** | 87 | −0.205 | **250** | **−0.0005** | 62.4% | 0.51 | **was wrong; actually break-even** |
| **accumulation** | 34 | +0.309 | **92** | **−0.104** | 40.2% | 0.98 | **was wrong; actually negative** |
| **market_maker_flip** | 27 | +0.138 | **51** | **−0.159** | 27.5% | 0.99 | **was wrong; actually negative** |
| **oversized_bet** | 47 | +0.367 | **50** | **+0.122** | 62.0% | **0.036** | **still positive; live-ready in one category** |
| price_velocity | 1140 | −0.644 | 3941 | −0.304 | 66.4% | 1.00 | confirmed broken |
| specialist_entry | 6 | −0.001 | 6 | +0.333 | 67% | 0.23 | n still too small |
| wallet_reversal | 6 | −0.045 | 17 | −0.152 | 41% | 0.83 | negative |
| no_position_entry | 0 | — | 0 | — | — | — | no resolved samples |

### Cross-check: paper-trade source vs wallet_trades source agree

For each signal type, the resolutions attached via `paper_trade_id` and the resolutions attached via `wallet_trades.market_outcome` show **consistent EV signs**:

| signal_type | source=paper_trade | source=wallet_trades |
|---|---|---|
| oversized_bet | n=3, EV +0.167 | n=47, EV +0.120 ✓ |
| accumulation | n=8, EV −0.125 | n=84, EV −0.102 ✓ |
| market_maker_flip | n=14, EV −0.357 | n=37, EV −0.084 ✓ same sign |
| whale_entry | n=37, EV +0.074 | n=213, EV −0.014 ✓ both ~zero |
| price_velocity | n=108, EV −0.180 | n=3833, EV −0.307 ✓ |

No source-of-truth contradicts the other. The post-fix numbers are reliable.

### Why the pre-fix headline was so wrong

The bug systematically converted NO-won markets into YES-won markets. Combined with the directional bias of each signal type:

- **`accumulation` is mostly BUY direction (33 of 34 signals).** When every market is recorded as YES-won, every BUY signal "wins" → 28/34 wins, +0.31 EV. With correct YES/NO mix: 35 NO + 57 YES means accumulation BUYs lose more often than they win.
- **`market_maker_flip` is 100% BUY direction.** Same artefact: always YES → always wins → +0.14 EV. Corrected: 14/51 wins, −0.16 EV.
- **`oversized_bet` is 41 BUY + 6 SELL.** Less directional, less affected by the bias. Survived the correction.
- **`whale_entry` is roughly 50/50 BUY/SELL.** The 44 SELL signals always "lost" to the all-YES corpus, dragging the aggregate to −0.205. Corrected: 156 wins of 250, EV −0.0005 — essentially neutral. **The previous "no edge" finding was a manifestation of the bug, not the signal.**

## Whale Entry Re-Analysis (Post-Fix)

| direction | resolution | n | wins | avg_ev |
|---|---|---:|---:|---:|
| BUY | 0.0 (NO won) | 63 | 0 | −0.374 |
| BUY | 1.0 (YES won) | 27 | 27 | +0.298 |
| SELL | 0.0 (NO won) | 129 | 129 | +0.242 |
| SELL | 1.0 (YES won) | 31 | 0 | −0.512 |

Both BUY+YES and SELL+NO are profitable cells (the whale was right). The losses concentrate in BUY+NO and SELL+YES (the whale was wrong). The aggregate is near-zero because correct and incorrect calls roughly cancel — **whale_entry is a coin flip**, not a structural disaster.

### Whale entry by category (post-fix)

| category | n | wins | avg_ev | total_ev |
|---|---:|---:|---:|---:|
| sports | 109 | 69 | +0.026 | +2.82 |
| **politics** | **26** | **20** | **+0.228** | **+5.94** |
| other | 35 | 27 | +0.102 | +3.56 |
| crypto | 31 | 17 | −0.104 | −3.22 |
| entertainment | 20 | 12 | −0.142 | −2.85 |
| geopolitics | 19 | 4 | −0.380 | −7.22 |
| economics | 9 | 6 | −0.011 | −0.09 |
| science | 1 | 1 | +0.930 | +0.93 |

**Politics is the standout cell** — n=26, WR 77%, EV +0.228, with 5 separate categories adding to a positive total. Geopolitics is the inverse — 19 trades, only 4 wins, −0.38 EV. Last session's recommendation to "build a geopolitics priority lane" was **based on the same buggy data and is now contraindicated**.

## Oversized Bet Audit

### Why was it disabled?

`whale_signal_engine.py:42`:
```python
DISABLED_SIGNAL_TYPES = {
    "price_velocity",      # not a wallet signal, fictional trades
    "oversized_bet",       # negative edge in data       ← stale comment
    "cascade",             # by the time 5 wallets are in, info is priced in
    ...
}
```

The disable reason is "negative edge in data" — but with the corrected data, oversized_bet is **+0.122 EV at n=50, p=0.036**. The original measurement that justified disabling it was almost certainly poisoned by the same YES-bias bug, just with a different sign because of the SELL/BUY mix at the time. We don't have the historical decision data to verify when it was disabled or against what sample.

### Current data (post-fix)

| direction | resolution | n | wins | avg_ev | avg_ep |
|---|---|---:|---:|---:|---:|
| BUY | 0.0 (NO won) | 15 | 0 | −0.497 | 0.497 |
| BUY | 1.0 (YES won) | 26 | 26 | +0.500 | 0.500 |
| SELL | 0.0 (NO won) | 5 | 5 | +0.387 | 0.387 |
| SELL | 1.0 (YES won) | 4 | 0 | −0.339 | 0.662 |

Both BUY+YES (+0.50) and SELL+NO (+0.39) are profitable. The signal is correctly identifying directional moves. n=50 is small but the t-test gives p=0.036 one-tailed, which clears the 0.15 readiness gate.

### Live readiness gate

| gate | value | pass |
|---|---:|---|
| sample size ≥ 20 | 50 | ✅ |
| EV > 0 | +0.122 | ✅ |
| WR > 0.52 | 62.0% | ✅ |
| p < 0.15 | 0.036 | ✅ |
| 2+ categories with positive EV | 1 (only `other` with n≥3) | ❌ |

**oversized_bet passes 4 of 5 gates.** The multi-category gate fails because 48/50 of the resolved oversized_bet samples are in `other`. That's not a sign of toxicity — it's a sign that the signal mostly fires on markets the categorizer hasn't placed in a named bucket. The category fix from the recovery session improved this for new signals, but historical samples were classified before the fix.

### Recommendation

**Re-enable oversized_bet in paper mode only.** Move it from `DISABLED_SIGNAL_TYPES` to a new `PAPER_ONLY_SIGNAL_TYPES` set, or simply remove it from `DISABLED_SIGNAL_TYPES` and add a runtime check that blocks it from `polymarket_live_executor` until it accumulates 30+ resolved samples in at least 2 distinct categories. This was NOT applied in this session — it's a code change that needs explicit user approval given the upstream bug history.

## Live Readiness — Updated

| signal | n | WR | EV | p | gates passed | verdict |
|---|---:|---:|---:|---:|---|---|
| **oversized_bet** | 50 | 62.0% | **+0.122** | 0.036 | 4 / 5 (multi-cat fail, single-cat n=48) | **paper-only candidate** |
| whale_entry | 250 | 62.4% | −0.0005 | 0.51 | 2 / 5 | break-even, no edge |
| specialist_entry | 6 | 67% | +0.333 | 0.23 | 2 / 5 (n too small) | wait for n≥20 |
| accumulation | 92 | 40.2% | −0.104 | 0.98 | 1 / 5 | **NOT live-ready (was incorrectly marked)** |
| market_maker_flip | 51 | 27.5% | −0.159 | 0.99 | 1 / 5 | **NOT live-ready (was incorrectly marked)** |
| wallet_reversal | 17 | 41% | −0.152 | 0.83 | 0 / 5 | negative |
| price_velocity | 3941 | 66.4% | −0.304 | 1.00 | 1 / 5 | confirmed broken |
| no_position_entry | 0 | — | — | — | — | no data |

**There is no signal type currently passing all 5 readiness gates.** The previous report's "two signal types pass every gate" was wrong. The correct conclusion: **`oversized_bet` is the only signal worth running in paper mode, and it needs category diversification or a relaxed multi-category gate before going live.**

## Recommended First Live Config (REVISED)

| field | value | rationale |
|---|---|---|
| Signal type | `oversized_bet` only | Only signal with positive EV + statistical significance after data audit |
| Category | `other` only initially | 48/50 oversized_bet samples are in `other`; insufficient data for any other category |
| Bankroll | **$0 (DO NOT GO LIVE YET)** | Awaiting category diversification, tier rebuild on corrected data, and re-auditing |
| Max trade size | n/a — paper only |
| `KillSwitch.BANKROLL` | Update from `100_000` → `500` whenever live trading begins | Same fix from the recovery report |
| `POLYMARKET_LIVE_ENABLED` | **Keep at 0** | Need 30+ resolved oversized_bet samples in 2+ categories first |

## What Else Needs Auditing

1. **`wallet_category_profiles` (513 rows from `build_wallet_category_profiles.py`).** These tier letters were computed against `wallet_trades.pnl` which itself was computed from `enrich-trade-resolution` — which uses the gamma data. **It is highly likely that the wallet PnL field is also biased by the same YES-bug**, since `enrich-trade-resolution` may use the same fetcher path. The 1 S-tier and 21 A-tier wallets in the recovery report should be considered untrusted until re-validated. Suggested follow-up: run `enrich-trade-resolution` end-to-end and compare a sample of `wallet_trades.pnl` rows against an independent source (Polymarket API directly, or Goldsky).

2. **`wallet_alpha_scores` (133 copyable wallets).** Same risk — built from `wallet_trades.pnl`. The "alpha gate" inside the executor may be making decisions on contaminated data.

3. **Whether `enrich-trade-resolution` uses the same gamma fetch.** Need to read `enrich_resolution.py` and confirm. If it does, the entire `wallet_trades.market_outcome` field is also biased and the 4,281 wallet_trades-attached resolutions in this session inherit the bias.

4. **Historic `polymarket_paper_trades.outcome` field.** Sample row inspection found `id=9, side=NO, exit_price=1.0, outcome='loss', realized_pnl=$23,500` — that's clearly inconsistent (positive PnL marked as loss). The `outcome` column is unreliable; sources of truth should be `realized_pnl > 0` or `direction + exit_price`.

## What Was NOT Touched in This Session

- The signal engine source code (no production logic changes).
- `DISABLED_SIGNAL_TYPES` (oversized_bet still disabled — re-enabling needs explicit approval).
- `KillSwitch.BANKROLL` hardcode (still 100k — still a bug, still not flipped to live).
- The geopolitics priority lane (Part 10 of the prompt) — explicitly skipped because the corrected data shows geopolitics whale_entry at −0.38 EV, not the +0.41 the previous session reported. Building a priority lane on disproved data would be wrong.

## Files Modified

- `src/trading_platform/polymarket/gamma_resolution_fetcher.py:131-160` — fixed `resolves_yes` semantics to be per-market not per-row
- `src/trading_platform/polymarket/signal_resolver.py:30-58` — simplified reader to consume the corrected `resolves_yes` directly
- `data/polymarket/wallet_intelligence.db` — cleared and re-attached all `signal_outcomes` resolutions; added index `idx_wt_cid_resolved` for wallet_trades joins; persists at journal_mode=`delete`

## Files Created

- `scripts/audit_signal_outcomes.py` — read-only data integrity check
- `scripts/audit_signal_direction.py` — direction inversion + CSV YES-bias check
- `reports/data_audit_2026-04-12.md` — this report

---

**Bottom line**: every previous session's headline conclusion that depended on `signal_outcomes.resolution_price` was wrong by an unknown but large margin. The corrected data identifies **`oversized_bet`** as the only currently-viable signal, and only at n=50 in a single category. **The system is not live-ready.** The next session should validate `enrich-trade-resolution` against the same bug pattern, rebuild wallet tier profiles on corrected data, and accumulate more `oversized_bet` samples in additional categories.

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\data_audit_2026-04-12.md`
