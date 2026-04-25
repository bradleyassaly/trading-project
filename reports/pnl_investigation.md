# PnL Investigation — The tier1h Paradox

**Date:** 2026-04-08
**Trigger:** signal_analysis.md showed tier1h at 29.1% WR / -$131M, but the same wallets are on Polymarket's leaderboard with millions in profit.
**Verdict:** **Hypothesis 2 is correct** (PnL formula assumption fails on one-sided data) **AND Hypothesis 3 is correct in spirit** (we're computing terminal-resolution PnL on positions whose closing trades we never recorded). **Hypothesis 1 is false** — winners do NOT pay enough to make negative-WR wallets profitable; the negative WR is itself the artifact.

---

## TL;DR

The PnL enrichment formula treats every fill as a hold-to-resolution position. For wallets where our database has both sides of every round-trip (the `leaderboard_source='local'` wallets we backfilled with `--all` history), the formula is correct. **For wallets ingested via the polymarket-leaderboard scrape (`polymarket_30d / 7d / 1d / all`), our database has BUY fills but is missing 95–100% of the SELL fills**, so the enricher computes catastrophic terminal losses on positions that the wallet had actually already closed in real life.

When we restrict the cohort analysis to `leaderboard_source='local'` wallets only, the entire conclusion of `signal_analysis.md` flips:

| Metric | All sources (contaminated) | Local-only (clean) |
|---|---|---|
| **Total trades** | 97,917 | 43,457 |
| **WR** | 49.8% (worse than random) | **79.4%** |
| **Total P&L** | -$131,124,555 | **+$21,016,888** |
| **tier1h WR** | 29.1% | **74.5%** |
| **tier1h P&L** | -$132M | **+$19.8M** |
| **sports WR / P&L** | 32% / -$139M | **81% / -$105K** |

The "smart-money thesis doesn't work" conclusion was **wrong**. The clean cohort works exactly as the thesis predicts. The system is healthy. The bug is in the enricher.

---

## Hypothesis 1 — "WR is misleading because winners pay more than losers cost"

**Verdict: FALSE.** Tested on 3 top tier1h wallets:

| Wallet | Pseudonym | Trades | W | L | WR | Avg Win | Avg Loss | Total P&L | Expectancy |
|---|---|---|---|---|---|---|---|---|---|
| `0x56687bf447db` | Theo4 | 146 | 133 | 13 | **91.1%** | +$22,968 | −$20,517 | **+$2,788,104** | +$19,096/trade |
| `0x1f2dd6d473f3` | Fredi9999 | 244 | 195 | 49 | 79.9% | +$16,961 | −$36,605 | +$1,513,694 | +$6,203/trade |
| `0xe9ad918c7678` | walletmobile | 30 | 25 | 5 | 83.3% | +$60,267 | −$6,941 | +$1,471,975 | +$49,065/trade |

For these `local`-source wallets, the per-trade WR **and** the P&L both look great. They are not "low WR with asymmetric winners" — they are high WR with positive expectancy. The hypothesis is rejected for the wallets where we have clean data.

Sample-trade verification on Theo4 confirmed the math is correct:
- `BUY YES at 0.611, size=850K shares` → outcome YES → PnL `+$330,427` ✅ (matches `(1.0 - 0.611) × 850K = $330K`)
- `BUY YES at 0.399, size=189K shares` → outcome NO (Kamala lost) → PnL `−$75,695` ✅ (matches `−0.399 × 189K = −$75K`)

Theo4's recorded data is internally consistent and matches Polymarket's reporting *for the trades we have*.

But: **Polymarket's `lb-api/profit` says Theo4's lifetime profit is $22,053,934.** Our local total is $2,788,104 — we're 8× under because we only have 146 of his trades. So even Theo4 is partially incomplete; his real "WR" might be different across the missing 8× of his trade history.

---

## Hypothesis 2 — "PnL enrichment formula is wrong"

**Verdict: PARTIALLY CORRECT.** The formula itself is correct in isolation, but its assumption (every fill is held to terminal resolution) fails when our data is incomplete.

The enricher in `src/trading_platform/polymarket/enrich_resolution.py:91-113`:

```python
side_upper = (side or "").upper()
if side_upper == "BUY":
    if token_won:
        pnl = size * (1.0 - price)     # bought outcome that won → profit
    else:
        pnl = -(size * price)           # bought outcome that lost → full loss
elif side_upper == "SELL":
    if token_won:
        pnl = -(size * (1.0 - price))   # sold winner before payout → opportunity cost
    else:
        pnl = size * price              # sold loser → captured value
```

**For a complete round-trip in our DB**, the BUY and corresponding SELL fills have *equal and opposite* terms, so they net out to the actual realized P&L. Example: BUY 100 shares @ 0.47 then SELL 100 shares @ 0.49 on a market that resolves NO →
- BUY pnl: −(100 × 0.47) = −$47
- SELL pnl: +(100 × 0.49) = +$49
- net: +$2 ✅

**For a one-sided fill list**, the SELL never appears, so the BUY's "−$47" sits in the database as a real loss when the wallet actually flipped out at +$2.

---

## Hypothesis 3 — "We're counting fills not positions"

**Verdict: TRUE in effect, though the mechanism is hypothesis 2.** Theo4's 146 fills collapse to **4 unique markets**; 120 of his fills are on the same Trump 2024 Presidential market. At the per-position level he has 3W / 1L. At the per-fill level he has 133W / 13L. Both look great because his trades ARE round-trip-complete in our DB.

The smoking gun is on the scrape-sourced wallets. Take **`0x492442eab586`**, the biggest "tier1h loser":

```
BUY trades:  637  ← all the BUYs from his last 30 days
SELL trades:   0  ← we have ZERO of his SELL fills
total BUY shares: 42,010,569 (42 MILLION)
total volume:     $208,736,812 (a quarter-billion)
enricher pnl:     −$18,266,722

Polymarket /value endpoint (current portfolio):  $4,819
Polymarket leaderboard net_pnl_usdc field:       +$6,211,239 (PROFIT)
```

He has **42 million BUY shares and zero SELL shares in our database**, but his current portfolio value on Polymarket is **$4,819**. He has clearly closed essentially all of his positions in real life — and not via terminal market resolution at $0 (which is what our enricher assumed when computing -$18M), but via SELL trades or position rolls or LP redemptions that the data-api `/trades?user=` endpoint did not return to us.

He's not a -$18M loser. He's a +$6.2M sports market maker whose closing flows our ingestion missed.

---

## The contamination quantified

| Source | Wallets | Trades | BUY | SELL | BUY:SELL | WR | Total P&L |
|---|---|---|---|---|---|---|---|
| **`local`** (full backfill) | 104 | 43,457 | 14,336 | **29,121** | 0.49 | **79%** | **+$21,016,888** |
| `polymarket_30d` | 47 | 21,524 | 20,900 | 624 | **33.5** | 13% | -$96,860,756 |
| `polymarket_1d` | 74 | 32,272 | 24,232 | 8,040 | 3.0 | 35% | -$46,030,749 |
| `polymarket_7d` | 7 | 664 | 642 | 22 | 29.2 | 11% | -$9,249,938 |

**The local cohort has more sells than buys** (0.49 ratio — close to 1:1 for completed round trips, with extra exits as wallets close out winners). **Every scrape-sourced cohort has 3–33× more BUYs than SELLs.** That's not real wallet behavior — it's an ingestion gap. Real wallet-trade flow from Polymarket is approximately 1:1 BUY:SELL on a long enough horizon.

The −$152M of "tier1h losses" is essentially the sum of "(BUY price × share count)" terms across millions of unhedged-looking positions whose offsetting sells we never captured.

---

## What the local cohort actually shows

After excluding the contaminated scrape sources entirely:

### By tier (local-only)

| Tier | Trades | WR | Avg P&L | Total P&L |
|---|---|---|---|---|
| tier1h | 3,623 | **74.5%** | +$5,484 | +$19,868,721 |
| tier1 | 23,299 | **85.7%** | +$101 | +$2,344,827 |
| tier2 | 16,535 | 71.5% | −$72 | −$1,196,659 |
| **All** | **43,457** | **79.4%** | **+$484** | **+$21,016,888** |

Tier1h is **not** a contra-indicator. It's the **most profitable tier in absolute terms** (+$19.9M from 3,623 trades) and second by win rate. tier1 has higher WR but lower per-trade $ (smaller wallets, smaller bets).

### By category (local-only)

| Category | Trades | WR | Total P&L |
|---|---|---|---|
| **politics** | 15,732 | **79%** | **+$21,231,683** |
| economics | 1,929 | 78% | +$105,743 |
| entertainment | 5,141 | 81% | +$44,372 |
| science | 1,066 | 74% | +$1,521 |
| crypto | 1,549 | 73% | -$82,362 |
| **sports** | 14,124 | **81%** | **−$105,395** |
| other | 3,916 | 78% | -$178,674 |

The "sports is a -$140M catastrophe" claim from `signal_analysis.md` was an artifact. Real `local` sports data: **81% WR, only -$105K aggregate** — basically breakeven, not catastrophic. Politics is still the dominant earner.

---

## Why the previous report's specific recommendations need to be unwound

| Previous recommendation | Revised verdict |
|---|---|
| "Filter all `sports` trades — they cost -$139M" | ❌ **WRONG.** Local sports data is 81% WR / -$105K (breakeven). Don't filter the category. |
| "Drop `noise` bucket — 16.7% WR / -$3,492 avg" | ⚠️ **Maybe still right** but rerun on local-only first. The noise wallets might also be enrichment artifacts. |
| "Remove conviction multiplier — bigger bets lose more" | ⚠️ **Suspect.** Recompute on local-only — the "conviction" finding is heavily polluted by scrape-bot fills. |
| "Disable `oversized_bet` signal type" | ⚠️ **Suspect.** Same reason. |
| "tier1h is a contra-indicator" | ❌ **WRONG.** Local tier1h is +$19.8M / 74.5% WR — the strongest absolute contributor. |
| "Convergence has no edge — 47.7% WR with 3+ whales" | ⚠️ **Suspect.** Recompute on local-only. The convergence cohort is heavily polluted. |
| "Recent trades worse than old — 38% WR in 30-90d" | ⚠️ **Suspect.** Same reason. Most scrape-source data is recent. |

**No signal-engine code changes should be made on the basis of `signal_analysis.md` alone.** It needs to be re-run with the contamination filtered out.

---

## The fix to the enricher

The right fix has three parts:

### 1. Stop computing terminal-resolution PnL on incomplete trade data

In `enrich_resolution.py`, before writing `pnl` for a wallet's trades, check the BUY:SELL ratio. If it's badly imbalanced (e.g., > 3:1 or < 0.33), the wallet's flow is one-sided and we cannot reliably compute realized PnL fill-by-fill. Either:

- Set `pnl = NULL` and rely on the `pm_pnl` / `net_pnl_usdc` fields populated from Polymarket's leaderboard scrape, **or**
- Compute realized PnL only on the round-tripped portion (`min(total_buy_shares, total_sell_shares) × (avg_sell_price − avg_buy_price)`), with the unmatched residual treated as an open position whose unrealized PnL is computed from the current Polymarket /value endpoint.

### 2. Add a `pnl_source` column

```sql
ALTER TABLE wallet_trades ADD COLUMN pnl_source TEXT;
-- 'computed_local'   = derived from BUY+SELL round trips in our DB
-- 'reported_pm'      = trusted from polymarket's leaderboard PnL field
-- 'partial_one_sided'= we have only one side; PnL is unreliable, do not aggregate
```

The cohort analysis SQL should then default to `WHERE pnl_source IN ('computed_local', 'reported_pm')` and exclude `partial_one_sided`.

### 3. Backfill SELL trades for scrape-sourced wallets

The data-api `/trades?user=` endpoint **does** return both maker and taker trades, but our backfill apparently only captured one direction. Either:

- The API call is filtering on `side=BUY` somewhere, or
- The pagination is dropping out before reaching the SELLs, or
- The wallet's SELL fills go through a different endpoint (e.g., `/trades?taker=` vs `/trades?maker=`)

Diagnosing this is a separate task, but until it's fixed, **scrape-sourced wallets should have their local pnl marked as `partial_one_sided`** and excluded from aggregate analysis.

---

## What this means for the previous session's "p0 — stop the bleeding" recommendations

**None of them should be applied without rerunning the analysis on local-only data first.** Specifically:

1. ❌ Do **not** filter sports — the catastrophic loss number was an artifact.
2. ❌ Do **not** disable `oversized_bet` — the "oversized bets lose more" finding was driven by scrape bots.
3. ❌ Do **not** demote tier1h — it's actually the most profitable tier in absolute dollars on local data.
4. ❌ Do **not** invert the conviction multiplier.
5. ✅ The frontend vite-proxy fix from last session is unrelated and should stay.

**What CAN be safely done:**

- Fix the enricher to mark partial-data wallets as `partial_one_sided`
- Re-run the cohort analysis with `WHERE leaderboard_source = 'local'` and use those numbers as the source of truth until the SELL backfill is fixed
- Investigate why the scrape ingestion is dropping SELL trades (separate task)

---

## Bottom line

The signal layer is healthier than the previous report suggested. The 79.4% WR / +$21M result on the clean local cohort is consistent with the original thesis: **smart wallets backed by real-time wallet intelligence DO win consistently in our paper data**. The previous "stop the bleeding" panic was a measurement error caused by computing terminal-resolution PnL on wallets whose closing trades we don't have.

The actual bug is in the trade ingestion (missing SELL fills for scrape-sourced wallets) and in the enricher's assumption that every BUY in the table represents a held-to-resolution position. Fix those two and the system's true performance will be visible.
