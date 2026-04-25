# Signal Layer Analysis

**Date:** 2026-04-08
**Mode:** Measure first, no new features

---

## TL;DR — three uncomfortable truths

1. **The smart-money thesis as currently implemented does not work.** Across 97,917 resolved trades from leaderboard wallets, the **base win rate is 49.8%** — worse than a coin flip — and the cumulative P&L is **−$131,124,555**. Following our tracked wallets indiscriminately loses money at scale.
2. **Tier and conviction rankings are inverted** from what they should be. `tier1h` (the "high-conviction whale" tier we treat as the top of the book) has the **worst** win rate at 29.1%; `tier1` is at 85.7%; `tier2` is at 71.5%. Higher position-size conviction also correlates with **lower** win rates (small bets 60.9% / oversized bets 53.0%) AND much larger losses ($-514 avg → $-18,552 avg).
3. **Convergence has no edge.** Markets where 3+ whales are aligned have **47.7% WR** vs 52.2% for single-whale markets. Convergence is not a positive signal; it's slightly negative.

The two filters that DO have edge: **politics category** (73.9% WR, +$16.9M) and **concentrated_whale bucket** (61.7% WR, +$4.5K avg). Everything else is noise or anti-edge.

The entire signal layer needs to be re-thought around "**when** are whales right" rather than "**which** whales are smart."

---

## Frontend bug fixed (Phase 5)

`src/trading_platform/frontend/vite.config.js` proxied `/api/*` to `http://localhost:8001`. **Inside the frontend Docker container, `localhost:8001` does not exist** — the API service is at `http://api:8001` (compose network DNS). Every browser request → vite proxy → connection refused → frontend rendered an empty state on every page that depends on the API.

Fix: changed the proxy target to `http://api:8001`, made it overridable via `VITE_API_TARGET` for bare-metal dev, added `host: 0.0.0.0`. Frontend rebuilt. `curl http://localhost:5173/api/system/status` now returns 200.

---

## Phase 1 — Inventory

### Signal types in `market_signals` (what's actually fired)

| Signal Type | Fired | Executed (paper trade) | Range |
|---|---|---|---|
| `price_velocity` | **3,738** | 157 | 1775615576 → 1775692692 |
| `accumulation` | 118 | 11 | 1775613880 → 1775667147 |
| `oversized_bet` | 83 | 5 | 1775620090 → 1775667442 |
| `market_maker_flip` | 51 | 27 | 1775527927 → 1775667589 |
| `whale_entry` | **1** | 0 | 1775527927 |

98% of all fired signals are `price_velocity` (the technical scanner with no wallet basis). Only **1 wallet-derived signal has ever fired** (`whale_entry`) and it didn't execute.

### Calibration table (signal_calibration)

| Signal Type | Sample | Bayesian WR | EV/trade | Kelly | Status |
|---|---|---|---|---|---|
| price_velocity | **41** | 0.698 | $3.56 | 0.25 | **live** |
| accumulation | 3 | 0.40 | $-0.0 | 0 | building |
| market_maker_flip | 2 | 0.25 | $-0.01 | 0 | building |
| wallet_reversal, cascade, oversized_bet, convergence, specialist_entry, whale_exit, no_position_entry, pre_deadline_surge, position_reduction, **whale_entry** | 0 | — | — | — | building |

🔴 **Eleven of 13 signal types have zero sample data.** They exist as code paths but have never been triggered enough to calibrate.

### Paper trades

| Signal | Total | Resolved | W | L | WR | Avg P&L |
|---|---|---|---|---|---|---|
| price_velocity | 192 | 41 | 29 | 12 | **71%** | **+$533,383** |
| market_maker_flip | 27 | 2 | 0 | 2 | 0% | -$1,800 |
| accumulation | 11 | 3 | 1 | 2 | 33% | -$601 |
| oversized_bet | 5 | 0 | — | — | — | — |

🔴 The 71% WR / +$533K avg on price_velocity is **fictional** — these are the pre-fillability-floor degenerate cheap-NO bets flagged in `data_validation.md` DV-5. The fillability filter (entry_price 0.05–0.95) was added in session 7; future price_velocity trades will look very different.

The two **real wallet-derived** signal types (accumulation, market_maker_flip) have **5 resolved trades total, 1 win, 4 losses** — net negative. Sample size too small to draw conclusions, but consistent with the broader picture.

---

## Phase 3 — What does and doesn't have edge

### 3A — Base rate

```
total = 97,917 resolved trades
wins  = 48,714
losses= 49,203
WR    = 49.8%
avg pnl = -$1,339
total pnl = -$131,124,555
```

🔴 **Following leaderboard wallets indiscriminately loses money at scale.** The aggregate is below random.

### 3A — By tier

| Tier | Trades | WR | Avg P&L |
|---|---|---|---|
| **tier1** | 23,299 | **85.7%** | +$100.64 |
| **tier2** | 16,535 | **71.5%** | -$72.37 |
| **tier1h** | 58,083 | **29.1%** | **-$2,277.31** |

🔴 **Tier ranking is inverted.** `tier1h` is supposed to be the highest tier ("high-conviction whales with $500K+ PnL"), but its 58K trades have 29.1% WR and lose $2,277 per trade on average. The whole `tier1h` cohort is actively destroying value.

Two plausible explanations:
- **The PM-leaderboard scrape has resolved-PnL accuracy issues.** Most tier1h wallets came in via the `polymarket_30d` scrape and their trades' `pnl` was computed by our enricher from `gamma_resolution.csv`, which may misclassify some sports/parlay outcomes.
- **The lifetime-PnL filter is selecting wallets who got very lucky on a few large bets and then revert to the mean** — the leaderboard captures past glory but doesn't measure forward-looking edge.

Either way: **`tier1h` is currently a contra-indicator, not a positive signal.**

### 3A — By bucket (≥20 trades)

| Bucket | Trades | WR | Avg P&L | Verdict |
|---|---|---|---|---|
| **concentrated_whale** | 556 | **61.7%** | **+$4,463** | ✅ keep |
| market_maker | 216 | 51.9% | $0 | neutral |
| unknown | 2,772 | 48.2% | +$7 | neutral |
| portfolio_diversifier | 457 | 47.7% | -$94 | drop |
| volume_trader | 990 | 44.6% | -$35 | drop |
| contrarian_whale | 29 | 41.4% | +$823 | sample too small |
| directional | 1,177 | 41.0% | -$189 | drop |
| arb_bot | 13,951 | 35.4% | -$631 | drop (or PnL formula wrong for arb) |
| **noise** | 7,302 | **16.7%** | **-$3,492** | filter out |

🟢 **`concentrated_whale` is the only bucket that pays.** 61.7% WR with +$4.5K avg per trade. This is the strongest positive signal in the data.

🔴 **`noise` is correctly named** — 7,302 trades, 16.7% WR, -$3,492 per trade. Anything classified as noise should be filtered out of the watchlist entirely, not just deprioritized.

🔴 **`arb_bot` 35.4% WR is suspicious** — true arb bots should hover around 50% WR by definition (they trade both sides). The low number is either (a) misclassification leaking real losing wallets in, or (b) the pnl formula is wrong for one side of a paired trade.

### 3B — By category (≥50 trades)

| Category | Trades | WR | Avg P&L | Total P&L | Verdict |
|---|---|---|---|---|---|
| entertainment | 9,294 | 84.4% | -$3 | -$31,587 | high WR but breakeven |
| **politics** | **21,133** | **73.9%** | **+$799** | **+$16,880,480** | ✅ **strong edge** |
| economics | 2,340 | 73.5% | -$106 | -$248,790 | high WR, slight loss |
| science | 1,255 | 73.4% | -$94 | -$117,869 | high WR, slight loss |
| other | 6,822 | 54.1% | -$1,118 | -$7,625,507 | drop |
| crypto | 2,537 | 49.5% | -$98 | -$248,381 | drop |
| **sports** | **54,536** | **32.4%** | **-$2,562** | **-$139,732,901** | 🔴 **kill** |

🟢 **Politics is the only category with both high WR and positive aggregate P&L.** +$16.9M across 21k trades.

🔴 **Sports is a catastrophe.** 54k trades (more than half our resolved trade set), 32.4% WR, **-$139.7M total P&L**. The aggregate -$131M base rate is essentially "leaderboard wallets aggressively losing money on sports markets."

The `sports` category dominates the leaderboard wallets' activity volume but is a net huge loss. This is the single largest fact in the report. **Filtering all sports trades out of the smart-money pipeline would flip the system from -$131M to +$8M aggregate P&L** (entertainment + politics + economics + science).

### 3C — By entry price bucket

| Entry Price | Trades | WR | Avg P&L |
|---|---|---|---|
| 0.00–0.10 | 31,596 | **82.0%** | +$23 |
| 0.10–0.20 | 6,517 | 44.1% | -$409 |
| 0.20–0.30 | 5,841 | 29.8% | -$842 |
| 0.30–0.40 | 7,763 | 23.8% | -$1,877 |
| 0.40–0.50 | 13,071 | **19.5%** | **-$3,339** |
| 0.50–0.60 | 15,109 | 25.4% | -$3,228 |
| 0.60–0.70 | 6,330 | 45.0% | -$872 |
| 0.70–0.80 | 3,517 | 35.9% | -$1,986 |
| 0.80–0.90 | 2,539 | 54.3% | -$1,651 |
| 0.90–1.00 | 5,634 | 79.7% | -$105 |

The bathtub-shaped distribution is exactly what you'd see if **most "wins" are degenerate trades on already-decided markets** at the extremes (0.00–0.10 or 0.90–1.00 — markets that resolved 1¢ off a wall) and **all the contested mid-price trades are losing money**.

The 0.40–0.60 zone is where genuine binary uncertainty lives — and it has a **22.4% combined WR with -$3,283 average P&L**. Whales lose at the casino, not because they're stupid, but because they take asymmetric small-edge bets that mostly lose.

🔴 **The signal engine should not be firing on contested-mid-price markets** unless the wallet+category combination has shown specific positive edge there.

### 3D — By conviction (bet size vs wallet's avg position size)

| Conviction Level | Trades | WR | Avg P&L |
|---|---|---|---|
| 1_small (<0.5×) | 59,021 | **60.9%** | -$514 |
| 2_normal (0.5–1.5×) | 2,999 | 43.1% | -$10,757 |
| 3_large (1.5–3×) | 676 | 49.7% | -$16,368 |
| 4_oversized (>3×) | 351 | 53.0% | -$18,552 |

🔴 **Oversized bets do not win more.** Their WR (53.0%) is lower than small bets (60.9%), and their average P&L is **35× more negative** ($-18,552 vs $-514). The `oversized_bet` signal type's foundational assumption — that big bets indicate conviction worth following — is wrong in our data. Big bets indicate big losses.

This single finding **kills the conviction-multiplier logic** in the fusion score and the entire `oversized_bet` signal type as currently designed.

### 3E — By whale convergence

| Convergence | Trades | WR |
|---|---|---|
| 1 whale | 24,093 | 52.2% |
| 2 whales | 17,732 | 52.9% |
| 3+ whales | 56,092 | **47.7%** |

🔴 **Convergence is not a positive signal.** More whales aligned actually means slightly **lower** WR. The `convergence` and `cascade` signal types' assumption — that multiple whales agreeing indicates an edge — is wrong in our data.

This is also exactly what you'd expect if leaderboard whales pile into the same losing markets (sports, exotics) more often than they pile into the politics markets that actually have edge. Convergence might still work as a signal **if conditioned on category=politics**, but as a global signal it's noise.

### 3F — By recency

| Period | Trades | WR | Avg P&L |
|---|---|---|---|
| last 30d | 46,405 | 44.8% | -$1,232 |
| 30–90d | 26,858 | **38.3%** | -$2,939 |
| 90–180d | 10,345 | 65.2% | -$1,233 |
| 180d+ | 14,309 | **76.1%** | **+$1,240** |

🔴 **Recent trades have worse WR than older trades.** Either:
- The leaderboard wallets have lost their edge over time (selection-survivor bias on the leaderboard scrape — these wallets were good ENOUGH historically to make the list, but the same skill doesn't predict the future)
- OR the resolution enricher's recent-trade pnl calculations are mismeasured
- OR sports and other negative-edge categories have risen as a share of recent activity

Whichever it is, the **recency-weighted win rate** that powers the conviction score should NOT just take the most recent N trades and treat them as the best signal — the data says recent trades are LESS predictive.

---

## Phase 4 — Signal logic critique (from Phase 3 results)

| Signal | Foundational Assumption | Data Says | Verdict |
|---|---|---|---|
| **whale_entry** | tracked wallets win more than 50% | leaderboard base rate is 49.8%, tier1h is 29.1% | 🔴 **assumption invalid** at the cohort level |
| **convergence** / **cascade** | multiple whales agreeing = edge | 3+ whales aligned has 47.7% WR (worse than 1 whale) | 🔴 **assumption invalid** |
| **oversized_bet** | big bets = conviction worth following | oversized bets have lower WR and 35× larger losses | 🔴 **assumption invalid** |
| **accumulation** | repeated entries = building conviction | only 3 resolved trades, WR 33%, -$601 avg | ⚠️ insufficient sample but trending negative |
| **market_maker_flip** | MM switching sides = informed | only 2 resolved, 0 wins, -$1,800 each | ⚠️ insufficient sample but trending negative |
| **specialist_entry** / **pre_deadline_surge** / **whale_reversal** / **whale_exit** / **no_position_entry** / **position_reduction** | various | **0 resolved trades**, never tested | ⚫ unproven, dormant code |
| **price_velocity** | rapid price moves = info edge | 71% WR / +$533K avg are fictional pre-fillability-floor stats; underlying logic also has no wallet basis | 🔴 disable or prove with realistic fills |
| **order_book_imbalance** | bid/ask imbalance = directional pressure | 0 resolved, never proven | ⚫ unproven |

### Cross-cutting flaws

1. **No category filter.** The signal engine fires on every category, but only `politics` has positive aggregate edge. Sports trades dominate the volume and dominate the losses. Every signal type should default to `category in ('politics', 'economics', 'science')` and require explicit override for other categories.

2. **No tier filter.** The engine treats all leaderboard wallets equally despite tier1h being a 29.1% WR contra-indicator. Either:
   - drop tier1h from the watchlist entirely until we understand why its WR is so low, or
   - rebuild the tier-assignment logic so tier1h actually represents forward-looking skill, not historical PnL

3. **Conviction multiplier inverted.** The fusion score treats large bets as positive evidence; the data says large bets have larger losses. The multiplier should be inverted (or removed) until we have a category-specific dataset where it actually works.

4. **No recency penalty.** The system trusts recent trades; the data says they predict worse than old trades. Either weight the rolling window very lightly or stop using it for sizing.

5. **noise bucket still in the watchlist.** 7,302 trades from `noise`-bucketed wallets, 16.7% WR, -$3,492/trade. These should be filtered at the watchlist level — they should never be eligible to fire any signal.

---

## Recommendations (priority order)

### 🔴 P0 — Stop the bleeding

1. **Filter all `sports` trades out of the smart-money pipeline.** 54,536 trades, -$139.7M aggregate. This single filter would flip leaderboard P&L from -$131M to +$8.6M.
2. **Drop `noise`-bucket wallets from the watchlist.** 7,302 trades, 16.7% WR, -$3,492 avg.
3. **Remove the conviction multiplier from the fusion score** (or invert it). Oversized bets lose more, not less.
4. **Disable `oversized_bet` signal type.** Foundational assumption is invalidated by the data.
5. **Investigate why tier1h is a contra-indicator.** Either fix the PnL calculation for the polymarket_30d-sourced trades, OR demote tier1h until forward edge is proven.

### 🟡 P1 — Filter to where edge actually exists

6. **Default category filter to `politics`** for all wallet-derived signals. Politics has +73.9% WR and +$16.9M aggregate P&L; the rest are net-negative or breakeven.
7. **Require concentrated_whale bucket** for high-allocation signals. It's the only bucket with positive avg P&L (+$4,463/trade) at meaningful sample size.
8. **Reject signals on contested mid-price markets (0.40–0.60)** unless the wallet has demonstrated specific edge in that price band. Currently 28k trades, 22.4% WR, -$3,283 avg.
9. **Remove convergence-as-positive-signal logic.** 3+ whale alignment is *negative* in our data. Convergence might still work as a politics-only signal but as a global one it's noise.

### 🟢 P2 — Investigate before acting

10. **Validate the resolution enricher** for recent trades. Recency hit-rate is suspiciously low (38.3% in the 30–90d window). Either edge has decayed OR enrichment is misclassifying recent outcomes.
11. **Re-evaluate the 11 dormant signal types.** 11 of 13 signal types have zero resolved samples. Either the trigger conditions are too narrow (so the signal never fires), OR the wallet stream isn't producing the right inputs. Worth a separate trace.

---

## Phase 5 — Frontend fix

**Bug:** `vite.config.js` proxied `/api` to `http://localhost:8001`. Inside the frontend Docker container, `localhost:8001` doesn't resolve to anything; the API service is at `http://api:8001` (compose DNS). Every browser fetch to `/api/*` proxied to a nonexistent service and got "connection refused", which the React pages rendered as empty states.

**Fix:** changed proxy target to `http://api:8001`, made overridable via `VITE_API_TARGET`, added `host: 0.0.0.0`. After rebuild, `curl http://localhost:5173/api/system/status` returns 200.

```js
// vite.config.js
const API_TARGET = process.env.VITE_API_TARGET || 'http://api:8001'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: { '/api': { target: API_TARGET, changeOrigin: true } },
  },
})
```

---

## What I deliberately did NOT do

Per the constraints: **no signal logic was changed in this session**. This is a measurement and diagnosis pass. The recommendations above are observations from the data and need explicit operator approval before any code change to the signal layer.

The Tier 2 enhancements task (maker/taker, toxicity, event hedging, volume anomaly) from the previous session prompt was also paused in favor of this measurement work — none of those features were built. If the recommendations above are accepted, several Tier 2 features become moot anyway (e.g., toxicity scoring for the entire leaderboard makes less sense if half the leaderboard turns out to be a contra-indicator).

---

## Bottom line

The system has been **paper-trading on a fundamentally inverted thesis**. The tracked wallets, taken as a cohort, lose money at scale (-$131M / 49.8% WR). Three multipliers (tier1h, convergence, oversized_bet) are not just neutral but actively negative. The only consistently positive signals in 97k resolved trades are: **(a) politics category, (b) concentrated_whale bucket, (c) wallets with specific category-bucket edge that we have not yet measured per-wallet**.

The right next move is **not** to add more signals or more enhancements. It is to:
1. Filter out the things that are bleeding money (sports, noise, oversized bets)
2. Restrict signals to the categories and buckets that have proven edge
3. Re-derive `tier1h` from forward-looking skill instead of historical PnL

Then re-measure.
