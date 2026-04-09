# Trading Thesis

## Core Claim

> Some Polymarket wallets have **persistent, category-specific informational
> edge**. By identifying which wallets have edge, in which categories, and
> copying their trades in real-time, we can capture a portion of that edge
> after transaction costs.

Every component of this codebase exists to test one piece of this claim.
This document is the canonical statement of what we are trying to prove,
how we measure it, and what would force us to abandon it.

---

## Five Sub-Claims

The core claim decomposes into five testable sub-claims. Each one has its
own measurement, its own evidence threshold, and a specific failure mode
that would reject it.

### Claim 1 — Persistent edge exists

**Statement.** Some Polymarket wallets win more than 55% of their resolved
trades over 50+ samples, and that win rate persists across rolling windows
(it's not random luck on a single hot streak).

**How we test it.** Compute the lifetime and 30-day rolling win rate over
all `pnl_reliable=1` trades for every leaderboard wallet. Compare cohort
WR to chance.

**Current evidence (clean cohort, post-quarantine).**
- 61,122 reliable resolved trades from 104 local-source wallets
- Cohort WR: **77.9%** lifetime
- 30-day rolling WR: confirmed (`persistence = "confirmed"` if rolling > 0.55)
- See `reports/signal_analysis_clean.md`

**What would reject this.** Cohort WR consistently below 55% for 90 days
on a non-shrinking sample. Or: top wallets' 30-day WR materially worse
than their lifetime WR (suggesting their edge has decayed).

### Claim 2 — Edge is category-specific

**Statement.** A wallet with edge in politics may have no edge in crypto.
The right unit of analysis is `(wallet, category)`, not just `(wallet)`.

**How we test it.** Compute per-category alpha scores for every wallet
with ≥10 resolved trades in that category. Count the number of distinct
categories where copyable wallets exist.

**Current evidence.**
- 7 categories scored
- All 7 have at least 11 copyable wallets
- politics: 23 copyable / 84 scored
- sports: 24 copyable / 71 scored
- entertainment: 22 copyable / 51 scored
- See `wallet_alpha_scores` table; `KPITracker._claim_2`.

**What would reject this.** Edge concentrated in one category only — i.e.
if politics had 50 copyable wallets and every other category had ≤2,
the "category-specific" framing would be redundant.

### Claim 3 — We can identify who has edge

**Statement.** Our `wallet_alpha_scores` separates wallets that *will*
keep winning from wallets that *got lucky historically*. The selectivity
ratio (copyable / scored) is conservative enough that copyable wallets
are genuinely a smaller, better population than the leaderboard at large.

**How we test it.** Track `is_copyable = True` count vs total scored.
Verify selectivity is in the 25–50% band — too low and we're not
filtering; too high and we're cherry-picking.

**Current evidence.**
- 387 wallet × category combos scored
- 130 copyable (33.6% selectivity)
- 56 distinct copyable wallets
- See `KPITracker._claim_3`.

**What would reject this.** Copyable wallets winning at the same rate as
non-copyable ones. Or: selectivity drifting above 70% (meaning the
filter has stopped filtering).

### Claim 4 — We can copy in real-time

**Statement.** When a copyable wallet trades on a market in their proven
category, our system can detect it, generate a hypothesis, and place a
paper trade fast enough that the edge hasn't already been priced in.

**How we test it.** Count alpha-gated paper trades. Every alpha-gated
trade is a real-time copy attempt. The 70% accuracy bar on hypothesis
outcomes is the test of whether the timing is fast enough.

**Current evidence.**
- live-collect WebSocket subscribed to ~200 markets, watching 235 wallets
- Alpha gate active in `whale_signal_engine._fire_signal` AND
  `polymarket_paper_executor.execute_signal` (defense in depth)
- Every alpha-gated trade generates a hypothesis row
- Hypothesis accuracy is **the** scorecard number
- See `KPITracker._claim_4` + `reports/hypothesis_alert_rollout.md`

**What would reject this.** Hypothesis accuracy below 50% on 50+ resolved
trades. Or: detection latency consistently > 30 minutes (we'd be racing
the market price discovery and losing).

### Claim 5 — Edge survives transaction costs

**Statement.** When we trade real money against the alpha-gated signals,
the realized return after spread, slippage, and Polymarket fees is
positive.

**How we test it.** Live trading with $500 starting capital, $25 max per
trade, only against copyable wallets in proven categories.

**Current evidence.** **NOT YET TESTED.** This claim cannot be evaluated
until paper trading + hypothesis accuracy clears the Phase 4 gate (>70%
on 50+ trades).

**What would reject this.** Live P&L meaningfully worse than paper P&L
on the same signal set (after accounting for slippage). Or: live WR
materially below paper WR — meaning the spread is eating the edge.

---

## The One Number

**Hypothesis accuracy.** Of all `trade_hypotheses` rows whose underlying
paper trade has resolved, what fraction had `hypothesis_correct = 1`?

```
accuracy = correct / total_resolved_hypotheses
```

This is what `KPITracker._thesis_scorecard()` returns. It's the body of
the daily Telegram digest, the hero card on the Command Center, and the
gating condition for "GO LIVE".

---

## Decision Framework

| Sample | Accuracy | Verdict |
|---|---|---|
| < 10 | n/a | **ACCUMULATING** — need 50+ resolved hypotheses |
| 10–49 | n/a | **PRELIMINARY** — directionally informative, not actionable |
| ≥ 50 | ≥ 70% | **✅ GO LIVE** — thesis confirmed, deploy small live capital |
| ≥ 50 | 55–69% | **🟡 PROMISING** — continue paper trading, tune filters |
| ≥ 50 | 50–54% | **⚠️ MARGINAL** — thesis is weak, investigate before continuing |
| ≥ 50 | < 50% | **🔴 REJECTED** — thesis does not hold, stop trading |

The thresholds are deliberately conservative. "Promising" is the default
holding state — we'd rather paper trade for an extra month than deploy
capital on a 60% number that turns out to be 51% with a wider window.

---

## Phase Roadmap

| Phase | Test | Status |
|---|---|---|
| **1 — Data foundation** | Claim 1 (persistent edge exists) | ✅ confirmed: 77.9% WR on 61k clean trades |
| **2 — Alpha scoring** | Claims 2, 3 (category-specific, identifiable) | ✅ confirmed: 130 copyable combos, 56 wallets, 7 categories |
| **3 — Real-time hypothesis testing** | Claim 4 (timing) | 🔄 **CURRENT** — hypotheses accumulating, scorecard live |
| **4 — Live execution** | Claim 5 (survives costs) | ⏳ gated on Phase 3 ≥70% accuracy on 50+ trades |
| **5 — Scale** | n/a | ⏳ gated on Phase 4 confirming positive EV on 100+ live trades |

---

## What this document is not

- It is not the architecture spec (see `ARCHITECTURE.md`)
- It is not the operator runbook (see `README.md`)
- It is not a list of features

It is the single canonical statement of what success looks like. If you
ever read code in this repo and wonder "why does this exist", trace back
to which sub-claim it tests. If it doesn't test one, it shouldn't exist.
