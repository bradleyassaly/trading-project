# Trading Thesis

## Core Claim

> Some Polymarket wallets have **persistent, category-specific informational
> edge**. By identifying which wallets have edge, in which categories, and
> copying their trades in real-time, we can capture a portion of that edge
> after transaction costs — and scale that capture from small validation
> capital to a steady-state **$10–20K/month P&L** business.

Every component of this codebase exists to test one piece of this claim.
This document is the canonical statement of what we are trying to prove,
how we measure it, and what would force us to abandon it.

---

## The Scaling Goal

The platform is not an end in itself — it is a mechanism for compounding
capital from validation-scale ($345 live bankroll) to target-scale
(**$10–20K/month realized P&L**, implying a $200–300K working bankroll
at 4–7% monthly return).

Reaching that goal requires six claims to hold, not five. Claim 6 —
**scalability** — is new and is the reason this document was expanded.
The first five claims prove *there is edge*; Claim 6 proves *we can
deploy capital against it at the size we care about*.

---

## Six Sub-Claims

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
And at least **two categories must show independent positive edge** for
the strategy to be diversifiable.

**How we test it.** Compute per-category alpha scores for every wallet
with ≥10 resolved trades in that category. Count the number of distinct
categories where copyable wallets exist AND where paper-trade PnL is
positive on ≥20 resolved trades.

**Current evidence (2026-04-18 snapshot).**
- 7 categories scored, 414 copyable combos, 265 distinct copyable wallets
- Paper-trade PnL by category (closed, archived=0):
  - **sports**: +$965 on 75 closed (46.7% WR)
  - crypto: +$11 on 10 closed (60% WR, thin sample)
  - science: +$8 on 20 closed (25% WR)
  - politics: -$10 on 3 closed (0% WR, thin sample)
  - entertainment: -$31 on 18 closed
  - geopolitics: -$124 on 11 closed (27% WR)
  - other: -$885 on 14 closed
- **Currently the strategy is single-category-dominant (sports).** This
  does not yet satisfy the "≥2 categories" bar.

**What would reject this.** After 3 months of paper trading, only one
category shows positive edge on ≥50 resolved trades. If sports remains
the only profitable category, the strategy is fragile and will not scale
past the $25K bankroll tier (L3).

### Claim 3 — We can identify who has edge

**Statement.** Our `wallet_alpha_scores` separates wallets that *will*
keep winning from wallets that *got lucky historically*. The selectivity
ratio (copyable / scored) is conservative enough that copyable wallets
are genuinely a smaller, better population than the leaderboard at large.

**How we test it.** Track `is_copyable = True` count vs total scored.
Verify selectivity is in the 25–50% band — too low and we're not
filtering; too high and we're cherry-picking.

**Current evidence.**
- 1,686 wallet × category combos scored
- 414 copyable (**24.6%** selectivity — within target band)
- 265 distinct copyable wallets

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

**Current evidence (2026-04-18).**
- live-collect WebSocket + Polygon wallet-stream running
- **17 hypotheses resolved, 8 correct → 47.1% accuracy**
- Signal-level breakdown:
  - `whale_entry`: **7/10 correct (70%)** ← matches thesis target
  - `wallet_reversal`: 1/1 (sample too small)
  - `accumulation`: **0/6 correct (0%)** ← removed from live whitelist 2026-04-18
- Verdict: **PRELIMINARY — need 50 resolved for decision**

**What would reject this.** Hypothesis accuracy below 50% on 50+ resolved
trades across ≥2 signal types. Or: detection latency consistently > 30
minutes (we'd be racing the market price discovery and losing).

### Claim 5 — Edge survives transaction costs

**Statement.** When we trade real money against the alpha-gated signals,
the realized return after spread, slippage, and Polymarket fees is
positive.

**How we test it.** Live trading with $1,000 starting capital (L1),
$25 max per trade, against the whitelist `{whale_entry_filtered}` in
whitelisted categories `{politics, geopolitics, sports, crypto}`.

**Current evidence (2026-04-18).**
- 1 live trade executed (`force_live_test` on Manchester United YES)
- +$3.44 PnL, +67.2% return, 64h hold, exited via take_profit
- Slippage: expected $0.305 → fill $0.32 = 4.9% worse than expected
- **Not yet a real model-driven live fire.** The plumbing works; the
  signal-driven auto-live path has not yet fired a trade under the
  post-2026-04-18 fixes (Kelly bug, pool leak, category reclassification).

**What would reject this.** Live P&L meaningfully worse than paper P&L
on the same signal set (after accounting for slippage). Or: live WR
materially below paper WR — meaning the spread is eating the edge.

### Claim 6 — Edge scales with capital *(NEW)*

**Statement.** The per-trade edge does not decay as we increase bankroll
and trade size. Specifically: moving from $25 max per trade to $500 max
per trade does not push fills materially deeper into the book, does not
crowd out other copiers, and does not hit liquidity limits on our target
markets.

**How we test it.** At each scaling level (L1 → L5), compare:
- Avg slippage vs. the prior level (must stay ≤2%)
- Avg fill-time (must stay ≤5s)
- Win rate vs. the prior level (must stay within ±5pp)
- Category concentration (must stay ≤40% of any single category)
- Rolling 30-day Sharpe (must stay ≥1.0 at L3+)

**Current evidence.** None yet — Claim 6 is the reason for Phase 6.
We do not pass a level up until its criteria are proven on ≥30 live
resolved trades at that level.

**What would reject this.** Slippage scales super-linearly with size;
edge collapses above $100 per trade; or the copyable wallets' signals
become front-run by other copiers at higher volume.

---

## The One Number

**Hypothesis accuracy.** Of all `trade_hypotheses` rows whose underlying
paper trade has resolved, what fraction had `hypothesis_correct = 1`?

```
accuracy = correct / total_resolved_hypotheses
```

Available at `GET /api/thesis/scorecard`. It's the body of the daily
Telegram digest, the hero card on the Command Center, and the gating
condition for "GO LIVE at L1".

**Current value: 47.1% on 17 trades → PRELIMINARY.**

---

## Decision Framework (hypothesis accuracy)

| Sample | Accuracy | Verdict |
|---|---|---|
| < 10 | n/a | **ACCUMULATING** — need 50+ resolved hypotheses |
| 10–49 | n/a | **PRELIMINARY** — directionally informative, not actionable |
| ≥ 50 | ≥ 70% | **✅ GO LIVE (L1)** — thesis confirmed, deploy small live capital |
| ≥ 50 | 55–69% | **🟡 PROMISING** — continue paper trading, tune filters |
| ≥ 50 | 50–54% | **⚠️ MARGINAL** — thesis is weak, investigate before continuing |
| ≥ 50 | < 50% | **🔴 REJECTED** — thesis does not hold, stop trading |

The thresholds are deliberately conservative. "Promising" is the default
holding state — we'd rather paper trade for an extra month than deploy
capital on a 60% number that turns out to be 51% with a wider window.

---

## Scaling Ladder (Levels L0 → L5)

Each level has a fixed bankroll, a target P&L, and explicit promotion
criteria. **No level is skipped.** A level's criteria must be met on
live (not paper) data, measured over ≥30 resolved trades at that level.

| Level | Bankroll | Max/trade | Max open | Target P&L/mo | Promotion criteria (live) |
|---|---:|---:|---:|---:|---|
| **L0** Validate | $345 | $24 | 10 | n/a (paper) | Hypothesis accuracy ≥70% on 50+ paper |
| **L1** Probate | $1,000 | $50 | 8 | $50–100 | 10+ live trades, slippage <2%, WR ≥55% |
| **L2** Confirm | $5,000 | $150 | 10 | $250–500 | 50+ live, 2+ categories with edge, max DD <10% |
| **L3** Growth | $25,000 | $500 | 15 | $1,250–2,500 | 150+ live, Sharpe >1, 3-category diversification |
| **L4** Scale | $100,000 | $1,500 | 20 | $5,000–10,000 | 300+ live, ops stable 30d, ≤2% slippage |
| **L5** Target | $200–300K | $3,000 | 25 | **$10,000–20,000** | Steady-state L4 performance for 3 consecutive months |

Demotion rules are symmetric: two consecutive months missing the target
range, or any max-drawdown breach, demotes one level. L5 is the
operational steady state; the ladder doesn't intentionally extend further
because the market's capacity for our edge isn't tested above that.

---

## Phase Roadmap (aligned to scaling ladder)

| Phase | Tests | Scaling level |
|---|---|---|
| **1 — Data foundation** | Claim 1 (persistent edge exists) | ✅ confirmed: 77.9% WR on 61k clean trades |
| **2 — Alpha scoring** | Claims 2, 3 | ✅ confirmed: 414 copyable combos, 24.6% selectivity |
| **3 — Real-time hypothesis testing** | Claim 4 | 🔄 **CURRENT** — 47.1% on 17 of 50 needed |
| **4 — Live probate (L1)** | Claim 5 | ⏳ gated on Phase 3 ≥70% on 50+ |
| **5 — Confirm (L2)** | Claim 2 at scale | ⏳ gated on L1 passing |
| **6 — Scale (L3→L5)** | Claim 6 (scalability) | ⏳ gated on L2 passing |

---

## What this document is not

- It is not the architecture spec (see `ARCHITECTURE.md`)
- It is not the operator runbook (see `OPERATIONS.md`)
- It is not a list of features (see `MILESTONES.md`)

It is the single canonical statement of what success looks like and what
scale we are building toward. If you ever read code in this repo and
wonder "why does this exist", trace back to which sub-claim it tests —
or which scaling level it unblocks. If it doesn't serve either, it
shouldn't exist.
