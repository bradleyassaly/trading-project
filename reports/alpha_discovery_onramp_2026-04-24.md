# Alpha Discovery On-Ramp
*2026-04-24*

The system has the plumbing to find alpha. What it lacks is a **standardized way to go from "I have a hypothesis" → "this is a paper-tested signal" → "this is live."** This document defines that path so the next 5 hypotheses each take days, not weeks, and never lose state in someone's notebook.

The whole pipeline is 6 stages. Each has a deliverable, a check, and a stop-on-fail rule.

---

## Stage 1 — Hypothesis (1 hr)

A hypothesis is a falsifiable claim about a slice of the market. Bad: "wallets that trade a lot are good." Good: "wallets whose first 30-day directional WR exceeds 60% maintain ≥55% WR over the next 30 days, in the same category."

**Deliverable:** A one-paragraph claim + the slice it applies to (signal_type, side, category, entry-price band, wallet tier).

**Check:** Can you state the failure mode? (e.g. "if the slice's 30d WR is <50% on n≥30, we kill it.")

**Stop-on-fail:** If the hypothesis isn't measurable from existing tables (`wallet_trades`, `signal_outcomes`, `polymarket_paper_trades`), don't proceed. Vague hypotheses produce vague results.

## Stage 2 — Backtest (2-4 hr)

Replay the hypothesis over historical data. Use `scripts/signal_engine_backtest.py` for signal-level claims and `tests/polymarket/test_*` patterns for wallet-level claims.

**Deliverable:** A short Markdown report with: (a) WR over the test window, (b) PnL, (c) profit factor, (d) sample size, (e) per-month drawdown, (f) one-sentence comparison to the random baseline.

**Check:** Does the slice beat random by at least 5pp WR on n≥50? Is profit factor ≥1.3?

**Stop-on-fail:** Anything below baseline + p≥0.05 dies here. Don't promote on borderline data.

## Stage 3 — Out-of-sample test (1 day)

Re-run the same code on a held-out window the backtest didn't see. Walk-forward, last-30-days, or whatever non-overlapping slice exists.

**Deliverable:** OOS WR + PnL + n.

**Check:** Does OOS WR drop more than 8pp vs in-sample? If yes, you overfit.

**Stop-on-fail:** Severe degradation on OOS = the claim only fit the noise in the original window.

## Stage 4 — Paper deployment (2-7 days)

Add the slice to `EXCLUDE_SIGNAL_TYPES`/`STAKE_MULTIPLIERS`/`EXCLUDE_SIGNAL_SIDE` as appropriate, restart `live-collect`, watch.

**Deliverable:** Live paper trades flowing under the new gate, with a clear log marker (`[STAKE_BOOST]`, `[SIDE_GATE]`, `[FAV_GATE]`, etc.) per fire.

**Check:** Within 7 days, does the paper-side outcome roughly match the OOS prediction? Sample size ≥30 resolved.

**Stop-on-fail:** Live paper diverges substantially from the OOS prediction — investigate latency, slippage, or gate interaction before escalating.

## Stage 5 — Live probation (3-14 days)

Add the signal type to `LIVE_SIGNAL_TYPES`. Live executor will route it through 14 gates including KillSwitch's MIN_RESOLVED + EV check (which now reads `signal_calibration.kelly_fraction` — wired today).

**Deliverable:** ≥10 live trades through the gate, slippage measured, fills logged.

**Check:** Live WR within 5pp of paper WR? Slippage p50 ≤ 2%?

**Stop-on-fail:** Slippage above target or live WR materially below paper WR ⇒ pull from `LIVE_SIGNAL_TYPES`, return to paper.

## Stage 6 — Stake escalation (ongoing)

Once live-validated, add the slice to `STAKE_MULTIPLIERS` with a multiplier proportional to the WR/EV evidence. Apply matching tighter SL/TP. Document in `reports/stake_multiplier_decisions_<date>.md`.

**Deliverable:** Logged stake-multiplier line, paper+live performance dashboard tile.

**Check:** Per-week PnL trend positive over the next 2 weeks at the boosted size?

**Stop-on-fail:** If the boosted size produces worse risk-adjusted returns than the unboosted one, demote.

---

## Existing alpha hypotheses — current stage

| Hypothesis | Stage | Evidence |
|---|---|---|
| `wallet_reversal YES` boost | 5 (live probation) | 11 paper, 73% WR, +$19.74 — `STAKE_MULTIPLIERS` 1.5× |
| `cascade YES` boost | 5 | 18 paper, 56% WR, +$13. `STAKE_MULTIPLIERS` 1.3× |
| `whale_entry_filtered` both sides | 5 | 14+6 paper, 43% YES / 67% NO. 1.2× / 1.3× |
| Long-shot YES (entry <0.30) | 5 | 74 paper, 50% WR, +$67. 1.25× layered |
| `cascade NO` block | 4 (paper-deployed) | 5 paper, 20% WR, blocked via `EXCLUDE_SIGNAL_SIDE` |
| `wallet_reversal NO` block | 4 | 20 paper, 35% WR, blocked |
| `accumulation NO` block | 4 | 5 paper, 0% WR, blocked |
| `network_leader_entry` cull from live | 3 (OOS test in progress) | 8 paper, 25% WR, -$5.43 — needs OOS test |
| Wallet "earliness" / momentum score | 1 (hypothesis) | Defined in `alpha_audit_2026-04-24.md`; awaits backtest |
| Direct CTFExchange MatchOrders WS | 1 | 9-min wallet-trade lag is the best argument for it |
| Specialist boost as currently shaped | 4→3 demote candidate | 7d paper showed negative lift; needs 30d re-test |

## Next 5 hypotheses to test (ranked by EV/effort)

1. **Wallet earliness** — define as `(wins in last 7d) / (trades in last 7d) − (lifetime WR)`. Boost wallets with positive delta. Backtest against the 8d cohort. *2-4 hr.*
2. **Category specialization z-score** — for each (wallet, category), compute WR z-score vs that wallet's lifetime average. Wallets >1σ in a category are de facto specialists. *2 hr.*
3. **Spread-of-spreads** — when a market's bid-ask spread compresses below historical median while a tier-1 wallet enters, the trade is more likely to fill at favorable price. *3 hr.*
4. **Cross-market correlation** — when a tier-1 wallet enters market A, are there market B's whose YES price moves correlate, and does fading B vs A produce alpha? *5 hr.*
5. **Re-entry on whale return** — once we've exited a position (SL or TP), does re-opening if the source wallet trades the same market again produce edge? *3 hr.*

Each one should follow stages 1-3 before any code touches paper. The on-ramp is built; this is the queue.

---

## What changed today (operational)

These were shipped today as part of unblocking the on-ramp:

- **`KellySizer` reads `signal_calibration.kelly_fraction`** — graduated signals now size at their calibrated fraction, not the static 0.25. New `kelly_fraction_source` field in the sizing dict.
- **Tier-1 poll interval 900s → 300s** (env override `TIER1_POLL_INTERVAL_SEC`). Direct attack on signal latency for tier-1 wallets on off-WS markets.
- **`[KS_BLOCK:CODE]` structured logging** in live executor — every kill-switch rejection now categorized (ENV / EMERGENCY / MIN_RESOLVED / EV / WR / DISABLED / UNKNOWN). One `grep` produces a histogram of binding gates.
- **`/api/system/readiness` endpoint** — single-shot READY/DEGRADED/NOT_READY answer. Watchdog also gained a `scheduler_failures` check that catches `consecutive_failures ≥ 5` from `state.json`.
- **Live executor `LIVE_SIGNAL_TYPES`** widened from `{whale_entry_filtered}` to `{whale_entry_filtered, wallet_reversal, cascade}` (the proven-WR YES winners). FAV_GATE 0.50 → 0.65 to expand live-eligible band.

Together these are the structural fixes that move us from "alpha is real but the pipe is leaky" to "alpha flows end-to-end."
