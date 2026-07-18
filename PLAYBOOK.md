# The Validated Playbook

How this system tests hypotheses without fooling itself. Every rule below was
paid for with a specific, dated failure. New lanes, signals, and experiments
MUST follow this; deviations are themselves experiments and need a reason.

## 1. Pre-register before the first data point

Declare in code/docs BEFORE any result exists: the hypothesis, the sample
target (n or days), the KILL criteria, and the PROMOTE criteria. No parameter
tuning inside the window — a changed parameter is a NEW experiment.

- Template: `src/trading_platform/polymarket/maker_experiment.py` (criteria in
  the module docstring, `--evaluate` prints progress against the gate).
- Paid for by: resolution_decay ran months without a binding kill criterion;
  its WR floor (0.30) sat below a breakeven its own exit policy had moved to
  0.47 — structurally unable to fire (2026-07-16 eval).

## 2. Distrust the instrument before trusting the result

Before a backtest/paper verdict counts, prove the measurement is achievable:
- Exits must be fillable (walk the book, charge spread; NO transient-spike
  marks). Paid for by: +$11/trade "alpha" that was -$0.56/trade live — 78% of
  paper take-profit "wins" were on markets that resolved to $0
  (memory: 2026-07-16-alpha-verdict; fix: cost_model.py spike-impact).
- Fill simulators must have data. Paid for by: the maker dry-run read
  market_ticks, which was EMPTY for its universe — 0 fills looked like "no
  flow" but was a blind instrument (commit 2f06e99).
- Accounting must see the whole position. Paid for by: fade-the-losers
  +72%/$ that evaporated under both-legs accounting — a flagged "loser" had
  made +$186k (memory: 2026-07-16-fade-losers-test).

## 3. Split-sample everything; verify adversarially

Identify on P1, measure on P2 — never rank and score on the same window.
Any surprising result gets an adversarial pass whose job is to REFUTE it
(check the cited code yourself, re-derive dollar claims, test against an
EXTERNAL ground truth). Findings that survive get acted on; a
plausible-but-wrong finding is worse than none.
- Paid for by: wallet-copy "winners" that regressed out of sample
  (2026-07-16 retest), and the fade backtest above.

## 4. External truth beats internal state

When our DB and Polymarket/on-chain disagree, the chain wins. Coverage and
correctness are MEASURED CONTINUOUSLY, not asserted:
- `scripts/data_quality_sla.py` (12h): sampled wallet coverage vs /activity,
  settled vs realtime split, resolution coverage. Alerts under 95%.
- `scripts/wallet_truth_compare.py`: on-demand per-wallet truth diff.
- Paid for by: the wallet layer silently holding 23% of an active wallet's
  trades for months (memory: 2026-07-16-data-layer-fix).

## 5. Monitor the business function, not just the infrastructure

A green stack with zero fills is an outage. The watchdog must include at
least one check per BUSINESS function (trading_liveness: no live fill in 18h
while blocked attempts accumulate → page).
- Paid for by: 3 days dark, every infra check green (2026-07-16 eval).

## 6. Deployed means running, and alerts must arrive

- "Merged" ≠ running: bind-mount deploys need the drift guard (commit
  1fc81bf) and a restart after code changes to long-lived processes.
- Never mark work "shipped" without `git branch --contains` proof. Paid for
  by: the slippage fix stranded on a worktree branch while memory said
  "FIXED" (memory: feedback_slippage_fix_never_merged).
- Alert paths are code: they fail (Telegram 400 on unescaped HTML silently
  dropped the ONE alert naming the dark lane). Test delivery, log failures
  loudly, retry degraded (plain text) rather than dropping.

## 7. Kills are decisions, not side effects

Gates may PAUSE a lane; only a declared criterion (or the operator) may KILL
one — and the kill must be announced on a channel that provably delivers.
Every ratchet needs a documented recovery path (re-promotion, probe lane).
- Paid for by: a per-wallet attribution side-effect silently ending all
  trading via a one-way DEMOTED ratchet with no clear path.

## 8. Experiments are bounded

Dry-run by default; live arming is an explicit operator env-flag; hard caps
on order count, outstanding notional, and a cumulative stop-loss enforced in
code. The experiment must be unable to exceed its budget by construction.

## Current experiment queue (each pre-registered before first data)

1. Maker experiment — resting NO on longshot sports (running; gate ~+14d).
2. resolution_decay narrowed band retest (blocked on demote-ratchet fix).
3. Copy final verdict on complete data (blocked on units sweep + FIFO rerun).
4. Maker-on-liquid-markets variant (design only if #1 shows pickoff-limited).
