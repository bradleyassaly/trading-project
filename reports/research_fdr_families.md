# Research FDR Families — pre-registration (G2)

**Registered 2026-07-08**, alongside the A1 harness (`signal_research_harness.py`
+ `research_ledger.py`). This document is the pre-registration surface for
hypothesis families. The ledger enforces the budgets mechanically: a NEW
hypothesis_id in a family at its budget **raises** — you cannot search first
and register later.

## Rules (procedural, like the mirror-copy kill rule)

1. Every family is registered HERE with a budget BEFORE its first run.
2. Default budget: **20 distinct hypotheses** per family (`FDR_MAX_FAMILY`).
3. Promotion of any harness-discovered predicate (slice promoter allowlist,
   a resolution_decay-style BUY re-entry, sizing changes) must cite a ledger
   row id whose family `bh_fdr_report` q-value passes at **q ≤ 0.10**.
4. A `pass` verdict requires ALL of: event-clustered 95% CI lower bound > 0,
   ex-top-3 EV > 0, within-event shuffle p ≤ 0.05, and same-sign EV in every
   walk-forward fold — at n_events ≥ 30. Anything less is `fail` or
   `insufficient`, and it stays in the ledger either way.
5. Re-running an existing hypothesis_id (new window, new lag) is free — the
   budget counts DISTINCT hypotheses, not runs.

## Registered families

| family | budget | scope | registered |
|---|---|---|---|
| `a1_examples` | 20 | Harness shakedown: price bands, category bands, crowding. NOT for promotion — calibration of the tool itself. | 2026-07-08 |
| `resolution_decay_refinements` | 20 | Predicates refining the one live edge: price/hours/subcategory cuts beyond the A2 lookup. Promotions must ALSO clear the A2 champion/challenger Brier gate. | 2026-07-08 |
| `alpha_candidates_2026_07` | 20 | New-strategy exploration over resolved whale-BUY flow: does a wallet-quality / bet-size / crowding / favorite-side filter carry net-of-cost, point-in-time, event-clustered edge? The wallet graph as a FEATURE is explicitly allowed post-copy-KILL (kill rule §"what a clean kill means"); copy-ENTRY is not being revived. | 2026-07-08 |
| `wallet_persistence` | 10 | Standing weekly measurement (scripts/wallet_persistence_test.py --record): does per-wallet skill persist OOS, and does top-5 SIZE-PROPORTIONAL exclusive copying clear the pass bar as event-n accumulates? First run: rho +0.56-0.66 (persistence REAL), top-5 prop +0.249/$ OOS but only 25 events + one −100% wallet ⇒ insufficient. This is the ONLY sanctioned path to revisiting the copy kill: structurally different thesis (proportional sizing × persistent selection), evidence accumulated in the ledger, promotion needs the standard bar + BH q ≤ 0.10 + shadow ladder. | 2026-07-09 |

Add a row here (and commit) before running a new family.
