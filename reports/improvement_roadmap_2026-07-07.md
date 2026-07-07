# Trading Platform: Long-Run Improvement Roadmap

**Author:** Chief architect | **Date:** 2026-07-07 | **Audience:** owner-operator
**Book state:** ~$300–386 bankroll, $1–5 discovery stakes, ~16 open positions, one proven live edge (`resolution_decay`), one reconciled-positive signal (`whale_entry_filtered` +$21/90d), zero naively-copyable wallets found.

---

## The thesis in one paragraph

Your reconciled data has already answered questions the roadmap keeps trying to re-ask. **No wallet is copyable hold-to-resolution. Calibration was worse than a coin flip. Exactly one signal earns money.** The winning move is not "build more" — it is to (1) *measure whether copy-trading should exist at all* before polishing it, (2) treat your own **uptime and reconciliation state as the highest-Sharpe strategy in the system** (outages have cost you more than any signal earns), and (3) **stop pretending you run a copy-trading + broad-signal platform and admit you run a resolution engine** — a fast, well-calibrated bettor on near-certain outcomes that Polymarket is slow to mark to 0/1. Everything that works points there. At $300, **diversification is the enemy**: every extra lane splits an already-too-small sample and guarantees no ladder ever promotes.

---

## 1. If you only do five things (highest leverage)

These are ranked by expected-dollars-per-effort **against this specific $300 book**, not by sophistication.

| # | Move | Why it compounds | Effort |
|---|------|------------------|--------|
| **1** | **Operator/uptime + reconciliation SLA with a dead-man's switch.** Alert + auto-halt when host, poller, balance-API, or reconciliation goes stale. | Your biggest negative-EV "strategy" is downtime and unreconciled state, not bad signals. A 5.7-day outage (6/15), a balance API frozen at $5.43 for 20 cycles, a breaker tripped on back-booked May losses — each plausibly cost more than `whale_entry_filtered`'s entire +$21/90d. Protecting the edge you have beats discovering a new one. | **S** |
| **2** | **Redeem sweep + capital-efficiency instrumentation.** Scheduled, idempotent on-chain redemption of resolved positions + daily deployed-%/idle-USDC/unredeemed-$ metrics. | 11 trapped positions and only ~$80 of $300 deployed means **60–70% of capital is idle or locked**. Freeing $30–50 is a 10–15% deployable-capital increase **this week, zero new alpha, zero new risk** — a larger, more certain P&L delta than any hypothetical signal. | **S** (metrics) / **M** (sweep — touches funds, must reconcile against on-chain truth, never DB status) |
| **3** | **Pre-registered copy-trading kill rule.** Write the stopping rule *before* you see the number: run the drift-haircut mirror-exit diagnostic (item C1+C2 below) as ONE read-only harness; if top-decile copyable-cohort net-of-cost EV ≤ 0, formally retire live copy-entry within 30 days and keep the wallet graph only as a feature source. | ~35 of ~50 proposals are conditionally worthless on one measurement nobody scheduled. This is the spine that turns a research program into something other than a sunk-cost machine. | **S** to write the rule; **M** for the diagnostic |
| **4** | **Held-out champion/challenger gate for every fitted curve/model.** Replace the in-sample `brier_after < brier_before*0.99` persistence check (`fit_calibration:238`, per-slice `:365`) with a strictly-later holdout tail. | A curve that memorizes noise *always* improves in-sample Brier and *always* persists — this is the mechanism behind the degenerate-collapse incidents that needed emergency guards. Every future ML/calibration/harness effort is meaningless until the instrument that judges them is honest. Hard prerequisite for items A-anything and P-anything downstream. | **M** |
| **5** | **Feature snapshot at signal-fire time (the missing feature store).** `_fire_signal` already assembles the rich dict (price, size, wallet_tier, alpha_score, converging_wallets, OB imbalance, whale_trade_ts) at ~L1285 and throws all but confidence+entry_price away. Dump it to a JSONB column, capturing the **literal point-in-time value** (not a mutable FK). | Nearly free (dict is in scope), unreconstructable later (live OB imbalance is gone forever), and the hard dependency for *every* future calibration/attribution/ML effort. Snapshotting the literal as-of-fire wallet tier is also the only place leakage can be prevented. Payoff is months out — which is exactly why it must start this week. | **S** |

**Cross-cutting theme these five share:** none of them is new alpha. Four of five are about *not bleeding the edge you have* and *making your instruments honest so you can trust any future number.* That is the correct posture for a $300 book with one working signal.

---

## 2. Governance gates that bind everything below

Three hard rules. Violating them makes the rest actively dangerous.

- **G1 — Reconciled-truth gate.** Every sizing/promotion/calibration computation reads `pnl_reliable=1 AND reconciled` rows, filtered by *economic date* (so back-booked losses can't drive today's cap), and checks a data-trust condition (min days since reconciliation, ledger-agreement streak) before acting. This block-lists most Kelly/ladder/slice work until met. *(Cross-cuts Risk + ML + Copy dimensions.)*
- **G2 — FDR / multiple-testing budget on all signal search.** The moment the harness (A1) and self-tuning slices (A5) exist, you can test thousands of predicates in minutes and *size into the winners*. Every candidate must clear a **purged, embargoed walk-forward AND a multiple-testing correction (BH-FDR / deflated Sharpe / minimum-track-record-length)** sized to how many predicates were tried. Add a **shuffle test** (refit the exact promotion pipeline on outcome-shuffled labels; confirm the signal does NOT clear). Without this the harness manufactures beautiful dead signals faster than live cycles ever did.
- **G3 — Event-clustered n, everywhere.** Outcome labels are **not IID** — many fills sit on the same event. A "Brier 0.32 on n=200" may be n=40 independent events. Add an `event_id`/condition_group and compute every metric (Brier, IC, WR, backtest CI) with **block resampling by event**. This is more foundational than any individual model; it silently corrects the over-confidence behind every thin-slice overfit.

**Net-of-cost is the default EV metric, not gross.** Every EV/Kelly/promotion number must subtract realistic round-trip cost (taker spread + depth-limited fill + gas + the 33-min indexing-lag fill price). `cost_model.py` exists; wire it in as a first-class harness output. At $1–5 stakes a +EV-gross / −EV-net trade is the single most common way these ideas die.

---

## 3. The roadmap, by phase and by trading mode

Legend: **[COPY]** copy-trading · **[AUTO]** autonomous signal · **[PLAT]** platform/infra/ML. Effort S/M/L/XL.

### NOW (next 2 weeks) — instrument, stop bleeding, decide

| Item | What + why it compounds | Mode | Effort |
|------|-------------------------|------|--------|
| **N1** | **Uptime/reconciliation SLA + dead-man's switch** (Top-5 #1). Screams and halts on stale host/poller/balance-API. Highest-Sharpe intervention in the doc. | [PLAT] | S |
| **N2** | **Capital-efficiency metrics + redeem sweep** (Top-5 #2). Metrics ship now (S); automated redemption verified against on-chain truth (M). **Do NOT couple idle-% → up-sizing** — that converts safe idle cash into correlated tail exposure. | [PLAT] | S/M |
| **N3** | **Feature snapshot at fire time** (Top-5 #5). Start accumulating now; there is no substitute and no backfill. | [PLAT] | S |
| **N4** | **Held-out champion/challenger gate** (Top-5 #4). Un-breaks every promotion decision downstream. | [PLAT/ML] | M |
| **N5** | **Fix the backtest tautology.** `backtest_framework.py:179` does a single 70/30 split and calls it walk-forward; `run_ensemble_backtest:229` scores the ensemble against the *same* `_CATEGORY_EV` constants baked into live `_compute_ensemble`. Refit per-fold priors from the train slice; swap to rolling windows. A day's work that un-breaks the promotion gate itself. Honest deliverable is per-fold dispersion — which will mostly reveal you lack data to validate anything, itself a valuable finding. | [AUTO/ML] | S |
| **N6** | **Write the copy-trading kill rule** (Top-5 #3) and schedule the diagnostic (C1+C2). Pre-registration is the whole point — write it before the number. | [COPY] | S |
| **N7** | **Complementary-leg guarantee-loss block.** Hard pre-trade check: never open two mutually-exclusive YES legs on one `condition_id`/event whose combined cost > guaranteed payoff (the 7/6 six-leg football incident = locked loss). A bankroll-fraction cluster cap does *not* catch two individually-fine small legs. Cheapest, most urgent risk fix. | [PLAT/Risk] | S |
| **N8** | **Wash/sybil gate on the copy cohort.** One line: add `is_likely_farmer != 1` to `naive_copy._get_cohort` (`naive_copy_signal.py:67-72`), which currently gates only on `wq_score` and lets clean-scored wash-traders into the shadow cohort. | [COPY] | S |
| **N9** | **Correlation-dedup of triple-counted signals.** `correlation_max`/`correlated_with` is computed every 6h and consumed *nowhere*. Auto-deprecate the lower-IC member of any ≥0.7 pair — `convergence`/`consensus_follower`/`cascade` triple-count the same multi-wallet-agreement event into Kelly (a real sizing bug). **Formalize a protected floor** (`resolution_decay`, `whale_entry_filtered`) so a transient IC dip can't kill your only two edges — the 2026-04-27 comment already learned this the hard way ($889 whale_entry alpha destroyed by IC-alone retirement). | [AUTO] | S |
| **N10** | **Get the synchronous Gamma GET off the fire-time hot path.** `_fire_signal:1251` does a per-signal live Gamma round-trip; p50 latency is the edge-killer for `resolution_decay`. Serve from the bulk index instead. (Not a "cut calls 10×" project — a latency fix.) | [PLAT] | M |

### NEAR (1–3 mo) — build the honest instruments, sharpen the one edge

| Item | What + why it compounds | Mode | Effort |
|------|-------------------------|------|--------|
| **A1** | **Counterfactual signal-research harness** — replay `wallet_trades → resolution` (60d, ~164k BUYs) with **strict point-in-time discipline (`ts < trade_ts − 33min` indexing lag)** and **net-of-cost EV as the primary output**. Bill it as a *directional-alpha* harness first; microstructure replay is dead on arrival (market_ticks pruned to 7d, order_book_snapshots only since 2026-04-30). Replaces months of live discovery cycles with a minutes-long replay — the whole ballgame for a throughput-constrained book. **Governed by G2.** | [AUTO] | L |
| **C1+C2** | **Scalp-aware, latency-haircut copyability diagnostic** (the item N6's kill rule reads). Simulate mirror-exit (not just hold-to-resolution) **with the drift haircut baked in from the start** — fast-lane (2-4s) and poller (p50 5min) scenarios kept separate, never averaged. This is what makes "nobody copyable" *believable* instead of merely suspected. Build drift-aware; do not build the perfect-fill version and correct later. | [COPY] | M |
| **A2** | **Learned per-subcategory decay curves for `resolution_decay`.** Replace the hand-coded `0.50 + 0.20·t + 0.15·price` `_confidence()` with an empirical `(subcategory × hours-bucket × price-bucket → resolve-YES rate)` lookup, **shrunk toward the category prior** (per-bucket n is tiny). Improves the *only proven edge*. Defer the game-start-vs-resolution "event-time" half — that timestamp isn't in the schema. | [AUTO] | M |
| **A3** | **Per-slice exit thresholds from the counterfactual** (NOT a classifier yet). `section_exit_counterfactual:781` already reconstructs `shares·payout − cost` vs booked P&L per slice; the −$8.58 football-stops loss is documented at L788. Read the verdict into a **daily-refreshed lookup table** (e.g. exempt `resolution_decay × sports` from stop-loss if stops lose >$X at n≥30). Attacks the highest-leverage lever — exits on the near-resolution lane — with near-zero overfit risk. Never override kill-switch hard stops. | [AUTO] | M |
| **P1** | **Canonical resolutions table.** Replace the 5 disagreeing resolution paths + rewritten `gamma_resolution.csv` with one table using a **monotonic write-once-wins precedence** (higher-confidence source never downgraded) and a **pre-cutover reconciliation report** showing where the 5 sources disagree today. Directly gates `resolution_decay`, which lives or dies on near-resolution truth. Hard dependency for P2/A2/ML labeling. | [PLAT] | M |
| **P2** | **Unify calibration on one canonical view.** `predicted_prob = confidence_raw`, `label = resolution-channel outcome only` (action-exit P&L contaminates labels — proven at counterfactual L787), joined to on-chain truth. Makes Brier numbers across jobs actually comparable. **Measure the n-loss on `resolution_decay` first** — resolution-only labels + N4's holdout floor together may starve your one live slice of trainable data. Enabler, not edge. | [PLAT/ML] | M |
| **R1** | **Trimmed (ex-top-3) EV as the edge input *everywhere*** — Kelly, kill-switch EV gate, ladder promotion, slice promotion. The whole book is lottery-shaped (politics +$54 raw / −$0.70 ex-top-3). This is the centerpiece risk fix. **Do NOT ship the variance-haircut half** for `resolution_decay`: its short-vol shape (many tiny wins, rare correlated losses) *is* its edge — a vol-scaled fraction would shrink your one working lane for having the return signature that makes it profitable. Trimmed EV: everywhere. Vol-haircut: separate, gated, lane-scoped, excludes `resolution_decay`. | [PLAT/Risk] | M |
| **R2** | **Enforce `portfolio_risk` as a pre-trade correlation gate.** `assess()` already computes cluster_flags/net-direction/`CLUSTER_MAX_BANKROLL_FRAC=0.20`; none is enforced at trade time. Wire it fail-**closed** with a **mandatory 60s cache / marginal-delta compute** (per-signal SQL on the 2-4s fast lane is a real regression). Keep N7's complementary-leg guard as a *separate* gate — the dollar cluster cap does not subsume two small locked-loss legs. | [PLAT/Risk] | M |
| **C3** | **Add `is_copyable_ci > 0` to the live copy gate (shadow first).** `is_copyable_ci` (`roi_lower_95>0 AND n≥20`) is the strongest anti-survivorship metric in the codebase and the live executor never reads it. **Do NOT** unify the three wallet scores — `cluster_id` runs on hardcoded placeholder features (`avg_entry=0.5, avg_hold=7.0, sl_rate=0.0`) and is meaningless. Just add the gate; captures ~80% of the value at S effort. | [COPY] | S |
| **C4** | **Regime/drift monitor on the two shipped edges.** Rolling EV-vs-CI-band with a Telegram alert on downtrend for `resolution_decay` and `whale_entry_filtered`. Losing the edge you have costs more than a marginal new one gains — higher EV than most new-alpha ideas. | [AUTO/COPY] | S |
| **P3** | **Raise chain-direct fast-lane HIT-RATE for *filtered* signals.** Half is built (`get_by_token_id` already does the Gamma fallback + write-back). New work: warm the markets table with each watched wallet's recent tokens on `_watched_reloader`; thread `OrderFilled` price into the staleness reference. **Instrument that converted fast-lane fills clear the EV gate** — more/faster copies of a −$0.058/trade universe just loses money faster unless the *filter* travels with the speedup. | [PLAT/COPY] | M |
| **P4** | **Cache `neg_risk` in the markets table + stop re-fetching the book 3× per entry.** `neg_risk` is immutable yet queried every order; the book is fetched in the depth check, inside `place_market_order`, and in `get_neg_risk` — three REST round-trips on the hot lane. Pure latency reduction. | [PLAT] | S |
| **P5** | **Slice promoter: DEMOTE/kill negative slices** (Wilson LB < 0), derive the allowlist instead of hardcoded EXCLUDE lists. Leave *promotion* to the already-tightened promoter (asymmetric caution: a stale-positive slice bleeds money; a missed-positive only misses upside). Governed by G2. | [AUTO] | S |
| **P6** | **Cost/slippage recording per fill** (record-only first, gate later). Persist `(mid_at_decision, best_ask, target, realized_fill, slippage_c, spread_paid_c)` on **every attempt including rejects/errors** — the comparator currently filters those out, so the trades you *didn't* get are invisible. This is the measurement substrate execution items need. Do not let it gate sizing for N weeks (buckets will be n=1-3). | [PLAT] | M |

### MEDIUM (3–6 mo) — only after instruments are honest and the kill decision is made

| Item | What + why | Mode | Effort |
|------|-----------|------|--------|
| **A4** | **Lead-lag "be-early" alpha** from `wallet_copy_graph` — front-run *follower flow* (not the leader's correctness). Gate behind A1 proving positive net-of-cost EV first. Fatal tension to resolve: your follower timestamps are the *same* 33-min-lagged data-api. Research bet, not a near-term ship. | [COPY→AUTO] | L |
| **A5** | **Cross-market dutch-book / logical-implication family — monitoring only, no trading.** Prerequisite is unbuilt (neg-risk sibling taxonomy; `event_slug` is on `markets`, not `wallet_trades`). The gap net of two thin-leg spreads is usually negative. Start with sum-of-YES monitoring; trade nothing. | [AUTO] | L |
| **R3** | **Per-lane stake ladder + Bayesian/Wilson promotion gates** (fix the 30-to-promote / 3-to-demote asymmetry as the *immediate* win; drop the "faster promotions" framing — at this book's n it's mostly a principled *brake*). Requires G1 + R1 first. Benefits mainly the one lane already working until others earn samples they may never earn. | [PLAT/Risk] | L |
| **C5** | **Leader-conviction sizing multiplier (shadow-logged).** `size_vs_avg` is computed then thrown away. Bounded multiplicative multiplier clamped by Kelly + per-leader budget — but there is **zero evidence** big leader entries copy better than nibbles, so shadow-log the would-be size and validate on the ex-top-3 frame before it moves capital. | [COPY] | S |
| **C6** | **Resting-limit thin-book exits — measurement spike first.** On near-resolution NO books, market SELLs can't fill an empty book; the `peak_no` MFE is never captured. But a resting SELL locks the whole conditional balance (documented livelock bug) and only helps where a taker eventually crosses. **Prove on the ~16 open positions how many would have a crossing taker before building the ladder.** Requires the open-orders ledger (P7). | [AUTO] | L |
| **P7** | **Canonical open-orders ledger** reconciled against `client.get_orders()` each cycle. The conditional-balance-lock is the single most bug-prone execution surface; a live_orders table is the prerequisite for *any* passive/resting strategy (C6, and passive entries). | [PLAT] | M |
| **P8** | **Wallet-quality forward-edge model** — the copy side has *no* self-improvement loop. Predict per-wallet forward edge from features (their own calibration, category concentration, entry-timing vs resolution, size behavior, edge decay) and continuously re-rank the universe. **Only if C1+C2 found a copyable cohort** — otherwise this is discovery before the filter. | [COPY/ML] | L |

### BETS (6mo+ / uncertain) — revisit only if bankroll is 5–10× and prerequisites hold

- **Passive maker / liquidity-provision lane** — a second full product with the highest operational risk in the set. Spread on $1–5 rests is rounding error; adverse selection is the whole game. **Not a $300 problem.** [COPY/AUTO, XL]
- **Mempool / pending-tx front-running** — optimizes the last 2s of a pipeline whose p50 is 5min. Strictly dominated by P3. Paid infra + speculative-fire unwind risk to be first to a −$0.058/trade edge. [PLAT, XL]
- **Gradient-boosted per-regime probability model** — nonexistent per-regime independent-event n. Will memorize event-level survivorship noise and isotonic will faithfully calibrate garbage. Trigger: only after a *linear* model beats the ensemble out-of-sample AND per-regime independent-event n reaches the hundreds — a year-plus away, if ever. [ML, XL]
- **Bandit / A-B allocation layer** — prediction-market outcomes take days and are event-correlated; independent-sample count (not dollars) is the binding constraint. Keep only the `experiment_id` tagging as cheap plumbing for N4. [ML, L]
- **Fitted crowding→slippage curve, OLS edge-decay slopes, exit-policy classifier** — all die on small-n / noisy-slope at this throughput. Use the cheap already-computed substitutes (step-function prior; `wallet_earliness` delta as a soft one-tier demotion; per-slice threshold table A3) until volume is 10–50× higher.

---

## 4. Ideas that sound good but the data does not support yet

- **Copy leader-discovery engine** — funnel before the filter. No wallet is copyable; finding *more* to copy is speculation stacked on speculation.
- **Proportional/partial mirror-exit trims** — uneconomic at $1–5 (CLOB 1-token min; two SELL round-trips on a $3 position destroy more than the info is worth). Keep only the dust floor + "don't full-exit on a token trim."
- **Anything routed on `cluster_id`** — it's computed on hardcoded placeholder features. Either wire real `avg_hold_hours` (exists in `wallet_strategy_analyzer`) or drop it from every gate. Silently relying on a placeholder cluster is worse than no cluster.
- **Lifting Kelly's 2% cap now** — the $6 cap vs $5 floor is a $1 window, but binding tier caps ($1–5) make it *smaller*, not bigger. No-op until the ladder promotes past Tier 1, which it never has. Sequence R1 (trimmed EV) before touching Kelly's room, or it differentiates on lottery-tail noise.
- **Per-topic-cluster daily-loss kill switch** — a cluster would have to lose ~$15-30 (most of a day's risk) to trip meaningfully; the whole-book breaker already covers that magnitude. The pre-trade cluster *block* (R2/N7) is the right shape, not a post-hoc halt.
- **Microstructure as a *gating* feature** — you can never backtest the conditioning (OB history since 4/30, capped, sparse). Log it into `confidence_factors` now to accumulate forward; do not let it gate sizing for many months.

---

## 5. Sequencing / prerequisites (what must precede what)

```
G1 (reconciled-truth gate) ──blocks──► R1, R3, all ladder/slice promotion
N4 (held-out gate) ─────────precedes──► A1, A2, P2, all "fitted" work
N3 (feature store) ─────────precedes──► all future calibration/ML/attribution
G3 (event-clustered n) ─────underlies─► every Brier/IC/CI/backtest number
G2 (FDR budget) ────────────gates─────► A1 harness, P5 slice promoter
P1 (canonical resolution) ──gates─────► A2, P2, resolution_decay correctness
N6 kill rule + C1+C2 diagnostic ──decides──► whether C3/C5/A4/P8 exist at all
R1 (trimmed EV) ────────────precedes──► R3, any Kelly-room change
P7 (open-orders ledger) ────precedes──► C6, passive entries
```

**The one decision that reshapes the whole roadmap:** run C1+C2 as a single read-only diagnostic (NEAR) against the N6 pre-registered rule. If drift-haircut mirror-exit EV is ≤ 0 across all clusters, **retire live copy-entry** and every [COPY]-entry item (C5, A4, P8, and half of C3) collapses into "wallet graph as feature-generator for the resolution engine" — freeing all that effort for [AUTO] items on the one edge that works.

---

## 6. How we'll know it's working

**Tier 0 — operator/capital health (the highest-Sharpe metrics; watch daily):**
- **Uptime %** and **max stale-interval** for host/poller/balance-API → target zero multi-hour gaps, alert fires within minutes.
- **Deployed-capital %** (target: move from ~27% toward 60–70%), **idle-USDC**, **unredeemed-$** → trapped capital trending to zero.
- **Days-since-reconciliation** and **ledger-agreement streak** → G1 gate stays green.

**Tier 1 — instrument honesty (proves the loop is self-improving, not self-deceiving):**
- **Held-out (not in-sample) Brier** on the two live edges, benchmarked against the **null model = entry mid-price as P(win)**. If calibrated confidence can't beat the raw market price out-of-sample, the alpha claim is suspect.
- **Event-clustered effective-n** per slice (not fill-count) → the real sample size behind every promotion.
- **Shuffle-test pass rate** and **FDR-adjusted survivor count** from the harness → is it finding edge or manufacturing overfit.

**Tier 2 — the edge itself, net of cost:**
- **`resolution_decay` net-of-cost EV per trade** with an event-clustered CI band, trending flat-or-up (regime monitor C4).
- **`whale_entry_filtered` reconciled 90d P&L** holding ≥ +$21 with a stable CI.
- **Realized-vs-modeled slippage** per fill (P6) → is execution cost what the gate assumes.
- **Fast-lane hit-rate for *filtered* signals** AND the share of converted fast-lane fills that clear the EV gate (P3).

**Tier 3 — the strategic verdicts (the quarter's real scorecard):**
- **Copy kill-rule outcome:** did C1+C2 find a net-of-cost-positive copyable cohort? A clean "no" that retires copy-entry is a *success*, not a failure.
- **Did we promote *anything*?** At $300 the honest near-term goal is statistical power, not P&L: did we accrue enough clean event-clustered n to promote a single lane past Tier 1 under the honest (N4/N5/G2) gates? If not, the answer is *concentrate harder on `resolution_decay`*, not add lanes.
- **Number of live signals:** trending **down** toward the resolution-engine thesis (features feeding one engine), not up. At this bankroll, a shrinking signal count is a sign of health.

---

*Bottom line: the roadmap's polite even-handedness across seven dimensions was itself the strategic error. Concentrate on `resolution_decay` as the whole thesis, treat uptime and capital-hygiene as your top alpha, make your instruments honest before you trust any number, and pre-commit to killing copy-entry if the one measurement that matters comes back negative.*

---

# Appendix: Completeness Critic Memo (gaps analysis)

What's missing is not another idea inside a dimension — it's the honest reckoning that the whole roadmap is a very well-engineered answer to a question the evidence has already closed. Here is what none of the seven dimensions will say to your face.

## 1. The roadmap refuses to state its own null result

The single most important sentence in the entire critique is buried in the copy-trading "missed" list: *"If drift-haircut mirror-exit EV is still ≤0 across all clusters, then EVERY entry-copy proposal here is polishing a strategy with no edge."* Every dimension proposes build paths; not one proposes a **kill-decision protocol**. That is the missing spine.

You already have three independent pieces of evidence pointing the same direction: zero wallets copyable hold-to-resolution (best −$0.058/trade ex-top-3), Brier 0.32 (calibration worse than a coin), and only ONE signal with positive reconciled history. A great autonomous trader treats this as a Bayesian prior, not a to-do list. The roadmap should open with a **pre-registered stopping rule**: "We run harness #1 + latency-haircut #7 as one read-only diagnostic. If the top-decile copyable cohort is still ≤0 net-of-cost, live copy-entry is formally retired within 30 days and the wallet graph survives only as a feature source." Writing that sentence down *before* you see the number is the difference between a research program and a sunk-cost machine. The roadmap's fatal tell is that ~35 of ~50 proposals are conditionally worthless on one measurement nobody has scheduled first.

## 2. Everyone measures signals; nobody measures the operator (you)

There is a person in this loop making the highest-variance decisions in the system, and the roadmap has zero instrumentation on him. MEMORY is a graveyard of operator-caused losses that dwarf any signal edge: the host was off ~5.7 days with no alert (6/15), the breaker tripped on back-booked May losses halting live since 7/3, equity snapshot froze at $5.43 for 20 runs, 11 trapped unredeemed positions, a stale-balance API that ran for 20 cycles before anyone noticed. **The biggest negative-EV "strategy" in this system is downtime and unreconciled state, not bad signals.** whale_entry_filtered earns +$21/90d; a single 5.7-day outage on a book that's supposed to be capturing near-resolution longshots plausibly costs more than that in missed resolution_decay cycles. No dimension proposes an **uptime/reconciliation SLA with its own kill-and-alert path** — a dead-man's switch that screams when the host, the poller, or the balance API goes stale. That is a higher-Sharpe intervention than any new alpha, and it's absent because "operational reliability" fell in the cracks between "autonomy guardrails" and "data infra."

## 3. The strategic pivot nobody will name: you are drifting toward being a resolution engine, so become one on purpose

Look at what actually works versus what the platform is *built* to do. The platform is built for copy-trading + broad autonomous signals. What survives contact with reconciled reality is **resolution_decay** — betting on near-certain outcomes near resolution on weather/sports longshots. That is not a copy-trading system. That is a **resolution-truth / late-stage-mispricing engine**, and it's winning precisely because it doesn't depend on anyone else being smart, only on the market being slow to mark a near-certain outcome to 0 or 1.

The provocative reframe: **stop treating resolution_decay as one signal among thirty and treat "we are the fastest, best-calibrated resolution engine on slow-resolving Polymarket markets" as the entire company.** The copy graph, the whale tracking, the cross-market dutch-book idea, the smart-money positioning — none of them are P&L lines in this world. They are all *features that sharpen the resolution engine*: "is smart money net-long this near-resolution market" (already flagged in the copy missed-list), "have 2+ tier-1 wallets exited" (whale_exit_detector's cluster primitive, currently wasted on a paper contrarian signal), "does the cross-market implication confirm this outcome is near-certain." Every dimension is trying to resurrect copy trading as its own book. The evidence says the winning move is to **demote all of it to feature-generators for the one engine that works** and pour the harness, the latency lane, and the calibration loop into *that*. The roadmap under-weights this by ~10x: it's scattered across four "missed" bullets in three dimensions instead of being the thesis.

## 4. Second-order effect the harness will cause: an industrial p-hacking machine with no governance

The Alpha "missed" list names this (FDR budget), but it's filed as one bullet when it should be a hard governance gate spanning #1, #7, and every slice promoter. Think through the second-order dynamic: the moment harness #1 exists, you can test thousands of predicates in minutes. Combined with self-tuning slices (#7) feeding their own live trades back into their own multipliers, you have a system that **manufactures beautiful dead signals faster than live cycles ever could**, and then *sizes into them*. On a $300 book with n=1–3 per slice, this is not a tail risk — it's the default outcome. The roadmap treats the harness as an unalloyed good ("replace months of live cycles with a minutes-long replay") without pricing in that a fast search over noisy history with no deflated-Sharpe / minimum-track-record gate is a machine for finding overfit. The harness needs a **built-in adversary**: every candidate signal must clear a purged, embargoed walk-forward *and* a multiple-testing correction sized to how many predicates were tried, or the harness is a liability, not an asset.

## 5. The bankroll is the actual constraint, and half the roadmap is denial about it

Read the critiques honestly and a pattern screams: proposal after proposal is scored down to "no-op today," "narrow to one lane," "needs 10-50x volume," "only matters at 5-10x bankroll." Per-lane ladders starve forever. Kelly's window is $1 wide. Slices have n=1–3. Crowding curves can't be fit. Market-making is rounding error. **At $300 with $1–5 stakes, most of these ideas are structurally un-validatable — not wrong, just un-testable at this size.** The roadmap never confronts the fork this creates:

- **Either** the honest near-term goal is *not profit but statistical power* — accept that $300 is a data-collection instrument, deliberately trade the widest-coverage discovery lane to accrue reconciled samples fastest, and judge the next quarter on "did we earn enough clean n to promote *anything*," not on P&L.
- **Or** the pivot is to **concentrate the whole bankroll behind the one proven edge** (resolution_decay), stop diluting attention across thirty signals and a dead copy book, and let it compound — accepting far less diversification in exchange for actually clearing venue-minimum economics.

You cannot do both, and the roadmap implicitly tries to, which is why everything is "medium, later, once bankroll scales." Nobody states that **at this size, diversification is the enemy**: every extra lane splits an already-too-small sample and pushes every ladder toward never promoting. A great small-bankroll trader over-concentrates on the one thing with an edge and treats everything else as unfunded research. The roadmap's polite even-handedness across seven dimensions is itself the strategic error.

## 6. The highest-ROI single thing, and it's not in any dimension as a headline

Rank by expected-dollars-per-effort against *this* book, and the winner isn't alpha, execution, or ML. It's the **redeem sweep + capital-efficiency instrumentation** (buried as an S-effort risk item) *fused with* the **operator/uptime SLA** (#2 above). Eleven trapped positions and a book deploying $80 of $300 means **60–70% of your capital is either idle or locked** — you could double deployable capital this week with zero new alpha and zero new risk. Freeing $30–50 of trapped capital is a 10–15% deployable increase; ending multi-day outages recovers missed cycles worth more than your only positive signal earns. That is a larger, safer, more certain P&L delta than harness #1's best hypothetical signal, and it's sitting in the roadmap as a scored-71 S-effort afterthought while an XL market-making proposal got a full slot. **The roadmap systematically over-weights new-edge discovery and under-weights not-bleeding-the-edge-you-have** — losing your one working signal to an outage, or leaving your capital trapped, costs more than any marginal new signal gains.

## The one-line version

The roadmap is a superb answer to "how do we build more?" when the evidence is demanding an answer to "should we still be copy-trading at all, and can we even measure anything at $300?" Missing: a pre-registered kill rule for copy-entry, an operator/uptime SLA treated as alpha, the explicit pivot to *resolution engine as the whole thesis*, an FDR governance gate on the harness before it p-hacks you, and the admission that at $300 diversification is the enemy and capital-hygiene beats new-signal discovery. Everything else is polishing rungs on a ladder that, per your own reconciled data, currently leads to −$0.058 a trade.
