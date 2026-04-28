# How to Scale the System Faster — L0→L5 Acceleration Roadmap
*2026-04-27*

The default L0→L5 timeline (per `ROADMAP.md`) was **5-6 months** assuming 5-day promotion windows per ladder rung. This document is the playbook to compress that to **8-10 weeks** without skipping safety gates.

## Where time is actually spent

| Constraint | Days lost / month | Why it's the binding factor |
|---|---:|---|
| **Hypothesis-resolution latency** | ~10 | Markets resolve in clumps. n=50 takes 5-10 days even at high signal volume |
| **Calibration convergence** | ~7 | Brier needs ~30+ resolved hypotheses post-correction to verify |
| **Live-trade slippage measurement** | ~5 | Need 10+ live fills to compute median slippage |
| **Per-level WR validation** | ~5 | Each ladder rung wants ≥10 trades at the new bankroll level |
| **External capital deposits** | ~2 | Manual; depends on how fast new tier is funded |

**Total: ~29 days "wasted" per ladder transition** — most is structural (markets resolve when they resolve), some is engineerable.

## The engineerable axes — ordered by leverage

### 1. Increase resolved-trade throughput (highest leverage)

The bottleneck isn't signal generation (~12,000/day) — it's **resolved hypothesis count** (28-83/day). More resolved hypotheses = faster calibration verification = faster ladder gates clear.

**Levers:**
- **Bias toward fast-resolving markets** — add `MAX_HOURS_TO_RESOLVE` filter at signal entry; prefer markets resolving in <72h. Currently we trade everything regardless of horizon. *Effort: 2h. Impact: 2-3× resolved-hypothesis density.*
- **Enable Phase B `resolution_decay` more aggressively** — currently 1-3 paper fires/day; the signal targets <24h-resolution markets specifically. Increase candidate pool by widening price band from $0.10-$0.30 → $0.05-$0.40. *Effort: 30m. Impact: 5-10× Phase B trade volume, all fast-resolving.*
- **Backfill resolved-market data more aggressively** — `enrich_trade_resolution` weekly is too slow. Run daily with chunk-resume. *Effort: 1h. Impact: 30-50% more historical resolved coverage = better calibration ground truth.*

### 2. Compress ladder gate-pass requirements with Bayesian early-stopping

Current gates are point-thresholds: "≥50% accuracy on n≥50." This wastes data — if the cohort hits 60% accuracy on n=30, statistically we've already cleared the gate but we wait for n=50.

**Levers:**
- **Bayesian early-stop**: pass a gate when posterior P(true_acc ≥ threshold) > 0.95. *Effort: 4h. Impact: 30-50% faster gate clearance.*
- **Per-signal-type gates instead of aggregate**: pass L1 if any single signal type hits 60% on n≥20, not the aggregate hitting 50% on n≥50. Allows the best signals to graduate while marginal ones stay paper. *Effort: 3h. Impact: 2× faster L1 promotion possible.*

### 3. Parallelize ladder progressions per signal lane

Currently one bankroll across all signals. **Innovation:** run multiple bankroll lanes — the `whale_entry × sports` slice could be at L2 sizes ($150/trade) while a less-validated slice stays at L1 ($50/trade).

**Levers:**
- **Per-(signal_type, sub_domain) bankroll allocation**: each proven slice gets independent capital, sized to its own validated edge. *Effort: 8h. Impact: top slices reach L3-L4 size in months not quarters.*
- **Auto-promote at slice level**: when `whale_entry × sports` hits n≥10 / WR≥80%, auto-add to `STAKE_MULTIPLIERS` at 1.5× without waiting for global gate. *Effort: 2h. Impact: highest-EV slice never bottlenecked by less-proven slices.*

### 4. Multiply signal sources

Today we fire from one source (whale wallets) + one independent (Phase B `resolution_decay`). More uncorrelated alpha sources = faster aggregate evidence.

**Levers:**
- **Phase C — order-flow imbalance signals graduated to capital**: `order_flow_imbalance` already fires (informational only). Promote to paper-eligible if 30d IC > 0.1. *Effort: 2h. Impact: +1 lane.*
- **Phase D — cross-platform Kalshi divergence**: when PM and Kalshi quote the same event with ≥5pp price gap, take the cheap side. Skeleton shipped already; needs Kalshi rails finished. *Effort: 1-2 days. Impact: +1 lane independent of Polymarket alpha cycle.*
- **Phase E — momentum continuation**: prices that move ≥10pp in <24h before resolution often continue. Currently no signal. *Effort: 4h. Impact: +1 lane, fast-resolving by definition.*

### 5. Accelerate wallet discovery

Insider pool grew 82 → 472 in 5 days via `pm_leaderboard_sync` auto-promote. The next 1000 promising wallets aren't on the PM top-500 leaderboard but ARE in the `wallet_trades` table from signals firing.

**Levers:**
- **Discovery from signal_outcomes**: any wallet whose signal resolved profitably ≥3 times gets auto-evaluated for promotion. *Effort: 3h. Impact: 5-10× discovery rate.*
- **Sub-domain specialist auto-promotion**: any wallet hitting z≥1.5 in a sub-domain with positive PnL on n≥10 gets promoted to a `subdomain_specialist_leaderboard`. *Effort: 2h. Impact: targeted alpha by sport.*

### 6. Tighten cycle time on the data feedback loops

Five daily loops (calibration / signal-decay / wallet-quality / pool-discovery / behavior). They run once per day. Some can run every 6h with no cost.

**Levers:**
- **Move `isotonic_calibration` to 6h cadence**: more frequent refits → faster convergence to ground truth. Cost: ~30s extra compute / day. *Effort: 2 min config. Impact: 25% faster calibration response.*
- **Move `signal_health` to 6h cadence**: decay-flag latency 24h → 6h; less time spent firing decayed signals. *Effort: 2 min. Impact: catches signal decay 4× faster.*
- **`wallet_subdomain_metrics` to 12h cadence**: new specialists appear faster. *Effort: 2 min. Impact: 2× discovery responsiveness.*

## Recommended scale-up sequence (next 10 days)

| Day | Ship | Effect |
|---|---|---|
| 1 | Phase B price-band widening + max-hours-to-resolve filter | Resolved-hypothesis throughput 2-3× |
| 2 | Per-signal-type ladder gates (Bayesian early-stop) | First L1 candidate likely whale_entry-sports lane within week |
| 3 | Auto-promote slice-level STAKE_MULTIPLIERS | whale_entry×sports auto-amplified when n≥10 |
| 4 | Discovery from signal_outcomes (10× wallet pool) | Insider pool 472 → ~1500 in 30 days |
| 5 | Move calibration + signal_health to 6h cadence | Loops respond 4× faster |
| 6-8 | Phase C order-flow signal graduation + Phase E momentum | +2 alpha lanes |
| 9 | Per-(signal, subdomain) bankroll allocation | whale_entry×sports lane on independent capital |
| 10 | Verify: at least one slice at L1 capital ($50/trade live) | First live trade landing |

## Theoretical compressed timeline

| Step | Default | Compressed |
|---|---|---|
| L0 → L1 | 30-45 days | **7-10 days** (per-signal gates) |
| L1 → L2 | 30-45 days | **14-21 days** (capital + cohort grows) |
| L2 → L3 | 30-45 days | **21-30 days** (cross-validation slows) |
| L3 → L4 | 30-45 days | **30 days** (capital deposit cadence binding) |
| L4 → L5 | 30-45 days | **30 days** (same) |

**Total compressed: 100-130 days vs default 150-225 days.** Realistic best case: ~3.5 months to L5 instead of 6 months.

## What NOT to do for speed

- **Don't skip slippage measurement** — even with the corrected calibration, first-live-trade variance can blow up bankroll without empirical slippage data.
- **Don't bypass the synthetic test trade** — caught 3 schema bugs already; the 24h canary is cheap insurance.
- **Don't lift `MIN_RESOLVED_HARD` below 5** — Bayesian gating gives the same speed gain without the structural risk.
- **Don't compound STAKE_MULTIPLIERS across categories AND sub-domains AND lanes simultaneously** — soft-cap total stake at 7% of bankroll regardless of how many boosts a signal collects.
- **Don't enable Phase D (cross-platform) until L2** — Kalshi adds 2nd-platform variance; better to be calibrated on one platform first.

## Highest single-day-EV move

**Per-signal-type ladder gates** (item #2). Without it, whale_entry×sports (currently 6/6 100% WR / +$889 PnL) waits behind 12 other signals to graduate. With it, that one slice could be at L1 live capital within a week regardless of aggregate accuracy.

If we ship just one item from this doc tomorrow, it's that one. ~3h of work; potentially compresses time-to-first-real-PnL by 2-3 weeks.
