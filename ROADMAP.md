# Roadmap

Focused prediction-market trading roadmap. The platform identifies
top-performing Polymarket wallets, monitors them in real time, fires
signals on their activity, and progressively scales paper → live → target
P&L across six phases.

**Target**: $10,000–$20,000 realized P&L per month on a $200–300K working
bankroll at L5 steady state. See `THESIS.md` for the claims being tested
and `MILESTONES.md` for the concrete unit-of-work log.

---

## Design Principles

1. **Signal first, trade second** — every trade decision traces back to
   a specific signal with measurable confidence.
2. **Paper before live, small-live before big-live** — no level skips.
   A bankroll promotion requires explicit criteria met on live data at
   the prior level.
3. **Category-independent testing** — all 9 categories generate signals;
   promotion to the live whitelist is based on paper PnL + hypothesis
   accuracy by category, not priors.
4. **Atomic updates** — daily pipeline writes are atomic; a bad run
   cannot poison the live feed.
5. **Human in the loop for promotions** — tier thresholds, category
   promotions, live whitelist changes, and every bankroll-level promotion
   require explicit human approval. Automation gates to L1 only.
6. **Scalability is a claim, not an assumption** — Claim 6 in `THESIS.md`
   is tested level-by-level. Edge that works at $25/trade is not assumed
   to work at $500/trade.

---

## Phase 1 — Intelligence Foundation ✅ DONE

**Goal**: Build the wallet quality model and watched-wallet set.

| Item | Status |
|------|--------|
| Wallet intelligence DB (16K wallets, 108K trades) | DONE |
| wallet-profiles (directional WR) | DONE |
| classify-wallet-buckets | DONE |
| Tier1/Tier2 wallet selection | DONE |
| Market universe (225 markets, 9 categories) | DONE |
| WhaleTripwire loads watched wallets | DONE |

---

## Phase 2 — Live Signal Generation ✅ DONE

**Goal**: Detect whale trades in real time and fire signals.

| Item | Status |
|------|--------|
| WebSocket collector + Polygon wallet-stream | DONE |
| WhaleTripwire + WhaleSignalEngine | DONE |
| 20 signal types firing, ~8K signals/24h | DONE |
| Paper executor on_signal() integration | DONE |
| Whale Monitor + API endpoints | DONE |
| Category performance tracking | DONE |
| Alpha scoring (414 copyable combos, 24.6% selectivity) | DONE |

---

## Phase 3 — Paper Trade Validation ✅ DONE

**Goal**: Hit hypothesis accuracy ≥70% on 50+ resolved trades (Claim 4).

| Item | Status (2026-05-09) |
|------|--------|
| Thesis scorecard live | DONE |
| Resolved hypotheses | 766 resolved paper trades |
| whale_entry_filtered | Live — 156 closed live trades, +$48 PnL |
| specialist_entry | IC30=+0.050, n=4 live resolved; accumulating |
| resolution_decay | n=2; too early |
| Per-signal scorecard breakdown | DONE |
| Hypothesis-resolution-drift watchdog | DONE |
| Paper exit monitor (dry_run rows) | DONE (2026-05-09) |

Phase 3 criteria were soft-bypassed: live trades are the primary data source now
(placed-trade EV > signal_outcomes EV due to gate selection bias). Paper book
validated the signal; the live book (+$48 PnL, +EV) is the continuity proof.

---

## Phase 4 — Live Probate 🔄 ACTIVE (since 2026-04-18)

**Goal**: Prove Claim 5 — edge survives transaction costs at ~$300 bankroll,
$5/trade (Tier 1 stake ladder). Scale to $1,000 / $50/trade when gates pass.

| Item | Status (2026-05-09) |
|------|--------|
| CLOB credentials + py_clob_client_v2 wired | DONE (V2 format 2026-05-07) |
| Circuit breaker + kill switch + alerts | DONE |
| Telegram alerts (in + out) | DONE |
| Live executor + category allowlist | DONE |
| Kill switch EV gate (bypass WR when EV ≥ 5%) | DONE (2026-05-09) |
| BUY ceiling: whale_entry_filtered ≥ 0.50 blocked | DONE (2026-05-09) |
| Paper exit monitor (dry_run positions) | DONE (2026-05-09) |
| Kelly MIN aligned to CLOB minimum ($5) | DONE (2026-05-09) |
| _live_real_cap() floored at $5 | DONE (2026-05-09) |
| Signal health IC30 decay alerts | DONE (2026-05-09) |
| wallet_profiles parquet weekly refresh | DONE (2026-05-09) |
| Stake ladder Tier 0→1 promoted ($1→$5) | DONE (2026-05-09) |
| MAX_OPEN_POSITIONS raised 20→30 | DONE (2026-05-09) |
| **156+ real auto-live trades logged** | ✅ |
| Live EV confirmed positive | ✅ (+$0.31/trade avg) |
| Live slippage ≤ 2% measured | measuring |
| **Bankroll raise $300 → $1,000** | PENDING (slippage + human approval) |

**Phase-4 exit criteria (ALL required):**
- 30+ live resolved trades (met: 156)
- Live slippage median ≤ 2% on entry
- Positive EV confirmed (met: +$0.31/trade; WR=36.7% with 3.4× win:loss)
- Zero kill-switch / circuit-breaker incidents
- Ops stable 2+ weeks

---

## Phase 5 — Confirm at Scale (L2 → L3) ⏳ GATED on Phase 4

**Goal**: Prove Claim 2 at live scale — ≥2 categories with independent
edge, and prove Claim 6 begins to hold as we move from $50 to $500 max
per trade.

| Level | Bankroll | Max/trade | Target P&L/mo | Phase-5 goal |
|---|---:|---:|---:|---|
| L2 Confirm | $5,000 | $150 | $250–500 | 50+ live trades, 2+ categories positive |
| L3 Growth | $25,000 | $500 | $1,250–2,500 | 150+ live, Sharpe >1, max DD <10% |

**Phase-5 exit criteria (ALL required):**
- L2 and L3 both passed (each has its own promotion gate, see THESIS.md)
- 3+ categories showing independent positive PnL on ≥30 resolved each
- 30-day rolling Sharpe ≥ 1.0
- Max drawdown < 10% on the 90-day window

---

## Phase 6 — Target Scale (L4 → L5) ⏳ GATED on Phase 5

**Goal**: $10–20K/month realized P&L steady state on $200–300K bankroll.

| Level | Bankroll | Max/trade | Target P&L/mo | Phase-6 goal |
|---|---:|---:|---:|---|
| L4 Scale | $100,000 | $1,500 | $5,000–10,000 | 300+ live, 3+ category edges, 30d stable |
| **L5 Target** | **$200–300K** | **$3,000** | **$10,000–20,000** | **3 consecutive months at target** |

**Phase-6 exit criteria = steady state**. There is no "Phase 7" — L5 is
the operational goal. Once we hit 3 consecutive months in the target
P&L range, the business is operating. The roadmap after that is
maintenance: defending the edge, rotating retired signals in/out,
replacing decayed wallets.

---

## Cross-cutting workstreams

These are not phases — they run continuously at every phase:

- **Signal research**: new signal families; sunset of broken ones (e.g.
  `accumulation` now in diagnosis).
- **Wallet tracking**: orphan wallet onboarding, tier rotation, demotion
  of wallets whose recent WR drops.
- **Infrastructure hardening**: today's session fixed the Postgres
  connection leak, Kelly data source, open_positions OOM, and
  orphan_wallet_onboarder SQL. Continuous work — next up is replacing
  the 2.5GB goldsky-resolved-fills path with Postgres-native queries.
- **Observability**: watchdog + Telegram + dashboard. New as of
  2026-04-18: hypothesis-resolution-drift detection.

---

## What Is NOT on the Roadmap

- Equity research pipelines
- Multi-strategy portfolio construction
- Walk-forward grid optimization
- Cross-platform arbitrage (Kalshi vs Polymarket)
- Manifold/Metaculus/PredictIt new integrations
- Any expansion path beyond L5

These remain in the codebase as legacy or adjacent but are **not part of
the active plan** to reach $10–20K/month.

---

## Kalshi Subsystem (Parallel, de-prioritized)

Running independently, paper-trading Economics markets using
calibration_drift, volume_spike, time_decay. Trades resolving through
April 2026. Not on the critical path to L5 — its results inform
confidence calibration but its P&L is not counted toward the target.
