# Roadmap

Focused prediction market trading roadmap. The platform identifies top-performing Polymarket wallets, monitors them in real time, fires signals on whale activity, and paper trades toward live execution.

---

## Design Principles

1. **Signal first, trade second** — every trade decision traces back to a specific signal with measurable confidence.
2. **Paper before live** — no real capital until paper performance is validated through explicit gates.
3. **Category-independent testing** — all 9 categories generate signals; promotion based on outcomes, not priors.
4. **Atomic updates** — daily pipeline writes are atomic; a bad run cannot poison the live feed.
5. **Human in the loop** — tier thresholds, category promotions, and live execution require human approval.

---

## Phase 1 — Intelligence Foundation (Current)

**Goal**: Build the wallet quality model and watched wallet set.

| Item | Status |
|------|--------|
| Wallet intelligence DB (16K wallets, 108K trades) | DONE |
| wallet-profiles --from-db (directional WR) | DONE |
| classify-wallet-buckets | DONE |
| Tier1/Tier2 wallet selection | DONE |
| Market universe (225 markets, 9 categories) | DONE |
| WhaleTripwire loads watched wallets | DONE |

---

## Phase 2 — Live Signal Generation (In Progress)

**Goal**: Detect whale trades in real time and fire signals.

| Item | Status |
|------|--------|
| WebSocket expanded to 225 markets | DONE |
| WhaleTripwire integrated into live collector | DONE |
| WhaleSignalEngine (whale_entry + convergence) | DONE |
| Paper executor on_signal() integration | DONE |
| ws_status.json health file | DONE |
| Whale Monitor GUI page | DONE |
| API endpoints (whale-feed, signals-feed, subscription-status, category-performance) | DONE |
| Category performance tracking | DONE |

---

## Phase 3 — Paper Trade Validation (1-2 months)

**Goal**: Accumulate 50+ resolved trades per category and validate signal edge.

| Item | Status |
|------|--------|
| 50+ resolved trades per category | PLANNED |
| Category promotion/demotion based on paper outcomes | PLANNED |
| Rolling 20-trade wallet quality monitoring | PLANNED |
| Wallet demotion on rolling WR < 0.45 | PLANNED |
| Leaderboard with atomic versioning | PLANNED |
| Paper trade resolution by category dashboard | PLANNED |

---

## Phase 4 — Automation and Reliability (2-3 months)

**Goal**: Run both processes unattended with health monitoring.

| Item | Status |
|------|--------|
| Scheduled daily intelligence pipeline | PLANNED |
| Continuous monitor with auto-restart | PLANNED |
| Process health monitoring + dead man's switch | PLANNED |
| Alerting on pipeline failures | PLANNED |
| Performance review automation (advisory, not acting) | PLANNED |

---

## Phase 5 — Live Execution (3-4 months, gated)

**Goal**: Deploy real capital on validated categories.

| Gate | Requirement | Status |
|------|------------|--------|
| 1 | 50+ paper trades resolved > 55% WR | Pending |
| 2 | 2+ categories with independent edge | Pending |
| 3 | Max drawdown < 20% over 30 days | Pending |
| 4 | Human review and approval | Pending |
| 5 | $500 initial capital, 1 category | Pending |

All gates must pass before any real money is deployed. Live execution requires an explicit human command — no automation path to live.

---

## Kalshi Subsystem (Parallel)

Running independently. Paper trades Economics markets using calibration_drift, volume_spike, and time_decay signals. Trades resolving April 2026. Results feed into confidence calibration for the broader system.

---

## What Is NOT on the Roadmap

- Equity research pipelines
- Multi-strategy portfolio construction
- Walk-forward grid optimization
- Cross-platform arbitrage (Kalshi vs Polymarket)
- Manifold/Metaculus/PredictIt new integrations

These are legacy infrastructure from a broader vision. They remain in the codebase but are not part of the active development plan.
