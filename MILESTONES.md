# Milestones

Unit-of-work log. For **what** we are trying to prove and **how** we
scale to $10–20K/month, see `THESIS.md` and `ROADMAP.md` (updated:
`ROADMAP_2026-04-24.md`).

---

## Session 2026-04-24 / 2026-04-25 — recovery + structural rebuild

The week-long unattended run (started 2026-04-18) failed at +6h on a
silent Postgres idle-disconnect. This two-day session restored the
system, then rebuilt missing layers across reliability, wallet
intelligence, alpha discovery, and observability.

**Reliability:**
- Pool keepalives + `max_lifetime` + `check_connection` (root-cause fix
  for the freeze)
- Escalating scheduler-failure alerts (1/5/25 + recovery)
- Scheduler `_load_state()` — timers persist across container restart
- `setup_logging("live_collect")` wired at the CLI entry point
  (was dropping every gate marker silently)

**Throughput / gate stack:**
- Removed the hard `_is_sports_market` block at the signal-engine level
- Added classifier-level sports fallback (wallet_derived → sports)
- Aligned paper FAV_GATE 0.50 → 0.65 (matched live)
- `LIVE_SIGNAL_TYPES` widened from `{whale_entry_filtered}` to
  `{whale_entry_filtered, wallet_reversal, cascade}`
- Per-(signal_type, side) gate excluding `cascade NO`,
  `wallet_reversal NO`, `accumulation NO`
- `consensus_follower` added to `EXCLUDE_SIGNAL_TYPES`
- Structured `[KS_BLOCK:*]` logging on every kill-switch rejection

**Sizing:**
- `KellySizer` reads `signal_calibration.kelly_fraction` (was static 0.25)
- `STAKE_MULTIPLIERS` for proven (signal, side) winners + long-shot YES
- `wallet_earliness` boost (recent vs lifetime WR delta)

**Wallet detection layer (new):**
- `wallet_behavior_metrics.py`: bootstrap-CI on per-trade ROI,
  per-(wallet, category) z-score, bankroll-relative sizing distribution,
  sybil/wash detection, k=5 strategy clusters
- First run: 1,151 wallets profiled, 148 pass `is_copyable_ci`,
  101 flagged `is_likely_farmer`, 3,369 z-scores written
- New gates wired into paper executor: `[FARMER_GATE]` (block),
  `[Z_SPECIALIST]` (×1.4), `[CONVICTION]` (×1.15), `[MECHANICAL]` (×0.85)
- PM leaderboard sync auto-backfills top-10 newly-seeded whales
  (closes the 15-mega-whale-with-zero-trades blind spot)

**Observability:**
- `/api/system/readiness` — single-shot READY/DEGRADED/NOT_READY
- `/api/live/funnel` — per-gate aggregate counts
- Watchdog reads `consecutive_failures` from `state.json`
- Daily synthetic test trade (`scripts/synthetic_test_trade.py`)

**Discovery / research:**
- `hypothesis_tracker` table + `upsert/list_all` helpers
- `reports/alpha_discovery_onramp_2026-04-24.md` — 6-stage promotion
  ladder + queue of 5 next hypotheses

**Documents written/updated:**
- `reports/weekly_review_2026-04-24.md`
- `reports/alpha_audit_2026-04-24.md`
- `reports/path_to_consistent_live_2026-04-24.md`
- `reports/alpha_discovery_onramp_2026-04-24.md`
- `reports/alpha_pipeline_evaluation_2026-04-25.md`
- `reports/wallet_detection_audit_2026-04-25.md`
- `ROADMAP_2026-04-24.md` — operational roadmap
- `README.md`, `ARCHITECTURE.md`, `MILESTONES.md` (this), `THESIS.md`

**Detection coverage shift:**
- Top whale tier: 77% → 95%
- 0 → 148 wallets passing bootstrap-CI copyability
- 0 → 101 farmers flagged and gated
- +17,594 new trade rows from previously-blind PM whales

---

## Completed

### Phase 1 — Intelligence Foundation
- **Wallet intelligence DB** — 16,259 wallets, 108,845 trades, 31K resolved.
- **wallet-profiles --from-db** — directional WR over 312 wallets.
- **Wallet bucket classification** — 11 behavioral buckets.
- **Market universe** — 226 markets across 9 categories from Gamma API.
- **WhaleTripwire** — 76 watched wallets loaded from wallet_profiles.

### Phase 2 — Live Signal Generation
- **Polymarket WebSocket collector** — 225 markets, live tick ingestion.
- **Polygon wallet-stream** — chain-level Transfer event monitoring, ~2–5s latency.
- **WhaleSignalEngine** — 20 signal types firing, ~8K/24h.
- **Paper executor** — Kelly sizing, portfolio tracking, resolution checking.
- **Whale Monitor GUI** + API endpoints — full dashboard coverage.
- **Alpha scoring** — 1,686 wallet×category combos, 414 copyable (24.6% selectivity).

### Research & Infrastructure
- **Signal Research Lab** — 8 signal families validated on Manifold backtests.
- **Kalshi live candle collector** — hourly OHLCV for 213 Economics markets.
- **Research replay framework** — registry-backed evaluation, cross-provider compare.
- **Test suite** — 1,817 passing tests.
- **Postgres migration** — moved from SQLite; MVCC eliminates disk-image corruption.

### 2026-04-18 Session (System Hardening + Scorecard Rescue)
- **Postgres connection leak fix** — `PolymarketPaperExecutor` + `WalletDB`
  now context-manager + GC safe; added `WalletDB.default_path()` static.
  Pool was exhausting every ~20h; now stable under 90-req stress test.
- **Scheduler mem bump** — 512m → 2g; `open_positions` task disabled
  (legacy, 2.5GB goldsky dataset, 13 days stale).
- **`orphan_wallet_onboarder` SQL fix** — `HAVING n` → `HAVING COUNT(*)`.
- **Kelly sizer data-source bug** — fallback from `signal_outcomes` to
  `polymarket_paper_trades` was dead code under Postgres; now active.
  `whale_entry_filtered` went from Kelly=$0 to Kelly=$10.
- **Live executor category reclassification** — mirrors paper path; sports
  markets tagged "other" now correctly pass the allowlist.
- **`LIVE_TRADE_CATEGORIES` expanded** — added `sports` (+$965 paper PnL)
  and `crypto` on data-driven grounds.
- **`accumulation` removed from `LIVE_SIGNAL_TYPES`** — 0/6 correct on the
  thesis scorecard, would have fired real money on a broken signal.
- **Thesis scorecard rescued** — 17 closed paper trades had orphan
  hypothesis rows with `actual_outcome=NULL`. Backfilled; scorecard is
  now live and reading 47.1% accuracy (PRELIMINARY, below 70% target).
- **Hypothesis-resolution-drift watchdog** — alerts if closed paper
  trades >1h old have unresolved matching hypotheses.
- **Telegram 2-way** — commands `/status`, `/positions`, `/readiness`,
  `/funnel`, `/insiders`, `/kill`, `/unkill` verified working.

---

## In Progress (Phase 3)

- **Hypothesis accumulation** — 17 of 50 resolved. Need 33 more to reach
  decision threshold. At current paper cadence (~25–50 closures/day),
  ETA 2–4 days.
- **Signal quality triage** — `whale_entry` validates at 70% on 10
  resolved. `accumulation` at 0% on 6 — investigate why (wrong
  direction? low-alpha wallets? wrong-side-of-whale?). Other signal
  types awaiting sample size.
- **Second category with edge** — currently sports-only by PnL.
  Diversification is a Phase-3 exit criterion, not just a nice-to-have.

---

## Next (gated on Phase 3 → 4 transition)

### Phase 4 — Live Probate (L1, $1,000 bankroll)
- First real auto-live fire via `whale_entry_filtered` in a whitelisted
  category (expected any time now post-fixes).
- Measure live slippage vs paper expectations.
- Raise `POLYMARKET_LIVE_BANKROLL_USD` 345 → 1,000 after 10 clean live trades.

### Phase 5 — Confirm + Growth (L2→L3, $5K → $25K)
- 2-category diversification on live PnL.
- Adjust max open positions upward with bankroll (10 → 15).
- 30-day rolling Sharpe tracking on live fills only.

### Phase 6 — Target Scale (L4→L5, $100K → $200–300K)
- $10–20K/month realized P&L.
- 3-category diversification minimum.
- Replacement-signal pipeline (as older signals decay).

---

## Sunset / Deprecated

- Manifold Markets parser — backtest only.
- Metaculus integration — backtest + divergence concept only.
- PredictIt parser — historical only.
- `wallet_profiler.py` (CSV path) — replaced by `wallet_profile_rebuild.py`.
- `open_positions` scheduler task — disabled 2026-04-18, needs a
  Postgres-native rewrite before re-enabling.
- `accumulation` signal on the live whitelist — paper-only pending
  diagnosis; re-add only when paper accuracy ≥55% on 20+ resolved.

---

## Go-Live Criteria (L0 → L1 promotion)

| Gate | Requirement | Current |
|------|------------|---------|
| 1 | ≥50 resolved hypotheses | **17** (34%) |
| 2 | ≥70% hypothesis accuracy | **47.1%** |
| 3 | ≥2 signal types at ≥60% on ≥20 resolved each | whale_entry 70%/10 only |
| 4 | ≥2 categories positive PnL on ≥20 resolved each | sports only |
| 5 | Max drawdown <20% over 30 days | 0.7% ✅ |
| 6 | Ops stable 2+ weeks (no data-loss, no silent failures) | Post-fix monitoring |
| 7 | Human review and approval | Pending gates 1–4 |

All seven gates must pass before bankroll is raised from $345 to $1,000.
Live execution with the current $345 bankroll continues on the narrow
whitelist (`whale_entry_filtered` only, 4 categories) as data-gathering.
