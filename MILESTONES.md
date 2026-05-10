# Milestones

Unit-of-work log. For **what** we are trying to prove and **how** we
scale to $10–20K/month, see `THESIS.md` and `ROADMAP.md` (updated:
`ROADMAP_2026-04-24.md`).

---

## Session 2026-05-09 — road to profitability: Phase 4 hardening

System evaluation and six targeted improvements to unlock live scale.
Live book: 156 closed trades, +$48.09 PnL. Stake ladder promoted Tier 0→1.

**Kill switch — EV gate bypass:**
- Added `EV_BYPASS_WR_THRESHOLD = 0.05` (5% EV floor)
- WR gate now skipped when blended EV ≥ 5%; unblocks asymmetric-payoff
  signals (whale_entry_filtered: 38% WR but 3.4× win:loss → +EV)
- Raised `MAX_OPEN_POSITIONS` 20 → 30 (30 × $5 = $150 = 50% of $300 bankroll)
- Widened `WR_FLOOR_OVERRIDES` to cover wallet_reversal, specialist_entry,
  resolution_decay at 0.35

**Entry price filter — whale_entry_filtered BUY ceiling:**
- `_BUY_ENTRY_CEILINGS = {"whale_entry_filtered": 0.50}` in live executor
- Blocks BUY at mid-market ≥ 0.50 (the 0.50–0.65 region was -$18 PnL vs
  the SELL-side +$44 PnL on the same signal)
- Live-price check added to catch price moves between signal fire and order

**Paper exit monitor:**
- `check_paper_exits()` in live_position_monitor.py processes dry_run=1 rows
- Resolves token_id via markets DB + Gamma API with in-pass cache
- Uses entry_price as effective fill; no SELL order placed (paper only)
- Wired into the monitor main() loop alongside check_live_exits()

**Stake ladder + sizing:**
- Promoted Tier 0 ($1) → Tier 1 ($5); real n=158, real_wr=0.367, PnL=+$49.48
- `KellySizer.MIN_TRADE_USD` lowered 10 → 5 to match CLOB minimum
- `_live_real_cap()` floored at $5.0 on all slice-multiplier branches
- EV-based promotion path: WR < 55% but positive avg PnL/trade now qualifies
- EV-based demotion exception: WR-based demotion blocked when EV > 0

**CLOB fill fix:**
- `avgFilledPrice=0` from live GTC orders treated as absent; fall back to
  current_price. Prevents $0 fills being recorded as valid execution price.
- Non-success CLOB statuses now emit descriptive error strings, not just "error"

**Signal health monitoring:**
- `signal_health.py` now sends Telegram alert on first IC30 decay transition
  (signal newly enters decay_flag=1) and when a monitored signal's IC30 first
  crosses below 0.02 warn threshold
- `_MONITORED_SIGNALS = {specialist_entry, whale_entry_filtered, resolution_decay}`
- Alert fires once per crossing, not every 6h run

**Infrastructure:**
- `wallet_profiles_parquet_export` weekly task: exports Postgres wallet_profiles
  → data/polymarket/wallet_profiles.parquet (was 34 days stale)
- Containers restarted to pick up all code changes

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

## In Progress (Phase 4 — Live Probate, active as of 2026-05-09)

- **Live book**: 156 closed trades, +$48.09 PnL, WR=36.7% (asymmetric payoff)
- **Bankroll**: ~$300, stake ladder Tier 1 ($5/trade), 30 max open positions
- **Slippage measurement**: ongoing — need median ≤2% on 30+ fills
- **specialist_entry signal**: IC30=+0.050, n=4 resolved; needs 30 to confirm edge
- **resolution_decay signal**: n=2 resolved; too early to evaluate
- **Next promotion gate**: raise bankroll to $1,000 after 30+ live trades with
  slippage ≤2% and no circuit-breaker incidents

## Next

### Phase 5 — Confirm + Growth (L2→L3, $5K → $25K)
- 2-category diversification on live PnL (sports + one other)
- Max open positions 30 → higher with bankroll
- 30-day rolling Sharpe tracking on live fills only

### Phase 6 — Target Scale (L4→L5, $100K → $200–300K)
- $10–20K/month realized P&L
- 3-category diversification minimum
- Replacement-signal pipeline (rotate in new signals as older ones decay)

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

## L1 → L2 Promotion Criteria (as of 2026-05-09)

Phase 4 (L1 Probate) is active. Current live stats vs L2 promotion gates:

| Gate | Requirement | Current (2026-05-09) |
|------|------------|---------|
| 1 | ≥30 live resolved trades | **156** ✅ |
| 2 | Slippage median ≤2% | measuring |
| 3 | Live WR ≥55% OR positive EV | EV=+$0.31/trade ✅ (WR=36.7%, asymmetric) |
| 4 | Zero circuit-breaker incidents | ✅ |
| 5 | No silent failures | ✅ |
| 6 | Human review + written approval | pending bankroll raise |

Bankroll raise $300 → $1,000 gated on slippage measurement and human approval.
