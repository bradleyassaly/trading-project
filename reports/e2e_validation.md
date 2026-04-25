# End-to-End Validation Audit

**Date:** 2026-04-07
**Mode:** Diagnostic only — no fixes applied
**Scope:** Full autonomous trading loop, raw data → execution → feedback

---

## Executive Summary

The autonomous trading loop is **broken at multiple critical handoffs** despite all
component code existing in source. The single largest gap is that the `api` Docker
container cannot open the SQLite wallet-intelligence database due to an NTFS bind-mount
incompatibility on WSL2, which cascades into **8 of 14 API endpoints returning HTTP 500**
and the wallet-intelligence layer being effectively dark to the frontend. A second
near-equal blocker is that all `trading-cli polymarket *` scheduled tasks fail with
`ModuleNotFoundError: sklearn`, so the daily refresh / wallet-profile / leaderboard /
position-sync pipeline has not run successfully since deployment. Net result: data
ingestion is fresh, signals are firing, paper trades are accumulating — but the feedback
loop from resolution → calibration → Kelly sizing → executor never closes.

**Loop status: NOT autonomous.** Open trades accumulate, but the system cannot learn
from outcomes because the calibration feedback handoffs are silently failing.

---

## Layer-by-Layer Findings

### Layer 0 — Infrastructure  🔴

| Item | Status | Notes |
|---|---|---|
| Docker compose up | ✅ | api, frontend, scheduler, watchdog all running |
| API health (/api/system/status) | ✅ | Returns Python constants only |
| Frontend (5173) | ✅ | Vite serving |
| Telegram alerter | ✅ | Bot reachable |
| **SQLite from container** | 🔴 | `unable to open database file` on `data/polymarket/wallet_intelligence.db` |
| **DB from host process** | ✅ | 587 MB, WAL mode, 16,259 wallet_profiles readable |

**🔴 L0-1 — Docker SQLite bind-mount failure.** Inside the `api` container,
`sqlite3.connect("data/polymarket/wallet_intelligence.db").execute("PRAGMA journal_mode")`
raises `unable to open database file`, even though the file is present at
`rwxrwxrwx`, 587 MB. Confirmed by direct `docker compose exec api python -c …`. The
cause is the well-known WSL2 NTFS bind-mount + SQLite WAL incompatibility — WAL needs
shared-memory mmap which Windows-NTFS-via-WSL2 cannot provide.
*Where:* `docker-compose.yml` data volume mount; `src/trading_platform/polymarket/wallet_db.py:270`.
*Fix (1–2 sentences):* Move the SQLite databases into a named Docker volume (or a
WSL2-native ext4 path) instead of bind-mounting from the Windows filesystem. Alternatively
disable WAL inside the container via `PRAGMA journal_mode=DELETE` for WSL2 environments.

**🔴 L0-2 — sklearn ImportError breaks all polymarket CLI tasks.**
`trading_platform/research/trade_ev_reliability.py:9` does
`from sklearn.ensemble import HistGradientBoostingClassifier`. sklearn is not in the
Docker image. Import chain: `cli → grouped_parser → paper.service → trade_ev_reliability`,
so every `trading-cli polymarket …` invocation aborts at import time. 6 of 12 scheduled
tasks fail this way.
*Where:* `src/trading_platform/research/trade_ev_reliability.py:9`; `pyproject.toml` deps.
*Fix:* Add `scikit-learn` to `pyproject.toml` dependencies, or move the sklearn import
inside the function that uses it so the module can load without it.

---

### Layer 1 — Data Ingestion  🔴 (mixed)

| Source | Status | Newest row | Notes |
|---|---|---|---|
| wallet_trades | 🟢 | 0.48 d old | 397,962 rows |
| categories | 🟢 | — | 0% null, 9% "other" (under 10% target) |
| **wallet_positions** | 🔴 | **34.6 h old** | refresh-positions task failing on sklearn |
| open_positions | 🔴 | stale | depends on refresh_positions |
| resolved markets | 🟢 | daily | fetch_resolutions runs OK manually |

**🔴 L1-D Position data stale (34.6h).** `refresh_positions` scheduled task is in
the sklearn-import failure set, so positions have not synced for >1 day. P&L and
open-position telemetry on the dashboard reflect yesterday's state.

---

### Layer 2 — Wallet Intelligence  🔴

| Table | Rows | Useful rows | Notes |
|---|---|---|---|
| wallet_profiles | 16,259 | **~2%** | win_rate populated on 2%, conviction>0 on 1%, net_pnl on 2%, wallet_bucket on **0%** |
| wallet_category_profiles | 1 | 1 | only `other / B` — should be ~13 signal types × buckets |
| wallet_tiers (dynamic S/A/B/C/D) | 0 | 0 | empty — tier engine never ran |

**🔴 L2-A wallet_profiles 98% empty.** The `wallet_profiles_rebuild` scheduled task
has been failing on the sklearn import since deployment, so the metrics columns
(win_rate, conviction, net_pnl, wallet_bucket) are NULL on the vast majority of rows.
Downstream consumers (signal engine, fusion gate, leaderboard) operate on a
near-empty intelligence base.
*Where:* `scripts/task_scheduler.py:120` (the task) → blocked by L0-2.
*Fix:* Resolve L0-2 then run `trading-cli data polymarket wallet-profiles --from-db`
once to backfill.

**🔴 L2-C wallet_category_profiles essentially empty.** Only 1 row exists despite
13 signal types × multiple buckets being expected. `build_all_profiles` only sees the
16 resolved paper trades, far below the threshold to write per-category aggregates.
*Where:* `polymarket/category_profiles.py` `build_all_profiles`.
*Fix:* Lower the minimum-sample threshold for early bootstrapping, or seed from
historical wallet_trades instead of paper resolutions.

---

### Layer 3 — Signal Generation  🟢 (with one 🔴)

| Item | Status | Notes |
|---|---|---|
| Signal engine running | ✅ | newest signal 2026-04-08 12:59:46 |
| Signal types active | ✅ | 5 of 13 firing (price_velocity, mm_flip, accumulation, oversized_bet, …) |
| price_velocity throughput | ✅ | 3,393 fired / 157 executed |
| **Fusion gate persistence** | 🔴 | 0 of 235 paper trades have `fusion_score` populated |
| **wallet_tier_at_fire** | 🔴 | NULL on every paper trade |

**🔴 L3-D Fusion gate not persisted.** The executor source contains the fusion
gate (`fusion_score`, `wallet_tier_at_fire`) but every observed `paper_trades` row
has these as NULL. Either the running container is stale (built before the fusion
gate code merged) or the INSERT statement does not include the new columns.
*Where:* `polymarket/polymarket_paper_executor.py` insert path.
*Fix:* Rebuild the api/scheduler images (`docker compose build --no-cache`) and
verify the columns are written. If still NULL, check the INSERT SQL includes the
new column names.

---

### Layer 4 — Paper Trade Execution  🔴

| Item | Status | Notes |
|---|---|---|
| Trades placed | ✅ | 235 paper trades over the run |
| **Stake sizing** | 🔴 | All FIXED constants per signal type, Kelly NOT used |
| price_velocity stake | — | $1,500 (constant) |
| mm_flip stake | — | $1,800 (constant) |
| accumulation stake | — | $1,800 (constant) |
| oversized_bet stake | — | $2,250 (constant) |
| Resolved trades | ⚠️ | 16 only (insufficient sample) |

**🔴 L4-A Kelly sizing not wired into executor.** The `BankrollAllocator` and
`get_stake_for(signal_type)` Kelly path exists in source, but the running paper
executor still pulls stakes from the static `SIGNAL_BANKROLL` constants. The
calibration → bankroll → executor handoff is therefore open-circuit.
*Where:* `polymarket/polymarket_paper_executor.py` size resolution; should call
`bankroll_allocator.get_stake_for(...)` instead of `SIGNAL_BANKROLL[signal_type]`.
*Fix:* Replace the constants lookup with the bankroll allocator call, falling back
to the constant only if the allocator returns 0.

---

### Layer 5 — Feedback Loop  🔴

| Handoff | Status | Notes |
|---|---|---|
| paper_trades → resolution | ✅ | check-resolutions task working |
| resolution → circuit breaker | 🔴 | `record_trade` never called; CB at peak=$100K, daily_pnl=$0 despite 16 resolved trades |
| resolution → signal_calibration | 🔴 | calibration says price_velocity stake $5K, executor uses $1,500 — divergence |
| calibration → BankrollAllocator | ✅ in source | not consumed by executor (see L4-A) |
| BankrollAllocator → executor | 🔴 | not wired (see L4-A) |

**🔴 L5-A Calibration / executor divergence.** `signal_calibration` reports
price_velocity allocated $79,400 stake $5K, but actual paper trades all use $1,500.
Calibration is being computed but ignored by the order pipeline.

**🔴 L5-D Circuit breaker never receives trade events.** Initialized at $100K,
peak=$100K, current=$100K, daily_pnl=$0 despite 16 resolved paper trades. Either
the resolver does not call `cb.record_trade(pnl)`, or the running container is
again stale relative to source.
*Where:* `polymarket/polymarket_paper_executor.py` `check_and_resolve_open_trades`
should call `CircuitBreaker(self._db_path).record_trade(realized_pnl)` per resolved
position.
*Fix:* Verify the call exists in source and rebuild the image; if missing, add the
hook in the resolution loop.

---

### Layer 6 — Live Execution Readiness  🟡

| Gate | Status | Notes |
|---|---|---|
| DRY_RUN flag | ✅ | True in source (correct) |
| KillSwitch sample-size gate | ⚠️ | only 16 resolved — gate would block live anyway |
| EV / win-rate gates | ⚠️ | dependent on near-empty wallet_profiles |
| Circuit breaker integration | ✅ in source | but never receives data (L5-D) |
| **/api/circuit-breaker/status** | 🔴 | returns **404** in running container |

**🔴 L6-A /api/circuit-breaker/status 404.** Endpoint exists in source
(`api/main.py`) and was added in the previous session, but is not registered in the
running container. Strongly suggests the api image was not rebuilt after the
circuit breaker work merged, which would also explain L3-D and L5-D.
*Fix:* `docker compose build --no-cache api && docker compose up -d api`.

---

### Layer 7 — Monitoring & Alerting  🟡

| Item | Status | Notes |
|---|---|---|
| Telegram alerter | ✅ | Bot Trading35838474bot reachable |
| health_watchdog | ⚠️ | running, but logged "api failing: Connection refused" + "db failing: disk I/O error" on first poll |
| scheduler state.json | ✅ | written, frontend can read it |
| Per-task log files | ✅ | logs/scheduler/*.log present |
| Failure → Telegram | ✅ in source | not yet observed firing |

**🟡 L7-C Watchdog DB I/O error.** Watchdog hits the same SQLite bind-mount problem
as L0-1 — symptom, not a separate bug.

---

### Layer 8 — Frontend  🔴 (cascading from L0/L2)

Endpoint health probe (14 endpoints):

| Status | Count | Endpoints |
|---|---|---|
| 200 | 5 | system/status, paper/bankroll, paper/pnl-history, system/execution-policy, system/scheduler-status |
| **500** | **8** | smart-money/universe-stats, signals/performance, live/readiness, calibration/status, tiers/summary, market-data/health, paper/positions-enriched, markets/top |
| **404** | **1** | circuit-breaker/status |

The 200s all return Python constants, not DB data. Every endpoint that goes through
`_get_wallet_db()` 500s because of L0-1. The Smart Money, Signal Lab, Live Readiness,
and Calibration pages are therefore non-functional in the GUI even though their
backend data exists on disk.

Page → endpoint dependency map (verified):

- Dashboard.jsx → []  (works)
- SmartMoney.jsx → smartMoneyAlerts, smartMoneyWinners  (broken — 500)
- SignalLab.jsx → paperCheckResolutions, paperPositionsEnriched, signalsPerformance  (broken — 500)
- LiveReadiness.jsx → liveReadiness, liveTrades, liveTestDryRun, liveEmergencyStop, liveClearStop  (broken — 500/404)

---

## Issue Table — Ranked by Severity

| ID | Severity | Title | Where | One-line fix |
|---|---|---|---|---|
| L0-1 | 🔴 | Docker SQLite bind-mount fails | docker-compose.yml + wallet_db.py:270 | Move DBs to a named volume / WSL2-native path, or disable WAL inside container |
| L0-2 | 🔴 | sklearn ImportError breaks all polymarket CLI tasks | research/trade_ev_reliability.py:9 | Add scikit-learn to pyproject.toml or move import inside function |
| L1-D | 🔴 | wallet_positions stale (34.6h) | scheduler refresh_positions | Cascading from L0-2 |
| L2-A | 🔴 | wallet_profiles 98% empty | scheduler wallet_profiles_rebuild | Cascading from L0-2; backfill once after fix |
| L2-C | 🔴 | wallet_category_profiles ~empty | category_profiles.build_all_profiles | Lower min-sample threshold or seed from wallet_trades |
| L3-D | 🔴 | Fusion score never persisted | paper_executor INSERT | Rebuild image; verify INSERT includes new columns |
| L4-A | 🔴 | Kelly sizing not wired | paper_executor stake resolution | Replace SIGNAL_BANKROLL constants with bankroll_allocator.get_stake_for |
| L5-A | 🔴 | Calibration ignored by executor | same as L4-A | Same fix as L4-A |
| L5-D | 🔴 | Circuit breaker receives no trades | paper_executor.check_and_resolve_open_trades | Add cb.record_trade(pnl) hook on resolution |
| L6-A | 🔴 | /api/circuit-breaker/status 404 | api/main.py registration | Rebuild api image |
| L7-C | 🟡 | Watchdog DB I/O error | health_watchdog | Cascading from L0-1 |
| L1-C | 🟢 | Categories backfill | — | Already complete (0% null) |

---

## Data Quality Assessment

| Dataset | Freshness | Completeness | Verdict |
|---|---|---|---|
| wallet_trades | 0.48d | 397,962 rows | ✅ |
| categories | — | 0% null, 9% other | ✅ |
| wallet_positions | 34.6h | stale | ❌ |
| wallet_profiles | — | 2% useful | ❌ |
| wallet_category_profiles | — | 1 row | ❌ |
| signal firings | live | 5 active types | ✅ |
| paper_trades | live | 235 placed, fusion=NULL | ⚠️ |
| resolved trades | — | 16 (insufficient) | ⚠️ |
| circuit_breaker_log | — | 0 events | ❌ |

---

## Autonomous Loop Status

| Handoff | Working? |
|---|---|
| Data → Intelligence | ❌ (wallet_profiles_rebuild fails on sklearn) |
| Intelligence → Signals | ✅ (signals firing, but on near-empty intelligence base) |
| Signals → Paper Trades | ✅ (235 placed) |
| Paper Trades → Resolution | ✅ (16 resolved) |
| Resolution → Calibration | ❌ (record_trade not called, fusion not persisted) |
| Calibration → Bankroll | ❌ (executor uses static constants) |
| Bankroll → Executor | ❌ (not wired) |
| Executor → Live | 🔒 DRY_RUN locked (correct) |

**Full loop closing: NO.** Open-loop accumulation only. The system places trades
but cannot learn from outcomes.

---

## Recommended Priority Actions

1. **Fix Docker SQLite bind-mount (L0-1).** Single highest-impact change — restores
   8 endpoints, the watchdog DB probe, and unblocks the entire frontend wallet
   intelligence surface.
2. **Add `scikit-learn` to `pyproject.toml` (L0-2).** Restores 6 scheduled tasks
   in one shot, which transitively fixes L1-D and unblocks the L2-A backfill.
3. **Rebuild api + scheduler images (`docker compose build --no-cache`).** Likely
   resolves L3-D, L5-D, and L6-A in one shot if the source already contains the
   fusion gate, circuit breaker hook, and `/api/circuit-breaker/status` route
   (which the audit suggests it does).
4. **After (3), backfill `wallet_profiles_rebuild` once manually** so the
   intelligence base is no longer 98% empty before resuming signal firing on it.
5. **Wire BankrollAllocator into the paper executor (L4-A / L5-A).** Replace
   the static `SIGNAL_BANKROLL[signal_type]` lookup with
   `bankroll_allocator.get_stake_for(signal_type, confidence)`.
6. **Lower the min-sample threshold in `category_profiles.build_all_profiles` (L2-C)**
   so the per-category profile table starts populating before live readiness gates
   need it.

After (1)–(5), re-run this audit. If all blockers clear, the loop becomes truly
autonomous in DRY_RUN mode, and live readiness gates begin receiving meaningful
sample-size data.
