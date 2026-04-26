# System Audit — 2026-04-26

15 commits in 72h (`70e3d9a` → `52ed82f`). Verdicts grounded in: `data/scheduler/state.json`, `logs/scheduler/*.log`, source grep, and yesterday's evaluation reports.

---

## 1. Ingestion / market data — **GREEN**

- **Changed:** `db_connection` keepalive (`70e3d9a`); `pm_leaderboard_sync` chained backfill (`6c11680`); `setup_logging` wired into live-collect (`70e3d9a`).
- **Verify:** `logs/scheduler/sync_wallet_trades.log` last entry 02:17 UTC: 200 wallets / 653 new trades / 0 errors; DB now at 1,073,841 trades. `refresh_universe.log` rolls 6h with 459 markets; `bankroll_refresh` running every 15 min, `last_status=ok`. `data_api_fetch` healthy (46s, ok).
- **Improvement:** none material; pipeline is steady.

## 2. Wallet intelligence — **AMBER**

- **Changed:** Insider pool 82→350 via `insider_promote` (`587d3c2`); `wallet_behavior_metrics` (CI / sybil / sizing / clusters) (`70e3d9a`); `wallet_earliness` EWMA (`7cb8a25`); `wallet_blocklist` override (`52ed82f`); `pnl_reconstruction → behavior_metrics` chain (`086fd20`).
- **Verify:** `wallet_behavior_metrics.log` 07:17: `wallets_processed=1172, copyable_ci=153, likely_farmers=104, category_zscores=3826`. `pnl_reconstruction.log` completed 1748 wallets in 3,077s with chained behavior refresh.
- **Why amber:** PM-whale trade-history coverage regressed to 75% (was 86%) per yesterday's session report; backfill running but lags new-seed ingestion.
- **Top-1 fix:** raise `backfill-top-wallets` cadence from manual/on-demand to 6h scheduler entry with `--limit 25` per cycle. **Effort: 30 min.**

## 3. Signal generation (13 types + decay gate + Phase B) — **AMBER**

- **Changed:** `whale_entry` raw re-enabled (`587d3c2`); `[DECAY_GATE]` reads `signal_health.decay_flag` (`adca1ed`, executor:514); per-(signal,side) `STAKE_MULTIPLIERS` (`587d3c2`, executor:951); Phase B `resolution_decay_signal` shipped + enabled (`eb5a594`, `7cb8a25`).
- **Verify:** `resolution_decay_signal.log` 03:33 UTC: `candidates=3 fired=3` (3 markets `PLACED` at $1 each — `bra2-acg-ava`, Miami temp, Denver temp). Subsequent runs at 03:49/04:04 show same candidates `gated` (already placed → de-dup), confirming both fire-path *and* idempotency work.
- **Why amber:** First Phase B cycle at 03:18 logged `'skipped': 'PHASE_B_RESOLUTION_DECAY_ENABLED not set'` — the env var was only injected into the cycle 15 min later. Live-loop env handling is racy across restarts. Also: `paper_trades` weren't observed via DB (Postgres-in-Docker; couldn't query from host) — verification leans on log lines.
- **Top-1 fix:** persist `PHASE_B_RESOLUTION_DECAY_ENABLED=1` in `.env` (currently runtime-only) so the flag survives every scheduler restart. **Effort: 5 min.**

## 4. Calibration & sizing — **GREEN**

- **Changed:** `isotonic_calibration.py` (`587d3c2`) + per-category curves (`086fd20`); Brier 0.35 → 0.24; `STAKE_MULTIPLIERS` and `kelly_sizer.py` updated (+48 lines); `wallet_earliness` boost in executor:980.
- **Verify:** `data/scheduler/state.json` shows `isotonic_calibration` ran 07:17 ok in 0.2s; `[CALIB] apply` path live in executor (line 940). `[EARLINESS]` log marker is wired (executor:982).
- **Improvement:** none in 24h; daily refit cadence holding.

## 5. Risk management & gates — **GREEN**

- **Changed:** 14-gate stack including `FAV_GATE 0.65`, `KS_BLOCK_WR` probation `0.45` (`adca1ed`, kill_switch:69), `LIVE_DISCOVERY` tier $1 (`086fd20`, kill_switch:86), `wallet_blocklist` override (`52ed82f`).
- **Verify:** `kill_switch.py:69` MIN_WIN_RATE_PROBATION=0.45 confirmed; `:86` DISCOVERY_MIN_RESOLVED=1, MAX=$1, env-gated `LIVE_DISCOVERY_ENABLED`. Yesterday's funnel showed `KS_BLOCK_WR` was top live blocker at 45 — fixed inflight.
- **Improvement:** today's funnel needs to show `KS_BLOCK_WR<10` to confirm the probation drop landed; queryable via `/api/funnel/decisions` once an operator pulls.

## 6. Exit management — **AMBER**

- **Changed:** `position_monitor` and `paper_check_exits` running every 15 min (state.json); `polymarket_paper_executor.py:1712-1822` carries TRAIL/SL/TP/time_decay/whale_mirror_exit logic.
- **Verify:** `paper_check_exits` last run 03:44 ok in 12.8s; `position_mark_to_market` 03:50 ok; `live_position_monitor` every 5 min, ok.
- **Why amber:** `src/.../exit_strategy.py` (114 lines) is the **stub** that says "NOT wired into the paper executor." The actual exit logic lives inline in `polymarket_paper_executor.py`. Two parallel implementations is a maintenance trap; `pre_resolve_decay` and `re-entry cap` aren't visible as named constants — likely embedded in the inline block but not auditable from the named module.
- **Top-1 fix:** delete `exit_strategy.py` stub OR move the executor's inline logic into it; pick one source of truth. **Effort: 1.5 h.**

## 7. Observability — **GREEN**

- **Changed:** 11 endpoints in `api/main.py` between lines 765–1499 (`/api/ladder/status`, `/api/portfolio/by-category`, `/api/leaderboard/recent`, `/api/heatmap/signal-category`, `/api/diff/24h`, `/api/explain/{id}`, `/api/wallet/{addr}/profile`, `/api/signal-health`, `/api/calibration`, `/api/funnel/decisions`, `/api/system/readiness`); dashboard tiles `LadderProgressTile`, `IntelligenceHealthTile`, `CalibrationCurveTile`, `InsiderGrowthTile` + pages `SignalHealth`/`DecisionFunnel` (`ca4db12`); 4 dead pages deleted (-1,967 lines).
- **Verify:** all endpoint paths grep-confirmed in `api/main.py`; `decision_trace` table populating (yesterday: 506 rows); `signal_anomaly_detector.log` shows `found=1 sent=1` regularly.
- **Improvement:** none; this layer is now the strongest part of the stack.

## 8. Research / backtesting — **AMBER**

- **Changed:** `backtest_robustness.py` adds PBO + DSR (`6c11680`); `signal_engine_backtest.py` weekly run wired (state.json: 604,800s); `signal_health.py` IC over 30d/14d, decay flags (`6c11680`).
- **Verify:** `signal_health` ran 07:17, `signals_processed=15, decay_flagged=4, pairwise_corr_pairs=144`. Last `signal_engine_backtest` ran 60 days back, 3,713s — within weekly window.
- **Why amber:** `enrich_trade_resolution` task **failed** (state.json:374) with `psycopg.IdleSessionTimeout`. Weekly cadence means PnL on resolved trades stale by a full week if not retried. Pool keepalive landed but apparently not on this code path.
- **Top-1 fix:** add a `try/except + reconnect-on-IdleSessionTimeout` wrapper around `enrich_trade_resolution`'s long query, OR run it in batches of 500 markets. **Effort: 1 h.**

## 9. Discovery / hypothesis pipeline — **AMBER**

- **Changed:** `hypothesis_tracker.py` (+166 lines, `61c9f3a`); `insider_promote.py` (+136 lines, `587d3c2`); `LIVE_DISCOVERY_ENABLED` discovery tier (`086fd20`); `alpha_discovery_onramp_2026-04-24.md` defines 6-stage pipeline; `cross_platform_divergence.py` (+244 lines, skeleton, `7cb8a25`).
- **Verify:** `insider_promote` ran 07:17 ok in 0.8s; `hypothesis_backfill` ran 02:24 ok; `synthetic_test_trade` **failed** with `function datetime(unknown) does not exist` — the SQL still uses SQLite syntax against Postgres (state.json:564).
- **Why amber:** synthetic-test canary is broken since 08:08 yesterday — this is the daily smoke-test that proves the end-to-end fire path works. Until fixed, we're flying without a daily integration heartbeat.
- **Top-1 fix:** replace `datetime('now')` with `NOW()` in `scripts/synthetic_test_trade.py`. **Effort: 5 min.**

---

## TOP 3 ACTIONABLE FIXES (next 24h) — ordered by EV

### #1 — Fix synthetic_test_trade SQL portability bug
- **What:** Replace `datetime('now')` with `NOW()` in `scripts/synthetic_test_trade.py` (the WHERE clause near `end_date_iso > datetime('now')`).
- **Why now:** the daily fire-path canary has been red for 24h+; without it we have no integration heartbeat. The decay-gate, behavior-gate, and stake-multiplier changes shipped yesterday all depend on the canary catching regressions.
- **Effort:** 5 min.
- **Success metric:** `state.json:synthetic_test_trade.last_status="ok"` and one synthetic trade rows into `paper_trades` per day.

### #2 — Persist `PHASE_B_RESOLUTION_DECAY_ENABLED=1` in `.env`
- **What:** Add `PHASE_B_RESOLUTION_DECAY_ENABLED=1` to the project's `.env` (or `docker-compose.yml` env block) so the flag survives every container restart.
- **Why now:** today's logs show one full 15-min cycle silently skipped because the runtime-only flag wasn't set yet. With Phase B finally producing the *first* independent (non-whale) signal lane (3 candidates fired), losing cycles is direct alpha leakage. Trades placed today: 3; without persistence we lose ~1 cycle per restart.
- **Effort:** 5 min.
- **Success metric:** `resolution_decay_signal.log` shows zero `'skipped': 'PHASE_B_RESOLUTION_DECAY_ENABLED not set'` lines over a 24h window.

### #3 — Wrap `enrich_trade_resolution` against IdleSessionTimeout
- **What:** Add a reconnect-on-IdleSessionTimeout decorator (or split the query into ≤500-market batches) in `enrich_trade_resolution` so the weekly task survives pg pool eviction.
- **Why now:** the task failed yesterday at 1,669s (state.json:374) with `psycopg.errors.IdleSessionTimeout`. PnL fields on resolved trades are now a full week stale, which directly degrades `wallet_behavior_metrics` (sizing percentiles), `signal_health` (IC computation), and `wallet_alpha_scores` recompute — three of yesterday's most important upgrades all rely on it.
- **Effort:** 1 h.
- **Success metric:** task runs to completion (`last_status=ok`, `consecutive_failures=0`) and resolved-trade PnL coverage in `wallet_trades` returns to ≥99% over 7d.

---

## One-line system verdict

Architecture is **operationally elite** for its scale; remaining gaps are 3 small operational fixes (SQL portability, env-var persistence, pool keepalive on long queries) totaling ~1.2h of work — all visible because the observability layer shipped yesterday actually surfaces them.
