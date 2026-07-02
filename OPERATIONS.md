# Operations — Failure Modes & Recovery

This document is the operator runbook. Every component of the system can
fail; this is what happens, what auto-recovers, and what needs you.

---

## Deploying the 2026-07-02 safety changes (one-time checklist)

The Phase-0.5 flaw burn-down (see `SCALING_PLAN_2026-07-02.md`) changed
live-money behavior. On the next deploy to the live host:

1. **Confirm `POLYMARKET_LIVE_ENABLED=1` in `.env`.** It is now the
   single authoritative master switch, enforced at the top of the
   dry-run gate — if unset, NOTHING trades live (previously the
   allowlist and 3-dim slice promotion bypassed it).
2. **Live BUYs work again.** `resolution_decay` BUYs had crashed with
   `NameError` since 2026-05-19 (favorite-guard constants deleted by the
   SELL-only commit). After restart, watch for `[LIVE][FAV_GATE]` and
   `[LIVE][BUY_REENABLED]` log lines to confirm BUYs flow and get
   risk-checked.
3. **Two behaviors are now opt-in (default OFF)** — leave them off until
   the ledger is reconciled:
   - `POLYMARKET_ALLOW_3DIM_LIVE=1` re-enables 3-dim slice live promotion
   - `POLYMARKET_EV_BYPASS_ENABLED=1` re-enables the EV waiver of the WR
     floor
4. **Depth/exposure gates now fail CLOSED in live mode.** If the CLOB
   book API degrades, live entries will block with
   `depth_unknown_block_live` / `exposure_check_failed_block_live`
   reasons instead of trading blind. Paper is unchanged (fail-open).
5. **Start the Phase-0 clock.** Run `python scripts/validate_realized_pnl.py`
   over full history and triage the 63 known fictitious closes. The
   daily review (`scripts/daily_system_review.py`) now has section 12
   (on-chain/DB equity agreement, 14-day streak toward the Phase-0 exit
   gate) and section 13 (per-signal EV verdicts split by exit-truth
   channel). No bankroll promotion until section 12 reads 14/14.

---

## Critical path for live trading

```
   Polymarket WS
        ↓
   live-collect ── DB write: wallet_alerts ──┐
        ↓                                      │
   whale_signal_engine ── ALPHA GATE           │
        ↓                                      │
   trade_hypotheses (rationale)                │
        ↓                                      ├── all writes go to
   polymarket_paper_executor                   │   wallet_intelligence.db
        ↓                                      │   (WAL mode, 60s busy_timeout)
   polymarket_paper_trades INSERT ─────────────┤
        ↓                                      │
   check_and_resolve_open_trades ──────────────┘
        ↓
   alert_trade_resolved → KPITracker → daily digest
```

If ANY step in this path can fail silently, that's a live-trading risk.
The fixes below all aim at "no silent failures, retry where safe, log
when not, alert on the critical points."

---

## Failure mode matrix

| Component | Failure Mode | Impact | Auto-Recovery | Manual Action |
|---|---|---|---|---|
| **api** | crash | GUI blank, no new alerts/trades emitted via API endpoints | `restart: unless-stopped` (compose) | none for transient |
| **api** | DB locked | endpoint returns 500 / wraps in try/except | 60s `busy_timeout`, `WAL` mode allows concurrent reads | none if <60s; investigate writers if persistent |
| **live-collect** | WS disconnect | no whale detection until reconnect | exponential reconnect backoff (2s base), `restart: unless-stopped` | check Polymarket status if >5min |
| **live-collect** | DB locked on `WhaleTripwire.reload()` | watched wallet set could be empty → silent blindness | **5-attempt retry with exponential backoff; if all fail, fall back to last cached wallet set; logs CRITICAL on full failure** | grep `[WhaleTripwire] CRITICAL` in logs |
| **live-collect** | crash | gap in monitoring | `restart: unless-stopped`; reload watched wallets on startup | verify ws_status.json after restart |
| **live-collect** | `name 'buf' is not defined` (historical) | crash loop | fixed in session 13; covered by `test_live_collector` | none — fixed |
| **scheduler** | crash mid-task | task partially complete | tasks are idempotent (`INSERT OR IGNORE` / `INSERT OR REPLACE`); state.json updated only on success | check `data/scheduler/state.json` |
| **scheduler** | task fails 3+ times | data drift | AlertManager `alert_consecutive_task_failure` fires (Telegram CRITICAL) | investigate the specific task |
| **watchdog** | crash | no health alerts → silent system death | `restart: unless-stopped` | manual `docker compose ps` check |
| **watchdog** | DB locked on probe | false alert | 60s busy_timeout + retry, swallowed if recovers within 30s | none |
| **wallet_intelligence.db** | locked >60s | writes fail | `WAL` mode + 60s busy_timeout + retry wrapper on hot writes | investigate the long-running writer |
| **wallet_intelligence.db** | corruption | system broken | restore from `data/backups/wallet_intelligence_<DATE>.db` (last 7 days) | `cp` from backup, restart all services |
| **wallet_intelligence.db** | orphaned `.db-shm` | "database is locked" loop with no actual writer | **stop all services, `rm *.db-shm *.db-wal`, restart** | one-time cleanup |
| **Polymarket data-api** | down or rate-limited | data sync stalls, scheduler tasks fail | scheduler retries on next cycle (1–24h depending on task) | wait for PM recovery, monitor task failures |
| **Polymarket CLOB WS** | down | no live signals | reconnect with backoff | wait for PM recovery |
| **Polymarket Gamma** | down | resolution checker can't resolve trades | trades stay open until Gamma is back; no data loss | wait for PM recovery |
| **Circuit breaker** | halted | no trades placed (intentional) | NEVER auto-resets max-drawdown halt | manual `POST /api/circuit-breaker/reset` after investigating |
| **Kill switch** | WR gate blocks positive-EV signal | trade skipped despite edge | EV bypass (≥5%) auto-skips WR gate for asymmetric-payoff signals | verify blended EV in kill_switch logs if a signal is blocked unexpectedly |
| **Live executor** | Order rejected by CLOB | no position entered | retry once, then skip | check CLOB error message |
| **Live executor** | Order timeout (2 min) | cancel order, no position | log + alert via Telegram | check CLOB status |
| **Live executor** | Partial fill | smaller position than intended | record actual fill, adjust P&L | review in `trade_fills` table |
| **Live executor** | Balance insufficient | order rejected, halt live | alert, halt live trading | deposit more capital |
| **Live executor** | API credentials expired | all orders fail | re-derive via `client.create_or_derive_api_creds()` | update .env |
| **Live executor** | Network error during order | unknown state | check order status, cancel if pending | manual review |
| **Wallet poller** | Gap in monitoring (outage) | missed signals | catchup poll on restart; stale signals logged to `stale_signals` table | review stale count |
| **Wallet poller** | Stale signal (>10min old) | logged but not traded | recorded in `stale_signals` for analysis | reduce poll interval if frequent |
| **Circuit breaker** | daily loss limit hit | trading paused until midnight | auto-resets at next UTC midnight via scheduler | none |
| **AlertManager** | Telegram API down | messages logged but not sent | dedup cache + rate limit state preserved across the outage | check Telegram BotFather status |
| **Frontend** | vite proxy misconfigured | dashboard shows no data | (fixed in session 11; proxy target = `http://api:8001`) | check `vite.config.js` |

---

## How the DB locking fix works

Until session 13 the system used `journal_mode=DELETE` because a previous
attempt to use WAL had failed on a WSL2 NTFS bind-mount. The actual root
cause was an **orphaned `wallet_intelligence.db-shm` file from a crashed
WAL session** that subsequent attempts to switch to WAL kept reading as
"another writer holds the lock", looping indefinitely with `database is
locked` errors.

The fix:

1. Stop all services that hold the DB
2. `rm /app/data/polymarket/wallet_intelligence.db-shm` (and any `-wal`)
3. Open the DB cleanly, run `PRAGMA journal_mode=WAL` — this succeeds now
4. Update **every** PRAGMA site in the codebase to use WAL with
   `busy_timeout=60000`. The list:
   - `db_connection.py:get_connection`
   - `wallet_db.py:WalletDB.__init__`
   - `polymarket_paper_executor.py:__init__` (both legacy + wallet conns)
   - `live_db.py:LiveTickStore.__init__`
   - `kalshi/paper_executor.py:__init__`
   - `api/artifact_reader.py` (live ticks reader connections)
5. Add `execute_with_retry` and `commit_with_retry` to `db_connection.py`
   for hot write paths
6. Add `db()` context manager for short-lived connections
7. Add `WhaleTripwire.reload()` retry+cache so the most-critical
   read path never fails silently
8. Daily backup task in the scheduler so we always have ≤24h-old
   restore points

If the orphaned shm file ever recurs (e.g. after a hard host crash),
the runbook is:

```
docker compose down
docker compose run --rm --no-deps api bash -c '
  rm -f /app/data/polymarket/wallet_intelligence.db-shm
  rm -f /app/data/polymarket/wallet_intelligence.db-wal
'
docker compose up -d
```

---

## Backup + restore

**Daily** the scheduler runs `db_backup`:

```python
sqlite3.connect(src).backup(sqlite3.connect(dst))
```

This uses SQLite's native backup API, which is safe to run while the DB
is being written to. Keeps the last 7 days under `data/backups/`.

**To restore:**

```bash
docker compose down
cp data/backups/wallet_intelligence_<DATE>.db data/polymarket/wallet_intelligence.db
rm -f data/polymarket/wallet_intelligence.db-shm data/polymarket/wallet_intelligence.db-wal
docker compose up -d
```

Then verify:
```bash
docker compose logs live-collect --tail 10
# Should show: whale detection: ENABLED (tier1=N, tier2=M)
```

---

## Health checks

Each service has a `healthcheck` directive in `docker-compose.yml`. The
`watchdog` service polls each service every 5 minutes and pages
Telegram (via AlertManager critical tier) on:

- API down (`http://api:8001/api/system/status` not 200)
- Scheduler stale (state.json older than 2× the longest task interval)
- DB unreachable
- Disk space < 5GB free

Recovery messages fire when a previously-down service comes back.

---

## Critical alerts the operator should never ignore

| Alert | What it means | Action |
|---|---|---|
| `🛑 CIRCUIT BREAKER TRIGGERED` | Cumulative drawdown ≥ 20% | DO NOT auto-reset; investigate the losing trades; `POST /api/circuit-breaker/reset` only after deciding what to fix |
| `⚠️ DAILY LOSS LIMIT HIT` | -5% intraday | Self-resolves at midnight; investigate same-day if possible |
| `🔴 SERVICE DOWN: api` | API container can't be reached | Check `docker compose logs api`, restart if needed |
| `🔴 LIVE TRADE EXECUTION FAILED` | Order to CLOB rejected | Check live executor logs; verify POLYMARKET_API_KEY and KillSwitch state |
| `🟡 TASK FAILING REPEATEDLY` | Same scheduled task failed 3+ times | Check the task's log file under `logs/scheduler/` |
| `[WhaleTripwire] CRITICAL: DB locked` | Whale detection is BLIND | Stop all services, clean stale shm files (see "How the DB locking fix works"), restart |
| `⚠️ Signal Health Alert — New decay detected` | A monitored signal's IC14 turned negative on ≥10 resolved — edge may be decaying | Review signal_health table; consider removing from LIVE_SIGNAL_TYPES if IC30 also negative |
| `⚠️ Signal Health Alert — IC30 dropped below 0.02` | Monitored signal approaching decay threshold | Watch next 6h run; if IC30 goes negative → action above |
