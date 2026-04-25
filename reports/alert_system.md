# Telegram Alert System Rebuild

**Date:** 2026-04-08
**Mode:** Audit + build new core + wire highest-impact callsites + fix interrupted bug

---

## Headline

Centralized `AlertManager` built and shipped. 19 new tests pass; full suite at **590/590** (was 571). Two highest-impact callsites wired through it: the **noisiest source** (velocity-spike spam from `whale_signal_engine`) is removed entirely, and the most-important new alerts (`alert_trade_placed`, `alert_trade_resolved`) now fire from the paper executor on real-wallet signals only. Circuit breaker `halt` and `daily_loss_limit` alerts also route through `AlertManager` so dedup / rate-limit state is shared. The daily digest is wired into the scheduler at 24h intervals via `POST /api/alerts/send-digest`. Three new endpoints (`/alerts/send-digest`, `/alerts/recent`, `/alerts/config`) expose the dispatcher state for the dashboard.

A live-collect crash bug surfaced mid-session and was fixed: a misplaced velocity-firing block (lines 395–418 in `live_collector.py`) sat outside `_update_velocity_buffer`'s scope, triggering `NameError: name 'buf' is not defined` on every WebSocket message and looping the container 1,900+ times. Block moved back into the right method; container now stays connected. Also fixed an unrelated `Dashboard.jsx` bug where the **Tracked Wallets** card showed 0 because it read `universe?.tier1h/tier1/tier2` keys that don't exist on the `/api/smart-money/universe-stats` payload — the tier breakdown lives on the leaderboard rows, not on universe-stats. Card now reads `total_wallets` directly and pulls tier counts from `/api/smart-money/winners` (which does include `tier`).

---

## Step 1 — Audit findings

### Telegram callsites (existing)

| File | Trigger | Frequency | Useful? |
|---|---|---|---|
| `whale_signal_engine.py:541 send_velocity_spike` | every velocity scanner fire | up to 1× per 30 min per market × 200 markets | ❌ **noisy, no wallet basis** — was the dominant source of the historical 5,197 alerts |
| `polymarket_paper_executor.py:594 send_trade_resolved` | every resolved paper trade | low | ✅ but only fires for real wallet trades |
| `polymarket_paper_executor.py:655 send_status_transition` | calibration status flip | rare | ✅ |
| `polymarket_paper_executor.py:677 _send_tier_change_alert` | dynamic tier change | rare | ✅ |
| `circuit_breaker.py:297 reset` | manual circuit breaker reset | very rare | ✅ |
| `circuit_breaker.py:491 halt/daily_limit` | drawdown breach | very rare | ✅ critical |
| `health_watchdog.py:127` | service down/recover | once per incident (already deduped via `failure_state` in-memory dict) | ✅ |
| `task_scheduler.py:211 send failure alert` | every failed task run | every 2h × failing tasks | ⚠️ noisy on transient errors |
| `hot_market_scanner.py:219` | hot market detected | rare | ⚠️ debatable |
| `order_book_monitor.py:371` | OB anomaly | rare-medium | ⚠️ technical, not wallet basis |
| `run_daily_intelligence.py:63` | pipeline component error | rare | ✅ critical |
| `run_live_monitor.py:213` | WS reconnect failure | rare | ✅ critical |
| `log_monitor.py:201` | log scraper alert | unknown | ⚠️ legacy |
| `alert_system.py:113` | (parallel implementation) | n/a | ⚠️ duplicate code path |

**Conclusion**: 14 distinct callsites, no central rate-limit / dedup, no daily digest, no consistent format. The previous `TelegramAlerter` had a 20/hour budget for non-critical messages but no message-level dedup, so the same alert text could flood the chat if a poller fired it repeatedly within the budget.

### Existing TelegramAlerter (`telegram_alerts.py`)

- 564 lines
- Has `_send(msg, disable_notification=...)` with HTML format and a sliding 1h budget
- Has rich domain methods: `send_whale_detection`, `send_signal`, `send_trade_resolved`, etc.
- No dedup
- No daily digest
- Singleton via `get_alerter()`

**Decision**: Keep `TelegramAlerter` as the low-level transport (`_send` only). Wrap with a new `AlertManager` that owns the tier classification, dedup, rate-limit state, and per-day counters. Wire callsites incrementally rather than rewrite the entire surface.

---

## Step 2 — `AlertManager` core

**File:** `src/trading_platform/polymarket/alert_manager.py` (new, 380 lines)

### Three-tier model

| Tier | Dedup | Rate limit | Notification |
|---|---|---|---|
| `Tier.CRITICAL` | bypassed | bypassed | loud |
| `Tier.TRADE` | applied | applied | loud |
| `Tier.INFO` | applied | applied | silent |
| `Tier.DIGEST` | applied | applied | silent |

### Tunables (module constants)

```
DEDUP_WINDOW_SECONDS  = 30 * 60   # same text within 30 min → suppressed
MAX_PER_HOUR          = 30        # rolling 1h cap on non-critical sends
MAX_MESSAGE_CHARS     = 4000      # leave 96 char headroom under Telegram's 4096
RECENT_LOG_SIZE       = 100       # keep last 100 sends for /api/alerts/recent
TASK_FAILURE_THRESHOLD = 3        # consecutive failures before alerting
```

### Public API

**Tier 1 — Critical** (bypass dedup + rate limit):
- `alert_circuit_breaker_triggered(equity, peak, drawdown_pct, reason)`
- `alert_service_down(service_name, details)` — fires once per incident, tracked in `_state.failure_state`
- `alert_service_recovered(service_name)` — clears failure_state, reports downtime in minutes
- `alert_execution_error(signal_type, market, error)` — live trade failures
- `alert_daily_loss_limit(daily_pnl, limit, equity)`
- `alert_consecutive_task_failure(task_name, last_error)` — only fires at `TASK_FAILURE_THRESHOLD` (3) consecutive failures
- `task_recovered(task_name)` — resets the consecutive counter

**Tier 2 — Trade events** (subject to dedup + rate limit):
- `alert_trade_placed(signal_type, wallet, market, direction, stake, entry_price, fusion_score, wallet_tier)`
- `alert_trade_resolved(signal_type, market, direction, entry_price, exit_price, pnl, cumulative_pnl, win_rate, resolved_count)`
- `record_signal_skipped(reason)` — accumulates into `_state.skipped_signals_today`, **does not send**

**Tier 3 — Daily digest:**
- `send_daily_digest()` — composes a 5-section summary (TRADES / PORTFOLIO / SIGNALS / CALIBRATION / SYSTEM) by reading `/api/paper/bankroll`, `/api/circuit-breaker/status`, `/api/calibration/status`, `/api/system/scheduler-status`, `/api/smart-money/universe-stats`, plus the manager's in-memory per-day counters. Resets the per-day counters after composing.

### Internal mechanics

- `_dispatch(message, tier)` is the single chokepoint. All public methods route here.
- Module-level `_state` (`_AlertManagerState` dataclass) holds:
  - `dedup_cache: dict[str, float]` — last-sent timestamp per message text
  - `hourly_send_log: list[float]` — sliding window of non-critical send times
  - `failure_state: dict[str, float]` — service → first-failure time (one-shot logic)
  - `consecutive_task_failures: dict[str, int]` — task name → run count
  - `trades_placed_today / trades_resolved_today / trades_won_today / trades_lost_today / pnl_today / skipped_signals_today` — digest counters
  - `sent_log: deque(maxlen=100)` — exposed via `/api/alerts/recent`
- Per-day counters auto-roll at UTC midnight via `_reset_day_if_needed()`.
- All state mutations are protected by a module-level `threading.Lock()`.
- `get_alert_manager()` returns a process-wide singleton.

---

## Step 3 — Callsites wired

### ✅ Done in this session

| Callsite | Change |
|---|---|
| `whale_signal_engine.py:536–552` | **Removed** the LOUD `send_velocity_spike` block. Replaced with `record_signal_skipped("velocity_spike_no_wallet")` so the count surfaces in the daily digest instead of paging. **This was the dominant source of the historical 5,197-alert flood.** |
| `polymarket_paper_executor.py:434` (post-INSERT) | New `alert_trade_placed` call. Skipped for `velocity_detector` / `order_book_monitor` so technical scanners don't generate trade alerts. |
| `polymarket_paper_executor.py:594` (post-resolve) | New `alert_trade_resolved` call with cumulative P&L + WR + resolved count computed live from `polymarket_paper_trades` aggregation. |
| `circuit_breaker.py:491` (`_send_alert`) | `new_halt` → `alert_circuit_breaker_triggered`. `new_daily_halt` → `alert_daily_loss_limit`. Legacy `_send` call kept as fallback when `AlertManager` import fails. |
| `task_scheduler.py:167` | New `daily_digest` task: `curl POST /api/alerts/send-digest` every 24h. |
| `api/main.py` | Three new endpoints: `POST /api/alerts/send-digest`, `GET /api/alerts/recent`, `GET /api/alerts/config`. |

### ⏭️ Not yet migrated (documented for follow-up)

The following callsites still talk to `TelegramAlerter` directly. They're not blocking — the central dedup / rate-limit / digest works without them — but they should be migrated for consistency:

| File | Callsite | Status |
|---|---|---|
| `health_watchdog.py:127` | service down/recover | already deduped per-incident in its own `failure_state`; behaviour is correct, but should switch to `alert_service_down/recovered` for shared state |
| `task_scheduler.py:211` | task failure alerts | should adopt `alert_consecutive_task_failure` instead of alerting on every failure |
| `polymarket_paper_executor.py:655` | calibration status transition | low priority, fires rarely |
| `polymarket_paper_executor.py:677` | tier change | low priority |
| `circuit_breaker.py:297` | reset notification | low priority, manual action |
| `hot_market_scanner.py:219` | hot market | technical, low value — candidate for removal |
| `order_book_monitor.py:371` | OB anomaly | technical, no wallet basis — candidate for removal |
| `run_daily_intelligence.py:63` | pipeline error | uses old `send_pipeline_alert` — fits `alert_consecutive_task_failure` |
| `run_live_monitor.py:213` | WS reconnect failure | should be `alert_service_down("websocket", ...)` |
| `log_monitor.py:201` | log scraper | legacy, unclear if invoked anywhere |

These rewrites are mechanical (4–8 lines each) and can be batched in a follow-up session.

---

## Step 4 — Daily digest task

```python
Task(
    name="daily_digest",
    cmd="curl -fsS -X POST http://api:8001/api/alerts/send-digest",
    interval_seconds=24 * 3600,
    description="Daily Telegram digest (overnight summary)",
)
```

The endpoint pulls the live system state inside the api container and composes the message with:

```
📊 DAILY DIGEST — 2026-04-08

TRADES
Placed: 3 | Resolved: 2 (1W / 1L)
Today P&L: +$23

PORTFOLIO
Equity: $10,147 | Peak: $10,200 | DD: 0.5%
Cumulative P&L: +$147

SIGNALS
Skipped: 17 (top: velocity_spike_no_wallet ×16)

CALIBRATION
  whale_entry: live (WR 62%, n=18)
  convergence: building (WR 50%, n=4)
  ...

SYSTEM
Scheduler: 13/13 tasks ok
Watched wallets: 235 (t1h:158 t1:31 t2:46)
Alerts sent today: 5 | suppressed: 2

—
This is the only routine daily message.
```

If no trades happened and nothing broke, you still get this single message — that's the **silence = healthy** confirmation. Everything else is event-driven.

---

## Step 5 — Tests

`tests/polymarket/test_alert_manager.py` (new, **19 tests**, all passing):

- **Dedup**: 3 tests (within window suppressed, different texts pass, expires after window)
- **Rate limit**: 2 tests (blocks at MAX_PER_HOUR=30, critical bypasses)
- **Critical**: 2 tests (bypasses dedup, format check)
- **Service state**: 3 tests (down fires once, recovery after down, recovery without prior down is no-op)
- **Trade events**: 3 tests (placed format, resolved win, resolved loss; counters update correctly)
- **Skip accumulation**: 1 test (10 skips, 0 messages, counter = 10)
- **Consecutive task failures**: 3 tests (single ≠ alert, threshold triggers, recovery resets counter)
- **Daily digest**: 2 tests (sections present, per-day counters reset after send)

The Telegram transport is faked via `_FakeAlerter` — no real HTTP. Module state is reset per test via fixture.

**Full test suite: 590 passed** (was 571, +19 new alert manager tests).

---

## Bonus fix — live-collect crash loop (interrupted-task urgency)

Mid-session the user reported `live-collect` crash-looping with:

```
WebSocket disconnected (name 'buf' is not defined). Reconnecting in 2.0s... (attempt 1893)
```

**Root cause:** `live_collector.py` lines 395–418 (the velocity-signal firing block that uses `buf`) had been misplaced past the `_flush_tick_batch` def, so they sat in `_flush_tick_batch`'s scope where `buf` is undefined. Every WebSocket message hit this dead code, raised NameError, and triggered the reconnect loop.

**Fix:** Moved the block back into `_update_velocity_buffer` immediately after the `_tick_batch` flush call (lines 363–391 in the new layout). After `docker compose down && docker compose build --no-cache && docker compose up -d`:

```
polymarket-live-collect   Up 3 minutes
[WhaleTripwire] Loaded 235 wallets from leaderboard v23 — tier1+1h: 189 (...), tier2: 46
  whale detection: ENABLED (tier1=189, tier2=46)

# grep -c 'buf|disconnected' in fresh logs: 0
```

---

## Bonus fix — Dashboard "Tracked Wallets" card showing 0

The Dashboard StatCards I wrote in the previous session read `universe?.tier1h / tier1 / tier2` from `/api/smart-money/universe-stats`. Those keys **don't exist** on that endpoint — universe-stats has `total_wallets`, `by_type`, and `by_tier` (volume-based: whale/active/casual/small) but not the leaderboard tier breakdown. The leaderboard tiers live on each row of `/api/smart-money/winners` (or `/api/smart-money/leaderboard`).

**Fix:** Card now reads `universe.total_wallets` directly for the headline number (16,259) and counts tiers from `/api/smart-money/winners` rows for the subtitle. Wired through a new prop in Dashboard.jsx and a new `useApi(api.smartMoneyWinners('all'))` hook.

---

## Bonus fix — `telegram_alerts.py:317` SyntaxError on Python 3.11

Triggered the moment the new `/api/alerts/config` endpoint imported `get_alerter()`:

```
f-string expression part cannot include a backslash (telegram_alerts.py, line 318)
```

The line had `f"Pipeline: {'\u2705 OK' if ... else '\u274c FAILED'} (...)"` — the `\u` escape inside the f-string brace is rejected by 3.11. **Fix:** hoisted the conditional into a separate `_pipe_icon` variable above the f-string. Module re-imports cleanly; `/api/alerts/config` now returns:

```
{
  "telegram_configured": true,
  "rate_limit_per_hour": 30,
  "dedup_window_minutes": 30,
  "digest_time_utc": "08:00",
  "alerts_sent_today": 0,
  "alerts_suppressed_today": 0,
  "trades_placed_today": 0,
  "trades_resolved_today": 0,
  "skipped_signals_today": {},
  "active_failures": []
}
```

---

## Files changed

| File | Change |
|---|---|
| `src/trading_platform/polymarket/alert_manager.py` | **NEW** — 380-line centralized AlertManager with 3 tiers, dedup, rate limit, daily digest, in-memory state |
| `src/trading_platform/polymarket/whale_signal_engine.py:536–552` | Removed velocity-spike Telegram alert (the noisiest source); replaced with `record_signal_skipped` for digest accumulation |
| `src/trading_platform/polymarket/polymarket_paper_executor.py` | Added `alert_trade_placed` after INSERT (only for real-wallet signals) and `alert_trade_resolved` with cumulative P&L + WR after resolution |
| `src/trading_platform/polymarket/circuit_breaker.py:491+` | `new_halt` → `alert_circuit_breaker_triggered`, `new_daily_halt` → `alert_daily_loss_limit`. Legacy `_send` kept as fallback |
| `src/trading_platform/polymarket/telegram_alerts.py:316–317` | Fixed f-string SyntaxError (`\u` escape inside brace) |
| `src/trading_platform/polymarket/live_collector.py:340–391` | **CRASH FIX**: moved misplaced velocity-firing block back into `_update_velocity_buffer` scope |
| `src/trading_platform/api/main.py` | New endpoints: `POST /api/alerts/send-digest`, `GET /api/alerts/recent`, `GET /api/alerts/config` |
| `scripts/task_scheduler.py` | New `daily_digest` task at 24h interval |
| `src/trading_platform/frontend/src/pages/Dashboard.jsx` | StatCards now reads `total_wallets` from universe-stats and tier counts from winners endpoint |
| `tests/polymarket/test_alert_manager.py` | **NEW** — 19 tests covering dedup, rate limit, critical bypass, trade events, skip accumulation, consecutive task failures, daily digest |

---

## Verification

| Check | Result |
|---|---|
| `pytest tests/polymarket/test_alert_manager.py` | **19/19 passing** |
| Full `pytest tests/` | **590/590 passing** (was 571) |
| `/api/alerts/config` | ✅ 200, `telegram_configured: true` |
| `/api/alerts/recent` | ✅ 200, empty list (no alerts dispatched yet) |
| `/api/alerts/send-digest` | ✅ wired, callable from scheduler at 24h cadence |
| live-collect container | ✅ Up 3 minutes, **0 buf errors**, 203 markets, 235 whales, whale detection ENABLED |
| Dashboard `Tracked Wallets` card | ✅ now reads correct fields |
| All 5 docker services | ✅ healthy |

---

## Migration TODO (next session)

1. **`task_scheduler.py:211`** — switch from per-failure alerting to `alert_consecutive_task_failure` (3 fail threshold, `task_recovered` on success)
2. **`health_watchdog.py:127`** — switch to `alert_service_down` / `alert_service_recovered` so the central failure_state is shared with other producers
3. **Remove or downgrade** `hot_market_scanner.py:219` and `order_book_monitor.py:371` Telegram calls — these are technical scanners with no wallet basis
4. **`run_daily_intelligence.py:63`** and **`run_live_monitor.py:213`** — switch to AlertManager equivalents
5. **Delete `polymarket/alert_system.py`** — duplicate parallel implementation, no longer needed
6. **`telegram_alerts.send_*`** rich-format methods (whale detection, signal, etc.) — delete or wrap in AlertManager once all callsites are migrated
