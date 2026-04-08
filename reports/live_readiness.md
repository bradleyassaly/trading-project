# Live Trading Readiness Report
Generated: 2026-04-08 (post-audit)

## Overall Status: 🔴 NOT READY FOR LIVE

The full architecture is in place — calibration, dynamic tiering,
fusion-gated paper executor, kill switch, dry-run audit log, Telegram
alerts, Docker orchestration. The blockers are statistical sample
size, missing CLOB credentials, and the master switch.

## Gate Checklist

| # | Gate | Status | Detail |
|---|------|--------|--------|
| 1 | Data pipeline running | ✅ | 397,962 wallet_trades, newest 2026-04-08 01:34 |
| 2 | Wallet universe ≥ 50 | ✅ | 219 wallets in leaderboard (149 tier1h, 24 tier1, 46 tier2) |
| 3 | **Resolved paper trades ≥ 50** | ❌ | 16 resolved / 216 open |
| 4 | Win rate > 52% (overall) | ✅ | 62.5% on the 16 resolved (10W / 6L) |
| 5 | Positive EV | ✅ | Total realized PnL +$4.97M on 16 resolved (price_velocity-driven outliers) |
| 6 | Calibration engine active | ⚠️ | Live but only `price_velocity` has reached 'live' status (15 trades) |
| 7 | Dynamic tiering active | ⚠️ | Schema + engine + tests + API exist; `wallet_category_profiles` table empty (0 rows) |
| 8 | Kill switch tested | ✅ | Verified blocks without `POLYMARKET_LIVE_ENABLED`, allows when set |
| 9 | Per-trade size limits | ✅ | `MAX_TRADE_USD=$100` (intentionally tiny); kelly-clamped 0..0.25 |
| 10 | Per-day loss limit | ✅ | `MAX_DAILY_LOSS_PCT=5%` |
| 11 | Concurrent positions limit | ✅ | `MAX_OPEN_POSITIONS=10` (live), separate from paper |
| 12 | Execution dry-run passed | ✅ | `live_trades` table has dry-run audit row from prior turn |
| 13 | **CLOB credentials set** | ❌ | `POLYMARKET_API_KEY/SECRET/PASSPHRASE` all missing |
| 14 | **Wallet credentials set** | ❌ | `POLYMARKET_PRIVATE_KEY/WALLET_ADDRESS` all missing |
| 15 | **`POLYMARKET_LIVE_ENABLED=1`** | ❌ | Master switch not set |
| 16 | `py-clob-client` installed | ❌ | Install via `pip install py-clob-client` before live |
| 17 | Telegram alerts working | ✅ | Bot token + chat id configured; tested end-to-end |
| 18 | Telegram alert types implemented | ✅ | Trade placed, trade resolved, signal status, tier change, OB anomaly, velocity spike, daily update, scheduler failure, watchdog |
| 19 | Docker installed | ✅ | Docker 29.3.0, Compose v5.1.0 |
| 20 | Docker orchestration files | ✅ | `docker-compose.yml`, `Dockerfile`, scheduler, watchdog all created |
| 21 | Orchestration tested 24/7 | ❌ | Files exist but `docker compose up` has not yet been run |
| 22 | Human approval | ❌ | Manual gate — never automate |

## Blocking Issues (must fix before live)

1. **Resolved paper trade sample (16 / 50)**. Only `price_velocity` has 15 resolved trades; that's the only signal type the kill switch will let through. The other 12 signal types are at 0–1 resolved each. Going live now would mean trading exclusively on velocity signals — fine if that's intentional, but no diversification.
2. **CLOB credentials missing**. Without `POLYMARKET_API_KEY/SECRET/PASSPHRASE` the `polymarket_live_executor` cannot place real orders. The dry-run path works, real orders will fail closed.
3. **Wallet credentials missing**. Without `POLYMARKET_PRIVATE_KEY/WALLET_ADDRESS` the EIP-712 order signing in py-clob-client cannot run.
4. **`py-clob-client` not installed**. Add to `pyproject.toml` or run `pip install py-clob-client` in the api+scheduler containers.
5. **Master switch off**. Until `POLYMARKET_LIVE_ENABLED=1` is in `.env`, the kill switch refuses every live trade by design — even with all credentials present.
6. **Dynamic tiering profiles empty**. The `wallet_category_profiles` table has 0 rows. The engine will populate it after the next resolution checker run wakes it up. Until then the fusion score uses the static tier1h/tier1/tier2 multiplier from `signal.wallet_tier`.
7. **Orchestration not yet run**. Files exist, `docker compose up -d` has not been executed — meaning resolution checker, calibration rebalance, and tier rebuild are not running on a schedule yet.
8. **wallet_trades category gap**. 289,117 of 397,962 fills (73%) have null category. Calibration / tiering will be biased toward markets that *do* have a category until this is fixed.

## Recommended First Live Parameters (when all blockers resolved)

| Parameter | Value | Rationale |
|---|---|---|
| Starting capital | $500 | Tiny sample size means tiny stakes |
| `MAX_TRADE_USD` | $25 | 5% per trade max |
| Max concurrent positions | 5 | Already enforced; lower from 10 for first week |
| `MAX_DAILY_LOSS_PCT` | 5% | Already default |
| Max drawdown halt | 20% | Add explicit circuit breaker (TODO) |
| Signal types enabled | `price_velocity` only | Only signal at 'live' calibration |
| Categories enabled | crypto, politics | Biggest measured samples |
| Execution mode | Limit orders w/ 2% slippage | Already wired in `clob_client.place_market_order` |
| First-week monitoring | Every trade via Telegram | Already wired |

## Priority action items (ordered)

1. **Run `docker compose up -d`** — gets the scheduler + watchdog + resolution checker running on a real cron, which immediately starts growing the resolved-trade sample.
2. **Backfill the wallet_trades category column** for the 289k null rows — without this the dynamic tier engine works on 27% of the data.
3. **Wait** for the resolution checker to push the resolved sample past 50 trades for at least 2 signal types. With the 30-min poll cycle and 216 open trades this should happen within days.
4. **Install `py-clob-client`** — `pip install py-clob-client` (also add to `pyproject.toml` so the Docker rebuild picks it up).
5. **Generate Polymarket API credentials** via the Polymarket UI's API keys page and add them to `.env`.
6. **Run a full dry-run cycle** — `POST /api/live/test-dry-run` for every signal type that hits 'live' status. Verify each shows up in `live_trades` with `status='dry_run'`.
7. **Add the master switch** — set `POLYMARKET_LIVE_ENABLED=1` in `.env`, restart api + scheduler.
8. **Flip `PolymarketLiveExecutor.DRY_RUN = False`** in your local instance. (This is intentionally not in `.env` — it requires editing source so a typo can't accidentally enable live.)
9. **First live trade**: manually invoke `POST /api/live/test-dry-run` (which now actually places real orders) on a single market. Watch Telegram. Hit `POST /api/live/emergency-stop` immediately if anything looks wrong.
10. **First week**: monitor every Telegram alert. If any fails, hit emergency stop and audit `live_trades`.

## Where the protections live

| Control | File |
|---|---|
| Master switch + emergency stop file | `polymarket/kill_switch.py` |
| Sample-size + EV + WR gates | `polymarket/kill_switch.py:KillSwitch.check` |
| Per-trade max size | `polymarket/kill_switch.py:MAX_TRADE_USD` |
| Daily loss limit | `polymarket/kill_switch.py:MAX_DAILY_LOSS_PCT` |
| Open position cap | `polymarket/kill_switch.py:MAX_OPEN_POSITIONS` |
| Kelly sizing | `polymarket/kelly_sizer.py` (cap 25% Kelly, 2% bankroll) |
| Fusion gate | `polymarket/fusion_score.py:compute_fusion` |
| Dry-run flag (in source, not env) | `polymarket/polymarket_live_executor.py:DRY_RUN` |
| Audit log | `live_trades` table — every attempt incl. blocked + dry-run |
| Telegram on every trade | `polymarket/telegram_alerts.py:send_paper_trade` + `send_trade_resolved` |
| Watchdog → Telegram on degradation | `scripts/health_watchdog.py` |
| Scheduler failure → Telegram | `scripts/task_scheduler.py:_send_failure_alert` |

## What's NOT yet implemented

- **Max drawdown circuit breaker** — kill switch handles daily loss but not cumulative drawdown across days. Add a `MAX_DRAWDOWN_HALT_PCT` check that walks the `live_trades` table cumulative PnL.
- **Per-market exposure cap** — kill switch limits open position count globally but not per-market. A single market could theoretically receive multiple trades.
- **Correlated position limit** — no awareness of which markets are politically/economically correlated.
- **Stale data protection** — no automatic block if the wallet trade sync hasn't run in N hours. The watchdog will alert but the live executor will keep trading.
- **Manual override / emergency stop button in GUI** — exists as `POST /api/live/emergency-stop` but no button in `LiveReadiness.jsx` yet. Add a button that fires the endpoint with a confirmation dialog.

These are all medium-priority and should be addressed before scaling
beyond the $500 first-week budget.
