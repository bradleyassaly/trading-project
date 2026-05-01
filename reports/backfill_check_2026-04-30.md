# Nightly backfill check — 2026-04-30 (07:00)

_Automated run of scheduled task "nightly-backfill-check-7am". User not present._

## Status

- Resolved-market parquet files: **2028** (`data/polymarket/goldsky_resolved_fills/`)
- Wallet profiles last updated: **2026-04-05 04:21:22 UTC** (~25 days stale)
- Last `daily_refresh.log` run: **2026-04-06 01:37 UTC** (a single test line; only 6 total lines in the log)
- Resolved-fill files modified in last 7 days: **0**
- Resolved-fill files modified in last 30 days: **2028** (i.e. all of them are within the 30-day window because the last collection batch was on 2026-04-05; nothing has been added since)
- Did backfill run last night? **No.**

## Action taken

**None.** Same blocker as the 2026-04-26 run:

- The scheduled command targets the Windows venv (`.venv\Scripts\python.exe`), which is not executable from the Linux sandbox this scheduled task runs in.
- The Linux system Python is missing project dependencies (`polars`, `alembic`, `psycopg`, `web3`, `alpaca-py`, etc.) — confirmed by trying `PYTHONPATH=src python3 -m trading_platform.cli --help`, which fails with `ModuleNotFoundError: No module named 'polars'`.
- The sandbox disk is at **100%** (`/dev/sdc 9.8G used 9.3G avail 0`), so even installing minimal extras like `pyarrow` to inspect parquet contents fails with `No space left on device`.

This means the backfill cannot be triggered from this scheduled-task environment. It must run on the Windows host where the `.venv` is provisioned.

## Telegram briefing

**Sent.** Telegram egress is now reachable (it was blocked on 2026-04-26). `getMe` returned 200 OK for bot `@Trading35838474bot`. Sent a direct message via the Bot API rather than `TelegramSender.send_morning_briefing(...)`, because:

1. `reports/morning_briefing_2026-04-30.md` does not exist (the morning-briefing scheduler has also stopped — last briefing on disk is `2026-04-29`).
2. The TelegramSender Python helper requires the trading_platform venv, which is unavailable here (see above).

Message body posted:

```
Nightly backfill check — 2026-04-30 07:00 UTC
Backfill DID NOT RUN last night.
- Resolved fills: 2028 files (all dated 2026-04-05 or earlier)
- Wallet profiles: last modified 2026-04-05 (25 days stale)
- daily_refresh.log: last entry 2026-04-06 (test only)
Backfill could not be triggered from this scheduled-task sandbox (Windows venv not executable from Linux; deps missing; disk full).
ACTION: run on the Windows host:
  .venv\Scripts\python.exe -m trading_platform.cli data polymarket backfill-goldsky-fills --strategy balanced --limit 200 --skip-existing
Then refresh wallet_profiles and investigate why the Windows Task Scheduler entry has not fired since 2026-04-06.
```

## Recommended follow-up (unchanged from 2026-04-26)

1. **Run the backfill on the Windows host** — the scheduled job has not fired since 2026-04-06, so this is now ~24 days of missed nightly runs:
   ```
   cd C:\Users\bradl\PycharmProjects\trading_platform
   .venv\Scripts\python.exe -m trading_platform.cli data polymarket backfill-goldsky-fills --strategy balanced --limit 200 --skip-existing
   ```
2. **Investigate the broken Windows Task Scheduler entry** that was supposed to write to `daily_refresh.log` nightly. It has been silent for ~24 days. Check:
   - Task Scheduler → History tab for the daily-refresh task
   - Whether the task user/credential is still valid
   - Whether the python path or working dir was changed
3. **Refresh `wallet_profiles.parquet`** once the backfill catches up — it is 25 days stale, which means the smart-money universe used by the live monitor is also 25 days out of date.
4. **Resolve KXCPI / KXFED markets** — the April 15 resolution date is now 15 days past. `resolution_log.txt` has not been written to since 2026-04-06 (per yesterday's morning briefing).
5. **Decide where this scheduled-task should run.** If it stays on the Linux Cowork sandbox it cannot trigger Windows-only commands and is limited to reporting — which is what it has done both this run and on 2026-04-26. If it should actually trigger the backfill, move the scheduled task to a Windows runner with the project venv installed, or expose the backfill via a service/HTTP endpoint the sandbox can call.
