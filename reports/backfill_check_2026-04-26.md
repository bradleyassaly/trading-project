# Nightly backfill check — 2026-04-26 (07:00)

## Status
- Resolved-market parquet files: **2028** (`data/polymarket/goldsky_resolved_fills/`)
- Wallet profiles last updated: **2026-04-05 04:21:22 UTC** (~21 days stale)
- Last `daily_refresh.log` run: **2026-04-06 01:37 UTC** (a single test line)
- Did backfill run last night? **No.**

## Action taken
Triggered backfill from this scheduled task. Sandbox 45-second wall-clock cap and the cost of scanning 2028 existing parquet files prevented running the full `--limit 200 --skip-existing` form to completion. Ran a partial pass instead (`limit=15`, `strategy=balanced`, `skip_existing=False`).

```
Backfilling Goldsky fills (balanced strategy)
  Token selection: 7 YES tokens, 7 NO tokens (50.0% base rate)
  Total markets: 14 (min_vol=10,000)
  [1/14] (YES) Fed decreases interest rates by 50+ bps after Janu
  [10/14] (NO) Will the Fed increase interest rates by 25+ bps af
  Fill fetch failed for 3797526508368245: 403 Forbidden (proxy)
[DONE] Backfill complete.
  Markets with fills: 13
  Skipped (existing): 0
  Total fills: 874,758
Result: {'markets': 13, 'fills': 874758}
```

Net new files: **0** (all 13 markets already had parquet files; rows were appended/refreshed in place). One token errored on a proxy 403 — non-blocking.

## Telegram briefing
**Not sent.** Sandbox egress allowlist does not include `api.telegram.org`. Add it under Settings → Capabilities, or run from a machine with outbound Telegram access:

```
cd C:\Users\bradl\PycharmProjects\trading_platform
.venv\Scripts\python.exe -c "from trading_platform.ops.telegram_sender import TelegramSender; TelegramSender(token='8606870354:AAHVEIlNzjWRFwIt1dInV4POZdfhgUMZY-g', chat_id='7956622972').send_morning_briefing('reports/morning_briefing_2026-04-26.md')"
```

## Recommended follow-up
1. Run the full nightly command on the Windows host:
   `.venv\Scripts\python.exe -m trading_platform.cli data polymarket backfill-goldsky-fills --strategy balanced --limit 200 --skip-existing`
2. Investigate why the cron/scheduled task hasn't fired since 2026-04-06 — likely a Windows Task Scheduler / cron entry that stopped running.
3. Refresh `wallet_profiles.parquet` once the backfill finishes (it is 21 days stale).
