# Morning Briefing — 2026-04-29

_Generated: 2026-04-29 (automated daily-briefing-8am)_

## TL;DR

- **CRITICAL: Pipeline appears stalled.** Last `monitor.log` entry is 2026-04-11 04:52 UTC; last `daily_refresh.log` entry is 2026-04-06; last `resolution_log.txt` entry is 2026-04-06. **No collector activity in ~18 days.**
- **Zero TIER 1 / TIER 2 alerts in the last 24 hours** (as a direct consequence of the above).
- **KXCPI / KXFED resolution date (April 15) is 14 days in the past** — these markets should already have resolved, but `resolution_log.txt` shows no entries past April 6.
- Smart money universe (`wallet_profiles.parquet`) is intact: **206 early-informed wallets**, 57 flagged highly informed.

---

## 1. Smart Money Alerts — Last 24h

**Strict 24h window (since 2026-04-28 00:00 UTC):** 0 TIER 1, 0 TIER 2.

The monitor stopped writing on 2026-04-11. For context, the most recent records in the log show:

| Tier  | Market                | Direction | Wallet              | Notional |
|-------|-----------------------|-----------|---------------------|----------|
| TIER1 | Will inflation rise?  | YES       | 0xaaa (placeholder) | $200     |
| TIER2 | Will inflation rise?  | YES       | 0xbbb (placeholder) | $200     |

The `0xaaa` / `0xbbb` placeholder addresses dominating the most recent records suggest the final entries came from a smoke / dry-run rather than live wallet activity. **Treat the April 10–11 burst as test traffic, not real signal.**

Real wallet activity (most recent legitimate window, 2026-04-04 → 2026-04-06):
- Top wallet by alert volume: `0x2005d16a84ce` (5,733 events) — sports / in-play heavy.
- Other heavy wallets: `0x68146921df11` (2,860), `0x507e52ef684c` (2,825), `0x972bf37cae72` (1,796).
- Top markets: tennis (Monte Carlo Masters, Bucharest, Linz), CS:GO matches, "Will inflation rise?", "US x Iran ceasefire by April 15?".

## 2. Newly Resolved Markets

`logs/resolution_log.txt` — last three entries:

```
[2026-04-05 01:27 UTC] Checked 65 open trades, resolved 0
[2026-04-05 13:00 UTC] No open trades to check.
[2026-04-06 13:00 UTC] No open trades to check.
```

**No resolutions logged in 23 days.** The resolution checker either has no open paper trades to monitor, or — more likely given other signals — the scheduler has not invoked it.

## 3. System Health

| Component             | Last successful run     | Status                 |
|-----------------------|-------------------------|------------------------|
| Monitor (smart money) | 2026-04-11 04:52 UTC    | STALE — 18 days silent |
| Daily refresh         | 2026-04-06 01:37 UTC    | STALE — 23 days silent |
| Resolution checker    | 2026-04-06 13:00 UTC    | STALE — 23 days silent |
| Wallet profiles snap  | Loaded OK (16,043 rows) | OK                     |

The most recent `daily_refresh.log` content is a single test stub (`REFRESH | test | all=0`), not a real refresh — confirming the scheduler hasn't completed a real run in over three weeks.

**Likely root causes to investigate, in priority order:**
1. Scheduler / cron service (Windows Task Scheduler entry for the Polymarket pipeline) — confirm it's still enabled and check the last-run status.
2. WebSocket feed (`ws_status.json` in `data/polymarket/`) — confirm not disconnected.
3. Database corruption — multiple `wallet_intelligence.db.CORRUPTED_*` files exist (2026-04-14 timestamps); a backup/restore may have left the live DB pointing at a stale path.

## 4. Smart Money Universe Snapshot

From `data/polymarket/wallet_profiles.parquet` (16,043 wallets total):

- **Early-informed (smart money):** 206
- **Highly informed:** 57
- **Domain mix:** 96 other, 82 geopolitics, 27 crypto, 1 politics

Top 5 by confidence_score:

| # | Wallet                                       | Early WR | Uncertain Early Trades | Best Domain  |
|---|----------------------------------------------|----------|-----------------------:|--------------|
| 1 | `0x94746ed6c69bdd5d5711b18d781d3978ef705091` | 74.2%    | 62                     | other        |
| 2 | `0xa7c1f914724f1c37a1e723ce5c9ac295ec8e0193` | 93.8%    | 16                     | geopolitics  |
| 3 | `0x945a49252f772a10c6ddd1d1e1e24ee20438a48c` | 71.8%    | 85                     | other        |
| 4 | `0x7ea571c40408f340c1c8fc8eaacebab53c1bde7b` | 79.5%    | 166                    | other        |
| 5 | `0xe9c6312464b52aa3eff13d822b003282075995c9` | 100.0%   | 40                     | crypto       |

## 5. Action Items

**No actionable Tier 1 signals on liquid markets today** — there are no live alerts to act on because the monitor isn't running. The only action that matters this morning is restoring the pipeline:

1. **Verify the scheduler.** Confirm the daily-refresh and monitor jobs are scheduled and check last-run status. Re-enable if disabled.
2. **Re-run a manual refresh** to bring the universe and wallet activity current:
   ```
   .venv\Scripts\python.exe -m trading_platform.cli daily-refresh
   ```
3. **Investigate the DB corruption files.** Three `wallet_intelligence.db.CORRUPTED_*` snapshots from 2026-04-14 suggest something happened that day; verify whether the live DB recovered cleanly or is still pointing at a stale read replica.
4. **Manually check KXCPI / KXFED resolution status on Kalshi** since the resolution checker hasn't run past April 6 — there may be unbooked P&L sitting in `paper_trades.db`.
5. Once collectors are restored, re-run this briefing to get a real 24h alert window.

## 6. April 15 Countdown — KXCPI / KXFED

**Target date 2026-04-15 has already passed (T+14 days).** These markets should already be resolved. Because the resolution checker has not run since 2026-04-06, any settlement P&L from these contracts is **unrecorded in the paper trades DB**. Manual reconciliation is required.

---

_Sources: `logs/monitor.log`, `logs/resolution_log.txt`, `logs/daily_refresh.log`, `data/polymarket/wallet_profiles.parquet`._
