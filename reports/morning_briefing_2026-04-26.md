# Morning Briefing — 2026-04-26

_Generated 2026-04-27 02:01 UTC by scheduled daily-briefing-8am task._

## TL;DR

System health is the headline today: **all three log streams are stale**. Monitor stopped writing on 2026-04-11 (15 days ago), resolution and daily-refresh logs both stopped on 2026-04-06 (20 days ago). No alerts can be analyzed from the last 24 hours because the collector hasn't produced any. Investigate the scheduler before relying on signals today.

## Smart Money Alerts — Last 24h

**Tier 1: 0 | Tier 2: 0**

The monitor log's most recent entry is `2026-04-11 04:52:25 UTC`, well outside the 24-hour window. There is nothing to triage from overnight.

For context (whole log history, 2026-04-05 → 2026-04-11):

- Tier 1 alerts: 3,126 total, dominated by wallet `0xaaa` on the placeholder market "Will inflation rise?"
- Tier 2 alerts: 15,327 total, dominated by wallet `0xbbb` on the same market
- Direction skew: overwhelmingly `YES` ($200 nominal fills throughout)

These look like fixture/test traffic rather than live wallets — `0xaaa`/`0xbbb` are not real on-chain addresses and the market title is a placeholder. Worth confirming with the operator that the monitor was emitting real data before it stopped.

## Resolved Markets

None. `logs/resolution_log.txt` has three entries, the most recent being `2026-04-06 13:00 UTC`: "No open trades to check." No resolutions, no P&L moves, no cash changes (last cash snapshot $9.76, P&L $0.00).

## System Health

| Collector | Last Write | Age | Status |
|---|---|---|---|
| `logs/monitor.log` | 2026-04-11 04:52 UTC | 15 days | **STALE** |
| `logs/resolution_log.txt` | 2026-04-06 13:00 UTC | 20 days | **STALE** |
| `logs/daily_refresh.log` | 2026-04-06 01:37 UTC | 20 days | **STALE** |

The daily-refresh log only contains a `--- START ... initial=False ---` block with a literal "Test line" payload (`REFRESH | test | all=0`) — looks like the last successful run was a smoke test, not a real refresh. The nightly job has either not been scheduled, has been failing silently, or its output is being routed elsewhere.

`data/polymarket/wallet_profiles.parquet` itself is also frozen — file mtime is 2026-04-05 04:21 UTC. So the underlying smart-money table hasn't been recomputed in three weeks; today's roster is whatever was produced before the freeze.

### Smart Money Roster (cached, last refreshed 2026-04-05)

206 wallets flagged `is_early_informed = True` out of 16,043 profiled. Top 5 by confidence score:

| Wallet | EWR | Uncertain Early Trades | Best Domain | Confidence |
|---|---|---|---|---|
| `0x94746ed6...05091` | 74.2% | 62 | other | 1.46 |
| `0xa7c1f914...e0193` | 93.8% | 16 | geopolitics | 1.37 |
| `0x945a4925...0a48c` | 71.8% | 85 | other | 1.34 |
| `0x7ea571c4...bde7b` | 79.5% | 166 | other | 1.31 |
| `0xe9c63124...995c9` | 100.0% | 40 | crypto | 1.31 |

## Action Items

1. **Restart the monitor / scheduler.** Three independent log streams went silent in the same week. Check the `logs/scheduler` directory, the systemd/cron unit, and `.env` for credential expiry.
2. **Backfill the smart-money roster.** Once collectors are running, re-run the wallet-profile build so the 70%+ EWR cohort reflects the last three weeks of fills, not 2026-04-05 state.
3. **Tier-1 acting today: none.** Without fresh signals there is nothing on a liquid market to act on. Hold position; do not act on stale tier-1s in the log.
4. **Verify the `0xaaa`/`0xbbb` traffic** that dominated the historical alerts — if those are placeholder addresses the alerting pipeline may have been pointed at a fixture stream rather than live data.

## April 15 Countdown — KXCPI / KXFED

April 15 has already passed: **T+11 days** as of today (2026-04-26). The next-major-resolution flag in the project instructions is therefore stale. If those markets resolved on schedule they should appear in `logs/resolution_log.txt` — and they do not, because that log has been silent since 2026-04-06. Either the markets resolved and the resolution worker missed them, or they were postponed. Worth a manual check on Kalshi's public resolutions before the next briefing.

---
_Sources: `logs/monitor.log`, `logs/resolution_log.txt`, `logs/daily_refresh.log`, `data/polymarket/wallet_profiles.parquet`._
