# Morning Briefing — 2026-04-27

Generated: 2026-04-27 (automated `daily-briefing-8am` scheduled task)

## TL;DR

**The monitoring stack appears to be down.** Every log file we depend on is stale by 16–22 days. Zero TIER 1 / TIER 2 alerts have fired in the last 24 hours, no resolutions have been logged since 2026-04-06, and the nightly `daily_refresh` job last ran on 2026-04-06 with what looks like a test payload (`REFRESH | test | all=0`). Yesterday's nightly-backfill-check briefing flagged the same outage — it has now persisted into a second consecutive day with no progress. **Do not trade off this briefing today.** Restore collectors first.

## System Health

| Component                       | Last write (UTC)        | Age vs. today    | Status |
|---------------------------------|-------------------------|------------------|--------|
| `logs/monitor.log`              | 2026-04-11 04:52        | 16 days stale    | DOWN   |
| `logs/resolution_log.txt`       | 2026-04-06 13:00        | 21 days stale    | DOWN   |
| `logs/daily_refresh.log`        | 2026-04-06 01:37        | 21 days stale    | DOWN   |
| `logs/orderflow.log`            | 2026-04-05 04:54        | 22 days stale    | DOWN   |
| `logs/wallet_trade_sync.log`    | 2026-04-06 10:02        | 21 days stale    | DOWN   |
| `logs/wallet_position_sync.log` | 2026-04-06 06:16        | 21 days stale    | DOWN   |
| `logs/compute_signals.log`      | 2026-04-06 09:12        | 21 days stale    | DOWN   |

**Gaps:** All collectors are silent. The most recent activity in the entire `logs/` directory is `monitor.log` on 2026-04-11. There are also several `wallet_intelligence.db.CORRUPTED_*` snapshots in `data/polymarket/` (most recent: 2026-04-14), suggesting an unresolved DB corruption event around that date that may be the root cause of the monitor going down a few days later.

**Daily refresh status:** Did NOT run last night. The most recent entry is a 0-second smoke run from 2026-04-06: `--- START daily-refresh | 2026-04-06 01:37 UTC | initial=False --- ... REFRESH | test | all=0 --- END daily-refresh | 2026-04-06 01:37 UTC | elapsed=0s ---`. No real refresh has run in 21 days.

## Smart Money Alerts — Last 24 Hours

**Count: 0 TIER 1 / 0 TIER 2 alerts in the last 24 hours.**

This is consistent with the monitor being offline, not with a quiet market. Lifetime totals in `monitor.log` are **3,126 TIER 1** and **15,327 TIER 2** entries across roughly 24 unique smart-money wallets and 400+ markets, so an empty 24-hour window is highly anomalous.

Top markets, top wallets, and direction breakdown for the overnight window: nothing to report — there are no overnight signals.

## Resolved Markets

**None recorded.** `logs/resolution_log.txt` has only three historical entries; the most recent is 2026-04-06 13:00 UTC ("No open trades to check."). No resolutions have been logged since.

Caveat: the resolution logger is offline, so any markets that resolved on-chain in the last three weeks (including the April 15 KXCPI / KXFED set) will not appear here. Verify outcomes externally before assuming any P&L.

## Smart Money Wallet Universe

From `data/polymarket/wallet_profiles.parquet`:

- Total profiles: **16,043**
- Smart money wallets (`is_early_informed == True`): **206**

Top 5 by `confidence_score`:

| Wallet                                          | Early WR | Uncertain Early Trades | Best Domain  | Confidence |
|-------------------------------------------------|---------:|-----------------------:|--------------|-----------:|
| 0x94746ed6c69bdd5d5711b18d781d3978ef705091      |   74.19% |                     62 | other        |     1.4631 |
| 0xa7c1f914724f1c37a1e723ce5c9ac295ec8e0193      |   93.75% |                     16 | geopolitics  |     1.3697 |
| 0x945a49252f772a10c6ddd1d1e1e24ee20438a48c      |   71.76% |                     85 | other        |     1.3435 |
| 0x7ea571c40408f340c1c8fc8eaacebab53c1bde7b      |   79.52% |                    166 | other        |     1.3128 |
| 0xe9c6312464b52aa3eff13d822b003282075995c9      |  100.00% |                     40 | crypto       |     1.3114 |

These are static profile facts (built from historical fills) and are not affected by the collector outage. The parquet itself, however, has not been refreshed since 2026-04-05 — so newly graduated smart-money wallets in the last three weeks are not represented.

## Action Items

1. **Get collectors back online.** Investigate why `monitor`, `daily_refresh`, and the wallet syncs all stopped on 2026-04-06 / 11. The `wallet_intelligence.db.CORRUPTED_20260414_*` files suggest a DB event on 2026-04-14 may have crashed downstream jobs.
2. **Restore real `daily_refresh`.** The last log entry is a smoke test, not a real run. Confirm the scheduler is wired to the production refresh command.
3. **Validate `wallet_intelligence.db` integrity.** Several corrupted snapshots exist; make sure the live DB is the intended copy before regenerating signals.
4. **Back-populate the gap.** Once collectors are healthy, re-run signal compute over the 2026-04-06 → 2026-04-27 window so we don't drop 21 days of smart-money activity.
5. **Do NOT act on Tier 1 signals today.** There are no fresh signals to act on; any trade decision today should come from a manual market read, not from this (silent) monitor.

## April 15 Countdown

**T-12 days. April 15 has already passed** (today is 2026-04-27). KXCPI and KXFED would already have resolved on Kalshi. Because `resolution_log.txt` has been silent since 2026-04-06, those outcomes are not reflected locally — confirm them directly against Kalshi and update `data/kalshi/paper_trades.db` if needed.

The next macro Kalshi event isn't derivable from these stale logs; pull the next CPI / FOMC dates from the Kalshi event calendar once data feeds are healthy again.

---
*This briefing was generated automatically and reflects only the state of local files at run time. The collector outage is the dominant signal today — re-run this briefing after restoring data feeds.*
