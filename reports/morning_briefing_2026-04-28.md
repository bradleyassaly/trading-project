# Morning Briefing — 2026-04-28

Generated: 2026-04-28 (automated `daily-briefing-8am` scheduled task)

## TL;DR

**System health is the headline today, not signals.** The three pipeline logs the briefing pulls from (`monitor.log`, `resolution_log.txt`, `daily_refresh.log`) have not been updated in 17–22 days. There are zero TIER 1 / TIER 2 alerts in the last 24h because the monitor stopped writing on 2026-04-11. No live action items today until the collectors are restarted. Yesterday's `nightly-backfill-check-7am` briefing already flagged this and the outage has not been resolved.

---

## 1. Smart-money alerts overnight

**Last 24h (2026-04-27 → 2026-04-28): 0 TIER 1 alerts, 0 TIER 2 alerts.**

The most recent entry in `logs/monitor.log` is `2026-04-11 04:52:25` — the monitor has been silent for ~17 days. There is nothing overnight to act on.

For context, here is the last full window the monitor produced (2026-04-05 through 2026-04-11, 18,453 alerts total):

| Date | Alerts |
|---|---|
| 2026-04-05 | 10,042 |
| 2026-04-06 | 7,394 |
| 2026-04-07 | 521 |
| 2026-04-08 | 192 |
| 2026-04-09 | 104 |
| 2026-04-10 | 184 |
| 2026-04-11 | 16 |

Direction split across that historical window: **YES 15,282 / NO 3,171** (~83% YES). Top historical markets by alert count were *"Will inflation rise?"* (1,136), *Rolex Monte Carlo Masters: Monfils vs Tallon* (873), and *Copa Colsanitas: Bouzkova vs Udvardy* (539). The high-volume TIER 2 wallets in that window were `0x2005d16a84ce` (5,733), `0x68146921df11` (2,860), and `0x507e52ef684c` (2,825) — those look like they were firing on sports/esports markets, not the smart-money domains we trade.

## 2. Newly resolved markets

**None in the last 24h.** `logs/resolution_log.txt` has only three lines, the most recent dated `2026-04-06 13:00 UTC`:

```
[2026-04-05 01:27 UTC] Checked 65 open trades, resolved 0 — Cash $9.76 | P&L $0.00 | WR 0.0%
[2026-04-05 13:00 UTC] No open trades to check.
[2026-04-06 13:00 UTC] No open trades to check.
```

The resolution checker has not run in 22 days.

## 3. System health

| Pipeline | Last log entry | Status |
|---|---|---|
| `monitor.log` | 2026-04-11 04:52 UTC | **STALE — 17d** |
| `resolution_log.txt` | 2026-04-06 13:00 UTC | **STALE — 22d** |
| `daily_refresh.log` | 2026-04-06 01:37 UTC | **STALE — 22d** (only contains a `test` line, never a real refresh) |
| `wallet_trade_sync.log` | 2026-04-06 10:02 UTC | **STALE — 22d** |
| `wallet_position_sync.log` | 2026-04-06 06:16 UTC | **STALE — 22d** |
| `compute_signals.log` | 2026-04-06 09:12 UTC | **STALE — 22d** |

Scheduler-side logs *are* current — `logs/scheduler/bankroll_refresh.log`, `alpha_score_recompute.log`, `build_leaderboard.log`, `calibration_report.log`, and `circuit_breaker_daily_reset.log` were all written today (2026-04-28). So scheduler infrastructure is up; what's broken is the top-level monitor / refresh / sync pipeline (or those processes were redirected elsewhere and the briefing is reading the wrong files).

**Last successful nightly refresh on record:** none. `daily_refresh.log` only contains a single test stub from 2026-04-06.

Yesterday's `nightly-backfill-check-7am` briefing additionally noted that the manual Goldsky backfill (`backfill-goldsky-fills --strategy balanced --limit 200 --skip-existing`) is hitting Goldsky subgraph statement timeouts on every wide-range fill query, while single-token smoke queries return fine — so the backfill code path likely needs to chunk the time range smaller or back off / retry. That diagnosis still stands; nothing about it has changed in 24 hours.

## 4. Wallet intelligence snapshot

From `data/polymarket/wallet_profiles.parquet`:

- **Total wallets profiled:** 16,043
- **Smart money (`is_early_informed == True`):** 206
- **Highly informed:** 89
- **Degraded:** 4,098
- **Best-domain mix among smart money:** other 96, geopolitics 82, crypto 27, politics 1

**Top 5 smart-money wallets by confidence score:**

| Wallet | Early WR | n trades | Best domain | Confidence |
|---|---|---|---|---|
| `0x94746ed6c69bdd5d5711b18d781d3978ef705091` | 74.2% | 62 | other | 1.463 |
| `0xa7c1f914724f1c37a1e723ce5c9ac295ec8e0193` | 93.8% | 16 | geopolitics | 1.370 |
| `0x945a49252f772a10c6ddd1d1e1e24ee20438a48c` | 71.8% | 85 | other | 1.343 |
| `0x7ea571c40408f340c1c8fc8eaacebab53c1bde7b` | 79.5% | 166 | other | 1.313 |
| `0xe9c6312464b52aa3eff13d822b003282075995c9` | 100.0% | 40 | crypto | 1.311 |

The wallet roster itself is intact — `wallet_profiles.parquet` is just the file, last refreshed 2026-04-05. The issue is purely that the monitor / refresh / sync pipelines aren't producing fresh activity to score against this roster.

## 5. Action items

1. **Restart the monitor / refresh / sync collectors** (or confirm they were re-routed). Until they are writing to `logs/` again, this briefing is operating on a 17–22 day blackout.
2. **No live signals to act on today.** No overnight TIER 1 alerts on liquid markets — because there are no overnight alerts at all.
3. **Verify the resolution checker** before any KXCPI/KXFED-related positioning — it has not run in 22 days.
4. **Fix the Goldsky backfill timeout** (chunk the time window, add backoff, or switch endpoints) — flagged in yesterday's briefing, still outstanding.
5. **Confirm the briefing's expected log paths** are still correct given that scheduler jobs are using a different directory.

## 6. April 15 KXCPI / KXFED resolution countdown

**April 15, 2026 has already passed — 13 days ago.** Today is 2026-04-28.

The project instructions list "April 15" as the next major resolution date, but that date is in the past. Likely interpretations:

- **April 15, 2027** → 352 days away (next annual cycle).
- The project instructions are stale and the next CPI / FED resolution date should be updated.

Either way, there is no near-term April 15 deadline driving today's actions. Recommend reviewing CPI / FED market expirations on Kalshi / Polymarket and refreshing the project's headline resolution date.

---

**Do not trade off this briefing today.** Restore collectors first.
