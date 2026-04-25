# Weekly Review — 2026-04-18 → 2026-04-24

## Summary

The week-long unattended run validated the operational hypothesis (the system stays up across all wallet/data ingestion services) but failed on the trading hypothesis (the paper-side scheduler died ~6h in and the system never recovered for the remaining 5+ days). After bringing the stack back up and patching the connection pool, we drained 56 frozen exits and pulled real numbers.

## Headline numbers (8-day window, post-drain)

| Metric | Value |
|---|---|
| Paper trades opened | 272 |
| Paper trades closed | 212 |
| Paper win rate | 37.3% |
| Paper realized PnL | **+$61.61** |
| Live trades placed (non-dry-run) | **0** |
| Hypotheses resolved | 164 |
| Hypothesis accuracy | 39.0% |
| Wallet trades freshness | 9 min (healthy) |
| Last paper trade fired | 14.6h ago (concerning) |

## What worked

- **Signal type winners (positive PnL, n ≥ 5)**: `wallet_reversal` (+$16.87, 48% WR), `market_maker_flip` (+$13.34, 33% WR but high volume), `whale_entry_filtered` (+$12.24, 50% WR), `cascade` (+$11.04, 48% WR), `specialist_entry` (+$9.32, 50% WR).
- **Category winners**: entertainment +$20.61 (47% WR), crypto +$9.93, politics +$8.91, sports +$7.82 (largest volume), geopolitics +$7.09 (50% WR).
- **Risk/reward asymmetry is favorable**: avg take_profit captures $1.82 vs avg stop_loss gives back $0.93 — ~2:1.
- **Backups, ingestion, wallet polling, live-collect** stayed healthy the entire week (separate pools).
- **Apr-18 gate exclusions validated**: `copyable_contrarian` 0/8 → -$3.81; `news_reactor` 2/10 → -$2.36; `no_position_entry` 1/6 → -$0.51. All three would have lost money if not gated.

## What didn't work

- **Postgres idle-session timeout** killed every paper-side scheduler task at 2026-04-19 02:30 UTC. Pool sizing fixed the leak but lacked recycle/keepalive logic. Patched today.
- **Specialist boost ineffective** in this sample: specialists 31.3% WR (n=16, +$3.73) vs generalists 37.8% WR (n=196, +$57.88). The cohort is small but does not show the expected lift. Worth re-running the walk-forward backtest.
- **`network_leader_entry` worst loser**: -$5.43 on 8 closed (25% WR). Hypothesis accuracy 30%. Candidate for next exclusion.
- **Specialist_entry**: only 10 closed; hypothesis accuracy 0/1. Not enough to evaluate yet.
- **0 live trades** for the entire week — the expanded `LIVE_TRADE_CATEGORIES` (politics/geopolitics/sports/crypto) didn't matter because the live executor's confidence/specialist gates are too tight on the current calibration.
- **Last paper trade 14.6h ago** despite fresh wallet activity → signal engine pipeline gap (separate from the freeze; likely related but needs investigation).

## Hypothesis accuracy by signal type (7d, n ≥ 5)

| Signal type | n | correct | acc % |
|---|---:|---:|---:|
| whale_entry | 10 | 7 | **70.0** |
| whale_entry_filtered | 19 | 11 | 57.9 |
| wallet_reversal | 10 | 5 | 50.0 |
| market_maker_flip | 31 | 15 | 48.4 |
| oversized_bet | 18 | 8 | 44.4 |
| cascade | 8 | 3 | 37.5 |
| network_leader_entry | 20 | 6 | 30.0 |
| accumulation | 24 | 6 | 25.0 |
| no_position_entry | 5 | 1 | 20.0 |
| news_reactor | 10 | 2 | 20.0 |
| copyable_contrarian | 7 | 0 | **0.0** |

## Incidents (5-day Postgres freeze)

- ~480 failed scheduler runs across 6 paper-side tasks. No alerts fired (per-failure path was firing, but the pattern was new — no escalation logic existed). Patched today: 1st/5th/25th-failure escalation + recovery alerts.
- 63 paper positions frozen unmanaged for 5+ days. Drained today via `/api/paper/check-exits` → 56 of 117 evaluated trades hit an exit immediately.
- **No** OOMs, **no** circuit-breaker halts, **no** Python tracebacks. Backups ran every day.

## Today's patches (2026-04-24)

1. `db_connection.py` — added TCP keepalives, `check=ConnectionPool.check_connection`, `max_idle=60s`, `max_lifetime=1800s`. Three layers vs the silent disconnect mode.
2. `task_scheduler.py` — escalating failure alerts (1st / 5th / 25th / every 100th) + recovery alerts. Eliminates the 480-message tail-spam mode while keeping the leading edge loud.
3. Drained 56 frozen exits (24 whale_mirror_exit, 16 stop_loss, 15 take_profit, 1 expiry).

## Outstanding (next session)

- Investigate why no paper trade has fired in 14.6h despite fresh wallet activity.
- Investigate why 0 live trades fired across 7 days despite expanded categories.
- Replay frozen-window signals against the patched scheduler to recover the lost evaluation surface.
