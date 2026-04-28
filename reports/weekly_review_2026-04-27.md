# Weekly Performance Review — Week ending 2026-04-27

**Generated:** 2026-04-27 (automated scheduled run)
**Coverage:** Past 7 calendar days (2026-04-20 → 2026-04-27)

---

## Data Freshness Alert

Critical: every primary data source covering this review window is stale. The strict 7-day window contains zero fresh activity. Last week's report (`reports/weekly_review_2026-04-26.md`) raised the same alarm and the situation has not improved — three of four feeds have not advanced since that report ran.

| Source | Last activity | Days stale (vs 2026-04-27) |
|---|---|---:|
| `data/kalshi/paper_trades.db` (mtime) | 2026-04-25 02:02 UTC | 2d |
| `data/kalshi/paper_trades.db` (latest `entry_ts`) | 2026-04-16 | 11d |
| `data/polymarket/wallet_profiles.parquet` (mtime) | 2026-04-05 04:21 UTC | 22d |
| `logs/monitor.log` (mtime / last entry) | 2026-04-11 04:52 UTC | 16d |

Because the strict 7-day window is empty, activity sections fall back to the **last 7 days of recorded activity** in each source and label that explicitly.

---

## 1. Paper Trade Performance by Signal Type

**Snapshot:** 67 total trades in `paper_trades.db`, **all in `open` status** — no closed/resolved/won outcomes recorded across the entire dataset. 66 of 67 are flagged `archived=1`; only the most recent trade (2026-04-16) remains active.

| Platform | Signal type | Count | Avg size (USD) | Avg confidence |
|---|---|---:|---:|---:|
| kalshi | kalshi_time_decay | 33 | 7.98 | 1.00 |
| kalshi | kalshi_volume_spike | 30 | 7.24 | 1.00 |
| kalshi | smart_money | 2 | 5.00 | 1.00 |
| polymarket | smart_money | 1 | 5.00 | 1.00 |
| polymarket | strategy_specialist | 1 | 4.15 | 0.55 |

**Win rate:** Cannot be computed. The schema lacks the `won` and `stake` columns the task script assumed; equivalents are `outcome` (all NULL) and `size_usd`. No `outcome` values are populated, so wins/losses are unknown for every trade.

**Active position:** 1 open, non-archived trade.

| ID | Ticker | Signal | Platform | Entry price | Size USD | Entered |
|---:|---|---|---|---:|---:|---|
| 67 | 0x9a353170f9d6bb59af | strategy_specialist | polymarket | 0.014 | 4.15 | 2026-04-16 |

**Open trades by signal_type (totals):**

| Signal type | Count | Total stake | Avg entry | Avg confidence |
|---|---:|---:|---:|---:|
| kalshi_time_decay | 33 | 263.18 | 50.97 | 1.000 |
| kalshi_volume_spike | 30 | 217.05 | 62.85 | 1.000 |
| smart_money | 3 | 15.00 | 33.40 | 1.001 |
| strategy_specialist | 1 | 4.15 | 0.014 | 0.553 |

**Read:** the paper-trade pipeline is producing entries but not closing them. Either the resolver is not running or the trades are still pre-resolution. With 0 settlements, this report cannot make any signal-quality judgments. **This is a blocker for every recommendation in §4.** The position has been unchanged for two consecutive weekly reviews.

---

## 2. Wallet Profile Health

Source: `data/polymarket/wallet_profiles.parquet` — file mtime 2026-04-05, **22 days stale**. All counts below are from that snapshot. No "this week vs last week" delta is computable: no historical wallet_profiles snapshots exist on disk (`backups/`, `data/polymarket/_deep_dive/` both empty for wallet_profiles), so promotion/demotion churn cannot be observed.

| Cohort | Count |
|---|---:|
| Total wallets profiled | 16,043 |
| Smart money | 206 |
| Highly informed | 89 |
| Early informed | 206 |
| Degraded (any) | 4,098 |

**Demotion candidates (smart_money AND is_degraded):** 88 of 206 smart-money wallets (43%) carry the `is_degraded` flag. This is unchanged from the prior week and is the single largest health concern in the dataset — nearly half the curated cohort is flagged for degradation.

**Promotion candidates (early_win_rate ≥ 70%, ≥ 20 unique resolved markets, not currently smart_money):**
- 1,129 wallets meet the raw threshold
- 661 also pass `~is_degraded` and are clean promotion candidates
- 32 wallets are already marked `is_highly_informed` but not `smart_money` — these are the strongest, most-vetted candidates and should be promoted first

**Domain mix among current smart money (best_domain):**

| Domain | Wallets |
|---:|---|
| other | 96 |
| geopolitics | 82 |
| crypto | 27 |
| politics | 1 |

Geopolitics-focused wallets dominate the curated cohort. "Politics" is functionally absent (n=1).

**Top smart-money wallets by edge (representative):**

| Wallet | early_win_rate | win_rate_30d | win_rate_90d | edge | markets | best_domain | degraded? | highly_informed? |
|---|---:|---:|---:|---:|---:|---|:---:|:---:|
| 0xbfbebe72…b3c93c | 0.900 | 0.000 | 0.483 | 0.704 | 29 | geopolitics | yes | no |
| 0xa7c1f914…8e0193 | 0.938 | 0.000 | 0.464 | 0.687 | 34 | geopolitics | yes | yes |
| 0x514550b8…b71f46 | 1.000 | 0.000 | 0.867 | 0.685 | 15 | geopolitics | no | no |
| 0x849ccb59…a4009 | 0.786 | 0.857 | 0.583 | 0.643 | 72 | other | yes | no |
| 0x94746ed6…705091 | 0.742 | 0.250 | 0.286 | 0.596 | 300 | other | yes | no |

**Read:** the top-of-list wallets show the same pattern flagged last week — high `early_win_rate` but `win_rate_30d = 0.000`. That divergence is what drives the `is_degraded` flag and is the right signal. The 32 highly-informed-but-not-smart-money wallets are the cleanest promotion lever; 88 degraded smart-money wallets are the cleanest demotion lever. Neither set has moved because the parquet hasn't refreshed.

---

## 3. Top Markets and Signals from the Week

Source: `logs/monitor.log` — last entry 2026-04-11. **Strict past-7-day window contains 0 alerts.** Counts below come from the most recent 7-day window of recorded data (2026-04-04 → 2026-04-11).

**Alert volume (last 7 days of recorded data):**

| Tier | Count |
|---:|---:|
| Tier 1 | 3,126 |
| Tier 2 | 15,327 |
| **Total** | **18,453** |

Tier 2 is ~5× Tier 1 — consistent with a wider-aperture screen feeding a tighter conviction tier.

**Side mix:** YES 15,282 / NO 3,171 (~83% YES). Heavy YES skew is worth investigating — either a real market bias in the universe being monitored or an asymmetry in the alerter's logic.

**Top markets (most active, last 7 days of recorded data):**

| Alerts | Market |
|---:|---|
| 1,136 | Will inflation rise? |
| 873 | Rolex Monte Carlo Masters: Gael Monfils vs Tallon |
| 539 | Copa Colsanitas: Marie Bouzkova vs Panna Udvardy |
| 476 | Counter-Strike: 3DMAX vs Voca (BO3) - PGL Buchares… |
| 468 | Rolex Monte Carlo Masters: Cameron Norrie vs Miomi… |
| 464 | Counter-Strike: Legacy vs Inner Circle Esports (BO… |
| 404 | Rolex Monte Carlo Masters: Valentin Vacherot vs Ju… |
| 397 | Bucharest Open: Mariano Navone vs Daniel Merida Ag… |
| 308 | Rolex Monte Carlo Masters Qualification: David Go… |
| 299 | Upper Austria Ladies Linz: Sloane Stephens vs Tatj… |

The "Will inflation rise?" market sits at the top — relevant to the upcoming KXCPI/KXFED resolutions flagged in project context. Most of the rest are tennis/CS:GO matches; sports markets dominate the alert stream.

**Most active watched wallets (last 7 days of recorded data):**

| Alerts | Wallet |
|---:|---|
| 5,733 | 0x2005d16a84ce |
| 2,860 | 0x68146921df11 |
| 2,825 | 0x507e52ef684c |
| 1,796 | 0x972bf37cae72 |
| 1,015 | 0xaaa |
| 712 | 0x204f72f35326 |
| 404 | 0x35417b3d09d7 |
| 224 | 0x1cdd071bb612 |
| 198 | 0xe9c6312464b5 |
| 185 | 0x2d6ac4f70307 |

The top wallet (`0x2005d16a84ce`) drives ~31% of all alerts in the window. If that wallet's profile has degraded, it is dominating the alert stream by sheer volume rather than edge — cross-check against `wallet_profiles.parquet` once it refreshes.

---

## 4. Recommended Threshold Adjustments

**Cannot be made.** Win-rate data is unavailable: every trade in `paper_trades.db` is `outcome IS NULL` and `status = 'open'`. Until the resolver runs and populates `outcome` / `return_pct`, there is no basis for tuning confidence floors, signal-score cutoffs, or smart-money edge thresholds.

What we'd want to compute once data is flowing:
- Realized win rate by `signal_type` × `confidence` decile (drives confidence floor)
- Win rate by `smart_money_edge` decile (drives edge threshold)
- Win rate by `weighted_net_volume` quintile (drives volume-spike threshold)
- Tier 1 vs Tier 2 alert conversion to settled paper-trade wins (drives tier promotion logic)

**Holding-pattern recommendation:** keep current thresholds frozen. Do not tighten on stale data, do not loosen without evidence.

---

## 5. Next Week Priorities

Restoration of data pipelines is the single dominant priority. Nothing else this report could recommend will be evidence-based until the feeds catch up.

1. **Refresh `wallet_profiles.parquet`.** 22 days stale. Re-run the wallet intelligence pipeline so promotion/demotion decisions can move forward. Acting on the 88 degraded smart-money wallets and 32 highly-informed promotion candidates depends on this being current.
2. **Restart `monitor.log` ingestion.** No entries since 2026-04-11. Either the monitor process is down or its log is being written elsewhere — confirm and restore.
3. **Run the paper-trade resolver.** 67 trades sit unresolved. Without `outcome` values, no signal-quality analysis is possible. This has been unchanged across two consecutive weekly reviews.
4. **Investigate `wallet_intelligence.db` corruption history.** Three `*.CORRUPTED_*` artifacts visible in `data/polymarket/`. Confirm `wallet_intelligence_clean.db` is the source of truth and that the corruptions are not still recurring.
5. **Promote the 32 vetted candidates** once the parquet refresh confirms they still qualify (`is_highly_informed` ∧ ¬`smart_money` ∧ ¬`is_degraded`).
6. **Demote or quarantine the 88 degraded smart-money wallets** (43% of cohort) once the refresh confirms their status. This is the highest-leverage cleanup available.
7. **KXCPI / KXFED watch.** April 15 resolution date noted in project context has already passed (today is 2026-04-27); confirm those markets resolved as expected and that any open paper trades against them have been settled in `paper_trades.db`.
8. **Snapshot `wallet_profiles.parquet` weekly.** No historical snapshots exist on disk, so week-over-week churn cannot be measured. Add a dated copy step to the daily refresh job.

---

## Appendix — Operational Notes

- Schema mismatch: the task script referenced `won` and `stake` columns; the actual `trades` schema uses `outcome` (NULL across all rows) and `size_usd`. Aggregation was adjusted; counts and avg-size figures above are correct.
- No write actions taken. This is a report-only run as specified for scheduled tasks.
- Comparison to last week: `reports/weekly_review_2026-04-26.md` raised the same data-freshness alerts. `paper_trades.db` mtime advanced by 9 days but no new `entry_ts` rows; `wallet_profiles.parquet` and `monitor.log` are unchanged. No observable progress on the pipeline restoration items 1–3 above.
