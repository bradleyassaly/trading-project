# Weekly Performance Review — Week ending 2026-04-26

**Generated:** 2026-04-26 (automated scheduled run)
**Coverage:** Past 7 calendar days (2026-04-19 → 2026-04-26)

---

## Data Freshness Alert

Several upstream feeds appear stale relative to the review window. Findings flagged below; the report covers what is available and notes gaps where they distort interpretation.

| Source | Last activity | Days stale |
|---|---|---|
| `data/kalshi/paper_trades.db` | 2026-04-16 (latest `entry_ts`) | 10d |
| `data/polymarket/wallet_profiles.parquet` | 2026-04-02 (mtime) | 24d |
| `logs/monitor.log` | 2026-04-11 04:52 UTC (mtime) | 15d |

Because the strict 7-day window (since 2026-04-19) contains zero new paper trades and zero new monitor alerts, the activity sections below report against the **last 7 days of recorded activity** in each source and label that explicitly.

---

## 1. Paper Trade Performance by Signal Type

**Snapshot:** 67 total trades in `paper_trades.db`, **all in `open` status** — no closed/resolved/won outcomes recorded across the entire dataset. 66 of 67 are flagged `archived=1`. No trades were entered in the strict 7-day window.

| Platform | Signal type | Count | Avg size (USD) | Avg confidence |
|---|---|---:|---:|---:|
| kalshi | kalshi_time_decay | 33 | 7.98 | 1.00 |
| kalshi | kalshi_volume_spike | 30 | 7.24 | 1.00 |
| kalshi | smart_money | 2 | 5.00 | 1.00 |
| polymarket | smart_money | 1 | 5.00 | 1.00 |
| polymarket | strategy_specialist | 1 | 4.15 | 0.55 |

**Win rates: not computable.** `outcome` is null on every row and `exit_ts`/`exit_price` are unset. Closed-trade resolution does not appear to be flowing into the paper-trades table — this blocks every downstream metric the briefing was meant to produce (signal-family win rate, return distribution, threshold tuning).

**Concentration risk.** 94% of paper trades come from two Kalshi signal families (time_decay + volume_spike). Smart-money signals — the platform's stated edge — represent 4.5% of trades (3 of 67). The pipeline that should generate paper trades from smart-money alerts is materially under-firing.

---

## 2. Wallet Profile Health

`wallet_profiles.parquet` (mtime 2026-04-02) — 16,043 wallets profiled, 206 flagged `smart_money=True`.

| Bucket | Count | Notes |
|---|---:|---|
| Smart money (total) | 206 | early_win_rate ≥ 70% & ≥ 20 markets |
| Smart money & active | 118 | not flagged degraded |
| Smart money & degraded | 88 | **43% of cohort** |
| `is_highly_informed` | 89 | premium tier |
| `is_early_informed` | 206 | == smart_money flag |
| All wallets degraded | 4,098 | universe-wide |

> **Week-over-week comparison unavailable.** No prior parquet snapshot is retained — only `wallet_open_positions.parquet` (a different file) and the current `wallet_profiles.parquet`. Recommend writing a dated snapshot (`wallet_profiles_YYYY-MM-DD.parquet`) on each refresh so the briefing can produce delta counts.

### Demote candidates (60 wallets)

Smart-money wallets currently flagged degraded with `win_rate_30d < 0.4`. Top 10 by recency-of-decline:

| Wallet | win_rate_30d | win_rate_90d | Domain | Markets | Edge |
|---|---:|---:|---|---:|---:|
| 0xa7c1f914…0193 | 0.000 | 0.464 | geopolitics | 33 | 0.687 |
| 0x945a4925…a48c | 0.000 | 0.327 | other | 349 | 0.468 |
| 0xbfbebe72…c93c | 0.000 | 0.483 | geopolitics | 29 | 0.704 |
| 0x90b9be5f…1951 | 0.000 | 0.793 | geopolitics | 31 | 0.490 |
| 0x4e84ce15…ef69 | 0.000 | 0.000 | geopolitics | 36 | 0.472 |
| 0xf904a930…26e5 | 0.000 | 0.603 | geopolitics | 58 | 0.386 |
| 0x6195e441…4570 | 0.000 | 0.500 | geopolitics | 40 | 0.440 |
| 0x8a1e1e8f…bd03 | 0.000 | 0.458 | geopolitics | 26 | 0.581 |
| 0x0509d64a…6e98 | 0.000 | 0.565 | geopolitics | 31 | 0.442 |
| 0x51f28ce0…38ef | 0.000 | 0.613 | other | 31 | 0.461 |

Pattern: geopolitics-domain wallets are over-represented in the demote pool (8 of 10 above). Likely cohort effect — recent geopolitics resolutions (US/Iran ceasefire, Hungary PM, Trump operations) appear to have run against early-mover positioning.

### Promote candidates (88 wallets)

Not currently flagged smart_money but meet the criteria (`early_win_rate > 0.8`, `≥ 30 markets`, not degraded). Top 10 by edge:

| Wallet | early_win_rate | win_rate_30d | Domain | Markets | Edge |
|---|---:|---:|---|---:|---:|
| 0x74ff920a…be5b | 0.850 | 0.667 | other | 32 | 0.529 |
| 0x9d057897…de5a | 0.813 | 0.000 | geopolitics | 33 | 0.410 |
| 0x3e6b98d9…8a98 | 1.000 | 1.000 | other | 35 | 0.408 |
| 0x8948d005…76e5 | 0.857 | 1.000 | geopolitics | 33 | 0.372 |
| 0xba8dd483…009d | 0.889 | 1.000 | other | 31 | 0.359 |
| 0x884c2286…851f | 0.846 | 0.000 | other | 65 | 0.349 |
| 0x7e1a8251…d5a3 | 0.857 | 0.000 | other | 50 | 0.334 |
| 0x58c3a4cb…20b7 | 0.846 | 0.000 | other | 30 | 0.317 |
| 0xd01c6c60…7f9d | 0.875 | 0.857 | other | 78 | 0.312 |
| 0x52cad574…54f0 | 0.857 | 0.000 | other | 61 | 0.309 |

Note the bimodal recency: several promote candidates have `win_rate_30d=0.000` — likely no resolved markets in the 30-day window rather than zero wins. Sanity-check before auto-promoting; an "insufficient data" flag would help here.

### Active domains (smart-money cohort)

| Domain | Smart-money wallets |
|---|---:|
| other | 96 |
| geopolitics | 82 |
| crypto | 27 |
| politics | 1 |

`other` dominates, which suggests the domain classifier is under-resolving or that "other" is acting as a catch-all. Worth a labeling pass.

---

## 3. Top Markets and Signals

### Monitor alerts (last 7 days of recorded log activity, 2026-04-04 → 2026-04-11)

**Strict 7-day window: 0 alerts** (log not updated since 2026-04-11 — investigate).

| Tier | Count |
|---|---:|
| TIER1 | 3,126 |
| TIER2 | 15,327 |
| **Total** | **18,453** |

### Top markets by smart-money activity

| Market | Total alerts | Tier 1 alerts |
|---|---:|---:|
| Will inflation rise? | 1,136 | 994 |
| Rolex Monte Carlo Masters: Monfils vs Tallon | 873 | 43 |
| Copa Colsanitas: Bouzkova vs Udvardy | 539 | — |
| CS:GO: 3DMAX vs Voca | 476 | — |
| Rolex Monte Carlo: Norrie vs Miomi | 468 | — |
| US x Iran ceasefire by April 15? | — | 137 |
| ETH > $2,100 on April? | — | 94 |
| US x Iran ceasefire by April 30? | — | 67 |
| BTC > $70,000 on April? | — | 63 |
| Trump ends military operations | — | 43 |

The Tier-1 list is dominated by economics + geopolitics, which is on-thesis. The total-alerts list is heavily polluted by tennis and esports matches — likely Tier 2 noise that is not gated by domain. **Tier 2 is firing 5× more than Tier 1**, and most of the volume is in markets that are unlikely to be smart-money-driven.

### Most active watched wallets

| Wallet | Alerts |
|---|---:|
| 0x2005d16a84ce | 5,733 |
| 0x68146921df11 | 2,860 |
| 0x507e52ef684c | 2,825 |
| 0x972bf37cae72 | 1,796 |
| 0xaaa | 1,015 |
| 0x204f72f35326 | 712 |
| 0x35417b3d09d7 | 404 |

The presence of `0xaaa` (test fixture) at rank 5 indicates the log contains seeded/sample rows — confirm the parser is reading the production stream, not a smoke-test artifact. The top 4 are real-shaped addresses but are firing alerts at a rate that, if real, dwarfs anything previously profiled. Worth a single-wallet drill-down.

---

## 4. Recommended Threshold Adjustments

**Win-rate-based tuning is blocked** until paper-trade resolution lands in the database. Recommendations are inferential:

1. **Tighten Tier 2 domain filter.** 83% of last-week's alerts were Tier 2 and tennis/esports markets dominate the volume. If the strategy is economics + geopolitics + crypto, gate Tier 2 by `best_domain ∈ {economics, geopolitics, crypto}` to cut noise without affecting on-thesis signal.
2. **Tighten smart-money inclusion criteria.** With 88 of 206 smart-money wallets currently degraded (43%), the entry bar (`early_win_rate > 70% & ≥ 20 markets`) is admitting cohorts that don't sustain edge. Consider adding `win_rate_90d > 0.55` as a guardrail — would shrink the cohort but the dropouts are already underperforming.
3. **Hold position sizing flat.** With no closed-trade returns, do not change `size_usd` defaults this week. Today's mean trade sizes (kalshi_time_decay 7.98, kalshi_volume_spike 7.24, smart_money 5.00) are already conservative.

---

## 5. Next Week Priorities

1. **Fix paper-trade resolution.** Identify why `outcome`, `exit_ts`, `exit_price`, `return_pct` are null on all 67 trades. Without this, every win-rate-driven feedback loop in the briefing is dark.
2. **Investigate stale `monitor.log`.** The file hasn't been written since 2026-04-11. Check the monitor service status and the log handler — it may be writing to a rotated file or failing silently.
3. **Refresh `wallet_profiles.parquet`.** The current snapshot is 24 days old. April 15 KXCPI/KXFED resolutions should have produced new resolved trades and shifted win rates; the briefing is missing all of that signal.
4. **Add dated wallet-profile snapshots.** `wallet_profiles_YYYY-MM-DD.parquet` so week-over-week deltas (the question this report could not answer) become trivial.
5. **Smart-money pipeline triage.** Only 3 of 67 paper trades were smart_money signal type. Either the alert→paper-trade bridge is broken or the firing thresholds are too tight. Decide which.
6. **Confirm production log stream.** `0xaaa` test wallet appearing in monitor.log warrants a parser and ingestion check.
7. **Process the demote/promote queue.** 60 demote, 88 promote candidates pre-computed above; review and apply once the wallet profile is refreshed.

---

*Generated by `weekly-briefing-monday` scheduled task. Source files: `data/kalshi/paper_trades.db`, `data/polymarket/wallet_profiles.parquet`, `logs/monitor.log`.*
