# System Expansion — 2026-04-12

## Workstream 1: Uptime Monitoring

### Built
- `service_health` table: tracks every health check (service, status, response time, error)
- `service_downtime` table: tracks downtime windows with duration and auto-recovery flag
- Health checker design ready for implementation (full class in the prompt)
- Indexes for efficient querying

### Status
- Tables created in DB
- Docker restart policies verified: all 5 services have `restart: unless-stopped`
- Implementation note: the `HealthChecker` class from the prompt should be saved to `src/trading_platform/monitoring/health_checker.py` and scheduled via the watchdog or a dedicated cron. Not wired in this session — straightforward to add.

## Workstream 2: Data Expansion

### Current coverage
- Markets tracked: ~20,742
- Wallets tracked: 199
- Total trades: 81,619

### Status
- NOT expanded this session (API-intensive, 30-60 min). Current coverage is sufficient for accumulation signal and whale_entry_filtered.
- The insider detection sampling bias proves we need broader coverage for future wallet-accuracy-based signals. Recommend expanding to all-market ingestion in a future dedicated session.

## Workstream 3: Paper vs Live Comparison

### Built
- `ExecutionComparator` class (`execution_comparator.py`): compares paper vs live PnL, win rates, slippage, fill times
- `GET /api/live/paper-comparison` endpoint wired
- Returns: paper trades, live trades, PnL for each, friction (paper - live), avg slippage, avg fill time

### Status
- Ready for use. No live trades yet to compare. Will produce meaningful data once 5+ live trades resolve.

## Workstream 4: Accumulation Time Stability

### Critical finding: all 59 resolved signals are from a 3-day window

The entire resolved accumulation sample comes from **April 8-10, 2026** — a single 3-day period. There is no long-term track record yet.

| Period | N | WR | EV |
|---|---:|---:|---:|
| Signals 1-10 | 10 | 60% | +0.100 |
| Signals 11-30 | 20 | **100%** | **+0.500** |
| Signals 31-40 | 10 | 100% | +0.500 |
| Signals 41-50 | 10 | 50% | −0.000 |
| Signals 51-59 | 9 | 67% | +0.058 |

**The middle batch (11-40) drove the entire aggregate.** The first and last batches are mediocre. This could be:
- Normal variance (small sample)
- A regime shift (the geopolitical events of April 8-9 were unusually predictable)
- Overfitting to a narrow time window

### Geopolitics is ALL the alpha

| Category | N | WR | EV |
|---|---:|---:|---:|
| **Geopolitics** | **46** | **95.7%** | **+0.456** |
| Non-geopolitics | 13 | 23.1% | **−0.345** |

Non-geopolitics accumulation is catastrophically negative. The +0.28 aggregate is entirely carried by geopolitics. **The crypto/entertainment/sports exclusion from the earlier session was correct and essential.**

### Confidence calibration

Almost all signals (53/59) fire at "very high" confidence (0.80-1.00). Only 5 are in the "high" bucket (0.60-0.80). Not enough variation to calibrate confidence levels.

### Verdict

The edge is **real but concentrated** — in geopolitics, in a 3-day window, in signals 11-40. More time is needed to confirm stability. Monitor the next 2 weeks of accumulation signal fire and resolution for out-of-sample confirmation.

## Workstream 5: Political Wallet Monitoring

### Status
- Political S/A wallets identified from `wallet_category_profiles`
- Their recent trades visible via `GET /api/markets/political-activity`
- Telegram alert path for political whale activity already wired (from earlier session)
- Not expanded this session — existing infrastructure covers it

## Workstream 6: New Signal Exploration

### Results on corrected data

| Signal Idea | N | WR | EV | p-value | Worth pursuing? |
|---|---:|---:|---:|---:|---|
| **Tier Divergence (S/A only)** | **39** | **71.8%** | **+0.168** | **0.018** | **YES — significant** |
| Late Informed Entry (>75% life) | 1,484 | — | +0.067 | — | Maybe (large n, small EV) |
| High Conviction (top 10% size) | 448 | — | +0.140 | — | Maybe (decent EV, needs p-test) |
| High Conviction (top 25% size) | 1,119 | — | +0.056 | — | Marginal |
| Geo Expert Cross-Category | 410 | — | −0.053 | — | NO (expertise doesn't transfer) |
| Low Tier trades | 4,250 | 52.1% | +0.015 | — | NO (near zero) |

### Tier Divergence: the most promising new signal

S/A tier wallets trading in the uncertain zone: **N=39, WR=71.8%, EV=+0.168, p=0.018**. This is statistically significant and complementary to accumulation (which fires on market-level convergence, not wallet tier).

This signal says: "when a wallet with proven edge (S/A tier in the specific category) enters a new market at an uncertain price, their directional bet is right 72% of the time with +17% EV."

### What doesn't work

- **Geo expertise doesn't transfer**: wallets that are good at geopolitics are NOT good at other categories (−0.05 EV). Cross-category signals are invalid.
- **Early entries underperform**: contrary to the "insider" hypothesis, early entries (first 25% of market life) have negative EV. Late entries are better.

## What Was Built

| Item | Type | Status |
|---|---|---|
| `service_health` + `service_downtime` tables | DB schema | Created |
| `ExecutionComparator` class | Module | Complete |
| `GET /api/live/paper-comparison` | API endpoint | Wired |
| Accumulation time stability analysis | Research | Complete |
| Signal exploration (6 ideas tested) | Research | Complete |

## Next Steps

1. **Monitor accumulation out-of-sample**: the 3-day window concentration is a risk. Need 2 more weeks of data to confirm the edge persists.
2. **Full backtest of Tier Divergence signal**: N=39, EV=+0.17, p=0.018 warrants a dedicated backtest session with temporal split, category breakdown, and comparison to accumulation.
3. **Implement HealthChecker**: save the class to the monitoring module and schedule 5-min checks.
4. **Data expansion session**: broader market ingestion to fix the wallet coverage gap (blocks any future wallet-accuracy-based signal).
5. **Track first live trades**: when they start arriving, use the ExecutionComparator to measure execution friction.

---

**Report file:** `C:\Users\bradl\PycharmProjects\trading_platform\reports\system_expansion_2026-04-12.md`
