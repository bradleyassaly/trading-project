# 🌅 Morning Briefing — Wednesday, April 08, 2026 [UPDATED: daily-briefing-8am]

**Generated:** 2026-04-08 ~07:00 local | **Days to KXCPI/KXFED Resolution:** 7 days (April 15)

---

## ⚠️ Nightly Backfill Check — FAILED TO RUN

**Backfill status: Did NOT complete successfully**

The last `daily_refresh.log` entry is from **2026-04-06 01:37 UTC** — only 1/4 steps ran, 0 items refreshed, elapsed time 0 seconds. No run detected for 2026-04-07 or 2026-04-08.

**Attempted to run backfill manually** (automated task):
```
.venv\Scripts\python.exe -m trading_platform.cli data polymarket backfill-goldsky-fills --strategy balanced --limit 200 --skip-existing
```
❌ **Could not execute** — The CLI requires Python 3.11+ (uses `datetime.UTC` introduced in 3.11), but only Python 3.10 is available in the Linux sandbox environment. The Windows `.venv` cannot execute on Linux. **Manual run required on the Windows host.**

---

## 📁 Data Status

| Item | Value |
|------|-------|
| Resolved market files | **2,027** (+ 1 combined.parquet = 2,028 total) |
| Newest market file mtime | 2026-04-05 00:53 EDT |
| Wallet profiles last updated | **2026-04-05 00:21 EDT** (3 days stale) |
| Daily refresh last ran | 2026-04-06 01:37 UTC (degraded — 0 items refreshed) |
| Smart money wallets | **204** (early_win_rate > 70%, is_early_informed = True) |

---

## ⚙️ System Health

| Component | Status | Detail |
|-----------|--------|--------|
| Daily Refresh | ❌ Down | Last successful run unknown; 2026-04-06 ran 0 items |
| Goldsky Fills | ⚠️ Stale | No new files since 2026-04-05 |
| Wallet Profiles | ⚠️ Stale | Last updated 2026-04-05 — 3 days old |

---

## 🎯 Action Required

1. **Run backfill manually on Windows host:**
   ```
   cd C:\Users\bradl\PycharmProjects\trading_platform
   .venv\Scripts\python.exe -m trading_platform.cli data polymarket backfill-goldsky-fills --strategy balanced --limit 200 --skip-existing
   ```

2. **Investigate daily_refresh degradation** — why is only Step 1/4 running with 0 items refreshed?

3. **7 days until April 15 KXCPI/KXFED resolution** — stale wallet profiles may affect signal quality.

---

## ⏳ April 15 Countdown

> **7 days** until KXCPI and KXFED resolution (April 15, 2026)

With wallet profiles 3 days stale and no successful backfill, incoming smart money signals may be incomplete. Prioritize fixing the refresh pipeline today.

---

---

## 📊 Smart Money Alerts — Overnight (Apr 7–8)

**Total alerts:** 520 &nbsp;|&nbsp; **TIER 1:** 352 &nbsp;|&nbsp; **TIER 2:** 168

### Directional Bias
- YES signals: **309 (88%)**
- NO signals: **43 (12%)**
- Net: **Strongly bullish / YES overnight**

---

### 🔥 TIER 1 — Top Markets

| Market | Alerts | Direction |
|--------|--------|-----------|
| Will inflation rise? | 259 | YES ⚠️ (see note) |
| ETH above $2,100 on Apr expiry? | 46 | YES ✅ |
| BTC above $70,000 on Apr expiry? | 15 | YES ✅ |
| Upper Austria Ladies Linz (Bejlek vs Udvardy) | 7 | YES |
| Will Real Madrid CF win on 2026-04-07? | 4 | YES |

> ⚠️ **Alert:** Wallet `0xaaa` generated 259 repetitive $200 trades on "Will inflation rise?" at ~30-second intervals throughout the night. This pattern suggests a **test wallet or bot noise** — not a genuine smart money signal. Treat with caution.

---

### 🔥 TIER 2 — Top Markets

| Market | Alerts | Direction |
|--------|--------|-----------|
| Will inflation rise? | 37 | YES |
| Michigan win 2026 NCAA Tournament? | 27 | YES |
| Connecticut win 2026 NCAA Tournament? | 25 | YES |
| Sarasota: Rybakov vs Damm | 19 | YES |
| Mexico City: Martin vs Napolitano | 10 | YES |

---

### 🧠 Top Active Smart Money Wallets

| Wallet | Alerts | Domain | Win Rate | Key Trade |
|--------|--------|--------|----------|-----------|
| 0xe9c6312464b5 | 60 | **Crypto** | **100%** | ETH YES ($2,700 single trade) |
| 0x204f72f35326 | 16 | Mixed | — | Tennis + soccer bets |
| 0x6d3c5bd13984 | 3 | — | — | **PSG NO — $4,636** (biggest trade) |
| 0x849ccb590793 | 4 | — | — | Real Madrid YES |

**Smart money universe:** 206 wallets (avg early win rate: 81.7%)

#### Top 5 by Confidence Score

| Wallet | Win Rate | Trades | Domain |
|--------|----------|--------|--------|
| 0xe9c6312464b5 | 100.0% | 40 | Crypto |
| 0xa7c1f914724f | 93.75% | 16 | Geopolitics |
| 0x94746ed6c69b | 74.19% | 62 | Other |
| 0x7ea571c40408 | 79.52% | 166 | Other |
| 0x945a49252f77 | 71.76% | 85 | Other |

---

## ✅ Resolved Markets

**None in the last 24 hours.**
- Apr 5 check: 65 open trades, 0 resolved | Cash: $9.76 | P&L: $0.00 | WR: 0%
- Apr 6 check: No open trades

---

## 🎯 Action Items

### Act Today
1. **ETH > $2,100 (April expiry)** — `0xe9c6312464b5` (100% win rate, 60 TIER 1 alerts, up to $2,700/trade). Strongest overnight signal. Verify current odds before entering.
2. **BTC > $70,000 (April expiry)** — Same wallet, 15x YES. Correlated crypto macro thesis.

### Watch / Verify
3. **PSG NO** — $4,636 single bet from `0x6d3c5bd13984`. Match was Apr 7 — check outcome.
4. **Real Madrid YES on Apr 7** — 3 smart money wallets in agreement. Verify match result.
5. **NCAA (Michigan + Connecticut YES)** — 52 TIER 2 alerts from one wallet. Check bracket.

### Investigate
6. **"Will inflation rise?" alert flood** — 296 alerts likely noise from test wallets. Confirm `0xaaa`/`0xbbb` are not live wallets before acting on inflation market.

---

*Auto-generated by daily-briefing-8am scheduled task (2026-04-08). Previous system health section above from nightly-backfill-check.*
