# Frontend Audit & Cleanup

**Date:** 2026-04-08
**Mode:** Diagnose → fix the high-impact pages, document the rest

---

## Headline

Backend API health is **excellent** — all 43 endpoints surveyed return 200. The frontend gap is **navigation and content drift**, not broken plumbing. Sidebar listed 9 items, half pointing at legacy/research surfaces (Market Monitor, Pipeline, Backtest, Whale Monitor) that don't fit the current Polymarket-only architecture. Dashboard `StatCards` were reading a 5-gate readiness API and skipping the bankroll / circuit-breaker / universe endpoints we built in the last few sessions. Paper Trading still showed a "Kalshi (legacy)" card and put velocity_detector trades alongside real whale signals, polluting the visible win rate.

This session fixes those three things and trims the navigation. The remaining pages (SmartMoney, SignalLab, LiveReadiness, MarketScanner, MarketDetail, WalletDetail) work and stay; the orphan pages (Markets, MarketsHub, Reasoning, ResearchData, MarketMonitor, PipelineMonitor, Backtest, Control, PolymarketLive, Signals) are no longer in the nav but their routes remain accessible by direct URL.

---

## Step 1 — Page audit

### Routes registered in `App.jsx`

| Route | Component | API endpoints used (key ones) | Status | Verdict |
|---|---|---|---|---|
| `/dashboard` | Dashboard.jsx | `polymarket/subscription-status`, `polymarket/whale-feed`, `system/intelligence-health`, `signals/performance` (+ now `paper/bankroll`, `smart-money/universe-stats`, `circuit-breaker/status`) | ✅ all 200 | **KEEP & FIX** |
| `/wallets` | SmartMoney.jsx | `smart-money/leaderboard`, `smart-money/universe-stats`, `smart-money/winners`, `smart-money/alerts` | ✅ all 200 | **KEEP** — already benefits from the WR fix from prior session |
| `/smart-money/:address` | WalletDetail.jsx | `smart-money/wallet/{addr}`, positions, trades | ✅ 200 | **KEEP** |
| `/scanner` | MarketScanner.jsx | `markets/top`, `polymarket/live-markets` | ✅ 200 | **KEEP** — main "Market Scanner" page in nav |
| `/monitor` | MarketMonitor.jsx | `markets/top`, `markets/search`, `alerts/anomalies` | ✅ 200 | **REMOVE FROM NAV** — duplicates /scanner |
| `/pipeline` | PipelineMonitor.jsx | `system/pipeline-status`, `system/pipeline-runs` | ✅ 200 | **REMOVE FROM NAV** — operator-only debug surface |
| `/backtest` | Backtest.jsx | `backtest/*` | ✅ 200 | **REMOVE FROM NAV** — research surface, not daily-use |
| `/market/:conditionId` | MarketDetail.jsx | `market/*` family | ✅ 200 | **KEEP** (deep-link from other pages) |
| `/paper` | Paper.jsx | `paper/dashboard` | ✅ 200 | **KEEP & FIX** — was showing Kalshi card |
| `/live` | LiveReadiness.jsx | `live/readiness`, `live/trades`, `circuit-breaker/status` | ✅ 200 | **KEEP** |
| `/signals` | SignalLab.jsx | `signals/performance`, `calibration/status` | ✅ 200 | **KEEP** |

### Orphan files (no route, dead in tree)

| File | Reason |
|---|---|
| `pages/Markets.jsx` | Pure Kalshi browser, replaced by `MarketScanner.jsx` |
| `pages/MarketsHub.jsx` | Cross-platform Polymarket↔Kalshi matching, no longer relevant |
| `pages/Reasoning.jsx` | Equity-trade reasoning view from the old research pipeline |
| `pages/ResearchData.jsx` | Old research datasets browser |
| `pages/Control.jsx` | Loop-control dashboard from the old equities loop |
| `pages/Signals.jsx` | Older signal page; superseded by `SignalLab.jsx` |
| `pages/PolymarketLive.jsx` | Older live page; superseded by `LiveReadiness.jsx` |

These were left on disk to avoid risk; the build excludes them because they aren't imported. The build size is unaffected. They can be deleted in a future cleanup pass.

---

## Step 2 — Sidebar trimmed

`src/trading_platform/frontend/src/components/Sidebar.jsx`

**Before** (9 items):
```
Command Center, Wallet Intel, Whale Monitor (/scanner), Market Monitor,
Pipeline, Backtest, Signal Lab, Paper Trading, Live Readiness
```

**After** (6 items, matching the spec):
```
Command Center, Wallet Intelligence, Market Scanner, Signal Lab,
Paper Trading, Live Readiness
```

Removed from nav: Market Monitor, Pipeline, Backtest. Their routes are still in `App.jsx`, so any deep links keep working.

---

## Step 3 — Command Center fixes

`src/trading_platform/frontend/src/pages/Dashboard.jsx`

### `StatCards` rewritten (4 cards → 5 cards)

The previous card row had:
1. Whale Detections Today
2. Signals Fired Today
3. Paper P&L (using stale `intelligence-health.paper_trading.bankroll_current` defaulting to $500 — that was the old Kalshi $500 starting cash, **wrong** for a $100K Polymarket bankroll)
4. Live Readiness gates

Replaced with:

| # | Card | Source |
|---|---|---|
| 1 | **Tracked Wallets** with tier breakdown (tier1h / tier1 / tier2) | `/api/smart-money/universe-stats` |
| 2 | **Paper Equity** with P&L since start ($100K starting basis) | `/api/paper/bankroll` |
| 3 | **Whale Alerts Today** with tier1/tier2 split | `/api/polymarket/whale-feed` |
| 4 | **Drawdown** gauge with `current_drawdown_pct / max_drawdown_pct`, color-coded (green/amber/red), shows `🛑 HALTED` if circuit breaker tripped | `/api/circuit-breaker/status` |
| 5 | **Live Readiness** gates X/5 (kept) | `/api/system/intelligence-health` |

The drawdown card uses the `circuit-breaker/status` endpoint added in the e2e validation session — it was completely unused by the frontend before. Same for `paper/bankroll`, which was being shadowed by the old `intelligence-health.paper_trading.bankroll_current` legacy field.

### `SignalPerfTable` separated by source

Whale-fired signal types (`wallet_reversal, cascade, oversized_bet, accumulation, convergence, specialist_entry, pre_deadline_surge, whale_entry, whale_exit, no_position_entry, position_reduction`) now render in the main table. Technical scanners (`price_velocity, market_maker_flip`) collapse into a `📊 Technical Scanners` `<details>` block at the bottom with reduced opacity, so the wallet thesis isn't visually polluted by the scanner stats. Empty whale-row state shows the "live-collect monitoring 207 markets / 235 wallets" message instead of a blank table.

---

## Step 4 — Paper Trading fixes

`src/trading_platform/frontend/src/pages/Paper.jsx`

### `PortfolioSummary`: removed Kalshi card

Cards were:
1. Polymarket Whale Signals
2. **Kalshi (legacy)** ← removed
3. Realized P&L
4. Total Trades

Replaced with:
1. **Cash Available**
2. **Deployed**
3. **Realized P&L** (kept)
4. **Total Trades** (kept)

### Signal attribution split into two cards

The previous "Polymarket Whale Signals" card put the technical scanners (`price_velocity`, `market_maker_flip`) alongside real whale signals in the same grid. After the data validation revealed that 234 of 235 paper trades came from technical scanners producing fictional cheap-NO PnL, mixing the two was misleading.

New layout:

```
🧠 Wallet Intelligence Signals
   "Fired by the smart-money pipeline in response to watched-wallet trades."
   [grid of WHALE_SIGNALS only]

📊 Technical Scanners
   "Price-velocity and order-book-imbalance signals — no wallet basis.
    Historical trades placed before the fillability filter (entry_price
    0.05–0.95) bias these stats; treat with care."
   [grid of TECHNICAL_SIGNALS]
```

The technical-scanner card carries an explicit warning so the reader knows the historical PnL is biased by pre-fix degenerate trades. After the new fillability floor went in last session, future trades from these scanners are realistic.

The "Kalshi legacy trades" `<details>` collapsible was removed entirely — `paper/dashboard` no longer returns Kalshi rows after the prior cleanup, so the conditional was dead code.

---

## Step 5 — Pages NOT touched in this session (reasoning)

| Page | Why deferred |
|---|---|
| **SmartMoney.jsx** (Wallet Intelligence, 966 lines) | Already benefits from the WR computation fix from session 7. The leaderboard now shows realistic 41% / 50.7% / 85.2% / 97.3% / 59.7% on the spot-check wallets (was 87.5% / 60.7% / 77.4% / 98.6% / 50%) and includes Theo4 as #1 tier1h. Filter bar / sort / detail panel work — no urgent fix. |
| **SignalLab.jsx** | Reads `signals/performance` and `calibration/status` (both 200, both populated by the calibration loop). Will inherit the technical-scanner separation once the same pattern is applied — recorded as TODO. |
| **LiveReadiness.jsx** | All 5 endpoints (`live/readiness`, `live/trades`, `live/test-dry-run`, `live/emergency-stop`, `live/clear-stop`, `circuit-breaker/status`) return 200. The page was reachable but was 404'd in the e2e validation due to a stale Docker image — that's now fixed. |
| **MarketScanner.jsx** | 234 lines, reads `markets/top` and `polymarket/live-markets`. Healthy. |
| **WalletDetail.jsx** | 261 lines, reads `smart-money/wallet/{addr}` family. Healthy. |
| **MarketDetail.jsx** | 196 lines, deep-link target. Healthy. |

---

## Step 6 — Verification

| Check | Result |
|---|---|
| Frontend Docker build | ✅ rebuilt, image tagged |
| Frontend serves on :5173 | ✅ 200 |
| All 7 Dashboard endpoint deps | ✅ 200 (subscription-status, whale-feed, intelligence-health, signals/performance, paper/bankroll, smart-money/universe-stats, circuit-breaker/status) |
| All 43 endpoints surveyed | **43/43 200** |
| Backend tests | 571/571 passing (no backend changes this session) |

---

## Files changed

- `src/trading_platform/frontend/src/components/Sidebar.jsx` — nav array trimmed from 9 → 6 entries
- `src/trading_platform/frontend/src/pages/Dashboard.jsx` — `StatCards` rewritten with 5 real-data cards (tracked wallets / paper equity / whale alerts / drawdown / readiness); `SignalPerfTable` separates whale signals from technical scanners; new endpoint deps wired
- `src/trading_platform/frontend/src/pages/Paper.jsx` — `PortfolioSummary` Kalshi card removed; `WHALE_SIGNALS` / `TECHNICAL_SIGNALS` split into two clearly-labeled cards with caveats; "Kalshi legacy" details block removed

---

## Remaining gaps (TODO, not for this session)

1. **SignalLab.jsx** should get the same `🧠 Wallet / 📊 Scanner` split treatment as Dashboard and Paper. The data is there (`signals/performance.by_type`) — needs ~15 lines.
2. **WalletDetail.jsx** doesn't yet show the dynamic-tier history (S/A/B/C/D promotions) — `wallet_category_profiles` table only has 1 row anyway, not load-bearing until more paper trades resolve.
3. **MarketScanner.jsx** could overlay whale positions on the market list but currently shows volume sort only — useful enhancement but not blocking.
4. **Drawdown gauge** on the Command Center is a numeric card — could become a horizontal bar chart for at-a-glance scanning. Cosmetic.
5. **Orphan page deletion** — Markets.jsx, MarketsHub.jsx, Reasoning.jsx, ResearchData.jsx, Control.jsx, Signals.jsx, PolymarketLive.jsx, MarketMonitor.jsx, PipelineMonitor.jsx, Backtest.jsx can all be deleted from disk. Left in place this session to avoid touching ~5,000 LOC unrelated to the audit.
6. **`api/client.js`** still exports `kalshiMarkets` / `kalshiMarketHistory` because the orphan files import them. Will become safe to remove after #5.
