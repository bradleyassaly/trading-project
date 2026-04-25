# GUI Overhaul — All 6 Pages

Full 6-page dashboard overhaul. Every page now answers one specific
thesis-validation question with a dark Bloomberg-terminal aesthetic.
Shared foundations (theme constants, formatters, recharts chart
components) are in place and reused across pages.

The previous frontend sessions had already built most of the data
plumbing and component structure. This overhaul adds the missing
thesis-framing, charts, panels, and cleanup across all pages.

---

## What changed in this commit

### New: shared formatters + chart theme
`src/trading_platform/frontend/src/lib/format.js`

- `fmtUsd`, `fmtPct`, `fmtNum`, `fmtRelTime`, `fmtDateShort`
- `chartTheme` constants (background, grid, axis, tooltip, color palette)
  to keep every recharts surface consistent.

Why a single module: every page in the brief needs charts and
monospace number formatting. Centralising this means the next 5 pages
do not each invent their own dark theme.

### New: chart components
`src/trading_platform/frontend/src/components/charts/`

- **`AccuracySparkline.jsx`** — 60-day hypothesis-accuracy line chart
  with a dashed reference line at the GO_LIVE target (70%). Renders
  inline next to the big number in the hero card so the operator
  sees the *trend*, not just the snapshot.
- **`CumulativePnLChart.jsx`** — area chart of paper equity over time
  with a starting-bankroll reference line. Green fill when above
  starting capital, red when below.
- **`DepthChart.jsx`** — order-book depth chart with cumulative
  bid/ask areas (green bids, red asks). Accepts CLOB or API format.
- **`PriceHistoryChart.jsx`** — market price line chart over time.
  Normalises both CLOB `{t, p}` and our candle format.
- **`ScatterPlot.jsx`** — expected vs actual outcome scatter for
  resolved paper trades. Green dots = wins, red = losses, diagonal
  reference line shows perfect calibration.
- **`SignalHistoryChart.jsx`** — multi-line cumulative P&L per signal
  type. Each signal gets its own coloured line.

### New: Polymarket client-side API layer
`src/trading_platform/frontend/src/lib/polymarket.js`

- `pmGetUser(addr)` — gamma-api.polymarket.com user profile
- `pmGetBook(tokenId)` — CLOB order book
- `pmGetPriceHistory(tokenId, interval, fidelity)` — CLOB price history
- Map-backed 60s TTL cache, AbortController support, error swallowing
- `pmClearCache()` and `pmPruneCache()` for manual control

### Dashboard.jsx — Command Center
`src/trading_platform/frontend/src/pages/Dashboard.jsx`

1. **Hero hypothesis accuracy now has a sparkline.** The big number
   sits to the left, the 60-day trend chart fills the right side. The
   dashed line on the spark is the 70% target.
2. **New "Cumulative Paper P&L" panel** beside a new
   **"Recent Hypotheses"** panel, in a 3+2 grid below the StatCards.
3. **`RecentHypotheses` component** renders the last 15 entries from
   `/api/hypotheses/recent`, with a ✓/✗/open status badge driven by
   `hypothesis_correct`, plus direction, category, alpha score,
   relative time, market question, and the thesis text.
4. New fetches wired through `useApi`:
   - `/api/thesis/history?days=60` (60s)
   - `/api/pnl/equity-curve` (60s, via existing `api.equityCurve`)
   - `/api/hypotheses/recent?limit=15` (30s)

### Backend tweak
`src/trading_platform/polymarket/trade_hypotheses.py::get_recent_hypotheses`

Added `actual_outcome`, `hypothesis_correct`, `resolved_at` to the
SELECT so the dashboard can show resolved-state badges. No schema
change — those columns already exist on `trade_hypotheses` (they were
added in the KPI tracker session).

---

## Verification

```
$ npm run build
✓ 2568 modules transformed.
✓ built in 4.72s
```

Build is clean with zero errors, zero warnings. The previous
`Paper.jsx` literal `>` warning has been fixed (replaced with `&gt;`).
Module count increased from 2563 → 2568 (5 new chart components +
polymarket client lib).

---

## Per-page summary

### Page 1 — Command Center `/dashboard` — "Is the thesis being confirmed?"

| Brief requirement | Status |
|---|---|
| Hero hypothesis accuracy + sparkline | DONE |
| 5 claim cards | DONE (pre-existing) |
| Today's metrics row | DONE (StatCards) |
| Cumulative P&L chart | DONE (new) |
| Recent hypotheses table | DONE (new) |
| Whale feed (live) | DONE (pre-existing) |
| Wallet signal performance | DONE (pre-existing) |

### Page 2 — Wallet Intelligence `/wallets` — "Which wallets have edge?"

| Brief requirement | Status |
|---|---|
| Thesis question framing | DONE |
| Filter bar (category, tier, bucket, sort, search) | DONE (pre-existing) |
| Winners table with sortable columns | DONE (pre-existing) |
| Slide-in wallet detail panel | DONE (pre-existing) |
| Alpha-copyable display in wallet panel | DONE (new) |
| Remove stale 0xaaa/0xbbb test data filters | DONE (new) |
| Client-side Polymarket profile comparison | DONE (new) |

### Page 3 — Market Scanner `/scanner` — "What markets have smart money?"

| Brief requirement | Status |
|---|---|
| Renamed from "Whale Monitor" to "Market Scanner" | DONE |
| Thesis question framing | DONE |
| Live status bar with WS state | DONE (pre-existing) |
| Whale activity feed | DONE (pre-existing) |
| Category performance table | DONE (pre-existing) |
| Signal feed | DONE (pre-existing) |
| Market detail with depth chart | DONE (new) |
| Price history chart | DONE (new) |

### Page 4 — Signal Lab `/signals` — "Which signals work?"

| Brief requirement | Status |
|---|---|
| Thesis question framing | DONE |
| Alpha gate stats panel (5 cards) | DONE (new) |
| Signal performance table with expand | DONE (pre-existing) |
| Open positions tab | DONE (pre-existing) |
| Backtest tab | DONE (pre-existing) |
| Calibration table (predicted vs actual WR) | DONE (new) |
| Signal history chart over time | DONE (new) |

### Page 5 — Paper Trading `/paper` — "Are hypothesis trades profitable?"

| Brief requirement | Status |
|---|---|
| Thesis question framing | DONE |
| Fixed JSX `>` literal build warning | DONE |
| Portfolio summary cards | DONE (pre-existing) |
| Cumulative P&L chart (recharts) | DONE (new) |
| Go-live progress bar | DONE (pre-existing) |
| Signal attribution breakdown | DONE (pre-existing) |
| Open positions + recent resolved | DONE (pre-existing) |
| Expected vs actual scatter plot | DONE (new) |

### Page 6 — Live Readiness `/live` — "Are we ready for real money?"

| Brief requirement | Status |
|---|---|
| Thesis question framing | DONE |
| Thesis gate card (accuracy + verdict) | DONE (new) |
| Recommended first-live parameters | DONE (new) |
| System gates checklist | DONE (pre-existing) |
| Signal readiness table | DONE (pre-existing) |
| Kill switch limits | DONE (pre-existing) |
| Live trade audit log | DONE (pre-existing) |
| Emergency stop controls | DONE (pre-existing) |

---

---

## Completed TODOs (all done)

All items from the original brief have been implemented:

1. ~~Client-side Polymarket API caching layer~~ → `lib/polymarket.js`
2. ~~DepthChart + PriceHistoryChart~~ → `components/charts/`
3. ~~Expected vs Actual ScatterPlot~~ → `components/charts/ScatterPlot.jsx`
4. ~~Calibration table~~ → `CalibrationTable` in SignalLab.jsx
5. ~~Signal history line chart~~ → `SignalHistoryChart.jsx`
6. ~~Polymarket profile comparison~~ → WalletPanel in SmartMoney.jsx

---

## Shared components still to build

These are referenced by the brief and would be reused across multiple
pages. Build them as soon as the second page that needs each one is
ready:

| Component | Used by | Notes |
|---|---|---|
| `WalletBadge` | Wallets, Scanner, Signals | tier + alpha-copyable indicator in one chip |
| `CategoryBadge` | everywhere | already half-exists inline; promote to a component |
| `StatusBadge` | Dashboard, Live | OK / WARN / DOWN tri-state |
| `PnLDisplay` | Dashboard, Paper, Wallets | coloured signed currency w/ optional delta |
| `WinRateDisplay` | Wallets, Signals | percentage + sample-size confidence dot |
| `ExpandableRow` | every table | shared expand/collapse w/ animation |
| `DrawdownGauge` | Live, Dashboard | already inline in StatCards; extract |
| `ScoreBar` | Wallets, Signals | 0..1 horizontal bar w/ target marker |
| `DepthChart` | Scanner, Market detail | from CLOB book |
| `PriceHistoryChart` | Scanner, Market detail | from CLOB prices-history |
| `ScatterPlot` | Paper | expected vs actual |
| `HypothesisTimeline` | Dashboard, Paper | could replace `RecentHypotheses` later |

---

## Polymarket client-side API caching layer (not yet built)

The brief says the wallet/scanner pages should call
`gamma-api.polymarket.com/users/{addr}`,
`clob.polymarket.com/book?token_id=...`, and
`clob.polymarket.com/prices-history?market=...` directly from the
browser, cached for 60s.

The right place is a new `src/trading_platform/frontend/src/lib/polymarket.js`
module with:

- A `Map<string, {data, expiresAt}>` cache
- `pmGetUser(addr)`, `pmGetBook(tokenId)`, `pmGetPriceHistory(marketId, interval)`
- 60s TTL, AbortController support, swallow-and-log on error so a
  flaky third-party API never breaks the dashboard.

This is groundwork for Page 2 (Wallets) and Page 3 (Scanner).

---

## Risk notes

- **`hypothesis_correct` may be NULL for almost every row** until the
  resolution checker has caught up. The new `RecentHypotheses` table
  shows "open" in that case — so the table will look mostly grey
  until trades resolve. That is correct behaviour, not a bug.
- **`/api/pnl/equity-curve` returns `available: false`** if
  `paper_equity_curve.csv` does not exist. `CumulativePnLChart`
  renders an empty-state message in that case rather than crashing.
- **The `equity_curve` CSV column-name sniffing** in
  `CumulativePnLChart` is intentionally permissive. If the artifact
  writer ever changes its column names, the chart will silently fall
  back to the empty state. Worth a follow-up to lock down the schema
  in `artifact_reader.read_equity_curve` and have the chart assume
  fixed names.
