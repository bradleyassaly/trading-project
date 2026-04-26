# Dashboard Audit — 2026-04-25

Goal alignment: every page should serve the path to **consistent live trading at L5 ($10-20K/mo)**. Anything that doesn't is dead code or a distraction.

## Page inventory (19 pages)

| Page | Route | Purpose | Goal-alignment |
|---|---|---|---|
| Dashboard | `/dashboard` | Command center, all key metrics | ✅ Primary surface |
| SmartMoney (Wallets) | `/wallets` | Wallet leaderboard | ✅ Critical |
| WalletDetail | `/wallets/:address` | Per-wallet drill-down | ✅ Critical (now with intelligence layers panel) |
| MarketScanner | `/scanner` | Active markets w/ signals | ✅ Critical |
| MarketDetail | `/market/:conditionId` | Per-market drill-down | ✅ Critical |
| MarketMonitor | `/monitor` | Real-time market activity | ✅ Useful |
| Paper | `/paper` | Paper trade ledger | ✅ Critical |
| LiveTrading | `/live` | Live trade ledger + control | ✅ Critical (path-to-live) |
| LiveReadiness | `/live-readiness` | Readiness gate detail | ✅ Critical (was missing — Telegram has it too) |
| PipelineMonitor | `/pipeline` | Scheduler / pipeline health | ✅ Critical |
| SignalLab | `/signals` | Signal experiments | 🟡 Useful but redundant with backtest |
| Backtest | `/backtest` | Backtest runner | 🟡 Useful — missing PBO + DSR display |
| Reasoning | n/a (no route) | Decision explanations | 🔴 Orphaned (no route in App.jsx visible) |
| ResearchData | n/a | Research data viewer | 🔴 Orphaned |
| Markets / MarketsHub | redirect | — | 🟡 Redirects to /scanner |
| Control | unknown | — | 🔴 Likely orphaned |
| PolymarketLive | unknown | — | 🔴 Likely orphaned |
| Signals | n/a | — | 🔴 Orphaned (replaced by SignalLab) |

**Conclusion:** 12 of 19 pages are load-bearing. ~5 are orphaned/dead routes. 2 are redundant.

## What's MISSING relative to today's work

| Missing | Surfaces today's data | Effort |
|---|---|---|
| ✅ Intelligence Layer Health tile (just added) | readiness + calibration + signal-health + funnel | done |
| ✅ Calibration Curve tile (just added) | reliability diagram from /api/calibration bins | done |
| 🟡 Insider pool growth chart | 82 → 350 trajectory; daily count | 1h |
| 🟡 Signal-health table (full) | Per-signal IC + decay flags as a sortable table | 1h |
| 🟡 Decision-trace funnel detail page | All gates with passed/rejected, by surface | 2h |
| 🟡 Wallet behavior cluster viz | k-means cluster centroids + wallet examples per cluster | 3h |
| 🟡 Cross-platform divergence tile | Top divergent (PM vs Kalshi) markets — paper-mode | 1h (when Kalshi rails live) |
| 🟡 Phase B `resolution_decay` performance tile | Trades fired + WR vs whale-mirror baseline | 1h after first 7d of data |

## What's REDUNDANT and could be retired

| Page | Why redundant | Action |
|---|---|---|
| `Reasoning.jsx` | Decision-trace funnel + WalletDetail panel cover this | Verify no inbound links; delete if orphaned |
| `Signals.jsx` | SignalLab + IntelligenceHealthTile cover this | Verify; delete if orphaned |
| `Control.jsx` | Telegram + readiness endpoint cover ops control | Verify; delete if orphaned |
| `PolymarketLive.jsx` | LiveTrading already covers it | Verify; delete if orphaned |

## What's MISALIGNED with goals

1. **No "path-to-L5" progress tile** — there's no place that shows "you're at L0, target L5, here's what you need next." Should add a ladder-progress card to Dashboard top.
2. **No discovery tier monitoring** — `LIVE_DISCOVERY_ENABLED=1` is on but there's no dashboard indicator. Should be in the Intelligence Layer Health tile or a separate badge.
3. **No Phase B signal lane indicator** — when `resolution_decay` fires, it lands in the same paper trades table. No visual distinction.
4. **Telegram bot has more functional parity with the API than the dashboard does** — `/wallet`, `/positions`, `/insiders`, `/funnel`, `/readiness` all exist as commands. The dashboard should match feature-for-feature.

## Just shipped this turn (dashboard)

- **`IntelligenceHealthTile`** consuming `/api/system/readiness` + `/api/calibration` + `/api/signal-health` + `/api/funnel/decisions` in one card
- **`CalibrationCurveTile`** rendering reliability bins as an SVG scatter (predicted vs realized, dot size = n)
- Both wired into Dashboard top, between StatCards and the P&L panel

## Recommended dashboard sequence to ship next session (ordered)

1. **Ladder-progress tile** at the top of Dashboard — explicit "L0 → L5" with current state. (1h)
2. **Discovery tier badge + Phase B lane indicator** — shows what's enabled. (30m)
3. **Insider pool growth chart** — daily count over time. (1h)
4. **Signal-health full table page** — `/signals/health` route consuming `/api/signal-health`. (1h)
5. **Decision-trace funnel detail page** — full breakdown by gate + drill-down to specific rejections. (2h)
6. **Retire orphaned pages** after verifying no inbound links. (30m)

Total: 6 hours to bring the dashboard to full parity with backend intelligence + path-to-L5 visibility.

## Today's net dashboard delta

- 1 composite tile (IntelligenceHealthTile)
- 1 visualization tile (CalibrationCurveTile)
- WalletDetail INTELLIGENCE LAYERS section (shipped earlier today)
- Wired 4 new endpoints into Dashboard

The dashboard is now backend-parity for the calibration / signal-health / readiness / decision-trace loops shipped today. The remaining 6h of work shifts it from "operator dashboard" to "operator dashboard + path-to-L5 ladder visualization."
