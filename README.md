# Trading Platform

Automated prediction market trading via smart money detection. Tracks 16K+ Polymarket wallets, identifies top performers by category and PnL, monitors them in real time via WebSocket, detects when they trade, converts activity into signals, and executes paper trades. Kalshi Economics paper trading runs in parallel with calibration and volume signals.

## Current Status

- **Wallet intelligence DB**: 16,259 wallets, 108K trades, 31K resolved with PnL
- **Watched wallets**: 50 tier1, 26 tier2 (directional win rate >= 53-58%)
- **Market universe**: 226 markets across 9 categories (politics, economics, crypto, finance, sports, culture, tech, mentions, weather)
- **Kalshi paper trades**: active on Economics markets, resolving through April 2026
- **WebSocket**: subscribing to 75+ live Polymarket markets with whale detection enabled
- **Signal types**: whale_entry + convergence (Polymarket), calibration_drift + volume_spike + time_decay (Kalshi)
- **Test suite**: 1,817 passing tests

## Two-Process Architecture

**Process 1 — Daily Intelligence Pipeline** (runs every 4 hours)
1. `data-api-fetch` — accumulate new trade CSVs
2. `wallet-profiles --from-db` — compute directional_win_rate per wallet
3. `classify-wallet-buckets` — behavioral bucket classification
4. `refresh-universe` — top 25 markets x 9 categories from Gamma API
5. WhaleTripwire reloads watched wallet set automatically

**Process 2 — Continuous Live Monitor** (always running)
- Loads watched wallets from DB at startup + every 4h
- Subscribes to 225 markets via CLOB WebSocket
- On whale detection: fire signal → paper trade immediately
- Writes to wallet_alerts + market_signals tables in real time

## Quick Start

```bash
# 1. Rebuild wallet profiles from DB (one-time after fresh clone)
trading-cli data polymarket wallet-profiles --from-db
trading-cli data polymarket classify-wallet-buckets

# 2. Refresh market universe
trading-cli data polymarket refresh-universe

# 3. Start live collector with whale detection
trading-cli data polymarket live-collect --config configs/polymarket.yaml

# 4. Start Kalshi candle collection
trading-cli data kalshi live-candles --lookback-days 30

# 5. Start API + frontend
uvicorn trading_platform.api.main:app --port 8001
cd src/trading_platform/frontend && npm run dev   # http://localhost:5173

# 6. Check paper trade status
trading-cli paper dashboard
```

## Data Sources

| Source | Type | Markets | Usage |
|--------|------|---------|-------|
| Kalshi API | Authenticated REST | 213 open Economics | Live signals + paper trading |
| Polymarket CLOB WebSocket | Public WS | 225 active | Live price + whale detection |
| Polymarket Data API | Public REST | All markets | Wallet trade accumulation |
| Polymarket Gamma API | Public REST | All markets | Market universe + resolution |

## Signal Library

| Signal | Platform | What it measures | Status |
|--------|----------|-----------------|--------|
| whale_entry | Polymarket | Tier1/tier2 wallet trade detected | Active |
| convergence | Polymarket | 2+ watched wallets same side within 2h | Active |
| calibration_drift | Kalshi | Mean-reversion on price overshoot | Active (56.2% WR) |
| volume_spike | Kalshi | Follow informed money on unusual volume | Active (55.2% WR) |
| time_decay | Kalshi | Fade uncertainty premium near close | Active (54.8% WR) |

## Go-Live Criteria

| Gate | Requirement | Status |
|------|------------|--------|
| 1 | 50+ paper trades resolved with > 55% win rate | Pending |
| 2 | At least 2 categories showing edge independently | Pending |
| 3 | Max drawdown < 20% of bankroll over 30 days | Pending |
| 4 | Human review and approval | Pending |
| 5 | Start with $500 real capital, 1 category only | Pending |

## Known Limitations

- Kalshi historical candlestick endpoint returns 404 on free API tier. Use live candle collector for open markets only.
- Paper executor does not account for spread/slippage. Real trades will have worse execution.
- Polymarket WebSocket occasionally sends messages as JSON arrays. The collector handles this.
- Whale detection depends on watched wallet set quality — requires regular profile rebuilds.
- Category classification uses keyword matching; some markets may be miscategorized.
