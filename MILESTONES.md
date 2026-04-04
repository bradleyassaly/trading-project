# Milestones

## Completed

- **Signal Research Lab** — 8 signal families implemented and validated. calibration_drift 56.2% WR, volume_spike 55.2% WR, time_decay 54.8% WR. Tested on 48,509 resolved Manifold markets + Metaculus data.

- **Kalshi Live Candle Collector** — Fetches hourly candles for 213 open Economics markets via authenticated `/series/{s}/markets/{t}/candlesticks` endpoint. Builds feature parquets with all signal columns. `days_to_close` populated from market close times.

- **Polymarket Live WebSocket Collector** — Connects to Polymarket CLOB WebSocket, subscribes to 75 active markets (30-day horizon, $10k+ volume, no sports). Stores ticks in SQLite with orderbook data (best_bid, best_ask, spread). Exports hourly OHLCV bars to parquet.

- **Manifold Markets Parser** — Parsed 48,509 resolved binary markets from Manifold data dump (2021-2024). Each market's bet history converted to hourly feature parquets using same `build_kalshi_features()` pipeline.

- **Metaculus Integration** — Fetches resolved binary questions from Metaculus public API. Converts community forecast history to feature parquets. 2,000+ questions available for backtesting.

- **PredictIt Parser** — Converts PredictIt historical CSV to daily feature parquets. Real USD data (capped at $850/contract).

- **Autonomous Market Scanner** — `KalshiMarketScanner` scans open markets per series, fetches candles, builds features, runs all 8 signal families. Returns `ScanResult` with ticker, yes_price, days_to_close, signal scores, confidence, recommended side, news context.

- **Paper Trade Executor** — `KalshiPaperExecutor` tracks paper trades in SQLite. $500 starting capital. Kelly-sized $5-15 per trade. Checks market resolutions and records outcomes. Skips duplicate positions and scheduled_release contexts.

- **Wallet Profiler** — Identifies early informed traders from on-chain Polymarket trade history. `early_win_rate >= 0.65` with 5+ early trades (>24h before close) = smart money. Separates genuine insiders from late arbitrageurs.

- **News Tagger** — `EconomicNewsCalendar` parses Kalshi tickers to build event calendar. Labels price moves as `scheduled_release`, `pre_event`, or `unscheduled`. Scans feature files for actual market tickers.

- **Polymarket Data API Fetcher** — Fetches all trades from `data-api.polymarket.com/trades` (no auth). Supports filtering by market or wallet. Output compatible with wallet profiler.

- **Blockchain Ingest** — Converts poly-trade-scan on-chain trade CSV to feature parquets. Maps token_ids to markets via live collector SQLite metadata.

- **Cross-Platform Backtest** — Single command backtests across Kalshi + Polymarket + Manifold + Metaculus. Results merged per signal family, best source wins.

- **React GUI** — Command Center, Signal Research, Kalshi Markets Browser, Polymarket Live (with price chart detail panel), Trade Reasoning, Loop Control. 6 pages, auto-refreshing.

- **1,619 passing tests** across all modules.

## In Progress

- KXCPI-26APR paper trades — resolves around April 15, 2026
- Polymarket data-api trade history accumulation for wallet profiling
- Goldsky orderbook depth integration for microstructure signals
- Confidence calibration on paper trade outcomes

## Next Milestones

- **First paper trade resolution** (April 15, 2026) — KXCPI and KXFED events
- **30 days of wallet trade history** — enough data for reliable smart money detection
- **50+ paper trades with measurable win rate** — statistical significance
- **All 8 signals firing with real data** — currently 3 of 8 validated
- **Cross-platform arbitrage** — detect Kalshi vs Polymarket price gaps on same events

## Go-Live Criteria

Before placing real money trades:
1. 50+ paper trades completed with resolution outcomes
2. Paper win rate > 55% (statistically significant at n=50)
3. At least 2 signals validated on live market data (not just backtest)
4. Max $25/trade, $500 total capital
5. Kill switch active (20% drawdown halt)
6. All 8 signal families producing non-NaN scores on live data

## Known Limitations

- Kalshi `/historical/markets/{ticker}/candlesticks` returns 404 on free tier. Historical candle data unavailable — collect forward only.
- Manifold uses play money (Mana). Volume not comparable to real-money markets.
- Polymarket WS occasionally wraps messages in JSON arrays. Handled.
- Wallet profiler needs external trade data (data API or blockchain). No automated pipeline yet.
- Paper executor does not account for spread/slippage. Real execution will be worse.
- `order` param on Gamma API breaks `tag_slug` filtering. Market selection uses `end_date_min`/`end_date_max` without tags instead.
