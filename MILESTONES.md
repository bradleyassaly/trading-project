# Milestones

## Completed

- **Signal Research Lab** — 8 signal families implemented. calibration_drift 56.2% WR, volume_spike 55.2% WR, time_decay 54.8% WR on 48,509 Manifold markets.
- **Kalshi live candle collector** — Hourly OHLCV collection for 213 Economics markets.
- **Polymarket WebSocket collector** — Live price tick collection via CLOB WebSocket. Expanded to 225 markets with whale detection.
- **Paper trade executor** — Kalshi + Polymarket paper trading with Kelly sizing, portfolio tracking, resolution checking.
- **Wallet intelligence DB** — 16,259 wallets, 108,845 trades, 31K resolved with PnL in wallet_intelligence.db.
- **wallet-profiles --from-db** — Rebuilt 312 wallets with directional_win_rate directly from DB trades (bypasses broken CSV resolution path).
- **Wallet bucket classification** — 11 behavioral buckets (directional, market_maker, arb_bot, concentrated_whale, etc).
- **Market universe** — 226 markets across 9 categories (politics, economics, crypto, finance, sports, culture, tech, mentions, weather) from Gamma API.
- **WhaleTripwire** — Loads 76 watched wallets (50 tier1, 26 tier2) from wallet_profiles.
- **WhaleSignalEngine** — whale_entry + convergence signal generation with DB recording.
- **Paper executor on_signal()** — Bankroll-allocated paper trades from whale signals (3% tier1, 1.5% tier2, 5% convergence).
- **WebSocket whale integration** — Live collector passes events through WhaleTripwire → SignalEngine → PaperExecutor.
- **Whale Monitor GUI** — Replaced MarketScanner with live whale feed, category performance table, signal feed.
- **API endpoints** — whale-feed, subscription-status, signals-feed, category-performance.
- **React GUI** — Dashboard, smart money wallets, signal monitor, paper trading, execution engine.
- **Research replay framework** — Registry-backed evaluation, cross-provider comparison, promotion gating.
- **Test suite** — 1,817 passing tests.

## Deprecated (preserved, not active)

- **Manifold Markets parser** — 48K resolved markets used for backtesting only.
- **Metaculus integration** — 2K markets, backtesting + divergence signal concept.
- **PredictIt parser** — Historical data, not actively used.
- **Wallet profiler CSV path** — `wallet_profiler.py` build_profiles() replaced by `wallet_profile_rebuild.py` rebuild_profiles() reading from DB.
- **Cross-platform backtest** — Manifold+Metaculus backtest infrastructure, not used in active pipeline.

## In Progress

- **KXCPI/KXFED paper trades** — Resolving through April 2026. First real signal validation.
- **Polymarket trade accumulation** — Data API fetch running, accumulating new trades for watched wallets.
- **Whale signal generation** — WebSocket running, waiting for tier1/tier2 wallet trades to hit monitored markets.

## Next

- **Category performance tracking** — Track signal win rate per category from resolved paper trades.
- **Rolling 20-trade wallet quality** — Demote wallets whose recent performance drops below 0.45 WR.
- **Leaderboard with atomic versioning** — Version-stamped wallet rankings, safe against bad pipeline runs.
- **Daily pipeline automation** — Scheduled every 4h: fetch → profile → classify → leaderboard → universe.
- **Continuous monitor automation** — Auto-restart, health checks, dead man's switch.
- **Paper trade resolution tracking by category** — Dashboard showing per-category win rates and P&L.

## Go-Live Criteria

| Gate | Requirement | Current Status |
|------|------------|---------------|
| 1 | 50+ paper trades resolved > 55% WR | 0 resolved (accumulating) |
| 2 | 2+ categories showing edge independently | Not enough data yet |
| 3 | Max drawdown < 20% of bankroll over 30 days | No drawdown data yet |
| 4 | Human review and approval | Pending |
| 5 | $500 real capital, 1 category only | Pending |
