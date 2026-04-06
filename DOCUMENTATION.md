# Technical Documentation

## Daily Intelligence Pipeline

Run these commands in order every 4 hours:

```bash
# Step 1: Accumulate new trades from Data API
trading-cli data polymarket data-api-fetch --hours-back 6

# Step 2: Rebuild wallet profiles from DB
trading-cli data polymarket wallet-profiles --from-db

# Step 3: Classify wallet buckets
trading-cli data polymarket classify-wallet-buckets

# Step 4: Refresh market universe
trading-cli data polymarket refresh-universe

# Step 5 (optional): Sync wallet positions
trading-cli data polymarket sync-wallet-positions --top-n 100
```

## Continuous Monitor

```bash
# Start live collector with whale detection (runs indefinitely)
trading-cli data polymarket live-collect --config configs/polymarket.yaml
```

On startup: loads MarketUniverse, WhaleTripwire (tier1/tier2 wallets), WhaleSignalEngine, PaperExecutor. Every event is checked for watched wallet activity.

## Active CLI Commands

### Polymarket Data

```bash
trading-cli data polymarket data-api-fetch --hours-back 6
trading-cli data polymarket wallet-profiles --from-db
trading-cli data polymarket classify-wallet-buckets
trading-cli data polymarket refresh-universe [--max-per-category 25]
trading-cli data polymarket live-collect [--config configs/polymarket.yaml] [--max-markets 75]
trading-cli data polymarket sync-wallet-trades [--top-n 200]
trading-cli data polymarket sync-wallet-positions [--top-n 100]
trading-cli data polymarket rebuild-wallet-profiles
trading-cli data polymarket build-leaderboard
trading-cli data polymarket seed-from-leaderboard
trading-cli data polymarket build-address-map
trading-cli data polymarket enrich-trade-resolution
trading-cli data polymarket compute-signals
trading-cli data polymarket clob-fetch [--hours-back 168]
trading-cli data polymarket orderbook-fetch
trading-cli data polymarket collect-orderflow
trading-cli data polymarket orderflow-status
```

### Kalshi Data

```bash
trading-cli data kalshi live-candles --lookback-days 30
trading-cli data kalshi recent-ingest --hours-back 12
trading-cli data kalshi historical-ingest --ticker KXCPI
```

### Research

```bash
trading-cli research kalshi-full-backtest
trading-cli research kalshi-alpha --tickers KXCPI,KXFED
```

### Paper Trading

```bash
trading-cli paper dashboard
trading-cli paper scan
```

## Deprecated CLI Commands

These commands still exist but are not part of the active pipeline:

```bash
# CSV-based wallet profiling (replaced by --from-db)
trading-cli data polymarket wallet-profiles --trades-csv ... --resolution-csv ...

# External data sources (backtesting only, not actively maintained)
trading-cli data manifold parse
trading-cli data metaculus fetch
trading-cli data predictit parse

# Cross-platform backtest flags
trading-cli research kalshi-full-backtest --include-manifold --include-metaculus
```

## API Endpoints

### Polymarket Whale Monitoring (NEW)

```
GET /api/polymarket/whale-feed          — Last 50 whale alerts with time_ago
GET /api/polymarket/subscription-status — WebSocket health: connected, markets, wallets
GET /api/polymarket/signals-feed        — Last 50 signals with execution status
GET /api/polymarket/category-performance — Signal win rate per category
```

### Polymarket Live Markets

```
GET /api/polymarket/live-markets            — Active markets with latest prices
GET /api/polymarket/market-ticks/{market_id} — Price history for a market
```

### Smart Money

```
GET /api/smart-money/wallets            — Top wallets by edge
GET /api/smart-money/signals            — Smart money directional signals
GET /api/smart-money/alerts             — Alert log (limit, wallet, tier filters)
GET /api/smart-money/open-positions     — Current smart money positions
GET /api/smart-money/actionable-signals — Multi-wallet convergence signals
GET /api/smart-money/leaderboard        — Ranked wallets
GET /api/smart-money/universe-stats     — DB statistics
GET /api/smart-money/winners            — Top wallets by profit (window=all|weekly|monthly)
GET /api/smart-money/wallet/{address}   — Wallet detail
GET /api/smart-money/wallet/{address}/positions — Wallet positions
GET /api/smart-money/wallet/{address}/trades    — Wallet trade history
GET /api/smart-money/mirror             — Mirror trading signals
```

### Paper Trading

```
GET /api/paper/dashboard    — Paper trading overview
GET /api/paper/portfolio    — Current positions
GET /api/paper/trades       — Trade history
GET /api/paper/scan         — Latest scan results
GET /api/paper/bankroll     — Bankroll status
GET /api/paper/pnl-history  — P&L over time
```

### Kalshi

```
GET /api/kalshi/markets                    — Active Kalshi markets
GET /api/kalshi/market/{ticker}/history    — Price history
```

### System

```
GET /api/health         — Health check
GET /api/system/status  — System status overview
```

### Research & Ops

```
GET  /api/research/run                    — Start backtest job
GET  /api/research/status/{job_id}        — Job status
GET  /api/research/datasets               — Dataset registry
GET  /api/research/replay/*               — Replay evaluation endpoints
GET  /api/ops/registry-summary            — Provider registry
GET  /api/ops/provider-monitoring         — Provider health
GET  /api/ops/provider-health             — Provider health summary
POST /api/loop/control                    — Pause/resume/trigger loop
```

## Data Directories

| Path | Contents |
|------|----------|
| `data/polymarket/wallet_intelligence.db` | Central wallet DB (profiles, trades, positions, signals, alerts) |
| `data/polymarket/market_universe.json` | Active market set by category |
| `data/polymarket/ws_status.json` | WebSocket health status |
| `data/polymarket/live/prices.db` | Live tick data |
| `data/polymarket/live/hourly_bars/` | Hourly OHLCV parquets |
| `data/polymarket/data_api_trades/` | Per-market trade CSVs |
| `data/kalshi/paper_trades.db` | Paper trade state (Kalshi + Polymarket) |
| `data/kalshi/live_candles/` | Hourly Kalshi OHLCV parquets |
| `artifacts/` | Research outputs, signal snapshots, charts |
| `configs/` | YAML workflow configs |

## Signal Descriptions

### Polymarket Signals (Active)

| Signal | Trigger | Confidence Formula |
|--------|---------|-------------------|
| whale_entry | Tier1/2 wallet trades $25+ on monitored market | `wr * min(conviction/10, 1) * tier_mult` |
| convergence | 2+ wallets same side same market within 2h | `base_confidence * 1.20` |

### Kalshi Signals (Active)

| Signal | What it measures | Validated WR |
|--------|-----------------|-------------|
| calibration_drift | Mean-reversion when price overshoots | 56.2% |
| volume_spike | Follow informed money on unusual volume | 55.2% |
| time_decay | Fade uncertainty premium near close | 54.8% |

### Kalshi Signals (Planned — Not Built)

| Signal | Concept |
|--------|---------|
| base_rate | Edge vs historical base rate |
| metaculus_divergence | Gap between Kalshi and Metaculus consensus |
| taker_imbalance | Net aggressive buyer vs seller flow |
| large_order | Unusually large single orders |
| unexplained_move | Price moves without volume |

## Configuration

### configs/polymarket.yaml

```yaml
market_selection:
  max_markets: 75
  min_volume: 10000
  end_date_max_days: 30
request_sleep_sec: 0.05
live_db_path: data/polymarket/live/prices.db
live_hourly_bars_dir: data/polymarket/live/hourly_bars
```

### configs/kalshi.yaml

Contains Kalshi-specific settings for signal thresholds, market selection, and paper trading parameters.
