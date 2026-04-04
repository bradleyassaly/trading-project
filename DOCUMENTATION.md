# Technical Documentation

## CLI Commands

### Kalshi Data
```bash
# Historical ingest (settled markets, requires API key)
trading-cli data kalshi historical-ingest --config configs/kalshi.yaml
trading-cli data kalshi historical-ingest --reprocess  # re-fetch candles for existing markets

# Live candle collection (open markets, authenticated series endpoint)
trading-cli data kalshi live-candles --lookback-days 30
trading-cli data kalshi live-candles --loop --interval 60  # continuous hourly

# Recent market ingest (live API)
trading-cli data kalshi recent-ingest --config configs/kalshi.yaml

# Dataset validation
trading-cli data kalshi validate-dataset --config configs/kalshi.yaml
```

### Polymarket Data
```bash
# Historical ingest (closed resolved markets from Gamma API)
trading-cli data polymarket ingest --config configs/polymarket.yaml

# Live WebSocket collector (open markets, no auth)
trading-cli data polymarket live-collect --config configs/polymarket.yaml

# Blockchain trade ingest (from poly-trade-scan CSV)
trading-cli data polymarket blockchain-ingest --trades-csv data/polymarket/raw/blockchain_trades.csv

# CLOB trade history fetch
trading-cli data polymarket clob-fetch --hours-back 168

# Data API trade fetch (best for wallet profiling, no auth)
trading-cli data polymarket data-api-fetch --hours-back 168

# Goldsky orderbook snapshot
trading-cli data polymarket orderbook-fetch

# Wallet profiling (smart money detection)
trading-cli data polymarket wallet-profiles \
  --trades-csv data/polymarket/data_api_trades/recent_*.csv \
  --resolution-csv data/polymarket/blockchain/resolution.csv
```

### Other Data Sources
```bash
# Manifold Markets dump parser (play money, 48k+ markets)
trading-cli data manifold parse --dump-dir ~/Downloads/manifold_dump --min-bets 10

# PredictIt historical CSV parser (real USD)
trading-cli data predictit parse --csv-path data/predictit/raw/market_data.csv

# Metaculus resolved questions (no auth)
trading-cli data metaculus fetch --limit 2000

# Economic news calendar
trading-cli data news upcoming --days 14
trading-cli data news label-moves --ticker KXCPI-26APR-T0.3
```

### Research & Backtesting
```bash
# Full backtest across all sources
trading-cli research kalshi-full-backtest \
  --include-polymarket --include-manifold --include-metaculus

# Kalshi-only backtest on live features
trading-cli research kalshi-full-backtest \
  --feature-dir data/kalshi/live/features
```

## Data Directories

| Path | Contents |
|------|----------|
| `data/kalshi/live/candles/` | 213 raw JSON candle files for open Economics markets |
| `data/kalshi/live/features/` | 213 parquet feature files with all 8 signal columns |
| `data/kalshi/raw/markets/` | Raw market JSON from historical ingest |
| `data/kalshi/paper_trades.db` | SQLite paper trading portfolio + trade log |
| `data/manifold/features/` | 48,509 parquet files (Manifold 2021-2024, play money) |
| `data/metaculus/features/` | Resolved question features from Metaculus API |
| `data/predictit/features/` | PredictIt historical daily features |
| `data/polymarket/live/prices.db` | SQLite tick DB from live WebSocket collector |
| `data/polymarket/live/hourly_bars/` | Hourly OHLCV parquets exported from live ticks |
| `data/polymarket/raw/` | Blockchain trade CSV, raw market JSON |
| `data/polymarket/data_api_trades/` | Trade CSVs from data-api.polymarket.com |
| `data/polymarket/wallet_profiles.parquet` | Smart money wallet flags |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Service health check |
| `/api/system/status` | GET | Loop state, active strategies |
| `/api/signals/performance` | GET | Signal family win rates (merged across all sources) |
| `/api/paper/portfolio` | GET | Paper trading P&L summary |
| `/api/paper/trades` | GET | Last 50 paper trades |
| `/api/paper/scan` | GET | Latest market scan results |
| `/api/kalshi/markets` | GET | Kalshi markets with signal values |
| `/api/polymarket/live-markets` | GET | Active Polymarket markets with prices |
| `/api/polymarket/market-ticks/{id}` | GET | Tick history + price chart data |
| `/api/loop/decisions` | GET | Autonomous loop decision log |
| `/api/loop/control` | POST | Pause/resume/trigger loop |

## Signal Descriptions

### calibration_drift (56.2% WR)
Measures when a market price deviates from its calibrated probability curve. High z-score = price has drifted up too far, fade it. Uses log-odds transformation for numerical stability. Direction: -1 (contrarian).

### volume_spike (55.2% WR)
Detects unusual volume relative to 20-day average. When volume spikes, follow the direction of the price move — informed money is entering. Uses z-score normalization. Direction: +1 (follow).

### time_decay (54.8% WR)
Measures the "tension" between price uncertainty and time remaining. As close date approaches, uncertainty premium should decay. High tension = fade it. Requires `days_to_close` column. Direction: -1 (fade).

### base_rate
Compares current price to historical base rate for similar event types. If CPI has been above threshold 70% of the time historically but price is at 50%, there's edge. Requires base rate database.

### metaculus_divergence
Compares Kalshi price to Metaculus community forecast for the same event. Large divergence = potential mispricing. Requires matched Kalshi-Metaculus question pairs.

### taker_imbalance, large_order, unexplained_move
Informed flow signals from order-level data. Detect when aggressive buyers/sellers move the market, when unusually large orders appear, or when price moves without visible volume (information leak). Require real-time order flow data.

## Insider Detection Architecture

1. **Data collection**: Fetch trade history from `data-api.polymarket.com/trades` (no auth, all markets)
2. **Wallet profiling**: For each wallet, compute win rate across resolved markets. Early win rate (trades >24h before close) separates genuine insiders from late arbitrageurs.
3. **Smart money flags**: Wallets with `early_win_rate >= 0.65` and `early_trades >= 5` are flagged as `is_early_informed = True`
4. **Real-time signal**: When flagged wallets trade on open markets, compute `smart_buy_volume - smart_sell_volume` imbalance as a directional signal
5. **Integration**: Signal feeds into market scanner alongside 8 Kalshi signal families

## Configuration

### configs/kalshi.yaml
- `historical_ingest.direct_series_tickers`: Economics/Politics series to track
- `historical_ingest.use_direct_series_fetch: true`: Uses authenticated events→markets path
- `historical_ingest.skip_historical_pagination: true`: Skips broken historical endpoint
- `lookback_days: 365`: How far back to scan

### configs/polymarket.yaml
- `market_selection.end_date_max_days: 30`: Only collect markets resolving within 30 days
- `market_selection.min_volume: 10000`: Minimum lifetime volume
- `market_selection.max_markets: 75`: Total markets to track
