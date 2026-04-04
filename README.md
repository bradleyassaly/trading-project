# Trading Platform

Autonomous prediction market trading platform. Scans Kalshi Economics markets (CPI, Fed, GDP, Jobs, PCE, Inflation), fires validated signals, places paper trades, and tracks P&L. Goal: profitable live trading via signal-based edge and insider detection.

## Current Status

- **Signal Research Lab**: calibration_drift 56.2% WR, volume_spike 55.2% WR, time_decay 54.8% WR (validated on 48,509 Manifold markets)
- **Live data**: 213 open Kalshi Economics markets with hourly candles, 75 active Polymarket markets via WebSocket
- **Autonomous loop**: scans markets every 15 min, runs 8 signal families, places paper trades
- **Paper portfolio**: $500 starting capital, $5-15 per trade, Kelly-sized
- **Insider detection**: wallet profiler identifies early informed traders from on-chain Polymarket data
- **Test suite**: 1,619 passing tests

## Quick Start

```bash
# 1. Start Kalshi live candle collection (requires .env with Kalshi API key)
trading-cli data kalshi live-candles --lookback-days 30

# 2. Start Polymarket live WebSocket collector
trading-cli data polymarket live-collect --config configs/polymarket.yaml

# 3. Run backtest across all data sources
trading-cli research kalshi-full-backtest \
  --include-manifold --include-metaculus \
  --manifold-feature-dir data/manifold/features \
  --manifold-resolution-data data/manifold/resolution.csv

# 4. Run one scan + paper trade cycle
python -c "
from dotenv import load_dotenv; load_dotenv()
from trading_platform.kalshi.market_scanner import KalshiMarketScanner
from trading_platform.kalshi.paper_executor import KalshiPaperExecutor
from trading_platform.kalshi.client import KalshiClient
from trading_platform.kalshi.auth import KalshiConfig
auth = KalshiConfig.from_env()
client = KalshiClient(auth)
scanner = KalshiMarketScanner(client)
executor = KalshiPaperExecutor()
executor.check_resolutions(client)
results = scanner.scan(['KXCPI','KXFED','KXGDP','KXJOBS','KXPCE','KXINFL'])
for r in results:
    if r.confidence > 0.3:
        executor.execute_trade(r)
print(executor.get_summary())
"

# 5. GUI
cd src/trading_platform/frontend && npm run dev   # http://localhost:5173
uvicorn trading_platform.api.main:app --port 8001  # API backend
```

## Architecture

```
Data Sources            Feature Generation       Signal Scoring         Execution
-----------            ------------------       --------------         ---------
Kalshi live candles    build_kalshi_features()  8 signal families      KalshiPaperExecutor
Polymarket WebSocket   build_polymarket_feat()  calibration_drift      SQLite trade DB
Manifold dump (48.5k)  resample_trades_bars()   volume_spike           Kelly-sized trades
Metaculus API (2k)     hourly OHLCV parquets    time_decay             $5-15 per trade
PredictIt CSV                                   base_rate              Resolution tracking
Blockchain trades                               metaculus_divergence
Data API trades                                 taker_imbalance
                                                large_order
                                                unexplained_move
```

## Signal Library (8 families)

| Signal | What it measures | Validated WR |
|--------|-----------------|-------------|
| calibration_drift | Mean-reversion when price overshoots calibrated probability | 56.2% |
| volume_spike | Follow informed money direction on unusual volume | 55.2% |
| time_decay | Fade uncertainty premium as close date approaches | 54.8% |
| base_rate | Edge vs. historical base rate of similar events | -- |
| metaculus_divergence | Gap between Kalshi and Metaculus consensus | -- |
| taker_imbalance | Net aggressive buyer vs seller flow | -- |
| large_order | Unusually large single orders (informed flow) | -- |
| unexplained_move | Price moves without corresponding volume (information leak) | -- |

Win rates validated on 48,509 resolved Manifold markets. Signals marked "--" require real-time order flow data not yet available in backtest.

## Data Sources

| Source | Type | Auth | Markets | Usage |
|--------|------|------|---------|-------|
| Kalshi live candles | Authenticated API | API key | 213 open Economics | Live trading signals |
| Polymarket WebSocket | Public WS | None | 75 active (30-day horizon) | Live price monitoring |
| Manifold dump | Static JSON | None | 48,509 resolved (2021-2024) | Backtesting |
| Metaculus API | Public REST | None | 2,000 resolved | Backtesting + divergence signal |
| PredictIt CSV | Static CSV | None | Historical | Backtesting |
| Polymarket Data API | Public REST | None | All markets | Wallet profiling + insider detection |
| Goldsky subgraph | Public GraphQL | None | Orderbook depth | Microstructure signals |

## Known Limitations

- Kalshi historical candlestick endpoint (`/historical/markets/{ticker}/candlesticks`) returns 404 on free API tier. Use live candle collector for open markets only.
- Manifold uses play money (Mana) not real USD. Volume figures are not directly comparable to Kalshi.
- Polymarket WebSocket occasionally sends messages as JSON arrays, not objects. The collector handles this.
- Wallet profiler requires trade history from data-api.polymarket.com or blockchain CSV. No automated download yet.
- Paper executor does not account for spread/slippage. Real trades will have worse execution.

## Roadmap

**Phase 1 (complete)**: Data foundation + signal validation on 48,509+ markets
**Phase 2 (in progress)**: Live data collection + autonomous paper trading
**Phase 3 (next)**: First paper trade resolutions + real money decision
