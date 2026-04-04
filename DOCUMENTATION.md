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

# Shared replay evaluation from the registry-backed reader layer
trading-cli research replay evaluate \
  --registry-path data/research/dataset_registry.json \
  --providers binance kalshi \
  --alignment-mode outer_union \
  --output-dir artifacts/research_replay/evaluation
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
| `artifacts/research_replay/evaluation/` | Replay evaluation summaries and metric tables |
| `artifacts/provider_monitoring/monitoring_history.jsonl` | Shared provider monitoring snapshot history |
| `artifacts/provider_monitoring/latest_transition_summary.json` | Latest shared provider/dataset status transitions |

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
| `/api/research/replay/evaluation-preview` | GET | Registry-backed replay evaluation preview |
| `/api/ops/providers/{provider}/history-summary` | GET | Compact provider monitoring history summary |
| `/api/ops/datasets/{dataset_key}/history-summary` | GET | Compact dataset monitoring history summary |

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

## G-15 - Registry-Backed Replay Evaluation and Monitoring History

Date: 2026-04-04
Status: DONE

### What Changed

- Added `src/trading_platform/research/replay_evaluation.py` as a narrow evaluation runner on top of the shared replay-consumer layer.
- Added `src/trading_platform/monitoring/history_summary.py` for compact provider and dataset trend summaries derived from existing monitoring-history and transition artifacts.
- Extended the FastAPI artifact readers and API routes with replay-evaluation previews plus provider and dataset history summaries.
- Extended the Research Data dashboard to show replay-evaluation summaries and compact monitoring-history views.
- Added the `research replay evaluate` CLI entry point.

### Why It Changed

The shared replay layer could already assemble and consume mixed-provider datasets, but there was no stable runner to evaluate those replay inputs through a machine-readable contract. Operators also had timeline snapshots, but not compact historical summaries that made degradation and recovery easy to scan. G-15 adds both without changing provider-specific ingest, feature, or sync behavior.

### Files Changed

- `src/trading_platform/research/replay_evaluation.py`
- `src/trading_platform/research/__init__.py`
- `src/trading_platform/monitoring/history_summary.py`
- `src/trading_platform/api/artifact_reader.py`
- `src/trading_platform/api/main.py`
- `src/trading_platform/frontend/src/api/client.js`
- `src/trading_platform/frontend/src/pages/ResearchData.jsx`
- `src/trading_platform/cli/commands/research_replay_evaluate.py`
- `src/trading_platform/cli/grouped_parser.py`
- `tests/test_replay_evaluation.py`
- `tests/api/test_api_endpoints.py`
- `tests/test_cli_grouping.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

### Tests Run

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_evaluation.py tests/test_replay_consumer.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`

### Exact Verification Commands

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_evaluation.py tests/test_replay_consumer.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m trading_platform.cli research replay evaluate --registry-path data/research/dataset_registry.json --providers binance kalshi --alignment-mode outer_union --output-dir artifacts/research_replay/evaluation`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m uvicorn trading_platform.api.main:app --port 8001`

### Design Notes

- The replay evaluation runner is intentionally narrow. It consumes `ReplayConsumerRequest` inputs and emits one stable summary payload plus an optional CSV metrics table.
- Supported first-pass metrics are Pearson correlation, Spearman correlation, directional accuracy, and quantile bucket top-minus-bottom spread.
- The evaluation summary records the replay-consumer request, resolved registry-backed context, selected feature and target columns, warnings, and metric rows so future runs can be compared without re-reading raw provider paths.
- Monitoring history summaries are derived from `artifacts/provider_monitoring/monitoring_history.jsonl` and `artifacts/provider_monitoring/latest_transition_summary.json`. No parallel history database was added.

### Known Limitations

- Replay evaluation is not a model-training framework. It is a bounded metrics runner over existing replay-consumer outputs.
- Mixed-provider evaluations can be sparse when datasets align imperfectly. The summary exposes warnings and row counts, but it does not try to repair provider-specific semantics.
- The dashboard shows compact trend summaries and a bounded metric table, not full charted historical analytics.

### Suggested Next Milestone

- `G-16 - Add cross-provider replay comparison workflows and richer research-selection views on top of shared evaluation artifacts`
