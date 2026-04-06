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

# Compare replay evaluations across providers and alignment modes
trading-cli research replay compare \
  --registry-path data/research/dataset_registry.json \
  --providers binance kalshi polymarket \
  --alignment-modes outer_union anchor \
  --comparison-mode provider \
  --output-dir artifacts/research_replay/comparison

# Persist append-only replay history and evaluate promotion-style research gates
trading-cli research replay gate \
  --evaluation-root artifacts/research_replay/evaluation \
  --comparison-root artifacts/research_replay/comparison \
  --history-output-dir artifacts/research_replay/history \
  --gating-output-dir artifacts/research_replay/gating

# Build review queues and replay-history drift summaries
trading-cli research replay review \
  --evaluation-root artifacts/research_replay/evaluation \
  --comparison-root artifacts/research_replay/comparison \
  --history-output-dir artifacts/research_replay/history \
  --gating-output-dir artifacts/research_replay/gating \
  --review-output-dir artifacts/research_replay/review
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
| `artifacts/research_replay/comparison/` | Replay comparison summaries, rankings, and candidate slices |
| `artifacts/research_replay/history/` | Append-only replay evaluation/comparison history artifacts |
| `artifacts/research_replay/gating/` | Latest replay research gating summaries and status buckets |
| `artifacts/research_replay/review/` | Latest review queue summaries and replay drift summaries |
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
| `/api/research/replay/comparison-preview` | GET | Ranked replay comparison preview across evaluation slices |
| `/api/research/replay/comparison-latest` | GET | Latest replay comparison artifact if one has been written |
| `/api/research/replay/gating-latest` | GET | Latest replay research gating summary artifact |
| `/api/research/replay/history` | GET | Recent shared replay history records filtered by candidate/provider |
| `/api/research/replay/review-queue-latest` | GET | Latest replay research review queue summary artifact |
| `/api/research/replay/drift-latest` | GET | Latest replay-history drift summary artifact |
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

## G-16 - Cross-Provider Replay Comparison and Research Selection Views

Date: 2026-04-04
Status: DONE

### What Changed

- Added `src/trading_platform/research/replay_comparison.py` as a narrow comparison runner on top of replay-evaluation summaries.
- Added `research replay compare` for artifact-backed or on-demand comparison runs.
- Extended API readers and routes with replay comparison preview and latest-comparison artifact support.
- Extended the Research Data dashboard with a ranked replay-comparison candidate panel.

### Why It Changed

G-15 made replay evaluations machine-readable, but there was still no stable way to compare providers, datasets, targets, or alignment modes and decide which slices looked worth pursuing next. G-16 adds that selection layer without changing provider-specific ingestion or replay pipelines.

### Files Changed

- `src/trading_platform/research/replay_comparison.py`
- `src/trading_platform/research/__init__.py`
- `src/trading_platform/api/artifact_reader.py`
- `src/trading_platform/api/main.py`
- `src/trading_platform/frontend/src/api/client.js`
- `src/trading_platform/frontend/src/pages/ResearchData.jsx`
- `src/trading_platform/cli/commands/research_replay_compare.py`
- `src/trading_platform/cli/grouped_parser.py`
- `tests/test_replay_comparison.py`
- `tests/test_replay_evaluation.py`
- `tests/api/test_api_endpoints.py`
- `tests/test_cli_grouping.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

### Tests Run

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_comparison.py tests/test_replay_evaluation.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/test_replay_comparison.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`

### Exact Verification Commands

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_comparison.py tests/test_replay_evaluation.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/test_replay_comparison.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m trading_platform.cli research replay compare --registry-path data/research/dataset_registry.json --providers binance kalshi polymarket --alignment-modes outer_union anchor --comparison-mode provider --output-dir artifacts/research_replay/comparison`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m uvicorn trading_platform.api.main:app --port 8001`

### Design Notes

- The comparison runner is artifact-first. If `--evaluation-summary-paths` are provided, it compares those summaries directly. Otherwise it generates bounded replay evaluations on demand from the shared registry and replay-consumer layer.
- Supported first comparison modes are explicit:
  - `provider`
  - `dataset`
  - `scope`
- Ranking is intentionally simple and transparent. Each evaluated slice gets a composite score built from row-count coverage plus existing evaluation metrics:
  - absolute correlation
  - directional accuracy above a 0.5 baseline
  - absolute top-minus-bottom spread
- Slices below `min_row_count` are retained as exclusions with warnings instead of silently disappearing.

### Produced Artifacts

- `artifacts/research_replay/comparison/latest_replay_comparison_summary.json`
- `artifacts/research_replay/comparison/latest_replay_comparison_rankings.csv`
- `artifacts/research_replay/comparison/latest_replay_candidate_slices.json`

These artifacts reference replay-evaluation summaries rather than inventing a new truth source.

### API / Dashboard Additions

- `GET /api/research/replay/comparison-preview`
- `GET /api/research/replay/comparison-latest`

The Research Data dashboard now shows:

- ranked replay candidate slices
- candidate counts and exclusions
- warning visibility for sparse or skipped slices

### Known Limitations

- Replay comparison is still a bounded ranking layer, not a general experiment-management system.
- Provider comparisons are only as good as the evaluation metrics and target availability already present in replay-evaluation summaries.
- The dashboard stays compact and inspection-oriented. It is not a full research notebook or visual analytics workbench.

### Suggested Next Milestone

- `G-17 - Shared replay evaluation history and promotion-style research gating`

## G-17 - Shared Replay Evaluation History and Promotion-Style Research Gating

Date: 2026-04-04
Status: DONE

### What Changed

- Added `src/trading_platform/research/replay_history.py` to persist append-only shared replay history from existing replay evaluation and replay comparison summary artifacts.
- Added `src/trading_platform/research/replay_gating.py` to evaluate promotion-style replay research gates and classify candidate slices as `promotable`, `watchlist`, or `rejected`.
- Added `research replay gate` as a thin CLI that updates shared replay history and writes latest gating summaries.
- Extended FastAPI artifact readers and routes with latest replay gating summaries plus recent shared replay history reads.
- Extended the Research Data dashboard with a lightweight research-gating panel that shows status counts and top promotable/watchlist slices.

### Why It Changed

G-16 made replay comparisons useful for ranking slices, but the platform still lacked a durable, cross-run memory of those results and a structured way to turn them into research-governance decisions. G-17 adds a stable history artifact layer plus configurable promotion-style gating without changing provider-specific ingest, feature, sync, registry, replay-evaluation, or replay-comparison behavior.

### Files Changed

- `src/trading_platform/research/replay_history.py`
- `src/trading_platform/research/replay_gating.py`
- `src/trading_platform/research/__init__.py`
- `src/trading_platform/cli/commands/research_replay_gate.py`
- `src/trading_platform/cli/grouped_parser.py`
- `src/trading_platform/api/artifact_reader.py`
- `src/trading_platform/api/main.py`
- `src/trading_platform/frontend/src/api/client.js`
- `src/trading_platform/frontend/src/pages/ResearchData.jsx`
- `tests/test_replay_history.py`
- `tests/test_replay_gating.py`
- `tests/api/test_api_endpoints.py`
- `tests/test_cli_grouping.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

### Tests Run

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_history.py tests/test_replay_gating.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/test_replay_comparison.py tests/test_replay_history.py tests/test_replay_gating.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`

### Exact Verification Commands

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_history.py tests/test_replay_gating.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/test_replay_comparison.py tests/test_replay_history.py tests/test_replay_gating.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m trading_platform.cli research replay gate --evaluation-root artifacts/research_replay/evaluation --comparison-root artifacts/research_replay/comparison --history-output-dir artifacts/research_replay/history --gating-output-dir artifacts/research_replay/gating`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m uvicorn trading_platform.api.main:app --port 8001`

### History Artifact Layout

- `artifacts/research_replay/history/shared_replay_history.jsonl`
  - append-only record stream across replay evaluation and replay comparison imports
  - stores `history_run_id`, `recorded_at`, source artifact references, provider/dataset scope, alignment mode, feature/target columns, key metrics, and comparison rank metadata when available
- `artifacts/research_replay/history/latest_replay_history_summary.json`
  - latest import summary with discovered source artifacts, appended record count, total record count, and warnings

### Gating Concepts

- Gates reuse the repo’s promotion-style gate contract from `trading_platform.governance.models` rather than inventing a new schema.
- G-17 introduces replay-research statuses:
  - `promotable`
  - `watchlist`
  - `rejected`
- Hard gates currently cover:
  - minimum sample size
  - minimum replay run count
  - minimum mean absolute Spearman signal strength
- Soft gates currently cover:
  - directional accuracy
  - mean composite score
  - score stability
  - comparison rank when available
  - cross-provider support
- This is research governance, not live trading promotion governance. It ranks and filters replay slices for further research review; it does not authorize paper/live deployment or bypass existing strategy governance.

### API Surface

- `GET /api/research/replay/gating-latest`
- `GET /api/research/replay/history?candidate_id=...&provider=...&limit=...`

These endpoints are read-only and artifact-backed.

### Known Limitations

- Shared replay history only becomes durable for runs that are imported into `artifacts/research_replay/history/`; it does not retroactively reconstruct overwritten historical artifacts.
- Current gating thresholds are CLI-configurable and typed in code, but there is not yet a dedicated YAML policy file for replay gating.
- Replay gating intentionally stays lightweight. It does not replace the repo’s strategy promotion, paper, or live governance systems.
- Comparison-rank gates are only meaningful when replay comparison artifacts exist for a candidate. Missing comparison rank is treated as a soft limitation, not a hard failure.

### Suggested Next Milestone

- `G-18 - Research promotion review queues and replay-history drift checks`

## G-18 - Research Promotion Review Queues and Replay-History Drift Checks

Date: 2026-04-04
Status: DONE

### What Changed

- Added `src/trading_platform/research/replay_review.py` as an additive review-governance layer on top of replay history and replay gating.
- Added deterministic review queue generation with queue states:
  - `promotable_review`
  - `watchlist_review`
  - `rejected_archive`
  - `needs_rerun`
- Added replay-history drift checks with `stable`, `warning`, and `drifted` statuses plus queue recommendations.
- Added `research replay review` as a thin CLI that refreshes replay history, rebuilds gating, evaluates drift, and writes latest review artifacts.
- Extended API readers/routes and the Research Data dashboard with latest review-queue and drift summaries.
- Hardened `tests/test_provider_registry_and_monitoring.py` to use a current-time-relative timestamp so monitoring verification remains deterministic.

### Why It Changed

G-17 could tell the system which replay slices were promotable, watchlist, or rejected, but it did not organize them into an explicit operator review workflow or monitor whether their replay behavior was drifting over time. G-18 adds those operator-facing layers without changing provider-specific ingest, replay evaluation, replay comparison, or gating logic.

### Files Changed

- `src/trading_platform/research/replay_review.py`
- `src/trading_platform/research/__init__.py`
- `src/trading_platform/cli/commands/research_replay_review.py`
- `src/trading_platform/cli/grouped_parser.py`
- `src/trading_platform/api/artifact_reader.py`
- `src/trading_platform/api/main.py`
- `src/trading_platform/frontend/src/api/client.js`
- `src/trading_platform/frontend/src/pages/ResearchData.jsx`
- `tests/test_replay_review.py`
- `tests/api/test_api_endpoints.py`
- `tests/test_cli_grouping.py`
- `tests/test_provider_registry_and_monitoring.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

### Tests Run

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_review.py tests/test_replay_history.py tests/test_replay_gating.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/test_replay_comparison.py tests/test_replay_history.py tests/test_replay_gating.py tests/test_replay_review.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`

### Exact Verification Commands

- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_replay_review.py tests/test_replay_history.py tests/test_replay_gating.py tests/api/test_api_endpoints.py tests/test_cli_grouping.py -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_shared_dataset_reader.py tests/test_replay_assembly.py tests/test_replay_consumer.py tests/test_replay_evaluation.py tests/test_replay_comparison.py tests/test_replay_history.py tests/test_replay_gating.py tests/test_replay_review.py tests/api/test_api_endpoints.py tests/test_provider_registry_and_monitoring.py tests/test_research_dataset_registry.py tests/test_cli_grouping.py tests/binance/test_registry_integration.py -q`
- `Remove-Item Env:TP_ALERT_SMTP_HOST -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PORT -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USERNAME -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_PASSWORD -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SMTP_USE_TLS -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_FROM -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_TO -ErrorAction SilentlyContinue; Remove-Item Env:TP_ALERT_SUBJECT_PREFIX -ErrorAction SilentlyContinue; C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe -q`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m trading_platform.cli research replay review --evaluation-root artifacts/research_replay/evaluation --comparison-root artifacts/research_replay/comparison --history-output-dir artifacts/research_replay/history --gating-output-dir artifacts/research_replay/gating --review-output-dir artifacts/research_replay/review`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m uvicorn trading_platform.api.main:app --port 8001`

### Queue Artifact Layout

- `artifacts/research_replay/review/latest_review_queue_summary.json`
  - latest deterministic queue assignment derived from latest gating plus drift summaries
- `artifacts/research_replay/review/latest_promotable_review.json`
- `artifacts/research_replay/review/latest_watchlist_review.json`
- `artifacts/research_replay/review/latest_needs_rerun.json`
- `artifacts/research_replay/review/latest_rejected_archive.json`

Each queue entry records queue state, provider scope, latest gating status, latest drift status, concise reasons, supporting metrics, timestamps, and provenance links back to replay history and gating artifacts.

### Drift-Check Concepts

- Drift checks are promotion-style named checks built with the same `PromotionGateResult` contract used elsewhere in governance.
- Current drift checks cover:
  - recent support sufficiency
  - Spearman degradation versus trailing history
  - directional-accuracy degradation versus trailing history
  - rank-percentile worsening
  - recent score stability
  - recent provider support
- Drift status semantics:
  - `stable`
  - `warning`
  - `drifted`
- Recommendations currently include:
  - `keep_in_queue`
  - `rerun`
  - `deprioritize`

### Queue State vs Gating Status

- Gating status answers whether a replay slice currently looks promotable, watchlist, or rejected from a research-quality perspective.
- Queue state answers where that slice belongs in the operator review workflow right now.
- A slice can therefore be:
  - `promotable` but moved to `needs_rerun` if drifted strongly
  - `watchlist` and remain in `watchlist_review`
  - `rejected` and live in `rejected_archive`

### API Surface

- `GET /api/research/replay/review-queue-latest`
- `GET /api/research/replay/drift-latest`

These endpoints are read-only and artifact-backed.

### Known Limitations

- Review queues are deterministic latest-state artifacts; they are not yet operator-action queues with accept/defer/rerun acknowledgements.
- Drift checks are history-window heuristics, not a full statistical change-point system.
- Cross-provider drift is currently inferred from shared candidate/provider history rather than a dedicated provider-agreement model.
- Queue generation depends on the quality of replay history imports; overwritten or missing upstream artifacts will reduce drift context.

### Suggested Next Milestone

- `G-19 - Research queue actions and replay-governance audit decisions`
