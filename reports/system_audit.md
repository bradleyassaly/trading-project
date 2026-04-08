# System Audit
Generated: 2026-04-08

## Codebase
| Metric | Value |
|--------|------:|
| Python files (`src/trading_platform/`) | 607 |
| JSX files (frontend) | 24 |
| Test files (`tests/`) | 243 |
| `api/main.py` size | 948 lines |
| Polymarket modules | 64 |
| Frontend pages | 18 |
| Polymarket CLI commands | 25 |
| Registered API routes | 102 |

### Polymarket modules (64)
`__init__`, `address_map`, `alert_log`, `alert_system`, `backtester`,
`bankroll_allocator`, `blockchain_ingest`, `client`, `clob_client`,
`clob_trades_fetcher`, `data_api_fetcher`, `enrich_resolution`, `features`,
`fusion_score`, `gamma_resolution_fetcher`, `goldsky_backfill`, `goldsky_client`,
`goldsky_stream`, `goldsky_wallet_profiler`, `graph_orderbook`, `graph_resolver`,
`historical_backfill`, `historical_ingest`, `hot_market_scanner`, `kelly_sizer`,
`kill_switch`, `leaderboard`, `leaderboard_ingest`, `live_collector`,
`live_db`, `market_close_times`, `market_data_service`, `market_universe`,
`models`, `open_positions`, `order_book_monitor`, `orderflow_collector`,
`performance_monitor`, `pmxt_smoke_test`, `polymarket_live_executor`,
`polymarket_paper_executor`, `position_fetcher`, `price_history_fetcher`,
`price_lookup`, `realtime_monitor`, `resolution_resolver`, `seed_leaderboard`,
`signal_engine`, `signal_evaluator`, `smart_money_loop`, `smart_money_signal`,
`telegram_alerts`, `wallet_analytics`, `wallet_buckets`, `wallet_db`,
`wallet_mirror`, `wallet_position_reconstructor`, `wallet_position_sync`,
`wallet_profile_rebuild`, `wallet_profiler`, `wallet_tiering`,
`wallet_trade_sync`, `whale_signal_engine`, `whale_tripwire`

### Frontend pages (18)
Backtest, Control, Dashboard, **LiveReadiness**, MarketDetail, **MarketMonitor**,
MarketScanner, Markets, MarketsHub, Paper, PipelineMonitor, PolymarketLive,
Reasoning, ResearchData, **SignalLab**, Signals, SmartMoney, WalletDetail

### Polymarket CLI commands (25)
backfill, blockchain_ingest, build_leaderboard, clob_fetch, collect_history,
daily_refresh, data_api_fetch, diagnose, fetch_resolutions, goldsky_backfill,
goldsky_profiles, ingest, ingest_top_wallets, live, mirror_scan,
open_positions, orderflow, performance_review, realtime_monitor,
refresh_positions, refresh_universe, smart_money_scan, smart_money_trade,
test_telegram, wallet_profiles

## Database (`data/polymarket/wallet_intelligence.db`, 21 tables)

| Table | Rows | Cols | Notes |
|---|---:|---:|---|
| `wallet_profiles` | 16,259 | 64 | wallet metadata |
| `wallet_trades` | 397,962 | 19 | raw fills, oldest 2022-12-12 |
| `wallet_market_positions` | 91,934 | 26 | reconstructed positions |
| `wallet_positions` | 23,178 | 21 | live snapshot |
| `market_price_history` | 242,573 | 6 | mph cache |
| `market_ticks` | 233,212 | 5 | live + historical pmxt-ish ticks |
| `market_signals` | 3,366 | 29 | signals fired |
| `wallet_alerts` | 4,850 | 17 | alert log |
| `market_anomalies` | 2,310 | 13 | order book anomalies |
| `polymarket_paper_trades` | **232** | 22 | **216 open / 16 resolved** |
| `signal_calibration` | 13 | 17 | one row per signal type |
| `calibration_reports` | 1 | 6 | |
| `live_trades` | 1 | 13 | dry-run audit log |
| `outcome_id_cache` | 0 | 5 | pmxt mapping (empty) |
| `pmxt_health` | 1 | 8 | |
| `wallet_category_profiles` | **0** | 17 | **dynamic tiering not yet built** |
| `wallet_tier_history` | 0 | 13 | |
| `leaderboard` | 219 | 22 | tier1h:149, tier1:24, tier2:46 |
| `leaderboard_meta` | 1 | 6 | |
| `category_performance` | 4 | 7 | |

### Wallet trade categories
`(null)` 289117, `crypto` 39640, `other` 36339, `sports` 15733,
`politics` 11114, `tweet_count` 4721, `economics` 1298

### Signal calibration status
| Signal | Status | Sample | Bayesian WR | Kelly |
|---|---|---:|---:|---:|
| `price_velocity` | **live** | 15 | 0.647 | 0.25 |
| `accumulation` | building | 1 | 0.333 | 0.0 |
| (11 others) | building | 0 | — | — |

## API (102 routes)

Highlights:
- **System**: `/api/system/status`, `/api/system/pipeline-status`, `/api/system/run-pipeline`
- **Smart money**: `/api/smart-money/{wallets,signals,winners,leaderboard,…}`
- **Paper**: `/api/paper/{bankroll,pnl-history,trades,positions-enriched,check-resolutions}`
- **Calibration**: `/api/calibration/{status,report,rebalance}` (5 endpoints)
- **Live**: `/api/live/{readiness,trades,emergency-stop,clear-stop,test-dry-run}`
- **Tiering**: `/api/tiers/{summary,category/{c},wallet/{w},history,rebuild}`
- **Market data (pmxt)**: `/api/market-data/{cid,/candles,/health}`
- **Market**: `/api/market/{cid}/{order-book,candles,flow,signals,intelligence}`
- **Backtest**: `/api/backtest/{run,convergence,wallet/{w}/positions,...}`

## Tests
**529 passed, 0 failed** in 22s.

## Environment
| Tool | Version |
|---|---|
| Python | 3.14.0 |
| Node | v24.14.0 |
| Docker | 29.3.0 ✅ (installed) |
| Docker Compose | v5.1.0 ✅ |
| `.env` | exists |
| Telegram bot | configured ✅ |
| Polymarket CLOB credentials | **NOT SET** |
| Polymarket wallet credentials | **NOT SET** |
| `POLYMARKET_LIVE_ENABLED` | **NOT SET** |

## Live readiness gates
| Gate | Status |
|---|---|
| Wallet universe ≥ 50 | ✅ (219) |
| Paper trades resolving | ✅ (16 resolved) |
| **Paper trades ≥ 50** | ❌ (16) |
| Calibration engine active | ✅ (1 signal live) |
| **Dynamic tiering populated** | ❌ (0 profiles) |
| Market data service implemented | ✅ |
| Kill switch implemented | ✅ |
| Live executor implemented | ✅ (DRY_RUN locked) |
| Telegram alerts working | ✅ |
| **Docker installed** | ✅ |
| **Docker orchestration files** | ❌ (must build) |
| **CLOB credentials set** | ❌ |
| **Live master switch** | ❌ (`POLYMARKET_LIVE_ENABLED=1`) |

## Summary

✅ **Working** — codebase, database, API surface, signal engine,
calibration, kill switch, paper executor, Telegram, kill-switch, dry-run
audit log, 529 passing tests.

⚠️ **Implemented but underweighted by data**:
- Dynamic tiering — schema + engine + tests + API exist, but `wallet_category_profiles` is empty (run hasn't fired because resolution events haven't happened since the migration).
- Calibration — only `price_velocity` has enough sample to be `live` (15 trades). The other 12 signals each need 15+ resolved trades.
- pmxt — module exists, lazy-init, fully fallback-safe; live calls require auth credentials we don't have.

❌ **Missing for live trading**:
1. Docker orchestration files (`docker-compose.yml`, Dockerfiles, scheduler, watchdog)
2. Polymarket CLOB API credentials (`POLYMARKET_API_KEY/SECRET/PASSPHRASE`)
3. Polymarket wallet credentials (`POLYMARKET_WALLET_ADDRESS/PRIVATE_KEY`)
4. `POLYMARKET_LIVE_ENABLED=1` master switch
5. ≥50 resolved paper trades for statistical significance
6. Active dynamic tier profiles (waiting on resolved trade volume)

📋 **Recommended next steps in order**:
1. Build Docker orchestration so the stack runs 24/7 (Part 2)
2. Run task_scheduler so resolution checker fires every 30 min → grows the resolved-trade sample
3. Fix wallet_trades category gap (289k null categories — significant data quality issue)
4. Add CLOB credentials when ready to dry-run real orders
5. Wait for ≥50 resolved trades before flipping live master switch
