# Architecture

This document explains the structural and conceptual design of the trading platform — why it is built the way it is, how the major subsystems relate to each other, and what principles guide decisions about where new code should go.

For operational instructions, see `README.md`. For the wallet intelligence system in detail, see `WALLET_INTELLIGENCE.md`. For the phase plan, see `ROADMAP.md`.

---

## Vision

The platform identifies the best-performing wallets on Polymarket by category and PnL, monitors them in real time, detects when they trade, converts that activity into signals, and automatically executes paper trades. A parallel Kalshi subsystem paper-trades Economics markets using calibration and volume signals.

The goal is automated prediction market trading via smart money detection, with a clear gated path from paper trading to live execution once performance is validated.

---

## Core Design Principles

**1. One decision pipeline across all environments**
Paper trading and live trading share the same signal logic. Only the execution adapter differs.

**2. Artifact-first for reproducibility**
Heavy outputs — feature matrices, signal snapshots, trade logs — live in files (parquet, CSV, JSON). SQLite databases serve as operational stores. The system must function without external services.

**3. Config-first workflows**
All workflows are driven by YAML configs with CLI flag overrides. Automation-friendly.

**4. Safety before autonomy**
Risk controls, kill switches, and paper validation must be in place before live capital. The system escalates to humans rather than proceeding when uncertain.

**5. Human-reviewed trading changes**
Agents may implement bounded changes. Human review is mandatory for any change that touches live broker behavior, risk limits, or promotion thresholds.

**6. Category-independent signal testing**
All 9 categories generate signals during testing. Category promotion/demotion is based on paper trade outcomes, not historical wallet PnL.

---

## Repository Layout

```
trading-project/
├── src/trading_platform/
│   ├── cli/                    # CLI entry points and commands
│   │   └── commands/           # One file per CLI command
│   ├── polymarket/             # Polymarket wallet intelligence system
│   │   ├── wallet_db.py        # SQLite central store (wallet_intelligence.db)
│   │   ├── wallet_profile_rebuild.py  # Directional WR computation
│   │   ├── wallet_buckets.py   # Behavioral classification
│   │   ├── market_universe.py  # 225-market category-organized universe
│   │   ├── whale_tripwire.py   # Real-time wallet detection
│   │   ├── whale_signal_engine.py  # Signal generation (whale_entry + convergence)
│   │   ├── polymarket_paper_executor.py  # Paper trade execution
│   │   ├── live_collector.py   # WebSocket price + whale detection
│   │   ├── smart_money_signal.py  # Legacy Goldsky-based signals
│   │   ├── realtime_monitor.py # Legacy Goldsky polling monitor
│   │   └── ...                 # Data fetchers, clients, models
│   ├── kalshi/                 # Kalshi Economics trading subsystem
│   │   ├── market_scanner.py   # Signal computation
│   │   ├── paper_executor.py   # Paper trade execution
│   │   ├── live_candle_collector.py  # Hourly candle collection
│   │   ├── signals.py          # Signal families
│   │   └── ...                 # Client, auth, models
│   ├── api/                    # FastAPI backend (port 8001)
│   │   ├── main.py             # Route definitions
│   │   └── artifact_reader.py  # DB/file readers for API responses
│   ├── frontend/src/           # React dashboard (Vite, port 5173)
│   │   ├── pages/MarketScanner.jsx  # Whale Monitor page
│   │   ├── pages/Paper.jsx     # Paper trading dashboard
│   │   └── ...                 # Dashboard, signals, wallets, etc.
│   ├── research/               # Alpha research and backtesting
│   └── monitoring/             # Provider health monitoring
├── configs/                    # YAML workflow configs
├── data/                       # Local data artifacts (gitignored)
│   ├── polymarket/
│   │   ├── wallet_intelligence.db  # Central wallet DB
│   │   ├── market_universe.json    # Active market set
│   │   ├── ws_status.json          # WebSocket health
│   │   └── live/prices.db          # Tick data
│   └── kalshi/
│       ├── paper_trades.db         # Paper trade state
│       └── live_candles/           # Hourly OHLCV parquets
├── tests/                      # 1,817 tests
└── scripts/                    # Automation and utility scripts
```

---

## The Two-Process Architecture

### Daily Intelligence Pipeline

Runs every 4 hours. Each step must succeed before the next runs.

```
Step 1: data-api-fetch
  └─ Fetch new trades from Polymarket Data API for tracked wallets
  └─ Append to wallet_trades table in wallet_intelligence.db

Step 2: wallet-profiles --from-db
  └─ Read all wallet_trades, compute directional_win_rate per wallet
  └─ Exclude tweet_count and sports from WR calculation
  └─ Write results to wallet_profiles table

Step 3: classify-wallet-buckets
  └─ Classify each wallet: directional, market_maker, arb_bot, etc.
  └─ Write wallet_type to wallet_profiles

Step 4: refresh-universe (optional)
  └─ Fetch top 25 markets per category from Gamma API
  └─ Write to data/polymarket/market_universe.json

Step 5: WhaleTripwire auto-reloads watched wallet set
  └─ Picks up new tier1/tier2 wallets from updated profiles
```

### Continuous Live Monitor

Always running. Connects to Polymarket CLOB WebSocket.

```
Startup:
  1. Load MarketUniverse (cached or fresh)
  2. Initialize WhaleTripwire (loads tier1/tier2 wallets from DB)
  3. Initialize WhaleSignalEngine + PaperExecutor
  4. Connect WebSocket, subscribe to market token IDs

Message Loop (per event):
  1. Store price tick (existing logic)
  2. tripwire.check_event(event)
  3. If whale detected → signal_engine.on_whale_trade(whale)
  4. If signal fired → paper_executor.on_signal(signal)

Periodic Tasks:
  - Every 60s: write ws_status.json (health)
  - Every 60m: export hourly OHLCV bars
  - Every 4h: tripwire.maybe_reload()
```

---

## Signal Design

### whale_entry
Fires for any tier1 or tier2 wallet trade where size >= $25.

```
confidence = directional_win_rate * min(conviction_score / 10.0, 1.0)
tier1 multiplier: 1.0x
tier2 multiplier: 0.75x
```

### convergence
Fires when 2+ watched wallets trade same side on same market within 2 hours.

```
confidence = base_confidence * 1.20 (capped at 1.0)
```

Both signals write to `market_signals` and `wallet_alerts` tables.

---

## Wallet Quality Model

**Tier 1**: directional_win_rate >= 0.58, resolved_trades >= 10, total_volume >= $5K
**Tier 2**: directional_win_rate >= 0.53, resolved_trades >= 5, total_volume >= $1K

**Conviction score** (continuous):
```
directional_wr * log10(resolved_trades + 1) * type_multiplier
type_multiplier: 1.0 (directional), 0.3 (other)
```

**Directional win rate** excludes tweet_count and sports categories. A "win" is defined as: net long on a market that resolved YES, or net short on a market that resolved NO.

---

## Category Strategy

All 9 categories monitored during testing phase:
`politics, economics, crypto, finance, sports, culture, tech, mentions, weather`

Category assignment via keyword matching on market question text (priority order — first match wins). Category performance tracked independently in `category_performance` table.

Promotion/demotion based on paper trade outcomes, not historical wallet PnL.

---

## Paper Trading Model

```
whale_entry tier1:  3% of bankroll max per trade
whale_entry tier2:  1.5% of bankroll max per trade
convergence:        5% of bankroll max per trade

actual_stake = max_allocation * confidence
Skip if confidence < 0.40 or duplicate position exists
```

Paper trades stored in `data/kalshi/paper_trades.db` with `platform='polymarket'`.

---

## Kalshi Subsystem

Separate pipeline, still active. Paper trades Economics markets (CPI, Fed, GDP, Jobs, PCE, Inflation) using 3 validated signals:
- **calibration_drift**: 56.2% WR on 48K Manifold markets
- **volume_spike**: 55.2% WR
- **time_decay**: 54.8% WR

Live candle collection runs hourly. Paper trades resolve through April 2026.

---

## Dashboard Layer

React SPA (Vite) at `http://localhost:5173`, backed by FastAPI at port 8001.

| Page | Shows |
|------|-------|
| Dashboard | System status, P&L summary, equity curve |
| Whale Monitor (/scanner) | Live whale feed, category performance, signal feed |
| Smart Money (/wallets) | Wallet leaderboard, profiles, positions |
| Signal Monitor (/signals) | Signal performance, actionable signals |
| Paper Trading (/paper) | Paper portfolio, open positions, trade history |
| Execution Engine (/engine) | Loop control, decisions log |

---

## Live Execution Gates

| # | Gate | Requirement |
|---|------|------------|
| 1 | Resolved trades | 50+ paper trades resolved with > 55% win rate |
| 2 | Category edge | At least 2 categories showing edge independently |
| 3 | Drawdown | Max drawdown < 20% of bankroll over 30 days |
| 4 | Human approval | Manual review and sign-off |
| 5 | Capital | Start with $500 real capital, 1 category only |

---

## What This Document Does Not Cover

- Equity research pipelines (legacy, not active)
- Manifold/Metaculus/PredictIt parsers (legacy, backtesting only)
- Multi-strategy portfolio construction (legacy)
- Walk-forward grid optimization (legacy)
- Database migrations (not used — schema changes via wallet_db.py)
