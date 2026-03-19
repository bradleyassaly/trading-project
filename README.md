# Trading Platform

A Python trading research and execution platform for ingesting market data, building features, generating signals, running backtests, validating strategies, constructing portfolios, and preparing the system for paper/live trading.

## Goal

The long-term goal of this repository is to become an **automated trading platform** that can:

1. ingest and maintain market data across multiple universes,
2. generate and validate alpha features,
3. test strategies with robust research workflows,
4. construct risk-aware portfolios,
5. rebalance according to execution policies,
6. run paper trading safely, and
7. eventually support controlled live trading.

## Current capabilities

Based on the current codebase, the project already supports:

- OHLCV ingestion and feature generation
- CLI-driven research workflows
- single-asset research and vectorized simulation
- parameter sweeps and walk-forward validation
- portfolio and top-N portfolio backtests
- basic execution/rebalance policies
- risk sizing utilities such as equal-weight and inverse-volatility sizing
- experiment and artifact output handling
- unit tests across ingest, features, configs, backtests, and artifact services

## Current project structure

```text
trading-project/
├─ configs/
├─ data/
├─ src/trading_platform/
│  ├─ backtests/
│  ├─ cli/
│  │  └─ commands/
│  ├─ config/
│  ├─ construction/
│  ├─ data/
│  ├─ execution/
│  ├─ experiments/
│  ├─ features/
│  ├─ metadata/
│  ├─ portfolio/
│  ├─ research/
│  ├─ risk/
│  ├─ schemas/
│  ├─ services/
│  ├─ signals/
│  ├─ simulation/
│  ├─ strategies/
│  └─ settings.py
├─ tests/
├─ pyproject.toml
└─ README.md
```

## CLI workflows

The CLI currently exposes commands for:

- `ingest`
- `features`
- `research`
- `pipeline`
- `list-universes`
- `list-strategies`
- `sweep`
- `walkforward`
- `portfolio`
- `portfolio-topn`

This makes the repository a good base for a research platform rather than just a single backtest script.

## Installation

```bash
git clone https://github.com/bradleyassaly/trading-project.git
cd trading-project
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows PowerShell
pip install -e .[dev]
```

## Example usage

### 1) Ingest data

```bash
trading-cli ingest --symbols AAPL MSFT NVDA --start 2018-01-01
```

### 2) Build features

```bash
trading-cli features --symbols AAPL MSFT NVDA
```

### 3) Run strategy research

```bash
trading-cli research --symbols AAPL --strategy sma_cross --fast 20 --slow 100 --engine vectorized
```

### 4) Run a parameter sweep

```bash
trading-cli sweep \
  --symbols AAPL \
  --strategy sma_cross \
  --fast-values 10 20 50 \
  --slow-values 100 150 200
```

### 5) Run walk-forward validation

```bash
trading-cli walkforward \
  --symbols AAPL \
  --strategy sma_cross \
  --train-years 5 \
  --test-years 1 \
  --select-by "Sharpe Ratio"
```

### 6) Run a cross-sectional top-N portfolio

```bash
trading-cli portfolio-topn \
  --symbols AAPL MSFT NVDA AMZN GOOGL META \
  --strategy sma_cross \
  --top-n 3 \
  --weighting-scheme inverse_vol \
  --vol-window 20 \
  --rebalance-frequency weekly
```

## Architectural direction

A useful way to think about this repository is as five layers:

### 1. Data layer
Responsible for raw data ingestion, metadata, parquet storage, and dataset versioning.

### 2. Research layer
Responsible for feature generation, signal generation, backtests, sweeps, walk-forward tests, and experiment tracking.

### 3. Portfolio layer
Responsible for combining signals into holdings, assigning weights, applying constraints, and producing target portfolios.

### 4. Execution layer
Responsible for rebalance calendars, turnover control, broker adapters, order generation, fills, slippage, and paper/live state.

### 5. Orchestration layer
Responsible for scheduled jobs, dependency ordering, logging, monitoring, retries, and promotion from research to paper to live.

## Recommended next architecture improvements

### 1) Separate research models from execution models
Right now the repository has strong research and simulation components, but the next milestone is to introduce explicit live-trading objects such as:

- `TargetPortfolio`
- `CurrentPortfolio`
- `OrderRequest`
- `FillEvent`
- `BrokerPosition`
- `ExecutionReport`

That prevents backtest-only assumptions from leaking into paper/live trading.

### 2) Add a formal portfolio construction pipeline
You already have portfolio, risk, construction, and execution modules. The next step is to make the handoff explicit:

```text
signals -> selection -> raw weights -> constraints -> target weights -> rebalance decision -> orders
```

That pipeline should be represented by stable interfaces and test fixtures.

### 3) Create a strategy contract and a feature contract
Strategies should consume a well-defined signal input schema and produce a well-defined output schema. That will make automated feature testing much easier when you start generating many features across universes.

Suggested conventions:

- features are columns with metadata
- signals are columns like `score`, `position`, `entry_flag`, `exit_flag`
- portfolio inputs are symbol-by-date score/return panels
- simulations consume a normalized contract rather than strategy-specific frames

### 4) Add environment-aware configuration
Move toward structured config for:

- data sources
- universes
- broker settings
- slippage/commission models
- paper vs live environments
- risk limits
- job schedules

This becomes important as soon as the platform runs unattended.

### 5) Add a state store for paper/live trading
For automated trading, you need durable state beyond parquet artifacts. Add a store for:

- latest signals
- desired target weights
- submitted orders
- fills
- current positions
- cash/equity snapshots
- last successful job timestamps

SQLite is enough to start. Postgres is better once the platform grows.

### 6) Unify the legacy and vectorized paths
The CLI already exposes both `legacy` and `vectorized` engines. Over time, pick one canonical simulation core and make the other either:

- a compatibility wrapper, or
- a deprecated path.

Dual engines are useful in transition, but they raise maintenance cost and test surface area.

### 7) Make universes first-class objects
Since your goal is automation across larger universes, introduce clear universe definitions such as:

- `sp500_large_cap`
- `nasdaq_100`
- `crypto_top_liquid`
- `futures_trend`

A universe should have:

- membership source
- refresh rules
- symbol metadata
- sector/group metadata
- liquidity filters
- trading calendar

## What to build next

To move from a research repo to an automated trading platform, I would build the next pieces in this order.

### Phase 1: production-grade research foundation

1. **Dataset registry**  
   Track what raw data, features, and experiment outputs were built, when, and from which configs.

2. **Feature registry**  
   A central place to register features, required inputs, lookback windows, and whether they are point-in-time safe.

3. **Cross-sectional research workflow**  
   Run feature generation and walk-forward tests across a whole universe, not just symbol-by-symbol.

4. **Research leaderboard**  
   Save per-feature and per-strategy metrics so you can rank candidates by return, Sharpe, turnover, drawdown, and stability.

### Phase 2: portfolio construction engine

5. **Constraint engine**  
   Build reusable constraints for:
   - max position weight
   - max sector/group weight
   - max names per sector/group
   - turnover caps
   - gross/net exposure caps
   - liquidity limits

6. **Ranking + selection framework**  
   Convert scores into holdings with rules like top-N, threshold, long-short, volatility-scaled, or regime-filtered selection.

7. **Transaction cost model**  
   Add slippage, spread, commissions, market impact proxies, and borrow assumptions where relevant.

### Phase 3: paper trading stack

8. **Broker abstraction**  
   Create interfaces like:
   - `BrokerDataClient`
   - `BrokerExecutionClient`
   - `BrokerPortfolioClient`

   Then implement one paper broker first.

9. **Order management system**  
   Generate target deltas, round to tradeable sizes, submit orders, reconcile fills, and maintain order state.

10. **Paper trading loop**  
   Run on a schedule:
   - refresh data
   - rebuild features
   - generate signals
   - construct target weights
   - compare with current portfolio
   - create orders
   - record fills and PnL

### Phase 4: controlled live trading

11. **Risk guardrails**  
   Add kill switches, max daily loss, stale-data checks, max order notional, duplicate-order prevention, and market-hours gating.

12. **Monitoring and alerting**  
   Log every run and send alerts on failures, missed schedules, broken data, unexpected positions, and large drawdowns.

13. **Promotion workflow**  
   Only allow strategies to move from research -> paper -> live if they pass predefined acceptance criteria.

## Suggested target architecture

```text
trading_platform/
├─ app/                 # orchestration and use-case entrypoints
├─ domain/
│  ├─ data/
│  ├─ signals/
│  ├─ portfolio/
│  ├─ execution/
│  └─ risk/
├─ infrastructure/
│  ├─ storage/
│  ├─ brokers/
│  ├─ market_data/
│  ├─ logging/
│  └─ scheduling/
├─ research/
│  ├─ features/
│  ├─ experiments/
│  ├─ validation/
│  └─ leaderboard/
├─ cli/
└─ tests/
```

You do not need to rewrite everything now. The practical move is to evolve toward this structure gradually while keeping the current package layout working.

## Milestones for the automated trading goal

### Near term

- make universe-level walk-forward research robust
- finish portfolio constraints and ranking workflows
- standardize result artifacts and experiment metadata
- add transaction cost and slippage realism
- improve reproducibility of research runs

### Mid term

- add broker abstraction and paper trading
- persist positions/orders/state in a database
- add job orchestration and monitoring
- support scheduled daily or intraday runs

### Long term

- multi-universe research
- live trading adapters
- portfolio-of-strategies allocation
- automatic strategy promotion and retirement
- feature discovery and alpha mining workflows

## Design principles

- prefer deterministic, testable pipelines
- separate research concerns from execution concerns
- persist every important artifact
- keep configuration explicit and versioned
- make every live action auditable
- treat transaction costs and operational risk as first-class

## Disclaimer

This repository is for research and system development. Any live trading deployment should start with paper trading, explicit risk limits, and manual supervision.
