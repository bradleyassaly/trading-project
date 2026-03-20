# Trading Platform

A modular Python trading platform for data ingestion, feature generation, walk-forward research, cross-sectional alpha discovery, composite signal construction, portfolio backtesting, and paper trading.

The repository has moved beyond a single-strategy backtester. It now supports a research pipeline that can generate and evaluate cross-sectional alpha candidates, promote robust signals, build low-redundancy composites, test implementability, and feed approved composite targets into the paper trading workflow.

## What The Platform Does

### Ingestion and Feature Generation
- Downloads and normalizes raw market data.
- Builds per-symbol feature datasets used by both simple signal strategies and `alpha_lab`.
- Stores reusable feature parquet files under `data/features`.

### Walk-Forward and Cross-Sectional Alpha Research
- Evaluates alpha candidates across symbols by date using cross-sectional ranking.
- Runs shared walk-forward folds across the universe.
- Computes rank IC, hit rate, long-short spread, turnover, and related out-of-sample metrics.

### Promotion and Redundancy Filtering
- Promotes or rejects candidates using configurable out-of-sample thresholds.
- Tracks rejection reasons directly in the leaderboard.
- Computes pairwise redundancy diagnostics across promoted candidates.

### Composite Signal Builder
- Selects promoted, low-redundancy signals.
- Normalizes component signals cross-sectionally by date.
- Builds composite scores with equal-weight and quality-weight variants.

### Portfolio Construction and Cost-Aware Backtesting
- Converts composite scores into daily cross-sectional portfolios.
- Supports long-only top-N and long-short quantile construction.
- Reuses the existing execution policy and portfolio simulation modules.
- Includes transaction-cost-aware backtesting with turnover-based costs.

### Liquidity, Capacity, and Implementability Analysis
- Applies minimum price, volume, and ADV filters.
- Estimates slippage from turnover and ADV usage.
- Produces implementability and capacity scenario reports alongside baseline portfolio outputs.

### Paper Trading Integration
- Supports the legacy strategy path and a composite-driven path.
- Builds approved composite targets from promoted alpha artifacts.
- Applies implementability filters before generating rebalance orders.
- Writes composite diagnostics and approved target weights for daily paper runs.

### Robustness and Stress Testing
- Breaks out performance by period, regime, and fold stability.
- Includes signal shuffle and extra-lag stress tests.
- Reports concentration, exposure, turnover distribution, and drawdown duration.

## Project Structure

```text
trading_platform/
├── src/trading_platform/
│   ├── broker/          # Broker abstractions and integrations
│   ├── cli/             # Command-line entrypoints
│   ├── construction/    # Portfolio weighting and constraints
│   ├── data/            # Data access and normalization helpers
│   ├── execution/       # Timing and rebalance policies
│   ├── features/        # Feature engineering
│   ├── jobs/            # Scheduled-style job wrappers
│   ├── paper/           # Paper trading state, order generation, composite integration
│   ├── portfolio/       # Portfolio utilities and backtest helpers
│   ├── reporting/       # Paper and account reporting
│   ├── research/
│   │   └── alpha_lab/   # Cross-sectional alpha research, promotion, composite, automation
│   ├── risk/            # Pre-trade risk checks
│   ├── services/        # Ingest, features, job artifacts, and workflow services
│   ├── signals/         # Legacy strategy signal loaders and registry
│   ├── simulation/      # Portfolio and single-asset simulation
│   ├── strategies/      # Strategy registry and implementations
│   └── universes/       # Named universes
├── data/                # Raw and processed datasets
├── artifacts/           # Research, portfolio, and paper-trading outputs
├── tests/               # Pytest suite
└── README.md
```

## CLI Examples

Install editable dependencies and use the packaged CLI:

```bash
pip install -e .[dev]
trading-cli --help
```

### Ingest

```bash
trading-cli ingest --symbols AAPL MSFT NVDA --start 2020-01-01
```

### Features

```bash
trading-cli features --symbols AAPL MSFT NVDA
```

### Alpha Research

This command runs the cross-sectional alpha evaluation flow and writes promoted signals, redundancy reports, composite scores, portfolio outputs, robustness reports, and implementability artifacts.

```bash
trading-cli alpha-research \
  --symbols AAPL MSFT NVDA \
  --feature-dir data/features \
  --signal-family momentum \
  --lookbacks 5 10 20 60 \
  --horizons 1 5 20 \
  --top-quantile 0.2 \
  --bottom-quantile 0.2 \
  --portfolio-top-n 10 \
  --commission 0.001 \
  --min-price 5 \
  --min-avg-dollar-volume 5000000 \
  --output-dir artifacts/alpha_research
```

### Composite Portfolio Backtest

There is no separate composite-only CLI command today. The composite portfolio backtest is run as part of `alpha-research`, and the main outputs are written to:

- `portfolio_returns.csv`
- `portfolio_metrics.csv`
- `portfolio_weights.csv`
- `implementability_report.csv`

### Paper Trading Workflow

Legacy strategy path:

```bash
trading-cli daily-paper-job \
  --symbols AAPL MSFT NVDA \
  --strategy sma_cross \
  --top-n 5 \
  --state-path artifacts/paper/state.json \
  --output-dir artifacts/paper \
  --timing next_bar
```

Composite-driven path:

```bash
trading-cli daily-paper-job \
  --symbols AAPL MSFT NVDA \
  --signal-source composite \
  --composite-artifact-dir artifacts/alpha_research \
  --composite-horizon 1 \
  --composite-weighting-scheme equal \
  --composite-portfolio-mode long_only_top_n \
  --top-n 5 \
  --min-price 5 \
  --min-avg-dollar-volume 5000000 \
  --state-path artifacts/paper/state.json \
  --output-dir artifacts/paper \
  --timing next_bar
```

## Automated Research Loop

The platform includes an automated alpha research loop that generates candidate configurations, skips already-tested candidates, evaluates new ones, updates the research registry, and refreshes promoted/redundancy artifacts.

```bash
trading-cli alpha-research-loop \
  --symbols AAPL MSFT NVDA \
  --feature-dir data/features \
  --signal-families momentum short_term_reversal vol_adjusted_momentum \
  --lookbacks 5 10 20 60 \
  --horizons 1 5 20 \
  --schedule-frequency daily \
  --output-dir artifacts/alpha_research_loop
```

## Typical Outputs

Depending on the command, the platform writes artifacts such as:

- feature parquet files under `data/features`
- alpha research leaderboards and fold results
- promoted signal and redundancy reports
- composite scores and composite diagnostics
- portfolio returns, metrics, weights, and diagnostics
- implementability and capacity reports
- paper orders, target weights, fills, and summaries
- research registry and research history tables

## Roadmap

- Add a dedicated composite portfolio CLI wrapper instead of exposing it only through `alpha-research`
- Expand signal families and search-space configuration for the automated alpha loop
- Persist composite-paper approvals and deployment state more explicitly
- Add richer monitoring and reporting around scheduled research and paper-trading jobs

## Development

Run the test suite with:

```bash
pytest
```
