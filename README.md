# Trading Platform

## Project Overview

This repository is a Python trading and research platform for:

- market data ingestion and feature generation
- single-strategy backtesting and walk-forward analysis
- cross-sectional alpha research across symbol universes
- signal promotion, redundancy filtering, and composite construction
- portfolio construction, cost-aware backtesting, and implementability analysis
- experiment tracking, scheduled research refresh, and monitoring
- paper trading and guarded live execution validation

The codebase supports both a legacy strategy workflow and a newer `alpha_lab` workflow. The legacy path covers research, sweeps, walk-forward validation, portfolio construction, paper trading, and live dry runs for classic strategies such as SMA crossovers and momentum. The `alpha_lab` path covers cross-sectional signal discovery, promotion, composite signals, portfolio diagnostics, automated research loops, and deployment-oriented controls.

## Current Architecture

### Ingestion

The ingestion layer downloads and normalizes OHLCV data for named universes or explicit symbols. Raw datasets feed both legacy strategy research and the alpha research pipeline.

### Feature Generation

Feature generation writes per-symbol parquet datasets under `data/features`. These feature files are shared by `alpha_lab`, paper trading, and multi-universe workflows. The alpha loader normalizes common schema variations such as `timestamp` / `date` / `Date`, `DatetimeIndex`, and multiple close-column names.

### Alpha Research

`alpha_lab` evaluates signals cross-sectionally by date with walk-forward folds. It computes out-of-sample rank IC, long-short spread, turnover proxies, fold-level diagnostics, and panel-based performance summaries.

### Automated Alpha Loop

The automated alpha loop generates candidate signals from configurable families and parameter sweeps, skips already-tested configurations, evaluates only new or stale candidates, and updates the research registry incrementally.

### Promotion And Redundancy Filtering

Promotion rules use out-of-sample metrics such as mean rank IC, fold coverage, dates evaluated, turnover, and worst-fold behavior. Rejected candidates include reason codes. Promoted candidates are further filtered with redundancy diagnostics based on shared score or performance correlations.

### Composite Signal Construction

Promoted low-redundancy signals can be combined into a composite score by symbol and date. The platform supports static weights, dynamic lifecycle weights, and regime-aware weights. Composite diagnostics track component selection, normalization, and weight concentration.

### Portfolio Construction And Backtesting

Composite scores can drive long-only top-N or long-short quantile portfolios. Backtests reuse the platform’s existing execution timing and reconciliation logic, include transaction costs, and write portfolio returns, weights, metrics, and diagnostics.

### Robustness, Implementability, And Multi-Universe Analysis

The research pipeline can break performance out by period, regime, and fold; run shuffle and lag stress tests; estimate liquidity and capacity constraints; and compare results across multiple named universes in one job.

### Experiment Tracking

Research and paper-trading runs can be registered into a shared experiment registry. The tracker stores configuration fingerprints, promotion state, composite and regime settings, portfolio metrics, robustness metrics, implementability metrics, and paper-trading summaries.

### Paper Trading

Paper trading supports both legacy strategy targets and approved composite alpha targets. The workflow builds target weights, generates rebalance orders, optionally applies simulated fills, and writes state, ledger, and diagnostics artifacts.

### Live Validation And Execution Controls

The live control layer sits between target generation and order submission. It adds pre-trade risk limits, approval gating, kill switches, stale-data and stale-config checks, blocked symbols, drift-alert blocking, and broker/account sanity checks. The default behavior is conservative: validate first, execute only when approved and safe.

## Repository Structure

The main code lives under `src/trading_platform`:

- `backtests/`: legacy backtest utilities
- `broker/`: broker abstractions, mock broker support, Alpaca integration
- `cli/`: parser, command entrypoints, and shared CLI helpers
- `config/`: configuration helpers
- `construction/`: portfolio construction, constraints, and selection logic
- `data/`: data access and normalization helpers
- `execution/`: rebalance timing, reconciliation, and open-order adjustment
- `experiments/`: experiment-oriented helpers and artifacts
- `features/`: feature builders and registry
- `jobs/`: job wrappers for repeatable workflows
- `live/`: live execution control layer and safeguards
- `metadata/`: metadata helpers
- `paper/`: paper trading models, services, composite integration, and state handling
- `portfolio/`: portfolio analytics and utilities
- `reporting/`: reporting builders such as paper account reports
- `research/`: research orchestration
- `research/alpha_lab/`: cross-sectional alpha evaluation, promotion, redundancy, composite building, lifecycle, regimes, automation, and data loading
- `risk/`: risk checks
- `schemas/`: schema helpers
- `services/`: service-layer workflows
- `signals/`: legacy signal loaders and signal utilities
- `simulation/`: simulation engines
- `strategies/`: strategy registry and implementations
- `universes/`: static universe definitions and registry access

Important repository locations:

- `data/raw`: raw downloaded market data
- `data/features`: per-symbol feature parquet files
- `artifacts/alpha_research`: default alpha research outputs
- `artifacts/alpha_research_loop`: default automated discovery outputs
- `artifacts/experiment_tracking`: shared experiment registry and reports
- `artifacts/paper`: paper trading state and ledgers
- `artifacts/live_execution`: live validation and execution-control artifacts
- `tests/`: pytest suite

## Installation And Setup

### Environment Setup

Use Python 3.10+.

```bash
python -m venv .venv
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Dependency Install

Install the project in editable mode with development dependencies:

```bash
pip install -e .[dev]
```

### Run The CLI

The package exposes a single CLI entrypoint:

```bash
trading-cli --help
```

You can also invoke the module directly:

```bash
python -m trading_platform.cli --help
```

## CLI Command Reference

All commands below come from `src/trading_platform/cli/parser.py`.

### Core Data And Research Commands

#### `ingest`

Purpose: download raw OHLCV data for explicit symbols or a named universe.

Important arguments: `--symbols` or `--universe`, `--start`

Example:

```bash
trading-cli ingest --universe magnificent7 --start 2020-01-01
```

#### `features`

Purpose: build feature datasets used by legacy research and `alpha_lab`.

Important arguments: `--symbols` or `--universe`, `--feature-groups`

Example:

```bash
trading-cli features --universe magnificent7 --feature-groups trend momentum volatility volume
```

#### `research`

Purpose: run legacy strategy backtests.

Important arguments: `--symbols` or `--universe`, `--strategy`, `--fast`, `--slow`, `--lookback`, `--rebalance-frequency`, `--engine`, `--output-dir`

Example:

```bash
trading-cli research --symbols AAPL MSFT NVDA --strategy sma_cross --fast 20 --slow 100 --engine vectorized --output-dir artifacts/research
```

#### `pipeline`

Purpose: run ingest, feature generation, and legacy research in one command.

Important arguments: `--symbols` or `--universe`, `--start`, `--feature-groups`, `--strategy`

Example:

```bash
trading-cli pipeline --universe magnificent7 --start 2018-01-01 --feature-groups trend momentum volatility volume --strategy sma_cross
```

### Discovery And Registry Commands

#### `list-universes`

Purpose: print the named universe registry.

Important arguments: none

Example:

```bash
trading-cli list-universes
```

#### `export-universes`

Purpose: export the static universe definitions to JSON for inspection or reuse.

Important arguments: `--output`

Example:

```bash
trading-cli export-universes --output artifacts/universes/universes.json
```

#### `list-strategies`

Purpose: print the registered legacy strategies.

Important arguments: none

Example:

```bash
trading-cli list-strategies
```

### Legacy Parameter Search Commands

#### `sweep`

Purpose: run a parameter sweep for a legacy strategy.

Important arguments: `--symbols` or `--universe`, `--strategy`, `--fast-values`, `--slow-values`, `--lookback-values`, `--engine`, `--output`

Example:

```bash
trading-cli sweep --symbols AAPL MSFT NVDA --strategy sma_cross --fast-values 10 20 30 --slow-values 50 100 150 --engine vectorized --output artifacts/experiments/sma_sweep.csv
```

#### `walkforward`

Purpose: run legacy walk-forward validation.

Important arguments: `--symbols` or `--universe`, `--strategy`, `--train-years`, `--test-years`, `--select-by`, `--engine`, `--output`

Example:

```bash
trading-cli walkforward --universe magnificent7 --strategy sma_cross --fast-values 10 20 --slow-values 50 100 --train-years 5 --test-years 1 --engine vectorized --output artifacts/experiments/walkforward.csv
```

### Portfolio Commands

#### `portfolio`

Purpose: run an equal-weight multi-symbol portfolio backtest using a legacy strategy.

Important arguments: `--symbols` or `--universe`, `--strategy`, `--rebalance-frequency`, `--output-dir`

Example:

```bash
trading-cli portfolio --universe magnificent7 --strategy sma_cross --rebalance-frequency weekly --output-dir artifacts/portfolio
```

#### `portfolio-topn`

Purpose: run a cross-sectional top-N legacy portfolio backtest.

Important arguments: `--symbols` or `--universe`, `--strategy`, `--top-n`, `--weighting-scheme`, `--vol-window`, `--group-map-path`, `--output-dir`

Example:

```bash
trading-cli portfolio-topn --universe magnificent7 --strategy momentum_hold --lookback 20 --top-n 3 --weighting-scheme inverse_vol --vol-window 20 --output-dir artifacts/portfolio_topn
```

### Config-Driven Batch Commands

#### `run-job`

Purpose: run a research workflow from a YAML or JSON config file.

Important arguments: `--config`, `--symbols`, `--fail-fast`

Example:

```bash
trading-cli run-job --config configs/research_job.yaml --fail-fast
```

#### `run-sweep`

Purpose: run a sweep from a YAML or JSON config file.

Important arguments: `--config`, `--fail-fast`

Example:

```bash
trading-cli run-sweep --config configs/sweep_job.yaml
```

#### `run-walk-forward`

Purpose: run walk-forward evaluation from a YAML or JSON config file.

Important arguments: `--config`

Example:

```bash
trading-cli run-walk-forward --config configs/walkforward_job.yaml
```

### Paper Trading And Broker Preview Commands

#### `paper-run`

Purpose: run one paper-trading cycle, update paper state, and write artifacts.

Important arguments: `--symbols` or `--universe`, `--signal-source`, `--strategy`, `--top-n`, `--timing`, `--state-path`, `--output-dir`, `--auto-apply-fills`, `--composite-artifact-dir`

Example:

```bash
trading-cli paper-run --symbols AAPL MSFT NVDA --signal-source composite --composite-artifact-dir artifacts/alpha_research --top-n 5 --timing next_bar --state-path artifacts/paper/paper_state.json --output-dir artifacts/paper --auto-apply-fills
```

#### `daily-paper-job`

Purpose: run the daily paper workflow for a legacy or composite-driven strategy.

Important arguments: `--symbols` or `--universe`, `--signal-source`, `--strategy`, `--top-n`, `--timing`, `--state-path`, `--output-dir`

Example:

```bash
trading-cli daily-paper-job --universe magnificent7 --signal-source composite --composite-artifact-dir artifacts/alpha_research --top-n 5 --state-path artifacts/paper/paper_state.json --output-dir artifacts/paper --timing next_bar
```

#### `paper-report`

Purpose: summarize a paper-trading account directory and optionally write report artifacts.

Important arguments: `--account-dir`, `--output-dir`

Example:

```bash
trading-cli paper-report --account-dir artifacts/paper --output-dir artifacts/paper/report
```

#### `live-dry-run`

Purpose: compute live broker rebalance orders without sending them.

Important arguments: `--symbols` or `--universe`, `--strategy`, `--top-n`, `--broker`, `--mock-equity`, `--mock-cash`, `--mock-positions-path`, `--order-type`, `--time-in-force`

Example:

```bash
trading-cli live-dry-run --universe magnificent7 --strategy sma_cross --top-n 5 --broker mock --mock-equity 100000 --mock-cash 100000 --order-type market --time-in-force day
```

### Alpha Research Commands

#### `alpha-research`

Purpose: run the full cross-sectional alpha workflow for one universe or symbol set, including evaluation, promotion, redundancy filtering, composite construction, portfolio backtest, robustness, implementability, and experiment registration.

Important arguments: `--symbols` or `--universe`, `--feature-dir`, `--signal-family`, `--lookbacks`, `--horizons`, `--portfolio-top-n`, `--commission`, `--min-price`, `--min-avg-dollar-volume`, `--regime-aware-enabled`, `--output-dir`, `--experiment-tracker-dir`

Example:

```bash
trading-cli alpha-research --universe magnificent7 --feature-dir data/features --signal-family momentum --lookbacks 5 10 20 60 --horizons 1 5 20 --portfolio-top-n 5 --commission 0.001 --min-price 5 --min-avg-dollar-volume 5000000 --regime-aware-enabled --output-dir artifacts/alpha_research --experiment-tracker-dir artifacts/experiment_tracking
```

#### `alpha-research-loop`

Purpose: run the automated alpha discovery loop with candidate generation, deduplication, promotion, and registry updates.

Important arguments: `--symbols` or `--universe`, `--feature-dir`, `--signal-families`, `--lookbacks`, `--horizons`, `--vol-windows`, `--combo-thresholds`, `--schedule-frequency`, `--force`, `--max-iterations`, `--output-dir`

Example:

```bash
trading-cli alpha-research-loop --universe nasdaq100 --feature-dir data/features --signal-families momentum mean_reversion volatility feature_combo --lookbacks 5 10 20 60 --vol-windows 10 20 60 --combo-thresholds 0.5 1.0 --horizons 1 5 20 --schedule-frequency daily --max-iterations 1 --output-dir artifacts/alpha_research_loop
```

### Experiment Tracking Commands

#### `experiments-list`

Purpose: list recent research and paper-trading experiments from the shared registry.

Important arguments: `--tracker-dir`, `--limit`

Example:

```bash
trading-cli experiments-list --tracker-dir artifacts/experiment_tracking --limit 10
```

#### `experiments-latest-model`

Purpose: show the latest approved research or composite configuration snapshot from the experiment tracker.

Important arguments: `--tracker-dir`

Example:

```bash
trading-cli experiments-latest-model --tracker-dir artifacts/experiment_tracking
```

#### `experiments-dashboard`

Purpose: build a summary dashboard artifact from the experiment registry.

Important arguments: `--tracker-dir`, `--output-dir`, `--top-metric`, `--limit`

Example:

```bash
trading-cli experiments-dashboard --tracker-dir artifacts/experiment_tracking --output-dir artifacts/experiment_tracking --top-metric portfolio_sharpe --limit 10
```

### Scheduled Research Operations Commands

#### `research-refresh`

Purpose: run the scheduled research refresh, evaluate new or stale candidates, and persist a new approved configuration snapshot.

Important arguments: `--symbols` or `--universe`, `--feature-dir`, `--signal-families`, `--schedule-frequency`, `--stale-after-days`, `--tracker-dir`, `--force`, `--output-dir`

Example:

```bash
trading-cli research-refresh --universe sp500 --feature-dir data/features --signal-families momentum mean_reversion volatility feature_combo --lookbacks 5 10 20 60 --horizons 1 5 20 --schedule-frequency daily --stale-after-days 30 --tracker-dir artifacts/experiment_tracking --output-dir artifacts/research_refresh
```

#### `research-monitor`

Purpose: build a monitoring report and drift alerts from recent alpha and paper-trading artifacts.

Important arguments: `--tracker-dir`, `--snapshot-dir`, `--output-dir`, `--recent-paper-runs`, `--performance-degradation-buffer`, `--turnover-spike-multiple`, `--concentration-spike-multiple`, `--signal-churn-threshold`

Example:

```bash
trading-cli research-monitor --tracker-dir artifacts/experiment_tracking --snapshot-dir artifacts/research_refresh/approved_configuration_snapshots --output-dir artifacts/research_monitoring --recent-paper-runs 10
```

#### `approved-config-diff`

Purpose: compare the latest approved configuration snapshot with the previous one.

Important arguments: `--snapshot-dir`

Example:

```bash
trading-cli approved-config-diff --snapshot-dir artifacts/research_refresh/approved_configuration_snapshots
```

### Multi-Universe Commands

#### `multi-universe-alpha-research`

Purpose: run the alpha research workflow across multiple named universes and build comparison artifacts.

Important arguments: `--universes`, `--feature-dir`, `--signal-family`, `--lookbacks`, `--horizons`, `--portfolio-top-n`, `--regime-aware-enabled`, `--output-dir`, `--experiment-tracker-dir`

Example:

```bash
trading-cli multi-universe-alpha-research --universes sp500 nasdaq100 liquid_top_100 --feature-dir data/features --signal-family momentum --lookbacks 5 10 20 60 --horizons 1 5 20 --portfolio-top-n 10 --regime-aware-enabled --output-dir artifacts/multi_universe_alpha_research --experiment-tracker-dir artifacts/experiment_tracking
```

#### `multi-universe-report`

Purpose: rebuild a comparison report from existing per-universe research outputs.

Important arguments: `--output-dir`

Example:

```bash
trading-cli multi-universe-report --output-dir artifacts/multi_universe_alpha_research
```

### Live Control Commands

#### `validate-live`

Purpose: run live execution control checks and write artifacts without sending orders.

Important arguments: `--symbols` or `--universe`, `--signal-source`, `--composite-artifact-dir`, `--approval-artifact`, `--max-gross-exposure`, `--max-order-notional`, `--max-daily-turnover`, `--min-cash-reserve`, `--drift-alerts-path`, `--output-dir`

Example:

```bash
trading-cli validate-live --universe magnificent7 --signal-source composite --composite-artifact-dir artifacts/alpha_research --top-n 5 --broker mock --approval-artifact artifacts/research_refresh/approved_configuration_snapshots/latest_approved_configuration.json --drift-alerts-path artifacts/research_monitoring/drift_alerts.csv --max-gross-exposure 1.0 --max-order-notional 25000 --max-daily-turnover 0.5 --min-cash-reserve 0.05 --output-dir artifacts/live_execution
```

#### `execute-live`

Purpose: run the same live control checks and only submit orders when approval and safety conditions are satisfied.

Important arguments: `--symbols` or `--universe`, `--signal-source`, `--composite-artifact-dir`, `--approved`, `--approval-artifact`, `--kill-switch`, `--blocked-symbols`, `--output-dir`

Example:

```bash
trading-cli execute-live --universe magnificent7 --signal-source composite --composite-artifact-dir artifacts/alpha_research --top-n 5 --broker mock --approved --approval-artifact artifacts/research_refresh/approved_configuration_snapshots/latest_approved_configuration.json --output-dir artifacts/live_execution
```

## Typical Workflows

### Basic Research Workflow

1. Run `ingest` for a universe or explicit symbols.
2. Run `features` to build parquet feature datasets.
3. Run `research`, `sweep`, `walkforward`, `portfolio`, or `portfolio-topn` for legacy strategies.
4. Inspect outputs under `artifacts/` or a command-specific output directory.

### Alpha Discovery Workflow

1. Build feature parquet files with `features`.
2. Run `alpha-research` for focused cross-sectional evaluation.
3. Run `alpha-research-loop` to discover new candidates incrementally.
4. Review `leaderboard.csv`, `promoted_signals.csv`, `rejected_signals.csv`, `near_miss_signals.csv`, `signal_family_summary.csv`, and `feature_availability_report.csv`.
5. Register and compare runs with `experiments-list` and `experiments-dashboard`.

### Multi-Universe Workflow

1. Ensure named universes exist in the registry with `list-universes`.
2. Run `multi-universe-alpha-research`.
3. Review per-universe subdirectories and the cross-universe comparison files in the root output directory.
4. Rebuild the comparison-only summary later with `multi-universe-report`.

### Paper Trading Workflow

1. Produce approved alpha artifacts with `alpha-research` or an approved configuration snapshot with `research-refresh`.
2. Run `paper-run` or `daily-paper-job` with `--signal-source composite` and `--composite-artifact-dir`.
3. Inspect `paper_orders.csv`, `approved_target_weights.csv`, `paper_summary.json`, and paper ledgers.
4. Use `paper-report` to summarize account behavior over time.

### Live Validation Workflow

1. Run `research-monitor` and review `drift_alerts.csv`.
2. Run `validate-live` to generate a pre-trade decision without sending orders.
3. Review `pretrade_risk_report.json`, `blocked_orders_report.csv`, `live_execution_decision.json`, and `approval_status_snapshot.json`.
4. Only run `execute-live` when the approval artifact is current and all critical checks pass.

## Artifacts And Outputs

Common output locations:

- `data/features/*.parquet`: feature datasets
- `artifacts/alpha_research/`: leaderboard, folds, promoted and rejected signals, redundancy reports, composite scores, dynamic weights, regime outputs, portfolio outputs, robustness, implementability, and diagnostics JSON
- `artifacts/alpha_research_loop/`: signal registry, research history, promoted and rejected signals, near-miss diagnostics, feature availability diagnostics, and schedule metadata
- `artifacts/experiment_tracking/`: experiment registry, dashboard summary, and latest model state
- `artifacts/research_refresh/`: refresh history and approved configuration snapshots
- `artifacts/research_monitoring/`: monitoring report and drift alerts
- `artifacts/multi_universe_alpha_research/`: per-universe result folders and cross-universe comparison artifacts
- `artifacts/paper/`: paper state, orders, target weights, fills, summaries, and optional composite diagnostics
- `artifacts/live_execution/`: validation or execution-control reports

Useful files to inspect first:

- `leaderboard.csv`
- `promoted_signals.csv`
- `rejected_signals.csv`
- `near_miss_signals.csv`
- `portfolio_metrics.csv`
- `robustness_report.csv`
- `implementability_report.csv`
- `experiment_registry.csv`
- `monitoring_report.json`
- `drift_alerts.csv`
- `live_execution_decision.json`

## Known Limitations / Current Status

- Universe definitions are static snapshots in code, not live constituent feeds.
- Some advanced signal families depend on feature availability such as benchmark returns, sector/group context, or volume fields; missing inputs can degrade or skip candidates.
- The first live execution layer is intentionally conservative and file-based. It is designed around validation, gating, and safe defaults rather than full broker automation.
- The current broker path includes mock support and guarded integration points, but not a broad multi-broker production execution framework.
- The alpha research CLI currently exposes a focused set of primary signal families directly; the automated loop is broader.
- Artifact schemas are still evolving as the research stack expands.

## Next Steps / Roadmap

- add richer and more maintainable data sources for prices, benchmarks, sector mappings, and corporate actions
- expand higher-quality feature families and cross-sectional normalization options
- improve post-trade analytics and paper-vs-backtest attribution
- add stronger risk-model and group-exposure controls
- improve live broker safeguards, approval workflows, and execution observability
- tighten configuration management around approved composite deployments

## Development Notes

### Testing

Run the full suite with:

```bash
pytest
```

Run a targeted file or subset when iterating:

```bash
pytest tests/test_alpha_lab.py tests/test_alpha_research_loop.py
```

### Adding New Research Modules

- Prefer extending `src/trading_platform/research/alpha_lab/` for cross-sectional research functionality.
- Reuse existing artifact-writing patterns and diagnostics tables instead of inventing new one-off formats.
- If a new workflow needs CLI support, add it in `src/trading_platform/cli/parser.py` and implement the command under `src/trading_platform/cli/commands/`.
- Keep file-based outputs simple: CSV, Parquet, and JSON are the project’s current standard.
- Add pytest coverage for new workflows, artifact generation, and empty-data edge cases.
