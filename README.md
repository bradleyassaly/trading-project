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

The CLI now uses grouped command families:

- `data`
- `research`
- `portfolio`
- `paper`
- `live`
- `experiments`

Examples:

```bash
trading-cli data ingest --universe magnificent7 --start 2020-01-01
trading-cli data features --universe magnificent7 --feature-groups trend momentum volatility volume
trading-cli data universes list
trading-cli research run --symbols AAPL MSFT NVDA --strategy sma_cross --fast 20 --slow 100 --engine vectorized --output-dir artifacts/research
trading-cli research run --symbols AAPL --strategy momentum_hold --lookback 20 --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/aapl
trading-cli research run --symbols AAPL --strategy breakout_hold --entry-lookback 55 --exit-lookback 20 --momentum-lookback 63 --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/aapl_breakout
trading-cli research run --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars 126 --skip-bars 5 --top-n 3 --rebalance-bars 21 --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/mag7_xsec_momentum
trading-cli research validate-signal --symbols AAPL --strategy sma_cross --fast 20 --slow 100 --output-dir artifacts/validate_signal/aapl
trading-cli research validate-signal --universe debug_liquid10 --strategy sma_cross --fast 20 --slow 100 --output-dir artifacts/validate_signal/debug_liquid10
trading-cli research validate-signal --universe debug_liquid10 --strategy sma_cross --fast 20 --slow 100 --fast-values 10 20 30 --slow-values 50 100 150 --output-dir artifacts/validate_signal/debug_liquid10_sweep
trading-cli research sweep --symbols AAPL MSFT NVDA --strategy sma_cross --fast-values 10 20 30 --slow-values 50 100 150
trading-cli research sweep --symbols AAPL --strategy breakout_hold --entry-lookback-values 20 55 100 --exit-lookback-values 10 20 50 --momentum-lookback-values 63
trading-cli research sweep --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars-values 63 126 252 --skip-bars-values 0 5 --top-n-values 2 3 5 --rebalance-bars-values 5 21
trading-cli research walkforward --universe magnificent7 --strategy sma_cross --fast-values 10 20 --slow-values 50 100 --train-bars 756 --test-bars 126 --step-bars 126
trading-cli research walkforward --symbols AAPL --strategy breakout_hold --entry-lookback-values 20 55 100 --exit-lookback-values 10 20 50 --momentum-lookback-values 63 --train-bars 756 --test-bars 126 --step-bars 126
trading-cli research walkforward --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars-values 126 252 --skip-bars-values 0 5 --top-n-values 3 5 --rebalance-bars-values 5 21 --train-bars 756 --test-bars 126 --step-bars 126
trading-cli research alpha --universe magnificent7 --feature-dir data/features --signal-family momentum --lookbacks 5 10 20 60 --horizons 1 5 20 --output-dir artifacts/alpha_research
trading-cli research loop --universe nasdaq100 --feature-dir data/features --signal-families momentum mean_reversion volatility feature_combo --max-iterations 1
trading-cli research multi-universe --universes sp500 nasdaq100 liquid_top_100 --feature-dir data/features --signal-family momentum
trading-cli research refresh --universe sp500 --feature-dir data/features --stale-after-days 30
trading-cli research monitor --tracker-dir artifacts/experiment_tracking --snapshot-dir artifacts/research_refresh/approved_configuration_snapshots
trading-cli portfolio backtest --universe magnificent7 --strategy sma_cross --rebalance-frequency weekly --output-dir artifacts/portfolio
trading-cli portfolio topn --universe magnificent7 --strategy momentum_hold --lookback 20 --top-n 3 --weighting-scheme inverse_vol
trading-cli paper run --symbols AAPL MSFT NVDA --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --top-n 5 --state-path artifacts/paper/paper_state.json --output-dir artifacts/paper
trading-cli paper daily --universe magnificent7 --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --state-path artifacts/paper/paper_state.json --output-dir artifacts/paper
trading-cli paper report --account-dir artifacts/paper --output-dir artifacts/paper/report
trading-cli live dry-run --universe magnificent7 --strategy sma_cross --top-n 5 --broker mock
trading-cli live validate --universe magnificent7 --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --approval-artifact artifacts/research_refresh/approved_configuration_snapshots/latest_approved_configuration.json --output-dir artifacts/live_execution
trading-cli live execute --universe magnificent7 --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --approved --output-dir artifacts/live_execution
trading-cli experiments list --tracker-dir artifacts/experiment_tracking --limit 10
trading-cli experiments latest --tracker-dir artifacts/experiment_tracking
trading-cli experiments dashboard --tracker-dir artifacts/experiment_tracking --output-dir artifacts/experiment_tracking --top-metric portfolio_sharpe
trading-cli experiments diff --snapshot-dir artifacts/research_refresh/approved_configuration_snapshots
```

Config-driven reproducible mode is now folded into grouped commands where practical:

- `trading-cli research run --config ...`
- `trading-cli research sweep --config ...`
- `trading-cli research walkforward --config ...`

## Migration Notes

Legacy flat commands still work through compatibility rewrites and print a deprecation note on use. Common mappings:

- `ingest` -> `data ingest`
- `features` -> `data features`
- `list-universes` -> `data universes list`
- `export-universes` -> `data universes export`
- `research` -> `research run`
- `sweep` -> `research sweep`
- `walkforward` -> `research walkforward`
- `pipeline` -> `research pipeline`
- `alpha-research` -> `research alpha`
- `alpha-research-loop` -> `research loop`
- `multi-universe-alpha-research` -> `research multi-universe`
- `multi-universe-report` -> `research multi-universe-report`
- `paper-run` -> `paper run`
- `daily-paper-job` -> `paper daily`
- `paper-report` -> `paper report`
- `live-dry-run` -> `live dry-run`
- `validate-live` -> `live validate`
- `execute-live` -> `live execute`
- `experiments-list` -> `experiments list`
- `experiments-latest-model` -> `experiments latest`
- `experiments-dashboard` -> `experiments dashboard`
- `approved-config-diff` -> `experiments diff`

## Canonical Schema Contract

Research-facing data loading now uses a shared canonical schema normalization layer in `src/trading_platform/data/canonical.py`.

Expected internal columns:

- required: `timestamp`, `close`, `symbol`
- optional when available: `open`, `high`, `low`, `volume`, `dollar_volume`

Normalization rules:

- `timestamp`, `date`, `Date`, and `DatetimeIndex` are normalized to `timestamp`
- `close`, `Close`, `adj_close`, `Adj Close`, and `adjusted_close` are normalized to `close`
- common OHLCV aliases are normalized to lowercase canonical names
- `symbol` is injected when missing
- extra columns are preserved

This canonical loader is used by `alpha_lab`, research prep paths, and signal-loading paths so schema quirks are handled once instead of in multiple ad hoc readers.

## Typical Workflows

### Basic Research Workflow

1. Run `data ingest` for a universe or explicit symbols.
2. Run `data features` to build parquet feature datasets.
3. Run `research validate-signal`, `research run`, `research sweep`, `research walkforward`, `portfolio backtest`, or `portfolio topn`.
4. Inspect artifacts under the selected output directory.

Date-bounded legacy research example:

```bash
trading-cli research run --symbols AAPL --strategy sma_cross --fast 20 --slow 100 --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/aapl_2020_2024
```

`breakout_hold` supports an optional momentum confirmation filter. When `--momentum-lookback` is provided, the strategy only enters a breakout if trailing return over that lookback is positive. Exits still follow the breakout exit rule independently of the momentum filter.

`xsec_momentum_topn` is a relative-strength portfolio strategy for small universes. On each rebalance date it ranks symbols by trailing return over `--lookback-bars`, optionally skips the most recent `--skip-bars`, selects the top `--top-n`, and holds them equally weighted until the next `--rebalance-bars` interval.

### Signal Validation Commands

Use `research validate-signal` when you want a trust-oriented validation pass on a single ticker or a small universe. The command checks feature availability, runs vectorized in-sample research, writes a parameter sweep when applicable, runs walk-forward validation, then writes per-symbol summaries, a combined leaderboard, and a JSON pass/fail report.

Single ticker validation:

```bash
trading-cli research validate-signal --symbols AAPL --strategy sma_cross --fast 20 --slow 100 --output-dir artifacts/validate_signal/aapl
```

Universe validation with the built-in debug universe:

```bash
trading-cli research validate-signal --universe debug_liquid10 --strategy sma_cross --fast 20 --slow 100 --output-dir artifacts/validate_signal/debug_liquid10
```

Validation with an explicit sweep grid:

```bash
trading-cli research validate-signal --universe debug_liquid10 --strategy sma_cross --fast 20 --slow 100 --fast-values 10 20 30 --slow-values 50 100 150 --output-dir artifacts/validate_signal/debug_liquid10_sweep
```

Standalone walk-forward command:

```bash
trading-cli research walkforward --universe debug_liquid10 --strategy sma_cross --fast-values 10 20 30 --slow-values 50 100 150 --train-bars 756 --test-bars 126 --step-bars 126 --engine vectorized --output artifacts/experiments/debug_liquid10_walkforward.csv
```

For daily legacy research, `--train-bars`, `--test-bars`, and `--step-bars` refer to trading bars/rows, not calendar days. The older `--train-period-days`, `--test-period-days`, and `--step-days` flags remain as compatibility aliases and now map to those same row counts.

One walk-forward window is only a basic sanity check. Prefer multi-window validation so the command can evaluate several rolling out-of-sample periods and report completed versus skipped windows across the effective date range.

The walk-forward CSV and summary outputs now include activity diagnostics such as `trade_count`, `entry_count`, `exit_count`, `percent_time_in_market`, and `average_holding_period_bars`. Use these fields to interpret flat `0.0%` test windows:

- `trade_count=0` and `percent_time_in_market=0` usually means the strategy was inactive in that window.
- low `percent_time_in_market` means the result came from brief exposure, so underperformance versus buy-and-hold may reflect low participation rather than only poor trade quality.
- nonzero trades with weak returns point more directly to poor timing or weak signal quality out of sample.

### Alpha Discovery Workflow

1. Build features with `data features`.
2. Run `research alpha` for focused cross-sectional evaluation.
3. Run `research loop` for incremental candidate discovery and registry updates.
4. Review `leaderboard.csv`, `promoted_signals.csv`, `near_miss_signals.csv`, `signal_family_summary.csv`, and `feature_availability_report.csv`.
5. Register and compare runs with `experiments list` and `experiments dashboard`.

### Deployment Workflow

1. Produce research outputs with `research alpha` or `research refresh`.
2. Use the deployment-facing artifact `approved/approved_model_state.json` for paper and live workflows.
3. Run `paper run` or `paper daily` with `--approved-model-state`.
4. Run `live validate` before any `live execute` invocation.

## Artifacts And Outputs

Research outputs remain in the main artifact directory, while deployment-facing artifacts are separated under `approved/` where supported.

Common output locations:

- `data/features/*.parquet`: feature datasets
- `artifacts/alpha_research/`: exploratory research outputs such as leaderboards, folds, diagnostics, portfolio results, robustness, and implementability reports
- `artifacts/alpha_research/approved/approved_model_state.json`: deployment-facing approved model-state package
- `artifacts/alpha_research_loop/`: signal registry, history, promoted/rejected signals, near-miss diagnostics, and schedule metadata
- `artifacts/alpha_research_loop/approved/approved_model_state.json`: approved deployment package built from loop outputs
- `artifacts/experiment_tracking/`: experiment registry, dashboard summary, and latest model state
- `artifacts/research_refresh/approved_configuration_snapshots/`: scheduled approval snapshots and diffs
- `artifacts/paper/`: paper state, orders, target weights, fills, summaries, and composite diagnostics
- `artifacts/live_execution/`: validation or execution-control reports

Useful files to inspect first:

- `leaderboard.csv`
- `promoted_signals.csv`
- `near_miss_signals.csv`
- `portfolio_metrics.csv`
- `robustness_report.csv`
- `implementability_report.csv`
- `approved/approved_model_state.json`
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
