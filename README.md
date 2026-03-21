# Trading Platform

## Project Overview

This repository is an end-to-end research to deploy trading system. It covers:

- market data ingestion and feature generation
- alpha research and walk-forward validation
- preset-driven research vs deploy workflows
- constrained portfolio construction for deployable implementations
- stateful paper trading with scheduled daily runs
- broker-safe live dry-run previews with reconciliation and health checks

The codebase supports both a legacy strategy workflow and a newer `alpha_lab` workflow. The current validated operational path centers on the versioned Nasdaq-100 cross-sectional momentum presets:

- `xsec_nasdaq100_momentum_v1_research`
- `xsec_nasdaq100_momentum_v1_deploy`

## Architecture Overview

The system is organized as a simple pipeline:

1. Data: ingest and normalize raw OHLCV history for named universes or explicit symbols.
2. Features: build reusable per-symbol feature files under `data/features`.
3. Signals: compute research signals, cross-sectional rankings, and diagnostics.
4. Portfolio construction:
   - research layer: `pure_topn`
   - deploy layer: `transition`
5. Paper trading: stateful local portfolio simulation with persistent ledgers and scheduled wrappers.
6. Live dry-run: broker-safe preview that reconciles target vs current holdings and writes proposed-order artifacts without sending orders.

### Research Layer

The research baseline uses `pure_topn`. This is the signal-truth mode: it keeps realized holdings tied to the current feasible selected top-N set and is the right path for walk-forward validation, benchmark comparison, and parameter selection.

### Deploy Layer

The deploy overlay uses `transition`. This keeps the validated signal family but adds implementation controls such as liquidity filters, inverse-vol weighting, and turnover caps. It is intentionally treated as a constrained implementation layer, not the underlying research truth.

### Paper Trading

Paper trading is stateful. Repeated runs update a local simulated portfolio state, append to durable ledgers, and write latest-summary artifacts for daily operational review.

### Live Dry-Run

Live dry-run is broker-safe. It loads broker/account state, builds deploy targets, reconciles target vs current holdings, generates proposed orders, writes audit artifacts, and never submits orders.

## Presets

Two versioned presets are the main operational entrypoints:

- `xsec_nasdaq100_momentum_v1_research`
  - strategy: `xsec_momentum_topn`
  - mode: `pure_topn`
  - intent: research baseline and signal validation
- `xsec_nasdaq100_momentum_v1_deploy`
  - strategy: `xsec_momentum_topn`
  - mode: `transition`
  - intent: constrained deployable implementation overlay

Interpretation:

- research preset = true signal
- deploy preset = constrained implementation

Presets populate validated defaults but still allow explicit CLI overrides.

## Key Commands

### Research

```bash
trading-cli research run --preset xsec_nasdaq100_momentum_v1_research --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/nasdaq100_xsec_v1_research
trading-cli research walkforward --preset xsec_nasdaq100_momentum_v1_research --train-bars 756 --test-bars 126 --step-bars 126 --output artifacts/experiments/nasdaq100_xsec_v1_research_walkforward.csv
trading-cli research compare-xsec-construction --preset xsec_nasdaq100_momentum_v1_research --train-bars 756 --test-bars 126 --step-bars 126 --max-position-weight 0.5 --min-avg-dollar-volume 50000000 --weighting-scheme inv_vol --vol-lookback-bars 20 --max-turnover-per-rebalance 0.5 --turnover-buffer-bps 0
trading-cli research decision-memo --preset xsec_nasdaq100_momentum_v1_research --deploy-preset xsec_nasdaq100_momentum_v1_deploy
```

### Paper Trading

```bash
trading-cli paper run --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
trading-cli paper run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
```

### Live Preview

```bash
trading-cli live dry-run --preset xsec_nasdaq100_momentum_v1_deploy --broker mock --output-dir artifacts/live_dry_run/nasdaq100_xsec
trading-cli live run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --broker mock --output-dir artifacts/live_dry_run/nasdaq100_xsec
```

## Paper Trading Outputs

Daily paper trading writes durable local artifacts such as:

- `paper_equity_curve.csv`
- `paper_positions_history.csv`
- `paper_orders_history.csv`
- `paper_run_summary.csv`
- `paper_run_summary_latest.json`
- `paper_run_summary_latest.md`
- `paper_health_checks.csv`

Use them to inspect equity continuity, realized holdings, generated orders, and pass/warn/fail operational checks across repeated runs.

## Live Dry-Run Outputs

Live preview writes an auditable proposed-orders package:

- `live_dry_run_proposed_orders.csv`
- `live_dry_run_reconciliation.csv`
- `live_dry_run_summary.json`
- `live_dry_run_summary.md`
- `live_dry_run_health_checks.csv`

The scheduled wrapper adds durable operational artifacts:

- `live_run_summary.csv`
- `live_run_summary_latest.json`
- `live_run_summary_latest.md`
- `live_health_checks.csv`
- `live_proposed_orders_history.csv`
- `live_reconciliation_history.csv`
- `live_notification_payload.json`

## Operational Workflow

The intended daily process is:

1. Run scheduled paper trading with the deploy preset.
2. Run live dry-run against the broker or mock account.
3. Inspect:
   - latest summary
   - proposed orders
   - health checks
4. Future step: guarded live execution once the execution path is promoted.

## Research Vs Deploy Insight

- `pure_topn` = research truth
- `transition` = deployable portfolio overlay
- turnover cap = smoothing layer between target portfolio and realized portfolio
- realized holdings may exceed `top_n` in `transition` mode

This distinction is deliberate. Research determines whether the signal family is valid; deploy mode measures whether the same family remains usable under implementation constraints.

## Future Work

- guarded live order execution
- intraday support
- multi-strategy portfolio construction
- explicit risk overlays

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
trading-cli data ingest --universe nasdaq100_current --start 2020-01-01 --failure-report artifacts/ingest/nasdaq100_current_failures.csv
trading-cli data ingest --universe nasdaq100_current --start 2020-01-01
trading-cli data features --universe magnificent7 --feature-groups trend momentum volatility volume
trading-cli data features --universe nasdaq100_current --feature-groups trend momentum volatility volume --failure-report artifacts/features/nasdaq100_current_failures.csv
trading-cli features build --universe nasdaq100_current --feature-groups trend momentum volatility volume
trading-cli data universes list
trading-cli research run --symbols AAPL MSFT NVDA --strategy sma_cross --fast 20 --slow 100 --engine vectorized --output-dir artifacts/research
trading-cli research run --symbols AAPL --strategy momentum_hold --lookback 20 --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/aapl
trading-cli research run --symbols AAPL --strategy breakout_hold --entry-lookback 55 --exit-lookback 20 --momentum-lookback 63 --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/aapl_breakout
trading-cli research run --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars 126 --skip-bars 0 --top-n 2 --rebalance-bars 21 --benchmark equal_weight --start 2020-01-01 --end 2024-12-31 --cost-bps 10 --output-dir artifacts/research/mag7_xsec_momentum
trading-cli research run --universe nasdaq100_current --strategy xsec_momentum_topn --lookback-bars 126 --skip-bars 0 --top-n 2 --rebalance-bars 21 --benchmark equal_weight --start 2020-01-01 --end 2024-12-31 --cost-bps 10 --output-dir artifacts/research/nasdaq100_current_xsec_momentum
trading-cli research validate-signal --symbols AAPL --strategy sma_cross --fast 20 --slow 100 --output-dir artifacts/validate_signal/aapl
trading-cli research validate-signal --universe debug_liquid10 --strategy sma_cross --fast 20 --slow 100 --output-dir artifacts/validate_signal/debug_liquid10
trading-cli research validate-signal --universe debug_liquid10 --strategy sma_cross --fast 20 --slow 100 --fast-values 10 20 30 --slow-values 50 100 150 --output-dir artifacts/validate_signal/debug_liquid10_sweep
trading-cli research sweep --symbols AAPL MSFT NVDA --strategy sma_cross --fast-values 10 20 30 --slow-values 50 100 150
trading-cli research sweep --symbols AAPL --strategy breakout_hold --entry-lookback-values 20 55 100 --exit-lookback-values 10 20 50 --momentum-lookback-values 63
trading-cli research sweep --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars-values 84 126 168 --skip-bars-values 0 5 10 --top-n-values 2 3 --rebalance-bars-values 10 21 42 --benchmark equal_weight
trading-cli research sweep --universe liquid_top_100 --strategy xsec_momentum_topn --lookback-bars-values 126 168 252 --skip-bars-values 0 5 10 21 --top-n-values 2 3 5 --rebalance-bars-values 21 42 --benchmark equal_weight
trading-cli research walkforward --universe magnificent7 --strategy sma_cross --fast-values 10 20 --slow-values 50 100 --train-bars 756 --test-bars 126 --step-bars 126
trading-cli research walkforward --symbols AAPL --strategy breakout_hold --entry-lookback-values 20 55 100 --exit-lookback-values 10 20 50 --momentum-lookback-values 63 --train-bars 756 --test-bars 126 --step-bars 126
trading-cli research walkforward --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars-values 84 126 168 --skip-bars-values 0 5 10 --top-n-values 2 3 --rebalance-bars-values 10 21 42 --benchmark equal_weight --train-bars 756 --test-bars 126 --step-bars 126
trading-cli research walkforward --universe nasdaq100_current --strategy xsec_momentum_topn --lookback-bars-values 126 252 --skip-bars-values 0 5 21 --top-n-values 2 3 5 --rebalance-bars-values 21 42 --benchmark equal_weight --train-bars 756 --test-bars 126 --step-bars 126
trading-cli research walkforward --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars-values 126 --skip-bars-values 0 --top-n-values 2 --rebalance-bars-values 21 --benchmark equal_weight --train-bars 756 --test-bars 126 --step-bars 126 --cost-bps 10 --output artifacts/experiments/mag7_xsec_walkforward_cost10.csv
trading-cli research run --preset xsec_nasdaq100_momentum_v1_research --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/nasdaq100_xsec_v1_research
trading-cli research walkforward --preset xsec_nasdaq100_momentum_v1_research --train-bars 756 --test-bars 126 --step-bars 126 --output artifacts/experiments/nasdaq100_xsec_v1_research_walkforward.csv
trading-cli research compare-xsec-construction --universe nasdaq100 --strategy xsec_momentum_topn --lookback-bars-values 84 --skip-bars-values 21 --top-n-values 2 --rebalance-bars-values 21 --start 2020-01-01 --train-bars 756 --test-bars 126 --step-bars 126 --cost-bps 10 --benchmark equal_weight --max-position-weight 0.5 --min-avg-dollar-volume 50000000 --weighting-scheme inv_vol --vol-lookback-bars 20 --max-turnover-per-rebalance 0.5 --turnover-buffer-bps 0
trading-cli research compare-xsec-construction --preset xsec_nasdaq100_momentum_v1_research --train-bars 756 --test-bars 126 --step-bars 126 --max-position-weight 0.5 --min-avg-dollar-volume 50000000 --weighting-scheme inv_vol --vol-lookback-bars 20 --max-turnover-per-rebalance 0.5 --turnover-buffer-bps 0
trading-cli research decision-memo --preset xsec_nasdaq100_momentum_v1_research --deploy-preset xsec_nasdaq100_momentum_v1_deploy
trading-cli research alpha --universe magnificent7 --feature-dir data/features --signal-family momentum --lookbacks 5 10 20 60 --horizons 1 5 20 --output-dir artifacts/alpha_research
trading-cli research loop --universe nasdaq100_current --feature-dir data/features --signal-families momentum mean_reversion volatility feature_combo --max-iterations 1
trading-cli research multi-universe --universes sp500 nasdaq100_current liquid_top_100 --feature-dir data/features --signal-family momentum
trading-cli research refresh --universe sp500 --feature-dir data/features --stale-after-days 30
trading-cli research monitor --tracker-dir artifacts/experiment_tracking --snapshot-dir artifacts/research_refresh/approved_configuration_snapshots
trading-cli portfolio backtest --universe magnificent7 --strategy sma_cross --rebalance-frequency weekly --output-dir artifacts/portfolio
trading-cli portfolio topn --universe magnificent7 --strategy momentum_hold --lookback 20 --top-n 3 --weighting-scheme inverse_vol
trading-cli paper run --symbols AAPL MSFT NVDA --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --top-n 5 --state-path artifacts/paper/paper_state.json --output-dir artifacts/paper
trading-cli paper run --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
trading-cli paper run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
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

Universe ingest is robust by default. If one ticker fails, the batch continues and prints a final summary with success count, failure count, and failed symbols. Use `--failure-report` to save a CSV report, or `--fail-fast` if you want the old stop-on-first-error behavior.

`nasdaq100` and `nasdaq100_current` currently resolve to the same explicit current-survivor ticker snapshot for reproducible present-day research. True historical point-in-time Nasdaq-100 membership is not yet implemented, so historical tests on this universe still carry survivorship bias.

Feature generation commands:

```bash
trading-cli data features --symbols AAPL MSFT --feature-groups trend momentum
trading-cli data features --universe nasdaq100_current --feature-groups trend momentum volatility volume --failure-report artifacts/features/nasdaq100_current_failures.csv
trading-cli features build --universe nasdaq100_current --feature-groups trend momentum volatility volume
```

`trading-cli data features ...` is the canonical path. `trading-cli features build ...` is supported as a compatibility alias and rewrites to `data features`. Universe feature builds now continue past symbols with missing normalized inputs, print a final success/failure summary, and can write a CSV failure report with `--failure-report`.

Date-bounded legacy research example:

```bash
trading-cli research run --symbols AAPL --strategy sma_cross --fast 20 --slow 100 --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/aapl_2020_2024
```

`breakout_hold` supports an optional momentum confirmation filter. When `--momentum-lookback` is provided, the strategy only enters a breakout if trailing return over that lookback is positive. Exits still follow the breakout exit rule independently of the momentum filter.

`xsec_momentum_topn` is a relative-strength portfolio strategy for small universes and broader liquid universes. On each rebalance date it ranks symbols by trailing return over `--lookback-bars`, optionally skips the most recent `--skip-bars`, selects the top `--top-n`, and holds them equally weighted until the next `--rebalance-bars` interval. The current benchmark option is `--benchmark equal_weight`, which compares against equal-weight buy-and-hold over the same universe and date range.

## Versioned Xsec Presets

The currently validated Nasdaq-100 xsec momentum family is the `84 / 21 / 2 / 21` family:

- `lookback_bars=84`
- `skip_bars=21`
- `top_n=2`
- `rebalance_bars=21`

Two versioned presets promote that family into an explicit workflow:

- `xsec_nasdaq100_momentum_v1_research`
  - Uses `pure_topn`
  - Intended for research, walk-forward validation, and side-by-side construction comparison
- `xsec_nasdaq100_momentum_v1_deploy`
  - Uses `transition`
  - Adds the validated implementation overlay controls:
  - `max_position_weight=0.5`
  - `min_avg_dollar_volume=50000000`
  - `weighting_scheme=inv_vol`
  - `vol_lookback_bars=20`
  - `max_turnover_per_rebalance=0.5`
  - `turnover_buffer_bps=0`

Presets are versioned so the validated family can evolve without silently changing the behavior of old commands, reports, or paper workflows. A preset fills command defaults, but any explicit CLI flag still overrides it.

The xsec workflow also supports an optional constrained portfolio layer:

- `--portfolio-construction-mode pure_topn|transition`: `pure_topn` keeps realized holdings tied to the current selected top-N set and is the research-clean baseline. `transition` allows gradual movement from the current portfolio toward the target and can temporarily carry more names than `top_n`.
- `--max-position-weight`: cap target weight per name after the target portfolio is built.
- `--min-avg-dollar-volume`: require rolling average dollar volume before a symbol is eligible.
- `--max-names-per-sector`: cap selected names per sector/group when metadata is available.
- `--turnover-buffer-bps`: require a replacement candidate to beat the weakest incumbent by at least this raw score gap. The implementation maps this to score units as `turnover_buffer_bps / 10000`.
- `--max-turnover-per-rebalance`: cap gross weight traded on each rebalance and move only partway toward the ideal target when needed.
- `--weighting-scheme equal|inv_vol`: choose equal-weight or inverse-vol target weights.
- `--vol-lookback-bars`: realized-vol lookback used by `inv_vol`.

**Research Baseline Vs Deployable Overlay**

`pure_topn` is now the default because it preserves the original research meaning of `xsec_momentum_topn`. In this mode, realized holdings stay tied to the feasible selected set up to `top_n`; stale names are not allowed to linger just because turnover is capped. Use this mode for signal research, parameter comparison, robustness checks, and benchmark-relative validation.

`transition` is the explicit deployable overlay for low-turnover portfolios. In that mode, realized holdings can temporarily exceed `top_n` while the portfolio moves gradually toward the target. Use this mode when you want to understand how real-world turnover controls, liquidity filters, and partial transitions change the clean research baseline.

Diagnostics now report the distinction explicitly with `portfolio_construction_mode`, target selected count, realized holdings count, holdings-to-top-N ratio, turnover-cap bindings, turnover-buffer blocks, liquidity/sector exclusions, and semantic-warning fields.

Use `trading-cli research compare-xsec-construction` when you want the platform to run both modes side by side from the same xsec walk-forward configuration and write a compact comparison summary, per-window deltas, and an HTML report. This command is intentionally explicit about the semantic split:

- `pure_topn` = research baseline
- `transition` = implementation overlay

Metrics to inspect first in the comparison output:

- `avg_excess_return_pct`
- `mean_turnover`
- `worst_test_max_drawdown_pct`
- `mean_average_realized_holdings_count`
- `total_turnover_cap_binding_count`

### Research Preset Examples

Use the research preset when the question is whether the signal family is valid and robust:

```bash
trading-cli research run --preset xsec_nasdaq100_momentum_v1_research --start 2020-01-01 --end 2024-12-31 --output-dir artifacts/research/nasdaq100_xsec_v1_research
trading-cli research walkforward --preset xsec_nasdaq100_momentum_v1_research --train-bars 756 --test-bars 126 --step-bars 126 --output artifacts/experiments/nasdaq100_xsec_v1_research_walkforward.csv
trading-cli research compare-xsec-construction --preset xsec_nasdaq100_momentum_v1_research --train-bars 756 --test-bars 126 --step-bars 126 --max-position-weight 0.5 --min-avg-dollar-volume 50000000 --weighting-scheme inv_vol --vol-lookback-bars 20 --max-turnover-per-rebalance 0.5 --turnover-buffer-bps 0
```

### Decision Memo Artifact

Generate a durable preset decision artifact with:

```bash
trading-cli research decision-memo --preset xsec_nasdaq100_momentum_v1_research --deploy-preset xsec_nasdaq100_momentum_v1_deploy
```

The command writes a versioned markdown memo and a machine-readable JSON summary under `artifacts/experiments/`. The memo records the selected research preset, deploy preset, core parameters, robustness findings, caveats, and next steps.

### Paper Trading With The Deploy Preset

Use the deploy preset when the question is whether the validated family is usable under implementation constraints:

```bash
trading-cli paper run --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
trading-cli paper run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
```

The paper run surfaces deploy-focused diagnostics such as:

- preset name
- `portfolio_construction_mode`
- selected names and target names
- realized holdings count and realized holdings minus `top_n`
- average gross exposure
- liquidity exclusions and sector-cap exclusions
- turnover-cap bindings and turnover-buffer blocked replacements
- semantic warnings and rebalance timestamp

### Daily Paper Trading Operations

For the validated Nasdaq-100 deploy overlay, the recommended local operational command is:

```bash
trading-cli paper run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
```

Repeated runs write durable, idempotent artifacts under the chosen output directory:

- `paper_equity_curve.csv`: persistent per-run equity / NAV history
- `paper_positions_history.csv`: realized holdings by run timestamp
- `paper_orders_history.csv`: generated paper orders and target changes by run timestamp
- `paper_run_summary.csv`: one summary row per run
- `paper_run_summary_latest.json`: latest machine-readable run summary
- `paper_run_summary_latest.md`: latest human-readable run summary
- `paper_health_checks.csv`: pass / warn / fail diagnostics for each run

The existing snapshot-style files are still written for the latest run:

- `paper_orders.csv`
- `paper_positions.csv`
- `paper_target_weights.csv`
- `paper_summary.json`
- `paper_equity_snapshot.csv`

What to inspect each day before trusting live deployment:

- `paper_run_summary_latest.md` for the quick operational read
- `paper_health_checks.csv` for new warnings or failures
- `paper_equity_curve.csv` for equity continuity and unexpected jumps
- `paper_positions_history.csv` and `paper_orders_history.csv` for holdings drift and turnover
- the terminal diagnostics for selected names, realized holdings count, liquidity exclusions, turnover-cap bindings, and semantic warnings

Recommended cadence:

- run once after the close or once before the next session open for daily strategies
- keep the preset version fixed until a new family is validated and promoted

Windows Task Scheduler example:

1. Program/script: `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\trading-cli.exe`
2. Add arguments:
   `paper run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec`
3. Start in:
   `C:\Users\bradl\PycharmProjects\trading_platform`

This scheduled wrapper calls the same paper-trading path as `paper run`, exits nonzero on hard failures, and is intended to be the local operational entrypoint for the validated deploy preset.

### Live Dry-Run Order Preview

Use `live dry-run` when you want a broker-account-aware order preview from the deploy preset without sending any live orders. This path loads the current broker account and positions, builds the deploy target portfolio, reconciles target versus current holdings, adjusts for open orders, and writes an auditable proposed-orders package.

Recommended deploy-preset preview:

```bash
trading-cli live dry-run --preset xsec_nasdaq100_momentum_v1_deploy --broker mock --output-dir artifacts/live_dry_run/nasdaq100_xsec
```

With a real broker connection:

```bash
trading-cli live dry-run --preset xsec_nasdaq100_momentum_v1_deploy --broker alpaca --output-dir artifacts/live_dry_run/nasdaq100_xsec
```

How it differs from paper trading:

- `paper run` updates a local simulated portfolio state and writes persistent paper ledgers over time.
- `live dry-run` does not mutate broker state and does not submit orders.
- `live dry-run` is the pre-send audit step for a real or mock brokerage account snapshot.

Artifacts written under the chosen output directory:

- `live_dry_run_summary.json`: machine-readable preview summary and health checks
- `live_dry_run_summary.md`: concise human-readable operational summary
- `live_dry_run_target_positions.csv`: target weights and target notionals
- `live_dry_run_current_positions.csv`: current broker/account positions
- `live_dry_run_proposed_orders.csv`: adjusted proposed orders after open-order awareness
- `live_dry_run_reconciliation.csv`: current vs target vs delta audit table
- `live_dry_run_health_checks.csv`: pass / warn / fail safety checks

Fields surfaced in the preview package include:

- preset name
- strategy and universe
- `portfolio_construction_mode`
- cash and equity
- selected names and target names
- realized holdings count and realized holdings minus `top_n`
- liquidity exclusions and sector-cap exclusions
- turnover-cap bindings and turnover-buffer blocked replacements
- target/current deltas and proposed order quantities
- blocked flags, warning flags, and no-op reasons where applicable

Recommended checklist before enabling real order submission:

- review `live_dry_run_summary.md` first
- confirm `live_dry_run_health_checks.csv` has no hard failures
- inspect `live_dry_run_reconciliation.csv` for large single-name changes, missing prices, or unexpected no-op rows
- confirm `live_dry_run_proposed_orders.csv` matches the expected deploy overlay behavior
- compare selected names, target names, realized holdings count, and turnover-cap bindings versus the latest paper run
- verify broker connectivity and account equity look sane for the intended account

### Scheduled Live Dry-Run

Use the scheduler-friendly wrapper when you want a durable daily broker-preview record with readiness semantics and latest-summary artifacts:

```bash
trading-cli live run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --broker mock --output-dir artifacts/live_dry_run/nasdaq100_xsec
```

The scheduled command reuses the same live dry-run target-construction and reconciliation logic, but additionally writes persistent operator artifacts:

- `live_run_summary.csv`: one upserted summary row per run key
- `live_run_summary_latest.json`: latest machine-readable operator summary
- `live_run_summary_latest.md`: latest human-readable operator summary
- `live_health_checks.csv`: persistent pass / warn / fail checks by run
- `live_proposed_orders_history.csv`: persistent proposed-order history
- `live_reconciliation_history.csv`: persistent target/current reconciliation history
- `live_notification_payload.json`: notification-ready summary for manual forwarding
- `runs/<timestamp>/live_run_summary.json`: optional per-run snapshot

Readiness meanings:

- `ready_for_manual_review`: no hard-fail checks; review the order package manually
- `degraded`: no hard fail, but more material warnings exist and need closer attention
- `blocked`: one or more fail checks; the command exits nonzero and the run should not be promoted further

Recommended daily review checklist:

- open `live_run_summary_latest.md` first
- confirm `live_run_summary_latest.json` shows the expected preset, broker, readiness, and target names
- check `live_health_checks.csv` for new `fail` rows or repeated `warn` patterns
- inspect `live_proposed_orders_history.csv` for unusually high order count or large single-name changes
- inspect `live_reconciliation_history.csv` for missing prices, blocked rows, or large drift from current holdings
- compare the scheduled live preview against the latest paper run to make sure deploy diagnostics are directionally consistent

Recommended validation ladder for this strategy:

```bash
trading-cli research sweep --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars-values 84 126 168 --skip-bars-values 0 5 10 --top-n-values 2 3 --rebalance-bars-values 10 21 42 --benchmark equal_weight --start 2020-01-01 --end 2024-12-31 --output artifacts/experiments/mag7_xsec_sweep.csv
trading-cli research walkforward --universe magnificent7 --strategy xsec_momentum_topn --lookback-bars-values 126 --skip-bars-values 0 --top-n-values 2 --rebalance-bars-values 21 --benchmark equal_weight --train-bars 756 --test-bars 126 --step-bars 126 --start 2020-01-01 --end 2024-12-31 --output artifacts/experiments/mag7_xsec_walkforward.csv
trading-cli research walkforward --universe liquid_top_100 --strategy xsec_momentum_topn --lookback-bars-values 126 252 --skip-bars-values 0 5 21 --top-n-values 2 3 5 --rebalance-bars-values 21 42 --benchmark equal_weight --train-bars 756 --test-bars 126 --step-bars 126 --start 2020-01-01 --end 2024-12-31 --output artifacts/experiments/liquid_top_100_xsec_walkforward.csv
trading-cli research walkforward --universe liquid_top_100 --strategy xsec_momentum_topn --lookback-bars-values 126 --skip-bars-values 0 --top-n-values 2 --rebalance-bars-values 21 --benchmark equal_weight --train-bars 756 --test-bars 126 --step-bars 126 --start 2020-01-01 --end 2024-12-31 --cost-bps 10 --output artifacts/experiments/liquid_top_100_xsec_walkforward_cost10.csv
trading-cli research walkforward --universe nasdaq100_current --strategy xsec_momentum_topn --lookback-bars-values 126 252 --skip-bars-values 0 5 21 --top-n-values 2 3 5 --rebalance-bars-values 21 42 --benchmark equal_weight --train-bars 756 --test-bars 126 --step-bars 126 --start 2020-01-01 --end 2024-12-31 --cost-bps 10 --output artifacts/experiments/nasdaq100_current_xsec_walkforward_cost10.csv
```

Validation ladder:

1. Start with `magnificent7` for a compact sanity check.
2. Expand to `nasdaq100_current`, `sp100`, or `liquid_top_100` for breadth.
3. Re-run walk-forward with `--cost-bps` to test friction sensitivity.
4. Compare the strategy against `--benchmark equal_weight` and prioritize stable excess return.

For xsec research, `--cost-bps` applies a simple linear cost on rebalance turnover. Artifacts now report `benchmark_type`, gross return, net return, cost drag, average turnover, annualized turnover, and per-rebalance transaction cost diagnostics.

Constrained xsec examples:

```bash
trading-cli research walkforward --universe nasdaq100 --strategy xsec_momentum_topn --lookback-bars-values 84 --skip-bars-values 21 --top-n-values 2 --rebalance-bars-values 21 --benchmark equal_weight --start 2020-01-01 --train-bars 756 --test-bars 126 --step-bars 126 --cost-bps 10 --output artifacts/experiments/nasdaq100_xsec_baseline_walkforward.csv
trading-cli research walkforward --universe nasdaq100 --strategy xsec_momentum_topn --lookback-bars-values 84 --skip-bars-values 21 --top-n-values 2 --rebalance-bars-values 21 --benchmark equal_weight --start 2020-01-01 --train-bars 756 --test-bars 126 --step-bars 126 --cost-bps 10 --portfolio-construction-mode pure_topn --output artifacts/experiments/nasdaq100_xsec_baseline_walkforward.csv
trading-cli research walkforward --universe nasdaq100 --strategy xsec_momentum_topn --lookback-bars-values 84 --skip-bars-values 21 --top-n-values 2 --rebalance-bars-values 21 --benchmark equal_weight --start 2020-01-01 --train-bars 756 --test-bars 126 --step-bars 126 --cost-bps 10 --portfolio-construction-mode transition --max-position-weight 0.5 --min-avg-dollar-volume 50000000 --weighting-scheme inv_vol --vol-lookback-bars 20 --max-turnover-per-rebalance 0.5 --turnover-buffer-bps 0 --output artifacts/experiments/nasdaq100_xsec_constrained_walkforward.csv
```

Compare constrained versus unconstrained runs with:

- `avg_test_return_pct`
- `avg_excess_return_pct`
- `worst_excess_return_pct`
- `total_trade_count`
- `mean_turnover`
- `mean_annualized_turnover`
- `worst_test_max_drawdown_pct`
- `percent_positive_windows`

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
- `nasdaq100` currently aliases `nasdaq100_current`, which is a present-day survivor universe rather than true point-in-time historical membership.
- Backtests on current-survivor universes introduce survivorship bias unless a historical membership model is added.
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
