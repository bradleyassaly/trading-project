# Trading Platform

Production-oriented local trading workflow for:

- market data ingest and feature generation
- research runs and walk-forward validation
- preset-driven deployment decisions
- scheduled paper trading
- broker-safe live dry-run previews
- local artifact inspection through a read-only dashboard

## Current Status

The repository is now organized around one supported workflow:

`data refresh-research-inputs -> research alpha -> research promote -> strategy-portfolio build -> paper run -> live dry-run`

The primary validated operational example is:

- `xsec_nasdaq100_momentum_v1_research`
- `xsec_nasdaq100_momentum_v1_deploy`

Those presets still represent the clearest single-strategy Nasdaq-100 momentum example, but the canonical hardening target is now the config-driven research-input refresh through promoted-strategy portfolio flow.

## Supported Vs Experimental

### Supported

- grouped CLI centered on `data`, `research`, `portfolio`, `paper`, `live`, `dashboard`, and `ops`
- config-driven research-input refresh plus standard `data/features` + `data/metadata` prep
- alpha research with explicit context slicing and conditional promotion support
- promoted-strategy to strategy-portfolio handoff
- canonical-bundle experiment harness for small promotion-policy and portfolio-policy variant comparisons
- config-first paper runs and live dry-runs
- scheduled paper trading artifacts
- broker-safe live dry-run artifacts
- local dashboard over the artifact tree with DB-backed normalized reads when available

### Experimental / Legacy

- `research loop`
- `research multi-universe`
- `research multi-universe-report`
- `research refresh`
- `research monitor`
- legacy flat command aliases and older top-level groups preserved only as thin rewrites to the canonical grouped commands
- legacy strategy workflows outside the validated Nasdaq-100 xsec momentum preset path
- broader orchestration/experimentation surfaces that are still useful, but not the primary supported operating path

## Install

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

## Hybrid Storage Architecture

The platform now supports a pragmatic hybrid storage model:

- files remain the primary data plane for heavy artifacts
- PostgreSQL is an optional control plane for normalized metadata, lineage, and cross-run queryability
- dashboard and ops reads can now prefer PostgreSQL for normalized history/lineage queries while still falling back to artifacts

Artifact-first is still the rule.

Keep these in files:

- large feature matrices
- signal snapshots
- walk-forward result grids
- charts, HTML reports, and images
- large diagnostics and leaderboard exports

Store these in PostgreSQL when DB metadata is enabled:

- research runs and portfolio runs
- artifact registry metadata and lineage links
- strategy definitions and promotion decisions
- candidate evaluations, portfolio decisions, and signal contributions
- order, order-event, fill, and position metadata
- universe membership and filter-level provenance summaries

When DB metadata is disabled, workflows continue to run in the existing artifact-first mode.
When DB metadata is enabled, the same workflows still write artifacts first and additionally write normalized metadata rows.

Read-path behavior follows the same rule:

- dashboard pages prefer PostgreSQL for normalized run history, trade decision history, promotion history, and linked execution/provenance metadata when DB metadata is enabled and rows exist
- dashboard pages fall back to the existing artifact readers when DB metadata is disabled or the relevant rows are missing
- heavy feature, signal, chart, and diagnostics payloads remain artifact-backed even when DB reads are enabled

## Database Setup

Phase 1 targets PostgreSQL through SQLAlchemy 2.x and Alembic.

Optional environment variables:

```bash
TRADING_PLATFORM_ENABLE_DATABASE_METADATA=1
TRADING_PLATFORM_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/trading_platform
TRADING_PLATFORM_DATABASE_SCHEMA=public
```

You can also put the same settings in workflow configs:

```yaml
database:
  enable: true
  database_url: postgresql+psycopg://postgres:postgres@localhost:5432/trading_platform
  database_schema: public
```

Local PostgreSQL example:

```bash
createdb trading_platform
```

Run migrations:

```bash
alembic upgrade head
```

Create a new revision later:

```bash
alembic revision -m "describe change"
```

The initial migration adds the phase-1 control-plane tables for:

- symbols, universes, and universe memberships
- research runs and portfolio runs
- strategy definitions, promotion decisions, and promoted strategies
- artifact registry and run-artifact links
- portfolio decisions, signal contributions, and position snapshots
- orders, order events, and fills
- candidate evaluations and universe filter results

The dashboard read path uses thin SQLAlchemy query services rather than exposing raw ORM rows directly:

- `RunQueryService`
- `DecisionQueryService`
- `ExecutionQueryService`
- `ArtifactQueryService`
- `OpsQueryService`

These services shape stable read models for the dashboard and ops tooling and keep the database layer optional.

## DB Lineage

When enabled, the intended lineage chain is:

`research run -> strategy definition -> promotion decision -> promoted strategy -> portfolio run -> candidate evaluation -> portfolio decision -> order / fill -> artifact registry`

This phase intentionally stores metadata and references, not heavyweight research payloads.

Current phase-1 workflow coverage:

- `research run`: creates a `ResearchRun`, records the strategy definition, and registers emitted research artifacts
- `research promote`: records promotion decisions and promoted-strategy state when promotions are selected
- `paper run`: creates a `PortfolioRun`, records candidate/trade decision metadata, order/fill metadata, position snapshots, and emitted paper artifacts
- `live dry-run`: creates a `PortfolioRun`, records decision metadata, preview order lifecycle metadata, broker position snapshots, and emitted live-preview artifacts

Current deferred items:

- full migration of walk-forward grids, leaderboard history, and large research diagnostics into relational form
- a dedicated execution-run table beyond the current portfolio-run linkage
- deep normalization of every universe-enrichment field into first-class relational tables
- automatic Postgres provisioning or vendor-specific ingestion pipelines

## Canonical Workflow

### 1. Refresh research inputs

```bash
trading-cli data refresh-research-inputs --config configs/research_input_refresh.yaml
```

This refreshes the canonical research inputs together:

- `data/features/<SYMBOL>.parquet`
- `data/metadata/sub_universe_snapshot.csv`
- `data/metadata/universe_enrichment.csv`
- `data/metadata/research_metadata_sidecar_summary.json`
- `data/metadata/research_input_refresh_summary.json`

### 2. Run alpha research

```bash
trading-cli research alpha --config configs/alpha_research.yaml
```

Key outputs include:

- `research_run.json`
- `signal_performance_by_regime.csv`
- `signal_performance_by_sub_universe.csv`
- `signal_performance_by_benchmark_context.csv`
- `context_features/<SYMBOL>.parquet`

`research alpha` now follows the same config-first pattern as the rest of the canonical flow:

- use `--config` for the primary path
- keep direct flags for ad hoc runs
- explicit CLI flags override config values when both are provided

Alpha research also supports structured candidate-depth expansion inside a single family:

- `signals.candidate_grid_preset: standard | broad_v1`
- `signals.signal_composition_preset: standard | composite_v1 | research_rich_v1`
- `signals.max_variants_per_family: <int>`
- `signals.enable_context_confirmations: true | false`
- `signals.enable_relative_features: true | false`
- `signals.enable_flow_confirmations: true | false`

`broad_v1` keeps the family fixed and emits a finite set of auditable variants with stable IDs such as:

- `cross_sectional_momentum_breadth_confirmed_lb20_hz5`
- `breakout_continuation_tight_breakout_lb10_hz1`
- `liquidity_flow_tilt_dollar_flow_emphasis_lb20_hz5`

The resulting artifacts preserve:

- `signal_family`
- `signal_variant`
- `candidate_id`
- `candidate_name`

### 3. Promote condition-aware strategies

```bash
trading-cli research promote --artifacts-root artifacts/alpha_research --output-dir artifacts/promoted_strategies --policy-config configs/promotion_experiment.yaml
```

The canonical `research promote` path now refreshes the research registry and promotion-candidate artifacts automatically into `artifacts/alpha_research/research_registry` by default before generating promoted strategy presets/configs.

### 4. Build the strategy portfolio

```bash
trading-cli strategy-portfolio build --promoted-dir artifacts/promoted_strategies --output-dir artifacts/strategy_portfolio
```

Optional first-pass policy experiments can now start from the same hardened exported bundle instead of rebuilding the full upstream path:

```bash
trading-cli strategy-portfolio experiment-bundle --config configs/canonical_bundle_experiment.yaml
```

The built-in `policy_sensitivity_v1` preset set keeps the canonical promoted/exported bundle fixed and compares:

- `baseline`
- `strict_promotion`
- `loose_promotion`
- `alternate_weighting`
- `combined_strict_weighting`

These vary only:

- promotion policy conditional-variant thresholds, diversity safeguards, and selection caps
- strategy-portfolio weighting mode, family-aware selection limits, and concentration smoothing

It writes variant-isolated outputs plus compact comparison artifacts such as:

- `experiment_summary.json`
- `experiment_variant_results.csv`
- `experiment_variant_results.json`
- `experiment_policy_comparison.csv`
- per-variant `strategy_portfolio/`, `run_bundle/`, and `daily_pipeline_config.json`

The quickest fields to compare are:

- promoted strategy count
- promoted signal family count
- conditional variant count
- selected conditional variant count
- selected strategy count
- signal family count
- allocation concentration (`max_strategy_weight`, `effective_strategy_count`)
- allocation change vs baseline (`allocation_l1_delta_vs_baseline`)
- paper/live readiness

To compare the same preset set across multiple promoted bundles or dates:

```bash
trading-cli strategy-portfolio experiment-bundle-matrix --config configs/canonical_bundle_experiment_matrix.yaml
```

This keeps the policy matrix fixed and varies only the bundle/date case. The top-level outputs are:

- `bundle_case_results.json`
- `experiment_time_stability.csv`
- `experiment_time_stability.json`
- `bundle_policy_stability_summary.json`

The most useful cross-date stability fields are:

- promoted strategy count mean/range by variant
- conditional variant count mean/range by variant
- selected strategy count mean/range by variant
- effective strategy count mean/range by variant
- max strategy weight mean/range by variant
- allocation L1 delta vs baseline mean/range by variant
- paper/live readiness pass counts

Use this before adding new strategy families:

- if policy sensitivity is stable across dates, the next step is broader strategy diversity
- if policy sensitivity is highly date-dependent, promotion and portfolio robustness should be improved first

### 5. Run paper trading

```bash
trading-cli paper run --config configs/workflows/paper_xsec_nasdaq100.yaml
```

### 6. Run the live dry-run preview

```bash
trading-cli live dry-run --config configs/workflows/live_xsec_nasdaq100.yaml
```

Secondary research commands such as `research run`, `research walkforward`, and `research memo` still exist for direct strategy research and validation, but they are no longer the primary end-to-end path documented here.

Canonical automated coverage now exists at three levels:

- one-shot config-driven supported-path smoke coverage
- repeated scheduled-style reuse coverage for the exported multi-strategy bundle
- repeated daily-config reuse coverage plus canonical bundle experiment coverage for small policy variant comparisons

## Main CLI Layout

### Stable top-level groups

- `data`
- `research`
- `portfolio`
- `paper`
- `live`
- `dashboard`
- `ops`

### `data`

- `data ingest`
- `data features`
- `data universes list`
- `data universes export`

### `research`

Main supported path:

- `research alpha`
- `research promote`
- `research leaderboard`
- `research compare-runs`

Experimental / advanced:

- `research run`
- `research walkforward`
- `research memo`
- `research registry build`
- `research promotion-candidates`
- `research sweep`
- `research validate-signal`
- `research loop`
- `research multi-universe`
- `research multi-universe-report`
- `research refresh`
- `research monitor`
- `research compare-xsec-construction`

### `portfolio`

- `portfolio backtest`
- `portfolio topn`
- `portfolio allocate-multi-strategy`
- `portfolio apply-execution-constraints`

### `paper`

- `paper run`
- `paper schedule`
- `paper run-multi-strategy`
- `paper daily`
- `paper report`

### `live`

- `live dry-run`
- `live schedule`
- `live submit`
- `live dry-run-multi-strategy`
- `live submit-multi-strategy`
- `live validate`
- `live execute`

### `dashboard`

- `dashboard serve`
- `dashboard build-static-data`

Main pages and APIs:

- `/`
- `/trades`
- `/ops`
- `/api/discovery/overview`
- `/api/discovery/recent-trades`
- `/api/discovery/recent-symbols`
- `/portfolio`
- `/symbols/<SYMBOL>`
- `/strategies/<STRATEGY_ID>`
- `/trades/<TRADE_ID>`
- `/api/trades-blotter`
- `/api/ops`
- `/api/chart/<SYMBOL>`
- `/api/trades/<SYMBOL>`
- `/api/signals/<SYMBOL>`
- `/api/trade/<TRADE_ID>`
- `/api/portfolio/overview`
- `/api/portfolio/equity`
- `/api/portfolio/activity`
- `/api/execution/diagnostics`
- `/api/strategies/<STRATEGY_ID>`

### `ops`

- `ops doctor`
- `ops pipeline run`
- `ops pipeline run-daily`
- `ops pipeline run-weekly`
- `ops monitor latest`
- `ops monitor run-health`
- `ops monitor strategy-health`
- `ops monitor portfolio-health`
- `ops monitor build-dashboard-data`
- `ops monitor notify`
- `ops registry list`
- `ops registry evaluate-promotion`
- `ops registry evaluate-degradation`
- `ops registry promote`
- `ops registry demote`
- `ops registry build-deploy-config`
- `ops broker health`
- `ops broker cancel-all`
- `ops execution simulate`
- `ops orchestrate ...`
- `ops system-eval ...`
- `ops experiment ...`
- `ops experiments ...`

## Config-First Workflows

Typed workflow configs are now supported for:

- research runs
- walk-forward evaluation
- paper runs
- live dry-runs

The goal is to keep defaults, overrides, and automation inputs in one place instead of duplicating long CLI invocations.

Example research workflow config:

```yaml
preset: xsec_nasdaq100_momentum_v1_research
strategy: xsec_momentum_topn
engine: vectorized
output_dir: artifacts/research/xsec_nasdaq100_v1
lookback_bars: 84
skip_bars: 21
top_n: 2
rebalance_bars: 21
portfolio_construction_mode: pure_topn
benchmark: equal_weight
cost_bps: 10.0
conditional_research:
  enabled: true
  condition_types: [regime, sub_universe, benchmark_context]
  min_sample_size: 30
  compare_to_baseline: true
  allow_variants: true
```

Example paper workflow config:

```yaml
preset: xsec_nasdaq100_momentum_v1_deploy
state_path: artifacts/paper/nasdaq100_xsec_state.json
output_dir: artifacts/paper/nasdaq100_xsec
execution_config: configs/execution.yaml
portfolio_construction_mode: transition
top_n: 2
```

CLI flags still override config-file values.

## Artifact Layout

The dashboard and downstream tooling should prefer the summary and history files below.

### Research

- `artifacts/research/.../*_run_summary.json`
- `artifacts/research/.../*_timeseries.csv`
- `artifacts/research/.../*_signals.csv`
- `artifacts/research/.../conditional_signal_performance.csv`
- `artifacts/research/.../conditional_signal_performance.json`
- `artifacts/research/.../conditional_research_summary.json`
- `artifacts/research/.../conditional_promotion_candidates.csv`
- `data/features/<SYMBOL>.parquet`

### Walk-forward

- `artifacts/walkforward/*.csv`
- `artifacts/walkforward/*_summary.csv`
- `artifacts/walkforward/*_workflow_summary.json`
- `artifacts/walkforward/*_report.html`

### Decision memo

- `artifacts/decision_memos/*_decision_memo.md`
- `artifacts/decision_memos/*_decision_memo.json`

### Deploy / portfolio

- `artifacts/generated_registry_multi_strategy.json`
- `artifacts/.../allocation_summary.json`
- `artifacts/.../combined_target_weights.csv`

### Paper

- `paper_summary.json`
- `paper_run_summary.csv`
- `paper_run_summary_latest.json`
- `paper_run_summary_latest.md`
- `paper_health_checks.csv`
- `paper_equity_curve.csv`
- `paper_fills.csv`
- `paper_positions.csv`
- `paper_orders.csv`
- `universe_membership.json`
- `universe_membership.csv`
- `universe_filter_results.json`
- `universe_filter_results.csv`
- `universe_build_summary.json`
- `sub_universe_snapshot.csv`
- `universe_enrichment.json`
- `universe_enrichment.csv`
- `point_in_time_membership.csv`
- `universe_enrichment_summary.json`
- `reference_data_coverage_summary.json`
- `membership_resolution_audit.csv`
- `taxonomy_resolution_audit.csv`
- `benchmark_mapping_resolution_audit.csv`
- `reference_data_manifest.json` when a maintained manifest is present
- `candidate_snapshot.json`
- `candidate_snapshot.csv`
- `trade_decisions.json`
- `trade_decisions.csv`
- `execution_decisions.json`
- `execution_decisions.csv`
- `exit_decisions.json`
- `exit_decisions.csv`
- `trade_lifecycle.json`
- `trade_lifecycle.csv`
- `paper_positions_history.csv`
- `paper_orders_history.csv`

### Live dry-run

- `live_dry_run_summary.json`
- `live_dry_run_reconciliation.csv`
- `universe_membership.json`
- `universe_membership.csv`
- `universe_filter_results.json`
- `universe_filter_results.csv`
- `universe_build_summary.json`
- `sub_universe_snapshot.csv`
- `universe_enrichment.json`
- `universe_enrichment.csv`
- `point_in_time_membership.csv`
- `universe_enrichment_summary.json`
- `reference_data_coverage_summary.json`
- `membership_resolution_audit.csv`
- `taxonomy_resolution_audit.csv`
- `benchmark_mapping_resolution_audit.csv`
- `reference_data_manifest.json` when a maintained manifest is present
- `candidate_snapshot.json`
- `trade_decisions.json`
- `execution_decisions.json`
- `exit_decisions.json`
- `trade_lifecycle.json`

## Hybrid Data Architecture

The platform now supports a conservative hybrid market-data split:

- `yfinance` remains the historical research source
- Alpaca can be used for the latest execution-time bars
- research runs, feature generation, and walkforward stay on the existing historical path unless you explicitly opt into Alpaca latest data for paper execution

Why this split:

- `yfinance` keeps historical research and backtests simple, local, and reproducible
- Alpaca gives paper and live-adjacent workflows a cleaner latest-bar source that matches the broker environment more closely

Supported config shape for paper workflows:

```yaml
data_sources:
  prices:
    historical: yfinance
    latest: alpaca
```

Enable Alpaca latest data from the CLI:

```bash
trading-cli paper run --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/state.json --output-dir artifacts/paper/run_a --use-alpaca-latest-data
```

Or through a paper workflow config:

```yaml
preset: xsec_nasdaq100_momentum_v1_deploy
state_path: artifacts/paper/state.json
output_dir: artifacts/paper/run_a
data_sources:
  prices:
    historical: yfinance
    latest: alpaca
```

Current behavior:

- historical frames still come from the existing parquet / yfinance-derived research path
- when enabled, the latest Alpaca OHLCV bars are merged into that history for paper target construction
- if Alpaca latest-bar fetch fails, the paper path falls back to the existing historical/yfinance-derived data and logs a warning

## Data Freshness Diagnostics

Paper runs now record the freshness of the market data actually used for execution decisions.

- `latest_bar_timestamp`
- `latest_bar_age_seconds`
- `latest_data_stale`
- `latest_data_source`
- `latest_data_fallback_used`

These diagnostics are included in the paper summary JSON and the target-construction diagnostics so operators can tell whether a run used fresh Alpaca bars, fell back to historical pricing, or is making decisions on stale data.

Set the stale-data threshold from the CLI:

```bash
trading-cli paper run --preset xsec_nasdaq100_momentum_v1_deploy --use-alpaca-latest-data --latest-data-max-age-seconds 3600
```

Or in workflow YAML:

```yaml
paper:
  execution:
    latest_data_max_age_seconds: 3600
```

## Paper Execution Snapshot Artifact

Each paper run now writes `execution_price_snapshot.csv` alongside the existing paper artifacts. It records the exact execution-time price inputs used for each symbol:

- `symbol`
- `decision_timestamp`
- `historical_price`
- `latest_price`
- `final_price_used`
- `price_source_used`
- `fallback_used`
- `latest_bar_timestamp`
- `latest_bar_age_seconds`

This is intended for operator debugging, not research. It helps explain which price path actually fed the paper rebalance and whether Alpaca latest-bar data replaced or failed back to historical prices.

## Trade Decision Journal

Paper runs and live dry-run previews now persist a structured decision journal so every proposed trade can be reconstructed from stored backend facts instead of inferred later in the UI.

The journal is intentionally lightweight:

- no database
- JSON artifacts preserve nested detail
- CSV artifacts provide flattened operator-friendly inspection
- missing facts stay missing rather than being synthesized

Decision stages currently recorded:

- candidate evaluation
- portfolio selection
- sizing context
- execution request / executable / rejected order outcomes
- exit decisions for symbols removed by rebalance
- lifecycle stage rows that connect those facts

Primary artifacts:

- `candidate_snapshot.json` and `candidate_snapshot.csv`
- `trade_decisions.json` and `trade_decisions.csv`
- `execution_decisions.json` and `execution_decisions.csv`
- `exit_decisions.json` and `exit_decisions.csv`
- `trade_lifecycle.json` and `trade_lifecycle.csv`

What they capture when available:

- `decision_id`, run context, symbol, side, strategy, and universe
- feature snapshot fields such as latest price and latest asset return
- decomposable signal output fields such as final score and raw signal components
- pass/fail screening checks
- candidate rank and rank percentile
- selected versus rejected status and explicit rejection reason when determinable
- scheduled versus effective target weights
- sizing inputs such as investable equity, reserve cash, current quantity, and target quantity
- execution provenance and rejection reasons from the execution realism layer
- exit trigger type and rebalance-driven exit rationale

Current limits:

- generic signal paths only persist the fields already available from the current snapshot pipeline
- richer screen-by-screen rejection reasons are strongest today in the xsec momentum path because that research pipeline already emits exclusion diagnostics
- exit attribution is currently based on explicit rebalance and target-transition facts; richer stop-loss or regime-driven exit traces can be added as those rules become first-class artifacts

How to inspect a decision:

1. open `candidate_snapshot.csv` to see the evaluated universe, score, rank, screen results, and selected versus rejected candidates
2. open `trade_decisions.csv` to see the selected/held/exited trade-level summary and sizing context
3. open `execution_decisions.csv` to inspect requested, executable, and rejected orders with execution-layer provenance
4. open `exit_decisions.csv` and `trade_lifecycle.json` to inspect why a symbol was removed and how the decision evolved end to end

These artifacts are also what the dashboard trade detail pages should prefer for explainability, so the backend and UI now share the same persisted facts.

## Universe And Screening Provenance

The trade decision journal now has an upstream companion layer for universe construction and sub-universe screening.

Conceptually the pipeline is now:

- base universe
- sequential filters / screens
- final eligible sub-universe
- candidate scoring and ranking
- portfolio selection and sizing
- order and execution outcomes

Base universe versus sub-universe:

- the base universe is the original symbol set, usually from `--universe` or explicit `--symbols`
- the sub-universe is the eligible symbol set after screens are applied
- when no filters are configured, the sub-universe defaults to the base universe

Supported first-pass filters:

- `symbol_include_list`
- `symbol_exclude_list`
- `min_price`
- `min_feature_history`
- `min_avg_dollar_volume`
- `sector_include_list`
- `sector_exclude_list`
- `min_volatility`
- `max_volatility`

Current behavior:

- price, feature-history, average-dollar-volume, and volatility filters use existing feature-frame data when available
- sector filters use the existing symbol-group metadata path when a `group_map_path` is configured
- if a configured metric is unavailable, the filter result is recorded as unavailable instead of being silently invented
- excluded symbols are now distinguishable from later scored-but-not-selected candidates

Universe provenance artifacts:

- `universe_membership.json` and `universe_membership.csv`
- `universe_filter_results.json` and `universe_filter_results.csv`
- `universe_build_summary.json`
- `sub_universe_snapshot.csv`

These artifacts show:

- which symbols were in the base universe
- which filters ran and in what order
- which symbols passed, failed, or were skipped at each stage
- the final eligible sub-universe
- per-filter failure counts and summary inclusion counts

Integration with the decision journal:

- `candidate_snapshot.*` now carries base-universe and sub-universe identifiers where available
- candidates excluded before ranking are recorded as `filtered_out`
- scored symbols that lost later remain separate from upstream universe exclusions
- trade and execution provenance now carry forward base/sub-universe identifiers for later UI explanation

Workflow config support:

```yaml
preset: xsec_nasdaq100_momentum_v1_deploy
output_dir: artifacts/paper/nasdaq100_xsec
screening:
  sub_universe_id: liquid_trend_candidates
  filters:
    - filter_name: min_price
      filter_type: min_price
      threshold: 5
    - filter_name: min_history
      filter_type: min_feature_history
      threshold: 252
    - filter_name: banned_names
      filter_type: symbol_exclude_list
      symbols: [TSLA]
```

Current limitations:

- filters are currently wired into paper trading and live dry-run target construction rather than the full research stack
- sector filters depend on configured group metadata rather than a richer built-in taxonomy layer
- regime-aware and benchmark-relative eligibility hooks are intentionally deferred, but the artifact schema leaves room for them

## Point-In-Time Membership And Metadata Enrichment

Universe provenance now includes a second pass for point-in-time membership resolution and symbol metadata enrichment.

Why this exists:

- to distinguish confirmed historical membership from present-day static fallback assumptions
- to reduce `unavailable` screening context when taxonomy, benchmark, liquidity, volatility, and regime fields can be derived locally
- to give candidate and trade explanations stronger upstream facts

Membership resolution statuses:

- `confirmed`
- `inferred`
- `static_fallback`
- `unavailable`

Current point-in-time behavior:

- if a membership history CSV is configured, membership is resolved against `effective_start` / `effective_end`
- if no point-in-time history is available, the system falls back to the configured base universe and labels it `static_fallback`
- custom symbol lists remain explicit base-universe membership inputs
- symbols outside a confirmed point-in-time membership window are excluded before later filters and ranking
- if maintained local reference data is available, the enrichment layer prefers that history before any legacy fallback files

Suggested screening config shape:

```yaml
screening:
  sub_universe_id: liquid_trend_candidates
  reference_data_root: artifacts/reference_data/v1
  membership_history_path: artifacts/universe_membership/nasdaq100_membership.csv
  taxonomy_snapshot_path: artifacts/reference_data/v1/taxonomy_snapshots.csv
  benchmark_mapping_path: artifacts/reference_data/v1/benchmark_mapping_snapshots.csv
  market_regime_path: artifacts/regime
  filters:
    - filter_name: min_price
      filter_type: min_price
      threshold: 5
    - filter_name: min_history
      filter_type: min_feature_history
      threshold: 252
```

Reusable enrichment fields now captured when available:

- point-in-time membership status, source, and resolution status
- sector / industry / group taxonomy
- benchmark context and simple relative-strength proxy
- latest price, average dollar volume, volatility, and feature-history availability
- current regime label from `market_regime.json` when configured
- metadata coverage status and missing fields

New enrichment artifacts:

- `universe_enrichment.json`
- `universe_enrichment.csv`
- `point_in_time_membership.csv`
- `universe_enrichment_summary.json`

These artifacts show:

- which symbols had confirmed versus fallback membership
- which taxonomy and benchmark fields were resolved
- which regime and feature-availability fields were attached
- coverage summaries for complete, partial, and sparse enrichment

How this improves downstream explainability:

- candidate rows now carry membership resolution status, taxonomy, benchmark context, and regime context when available
- trade explanations can distinguish static-survivor fallback membership from confirmed point-in-time membership
- benchmark-relative and taxonomy-aware screens have more reusable upstream facts instead of ad hoc recomputation
- metadata gaps are explicit through resolution-status fields and missing-field lists

Current limits:

- exact historical index membership still depends on user-provided membership-history files
- benchmark context is currently lightweight and uses either a configured benchmark symbol or a synthetic equal-weight universe proxy
- regime enrichment is optional and depends on an existing `market_regime.json` artifact

## Versioned Local Reference Data

The universe enrichment layer now supports maintained local reference datasets so historical context is reproducible instead of depending only on runtime fallback logic.

Reference-data conventions:

- set `screening.reference_data_root` to a versioned directory such as `artifacts/reference_data/v1`
- the loader looks for:
  - `universe_membership_history.csv`
  - `taxonomy_snapshots.csv`
  - `benchmark_mapping_snapshots.csv`
  - `reference_data_manifest.json`
- explicit file paths still override the default filenames if you need a non-standard layout
- if maintained files are missing, the system falls back to the older behavior and labels that fallback explicitly in the stored artifacts

Resolution statuses remain explicit:

- `confirmed`: resolved from maintained point-in-time reference data or a directly supported benchmark frame
- `inferred`: reserved for future heuristic point-in-time resolution paths
- `fallback`: resolved from weaker local assumptions or compatibility paths
- `static_fallback`: the symbol remained in the configured base universe because no point-in-time membership history existed
- `unavailable`: the field could not be resolved from maintained data or fallback logic

Recommended local layout:

```text
artifacts/
  reference_data/
    v1/
      reference_data_manifest.json
      universe_membership_history.csv
      taxonomy_snapshots.csv
      benchmark_mapping_snapshots.csv
```

The manifest is intentionally simple. A minimal example:

```json
{
  "version": "2026.03.24",
  "datasets": {
    "membership_history": {"version": "m1"},
    "taxonomy_snapshots": {"version": "t1"},
    "benchmark_mapping_snapshots": {"version": "b1"}
  }
}
```

New audit and coverage artifacts:

- `reference_data_coverage_summary.json`
- `membership_resolution_audit.csv`
- `taxonomy_resolution_audit.csv`
- `benchmark_mapping_resolution_audit.csv`
- `reference_data_manifest.json` is copied into the run output when present

These make it easy to inspect:

- how many symbols were resolved from maintained reference data versus fallback logic
- which membership decisions were confirmed point-in-time and which remained fallback-heavy
- which taxonomy and benchmark fields came from maintained dated snapshots
- which symbols still need better local reference coverage

How this improves explainability:

- trade and candidate explanations can now reference maintained membership, taxonomy, and benchmark sources instead of only best-effort runtime derivations
- historical universe eligibility becomes more auditable because effective-date snapshots are preserved locally
- benchmark-relative and taxonomy-aware screens have cleaner upstream provenance and lower `unavailable` rates

Current limitations and maintenance notes:

- the system only uses local maintained files; it does not fetch vendor data automatically
- richer inferred-resolution logic is intentionally deferred until there is a reliable local source for that inference
- refreshing coverage is a file maintenance task today: add or replace the versioned snapshot files and bump the manifest version when needed

## Paper Slippage Modeling

Paper-only slippage is now available and remains disabled by default.

- supported models: `none`, `fixed_bps`
- buy orders worsen upward by `buy_bps`
- sell orders worsen downward by `sell_bps`
- slippage is applied only to paper execution-price estimation and fills
- research, walkforward, and historical price history are unchanged

Enable from the CLI:

```bash
trading-cli paper run --preset xsec_nasdaq100_momentum_v1_deploy --slippage-model fixed_bps --slippage-buy-bps 5 --slippage-sell-bps 5
```

Or in workflow YAML:

```yaml
paper:
  execution:
    slippage:
      enabled: true
      model: fixed_bps
      buy_bps: 5
      sell_bps: 5
```

Why this stays paper-only:

- it improves execution realism for operator monitoring
- it avoids contaminating research and walkforward history with execution assumptions
- it keeps the historical alpha path reproducible while making paper trading more honest

Broader data-domain expansion is still intentionally deferred:

- no macro
- no derivatives
- no FX
- no crypto

## Signal Ensembling

The platform now supports an optional ensemble layer that combines multiple promoted signal members into one auditable portfolio input.

Why this exists:

- to reduce dependence on a single promoted candidate
- to compare candidate-level versus family-level signal blending
- to keep ensemble decisions interpretable and file-auditable

Defaults:

- disabled by default
- existing single-signal and composite paths stay unchanged unless you enable ensemble mode
- no learned meta-model or optimizer is used in this first version

Supported ensemble modes:

- `disabled`
- `candidate_weighted`
- `family_weighted`

Supported weighting methods:

- `equal`
- `performance_weighted`
- `rank_weighted`

Supported score normalization:

- `raw`
- `zscore`
- `rank_pct`

Research CLI example:

```bash
trading-cli research alpha --symbols AAPL MSFT NVDA --signal-family momentum --lookbacks 5 10 --horizons 1 --enable-ensemble --ensemble-mode candidate_weighted --ensemble-weight-method equal --ensemble-normalize-scores rank_pct
```

Paper CLI example:

```bash
trading-cli paper run --symbols AAPL MSFT NVDA --signal-source ensemble --composite-artifact-dir artifacts/alpha_research/run_a --enable-ensemble --ensemble-mode family_weighted --ensemble-weight-method rank_weighted
```

Paper workflow YAML example:

```yaml
paper:
  ensemble:
    enabled: true
    mode: family_weighted
    weight_method: equal
    normalize_scores: rank_pct
    max_members: 5
    require_promoted_only: true
```

Ensemble diagnostics and artifacts:

- `ensemble_member_summary.csv`
- `ensemble_signal_snapshot.csv`
- `ensemble_research_summary.json`
- `paper_ensemble_decision_snapshot.csv`

These artifacts show:

- which members were eligible
- which members were included or excluded
- normalized weights
- top contributing candidates and families
- the final ensemble score used by paper target construction

Current limitations:

- this is an arithmetic ensemble, not a learned meta-model
- member selection currently relies on existing promoted-candidate metrics
- ensemble paper trading is wired through the promoted-signal artifact path, not live trading

## Experimental Equity-Only Signal Families

The alpha research path now includes a small set of additional equity-only signal families built from the same local price and volume inputs:

- `volatility_adjusted_momentum`
- `volatility_adjusted_reversal`
- `short_horizon_mean_reversion`
- `momentum_acceleration`
- `cross_sectional_relative_strength`
- `cross_sectional_momentum`
- `breakout_continuation`
- `benchmark_relative_rotation`
- `regime_conditioned_momentum`
- `volatility_dispersion_selection`
- `sector_relative_momentum`
- `liquidity_flow_tilt`
- `volume_shock_momentum`

These stay additive and experimental:

- no macro
- no derivatives
- no FX
- no crypto
- no ML meta-model

The current focus is candidate depth rather than more infrastructure:

- major families can now emit multiple parameterized variants through the alpha config
- `candidate_grid_preset: broad_v1` expands the candidate pool materially without turning into unrestricted search
- `signal_composition_preset: composite_v1` upgrades those candidates from mostly single-measure scores into compact multi-factor composites
- candidate identity remains inspectable in research, promotion, portfolio, and experiment artifacts via `signal_family`, `signal_variant`, `candidate_id`, and `candidate_name`

The richer signal layer stays intentionally bounded and auditable:

- multi-horizon price structure: return structure, trend slope/persistence, breakout distance/percentile, reversal intensity
- relative context: benchmark-relative return, cross-sectional return rank, cross-sectional relative-strength rank
- volatility/dispersion context: realized volatility, vol-adjusted return, cross-sectional volatility rank, market dispersion
- liquidity/flow context: dollar volume, volume and dollar-volume ratios, bounded flow confirmations
- promotion diagnostics now include `signal_family_summary.csv` so family survival is visible before portfolio export

`composite_v1` is the next recommended experiment preset after candidate-grid expansion because it improves feature quality without changing the canonical path:

- `data refresh-research-inputs`
- `research alpha`
- `research promote`
- `strategy-portfolio build/export`
- `paper/live daily readiness`

Run a lightweight family comparison with:

```bash
python -m trading_platform.diagnostics.signal_family_comparison --output-root artifacts/diagnostics/signal_family_comparison --base-config configs/orchestration_signal_promotion_test.yaml
```

That diagnostic writes:

- `signal_family_comparison.json`
- `signal_family_comparison.md`
- per-family repeated promotion-frequency artifacts under `artifacts/diagnostics/signal_family_comparison/<signal_family>/`

Use it to compare which families actually produce:

- promoted strategies
- family diversity in promoted and exported portfolios
- portfolio and paper-stage reach
- better `portfolio_sharpe` distributions

Current guidance:

- keep `momentum` as the default baseline unless a family comparison run shows a clear win
- use `short_horizon_mean_reversion` or `volatility_adjusted_reversal` when you want explicit reversal families instead of adding more trend variants
- use `cross_sectional_momentum` as a direct relative-strength alias when you want clearer family naming in portfolio outputs
- use `breakout_continuation` when you want an explicit trend-continuation family instead of pure medium-horizon momentum
- use `benchmark_relative_rotation` when you want benchmark-relative leadership to compete directly in promotion and portfolio construction
- use `regime_conditioned_momentum` when you want a simple family that should interact naturally with conditional promotion and risk-on/risk-off context
- use `volatility_dispersion_selection` when you want leadership filtered by volatility and dispersion state rather than raw momentum alone
- use `sector_relative_momentum` when you want within-group leadership to compete against broad market-relative families
- use `liquidity_flow_tilt` when you want volume and dollar-flow confirmation to compete with price-led families
- treat the new families as equity-only experimental paths for controlled follow-up testing

## Strategy Portfolio Weighting Modes

Promoted strategies can now be combined with a few transparent rule-based weighting modes in the strategy-portfolio policy:

- `equal_weight`
- `metric_weighted`
- `capped_metric_weighted`
- `inverse_count_by_signal_family`
- `score_then_cap`

Backward-compatible aliases still load:

- `equal`
- `metric_proportional`

When to use them:

- `equal_weight`: safest baseline for sparse promoted sets
- `metric_weighted`: simple score tilt when you want stronger preference for the top-ranked strategies
- `capped_metric_weighted`: preferred experimental mode when you want score sensitivity with less concentration
- `inverse_count_by_signal_family`: better family balance when the promoted set clusters in one family
- `score_then_cap`: rank-aware weighting that is less sensitive to raw metric magnitude

Run a lightweight weighting comparison with:

```bash
python -m trading_platform.diagnostics.strategy_weighting_comparison --output-root artifacts/diagnostics/strategy_weighting_comparison
```

That writes:

- `strategy_weighting_comparison.json`
- `strategy_weighting_comparison.md`

Use it to compare:

- selected strategy count
- assigned weights
- effective strategy concentration
- family concentration
- run-config export readiness

Current weighting guidance:

- keep `equal_weight` as the simplest operator baseline
- use `inverse_count_by_signal_family` as the next experimental default when promoted strategies span multiple signal families
- use `capped_metric_weighted` when you want some score sensitivity without the concentration of fully `metric_weighted` allocations

### Live dry-run

- `live_dry_run_summary.json`
- `live_dry_run_summary.md`
- `live_dry_run_health_checks.csv`
- `live_dry_run_target_positions.csv`
- `live_dry_run_current_positions.csv`
- `live_dry_run_proposed_orders.csv`
- `live_dry_run_reconciliation.csv`

### Scheduled live dry-run

- `live_run_summary.csv`
- `live_run_summary_latest.json`
- `live_run_summary_latest.md`
- `live_health_checks.csv`
- `live_proposed_orders_history.csv`
- `live_reconciliation_history.csv`

## Dashboard

The dashboard is intentionally a lightweight internal trading terminal:

- server-rendered HTML
- local-first multi-page trading workspace
- read-only with a hybrid read path
- no SPA framework or client-side trading logic
- centered on trade explainability and drill-down analysis

Read-path policy:

- ops, run history, recent trade decisions, and DB-linked trade lineage prefer PostgreSQL when DB metadata is enabled and populated
- those DB-backed list/detail paths now support thin filtering and offset/limit pagination
- the HTML workspace pages now use the same normalized paged payloads as the JSON endpoints, with URL-driven filter state, active filter chips, and previous/next pagination controls
- charts, large signal history, heavy research tables, and other large payloads remain artifact-driven
- if a DB-backed page cannot find the needed rows, it falls back to the artifact-derived payload for the same page
- dashboard payloads now carry a lightweight `source` label such as `db`, `artifact`, or `hybrid` for testing and operator debugging

Run locally:

```bash
trading-cli dashboard serve --artifacts-root artifacts --host 127.0.0.1 --port 8000
```

Build static dashboard data:

```bash
trading-cli dashboard build-static-data --artifacts-root artifacts --output-dir artifacts/dashboard_data
```

Key workspace pages:

```text
http://127.0.0.1:8000/
http://127.0.0.1:8000/trades
http://127.0.0.1:8000/trades/<TRADE_ID>
http://127.0.0.1:8000/strategies
http://127.0.0.1:8000/strategies/<STRATEGY_ID>
http://127.0.0.1:8000/portfolio
http://127.0.0.1:8000/ops
http://127.0.0.1:8000/symbols/AAPL
```

What each page is for:

- `/`
  top-level command center with equity/performance snapshot, recent trades, open positions, strategy pulse, and quick ops awareness
- `/trades`
  blotter for recent open and closed trades with direct links into individual trade detail pages and server-rendered filter/pagination controls
- `/runs`
  normalized portfolio/research run history with server-rendered filters, pagination, and direct run-detail drill-down
- `/trades/<TRADE_ID>`
  centerpiece trade intelligence view showing summary, signal evidence, decision provenance, portfolio context, execution review, outcome review, and related metadata
- `/strategies`
  strategy registry plus recent promotion history with server-rendered promotion filters and pagination
- `/strategies/<STRATEGY_ID>`
  strategy-linked trade history and run/source comparisons
- `/portfolio`
  open positions, exposure, recent activity, contributors/detractors, and allocation context
- `/ops`
  run health, latest stages, live risk checks, execution diagnostics, and orchestration history
- `/symbols/<SYMBOL>`
  symbol-centric trade and provenance inspection across available artifacts

Chart payloads are exposed as stable read-only JSON:

```text
GET /api/chart/AAPL?timeframe=1d&lookback=200
GET /api/chart/AAPL?timeframe=1d&lookback=200&source=research&run_id=sample_run&mode=paper
GET /api/trades/AAPL?source=paper_trading&run_id=2026-03-22T00-00-00+00-00
GET /api/signals/AAPL?lookback=200&source=research&run_id=sample_run
GET /api/trades-blotter
GET /api/ops
GET /api/runs
GET /api/runs/<RUN_ID>
GET /api/discovery/overview
GET /api/discovery/recent-trades
GET /api/discovery/recent-symbols
```

Existing endpoints stay intentionally thin. The server-rendered workspace now uses the same hybrid service layer behind those payloads instead of introducing a broad new REST surface.

Internally, the supported read path is now:

- DB query services for normalized run/decision/execution history when available
- one hybrid dashboard service that composes DB-backed reads with artifact fallback
- one shared page-state helper for filtered/paginated HTML list views
- route handlers that stay thin and avoid page-local query logic

HTML workspace pages that now accept query params directly:

- `/runs`
  `status`, `run_kind`, `run_type`, `mode`, `strategy`, `date_from`, `date_to`, `limit`, `offset`
- `/trades`
  `symbol`, `strategy`, `status`, `run_id`, `date_from`, `date_to`, `limit`, `offset`
- `/ops`
  `status`, `activity_type`, `date_from`, `date_to`, `limit`, `offset`
- `/strategies`
  `strategy`, `decision`, `status`, `date_from`, `date_to`, `limit`, `offset`

Those HTML routes are intentionally URL-driven:

- filter controls submit via standard query params
- active filter chips are derived from the same normalized page state
- previous/next pagination links preserve the active filters
- run/trade detail links carry a lightweight `back_to` query param so list-state round-trips stay intact without a client-side router

Dashboard pages that now prefer DB-backed reads when available:

- `/ops`
  recent portfolio/research runs, recent failures, recent promotions, and recent DB-backed activity
- `/trades`
  recent portfolio decisions from `PortfolioDecision` plus linked candidate context where available, with filter/pagination support
- `/trades/<TRADE_ID>`
  DB-backed decision lineage, candidate evaluations, universe-filter facts, linked run artifacts, and linked order/fill lifecycle when the trade id maps to a DB decision id
- `/runs/<RUN_ID>` and `/api/runs/<RUN_ID>`
  normalized run metadata, linked artifacts, linked candidate/trade decisions, and linked promotions when the control-plane has those relationships
- `/api/runs`, `/api/runs/latest`, `/api/trades-blotter`, `/api/trade/<TRADE_ID>`, `/api/ops`, and `/api/strategies`
  same hybrid preference rules as the HTML pages

Read paths that still primarily rely on artifacts:

- chart bars, indicator overlays, and signal history
- explicit trade ledgers and reconstructed trade history when no DB decision rows exist
- portfolio equity curves, drawdown history, and execution diagnostics derived from CSV artifacts
- research diagnostics, walk-forward outputs, large leaderboards, and image/report artifacts

The dashboard chart path stays artifact-driven:

- bars and indicator overlays come from `data/features/<SYMBOL>.parquet`
- signal markers come from matching `artifacts/research/.../<SYMBOL>_*_signals.csv`
- trade history prefers an explicit ledger artifact such as `paper_trades.csv` or another `*_trades.csv` when present
- if no trade ledger exists, the dashboard reconstructs trades from fills as a fallback
- orders, fills, and positions come from matching paper/live CSV artifacts such as `paper_fills.csv`, `paper_orders.csv`, `paper_positions.csv`, and `live_dry_run_current_positions.csv`
- optional decision provenance can come from lightweight artifacts such as `decision_provenance.csv`, `selection_decisions.csv`, `portfolio_selection.csv`, `order_intents.csv`, or matching `.json` variants

Trade detail lineage is assembled opportunistically from the artifacts that exist today:

- market data and indicators from feature parquet files
- signal labels/scores from matching research signal CSVs
- ranking, target-weight, and selection context from optional provenance artifacts
- trade state from explicit trade ledgers
- order/fill execution review from paper/live order and fill CSVs
- current portfolio context from latest position artifacts

If any layer is missing, the dashboard keeps the section visible but explicit about the missing evidence instead of inventing it.

Expected trade ledger columns are intentionally lightweight:

- `symbol`
- `trade_id`
- `side`
- `qty`
- `entry_ts`
- `entry_price`
- `exit_ts`
- `exit_price`
- `realized_pnl`
- `status`

Optional decision provenance fields supported by the dashboard:

- `symbol`
- `trade_id`
- `strategy_id`
- `timestamp` or `ts`
- `signal_type`
- `signal_value`
- `ranking_score`
- `universe_rank`
- `selection_included` or `selection_status`
- `exclusion_reason`
- `target_weight`
- `sizing_rationale`
- `constraint_hits`
- `order_intent_summary`
- `source`
- `run_id`
- `mode`

The symbol page renders:

- a lightweight price chart with optional candlestick rendering when OHLC bars are available
- signal markers distinct from fill markers
- current position summary
- source/run selector pills when multiple matching artifacts are available
- lookback shortcuts, lightweight overlay toggles, and hover/readout details for chart elements
- a compact decision provenance panel when optional provenance artifacts exist
- a lightweight related-source comparison table when multiple matching sources/runs exist
- explicit trade history when a ledger exists, otherwise reconstructed history from fills

The trade detail page renders:

- trade summary with timestamps, side, quantity, status, and pnl
- "Why This Trade Happened" with nearby signal, rank, target weight, and constraint hits when available
- portfolio context showing current position state and selection metadata
- execution review using discovered orders and fills in the trade window
- outcome review including realized/unrealized pnl and lifecycle timeline
- related strategy/run metadata and nearby trades in the same symbol

Minimal artifact assumptions for the upgraded workspace:

- no new database is required
- existing `paper_trades.csv`, `paper_orders.csv`, `paper_fills.csv`, `paper_positions.csv`, `*_signals.csv`, and provenance CSV/JSON files are reused
- the blotter and trade detail pages enrich rows by joining those artifacts on `symbol`, `trade_id`, and nearby timestamps
- static exports now include `trades_blotter.json` and `ops.json` in addition to the previous dashboard payloads

When DB metadata is enabled, the dashboard additionally uses the control-plane tables for:

- recent `ResearchRun` and `PortfolioRun` history
- linked artifact registry lookups
- `PortfolioDecision` and `CandidateEvaluation` history
- `UniverseFilterResult` context for candidate eligibility
- linked `Order`, `OrderEvent`, and `Fill` metadata when a portfolio decision has direct execution linkage
- recent promotion lineage from `PromotionDecision` and `PromotedStrategy`

Supported DB-backed list filters are intentionally small and explicit:

- runs:
  `status`, `run_type`, `mode`, `strategy`, `date_from`, `date_to`, `limit`, `offset`
- trades:
  `symbol`, `strategy`, `status` or `decision_status`, `run_id`, `date_from`, `date_to`, `limit`, `offset`
- ops:
  `status`, `activity_type`, `date_from`, `date_to`, `limit`, `offset`
- promotion history:
  `strategy`, `decision`, `status`, `date_from`, `date_to`, `limit`, `offset`

List payloads now include stable pagination metadata:

- `total_count`
- `limit`
- `offset`
- `has_more`
- `source`

Current DB-backed detail coverage:

- run detail:
  normalized run summary, linked artifacts, linked decisions/candidates for portfolio runs, and linked promotions for research runs
- trade detail:
  normalized decision lineage, candidate evaluations, universe filter results, signal contributions, and execution linkage when present
- strategies/research:
  recent promotion history from normalized promotion tables, while heavier research diagnostics remain artifact-backed

The HTML workspace and JSON endpoints intentionally share the same read-path contract:

- the route parses query params into a small page-state object
- the page-state is converted into service/query filters
- the hybrid service prefers DB-backed rows when available
- the rendered page reuses the same paged payload metadata for filter chips, pagination, and detail drill-down links
- if the DB path is disabled or empty, the page still renders from the artifact-backed payload with the same URL state shape

Chart API query params:

- `timeframe`
- `lookback`
- `source`
- `run_id`
- `mode`

When selectors are omitted, the dashboard preserves the previous behavior and uses the latest matching artifact it can find.

Optional run metadata manifest:

- file name: `run_metadata.json`
- location: in an artifact directory or one of its parent run directories
- expected lightweight fields:
  - `run_id`
  - `source`
  - `mode`
  - `strategy_id`
  - `timeframe`
  - `lookback`
  - `artifact_group`

When present, the dashboard prefers `run_metadata.json` over directory-name inference for source/run/strategy context. If absent, it falls back to the existing path-based heuristics.

## Promotion Viability Diagnostic

To answer whether research can currently reach promotion and downstream portfolio or paper stages, use the diagnostic-only orchestration profile:

- config: `configs/orchestration_signal_promotion_test.yaml`
- recorded diagnostic artifact: `artifacts/diagnostics/promotion_viability/signal_promotion_diagnostic.md`

The intended rerun path is:

1. create the tiny deterministic feature fixture under `artifacts/diagnostics/promotion_viability/features`
2. run `research alpha`
3. run `strategy-validation build --policy-config configs/strategy_validation_experiment.yaml`
4. run `research promote --policy-config configs/promotion_experiment.yaml`
5. run `ops orchestrate run --config configs/orchestration_signal_promotion_test.yaml`

This diagnostic is explicitly for viability testing. It keeps the main production-style configs intact and records the funnel counts, bottleneck assessment, exact commands used, and the before-versus-after outcome of the manifest normalization fix.

Common failure mode to check first:

- if `portfolio_metrics.csv` contains `portfolio_sharpe` but `research_run.json` shows `top_metrics.portfolio_sharpe: null`, promotion-candidate generation will reject the run even when validation passes
- if leaderboard rows write `rejection_reason=none`, that sentinel should be normalized to null before promotion readiness is evaluated

The promotion-viability diagnostic artifact records both the failing and fixed outcomes so this can be used as a quick regression check after research-registry changes.

## Conditional Research And Promotion

Conditional research is an additive layer on top of the existing broad research registry and promotion flow.

The intent is to answer:

- where a signal works better than its unconditional baseline
- whether the improvement is large enough to matter
- whether the supporting sample is large enough to trust
- whether the promoted strategy should only be active when that condition is true

Supported condition types in the current first pass:

- `regime`
  driven from existing `signal_performance_by_regime.csv` alpha-research artifacts
- `sub_universe`
  now emitted directly by the alpha-research runner as `signal_performance_by_sub_universe.csv`
- `benchmark_context`
  now emitted directly by the alpha-research runner as `signal_performance_by_benchmark_context.csv`

Alpha-research condition-slice artifacts:

- `signal_performance_by_regime.csv`
  regime-sliced signal performance using the existing regime labels
- `signal_performance_by_sub_universe.csv`
  signal performance by `sub_universe_id` when the research feature panels carry explicit sub-universe membership columns such as `sub_universe_id`, `sub_universe`, `sub_universe_label`, or boolean `sub_universe_*` flags
- `signal_performance_by_benchmark_context.csv`
  signal performance by benchmark-relative context label; the runner prefers explicit `benchmark_context_label` or `benchmark_context` columns and otherwise derives labels from the existing equity-context features `market_return_<lookback>`, `relative_return_<lookback>`, and `breadth_impulse_<lookback>`
- `context_features/<SYMBOL>.parquet`
  per-run context-enriched research panels with persisted explicit labels such as `sub_universe_id`, `sub_universe_label`, and lookback-specific `benchmark_context_label_<lookback>` columns
- `research_context_coverage.json`
  lightweight coverage summary showing whether the run used explicit upstream labels, metadata-sidecar labels, or derived benchmark-context fallbacks

Standard metadata sidecar workflow:

- maintained universe/enrichment workflows now publish lightweight research sidecars into the standard sibling metadata directory:
  `data/metadata/`
- the normal paper-trading and live-preview artifact writers refresh that directory whenever they emit universe provenance artifacts
- the canonical sidecar set is:
  - `data/metadata/sub_universe_snapshot.csv`
  - `data/metadata/universe_enrichment.csv`
  - `data/metadata/research_metadata_sidecar_summary.json`
- these sidecars are generated from the same maintained universe provenance bundle used for paper/live explainability; they are not a separate source of truth
- alpha research keeps the same discovery rule as before:
  it looks for a sibling `metadata/` directory next to the feature directory, which means the default `data/features` layout naturally resolves to `data/metadata`

Dedicated research-input refresh command:

- use `trading-cli data refresh-research-inputs --symbols ...` or `--universe ...` to rebuild research-ready feature files and metadata sidecars together in one deterministic step
- the same command also supports a versioned JSON/YAML spec via `--config`
- default locations:
  - features: `data/features/`
  - metadata: `data/metadata/`
  - normalized inputs: `data/normalized/`
- the command reuses the standard feature builder plus the maintained universe-provenance/enrichment writer
- it writes:
  - refreshed `data/features/<SYMBOL>.parquet`
  - refreshed `data/metadata/sub_universe_snapshot.csv`
  - refreshed `data/metadata/universe_enrichment.csv`
  - refreshed `data/metadata/research_metadata_sidecar_summary.json`
  - `data/metadata/research_input_refresh_summary.json`
  - `data/metadata/research_input_bundle_manifest.json`
- optional failure rows are written to `data/metadata/research_input_refresh_failures.csv` when some symbols fail feature generation but others succeed
- this command is now the primary intended way to prepare research inputs; paper/live still refresh metadata opportunistically but are no longer the main intended publisher path
- config-driven runs use the same orchestration service and write the same outputs as flag-driven runs; the config path only changes how inputs are declared

Config-driven refresh spec:

- example config: `configs/research_input_refresh.yaml`
- supported top-level fields:
  - `symbols` or `universe`
  - `feature_groups`
  - `sub_universe_id`
  - `feature_dir`
  - `metadata_dir`
  - `normalized_dir`
  - `reference_data_root`
  - `universe_membership_path`
  - `taxonomy_snapshot_path`
  - `benchmark_mapping_path`
  - `market_regime_path`
  - `group_map_path`
  - `benchmark`
  - `failure_policy`
- the loader also supports nested sections for readability:
  - `selection`
  - `outputs`
  - `reference_data`
  - `failure_handling`
- `failure_policy: partial_success` preserves the current behavior
- `failure_policy: fail` still writes refresh summaries and failure rows, but marks the overall refresh status as failed when any symbol build fails

Example:

```bash
trading-cli data refresh-research-inputs \
  --universe nasdaq100 \
  --sub-universe-id liquid_trend_candidates \
  --reference-data-root artifacts/reference_data/v1
```

Config example:

```bash
trading-cli data refresh-research-inputs \
  --config configs/research_input_refresh.yaml
```

Generation and fallback rules:

- unconditional research still runs exactly as before
- the alpha runner emits the new sub-universe and benchmark-context artifacts on every run so downstream conditional research sees stable paths
- the runner now also writes context-enriched symbol panels under `context_features/` so the label state used during research is persisted and auditable
- if a sibling `metadata/` directory exists next to the feature directory, the runner automatically looks for `sub_universe_snapshot.csv` or `universe_enrichment.csv` and persists those explicit sub-universe labels into the context feature panels before evaluation
- with the default project layout, those sidecars are now refreshed automatically in `data/metadata` by the standard paper/live universe-provenance workflow
- benchmark-context labels are now persisted upstream into the run-local context panels as explicit `benchmark_context_label_<lookback>` columns whenever the required equity-context inputs exist
- downstream slicing prefers those explicit lookback-specific label columns first and only falls back to on-the-fly derivation when no explicit labels are present
- when sub-universe metadata is missing, `signal_performance_by_sub_universe.csv` is written with the expected columns but no rows
- when benchmark context is unavailable and equity-context features are not present, `signal_performance_by_benchmark_context.csv` is written with the expected columns but no rows
- derived benchmark-context labels are explicit and auditable rather than inferred later by the promotion layer
- `research_run.json` and the run manifest include artifact-path references for the new slice outputs so conditional research and promotion can discover them automatically

Condition handling rules:

- unconditional promotion remains the default
- conditional slices are compared against the unconditional baseline using stored metrics
- slices with missing metrics or small sample sizes are kept explicit and rejected rather than overinterpreted
- conditional promotion is only considered when `enable_conditional_variants=true` in the promotion policy

Condition-aware research artifacts:

- `conditional_signal_performance.csv`
  flattened condition-level metrics and baseline comparisons
- `conditional_signal_performance.json`
  JSON view of the same condition rows
- `conditional_research_summary.json`
  condition coverage summary, best slice, and promotion-candidate summaries
- `conditional_promotion_candidates.csv`
  promotion-ready conditional slices with eligibility, improvement, and rejection reasons

Condition-aware manifest metadata:

- each `research_run.json` now includes a `conditional_research` section
- that section records:
  - available vs unavailable condition types
  - best condition summary
  - promotion-candidate rows
  - emitted conditional artifact paths

Condition-aware promotion behavior:

- `research promote` still supports broad unconditional promotion exactly as before
- when the promotion policy enables conditional variants, the promotion pipeline can emit a conditional preset variant instead of the unconditional baseline for that research run
- promoted rows now record:
  - `promotion_variant`
  - `condition_id`
  - `condition_type`
  - `conditional_promotion_summary`
  - `activation_conditions`

Those activation conditions are also carried into the generated preset payload and the promoted-strategy index so later paper/live portfolio logic can explain why a strategy is active or inactive under a given context.

Promotion policy fields for conditional variants:

- `enable_conditional_variants`
- `emit_conditional_variants_alongside_baseline`
- `conditional_variant_allowance`
- `conditional_variant_score_bonus`
- `allowed_condition_types`
- `min_condition_sample_size`
- `min_condition_improvement`
- `compare_condition_to_unconditional`
- `max_strategies_per_family`
- `min_families_if_available`

Strategy-portfolio diversity and concentration controls:

- `max_strategies_per_signal_family`
- `min_families_if_available`
- `allow_conditional_variant_siblings`
- `conditional_variant_score_bonus`
- `weighting_smoothing_power`
- `max_weight_per_strategy`

Current limitations:

- regime is the strongest built-in condition source today because the alpha-research path already emits regime performance artifacts
- sub-universe and benchmark-relative conditional promotion now have first-class alpha-runner artifacts, but explicit upstream label coverage still depends on the maintained feature and metadata refresh path
- this phase carries activation metadata forward; it does not yet fully gate live or paper trading on those conditions

For repeated lightweight frequency checks, run:

```bash
python -m trading_platform.diagnostics.promotion_frequency --output-root artifacts/diagnostics/promotion_frequency --base-config configs/orchestration_signal_promotion_test.yaml
```

That diagnostic reuses the same small promotion path across several deterministic mini-runs and writes:

- `artifacts/diagnostics/promotion_frequency/signal_promotion_frequency.json`
- `artifacts/diagnostics/promotion_frequency/signal_promotion_frequency.csv`
- `artifacts/diagnostics/promotion_frequency/signal_promotion_frequency.md`

For a baseline-versus-expanded equity-only feature comparison, run:

```bash
python -m trading_platform.diagnostics.equity_feature_expansion --output-root artifacts/diagnostics/equity_feature_expansion --base-config configs/orchestration_signal_promotion_test.yaml
```

That comparison keeps the baseline unchanged by default and only enables the additive equity-context path for the expanded leg:

- `signal_family=equity_context_momentum`
- `equity_context_enabled=true`
- `equity_context_include_volume=false` in the default deterministic fixture comparison

The current equity-only context features now support richer but still bounded signal composition:

- benchmark-relative return context via `relative_return_<lookback>` plus `cross_sectional_relative_rank_<lookback>`
- realized-volatility context via `realized_vol_<lookback>`, `vol_adjusted_return_<lookback>`, and `cross_sectional_vol_rank_<lookback>`
- trend structure via `trend_slope_<lookback>`, `trend_persistence_<lookback>`, `breakout_distance_<lookback>`, and `breakout_percentile_<lookback>`
- simple breadth and benchmark regime context via `breadth_positive_<lookback>`, `breadth_impulse_<lookback>`, and `market_trend_strength_<lookback>`
- optional liquidity confirmation via `volume_ratio_<lookback>`, `dollar_volume_ratio_<lookback>`, and `flow_confirmation_<lookback>` when volume exists in the feature inputs

Broader data domains were intentionally deferred here:

- no macro features
- no derivatives features
- no FX features
- no crypto features

For experiment campaigns, the intended confidence ladder is:

- fast: debug only
- medium: default selection
- full: confirmation
- repeated medium campaigns: preferred default-setting evidence when you want more confidence without paying the full-campaign cost

The medium experiment campaign configs now use repeated runs per variant so recommendation confidence can move past single-run low-confidence outcomes.

## Operating Baseline

The current recommended recurring paper-trading baseline is:

- `regime: off`
- `adaptive allocation: off`
- `governance policy: loose`

Use the dedicated operating config:

- `configs/orchestration_operating_baseline.yaml`

One baseline orchestration cycle:

```bash
trading-cli ops orchestrate run --config configs/orchestration_operating_baseline.yaml
```

Inspect the latest baseline run:

```bash
trading-cli ops orchestrate show-run --run artifacts/orchestration_runs_operating_baseline/operating_baseline/<RUN_ID>
```

Rebuild system-evaluation history for the baseline run root:

```bash
trading-cli ops system-eval build --runs-root artifacts/orchestration_runs_operating_baseline/operating_baseline --output-dir artifacts/orchestration_runs_operating_baseline/system_eval_history
```

Once you have multiple baseline runs, compare recent cohorts:

```bash
trading-cli ops system-eval compare --history artifacts/orchestration_runs_operating_baseline/system_eval_history --output-dir artifacts/orchestration_runs_operating_baseline/system_eval_compare --latest-count 3 --previous-count 3
```

Current baseline guidance:

- use the operating baseline config for recurring paper-trading cycles
- keep experiment configs for campaign comparison, not routine operation
- keep the equity-context research expansion out of the default operating path for now because it did not improve the richer ablation comparison

### Running The Operating Baseline Daily On A Cloud Instance

Recommended deployment model:

- one small VM or instance
- one checked-out repo with a local virtualenv
- cron on Linux or Task Scheduler on Windows
- file-based artifacts under `artifacts/`, no database required

Setup:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
```

Daily run entrypoints:

- Linux: `scripts/run_operating_baseline_daily.sh`
- Windows PowerShell: `scripts/run_operating_baseline_daily.ps1`

Those wrappers:

1. activate the local virtualenv when present
2. run `python -m trading_platform.system.operating_baseline_daily --config configs/orchestration_operating_baseline.yaml --summary-dir artifacts/operating_baseline_daily --alerts-config configs/alerts.yaml`
3. append console output to the daily log under `artifacts/operating_baseline_daily/logs/`

Run locally with:

```bash
bash scripts/run_operating_baseline_daily.sh
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_operating_baseline_daily.ps1
```

Both wrappers pass through extra arguments to the Python entrypoint, so you can temporarily override the alert config for a local no-send validation run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_operating_baseline_daily.ps1 --alerts-config artifacts/operating_baseline_daily/alerts_local_validation.yaml
```

```bash
bash scripts/run_operating_baseline_daily.sh --alerts-config artifacts/operating_baseline_daily/alerts_local_validation.yaml
```

Before real email sending works, update the placeholder values in `configs/alerts.yaml`:

- `smtp_host`: your real SMTP host
- `smtp_port`: your provider port, usually `587` for TLS
- `smtp_username`: your real SMTP username/login
- `email_from`: the approved sender address for that SMTP account
- `email_to`: the real operator inbox or distribution list

Do not put the SMTP password in the file. Keep `smtp_password_env_var: TRADING_PLATFORM_SMTP_PASSWORD` and set the password through the environment instead.

The SMTP password must come from the environment variable referenced by `configs/alerts.yaml`:

```bash
export TRADING_PLATFORM_SMTP_PASSWORD="your-smtp-password"
```

```powershell
$env:TRADING_PLATFORM_SMTP_PASSWORD = "your-smtp-password"
```

Real-email checklist:

1. edit `configs/alerts.yaml` and replace the example SMTP host, username, sender, and recipients
2. set `TRADING_PLATFORM_SMTP_PASSWORD` in the shell, cron environment, or Task Scheduler environment
3. run one local no-send validation first with an override config where `email_enabled: false`
4. run the normal wrapper and confirm `daily_alerts.json` shows a non-null `email_result`

The wrappers run:

1. `configs/orchestration_operating_baseline.yaml`
2. system-eval history refresh under `artifacts/orchestration_runs_operating_baseline/system_eval_history`
3. daily summary and alert refresh under `artifacts/operating_baseline_daily`

Local module entrypoint if you want to wire your own scheduler:

```bash
python -m trading_platform.system.operating_baseline_daily --config configs/orchestration_operating_baseline.yaml --summary-dir artifacts/operating_baseline_daily
```

Enable alerts explicitly by passing the alert policy config:

```bash
python -m trading_platform.system.operating_baseline_daily --config configs/orchestration_operating_baseline.yaml --summary-dir artifacts/operating_baseline_daily --alerts-config configs/alerts.yaml
```

Predictable outputs:

- daily log: `artifacts/operating_baseline_daily/logs/YYYY-MM-DD.log`
- daily summary JSON: `artifacts/operating_baseline_daily/daily_baseline_summary.json`
- daily summary Markdown: `artifacts/operating_baseline_daily/daily_baseline_summary.md`
- daily alerts JSON: `artifacts/operating_baseline_daily/daily_alerts.json`
- daily alerts Markdown: `artifacts/operating_baseline_daily/daily_alerts.md`
- orchestration runs: `artifacts/orchestration_runs_operating_baseline/operating_baseline/<RUN_ID>`
- system-eval history: `artifacts/orchestration_runs_operating_baseline/system_eval_history`

Inspect failures:

- check the daily log first
- inspect the latest orchestration run with `trading-cli ops orchestrate show-run --run <RUN_DIR>`
- inspect the latest evaluation with `trading-cli ops system-eval show --evaluation artifacts/orchestration_runs_operating_baseline/system_eval_history`

Rebuild system-eval history manually:

```bash
trading-cli ops system-eval build --runs-root artifacts/orchestration_runs_operating_baseline/operating_baseline --output-dir artifacts/orchestration_runs_operating_baseline/system_eval_history
```

Optional dashboard refresh:

```bash
python -m trading_platform.system.operating_baseline_daily --config configs/orchestration_operating_baseline.yaml --summary-dir artifacts/operating_baseline_daily --refresh-dashboard-static-data --dashboard-output-dir artifacts/dashboard_data
```

### Alerts

The daily baseline runner supports a small alerting layer through:

- `configs/alerts.yaml`
- SMTP email first
- optional SMS only for high-severity events

Required secret environment variables:

- `TRADING_PLATFORM_SMTP_PASSWORD` for the example config

Operator note:

- `configs/alerts.yaml` in the repo is intentionally an example file
- `smtp.example.com`, `alerts@example.com`, and `ops@example.com` are placeholders and must be changed before real sending works
- the wrappers do not inject credentials; they only read the environment variable named in `smtp_password_env_var`

Alert config fields include:

- `email_enabled`
- `sms_enabled`
- `smtp_host`
- `smtp_port`
- `smtp_username`
- `smtp_password_env_var`
- `email_from`
- `email_to`
- `sms_provider`
- `sms_target`
- `email_min_severity`
- `sms_min_severity`
- `send_daily_success_summary`
- `send_on_failure`
- `send_on_zero_promotions`
- `send_on_monitoring_warnings`
- `send_on_kill_switch_recommendations`

Recommended policy:

- email: enable `info` or `warning` severity so the operator gets one daily summary plus actionable warnings
- SMS: keep disabled by default, or use `critical` only for failures or kill-switch-style events

Current minimal SMS support stays intentionally conservative:

- `sms_provider: stub` for local testing
- `sms_provider: email_gateway` if you want to route critical alerts to carrier/email-to-SMS gateway targets without adding a vendor SDK

If you want to inspect the dashboard on the same instance:

```bash
trading-cli dashboard serve --artifacts-root artifacts --host 127.0.0.1 --port 8000
```

Scheduling guidance:

Linux cron example:

```cron
15 18 * * 1-5 cd /opt/trading_platform && /bin/bash scripts/run_operating_baseline_daily.sh
```

Windows Task Scheduler action:

- Program/script: `powershell.exe`
- Arguments: `-ExecutionPolicy Bypass -File C:\path\to\trading_platform\scripts\run_operating_baseline_daily.ps1`

Common operator note:

- if system evaluation reports `null` for per-run return or sharpe, that usually means the latest paper run only produced one equity observation; use the history-level metrics in `system_eval_history` for recurring baseline tracking

Portfolio and strategy pages:

- `/portfolio` now includes portfolio equity, drawdown, current positions, exposure, and recent order/fill/trade activity when those artifacts exist
- `/` now includes a lightweight discovery index for recent symbols, trades, strategies, and run/source/mode contexts so operators can navigate without guessing routes
- `/strategies/<STRATEGY_ID>` aggregates explicit trade ledgers across symbols and reports closed/open counts, win rate, average win/loss, expectancy, average holding period, cumulative realized pnl, recent symbols traded, and a basic run/source comparison table when multiple ledgers exist
- `/trades/<TRADE_ID>` provides post-trade inspection with source/run context, nearby signals/fills/orders, a focused trade-window chart, a lightweight explain-why panel, decision provenance rows, and an order-lifecycle timeline when artifact context exists
- symbol, portfolio, strategy, and execution views now share a consistent dark terminal-style UI shell with compact metric cards, selector pills, and denser tables
- symbol, portfolio, strategy, and execution pages now surface source/run context and lightweight freshness indicators so operators can see when artifacts may be stale

Explain-why and performance panels:

- symbol and trade views now surface nearby signal labels, scores, indicator snapshots, and regime context when those fields are present in artifacts
- symbol and trade views also surface optional decision provenance fields such as ranking score, universe rank, selection status, target weight, constraint hits, and order intent summaries when those artifacts exist
- portfolio and strategy views now include simple breakdowns such as pnl by symbol, best/worst recent trades, and recent realized pnl by period

Execution diagnostics:

- `/api/execution/diagnostics` computes lightweight signal-to-fill and fill-quality proxies from existing signals, orders, fills, and rejected-order artifacts
- current metrics include signal-to-fill latency, signal-vs-fill price comparison, slippage proxy in bps, filled/canceled/rejected counts, missing-fill counts, and orphan-signal counts when the required artifacts exist

## Migration Notes

The older command surface is preserved where practical, but the intended destination is the grouped CLI above.

Examples:

- `trading-cli decision-memo ...` -> `trading-cli research memo ...`
- `trading-cli paper run-preset-scheduled ...` -> `trading-cli paper schedule ...`
- `trading-cli live run-preset-scheduled ...` -> `trading-cli live schedule ...`
- `trading-cli pipeline run ...` -> `trading-cli ops pipeline run ...`
- `trading-cli registry list ...` -> `trading-cli ops registry list ...`
- `trading-cli monitor latest ...` -> `trading-cli ops monitor latest ...`
- `trading-cli broker health ...` -> `trading-cli ops broker health ...`
- `trading-cli execution simulate ...` -> `trading-cli ops execution simulate ...`

Deprecated aliases print migration messages when they are rewritten.

## Known Limitations

- universes are still code-defined snapshots
- `nasdaq100` remains a current-membership approximation, so historical testing still has survivorship bias
- advanced alpha-lab and orchestration components are still broader than the current validated production path
- artifact schemas are much more consistent than before, but some older legacy outputs still exist in parallel
- candlestick rendering is lightweight SVG rather than a full charting package
- source/run selection depends on artifact path naming, so older layouts without clear run directories may expose weaker source metadata
- decision provenance and order lifecycle are opportunistic views over current artifacts, so missing fields simply omit sections rather than synthesizing unsupported explanations
- discovery views are artifact-driven and currently lean most heavily on explicit trade ledgers, so very sparse artifact trees may show limited symbols or strategies until paper/live/research outputs exist
- strategy detail pages currently depend on explicit trade ledgers for the best aggregates; they do not attempt to reconstruct full cross-symbol strategy ledgers from fills
- execution diagnostics are intentionally lightweight proxies and depend on timestamp alignment between signals, orders, and fills
- symbol chart trades still fall back to fill reconstruction when no explicit trade ledger exists, so partial fills and intraday sequencing remain simplified

## Next Roadmap

Automated alpha generation is the next major step. The practical roadmap is:

1. make alpha candidate generation config-first and schedule-safe
2. standardize candidate, validation, and promotion artifacts around the same summary schema
3. add explicit champion/challenger routing into paper and live dry-run workflows
4. tighten approval policies so generated candidates can graduate with auditable constraints
5. add broader multi-strategy canonical-flow smoke coverage around promoted-strategy portfolio bundles

## Development

Run targeted tests while iterating:

```bash
pytest tests/test_cli_grouping.py tests/test_config_loader.py
```

Canonical supported-path smoke coverage now lives in:

- `tests/test_canonical_workflow_smoke.py`

That test now covers both:

- one-shot supported-path validation from config-driven research-input refresh through alpha research, promotion, strategy-portfolio export, and shared paper/live multi-strategy config readiness
- repeated scheduled-style reuse of the same exported promoted multi-strategy bundle for paper and live dry-run command readiness
- repeated real `schedule_type: daily` pipeline-config reuse against that same exported multi-strategy bundle

Run the full suite:

```bash
pytest
```
