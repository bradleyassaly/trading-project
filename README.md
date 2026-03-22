# Trading Platform

## Project Overview

This repository is an end-to-end research to deploy trading system. It covers:

- market data ingestion and feature generation
- alpha research and walk-forward validation
- preset-driven research vs deploy workflows
- constrained portfolio construction for deployable implementations
- multi-strategy portfolio allocation across approved deploy sleeves
- strategy registry, promotion, and degradation governance
- stateful paper trading with scheduled daily runs
- broker-safe live dry-run previews with reconciliation and health checks
- guarded live broker submission with explicit pre-trade checks, kill switches, and audit artifacts
- a local read-only dashboard for inspecting artifact state across runs, strategies, portfolios, execution, and live readiness

The codebase supports both a legacy strategy workflow and a newer `alpha_lab` workflow. The current validated operational path centers on the versioned Nasdaq-100 cross-sectional momentum presets:

- `xsec_nasdaq100_momentum_v1_research`
- `xsec_nasdaq100_momentum_v1_deploy`

## Fresh Machine Quickstart

From a clean clone, the shortest install-to-dashboard path is:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
trading-cli doctor --artifacts-root artifacts --monitoring-config configs/monitoring.yaml --execution-config configs/execution.yaml --broker-config configs/broker.yaml --dashboard-config configs/dashboard.yaml --output-dir artifacts/system_check
python -m pytest tests/test_end_to_end_smoke.py
trading-cli dashboard serve --artifacts-root artifacts --host 127.0.0.1 --port 8000
```

Windows PowerShell activation:

```powershell
.venv\Scripts\Activate.ps1
```

Notes:

- `python -m pip install -e .[dev]` performs an editable install and exposes the `trading-cli` entry point
- YAML config support is installed as a core dependency because the repo uses YAML heavily for operational configs
- the smoke test is deterministic and CI-safe
- the dashboard is local-only and read-only in v1

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

### Strategy Registry

The strategy registry is the governance layer above research outputs and above the multi-strategy allocator. It tracks which versioned strategies exist, what stage each strategy is in, what artifacts support it, and whether objective rules currently support promotion, continued paper testing, approval, live disablement, or retirement.

### Orchestration Runner

The orchestration runner is the reproducible scheduling layer above data, research, governance, portfolio allocation, paper trading, and live dry-run. It exists to make the daily or weekly operating cycle explicit, auditable, and file-backed instead of relying on ad hoc command ordering.

What it does:

- runs enabled stages in a configured order
- captures per-stage status, timestamps, outputs, and failures
- preserves artifact paths from underlying services
- optionally continues after failures when configured
- writes one timestamped run directory with machine-readable and human-readable summaries

Typical usage:

- `daily`: refresh data, refresh features, evaluate candidates, build the approved multi-strategy book, run paper, run live dry-run, write one operator summary
- `weekly`: run a heavier governance or research cycle before rebuilding the registry-backed portfolio
- `ad_hoc`: replay or test a specific stage combination with the same explicit config model

### Pipeline Config Example

```yaml
run_name: daily_governance
schedule_type: daily
universes:
  - nasdaq100
preset_filters:
  - xsec_nasdaq100_momentum_v1_deploy
registry_path: artifacts/strategy_registry.json
governance_config_path: configs/governance.yaml
monitoring_config_path: configs/monitoring.yaml
notification_config_path: configs/notifications.yaml
multi_strategy_output_path: artifacts/generated_registry_multi_strategy.yaml
paper_state_path: artifacts/paper/multi_strategy_state.json
output_root_dir: artifacts/orchestration
fail_fast: false
continue_on_stage_error: true
max_parallel_jobs: 1
tags: [daily, governance]
notes: Daily registry-backed portfolio refresh.
stages:
  data_refresh: true
  feature_generation: true
  research: false
  promotion_evaluation: true
  registry_mutation: true
  multi_strategy_config_generation: true
  portfolio_allocation: true
  paper_trading: true
  live_dry_run: true
  reporting: true
  monitoring: true
auto_promote_qualifying_candidates: true
registry_include_paper_strategies: false
registry_selection_weighting_scheme: equal
```

Key fields:

- `schedule_type`: `daily`, `weekly`, or `ad_hoc`
- `stages`: explicit booleans for every stage so skipped behavior is visible in config and artifacts
- `fail_fast`: stop on the first failed enabled stage
- `continue_on_stage_error`: continue later enabled stages even after a failure
- `registry_path` / `governance_config_path`: inputs for candidate evaluation and optional registry mutation
- `monitoring_config_path`: thresholds for the final monitoring stage and standalone monitor commands
- `notification_config_path`: optional SMTP/SMS notification settings used after monitoring completes
- `execution_config_path`: optional execution-realism config applied by paper and live dry-run stages
- `multi_strategy_output_path`: generated config artifact that feeds the existing multi-strategy allocator
- `paper_state_path`: persistent state file reused by the paper trading stage
- `max_parallel_jobs`: reserved for future scheduler/parallelization support; currently kept explicit for config stability

### Monitoring And Alerts

The monitoring layer sits after orchestration and evaluates three things independently:

- run health: did the pipeline complete cleanly and write the artifacts it should have written
- strategy health: are registered strategies still behaving acceptably in paper and live-preview diagnostics
- portfolio health: does the combined portfolio remain inside explicit exposure, concentration, turnover, and diversification expectations

Everything is file-based and deterministic. Monitoring reads artifacts that already exist; it does not recompute target portfolios or strategy logic.

Health statuses:

- `healthy`: no warning or critical alerts
- `warning`: one or more warning alerts and no critical alerts
- `critical`: one or more critical alerts

Alert severities:

- `info`: informational audit signal only
- `warning`: degradation or unusual behavior worth review
- `critical`: material problem, missing artifact, or threshold breach

### Monitoring Config Example

```yaml
maximum_failed_stages: 0
stale_artifact_max_age_hours: 24
minimum_approved_strategy_count: 1
minimum_generated_position_count: 2
maximum_gross_exposure: 1.0
maximum_net_exposure: 1.0
maximum_symbol_concentration: 0.20
maximum_turnover: 0.25
maximum_drawdown: 0.20
minimum_rolling_sharpe: 0.50
maximum_benchmark_underperformance: 0.05
maximum_missing_data_incidents: 1
maximum_zero_weight_runs: 0
max_drift_between_sleeve_target_and_final_combined_weight: 0.10
unusual_order_count_change_multiple: 3.0
maximum_rejected_order_ratio: 0.25
maximum_clipped_order_ratio: 0.25
maximum_turnover_after_execution: 0.30
maximum_execution_cost: 250.0
maximum_zero_executable_order_runs: 0
maximum_live_risk_check_failures: 0
maximum_live_submission_failures: 0
maximum_duplicate_order_skip_events: 5
```

Thresholds stay explicit in config so changes to operating policy are reviewable in git.

### Dashboard

The dashboard is a local-first, read-only operator UI that sits on top of the existing artifact tree. It does not compute portfolio logic and it does not place trades. It reads the same JSON and CSV artifacts already written by orchestration, governance, allocation, monitoring, execution, live dry-run, and live submit workflows.

Pages:

- `Overview`: latest run status, monitoring health, alert counts, approved strategy count, generated position count, executable order count, broker health, and quick artifact links
- `Strategies`: strategy registry table, family/status filters, and champion/challenger comparison when available
- `Portfolio`: latest combined portfolio, sleeve weights, top positions, overlap diagnostics, and clipped symbols
- `Execution`: requested vs executable orders, rejection reasons, expected cost, and liquidity diagnostics
- `Live`: latest dry-run summary, latest live submit summary, risk checks, blocked reasons, and duplicate-order skip events
- `Runs`: recent orchestration runs, per-stage statuses, failures, and artifact directories

Key API endpoints:

- `/api/overview`
- `/api/runs`
- `/api/runs/latest`
- `/api/strategies`
- `/api/portfolio/latest`
- `/api/execution/latest`
- `/api/live/latest`
- `/api/alerts/latest`

Dashboard commands:

```bash
trading-cli dashboard serve --artifacts-root artifacts --host 127.0.0.1 --port 8000
trading-cli dashboard build-static-data --artifacts-root artifacts --output-dir artifacts/dashboard_data
```

Notes:

- the dashboard is read-only in v1
- it degrades gracefully when some artifact categories do not exist yet
- it uses filesystem discovery only; there is no database and no background worker

### Operator Quickstart

For a minimal local setup, start with the repo example configs:

- `configs/minimal_local_demo.yaml`
- `configs/pipeline_daily.yaml`
- `configs/monitoring.yaml`
- `configs/notifications.yaml`
- `configs/execution.yaml`
- `configs/broker.yaml`
- `configs/dashboard.yaml`

Recommended first-pass local sequence:

```bash
trading-cli doctor --artifacts-root artifacts --monitoring-config configs/monitoring.yaml --execution-config configs/execution.yaml --broker-config configs/broker.yaml --dashboard-config configs/dashboard.yaml
trading-cli pipeline run --config configs/minimal_local_demo.yaml
trading-cli monitor latest --pipeline-root artifacts/orchestration --config configs/monitoring.yaml --output-dir artifacts/monitoring/latest
trading-cli dashboard serve --artifacts-root artifacts --host 127.0.0.1 --port 8000
```

What this does:

- validates that local config files and key directories are readable
- runs a small registry-backed allocation path
- evaluates the newest pipeline run for health and alerts
- starts the local read-only dashboard on top of the artifact tree

### Daily Operator Workflow

Recommended daily operating loop:

1. Run `trading-cli doctor` before changing configs or enabling any live-related workflow.
2. Run `trading-cli pipeline run-daily --config configs/pipeline_daily.yaml`.
3. Check `artifacts/orchestration/.../run_summary.md` and `artifacts/orchestration/.../monitoring/run_health.md`.
4. Review `artifacts/paper/.../paper_run_summary_latest.md` and `artifacts/live_dry_run/.../live_dry_run_summary.md`.
5. Open the dashboard and inspect the `Overview`, `Execution`, `Live`, and `Runs` pages.
6. If live readiness is acceptable, use validate-only submit before any real submission workflow.

### Weekly Governance Workflow

Recommended weekly governance loop:

1. Refresh research artifacts and walk-forward outputs for candidate strategies.
2. Run `trading-cli registry evaluate-promotion` for new candidates.
3. Run `trading-cli registry evaluate-degradation` for active paper and approved strategies.
4. Promote or demote explicitly with `trading-cli registry promote` and `trading-cli registry demote`.
5. Regenerate the registry-backed multi-strategy config.
6. Re-run allocation, monitoring, and paper/live dry-run checks before changing the active lineup.

### Validate-Only Live Workflow

The safest live path is still validate-only:

```bash
trading-cli live dry-run-multi-strategy --config artifacts/generated_registry_multi_strategy.json --execution-config configs/execution.yaml --broker mock --output-dir artifacts/live_dry_run/multi_strategy
trading-cli live submit-multi-strategy --config artifacts/generated_registry_multi_strategy.json --execution-config configs/execution.yaml --broker-config configs/broker.yaml --validate-only --output-dir artifacts/live_submit/multi_strategy
```

Interpretation:

- `dry-run` shows the reconciled executable order package without any broker submission
- `submit ... --validate-only` runs the same guarded pre-trade checks and broker payload transformation but does not send orders
- use validate-only as the last operator check before enabling live submission

### Alert Response Checklist

When monitoring or notifications surface alerts:

1. Read the latest `run_health.md`, `strategy_alerts.md`, or `portfolio_health.md`.
2. Check whether the issue is a missing artifact, a stale artifact, a threshold breach, or a live safety failure.
3. Confirm whether execution artifacts show rejected or clipped orders that materially changed the target portfolio.
4. If live submission was blocked, inspect `live_risk_checks.json` and `live_submission_summary.json`.
5. If the issue is operational rather than market-driven, fix config or data problems first and rerun the affected stage.

Severity guidance:

- `info`: audit-only, no immediate action required
- `warning`: review before proceeding to live-facing workflows
- `critical`: stop and resolve before paper promotion or live enablement

### Kill Switch And Rollback Guidance

Live safety controls are file-backed and intentionally conservative.

Recommended controls:

- keep `live_trading_enabled: false` in `configs/broker.yaml` until explicitly needed
- require `manual_enable_flag_path` for any real live submission path
- use `global_kill_switch_path` as an immediate stop mechanism
- prefer `--validate-only` for routine checks

Emergency steps:

1. Create the configured kill-switch file.
2. Run `trading-cli broker cancel-all --broker-config configs/broker.yaml`.
3. Disable live trading in the broker config or remove the manual enable flag.
4. Re-run `trading-cli doctor` and `trading-cli monitor latest` before considering re-enable.

### Common Failure Modes

Common operational failure patterns:

- missing registry, monitoring, or broker config files
- stale or incomplete allocation artifacts after a partial pipeline run
- generated portfolios with zero or too few positions
- rejected or clipped execution packages due to liquidity, lot size, or cash constraints
- live submission blocked by kill switch, manual-enable flag absence, monitoring status, or notional caps
- duplicate open-order protection skipping materially identical orders

The intended response is explicit inspection of artifacts, not guessing. Every stage writes human-readable and machine-readable summaries so the operator can trace what was blocked, clipped, rejected, or skipped.

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

## Multi-Strategy Allocation

Multi-strategy allocation is a portfolio layer above individual approved deploy presets. Each preset still owns its own target construction. The allocator treats each preset as a sleeve, scales sleeves by capital weight, nets overlapping symbols, applies portfolio-level caps, and preserves sleeve provenance in auditable artifacts.

Why it exists:

- research diagnostics for combined sleeves instead of isolated single-strategy targets
- paper trading of the actual unified book
- live dry-run of one reconciled broker-order preview for the combined portfolio
- stable sleeve attribution, overlap, and concentration reporting

### Multi-Strategy Config Example

```yaml
gross_leverage_cap: 1.0
net_exposure_cap: 1.0
max_position_weight: 0.15
max_symbol_concentration: 0.20
turnover_cap: 0.25
cash_reserve_pct: 0.05
group_map_path: metadata/symbol_groups.csv
sector_caps:
  - group: Technology
    max_weight: 0.45
  - group: Healthcare
    max_weight: 0.25
sleeves:
  - sleeve_name: core_momentum
    preset_name: xsec_nasdaq100_momentum_v1_deploy
    target_capital_weight: 0.70
    enabled: true
    min_capital_weight: 0.50
    max_capital_weight: 0.80
    rebalance_priority: 1
    tags: [core, deploy]
    notes: Primary validated deploy sleeve.
  - sleeve_name: research_overlay
    preset_name: xsec_nasdaq100_momentum_v1_research
    target_capital_weight: 0.30
    enabled: true
    rebalance_priority: 2
    tags: [satellite]
```

Key config fields:

- `sleeve_name`: artifact-facing sleeve identifier
- `preset_name`: existing preset used to build that sleeve's standalone target portfolio
- `target_capital_weight`: desired sleeve capital share; enabled sleeve weights are normalized if they do not sum to `1.0`
- `enabled`: keep a sleeve in config without including it in the run
- `min_capital_weight` / `max_capital_weight`: optional validation bounds
- `rebalance_priority`, `notes`, `tags`: optional audit metadata
- `gross_leverage_cap`: cap on total absolute final exposure
- `net_exposure_cap`: cap on absolute final net exposure
- `max_position_weight`: cap on final net single-name weight
- `max_symbol_concentration`: cap on gross sleeve overlap allocated to one symbol before final netting
- `sector_caps`: optional group caps applied with the configured symbol-group map
- `turnover_cap`: combined-portfolio turnover diagnostic threshold
- `cash_reserve_pct`: cash fraction preserved by paper/live reconciliation

## Strategy Registry

The strategy registry is an explicit JSON/YAML file that records strategy versions and lifecycle state. It is designed to be audited directly, mutated only by explicit commands, and used as the source for registry-backed multi-strategy portfolio generation.

Lifecycle statuses:

- `research`: raw research output tracked but not yet staged
- `candidate`: version is under review and eligible for further governance evaluation
- `paper`: eligible for paper trading and further observation
- `approved`: eligible for approved registry-backed multi-strategy selection
- `live_disabled`: previously active but blocked from further live promotion
- `retired`: no longer eligible for allocation or promotion

Current deployment stages are tracked separately from status but use the same explicit stage labels so registry-backed selectors can filter by operational state without inferring from hidden logic.

### Example Registry File

```json
{
  "schema_version": 1,
  "updated_at": "2026-03-21T12:00:00Z",
  "entries": [
    {
      "strategy_id": "xsec_nasdaq100_momentum_v1",
      "strategy_name": "Nasdaq-100 Cross-Sectional Momentum",
      "family": "xsec_momentum",
      "version": "v1",
      "preset_name": "xsec_nasdaq100_momentum_v1_deploy",
      "research_artifact_paths": ["artifacts/research/nasdaq100_xsec_v1_research"],
      "created_at": "2026-03-20T18:30:00Z",
      "status": "approved",
      "owner": "research",
      "source": "walkforward_validation",
      "current_deployment_stage": "approved",
      "notes": "Validated deploy overlay.",
      "tags": ["core", "deploy"],
      "universe": "nasdaq100",
      "signal_type": "cross_sectional_momentum",
      "rebalance_frequency": "monthly",
      "benchmark": "equal_weight",
      "risk_profile": "medium",
      "paper_artifact_path": "artifacts/paper/nasdaq100_xsec",
      "live_artifact_path": "artifacts/live_dry_run/nasdaq100_xsec"
    }
  ],
  "audit_log": [
    {
      "timestamp": "2026-03-21T12:00:00Z",
      "strategy_id": "xsec_nasdaq100_momentum_v1",
      "action": "promote",
      "from_status": "paper",
      "to_status": "approved",
      "note": "Promotion criteria passed."
    }
  ]
}
```

### Promotion And Demotion Workflow

1. Research produces stable artifacts for a versioned strategy or preset.
2. Register that version in the strategy registry with explicit artifact paths and status.
3. Run `registry evaluate-promotion` against a governance criteria file to generate an auditable pass/fail report.
4. If the result is acceptable, run `registry promote` to advance the lifecycle and append an audit event.
5. While active in `paper` or `approved`, run `registry evaluate-degradation` against paper/live diagnostics to check for objective breaches.
6. If degradation is material, run `registry demote` to step the strategy down and preserve the transition in the audit log.

### Champion / Challenger

Multiple versions from the same family can coexist in the registry. Family comparison utilities identify the current champion per family, compare challengers against it, and generate:

- `family_comparison.csv`
- `champion_challenger_report.md`

These are written automatically when the registry builds a multi-strategy config artifact.

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
trading-cli paper run-multi-strategy --config configs/multi_strategy.yaml --state-path artifacts/paper/multi_strategy_state.json --output-dir artifacts/paper/multi_strategy
```

### Live Preview

```bash
trading-cli live dry-run --preset xsec_nasdaq100_momentum_v1_deploy --broker mock --output-dir artifacts/live_dry_run/nasdaq100_xsec
trading-cli live run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --broker mock --output-dir artifacts/live_dry_run/nasdaq100_xsec
trading-cli live dry-run-multi-strategy --config configs/multi_strategy.yaml --broker mock --output-dir artifacts/live_dry_run/multi_strategy
```

### Portfolio Allocation Only

```bash
trading-cli portfolio allocate-multi-strategy --config configs/multi_strategy.yaml --output-dir artifacts/portfolio/multi_strategy
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

Multi-strategy paper and allocation runs also write:

- `combined_target_weights.csv`
- `sleeve_target_weights.csv`
- `symbol_overlap_report.csv`
- `allocation_summary.json`
- `allocation_summary.md`
- `sleeve_attribution.csv`
- `portfolio_diagnostics.csv`
- `overlap_matrix.csv`

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

Multi-strategy workflow:

1. Approve and stabilize the underlying deploy presets first.
2. Define a multi-strategy config that references those presets as sleeves.
3. Run `portfolio allocate-multi-strategy` to inspect overlap, concentration, sleeve attribution, and constraint bindings without mutating any state.
4. Run `paper run-multi-strategy` to simulate the unified combined portfolio.
5. Run `live dry-run-multi-strategy` to preview one reconciled order package for the combined portfolio without submitting anything.

Registry-driven workflow:

1. Research validates and versions a strategy/preset.
2. Register the version and attach its research, paper, and live artifact paths.
3. Evaluate promotion thresholds with `registry evaluate-promotion`.
4. Promote explicitly with `registry promote`.
5. Build a multi-strategy config directly from all `approved` entries, or `approved` plus `paper`, with `registry build-multi-strategy-config`.
6. Feed that generated config into `portfolio allocate-multi-strategy`, `paper run-multi-strategy`, or `live dry-run-multi-strategy`.

Pipeline-runner workflow:

1. Define a pipeline config with the exact stages, paths, and failure policy you want.
2. Run `pipeline run`, `pipeline run-daily`, or `pipeline run-weekly`.
3. Inspect the timestamped run directory under the configured output root.
4. Review `run_summary.md` first, then `stage_status.csv`, then any stage-specific artifacts for failures or promotions.
5. Reuse the generated multi-strategy config and allocation artifacts for downstream inspection when needed.

The registry-backed config builder supports reproducible selection filters such as `--universe`, `--family`, `--tag`, `--deployment-stage`, `--max-strategies`, and sleeve weighting via `--weighting-scheme equal|score_weighted`.

### Pipeline Commands

```bash
trading-cli pipeline run --config configs/pipeline.yaml
trading-cli pipeline run-daily --config configs/pipeline_daily.yaml
trading-cli pipeline run-weekly --config configs/pipeline_weekly.yaml
```

### Monitoring Commands

```bash
trading-cli monitor run-health --run-dir artifacts/orchestration/daily_governance/2026-03-21T00-00-00+00-00 --config configs/monitoring.yaml
trading-cli monitor strategy-health --registry artifacts/strategy_registry.json --artifacts-root artifacts --config configs/monitoring.yaml --output-dir artifacts/monitoring/strategy
trading-cli monitor portfolio-health --allocation-dir artifacts/portfolio/multi_strategy --config configs/monitoring.yaml --output-dir artifacts/monitoring/portfolio
trading-cli monitor latest --pipeline-root artifacts/orchestration --config configs/monitoring.yaml --output-dir artifacts/monitoring/latest
trading-cli monitor build-dashboard-data --pipeline-root artifacts/orchestration --output-dir artifacts/monitoring/dashboard
trading-cli monitor notify --alerts artifacts/monitoring/latest/alerts.json --config configs/notifications.yaml
```

Primary monitoring outputs:

- `run_health.json` / `run_health.md`: pipeline-level status, counts, and alerts
- `strategy_health.csv` / `strategy_health.json` / `strategy_alerts.md`: per-strategy metrics and alerts
- `portfolio_health.csv` / `portfolio_health.json` / `portfolio_health.md`: combined-portfolio metrics and alerts
- `run_history.csv`, `strategy_health_history.csv`, `portfolio_health_history.csv`: append-only trend histories for later dashboards

Execution-aware monitoring can also alert on:

- rejected order count above threshold
- liquidity breach count above threshold
- short-availability failures
- live risk-check failures
- duplicate-order skip spikes
- broker-health failures and kill-switch blocks during live submission

### Notifications

The notification layer is intentionally simple. It reads monitoring alerts, filters by `min_severity`, aggregates them into one message, and sends them synchronously.

Supported channels:

- `email`: SMTP via the Python standard library
- `sms`: stub-only for now; intended as a pluggable future hook

Example notification config:

```yaml
smtp_host: smtp.example.com
smtp_port: 587
from_address: alerts@example.com
min_severity: warning
smtp_username: alerts@example.com
smtp_password: ${SMTP_PASSWORD}
smtp_use_tls: true
subject_prefix: Trading Platform
channels:
  - channel_type: email
    recipients:
      - ops@example.com
  - channel_type: sms
    recipients:
      - "+15555550123"
```

Recommended thresholds:

- start with `min_severity: critical` until the monitoring thresholds are stable
- move to `warning` only after the warning stream is low-noise and actionable
- keep SMS at `critical`-only operationally, even if email receives warnings

Example aggregated message:

```text
Trading Platform: critical alerts (2)

info=0 warning=1 critical=1

[warning] portfolio_turnover portfolio:artifacts/portfolio/multi_strategy - turnover_estimate=0.30 exceeds maximum_turnover=0.25
[critical] failed_stages run:daily_governance - failed_stage_count=1 exceeds maximum_failed_stages=0
```

Pipeline integration:

- if `notification_config_path` is set on the pipeline config, the `monitoring` stage sends one aggregated notification after health evaluation
- if no alerts meet the configured minimum severity, nothing is sent
- notification results are written to `monitoring/notification_summary.json`

Behavior:

- `pipeline run` executes exactly what the config declares
- `pipeline run-daily` validates `schedule_type: daily`
- `pipeline run-weekly` validates `schedule_type: weekly`
- every run writes a timestamped artifact directory
- the terminal prints a concise stage-by-stage status summary

If the optional `monitoring` stage is enabled in the pipeline config, the final run summary also includes:

- overall monitoring health status
- alert counts by severity
- critical alert messages when present

### Pipeline Artifacts

Each run writes a directory shaped like:

```text
artifacts/orchestration/<run_name>/<timestamp>/
  run_summary.json
  run_summary.md
  stage_status.csv
  pipeline_config_snapshot.json
  errors.json
  promotion_evaluation/
  registry_mutation/
  portfolio_allocation/
  paper_trading/
  live_dry_run/
```

Core top-level files:

- `run_summary.json`: machine-readable run result with stage records and selected outputs
- `run_summary.md`: operator-facing markdown summary for terminal or git review
- `stage_status.csv`: one row per stage with timestamps, status, duration, inputs, outputs, and error text
- `pipeline_config_snapshot.json`: exact resolved config used for the run
- `errors.json`: emitted only when one or more stages fail

Candidate-promotion batch artifacts:

- `promotion_batch_summary.csv`
- `promoted_strategies.csv`
- `rejected_strategies.csv`

The orchestrator preserves lower-level artifacts from existing services rather than replacing them, so allocation, paper, and live outputs remain inspectable in their native schemas.

When pipeline monitoring is enabled, the run also writes:

- `monitoring/run_health.json`
- `monitoring/run_health.md`
- `monitoring/alerts.json`
- `monitoring/alerts.csv`
- `monitoring/run_history.csv`

### Daily Vs Weekly Guidance

Daily workflow is usually the lighter operational path:

- data refresh
- feature generation
- promotion evaluation
- optional registry mutation
- registry-backed multi-strategy config generation
- allocation
- paper run
- live dry-run

Weekly workflow is usually the heavier governance path:

- broader research refresh
- promotion/degradation review
- registry mutation after review
- rebuilt portfolio composition
- optional paper/live validation afterward

### Operational Safety Notes

- start with `registry_mutation: false` until the evaluation artifacts look correct
- use `continue_on_stage_error: true` only when partial downstream artifacts are operationally useful
- treat `live_dry_run` as preview-only; it does not submit orders
- prefer relative artifact paths rooted in the repo or explicit absolute paths to avoid ambiguous scheduler environments
- check `pipeline_config_snapshot.json` into review when changing automation behavior so scheduling changes stay auditable
- treat `critical` monitoring alerts as blockers until the artifact or threshold breach is explained

## Research Vs Deploy Insight

- `pure_topn` = research truth
- `transition` = deployable portfolio overlay
- turnover cap = smoothing layer between target portfolio and realized portfolio
- realized holdings may exceed `top_n` in `transition` mode

This distinction is deliberate. Research determines whether the signal family is valid; deploy mode measures whether the same family remains usable under implementation constraints.

## Future Work

- guarded live order execution
- intraday support
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
- `governance/`: strategy registry models, persistence, evaluation, and registry-backed portfolio selection
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
- `registry`

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
trading-cli portfolio allocate-multi-strategy --config configs/multi_strategy.yaml --output-dir artifacts/portfolio/multi_strategy
trading-cli portfolio apply-execution-constraints --config configs/execution.yaml --allocation-dir artifacts/portfolio/multi_strategy --output-dir artifacts/execution/multi_strategy
trading-cli execution simulate --config configs/execution.yaml --targets artifacts/execution/requested_orders.csv --output-dir artifacts/execution/simulated
trading-cli paper run --symbols AAPL MSFT NVDA --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --top-n 5 --execution-config configs/execution.yaml --state-path artifacts/paper/paper_state.json --output-dir artifacts/paper
trading-cli paper run --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
trading-cli paper run-multi-strategy --config configs/multi_strategy.yaml --execution-config configs/execution.yaml --state-path artifacts/paper/multi_strategy_state.json --output-dir artifacts/paper/multi_strategy
trading-cli paper run-preset-scheduled --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
trading-cli paper daily --universe magnificent7 --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --state-path artifacts/paper/paper_state.json --output-dir artifacts/paper
trading-cli paper report --account-dir artifacts/paper --output-dir artifacts/paper/report
trading-cli live dry-run --universe magnificent7 --strategy sma_cross --top-n 5 --execution-config configs/execution.yaml --broker mock
trading-cli live dry-run-multi-strategy --config configs/multi_strategy.yaml --execution-config configs/execution.yaml --broker mock --output-dir artifacts/live_dry_run/multi_strategy
trading-cli live submit --universe magnificent7 --strategy sma_cross --execution-config configs/execution.yaml --broker-config configs/broker.yaml --validate-only --output-dir artifacts/live_submit
trading-cli live submit-multi-strategy --config configs/multi_strategy.yaml --execution-config configs/execution.yaml --broker-config configs/broker.yaml --output-dir artifacts/live_submit/multi_strategy
trading-cli broker health --broker-config configs/broker.yaml
trading-cli broker cancel-all --broker-config configs/broker.yaml
trading-cli dashboard serve --artifacts-root artifacts --host 127.0.0.1 --port 8000
trading-cli dashboard build-static-data --artifacts-root artifacts --output-dir artifacts/dashboard_data
trading-cli live validate --universe magnificent7 --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --approval-artifact artifacts/research_refresh/approved_configuration_snapshots/latest_approved_configuration.json --output-dir artifacts/live_execution
trading-cli live execute --universe magnificent7 --signal-source composite --approved-model-state artifacts/alpha_research/approved/approved_model_state.json --approved --output-dir artifacts/live_execution
trading-cli experiments list --tracker-dir artifacts/experiment_tracking --limit 10
trading-cli experiments latest --tracker-dir artifacts/experiment_tracking
trading-cli experiments dashboard --tracker-dir artifacts/experiment_tracking --output-dir artifacts/experiment_tracking --top-metric portfolio_sharpe
trading-cli experiments diff --snapshot-dir artifacts/research_refresh/approved_configuration_snapshots
trading-cli registry list --registry artifacts/strategy_registry.json
trading-cli registry evaluate-promotion --registry artifacts/strategy_registry.json --strategy-id xsec_nasdaq100_momentum_v1 --config configs/governance.yaml --output-dir artifacts/registry_eval/xsec_nasdaq100_momentum_v1
trading-cli registry promote --registry artifacts/strategy_registry.json --strategy-id xsec_nasdaq100_momentum_v1
trading-cli registry evaluate-degradation --registry artifacts/strategy_registry.json --strategy-id xsec_nasdaq100_momentum_v1 --config configs/governance.yaml --output-dir artifacts/registry_eval/xsec_nasdaq100_momentum_v1
trading-cli registry demote --registry artifacts/strategy_registry.json --strategy-id xsec_nasdaq100_momentum_v1
trading-cli registry build-multi-strategy-config --registry artifacts/strategy_registry.json --output-path artifacts/generated_registry_multi_strategy.json --include-paper --weighting-scheme score_weighted
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

## Multi-Strategy Artifacts

`portfolio allocate-multi-strategy`, `paper run-multi-strategy`, and `live dry-run-multi-strategy` all write the same allocation-layer audit package:

- `combined_target_weights.csv`: final symbol-level target weights after sleeve scaling, netting, and portfolio constraints
- `sleeve_target_weights.csv`: per-sleeve symbol targets before final netting, including sleeve provenance
- `symbol_overlap_report.csv`: per-symbol sleeve overlap and long/short conflict report
- `allocation_summary.json`: machine-readable exposure, turnover, normalization, and constraint summary
- `allocation_summary.md`: concise human-readable summary
- `sleeve_attribution.csv`: gross, net, and final matched contribution by sleeve
- `portfolio_diagnostics.csv`: stable metric table for gross/net exposure, overlap concentration, turnover, and effective counts
- `overlap_matrix.csv`: pairwise sleeve overlap matrix

The most important allocation diagnostics are:

- gross and net exposure before and after constraints
- sleeve capital weights before and after normalization
- per-symbol overlap and long/short conflicts
- turnover estimate at the combined-portfolio level
- symbols clipped by position, concentration, group, gross, or net caps
- effective number of sleeves and effective number of positions

## Execution Realism

Execution realism sits between ideal target weights and anything you would actually trade. The goal is to make frictions and practical constraints explicit, deterministic, and auditable rather than to imply broker-grade fill precision.

The execution layer supports:

- reusable target-weight to desired-order conversion from current positions, prices, equity, and reserve cash
- commissions via `per_share`, `bps`, or `flat` models
- slippage proxies via `fixed_bps`, `spread_plus_bps`, or `liquidity_scaled`
- minimum price and minimum average dollar volume tradeability filters
- lot-size rounding, minimum trade-notional filtering, and per-name notional-change caps
- ADV participation caps and configurable partial-fill behavior
- short-selling, short-borrow proxy, and max short-gross checks
- turnover caps, cash-buffer affordability checks, and explicit missing-liquidity handling
- expected fill-price, fee, slippage, participation, and rejection/clipping diagnostics

Example execution config:

```yaml
enabled: true
price_source_assumption: close
commission_model_type: per_share
commission_per_share: 0.005
commission_bps: 0.5
flat_commission_per_order: 0.0
slippage_model_type: liquidity_scaled
fixed_slippage_bps: 1.0
half_spread_bps: 2.0
liquidity_slippage_bps: 5.0
max_participation_of_adv: 0.05
min_average_dollar_volume: 1000000
min_price: 5.0
min_trade_notional: 50.0
lot_size: 1
max_turnover_per_rebalance: 0.5
max_position_notional_change: 25000
allow_shorts: true
enforce_short_borrow_proxy: true
max_short_gross_exposure: 0.30
short_borrow_blocklist: [GME]
partial_fill_behavior: allow_partial
missing_liquidity_behavior: warn_and_clip
stale_market_data_behavior: warn
cash_buffer_bps: 25
tags: [paper, live]
```

Primary execution artifacts:

- `requested_orders.csv`: raw desired orders derived from target deltas before execution constraints
- `executable_orders.csv`: requested versus adjusted executable orders, fill-price proxy, fees, slippage, fill fraction, and provenance
- `rejected_orders.csv`: orders rejected by tradeability, liquidity, short, turnover, or affordability constraints
- `execution_summary.json` / `execution_summary.md`: total counts, turnover before/after constraints, expected aggregate cost, and rejection/clipping totals
- `liquidity_constraints_report.csv`: symbol-level liquidity, participation, stale-data, and borrow diagnostics
- `turnover_summary.csv`: requested versus executed notional and turnover summary
- `symbol_tradeability_report.csv`: per-symbol tradeability checks even when no final executable order survives

Paper and live dry-run accept an optional execution config:

- `paper run --execution-config ...` applies the same execution layer to single-strategy paper workflows
- `paper run-multi-strategy --execution-config ...` filters and clips orders before paper-state application and records estimated cost
- `live dry-run --execution-config ...` previews executable orders rather than idealized target deltas for single-strategy workflows
- `live dry-run-multi-strategy --execution-config ...` previews executable orders rather than idealized target deltas
- `execution_config_path` on the pipeline config forwards the same assumptions into orchestration-driven paper and live dry-run stages

Requested vs executable orders:

- requested orders reflect the ideal portfolio delta implied by target weights and current holdings
- executable orders reflect what remains after tradeability, rounding, participation, turnover, short, and cash checks
- rejected orders are first-class artifacts with human-readable reasons instead of silent drops

Optional research/backtest seam:

- `portfolio backtest` remains backward compatible by default
- when an execution config is supplied through the shared service seam, backtest summaries can use a deterministic transaction-cost estimate derived from the same execution assumptions
- this is intentionally a lightweight friction overlay, not a full intraday fill simulator

Current limitations:

- costs are proxies, not broker-quality transaction-cost analysis
- liquidity checks use only available artifact fields and reject explicitly when required inputs are missing
- `portfolio apply-execution-constraints` is intended as a diagnostic helper and uses simple quantity proxies from allocation artifacts
- price-source assumptions are coarse (`close`, `next_open`, `vwap_proxy`) and do not imply intraday execution precision

## Live Broker Submission

Live submission is a separate guarded path above dry-run. The live submit commands reuse the same target generation, reconciliation, open-order adjustment, and optional execution-realism layer as dry-run, then add hard pre-trade checks before any order can be sent.

Primary safeguards:

- `live_trading_enabled` must be explicitly true in broker config
- optional manual enable flag must exist if `require_manual_enable_flag` is enabled
- global kill switch blocks submission immediately if the configured file exists
- broker health must pass before submission
- expected account id can be enforced
- non-empty orders, max order count, total notional, and per-symbol notional are checked
- projected gross, net, and max position weight are checked
- shorts can be blocked at the live layer even if research or paper supports them
- market data freshness can be enforced
- monitoring status can be required to be `healthy` before submit
- existing open-order policy is explicit: either block, or cancel-all first when configured

Duplicate protection is conservative:

- each outgoing order gets a deterministic `client_order_id`
- if a materially identical open order already exists, the new order is skipped and recorded instead of blindly resubmitted
- optional cancel-all happens only when `cancel_existing_open_orders_before_submit` is explicitly enabled

Example broker config:

```yaml
broker_name: mock
live_trading_enabled: false
require_manual_enable_flag: true
manual_enable_flag_path: flags/live.enable
global_kill_switch_path: flags/live.kill
expected_account_id: mock-account
max_orders_per_run: 10
max_total_notional_per_run: 50000
max_symbol_notional_per_order: 10000
max_gross_exposure: 1.0
max_net_exposure: 1.0
max_position_weight: 0.20
max_position_change_notional: 15000
allowed_order_types: [market]
default_order_type: market
allow_shorts_live: false
cancel_existing_open_orders_before_submit: false
skip_submission_if_existing_open_orders: true
require_fresh_market_data: true
max_market_data_age_seconds: 900
require_clean_monitoring_status: true
allowed_monitoring_statuses: [healthy]
monitoring_status_path: artifacts/monitoring/latest/run_health.json
mock_equity: 100000
mock_cash: 100000
tags: [staged_rollout]
```

Validate-only vs live submit:

- `live submit --validate-only` builds the exact broker order package, runs all hard checks, writes artifacts, and does not submit
- `live submit` only sends orders if every hard check passes
- both modes write the same audit package, so the operator can diff validate-only vs actual submit runs

Primary live submission artifacts:

- `live_risk_checks.json` / `live_risk_checks.md`: per-check pass/fail results and hard-block reasons
- `broker_order_requests.csv`: deterministic broker payloads that would be or were sent
- `broker_order_results.csv`: submission, skip, reject, and cancel results
- `live_submission_summary.json` / `live_submission_summary.md`: aggregate risk and submission summary

Operational rollout guidance:

1. Start with `broker_name: mock` and `live_trading_enabled: false`.
2. Run `live submit --validate-only` repeatedly until the risk checks and payloads are stable.
3. Enable the manual flag and leave the global kill switch path configured before any real submit.
4. Keep `skip_submission_if_existing_open_orders: true` for the first live rollout.
5. Only after repeated clean validate-only runs should `live_trading_enabled` be switched on.

Kill switch behavior:

- if the kill-switch file exists, submission is hard blocked
- the block is written into `live_risk_checks.*` and `live_submission_summary.*`
- `broker cancel-all` is the emergency command to clear outstanding mock or future real-broker orders

Operational checklist before enabling live trading:

- latest monitoring status is acceptable
- broker health passes
- expected account id matches
- execution realism is configured if the strategy depends on liquidity filtering
- validate-only output matches operator expectations
- manual enable flag is present
- kill switch is not active

## Governance Criteria

Promotion criteria are explicit and file-based. The current governance config supports thresholds such as:

- minimum walk-forward folds
- minimum mean test return
- minimum Sharpe
- maximum drawdown
- minimum hit rate
- minimum IC / rank IC
- maximum turnover
- maximum redundancy / correlation
- minimum paper-trading observation window
- minimum trade count

Degradation criteria support rules such as:

- rolling underperformance versus benchmark
- drawdown breach
- excessive turnover
- signal instability
- missing-data failures
- excessive live dry-run warning or fail checks

Evaluation commands write deterministic audit artifacts:

- `promotion_decision.json`
- `promotion_decision.md`
- `strategy_metrics_snapshot.json`
- `degradation_report.json`
- `degradation_report.md`

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

### Developer Workflow

The repo includes a minimal `Makefile` for standardized local commands:

```bash
make install
make lint
make test
make smoke
make doctor
```

Equivalent direct commands:

```bash
python -m pip install -e .[dev]
python -m ruff check src tests --select E9,F63,F7,F82
python -m pytest
python -m pytest tests/test_end_to_end_smoke.py
trading-cli doctor --artifacts-root artifacts --monitoring-config configs/monitoring.yaml --execution-config configs/execution.yaml --broker-config configs/broker.yaml --dashboard-config configs/dashboard.yaml --output-dir artifacts/system_check
```

The lint command is intentionally pragmatic for this pass:

- it checks high-signal correctness failures such as syntax issues and undefined names
- it does not force a broad repo-wide style cleanup during release hardening

### Contributing

Preferred contributor loop:

1. Create or activate a virtual environment.
2. Run `make install`.
3. Make the smallest practical change.
4. Run `make lint`, `make smoke`, and the relevant pytest subset locally.
5. Run `make test` before opening or updating a PR.
6. Keep changes additive and preserve existing CLI/config behavior unless a deliberate migration is required.

### Release Checklist

Before tagging or pushing a release candidate:

1. Run `make install` in a fresh virtual environment.
2. Run `make lint`.
3. Run `make test`.
4. Run `make smoke`.
5. Run `make doctor`.
6. Review example configs under `configs/` and any README command changes.
7. Confirm GitHub Actions passes for both the main CI workflow and the smoke workflow.

### Adding New Research Modules

- Prefer extending `src/trading_platform/research/alpha_lab/` for cross-sectional research functionality.
- Reuse existing artifact-writing patterns and diagnostics tables instead of inventing new one-off formats.
- If a new workflow needs CLI support, add it in `src/trading_platform/cli/parser.py` and implement the command under `src/trading_platform/cli/commands/`.
- Keep file-based outputs simple: CSV, Parquet, and JSON are the project’s current standard.
- Add pytest coverage for new workflows, artifact generation, and empty-data edge cases.
