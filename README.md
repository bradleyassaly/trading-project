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

`data ingest -> data features -> research run -> walkforward -> decision memo -> deploy preset -> paper scheduled -> live dry-run`

The primary validated operational example is:

- `xsec_nasdaq100_momentum_v1_research`
- `xsec_nasdaq100_momentum_v1_deploy`

Those presets represent the current supported Nasdaq-100 cross-sectional momentum path. They are the clearest example of how the platform is meant to be run in production.

## Supported Vs Experimental

### Supported

- grouped CLI centered on `data`, `research`, `portfolio`, `paper`, `live`, `dashboard`, and `ops`
- config-first research runs, walk-forward runs, paper runs, and live dry-runs
- preset-driven research and deploy handoff
- registry-backed deployment config generation
- scheduled paper trading artifacts
- broker-safe live dry-run artifacts
- local dashboard over the artifact tree

### Experimental / Legacy

- `research alpha`
- `research loop`
- `research multi-universe`
- `research multi-universe-report`
- `research refresh`
- `research monitor`
- legacy flat command aliases and older top-level groups preserved for migration
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

## Canonical Workflow

### 1. Data ingest

```bash
trading-cli data ingest --universe nasdaq100 --start 2015-01-01
```

### 2. Feature generation

```bash
trading-cli data features --universe nasdaq100
```

### 3. Research run

Preset-driven:

```bash
trading-cli research run --preset xsec_nasdaq100_momentum_v1_research --output-dir artifacts/research/xsec_nasdaq100_v1
```

Config-driven:

```bash
trading-cli research run --config configs/workflows/research_xsec_nasdaq100.yaml
```

### 4. Walk-forward validation

```bash
trading-cli research walkforward --preset xsec_nasdaq100_momentum_v1_research --output artifacts/walkforward/xsec_nasdaq100_v1.csv
```

Or:

```bash
trading-cli research walkforward --config configs/workflows/walkforward_xsec_nasdaq100.yaml
```

### 5. Decision memo

```bash
trading-cli research memo --preset xsec_nasdaq100_momentum_v1_research --deploy-preset xsec_nasdaq100_momentum_v1_deploy --output-dir artifacts/decision_memos
```

### 6. Deploy preset / build deploy config

For the validated single-preset path, the deploy preset is the handoff:

- `xsec_nasdaq100_momentum_v1_deploy`

For registry-backed portfolio deployment:

```bash
trading-cli ops registry build-deploy-config --registry artifacts/strategy_registry.json --output-path artifacts/generated_registry_multi_strategy.json
```

### 7. Scheduled paper run

```bash
trading-cli paper schedule --preset xsec_nasdaq100_momentum_v1_deploy --state-path artifacts/paper/nasdaq100_xsec_state.json --output-dir artifacts/paper/nasdaq100_xsec
```

Or:

```bash
trading-cli paper run --config configs/workflows/paper_xsec_nasdaq100.yaml
```

### 8. Live dry-run

```bash
trading-cli live schedule --preset xsec_nasdaq100_momentum_v1_deploy --broker mock --output-dir artifacts/live_dry_run/nasdaq100_xsec
```

Or:

```bash
trading-cli live dry-run --config configs/workflows/live_xsec_nasdaq100.yaml
```

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

- `research run`
- `research walkforward`
- `research memo`
- `research registry build`
- `research leaderboard`
- `research compare-runs`
- `research promotion-candidates`
- `research promote`

Experimental / advanced:

- `research sweep`
- `research validate-signal`
- `research alpha`
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
- `/api/discovery/overview`
- `/api/discovery/recent-trades`
- `/api/discovery/recent-symbols`
- `/portfolio`
- `/symbols/<SYMBOL>`
- `/strategies/<STRATEGY_ID>`
- `/trades/<TRADE_ID>`
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
- `paper_positions_history.csv`
- `paper_orders_history.csv`

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
- compact dark theme tuned for dense operational views
- artifact-driven and read-only
- no SPA framework, database, or client-side trading logic

Run locally:

```bash
trading-cli dashboard serve --artifacts-root artifacts --host 127.0.0.1 --port 8000
```

Build static dashboard data:

```bash
trading-cli dashboard build-static-data --artifacts-root artifacts --output-dir artifacts/dashboard_data
```

Symbol detail pages are available at:

```text
http://127.0.0.1:8000/symbols/AAPL
```

The dashboard home page now acts as a discovery index:

- recent symbols with direct links into `/symbols/<SYMBOL>`
- recent trades with direct links into `/trades/<TRADE_ID>`
- recent strategies with direct links into `/strategies/<STRATEGY_ID>`
- recent run/source/mode contexts inferred from existing trade artifacts

Chart payloads are exposed as stable read-only JSON:

```text
GET /api/chart/AAPL?timeframe=1d&lookback=200
GET /api/chart/AAPL?timeframe=1d&lookback=200&source=research&run_id=sample_run&mode=paper
GET /api/trades/AAPL?source=paper_trading&run_id=2026-03-22T00-00-00+00-00
GET /api/signals/AAPL?lookback=200&source=research&run_id=sample_run
GET /api/discovery/overview
GET /api/discovery/recent-trades
GET /api/discovery/recent-symbols
```

The dashboard chart path stays artifact-driven:

- bars and indicator overlays come from `data/features/<SYMBOL>.parquet`
- signal markers come from matching `artifacts/research/.../<SYMBOL>_*_signals.csv`
- trade history prefers an explicit ledger artifact such as `paper_trades.csv` or another `*_trades.csv` when present
- if no trade ledger exists, the dashboard reconstructs trades from fills as a fallback
- orders, fills, and positions come from matching paper/live CSV artifacts such as `paper_fills.csv`, `paper_orders.csv`, `paper_positions.csv`, and `live_dry_run_current_positions.csv`
- optional decision provenance can come from lightweight artifacts such as `decision_provenance.csv`, `selection_decisions.csv`, `portfolio_selection.csv`, `order_intents.csv`, or matching `.json` variants

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
3. run `research registry build`
4. run `strategy-validation build --policy-config configs/strategy_validation_experiment.yaml`
5. run `research promotion-candidates`
6. run `research promote --policy-config configs/promotion_experiment.yaml`
7. run `ops orchestrate run --config configs/orchestration_signal_promotion_test.yaml`

This diagnostic is explicitly for viability testing. It keeps the main production-style configs intact and records the funnel counts, bottleneck assessment, exact commands used, and the before-versus-after outcome of the manifest normalization fix.

Common failure mode to check first:

- if `portfolio_metrics.csv` contains `portfolio_sharpe` but `research_run.json` shows `top_metrics.portfolio_sharpe: null`, promotion-candidate generation will reject the run even when validation passes
- if leaderboard rows write `rejection_reason=none`, that sentinel should be normalized to null before promotion readiness is evaluated

The promotion-viability diagnostic artifact records both the failing and fixed outcomes so this can be used as a quick regression check after research-registry changes.

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

The current equity-only context features are intentionally narrow:

- benchmark-relative return context via `relative_return_<lookback>`
- realized-volatility context via `realized_vol_20`
- simple breadth context via `breadth_positive_<lookback>` and `breadth_impulse_<lookback>`
- optional volume regime context via `volume_ratio_20` when volume exists in the feature inputs

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
3. connect alpha generation outputs directly into `research registry build` and `research promote`
4. add explicit champion/challenger routing into paper and live dry-run workflows
5. tighten approval policies so generated candidates can graduate with auditable constraints

## Development

Run targeted tests while iterating:

```bash
pytest tests/test_cli_grouping.py tests/test_config_loader.py
```

Run the full suite:

```bash
pytest
```
