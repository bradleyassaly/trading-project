# Milestones

## Status Legend

- NOT_STARTED
- IN_PROGRESS
- BLOCKED
- REVIEW_NEEDED
- DONE
- DEFERRED

---

## Strategic Direction

The system has completed most of the forward pipeline foundation: structured decisions, promotion, portfolio construction, persistent paper state, execution simulation, KPI contracts, monitoring, ingestion, feature storage, and automated research. The next priority is **not** broad feature expansion. The next priority is to complete the **closed-loop learning and control layer** so the platform can measure real outcomes, detect degradation, recalibrate, constrain risk, and remove weak strategies.

### Current build plan

#### Phase 1 — Closed-loop learning and control core (current priority)
Complete these milestones in order:
1. **K-01 — Introduce trade outcome attribution**
2. **J-03 — Add risk controls / kill switch**
3. **J-04 — Add drift detection**
4. **K-02 — Add calibration pipeline**
5. **K-03 — Add strategy decay detection**
6. **K-04 — Add auto-demotion / retraining loop**

#### Phase 1.5 — Re-evaluation checkpoint
After Phase 1 is complete:
- run extended paper trading
- measure expected vs realized outcomes
- assess calibration quality
- assess drift / decay behavior
- assess whether risk controls and demotion logic behave correctly
- decide whether the system is close enough to the target goals before expanding scope

#### Phase 2 — Visibility and operator tooling
Only after the Phase 1 checkpoint:
- L-01 through L-05

#### Phase 3 — Expansion to prediction markets / Kalshi
Only after the Phase 1 checkpoint and only if the learning loop is working:
- M-01 through M-07

### Architecture intent for remaining work
The platform should evolve from:
- research + promotion + paper execution

to:
- governed closed-loop trading with attribution, calibration, drift detection, decay detection, risk halts, and lifecycle actions

---

## A-01 — Introduce TradeDecision domain model
Status: DONE

Objective:
Create a first-class structured model for a candidate trade decision that can be reused across replay, paper, and future live flows.

Expected scope:
- add TradeDecision schema/model
- define required fields and serialization
- add tests for construction and round-trip serialization
- document the model and intended usage

Suggested fields:
- decision_id
- timestamp
- strategy_id
- strategy_family
- candidate_id
- instrument
- side
- horizon_days
- predicted_return
- expected_value_gross
- expected_cost
- expected_value_net
- confidence_score
- reliability_score
- regime_label
- sizing_signal
- vetoed
- veto_reasons
- rationale_summary
- metadata

Acceptance criteria:
- TradeDecision exists as a stable domain contract
- serialization is deterministic
- tests cover missing/invalid field behavior where applicable
- docs mention how future milestones should use it

Verification:
- targeted unit tests pass
- existing relevant tests still pass

Dependencies:
- none

---

## A-02 — Emit TradeDecision records from replay/research evaluation
Status: DONE

Objective:
Ensure replay/evaluation workflows produce structured TradeDecision artifacts instead of only aggregate summaries.

Expected scope:
- wire TradeDecision emission into replay/evaluation path
- output structured decision logs
- ensure logs are reproducible and versioned

Acceptance criteria:
- at least one replay path emits decision-level artifacts
- artifacts can be inspected per trade candidate
- output format is documented

Dependencies:
- A-01

---

## A-03 — Add EV decomposition fields
Status: DONE

Objective:
Standardize decomposition of expected value into gross alpha, cost, and net EV.

Expected scope:
- define EV decomposition helper or schema fields
- ensure outputs distinguish gross vs net expectation
- add tests and documentation

Acceptance criteria:
- decision outputs clearly separate expected return drivers
- code does not silently mix gross and net quantities
- documentation explains the intended semantics

Dependencies:
- A-01
- A-02

---

## A-04 — Add reliability, uncertainty, and calibration fields to decisions
Status: DONE

Objective:
Capture confidence and reliability in structured decision outputs.

Expected scope:
- extend decision schema and production paths
- include calibration/reliability information where available
- use null/optional semantics cleanly when unavailable

Acceptance criteria:
- decisions can represent uncertainty explicitly
- downstream artifacts remain backward-compatible or clearly versioned
- tests validate field presence/behavior

Dependencies:
- A-01
- A-02

---

## A-05 — Add veto and rationale logging
Status: DONE

Objective:
Record why a trade was allowed, rejected, or modified.

Expected scope:
- structured veto reason fields
- rationale summary generation or payload assembly
- tests for multiple veto reasons and pass-through cases

Acceptance criteria:
- all rejected decisions can be explained structurally
- all approved decisions can carry rationale fields
- schema is suitable for dashboard drill-down

Dependencies:
- A-01
- A-02

---

## B-01 — Introduce StrategyScorecard domain model
Status: DONE

Objective:
Create a standardized scorecard contract for comparing strategy candidates.

Expected scope:
- add StrategyScorecard schema/model
- document core fields
- add serialization tests

Suggested fields:
- candidate_id
- strategy_family
- training_period
- validation_period
- prediction_count
- realized_return
- expected_return
- turnover
- slippage_estimate
- drawdown
- calibration_score
- stability_score
- regime_robustness_score
- readiness_flags
- rejection_reasons

Acceptance criteria:
- comparable scorecard model exists
- tests and docs are present

Dependencies:
- none

---

## B-02 — Build promotion gate engine
Status: DONE

Objective:
Turn promotion into an explicit rules engine rather than ad hoc logic.

Expected scope:
- promotion gate evaluation module
- gate results payload
- structured pass/fail reasons
- tests for edge cases and threshold behavior

Acceptance criteria:
- promotion is machine-checkable
- each failed gate emits explicit reason(s)
- promotion result is serializable and inspectable

Dependencies:
- B-01

---

## B-03 — Add PromotionDecision contract
Status: DONE

Objective:
Introduce a first-class promotion decision contract that captures final promotion outcome, structured gate results, rejection reasons, and summary decision metadata.

Acceptance criteria:
- promotion decision is represented as a typed serializable contract
- final status, gate results, failed/passed gates, and rejection reasons are machine-readable
- decision is documented and test-covered

Dependencies:
- B-02

---

## B-04 — Add live-readiness gate skeleton
Status: DONE

Objective:
Create a placeholder live-readiness framework even if live execution is not yet implemented.

Acceptance criteria:
- live-readiness contract exists
- no live path is enabled automatically
- documentation clarifies this is governance scaffolding

Dependencies:
- B-02

---

## C-01 — Normalize strategy outputs into a shared portfolio input contract
Status: DONE

Objective:
All strategies should emit a consistent portfolio-facing representation.

Acceptance criteria:
- shared forecast/allocation input contract exists
- portfolio layer does not depend on family-specific ad hoc output shapes
- tests cover multiple strategy cases

Dependencies:
- A-01

---

## C-02 — Add strategy conflict resolution logic
Status: DONE

Objective:
Handle competing strategy intents on the same instrument or exposure bucket.

Acceptance criteria:
- explicit conflict resolution rules exist
- behavior is documented
- tests cover conflicting signals

Dependencies:
- C-01

---

## C-03 — Add exposure budgets and concentration rules
Status: DONE

Objective:
Enforce portfolio-level controls at the portfolio construction layer.

Acceptance criteria:
- configurable exposure rules exist
- portfolio output shows when constraints bind
- tests cover limit enforcement

Dependencies:
- C-01

---

## C-04 — Add allocation diagnostics and rationale outputs
Status: DONE

Objective:
Explain why the portfolio allocated capital as it did.

Acceptance criteria:
- portfolio allocation outputs include rationale fields
- diagnostics show constraint effects and trade-offs
- data is usable by dashboard layer

Dependencies:
- C-02
- C-03

---

## D-01 — Introduce persistent paper state model
Status: DONE

Objective:
Persist paper portfolio state across runs.

Acceptance criteria:
- paper state is modeled explicitly
- restart-safe read/write behavior exists
- tests cover missing/corrupt/partial state cases where practical

Dependencies:
- C-01

---

## D-02 — Add order lifecycle domain objects
Status: DONE

Objective:
Create explicit models for order intent, submitted order, fill, cancellation, and status changes.

Acceptance criteria:
- order lifecycle objects exist
- serialization and tests exist
- future reconciliation can build on them cleanly

Dependencies:
- D-01

---

## D-03 — Add reconciliation engine skeleton
Status: DONE

Objective:
Introduce a comparison layer between intended state and realized/reported state.

Acceptance criteria:
- reconciliation result contract exists
- mismatches are representable and inspectable
- documentation explains intended future integration

Dependencies:
- D-01
- D-02

---

## D-04 — Refactor paper path toward shared decision pipeline
Status: DONE

Objective:
Reduce divergence between replay/paper paths.

Acceptance criteria:
- duplicated logic is reduced
- shared contracts are used
- tests verify equivalent behavior where expected

Dependencies:
- A-02
- D-01

---

## E-01 — Introduce KPI schema / warehouse contract
Status: DONE

Objective:
Define structured KPI payloads for strategy, trade, portfolio, and system metrics.

Acceptance criteria:
- schema exists
- basic producer/consumer tests exist
- dashboard-facing semantics are documented

Dependencies:
- A-05
- C-04
- D-03

---

## E-02 — Add trade explorer payloads
Status: DONE

Objective:
Expose decision and outcome records in a dashboard-ready structure.

Acceptance criteria:
- trade explorer payload shape exists
- records support drill-down
- rationale and outcome data are both present

Dependencies:
- A-05
- E-01

---

## E-03 — Add strategy health payloads
Status: DONE

Objective:
Create dashboard-ready strategy health summaries and trend views.

Acceptance criteria:
- strategy health contract exists
- includes at least return/risk/EV/reliability style metrics where available
- documentation explains intended UI usage

Dependencies:
- B-01
- E-01

---

## F-01 — Clarify subsystem boundaries
Status: DONE

Objective:
Strengthen boundaries between research, portfolio, risk, execution, and reporting modules.

Acceptance criteria:
- architecture note added
- at least one concrete boundary improvement merged
- docs updated

Dependencies:
- none

---

## F-02 — Expand test strategy by layer
Status: DONE

Objective:
Document and improve testing by subsystem risk.

Acceptance criteria:
- testing strategy note exists
- new tests added in at least one under-covered area
- docs updated

Dependencies:
- none

---

## F-03 — Add performance profiling and caching plan
Status: DONE

Objective:
Document and begin addressing expensive recomputation in research/replay flows.

Acceptance criteria:
- profiling note exists
- at least one high-value optimization target is identified
- one small optimization may be implemented if clearly in scope

Dependencies:
- none

---

## G-01 — Introduce unified data ingestion framework
Status: DONE

Objective:
Support ingestion of multiple asset classes and frequencies in a standardized, reproducible format.

Expected scope:
- add ingestion adapters for equities (daily) and crypto (intraday)
- define normalized OHLCV schema
- ensure outputs are versioned and reproducible
- document ingestion pipeline and usage

Acceptance criteria:
- at least one equity and one crypto source are supported
- ingestion outputs follow a consistent schema
- artifacts are reproducible across runs
- documentation explains supported data sources and formats

Dependencies:
- none

---

## G-02 — Add multi-frequency time alignment layer
Status: DONE

Objective:
Enable safe alignment of datasets with different frequencies (e.g., daily and intraday).

Expected scope:
- implement resampling utilities
- define alignment rules (forward-fill, aggregation)
- prevent forward-looking data leakage
- add tests for mixed-frequency scenarios

Acceptance criteria:
- daily and intraday data can be combined safely
- no forward-looking leakage is introduced
- alignment behavior is documented and test-covered

Dependencies:
- G-01

---

## G-03 — Introduce feature store
Status: DONE

Objective:
Persist computed features for reuse across research, replay, and paper trading.

Expected scope:
- define feature storage schema
- implement read/write interface
- support feature versioning
- document feature lifecycle

Acceptance criteria:
- features can be stored and retrieved deterministically
- feature versioning is supported
- redundant recomputation is reduced
- documentation explains usage

Dependencies:
- G-01

---

## G-04 — Add data quality validation layer
Status: DONE

Objective:
Ensure integrity and reliability of ingested data.

Expected scope:
- detect missing data
- detect outliers
- validate schema consistency
- add validation reports

Acceptance criteria:
- data validation checks run on ingestion
- issues are surfaced in structured outputs
- validation logic is documented and test-covered

Dependencies:
- G-01

---

## G-05 — Add Binance crypto public market-data ingestion
Status: DONE

Objective:
Bring Binance public crypto market data into the system as a first-class research input with resumable REST ingestion, explicit crypto normalization, and grouped CLI workflows.

Expected scope:
- add a Binance provider package with centralized REST client, retry/backoff handling, and exchange-driven symbol validation
- ingest public `exchangeInfo`, `klines`, `aggTrades`, and optional `bookTicker` snapshots
- persist provider-specific raw artifacts plus resumable checkpoints and structured ingest summaries
- normalize raw Binance artifacts into explicit crypto parquet outputs suitable for research consumption
- add grouped CLI commands under `data crypto binance ...`
- document the milestone and add focused mocked tests

Acceptance criteria:
- Binance symbols and intervals are config-driven and validated from exchange metadata
- historical ingestion is resumable and idempotent across checkpoints and raw artifact writes
- normalized outputs include explicit crypto fields and provenance back to raw artifacts
- CLI commands support bounded local test runs and emit machine-readable summaries
- tests cover config parsing, pagination, resumability, normalization, and CLI wiring

Dependencies:
- G-01
- G-04

---

## G-06 — Add Binance websocket incremental append and unified crypto market-data projections
Status: DONE

Objective:
Extend the Binance crypto source with production-style public websocket ingestion, incremental append safety, and stable projected crypto market-data datasets for downstream research use.

Expected scope:
- add a centralized Binance websocket ingestion service for `kline`, `aggTrade`, and `bookTicker` public streams
- support bounded runs, reconnect/backoff handling, duplicate tolerance, and checkpointed restart safety
- persist provider-specific raw incremental websocket artifacts and normalized incremental parquet datasets
- build explicit mixed-source projected datasets from historical REST and websocket incremental normalized inputs
- add grouped CLI commands for websocket ingest and projection rebuilds
- document schema intent, artifact layout, and operational limits, and add focused mocked tests

Acceptance criteria:
- websocket runs can ingest bounded public market-data sessions without live account/auth dependencies
- repeated websocket runs can restart safely from checkpoint state without duplicate normalized appends
- historical REST and websocket incremental normalized outputs feed shared projected datasets
- projected datasets have explicit schema intent, uniqueness rules, and source/provenance metadata
- tests cover parsing, reconnects, checkpoint/resume, duplicate handling, projections, and CLI wiring

Dependencies:
- G-05

---

## G-07 — Add scheduled Binance incremental sync orchestration and feature-store consumers for projected crypto datasets
Status: DONE

Objective:
Add a bounded restart-safe Binance sync runner that composes websocket incremental ingest, projection refresh, and projected-dataset feature refresh into one operational workflow, while publishing explicit crypto feature artifacts for research consumers.

Expected scope:
- add a Binance sync runner that wraps existing websocket ingest and projection steps without duplicating their core logic
- support bounded sync runs with optional projection and feature refresh steps plus structured step-level summaries
- add grouped CLI commands for one-command sync and direct projected-feature refresh workflows
- build explicit projected-dataset feature consumers for returns, volatility, volume, trade-intensity, and top-of-book microstructure features
- publish feature outputs to both provider-specific parquet artifacts and the repo's local feature-store convention
- extend the Binance config surface and add focused mocked tests plus docs

Acceptance criteria:
- `data crypto binance sync` can run a bounded incremental cycle with restart-safe websocket state reuse and a machine-readable sync summary
- projected Binance datasets feed an explicit crypto feature dataset without falling back to raw provider artifacts
- feature refreshes are deterministic and idempotent for selected symbol and interval slices, with documented uniqueness rules
- feature-store manifests are written for refreshed Binance feature slices
- tests cover sync step ordering, failure handling, feature refresh behavior, CLI wiring, and config parsing

Dependencies:
- G-06

---

## G-08 — Add scheduler-facing sync manifests, freshness monitoring, and research consumers for projected crypto feature datasets
Status: DONE

Objective:
Make the Binance crypto pipeline scheduler-friendly and research-usable by adding explicit sync manifests, freshness status outputs for projected and feature datasets, and narrow readers for projected-feature artifacts.

Expected scope:
- extend the Binance sync runner to write stable scheduler-facing per-run and latest sync manifests
- add freshness/status outputs for projected datasets and projected-feature datasets with staleness classification
- add grouped CLI status inspection for the latest Binance sync and dataset freshness
- add research readers that load Binance feature-store artifacts with symbol, interval, and time filtering
- add a small research dataset assembly helper that can attach a forward-return target over selected bar horizons
- document the new artifacts and add focused mocked tests

Acceptance criteria:
- each Binance sync run emits a stable sync manifest with run metadata, step outcomes, counts, warnings, failures, and artifact references
- freshness status can classify projected and feature datasets as stale or fresh using configured thresholds
- `data crypto binance status` exposes the latest freshness state in human-readable or JSON form
- Binance projected-feature artifacts can be loaded through a stable reader path suitable for replay and research dataset assembly
- tests cover manifest generation, freshness classification, CLI wiring, empty data handling, and research consumer filtering

Dependencies:
- G-07

---

## G-09 — Add crypto research loop adapters, freshness alerting, and scheduler-oriented health checks for Binance datasets
Status: DONE

Objective:
Integrate Binance projected-feature datasets into the broader research workflow and improve operational trust with explicit freshness alerts and scheduler-facing health checks.

Expected scope:
- add a research dataset adapter and materializer for Binance feature-store outputs with explicit target-generation behavior
- define a stable Binance research dataset contract for symbol, interval, timestamp, features, and target columns
- add rule-based Binance freshness alert evaluation from the latest sync manifest plus status artifacts
- add scheduler-oriented Binance health checks covering latest sync success, freshness, and required scope presence
- add grouped CLI commands for Binance alerts and health checks
- extend config, tests, and documentation without regressing existing Binance flows

Acceptance criteria:
- Binance research consumers can materialize replay-ready datasets from the existing feature-store outputs without duplicating feature generation
- alert evaluation writes stable artifacts and surfaces stale datasets, unhealthy latest syncs, and missing required scopes
- health checks produce machine-readable pass/warn/fail style outputs suitable for scheduler or orchestrator polling
- CLI commands expose bounded human-readable and JSON-friendly summaries
- tests cover research dataset loading, target-generation alignment, alert rules, health-check semantics, and CLI wiring

Dependencies:
- G-08

---

## G-10 — Add cross-asset research dataset registry integration and notification-backed Binance monitoring workflows
Status: DONE

Objective:
Make Binance research datasets discoverable alongside other asset classes and turn Binance monitoring results into actionable, transition-aware notification workflows.

Expected scope:
- add a narrow shared research dataset registry for cross-asset dataset discovery and metadata inspection
- publish Binance research datasets into that registry with manifest, freshness, and monitoring references
- extend the Binance research consumer path with registry-backed discovery and loading helpers
- add a notification workflow that evaluates current alert and health artifacts, tracks status transitions, and optionally delivers through the existing notification service
- add grouped CLI support for Binance notify evaluation
- update config, tests, and docs without regressing current Binance ingest, projection, feature, sync, status, research, alert, or health behavior

Acceptance criteria:
- Binance research datasets can be discovered through a stable cross-asset registry entry rather than only by ad hoc file paths
- downstream code can resolve registry metadata and load Binance research datasets by dataset key with symbol, interval, and time filtering
- Binance monitoring writes stable notification-evaluation artifacts with transition-aware `healthy/warning/critical` decisions
- notification delivery reuses the repo's existing notification service when a shared notification config is provided, and otherwise still writes would-send artifacts
- tests cover registry publication, registry-backed loading, notification transitions, duplicate suppression, CLI wiring, and empty or missing-artifact cases

Dependencies:
- G-09

---

## G-11 — Add additional asset-class publishers to the shared research dataset registry and unify scheduler monitoring dashboards across providers
Status: DONE

Objective:
Expand the shared research dataset registry beyond Binance and add a machine-readable cross-provider monitoring layer that lets schedulers and operators inspect registry-backed research datasets consistently across providers.

Expected scope:
- extend the shared research dataset registry contract so providers can publish directory-backed feature datasets, not just single parquet files
- publish Kalshi and Polymarket prediction-market feature artifacts into the shared registry from their existing manifests, summaries, validation outputs, and feature directories
- add shared CLI flows for dataset-registry publication and listing without hard-coded provider paths in downstream code
- build a cross-provider monitoring summary that reads registry entries plus provider truth artifacts and emits stable aggregate JSON summaries for schedulers and future dashboards
- add grouped CLI support for provider-summary and provider-health inspection
- update provider configs, tests, and docs without regressing the existing Binance registry, health, alerts, or notify flows

Acceptance criteria:
- Kalshi and Polymarket research datasets publish stable entries into `data/research/dataset_registry.json` using existing provider artifacts as the source of truth
- shared registry loading supports both single-parquet and directory-backed research datasets
- downstream code can list registry entries across providers and filter them by provider, asset class, and dataset name
- a shared cross-provider monitoring build writes aggregate artifacts such as `latest_registry_summary.json`, `latest_monitoring_summary.json`, and `cross_provider_health_summary.json`
- tests cover Kalshi and Polymarket publication, cross-provider listing and filtering, aggregate status semantics, missing-artifact handling, and CLI wiring

Dependencies:
- G-10

---

## G-12 — Add cross-provider research dataset readers and dashboard/API consumers for the shared registry and provider monitoring summaries
Status: DONE

Objective:
Make the shared research dataset registry and provider monitoring artifacts first-class read surfaces for the rest of the platform through a stable registry-backed reader layer plus lightweight API and dashboard consumers.

Expected scope:
- add a shared registry-backed research dataset reader contract that can list, resolve, inspect, and load provider datasets without hard-coded provider paths
- encapsulate provider-specific dataset quirks behind narrow reader helpers while keeping the shared registry as the entry point
- add reusable readers for the latest registry publication summary, provider monitoring summary, and provider health rollup artifacts
- expose those shared registry and monitoring artifacts through the existing FastAPI artifact-backed API
- add a lightweight dashboard page for inspecting shared datasets and provider health without introducing a new UI framework
- update tests, docs, and milestone state without regressing Binance, Kalshi, or Polymarket provider flows

Acceptance criteria:
- downstream code can list and load shared research datasets across Binance, Kalshi, and Polymarket through one registry-backed reader interface
- registry-backed reads support provider, asset-class, dataset-name, symbol, interval, and time filtering where the underlying dataset supports those fields
- API consumers can read registry contents, dataset detail, bounded dataset previews, registry publication summaries, provider monitoring summaries, and provider health summaries
- a lightweight dashboard surface can inspect the shared registry and provider health rollups without relying on provider-specific hard-coded file paths
- tests cover registry-backed resolution, ambiguous and missing dataset handling, monitoring artifact readers, and the new API routes

Dependencies:
- G-11

---

## H-01 — Expand candidate grid generation
Status: DONE

Objective:
Extend candidate generation to produce broader parameter and signal variations.

Acceptance criteria:
- multiple variants per signal family are generated
- outputs remain compatible with promotion pipeline
- documentation explains configuration options

Dependencies:
- none

---

## H-02 — Introduce automated research loop
Status: DONE

Objective:
Enable the system to autonomously generate, evaluate, and promote strategies.

Expected scope:
- orchestrate candidate generation, evaluation, and promotion
- support scheduled or continuous runs
- persist outputs for inspection

Acceptance criteria:
- research loop can run unattended
- candidates flow through full lifecycle
- outputs are reproducible and logged

Dependencies:
- H-01
- B-02

---

## H-03 — Add research resource allocation
Status: DONE

Objective:
Allocate compute resources dynamically across candidate strategies.

Expected scope:
- prioritize promising candidates
- implement early stopping for weak candidates
- define allocation rules

Acceptance criteria:
- compute usage is biased toward higher-quality candidates
- weak candidates are pruned early
- behavior is documented and test-covered

Dependencies:
- H-02

---

## H-04 — Add experiment tracking system
Status: DONE

Objective:
Track experiments, configurations, and results in a structured and queryable format.

Expected scope:
- define experiment metadata schema
- persist configurations and outputs
- enable querying of past runs

Acceptance criteria:
- experiments are reproducible
- configs and outputs are linked
- documentation explains tracking system

Dependencies:
- H-01

---

## I-01 — Introduce execution simulator
Status: DONE

Objective:
Simulate realistic trade execution behavior.

Expected scope:
- model slippage and spread
- simulate partial fills
- incorporate latency assumptions
- add tests for execution scenarios

Acceptance criteria:
- execution simulation produces realistic outcomes
- results integrate with existing decision pipeline
- behavior is documented and test-covered

Dependencies:
- D-02

---

## I-02 — Add transaction cost modeling
Status: DONE

Objective:
Improve accuracy of expected cost estimates in trade evaluation.

Expected scope:
- define cost model components (spread, fees, slippage)
- integrate with EV calculations
- support configurable parameters

Acceptance criteria:
- costs are explicitly modeled and reported
- EV reflects realistic cost assumptions
- tests validate cost calculations

Dependencies:
- I-01
- A-03

---

## I-03 — Introduce broker/exchange abstraction layer
Status: DONE

Objective:
Create a unified interface for execution across paper and future live environments.

Expected scope:
- define broker interface
- support paper execution adapter
- allow extension to real exchanges

Acceptance criteria:
- execution layer is decoupled from strategy logic
- at least one adapter (paper) is implemented
- interface is documented

Dependencies:
- D-02

---

## I-04 — Add intraday execution support
Status: DONE

Objective:
Support higher-frequency decision and execution cycles.

Expected scope:
- enable intraday scheduling
- handle rapid decision generation
- ensure compatibility with execution simulator

Acceptance criteria:
- system can process intraday data without failure
- execution pipeline supports frequent updates
- behavior is documented

Dependencies:
- G-01
- I-01

---

## J-01 — Introduce real-time KPI monitoring
Status: DONE

Objective:
Track key system and trading metrics continuously.

Expected scope:
- define KPI set (PnL, drawdown, exposure, EV vs realized)
- implement monitoring outputs
- support dashboard integration

Acceptance criteria:
- KPIs are updated in near real-time
- outputs are structured and queryable
- documentation explains metrics

Dependencies:
- E-01

---

## J-02 — Add system health monitoring
Status: DONE

Objective:
Detect failures or anomalies in system operation.

Expected scope:
- monitor data freshness
- detect pipeline failures
- surface warnings and errors

Acceptance criteria:
- system health issues are detectable and logged
- alerts or flags are generated
- behavior is documented

Dependencies:
- G-01
- D-01

---

## J-03 — Add risk controls / kill switch
Status: DONE
Priority: PHASE_1
Recommended order: 2 of 6

Objective:
Prevent catastrophic losses through automated safeguards.

Expected scope:
- define drawdown limits
- define abnormal behavior triggers
- implement trading halt mechanism
- define strategy-level, instrument-level, and portfolio-level halt / constrain rules
- emit structured kill-switch and restriction events

Acceptance criteria:
- system can halt trading under defined conditions
- triggers are configurable
- behavior is documented and test-covered
- kill switch can move the system into restricted or halted operating states
- lifecycle actions are logged for later review

Dependencies:
- J-01
- recommended after K-01 so expected vs realized behavior can inform safeguards

---

## J-04 — Add drift detection
Status: DONE
Priority: PHASE_1
Recommended order: 3 of 6

Objective:
Detect degradation in model, strategy, feature, or execution behavior.

Expected scope:
- compare expected vs realized performance
- track statistical deviations
- generate structured drift signals
- support severity levels such as info / watch / warning / critical
- include at least performance drift, decision drift, and execution drift in the initial implementation

Acceptance criteria:
- drift conditions are detectable
- outputs are structured and explainable
- documentation explains methodology
- drift signals can be joined back to strategy, regime, and time window

Dependencies:
- K-01

---

## K-01 — Introduce trade outcome attribution
Status: DONE
Priority: PHASE_1
Recommended order: 1 of 6

Objective:
Compare predicted outcomes with realized results and explain discrepancies.

Expected scope:
- add first-class contracts such as TradeOutcome and TradeAttribution, or equivalent platform-owned models
- compute realized vs expected gross return, cost, and net return
- decompose error sources such as alpha error, cost error, timing error, execution error, sizing error, and regime mismatch where feasible
- integrate with KPI outputs and paper execution artifacts
- support aggregation by strategy, regime, instrument, confidence bucket, and horizon

Acceptance criteria:
- attribution results are structured and inspectable
- discrepancies are explainable
- methodology is documented
- outputs are reproducible and traceable from decision to outcome
- at least one end-to-end paper or replay path emits attribution artifacts

Dependencies:
- A-02
- E-01
- D-02
- D-03

---

## K-02 — Add calibration pipeline
Status: DONE
Priority: PHASE_1
Recommended order: 4 of 6

Objective:
Improve reliability and confidence scoring over time using realized outcomes.

Expected scope:
- update calibration models based on outcomes
- adjust confidence scores and/or EV scaling
- track calibration metrics
- preserve both raw and calibrated predictions
- support at least an initial bucketed or monotonic calibration method

Acceptance criteria:
- calibration improves prediction reliability
- outputs are measurable and tracked
- behavior is documented
- calibrated outputs can be joined back to decisions and outcomes

Dependencies:
- K-01

---

## K-03 — Add strategy decay detection
Status: DONE
Priority: PHASE_1
Recommended order: 5 of 6

Objective:
Identify strategies that are losing effectiveness.

Expected scope:
- monitor performance degradation
- define decay thresholds or a decay score
- combine evidence from attribution, calibration, drift, and drawdown behavior
- flag affected strategies with structured severity / lifecycle recommendations

Acceptance criteria:
- decay conditions are detectable
- outputs are structured
- documentation explains thresholds or scoring
- strategy decay can be evaluated on a rolling basis

Dependencies:
- J-04
- K-02
- J-03

---

## K-04 — Add auto-demotion / retraining loop
Status: DONE
Priority: PHASE_1
Recommended order: 6 of 6

Objective:
Automatically constrain, demote, or retrain underperforming strategies without bypassing governance.

Expected scope:
- integrate with promotion/demotion logic
- trigger retraining workflows
- log lifecycle actions taken
- support staged responses such as watch, constrain, demote, retrain
- require retrained candidates to re-enter normal promotion gates

Acceptance criteria:
- weak strategies are demoted automatically
- retraining can be triggered programmatically
- behavior is documented
- governance trail exists for every lifecycle action

Dependencies:
- K-03
- B-02

---

## L-01 — Add trade explorer payload
Status: DEFERRED
Priority: PHASE_2_AFTER_REEVALUATION

Objective:
Expose decision and outcome data for per-trade inspection.

Acceptance criteria:
- payload supports drill-down
- includes rationale, EV, and outcome data
- format is dashboard-ready

Dependencies:
- E-02
- K-01

---

## L-02 — Add strategy health payload
Status: DEFERRED
Priority: PHASE_2_AFTER_REEVALUATION

Objective:
Expose strategy-level performance summaries.

Acceptance criteria:
- includes return, risk, reliability, calibration, and decay-style metrics where available
- supports trend analysis
- format is dashboard-ready

Dependencies:
- E-03
- K-02
- K-03

---

## L-03 — Add portfolio visualization payload
Status: DEFERRED
Priority: PHASE_2_AFTER_REEVALUATION

Objective:
Expose portfolio allocation and exposure data.

Acceptance criteria:
- allocation data is structured
- constraints and exposures are visible
- format supports visualization

Dependencies:
- C-04
- J-03

---

## L-04 — Add decision flow visualization payload
Status: DEFERRED
Priority: PHASE_2_AFTER_REEVALUATION

Objective:
Expose decision pipeline stages for visualization.

Acceptance criteria:
- shows signal → filters → veto → allocation → execution → outcome → attribution
- data is structured and traceable
- documentation explains flow

Dependencies:
- A-05
- C-04
- K-01

---

## L-05 — Add KPI dashboard backend API
Status: DEFERRED
Priority: PHASE_2_AFTER_REEVALUATION

Objective:
Provide backend services for dashboard consumption.

Acceptance criteria:
- API serves KPI, trade, and strategy data
- supports filtering and drill-down
- documentation explains endpoints

Dependencies:
- E-01
- L-01
- L-02
- L-03
- L-04

---

# 🟧 M — Prediction Markets (Kalshi Integration)

## M-01 — Introduce prediction market instrument model
Status: DEFERRED
Priority: PHASE_3_AFTER_REEVALUATION

Objective:
Extend the system to support prediction market contracts as first-class instruments.

Expected scope:
- define PredictionMarketInstrument contract
- include fields such as:
  - event_id
  - contract_id
  - expiration_timestamp
  - strike/condition description
  - payoff structure (binary, scalar)
- integrate with existing instrument abstractions where applicable

Acceptance criteria:
- prediction market instruments can be represented alongside equities/crypto
- schema is serializable and documented
- tests validate construction and usage

Dependencies:
- A-01

---

## M-02 — Add Kalshi data ingestion adapter
Status: DEFERRED
Priority: PHASE_3_AFTER_REEVALUATION

Objective:
Ingest Kalshi market data into the unified ingestion framework.

Expected scope:
- implement Kalshi adapter under ingestion layer
- normalize:
  - price
  - implied probability
  - volume/liquidity
  - order book where available
- map to unified schema with asset_class = prediction_market

Acceptance criteria:
- Kalshi data flows through G-01 ingestion framework
- normalized artifacts are produced
- validation layer (G-04) applies cleanly
- no external credentials required for basic historical or public data

Dependencies:
- G-01

---

## M-03 — Add prediction market feature support
Status: DEFERRED
Priority: PHASE_3_AFTER_REEVALUATION

Objective:
Enable feature generation for prediction market instruments.

Expected scope:
- compute features such as:
  - implied probability changes
  - momentum / drift
  - volatility of probability
- integrate with feature store (G-03)

Acceptance criteria:
- features are stored and versioned
- feature generation is deterministic
- compatible with existing research pipeline

Dependencies:
- G-03

---

## M-04 — Extend TradeDecision for probabilistic markets
Status: DEFERRED
Priority: PHASE_3_AFTER_REEVALUATION

Objective:
Adapt decision modeling to prediction markets.

Expected scope:
- incorporate:
  - implied probability vs model probability
  - edge (expected value)
- ensure compatibility with EV decomposition

Acceptance criteria:
- TradeDecision supports prediction market semantics
- EV calculations reflect probabilistic payoff
- backward compatibility preserved

Dependencies:
- A-03

---

## M-05 — Add Kalshi execution adapter (paper-first)
Status: DEFERRED
Priority: PHASE_3_AFTER_REEVALUATION

Objective:
Support simulated execution of prediction market trades.

Expected scope:
- integrate with execution simulator (I-01)
- model:
  - bid/ask spread
  - partial fills
  - contract settlement logic
- no live trading required

Acceptance criteria:
- Kalshi trades can be simulated in paper mode
- integrates with order lifecycle models
- results are traceable and inspectable

Dependencies:
- I-01
- D-02

---

## M-06 — Add prediction market portfolio handling
Status: DEFERRED
Priority: PHASE_3_AFTER_REEVALUATION

Objective:
Incorporate prediction market positions into portfolio construction.

Expected scope:
- handle binary payoff positions
- integrate with exposure rules
- treat contracts as distinct instruments

Acceptance criteria:
- portfolio layer can include prediction markets
- constraints apply consistently
- allocation diagnostics include these positions

Dependencies:
- C-01

---

## M-07 — Add prediction market evaluation & calibration
Status: DEFERRED
Priority: PHASE_3_AFTER_REEVALUATION

Objective:
Evaluate prediction accuracy and calibration.

Expected scope:
- compare:
  - model probability vs market probability
  - model vs realized outcome
- integrate with feedback loop (K-01)

Acceptance criteria:
- calibration metrics are computed
- results are included in reporting layer
- supports strategy improvement

Dependencies:
- K-01
- K-02

Kalshi / Prediction Markets Expansion
Objective

Extend the platform to support event-driven prediction markets (starting with Kalshi and later Polymarket), enabling research, backtesting, paper trading, and eventually live trading of event-based strategies.

K-00 - Kalshi Ingest Validation and Data-Quality Reporting

Status: DONE

Add a post-ingest validation layer for real Kalshi datasets so normalized market, trade, candle, resolution, and ingest-metadata artifacts are audited before research, backtesting, or paper trading consumes them.

Requirements:

Inspect normalized artifacts such as:
- data/kalshi/normalized/markets.parquet
- data/kalshi/normalized/trades/*
- data/kalshi/normalized/candles/*
- data/kalshi/normalized/resolution.csv
- ingest_summary.json / ingest_manifest.json / ingest_checkpoint.json

Report at minimum:
- total markets, trades, candles, and resolved markets
- resolution/trade/candle coverage rates
- duplicate ticker and market-id diagnostics
- invalid timestamps, missing categories, and category distribution
- date-range coverage and cross-layer schema mismatches
- filtered vs retained market diagnostics with effective filter config
- synthetic-marker detection in real-data defaults

Artifacts:
- kalshi_data_validation_summary.json
- kalshi_data_validation_details.json
- kalshi_data_validation_report.md

Acceptance criteria:
- validation can run independently from the CLI and programmatically
- findings are classified as PASS / WARNING / FAIL with actionable reasons
- threshold policy is configurable
- Kalshi historical ingest emits enough filter diagnostics for the validator to explain market exclusions
- backtest and paper paths can optionally require a passing validation summary

Tier 1 - Core System (Must Complete First)
K-01 - Kalshi Resolved-Market Backtest Framework

Status: DONE

Build a production-grade research/backtest runner for Kalshi markets using resolved historical data.

Requirements:

Load historical market + trade data from local artifacts
Restrict evaluation to resolved markets only
Support configurable:
entry timing rules
holding periods
execution assumptions (fill price, slippage, latency)
Output structured artifacts:
kalshi_backtest_summary.json
kalshi_signal_diagnostics.json
kalshi_trade_log.jsonl
kalshi_backtest_report.md

Metrics:

Win rate
Average predicted edge
Realized return
Calibration (Brier score or equivalent)
Performance by:
category
confidence bucket
signal type

Goal:
Establish a reliable research/evaluation loop for event-market strategies.

K-02 — Informed Flow Signal Family

Status: DONE

Implement microstructure-based signals using Kalshi trade/order flow data.

Signals:

Taker imbalance (buy vs sell pressure)
Large aggressive trade detection
Unexplained short-horizon price movement
(Optional extension) Flow persistence / repeated directional sweeps

Requirements:

Fully configurable thresholds
Clean signal output schema (confidence, direction, supporting features)
Plug directly into K-01 backtest framework

Goal:
Create differentiated alpha signals not based on simple price history.

K-03 — Kalshi Paper Trading Integration

Status: DONE

Enable live paper trading on real Kalshi markets using the existing execution framework.

Requirements:

Integrate KalshiBroker into paper trading engine
Real-time market polling / snapshotting
Order simulation or paper execution path
Persistent state (positions, P&L, history)
Daily validation/report generation

Risk Controls:

Max exposure per market
Max exposure per category
Drawdown limits (reuse global risk engine)
No-trade conditions for low liquidity or near settlement

Goal:
Close the loop: signal → trade → outcome → evaluation.

Tier 2 — Alpha Expansion (After Core is Working)
K-04 — Cross-Market Arbitrage Monitor

Status: DONE

Detect pricing discrepancies between Kalshi and Polymarket.

Requirements:

Normalize market definitions across platforms
Fuzzy-match equivalent markets
Compute spread and implied probabilities
Log opportunities and persistence over time

Outputs:

cross_market_opportunities.jsonl
Opportunity summary report

Goal:
Validate whether cross-market inefficiencies produce real, persistent edge.

K-05 — Signal Ensemble & Portfolio Construction

Status: TODO

Combine multiple Kalshi signals into a unified decision layer.

Features:

Weight signals by:
historical performance (IC / win rate)
confidence
category reliability
Handle conflicting signals
Portfolio-level exposure management

Goal:
Move from single-signal trades to robust multi-signal strategies.

K-06 — Automated Market Categorization

Status: TODO

Improve classification of markets into categories (economic, political, weather, etc.).

Approach:

Replace keyword rules with:
lightweight LLM classification or
local ML classifier
Store category + confidence

Goal:
Improve base-rate modeling and signal conditioning.

Tier 3 — Advanced Capabilities (After Edge is Proven)
K-07 — Polymarket Integration

Status: TODO

Build full data + execution adapter for Polymarket.

Features:

On-chain data ingestion (Polygon)
Wallet-level behavior tracking
Trade + liquidity data normalization

Goal:
Expand data surface and enable cross-market strategies.

K-08 — News & Event Reaction Signals

Status: TODO

Incorporate real-time news to detect lag in market repricing.

Sources:

Public news APIs (e.g., NewsAPI, GDELT)

Features:

Event detection
Mapping to active markets
Time-to-repricing analysis

Goal:
Capture edge from faster reaction to public information.

K-09 — Autonomous Strategy Discovery (Prediction Markets)

Status: TODO

Extend the existing autonomous research loop to Kalshi strategies.

Features:

Periodic re-training / evaluation on fresh data
Automatic proposal of new signals
Promotion via governance layer

Goal:
Continuously discover new edge without manual intervention.

K-10 — Live Trading Deployment

Status: TODO

Deploy real capital on Kalshi once paper trading proves positive expectation.

Constraints:

Minimum sample size (e.g., 50+ resolved trades)
Strict position sizing ($10–$25 initial trades)
Conservative risk limits

Goal:
Transition from research system → real trading system safely.

Success Criteria for Kalshi Expansion

The system is considered successful when:

Backtests show stable edge across multiple market categories
Paper trading produces consistent positive expected value
Signal diagnostics show calibration (confidence aligns with outcomes)
Execution system operates without failures over extended runs
Cross-market opportunities demonstrate measurable persistence
Notes
All Kalshi functionality should follow existing platform principles:
platform-owned abstractions
structured artifacts
reproducible research
governance before promotion
Avoid premature optimization or overbuilding before validating edge in K-01 and K-02.

---

## Re-evaluation checkpoint criteria

Before moving to Phase 2 or Phase 3, review the system against the following questions:
- Are positive expected value trades producing positive realized net outcomes after realistic costs?
- Are high-confidence or high-reliability trades actually outperforming lower-confidence trades?
- Does attribution explain a meaningful portion of expected-vs-realized gaps?
- Are drift signals useful rather than noisy?
- Does decay detection identify weak strategies early enough to matter?
- Does the kill switch or restriction logic trigger appropriately under stress?
- Does auto-demotion improve the active set rather than churn it excessively?

If the answer is not yet clearly yes for most of these, continue improving Phase 1 before expanding scope.

---

## Notes

### Milestone completion requirements
A milestone is not complete until:
- code is implemented
- tests are added or updated
- documentation is updated
- verification commands are listed
- status is updated in this file

### Review policy
Any milestone that changes trading behavior, promotion logic, portfolio allocation, calibration behavior, drift detection, kill-switch behavior, or strategy lifecycle transitions should be reviewed carefully before merge.

### Maintenance Notes
- 2026-04-01: Kalshi historical ingest was audited and hardened so real-data research defaults now point to `data/kalshi/features/real` and `data/kalshi/normalized/resolution.csv`, while synthetic fixture generation was segregated under `data/kalshi/synthetic/...`. The ingest path is now cutoff-aware, checkpointed, and emits raw plus normalized summary artifacts suitable for audit and reproducible backtests.
- 2026-04-01: Kalshi auth loading now supports `private_key_pem` or `private_key_path` with explicit precedence, clearer validation, and YAML auth overrides for historical ingest and related Kalshi CLI workflows.
- 2026-04-02: Kalshi historical ingest now applies category / excluded-series / min-volume filtering inside each paginated market fetch before raw-market writes. Retained markets can begin downstream normalization immediately while irrelevant markets are discarded in-flight, and ingest summaries now report page-level fetch/retain/discard diagnostics plus retained ticker samples.
- 2026-04-02: Kalshi authenticated live-bridge reads during historical ingest now retry `429 Too Many Requests` responses with `Retry-After` support, bounded exponential backoff plus jitter, and distinct `live/authenticated` operator logging. Historical ingest YAML now exposes separate authenticated throttling settings so recent-settled market bridging can continue through transient rate limits without changing public historical retry behavior.
- 2026-04-02: Kalshi live-bridge ingest now stops paginating once settled live pages fall entirely outside the lookback window, logs retained and discarded ticker samples per page, persists raw markets only when processing actually starts, and fails fast when retained-market fetch volume grows without any processing progress. This prevents the prior runaway behavior where the live `/markets?status=settled` cursor could traverse the wider settled universe indefinitely while top-level normalization waited for download completion.
- 2026-04-02: Kalshi historical ingest now emits structured run and stage status artifacts under `artifacts/kalshi_ingest/<run_id>/` with heartbeat-updated `ingest_status.json` plus final `ingest_run_summary.json`. Operators can now see whether the run is in initialization, checkpointing, cutoff discovery, market-universe fetch, retained-market processing, normalization, or final summary, along with page counts, retained-market progress, stop reasons, and fail-fast outcomes.
- 2026-04-02: Kalshi historical ingest checkpoints now support robust resume semantics. The checkpoint captures last completed/current stage, queued retained markets, processed tickers, failed-ticker retry metadata, pagination state, and resume counters. Operators can resume the latest interrupted run, resume from an explicit checkpoint path, or force a fresh run, and checkpoint writes now use a backup file so a corrupt primary checkpoint can still be recovered safely.
- 2026-04-02: Kalshi historical-ingest resume now hardens poisoned saved live-pagination cursors. Retryable resume-time `502/503/504` and transport failures on `/markets?status=settled&cursor=...` now use bounded retry/backoff first, then can fall back to the in-memory loaded backup checkpoint, and finally can clear only the saved live cursor while preserving processed and queued work. Status artifacts now record cursor retry counts, last HTTP status, and whether backup or cursor-reset recovery was used.
- 2026-04-02: Kalshi historical-ingest resume recovery is now operator-selectable through a single `resume_recovery_mode` contract with `automatic`, `backup_only`, `cursor_reset_only`, and `fail_fast`. CLI, YAML config, runtime ingest logic, and structured status/summary artifacts now consistently record the configured mode, attempted backup/cursor-reset recoveries, and the actual recovery action taken.
- 2026-04-02: Kalshi category-specific research ingest now has a dedicated live-filtered path. `data kalshi recent-ingest` uses the authenticated `/markets` endpoint as the primary discovery source for recent filtered markets, persists `source_endpoint` and `source_mode` into raw plus normalized artifacts, supports direct `/historical/markets/{ticker}` fetches for explicitly named older contracts, and leaves `historical-ingest` available for explicit archive crawling instead of as the default Economics/Politics research path.
- 2026-04-02: Kalshi `recent-ingest` no longer requires trade or candlestick history for a market to be retained. Valid market-only rows with core metadata plus settlement and/or pricing snapshots now write raw and normalized artifacts even when trade and candle payloads are empty, and validation treats those recent market-only datasets as passing when core fields are complete. Recent-ingest summaries now expose structured exclusion counts for missing core fields, category/series filters, lookback exclusions, and no-trade-data exclusions.
- 2026-04-02: Kalshi `recent-ingest` core-field validation now matches live `/markets` schema variability. Only `ticker` and `status` are required core fields, category and time fields are optional, and recent normalization now maps alternate time keys such as `close_date`, `expiration_ts`, and `end_date` into the normalized `close_time` field when present.
- 2026-04-02: Kalshi `recent-ingest` filter planning now defaults back to category-first research behavior when no explicit `recent_ingest_series_tickers` are configured. Series-driven runs only activate for real series values, infer compatible categories from `economics_series` / `politics_series`, ignore incompatible category filters when needed, and emit machine-readable filter-conflict diagnostics so zero-result runs are easier to explain.
- 2026-04-02: Kalshi `recent-ingest` now treats `recent_ingest_limit` as a total live `/markets` fetch budget across pagination instead of a per-page size. The fetch loop shrinks each request to the remaining budget, logs page number plus cumulative fetched counts, and stops with `recent_limit_reached` once the total fetched-record cap is hit.
- 2026-04-02: Kalshi `recent-ingest` now prioritizes higher-signal markets by supporting a recent-specific `min_volume` threshold, excluding low-signal ticker types containing `CROSSCATEGORY`, `SPORTSMULTIGAME`, or `EXTENDED`, logging exclusion reason plus volume for filtered markets, and skipping feature generation when retained markets have fewer than `min_trades` trades.
- 2026-04-02: Kalshi `recent-ingest` market-type exclusions are now configurable under `recent_ingest.exclude_market_type_patterns` and can be disabled per run with `--disable-market-type-filter`. Recent-ingest now logs whether the filter is enabled, reports `excluded_by_market_type`, and warns when all fetched markets were removed by the type filter.
- 2026-04-02: Kalshi historical-ingest category-filtered discovery now treats `/events` as the primary path. When `use_events_for_category_filter=true` and `use_direct_series_fetch=false`, the ingest skips `/historical/markets` entirely, and the new `skip_historical_pagination` flag now defaults to true in code, CLI config loading, and the checked-in Kalshi YAML.
- 2026-04-03: Binance public crypto ingestion now has a first milestone implementation under `data crypto binance ...`. The repo now supports checkpointed REST ingestion of `exchangeInfo`, `klines`, `aggTrades`, and optional `bookTicker` snapshots, plus explicit normalized crypto parquet outputs with raw-artifact provenance and grouped CLI/config coverage. Websocket incremental mode remains intentionally deferred to the next milestone.
- 2026-04-03: Binance crypto ingestion now supports bounded public websocket incremental append runs plus mixed-source projections. `data crypto binance websocket-ingest` writes checkpointed raw JSONL, deduped incremental parquet outputs, reconnect/duplicate telemetry, and automatic projection refreshes, while `data crypto binance project` rebuilds stable `crypto_ohlcv_bars`, `crypto_agg_trades`, and `crypto_top_of_book` datasets from historical REST plus websocket incremental normalized inputs.
- 2026-04-03: Binance crypto now supports bounded sync orchestration and projected-dataset feature consumption. `data crypto binance sync` composes websocket incremental ingest, projection refresh, and feature refresh into one restart-safe cycle with step-level summaries, while `data crypto binance features` publishes explicit `crypto_market_features` parquet slices plus local feature-store manifests derived only from `crypto_ohlcv_bars`, `crypto_agg_trades`, and `crypto_top_of_book`.
- 2026-04-03: Binance crypto syncs now emit scheduler-facing manifests and freshness status artifacts. `data crypto binance sync` writes a per-run manifest plus `latest_sync_manifest.json`, projected and feature freshness is materialized under `data/binance/status`, and new research readers can load `crypto_market_features` from the feature store with symbol, interval, and time filtering for replay-oriented dataset assembly.
- 2026-04-03: Binance crypto now has research-loop-ready dataset adapters plus rule-based alerts and health checks. `materialize_binance_research_dataset()` turns feature-store slices into replay-ready research parquet with explicit forward-return targets, `data crypto binance alerts` emits stale/missing/unhealthy scheduler artifacts, and `data crypto binance health-check` summarizes whether Binance datasets are fit for downstream research use.
- 2026-04-03: The shared research dataset registry now publishes additional providers beyond Binance. Kalshi and Polymarket feature directories can publish stable registry entries from their existing ingest and validation artifacts, the shared registry loader now supports directory-backed parquet datasets, and new grouped CLI flows build `latest_registry_summary.json`, `latest_monitoring_summary.json`, and `cross_provider_health_summary.json` for scheduler-facing cross-provider monitoring.
