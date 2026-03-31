# Milestones

## Status Legend

- NOT_STARTED
- IN_PROGRESS
- BLOCKED
- REVIEW_NEEDED
- DONE

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

* add ingestion adapters for equities (daily) and crypto (intraday)
* define normalized OHLCV schema
* ensure outputs are versioned and reproducible
* document ingestion pipeline and usage

Acceptance criteria:

* at least one equity and one crypto source are supported
* ingestion outputs follow a consistent schema
* artifacts are reproducible across runs
* documentation explains supported data sources and formats

Dependencies:

* none

---

## G-02 — Add multi-frequency time alignment layer

Status: DONE

Objective:
Enable safe alignment of datasets with different frequencies (e.g., daily and intraday).

Expected scope:

* implement resampling utilities
* define alignment rules (forward-fill, aggregation)
* prevent forward-looking data leakage
* add tests for mixed-frequency scenarios

Acceptance criteria:

* daily and intraday data can be combined safely
* no forward-looking leakage is introduced
* alignment behavior is documented and test-covered

Dependencies:

* G-01

---

## G-03 — Introduce feature store

Status: DONE

Objective:
Persist computed features for reuse across research, replay, and paper trading.

Expected scope:

* define feature storage schema
* implement read/write interface
* support feature versioning
* document feature lifecycle

Acceptance criteria:

* features can be stored and retrieved deterministically
* feature versioning is supported
* redundant recomputation is reduced
* documentation explains usage

Dependencies:

* G-01

---

## G-04 — Add data quality validation layer

Status: DONE

Objective:
Ensure integrity and reliability of ingested data.

Expected scope:

* detect missing data
* detect outliers
* validate schema consistency
* add validation reports

Acceptance criteria:

* data validation checks run on ingestion
* issues are surfaced in structured outputs
* validation logic is documented and test-covered

Dependencies:

* G-01

---

## H-01 — Expand candidate grid generation

Status: DONE

Objective:
Extend candidate generation to produce broader parameter and signal variations.

Acceptance criteria:

* multiple variants per signal family are generated
* outputs remain compatible with promotion pipeline
* documentation explains configuration options

Dependencies:

* none

---

## H-02 — Introduce automated research loop

Status: DONE

Objective:
Enable the system to autonomously generate, evaluate, and promote strategies.

Expected scope:

* orchestrate candidate generation, evaluation, and promotion
* support scheduled or continuous runs
* persist outputs for inspection

Acceptance criteria:

* research loop can run unattended
* candidates flow through full lifecycle
* outputs are reproducible and logged

Dependencies:

* H-01
* B-02

---

## H-03 — Add research resource allocation

Status: DONE

Objective:
Allocate compute resources dynamically across candidate strategies.

Expected scope:

* prioritize promising candidates
* implement early stopping for weak candidates
* define allocation rules

Acceptance criteria:

* compute usage is biased toward higher-quality candidates
* weak candidates are pruned early
* behavior is documented and test-covered

Dependencies:

* H-02

---

## H-04 — Add experiment tracking system

Status: DONE

Objective:
Track experiments, configurations, and results in a structured and queryable format.

Expected scope:

* define experiment metadata schema
* persist configurations and outputs
* enable querying of past runs

Acceptance criteria:

* experiments are reproducible
* configs and outputs are linked
* documentation explains tracking system

Dependencies:

* H-01

---

## I-01 — Introduce execution simulator

Status: DONE

Objective:
Simulate realistic trade execution behavior.

Expected scope:

* model slippage and spread
* simulate partial fills
* incorporate latency assumptions
* add tests for execution scenarios

Acceptance criteria:

* execution simulation produces realistic outcomes
* results integrate with existing decision pipeline
* behavior is documented and test-covered

Dependencies:

* D-02

---

## I-02 — Add transaction cost modeling

Status: DONE

Objective:
Improve accuracy of expected cost estimates in trade evaluation.

Expected scope:

* define cost model components (spread, fees, slippage)
* integrate with EV calculations
* support configurable parameters

Acceptance criteria:

* costs are explicitly modeled and reported
* EV reflects realistic cost assumptions
* tests validate cost calculations

Dependencies:

* I-01
* A-03

---

## I-03 — Introduce broker/exchange abstraction layer

Status: DONE

Objective:
Create a unified interface for execution across paper and future live environments.

Expected scope:

* define broker interface
* support paper execution adapter
* allow extension to real exchanges

Acceptance criteria:

* execution layer is decoupled from strategy logic
* at least one adapter (paper) is implemented
* interface is documented

Dependencies:

* D-02

---

## I-04 — Add intraday execution support

Status: NOT_STARTED

Objective:
Support higher-frequency decision and execution cycles.

Expected scope:

* enable intraday scheduling
* handle rapid decision generation
* ensure compatibility with execution simulator

Acceptance criteria:

* system can process intraday data without failure
* execution pipeline supports frequent updates
* behavior is documented

Dependencies:

* G-01
* I-01

---

## J-01 — Introduce real-time KPI monitoring

Status: NOT_STARTED

Objective:
Track key system and trading metrics continuously.

Expected scope:

* define KPI set (PnL, drawdown, exposure, EV vs realized)
* implement monitoring outputs
* support dashboard integration

Acceptance criteria:

* KPIs are updated in near real-time
* outputs are structured and queryable
* documentation explains metrics

Dependencies:

* E-01

---

## J-02 — Add system health monitoring

Status: NOT_STARTED

Objective:
Detect failures or anomalies in system operation.

Expected scope:

* monitor data freshness
* detect pipeline failures
* surface warnings and errors

Acceptance criteria:

* system health issues are detectable and logged
* alerts or flags are generated
* behavior is documented

Dependencies:

* G-01
* D-01

---

## J-03 — Add risk controls / kill switch

Status: NOT_STARTED

Objective:
Prevent catastrophic losses through automated safeguards.

Expected scope:

* define drawdown limits
* define abnormal behavior triggers
* implement trading halt mechanism

Acceptance criteria:

* system can halt trading under defined conditions
* triggers are configurable
* behavior is documented and test-covered

Dependencies:

* J-01

---

## J-04 — Add drift detection

Status: NOT_STARTED

Objective:
Detect degradation in model or strategy performance.

Expected scope:

* compare expected vs realized performance
* track statistical deviations
* generate drift signals

Acceptance criteria:

* drift conditions are detectable
* outputs are structured and explainable
* documentation explains methodology

Dependencies:

* K-01

---

## K-01 — Introduce trade outcome attribution

Status: NOT_STARTED

Objective:
Compare predicted outcomes with realized results and explain discrepancies.

Expected scope:

* compute realized vs expected return
* decompose error sources (alpha, cost, timing)
* integrate with KPI outputs

Acceptance criteria:

* attribution results are structured and inspectable
* discrepancies are explainable
* documentation explains methodology

Dependencies:

* A-02
* E-01

---

## K-02 — Add calibration pipeline

Status: NOT_STARTED

Objective:
Improve reliability and confidence scoring over time.

Expected scope:

* update calibration models based on outcomes
* adjust confidence scores
* track calibration metrics

Acceptance criteria:

* calibration improves prediction reliability
* outputs are measurable and tracked
* behavior is documented

Dependencies:

* K-01

---

## K-03 — Add strategy decay detection

Status: NOT_STARTED

Objective:
Identify strategies that are losing effectiveness.

Expected scope:

* monitor performance degradation
* define decay thresholds
* flag affected strategies

Acceptance criteria:

* decay conditions are detectable
* outputs are structured
* documentation explains thresholds

Dependencies:

* J-04

---

## K-04 — Add auto-demotion / retraining loop

Status: NOT_STARTED

Objective:
Automatically remove or retrain underperforming strategies.

Expected scope:

* integrate with promotion/demotion logic
* trigger retraining workflows
* log actions taken

Acceptance criteria:

* weak strategies are demoted automatically
* retraining can be triggered programmatically
* behavior is documented

Dependencies:

* K-03
* B-02

---

## L-01 — Add trade explorer payload

Status: NOT_STARTED

Objective:
Expose decision and outcome data for per-trade inspection.

Acceptance criteria:

* payload supports drill-down
* includes rationale, EV, and outcome data
* format is dashboard-ready

Dependencies:

* E-02

---

## L-02 — Add strategy health payload

Status: NOT_STARTED

Objective:
Expose strategy-level performance summaries.

Acceptance criteria:

* includes return, risk, and reliability metrics
* supports trend analysis
* format is dashboard-ready

Dependencies:

* E-03

---

## L-03 — Add portfolio visualization payload

Status: NOT_STARTED

Objective:
Expose portfolio allocation and exposure data.

Acceptance criteria:

* allocation data is structured
* constraints and exposures are visible
* format supports visualization

Dependencies:

* C-04

---

## L-04 — Add decision flow visualization payload

Status: NOT_STARTED

Objective:
Expose decision pipeline stages for visualization.

Acceptance criteria:

* shows signal → filters → veto → allocation → execution
* data is structured and traceable
* documentation explains flow

Dependencies:

* A-05
* C-04

---

## L-05 — Add KPI dashboard backend API

Status: NOT_STARTED

Objective:
Provide backend services for dashboard consumption.

Acceptance criteria:

* API serves KPI, trade, and strategy data
* supports filtering and drill-down
* documentation explains endpoints

Dependencies:

* E-01

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
Any milestone that changes trading behavior, promotion logic, or portfolio allocation should be reviewed carefully before merge.
