# Documentation / Build Log

This file is the running implementation log for milestone work.

Each completed or partially completed milestone should append a new section using the template below.

---

## Entry Template

### [MILESTONE_ID] - [MILESTONE_TITLE]
Date: YYYY-MM-DD
Status: DONE | REVIEW_NEEDED | BLOCKED

#### Summary
[Short summary of what changed.]

#### Why
[Why this milestone was implemented and how it fits the roadmap.]

#### Files Changed
- path/to/file
- path/to/file

#### Tests Run
- `pytest ...`
- `python -m ...`

#### Verification Commands
- `...`
- `...`

#### Design Notes
[Important implementation details, assumptions, or trade-offs.]

#### Known Issues / Limitations
- [item]
- [item]

#### Recommended Next Milestone
- [MILESTONE_ID] - [title]

---

### A-01 - Introduce TradeDecision domain model
Date: 2026-03-30
Status: DONE

#### Summary
Added a first-class `TradeDecision` domain model to the decision journal layer as an additive contract. The new model supports deterministic serialization, round-trip construction, and explicit optional fields for confidence, reliability, veto reasons, and metadata.

#### Why
This milestone establishes the canonical structured decision contract requested by the roadmap so later milestones can emit replay/paper decision records without relying on loosely structured dictionaries.

#### Files Changed
- `src/trading_platform/decision_journal/models.py`
- `src/trading_platform/decision_journal/__init__.py`
- `tests/test_decision_journal_models.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\models.py src\trading_platform\decision_journal\__init__.py tests\test_decision_journal_models.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal_models.py tests\test_db_layer.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\models.py src\trading_platform\decision_journal\__init__.py tests\test_decision_journal_models.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal_models.py tests\test_db_layer.py`

#### Design Notes
Kept the change additive: `TradeDecisionRecord` remains intact. `TradeDecision` normalizes `veto_reasons` and `metadata` for deterministic output and provides `from_dict()` for contract round-trips.

#### Known Issues / Limitations
- `tests/test_paper_run_db_integration.py` currently fails with an unrelated Alembic SQLite duplicate-column migration error on `promoted_strategies.condition_id`. That path was not modified for A-01.
- The new `TradeDecision` model is not yet wired into replay/paper producers; that is intended for later milestones.
- `TradeDecision.from_dict()` currently surfaces missing required fields as `KeyError`; that is an intentional v1 contract behavior for A-01 and can be refined in a later milestone if needed.

#### Recommended Next Milestone
- A-02 - Emit TradeDecision records from replay/research evaluation

### A-02 - Emit TradeDecision records from replay/research evaluation
Date: 2026-03-30
Status: DONE

#### Summary
Added a versioned `TradeDecision` contract emission path to the paper/replay artifact flow. The paper artifact writer now emits `trade_decision_contracts_v1.json` and `trade_decision_contracts_v1.csv` alongside the existing decision journal outputs, using the replay/evaluation candidate rows and EV diagnostics already produced by the paper pipeline.

#### Why
This milestone makes replay/evaluation output machine-readable at the trade-decision level so later milestones can consume structured records instead of only aggregate summaries.

#### Files Changed
- `src/trading_platform/decision_journal/service.py`
- `src/trading_platform/paper/service.py`
- `tests/test_decision_journal.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\service.py src\trading_platform\paper\service.py tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_daily_trading.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\service.py src\trading_platform\paper\service.py tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_daily_trading.py`

#### Design Notes
The new emission path is additive. Existing `trade_decisions.json/csv` outputs remain unchanged. The new artifact uses `TradeDecision` as the row contract and includes schema versioning in the filename and per-row metadata for reproducibility.

#### Known Issues / Limitations
- The new artifact is currently emitted from the paper evaluation artifact path only.
- `TradeDecision.side` is derived from the candidate row context, which is sufficient for structured logging but not yet a full canonical order-side contract.
- The contract is currently built from existing candidate and EV diagnostics; it does not change execution or trading semantics.

#### Recommended Next Milestone
- A-03 - Add EV decomposition fields

### A-03 - Add EV decomposition fields
Date: 2026-03-30
Status: DONE

#### Summary
Tightened the `TradeDecision` contract emission path so expected value decomposition is assembled explicitly from available EV inputs instead of silently collapsing gross, cost, and net fields together. The decision-contract metadata now records whether each EV component came directly from prediction inputs, candidate-level fields, or a derived fallback.

#### Why
This milestone standardizes EV decomposition semantics for dashboard-facing trade decisions. It preserves current trading behavior while making it clear when the system has explicit EV components versus when it had to derive them from partial inputs.

#### Files Changed
- `src/trading_platform/decision_journal/service.py`
- `tests/test_decision_journal.py`
- `tests/test_decision_journal_models.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\service.py tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_daily_trading.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\service.py tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_daily_trading.py`

#### Design Notes
The implementation is additive. `predicted_return` remains the primary trade-return forecast field, while `expected_value_gross`, `expected_cost`, and `expected_value_net` are now resolved through an explicit helper with provenance metadata such as `expected_value_net_source` and `ev_decomposition_status`. When upstream rows do not provide the full decomposition, the contract records that the missing values were derived rather than pretending they were directly supplied.

#### Known Issues / Limitations
- Prediction lookup in `build_trade_decision_contracts()` remains keyed by symbol, so same-symbol multi-strategy rows still rely on upstream symbol uniqueness within a batch.
- Candidate rows without direct EV decomposition still fall back to derived values; the improvement in A-03 is explicit provenance, not broader upstream model changes.
- This milestone does not change EV gating, ranking, sizing, or execution behavior.

#### Recommended Next Milestone
- A-04 - Add reliability, uncertainty, and calibration fields to decisions

### A-04 - Add reliability, uncertainty, and calibration fields to decisions
Date: 2026-03-30
Status: DONE

#### Summary
Extended `TradeDecision` with additive optional quality fields for `probability_positive`, `uncertainty_score`, and `calibration_score`, while tightening source resolution for confidence and reliability. The decision-contract producer now emits these fields with clean `None` semantics when upstream data is unavailable and records source metadata for each value.

#### Why
This milestone makes structured trade decisions more useful for explainability and future dashboard drill-downs by representing confidence, reliability, uncertainty, and calibration in a first-class way instead of leaving them buried in ad hoc candidate metadata.

#### Files Changed
- `src/trading_platform/decision_journal/models.py`
- `src/trading_platform/decision_journal/service.py`
- `tests/test_decision_journal.py`
- `tests/test_decision_journal_models.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\models.py src\trading_platform\decision_journal\service.py tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_daily_trading.py tests\test_paper_trading_service.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\models.py src\trading_platform\decision_journal\service.py tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_daily_trading.py tests\test_paper_trading_service.py`

#### Design Notes
The contract change is backward-compatible: all new fields are optional. `confidence_score` continues to prefer `ev_confidence`, `reliability_score` continues to prefer `ev_reliability`, `uncertainty_score` is sourced from residual-std diagnostics when present, and `calibration_score` prefers the calibrated reliability signal before falling back to probability-style inputs. Metadata now records the exact source used for each quality field.

#### Known Issues / Limitations
- `calibration_score` currently represents the best per-decision calibrated quality signal already available in the candidate row, not a new dedicated EV calibration model output.
- `uncertainty_score` is currently tied to regression residual-std diagnostics when present; non-regression paths may legitimately leave it unset.
- This milestone extends structured output only and does not change EV gating, ranking, sizing, or execution behavior.

#### Recommended Next Milestone
- A-05 - Add veto and rationale logging

### A-05 - Add veto and rationale logging
Date: 2026-03-30
Status: DONE

#### Summary
Extended `TradeDecision` with additive structured rationale fields so decision artifacts now carry deterministic `rationale_labels` and `rationale_context` alongside the existing `veto_reasons` and `rationale_summary`. The producer path now assembles no-veto, single-veto, and multi-veto cases explicitly without changing trading behavior.

#### Why
This milestone makes trade-decision explanations more machine-readable and dashboard-friendly. Rejected decisions can now be inspected structurally through veto counts, labeled rationale tags, and normalized rationale context instead of relying only on a single summary string.

#### Files Changed
- `src/trading_platform/decision_journal/models.py`
- `src/trading_platform/decision_journal/service.py`
- `tests/test_decision_journal.py`
- `tests/test_decision_journal_models.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\models.py src\trading_platform\decision_journal\service.py tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\models.py src\trading_platform\decision_journal\service.py tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_decision_journal_models.py tests\test_paper_artifacts_with_fills.py`

#### Design Notes
The change is additive. Existing `veto_reasons` and `rationale_summary` remain in place, while `rationale_labels` and `rationale_context` provide deterministic drill-down structure for dashboards and audits. The producer derives rationale labels from status/outcome/stage/action fields and only appends veto reasons that are not already present, keeping output stable and de-duplicated.

#### Known Issues / Limitations
- The rationale structure still reflects upstream candidate-row semantics; it does not introduce a new governance taxonomy or modify decision logic.
- `rationale_context` intentionally captures only a small stable subset of explanation fields to avoid turning the contract into an unbounded dump of candidate metadata.
- This milestone does not change promotion, sizing, allocation, risk, or execution behavior.

#### Recommended Next Milestone
- B-01 - Introduce StrategyScorecard domain model

### B-01 - Introduce StrategyScorecard domain model
Date: 2026-03-30
Status: DONE

#### Summary
Added a first-class `StrategyScorecard` governance contract for comparing strategy candidates using a deterministic, typed, machine-readable schema. The new model supports round-trip serialization, normalized list fields, and optional readiness/rejection metadata without changing any promotion behavior.

#### Why
This milestone establishes the standardized scorecard object required for B-series governance work so later gate evaluation milestones can consume an explicit contract instead of ad hoc dictionaries or raw leaderboard rows.

#### Files Changed
- `src/trading_platform/governance/models.py`
- `src/trading_platform/governance/__init__.py`
- `tests/test_governance_models.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py tests\test_governance_models.py tests\test_governance_registry.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_governance_registry.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py tests\test_governance_models.py tests\test_governance_registry.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_governance_registry.py`

#### Design Notes
The scorecard is additive and isolated to the governance model layer. It includes core comparison fields suggested by the milestone, normalizes `readiness_flags` and `rejection_reasons` deterministically, and leaves all promotion thresholds and selection logic untouched.

#### Known Issues / Limitations
- `StrategyScorecard` is introduced as a standalone contract only; existing leaderboard and promotion flows do not yet emit it.
- The model keeps `training_period` and `validation_period` as strings to avoid broad date-shape decisions before downstream producers exist.
- B-02 will need a human-reviewed decision on whether the promotion gate engine wraps or replaces existing threshold logic.

#### Recommended Next Milestone
- B-02 - Build promotion gate engine

### B-02 - Build promotion gate engine
Date: 2026-03-30
Status: DONE

#### Summary
Added an explicit promotion gate evaluation layer around the existing `alpha_lab` threshold-driven promotion path. The promotion function now emits structured named gate results, a machine-readable gate summary, and explicit passed/failed gate name lists while preserving the existing `promotion_status` and `rejection_reason` behavior.

#### Why
This milestone formalizes the current promotion checks into explicit gates so promotion becomes inspectable and machine-checkable without intentionally changing threshold values or policy semantics.

#### Files Changed
- `src/trading_platform/governance/models.py`
- `src/trading_platform/governance/__init__.py`
- `src/trading_platform/research/alpha_lab/promotion.py`
- `tests/test_governance_models.py`
- `tests/test_alpha_lab_promotion.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py src\trading_platform\research\alpha_lab\promotion.py tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py::test_apply_promotion_rules_adds_expected_rejection_reasons`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py src\trading_platform\research\alpha_lab\promotion.py tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py::test_apply_promotion_rules_adds_expected_rejection_reasons`

#### Design Notes
The implementation is intentionally narrow. It introduces `PromotionGateResult` and `PromotionGateEvaluation` as additive contracts in the governance layer, and the existing `apply_promotion_rules()` path now maps each current threshold check to a named gate while deriving `promotion_status` and `rejection_reason` from the same ordered reason codes as before. This milestone does not attempt to unify other promotion entrypoints.

#### Known Issues / Limitations
- The explicit gate engine currently wraps only the `alpha_lab` promotion path; registry and other promotion flows still use their existing logic.
- Machine-readable gate payloads are stored in DataFrame columns as lists/dicts, which is additive but may need downstream normalization in later milestones.
- This milestone preserves current threshold semantics to the extent covered by focused alignment tests, but broader end-to-end promotion workflows were intentionally not refactored here.

#### Recommended Next Milestone
- B-03 - Add PromotionDecision contract

### B-03 - Add PromotionDecision contract
Date: 2026-03-30
Status: DONE

#### Summary
Added a first-class `PromotionDecision` governance contract and emitted it additively from the existing `alpha_lab` promotion path. The new payload captures final promotion outcome, structured gate results, failed/passed gate names, rejection reasons, and deterministic summary decision metadata without intentionally changing promotion thresholds or decision semantics.

#### Why
The gate engine from B-02 made threshold checks explicit, but the final promotion outcome was still spread across legacy DataFrame columns. This milestone adds one typed machine-readable contract that can be serialized, round-tripped, and consumed by downstream governance and reporting paths while preserving existing `promotion_status` and `rejection_reason` behavior.

#### Files Changed
- `src/trading_platform/governance/models.py`
- `src/trading_platform/governance/__init__.py`
- `src/trading_platform/research/alpha_lab/promotion.py`
- `tests/test_governance_models.py`
- `tests/test_alpha_lab_promotion.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py src\trading_platform\research\alpha_lab\promotion.py tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py::test_apply_promotion_rules_adds_expected_rejection_reasons`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py src\trading_platform\research\alpha_lab\promotion.py tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_alpha_lab_promotion.py tests\test_alpha_lab.py::test_apply_promotion_rules_adds_expected_rejection_reasons`

#### Design Notes
`PromotionDecision` is additive and intentionally narrow. It wraps the already-computed promotion gate evaluation into a typed decision contract with deterministic serialization and round-trip behavior. The `alpha_lab` producer path now emits a `promotion_decision` payload column while keeping the legacy `promotion_status`, `rejection_reason`, gate summary, and gate result columns intact.

#### Known Issues / Limitations
- The contract is currently emitted only by the `alpha_lab` promotion path.
- The repo milestone text had drifted; this entry reflects the user-scoped B-03 contract milestone implemented in this session.
- Downstream consumers still read legacy columns unless they are explicitly upgraded to consume `promotion_decision`.

#### Recommended Next Milestone
- B-04 - Add live-readiness gate skeleton

### B-04 - Add live-readiness gate skeleton
Date: 2026-03-30
Status: DONE

#### Summary
Added typed live-readiness governance scaffolding in the governance model layer. The new `LiveReadinessCheckResult` and `LiveReadinessDecision` contracts are machine-readable, deterministic, and serializable, and `build_live_readiness_skeleton()` emits a default not-ready decision that makes it explicit that no live trading path is enabled by default.

#### Why
This milestone establishes governance scaffolding for future live-readiness evaluation without changing promotion semantics or enabling execution. The skeleton captures the required readiness dimensions the roadmap already points to: monitoring, reconciliation, execution support, capital controls, risk controls, and operator approval.

#### Files Changed
- `src/trading_platform/governance/models.py`
- `src/trading_platform/governance/__init__.py`
- `tests/test_governance_models.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py tests\test_governance_models.py tests\test_live_execution_control.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_live_execution_control.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\models.py src\trading_platform\governance\__init__.py tests\test_governance_models.py tests\test_live_execution_control.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_governance_models.py tests\test_live_execution_control.py`

#### Design Notes
The implementation is intentionally narrow. `LiveReadinessDecision` is a governance contract only; it does not change live execution code paths, approvals, risk checks, or broker behavior. The skeleton builder always emits `ready_for_live=False` and `live_trading_enabled=False`, making the absence of live enablement explicit rather than implicit.

#### Known Issues / Limitations
- No producer path emits live-readiness decisions yet; this milestone adds scaffolding only.
- The live-readiness decision is intentionally conservative and does not infer readiness from existing live dry-run artifacts.
- Promotion semantics and live execution behavior are preserved because this milestone does not integrate into those paths.

#### Recommended Next Milestone
- C-01 - Normalize strategy outputs into a shared portfolio input contract

### C-01 - Normalize strategy outputs into a shared portfolio input contract
Date: 2026-03-30
Status: DONE

#### Summary
Added a shared typed `StrategyPortfolioInput` contract in the portfolio layer and used it to normalize per-sleeve strategy outputs before multi-strategy allocation. The allocator now emits deterministic `strategy_input_rows` in memory and a `strategy_portfolio_inputs.json` artifact on disk while preserving the existing allocation behavior and outputs.

#### Why
The multi-strategy allocator previously consumed sleeve outputs through internal ad hoc bundle fields only. This milestone introduces a first-class machine-readable portfolio-facing contract so downstream portfolio logic has a consistent normalized representation regardless of strategy family or target-construction source.

#### Files Changed
- `src/trading_platform/portfolio/contracts.py`
- `src/trading_platform/portfolio/multi_strategy.py`
- `tests/test_multi_strategy_allocation.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Design Notes
The normalized contract is additive and intentionally narrow. It wraps existing sleeve outputs without changing target-generation logic, overlap handling, or portfolio constraints. The allocator still computes weights the same way, but it now has an explicit family-agnostic contract available for later conflict, exposure, and diagnostics milestones.

#### Known Issues / Limitations
- The normalized input contract currently wraps the existing sleeve bundles rather than replacing them outright.
- Downstream consumers still primarily use the existing allocation artifacts unless they are updated to consume `strategy_portfolio_inputs.json`.
- This milestone does not change conflict resolution or exposure rules; it only standardizes the portfolio input shape.

#### Recommended Next Milestone
- C-02 - Add strategy conflict resolution logic

### C-02 - Add strategy conflict resolution logic
Date: 2026-03-30
Status: DONE

#### Summary
Formalized the existing multi-strategy overlap and netting behavior into typed `ConflictResolutionRecord` contracts and additive `conflict_resolution_rows` output. The allocator now emits a deterministic conflict-resolution report that records whether symbols were passed through, combined in the same direction, or netted across opposing sleeves.

#### Why
The allocator already had conflict behavior, but it was only inferable from overlap rows and final weights. This milestone makes the resolution rule explicit and machine-readable without changing the underlying netting policy.

#### Files Changed
- `src/trading_platform/portfolio/contracts.py`
- `src/trading_platform/portfolio/multi_strategy.py`
- `tests/test_multi_strategy_allocation.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Design Notes
This is a formalization milestone. Opposing sleeves are still netted and same-direction sleeves are still combined exactly as before; the difference is that the rule used is now explicit in a typed contract and artifact.

#### Known Issues / Limitations
- Conflict resolution remains symbol-level; bucket-level or factor-level conflict policy is still future work.
- The current implementation formalizes the existing policy rather than introducing configurable alternative resolution strategies.

#### Recommended Next Milestone
- C-03 - Add exposure budgets and concentration rules

### C-03 - Add exposure budgets and concentration rules
Date: 2026-03-30
Status: DONE

#### Summary
Formalized the allocator’s existing exposure and concentration controls into typed `ExposureConstraintDecision` contracts and additive `exposure_constraint_rows` output. The allocator now records both concrete binding actions and monitor-style portfolio budget rows for symbol concentration, position caps, gross leverage, and net exposure.

#### Why
The portfolio layer already enforced these limits, but the constraint effects were scattered across summary metrics and clip rows. This milestone makes the controls and their binding status inspectable in a dedicated machine-readable form without changing the configured rules.

#### Files Changed
- `src/trading_platform/portfolio/contracts.py`
- `src/trading_platform/portfolio/multi_strategy.py`
- `tests/test_multi_strategy_allocation.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Design Notes
This milestone wraps the existing caps and scaling steps rather than redesigning them. The underlying exposure policy remains unchanged; the allocator simply emits structured constraint decisions that are easier to audit and reuse.

#### Known Issues / Limitations
- Constraint decisions currently reflect the existing symbol and portfolio caps only; richer budget hierarchies would require a future policy milestone.
- Sector caps still depend on the configured group map and existing group-cap logic.

#### Recommended Next Milestone
- C-04 - Add allocation diagnostics and rationale outputs

### C-04 - Add allocation diagnostics and rationale outputs
Date: 2026-03-30
Status: DONE

#### Summary
Added typed `AllocationRationaleRecord` contracts and additive `allocation_rationale_rows` output to explain final per-symbol portfolio weights. The rationale rows combine normalized strategy-input provenance, conflict-resolution outcomes, and symbol-level constraint actions into a dashboard-friendly explanation layer.

#### Why
The allocator already emitted enough raw data to infer why a symbol received its final target, but not in one inspectable object. This milestone turns those effects into explicit rationale outputs without changing allocation math.

#### Files Changed
- `src/trading_platform/portfolio/contracts.py`
- `src/trading_platform/portfolio/multi_strategy.py`
- `tests/test_multi_strategy_allocation.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\portfolio\contracts.py src\trading_platform\portfolio\multi_strategy.py tests\test_multi_strategy_allocation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_multi_strategy_allocation.py`

#### Design Notes
The new rationale rows are derived from the allocator’s existing normalized inputs, conflict records, and constraint rows. They are additive and intended for diagnostics and dashboard consumption; they do not participate in weight calculation.

#### Known Issues / Limitations
- Rationale rows currently focus on per-symbol effects and do not yet include higher-level sleeve trade-off narratives.
- Portfolio-wide constraint rationale is available through exposure-constraint rows rather than repeated on every symbol row.

#### Recommended Next Milestone
- D-01 - Introduce persistent paper state model

### D-01 - Introduce persistent paper state model
Date: 2026-03-30
Status: DONE

#### Summary
Added an explicit `PersistentPaperState` contract for the on-disk paper portfolio snapshot and updated `JsonPaperStateStore` to read and write that contract. The store now handles missing files, corrupt JSON, and partial payloads defensively while still restoring the runtime `PaperPortfolioState` used by the paper trading path.

#### Why
The repo already had persistent paper state behavior, but the storage contract was implicit inside the service layer. This milestone makes the persisted shape explicit, versioned, and round-trip testable so paper state can survive restarts more safely and future reconciliation work has a stable base contract.

#### Files Changed
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `tests/test_paper_state_model.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\paper\models.py src\trading_platform\paper\service.py tests\test_paper_state_model.py tests\test_paper_trading_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_paper_state_model.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\paper\models.py src\trading_platform\paper\service.py tests\test_paper_state_model.py tests\test_paper_trading_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_paper_state_model.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Design Notes
The persisted state contract is additive and intentionally scoped to storage. Runtime paper trading still operates on `PaperPortfolioState`; the new contract only standardizes serialization, schema versioning, and defensive restoration from partial or corrupt on-disk payloads.

#### Known Issues / Limitations
- Corrupt or malformed persisted rows are skipped conservatively rather than surfaced as a richer diagnostics payload.
- The store still uses a single JSON snapshot file; journaling or atomic-rename durability improvements would be future work.
- This milestone does not introduce broker reconciliation or order lifecycle state beyond the existing paper portfolio snapshot.

#### Recommended Next Milestone
- D-02 - Add order lifecycle domain objects

### D-02 - Add order lifecycle domain objects
Date: 2026-03-30
Status: DONE

#### Summary
Added explicit typed order lifecycle contracts for paper and future execution flows. The new models cover order intent, submitted order, fill records, cancellations, status events, and an aggregate lifecycle record with deterministic serialization and round-trip behavior.

#### Why
This milestone creates the machine-readable order lifecycle layer that later reconciliation and dashboard milestones can build on without depending on broker-specific payloads or ad hoc paper-order dictionaries.

#### Files Changed
- `src/trading_platform/execution/order_lifecycle.py`
- `tests/test_order_lifecycle.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\order_lifecycle.py src\trading_platform\execution\reconciliation.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_order_lifecycle.py tests\test_reconciliation.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\order_lifecycle.py src\trading_platform\execution\reconciliation.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_order_lifecycle.py tests\test_reconciliation.py`

#### Design Notes
The contract layer is additive and intentionally paper-first. `build_paper_order_lifecycle_records()` derives lifecycle records from existing `PaperOrder` and `BrokerFill` objects without introducing broker integration or changing order generation behavior.

#### Known Issues / Limitations
- The lifecycle builder currently keys fills by symbol because paper fills do not yet expose stable order identifiers.
- Cancellation and rejection records are modeled but not yet emitted by the current paper execution path.
- This milestone introduces contracts only; it does not change execution semantics.

#### Recommended Next Milestone
- D-03 - Add reconciliation engine skeleton

### D-03 - Add reconciliation engine skeleton
Date: 2026-03-30
Status: DONE

#### Summary
Added a typed reconciliation skeleton that compares intended targets, order lifecycle records, and realized paper positions. The new result contract emits structured mismatches and diagnostics suitable for inspection and future operational workflows.

#### Why
This milestone establishes the first machine-readable reconciliation layer so the paper and future live paths can reason explicitly about missing lifecycle records, unfilled intents, and unexpected realized positions instead of relying on implicit state comparisons.

#### Files Changed
- `src/trading_platform/execution/reconciliation.py`
- `tests/test_order_lifecycle.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\order_lifecycle.py src\trading_platform\execution\reconciliation.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_order_lifecycle.py tests\test_reconciliation.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\order_lifecycle.py src\trading_platform\execution\reconciliation.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_order_lifecycle.py tests\test_reconciliation.py`

#### Design Notes
The reconciliation layer is a scaffold, not a full engine. `build_order_lifecycle_reconciliation_skeleton()` focuses on a narrow set of structured mismatch types while leaving current rebalance-order generation logic intact.

#### Known Issues / Limitations
- The skeleton does not yet reconcile prices, partial fills versus target quantities, or lot-level realized PnL.
- Reconciliation currently targets paper-state artifacts only and does not read broker-side state.
- Additional mismatch taxonomies will be needed once richer lifecycle events are emitted.

#### Recommended Next Milestone
- D-04 - Refactor paper path toward shared decision pipeline

### D-04 - Refactor paper path toward shared decision pipeline
Date: 2026-03-30
Status: DONE

#### Summary
Refactored the paper trading path so shared decision objects are built once during the paper run and carried on the run result. The paper artifact writer now serializes prebuilt `TradeDecision` contracts, order lifecycle records, and reconciliation results instead of rebuilding them from raw diagnostics at write time.

#### Why
This milestone reduces divergence between the paper runtime path and the shared decision-contract path. It reuses the existing `TradeDecision` model plus the new D-02 and D-03 contracts so downstream paper artifacts reflect the same structured decision pipeline the run itself produced.

#### Files Changed
- `src/trading_platform/decision_journal/service.py`
- `src/trading_platform/execution/reconciliation.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `tests/test_decision_journal.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\service.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\execution\reconciliation.py tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\decision_journal\service.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\execution\reconciliation.py tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_decision_journal.py tests\test_paper_artifacts_with_fills.py tests\test_order_lifecycle.py tests\test_reconciliation.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Design Notes
The change is additive. `PaperTradingRunResult` now carries `trade_decision_contracts`, `order_lifecycle_records`, and an `OrderLifecycleReconciliationResult`, while the artifact writer keeps a compatibility fallback for older or manually constructed results that do not populate those fields.

#### Known Issues / Limitations
- The paper reconciliation output is still a scaffold and intentionally limited to a small mismatch taxonomy.
- The lifecycle builder still infers fill ownership by symbol because paper fills do not yet carry stable order identifiers.
- This milestone reduces pipeline drift but does not yet unify the live preview path onto the same richer paper result contract.

#### Recommended Next Milestone
- E-01 - Introduce KPI schema / warehouse contract

### E-01 - Introduce KPI schema / warehouse contract
Date: 2026-03-30
Status: DONE

#### Summary
Added a typed KPI payload contract for warehouse-friendly metric rows and wired paper artifacts to emit `kpi_payload.json` plus a flat `kpi_records.csv`. The KPI schema captures portfolio, trade-decision, and strategy-level metrics in a consistent machine-readable row format.

#### Why
This milestone establishes a stable reporting contract for downstream warehousing and dashboards without changing how paper trading calculates PnL, execution costs, or strategy attribution.

#### Files Changed
- `src/trading_platform/reporting/dashboard_payloads.py`
- `src/trading_platform/paper/service.py`
- `tests/test_reporting_dashboard_payloads.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\dashboard_payloads.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\dashboard_payloads.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Design Notes
The KPI contract is intentionally row-oriented and additive. Each `KpiRecord` carries scope, entity identity, metric name, metric value, optional tags, dimensions, and metadata so warehouse ingestion does not need to infer semantics from file names alone.

#### Known Issues / Limitations
- The current KPI producer is paper-focused and does not yet aggregate replay or live-run directories.
- KPI units are conservative (`usd`, `count`, `ratio`, `scalar`) and may need expansion once dashboard consumers standardize broader metric taxonomies.

#### Recommended Next Milestone
- E-02 - Add trade explorer payloads

### E-02 - Add trade explorer payloads
Date: 2026-03-30
Status: DONE

#### Summary
Added a typed trade explorer payload contract and paper artifact outputs for `trade_explorer_payload.json` and `trade_explorer_rows.csv`. The payload joins shared `TradeDecision` records, order lifecycle state, reconciliation status, and realized trade fields into one drill-down-friendly row per symbol/trade context.

#### Why
This milestone creates a stable, dashboard-ready trade exploration shape so trade-level inspection no longer depends on manually stitching together decision contracts, lifecycle rows, attribution trades, and mismatch reports.

#### Files Changed
- `src/trading_platform/reporting/dashboard_payloads.py`
- `src/trading_platform/paper/service.py`
- `tests/test_reporting_dashboard_payloads.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\dashboard_payloads.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\dashboard_payloads.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Design Notes
The payload stays additive and deterministic. `TradeExplorerRow` preserves structured veto data, expected-value fields, lifecycle status, and reconciliation status, while tolerating partial upstream data by falling back cleanly to whichever source currently exists.

#### Known Issues / Limitations
- The current producer merges by symbol for paper-mode artifacts because paper fills and attribution rows do not yet have a richer order-to-trade linkage model.
- Closed-trade entry/exit fields remain sparse unless attribution trade rows are available.

#### Recommended Next Milestone
- E-03 - Add strategy health payloads

### E-03 - Add strategy health payloads
Date: 2026-03-30
Status: DONE

#### Summary
Added a typed strategy health payload contract and paper artifact outputs for `strategy_health_payload.json` and `strategy_health_payload.csv`. The payload summarizes per-strategy PnL, costs, turnover, win rate, decision counts, veto counts, mismatch counts, and average decision quality fields in a dashboard-friendly format.

#### Why
This milestone provides a stable strategy-level summary payload for operational dashboards without forcing the monitoring subsystem’s alert policy into the reporting layer. It captures observed health metrics and mismatch context while preserving current trading and monitoring behavior.

#### Files Changed
- `src/trading_platform/reporting/dashboard_payloads.py`
- `src/trading_platform/paper/service.py`
- `tests/test_reporting_dashboard_payloads.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\dashboard_payloads.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\dashboard_payloads.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests\test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests\test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Design Notes
`StrategyHealthRow` intentionally uses a neutral `observed` or `mismatch_detected` status rather than inventing new health-policy thresholds. This keeps the payload informational and additive while exposing enough metrics for future monitoring or dashboard layers to apply their own policy logic.

#### Known Issues / Limitations
- Strategy health payloads currently summarize the latest paper run only; they do not yet provide historical trend windows across runs.
- Health status remains observational and does not replace `monitoring.service.evaluate_strategy_health()`.

#### Recommended Next Milestone
- F-01 - Clarify subsystem boundaries

### F-01 - Clarify subsystem boundaries
Date: 2026-03-30
Status: DONE

#### Summary
Added an architecture note for the core system layers and made one small concrete boundary improvement in the target-construction path. The paper layer now configures target-construction runtime dependencies through an explicit contract instead of mutating that module's globals ad hoc.

#### Why
This milestone is about structural clarity, not behavior change. The repo already had a practical layering shape, but one real boundary leak remained: `paper.service` reached into `target_construction_service` implementation details and reassigned module globals directly. Replacing that with an explicit configuration API makes the dependency direction clearer and easier to test.

#### Files Changed
- `docs/architecture_layers.md`
- `src/trading_platform/services/target_construction_service.py`
- `src/trading_platform/paper/service.py`
- `tests/test_target_construction_boundaries.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\services\target_construction_service.py src\trading_platform\paper\service.py tests\test_target_construction_boundaries.py tests\test_universe_provenance.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_target_construction_boundaries.py tests\test_universe_provenance.py::test_build_target_construction_result_integrates_universe_filters tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\services\target_construction_service.py src\trading_platform\paper\service.py tests\test_target_construction_boundaries.py tests\test_universe_provenance.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests\test_target_construction_boundaries.py tests\test_universe_provenance.py::test_build_target_construction_result_integrates_universe_filters tests\test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders`

#### Design Notes
The new `TargetConstructionRuntimeDependencies` and `configure_runtime_dependencies()` API keep the target-construction layer in control of how runtime hooks are consumed. This is a narrow boundary fix only; no portfolio selection, signal generation, or paper trading semantics were changed.

#### Known Issues / Limitations
- The target-construction layer still uses runtime-configured module-level dependencies; this milestone makes the configuration explicit, but it does not fully redesign the service into pure dependency injection.
- Live preview still depends on paper-facing helpers for some target preview behavior; that is outside F-01 scope.

#### Recommended Next Milestone
- F-02 - Expand test strategy by layer

### F-02 - Expand test strategy by layer
Date: 2026-03-30
Status: DONE

#### Summary
Added a layer-oriented testing strategy note and expanded orchestration coverage in one under-covered area. The new tests directly protect the research-stage placeholder path used when a strategy is intentionally not yet wired to a direct orchestration service runner.

#### Why
This milestone is about making test intent explicit by subsystem and tightening one practical gap without broadening into a repo-wide test redesign. The orchestration placeholder path is part of the documented runtime behavior, so it benefits from direct tests even though it is not a full research execution path.

#### Files Changed
- `docs/test_strategy_by_layer.md`
- `tests/test_orchestration_service.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check tests\test_orchestration_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_orchestration_service.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check tests\test_orchestration_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_orchestration_service.py`

#### Design Notes
The new strategy note organizes expected coverage by layer: research, portfolio, execution, state, reporting, and orchestration. The test expansion stays intentionally small and checks that the orchestration research stage emits a deterministic placeholder artifact for `xsec_momentum_topn` rather than treating that documented stub mode as an untested edge case.

#### Known Issues / Limitations
- The milestone adds test guidance and one focused orchestration test expansion only; it does not rebalance the whole test suite or introduce coverage thresholds.
- Ruff on `src/trading_platform/orchestration/service.py` currently reports unrelated pre-existing unused imports. F-02 did not broaden scope into cleanup there.

#### Recommended Next Milestone
- F-03 - Add performance profiling and caching plan

### F-03 - Add performance profiling and caching plan
Date: 2026-03-30
Status: DONE

#### Summary
Added a performance profiling note and implemented one low-risk same-run cache in the orchestration layer. The pipeline now caches strategy execution handoff resolution and execution-config loading within a single `run_orchestration_pipeline()` invocation.

#### Why
This milestone is intended to identify repeated expensive paths and make one safe improvement without changing trading semantics. Within one orchestration run, adjacent stages were reloading the same execution config and re-resolving the same strategy handoff for allocation, paper, and live stages even though the inputs were unchanged.

#### Files Changed
- `docs/performance_profiling_and_caching_plan.md`
- `src/trading_platform/orchestration/service.py`
- `tests/test_orchestration_service.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\orchestration\service.py tests\test_orchestration_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_orchestration_service.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\orchestration\service.py tests\test_orchestration_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_orchestration_service.py`

#### Design Notes
The cache is intentionally narrow: it is in-memory only, scoped to one pipeline run, and keyed by the generated multi-strategy config path plus the stage-relevant execution-handoff toggles. This avoids repeated config and handoff setup work without introducing cross-run invalidation problems or changing allocation, paper, or live semantics.

#### Known Issues / Limitations
- The profiling note identifies broader replay/research read-heavy paths, but F-03 does not yet instrument those flows with timing or cache-hit metrics.
- This optimization targets orchestration setup overhead only; it is not a substitute for deeper profiling of research artifact loading and replay loops.

#### Recommended Next Milestone
- G-01 - Introduce unified data ingestion framework

### G-01 - Introduce unified data ingestion framework
Date: 2026-03-30
Status: DONE

#### Summary
Added a unified market-data ingestion contract with deterministic manifest output and adapter-based normalization. The legacy Yahoo equity ingest path now writes both the existing normalized parquet and a new versioned market-data artifact set, while a crypto intraday scaffold adapter provides a clean normalization interface without requiring external exchange credentials.

#### Why
This milestone establishes a shared ingestion foundation for multiple asset classes and frequencies without entangling data acquisition with feature engineering or strategy logic. The repo already had a daily-equity ingest path, but it lacked an explicit cross-asset contract and versioned artifact metadata.

#### Files Changed
- `src/trading_platform/data/ingest.py`
- `src/trading_platform/data/normalize.py`
- `src/trading_platform/ingestion/contracts.py`
- `src/trading_platform/ingestion/framework.py`
- `src/trading_platform/ingestion/normalize.py`
- `src/trading_platform/ingestion/__init__.py`
- `tests/test_ingest.py`
- `tests/test_normalize.py`
- `tests/test_ingestion_framework.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\data\ingest.py src\trading_platform\data\normalize.py src\trading_platform\ingestion\contracts.py src\trading_platform\ingestion\framework.py src\trading_platform\ingestion\normalize.py src\trading_platform\ingestion\__init__.py tests\test_ingest.py tests\test_normalize.py tests\test_ingestion_framework.py tests\test_ingest_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_ingest.py tests/test_normalize.py tests/test_ingestion_framework.py tests/test_ingest_service.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\data\ingest.py src\trading_platform\data\normalize.py src\trading_platform\ingestion\contracts.py src\trading_platform\ingestion\framework.py src\trading_platform\ingestion\normalize.py src\trading_platform\ingestion\__init__.py tests\test_ingest.py tests\test_normalize.py tests\test_ingestion_framework.py tests\test_ingest_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_ingest.py tests/test_normalize.py tests/test_ingestion_framework.py tests/test_ingest_service.py`

#### Design Notes
`MarketDataArtifactManifest` is the typed machine-readable contract for normalized market-data artifact sets. The canonical dataset now carries explicit `timeframe`, `provider`, `asset_class`, and `schema_version` columns, while the new unified artifact paths separate daily and intraday datasets cleanly so future mixed-frequency support does not depend on overwriting one flat normalized-data directory.

#### Known Issues / Limitations
- The crypto intraday adapter is intentionally scaffold-only in G-01; it normalizes provided raw frames but does not fetch from a live external crypto provider yet.
- The legacy `ingest_symbol()` return value and daily normalized parquet path are preserved for backward compatibility, so the repo currently writes both legacy and unified artifacts for the Yahoo equity path.

#### Recommended Next Milestone
- G-04 - Add data quality validation layer

### G-04 - Add data quality validation layer
Date: 2026-03-30
Status: DONE

#### Summary
Added a structured market-data validation report contract and wired validation-report emission into the unified ingestion artifact path. The validation layer now checks missing required columns, duplicate timestamps, non-monotonic timestamps, null required values, negative volume, non-positive prices, and obvious OHLC consistency errors.

#### Why
G-01 introduced a shared market-data contract, but it still needed a machine-readable quality layer for auditability and future monitoring. This milestone formalizes validation results without breaking existing callers that still expect `validate_bars()` to raise a `ValueError` on invalid inputs.

#### Files Changed
- `src/trading_platform/data/validate.py`
- `src/trading_platform/ingestion/contracts.py`
- `src/trading_platform/ingestion/framework.py`
- `src/trading_platform/ingestion/validation.py`
- `src/trading_platform/ingestion/__init__.py`
- `tests/test_validate.py`
- `tests/test_ingestion_validation.py`
- `tests/test_ingestion_framework.py`
- `tests/test_ingest.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\data\validate.py src\trading_platform\ingestion\validation.py src\trading_platform\ingestion\contracts.py src\trading_platform\ingestion\framework.py src\trading_platform\ingestion\__init__.py tests\test_validate.py tests\test_ingestion_validation.py tests\test_ingestion_framework.py tests\test_ingest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_validate.py tests/test_ingestion_validation.py tests/test_ingestion_framework.py tests/test_ingest.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\data\validate.py src\trading_platform\ingestion\validation.py src\trading_platform\ingestion\contracts.py src\trading_platform\ingestion\framework.py src\trading_platform\ingestion\__init__.py tests\test_validate.py tests\test_ingestion_validation.py tests\test_ingestion_framework.py tests\test_ingest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_validate.py tests/test_ingestion_validation.py tests/test_ingestion_framework.py tests/test_ingest.py`

#### Design Notes
`MarketDataValidationReport` is the typed contract for data-quality checks, while `validate_market_data_frame()` returns structured issues instead of raising immediately. The legacy `validate_bars()` API remains intact by delegating to the new report builder and then raising on the first error for backward compatibility.

#### Known Issues / Limitations
- The new validation layer focuses on safe deterministic checks only; it does not yet attempt regime-aware anomaly detection or vendor-specific heuristics.
- Validation reports are currently emitted as JSON sidecars in the unified ingestion artifact tree and are not yet aggregated into broader monitoring dashboards.

#### Recommended Next Milestone
- G-02 - Add multi-frequency time alignment layer

### G-02 - Add multi-frequency time alignment layer
Date: 2026-03-30
Status: DONE

#### Summary
Added a shared mixed-frequency alignment utility with explicit timestamp semantics and a no-lookahead daily-to-intraday helper. The new alignment layer supports safe backward `merge_asof` joins and a `period_end_effective_next` mode for slower-bar data that should only become usable once the next period begins.

#### Why
The repo already contained subsystem-specific `merge_asof` alignment logic, but it did not have a shared utility for mixed-frequency joins with explicit leakage prevention semantics. This milestone provides that common foundation without changing existing strategy or fundamentals alignment behavior in place.

#### Files Changed
- `src/trading_platform/ingestion/alignment.py`
- `src/trading_platform/ingestion/__init__.py`
- `docs/multi_frequency_alignment.md`
- `tests/test_ingestion_alignment.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\ingestion\alignment.py src\trading_platform\ingestion\__init__.py tests\test_ingestion_alignment.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_ingestion_alignment.py tests/test_fundamentals.py::test_align_symbol_daily_features_normalizes_merge_asof_keys_before_alignment`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\ingestion\alignment.py src\trading_platform\ingestion\__init__.py tests\test_ingestion_alignment.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_ingestion_alignment.py tests/test_fundamentals.py::test_align_symbol_daily_features_normalizes_merge_asof_keys_before_alignment`

#### Design Notes
The new `TimeAlignmentConfig` makes timestamp semantics explicit instead of assuming every right-hand series is an event-time series. The daily-to-intraday helper uses `period_end_effective_next` mode so a daily close bar cannot be consumed by intraday rows from the same day.

#### Known Issues / Limitations
- The new alignment utility is additive and does not automatically replace existing subsystem-specific alignment code yet.
- The `period_end_effective_next` mode infers the final bar step from observed spacing for the last right-hand row in a group; this is conservative but still a heuristic for trailing rows.

#### Recommended Next Milestone
- G-03 - Introduce feature store

### G-03 - Introduce feature store
Date: 2026-03-30
Status: DONE

#### Summary
Added a deterministic filesystem-backed feature-store contract with manifest metadata and a local read/write path. The feature service now writes computed feature parquet outputs into the new store additively while keeping the existing `data/features` artifact path intact.

#### Why
The repo already computed features deterministically, but it did not expose a typed persistence contract for later reuse across research, replay, and paper workflows. This milestone introduces that contract without forcing current consumers to switch storage locations immediately.

#### Files Changed
- `src/trading_platform/features/store.py`
- `src/trading_platform/services/feature_service.py`
- `src/trading_platform/settings.py`
- `tests/test_feature_store.py`
- `tests/test_feature_service.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\features\store.py src\trading_platform\services\feature_service.py src\trading_platform\settings.py tests\test_feature_store.py tests\test_feature_service.py tests\test_feature_build.py tests\test_pipeline_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_feature_store.py tests/test_feature_service.py tests/test_feature_build.py tests/test_pipeline_service.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\features\store.py src\trading_platform\services\feature_service.py src\trading_platform\settings.py tests\test_feature_store.py tests\test_feature_service.py tests\test_feature_build.py tests\test_pipeline_service.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_feature_store.py tests/test_feature_service.py tests/test_feature_build.py tests/test_pipeline_service.py`

#### Design Notes
`FeatureStoreArtifact` is the typed contract for persisted feature snapshots, including feature-set identity, row count, timestamp coverage, and feature-column metadata. `LocalFeatureStore` uses a stable `{timeframe}/{symbol}/{feature_set_id}` directory layout and JSON sidecar manifests so later workflows can discover persisted features without inferring semantics from filenames alone.

#### Known Issues / Limitations
- The current integration writes to both the legacy `data/features` location and the new feature-store root to preserve backward compatibility.
- The feature service currently records a default timeframe of `1d`; broader intraday feature-store integration will depend on future multi-frequency feature workflows.

#### Recommended Next Milestone
- H-01 - Expand candidate grid generation

### H-01 - Expand candidate grid generation
Date: 2026-03-30
Status: DONE

#### Summary
Expanded the automated alpha-lab candidate generator so it now emits explicit traceability fields and named variants alongside the existing parameter sweeps. Generated candidates now carry deterministic `candidate_id`, `candidate_name`, `variant_id`, `signal_variant`, `parameters_json`, `variant_parameters_json`, and `config_json` fields, and the broad preset adds multiple named variants for supported generated-signal families.

#### Why
The repo already had parameter sweep generation, but the automated research loop still treated candidates mostly as flat IDs without first-class variant traceability. This milestone makes candidate identity inspectable end to end without changing promotion thresholds or the downstream promotion contract.

#### Files Changed
- `src/trading_platform/research/alpha_lab/generation.py`
- `src/trading_platform/research/alpha_lab/automation.py`
- `tests/test_alpha_research_loop.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\generation.py src\trading_platform\research\alpha_lab\automation.py tests\test_alpha_research_loop.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\generation.py src\trading_platform\research\alpha_lab\automation.py tests\test_alpha_research_loop.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py`

#### Design Notes
The generated-signal path now supports `candidate_grid_preset` and `max_variants_per_family` directly on `SignalGenerationConfig`. The `broad_v1` preset keeps base candidates intact and adds deterministic variant transforms such as smoothing, volatility scaling, z-scoring, and clipping where they are safe to apply without changing the baseline candidate semantics.

#### Known Issues / Limitations
- The richer runner path and the automated generated-signal path still use different variant template families; this milestone aligned the traceability shape, not the full signal library.
- Some generated families still emit only the `base` variant when there is not yet a safe low-risk transform to add.

#### Recommended Next Milestone
- H-02 - Introduce automated research loop

### H-02 - Introduce automated research loop
Date: 2026-03-30
Status: DONE

#### Summary
Formalized the existing automated alpha research loop as a minimal observable orchestration path by persisting the generated candidate grid, a candidate-grid manifest, and a structured run-summary artifact for each loop iteration. The loop continues to generate candidates, evaluate them, and apply the existing promotion rules, but now writes clearer machine-readable outputs for inspection and audit.

#### Why
Most of the loop logic already existed, but the run-level artifact story was thin and candidate traceability did not survive cleanly through the full lifecycle. This milestone makes the loop easier to inspect and rerun without introducing scheduling systems, hidden state, or changes to promotion policy semantics.

#### Files Changed
- `src/trading_platform/research/alpha_lab/generation.py`
- `src/trading_platform/research/alpha_lab/automation.py`
- `tests/test_alpha_research_loop.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\generation.py src\trading_platform\research\alpha_lab\automation.py tests\test_alpha_research_loop.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_lab.py::test_run_alpha_research_broad_candidate_grid_emits_variant_identity`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\generation.py src\trading_platform\research\alpha_lab\automation.py tests\test_alpha_research_loop.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_lab.py::test_run_alpha_research_broad_candidate_grid_emits_variant_identity`

#### Design Notes
The loop now emits `candidate_grid.csv`, `candidate_grid_manifest.json`, and `research_loop_run_summary.json` under the loop output directory. These artifacts summarize the generation config, per-family variant coverage, run counts, and the exact downstream artifact paths without changing how candidates are scored or promoted.

#### Known Issues / Limitations
- A directly adjacent but separate runner-path regression test currently fails in `tests/test_alpha_lab.py::test_run_alpha_research_broad_candidate_grid_emits_variant_identity` because `runner.py` attempts to write a Parquet column with an empty struct `metadata` type. This milestone did not modify that path.
- The loop still uses wall-clock timestamps for `run_id` and `last_evaluated_at`, so the run manifests are inspectable but not bit-for-bit identical across different execution times.

#### Recommended Next Milestone
- H-03 - Add research resource allocation

### H-03 - Add research resource allocation
Date: 2026-03-30
Status: DONE

#### Summary
Added a deterministic research resource-allocation layer to the automated alpha loop in `src/trading_platform/research/alpha_lab/automation.py`. The loop now computes an explicit candidate allocation plan before evaluation, with transparent family-priority scoring, optional per-family variant caps, optional iteration caps, and structured selected/deferred/pruned statuses.

#### Why
The automated loop previously evaluated every pending candidate uniformly. This milestone makes prioritization explicit and inspectable without changing promotion semantics or introducing opaque scheduling behavior.

#### Files Changed
- `src/trading_platform/research/alpha_lab/automation.py`
- `tests/test_alpha_research_loop.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\automation.py src\trading_platform\research\experiment_tracking.py tests\test_alpha_research_loop.py tests\test_experiment_tracking.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py tests/test_experiment_tracking.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\automation.py src\trading_platform\research\experiment_tracking.py tests\test_alpha_research_loop.py tests\test_experiment_tracking.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py tests/test_experiment_tracking.py`

#### Design Notes
`ResearchResourceAllocationConfig` is the typed policy surface. The loop now emits `candidate_allocation_plan.csv`, `candidate_allocation_deferred.csv`, and `candidate_allocation_summary.json`. Family priority is derived from existing completed-registry history using transparent promotion-rate and mean-rank-IC inputs; candidates are then ordered deterministically by score and stable keys.

#### Known Issues / Limitations
- Default allocation settings remain effectively non-disruptive unless explicit caps are configured, so the new layer is more about transparency and controlled throttling than aggressive pruning.
- The current family-priority score is intentionally simple and artifact-driven; it is not meant to be a sophisticated search-optimization policy.

#### Recommended Next Milestone
- H-04 - Add experiment tracking system

### H-04 - Add experiment tracking system
Date: 2026-03-30
Status: DONE

#### Summary
Added structured experiment tracking for automated alpha-loop runs using the repo’s existing experiment registry infrastructure. The loop now registers each run under the experiment tracker and persists queryable references to config, candidate-grid, allocation, and promotion artifacts.

#### Why
The loop already produced useful artifacts, but those runs were not registered in the shared experiment-tracking registry. This milestone makes automated research runs discoverable and reproducible through the same artifact-first tracking pattern used elsewhere in the repo.

#### Files Changed
- `src/trading_platform/research/alpha_lab/automation.py`
- `src/trading_platform/research/experiment_tracking.py`
- `tests/test_alpha_research_loop.py`
- `tests/test_experiment_tracking.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\automation.py src\trading_platform\research\experiment_tracking.py tests\test_alpha_research_loop.py tests\test_experiment_tracking.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py tests/test_experiment_tracking.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\research\alpha_lab\automation.py src\trading_platform\research\experiment_tracking.py tests\test_alpha_research_loop.py tests\test_experiment_tracking.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_alpha_research_loop.py tests/test_experiment_tracking.py`

#### Design Notes
`build_automated_alpha_loop_experiment_record()` maps loop artifacts into the existing `experiment_registry.csv` / parquet / JSON format. The loop now records `experiment_registry_path`, `experiment_registry_parquet_path`, and `experiment_registry_json_path` in its returned paths, using a local tracker directory under the loop output root by default.

#### Known Issues / Limitations
- The loop experiment record is intentionally lightweight and does not attempt to infer paper/live portfolio metrics that do not exist in the automated alpha-loop path.
- The adjacent `runner.py` Parquet empty-struct metadata issue remains separate and was not changed here.

#### Recommended Next Milestone
- I-01 - Introduce execution simulator

### I-01 - Introduce execution simulator
Date: 2026-03-30
Status: DONE

#### Summary
Introduced a typed paper-execution simulation layer on top of the existing execution realism engine. Paper runs now preserve requested orders separately from executable orders, emit a `PaperExecutionSimulationReport`, and thread simulated partial fills, spread-aware assumptions, slippage, and configurable submission/fill delay metadata into lifecycle and artifact outputs.

#### Why
The repo already had realistic execution primitives, but the paper path collapsed those results into plain executable orders and broker fills. This milestone makes simulated execution inspectable and deterministic without changing the default paper-trading behavior when execution simulation is not explicitly configured.

#### Files Changed
- `src/trading_platform/execution/models.py`
- `src/trading_platform/execution/service.py`
- `src/trading_platform/execution/order_lifecycle.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `tests/test_execution_realism.py`
- `tests/test_order_lifecycle.py`
- `tests/test_paper_execution_realism.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\models.py src\trading_platform\execution\service.py src\trading_platform\execution\order_lifecycle.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py tests\test_execution_realism.py tests\test_order_lifecycle.py tests\test_paper_execution_realism.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_execution_realism.py tests/test_order_lifecycle.py tests/test_paper_execution_realism.py tests/test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests/test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests/test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests/test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\models.py src\trading_platform\execution\service.py src\trading_platform\execution\order_lifecycle.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py tests\test_execution_realism.py tests\test_order_lifecycle.py tests\test_paper_execution_realism.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_execution_realism.py tests/test_order_lifecycle.py tests/test_paper_execution_realism.py tests/test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_paper_trading_service.py::test_run_paper_trading_cycle_builds_orders tests/test_paper_trading_service.py::test_run_paper_trading_cycle_supports_xsec_strategy tests/test_paper_trading_service.py::test_run_paper_trading_cycle_selects_composite_mode tests/test_paper_trading_service.py::test_run_paper_trading_cycle_supports_ensemble_signal_source`

#### Design Notes
The runtime path stays backward-compatible: `result.orders` still means executable paper orders, while `result.requested_orders` now retains pre-simulation intent. The new simulation report is additive and lifecycle reconciliation uses it to surface partial fills and rejections even when no actual broker fill was applied yet.

#### Known Issues / Limitations
- Paper execution simulation still relies on the existing execution engine inputs, so richer market-data-driven liquidity fields will need future work to fully reflect ADV/staleness inside the paper path.
- Delay assumptions are currently modeled as explicit metadata and summary fields, not a full event-clock engine.

#### Recommended Next Milestone
- I-02 - Add transaction cost modeling

### I-02 - Add transaction cost modeling
Date: 2026-03-30
Status: DONE

#### Summary
Added a typed transaction-cost modeling contract and artifact path around the paper execution flow. The new `TransactionCostModelConfig`, `TransactionCostRecord`, and `TransactionCostReport` formalize spread, slippage, and commission components for both estimated orders and realized fills.

#### Why
The repo already computed cost fields in several places, but they were not exposed through a first-class machine-readable contract. This milestone makes cost outputs explicit and reusable for EV, dashboarding, and audit without changing alpha logic or default order-generation semantics.

#### Files Changed
- `src/trading_platform/execution/costs.py`
- `src/trading_platform/execution/__init__.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `tests/test_transaction_costs.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\costs.py src\trading_platform\execution\__init__.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py tests\test_transaction_costs.py tests\test_paper_artifacts_with_fills.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_transaction_costs.py tests/test_paper_artifacts_with_fills.py tests/test_pnl_attribution.py::test_cost_model_slippage_and_commission_flow_into_gross_vs_net_attribution`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\execution\costs.py src\trading_platform\execution\__init__.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py tests\test_transaction_costs.py tests\test_paper_artifacts_with_fills.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_transaction_costs.py tests/test_paper_artifacts_with_fills.py tests/test_pnl_attribution.py::test_cost_model_slippage_and_commission_flow_into_gross_vs_net_attribution`

#### Design Notes
The typed cost report is built from existing `PaperOrder` and `BrokerFill` fields, so it does not alter how costs are computed. It simply provides a stable per-stage contract and artifact outputs that downstream EV and reporting code can consume additively.

#### Known Issues / Limitations
- The report currently reflects the repo’s existing paper cost semantics; it does not yet unify execution-simulator commissions with paper-fill commissions into a single pricing engine.
- Cost reporting is paper-oriented in this milestone and does not yet cover separate live broker fee schedules.

#### Recommended Next Milestone
- J-01 - Introduce real-time KPI monitoring

### J-01 - Introduce real-time KPI monitoring
Date: 2026-03-30
Status: DONE

#### Summary
Added a typed real-time KPI monitoring payload for paper runs. The new reporting contract emits structured metrics for equity, pnl, drawdown, exposure, realized-vs-expected gap, fill rate, partial-fill ratio, slippage quality, and execution cost, and writes dedicated JSON/CSV monitoring artifacts.

#### Why
The repo already had KPI and monitoring subsystems, but there was no focused paper-run payload for near-real-time dashboard consumption. This milestone adds an artifact-first monitoring surface without introducing alerting or kill-switch behavior.

#### Files Changed
- `src/trading_platform/reporting/realtime_monitoring.py`
- `src/trading_platform/paper/service.py`
- `tests/test_realtime_monitoring.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\realtime_monitoring.py src\trading_platform\paper\service.py tests\test_realtime_monitoring.py tests\test_paper_artifacts_with_fills.py tests\test_reporting_dashboard_payloads.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_realtime_monitoring.py tests/test_paper_artifacts_with_fills.py tests/test_reporting_dashboard_payloads.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\realtime_monitoring.py src\trading_platform\paper\service.py tests\test_realtime_monitoring.py tests\test_paper_artifacts_with_fills.py tests\test_reporting_dashboard_payloads.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_realtime_monitoring.py tests/test_paper_artifacts_with_fills.py tests/test_reporting_dashboard_payloads.py`

#### Design Notes
The payload is intentionally observational. Metric semantics are explicit in-field metadata where needed, such as `drawdown` being based on `initial_cash_basis` and `realized_vs_expected_gap` comparing realized usd pnl with the summed net expected-return contract values.

#### Known Issues / Limitations
- The monitoring payload is single-run scoped and does not yet maintain a rolling history or alert thresholds by itself.
- `drawdown` is currently a current-state proxy based on paper baseline equity, not a historical max-drawdown curve.

#### Recommended Next Milestone
- J-02 - Add system health monitoring

### J-02 - Add system health monitoring
Date: 2026-04-01
Status: DONE

#### Summary
Added a typed system-health payload for paper-run reporting. The new contract emits structured checks for data freshness, stale signals, missing reporting artifacts, and core pipeline integrity, and writes dedicated JSON/CSV health artifacts alongside the existing KPI and dashboard payloads.

#### Why
The repository already had broader monitoring utilities, but the paper-run artifact set did not expose a focused, dashboard-friendly system-health surface inside the reporting layer. This milestone makes run health observable in the same artifact bundle without introducing alerting or kill-switch behavior.

#### Files Changed
- `src/trading_platform/reporting/system_health.py`
- `src/trading_platform/paper/service.py`
- `tests/test_reporting_dashboard_payloads.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\system_health.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_reporting_dashboard_payloads.py tests/test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_realtime_monitoring.py tests/test_paper_execution_realism.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\system_health.py src\trading_platform\paper\service.py tests\test_reporting_dashboard_payloads.py tests\test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_reporting_dashboard_payloads.py tests/test_paper_artifacts_with_fills.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_realtime_monitoring.py tests/test_paper_execution_realism.py`

#### Design Notes
The health payload is additive and observational. It derives check results from existing paper-run diagnostics and artifact availability, so current trading behavior remains unchanged unless future milestones choose to consume these checks for gating.

#### Known Issues / Limitations
- The payload is scoped to the paper-run artifact bundle and does not yet aggregate rolling health history across runs by itself.
- Stale-signal detection currently uses paper-run freshness diagnostics and score availability rather than a separate signal-age registry.

#### Recommended Next Milestone
- J-03 - Add risk controls / kill switch

### J-03 - Add risk controls / kill switch
Date: 2026-04-01
Status: DONE

#### Summary
Added a typed paper risk-control contract and artifact set, plus optional pre-trade drawdown throttling/halting in the paper execution path. Paper runs now emit structured risk triggers, lifecycle events, and actions across portfolio, strategy, and instrument scopes, and expose the resulting operating state through artifacts and KPI records.

#### Why
Phase 1 needs explicit, inspectable safeguards before the platform can act as a governed closed-loop system. This milestone introduces default-off controls that can constrain or halt paper trading when configured, while preserving existing behavior unless the new risk settings are enabled.

#### Files Changed
- `src/trading_platform/risk/controls.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `src/trading_platform/reporting/dashboard_payloads.py`
- `tests/test_risk_controls.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\risk\controls.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_risk_controls.py tests\test_paper_artifacts_with_fills.py tests\test_trade_outcome_attribution.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_risk_controls.py tests/test_paper_artifacts_with_fills.py tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\risk\controls.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_risk_controls.py tests\test_paper_artifacts_with_fills.py tests\test_trade_outcome_attribution.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_risk_controls.py tests/test_paper_artifacts_with_fills.py tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py`

#### Design Notes
`PaperRiskControlReport` is the new additive contract. Pre-trade integration is intentionally conservative: when enabled, the current paper path can throttle or halt orders on drawdown before fill application, while post-run reporting adds structured expected-vs-realized divergence and execution-anomaly triggers by strategy and instrument. The artifact bundle now includes `paper_risk_controls.json` plus CSVs for triggers, actions, and events.

#### Known Issues / Limitations
- Current active gating is limited to portfolio drawdown so the paper path stays backward-compatible and avoids speculative state persistence; strategy/instrument divergence and execution anomalies are logged immediately but act as structured recommendations for now.
- Drawdown is measured against the paper baseline equity currently available in state, not a fully persisted rolling peak-equity registry.

#### Recommended Next Milestone
- J-04 - Add drift detection

### J-04 - Add drift detection
Date: 2026-04-01
Status: DONE

#### Summary
Added a typed drift-detection layer for the paper path. Paper runs now compute deterministic drift metric snapshots and triggered drift signals across performance, decision, and execution categories, write dedicated JSON/CSV artifacts, and expose top-level drift metrics through the KPI payload.

#### Why
With trade outcome attribution and risk controls in place, the next missing Phase 1 capability was an inspectable way to detect behavioral decay before automatically routing anything into controls. This milestone adds explicit drift diagnostics without changing default trading behavior.

#### Files Changed
- `src/trading_platform/reporting/drift_detection.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `src/trading_platform/reporting/dashboard_payloads.py`
- `tests/test_drift_detection.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\drift_detection.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_drift_detection.py tests\test_paper_artifacts_with_fills.py tests\test_trade_outcome_attribution.py tests\test_risk_controls.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_drift_detection.py tests/test_paper_artifacts_with_fills.py tests/test_trade_outcome_attribution.py tests/test_risk_controls.py tests/test_pnl_attribution.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\drift_detection.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_drift_detection.py tests\test_paper_artifacts_with_fills.py tests\test_trade_outcome_attribution.py tests\test_risk_controls.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_drift_detection.py tests/test_paper_artifacts_with_fills.py tests/test_trade_outcome_attribution.py tests/test_risk_controls.py tests/test_pnl_attribution.py`

#### Design Notes
`DriftMetricSnapshot`, `DriftSignal`, and `DriftSummaryReport` are the additive typed contracts. The current implementation compares recent outcome windows against either a baseline half-window or explicit expected references when history is too short, and covers forecast-gap/win-rate performance drift, confidence and regime-mix decision drift, and cost/fill-quality execution drift. Recommended actions are advisory only; no routing into `J-03` risk controls is enabled by default.

#### Known Issues / Limitations
- Recent-vs-baseline comparisons are currently scoped to the outcomes available inside the current paper artifact bundle rather than a persisted long-horizon drift registry.
- Decision drift is centered on executed trade outcomes and execution-linked observations; broader candidate-universe drift can be layered later without changing this contract.

#### Recommended Next Milestone
- K-02 - Add calibration pipeline

### K-02 - Add calibration pipeline
Date: 2026-04-01
Status: DONE

#### Summary
Added a typed calibration layer for paper-run trade outcomes. The platform now derives conservative bucket-based confidence and expected-value adjustments from realized outcomes, preserves both raw and calibrated values per trade, and writes deterministic calibration records, bucket summaries, adjustment tables, and scope summaries for portfolio, strategy, and regime views when sample thresholds are met.

#### Why
Phase 1 needed a reusable, inspectable calibration surface between attribution/drift and any future decision-time weighting logic. This milestone adds that layer without changing default execution, ranking, or promotion behavior.

#### Files Changed
- `src/trading_platform/reporting/calibration.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `src/trading_platform/reporting/dashboard_payloads.py`
- `tests/test_calibration_pipeline.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\calibration.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_calibration_pipeline.py tests\test_paper_artifacts_with_fills.py tests\test_trade_outcome_attribution.py tests\test_drift_detection.py tests\test_risk_controls.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_calibration_pipeline.py tests/test_paper_artifacts_with_fills.py tests/test_trade_outcome_attribution.py tests/test_drift_detection.py tests/test_risk_controls.py tests/test_pnl_attribution.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\calibration.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_calibration_pipeline.py tests\test_paper_artifacts_with_fills.py tests\test_trade_outcome_attribution.py tests\test_drift_detection.py tests\test_risk_controls.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_calibration_pipeline.py tests/test_paper_artifacts_with_fills.py tests/test_trade_outcome_attribution.py tests/test_drift_detection.py tests/test_risk_controls.py tests/test_pnl_attribution.py`

#### Design Notes
`CalibrationBucket`, `CalibratedPredictionAdjustment`, `CalibrationRecord`, `CalibrationScopeSummary`, and `CalibrationSummaryReport` are the new additive contracts. The current implementation uses deterministic fixed buckets plus sample-threshold and shrinkage rules, applies adjustments only when there is enough evidence, and stores both raw and calibrated confidence / expected-net-return values per realized trade. KPI output now includes top-level calibration counts and calibration-error metrics.

#### Known Issues / Limitations
- Calibration is currently derived from the outcomes available in the current paper artifact bundle rather than a longer rolling calibration registry, so this is intentionally conservative and local.
- The adjustments are advisory outputs only; they are not wired into trade selection, sizing, or EV gating by default.

#### Recommended Next Milestone
- K-03 - Add strategy decay detection

### K-03 - Add strategy decay detection
Date: 2026-04-01
Status: DONE

#### Summary
Added a typed strategy decay detection layer that combines attribution gaps, drift pressure, calibration quality, realized instability, and risk-context signals into conservative per-strategy decay records, triggered decay signals, and lifecycle recommendations. Paper runs now emit deterministic JSON/CSV decay artifacts and expose top-level decay KPIs.

#### Why
With attribution, drift, calibration, and risk controls already in place, the next Phase 1 step was to synthesize them into an explicit strategy-health judgment. This milestone adds that diagnosis layer without automatically demoting, retraining, or changing default trading behavior.

#### Files Changed
- `src/trading_platform/reporting/strategy_decay.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `src/trading_platform/reporting/dashboard_payloads.py`
- `tests/test_strategy_decay.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\strategy_decay.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_strategy_decay.py tests\test_paper_artifacts_with_fills.py tests\test_calibration_pipeline.py tests\test_drift_detection.py tests\test_risk_controls.py tests\test_trade_outcome_attribution.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_strategy_decay.py tests/test_paper_artifacts_with_fills.py tests/test_calibration_pipeline.py tests/test_drift_detection.py tests/test_risk_controls.py tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\strategy_decay.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_strategy_decay.py tests\test_paper_artifacts_with_fills.py tests\test_calibration_pipeline.py tests\test_drift_detection.py tests\test_risk_controls.py tests\test_trade_outcome_attribution.py tests\test_pnl_attribution.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_strategy_decay.py tests/test_paper_artifacts_with_fills.py tests/test_calibration_pipeline.py tests/test_drift_detection.py tests/test_risk_controls.py tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py`

#### Design Notes
`StrategyDecayRecord`, `StrategyDecaySignal`, `StrategyLifecycleRecommendation`, and `StrategyDecaySummaryReport` are the new additive contracts. The current implementation computes a conservative weighted decay score only when trade-count thresholds are met, explicitly emits insufficient-data states otherwise, and keeps all outputs advisory: recommended actions can reach `demote_candidate`, but no demotion or retraining is executed in this milestone.

#### Known Issues / Limitations
- Decay scoring currently uses the current paper artifact bundle rather than a persisted long-window registry, so the diagnosis is intentionally local and conservative.
- The current output is strategy-centric; regime context is captured in metadata and contributing evidence rather than as a separate first-class decay record type.

#### Recommended Next Milestone
- K-04 - Add auto-demotion / retraining loop

### K-01 - Introduce trade outcome attribution
Date: 2026-04-01
Status: DONE

#### Summary
Added first-class trade outcome and attribution contracts for the paper path. Closed paper trades now emit deterministic expected-vs-realized outcome records, forecast-gap decomposition fields, grouped attribution aggregates, and dedicated JSON/CSV artifacts that can be consumed by KPI and reporting workflows.

#### Why
The repository already tracked realized PnL attribution, but it did not expose a stable contract for comparing predicted trade outcomes against realized outcomes. This milestone creates that bridge without changing existing trading or promotion semantics by default.

#### Files Changed
- `src/trading_platform/reporting/outcome_attribution.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `src/trading_platform/reporting/dashboard_payloads.py`
- `tests/test_trade_outcome_attribution.py`
- `tests/test_pnl_attribution.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\paper\service.py tests\test_pnl_attribution.py tests\test_trade_outcome_attribution.py src\trading_platform\reporting\outcome_attribution.py src\trading_platform\paper\models.py src\trading_platform\reporting\dashboard_payloads.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\paper\service.py tests\test_pnl_attribution.py tests\test_trade_outcome_attribution.py src\trading_platform\reporting\outcome_attribution.py src\trading_platform\paper\models.py src\trading_platform\reporting\dashboard_payloads.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py`

#### Design Notes
`TradeOutcome`, `TradeAttribution`, `TradeAttributionAggregate`, and `TradeOutcomeAttributionReport` are additive typed contracts owned by the platform. The paper service now persists enough decision-time metadata on open lots to connect later exits back to expected returns, costs, confidence, horizon, and regime fields, and writes `trade_outcomes.csv`, `trade_outcome_attribution.csv`, `trade_outcome_aggregates.csv`, and JSON summaries alongside existing paper artifacts.

#### Known Issues / Limitations
- The current decomposition is intentionally conservative and heuristic for fields such as timing error, sizing error, and regime mismatch because the legacy paper path does not yet preserve a richer execution-clock or regime-history contract.
- Predicted gross/net returns are only populated when the upstream EV gate or decision provenance already provides them; the attribution layer does not invent alpha forecasts when no prediction artifact exists.

#### Recommended Next Milestone
- J-03 - Add risk controls / kill switch

### K-04 - Add auto-demotion / retraining loop
Date: 2026-04-01
Status: DONE

#### Summary
Added a typed strategy lifecycle policy layer that converts `K-03` decay recommendations and `J-03` risk context into explicit, auditable lifecycle actions. Paper runs now emit deterministic lifecycle state, action, transition, demotion, and retraining-trigger artifacts, and the governance layer can apply those outputs to the strategy registry conservatively.

#### Why
Phase 1 needed a governed bridge from diagnosis to action. This milestone adds that control layer without silently changing default trading behavior or bypassing promotion governance for retrained strategies.

#### Files Changed
- `src/trading_platform/governance/lifecycle.py`
- `src/trading_platform/governance/__init__.py`
- `src/trading_platform/paper/models.py`
- `src/trading_platform/paper/service.py`
- `src/trading_platform/reporting/dashboard_payloads.py`
- `tests/test_strategy_lifecycle.py`
- `tests/test_paper_artifacts_with_fills.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\lifecycle.py src\trading_platform\governance\__init__.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_strategy_lifecycle.py tests\test_paper_artifacts_with_fills.py tests\test_strategy_decay.py tests\test_calibration_pipeline.py tests\test_drift_detection.py tests\test_risk_controls.py tests\test_trade_outcome_attribution.py tests\test_pnl_attribution.py tests\test_governance_registry.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_strategy_lifecycle.py tests/test_paper_artifacts_with_fills.py tests/test_strategy_decay.py tests/test_calibration_pipeline.py tests/test_drift_detection.py tests/test_risk_controls.py tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py tests/test_governance_registry.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\governance\lifecycle.py src\trading_platform\governance\__init__.py src\trading_platform\paper\models.py src\trading_platform\paper\service.py src\trading_platform\reporting\dashboard_payloads.py tests\test_strategy_lifecycle.py tests\test_paper_artifacts_with_fills.py tests\test_strategy_decay.py tests\test_calibration_pipeline.py tests\test_drift_detection.py tests\test_risk_controls.py tests\test_trade_outcome_attribution.py tests\test_pnl_attribution.py tests\test_governance_registry.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_strategy_lifecycle.py tests/test_paper_artifacts_with_fills.py tests/test_strategy_decay.py tests/test_calibration_pipeline.py tests/test_drift_detection.py tests/test_risk_controls.py tests/test_trade_outcome_attribution.py tests/test_pnl_attribution.py tests/test_governance_registry.py`

#### Design Notes
`StrategyLifecycleState`, `StrategyLifecycleAction`, `LifecycleTransitionRecord`, `DemotionDecision`, `RetrainingTrigger`, and `StrategyLifecycleSummaryReport` are the new platform-owned contracts. The current policy uses `K-03` decay recommendations as the primary trigger source, upgrades actions when strategy-scoped risk context exists, and supports cooldown/dedup suppression to avoid repeated action thrashing. Demotion is the only lifecycle action that mutates registry status directly; watch, constrain, and retrain are recorded in artifacts and registry metadata/audit trails.

Retraining is intentionally implemented as a structured trigger and governance handoff rather than an automatic replacement workflow. The handoff explicitly records that any retrained candidate must re-enter the normal research and promotion flow before it can become active again.

#### Known Issues / Limitations
- The paper artifact flow emits lifecycle actions and state transitions, but it does not automatically mutate a registry unless a caller explicitly applies the report through the governance helper.
- `constrain` currently remains advisory for allocation/execution behavior; it is logged as lifecycle state and KPI output, but it does not automatically rescale positions by default.
- Cooldown behavior is supported when prior lifecycle state is available, but the paper artifact flow currently evaluates a single run in isolation unless an external caller passes prior state back in.

#### Recommended Next Milestone
- Phase 1 re-evaluation checkpoint

### Phase 1.5 Utility - Daily System Report
Date: 2026-04-01
Status: DONE

#### Summary
Added a compact validation utility at `scripts/daily_system_report.py` that reads a paper-trading artifact bundle and emits a deterministic daily JSON report, with an optional markdown summary, for Phase 1.5 closed-loop validation.

#### Why
Phase 1 is complete, but the platform still needs a single operator-friendly checkpoint artifact to evaluate whether attribution, calibration, drift, decay, lifecycle, and risk outputs are behaving usefully during extended paper trading. This utility is for validation and review, not dashboard serving.

#### Files Changed
- `src/trading_platform/reporting/daily_system_report.py`
- `scripts/daily_system_report.py`
- `tests/test_daily_system_report.py`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\daily_system_report.py scripts\daily_system_report.py tests\test_daily_system_report.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_daily_system_report.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\daily_system_report.py scripts\daily_system_report.py tests\test_daily_system_report.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_daily_system_report.py`

#### Design Notes
The utility reads existing structured artifacts first instead of recomputing attribution, calibration, drift, decay, lifecycle, or risk logic. It supports `--artifact-dir`, `--output-json`, optional `--output-md`, and `--strict` for required-artifact enforcement. Missing optional artifacts degrade gracefully into warnings so the report can still be generated during incomplete validation runs.

Example command:
- `python scripts/daily_system_report.py --artifact-dir artifacts\paper\run_live_validation --output-json artifacts\paper\run_live_validation\daily_system_report.json --output-md artifacts\paper\run_live_validation\daily_system_report.md`

#### Known Issues / Limitations
- The utility is intentionally a read-only validation tool; it does not serve data over an API and does not change trading behavior.
- Evaluation flags are deterministic heuristics for Phase 1.5 review, not a replacement for the underlying calibration, drift, or lifecycle contracts.
- The report quality depends on the quality and completeness of the artifact bundle passed in.

#### Recommended Next Milestone
- Phase 1.5 re-evaluation checkpoint execution and review

### Phase 1.5 Utility - Validation Window Reviewer
Date: 2026-04-01
Status: DONE

#### Summary
Added a compact multi-run validation reviewer at `scripts/review_validation_window.py` with reusable aggregation logic in `src/trading_platform/reporting/validation_window_review.py`. It scans a validation root containing timestamped daily-validation runs, loads each run's `daily_validation_run_summary.json` and `daily_system_report.json`, and emits a deterministic checkpoint review in JSON with optional markdown.

#### Why
The daily validation runner and daily system report make single runs inspectable, but the Phase 1.5 checkpoint requires a windowed judgment about whether the closed-loop system is behaving meaningfully over time. This utility provides that review layer without recomputing attribution, calibration, drift, decay, lifecycle, or risk logic.

#### Files Changed
- `src/trading_platform/reporting/validation_window_review.py`
- `scripts/review_validation_window.py`
- `tests/test_validation_window_review.py`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\validation_window_review.py scripts\review_validation_window.py tests\test_validation_window_review.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_validation_window_review.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\validation_window_review.py scripts\review_validation_window.py tests\test_validation_window_review.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_validation_window_review.py`

#### Design Notes
The reviewer intentionally consumes only existing validation artifacts first, especially `daily_system_report.json` and `daily_validation_run_summary.json`. It supports optional `--days`, `--max-runs`, `--min-valid-runs`, and `--strict`, skips incomplete runs with warnings by default, and emits deterministic checkpoint statuses for EV alignment, calibration usefulness, drift signal quality, decay signal quality, lifecycle churn, risk control behavior, and an overall validation status.

Example command:
- `python scripts/review_validation_window.py --validation-root artifacts\paper\run_live_validation --output-json artifacts\paper\run_live_validation\validation_window_review.json --output-md artifacts\paper\run_live_validation\validation_window_review.md`
- `python scripts/review_validation_window.py --validation-root artifacts\paper\run_live_validation --output-json artifacts\paper\run_live_validation\validation_window_review.json --days 14 --min-valid-runs 5`

#### Known Issues / Limitations
- This is a compact Phase 1.5 review utility, not a dashboard service, notebook workflow, or persistent monitoring process.
- The checkpoint heuristics intentionally summarize existing per-run artifacts; they do not recompute or replace the underlying attribution, calibration, drift, decay, lifecycle, or risk models.
- Strict mode fails on malformed or incomplete run folders; non-strict mode skips them and records warnings in the window summary.

#### Recommended Next Milestone
- Phase 1.5 checkpoint review and human go/no-go decision

### Maintenance - Kalshi Historical Ingest Audit and Research-Readiness Hardening
Date: 2026-04-01
Status: DONE

#### Summary
Audited the Kalshi historical ingest path end to end and fixed the main trustworthiness gap: the repository’s default Kalshi research dataset was still synthetic (`SYNTH-*`) while the ingest pipeline only partially wrote real raw artifacts. The ingest now writes a reproducible raw/normalized/features layout, uses Kalshi’s historical/live cutoff boundary to combine archived and recent settled markets, stores raw trades and candlesticks alongside normalized parquet outputs, emits resumable checkpoints and a post-run summary artifact, and points Kalshi research/backtest defaults at real ingested data rather than the synthetic placeholder path.

#### Why
The prior layout made it too easy to mistake synthetic fixtures for real Kalshi historical research inputs. It also ignored the historical cutoff boundary, did not fetch live settled markets beyond that boundary, did not persist candlestick artifacts, and lacked restart-safe checkpoints or a run summary that could be audited after a long ingest. Those gaps made the resulting dataset less trustworthy for K-01/K-02 research and later paper-trading validation.

#### Files Changed
- `configs/kalshi_research.yaml`
- `scripts/generate_kalshi_synthetic_data.py`
- `src/trading_platform/cli/commands/kalshi_alpha_research.py`
- `src/trading_platform/cli/commands/kalshi_features.py`
- `src/trading_platform/cli/commands/kalshi_full_backtest.py`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `src/trading_platform/cli/grouped_parser.py`
- `src/trading_platform/kalshi/client.py`
- `src/trading_platform/kalshi/historical_ingest.py`
- `src/trading_platform/kalshi/research.py`
- `tests/kalshi/test_client.py`
- `tests/kalshi/test_historical_ingest.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `DOCUMENTATION.md`
- `MILESTONES.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/historical_ingest.py src/trading_platform/kalshi/client.py src/trading_platform/cli/commands/kalshi_historical_ingest.py src/trading_platform/cli/commands/kalshi_features.py src/trading_platform/cli/commands/kalshi_alpha_research.py src/trading_platform/cli/commands/kalshi_full_backtest.py src/trading_platform/cli/grouped_parser.py src/trading_platform/kalshi/research.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_client.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_client.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/historical_ingest.py src/trading_platform/kalshi/client.py src/trading_platform/cli/commands/kalshi_historical_ingest.py src/trading_platform/cli/commands/kalshi_features.py src/trading_platform/cli/commands/kalshi_alpha_research.py src/trading_platform/cli/commands/kalshi_full_backtest.py src/trading_platform/cli/grouped_parser.py src/trading_platform/kalshi/research.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_client.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_client.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Design Notes
The ingest path now treats `data/kalshi/raw/...` as the source-of-truth capture layer, `data/kalshi/normalized/...` as reproducible research inputs, and `data/kalshi/features/real/...` as derived feature artifacts. The market universe download is cutoff-aware: archived settled markets come from `/historical/markets`, while newer settled markets are pulled from the live `/markets` endpoint and filtered client-side by `close_time`. Trade downloads are likewise split across `/historical/trades` and the live `/markets/trades` endpoint around the Kalshi cutoff timestamp and then deduplicated. Raw candlestick payloads are now stored under `data/kalshi/raw/candles`, normalized candlestick parquet files are written under `data/kalshi/normalized/candles`, and both normalized and legacy resolution CSVs are emitted for backward compatibility.

The run now emits both `ingest_manifest.json` and `ingest_summary.json`, with the summary including output layout, cutoff timestamps, counts for markets/trades/candlesticks/resolutions/features, and any skipped or failed stages. Checkpoint state is stored in `data/kalshi/raw/ingest_checkpoint.json` and includes pagination cursors plus processed tickers so long backfills can resume without reprocessing already-normalized markets. Kalshi research-facing defaults were also updated so K-01/K-02 CLI flows now resolve to `data/kalshi/features/real` and `data/kalshi/normalized/resolution.csv` instead of the ambiguous synthetic locations.

#### Known Issues / Limitations
- The audit confirmed that the current repository snapshot still contains synthetic `SYNTH-*` artifacts under `data/kalshi/features` and `data/kalshi/resolution.csv`; those are now explicitly segregated by moving the synthetic generator to `data/kalshi/synthetic/...`, but existing files on disk should be regenerated or cleaned by the operator if they want a fully unambiguous local workspace.
- Candlestick normalization is intentionally tolerant of field-name variations because the live historical candlestick payload shape has not been validated here against a fresh live backfill in this network-restricted environment.
- The ingest summary records skipped or failed stages, but it does not yet persist per-page retry telemetry or per-market API latency diagnostics.

#### Recommended Next Milestone
- Faster bulk backfills via batched market partitioning and richer ingest telemetry once a full live historical refresh has been run against Kalshi production data

### Maintenance - Kalshi Auth Loader Hardening for Historical Ingest
Date: 2026-04-01
Status: DONE

#### Summary
Audited and hardened the Kalshi authentication loading path used by historical ingest and other live-read Kalshi workflows. The loader now supports either inline `private_key_pem` text or a file-based `private_key_path`, detects the common operator mistake where a file path is accidentally placed into `private_key_pem`, and raises clearer validation errors for missing or malformed key material before cryptography fails deep inside request signing.

#### Why
The historical ingest path now bridges recent settled markets through authenticated live endpoints, which exposed an ambiguity in the old auth contract: the code could accept a string in `private_key_pem` that was actually a file path, then fail later with an opaque PEM parsing error. The auth path also only loaded credentials from env, so YAML-driven workflows had no first-class way to specify file-based key material for operator convenience.

#### Files Changed
- `configs/kalshi.yaml`
- `src/trading_platform/kalshi/auth.py`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `src/trading_platform/cli/commands/kalshi_paper_run.py`
- `src/trading_platform/cli/commands/cross_market_monitor.py`
- `tests/kalshi/test_auth.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `DOCUMENTATION.md`
- `MILESTONES.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/auth.py src/trading_platform/cli/commands/kalshi_historical_ingest.py src/trading_platform/cli/commands/cross_market_monitor.py src/trading_platform/cli/commands/kalshi_paper_run.py tests/kalshi/test_auth.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_auth.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py tests/kalshi/test_client.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/auth.py src/trading_platform/cli/commands/kalshi_historical_ingest.py src/trading_platform/cli/commands/cross_market_monitor.py src/trading_platform/cli/commands/kalshi_paper_run.py tests/kalshi/test_auth.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_auth.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py tests/kalshi/test_client.py`

#### Design Notes
`KalshiConfig` now supports three loading modes without changing the request-signing interface:
- inline PEM text via `private_key_pem`
- file-based PEM loading via `private_key_path`
- env-backed loading via `from_env()`

Precedence is explicit: `private_key_pem` wins over `private_key_path`. If the `private_key_pem` value looks like a filesystem path rather than PEM text, the loader now raises a targeted error telling the operator to use `private_key_path` instead. File-based loading validates path existence and readability before config construction, and malformed PEM content now raises a clearer message from `_load_private_key(...)` explaining the expected formats.

The historical ingest CLI now accepts optional YAML auth config under:
- `auth.api_key_id`
- `auth.private_key_pem`
- `auth.private_key_path`

If that section is absent, existing env-based behavior remains intact. The same loader path is now reused by the Kalshi paper-trading and cross-market monitor Kalshi client builders so the auth contract stays consistent across Kalshi workflows.

#### Known Issues / Limitations
- The YAML auth path is intended as an operator convenience, but the recommended default remains file-based key loading or environment variables rather than embedding raw PEM text directly in a committed config file.
- The config still stores resolved PEM text on the `KalshiConfig` object after loading, because request signing expects immediate PEM access. This is acceptable for in-process use but is not a secret-storage abstraction.

#### Recommended Next Milestone
- Live validation of a full cutoff-bridged historical ingest using `auth.private_key_path` plus a follow-up pass on operator-facing ingest diagnostics

### K-04 - Cross-Market Monitoring for Kalshi vs Polymarket
Date: 2026-04-01
Status: DONE

#### Summary
Added a monitor-only cross-market research subsystem that normalizes active Kalshi and Polymarket markets into platform-owned models, matches economically equivalent markets conservatively, computes implied-probability dislocations, and writes structured artifacts for later analysis. The new workflow appends match and opportunity history over time, produces a machine-readable summary, and writes a markdown report with strongest and rejected examples.

#### Why
K-04 closes the next analytics gap in the Kalshi expansion path without enabling auto-trading. The platform can now compare equivalent event contracts across venues, reject ambiguous or settlement-mismatched pairs explicitly, and measure whether dislocations persist enough to justify deeper execution work later.

#### Files Changed
- `configs/kalshi_research.yaml`
- `src/trading_platform/cli/commands/cross_market_monitor.py`
- `src/trading_platform/cli/grouped_parser.py`
- `src/trading_platform/polymarket/__init__.py`
- `src/trading_platform/polymarket/client.py`
- `src/trading_platform/prediction_markets/__init__.py`
- `src/trading_platform/prediction_markets/cross_market.py`
- `tests/test_cli_grouping.py`
- `tests/test_cross_market_monitor.py`
- `tests/test_cross_market_monitor_cli.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/prediction_markets/cross_market.py src/trading_platform/prediction_markets/__init__.py src/trading_platform/polymarket/client.py src/trading_platform/cli/commands/cross_market_monitor.py src/trading_platform/cli/grouped_parser.py tests/test_cross_market_monitor.py tests/test_cross_market_monitor_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_cross_market_monitor.py tests/test_cross_market_monitor_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cross_market_monitor.py tests/test_cross_market_monitor_cli.py tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/prediction_markets/cross_market.py src/trading_platform/prediction_markets/__init__.py src/trading_platform/polymarket/client.py src/trading_platform/cli/commands/cross_market_monitor.py src/trading_platform/cli/grouped_parser.py tests/test_cross_market_monitor.py tests/test_cross_market_monitor_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_cross_market_monitor.py tests/test_cross_market_monitor_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cross_market_monitor.py tests/test_cross_market_monitor_cli.py tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Design Notes
The implementation keeps venue-specific behavior isolated behind `KalshiMarketAdapter` and `PolymarketMarketAdapter`, while the monitor itself operates on typed `NormalizedPredictionMarket` records and emits typed match/opportunity artifacts. Matching intentionally favors false negatives over false positives: it combines normalized title similarity, token overlap, category alignment, and expiration proximity, then rejects pairs with explicit ambiguity, numeric-token mismatches, or settlement-rule mismatches. The append-only JSONL artifacts support simple snapshot replay and persistence analysis without introducing a separate database layer.

#### Known Issues / Limitations
- The Polymarket adapter currently uses public Gamma market metadata and binary outcome prices only; it does not yet normalize deeper liquidity or order-book state.
- Settlement-rule mismatch detection is conservative but still text-based, so some economically equivalent markets may be rejected if venue wording differs materially.
- The monitor logs opportunities and persistence only. It does not estimate executable edge after fees, crossing costs, queue position, or transfer friction between venues.
- No live cross-market scan command was run during verification because the current environment is test-only and network-restricted.

#### Recommended Next Milestone
- K-05 - Signal ensemble and portfolio construction for prediction-market strategies, or a dedicated execution-feasibility pass if cross-market dislocations need cost-adjusted validation first.

### K-03 - Kalshi Paper Trading Integration
Date: 2026-04-02
Status: DONE

#### Summary
Added a Kalshi-specific paper trading runner that polls live markets, computes the existing Kalshi signal families on recent trades, applies a liquidity-aware paper execution model, persists restart-safe paper state, and emits structured paper-trading artifacts. The new CLI entry point is `trading-cli paper kalshi-run`.

#### Why
K-01 and K-02 established resolved-market evaluation and differentiated Kalshi signals, but execution realism and live paper loop coverage were still missing. This milestone closes that gap by wiring signal generation to a persistent paper-only execution loop with explicit liquidity and risk assumptions.

#### Files Changed
- `configs/kalshi_research.yaml`
- `src/trading_platform/kalshi/signal_registry.py`
- `src/trading_platform/kalshi/paper.py`
- `src/trading_platform/cli/commands/kalshi_full_backtest.py`
- `src/trading_platform/cli/commands/kalshi_paper_run.py`
- `src/trading_platform/cli/grouped_parser.py`
- `tests/kalshi/test_kalshi_paper.py`
- `tests/kalshi/test_kalshi_paper_cli.py`
- `tests/test_cli_grouping.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/paper.py src/trading_platform/kalshi/signal_registry.py src/trading_platform/cli/commands/kalshi_paper_run.py src/trading_platform/cli/commands/kalshi_full_backtest.py src/trading_platform/cli/grouped_parser.py tests/kalshi/test_kalshi_paper.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_paper.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/paper.py src/trading_platform/kalshi/signal_registry.py src/trading_platform/cli/commands/kalshi_paper_run.py src/trading_platform/cli/commands/kalshi_full_backtest.py src/trading_platform/cli/grouped_parser.py tests/kalshi/test_kalshi_paper.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_paper.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Design Notes
The new paper path is intentionally Kalshi-specific rather than forcing the broader equity paper service to absorb prediction-market semantics. `KalshiPaperTrader` persists cash, open positions, trade history, drawdown state, and halt state in a dedicated JSON contract, then writes:
- `kalshi_paper_positions.json`
- `kalshi_paper_trade_log.jsonl`
- `kalshi_paper_session_summary.json`
- `kalshi_paper_report.md`

Execution assumptions are explicit and serialized into both the session summary and each trade record. The paper execution model uses quoted bids plus implied asks from the Kalshi orderbook, applies a size-dependent spread penalty, caps size using top-level liquidity and recent market volume, and rejects thin, wide-spread, stale, sparse-trade, and near-settlement markets before entry. Positions are long-only at the contract layer (`BUY_YES` / `BUY_NO`) and are closed on settlement, signal reversal, or max holding window.

To keep K-01, K-02, and K-03 aligned, the previous CLI-local signal-family discovery was moved into `trading_platform.kalshi.signal_registry`. Both the resolved-market backtest CLI and the new paper CLI now build the same configured Kalshi signal family set, including the informed-flow family.

#### Known Issues / Limitations
- The current live paper path still depends on trade-level and top-of-book snapshots, not full queue reconstruction, so execution realism is improved but not exchange-matching accurate.
- The paper runner currently opens at most one position per market and chooses the single strongest eligible signal family per market rather than blending multiple Kalshi signals into one position-sizing decision.
- `python -m ruff check src/trading_platform/kalshi ... tests/kalshi ...` still reports pre-existing unused-import findings in untouched Kalshi files and tests outside the K-03 write scope, so verification used targeted Ruff checks plus broader pytest coverage.

#### Recommended Next Milestone
- K-04 - Cross-Market Arbitrage Monitor

### Phase 1.5 Utility - Validation Email Alerting
Date: 2026-04-01
Status: DONE

#### Summary
Added SMTP-based validation alerting in `src/trading_platform/reporting/validation_alerting.py` with a thin CLI wrapper at `scripts/send_validation_alert.py`. The utility evaluates existing validation artifacts, decides whether a validation alert should be triggered, supports dry-run and no-send modes, and uses a small dedupe registry to avoid resending the same alert for the same artifact/status signature.

#### Why
Daily validation and window review artifacts are now accumulating, but the operator still needs a compact way to be notified when a scheduled validation run fails or when validation outputs become concerning. This utility adds that operational support without changing trading behavior or broadening into a larger notification platform.

#### Files Changed
- `src/trading_platform/reporting/validation_alerting.py`
- `scripts/send_validation_alert.py`
- `tests/test_validation_alerting.py`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\validation_alerting.py scripts\send_validation_alert.py tests\test_validation_alerting.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_validation_alerting.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\validation_alerting.py scripts\send_validation_alert.py tests\test_validation_alerting.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_validation_alerting.py`

#### Design Notes
The utility reads existing artifacts first:
- `daily_validation_run_summary.json`
- `daily_system_report.json`
- `validation_window_review.json`

It currently supports three deterministic alert types:
- `run_failure`
- `daily_concerning_status`
- `window_concerning_status`

SMTP settings can be supplied by CLI args or environment variables. Supported environment variables:
- `TP_ALERT_SMTP_HOST`
- `TP_ALERT_SMTP_PORT`
- `TP_ALERT_SMTP_USERNAME`
- `TP_ALERT_SMTP_PASSWORD`
- `TP_ALERT_SMTP_USE_TLS`
- `TP_ALERT_FROM`
- `TP_ALERT_TO`
- `TP_ALERT_SUBJECT_PREFIX`

Resolution precedence:
- CLI arguments override environment variables
- environment variables are used when the corresponding CLI arguments are omitted
- if required SMTP fields are still missing, the command fails with a clear error naming the missing fields

Environment setup examples:
- Windows PowerShell:
  `setx TP_ALERT_SMTP_HOST smtp.example.com`
  `setx TP_ALERT_SMTP_PORT 587`
  `setx TP_ALERT_SMTP_USERNAME alerts@example.com`
  `setx TP_ALERT_SMTP_PASSWORD your-secret`
  `setx TP_ALERT_SMTP_USE_TLS true`
  `setx TP_ALERT_FROM alerts@example.com`
  `setx TP_ALERT_TO ops@example.com,backup@example.com`
- Bash:
  `export TP_ALERT_SMTP_HOST=smtp.example.com`
  `export TP_ALERT_SMTP_PORT=587`
  `export TP_ALERT_SMTP_USERNAME=alerts@example.com`
  `export TP_ALERT_SMTP_PASSWORD=your-secret`
  `export TP_ALERT_SMTP_USE_TLS=true`
  `export TP_ALERT_FROM=alerts@example.com`
  `export TP_ALERT_TO=ops@example.com,backup@example.com`

Example commands:
- `python scripts/send_validation_alert.py --daily-run-summary artifacts\paper\run_live_validation\2026-04-01T09-30-00\daily_validation_run_summary.json --daily-report artifacts\paper\run_live_validation\2026-04-01T09-30-00\daily_system_report.json --dry-run`
- `python scripts/send_validation_alert.py --validation-root artifacts\paper\run_live_validation --latest-successful-run --dry-run`
- `python scripts/send_validation_alert.py --window-review artifacts\paper\run_live_validation\validation_window_review.json --smtp-host smtp.gmail.com --smtp-port 587 --smtp-username you@example.com --smtp-password %TP_ALERT_SMTP_PASSWORD% --smtp-use-tls --from you@example.com --to you@example.com`

Latest-successful-run resolution rules:
- scan timestamped child folders under the validation root
- require `daily_validation_run_summary.json`
- require `daily_system_report.json`
- require parseable JSON
- when `--latest-successful-run` is used, require non-failure run summary status with zero paper/report exit codes

#### Known Issues / Limitations
- This utility is Phase 1.5 validation alerting only; it is not intended to replace broader production alert infrastructure.
- Dedupe is local and file-based. If the registry is deleted or a status signature changes, the alert may be sent again.
- The email body is intentionally compact and text-only; it summarizes top-level statuses and reasons rather than embedding full artifact payloads.

#### Recommended Next Milestone
- Phase 1.5 checkpoint review and human go/no-go decision

### K-01 - Kalshi Resolved-Market Backtest Framework
Date: 2026-04-01
Status: DONE

#### Summary
Rebuilt the Kalshi backtest path into a resolved-market research runner that loads local historical artifacts, evaluates only settled markets, supports explicit timing and execution assumptions, and writes structured JSON, JSONL, CSV, and Markdown outputs. The upgraded `research kalshi-full-backtest` command now reads YAML config values for signal families, artifact paths, and execution assumptions, while preserving compatibility outputs for older Kalshi research flows.

#### Why
The historical ingest path is now strong enough to support resolved-market evaluation, but the previous Kalshi backtest path was too lightweight for production-style research. This milestone establishes a typed, auditable backtest runner so Kalshi signal families can be evaluated with explicit assumptions and dashboard-friendly artifacts before extending into paper trading or additional signal families.

#### Files Changed
- `src/trading_platform/kalshi/backtest.py`
- `src/trading_platform/cli/commands/kalshi_full_backtest.py`
- `src/trading_platform/cli/grouped_parser.py`
- `tests/kalshi/test_kalshi_backtest.py`
- `tests/test_cli_grouping.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/backtest.py src/trading_platform/cli/commands/kalshi_full_backtest.py src/trading_platform/cli/grouped_parser.py tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_research.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_research.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/backtest.py src/trading_platform/cli/commands/kalshi_full_backtest.py src/trading_platform/cli/grouped_parser.py tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_research.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_research.py tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Design Notes
The runner remains additive and preserves the existing `KalshiBacktester.run(...)` contract while extending it with explicit execution assumptions through `KalshiExecutionAssumptions`. It now:
- filters to resolved markets only using resolution data and raw market status metadata when available
- selects entries using either `hours_before_close` or `last_bar`
- supports optional holding-window exits before settlement
- applies explicit entry and exit slippage assumptions
- maps signal values into predicted probabilities so calibration can be reported as Brier score
- emits summary breakdowns by signal family, market category, and confidence bucket

The CLI keeps compatibility artifacts (`full_backtest_results.csv`, `full_backtest_summary.md`) by copying from the richer structured output set after the run completes. The base-rate signal family is wired directly into the framework so the resolved-market runner can be exercised immediately using existing ingest-produced `base_rate_edge` features.

#### Known Issues / Limitations
- The current backtest uses simple point-based execution assumptions rather than a full order-book or liquidity-aware execution model.
- Category and close-time metadata are taken from local raw market JSON artifacts when present; missing metadata falls back to `"unknown"` category and simpler timing behavior.
- The confidence and calibration mapping is intentionally lightweight for K-01 and should be revisited once more advanced Kalshi signal families are introduced.

#### Recommended Next Milestone
- K-02 - Informed Flow Signal Family

### K-02 - Informed Flow Signal Family
Date: 2026-04-01
Status: DONE

#### Summary
Added the first Kalshi informed-flow signal family on top of the resolved-market backtest framework. The informed-flow path now scores taker imbalance, large aggressive trade footprint, and unexplained short-horizon price moves using typed, configurable signal builders, and the backtester persists those richer signal diagnostics into the existing Kalshi summary, diagnostics, trade-log, and report artifacts.

#### Why
K-01 established the resolved-market backtest and reporting loop, but it still needed differentiated microstructure-style signals to evaluate whether trade-flow features produce real edge. This milestone adds that first non-price-history signal family without changing the ingest contract or creating a separate reporting stack.

#### Files Changed
- `configs/kalshi_research.yaml`
- `src/trading_platform/kalshi/signals.py`
- `src/trading_platform/kalshi/signals_informed_flow.py`
- `src/trading_platform/kalshi/backtest.py`
- `src/trading_platform/cli/commands/kalshi_full_backtest.py`
- `tests/kalshi/test_signals_informed_flow.py`
- `tests/kalshi/test_kalshi_backtest.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/signals.py src/trading_platform/kalshi/signals_informed_flow.py src/trading_platform/kalshi/backtest.py src/trading_platform/cli/commands/kalshi_full_backtest.py tests/kalshi/test_signals_informed_flow.py tests/kalshi/test_kalshi_backtest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_signals_informed_flow.py tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_research.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/signals.py src/trading_platform/kalshi/signals_informed_flow.py src/trading_platform/kalshi/backtest.py src/trading_platform/cli/commands/kalshi_full_backtest.py tests/kalshi/test_signals_informed_flow.py tests/kalshi/test_kalshi_backtest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_signals_informed_flow.py tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_research.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi tests/test_cli_grouping.py tests/test_research_cli_commands.py`

#### Design Notes
The existing Polars-based informed-flow feature builders remain intact. K-02 adds a separate typed scoring layer on top of those feature columns through `InformedFlowSignalConfig` and richer `KalshiSignalFamily.build_signal_frame(...)` outputs. That scoring layer standardizes:
- signed signal value used by the backtester
- direction
- confidence
- signal probability
- supporting features copied into diagnostics and the trade log

Thresholds are configurable through `signals.informed_flow` in `configs/kalshi_research.yaml`, and the full-backtest CLI now constructs informed-flow signal families from that config. The resolved-market backtester consumes these richer signal frames when available and writes:
- candidate-signal summaries by family
- candidate breakdowns by category and confidence bucket
- supporting feature summaries by signal family
- supporting feature payloads in each `kalshi_trade_log.jsonl` row

The older scalar-signal behavior is preserved by keeping `KalshiSignalFamily.score()` backward-compatible. Existing signal-family tests and non-Kalshi code paths continue to see raw feature-column scoring semantics unless they opt into the richer signal-frame API.

#### Known Issues / Limitations
- These signals still operate on bar-aggregated historical artifacts rather than a full limit-order-book reconstruction, so “aggressive” flow is inferred from taker-side and size proxies rather than true queue dynamics.
- Unexplained-move diagnostics use a simple catalyst penalty based on the local base-rate category match, not a complete event calendar.
- The backtester still uses fixed point slippage and simple holding-window exits; K-02 improves signal quality and diagnostics, not execution realism.

#### Recommended Next Milestone
- K-03 - Kalshi Paper Trading Integration

### Phase 1.5 Utility - Daily Validation Runner
Date: 2026-04-01
Status: DONE

#### Summary
Added a compact orchestration utility at `scripts/run_daily_validation.py` that runs one paper-trading validation cycle and then immediately generates the Phase 1.5 daily system report against that exact artifact bundle. The runner also writes a deterministic `daily_validation_run_summary.json` artifact for scheduled-run auditability.

#### Why
Phase 1.5 needs a repeatable daily validation workflow, not just a read-only report builder. This utility makes scheduled paper-validation runs consistent and inspectable without duplicating paper or reporting business logic and without expanding into a scheduler or service.

#### Files Changed
- `src/trading_platform/reporting/daily_validation_runner.py`
- `scripts/run_daily_validation.py`
- `tests/test_daily_validation_runner.py`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\daily_validation_runner.py scripts\run_daily_validation.py tests\test_daily_validation_runner.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_daily_validation_runner.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\reporting\daily_validation_runner.py scripts\run_daily_validation.py tests\test_daily_validation_runner.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_daily_validation_runner.py`

#### Design Notes
The runner is intentionally thin and reuses existing entry points through subprocess orchestration rather than reimplementing the paper pipeline or the daily report logic. It now routes config inputs explicitly before launching the paper subprocess:
- activated strategy-portfolio inputs such as `activated_strategy_portfolio.json`, or JSON payloads with `active_strategies` / `strategies` / `sleeves`, are executed through `paper run-multi-strategy`
- workflow-style paper configs with `symbols`, `universe`, or `preset` are executed through `paper run`

The run summary artifact now records `config_type`, `paper_command_used`, and `execution_mode` so scheduled validation runs are auditable. The runner also supports optional `--timestamp-run-dir` to isolate repeated runs under a stable root while updating a small latest-run pointer file.

Integrated alerting is optional and reuses `src/trading_platform/reporting/validation_alerting.py` directly rather than reimplementing SMTP or alert decision logic. New flags:
- `--send-alerts`
- `--alert-dry-run`
- `--alert-no-send`
- `--alert-registry-path`
- `--alert-decision-output`
- `--alert-subject-prefix`

When alerting is enabled, the runner evaluates alerts against the exact current-run artifacts:
- `daily_validation_run_summary.json`
- `daily_system_report.json` when present

The summary artifact now includes an `alerting` section with:
- whether alerting was enabled
- whether alerts were evaluated
- whether an alert triggered
- alert types
- send mode and sent status
- decision artifact path
- any alerting error

Example command:
- `python scripts/run_daily_validation.py --config artifacts\strategy_portfolio\run_current\activated\activated_strategy_portfolio.json --state-path artifacts\paper\validation_state.json --output-dir artifacts\paper\run_live_validation --report-json daily_system_report.json --report-md daily_system_report.md --strict-report`
- `python scripts/run_daily_validation.py --config artifacts\strategy_portfolio\run_current\activated\activated_strategy_portfolio.json --state-path artifacts\paper\validation_state.json --output-dir artifacts\paper\run_live_validation --report-json daily_system_report.json --timestamp-run-dir`
- `python scripts/run_daily_validation.py --config artifacts\strategy_portfolio\run_current\activated\activated_strategy_portfolio.json --state-path artifacts\paper\validation_state.json --output-dir artifacts\paper\run_live_validation --report-json daily_system_report.json --timestamp-run-dir --send-alerts --alert-subject-prefix [VALIDATION]`
- `python scripts/run_daily_validation.py --config artifacts\strategy_portfolio\run_current\activated\activated_strategy_portfolio.json --state-path artifacts\paper\validation_state.json --output-dir artifacts\paper\run_live_validation --report-json daily_system_report.json --timestamp-run-dir --send-alerts`

If SMTP environment variables are already set for `send_validation_alert.py`, the runner can send alerts without repeating SMTP CLI flags.

Exit-code policy:
- `0` when paper/report succeed and alerting succeeds or is disabled
- `1` when config detection fails, the paper run fails, or the report fails in strict mode
- `2` when paper/report succeed but the integrated alerting step fails

#### Known Issues / Limitations
- The runner is an orchestration utility for scheduled Phase 1.5 validation runs; it is not a scheduler, daemon, or monitoring service.
- In `--timestamp-run-dir` mode, relative report paths are resolved under the timestamped artifact directory; absolute report paths are respected as provided.
- Relative `--alert-decision-output` paths are also resolved under the current artifact directory so each timestamped run keeps its own alert decision artifact.
- Config routing is intentionally conservative and currently recognizes multi-strategy validation inputs primarily through activated-portfolio filenames and stable JSON keys. Ambiguous configs fail fast instead of falling back to the wrong paper command.
- The summary artifact records exit status, warnings, and alerting state, but it does not capture subprocess stdout/stderr payloads.

#### Recommended Next Milestone
- Phase 1.5 re-evaluation checkpoint execution and review

### Maintenance - Kalshi Historical Ingest CLI and Rate Limit Fixes
Date: 2026-04-01
Status: DONE

#### Summary
Fixed the `trading-cli data kalshi historical-ingest` command so it constructs a real `HistoricalIngestConfig` from `configs/kalshi.yaml` and project-root-relative output paths instead of relying on mismatched config shapes. Historical request pacing is now driven by the ingest config all the way through the Kalshi client, and the default historical sleep was raised from `0.2` to `0.05` seconds per request. The Kalshi config comment was also clarified so `environment.demo` stays `false` for production-only historical reads.

#### Why
The historical ingest CLI was broken because it mixed YAML dicts with the dataclass-based ingest pipeline contract. The historical client also had a hardcoded public-endpoint sleep value, which meant the configured rate limit was not actually being honored consistently. These changes keep the historical ingest path explicit, typed, and aligned with the intended Kalshi production-read behavior.

#### Files Changed
- `configs/kalshi.yaml`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `src/trading_platform/cli/grouped_parser.py`
- `src/trading_platform/kalshi/client.py`
- `src/trading_platform/kalshi/historical_ingest.py`
- `tests/kalshi/test_client.py`
- `tests/kalshi/test_historical_ingest.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/cli/commands/kalshi_historical_ingest.py src/trading_platform/cli/grouped_parser.py src/trading_platform/kalshi/client.py src/trading_platform/kalshi/historical_ingest.py tests/kalshi/test_client.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\trading-cli.exe data kalshi historical-ingest --config configs/kalshi.yaml`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/cli/commands/kalshi_historical_ingest.py src/trading_platform/cli/grouped_parser.py src/trading_platform/kalshi/client.py src/trading_platform/kalshi/historical_ingest.py tests/kalshi/test_client.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Design Notes
The CLI now pins the historical ingest artifact layout to project-root-relative Kalshi paths, uses `ingestion.backfill_days` for the lookback window, eagerly creates the required directories, and forces historical reads onto Kalshi production endpoints even when a demo trading config is present. The client now accepts `historical_sleep_sec` at construction time and forwards optional `sleep` and pagination values through the historical market/trade pagination helpers so the ingest pipeline can control read pacing explicitly.

#### Known Issues / Limitations
- The requested live verification command now reaches the production historical API and starts fetching real markets, but Kalshi returned HTTP `429 Too Many Requests` on a later paginated request during verification. A direct one-page live read succeeded and returned these first five real tickers:
  - `KXMVESPORTSMULTIGAMEEXTENDED-S20251E55BB74B0B-0B61CF13553`
  - `KXMVESPORTSMULTIGAMEEXTENDED-S2025CDD0415C565-E866B2E1C89`
  - `KXMVESPORTSMULTIGAMEEXTENDED-S2025C9A4A812186-00EBEF44D86`
  - `KXMVESPORTSMULTIGAMEEXTENDED-S2025C24E2BF91C5-59441BE2D3B`
  - `KXMVESPORTSMULTIGAMEEXTENDED-S20255ED2465C7C0-C6BA13A6CAD`
- The full repository test suite currently contains `1239` tests rather than `1234`; all `1239` passed after these changes.

#### Recommended Next Milestone
- Phase 1.5 checkpoint review and human go/no-go decision

### Maintenance - Kalshi Historical Ingest 429 Retry Handling
Date: 2026-04-01
Status: DONE

#### Summary
Added robust HTTP 429 handling to the Kalshi public historical client path with exponential backoff, jitter, and `Retry-After` support. The historical markets pagination path now applies close-time filtering client-side instead of sending unsupported close-time query parameters, and the CLI start message now reflects the configured lookback window instead of hardcoding `365`.

#### Why
The historical ingest command was reaching the live Kalshi historical API but could fail during pagination with `429 Too Many Requests`. The client also sent close-time filters to `/historical/markets` even though that endpoint paginates by cursor and does not expose close-time query parameters, so the lookback window needed to be enforced locally without changing the ingest contract.

#### Files Changed
- `src/trading_platform/kalshi/client.py`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `tests/kalshi/test_client.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/client.py src/trading_platform/cli/commands/kalshi_historical_ingest.py tests/kalshi/test_client.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src/trading_platform/kalshi/client.py src/trading_platform/cli/commands/kalshi_historical_ingest.py tests/kalshi/test_client.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe`

#### Design Notes
The retry logic is intentionally scoped to `_get_public()` so authenticated Kalshi flows and non-Kalshi code paths remain unchanged. Immediate success still behaves as before. On `429`, the client now retries up to five times, prefers `Retry-After` when Kalshi provides it, and otherwise uses bounded exponential backoff with jitter before retrying the same request. Non-429 HTTP failures still raise the underlying `requests.HTTPError`, but now include clearer context about which Kalshi path failed.

The historical markets client keeps the existing `min_close_ts` / `max_close_ts` method signature for backward compatibility, but applies those bounds client-side after each page is fetched. This keeps the ingest lookback semantics intact while avoiding unsupported or misleading query parameters on the Kalshi historical markets endpoint.

#### Known Issues / Limitations
- Client-side close-time filtering preserves the ingest contract but does not reduce the number of pages Kalshi must return before filtering, so large historical backfills may still take time even when the requested lookback window is small.
- The retry policy is currently hardcoded for the public historical client path. If operators need finer control, retry counts and backoff bounds can be promoted into config later.

#### Recommended Next Milestone
- Phase 1.5 checkpoint review and human go/no-go decision

### K-00 - Kalshi Ingest Validation and Data-Quality Reporting
Date: 2026-04-02
Status: DONE

#### Summary
Added a typed Kalshi post-ingest validation subsystem that audits normalized market, trade, candle, resolution, and ingest-metadata artifacts and writes three operator-facing outputs: `kalshi_data_validation_summary.json`, `kalshi_data_validation_details.json`, and `kalshi_data_validation_report.md`. The historical ingest summary now carries explicit filter-breakdown diagnostics, the CLI exposes `trading-cli data kalshi validate-dataset`, historical ingest runs validation automatically unless skipped, and Kalshi backtest/paper commands can optionally require a passing validation summary before execution.

#### Why
K-01, K-02, and K-03 now depend on real Kalshi data being trustworthy. This milestone adds an explicit audit step between ingest and downstream use so the platform can fail fast on empty, inconsistent, duplicated, poorly covered, or synthetic-contaminated datasets instead of silently trusting the normalized layout.

#### Files Changed
- `configs/kalshi.yaml`
- `src/trading_platform/kalshi/historical_ingest.py`
- `src/trading_platform/kalshi/validation.py`
- `src/trading_platform/cli/commands/kalshi_validate_dataset.py`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `src/trading_platform/cli/commands/kalshi_full_backtest.py`
- `src/trading_platform/cli/commands/kalshi_paper_run.py`
- `src/trading_platform/cli/grouped_parser.py`
- `tests/kalshi/test_validation.py`
- `tests/kalshi/test_validation_cli.py`
- `tests/kalshi/test_historical_ingest.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\validation.py src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_validate_dataset.py src\trading_platform\cli\commands\kalshi_historical_ingest.py src\trading_platform\cli\commands\kalshi_full_backtest.py src\trading_platform\cli\commands\kalshi_paper_run.py src\trading_platform\cli\grouped_parser.py tests\kalshi\test_validation.py tests\kalshi\test_validation_cli.py tests\kalshi\test_historical_ingest.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_validation.py tests/kalshi/test_validation_cli.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_kalshi_paper_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_paper_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_cli_grouping.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\validation.py src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_validate_dataset.py src\trading_platform\cli\commands\kalshi_historical_ingest.py src\trading_platform\cli\commands\kalshi_full_backtest.py src\trading_platform\cli\commands\kalshi_paper_run.py src\trading_platform\cli\grouped_parser.py tests\kalshi\test_validation.py tests\kalshi\test_validation_cli.py tests\kalshi\test_historical_ingest.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_validation.py tests/kalshi/test_validation_cli.py tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_kalshi_paper_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_backtest.py tests/kalshi/test_kalshi_paper_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_cli_grouping.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -c "from trading_platform.kalshi.validation import run_kalshi_data_validation; result=run_kalshi_data_validation(); print(result.status); print(result.artifacts.summary_path); print(result.summary_payload)"`

#### Design Notes
The validator is additive and typed. `KalshiDataValidationConfig`, `KalshiValidationThresholds`, `KalshiValidationFinding`, and `KalshiDataValidationResult` keep the policy and outputs explicit instead of passing around loosely structured dicts. Validation supports the current normalized directory layout and tolerates file-or-directory trade/candle inputs so future ingest refactors can reuse the same runner.

The ingest pipeline now emits `filter_diagnostics` with actual drop counts by category, series-pattern, min-volume, and bracket filters. That keeps validation grounded in what the ingest really did rather than forcing the report to infer exclusions from partial summary counts.

Backtest and paper validation gating is optional. Existing Kalshi research and paper flows remain unchanged unless the operator explicitly passes `--require-validation-pass`.

Running the new validator against the repository's current on-disk Kalshi dataset produced `FAIL`: `data/kalshi/normalized/markets.parquet` and `data/kalshi/normalized/resolution.csv` were absent, normalized trade/candle directories were empty, and the current real-data normalized dataset therefore had zero market/trade/candle coverage. That result is expected from the present workspace contents and is why the new default thresholds remain intentionally conservative.

#### Known Issues / Limitations
- The validator currently treats missing/empty normalized layers as coverage failures but does not yet distinguish "ingest never completed" from "ingest completed badly" with separate status codes.
- Cross-layer schema checks focus on ticker presence and required-column availability; they do not yet validate deeper semantic invariants such as candle monotonicity or trade-side/value consistency.
- The current workspace Kalshi dataset is incomplete, so threshold tuning based on real observed coverage distributions is still preliminary.

#### Recommended Next Milestone
- K-05 - Signal Ensemble & Portfolio Construction

### Maintenance - Kalshi Historical Ingest Structured Progress and Stage Status Reporting
Date: 2026-04-02
Status: DONE

#### Summary
Added a reusable ingest observability layer and wired it into the Kalshi historical ingest pipeline so long-running runs now emit heartbeat-updated machine-readable status plus explicit stage transitions. Each run now writes `artifacts/kalshi_ingest/<run_id>/ingest_status.json` during execution and `artifacts/kalshi_ingest/<run_id>/ingest_run_summary.json` at the end, while the ingest result and CLI surface those artifact paths for operators.

#### Why
The live-bridge pagination fixes made Kalshi ingest safer, but operators still needed a clear way to answer whether a run was actively fetching pages, processing retained markets, writing normalized outputs, checkpointing, failing fast, or simply finished. This change adds explicit run-level and stage-level state without changing ingest semantics or trading behavior.

#### Files Changed
- `configs/kalshi.yaml`
- `src/trading_platform/ingest/status.py`
- `src/trading_platform/kalshi/historical_ingest.py`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `tests/kalshi/test_historical_ingest.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\ingest\status.py src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_historical_ingest.py tests\kalshi\test_historical_ingest.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\ingest\status.py src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_historical_ingest.py tests\kalshi\test_historical_ingest.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py`

#### Design Notes
The new `IngestStatusTracker` is generic and additive. It tracks stable per-stage records with timestamps, elapsed time, item counts, rates, last progress messages, and arbitrary counters. Kalshi historical ingest now uses the following explicit stages:
- `initialization`
- `checkpoint_load`
- `cutoff_discovery`
- `market_universe_fetch`
- `retained_market_processing`
- `normalization`
- `checkpoint_write`
- `final_summary`

The status artifact is updated on every stage transition and on every heartbeat-worthy progress update such as page fetches, retained-market starts, market completions, and checkpoint writes. This means external tooling can watch `updated_at`, `current_stage`, and the run counters to distinguish:
- slow but healthy page scanning
- retained-market processing that has started and is completing
- zero-retained runs that exit cleanly
- fail-fast stops such as `fail_fast_zero_retained_pages` or `fail_fast_retained_without_processing`
- normal live-bridge completion reasons such as `aged_out_pages` or `cursor_exhausted`

Run-level counters now include page counts, retained-market counts, processed/completed/failed counts, raw-market writes, normalized-output writes, stop reason, and fail-fast reason. The final run summary also records page diagnostics, filter diagnostics, checkpoint-write counts, the first retained-processing milestone, and top error categories from skipped or failed market stages.

Observability writes are resilient by design. If the status tracker cannot write one of its JSON artifacts, it logs a warning and the ingest continues rather than failing the data pipeline for a reporting-only problem.

#### Known Issues / Limitations
- Stage reporting currently focuses on the Kalshi historical ingest path; other ingest pipelines do not yet reuse the tracker.
- `market_universe_fetch` and `retained_market_processing` can overlap conceptually during streaming ingest, so `current_stage` reflects the most recent active substage rather than a full concurrent-stage graph.
- The final summary path is separate from the existing Kalshi `ingest_summary.json`; both are written intentionally so existing downstream tooling remains backward-compatible.

#### Recommended Next Milestone
- K-05 - Signal Ensemble & Portfolio Construction

### Maintenance - Kalshi Historical Ingest Early Filtering and Streaming Retained-Market Processing
Date: 2026-04-02
Status: DONE

#### Summary
Refactored Kalshi historical ingest so category, excluded-series-pattern, synthetic-marker, and minimum-volume filters are enforced during each paginated market fetch instead of after the full market universe is written to disk. The ingest now writes raw market JSON only for retained markets, logs per-page fetch/retain/discard progress with sample retained tickers, and can begin trade/candle/feature processing as retained markets arrive instead of waiting for the entire fetched universe to finish.

#### Why
The previous flow fetched and wrote large numbers of irrelevant markets before any real-data normalization could start. In practice that meant broad sports and bracket universes could dominate runtime even when config intended to keep only Economics / Politics / Climate and exclude series such as `KXNBA`. This change reduces wasted I/O and lets normalization start much earlier in the run.

#### Files Changed
- `src/trading_platform/kalshi/historical_ingest.py`
- `tests/kalshi/test_historical_ingest.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\historical_ingest.py tests\kalshi\test_historical_ingest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_validation.py tests/kalshi/test_validation_cli.py tests/kalshi/test_kalshi_paper_cli.py tests/kalshi/test_kalshi_backtest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_cli_grouping.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\historical_ingest.py tests\kalshi\test_historical_ingest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_validation.py tests/kalshi/test_validation_cli.py tests/kalshi/test_kalshi_paper_cli.py tests/kalshi/test_kalshi_backtest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/test_cli_grouping.py`

#### Design Notes
The fetch loop now applies early filters page-by-page through dedicated helpers before calling `_write_raw_market(...)`. This means irrelevant markets never hit `data/kalshi/raw/markets`. The processing path itself was moved into `_process_market_artifacts(...)`, and the new `_run_streaming()` flow hands retained markets into that helper as soon as they are discovered while still preserving checkpointed resumability.

The late whole-set filter pass remains only for any logic that still depends on seeing the retained set together, especially `max_markets_per_event`. Early filters now cover the highest-volume waste sources:
- preferred category allowlist
- excluded series patterns / prefix-like regexes
- minimum volume threshold
- synthetic ticker suppression

Observability was expanded with:
- page count
- total fetched / retained / discarded market counts
- discarded counts by category / series-pattern / min-volume / synthetic reason
- retained sample tickers
- last pagination cursor

#### Known Issues / Limitations
- `max_markets_per_event` still requires a retained-set pass after download because bracket detection depends on cross-market event counts; it is not yet fully enforced during pagination.
- The before/after performance comparison for this change is architectural rather than benchmarked against a live Kalshi run in this session. The new path eliminates raw writes and downstream processing for discarded markets and allows retained-market normalization to begin before full-universe completion, but no live wall-clock benchmark was executed here.
- The old non-streaming body remains in `run()` after an immediate return into `_run_streaming()` for review safety; it is unreachable and can be deleted in a future cleanup pass once this refactor is fully accepted.

#### Recommended Next Milestone
- K-05 - Signal Ensemble & Portfolio Construction

### Maintenance - Kalshi Authenticated Live-Bridge 429 Retry Handling
Date: 2026-04-02
Status: DONE

#### Summary
Added robust `429 Too Many Requests` retry handling to the authenticated Kalshi `GET` path used by the live market bridge during historical ingest. Authenticated reads now honor `Retry-After` when present, otherwise use bounded exponential backoff with jitter, and log rate-limit events as `live/authenticated` so operators can distinguish them from public historical endpoint throttling. Historical ingest config and YAML now expose separate authenticated live-read throttle settings.

#### Why
The historical ingest had progressed to the live bridge stage but could fail on authenticated `/markets` pagination with a hard `429` from Kalshi. That blocked normalized output generation even though the public historical path already had rate-limit resilience. This maintenance change hardens the recent-settled live bridge without changing public historical retry semantics, auth header behavior, or non-429 error handling.

#### Files Changed
- `configs/kalshi.yaml`
- `src/trading_platform/kalshi/client.py`
- `src/trading_platform/kalshi/historical_ingest.py`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `tests/kalshi/test_client.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\client.py src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_historical_ingest.py tests\kalshi\test_client.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_historical_ingest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py tests/kalshi/test_broker.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\client.py src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_historical_ingest.py tests\kalshi\test_client.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_historical_ingest_cli.py tests/kalshi/test_historical_ingest.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py tests/kalshi/test_broker.py`

#### Design Notes
The Kalshi client now uses a shared GET retry helper so public and authenticated reads follow the same control flow while keeping separate retry policies and log labels. Public historical calls still use their existing policy, while authenticated live calls now default to the same retry shape unless ingest overrides it. Auth headers are still built exactly once per request path and reused across retries, preserving the current signing/header behavior.

Historical ingest config now carries:
- `authenticated_request_sleep_sec`
- `authenticated_rate_limit_max_retries`
- `authenticated_rate_limit_backoff_base_sec`
- `authenticated_rate_limit_backoff_max_sec`
- `authenticated_rate_limit_jitter_max_sec`

The historical ingest CLI forwards those settings into `KalshiClient` and records them in the ingest summary artifact for auditability.

#### Known Issues / Limitations
- This change hardens retry handling for authenticated GET requests only. Authenticated POST and DELETE behavior is unchanged.
- No live Kalshi benchmark run was executed in this session, so the recommended throttle values are conservative rather than empirically optimized against current account-level limits.
- The shared helper distinguishes `public historical` from `live/authenticated` in logs, but it does not yet emit structured per-path retry counters into a dedicated monitoring artifact.

#### Recommended Next Milestone
- K-05 - Signal Ensemble & Portfolio Construction

### Maintenance - Kalshi Live-Bridge Pagination and Streaming Processing Fix
Date: 2026-04-02
Status: DONE

#### Summary
Audited and fixed the Kalshi historical-ingest live bridge so settled live-market pagination no longer walks the broader settled universe indefinitely while waiting to finish before top-level normalization. The live fetch loop now stops once pages fall completely outside the ingest lookback window, logs per-page retained and discarded ticker samples with discard reasons, writes raw market JSON only when retained-market processing actually starts, and fails fast if retained-market volume grows without any processing progress.

#### Why
The previous streaming refactor still had a practical stall mode:
- live `/markets?status=settled` pagination only filtered by close time client-side and never stopped when pages became entirely older than the lookback window
- `max_markets_per_event` remained a late whole-set filter, so the live bridge could still retain too many markets before the full-universe pass
- top-level normalized market and resolution outputs were only written after `_download_market_universe()` completed
- `processed_tickers` only grows after a market survives trade fetch plus downstream normalization steps, so long stretches of retained-but-not-processed markets looked like zero progress in checkpoint state

Together, that could produce a run where live-market fetching dominated the session, `market_download_complete` stayed `false`, top-level normalized outputs never appeared, and operators saw massive raw retention without clear visibility into whether any retained market had actually begun processing.

#### Files Changed
- `configs/kalshi.yaml`
- `src/trading_platform/kalshi/historical_ingest.py`
- `src/trading_platform/cli/commands/kalshi_historical_ingest.py`
- `tests/kalshi/test_historical_ingest.py`
- `tests/kalshi/test_historical_ingest_cli.py`
- `MILESTONES.md`
- `DOCUMENTATION.md`

#### Tests Run
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_historical_ingest.py tests\kalshi\test_historical_ingest.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py`

#### Verification Commands
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\python.exe -m ruff check src\trading_platform\kalshi\historical_ingest.py src\trading_platform\cli\commands\kalshi_historical_ingest.py tests\kalshi\test_historical_ingest.py tests\kalshi\test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_historical_ingest.py tests/kalshi/test_historical_ingest_cli.py`
- `C:\Users\bradl\PycharmProjects\trading_platform\.venv\Scripts\pytest.exe tests/kalshi/test_client.py tests/kalshi/test_kalshi_paper_cli.py tests/test_cross_market_monitor_cli.py`

#### Design Notes
The core behavioral fixes are:
- live-page stop condition once a settled `/markets` page is completely older than `lookback_days`
- richer page diagnostics:
  - fetched count
  - retained count
  - discarded count by reason
  - retained ticker samples
  - discarded ticker samples with reasons
- first-processing log emitted when `_process_market_artifacts(...)` begins for the first retained market
- raw market JSON moved behind processing start instead of being written during page fetch
- configurable fail-fast guards:
  - `max_live_pages_without_retained_markets`
  - `max_raw_markets_without_processing`

The ingest also now tolerates a few alternative live-market field names such as `market_ticker`, `seriesTicker`, `eventTicker`, `market_category`, and `expiration_time` in the filtering and normalization helpers so the live bridge is less brittle to payload-shape differences.

#### Known Issues / Limitations
- `max_markets_per_event` is still fundamentally a whole-set filter. This fix prevents it from blocking all downstream progress indefinitely, but it is not yet a fully online event-cardinality filter.
- Top-level normalized market and resolution outputs are still emitted after market-universe download completes; the fix here ensures the live bridge completes promptly when pages age out, while normalized trade/candle outputs can begin as retained-market processing starts.
- I did not run a live Kalshi ingest against the real API in this session, so the root-cause explanation is based on code audit, checkpoint state, and the newly added regression coverage rather than a fresh production reproduction.

#### Recommended Next Milestone
- K-05 - Signal Ensemble & Portfolio Construction
