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
- I-03 - Introduce broker/exchange abstraction layer
