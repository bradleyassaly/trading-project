# AGENTS.md

## Purpose

This file provides repository-specific instructions for coding agents working in this trading platform.

Agents must follow these rules when implementing tasks.

---

## Core Principles

1. Preserve system correctness over convenience.
2. Prefer explicit domain models over loosely structured dictionaries.
3. Do not silently change trading behavior.
4. Keep replay, paper, and future live logic aligned wherever practical.
5. Produce machine-readable outputs suitable for dashboarding and audit.
6. Update docs whenever behavior or interfaces change.
7. Do not broaden scope beyond the assigned milestone unless explicitly instructed.

---

## What This Repository Is Building Toward

This platform is intended to become a governed, explainable, multi-strategy research, paper, and live trading system.

Important implications:
- explainability is a product requirement, not a nice-to-have
- promotion must be rules-driven
- paper/live parity matters
- auditability matters
- code changes must support future dashboard and operational workflows

---

## Required Working Style

When assigned a milestone, the agent must:

1. Read the relevant entries in:
   - `ROADMAP.md`
   - `MILESTONES.md`
   - this file

2. Stay within the milestone scope.

3. Before finishing, update:
   - `MILESTONES.md`
   - `DOCUMENTATION.md`

4. Include in `DOCUMENTATION.md`:
   - what changed
   - why it changed
   - files changed
   - tests run
   - exact verification commands
   - known limitations
   - suggested next milestone

5. Prefer small, reviewable changes over broad rewrites unless the milestone explicitly calls for refactoring.

---

## Non-Negotiable Constraints

### Trading behavior
- Do not silently alter trade entry, exit, sizing, ranking, promotion, or portfolio allocation semantics.
- If a milestone requires behavior changes, document them explicitly.

### Risk and governance
- Do not weaken checks, thresholds, or validation logic without clearly documenting why.
- Do not remove safeguards just to make tests pass.

### Tests
- Do not delete or weaken relevant tests without justification.
- Add or update tests for any new model, schema, or behavior change.

### Documentation
- Do not consider work complete unless docs are updated.

### Backward compatibility
- Prefer backward-compatible changes when practical.
- If a breaking change is necessary, document it clearly in `DOCUMENTATION.md`.

---

## Preferred Design Style

### Domain modeling
Prefer first-class typed models/contracts for important objects such as:
- TradeDecision
- StrategyScorecard
- PromotionDecision
- PortfolioTarget
- RiskCheckResult
- OrderIntent
- ExecutionReport
- ReconciliationResult
- KPI payloads

Avoid spreading important logic across ad hoc dictionaries with inconsistent keys.

### Separation of concerns
Try to preserve or improve boundaries between:
- data
- features
- signals
- research/evaluation
- promotion/governance
- portfolio construction
- risk
- execution
- reporting/dashboard payloads

### Configuration
Use config for:
- thresholds
- environment settings
- limits
- tunable parameters

Do not bury core business semantics behind excessive config branching.

### Artifacts
Outputs should be:
- deterministic where practical
- machine-readable
- version-friendly
- suitable for dashboard ingestion and debugging

---

## Testing Expectations

At minimum, milestone work should include appropriate tests from the following categories when relevant:

- unit tests for models and helper logic
- serialization/contract tests for schema objects
- integration tests for CLI or workflow wiring
- regression tests when behavior changes
- failure-mode tests when state/reconciliation logic is added

When adding a new contract or schema:
- test construction
- test serialization
- test edge cases
- test backward/optional field behavior when relevant

---

## Documentation Expectations

Each completed milestone must update `DOCUMENTATION.md` with:

- milestone ID and title
- summary of implementation
- files changed
- tests run
- verification commands
- design notes
- follow-up opportunities
- known issues or limitations

Update `MILESTONES.md` status accordingly.

---

## Performance Guidance

Prefer:
- reusing existing computed artifacts where correct
- avoiding repeated expensive recomputation
- small targeted optimizations backed by profiling or clear reasoning
- readability and correctness first, performance second, unless the milestone is explicitly performance-oriented

Do not introduce premature complexity without evidence.

---

## Safety Guidance

This is trading infrastructure.

Be conservative when touching:
- expected value calculations
- portfolio allocation
- promotion logic
- execution logic
- state persistence
- reconciliation
- risk controls

When in doubt:
- document assumptions
- keep changes minimal
- leave clear notes for review

---

## Completion Checklist

Before marking a milestone complete, ensure:

- code changes are implemented
- tests are added/updated and passing
- docs are updated
- `MILESTONES.md` status is updated
- `DOCUMENTATION.md` entry is added
- verification commands are included
- scope stayed within the assigned milestone

If any of the above is missing, the milestone is not complete.