# Multi-Agent Milestone Execution Spec

## Goal

Allow Codex CLI to complete multiple low-to-medium-risk milestones autonomously using gated self-review.

---

## Roles

### Orchestrator
Responsibilities:
- Read `ROADMAP.md`, `MILESTONES.md`, `AGENTS.md`, `IMPLEMENT.md`, and `DOCUMENTATION.md`
- Select the next eligible milestone
- Delegate to Builder
- Request Reviewer, Governance, and Verifier passes
- Decide whether to continue or stop
- Update milestone status if needed

### Builder
Responsibilities:
- Implement only the assigned milestone scope
- Add or update tests
- Run milestone verification commands
- Update `DOCUMENTATION.md`
- Set milestone to `REVIEW_NEEDED` when implementation is complete

### Reviewer
Responsibilities:
- Compare completed work against milestone acceptance criteria
- Check diff scope
- Check code quality and maintainability
- Confirm docs/tests/status updates are present

### Governance
Responsibilities:
- Enforce `AGENTS.md`
- Detect silent changes to EV semantics, promotion logic, allocation logic, risk controls, and execution behavior
- Detect missing auditability/explainability fields where required

### Verifier
Responsibilities:
- Run required verification commands
- Summarize failures cleanly
- Confirm whether the milestone passes the required checks

---

## Milestone Status Flow

- NOT_STARTED
- IN_PROGRESS
- REVIEW_NEEDED
- BLOCKED
- DONE

Transition rules:
- Orchestrator selects milestone -> IN_PROGRESS
- Builder completes implementation -> REVIEW_NEEDED
- Reviewer + Governance + Verifier all pass -> DONE
- Any serious issue -> BLOCKED
- Fixable issue -> back to Builder, remain REVIEW_NEEDED or IN_PROGRESS

---

## Automatic Continuation Rule

The Orchestrator may continue to the next milestone only if:

1. Builder reports milestone implementation complete
2. Reviewer reports acceptance criteria satisfied
3. Governance reports no prohibited behavior drift
4. Verifier reports required checks passed
5. `DOCUMENTATION.md` and `MILESTONES.md` are updated
6. No hard stop rule is triggered

---

## Hard Stop Rules

Require human review before continuing if any milestone touches:

- live broker or order-routing behavior
- risk-limit semantics
- promotion threshold semantics
- expected value definition semantics
- portfolio allocation semantics
- reconciliation source-of-truth logic
- operational database migrations
- secrets, credentials, or production environment configuration

---

## Required Output Per Milestone

Builder must provide:
- summary of changes
- files changed
- tests run
- verification commands
- known limitations
- recommended next milestone

Reviewer must provide:
- pass/fail on acceptance criteria
- scope-creep check
- code quality notes
- requested fixes if any

Governance must provide:
- pass/fail on repo rules
- explicit note on whether trading behavior changed
- required escalation if stop rule triggered

Verifier must provide:
- commands run
- pass/fail results
- concise failure summary if applicable