# Test Strategy By Layer

This note defines the preferred testing shape for the repository's major layers. The goal is not to force every change through the same test depth, but to make risk-based coverage expectations explicit.

## Research

### Responsibilities
The research layer builds datasets, features, signals, evaluations, and promotion inputs.

### Preferred tests
- Unit tests for feature transforms, signal logic, leaderboard calculations, and typed research contracts.
- Integration tests for research workflows that read artifacts and write summarized outputs.
- Regression tests for promotion-facing fields and evaluation semantics when schemas or filtering logic change.

### Avoid
- Broker or paper-state dependencies in research-layer unit tests.
- Full end-to-end orchestration tests for small research helper changes when focused workflow tests are sufficient.

## Portfolio

### Responsibilities
The portfolio layer normalizes strategy outputs, resolves conflicts, applies exposure rules, and produces target weights plus rationale.

### Preferred tests
- Unit tests for portfolio contracts, constraint helpers, and conflict-resolution logic.
- Integration tests for multi-strategy allocation outputs and artifact summaries.
- Regression tests for concentration caps, turnover controls, and additive rationale payloads.

### Avoid
- Coupling portfolio tests to execution fills or live-preview integrations unless the change explicitly crosses that boundary.

## Execution

### Responsibilities
The execution layer models order intent and lifecycle state, and compares intended versus realized state.

### Preferred tests
- Unit tests for order lifecycle contracts, serialization, and reconciliation mismatch taxonomies.
- Failure-mode tests for missing, partial, or inconsistent execution state.
- Narrow integration tests for paper execution adapters and artifact emission.

### Avoid
- Introducing external broker dependencies into contract-level tests.

## State

### Responsibilities
The state layer persists paper-run and future operational state safely across runs.

### Preferred tests
- Unit tests for state models and round-trip serialization.
- Failure-mode tests for missing, corrupt, and partial persisted state.
- Narrow integration tests for read/write stores with realistic snapshots.

### Avoid
- Reaching through persistence tests into unrelated strategy or portfolio policy.

## Reporting

### Responsibilities
The reporting layer converts structured runtime outputs into dashboard- and warehouse-friendly payloads.

### Preferred tests
- Unit tests for payload contracts and deterministic serialization.
- Integration tests for artifact writers and additive payload assembly from paper/replay results.
- Regression tests for row-count summaries, status derivation, and inspectable rationale fields.

### Avoid
- UI or web-server coverage in payload contract tests unless the schema itself changes.

## Orchestration

### Responsibilities
The orchestration layer sequences stage execution and preserves machine-readable run summaries.

### Preferred tests
- Unit tests for stage ordering, failure propagation, and placeholder/stub behavior.
- Integration tests for pipeline summaries and stage output wiring.

### Current under-covered area addressed in F-02
The orchestration research stage can intentionally emit a placeholder artifact for strategies that are not yet wired to a direct service-layer runner. That placeholder path is important because it is a documented non-error execution mode, so F-02 adds direct tests for it.

## Test Selection Guidance

- Contract-only changes should usually run the smallest unit and serialization set that exercises the touched models.
- Workflow changes should add one layer of adjacent integration coverage, but not broaden into repo-wide sweeps unless the change crosses major subsystem boundaries.
- Behavior-sensitive changes in promotion, allocation, execution, or persistence should include a focused regression test that demonstrates preserved semantics.
