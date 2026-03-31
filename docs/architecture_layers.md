# Architecture Layers

This document defines the intended subsystem boundaries for the trading platform.

## Research

Responsibilities:
- generate signals, candidate evaluations, EV estimates, and promotion inputs
- run replay and research workflows
- emit governed research artifacts and scorecards

Inputs:
- normalized market data and features
- configuration and strategy definitions
- historical evaluation artifacts

Outputs:
- candidate/decision artifacts
- strategy scorecards and promotion inputs
- replay summaries and research diagnostics

Must not depend on:
- paper-state persistence details
- broker-specific execution adapters
- dashboard UI code

## Portfolio

Responsibilities:
- normalize strategy outputs into shared portfolio inputs
- resolve conflicts across sleeves
- apply exposure, concentration, and allocation constraints
- produce final target weights and portfolio rationale

Inputs:
- research-layer strategy outputs
- portfolio construction config
- universe metadata and grouping context

Outputs:
- scheduled and effective target weights
- allocation diagnostics and rationale artifacts
- strategy ownership/provenance for downstream attribution

Must not depend on:
- broker/live execution state
- paper trade accounting internals
- dashboard presentation code

## Execution

Responsibilities:
- represent order lifecycle state
- translate targets into executable orders
- simulate or reconcile execution outcomes
- apply execution-oriented policies without changing upstream intent semantics

Inputs:
- target weights or order intents
- latest prices and execution settings
- realized fills or broker order state

Outputs:
- executable orders
- lifecycle records
- reconciliation payloads
- execution diagnostics

Must not depend on:
- research signal generation
- portfolio selection heuristics
- persistence format details beyond typed contracts

## State

Responsibilities:
- persist and restore paper/live-adjacent runtime state
- handle partial, missing, and corrupt state safely
- provide stable typed contracts for stateful workflows

Inputs:
- typed portfolio, order, and attribution state
- storage paths and schema versions

Outputs:
- restored runtime state objects
- deterministic persisted snapshots

Must not depend on:
- dashboard consumers
- research candidate generation
- portfolio construction policy decisions

## Reporting

Responsibilities:
- convert typed runtime artifacts into warehouse-friendly and dashboard-friendly payloads
- preserve deterministic, machine-readable semantics
- summarize rather than control trading behavior

Inputs:
- decision contracts
- attribution artifacts
- lifecycle and reconciliation outputs
- monitoring summaries

Outputs:
- KPI payloads
- trade explorer payloads
- strategy health payloads
- CSV/JSON reporting artifacts

Must not depend on:
- signal generation internals
- target selection logic
- broker submission side effects

## Boundary Rules

- Research may feed portfolio, but portfolio must not call back into research scoring logic.
- Portfolio may feed execution, but execution must not change portfolio policy semantics.
- Execution and state may exchange typed contracts, but state persistence must remain storage-focused.
- Reporting may read any layer's typed artifacts, but upstream layers must not depend on reporting payload builders.

## Current Boundary Issue Fixed In F-01

Previous issue:
- `paper.service` directly mutated `target_construction_service` module globals to inject runtime dependencies such as loaders, registries, and execution-policy helpers.

Why that was a boundary problem:
- it made the paper layer reach inside the target-construction layer's implementation details
- it obscured which dependencies were part of the supported interface
- it increased the risk of accidental import-order coupling

Fix:
- `target_construction_service.configure_runtime_dependencies()` now provides an explicit configuration boundary
- `paper.service` configures the target-construction layer through `TargetConstructionRuntimeDependencies` instead of mutating module globals ad hoc

This keeps behavior unchanged while making the intended dependency direction explicit:
- paper configures target-construction runtime inputs
- target-construction owns how those dependencies are consumed
