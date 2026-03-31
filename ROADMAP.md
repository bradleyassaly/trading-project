# Trading Platform Roadmap

## Purpose

This repository is evolving from a strong research and replay-oriented trading platform into a production-grade, explainable, multi-strategy trading operating system that can:

- research and evaluate new strategies continuously
- promote or reject strategies through objective governance
- run paper and live trading through a shared decision pipeline
- explain and justify all trade and portfolio decisions
- monitor system health, execution quality, and strategy degradation
- support controlled autonomous development with human approval gates

---

## Current State

The system already includes meaningful infrastructure in the following areas:

- structured research workflows
- candidate generation and evaluation
- signal family / candidate variant support
- promotion workflow
- replay infrastructure
- early paper trading and multi-strategy portfolio work
- diagnostics around reliability, weighting, and signal quality
- config-driven workflows
- growing test coverage
- artifact-based reproducibility

The platform is currently best described as:

**proto-production research and paper-trading infrastructure**

It is not yet a fully autonomous or production-hardened live trading platform.

---

## Target End State

The desired end state is a governed, explainable, multi-strategy platform with the following properties:

### Research
- generates and evaluates multiple strategy candidates reproducibly
- measures expected value, uncertainty, calibration, and regime behavior
- supports walk-forward validation and comparable scorecards

### Governance
- promotes candidates only through explicit stage gates
- records machine-readable rejection and approval reasons
- distinguishes research, candidate, paper-ready, and live-ready states

### Portfolio
- arbitrates capital across multiple strategies
- handles overlap, concentration, correlation, and capacity
- explains sizing and allocation decisions at strategy and instrument levels

### Execution
- uses a unified decision pipeline across replay, paper, and live
- maintains persistent state and reconciliation against broker truth
- supports order lifecycle management, restart safety, and kill switches

### Explainability
- logs every trade, non-trade, veto, exit, and sizing decision
- powers modern dashboards with KPI summaries, visual drill-downs, and natural-language rationale
- exposes why each trade happened and whether it was justified

### Operations
- monitors data freshness, run health, execution quality, and drift
- provides incident logging, alerts, and operator-facing dashboards
- supports extended unattended paper trading before limited live deployment

### Controlled Autonomy
- allows agentic workflows to propose code changes, run tests, execute research, and summarize results
- requires human approval for merge, promotion, and live activation
- avoids uncontrolled self-modifying production behavior

---

## Design Principles

1. **One decision pipeline**
   - Replay, paper, and live should share the same core decision logic.
   - Environment, clock, and execution adapters may differ; strategy decisions should not.

2. **Governance before autonomy**
   - The system must reject weak or unclear strategies before it tries to scale autonomous operation.

3. **Explainability is first-class**
   - Every important action should be queryable, measurable, and explainable.

4. **Artifacts for reproducibility, database for operations**
   - Keep artifacts for experiment reproducibility.
   - Introduce relational/event-backed operational state as the system matures.

5. **Safety before live scale**
   - Risk checks, reconciliation, and observability must be in place before significant autonomy or live capital.

6. **Human-reviewed autonomous development**
   - Agents may propose and implement bounded changes, but human review remains mandatory for important trading behavior changes.

---

## Program Structure

The roadmap is organized into six programs.

### Program A — Core Decision Intelligence
Goal: make every trade and non-trade structured, explainable, and measurable.

Includes:
- trade decision schema
- EV decomposition
- uncertainty and calibration fields
- rationale / veto logging
- decision artifact standardization

### Program B — Promotion Governance
Goal: strategies advance only through machine-checkable gates.

Includes:
- strategy scorecards
- promotion gate engine
- rejection reasons
- paper-readiness and live-readiness checks
- governance audit trail

### Program C — Multi-Strategy Portfolio Intelligence
Goal: move from multiple strategies to a real portfolio.

Includes:
- normalized strategy forecasts
- capital arbitration
- overlap/conflict handling
- exposure and concentration controls
- allocation diagnostics

### Program D — Paper/Live Operational Engine
Goal: production-like paper trading and live readiness.

Includes:
- persistent state
- order lifecycle models
- reconciliation
- restart safety
- unified paper/live behavior

### Program E — Explainability, KPI, and Dashboard Layer
Goal: modern operator-facing observability and decision explanation.

Includes:
- KPI warehouse/schema
- trade explorer payloads
- decision explanation payloads
- strategy health views
- system health and incident views

### Program F — Codebase Hardening and Platform Architecture
Goal: keep the codebase scalable, testable, and clean.

Includes:
- stronger domain models
- clearer subsystem boundaries
- better tests
- performance and caching improvements
- operational storage architecture

---

## Phase Plan

## Phase 1 — Structured Decision Intelligence
Objective:
Introduce formal trade decision objects and standardized decision logging.

Success criteria:
- every candidate trade produces a structured decision record
- EV-related fields are standardized
- rationale, vetoes, and thresholds are recorded consistently
- replay outputs can be inspected at decision level

Programs:
- A
- partial F

---

## Phase 2 — Promotion Scorecards and Governance
Objective:
Turn promotion into a strict stage-gate system with machine-readable reasons.

Success criteria:
- every candidate receives a comparable scorecard
- promotion decisions are reproducible and explainable
- rejection reasons are structured and logged
- paper readiness is explicitly evaluated

Programs:
- B
- partial A

---

## Phase 3 — Multi-Strategy Portfolio Arbitration
Objective:
Build a portfolio layer that can allocate across multiple strategies coherently.

Success criteria:
- strategy outputs are normalized into a shared contract
- conflicts and overlap are handled explicitly
- exposure budgets and concentration rules exist
- final portfolio targets are explainable

Programs:
- C
- partial B

---

## Phase 4 — Production-Like Paper Trading
Objective:
Make paper trading operationally trustworthy and restart-safe.

Success criteria:
- persistent paper state exists
- order lifecycle and reconciliation exist
- paper runs can recover cleanly after interruption
- paper and replay share core decision behavior

Programs:
- D
- partial F

---

## Phase 5 — Dashboard, KPI Warehouse, and Explainability UX
Objective:
Expose strategy, trade, portfolio, and system decisions through a modern dashboard.

Success criteria:
- KPI schema and warehouse layer exist
- trade explorer supports drill-down by decision
- strategy health views exist
- operator can answer “what happened and why?” quickly

Programs:
- E
- partial A and D

---

## Phase 6 — Live Readiness and Controlled Autonomy
Objective:
Enable tightly controlled live deployment and bounded autonomous development.

Success criteria:
- live broker integration path is risk-gated
- kill switches and reconciliation are in place
- live readiness gates exist
- agentic development workflows are documented and bounded

Programs:
- D
- E
- F

---

## Operating Rules

- No milestone is considered complete without code, tests, docs, and verification commands.
- No material trading behavior change should be merged without explicit documentation.
- No autonomous coding agent should broaden scope silently.
- All important outputs should be machine-readable and dashboard-friendly.
- The system should optimize for auditability, not just speed of iteration.

---

## Definition of Done for the Platform

The platform is approaching the intended target state when it can:

- run repeatable research workflows
- produce comparable scorecards across strategies
- paper trade multiple strategies for extended periods unattended
- explain entries, exits, non-trades, sizing, and vetoes
- detect operational issues automatically
- maintain persistent trading state and reconciliation
- support small-cap live deployment with hard controls
- use agents to accelerate development without weakening governance