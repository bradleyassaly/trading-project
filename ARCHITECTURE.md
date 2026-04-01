# Architecture

This document explains the structural and conceptual design of the trading platform — why it is built the way it is, how the major subsystems relate to each other, and what principles guide decisions about where new code should go. It is intended as the starting point for any contributor or AI agent working on the codebase.

For operational instructions (CLI commands, config options, artifact paths), see `README.md`. For what is being built next, see `ROADMAP.md` and `MILESTONES.md`. For agent governance rules, see `AGENTS.md` and `MULTI_AGENT_SPEC.md`.

---

## Vision

The platform is designed to become a fully autonomous, explainable, multi-strategy trading operating system. At its target state it can:

- research and evaluate strategy candidates continuously and reproducibly
- promote or reject strategies through objective, machine-checkable governance gates
- allocate capital across multiple active strategies with explicit conflict and concentration handling
- execute trades through a unified decision pipeline shared across replay, paper, and live environments
- explain every trade, non-trade, sizing decision, and veto in structured, queryable form
- monitor its own health and escalate to a human when something requires attention
- support bounded autonomous development workflows where agents propose and implement changes but humans approve all trading behavior changes

The current state is best described as **proto-production research and paper-trading infrastructure**. The system has strong bones — reproducible research, artifact-first data contracts, config-driven workflows, a promotion gate system, and an explainability layer — but is not yet a fully autonomous or production-hardened live system. Every architectural decision should be evaluated against whether it moves the platform closer to that target state.

---

## Core Design Principles

These principles explain why the system is structured the way it is. When in doubt about where new code belongs or how to design a new feature, return to these.

**1. One decision pipeline across all environments**

Replay, paper trading, and live trading must share the same core strategy decision logic. Only the clock source, data adapter, and execution adapter differ by environment. This prevents the classic bug where a backtested strategy behaves differently in paper trading because it was re-implemented rather than reused.

**2. Artifact-first for reproducibility, database for operations**

Heavy research outputs — feature matrices, signal snapshots, walk-forward grids, charts — live in files (parquet, CSV, JSON, HTML). This makes experiments reproducible and inspectable without a running database. PostgreSQL is an optional control plane for normalized metadata, lineage, and cross-run queryability. The system must function without a database. Nothing important should be stored only in a database.

**3. Config-first workflows**

All workflows are driven by YAML configs with CLI flag overrides. Long CLI invocations are not the canonical interface. This makes automation, scheduling, and reproducibility straightforward.

**4. Governance before autonomy**

The promotion system is intentionally strict. A strategy must pass explicit stage gates — research quality, runtime computability, paper readiness, and live readiness — before it can touch real capital. The system should reject weak strategies automatically rather than relying on human discretion at every step.

**5. Explainability is first-class, not an afterthought**

Every trade, non-trade, candidate evaluation, universe filter result, and sizing decision is persisted as a structured artifact. The dashboard is built over these artifacts. Explainability cannot be bolted on after the fact — it must be embedded in the decision pipeline itself.

**6. Safety before autonomy**

Risk controls, kill switches, reconciliation against broker truth, and circuit breakers must be in place before significant autonomy or live capital. The autonomous loop should be designed to escalate to a human rather than to proceed when uncertain.

**7. Human-reviewed autonomous development**

Agents (Claude Code, Codex CLI, etc.) may propose and implement bounded changes. Human review is mandatory for any change that touches live broker behavior, risk limit semantics, promotion threshold semantics, expected value definitions, portfolio allocation logic, reconciliation source-of-truth logic, database migrations, or credentials.

---

## Repository Layout

```
trading-project/
├── src/trading_platform/      # All application source code
│   ├── cli/                   # CLI entry points and command groups
│   ├── data/                  # Data ingestion, features, fundamentals, universes
│   ├── research/              # Alpha research, signal families, walk-forward
│   ├── portfolio/             # Portfolio construction, allocation, constraints
│   ├── paper/                 # Paper trading engine and state management
│   ├── live/                  # Live trading adapters and dry-run previews
│   ├── dashboard/             # Dashboard server, routes, and read services
│   ├── orchestration/         # Pipeline orchestration, scheduling, ops
│   ├── integrations/          # Optional third-party library adapters
│   └── ops/                   # Health checks, monitoring, incident logging
├── configs/                   # YAML workflow configs (versioned, not secrets)
├── data/                      # Local market data artifacts (gitignored large files)
├── artifacts/                 # Research, paper, and live run outputs (gitignored)
├── tests/                     # Test suite mirroring src layout
├── migrations/                # Alembic database migrations
├── scripts/                   # Utility and diagnostic scripts
├── docs/                      # Extended documentation
├── AGENTS.md                  # Governance rules for agentic workflows
├── MULTI_AGENT_SPEC.md        # Multi-agent role definitions and stop rules
├── ROADMAP.md                 # Six-program roadmap and phase plan
├── MILESTONES.md              # Granular milestone tracking
├── IMPLEMENT.md               # Implementation guidance for agents
├── REVIEW_CHECKLIST.md        # Code review and acceptance criteria
└── DOCUMENTATION.md           # Living documentation updated by builders
```

---

## Subsystem Map

The platform is composed of seven major subsystems. Each has a clear responsibility boundary and communicates with others primarily through artifacts and typed config objects, not through direct function calls across subsystem boundaries where avoidable.

```
┌─────────────────────────────────────────────────────────────┐
│                        CLI Layer                            │
│   trading-cli [data|research|portfolio|paper|live|ops|dash] │
└────────────────────────────┬────────────────────────────────┘
                             │ config objects
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
┌──────────────┐   ┌──────────────────┐   ┌──────────────────┐
│  Data Layer  │   │ Research Layer   │   │  Portfolio Layer  │
│              │   │                  │   │                   │
│ ingest       │──▶│ signal families  │──▶│ arbitration       │
│ features     │   │ alpha research   │   │ weighting modes   │
│ fundamentals │   │ walk-forward     │   │ exposure controls │
│ universes    │   │ promotion gates  │   │ constraint engine │
└──────────────┘   └──────────────────┘   └──────────────────┘
        │                    │                    │
        │              artifacts/            artifacts/
        │            alpha_research/      strategy_portfolio/
        ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│                   Decision Pipeline                         │
│   (shared core logic across replay, paper, and live)        │
│                                                             │
│   universe build → screening → scoring → ranking →          │
│   candidate eval → selection → sizing → order intent        │
└──────────────────────────┬──────────────────────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌─────────────┐ ┌──────────────┐ ┌──────────────┐
    │   Replay    │ │    Paper     │ │     Live     │
    │   Engine    │ │    Engine    │ │    Engine    │
    └─────────────┘ └──────────────┘ └──────────────┘
           │               │               │
           └───────────────┴───────────────┘
                           │
                    artifacts/paper/
                    artifacts/live/
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Dashboard + Ops Layer                     │
│                                                             │
│   artifact readers → hybrid service → HTML workspace        │
│   DB query services (optional) → JSON APIs                  │
│   ops monitoring → health checks → incident log             │
└─────────────────────────────────────────────────────────────┘
```

---

## The Research Pipeline in Detail

The research pipeline is the most complex subsystem. It is responsible for generating, evaluating, and governing strategy candidates before they ever touch capital.

### Stage 1 — Research Input Refresh

`trading-cli data refresh-research-inputs`

Produces the canonical research inputs that all downstream stages consume:

- `data/features/<SYMBOL>.parquet` — OHLCV and derived technical features, one file per symbol
- `data/metadata/sub_universe_snapshot.csv` — sub-universe membership at the as-of date
- `data/metadata/universe_enrichment.csv` — taxonomy, benchmark context, and enrichment fields
- `data/fundamentals/daily_fundamental_features.parquet` — point-in-time daily fundamental features (optional)

**Why artifact-first here:** Research must be reproducible. If features were recomputed on demand during research, two runs on different days could produce different results from the same config. Feature files are the snapshot that pins research to a specific state of the world.

**Point-in-time safety:** Fundamental features use `available_date` (derived from SEC filing acceptance timestamps) to determine when each data point becomes visible. Forward-fill only begins at `available_date`. No future filing can backfill prior dates.

### Stage 2 — Alpha Research

`trading-cli research alpha --config configs/alpha_research.yaml`

Evaluates one or more signal families across the candidate universe and produces a ranked leaderboard of strategy candidates. Key concepts:

- **Signal families** are named groups of related signals (e.g., `momentum`, `breakout_continuation`, `fundamental_value`). Each family can emit multiple parameterized variants.
- **Candidate variants** have stable IDs combining family, variant, lookback, and horizon. This makes results comparable across runs.
- **Context slicing** evaluates each candidate's performance separately by regime, sub-universe, and benchmark context. This is the foundation for conditional strategies.
- **Runtime computability checks** verify that a promoted candidate can actually produce non-null scores on the current feature set — not just that it performed well historically.

Output artifacts under `artifacts/alpha_research/<run_id>/`:
- `leaderboard.csv` — ranked candidates with IC, Sharpe, and other metrics
- `promoted_signals.csv` — candidates that pass the computability filter
- `signal_performance_by_regime.csv`, `_by_sub_universe.csv`, `_by_benchmark_context.csv` — conditional performance slices
- `research_registry/` — run-local registry bundle consumed by the promotion stage

### Stage 3 — Promotion

`trading-cli research promote --policy-config configs/promotion_experiment.yaml`

Promotion is a strict gate, not a ranking exercise. A candidate either passes or it does not. The promotion policy controls:

- minimum IC, Sharpe, and sample size thresholds
- whether conditional variants (regime-specific, sub-universe-specific) are eligible
- diversity constraints across signal families
- bootstrap safety for first-time promotions

**Why strict gates matter:** The system is designed to reject weak strategies automatically. If promotion is too permissive, the portfolio layer inherits noise and the signal degrades. The governance principle is that the system should know what it does not know.

Promotion writes to `artifacts/promoted_strategies/` and, when DB tracking is enabled, to the `promoted_strategies` and `signal_candidates` control-plane tables.

### Stage 4 — Strategy Portfolio Build

`trading-cli strategy-portfolio build`

Takes the promoted strategy set and constructs a portfolio with explicit weighting, concentration controls, and conditional activation logic. Conditional strategies are first-class: they carry `activation_conditions` and are evaluated at runtime to determine `is_active`.

Portfolio build writes `strategy_portfolio.json`, which is the contract consumed by paper and live execution.

---

## The Decision Pipeline in Detail

The decision pipeline is the shared core that produces trade decisions from strategy inputs. It runs identically in replay, paper, and live contexts. Only the adapters at the edges differ.

### Stages within each run

```
1. Universe Build
   Load base universe symbols from config or preset.

2. Point-in-Time Membership Resolution
   Resolve which symbols were actually in the universe at the as-of date.
   Status: confirmed | inferred | static_fallback | unavailable

3. Sub-Universe Screening
   Apply sequential filters: min_price, min_avg_dollar_volume, symbol_include/exclude_list, etc.
   Each filter records pass/fail per symbol in universe_filter_results artifacts.

4. Feature Loading
   Load feature parquet files for eligible symbols.
   For paper/live, optionally merge latest bars from Alpaca.

5. Signal Scoring
   Apply promoted signal(s) to produce per-symbol scores.
   Ensemble mode blends multiple candidates.

6. Ranking and Candidate Evaluation
   Rank symbols by score. Evaluate each candidate for inclusion.
   Record rank, score, selection status, and rejection reason in candidate_snapshot artifacts.

7. Portfolio Selection and Sizing
   Select top-N or weight-based set of symbols.
   Apply sizing constraints: max weight, min weight, exposure budgets.
   Record target weights in trade_decisions artifacts.

8. Order Intent Generation
   Compare target weights to current holdings.
   Generate rebalance orders. Apply execution constraints.
   Record executable and rejected orders in execution_decisions artifacts.

9. Fill Simulation (paper) or Submission (live)
   Apply slippage model and cost model to fills.
   Update paper state: cash, positions, realized PnL, unrealized PnL.
```

Every stage persists its outputs as structured artifacts. This is what makes the dashboard's explainability possible — each layer of the pipeline is independently inspectable.

---

## Artifact Contracts

Artifacts are the primary data contract between subsystems. A subsystem should never depend on another subsystem's internal Python objects — only on the artifacts that subsystem writes.

### Key artifact locations

| Subsystem | Primary artifacts |
|-----------|------------------|
| Data | `data/features/<SYMBOL>.parquet`, `data/metadata/*.csv`, `data/fundamentals/*.parquet` |
| Research | `artifacts/alpha_research/<run_id>/leaderboard.csv`, `promoted_signals.csv`, `research_registry/` |
| Promotion | `artifacts/promoted_strategies/promoted_strategies.json`, `promotion_candidates.csv` |
| Portfolio | `artifacts/strategy_portfolio/strategy_portfolio.json`, `activated_strategy_portfolio.json` |
| Paper | `artifacts/paper/paper_fills.csv`, `paper_positions.csv`, `paper_orders.csv`, `portfolio_equity_curve.csv` |
| Decision journal | `candidate_snapshot.csv`, `trade_decisions.csv`, `execution_decisions.csv`, `exit_decisions.csv`, `trade_lifecycle.csv` |
| Universe provenance | `universe_membership.csv`, `universe_filter_results.csv`, `universe_enrichment.csv`, `point_in_time_membership.csv` |
| PnL attribution | `strategy_pnl_attribution.csv`, `symbol_pnl_attribution.csv`, `trade_pnl_attribution.csv` |

### Rules for artifact design

- Artifacts must be human-readable (CSV or JSON preferred for structured data, parquet for large feature matrices)
- Every artifact must be writable without a database connection
- Artifact schemas must be backward-compatible — adding fields is allowed, removing fields requires a migration
- Heavy payloads (feature matrices, signal snapshots) stay in parquet; metadata and decisions stay in CSV/JSON
- Missing data is represented as missing fields or null values, never as synthetic defaults

---

## Storage Architecture

The platform uses a two-tier storage model.

**Tier 1 — File artifacts (always on)**

All research, decision, and execution outputs are written to the local filesystem as files. The system must function fully in file-only mode. This is the source of truth for reproducibility.

**Tier 2 — PostgreSQL control plane (optional)**

When `TRADING_PLATFORM_ENABLE_DATABASE_METADATA=1`, the system additionally writes normalized metadata to PostgreSQL via SQLAlchemy 2.x and Alembic. The database stores:

- research runs and portfolio runs (lineage)
- alpha research candidate definitions and metrics
- promoted strategies and promotion decisions
- artifact registry and run-artifact links
- portfolio decisions, signal contributions, and position snapshots
- orders, order events, and fills

The database read path is exposed through thin query services (`RunQueryService`, `DecisionQueryService`, `ExecutionQueryService`, `ArtifactQueryService`, `OpsQueryService`). Dashboard pages prefer the database when available and fall back to artifact readers when not.

**What never goes in the database:** large feature matrices, signal snapshots, walk-forward grids, charts, HTML reports, and diagnostics. These stay in files.

---

## The Dashboard Layer

The dashboard is an internal trading terminal, not a public-facing application. It is intentionally lightweight:

- server-rendered HTML with vanilla JS for interactive elements
- read-only by design (no trading actions from the browser in the current architecture)
- hybrid read path: prefers PostgreSQL for normalized history, falls back to artifact readers
- no SPA framework — pages are rendered server-side and enriched with fetch() calls

### What each major page is for

| Page | Purpose |
|------|---------|
| `/` | System command center: equity snapshot, recent trades, open positions, strategy pulse, alerts |
| `/trades` | Trade blotter with filter/pagination and drill-down links |
| `/trades/<id>` | Trade intelligence view: why this trade happened, signal evidence, decision provenance, execution review |
| `/strategies` | Strategy registry and promotion history |
| `/strategies/<id>` | Strategy-linked trade history and run comparisons |
| `/portfolio` | Open positions, exposure, contributors/detractors, allocation context |
| `/execution` | Latest fills, symbol coverage, handoff diagnostics |
| `/runs` | Normalized run history across research, promotion, portfolio, and paper stages |
| `/ops` | Run health, live risk checks, execution diagnostics, orchestration history |
| `/symbols/<SYMBOL>` | Symbol-centric trade and provenance inspection |

### Dashboard explainability model

Trade explainability is assembled opportunistically from persisted artifacts. The trade detail page layers:

1. Market data and indicators from feature parquet files
2. Signal labels and scores from research signal CSVs
3. Ranking, target-weight, and selection context from `candidate_snapshot` artifacts
4. Trade state from the paper trade ledger
5. Order and fill execution review from paper order and fill CSVs
6. Portfolio context from latest position artifacts

If any layer is missing, the dashboard renders the section as explicitly absent rather than synthesizing data.

---

## Optional Integrations

All third-party library integrations live behind thin adapters under `src/trading_platform/integrations/`. The platform's canonical workflows do not depend on them being installed.

| Integration | Purpose | Extra |
|-------------|---------|-------|
| `FinanceDatabase` | Security classification bootstrapping | `[classification]` |
| `Alphalens` | Factor IC diagnostics | `[research_diagnostics]` |
| `PyPortfolioOpt` | Allocator research adapters | `[portfolio_optimizers]` |
| `QuantStats` | Performance reporting | `[validation]` |
| `vectorbt` | Backtester benchmark validation | `[validation]` |
| `Alpaca` | Latest-bar data source for paper execution | configured via env vars |

An integration may fail gracefully if its package is not installed. The canonical path must remain functional without it.

---

## Multi-Agent Development Governance

The platform supports bounded agentic development workflows. The `MULTI_AGENT_SPEC.md` defines four roles:

- **Orchestrator** — selects the next eligible milestone and delegates to Builder
- **Builder** — implements the assigned scope, adds tests, updates docs, runs verification commands
- **Reviewer** — checks acceptance criteria, scope creep, and code quality
- **Governance** — enforces `AGENTS.md` rules, detects silent behavior drift in trading logic
- **Verifier** — runs required verification commands and summarizes results

### Hard stop rules — always require human review

Any change touching the following areas must pause for human review before continuing:

- live broker or order-routing behavior
- risk limit semantics
- promotion threshold semantics
- expected value definition semantics
- portfolio allocation semantics
- reconciliation source-of-truth logic
- operational database migrations
- secrets, credentials, or production environment configuration

### What agents may do without human review

- implement new signal families or candidate variants
- add new CLI commands that do not touch execution or live trading
- add tests, diagnostics, and monitoring artifacts
- extend the dashboard with new read-only pages or payloads
- add new data adapters for new market types (e.g., prediction markets) in isolated modules
- refactor internals within a subsystem without changing its artifact contracts

---

## Expanding to New Market Types

The platform is designed to support multiple market types beyond the initial equity universe. The architecture for adding a new market type (e.g., Kalshi prediction markets, crypto) is:

1. Create a new data module under `src/trading_platform/data/<market_type>/` following the same config-driven, artifact-first pattern as the equity data module
2. Normalize the new market's native data representation into the platform's canonical feature schema — for prediction markets, treat the yes-price (0–100) as the price series
3. Add new signal families under `src/trading_platform/research/signals/` specific to the market type's dynamics
4. Wire the new market type into the CLI under the `data` command group
5. Do not modify any existing equity pipeline code — the new market type lives in parallel until the common abstractions are validated

The canonical feature schema must remain the lingua franca between data, research, and portfolio subsystems. New market types adapt to the schema; the schema does not adapt to each new market type.

---

## What This Document Does Not Cover

- CLI usage and command reference → `README.md`
- Individual milestone scope and acceptance criteria → `MILESTONES.md`
- Agent governance rules and prohibited behaviors → `AGENTS.md`
- Implementation guidance for specific features → `IMPLEMENT.md`
- Operational runbooks → `DOCUMENTATION.md`

---

*Last updated: March 2026. This document should be updated whenever a new subsystem is added, a major design decision is made, or the target architecture changes. Builders are expected to update it as part of milestone completion.*