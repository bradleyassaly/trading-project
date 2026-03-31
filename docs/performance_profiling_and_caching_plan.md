# Performance Profiling And Caching Plan

This note captures one immediate low-risk optimization target and a broader profiling plan for later milestones. The emphasis is on measuring repeated work before making larger structural changes.

## Immediate Target

### Path
The orchestration pipeline repeatedly resolves the same strategy execution handoff and reloads the same execution config across `portfolio_allocation`, `paper_trading`, and `live_dry_run` within a single run.

### Why it matters
- These stages can run back-to-back against the same generated multi-strategy config.
- Re-resolving handoff and re-reading config adds repeated file I/O and repeated config construction.
- The work is semantically stable within one pipeline run, so run-scoped caching is safe.

### F-03 implementation
F-03 adds a context-scoped cache in `orchestration.service` for:
- strategy execution handoff resolution
- execution config loading

The cache is:
- in-memory only
- scoped to a single `run_orchestration_pipeline()` invocation
- keyed by the current config path and stage-relevant toggles

This preserves behavior while removing repeated same-run setup work.

## Broader Profiling Focus

### Candidate path
Research and replay flows that repeatedly read feature and lifecycle artifacts from disk across many symbols or dates.

### Examples already visible in the repo
- EV regression / reliability workflows repeatedly reading `trade_candidate_dataset.csv` and lifecycle artifacts by day
- reporting and evaluation helpers repeatedly reading artifact CSVs across run directories
- multi-stage orchestration flows that rehydrate the same config inputs for adjacent stages

### Recommended profiling sequence
1. Add narrow timing capture around high-fanout loops, especially per-symbol and per-day artifact reads.
2. Measure:
   - wall-clock duration
   - file-read counts
   - symbol/day cardinality
   - cache hit rate where caches already exist
3. Optimize only the highest repeated-cost paths that are semantically read-only.

## Safe Optimization Principles

- Prefer run-scoped memoization over persistent caches unless invalidation semantics are already well-defined.
- Cache only deterministic read-side helpers.
- Never cache values whose correctness depends on changing market state unless the scope is tightly bounded to one run.
- Do not mix performance changes with trading-policy changes.

## Follow-up Opportunities

- Add explicit timing summaries to orchestration run outputs for stage setup versus core stage execution.
- Add cached readers for repeated artifact loads in replay and EV analysis paths where the same files are read multiple times in one command.
- Profile candidate-frame loading in `trade_ev_reliability` and related research workflows before introducing shared file-frame caches there.
