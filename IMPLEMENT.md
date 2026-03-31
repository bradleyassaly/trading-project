# IMPLEMENT.md

## Purpose

This file is the execution runbook for Codex milestone work.

Codex must:
- follow milestone scope strictly
- keep diffs scoped
- validate after each milestone
- update documentation continuously
- stop when hard-stop rules are triggered

---

## Source of Truth

Read these before beginning work:
1. `ROADMAP.md`
2. `MILESTONES.md`
3. `AGENTS.md`
4. `REVIEW_CHECKLIST.md`
5. `DOCUMENTATION.md`

Use `MILESTONES.md` as the source of truth for what is next.

---

## Execution Rules

1. Work one milestone at a time unless explicitly instructed to chain multiple milestones.
2. Do not broaden scope silently.
3. Prefer explicit domain models over ad hoc dictionaries.
4. Do not silently change trading behavior.
5. Add or update tests whenever behavior or contracts change.
6. Update docs before declaring a milestone complete.

---

## Required Milestone Flow

For each milestone:

1. Read milestone definition
2. Set status to `IN_PROGRESS`
3. Implement the milestone
4. Add/update tests
5. Run verification commands
6. Fix any failures that are in scope
7. Update `DOCUMENTATION.md`
8. Set status to `REVIEW_NEEDED`
9. Request Reviewer, Governance, and Verifier passes
10. Mark `DONE` only if all passes succeed and no stop rule is triggered

---

## Multi-Agent Delegation Pattern

Use the following subagent sequence:

1. Builder implements
2. Reviewer audits against acceptance criteria
3. Governance checks repo and trading-system rules
4. Verifier runs required commands
5. Orchestrator decides continue or stop

---

## Documentation Requirements

Every milestone entry in `DOCUMENTATION.md` must include:
- milestone ID and title
- summary
- why it was done
- files changed
- tests run
- verification commands
- design notes
- known issues
- recommended next milestone

---

## Stop Immediately If

- a hard-stop rule is triggered
- required tests cannot be made to pass within scope
- milestone requirements are ambiguous in a way that could change system semantics
- a change would require weakening safeguards or tests

In those cases:
- set milestone status to `BLOCKED` or leave `REVIEW_NEEDED`
- write a clear handoff note in `DOCUMENTATION.md`