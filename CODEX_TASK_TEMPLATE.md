# Codex Task Template

Use this template when assigning a milestone to Codex.

---

## Task

Implement milestone: **[MILESTONE_ID] — [MILESTONE_TITLE]**

Before making changes, read:
- `ROADMAP.md`
- `MILESTONES.md`
- `AGENTS.md`

Work only within the scope of this milestone unless a very small adjacent fix is required for correctness. Do not broaden scope silently.

---

## Objective

[Describe the milestone objective in 2–5 sentences.]

---

## In Scope

- [item]
- [item]
- [item]

---

## Out of Scope

- [item]
- [item]
- [item]

---

## Architectural Constraints

- Preserve or improve subsystem boundaries.
- Do not silently change trading behavior beyond the milestone’s stated purpose.
- Prefer explicit typed/domain models over loose dictionaries.
- Keep outputs machine-readable and suitable for future dashboard/reporting use.
- Keep replay/paper/live alignment in mind where relevant.

---

## Expected Deliverables

- code changes implementing the milestone
- tests for the added/changed behavior
- documentation updates
- `MILESTONES.md` status update
- `DOCUMENTATION.md` milestone entry

---

## Acceptance Criteria

- [criterion]
- [criterion]
- [criterion]

---

## Suggested Files / Areas to Inspect

- [path]
- [path]
- [path]

---

## Testing Requirements

At minimum, run and report:
- [test command]
- [test command]

If additional relevant tests are needed, run them too.

---

## Documentation Requirements

Update `DOCUMENTATION.md` with:
- milestone ID and title
- summary of what changed
- files changed
- tests run
- exact verification commands
- known issues / limitations
- suggested next milestone

Update `MILESTONES.md`:
- set status appropriately
- add brief implementation note if useful

---

## Output Format

When finished, provide:

1. Summary of what changed
2. Files changed
3. Tests run and results
4. Verification commands
5. Known issues / limitations
6. Recommended next milestone

Do not mark the milestone complete unless code, tests, and documentation are all updated.