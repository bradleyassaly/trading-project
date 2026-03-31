# REVIEW_CHECKLIST.md

## Reviewer Checklist

For every milestone, verify:

### Scope
- Was only the assigned milestone implemented?
- Was adjacent scope added unnecessarily?
- Were unrelated files changed without reason?

### Acceptance Criteria
- Does the implementation satisfy each acceptance criterion in `MILESTONES.md`?
- Are required outputs or fields present?
- Are edge cases handled where expected?

### Code Quality
- Is the design consistent with existing architecture?
- Did the change improve or preserve subsystem boundaries?
- Are important models/contracts explicit and typed where appropriate?

### Tests
- Were tests added or updated?
- Do the tests actually cover the changed behavior?
- Were relevant existing tests run?

### Documentation
- Was `DOCUMENTATION.md` updated?
- Was `MILESTONES.md` updated correctly?
- Are verification commands listed?

### Trading-System Safety
- Did the change alter EV semantics?
- Did the change alter promotion semantics?
- Did the change alter allocation semantics?
- Did the change alter risk or execution semantics?
- If yes, should a human review be required?

### Final Review Result
- PASS
- PASS WITH NOTES
- FAIL