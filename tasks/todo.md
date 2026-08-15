# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Remediate all known Phase 3 M0 exact-tree data-integrity blockers with strict RED→GREEN TDD and commit one clean verified candidate.

## Success Criteria
- One warehouse-scoped, nonblocking, fail-closed lock covers the complete refresh transaction before discovery or mutation and resists symlink/hardlink/path alias tricks.
- CBOE canonical Parquet publication uses a durable same-directory temporary file, atomic replacement, cleanup, and predecessor preservation across write/fsync/replace faults.
- Every discovered identity receives a deterministic terminal run status (`succeeded`, `failed`, or `not_attempted`) in a unique create-only durable artifact on success and controlled failure, without leaking child output or altering predecessor publication.
- Source attestation uses a trusted absolute Git executable with a sanitized environment, proves commit/tree/blob identity in a real repository, and materializes the same reviewed commit/tree bytes.
- Focused tests, full 100% coverage, RuntimeWarning-as-error, secret scan, and diff checks pass without real refreshes, broker connections, or production warehouse writes.
- Only intended source/test/plan changes are committed; no push or merge occurs.

## Dependency Graph
- T1 -> T2
- T1 -> T3
- T1 -> T4
- T1 -> T5
- T2 -> T6
- T3 -> T6
- T4 -> T6
- T5 -> T6
- T6 -> T7

## Tasks
- [x] T1 Verify repository guidance, clean baseline identity, affected code, and existing tests
  depends_on: []
- [ ] T2 Add failing lock regressions, capture RED, implement lock, and capture GREEN
  depends_on: [T1]
- [ ] T3 Add failing atomic CBOE publication regressions, capture RED, implement, and capture GREEN
  depends_on: [T1]
- [ ] T4 Add failing exhaustive run-result regressions, capture RED, implement, and capture GREEN
  depends_on: [T1]
- [ ] T5 Add failing trusted-Git/source-binding regressions, capture RED, implement, and capture GREEN
  depends_on: [T1]
- [ ] T6 Run targeted and repository-wide verification gates and inspect exact staged diff
  depends_on: [T2, T3, T4, T5]
- [ ] T7 Commit the clean candidate and record exact commit/tree and evidence
  depends_on: [T6]

## Review
- Pending.
