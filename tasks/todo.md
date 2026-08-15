# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Repair the Phase 3 M0 daily-update test seam so mocked tests cannot fall through to a real IB connection.

## Success Criteria
- Daily-update main-path tests patch `create_ib_client_or_adapter`, preserving the provider factory runtime contract.
- A centralized fail-closed test guard turns any accidental real `IBClient.connect()` call into an assertion before network I/O.
- The reproduced targeted suite, full 100% coverage suite, and RuntimeWarning-as-error suite pass.
- Only the minimal test-seam/plan changes are committed; no refresh, broker connection, or warehouse mutation occurs.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4
- T4 -> T5

## Tasks
- [x] T1 Inspect repository guidance, branch state, production construction path, and stale mocks
  depends_on: []
- [x] T2 Add a fail-closed real-IB connection guard and record safe RED evidence
  depends_on: [T1]
- [x] T3 Centralize mocks on the provider factory and verify targeted GREEN
  depends_on: [T2]
- [x] T4 Run full coverage and RuntimeWarning-as-error gates and inspect the final diff
  depends_on: [T3]
- [x] T5 Commit the verified minimal fix and record commit/tree identity
  depends_on: [T4]

## Review
- RED evidence:
  - With the fail-closed fixture installed first, `TestMain::test_end_to_end` failed at the guarded `IBClient.connect()` call with the intended "must mock the IB client factory" assertion; no socket connection was made.
- GREEN evidence:
  - The same focused regression passed after replacing the stale constructor patch with the centralized factory patch.
  - Requested four-file target: 218 passed.
  - Requested four-file RuntimeWarning gate: 218 passed; only the third-party eventkit DeprecationWarning remains.
- Final verification:
  - Full coverage gate: 493 passed, 19 failed, 90.52% aggregate coverage. The 14 daily-update failures are fixed; remaining baseline blockers are 16 stale `fetch_ib_historical` factory mocks, 1 stale daily storage-compat factory mock, and 2 Linux UID-specific launchctl assertions.
  - `git diff --check` passes.
