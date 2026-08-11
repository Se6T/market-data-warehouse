# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Implement Phase 3 M0: an atomic, fail-closed, all-universe bronze refresh and DuckDB rebuild with immutable audit artifacts.

## Success Criteria
- Discover the complete active canonical bronze inventory for equity, volatility, crypto, and futures before any refresh.
- Refresh every discovered identity through its owner-specific existing pipeline, with mocked external I/O in tests.
- Build and validate all asset classes together in a new temporary DuckDB without cross-asset erasure.
- Publish the database only after complete validation using fsync plus atomic replacement; every earlier failure preserves the old DB bytes.
- Emit a canonical JSON manifest containing source-tree provenance, requested as-of, step status, complete inventories, physical schemas and schema hashes, latest sessions, row counts, database SHA256, and publication evidence.
- Pass focused tests and RuntimeWarning-as-error gates; disclose baseline-only full-suite failures rather than hiding them.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4
- T4 -> T5
- T5 -> T6

## Tasks
- [x] T1 Inspect repository guidance, schemas, update owners, rebuild path, and baseline state
  depends_on: []
- [x] T2 Add Phase 3 M0 behavioral and failure-injection tests; record expected RED failures
  depends_on: [T1]
- [x] T3 Implement temporary multi-asset loading, orchestration, validation, manifesting, fsync, and atomic publication
  depends_on: [T2]
- [x] T4 Update README.md, AGENTS.md, CLAUDE.md, and durable project memory for the new operator contract
  depends_on: [T3]
- [x] T5 Run focused, full, RuntimeWarning-as-error, static, secret, diff, and baseline-comparison gates
  depends_on: [T4]
- [ ] T6 Commit the verified implementation with the mandated message
  depends_on: [T5]

## Review
- RED evidence:
  - Initial M0 test import failed because `scripts/refresh_all_and_rebuild.py` did not exist.
  - The expanded focused suite reproduced 22 passing / 3 failing corrupt-Parquet variants; the fixture incorrectly used dataset-aware `pq.read_table(path)` under Hive directories. Reading the physical file with `pq.ParquetFile(path).read()` exposed the intended corruptions without weakening production validation.
  - Added focused RED probes for the owner-native futures preset, warehouse propagation, complete manifest schema/status evidence, overlapping output paths, and same-name/wrong-type Parquet schema corruption.
- GREEN evidence:
  - Focused M0/DB/rebuild suite: 76 passed with `-W error::RuntimeWarning` (only the third-party eventkit DeprecationWarning remains).
  - Changed modules `clients/db_client.py` and `scripts/refresh_all_and_rebuild.py` reached 100% in the focused coverage report; the repository-wide coverage command cannot pass on a focused subset because it measures every configured client/script.
- Baseline comparison:
  - Candidate full suite: 424 passed, 34 failed.
  - Archived clean HEAD full suite: 398 passed, 34 failed.
  - Failure node IDs are identical: 15 `daily_update` mock-seam failures, 16 `fetch_ib_historical` mock-seam failures, 2 Linux-inapplicable `launchctl` failures, and 1 storage compatibility mock-seam failure. No branch regression.
  - Full RuntimeWarning-as-error candidate and archived HEAD runs have the same 34 failure identities and pre-existing unawaited-IB coroutine warnings.
- Safety proof:
  - Failure injection covers inventory, refresh, rebuild, validation, owner-command failure, inventory/date/schema/count/identity drift, and both publication states; predecessor DB bytes remain unchanged.
  - Physical Parquet schema is read without Hive partition injection and validated by ordered names and Arrow types.
  - Owner subprocess output is captured but never persisted or printed; generic exceptions print only their type.
  - `py_compile`, `git diff --check`, and `scripts/pre-commit-secrets-scan.sh` pass.
