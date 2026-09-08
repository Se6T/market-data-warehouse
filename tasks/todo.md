# Current Plan — September PAPER Data Admission

## Goal and dependency graph
Repair the reproduced APGE cash-acquisition retirement and VXM holiday-session
admission failures without synthesizing bars or weakening publication gates.

- diagnosis depends_on: [] — complete: PAPER-only read-only probe reproduces
  APGE error 200 and VXM's nonexistent September 7 completed-session requirement.
- regressions depends_on: [diagnosis] — pending: exact active membership and
  CFE completed-session/publication boundary tests, including stale rejection.
- implementation depends_on: [regressions] — pending: coherent active preset
  retirement (no replacement) and identity-specific VXM session handling.
- release depends_on: [implementation] — pending: full coverage, warning,
  compile and secret gates; frozen independent review; fork PR/CI/merge.
- operation depends_on: [release] — pending: downstream pin/release approval,
  preserve/archive APGE bronze, one canonical refresh, production admission.

No broker order, authority, runtime activation or operational data mutation is
authorized by these source edits alone. Those retain their independent gates.

# Previous completed plan — Restore Atomic All-Universe Refresh

## Goal
Restore the hardened `refresh_all_and_rebuild.py` transaction expected by Portfolio Engine, preserve every bronze asset class in one immutable DuckDB generation, and prove the real public-data refresh works without scheduler or broker-account access.

## Dependency Graph
- task-1-restore-contract depends_on: []
- task-2-adapt-current-sources depends_on: [task-1-restore-contract]
- task-3-regression-tests depends_on: [task-2-adapt-current-sources]
- task-4-real-refresh depends_on: [task-3-regression-tests]
- task-5-cross-repo-verification depends_on: [task-4-real-refresh]

## Tasks

### task-1-restore-contract
- depends_on: []
- Restore the last hardened atomic refresh implementation and its complete tests from repository history.
- Status: done.

### task-2-adapt-current-sources
- depends_on: [task-1-restore-contract]
- Preserve existing equity/futures bronze without connecting to IB; refresh public volatility/crypto sources and rebuild all discovered classes together.
- Status: done.

### task-3-regression-tests
- depends_on: [task-2-adapt-current-sources]
- Prove sequential asset-class replacement cannot erase other partitions and missing optional futures does not fabricate data.
- Status: done.

### task-4-real-refresh
- depends_on: [task-3-regression-tests]
- Run the production command for 2026-08-21 and admit the immutable successor bundle.
- Status: done. Published generation `c72d8f3462ed491b8204e48a2f55b6d4` from commit `7eca0022ff43a71cf828b397d4715fd477cea5c1`.

### task-5-cross-repo-verification
- depends_on: [task-4-real-refresh]
- Run MDW 100% coverage gates and Portfolio Engine full CI/build/installed-wheel/read-only admission gates.
- Status: done. MDW CI, Portfolio Engine CI, the 1,688-test local suite, and real read-only bundle admission passed.
