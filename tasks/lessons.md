# Lessons

- Before finalizing a plan or design summary in a dirty worktree, re-run `git status --short` and inspect current diffs for touched runtime files so the plan matches the latest local code, not an earlier snapshot.
- When the user calls out stale documentation, audit every repo doc that describes runtime behavior, operations, architecture, or examples instead of updating only the most obvious file.
- When answering questions about the current storage path, inspect both ingestion scripts and the storage client so you distinguish the system of record from the publish sidecar.
- When a user corrects an operational assumption like a live database lock, re-check the actual process and file state immediately before continuing so recovery work is based on current runtime conditions rather than stale observations.
- When turning a one-off recovery into a productized fallback path, scope the provider chain to the repo's actual universe and calendar model instead of assuming one exchange-specific source is sufficient.
- When promoting a globally installed machine service, keep the launchd label scoped to the service itself rather than the repo or an old application name.
- When a user rephrases a packaging request, restate the exact artifact they want to run before implementing helpers so we build the app/runtime path they actually need instead of a nearby convenience wrapper.
- When a macOS sidebar must be reliably clickable, prefer explicit button or navigation-link rows over implicit `List(selection:)` tagging so interaction is obvious and testable.
- When asked to update “all docs,” explicitly sweep root operator docs, local feature docs, and agent-facing guides together instead of assuming the README alone is enough.
- Do not run the macOS UI smoke harness unless the user explicitly asks for UI automation or smoke verification in that turn; default to build plus unit-test verification for ongoing implementation work.
- When a recent-bar recovery fails for a single ticker, check whether the security was delisted before treating it as a provider outage; if it was delisted, remove it from future sync/backfill inputs and archive its parquet outside the canonical bronze tree.
- When a user asks to turn a one-off strategy into a reusable module, default the design toward configurable universe inputs like presets, explicit ticker lists, and warehouse-discovered symbols instead of hard-wiring the first index used in the analysis.
- When reporting quantitative research quality, do not present exploratory results as production-grade unless total-return validation, realistic costs, walk-forward/robustness checks, and baseline/null-model comparisons are actually implemented and reported.
- Never run `rebuild_duckdb_from_parquet.py` sequentially for multiple non-futures asset classes: each invocation recreates the shared symbol/daily tables. Use the atomic all-universe rebuild so one asset refresh cannot erase the others.
- Broker-preservation mode must batch source-identity verification rather than running Git verification once per preserved symbol, and public writers must canonicalize legacy IDs even when there are no new rows.
- Immutable per-run evidence must use a unique default path; a fixed `O_EXCL` inventory filename makes a safe retry fail after any earlier run.
