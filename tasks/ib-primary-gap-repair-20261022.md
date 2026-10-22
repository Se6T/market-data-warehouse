# IB-primary daily-gap repair

Scope: fix CBOE redirect and Nasdaq nullable payload parsing; diagnose stale daily closes through read-only PAPER IB API; normalize proven IB share-class notation; prefer IB for available volatility indices, retain CBOE fallback for unresolved indices. Preserve canonical bronze identity/history, snapshot immutability, admission and order guards. No trading operations.

Dependency graph: investigate -> fixes -> tests/review -> merged release -> dependency/consumer pins -> deployment -> owner refresh -> actual data and service verification.

- investigate: depends_on: []; reproduce missing identities, errors, and source responses.
- fixes: depends_on: [investigate]; minimal source fixes and targeted regression cases.
- validation: depends_on: [fixes]; existing focused tests and independent diff review.
- release: depends_on: [validation]; merge canonical MDW source; update exact PE pins preserving all unrelated changes.
- operational: depends_on: [release]; deploy reviewed sources, run market-data owners only, publish through normal MDW transaction and verify session coverage; never dispatch orders during repair.

Terminal/delisted instruments are not backfilled with invented later bars or acquisition-successor prices. Document them separately from broken ingestion.
