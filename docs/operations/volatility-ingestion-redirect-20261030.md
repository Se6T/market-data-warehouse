# Volatility ingestion failure: CBOE redirect and undeployed fix

The installed CBOE owner at MDW commit `5fea1dd` calls `httpx.get(url, timeout=30)` without redirect support. CBOE responds with HTTP 307 from `cdn.cboe.com` to `cdn-api.cboe.com`. Running the installed owner with VIX, `--end 2026-10-29`, and a temporary warehouse reproduced exit 1 and the exact redirect error. All ten volatility identities were failed in the actual Oct 29 published manifest; canonical VIX bronze still ended Oct 7.

The source branch already contained the redirect repair and IBKR-primary volatility transport, but the installed dependency still used the predecessor. Source tests passing did not establish deployment. The repaired public owner was exercised against real CBOE responses in a temporary warehouse: all ten indices reached Oct 29; the IBKR-primary owner also completed all ten (nine IBKR, VXSMH CBOE fallback). Production verification belongs to the deployment receipt and installed-run evidence, not these sandbox results.

The production analytical database is `~/market-warehouse/duckdb/current/market.duckdb`, published through immutable bundles. The legacy `duckdb/market.duckdb` is not the production pointer. Canonical SPY bronze was already current through Oct 29; diagnosing it from the legacy database was incorrect.

Recovery scope: deploy the repaired ingestion owner, retain per-identity failure isolation, verify requested identities during each ingestion transaction, preserve bounded owner diagnostics, and surface degraded refreshes rather than hiding them behind an overall successful publication. No independent scheduled watcher and no trading-policy changes.
