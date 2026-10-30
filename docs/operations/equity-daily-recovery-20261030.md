# Equity daily-data recovery (2026-10-30)

## Observed failures

The production snapshot requested 2026-10-29 and retained 21 failed equity identities. A bounded read-only PAPER broker probe resolved the four space-spelled share classes and HUCK/SKYD, but their daily history requests (and SPY control) returned IB error 162: **Trading TWS session is connected from a different IP address**. This is a market-data entitlement/session issue, not a failure of the six corrected contract spellings. No other trading session was disconnected or taken over.

The public recovery path had two independent defects:

- Nasdaq can return `data.tradesTable: null`; the earlier `data: null` repair did not cover that nested case, so the fallback chain still raised AttributeError for the four share classes.
- Provider lookups still used DOMO and PSKY. Direct Nasdaq queries for HUCK and SKYD returned October 29 bars; the old names did not.

The existing Nasdaq/Stooq spelling variants did not supply the share-class bars. The Yahoo chart endpoints for BH-A, CRD-A, GEF-B and MOG-A each returned real daily history. The last-resort fallback is per identity, after IBKR and the existing public providers, with exact exchange-local session matching and explicit `yahoo:chart` source attribution.

## Terminal securities

Source-reviewed terminal/exchange-exclusion records live in `metadata/equity-lifecycle.json`. Once the source-backed corporate action is effective, the owner excludes the security from new daily requests and the live strategy universe. Final-bar completeness is not a prerequisite; the actual existing history is retained unchanged. It publishes the unchanged history as `preserved`, with the reason, source URL and excerpt in the ingestion freshness sidecar. Acquired/cancelled securities are labeled **retired**; OTC departures are labeled **excluded_otc**, never falsely labeled current through the requested trading date.

GETY is an important distinction: it continued OTC under GETY after NYSE suspension and later filed Chapter 11; it was not acquired. Its already-ingested tail through October 21 is preserved unchanged, not represented as its final OTC trading date. WALD's later issuer filing identifies the OTC share ticker as MLKMF (not the earlier unconfirmed MLKM). Neither OTC listing is silently admitted into the exchange-listed trading universe.

Canonical bronze files, stable IDs and historical DuckDB rows remain present. No prices are grafted from an acquirer, no old history is deleted, and the pre/post identity-set, hash-preservation and strict PE manifest-schema checks remain unchanged. An incomplete final historical session does not keep a verified cancelled listing active or block its removal. CCO is excluded on its verified October 14 cash acquisition, retaining its October 13 snapshot without inventing a closing-day bar.

This repair does not resolve the other-IP IBKR session ownership. IB remains the first source on subsequent runs; explicit per-symbol fallbacks preserve data availability while that condition exists. No orders, LIVE session changes, independent watchdogs or scheduled verification loops are involved.
