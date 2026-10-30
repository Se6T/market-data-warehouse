# IB-primary daily market-data repair

## Source policy

- Equities retain the existing IBKR-first `daily_update.py` path. Public Nasdaq/Stooq data is recovery only.
- The all-universe refresh now invokes `fetch_volatility_daily.py`: one bounded read-only IBKR session, per-symbol current-bar validation, CBOE fallback only for unresolved/stale IB series. CBOE direct fetch remains available for historical/backfill consumers.
- Current broker evidence: VIX, VVIX, COR1M, COR3M, OVX, RVX, VIX3M, VXEEM, VXN all qualify on CBOE and return 2026-10-21 daily bars. VXSMH returns IB Error200 and requires CBOE fallback.
- Existing historical observations are preserved by the existing atomic merge writer. This repair does not silently rewrite old CBOE history with IB values. Provider series can differ: the Oct21 COR1M close returned by IB was 8.30, CBOE 8.40. IB is selected as requested, not presented as numerically identical.

## Fixed defects

1. Follow CBOE's HTTP307 redirect to cdn-api.cboe.com.
2. Treat Nasdaq `data:null` as unavailable rather than crashing and suppressing Stooq.
3. Translate four proven IB share-class spellings at the contract boundary only: BH.A -> BH A, CRD.A -> CRD A, GEF.B -> GEF B, MOG.A -> MOG A.
4. Resolve same-public-issuer ticker changes DOMO -> HUCK and PSKY -> SKYD at that same boundary; warehouse identities and history remain stable. See dated issuer/exchange evidence in `ib-primary-gap-diagnosis-20261022.md`. Do not substitute acquisition-successor prices for cancelled securities.

## Actual verification

- Read-only broker probes: all four space-spelled shares and nine volatility indices returned Oct21 data. Original dotted spellings and the thirteen old equity tickers returned Error200.
- Sandbox owner run on copies of real bronze: four class shares, 32 daily bars appended, 0 failures, 0 fallback attempts, 0 validation issues.
- Sandbox volatility owner run: all ten updated through Oct21; nine IBKR and VXSMH CBOE fallback.
- Regression suites cover redirects, nullable fallback payload, contract spelling, IB-source priority, per-symbol fallback, stale-source no-write, and failure isolation; existing daily and atomic-refresh tests remain applicable.

## Remaining terminal/moved instruments

The old tickers ACVA, ATKR, BLFS, BZH, CCO, DBRG, GBTG, SLP, TBPH, WBD were acquired/cancelled; WALD moved off Nasdaq to OTC. They must not receive invented post-delisting bars. Their preserved history/failure reporting is separate from the repaired active-symbol ingestion. This release does not silently delete canonical identities, change universe membership, or admit OTC instruments into strategy trading.
