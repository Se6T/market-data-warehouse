# Merkur FactsheetsLIVE historical-series endpoint verification (2026-07-23)

## Finding

The product-details GET itself is the only historical-series endpoint used by the page. It returns HTML with a server-rendered JavaScript array `var prices = [...]`; the browser makes no XHR/fetch request for chart data. There is no CSV/JSON download link and tested `/api/...`, `.json`, `.csv`, `?format=json`, `?_format=json`, and `?download=csv` variants did not expose a separate API.

**Request contract:** `GET https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/{ISIN}`. `{ISIN}` is the sole path parameter. No query parameters, auth, cookies, request body, or special headers are required. Response is `text/html; charset=UTF-8`. Parse `var prices`, where each record is `{"date":new Date(Y,M0,D),"value":V}` and `M0` is zero-based.

**Semantics:** chart heading is `Indexierte Wertentwicklung in Euro (EUR)`. Values are an EUR-denominated indexed performance series rebased to 100, not nominal historical NAV/unit prices. The page exposes only the latest nominal `Rücknahmepreis`.

## Verified coverage and samples

### IE00B6R52259 — iShares MSCI ACWI UCITS ETF USD (Acc)
- URL: `https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/IE00B6R52259`
- Coverage: 3754 observations, 2011-10-21 through 2026-07-22
- First record: `2011-10-21,100.0`
- Latest record: `2026-07-22,567.0184808520336`
- Latest nominal redemption price on page (not historical array): 120,62 USD (22.07.2026)

### LU0171310443 — BlackRock Global Funds - World Technology Fund A2 EUR
- URL: `https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/LU0171310443`
- Coverage: 5280 observations, 2005-07-11 through 2026-07-23
- First record: `2005-07-11,100.0`
- Latest record: `2026-07-23,1364.65517`
- Latest nominal redemption price on page (not historical array): 126,64 EUR (23.07.2026)

### LU0386882277 — Pictet - Global Megatrend Selection - P EUR
- URL: `https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/LU0386882277`
- Coverage: 4528 observations, 2008-10-31 through 2026-07-22
- First record: `2008-10-31,100.0`
- Latest record: `2026-07-22,528.3399800000001`
- Latest nominal redemption price on page (not historical array): 410,89 EUR (22.07.2026)

### LU0333595436 — JSS Sustainable Equity - Green Planet P EUR dist
- URL: `https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/LU0333595436`
- Coverage: 4651 observations, 2007-12-27 through 2026-07-21
- First record: `2007-12-27,100.0`
- Latest record: `2026-07-21,354.46013`
- Latest nominal redemption price on page (not historical array): 353,62 EUR (21.07.2026)

### LU0075056555 — BlackRock World Mining Fund A2 USD
- URL: `https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/LU0075056555`
- Coverage: 6743 observations, 1999-01-04 through 2026-07-23
- First record: `1999-01-04,100.0`
- Latest record: `2026-07-23,1674.6974757913533`
- Latest nominal redemption price on page (not historical array): 101,61 USD (23.07.2026)

### LU0171307068 — BlackRock Global Funds - World Healthscience F. A2 EUR
- URL: `https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/LU0171307068`
- Coverage: 6333 observations, 2001-04-06 through 2026-07-23
- First record: `2001-04-06,100.0`
- Latest record: `2026-07-23,596.41577`
- Latest nominal redemption price on page (not historical array): 66,56 EUR (23.07.2026)

### DE000A3CU5D7 — UniThemen Blockchain
- URL: `https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/DE000A3CU5D7`
- Coverage: 595 observations, 2024-02-29 through 2026-07-22
- First record: `2024-02-29,100.0`
- Latest record: `2026-07-22,171.90919`
- Latest nominal redemption price on page (not historical array): 169,32 EUR (22.07.2026)

## Retained artifacts

- `scripts/research/merkur_download_prices.py` — verified, reproducible downloader/parser
- This verification report — endpoint contract, coverage, and representative samples

The seven extracted CSV series, captured HTML/JavaScript pages, and coverage scratch JSON were reproducible from the public endpoint and were deleted during the 2026-08-21 filesystem cleanup.

The site warns that points more than five years old may be grouped for data-capacity reasons. Therefore this is business-day-like history, but the publisher does not contractually guarantee an ungrouped NAV for every valuation day.
