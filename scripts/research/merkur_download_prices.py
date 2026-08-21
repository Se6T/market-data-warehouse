#!/usr/bin/env python3
"""Download Merkur/FactsheetsLIVE inline historical indexed-EUR series."""
import argparse, csv, datetime as dt, html, re
from pathlib import Path
import requests

BASE = "https://merkur-leben-at.tools.factsheetslive.com/page/productdetails/{}"
DEFAULT_ISINS = [
    "IE00B6R52259", "LU0171310443", "LU0386882277", "LU0333595436",
    "LU0075056555", "LU0171307068", "DE000A3CU5D7",
]
PRICE_BLOCK = re.compile(r"var prices = (\[.*?\]);\s*var dataSet", re.S)
POINT = re.compile(r'\{"date":new Date\((\d+),\s*(\d+),\s*(\d+)\),"value":([0-9.Ee+\-]+)\}')
TITLE = re.compile(r'<h1 class="product-details-name">(.*?)</h1>', re.S)

def fetch(isin):
    url = BASE.format(isin)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    block = PRICE_BLOCK.search(response.text)
    if not block:
        raise RuntimeError(f"No inline price series found at {url}")
    rows = []
    for year, month0, day, value in POINT.findall(block.group(1)):
        date = dt.date(int(year), int(month0) + 1, int(day)).isoformat()
        rows.append((date, value))
    if not rows:
        raise RuntimeError(f"Price block at {url} contained no parseable records")
    title_match = TITLE.search(response.text)
    title = html.unescape(re.sub("<.*?>", "", title_match.group(1))).strip() if title_match else ""
    return url, title, rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("isins", nargs="*", default=DEFAULT_ISINS)
    ap.add_argument("-o", "--output", type=Path, default=Path("merkur_series"))
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    for isin in args.isins:
        url, title, rows = fetch(isin)
        target = args.output / f"{isin}.csv"
        with target.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "indexed_eur_value"])
            w.writerows(rows)
        print(f"{isin}: {len(rows)} records, {rows[0][0]}..{rows[-1][0]} -> {target} ({title}; {url})")
if __name__ == "__main__":
    main()
