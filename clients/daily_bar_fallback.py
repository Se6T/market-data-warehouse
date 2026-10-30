"""Fallback daily-bar client for U.S. equity symbols."""

from __future__ import annotations

import csv
import io
import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests
from requests import Response
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout as ReqTimeout

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15
_DEFAULT_MAX_RETRIES = 2
_DEFAULT_BACKOFF_FACTOR = 1.0
_DEFAULT_MIN_INTERVAL_SECONDS = 0.2
_NASDAQ_BASE_URL = "https://api.nasdaq.com/api/quote"
_STOOQ_DAILY_URL = "https://stooq.com/q/d/l/"
_YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
_USER_AGENT = "market-data-warehouse/1.0"
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_ISSUER_LOOKUP_ALIASES = {"DOMO": "HUCK", "PSKY": "SKYD"}
_YAHOO_CLASS_SHARE_SYMBOLS = {
    "BH.A": "BH-A",
    "CRD.A": "CRD-A",
    "GEF.B": "GEF-B",
    "MOG.A": "MOG-A",
}


@dataclass(frozen=True)
class FallbackDailyBar:
    """Normalized fallback daily bar."""

    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str

    @property
    def date(self) -> str:
        """Expose an IB BarData-like ``date`` attribute for script reuse."""
        return self.trade_date.isoformat()


class DailyBarFallbackClient:
    """Fetch daily OHLCV bars from public fallback providers.

    Provider order:
    1. Nasdaq historical quote API (`stocks`, then `etf`)
    2. Stooq U.S. daily CSV endpoint
    3. Yahoo Finance chart endpoint (last-resort identity-specific fallback)
    """

    def __init__(
        self,
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
        min_interval_seconds: float = _DEFAULT_MIN_INTERVAL_SECONDS,
        session: requests.Session | None = None,
    ):
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._min_interval_seconds = min_interval_seconds
        self._last_request_monotonic = 0.0
        self._session = session or requests.Session()
        # These caches deliberately live only for this client instance.  A
        # failed provider request is cached as ``None`` so one batch does not
        # repeat the same terminal retry sequence for every target date;
        # constructing the next scheduled client starts fresh.
        self._nasdaq_history_cache: dict[
            tuple[str, str], tuple[date, dict | None]
        ] = {}
        self._stooq_history_cache: dict[str, str | None] = {}
        self._yahoo_history_cache: dict[
            str, tuple[date, date, dict | None]
        ] = {}
        self._session.headers.update(
            {
                "Accept": "application/json,text/csv,*/*",
                "User-Agent": _USER_AGENT,
            }
        )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "DailyBarFallbackClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def get_daily_bar(self, symbol: str, trade_date: date) -> FallbackDailyBar | None:
        """Return a fallback OHLCV bar for ``symbol`` on ``trade_date``."""
        # Provider lookup symbols may follow a completed issuer transition;
        # the caller's canonical identity is intentionally never changed.
        canonical_symbol = symbol.upper()
        lookup_symbol = _ISSUER_LOOKUP_ALIASES.get(canonical_symbol, canonical_symbol)

        for asset_class in ("stocks", "etf"):
            bar = self._fetch_nasdaq_bar(lookup_symbol, trade_date, asset_class)
            if bar is not None:
                return bar

        bar = self._fetch_stooq_bar(lookup_symbol, trade_date)
        if bar is not None:
            return bar

        return self._fetch_yahoo_bar(lookup_symbol, trade_date)

    def fetch_daily_bar(self, symbol: str, trade_date: date) -> FallbackDailyBar | None:
        """Backward-compatible alias for callers that use the fetch_* name."""
        return self.get_daily_bar(symbol, trade_date)

    def _fetch_nasdaq_bar(
        self,
        symbol: str,
        trade_date: date,
        asset_class: str,
    ) -> FallbackDailyBar | None:
        # The Nasdaq API returns rows newest-first, bounded by `limit`, with
        # `fromdate` merely anchoring the oldest allowed date. To reach an
        # older target date the request must page the full record count.
        cache_key = (symbol, asset_class)
        cached = self._nasdaq_history_cache.get(cache_key)
        if cached is not None and trade_date >= cached[0]:
            payload = cached[1]
        else:
            payload = self._get_json(
                f"{_NASDAQ_BASE_URL}/{symbol}/historical",
                params={
                    "assetclass": asset_class,
                    "fromdate": trade_date.isoformat(),
                    "limit": "10000",
                },
            )
            self._nasdaq_history_cache[cache_key] = (trade_date, payload)
        if payload is None:
            return None
        status = payload.get("status")
        if not isinstance(status, dict) or status.get("rCode") != 200:
            return None

        data = payload.get("data")
        trades_table = data.get("tradesTable") if isinstance(data, dict) else None
        rows = trades_table.get("rows") if isinstance(trades_table, dict) else None
        if not isinstance(rows, list):
            return None
        target = trade_date.strftime("%m/%d/%Y")
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("date") != target:
                continue
            return self._make_bar(
                trade_date,
                row.get("open"), row.get("high"), row.get("low"),
                row.get("close"), row.get("volume"),
                source=f"nasdaq:{asset_class}",
            )
        return None

    def _fetch_stooq_bar(self, symbol: str, trade_date: date) -> FallbackDailyBar | None:
        if symbol in self._stooq_history_cache:
            text = self._stooq_history_cache[symbol]
        else:
            text = self._get_text(
                _STOOQ_DAILY_URL,
                params={"s": f"{symbol.lower()}.us", "i": "d"},
            )
            self._stooq_history_cache[symbol] = text
        if not text:
            return None

        reader = csv.DictReader(io.StringIO(text))
        target = trade_date.isoformat()
        for row in reader:
            if row.get("Date") != target:
                continue
            return self._make_bar(
                trade_date,
                row.get("Open"), row.get("High"), row.get("Low"),
                row.get("Close"), row.get("Volume"),
                source="stooq:us",
            )
        return None

    def _fetch_yahoo_bar(
        self, lookup_symbol: str, trade_date: date
    ) -> FallbackDailyBar | None:
        yahoo_symbol = _YAHOO_CLASS_SHARE_SYMBOLS.get(lookup_symbol, lookup_symbol)
        cached = self._yahoo_history_cache.get(yahoo_symbol)
        if cached is not None and cached[0] <= trade_date <= cached[1]:
            payload = cached[2]
        else:
            payload = self._get_json(
                f"{_YAHOO_CHART_URL}/{yahoo_symbol}",
                params={"interval": "1d", "range": "3mo"},
                provider_name="yahoo",
            )
            coverage = self._yahoo_coverage(payload, trade_date)
            self._yahoo_history_cache[yahoo_symbol] = (*coverage, payload)
        if not isinstance(payload, dict):
            return None

        chart = payload.get("chart")
        result = chart.get("result") if isinstance(chart, dict) else None
        result = result[0] if isinstance(result, list) and result else None
        if not isinstance(result, dict):
            return None
        meta = result.get("meta")
        timezone_name = meta.get("exchangeTimezoneName") if isinstance(meta, dict) else None
        try:
            if not isinstance(timezone_name, str):
                return None
            timezone = ZoneInfo(timezone_name)
        except (TypeError, ValueError):
            return None
        timestamps = result.get("timestamp")
        quote = result.get("indicators", {}).get("quote") if isinstance(result.get("indicators"), dict) else None
        quote = quote[0] if isinstance(quote, list) and quote else None
        if not isinstance(timestamps, list) or not isinstance(quote, dict):
            return None
        for index, timestamp in enumerate(timestamps):
            try:
                session_date = datetime.fromtimestamp(float(timestamp), timezone).date()
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            if session_date != trade_date:
                continue
            fields = {name: values[index] if isinstance(values, list) and index < len(values) else None
                      for name, values in (("open", quote.get("open")), ("high", quote.get("high")),
                                           ("low", quote.get("low")), ("close", quote.get("close")),
                                           ("volume", quote.get("volume")))}
            return self._make_bar(trade_date, **fields, source="yahoo:chart")
        return None

    @staticmethod
    def _yahoo_coverage(payload: dict | None, requested: date) -> tuple[date, date]:
        dates: list[date] = []
        try:
            if not isinstance(payload, dict):
                return requested, requested
            result = payload["chart"]["result"][0]
            timezone_name = result["meta"]["exchangeTimezoneName"]
            if not isinstance(timezone_name, str):
                return requested, requested
            timezone = ZoneInfo(timezone_name)
            dates = [datetime.fromtimestamp(float(ts), timezone).date() for ts in result.get("timestamp", [])]
        except (KeyError, IndexError, TypeError, ValueError, OverflowError, OSError):
            pass
        return (min(dates), max(dates)) if dates else (requested, requested)

    @classmethod
    def _make_bar(cls, trade_date: date, open: Any, high: Any, low: Any,
                  close: Any, volume: Any, *, source: str) -> FallbackDailyBar | None:
        try:
            values = [cls._parse_decimal(value) for value in (open, high, low, close)]
            parsed_volume = cls._parse_integer(volume)
            if any(not math.isfinite(value) or value <= 0 for value in values) or parsed_volume < 0:
                return None
            return FallbackDailyBar(
                trade_date=trade_date,
                open=values[0], high=values[1], low=values[2], close=values[3],
                volume=parsed_volume, source=source,
            )
        except (TypeError, ValueError):
            return None

    def _get_json(self, url: str, params: dict[str, Any], provider_name: str = "nasdaq") -> dict | None:
        response = self._get(url, params=params, provider_name=provider_name)
        if response is None:
            return None
        payload = self._safe_json(response)
        return payload or None

    def _get_text(self, url: str, params: dict[str, Any]) -> str | None:
        response = self._get(url, params=params, provider_name="stooq")
        if response is None:
            return None
        return response.text

    def _get(
        self,
        url: str,
        params: dict[str, Any],
        provider_name: str,
    ) -> Response | None:
        if self._max_retries < 0:
            return None

        for attempt in range(self._max_retries + 1):
            self._throttle()
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
            except (ReqConnectionError, ReqTimeout) as exc:
                if attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    continue
                log.warning(
                    "Fallback %s request failed for %s: %s",
                    provider_name, url, exc,
                )
                return None

            if response.status_code == 200:
                return response

            if response.status_code in _RETRYABLE_STATUS_CODES and attempt < self._max_retries:
                self._sleep_backoff(attempt, response)
                continue

            log.warning(
                "Fallback %s request returned HTTP %s for %s",
                provider_name, response.status_code, url,
            )
            return None

    def _throttle(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_monotonic
        remaining = self._min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
            now = time.monotonic()
        self._last_request_monotonic = now

    def _sleep_backoff(self, attempt: int, response: Response | None = None) -> None:
        retry_after = None if response is None else response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                time.sleep(float(retry_after))
                return
            except (TypeError, ValueError):
                pass
        time.sleep(self._backoff_factor * (2 ** attempt))

    @staticmethod
    def _safe_json(response: Response) -> dict:
        try:
            return response.json()
        except Exception:
            return {}

    @staticmethod
    def _parse_decimal(value: Any) -> float:
        if value is None:
            raise ValueError("missing decimal field")
        cleaned = str(value).replace("$", "").replace(",", "").strip()
        return float(cleaned)

    @staticmethod
    def _parse_integer(value: Any) -> int:
        if value is None:
            raise ValueError("missing integer field")
        cleaned = str(value).replace(",", "").strip()
        if cleaned in {"N/A", "NA", "--", "-", ""}:
            return 0
        return int(cleaned)


def _parse_float(value: Any) -> float:
    if value is None:
        raise ValueError("missing float field")
    return DailyBarFallbackClient._parse_decimal(value)


def _parse_int(value: Any) -> int:
    if value is None:
        raise ValueError("missing int field")
    return DailyBarFallbackClient._parse_integer(value)
