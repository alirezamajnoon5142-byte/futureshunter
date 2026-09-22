"""Cached + rate-aware public market-data transport for the live metals range scalper.

V7.8.8 transport patch:
- semantic candle caching so the micro and meso detectors share the same 5m/15m data;
- short ticker/funding caches to eliminate duplicate reads inside one symbol evaluation;
- serialized public REST access with adaptive cooldown after MEXC 510s/timeouts;
- one bounded retry, then fail closed (never trade from stale 1m/ticker data).

This patch only touches public-data reads used by RANGE_SCALPER. Live execution,
reconciliation, stops/TPs, and Core FuturesHunter scans are unchanged.
"""
import copy
import os
import threading
import time

import v786_range_overlay as base

_ORIGINAL_HTTP_JSON = base._http_json
_ORIGINAL_CLOSED_CANDLES = base._closed_candles
_ORIGINAL_TICKER = base._ticker

_REQUEST_LOCK = threading.Lock()
_CACHE_LOCK = threading.RLock()
_LAST_REQUEST = 0.0
_BACKOFF_UNTIL = 0.0

MIN_REQUEST_GAP_SECONDS = max(
    0.85, float(os.getenv("V786_RANGE_HTTP_MIN_GAP_SECONDS", "1.10"))
)
RATE_LIMIT_BACKOFF_SECONDS = max(
    2.0, float(os.getenv("V788_RANGE_510_BACKOFF_SECONDS", "4.0"))
)
TIMEOUT_BACKOFF_SECONDS = max(
    0.75, float(os.getenv("V788_RANGE_TIMEOUT_BACKOFF_SECONDS", "1.5"))
)
MAX_PUBLIC_RETRIES = min(
    2, max(0, int(os.getenv("V788_RANGE_PUBLIC_RETRIES", "1")))
)

CANDLE_TTL_SECONDS = {
    "Min1": max(15.0, float(os.getenv("V788_RANGE_CACHE_MIN1_SECONDS", "50"))),
    "Min5": max(60.0, float(os.getenv("V788_RANGE_CACHE_MIN5_SECONDS", "240"))),
    "Min15": max(120.0, float(os.getenv("V788_RANGE_CACHE_MIN15_SECONDS", "600"))),
}
TICKER_TTL_SECONDS = max(
    1.0, float(os.getenv("V788_RANGE_CACHE_TICKER_SECONDS", "4"))
)
FUNDING_TTL_SECONDS = max(
    15.0, float(os.getenv("V788_RANGE_CACHE_FUNDING_SECONDS", "60"))
)

_CANDLE_CACHE = {}
_TICKER_CACHE = {}
_HTTP_CACHE = {}
_STATS = {
    "candle_hits": 0,
    "candle_misses": 0,
    "ticker_hits": 0,
    "ticker_misses": 0,
    "http_hits": 0,
    "http_misses": 0,
    "retries": 0,
    "rate_limit_events": 0,
    "timeout_events": 0,
}


def _cache_get(cache, key):
    now = time.monotonic()
    with _CACHE_LOCK:
        item = cache.get(key)
        if not item:
            return None
        expires_at, value = item
        if now >= expires_at:
            cache.pop(key, None)
            return None
        return copy.deepcopy(value)


def _cache_set(cache, key, value, ttl):
    with _CACHE_LOCK:
        cache[key] = (time.monotonic() + max(0.0, float(ttl)), copy.deepcopy(value))
    return value


def _generic_cache_policy(url, params):
    """Cache only low-frequency metadata-ish endpoints.

    Klines are cached semantically by _cached_closed_candles below because their
    start/end timestamps change every call. Tickers are also cached by symbol.
    """
    lower = str(url or "").lower()
    p = params or {}
    symbol = str(p.get("symbol") or "").upper()

    if "/funding_rate/" in lower:
        path_symbol = str(url).rstrip("/").split("/")[-1].upper()
        return ("funding", path_symbol), FUNDING_TTL_SECONDS

    if lower.endswith("/api/v1/contract/ticker") and symbol:
        return ("ticker_http", symbol), TICKER_TTL_SECONDS

    return None, 0.0


def _classify_transient(exc):
    text = f"{type(exc).__name__}: {exc}".lower()
    if "510" in text or "too frequent" in text or "rate limit" in text:
        return "rate_limit"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    return None


def _paced_http_json(url, params=None, timeout=8):
    global _LAST_REQUEST, _BACKOFF_UNTIL

    cache_key, cache_ttl = _generic_cache_policy(url, params)
    if cache_key is not None:
        cached = _cache_get(_HTTP_CACHE, cache_key)
        if cached is not None:
            _STATS["http_hits"] += 1
            return cached
        _STATS["http_misses"] += 1

    last_exc = None
    for attempt in range(MAX_PUBLIC_RETRIES + 1):
        with _REQUEST_LOCK:
            now = time.monotonic()
            wait_until = max(
                _BACKOFF_UNTIL,
                _LAST_REQUEST + MIN_REQUEST_GAP_SECONDS,
            )
            delay = wait_until - now
            if delay > 0:
                time.sleep(delay)

            try:
                value = _ORIGINAL_HTTP_JSON(url, params=params, timeout=timeout)
                _LAST_REQUEST = time.monotonic()
                if _BACKOFF_UNTIL <= _LAST_REQUEST:
                    _BACKOFF_UNTIL = 0.0
                if cache_key is not None:
                    _cache_set(_HTTP_CACHE, cache_key, value, cache_ttl)
                return value
            except Exception as exc:
                _LAST_REQUEST = time.monotonic()
                last_exc = exc
                kind = _classify_transient(exc)
                if kind == "rate_limit":
                    _STATS["rate_limit_events"] += 1
                    _BACKOFF_UNTIL = max(
                        _BACKOFF_UNTIL,
                        _LAST_REQUEST + RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1),
                    )
                elif kind == "timeout":
                    _STATS["timeout_events"] += 1
                    _BACKOFF_UNTIL = max(
                        _BACKOFF_UNTIL,
                        _LAST_REQUEST + TIMEOUT_BACKOFF_SECONDS * (attempt + 1),
                    )
                else:
                    raise

        if attempt < MAX_PUBLIC_RETRIES:
            _STATS["retries"] += 1
            time.sleep(0.05)
            continue
        break

    raise last_exc


def _cached_closed_candles(symbol, interval, lookback_seconds):
    symbol = str(symbol or "").upper()
    interval = str(interval or "")
    key = (symbol, interval, int(lookback_seconds))
    cached = _cache_get(_CANDLE_CACHE, key)
    if cached is not None:
        _STATS["candle_hits"] += 1
        return cached

    _STATS["candle_misses"] += 1
    rows = _ORIGINAL_CLOSED_CANDLES(symbol, interval, lookback_seconds)
    ttl = CANDLE_TTL_SECONDS.get(interval, 30.0)
    return _cache_set(_CANDLE_CACHE, key, rows, ttl)


def _cached_ticker(symbol):
    symbol = str(symbol or "").upper()
    cached = _cache_get(_TICKER_CACHE, symbol)
    if cached is not None:
        _STATS["ticker_hits"] += 1
        return cached

    _STATS["ticker_misses"] += 1
    value = _ORIGINAL_TICKER(symbol)
    return _cache_set(_TICKER_CACHE, symbol, value, TICKER_TTL_SECONDS)


def range_transport_stats():
    with _CACHE_LOCK:
        return {
            **_STATS,
            "candle_cache_entries": len(_CANDLE_CACHE),
            "ticker_cache_entries": len(_TICKER_CACHE),
            "http_cache_entries": len(_HTTP_CACHE),
            "min_gap_seconds": MIN_REQUEST_GAP_SECONDS,
            "backoff_remaining_seconds": round(
                max(0.0, _BACKOFF_UNTIL - time.monotonic()), 3
            ),
        }


base._http_json = _paced_http_json
base._closed_candles = _cached_closed_candles
base._ticker = _cached_ticker
base._range_transport_stats = range_transport_stats

print(
    "[V7DIAG] V7.8.8 range transport cache armed "
    f"gap={MIN_REQUEST_GAP_SECONDS:.2f}s "
    f"ttl=1m:{CANDLE_TTL_SECONDS['Min1']:.0f}s/"
    f"5m:{CANDLE_TTL_SECONDS['Min5']:.0f}s/"
    f"15m:{CANDLE_TTL_SECONDS['Min15']:.0f}s "
    f"ticker:{TICKER_TTL_SECONDS:.0f}s funding:{FUNDING_TTL_SECONDS:.0f}s "
    f"retries={MAX_PUBLIC_RETRIES}",
    flush=True,
)
