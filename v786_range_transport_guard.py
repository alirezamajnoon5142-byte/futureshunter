"""Paced public-market-data transport for the V7.8.6 range scalper.

MEXC can reject bursts with code 510. The range engine does not need millisecond
kline polling because its structural confirmations use closed 1m/5m/15m candles.
Serialize its public REST requests with a small minimum gap so it coexists with
the main FuturesHunter scanner without weakening live execution/reconciliation.
"""
import os
import threading
import time

import v786_range_overlay as base

_ORIGINAL_HTTP_JSON = base._http_json
_LOCK = threading.Lock()
_LAST_REQUEST = 0.0
MIN_REQUEST_GAP_SECONDS = max(0.20, float(os.getenv("V786_RANGE_HTTP_MIN_GAP_SECONDS", "0.45")))


def _paced_http_json(url, params=None, timeout=8):
    global _LAST_REQUEST
    with _LOCK:
        now = time.monotonic()
        delay = MIN_REQUEST_GAP_SECONDS - (now - _LAST_REQUEST)
        if delay > 0:
            time.sleep(delay)
        try:
            return _ORIGINAL_HTTP_JSON(url, params=params, timeout=timeout)
        finally:
            _LAST_REQUEST = time.monotonic()


base._http_json = _paced_http_json
print(
    f"[V7DIAG] V7.8.6 range transport guard armed gap={MIN_REQUEST_GAP_SECONDS:.2f}s",
    flush=True,
)
