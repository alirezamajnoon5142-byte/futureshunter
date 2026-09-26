"""FuturesHunter V8.3 — live selector challenger with hard-risk preservation.

This layer wraps V8.1's live 4H swing candidate generation. It may admit only
near-threshold candidates (within 2 points of the V8.1 score threshold) and only
when every existing hard gate, stop geometry rule, macro event veto, portfolio
limit, execution path, and risk budget still passes unchanged.

It does NOT alter position sizing, leverage, stop placement, TP geometry,
portfolio caps, correlation caps, live-only strategy tagging, or execution-cost
checks owned by the downstream executor.
"""
import os
import threading
import time

import live_executor_v70 as live
import v810_swing4h_live as swing

ENABLED = os.getenv("V830_SELECTOR_CHALLENGER_ENABLED", "true").lower() == "true"
DELTA = min(5.0, max(0.0, float(os.getenv("V830_SELECTOR_THRESHOLD_DELTA", "2.0"))))

_PATCHED = False
_LOCK = threading.RLock()
_ORIGINAL_DIRECTION_SCORE = None
_ORIGINAL_DIAGNOSTIC = None


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _patch():
    global _PATCHED, _ORIGINAL_DIRECTION_SCORE, _ORIGINAL_DIAGNOSTIC
    with _LOCK:
        if _PATCHED:
            return True
        if not hasattr(swing, "_direction_score") or not hasattr(live, "diagnostic_state"):
            return False

        _ORIGINAL_DIRECTION_SCORE = swing._direction_score
        _ORIGINAL_DIAGNOSTIC = live.diagnostic_state

        # V8.1 _direction_score already enforces the existing structural setup
        # requirements and returns macro-event blocked=True for HIGH/EXTREME.
        # We leave that function's logic unchanged. The only live change is the
        # score floor used by V8.1 _candidate.
        original_min = float(swing.MIN_SCORE)
        challenged_min = max(65.0, original_min - DELTA)
        swing.MIN_SCORE = challenged_min

        def diagnostic_state():
            state = _ORIGINAL_DIAGNOSTIC()
            state.update({
                "v830_selector_challenger_enabled": ENABLED,
                "v830_original_min_score": original_min,
                "v830_live_min_score": challenged_min,
                "v830_threshold_delta": DELTA,
                "v830_hard_risk_preserved": True,
            })
            return state

        live.diagnostic_state = diagnostic_state
        _PATCHED = True
        live._diag(
            f"V8.3 SELECTOR CHALLENGER live: score floor {original_min:.1f}->{challenged_min:.1f}; "
            "all V8.1 hard gates/risk geometry/portfolio limits preserved"
        )
        return True


def _bootstrap():
    deadline = time.time() + 360
    while time.time() < deadline:
        try:
            if ENABLED and _patch():
                return
        except Exception as exc:
            try:
                live._diag(f"V8.3 bootstrap retry: {type(exc).__name__}: {exc}")
            except Exception:
                pass
        time.sleep(0.25)
    try:
        live._diag("V8.3 bootstrap gave up; selector challenger not armed")
    except Exception:
        pass


if ENABLED:
    threading.Thread(target=_bootstrap, name="V830Bootstrap", daemon=True).start()
    try:
        live._diag("V8.3 selector challenger bootstrap armed")
    except Exception:
        pass
