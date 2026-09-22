"""FuturesHunter V7.9.2 — high-quality second crypto slot.

Relaxes only the blunt one-crypto-direction correlation veto. A second same-direction
Core crypto position may pass when the new signal is objectively strong. No other
live gate reason is overridden. RANGE_SCALPER and manual/untracked positions are
untouched.
"""
import os
import re
import sys
import threading
import time

import live_executor_v70 as live
import v791_challenger_rotation as v791

ENABLED = os.getenv("V792_SECOND_CRYPTO_ENABLED", "true").lower() == "true"
MODE = os.getenv("V792_SECOND_CRYPTO_MODE", "live").strip().lower()
MIN_SCORE = max(75.0, float(os.getenv("V792_SECOND_CRYPTO_MIN_SCORE", "80")))
MIN_RAW = max(60.0, float(os.getenv("V792_SECOND_CRYPTO_MIN_RAW", "68")))
MIN_OI = max(6.0, float(os.getenv("V792_SECOND_CRYPTO_MIN_OI", "10")))
MAX_COST_R = min(0.30, max(0.01, float(os.getenv("V792_SECOND_CRYPTO_MAX_COST_R", "0.15"))))
REQUIRE_STRATEGY = os.getenv("V792_SECOND_CRYPTO_REQUIRE_STRATEGY", "STRONG_CONFIRM").strip().upper()

_PATCHED = False
_PATCH_LOCK = threading.RLock()


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _norm(v):
    return str(v or "").strip().upper()


def _strategy_consensus(result, gate):
    candidates = [
        (gate or {}).get("strategy_consensus"),
        (result or {}).get("strategy_consensus"),
    ]
    for key in ("strategy_shadow", "strategy", "strategy_ensemble"):
        obj = (result or {}).get(key)
        if isinstance(obj, dict):
            candidates.extend([obj.get("consensus"), obj.get("state"), obj.get("decision")])
    for value in candidates:
        value = _norm(value)
        if value:
            return value
    return ""


def _cost_r(result, gate):
    for value in (
        (gate or {}).get("estimated_cost_r"),
        (gate or {}).get("cost_r"),
        (result or {}).get("estimated_cost_r"),
        (result or {}).get("cost_r"),
    ):
        if value is not None:
            return _f(value, 999.0)
    return 999.0


def _event_risk(main):
    try:
        snap = main.get_macro_snapshot() or {}
        return _norm(snap.get("event_risk") or "LOW")
    except Exception:
        return "UNKNOWN"


def _correlation_count_and_direction(reasons):
    for reason in reasons:
        text = str(reason)
        m = re.search(r"live correlation guard:\s*(\d+)\s+CRYPTO\s+(LONG|SHORT)\s+position", text, re.I)
        if m:
            return int(m.group(1)), m.group(2).upper()
    return None, ""


def _only_correlation_block(reasons):
    if not reasons:
        return False
    seen = False
    for reason in reasons:
        s = str(reason).lower()
        if "live correlation guard" in s and "crypto" in s:
            seen = True
            continue
        return False
    return seen


def _eligible_second_slot(main, result, gate):
    if not ENABLED or MODE not in {"live", "shadow"}:
        return None
    if (gate or {}).get("eligible"):
        return None

    reasons = list((gate or {}).get("reasons") or [])
    if not _only_correlation_block(reasons):
        return None

    count, blocked_direction = _correlation_count_and_direction(reasons)
    if count != 1:
        return None

    c = v791._candidate_metrics(result)
    if c["direction"] not in {"LONG", "SHORT"} or c["direction"] != blocked_direction:
        return None
    if c["state"] and c["state"] != "ENTRY":
        return None
    if c["regime"] not in {"BREAKOUT", "TREND_CONTINUATION"}:
        return None
    if c["score"] < MIN_SCORE or c["raw"] < MIN_RAW or c["oi"] < MIN_OI:
        return None

    consensus = _strategy_consensus(result, gate)
    if REQUIRE_STRATEGY and consensus != REQUIRE_STRATEGY:
        return None

    cost = _cost_r(result, gate)
    if cost > MAX_COST_R:
        return None

    event_risk = _event_risk(main)
    if event_risk == "EXTREME":
        return None

    return {
        "candidate": c,
        "strategy": consensus,
        "cost_r": cost,
        "event_risk": event_risk,
        "prior_reasons": reasons,
    }


def _patch(main):
    global _PATCHED
    with _PATCH_LOCK:
        if _PATCHED:
            return True
        if not getattr(v791, "_PATCHED", False):
            return False
        if not hasattr(main, "_v71_live_candidate_gate"):
            return False

        original_gate = main._v71_live_candidate_gate

        def gate_wrapper(result, trades):
            gate = original_gate(result, trades)
            try:
                decision = _eligible_second_slot(main, result, gate)
                if not decision:
                    return gate

                c = decision["candidate"]
                if MODE == "shadow":
                    live._diag(
                        f"V7.9.2 SECOND CRYPTO SHADOW would allow {c['symbol']} {c['direction']} "
                        f"score={c['score']:.1f} raw={c['raw']:.0f} OI={c['oi']:.0f}/15 "
                        f"strategy={decision['strategy']} cost={decision['cost_r']:.2f}R"
                    )
                    return gate

                out = dict(gate or {})
                out["eligible"] = True
                out["second_crypto_override"] = True
                out["second_crypto_slot"] = 2
                out["second_crypto_cost_r"] = decision["cost_r"]
                out["reasons"] = [
                    f"V7.9.2 high-quality second crypto slot: Core {c['score']:.1f}, "
                    f"Raw {c['raw']:.0f}, OI {c['oi']:.0f}/15, {decision['strategy']}, "
                    f"cost {decision['cost_r']:.2f}R"
                ]
                live._diag(
                    f"V7.9.2 SECOND CRYPTO LIVE ALLOW {c['symbol']} {c['direction']} "
                    f"score={c['score']:.1f} raw={c['raw']:.0f} OI={c['oi']:.0f}/15 "
                    f"regime={c['regime']} strategy={decision['strategy']} cost={decision['cost_r']:.2f}R"
                )
                return out
            except Exception as exc:
                live._diag(f"V7.9.2 second-crypto wrapper fail-closed: {type(exc).__name__}: {exc}")
                return gate

        main._v71_live_candidate_gate = gate_wrapper
        _PATCHED = True
        live.V70_VERSION = "7.9.2-second-crypto-slot"
        live._diag(
            f"V7.9.2 second crypto slot armed mode={MODE} score>={MIN_SCORE:.0f} raw>={MIN_RAW:.0f} "
            f"OI>={MIN_OI:.0f}/15 strategy={REQUIRE_STRATEGY or 'ANY'} cost<={MAX_COST_R:.2f}R "
            "max_same_direction_crypto=2; all non-correlation live vetoes preserved"
        )
        return True


def _bootstrap():
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            main = sys.modules.get("__main__")
            if main is not None and _patch(main):
                return
        except Exception as exc:
            live._diag(f"V7.9.2 bootstrap retry: {type(exc).__name__}: {exc}")
        time.sleep(0.25)
    live._diag("V7.9.2 bootstrap gave up after 300s; second crypto slot not patched")


if ENABLED:
    threading.Thread(target=_bootstrap, name="V792SecondCrypto", daemon=True).start()
    live._diag("V7.9.2 high-quality second crypto slot bootstrap armed")
