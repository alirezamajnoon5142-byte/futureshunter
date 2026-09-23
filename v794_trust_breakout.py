"""FuturesHunter V7.9.4 — Trust the Breakout.

Surgically rescues high-quality BREAKOUT candidates when the ONLY live-selector
block is the S/R gate treating an already-consumed level as still active.

No regime, correlation, cooldown, cost, event-risk, loss-cluster, strategy, or
other live veto is bypassed.
"""
import os
import sys
import threading
import time

import live_executor_v70 as live
import v791_challenger_rotation as v791

ENABLED = os.getenv("V794_TRUST_BREAKOUT_ENABLED", "true").lower() == "true"
MODE = os.getenv("V794_TRUST_BREAKOUT_MODE", "live").strip().lower()
MIN_CORE = max(80.0, float(os.getenv("V794_MIN_CORE", "85")))
MIN_RAW = max(65.0, float(os.getenv("V794_MIN_RAW", "72")))
MIN_OI = max(6.0, float(os.getenv("V794_MIN_OI", "10")))
MAX_COST_R = min(0.25, max(0.01, float(os.getenv("V794_MAX_COST_R", "0.15"))))
ALLOWED_STRATEGY = {
    x.strip().upper()
    for x in os.getenv("V794_ALLOWED_STRATEGY", "CONFIRM,STRONG_CONFIRM").split(",")
    if x.strip()
}

_PATCHED = False
_PATCH_LOCK = threading.RLock()


def _norm(v):
    return str(v or "").strip().upper()


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _is_structural_reason(reason):
    s = str(reason or "").lower()
    return (
        "major support" in s
        or "major resistance" in s
        or "crowded structural path" in s
    )


def _eligible_rescue(result, gate):
    if not ENABLED or MODE not in {"live", "shadow"}:
        return None
    if not isinstance(gate, dict) or gate.get("eligible"):
        return None

    reasons = list(gate.get("reasons") or [])
    if not reasons or not all(_is_structural_reason(r) for r in reasons):
        return None

    c = v791._candidate_metrics(result)
    if _norm(c.get("state")) != "ENTRY":
        return None
    if _norm(c.get("regime")) != "BREAKOUT":
        return None
    if _f(c.get("score")) < MIN_CORE or _f(c.get("raw")) < MIN_RAW or _f(c.get("oi")) < MIN_OI:
        return None

    strategy = _norm(gate.get("strategy_consensus"))
    if strategy not in ALLOWED_STRATEGY:
        return None

    cost_r = _f(gate.get("estimated_cost_r"), 999.0)
    if cost_r > MAX_COST_R:
        return None

    sr = gate.get("support_resistance") or (result or {}).get("support_resistance") or {}
    if not sr.get("ok"):
        return None

    # Critical invariant: rescue only when the exact raw nearest level that
    # generated the structural objection has CLOSED-CANDLE breakout evidence.
    raw = sr.get("raw_nearest_zone") or {}
    evidence = raw.get("breakout") or {}
    if not raw or not evidence.get("confirmed") or not evidence.get("live_hold"):
        return None

    modes = list(evidence.get("modes") or [])
    if not modes:
        return None

    return {
        "candidate": c,
        "strategy": strategy,
        "cost_r": cost_r,
        "raw_zone": raw,
        "evidence": evidence,
        "prior_reasons": reasons,
    }


def _patch(main):
    global _PATCHED
    with _PATCH_LOCK:
        if _PATCHED:
            return True
        if not hasattr(main, "_v71_live_candidate_gate"):
            return False

        original_gate = main._v71_live_candidate_gate

        def gate_wrapper(result, trades):
            gate = original_gate(result, trades)
            try:
                rescue = _eligible_rescue(result, gate)
                if not rescue:
                    return gate

                c = rescue["candidate"]
                z = rescue["raw_zone"]
                ev = rescue["evidence"]
                modes = ",".join(ev.get("modes") or ["CLOSED_CANDLE"])
                side = "support" if _norm(c.get("direction")) == "SHORT" else "resistance"

                if MODE == "shadow":
                    live._diag(
                        f"V7.9.4 TRUST BREAKOUT SHADOW would rescue {c['symbol']} {c['direction']} "
                        f"Core={_f(c['score']):.1f} Raw={_f(c['raw']):.0f} OI={_f(c['oi']):.0f}/15 "
                        f"strategy={rescue['strategy']} cost={rescue['cost_r']:.2f}R "
                        f"consumed_{side}={_f(z.get('low')):.8g}-{_f(z.get('high')):.8g} via={modes}"
                    )
                    return gate

                out = dict(gate)
                out["eligible"] = True
                out["decision"] = "ALLOW"
                out["trust_breakout_override"] = True
                out["trust_breakout_version"] = "7.9.4"
                out["trust_breakout_prior_reasons"] = list(rescue["prior_reasons"])
                out["reasons"] = [
                    f"V7.9.4 consumed-{side} breakout rescue: Core {_f(c['score']):.1f}, "
                    f"Raw {_f(c['raw']):.0f}, OI {_f(c['oi']):.0f}/15, "
                    f"{rescue['strategy']}, cost {rescue['cost_r']:.2f}R, via {modes}"
                ]
                notes = list(out.get("notes") or [])
                notes.append(
                    f"V7.9.4 raw nearest {side} {_f(z.get('low')):.8g}-{_f(z.get('high')):.8g} "
                    f"confirmed consumed by CLOSED candles ({modes}); live_hold=True"
                )
                out["notes"] = notes
                live._diag(
                    f"V7.9.4 TRUST BREAKOUT LIVE ALLOW {c['symbol']} {c['direction']} "
                    f"Core={_f(c['score']):.1f} Raw={_f(c['raw']):.0f} OI={_f(c['oi']):.0f}/15 "
                    f"strategy={rescue['strategy']} cost={rescue['cost_r']:.2f}R "
                    f"consumed_{side}={_f(z.get('low')):.8g}-{_f(z.get('high')):.8g} via={modes}"
                )
                return out
            except Exception as exc:
                live._diag(f"V7.9.4 trust-breakout wrapper fail-closed: {type(exc).__name__}: {exc}")
                return gate

        main._v71_live_candidate_gate = gate_wrapper
        _PATCHED = True
        live.V70_VERSION = "7.9.4-trust-breakout"
        live._diag(
            f"V7.9.4 Trust Breakout armed mode={MODE} BREAKOUT only Core>={MIN_CORE:.0f} "
            f"Raw>={MIN_RAW:.0f} OI>={MIN_OI:.0f}/15 strategy={','.join(sorted(ALLOWED_STRATEGY))} "
            f"cost<={MAX_COST_R:.2f}R closed-candle consumed-S/R required; all non-S/R vetoes preserved"
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
            live._diag(f"V7.9.4 bootstrap retry: {type(exc).__name__}: {exc}")
        time.sleep(0.25)
    live._diag("V7.9.4 bootstrap gave up after 300s; Trust Breakout not patched")


if ENABLED:
    threading.Thread(target=_bootstrap, name="V794TrustBreakout", daemon=True).start()
    live._diag("V7.9.4 Trust Breakout bootstrap armed")
