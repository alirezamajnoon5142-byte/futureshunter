"""FuturesHunter V7.9.6 Smart Retrace Recovery.

Generalizes the one-off SPX retrace behavior.

A Core setup is armed for one retrace only when:
- the normal live selector already allowed it,
- execute_signal rejected it ONLY because live price was beyond the chase limit,
- the original setup was high quality.

On a return to the original entry zone the setup is revalidated from the
latest full scan and must pass the complete CURRENT live gate again before
execution. No risk, correlation, cooldown, S/R, macro, loss-cluster, breaker,
or exchange protection rule is bypassed.
"""
import copy
import json
import os
import re
import sys
import threading
import time

import live_executor_v70 as live

ENABLED = os.getenv("V796_SMART_RETRACE_ENABLED", "true").lower() == "true"
EXPIRY_MINUTES = max(10.0, float(os.getenv("V796_RETRACE_EXPIRY_MINUTES", "120")))
POLL_SECONDS = max(3.0, float(os.getenv("V796_RETRACE_POLL_SECONDS", "5")))
MAX_SCAN_AGE = max(60.0, float(os.getenv("V796_RETRACE_MAX_SCAN_AGE", "240")))
MAX_WATCHES = max(1, min(10, int(os.getenv("V796_RETRACE_MAX_WATCHES", "5"))))
MIN_CORE = max(75.0, float(os.getenv("V796_RETRACE_MIN_CORE", "85")))
MIN_RAW = max(65.0, float(os.getenv("V796_RETRACE_MIN_RAW", "72")))
MIN_OI = max(6.0, float(os.getenv("V796_RETRACE_MIN_OI", "10")))
MAX_COST_R = min(0.25, max(0.01, float(os.getenv("V796_RETRACE_MAX_COST_R", "0.15"))))
ALLOWED_REGIMES = {
    x.strip().upper()
    for x in os.getenv("V796_RETRACE_REGIMES", "BREAKOUT,TREND_CONTINUATION").split(",")
    if x.strip()
}
ALLOWED_STRATEGY = {
    x.strip().upper()
    for x in os.getenv("V796_RETRACE_STRATEGY", "CONFIRM,STRONG_CONFIRM").split(",")
    if x.strip()
}
STATE_KEY = "v796_smart_retrace_watches"

_LOCK = threading.RLock()
_WATCHES = {}
_PATCHED = False
_LAST_SCAN_TS = 0.0
_MAIN = None
_ORIGINAL_EXECUTE = None
_LAST_WAIT_LOG = {}


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _norm(v):
    return str(v or "").strip().upper()


def _key(symbol, direction):
    return f"{_norm(symbol)}|{_norm(direction)}"


def _json_safe(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, dict):
        return {str(k): _json_safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
    try:
        return float(v)
    except Exception:
        return str(v)


def _persist():
    try:
        with _LOCK:
            payload = {"version": "7.9.6", "watches": list(_WATCHES.values()), "ts": time.time()}
        live._state_set(STATE_KEY, _json_safe(payload))
    except Exception as exc:
        live._diag(f"V7.9.6 retrace persistence warning: {type(exc).__name__}: {exc}")


def _restore():
    try:
        state = live._state_get(STATE_KEY) or {}
        now = time.time()
        restored = 0
        with _LOCK:
            for w in state.get("watches") or []:
                if not isinstance(w, dict):
                    continue
                if _f(w.get("expires_at")) <= now:
                    continue
                symbol = _norm(w.get("symbol"))
                direction = _norm(w.get("direction"))
                if not symbol or direction not in {"LONG", "SHORT"}:
                    continue
                _WATCHES[_key(symbol, direction)] = w
                restored += 1
        if restored:
            live._diag(f"V7.9.6 SMART RETRACE restored {restored} active watch(es)")
    except Exception as exc:
        live._diag(f"V7.9.6 retrace restore warning: {type(exc).__name__}: {exc}")


def _entry_zone(result):
    plan = result.get("risk_plan") or {}
    entry = _f(result.get("price") or plan.get("entry"))
    low = _f(plan.get("entry_low"), entry)
    high = _f(plan.get("entry_high"), entry)
    if low <= 0 or high <= 0:
        low = high = entry
    if low > high:
        low, high = high, low
    return entry, low, high


def _quality(result):
    gate = result.get("v70_live_gate") or {}
    strategy = _norm(
        gate.get("strategy_consensus")
        or (result.get("strategy_ensemble") or {}).get("consensus")
    )
    cost_r = _f(gate.get("estimated_cost_r"), 999.0)
    return {
        "state": _norm(result.get("signal_state")),
        "regime": _norm(result.get("regime")),
        "core": _f(result.get("best_score")),
        "raw": _f(result.get("raw_score")),
        "oi": _f(result.get("oi_score")),
        "strategy": strategy,
        "cost_r": cost_r,
        "gate_eligible": bool(gate.get("eligible")),
    }


def _can_arm(result):
    if not isinstance(result, dict):
        return False, "invalid result"
    if _norm(result.get("live_strategy_tag") or "CORE") != "CORE":
        return False, "non-Core strategy"
    q = _quality(result)
    if not q["gate_eligible"]:
        return False, "live selector was not ALLOW"
    if q["state"] != "ENTRY":
        return False, f"state={q['state'] or 'N/A'}"
    if q["regime"] not in ALLOWED_REGIMES:
        return False, f"regime={q['regime'] or 'N/A'}"
    if q["core"] < MIN_CORE:
        return False, f"Core {q['core']:.1f} < {MIN_CORE:.1f}"
    if q["raw"] < MIN_RAW:
        return False, f"Raw {q['raw']:.0f} < {MIN_RAW:.0f}"
    if q["oi"] < MIN_OI:
        return False, f"OI {q['oi']:.0f}/15 < {MIN_OI:.0f}/15"
    if q["strategy"] not in ALLOWED_STRATEGY:
        return False, f"strategy={q['strategy'] or 'NO_DATA'}"
    if q["cost_r"] > MAX_COST_R:
        return False, f"cost {q['cost_r']:.2f}R > {MAX_COST_R:.2f}R"
    plan = result.get("risk_plan") or {}
    if min(_f(plan.get("stop")), _f(plan.get("tp1")), _f(plan.get("tp2")), _f(plan.get("tp3"))) <= 0:
        return False, "invalid original geometry"
    return True, ""


def _arm(result, reason):
    ok, why = _can_arm(result)
    if not ok:
        live._diag(
            f"V7.9.6 SMART RETRACE not armed {result.get('symbol')} {result.get('direction')}: {why}"
        )
        return False

    symbol = _norm(result.get("symbol"))
    direction = _norm(result.get("direction"))
    k = _key(symbol, direction)
    q = _quality(result)
    entry, low, high = _entry_zone(result)
    plan = result.get("risk_plan") or {}
    now = time.time()

    m = re.search(r"entry chase\s+([0-9.]+)R", str(reason or ""), re.I)
    missed_drift_r = _f(m.group(1)) if m else 0.0

    watch = {
        "key": k,
        "symbol": symbol,
        "direction": direction,
        "armed_at": now,
        "expires_at": now + EXPIRY_MINUTES * 60.0,
        "entry": entry,
        "entry_low": low,
        "entry_high": high,
        "stop": _f(plan.get("stop")),
        "tp1": _f(plan.get("tp1")),
        "tp2": _f(plan.get("tp2")),
        "tp3": _f(plan.get("tp3")),
        "risk": abs(_f(plan.get("stop")) - entry),
        "stop_pct": abs(_f(plan.get("stop")) - entry) / max(entry, 1e-12) * 100.0,
        "original_core": q["core"],
        "original_raw": q["raw"],
        "original_oi": q["oi"],
        "original_regime": q["regime"],
        "original_strategy": q["strategy"],
        "original_cost_r": q["cost_r"],
        "missed_drift_r": missed_drift_r,
        "source_reason": str(reason or "")[:240],
    }

    with _LOCK:
        existing = _WATCHES.get(k)
        if existing and _f(existing.get("expires_at")) > now:
            # Keep the stronger/newer definition only. Do not stack watches.
            old_quality = (
                _f(existing.get("original_core"))
                + 0.25 * _f(existing.get("original_raw"))
                + _f(existing.get("original_oi"))
            )
            new_quality = q["core"] + 0.25 * q["raw"] + q["oi"]
            if new_quality <= old_quality + 1.0:
                live._diag(
                    f"V7.9.6 SMART RETRACE duplicate suppressed {symbol} {direction}; "
                    f"existing zone={_f(existing.get('entry_low')):.8g}-{_f(existing.get('entry_high')):.8g}"
                )
                return False
        elif len(_WATCHES) >= MAX_WATCHES:
            # Evict the earliest-expiring watch, never exceed the configured cap.
            victim_key = min(_WATCHES, key=lambda x: _f(_WATCHES[x].get("expires_at")))
            victim = _WATCHES.pop(victim_key)
            live._diag(
                f"V7.9.6 SMART RETRACE cap={MAX_WATCHES}; evicted "
                f"{victim.get('symbol')} {victim.get('direction')}"
            )
        _WATCHES[k] = watch

    _persist()
    live._diag(
        f"V7.9.6 SMART RETRACE ARMED {symbol} {direction} "
        f"zone={low:.8g}-{high:.8g} ref={entry:.8g} expiry={EXPIRY_MINUTES:.0f}m "
        f"missed={missed_drift_r:.2f}R Core/Raw/OI={q['core']:.1f}/{q['raw']:.0f}/{q['oi']:.0f} "
        f"strategy={q['strategy']} cost={q['cost_r']:.2f}R active={len(_WATCHES)}/{MAX_WATCHES}"
    )
    try:
        live._msg(
            f"🪃 SMART RETRACE ARMED\n{symbol} {direction}\n"
            f"Missed only on chase ({missed_drift_r:.2f}R). "
            f"Watching original zone {low:.8g}–{high:.8g} for up to {EXPIRY_MINUTES:.0f}m. "
            f"Fresh thesis + full live gate required."
        )
    except Exception:
        pass
    return True


def _remove(k, why):
    with _LOCK:
        w = _WATCHES.pop(k, None)
    if w:
        _persist()
        live._diag(
            f"V7.9.6 SMART RETRACE REMOVED {w.get('symbol')} {w.get('direction')}: {why}"
        )


def _fresh_result(symbol):
    main = _MAIN
    if main is None:
        return None, "main unavailable"
    if time.time() - _LAST_SCAN_TS > MAX_SCAN_AGE:
        return None, "latest full scan stale"
    for row in list(getattr(main, "V681_LAST_SCAN_RESULTS", []) or []):
        if _norm(row.get("symbol")) == symbol:
            return copy.deepcopy(row), ""
    return None, "symbol absent from latest full scan"


def _fresh_valid(result, watch):
    if not isinstance(result, dict):
        return False, "no fresh result"
    if _norm(result.get("direction")) != watch["direction"]:
        return False, f"direction flipped to {_norm(result.get('direction')) or 'N/A'}"
    if _norm(result.get("signal_state")) != "ENTRY":
        return False, f"state fell to {_norm(result.get('signal_state')) or 'N/A'}"
    if _norm(result.get("regime")) not in ALLOWED_REGIMES:
        return False, f"regime changed to {_norm(result.get('regime')) or 'N/A'}"
    if _f(result.get("best_score")) < MIN_CORE:
        return False, f"Core fell to {_f(result.get('best_score')):.1f}"
    if _f(result.get("raw_score")) < MIN_RAW:
        return False, f"Raw fell to {_f(result.get('raw_score')):.0f}"
    if _f(result.get("oi_score")) < MIN_OI:
        return False, f"OI fell to {_f(result.get('oi_score')):.0f}/15"
    return True, ""


def _restore_geometry(result, watch):
    out = copy.deepcopy(result)
    out["price"] = watch["entry"]
    out["risk_plan"] = {
        "entry": watch["entry"],
        "entry_low": watch["entry_low"],
        "entry_high": watch["entry_high"],
        "stop": watch["stop"],
        "stop_pct": watch["stop_pct"],
        "risk": watch["risk"],
        "tp1": watch["tp1"],
        "tp2": watch["tp2"],
        "tp3": watch["tp3"],
    }
    out["live_strategy_tag"] = "CORE"
    out["v796_smart_retrace"] = True
    out["v796_retrace_armed_at"] = watch["armed_at"]
    # Force current live-context regeneration rather than reusing stale
    # risk/strategy metadata from an earlier selected-best pass.
    out.pop("risk_challenger", None)
    out.pop("live_risk_challenger", None)
    out.pop("strategy_ensemble", None)
    out.pop("v70_live_gate", None)
    return out


def _has_open_symbol(symbol):
    try:
        return any(
            _f(p.get("holdVol") or p.get("vol") or p.get("positionVol")) > 0
            for p in (live.positions(symbol) or [])
        )
    except Exception:
        # Fail closed: if exchange state is unreadable, don't trigger.
        return True


def _trigger(k, watch, px):
    main = _MAIN
    result, why = _fresh_result(watch["symbol"])
    if result is None:
        live._diag(
            f"V7.9.6 SMART RETRACE touch {watch['symbol']} {watch['direction']} px={px:.8g}: {why}"
        )
        return False

    ok, why = _fresh_valid(result, watch)
    if not ok:
        _remove(k, f"thesis invalid on retrace: {why}")
        return False

    trades = []
    try:
        trades = main.load_json(main.TRADES_FILE, [])
    except Exception:
        pass

    result = _restore_geometry(result, watch)
    try:
        main._v71_attach_live_context(result, trades)
        strategy = _norm((result.get("strategy_ensemble") or {}).get("consensus"))
        if strategy not in ALLOWED_STRATEGY:
            _remove(k, f"fresh strategy={strategy or 'NO_DATA'}")
            return False

        gate = main._v71_live_candidate_gate(result, trades)
        if not isinstance(gate, dict) or not gate.get("eligible"):
            # A transient portfolio/correlation/cooldown block should not place
            # the order, but the watch can remain alive until expiry. The next
            # poll will re-evaluate if price is still in the zone.
            live._diag(
                f"V7.9.6 SMART RETRACE touch {watch['symbol']} {watch['direction']} "
                f"live gate SKIP: " + "; ".join((gate or {}).get("reasons") or ["unknown"])
            )
            return False

        current_cost = _f(gate.get("estimated_cost_r"), 999.0)
        if current_cost > MAX_COST_R:
            live._diag(
                f"V7.9.6 SMART RETRACE touch {watch['symbol']} cost={current_cost:.2f}R "
                f"> {MAX_COST_R:.2f}R; waiting"
            )
            return False

        source_key = (
            f"v796_retrace_{watch['symbol']}_{watch['direction']}_"
            f"{int(_f(watch.get('armed_at')))}"
        )
        trade = main._v71_live_stub_trade(result, source_key)
        live._diag(
            f"V7.9.6 SMART RETRACE LIVE TRIGGER {watch['symbol']} {watch['direction']} "
            f"px={px:.8g} zone={watch['entry_low']:.8g}-{watch['entry_high']:.8g} "
            f"Core/Raw/OI={_f(result.get('best_score')):.1f}/{_f(result.get('raw_score')):.0f}/"
            f"{_f(result.get('oi_score')):.0f} strategy={strategy} selector={_f(gate.get('selector_score')):.2f}"
        )

        # Use the captured pre-V7.9.6 executor to avoid re-arming this same
        # watch recursively if the price is fractionally outside drift limits.
        outcome = _ORIGINAL_EXECUTE(result, trade)
        if isinstance(outcome, dict) and outcome.get("executed"):
            _remove(k, "filled")
            try:
                live._msg(
                    f"🎯 SMART RETRACE FILLED\n{watch['symbol']} {watch['direction']}\n"
                    f"Original entry zone revisited and fresh live thesis passed."
                )
            except Exception:
                pass
            return True

        reason = (outcome or {}).get("reason") if isinstance(outcome, dict) else str(outcome)
        live._diag(
            f"V7.9.6 SMART RETRACE executor no-fill {watch['symbol']}: {reason}"
        )
        # Keep watching on another tiny drift miss; hard executor failures are
        # left to normal fail-closed protections and expiry.
        return False
    except Exception as exc:
        live._diag(
            f"V7.9.6 SMART RETRACE fail-closed {watch['symbol']}: {type(exc).__name__}: {exc}"
        )
        return False


def _watch_loop():
    global _LAST_WAIT_LOG
    while ENABLED:
        now = time.time()
        with _LOCK:
            items = [(k, dict(v)) for k, v in _WATCHES.items()]
        for k, w in items:
            try:
                if now >= _f(w.get("expires_at")):
                    _remove(k, "expired")
                    continue
                if _has_open_symbol(w["symbol"]):
                    _remove(k, "symbol already has an exchange position")
                    continue
                px = live._fair_price(w["symbol"])
                low, high = _f(w.get("entry_low")), _f(w.get("entry_high"))
                if low <= px <= high:
                    _trigger(k, w, px)
                else:
                    last = _LAST_WAIT_LOG.get(k, 0.0)
                    if now - last >= 300:
                        _LAST_WAIT_LOG[k] = now
                        live._diag(
                            f"V7.9.6 SMART RETRACE waiting {w['symbol']} {w['direction']} "
                            f"px={px:.8g} target={low:.8g}-{high:.8g}"
                        )
            except Exception as exc:
                live._diag(
                    f"V7.9.6 SMART RETRACE watcher warning {w.get('symbol')}: "
                    f"{type(exc).__name__}: {exc}"
                )
        time.sleep(POLL_SECONDS)


def _patch(main):
    global _PATCHED, _MAIN, _ORIGINAL_EXECUTE, _LAST_SCAN_TS
    with _LOCK:
        if _PATCHED:
            return True
        required = (
            "save_scan_snapshot", "_v71_attach_live_context", "_v71_live_candidate_gate",
            "_v71_live_stub_trade", "V681_LAST_SCAN_RESULTS",
        )
        if any(not hasattr(main, x) for x in required):
            return False

        _MAIN = main
        _ORIGINAL_EXECUTE = live.execute_signal
        original_save = main.save_scan_snapshot

        def save_scan_snapshot_wrapper(results):
            global _LAST_SCAN_TS
            out = original_save(results)
            _LAST_SCAN_TS = time.time()
            return out

        def execute_signal_wrapper(result, paper_trade=None):
            outcome = _ORIGINAL_EXECUTE(result, paper_trade)
            try:
                reason = (
                    outcome.get("reason")
                    if isinstance(outcome, dict)
                    else ""
                )
                if (
                    isinstance(outcome, dict)
                    and not outcome.get("executed")
                    and str(reason or "").lower().startswith("entry chase ")
                ):
                    _arm(result, reason)
            except Exception as exc:
                live._diag(
                    f"V7.9.6 retrace arm wrapper warning: {type(exc).__name__}: {exc}"
                )
            return outcome

        main.save_scan_snapshot = save_scan_snapshot_wrapper
        live.execute_signal = execute_signal_wrapper

        if getattr(main, "V681_LAST_SCAN_RESULTS", None):
            _LAST_SCAN_TS = time.time()

        _restore()
        threading.Thread(target=_watch_loop, name="V796SmartRetrace", daemon=True).start()
        _PATCHED = True
        live.V70_VERSION = "7.9.6-smart-retrace"
        live._diag(
            f"V7.9.6 SMART RETRACE armed Core-only chase-miss recovery "
            f"Core>={MIN_CORE:.0f} Raw>={MIN_RAW:.0f} OI>={MIN_OI:.0f}/15 "
            f"strategy={','.join(sorted(ALLOWED_STRATEGY))} cost<={MAX_COST_R:.2f}R "
            f"regimes={','.join(sorted(ALLOWED_REGIMES))} expiry={EXPIRY_MINUTES:.0f}m "
            f"max_watches={MAX_WATCHES}; full live gate revalidation required"
        )
        return True


def _bootstrap():
    deadline = time.time() + 300
    while time.time() < deadline:
        main = sys.modules.get("__main__")
        try:
            if main is not None and _patch(main):
                return
        except Exception as exc:
            live._diag(f"V7.9.6 bootstrap retry: {type(exc).__name__}: {exc}")
        time.sleep(0.25)
    live._diag("V7.9.6 bootstrap gave up; Smart Retrace not armed")


if ENABLED:
    threading.Thread(target=_bootstrap, name="V796Bootstrap", daemon=True).start()
    live._diag("V7.9.6 Smart Retrace bootstrap armed")
