"""V7.9.5 one-shot SPX retrace-entry watch.

Operator instruction: the 2026-09-24 SPX_USDT SHORT was live-ALLOWed but missed
only because live price had already moved 0.25R beyond the 0.20R chase limit.
Arm a temporary retrace watch at the original entry zone.

The watcher never bypasses the current live selector/risk/executor. It requires
a recent fresh scan to remain a strong SPX SHORT ENTRY before handing the setup
back through the normal live gate and execution path.
"""
import copy
import os
import sys
import threading
import time

import live_executor_v70 as live

ENABLED = os.getenv("V795_SPX_RETRACE_ENABLED", "true").lower() == "true"
SYMBOL = os.getenv("V795_SPX_RETRACE_SYMBOL", "SPX_USDT").upper()
DIRECTION = "SHORT"
ENTRY = float(os.getenv("V795_SPX_RETRACE_ENTRY", "0.4301"))
ENTRY_LOW = float(os.getenv("V795_SPX_RETRACE_LOW", "0.42996909"))
ENTRY_HIGH = float(os.getenv("V795_SPX_RETRACE_HIGH", "0.43049272"))
STOP = float(os.getenv("V795_SPX_RETRACE_STOP", "0.43959272"))
TP1 = float(os.getenv("V795_SPX_RETRACE_TP1", "0.41586091"))
TP2 = float(os.getenv("V795_SPX_RETRACE_TP2", "0.41111455"))
TP3 = float(os.getenv("V795_SPX_RETRACE_TP3", "0.40162183"))
EXPIRY_MINUTES = max(10.0, float(os.getenv("V795_SPX_RETRACE_EXPIRY_MINUTES", "120")))
POLL_SECONDS = max(3.0, float(os.getenv("V795_SPX_RETRACE_POLL_SECONDS", "5")))
MAX_SCAN_AGE = max(60.0, float(os.getenv("V795_SPX_RETRACE_MAX_SCAN_AGE", "240")))
MIN_CORE = max(75.0, float(os.getenv("V795_SPX_RETRACE_MIN_CORE", "85")))
MIN_RAW = max(65.0, float(os.getenv("V795_SPX_RETRACE_MIN_RAW", "72")))
MIN_OI = max(6.0, float(os.getenv("V795_SPX_RETRACE_MIN_OI", "10")))
ALLOWED_REGIMES = {"TREND_CONTINUATION", "BREAKOUT"}
ALLOWED_STRATEGY = {"CONFIRM", "STRONG_CONFIRM"}

_PATCHED = False
_PATCH_LOCK = threading.RLock()
_ARMED_AT = None
_LAST_SCAN_TS = 0.0
_FIRED = False
_LAST_STATUS_LOG = 0.0


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _norm(v):
    return str(v or "").strip().upper()


def _zone(px):
    return ENTRY_LOW <= float(px) <= ENTRY_HIGH


def _already_open():
    try:
        for p in live.positions(SYMBOL) or []:
            if _f(p.get("holdVol") or p.get("vol") or p.get("positionVol")) <= 0:
                continue
            side = int(p.get("positionType") or 0)
            # MEXC: 1=LONG, 2=SHORT on isolated/hedge position rows.
            if side == 2:
                return True
    except Exception:
        return False
    return False


def _fresh_result(main):
    if time.time() - _LAST_SCAN_TS > MAX_SCAN_AGE:
        return None, "latest full scan stale"
    for row in list(getattr(main, "V681_LAST_SCAN_RESULTS", []) or []):
        if str(row.get("symbol") or "").upper() == SYMBOL:
            return copy.deepcopy(row), ""
    return None, "SPX absent from latest full scan"


def _validate_fresh(result):
    if not isinstance(result, dict):
        return False, "no fresh result"
    if _norm(result.get("direction")) != DIRECTION:
        return False, f"fresh direction={_norm(result.get('direction')) or 'N/A'}"
    if _norm(result.get("signal_state")) != "ENTRY":
        return False, f"fresh state={_norm(result.get('signal_state')) or 'N/A'}"
    if _norm(result.get("regime")) not in ALLOWED_REGIMES:
        return False, f"fresh regime={_norm(result.get('regime')) or 'N/A'}"
    if _f(result.get("best_score")) < MIN_CORE:
        return False, f"Core {_f(result.get('best_score')):.1f} < {MIN_CORE:.1f}"
    if _f(result.get("raw_score")) < MIN_RAW:
        return False, f"Raw {_f(result.get('raw_score')):.0f} < {MIN_RAW:.0f}"
    if _f(result.get("oi_score")) < MIN_OI:
        return False, f"OI {_f(result.get('oi_score')):.0f}/15 < {MIN_OI:.0f}/15"
    return True, ""


def _original_geometry(result):
    out = copy.deepcopy(result)
    out["price"] = ENTRY
    out["risk_plan"] = {
        "entry": ENTRY,
        "entry_low": ENTRY_LOW,
        "entry_high": ENTRY_HIGH,
        "stop": STOP,
        "stop_pct": abs(STOP - ENTRY) / ENTRY * 100.0,
        "risk": abs(STOP - ENTRY),
        "tp1": TP1,
        "tp2": TP2,
        "tp3": TP3,
    }
    out["v795_retrace_instruction"] = True
    out["v795_original_entry"] = ENTRY
    return out


def _try_execute(main, px):
    global _FIRED
    result, why = _fresh_result(main)
    if result is None:
        live._diag(f"V7.9.5 SPX RETRACE touch px={px:.8g} but no execution: {why}")
        return False

    ok, why = _validate_fresh(result)
    if not ok:
        live._diag(f"V7.9.5 SPX RETRACE touch px={px:.8g} thesis not fresh enough: {why}")
        return False

    trades = []
    try:
        trades = main.load_json(main.TRADES_FILE, [])
    except Exception:
        pass

    result = _original_geometry(result)
    try:
        main._v71_attach_live_context(result, trades)
        strategy = _norm((result.get("strategy_ensemble") or {}).get("consensus"))
        if strategy not in ALLOWED_STRATEGY:
            live._diag(
                f"V7.9.5 SPX RETRACE touch px={px:.8g} strategy={strategy or 'NO_DATA'}; no order"
            )
            return False

        gate = main._v71_live_candidate_gate(result, trades)
        if not isinstance(gate, dict) or not gate.get("eligible"):
            live._diag(
                f"V7.9.5 SPX RETRACE touch px={px:.8g} live gate SKIP: "
                + "; ".join((gate or {}).get("reasons") or ["unknown gate failure"])
            )
            return False

        source_key = f"v795_spx_retrace_{int(_ARMED_AT or time.time())}"
        trade = main._v71_live_stub_trade(result, source_key)
        live._diag(
            f"V7.9.5 SPX RETRACE LIVE TRIGGER px={px:.8g} zone={ENTRY_LOW:.8g}-{ENTRY_HIGH:.8g} "
            f"Core={_f(result.get('best_score')):.1f} Raw={_f(result.get('raw_score')):.0f} "
            f"OI={_f(result.get('oi_score')):.0f}/15 strategy={strategy} selector={_f(gate.get('selector_score')):.2f}"
        )
        outcome = main._v71_execute_live_gate(result, trade, gate, bridge_mode=True)
        if isinstance(outcome, dict) and outcome.get("executed"):
            _FIRED = True
            live._msg(
                f"🎯 SPX RETRACE ENTRY FILLED\n{SYMBOL} SHORT\n"
                f"Original zone {ENTRY_LOW:.8g}–{ENTRY_HIGH:.8g} revisited. "
                f"Fresh thesis + normal live gates passed."
            )
            return True
        live._diag(
            f"V7.9.5 SPX RETRACE executor did not fill: "
            f"{(outcome or {}).get('reason') if isinstance(outcome, dict) else outcome}"
        )
    except Exception as exc:
        live._diag(f"V7.9.5 SPX RETRACE fail-closed: {type(exc).__name__}: {exc}")
    return False


def _watch(main):
    global _ARMED_AT, _LAST_STATUS_LOG
    _ARMED_AT = time.time()
    deadline = _ARMED_AT + EXPIRY_MINUTES * 60.0
    live._diag(
        f"V7.9.5 SPX RETRACE WATCH ARMED {SYMBOL} SHORT zone={ENTRY_LOW:.8g}-{ENTRY_HIGH:.8g} "
        f"entry={ENTRY:.8g} expiry={EXPIRY_MINUTES:.0f}m fresh_scan<={MAX_SCAN_AGE:.0f}s "
        f"min Core/Raw/OI={MIN_CORE:.0f}/{MIN_RAW:.0f}/{MIN_OI:.0f}; normal live gates preserved"
    )
    while ENABLED and not _FIRED and time.time() < deadline:
        try:
            if _already_open():
                live._diag("V7.9.5 SPX RETRACE WATCH cancelled: SPX SHORT already open")
                return
            px = live._fair_price(SYMBOL)
            if _zone(px):
                _try_execute(main, px)
                if _FIRED:
                    return
            elif time.time() - _LAST_STATUS_LOG >= 300:
                _LAST_STATUS_LOG = time.time()
                live._diag(
                    f"V7.9.5 SPX RETRACE waiting px={px:.8g} target={ENTRY_LOW:.8g}-{ENTRY_HIGH:.8g}"
                )
        except Exception as exc:
            if time.time() - _LAST_STATUS_LOG >= 120:
                _LAST_STATUS_LOG = time.time()
                live._diag(f"V7.9.5 SPX RETRACE watcher warning: {type(exc).__name__}: {exc}")
        time.sleep(POLL_SECONDS)
    if not _FIRED:
        live._diag("V7.9.5 SPX RETRACE WATCH EXPIRED without fill")


def _patch(main):
    global _PATCHED, _LAST_SCAN_TS
    with _PATCH_LOCK:
        if _PATCHED:
            return True
        required = (
            "save_scan_snapshot", "_v71_attach_live_context", "_v71_live_candidate_gate",
            "_v71_live_stub_trade", "_v71_execute_live_gate", "V681_LAST_SCAN_RESULTS",
        )
        if any(not hasattr(main, name) for name in required):
            return False

        original_save = main.save_scan_snapshot

        def save_scan_snapshot_wrapper(results):
            global _LAST_SCAN_TS
            out = original_save(results)
            _LAST_SCAN_TS = time.time()
            return out

        main.save_scan_snapshot = save_scan_snapshot_wrapper
        # Existing V681_LAST_SCAN_RESULTS was populated before this overlay on
        # some restarts. Give it a short initial freshness window; next scan
        # refreshes the timestamp properly.
        if getattr(main, "V681_LAST_SCAN_RESULTS", None):
            _LAST_SCAN_TS = time.time()

        threading.Thread(target=_watch, args=(main,), name="V795SPXRetrace", daemon=True).start()
        _PATCHED = True
        live._diag("V7.9.5 SPX retrace-entry integration armed")
        return True


def _bootstrap():
    deadline = time.time() + 300
    while time.time() < deadline:
        main = sys.modules.get("__main__")
        try:
            if main is not None and _patch(main):
                return
        except Exception as exc:
            live._diag(f"V7.9.5 bootstrap retry: {type(exc).__name__}: {exc}")
        time.sleep(0.25)
    live._diag("V7.9.5 bootstrap gave up; SPX retrace watch not armed")


if ENABLED:
    threading.Thread(target=_bootstrap, name="V795Bootstrap", daemon=True).start()
    live._diag("V7.9.5 SPX retrace-entry bootstrap armed")
