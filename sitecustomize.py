"""FuturesHunter V7.8.4 capital-efficiency runtime overlay.

Loaded through PYTHONPATH=. before FuturesHunter_Render.py imports live_executor_v70.
The overlay is deliberately future-entry focused: it does not alter existing
exchange protection, current position size, stored TP/SL prices, or risk pct.
"""
import builtins
import os
import sys

_ORIGINAL_IMPORT = builtins.__import__
_PATCHED = False

COMPOUND_NOTIONAL = os.getenv("V784_COMPOUND_NOTIONAL", "true").lower() == "true"
TINY_TWO_SLICE = os.getenv("V784_TINY_TWO_SLICE", "true").lower() == "true"
ELITE_CHASE = os.getenv("V784_ELITE_CHASE", "true").lower() == "true"
ELITE_MIN_SELECTOR = float(os.getenv("V784_ELITE_MIN_SELECTOR", "100"))
ELITE_MAX_COST_R = float(os.getenv("V784_ELITE_MAX_COST_R", "0.15"))
ELITE_MIN_CORE = float(os.getenv("V784_ELITE_MIN_CORE", "80"))
ELITE_MAX_DRIFT_R = min(0.25, max(0.20, float(os.getenv("V784_ELITE_MAX_DRIFT_R", "0.25"))))


class _TruthyZero(float):
    """Numerically zero, but truthy so legacy manager doesn't backfill TP2."""
    def __new__(cls):
        return float.__new__(cls, 0.0)

    def __bool__(self):
        return True


def _f(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _is_tiny_two_slice_row(mod, row):
    if not TINY_TWO_SLICE or row is None or len(row) <= 16:
        return False
    try:
        contracts = _f(row[4])
        tp1_vol = _f(row[15])
        tp2_vol = _f(row[16])
        if contracts <= 0 or tp1_vol <= 0 or abs(tp2_vol) > 1e-12:
            return False
        symbol = str(row[1] or "")
        c = mod.contract(symbol)
        step = max(_f(c.get("volUnit"), 1.0), 1e-12)
        min_vol = max(_f(c.get("minVol"), step), step)
        return contracts + 1e-9 >= 2.0 * min_vol and contracts < 3.0 * min_vol - 1e-9
    except Exception:
        return False


def _elite_candidate(result):
    if not ELITE_CHASE or not isinstance(result, dict):
        return False, {}
    gate = result.get("v70_live_gate") or {}
    strategy = result.get("strategy_ensemble") or {}
    sr = result.get("support_resistance") or gate.get("support_resistance") or {}
    selector = _f(gate.get("selector_score"))
    cost_r = _f(gate.get("estimated_cost_r"), 99.0)
    core = _f(result.get("best_score"))
    consensus = str(strategy.get("consensus") or "").upper()
    room_r = _f(sr.get("nearest_room_r"), 99.0)
    breakout = bool(sr.get("breakout_confirmed_zones"))
    structure_clear = room_r >= 1.25 or breakout
    allowed_consensus = consensus in {"CONFIRM", "STRONG_CONFIRM"}
    ok = (
        bool(gate.get("eligible"))
        and selector >= ELITE_MIN_SELECTOR
        and cost_r <= ELITE_MAX_COST_R
        and core >= ELITE_MIN_CORE
        and allowed_consensus
        and structure_clear
    )
    return ok, {
        "selector": selector,
        "cost_r": cost_r,
        "core": core,
        "consensus": consensus,
        "room_r": room_r,
        "breakout": breakout,
    }


def _apply(mod):
    global _PATCHED
    if _PATCHED or getattr(mod, "_V784_CAPITAL_EFFICIENCY_PATCHED", False):
        return
    required = ["_risk_limits", "_three_way_split", "_partial_market_close", "_manage_open_trade", "_adaptive_derisk", "execute_signal"]
    if any(not hasattr(mod, name) for name in required):
        return

    original_risk_limits = mod._risk_limits
    original_split = mod._three_way_split
    original_partial_close = mod._partial_market_close
    original_manage_open_trade = mod._manage_open_trade
    original_adaptive = mod._adaptive_derisk
    original_execute = mod.execute_signal
    original_diagnostic = mod.diagnostic_state

    def risk_limits(equity):
        risk_usdt, max_notional, equity_kill = original_risk_limits(equity)
        eq = max(0.0, _f(equity))
        if COMPOUND_NOTIONAL and eq > 0:
            # Still capped at 1.0x current equity; this increases capacity only
            # as realized equity grows. Risk per trade stays controlled by RISK_PCT.
            max_notional = eq
        return risk_usdt, max_notional, equity_kill

    def three_way_split(contracts, step, min_vol):
        normal = original_split(contracts, step, min_vol)
        if normal or not TINY_TWO_SLICE:
            return normal
        c = _f(contracts)
        step_f = max(_f(step), 1e-12)
        min_f = max(_f(min_vol, step_f), step_f)
        if c + 1e-9 < 2.0 * min_f:
            return None
        first = mod._floor_step(c * 0.5, step_f)
        if first < min_f:
            first = min_f
        runner = mod._floor_step(c - first, step_f)
        if runner < min_f:
            first = mod._floor_step(c - min_f, step_f)
            runner = mod._floor_step(c - first, step_f)
        if first >= min_f and runner >= min_f:
            mod._diag(
                f"CAPITAL EFFICIENCY TWO_SLICE contracts={c:g} "
                f"tp1={first:g} tp2=0 runner={runner:g}"
            )
            return first, 0.0, runner
        return None

    def partial_market_close(symbol, direction, position_id, close_vol, signal_id, stage):
        # Two-slice fallback has no TP2 size reduction. TP2 is a protection
        # milestone only: the existing manager will ratchet the runner to +1R.
        if TINY_TWO_SLICE and str(stage or "").upper() == "P2" and abs(_f(close_vol)) <= 1e-12:
            mod._diag(f"CAPITAL EFFICIENCY TWO_SLICE TP2 {symbol}: no size close; ratchet-only milestone")
            return True, "two-slice TP2 ratchet-only"
        return original_partial_close(symbol, direction, position_id, close_vol, signal_id, stage)

    def adaptive_derisk(row, targets, px, tp1_vol):
        # On a two-slice micro-position TP1 is ~50%, so do not let the +0.75R
        # adaptive bank prematurely sell half. Stage 1 risk compression remains;
        # normal TP1 and later positive-R locks still work unchanged.
        if _is_tiny_two_slice_row(mod, row) and not bool(row[17]):
            old_bank = mod.ADAPTIVE_BANK_MFE_R
            try:
                mod.ADAPTIVE_BANK_MFE_R = 999.0
                return original_adaptive(row, targets, px, tp1_vol)
            finally:
                mod.ADAPTIVE_BANK_MFE_R = old_bank
        return original_adaptive(row, targets, px, tp1_vol)

    def manage_open_trade(row, exchange_pos):
        # Persist tp2_vol=0 for honest accounting, but pass a truthy numeric zero
        # to the legacy manager so it does not mistake intentional two-slice mode
        # for an uninitialized split after every restart/reconcile pass.
        if _is_tiny_two_slice_row(mod, row):
            proxy = list(row)
            proxy[16] = _TruthyZero()
            return original_manage_open_trade(tuple(proxy), exchange_pos)
        return original_manage_open_trade(row, exchange_pos)

    def execute_signal(result, paper_trade=None):
        elite, meta = _elite_candidate(result)
        if not elite:
            return original_execute(result, paper_trade)
        # The executor already uses an RLock. Hold it while temporarily raising
        # the drift ceiling so both pre-submit and post-fill guards use the same
        # elite limit, then restore the baseline immediately.
        with mod._lock:
            old_limit = mod.MAX_ENTRY_DRIFT_R
            try:
                mod.MAX_ENTRY_DRIFT_R = max(old_limit, ELITE_MAX_DRIFT_R)
                mod._diag(
                    "CAPITAL EFFICIENCY ELITE CHASE "
                    f"selector={meta['selector']:.1f} core={meta['core']:.1f} "
                    f"strategy={meta['consensus']} cost={meta['cost_r']:.2f}R "
                    f"room={meta['room_r']:.2f}R limit={mod.MAX_ENTRY_DRIFT_R:.2f}R"
                )
                return original_execute(result, paper_trade)
            finally:
                mod.MAX_ENTRY_DRIFT_R = old_limit

    def diagnostic_state():
        state = original_diagnostic()
        state.update({
            "version": "7.8.4-capital-efficiency-overlay",
            "capital_efficiency_overlay": True,
            "compound_notional": COMPOUND_NOTIONAL,
            "tiny_two_slice": TINY_TWO_SLICE,
            "elite_chase": ELITE_CHASE,
            "elite_max_drift_r": ELITE_MAX_DRIFT_R,
            "baseline_max_entry_drift_r": mod.MAX_ENTRY_DRIFT_R,
        })
        return state

    mod._risk_limits = risk_limits
    mod._three_way_split = three_way_split
    mod._partial_market_close = partial_market_close
    mod._adaptive_derisk = adaptive_derisk
    mod._manage_open_trade = manage_open_trade
    mod.execute_signal = execute_signal
    mod.diagnostic_state = diagnostic_state
    mod.V70_VERSION = "7.8.4-capital-efficiency-overlay"
    mod._V784_CAPITAL_EFFICIENCY_PATCHED = True
    _PATCHED = True
    try:
        mod._diag(
            "V7.8.4 capital-efficiency overlay active: "
            "1x-equity compounding cap, tiny 50/runner fallback, elite 0.25R chase"
        )
    except Exception:
        pass


def _import(name, globals=None, locals=None, fromlist=(), level=0):
    module = _ORIGINAL_IMPORT(name, globals, locals, fromlist, level)
    if name == "live_executor_v70" or name.endswith(".live_executor_v70"):
        target = sys.modules.get("live_executor_v70")
        if target is not None:
            _apply(target)
            # Narrow hook: once the target is patched, restore normal imports.
            builtins.__import__ = _ORIGINAL_IMPORT
    return module


builtins.__import__ = _import
print("[V7DIAG] V7.8.4 capital-efficiency overlay armed", flush=True)
