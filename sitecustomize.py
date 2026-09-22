"""FuturesHunter V7.8.5 capital-efficiency + fake-breakout runtime overlay.

Loaded through PYTHONPATH=. before FuturesHunter_Render.py imports live_executor_v70.
The overlay preserves V7.8.4 capital-efficiency behavior and adds a conservative
breakout-integrity assessment for future live entries. It does not alter any
existing exchange position, stop, TP, size, or realized trade state.
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

# V7.8.5 fake-breakout guard. Default is SHADOW so a merge alone cannot silently
# change live entry behavior. Promote with V785_FAKE_BREAKOUT_MODE=block only
# after reviewing shadow classifications against research outcomes.
FAKE_BREAKOUT_GUARD = os.getenv("V785_FAKE_BREAKOUT_GUARD", "true").lower() == "true"
FAKE_BREAKOUT_MODE = os.getenv("V785_FAKE_BREAKOUT_MODE", "shadow").strip().lower()
FAKE_BREAKOUT_BLOCK_SCORE = max(3, int(os.getenv("V785_FAKE_BREAKOUT_BLOCK_SCORE", "5")))
FAKE_BREAKOUT_MIN_DEPTH_ATR = max(0.02, float(os.getenv("V785_FAKE_BREAKOUT_MIN_DEPTH_ATR", "0.10")))
FAKE_BREAKOUT_STRONG_DEPTH_ATR = max(
    FAKE_BREAKOUT_MIN_DEPTH_ATR,
    float(os.getenv("V785_FAKE_BREAKOUT_STRONG_DEPTH_ATR", "0.18")),
)
FAKE_BREAKOUT_MIN_RAW = float(os.getenv("V785_FAKE_BREAKOUT_MIN_RAW", "64"))
FAKE_BREAKOUT_MIN_MARGIN = float(os.getenv("V785_FAKE_BREAKOUT_MIN_MARGIN", "4.0"))
FAKE_BREAKOUT_MIN_OI = float(os.getenv("V785_FAKE_BREAKOUT_MIN_OI", "10"))
FAKE_BREAKOUT_MIN_RV = float(os.getenv("V785_FAKE_BREAKOUT_MIN_RV", "0.90"))


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


def _breakout_depth(tf, direction):
    """Return ATR penetration beyond the previous 20-bar boundary, or None."""
    if not isinstance(tf, dict):
        return None
    atr = _f(tf.get("atr"))
    close = _f(tf.get("close"))
    if atr <= 0 or close <= 0:
        return None
    if direction == "LONG":
        level = _f(tf.get("high20_prev"))
        if level > 0 and close > level:
            return (close - level) / atr
    elif direction == "SHORT":
        level = _f(tf.get("low20_prev"))
        if level > 0 and close < level:
            return (level - close) / atr
    return None


def _fake_breakout_assessment(result):
    """Classify fragile BREAKOUT entries without pretending to predict certainty.

    A high risk score means several independent signs say the break is easy to
    reclaim: shallow penetration, weak close, one-timeframe-only confirmation,
    thin raw-score margin, weak participation, or non-confirming strategy/OI.
    """
    meta = {
        "active": False,
        "risk_score": 0,
        "risk_level": "NONE",
        "reasons": [],
        "block": False,
    }
    if not FAKE_BREAKOUT_GUARD or not isinstance(result, dict):
        return meta
    if str(result.get("regime") or "").upper() != "BREAKOUT":
        return meta
    direction = str(result.get("direction") or "").upper()
    if direction not in {"LONG", "SHORT"}:
        return meta

    meta["active"] = True
    tf5 = result.get("5m") or {}
    tf15 = result.get("15m") or {}
    depths = []
    direct = []
    for label, tf in (("5m", tf5), ("15m", tf15)):
        depth = _breakout_depth(tf, direction)
        if depth is not None:
            depths.append(depth)
            direct.append(label)

    max_depth = max(depths) if depths else 0.0
    meta["direct_timeframes"] = direct
    meta["max_depth_atr"] = round(max_depth, 4)

    risk = 0
    reasons = []

    if not direct:
        risk += 3
        reasons.append("BREAKOUT regime but neither 5m nor 15m close is beyond the 20-bar boundary")
    elif len(direct) == 1:
        risk += 1
        reasons.append(f"break confirmed on only one timeframe ({direct[0]})")

    if max_depth < FAKE_BREAKOUT_MIN_DEPTH_ATR:
        risk += 2
        reasons.append(f"shallow penetration {max_depth:.2f} ATR < {FAKE_BREAKOUT_MIN_DEPTH_ATR:.2f}")
    elif max_depth < FAKE_BREAKOUT_STRONG_DEPTH_ATR:
        risk += 1
        reasons.append(f"modest penetration {max_depth:.2f} ATR")

    cl5 = _f(tf5.get("close_location"), 0.5)
    cl15 = _f(tf15.get("close_location"), 0.5)
    if direction == "LONG":
        decisive5 = cl5 >= 0.60
        decisive15 = cl15 >= 0.60
    else:
        decisive5 = cl5 <= 0.40
        decisive15 = cl15 <= 0.40
    if not decisive5 and not decisive15:
        risk += 2
        reasons.append(f"weak directional close locations 5m={cl5:.2f}, 15m={cl15:.2f}")
    elif not (decisive5 and decisive15):
        risk += 1
        reasons.append("only one timeframe closed decisively in breakout direction")

    rv5 = _f(tf5.get("rv"))
    rv15 = _f(tf15.get("rv"))
    vol_trend = _f(tf15.get("volume_trend"))
    if max(rv5, rv15) < FAKE_BREAKOUT_MIN_RV and vol_trend < 5:
        risk += 1
        reasons.append(f"weak participation rv5={rv5:.2f} rv15={rv15:.2f} volume_trend={vol_trend:.1f}")

    raw = _f(result.get("raw_score"))
    if raw < FAKE_BREAKOUT_MIN_RAW:
        risk += 1
        reasons.append(f"raw factor score {raw:.0f} < {FAKE_BREAKOUT_MIN_RAW:.0f}")

    best = _f(result.get("best_score"))
    threshold = _f(result.get("entry_threshold"), 68.0)
    margin = best - threshold
    meta["entry_margin"] = round(margin, 2)
    if margin < FAKE_BREAKOUT_MIN_MARGIN:
        risk += 1
        reasons.append(f"thin ENTRY margin {margin:.1f} < {FAKE_BREAKOUT_MIN_MARGIN:.1f}")

    oi = _f(result.get("oi_score"))
    if oi < FAKE_BREAKOUT_MIN_OI:
        risk += 1
        reasons.append(f"OI confirmation {oi:.0f}/15 < {FAKE_BREAKOUT_MIN_OI:.0f}/15")

    strategy = result.get("strategy_ensemble") or {}
    consensus = str(strategy.get("consensus") or "").upper()
    meta["strategy_consensus"] = consensus or "UNKNOWN"
    if consensus and consensus not in {"CONFIRM", "STRONG_CONFIRM"}:
        risk += 2
        reasons.append(f"strategy ensemble is {consensus}, not CONFIRM")

    meta["risk_score"] = int(risk)
    meta["reasons"] = reasons
    if risk >= FAKE_BREAKOUT_BLOCK_SCORE:
        meta["risk_level"] = "HIGH"
    elif risk >= max(3, FAKE_BREAKOUT_BLOCK_SCORE - 2):
        meta["risk_level"] = "MEDIUM"
    else:
        meta["risk_level"] = "LOW"
    meta["block"] = bool(
        FAKE_BREAKOUT_MODE == "block" and risk >= FAKE_BREAKOUT_BLOCK_SCORE
    )
    return meta


def _apply(mod):
    global _PATCHED
    if _PATCHED or getattr(mod, "_V785_BREAKOUT_GUARD_PATCHED", False):
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
        if TINY_TWO_SLICE and str(stage or "").upper() == "P2" and abs(_f(close_vol)) <= 1e-12:
            mod._diag(f"CAPITAL EFFICIENCY TWO_SLICE TP2 {symbol}: no size close; ratchet-only milestone")
            return True, "two-slice TP2 ratchet-only"
        return original_partial_close(symbol, direction, position_id, close_vol, signal_id, stage)

    def adaptive_derisk(row, targets, px, tp1_vol):
        if _is_tiny_two_slice_row(mod, row) and not bool(row[17]):
            old_bank = mod.ADAPTIVE_BANK_MFE_R
            try:
                mod.ADAPTIVE_BANK_MFE_R = 999.0
                return original_adaptive(row, targets, px, tp1_vol)
            finally:
                mod.ADAPTIVE_BANK_MFE_R = old_bank
        return original_adaptive(row, targets, px, tp1_vol)

    def manage_open_trade(row, exchange_pos):
        if _is_tiny_two_slice_row(mod, row):
            proxy = list(row)
            proxy[16] = _TruthyZero()
            return original_manage_open_trade(tuple(proxy), exchange_pos)
        return original_manage_open_trade(row, exchange_pos)

    def execute_signal(result, paper_trade=None):
        fb = _fake_breakout_assessment(result)
        if fb.get("active"):
            try:
                result["v785_fake_breakout"] = fb
            except Exception:
                pass
            mod._diag(
                "FAKE BREAKOUT GUARD "
                f"{result.get('symbol')} {result.get('direction')} "
                f"risk={fb.get('risk_score')} level={fb.get('risk_level')} "
                f"depth={_f(fb.get('max_depth_atr')):.2f}ATR "
                f"margin={_f(fb.get('entry_margin')):.1f} "
                f"mode={FAKE_BREAKOUT_MODE}"
            )
            if fb.get("block"):
                reason = "; ".join((fb.get("reasons") or [])[:3]) or "fragile breakout"
                mod._diag(
                    f"FAKE BREAKOUT BLOCK {result.get('symbol')} {result.get('direction')} — {reason}"
                )
                return {
                    "executed": False,
                    "reason": f"fake-breakout guard risk {fb.get('risk_score')} >= {FAKE_BREAKOUT_BLOCK_SCORE}: {reason}",
                }

        elite, meta = _elite_candidate(result)
        if not elite:
            return original_execute(result, paper_trade)
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
            "version": "7.8.5-fake-breakout-guard",
            "capital_efficiency_overlay": True,
            "compound_notional": COMPOUND_NOTIONAL,
            "tiny_two_slice": TINY_TWO_SLICE,
            "elite_chase": ELITE_CHASE,
            "elite_max_drift_r": ELITE_MAX_DRIFT_R,
            "baseline_max_entry_drift_r": mod.MAX_ENTRY_DRIFT_R,
            "fake_breakout_guard": FAKE_BREAKOUT_GUARD,
            "fake_breakout_mode": FAKE_BREAKOUT_MODE,
            "fake_breakout_block_score": FAKE_BREAKOUT_BLOCK_SCORE,
            "fake_breakout_min_depth_atr": FAKE_BREAKOUT_MIN_DEPTH_ATR,
        })
        return state

    mod._risk_limits = risk_limits
    mod._three_way_split = three_way_split
    mod._partial_market_close = partial_market_close
    mod._adaptive_derisk = adaptive_derisk
    mod._manage_open_trade = manage_open_trade
    mod.execute_signal = execute_signal
    mod.diagnostic_state = diagnostic_state
    mod.V70_VERSION = "7.8.5-fake-breakout-guard"
    mod._V785_BREAKOUT_GUARD_PATCHED = True
    _PATCHED = True
    try:
        mod._diag(
            "V7.8.5 overlay active: V7.8.4 capital efficiency + "
            f"fake-breakout guard mode={FAKE_BREAKOUT_MODE} block_score={FAKE_BREAKOUT_BLOCK_SCORE}"
        )
    except Exception:
        pass


def _import(name, globals=None, locals=None, fromlist=(), level=0):
    module = _ORIGINAL_IMPORT(name, globals, locals, fromlist, level)
    if name == "live_executor_v70" or name.endswith(".live_executor_v70"):
        target = sys.modules.get("live_executor_v70")
        if target is not None:
            _apply(target)
            builtins.__import__ = _ORIGINAL_IMPORT
    return module


builtins.__import__ = _import
print("[V7DIAG] V7.8.5 fake-breakout overlay armed", flush=True)
