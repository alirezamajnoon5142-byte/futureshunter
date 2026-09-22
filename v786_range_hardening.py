"""V7.8.6 range-scalper hardening.

Adds two production safeguards on top of v786_range_overlay:
1) reject ranges whose target edge is too small versus estimated execution cost;
2) add a slower 5m/15m repeated-range detector for metals such as XAU/XAUT.

The module monkey-patches only the range candidate function. Core trend/breakout
logic and existing live-position management are untouched.
"""
import os
import statistics
import time

import v786_range_overlay as base

_ORIGINAL_RANGE_CANDIDATE = base._range_candidate

EXECUTION_OVERHEAD_BPS = max(4.0, float(os.getenv("V786_RANGE_EXECUTION_OVERHEAD_BPS", "8.0")))
MIN_TP1_COST_MULT = max(1.0, float(os.getenv("V786_RANGE_MIN_TP1_COST_MULT", "1.35")))
MIN_TP3_COST_MULT = max(MIN_TP1_COST_MULT, float(os.getenv("V786_RANGE_MIN_TP3_COST_MULT", "3.0")))
MESO_ENABLED = os.getenv("V786_RANGE_MESO_ENABLED", "true").lower() == "true"
MESO_MIN_WIDTH_PCT = max(0.20, float(os.getenv("V786_RANGE_MESO_MIN_WIDTH_PCT", "0.35")))
MESO_MAX_WIDTH_PCT = min(3.0, float(os.getenv("V786_RANGE_MESO_MAX_WIDTH_PCT", "1.80")))
MESO_ENTRY_BAND = min(0.28, max(0.08, float(os.getenv("V786_RANGE_MESO_ENTRY_BAND", "0.16"))))
MESO_EXPIRY_MINUTES = min(240, max(45, int(os.getenv("V786_RANGE_MESO_EXPIRY_MINUTES", "120"))))


def _economic_edge_ok(result):
    if not result:
        return False
    entry = abs(base._f(result.get("price")))
    plan = result.get("risk_plan") or {}
    if entry <= 0:
        return False
    tp1 = base._f(plan.get("tp1")); tp3 = base._f(plan.get("tp3"))
    meta = result.setdefault("v786_range_meta", {})
    spread_bps = max(0.0, base._f(meta.get("spread_bps")))
    estimated_cost_bps = spread_bps + EXECUTION_OVERHEAD_BPS
    tp1_edge_bps = abs(tp1 - entry) / entry * 10000.0
    tp3_edge_bps = abs(tp3 - entry) / entry * 10000.0
    meta.update({
        "estimated_cost_bps": round(estimated_cost_bps, 2),
        "tp1_edge_bps": round(tp1_edge_bps, 2),
        "tp3_edge_bps": round(tp3_edge_bps, 2),
    })
    return (
        tp1_edge_bps >= estimated_cost_bps * MIN_TP1_COST_MULT
        and tp3_edge_bps >= estimated_cost_bps * MIN_TP3_COST_MULT
    )


def _meso_candidate(symbol):
    if not MESO_ENABLED:
        return None
    five = base._closed_candles(symbol, "Min5", 10 * 3600)
    fifteen = base._closed_candles(symbol, "Min15", 36 * 3600)
    if len(five) < 48 or len(fifteen) < 36:
        return None

    last, bid, ask, spread_bps = base._ticker(symbol)
    if last <= 0 or spread_bps > base.RANGE_MAX_SPREAD_BPS:
        return None

    a5 = base._atr(five)
    if a5 <= 0:
        return None

    tol = max(last * 0.00045, a5 * 0.22)
    min_touches = max(3, base.RANGE_MIN_TOUCHES)
    highs = [c for c in base._clusters(base._pivots(five[-60:], "H"), tol) if c["count"] >= min_touches]
    lows = [c for c in base._clusters(base._pivots(five[-60:], "L"), tol) if c["count"] >= min_touches]
    if not highs or not lows:
        return None

    best = None
    recent = five[-24:]
    for hi in highs:
        for lo in lows:
            upper, lower = hi["price"], lo["price"]
            if upper <= lower:
                continue
            width = upper - lower
            width_pct = width / last * 100.0
            if not (MESO_MIN_WIDTH_PCT <= width_pct <= MESO_MAX_WIDTH_PCT):
                continue
            if not (lower - 0.06 * width <= last <= upper + 0.06 * width):
                continue
            outside = sum(
                1 for row in recent
                if row["close"] > upper + 0.06 * width or row["close"] < lower - 0.06 * width
            )
            if outside > 2:
                continue
            score = hi["count"] + lo["count"] - 1.75 * outside
            if best is None or score > best[0]:
                best = (score, lower, upper, lo["count"], hi["count"], width, width_pct)
    if best is None:
        return None

    _, lower, upper, low_touches, high_touches, width, width_pct = best
    adx15 = base._adx_proxy(fifteen)
    eff15 = base._efficiency(fifteen, 10)
    if adx15 > 28 or eff15 > 0.50:
        return None

    breakout_buffer = max(0.08 * a5, 0.025 * width)
    if any(row["close"] > upper + breakout_buffer or row["close"] < lower - breakout_buffer for row in five[-2:]):
        return None

    vols = [row["vol"] for row in five[-25:-1] if row["vol"] > 0]
    median_vol = statistics.median(vols) if vols else 0.0
    vol_ratio = five[-1]["vol"] / median_vol if median_vol > 0 else 1.0
    if vol_ratio > 2.0:
        return None

    candle = five[-1]
    candle_range = max(1e-9, candle["high"] - candle["low"])
    body_hi = max(candle["open"], candle["close"])
    body_lo = min(candle["open"], candle["close"])
    upper_wick = (candle["high"] - body_hi) / candle_range
    lower_wick = (body_lo - candle["low"]) / candle_range
    edge = MESO_ENTRY_BAND * width

    direction = None
    if (
        last >= upper - edge
        and candle["high"] >= upper - tol
        and candle["close"] < upper - 0.025 * width
        and upper_wick >= 0.18
    ):
        direction = "SHORT"
    elif (
        last <= lower + edge
        and candle["low"] <= lower + tol
        and candle["close"] > lower + 0.025 * width
        and lower_wick >= 0.18
    ):
        direction = "LONG"
    if direction is None:
        return None

    quality = 68.0
    quality += min(12.0, (low_touches + high_touches - 6) * 3.0)
    quality += max(0.0, 7.0 - adx15 * 0.20)
    quality += max(0.0, 6.0 - eff15 * 10.0)
    quality += max(0.0, 4.0 - spread_bps)
    if vol_ratio <= 1.35:
        quality += 3.0
    quality = min(97.0, round(quality, 1))

    elite_touches = max(4, base.RANGE_ELITE_TOUCHES)
    elite = (
        low_touches >= elite_touches
        and high_touches >= elite_touches
        and quality >= 86.0
        and spread_bps <= base.RANGE_ELITE_SPREAD_BPS
        and adx15 <= 20
        and eff15 <= 0.35
        and vol_ratio <= 1.45
    )
    leverage = base.RANGE_ELITE_LEVERAGE if elite else base.RANGE_NORMAL_LEVERAGE
    notional_mult = base.RANGE_ELITE_NOTIONAL_MULT if elite else base.RANGE_NORMAL_NOTIONAL_MULT
    risk_pct = base.RANGE_ELITE_RISK_PCT if elite else base.RANGE_NORMAL_RISK_PCT

    stop_buffer = max(0.10 * width, 0.24 * a5)
    midpoint = (lower + upper) / 2.0
    if direction == "SHORT":
        entry = bid if bid > 0 else last
        stop = upper + stop_buffer
        tp1, tp2, tp3 = midpoint, lower + 0.20 * width, lower + 0.06 * width
        if not (stop > entry > tp1 > tp2 > tp3):
            return None
    else:
        entry = ask if ask > 0 else last
        stop = lower - stop_buffer
        tp1, tp2, tp3 = midpoint, upper - 0.20 * width, upper - 0.06 * width
        if not (stop < entry < tp1 < tp2 < tp3):
            return None

    result = {
        "symbol": symbol,
        "direction": direction,
        "price": entry,
        "best_score": quality,
        "raw_score": quality,
        "weighted_score": quality,
        "entry_threshold": 72.0,
        "oi_score": 0.0,
        "regime": "RANGE_SCALPER",
        "risk_plan": {"entry": entry, "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3},
        "live_strategy_tag": "RANGE_SCALPER",
        "live_risk_pct_override": risk_pct,
        "live_expiry_ts": time.time() + MESO_EXPIRY_MINUTES * 60,
        "v786_range_leverage": leverage,
        "v786_range_notional_mult": notional_mult,
        "v786_range_meta": {
            "horizon": "5m/15m",
            "lower": lower,
            "upper": upper,
            "width_pct": width_pct,
            "low_touches": low_touches,
            "high_touches": high_touches,
            "adx15": adx15,
            "eff15": eff15,
            "spread_bps": spread_bps,
            "vol_ratio": vol_ratio,
            "elite": elite,
        },
    }
    return result if _economic_edge_ok(result) else None


def _combined_range_candidate(symbol):
    micro = _ORIGINAL_RANGE_CANDIDATE(symbol)
    if micro is not None:
        meta = micro.setdefault("v786_range_meta", {})
        meta.setdefault("horizon", "1m/5m")
        if _economic_edge_ok(micro):
            return micro
    return _meso_candidate(symbol)


base._range_candidate = _combined_range_candidate
print(
    "[V7DIAG] V7.8.6 range hardening armed: cost-aware micro + live 5m/15m meso ranges",
    flush=True,
)
