"""FuturesHunter V7.9.0 read-only metals range observer.

Adds visibility without changing RANGE_SCALPER qualification or execution:
- independently inspects every configured metal, even when a manual/live position already exists;
- reports best micro (1m/5m) and meso (5m/15m) range structure;
- reports range position, touch counts, trend vetoes, rejection state and upside/downside breakout danger;
- emits a compact Telegram summary periodically through the already-configured notifier.

All market reads go through the V7.8.8 cached/rate-aware transport. Trading decisions
still come exclusively from the existing RANGE_SCALPER candidate function.
"""
import os
import threading
import time
import statistics

import v786_range_overlay as base
import v786_range_hardening as hard
import v787_metals_range_economics as econ

OBSERVER_ENABLED = os.getenv("V790_RANGE_OBSERVER_ENABLED", "true").lower() == "true"
OBSERVER_SECONDS = max(60, int(os.getenv("V790_RANGE_OBSERVER_SECONDS", "120")))
TELEGRAM_SECONDS = max(300, int(os.getenv("V790_RANGE_OBSERVER_TELEGRAM_SECONDS", "1200")))

_LOCK = threading.RLock()
_SNAPSHOTS = {}
_STARTED = False
_LAST_TELEGRAM = 0.0


def _f(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)


def _cluster_range(rows, last, atr, horizon):
    if not rows or last <= 0 or atr <= 0:
        return None
    if horizon == "micro":
        sample = rows[-65:]
        tol = max(last * 0.00035, atr * 0.20)
        min_touches = base.RANGE_MIN_TOUCHES
        min_width = base.RANGE_MIN_WIDTH_PCT
        max_width = base.RANGE_MAX_WIDTH_PCT
        outside_frac = 0.08
        recent = rows[-30:]
        penalty = 1.50
    else:
        sample = rows[-60:]
        tol = max(last * 0.00045, atr * 0.22)
        min_touches = max(3, base.RANGE_MIN_TOUCHES)
        min_width = hard.MESO_MIN_WIDTH_PCT
        max_width = hard.MESO_MAX_WIDTH_PCT
        outside_frac = 0.06
        recent = rows[-24:]
        penalty = 1.75

    highs = [c for c in base._clusters(base._pivots(sample, "H"), tol) if c["count"] >= min_touches]
    lows = [c for c in base._clusters(base._pivots(sample, "L"), tol) if c["count"] >= min_touches]
    best = None
    for hi in highs:
        for lo in lows:
            upper, lower = _f(hi.get("price")), _f(lo.get("price"))
            if upper <= lower:
                continue
            width = upper - lower
            width_pct = width / last * 100.0
            if not (min_width <= width_pct <= max_width):
                continue
            if not (lower - outside_frac * width <= last <= upper + outside_frac * width):
                continue
            outside = sum(
                1 for r in recent
                if r["close"] > upper + outside_frac * width or r["close"] < lower - outside_frac * width
            )
            if outside > 2:
                continue
            score = hi["count"] + lo["count"] - penalty * outside
            if best is None or score > best[0]:
                best = (score, lower, upper, int(lo["count"]), int(hi["count"]), width, width_pct, outside, tol)
    if best is None:
        return None
    _, lower, upper, lt, ht, width, width_pct, outside, tol = best
    pos = (last - lower) / width if width > 0 else 0.5
    return {
        "lower": lower, "upper": upper, "width": width, "width_pct": width_pct,
        "low_touches": lt, "high_touches": ht, "outside": outside,
        "position": max(-0.25, min(1.25, pos)), "tolerance": tol,
    }


def _wick(row):
    if not row:
        return 0.0, 0.0
    rng = max(1e-12, _f(row.get("high")) - _f(row.get("low")))
    body_hi = max(_f(row.get("open")), _f(row.get("close")))
    body_lo = min(_f(row.get("open")), _f(row.get("close")))
    return max(0.0, (_f(row.get("high")) - body_hi) / rng), max(0.0, (body_lo - _f(row.get("low"))) / rng)


def _volume_ratio(rows):
    vols = [_f(r.get("vol")) for r in rows[-25:-1] if _f(r.get("vol")) > 0]
    med = statistics.median(vols) if vols else 0.0
    return _f(rows[-1].get("vol")) / med if rows and med > 0 else 1.0


def _danger(range_box, rows, last, adx, eff, vol_ratio, direction):
    if not range_box or not rows:
        return 5, "UNKNOWN"
    lower, upper, width = range_box["lower"], range_box["upper"], range_box["width"]
    pos = range_box["position"]
    atr = base._atr(rows)
    buf = max(0.08 * atr, 0.025 * width)
    recent = rows[-2:]
    upper_break = any(_f(r.get("close")) > upper + buf for r in recent)
    lower_break = any(_f(r.get("close")) < lower - buf for r in recent)
    up_wick, down_wick = _wick(rows[-1])
    score = 0
    if direction == "UP":
        if pos >= 0.82: score += 2
        if pos >= 0.95: score += 1
        if upper_break: score += 4
        elif _f(rows[-1].get("close")) >= upper - 0.03 * width: score += 2
        if adx > 28: score += 1
        if eff > 0.50: score += 1
        if vol_ratio > 1.50: score += 1
        if up_wick >= 0.25 and _f(rows[-1].get("close")) < upper: score -= 2
    else:
        if pos <= 0.18: score += 2
        if pos <= 0.05: score += 1
        if lower_break: score += 4
        elif _f(rows[-1].get("close")) <= lower + 0.03 * width: score += 2
        if adx > 28: score += 1
        if eff > 0.50: score += 1
        if vol_ratio > 1.50: score += 1
        if down_wick >= 0.25 and _f(rows[-1].get("close")) > lower: score -= 2
    score = max(0, min(10, score))
    label = "HIGH" if score >= 7 else ("MODERATE" if score >= 4 else "LOW")
    return score, label


def _hypothetical_cost_bps(symbol, direction, spread_bps, horizon_minutes):
    dummy = {
        "symbol": symbol,
        "direction": direction,
        "live_expiry_ts": time.time() + horizon_minutes * 60,
    }
    try:
        funding_bps, _ = econ._adverse_funding_cost_bps(dummy)
    except Exception:
        funding_bps = econ.UNKNOWN_FUNDING_BUFFER_BPS
    return 2.0 * econ.API_TAKER_BPS_PER_SIDE + 2.0 * econ.SLIPPAGE_BPS_PER_SIDE + max(0.0, spread_bps) + max(0.0, funding_bps)


def diagnose_symbol(symbol):
    symbol = str(symbol or "").upper()
    try:
        one = base._closed_candles(symbol, "Min1", 90 * 60)
        five = base._closed_candles(symbol, "Min5", 10 * 3600)
        fifteen = base._closed_candles(symbol, "Min15", 36 * 3600)
        last, bid, ask, spread = base._ticker(symbol)
        if last <= 0 or len(one) < 35 or len(five) < 24 or len(fifteen) < 18:
            return {"symbol": symbol, "status": "DATA_INCOMPLETE", "last": last, "spread_bps": spread, "ts": time.time()}

        a1, a5 = base._atr(one), base._atr(five)
        micro = _cluster_range(one, last, a1, "micro")
        meso = _cluster_range(five, last, a5, "meso")
        adx5, adx15 = base._adx_proxy(five), base._adx_proxy(fifteen)
        eff5, eff15 = base._efficiency(five, 12), base._efficiency(fifteen, 8)
        vr1, vr5 = _volume_ratio(one), _volume_ratio(five)

        chosen = micro or meso
        horizon = "1m/5m" if micro else ("5m/15m" if meso else "none")
        rows = one if micro else five
        adx = adx5 if micro else adx15
        eff = eff5 if micro else eff15
        vr = vr1 if micro else vr5
        up_danger, up_label = _danger(chosen, rows, last, adx, eff, vr, "UP")
        down_danger, down_label = _danger(chosen, rows, last, adx, eff, vr, "DOWN")

        rejection_short = False
        rejection_long = False
        status = "NO_VALID_RANGE"
        pos_pct = None
        if chosen:
            pos = chosen["position"]
            pos_pct = pos * 100.0
            uw, lw = _wick(rows[-1])
            close = _f(rows[-1].get("close"))
            if horizon == "1m/5m":
                edge = base.RANGE_ENTRY_BAND * chosen["width"]
                rejection_short = last >= chosen["upper"] - edge and rows[-1]["high"] >= chosen["upper"] - chosen["tolerance"] and close < chosen["upper"] - 0.03 * chosen["width"] and uw >= 0.20
                rejection_long = last <= chosen["lower"] + edge and rows[-1]["low"] <= chosen["lower"] + chosen["tolerance"] and close > chosen["lower"] + 0.03 * chosen["width"] and lw >= 0.20
                trend_veto = adx5 > 28 or adx15 > 34 or eff5 > 0.48 or eff15 > 0.62
            else:
                edge = hard.MESO_ENTRY_BAND * chosen["width"]
                rejection_short = last >= chosen["upper"] - edge and rows[-1]["high"] >= chosen["upper"] - chosen["tolerance"] and close < chosen["upper"] - 0.025 * chosen["width"] and uw >= 0.18
                rejection_long = last <= chosen["lower"] + edge and rows[-1]["low"] <= chosen["lower"] + chosen["tolerance"] and close > chosen["lower"] + 0.025 * chosen["width"] and lw >= 0.18
                trend_veto = adx15 > 28 or eff15 > 0.50

            if up_label == "HIGH" and pos >= 0.80:
                status = "UPSIDE_BREAKOUT_DANGER"
            elif down_label == "HIGH" and pos <= 0.20:
                status = "DOWNSIDE_BREAKOUT_DANGER"
            elif trend_veto:
                status = "TREND_VETO"
            elif rejection_short:
                status = "SHORT_REJECTION_READY"
            elif rejection_long:
                status = "LONG_REJECTION_READY"
            elif pos >= 0.82:
                status = "NEAR_UPPER_WAIT_REJECTION"
            elif pos <= 0.18:
                status = "NEAR_LOWER_WAIT_REJECTION"
            else:
                status = "MID_RANGE"

        short_cost = _hypothetical_cost_bps(symbol, "SHORT", spread, 120 if horizon == "5m/15m" else 60)
        long_cost = _hypothetical_cost_bps(symbol, "LONG", spread, 120 if horizon == "5m/15m" else 60)
        return {
            "symbol": symbol, "ts": time.time(), "last": last, "bid": bid, "ask": ask,
            "spread_bps": spread, "status": status, "horizon": horizon,
            "range": chosen, "micro": micro, "meso": meso, "position_pct": pos_pct,
            "adx5": adx5, "adx15": adx15, "eff5": eff5, "eff15": eff15,
            "vol1": vr1, "vol5": vr5, "short_rejection": rejection_short,
            "long_rejection": rejection_long, "upside_danger": up_danger,
            "upside_label": up_label, "downside_danger": down_danger,
            "downside_label": down_label, "short_cost_bps": short_cost, "long_cost_bps": long_cost,
        }
    except Exception as exc:
        return {"symbol": symbol, "ts": time.time(), "status": "ERROR", "error": f"{type(exc).__name__}: {exc}"}


def _fmt_snapshot(s):
    symbol = s.get("symbol", "?")
    if s.get("status") in {"ERROR", "DATA_INCOMPLETE"}:
        return f"{symbol} {s.get('status')} {s.get('error','')}"
    r = s.get("range") or {}
    if not r:
        return f"{symbol} px={_f(s.get('last')):.8g} NO_RANGE spread={_f(s.get('spread_bps')):.2f}bps ADX5/15={_f(s.get('adx5')):.1f}/{_f(s.get('adx15')):.1f}"
    return (
        f"{symbol} px={_f(s.get('last')):.8g} {s.get('status')} {s.get('horizon')} "
        f"box={_f(r.get('lower')):.8g}-{_f(r.get('upper')):.8g} pos={_f(s.get('position_pct')):.0f}% "
        f"touches={int(r.get('low_touches') or 0)}/{int(r.get('high_touches') or 0)} "
        f"ADX5/15={_f(s.get('adx5')):.1f}/{_f(s.get('adx15')):.1f} eff={_f(s.get('eff5')):.2f}/{_f(s.get('eff15')):.2f} "
        f"vol={_f(s.get('vol1')):.2f}/{_f(s.get('vol5')):.2f} spread={_f(s.get('spread_bps')):.2f}bps "
        f"UP={s.get('upside_label')}({int(s.get('upside_danger') or 0)}/10) DOWN={s.get('downside_label')}({int(s.get('downside_danger') or 0)}/10)"
    )


def _telegram_summary(mod, snapshots):
    valid = [s for s in snapshots if s.get("range")]
    near = [s for s in valid if s.get("status") in {"SHORT_REJECTION_READY", "LONG_REJECTION_READY", "NEAR_UPPER_WAIT_REJECTION", "NEAR_LOWER_WAIT_REJECTION", "UPSIDE_BREAKOUT_DANGER", "DOWNSIDE_BREAKOUT_DANGER"}]
    executable = [s for s in valid if s.get("status") in {"SHORT_REJECTION_READY", "LONG_REJECTION_READY"}]
    ranked = sorted(valid, key=lambda s: (0 if s.get("status") in {"SHORT_REJECTION_READY", "LONG_REJECTION_READY"} else 1, -max(_f(s.get("position_pct")), 100.0-_f(s.get("position_pct")))))
    lines = [
        "🔎 METALS RANGE OBSERVER",
        f"Scanned: {len(snapshots)} | structural ranges: {len(valid)} | near edge: {len(near)} | rejection-ready: {len(executable)}",
    ]
    for s in ranked[:4]:
        r = s.get("range") or {}
        lines.append(
            f"• {s['symbol']} {s.get('status')} | px {_f(s.get('last')):.8g} | "
            f"{_f(r.get('lower')):.8g}-{_f(r.get('upper')):.8g} | pos {_f(s.get('position_pct')):.0f}% | "
            f"UP danger {s.get('upside_label')} {int(s.get('upside_danger') or 0)}/10"
        )
    lines.append("Observer only — trade thresholds unchanged.")
    try:
        mod._msg("\n".join(lines))
    except Exception:
        pass


def _loop(mod):
    global _LAST_TELEGRAM
    time.sleep(8)
    mod._diag(f"V7.9.0 RANGE OBSERVER started interval={OBSERVER_SECONDS}s telegram={TELEGRAM_SECONDS}s")
    while True:
        snapshots = []
        for symbol in base.RANGE_SYMBOLS:
            s = diagnose_symbol(symbol)
            with _LOCK:
                _SNAPSHOTS[symbol] = s
            snapshots.append(s)
            mod._diag("RANGE OBSERVER " + _fmt_snapshot(s))
            time.sleep(0.10)
        now = time.time()
        if now - _LAST_TELEGRAM >= TELEGRAM_SECONDS:
            _telegram_summary(mod, snapshots)
            _LAST_TELEGRAM = now
        time.sleep(OBSERVER_SECONDS)


def range_observer_snapshot(symbol=None):
    with _LOCK:
        if symbol:
            return dict(_SNAPSHOTS.get(str(symbol).upper()) or {})
        return {k: dict(v) for k, v in _SNAPSHOTS.items()}


def _start(mod):
    global _STARTED
    if _STARTED or not OBSERVER_ENABLED:
        return
    _STARTED = True
    threading.Thread(target=_loop, args=(mod,), name="FH-V790-RangeObserver", daemon=True).start()
    original_diag = mod.diagnostic_state
    def diagnostic_state():
        state = original_diag()
        state.update({
            "version": "7.9.0-range-observer",
            "range_observer_enabled": OBSERVER_ENABLED,
            "range_observer_seconds": OBSERVER_SECONDS,
            "range_observer_telegram_seconds": TELEGRAM_SECONDS,
        })
        return state
    mod.diagnostic_state = diagnostic_state
    mod.V70_VERSION = "7.9.0-range-observer"
    mod._range_observer_snapshot = range_observer_snapshot
    mod._diag(f"V7.9.0 range observer armed interval={OBSERVER_SECONDS}s telegram={TELEGRAM_SECONDS}s trade_logic_unchanged=True")


def _install_import_hook():
    import builtins
    import sys
    previous = builtins.__import__
    def hooked(name, globals=None, locals=None, fromlist=(), level=0):
        module = previous(name, globals, locals, fromlist, level)
        if name == "live_executor_v70" or name.endswith(".live_executor_v70"):
            target = sys.modules.get("live_executor_v70")
            if target is not None:
                _start(target)
                builtins.__import__ = previous
        return module
    target = sys.modules.get("live_executor_v70")
    if target is not None:
        _start(target)
    else:
        builtins.__import__ = hooked


_install_import_hook()
print("[V7DIAG] V7.9.0 read-only range observer bootstrap armed", flush=True)
