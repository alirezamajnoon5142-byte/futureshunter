"""FuturesHunter V7.8.6 combined runtime overlay.

Preserves V7.8.4 capital-efficiency behavior and V7.8.5 fake-breakout
classification, and adds a LIVE metals range-scalper for repeated 1m/5m ranges.
Core trend/breakout trades keep their existing leverage/risk behavior.
"""
import builtins
import os
import statistics
import sys
import threading
import time

import requests

_ORIGINAL_IMPORT = builtins.__import__
_PATCHED = False
_RANGE_THREAD_STARTED = False
_TLS = threading.local()
_LAST_RANGE_ENTRY = {}

COMPOUND_NOTIONAL = os.getenv("V784_COMPOUND_NOTIONAL", "true").lower() == "true"
TINY_TWO_SLICE = os.getenv("V784_TINY_TWO_SLICE", "true").lower() == "true"
ELITE_CHASE = os.getenv("V784_ELITE_CHASE", "true").lower() == "true"
ELITE_MIN_SELECTOR = float(os.getenv("V784_ELITE_MIN_SELECTOR", "100"))
ELITE_MAX_COST_R = float(os.getenv("V784_ELITE_MAX_COST_R", "0.15"))
ELITE_MIN_CORE = float(os.getenv("V784_ELITE_MIN_CORE", "80"))
ELITE_MAX_DRIFT_R = min(0.25, max(0.20, float(os.getenv("V784_ELITE_MAX_DRIFT_R", "0.25"))))

FAKE_BREAKOUT_GUARD = os.getenv("V785_FAKE_BREAKOUT_GUARD", "true").lower() == "true"
FAKE_BREAKOUT_MODE = os.getenv("V785_FAKE_BREAKOUT_MODE", "shadow").strip().lower()
FAKE_BREAKOUT_BLOCK_SCORE = max(3, int(os.getenv("V785_FAKE_BREAKOUT_BLOCK_SCORE", "5")))
FAKE_BREAKOUT_MIN_DEPTH_ATR = max(0.02, float(os.getenv("V785_FAKE_BREAKOUT_MIN_DEPTH_ATR", "0.10")))
FAKE_BREAKOUT_STRONG_DEPTH_ATR = max(FAKE_BREAKOUT_MIN_DEPTH_ATR, float(os.getenv("V785_FAKE_BREAKOUT_STRONG_DEPTH_ATR", "0.18")))
FAKE_BREAKOUT_MIN_RAW = float(os.getenv("V785_FAKE_BREAKOUT_MIN_RAW", "64"))
FAKE_BREAKOUT_MIN_MARGIN = float(os.getenv("V785_FAKE_BREAKOUT_MIN_MARGIN", "4.0"))
FAKE_BREAKOUT_MIN_OI = float(os.getenv("V785_FAKE_BREAKOUT_MIN_OI", "10"))
FAKE_BREAKOUT_MIN_RV = float(os.getenv("V785_FAKE_BREAKOUT_MIN_RV", "0.90"))

RANGE_ENABLED = os.getenv("V786_RANGE_SCALPER_ENABLED", "true").lower() == "true"
RANGE_SYMBOLS = [x.strip().upper() for x in os.getenv("V786_RANGE_SYMBOLS", "SILVER_USDT,GOLD_USDT").split(",") if x.strip()]
RANGE_SCAN_SECONDS = max(8, int(os.getenv("V786_RANGE_SCAN_SECONDS", "12")))
RANGE_COOLDOWN_SECONDS = max(300, int(os.getenv("V786_RANGE_COOLDOWN_SECONDS", "900")))
RANGE_MAX_TRADES_PER_DAY = max(1, int(os.getenv("V786_RANGE_MAX_TRADES_PER_DAY", "8")))
RANGE_MIN_TOUCHES = max(2, int(os.getenv("V786_RANGE_MIN_TOUCHES", "2")))
RANGE_ELITE_TOUCHES = max(3, int(os.getenv("V786_RANGE_ELITE_TOUCHES", "3")))
RANGE_MIN_WIDTH_PCT = max(0.12, float(os.getenv("V786_RANGE_MIN_WIDTH_PCT", "0.22")))
RANGE_MAX_WIDTH_PCT = min(2.50, float(os.getenv("V786_RANGE_MAX_WIDTH_PCT", "1.25")))
RANGE_ENTRY_BAND = min(0.30, max(0.08, float(os.getenv("V786_RANGE_ENTRY_BAND", "0.18"))))
RANGE_MAX_SPREAD_BPS = min(15.0, max(1.0, float(os.getenv("V786_RANGE_MAX_SPREAD_BPS", "5.0"))))
RANGE_ELITE_SPREAD_BPS = min(RANGE_MAX_SPREAD_BPS, float(os.getenv("V786_RANGE_ELITE_SPREAD_BPS", "3.0")))
RANGE_NORMAL_LEVERAGE = min(4, max(1, int(os.getenv("V786_RANGE_NORMAL_LEVERAGE", "3"))))
RANGE_ELITE_LEVERAGE = min(4, max(RANGE_NORMAL_LEVERAGE, int(os.getenv("V786_RANGE_ELITE_LEVERAGE", "4"))))
RANGE_NORMAL_NOTIONAL_MULT = min(2.0, max(1.0, float(os.getenv("V786_RANGE_NORMAL_NOTIONAL_MULT", "1.50"))))
RANGE_ELITE_NOTIONAL_MULT = min(2.5, max(RANGE_NORMAL_NOTIONAL_MULT, float(os.getenv("V786_RANGE_ELITE_NOTIONAL_MULT", "2.00"))))
RANGE_NORMAL_RISK_PCT = min(0.0060, max(0.0010, float(os.getenv("V786_RANGE_NORMAL_RISK_PCT", "0.0035"))))
RANGE_ELITE_RISK_PCT = min(0.0075, max(RANGE_NORMAL_RISK_PCT, float(os.getenv("V786_RANGE_ELITE_RISK_PCT", "0.0045"))))
RANGE_EXPIRY_MINUTES = min(180, max(20, int(os.getenv("V786_RANGE_EXPIRY_MINUTES", "60"))))
RANGE_REST = os.getenv("V786_MEXC_CONTRACT_REST", "https://contract.mexc.com").rstrip("/")


class _TruthyZero(float):
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
        contracts = _f(row[4]); tp1_vol = _f(row[15]); tp2_vol = _f(row[16])
        if contracts <= 0 or tp1_vol <= 0 or abs(tp2_vol) > 1e-12:
            return False
        c = mod.contract(str(row[1] or ""))
        step = max(_f(c.get("volUnit"), 1.0), 1e-12)
        min_vol = max(_f(c.get("minVol"), step), step)
        return contracts + 1e-9 >= 2.0 * min_vol and contracts < 3.0 * min_vol - 1e-9
    except Exception:
        return False


def _elite_candidate(result):
    if not ELITE_CHASE or not isinstance(result, dict):
        return False, {}
    gate = result.get("v70_live_gate") or {}; strategy = result.get("strategy_ensemble") or {}
    sr = result.get("support_resistance") or gate.get("support_resistance") or {}
    selector = _f(gate.get("selector_score")); cost_r = _f(gate.get("estimated_cost_r"), 99.0)
    core = _f(result.get("best_score")); consensus = str(strategy.get("consensus") or "").upper()
    room_r = _f(sr.get("nearest_room_r"), 99.0); breakout = bool(sr.get("breakout_confirmed_zones"))
    ok = bool(gate.get("eligible")) and selector >= ELITE_MIN_SELECTOR and cost_r <= ELITE_MAX_COST_R and core >= ELITE_MIN_CORE and consensus in {"CONFIRM", "STRONG_CONFIRM"} and (room_r >= 1.25 or breakout)
    return ok, {"selector": selector, "cost_r": cost_r, "core": core, "consensus": consensus, "room_r": room_r}


def _breakout_depth(tf, direction):
    if not isinstance(tf, dict):
        return None
    atr = _f(tf.get("atr")); close = _f(tf.get("close"))
    if atr <= 0 or close <= 0:
        return None
    if direction == "LONG":
        level = _f(tf.get("high20_prev")); return (close - level) / atr if level > 0 and close > level else None
    level = _f(tf.get("low20_prev")); return (level - close) / atr if level > 0 and close < level else None


def _fake_breakout_assessment(result):
    meta = {"active": False, "risk_score": 0, "risk_level": "NONE", "reasons": [], "block": False}
    if not FAKE_BREAKOUT_GUARD or not isinstance(result, dict) or str(result.get("regime") or "").upper() != "BREAKOUT":
        return meta
    direction = str(result.get("direction") or "").upper()
    if direction not in {"LONG", "SHORT"}:
        return meta
    meta["active"] = True; tf5, tf15 = result.get("5m") or {}, result.get("15m") or {}
    depths, direct = [], []
    for label, tf in (("5m", tf5), ("15m", tf15)):
        d = _breakout_depth(tf, direction)
        if d is not None: depths.append(d); direct.append(label)
    max_depth = max(depths) if depths else 0.0; risk = 0; reasons = []
    if not direct: risk += 3; reasons.append("no closed 5m/15m penetration beyond the 20-bar edge")
    elif len(direct) == 1: risk += 1; reasons.append(f"only {direct[0]} confirms the break")
    if max_depth < FAKE_BREAKOUT_MIN_DEPTH_ATR: risk += 2; reasons.append(f"shallow penetration {max_depth:.2f} ATR")
    elif max_depth < FAKE_BREAKOUT_STRONG_DEPTH_ATR: risk += 1; reasons.append(f"modest penetration {max_depth:.2f} ATR")
    cl5, cl15 = _f(tf5.get("close_location"), 0.5), _f(tf15.get("close_location"), 0.5)
    good5 = cl5 >= 0.60 if direction == "LONG" else cl5 <= 0.40; good15 = cl15 >= 0.60 if direction == "LONG" else cl15 <= 0.40
    if not good5 and not good15: risk += 2; reasons.append("weak close location on both timeframes")
    elif not (good5 and good15): risk += 1; reasons.append("only one timeframe closed decisively")
    if max(_f(tf5.get("rv")), _f(tf15.get("rv"))) < FAKE_BREAKOUT_MIN_RV and _f(tf15.get("volume_trend")) < 5: risk += 1; reasons.append("weak participation")
    raw = _f(result.get("raw_score")); margin = _f(result.get("best_score")) - _f(result.get("entry_threshold"), 68.0); oi = _f(result.get("oi_score"))
    if raw < FAKE_BREAKOUT_MIN_RAW: risk += 1; reasons.append(f"raw {raw:.0f} < {FAKE_BREAKOUT_MIN_RAW:.0f}")
    if margin < FAKE_BREAKOUT_MIN_MARGIN: risk += 1; reasons.append(f"thin ENTRY margin {margin:.1f}")
    if oi < FAKE_BREAKOUT_MIN_OI: risk += 1; reasons.append(f"OI {oi:.0f}/15")
    consensus = str((result.get("strategy_ensemble") or {}).get("consensus") or "").upper()
    if consensus and consensus not in {"CONFIRM", "STRONG_CONFIRM"}: risk += 2; reasons.append(f"strategy={consensus}")
    meta.update({"risk_score": int(risk), "risk_level": "HIGH" if risk >= FAKE_BREAKOUT_BLOCK_SCORE else ("MEDIUM" if risk >= max(3, FAKE_BREAKOUT_BLOCK_SCORE - 2) else "LOW"), "reasons": reasons, "block": bool(FAKE_BREAKOUT_MODE == "block" and risk >= FAKE_BREAKOUT_BLOCK_SCORE), "max_depth_atr": round(max_depth, 4), "entry_margin": round(margin, 2)})
    return meta


def _http_json(url, params=None, timeout=8):
    r = requests.get(url, params=params or {}, timeout=timeout); r.raise_for_status(); p = r.json()
    if isinstance(p, dict) and p.get("success") is False:
        raise RuntimeError(f"MEXC public error {p.get('code')} {p.get('message')}")
    return p.get("data") if isinstance(p, dict) and "data" in p else p


def _closed_candles(symbol, interval, lookback_seconds):
    now = int(time.time())
    data = _http_json(f"{RANGE_REST}/api/v1/contract/kline/{symbol}", {"interval": interval, "start": now - int(lookback_seconds), "end": now})
    rows = []
    if isinstance(data, dict) and isinstance(data.get("time"), list):
        keys = {k: data.get(k) or [] for k in ("time", "open", "high", "low", "close", "vol")}
        n = min(len(keys["time"]), len(keys["open"]), len(keys["high"]), len(keys["low"]), len(keys["close"]))
        for i in range(n):
            rows.append({"time": _f(keys["time"][i]), "open": _f(keys["open"][i]), "high": _f(keys["high"][i]), "low": _f(keys["low"][i]), "close": _f(keys["close"][i]), "vol": _f(keys["vol"][i]) if i < len(keys["vol"]) else 0.0})
    elif isinstance(data, list):
        for x in data:
            if isinstance(x, dict): rows.append({k: _f(x.get(k)) for k in ("time", "open", "high", "low", "close", "vol")})
    rows = [x for x in rows if x["time"] > 0 and x["close"] > 0]; rows.sort(key=lambda x: x["time"])
    sec = {"Min1": 60, "Min5": 300, "Min15": 900}.get(interval, 60)
    if rows and rows[-1]["time"] + sec > time.time() - 2: rows = rows[:-1]
    return rows


def _ticker(symbol):
    data = _http_json(f"{RANGE_REST}/api/v1/contract/ticker", {"symbol": symbol})
    if isinstance(data, list): data = next((x for x in data if str(x.get("symbol") or "") == symbol), data[0] if data else {})
    data = data or {}; last = _f(data.get("lastPrice") or data.get("fairPrice") or data.get("indexPrice")); bid = _f(data.get("bid1") or data.get("bidPrice") or data.get("bid")); ask = _f(data.get("ask1") or data.get("askPrice") or data.get("ask"))
    spread_bps = ((ask - bid) / last * 10000.0) if last > 0 and ask > 0 and bid > 0 and ask >= bid else 99.0
    return last, bid, ask, spread_bps


def _atr(rows, n=14):
    if len(rows) < n + 1: return 0.0
    tr = [max(rows[i]["high"] - rows[i]["low"], abs(rows[i]["high"] - rows[i-1]["close"]), abs(rows[i]["low"] - rows[i-1]["close"])) for i in range(1, len(rows))]
    return sum(tr[-n:]) / max(1, min(n, len(tr)))


def _adx_proxy(rows, n=14):
    if len(rows) < n + 2: return 99.0
    plus, minus, tr = [], [], []
    for i in range(1, len(rows)):
        up = rows[i]["high"] - rows[i-1]["high"]; dn = rows[i-1]["low"] - rows[i]["low"]
        plus.append(up if up > dn and up > 0 else 0.0); minus.append(dn if dn > up and dn > 0 else 0.0)
        tr.append(max(rows[i]["high"] - rows[i]["low"], abs(rows[i]["high"] - rows[i-1]["close"]), abs(rows[i]["low"] - rows[i-1]["close"])))
    s_tr = sum(tr[-n:])
    if s_tr <= 0: return 0.0
    pdi = 100.0 * sum(plus[-n:]) / s_tr; mdi = 100.0 * sum(minus[-n:]) / s_tr
    return 100.0 * abs(pdi - mdi) / max(1e-9, pdi + mdi)


def _efficiency(rows, n=12):
    c = [x["close"] for x in rows[-n:]]
    if len(c) < 3: return 1.0
    path = sum(abs(c[i] - c[i-1]) for i in range(1, len(c)))
    return abs(c[-1] - c[0]) / path if path > 0 else 0.0


def _pivots(rows, side, wing=2):
    vals = []; key = "high" if side == "H" else "low"
    for i in range(wing, len(rows) - wing):
        v = rows[i][key]; around = [rows[j][key] for j in range(i-wing, i+wing+1) if j != i]
        if (side == "H" and v >= max(around)) or (side == "L" and v <= min(around)): vals.append((i, v))
    return vals


def _clusters(points, tolerance):
    out = []
    for idx, price in sorted(points, key=lambda x: x[1]):
        hit = next((c for c in out if abs(price - c["price"]) <= tolerance), None)
        if hit is None: out.append({"price": price, "count": 1, "last_idx": idx, "values": [price]})
        else:
            hit["values"].append(price); hit["count"] += 1; hit["last_idx"] = max(hit["last_idx"], idx); hit["price"] = sum(hit["values"]) / len(hit["values"])
    return out


def _range_candidate(symbol):
    one = _closed_candles(symbol, "Min1", 90*60); five = _closed_candles(symbol, "Min5", 6*3600); fifteen = _closed_candles(symbol, "Min15", 12*3600)
    if len(one) < 35 or len(five) < 24 or len(fifteen) < 18: return None
    last, bid, ask, spread_bps = _ticker(symbol)
    if last <= 0 or spread_bps > RANGE_MAX_SPREAD_BPS: return None
    a1, a5 = _atr(one), _atr(five)
    if a1 <= 0 or a5 <= 0: return None
    tol = max(last*0.00035, a1*0.20)
    highs = [c for c in _clusters(_pivots(one[-65:], "H"), tol) if c["count"] >= RANGE_MIN_TOUCHES]
    lows = [c for c in _clusters(_pivots(one[-65:], "L"), tol) if c["count"] >= RANGE_MIN_TOUCHES]
    if not highs or not lows: return None
    best = None; recent = one[-30:]
    for hi in highs:
        for lo in lows:
            upper, lower = hi["price"], lo["price"]
            if upper <= lower: continue
            width = upper-lower; width_pct = width/last*100.0
            if not (RANGE_MIN_WIDTH_PCT <= width_pct <= RANGE_MAX_WIDTH_PCT): continue
            if not (lower-0.08*width <= last <= upper+0.08*width): continue
            outside = sum(1 for r in recent if r["close"] > upper+0.08*width or r["close"] < lower-0.08*width)
            if outside > 2: continue
            score = hi["count"] + lo["count"] - 1.5*outside
            if best is None or score > best[0]: best = (score, lower, upper, lo["count"], hi["count"], width, width_pct)
    if best is None: return None
    _, lower, upper, low_touches, high_touches, width, width_pct = best
    adx5, adx15 = _adx_proxy(five), _adx_proxy(fifteen); eff5, eff15 = _efficiency(five,12), _efficiency(fifteen,8)
    if adx5 > 28 or adx15 > 34 or eff5 > 0.48 or eff15 > 0.62: return None
    buffer = max(0.08*a1, 0.025*width)
    if any(r["close"] > upper+buffer or r["close"] < lower-buffer for r in one[-2:]): return None
    vols = [r["vol"] for r in one[-25:-1] if r["vol"] > 0]; med_vol = statistics.median(vols) if vols else 0.0; vol_ratio = one[-1]["vol"]/med_vol if med_vol > 0 else 1.0
    if vol_ratio > 2.2: return None
    edge = RANGE_ENTRY_BAND*width; c = one[-1]; body_hi, body_lo = max(c["open"],c["close"]), min(c["open"],c["close"]); rng = max(1e-9,c["high"]-c["low"]); upper_wick = (c["high"]-body_hi)/rng; lower_wick = (body_lo-c["low"])/rng
    direction = None
    if last >= upper-edge and c["high"] >= upper-tol and c["close"] < upper-0.03*width and upper_wick >= 0.20: direction = "SHORT"
    elif last <= lower+edge and c["low"] <= lower+tol and c["close"] > lower+0.03*width and lower_wick >= 0.20: direction = "LONG"
    if not direction: return None
    quality = 62.0 + min(12.0,(low_touches+high_touches-4)*3.0) + max(0.0,8.0-adx5*0.25) + max(0.0,6.0-eff5*10.0) + max(0.0,5.0-spread_bps) + (3.0 if vol_ratio <= 1.35 else 0.0); quality = min(96.0, quality)
    elite = low_touches >= RANGE_ELITE_TOUCHES and high_touches >= RANGE_ELITE_TOUCHES and quality >= 84.0 and spread_bps <= RANGE_ELITE_SPREAD_BPS and adx5 <= 21 and eff5 <= 0.35 and vol_ratio <= 1.5
    leverage = RANGE_ELITE_LEVERAGE if elite else RANGE_NORMAL_LEVERAGE; notional_mult = RANGE_ELITE_NOTIONAL_MULT if elite else RANGE_NORMAL_NOTIONAL_MULT; risk_pct = RANGE_ELITE_RISK_PCT if elite else RANGE_NORMAL_RISK_PCT
    stop_buf = max(0.12*width, 0.28*a1); mid = (lower+upper)/2.0
    if direction == "SHORT":
        entry = bid if bid > 0 else last; stop = upper+stop_buf; tp1, tp2, tp3 = mid, lower+0.20*width, lower+0.05*width
        if not (stop > entry > tp1 > tp2 > tp3): return None
    else:
        entry = ask if ask > 0 else last; stop = lower-stop_buf; tp1, tp2, tp3 = mid, upper-0.20*width, upper-0.05*width
        if not (stop < entry < tp1 < tp2 < tp3): return None
    return {"symbol":symbol,"direction":direction,"price":entry,"best_score":round(quality,1),"raw_score":round(quality,1),"weighted_score":round(quality,1),"entry_threshold":70.0,"oi_score":0.0,"regime":"RANGE_SCALPER","risk_plan":{"entry":entry,"stop":stop,"tp1":tp1,"tp2":tp2,"tp3":tp3},"live_strategy_tag":"RANGE_SCALPER","live_risk_pct_override":risk_pct,"live_expiry_ts":time.time()+RANGE_EXPIRY_MINUTES*60,"v786_range_leverage":leverage,"v786_range_notional_mult":notional_mult,"v786_range_meta":{"lower":lower,"upper":upper,"width_pct":width_pct,"low_touches":low_touches,"high_touches":high_touches,"adx5":adx5,"adx15":adx15,"eff5":eff5,"eff15":eff15,"spread_bps":spread_bps,"vol_ratio":vol_ratio,"elite":elite}}


def _range_recently_traded(mod, symbol):
    now = time.time()
    if now-_LAST_RANGE_ENTRY.get(symbol,0) < RANGE_COOLDOWN_SECONDS: return True
    try:
        row = mod._db("SELECT EXTRACT(EPOCH FROM (NOW()-COALESCE(opened_at,updated_at))) FROM fh_live_trades WHERE symbol=%s AND payload->>'strategyTag'='RANGE_SCALPER' ORDER BY updated_at DESC LIMIT 1", (symbol,), "one")
        return bool(row and row[0] is not None and _f(row[0]) < RANGE_COOLDOWN_SECONDS)
    except Exception: return False


def _range_daily_cap_hit(mod, symbol):
    try:
        row = mod._db("SELECT COUNT(*) FROM fh_live_trades WHERE symbol=%s AND payload->>'strategyTag'='RANGE_SCALPER' AND opened_at >= date_trunc('day',NOW())", (symbol,), "one")
        return bool(row and int(row[0] or 0) >= RANGE_MAX_TRADES_PER_DAY)
    except Exception: return False


def _range_loop(mod):
    time.sleep(20); mod._diag(f"V7.8.6 RANGE SCALPER live loop started symbols={','.join(RANGE_SYMBOLS)} scan={RANGE_SCAN_SECONDS}s")
    while True:
        try:
            if not RANGE_ENABLED or not getattr(mod,"ENABLED",False) or not getattr(mod,"ARMED",False): time.sleep(RANGE_SCAN_SECONDS); continue
            halted, _ = mod.halt_status()
            if halted: time.sleep(RANGE_SCAN_SECONDS); continue
            open_positions = mod.positions() or []; open_symbols = {str(p.get("symbol") or p.get("contractCode") or "").upper() for p in open_positions if _f(p.get("holdVol") or p.get("vol") or p.get("positionVol")) > 0}
            for symbol in RANGE_SYMBOLS:
                if symbol in open_symbols or _range_recently_traded(mod,symbol) or _range_daily_cap_hit(mod,symbol): continue
                try:
                    result = _range_candidate(symbol)
                    if not result: continue
                    meta = result.get("v786_range_meta") or {}; mod._diag(f"RANGE SCALPER SIGNAL {symbol} {result['direction']} quality={result['best_score']:.1f} range={meta.get('lower'):.6g}-{meta.get('upper'):.6g} touches={meta.get('low_touches')}/{meta.get('high_touches')} spread={meta.get('spread_bps'):.2f}bps lev={result['v786_range_leverage']}x elite={meta.get('elite')}")
                    outcome = mod.execute_signal(result, {"signal_id":f"rng_{int(time.time())}_{symbol}_{result['direction']}"})
                    if outcome.get("executed"):
                        _LAST_RANGE_ENTRY[symbol] = time.time(); mod._msg(f"⚡ V7.8.6 LIVE RANGE SCALP\n{symbol} {result['direction']}\nRange: {meta.get('lower'):.6g} - {meta.get('upper'):.6g}\nLeverage: {result['v786_range_leverage']}x | Quality: {result['best_score']:.1f}\nRisk budget: {result['live_risk_pct_override']*100:.2f}% equity | Notional cap: {result['v786_range_notional_mult']:.2f}x equity")
                    else: mod._diag(f"RANGE SCALPER SKIP {symbol}: {outcome.get('reason')}")
                except Exception as e: mod._diag(f"RANGE SCALPER {symbol} error: {type(e).__name__}: {e}")
                time.sleep(0.8)
        except Exception as e:
            try: mod._diag(f"RANGE SCALPER loop error: {type(e).__name__}: {e}")
            except Exception: pass
        time.sleep(RANGE_SCAN_SECONDS)


def _start_range_thread(mod):
    global _RANGE_THREAD_STARTED
    if _RANGE_THREAD_STARTED or not RANGE_ENABLED: return
    _RANGE_THREAD_STARTED = True; threading.Thread(target=_range_loop,args=(mod,),name="FH-V786-RangeScalper",daemon=True).start()


def _apply(mod):
    global _PATCHED
    if _PATCHED or getattr(mod,"_V786_RANGE_SCALPER_PATCHED",False): return
    required = ["_risk_limits","_three_way_split","_partial_market_close","_manage_open_trade","_adaptive_derisk","execute_signal"]
    if any(not hasattr(mod,name) for name in required): return
    original_risk_limits = mod._risk_limits; original_split = mod._three_way_split; original_partial_close = mod._partial_market_close; original_manage_open_trade = mod._manage_open_trade; original_adaptive = mod._adaptive_derisk; original_execute = mod.execute_signal; original_diagnostic = mod.diagnostic_state

    def risk_limits(equity):
        risk_usdt,max_notional,equity_kill = original_risk_limits(equity); eq = max(0.0,_f(equity)); mult = _f(getattr(_TLS,"notional_mult",0.0))
        if mult > 0 and eq > 0: max_notional = eq*mult
        elif COMPOUND_NOTIONAL and eq > 0: max_notional = eq
        return risk_usdt,max_notional,equity_kill

    def three_way_split(contracts,step,min_vol):
        normal = original_split(contracts,step,min_vol)
        if normal or not TINY_TWO_SLICE: return normal
        c = _f(contracts); step_f = max(_f(step),1e-12); min_f = max(_f(min_vol,step_f),step_f)
        if c+1e-9 < 2.0*min_f: return None
        first = max(min_f,mod._floor_step(c*0.5,step_f)); runner = mod._floor_step(c-first,step_f)
        if runner < min_f: first = mod._floor_step(c-min_f,step_f); runner = mod._floor_step(c-first,step_f)
        if first >= min_f and runner >= min_f: mod._diag(f"CAPITAL EFFICIENCY TWO_SLICE contracts={c:g} tp1={first:g} tp2=0 runner={runner:g}"); return first,0.0,runner
        return None

    def partial_market_close(symbol,direction,position_id,close_vol,signal_id,stage):
        if TINY_TWO_SLICE and str(stage or "").upper()=="P2" and abs(_f(close_vol))<=1e-12: mod._diag(f"CAPITAL EFFICIENCY TWO_SLICE TP2 {symbol}: ratchet-only milestone"); return True,"two-slice TP2 ratchet-only"
        return original_partial_close(symbol,direction,position_id,close_vol,signal_id,stage)

    def adaptive_derisk(row,targets,px,tp1_vol):
        if _is_tiny_two_slice_row(mod,row) and not bool(row[17]):
            old_bank = mod.ADAPTIVE_BANK_MFE_R
            try: mod.ADAPTIVE_BANK_MFE_R = 999.0; return original_adaptive(row,targets,px,tp1_vol)
            finally: mod.ADAPTIVE_BANK_MFE_R = old_bank
        return original_adaptive(row,targets,px,tp1_vol)

    def manage_open_trade(row,exchange_pos):
        if _is_tiny_two_slice_row(mod,row): proxy=list(row); proxy[16]=_TruthyZero(); return original_manage_open_trade(tuple(proxy),exchange_pos)
        return original_manage_open_trade(row,exchange_pos)

    def execute_signal(result,paper_trade=None):
        fb = _fake_breakout_assessment(result)
        if fb.get("active"):
            result["v785_fake_breakout"] = fb; mod._diag(f"FAKE BREAKOUT GUARD {result.get('symbol')} {result.get('direction')} risk={fb.get('risk_score')} level={fb.get('risk_level')} depth={_f(fb.get('max_depth_atr')):.2f}ATR margin={_f(fb.get('entry_margin')):.1f} mode={FAKE_BREAKOUT_MODE}")
            if fb.get("block"):
                reason = "; ".join((fb.get("reasons") or [])[:3]) or "fragile breakout"; mod._diag(f"FAKE BREAKOUT BLOCK {result.get('symbol')} {result.get('direction')} — {reason}"); return {"executed":False,"reason":f"fake-breakout guard: {reason}"}
        if str((result or {}).get("live_strategy_tag") or "").upper()=="RANGE_SCALPER":
            leverage = min(4,max(1,int(_f(result.get("v786_range_leverage"),RANGE_NORMAL_LEVERAGE)))); mult = min(2.5,max(1.0,_f(result.get("v786_range_notional_mult"),RANGE_NORMAL_NOTIONAL_MULT)))
            with mod._lock:
                old_lev = mod.MAX_LEVERAGE
                try:
                    _TLS.notional_mult = mult; mod.MAX_LEVERAGE = leverage; mod._diag(f"RANGE SCALPER LIVE EXECUTION {result.get('symbol')} {result.get('direction')} lev={leverage}x max_notional={mult:.2f}x_equity risk={_f(result.get('live_risk_pct_override'))*100:.2f}%"); return original_execute(result,paper_trade)
                finally: mod.MAX_LEVERAGE = old_lev; _TLS.notional_mult = 0.0
        elite,meta = _elite_candidate(result)
        if not elite: return original_execute(result,paper_trade)
        with mod._lock:
            old_limit = mod.MAX_ENTRY_DRIFT_R
            try: mod.MAX_ENTRY_DRIFT_R = max(old_limit,ELITE_MAX_DRIFT_R); mod._diag(f"CAPITAL EFFICIENCY ELITE CHASE selector={meta['selector']:.1f} core={meta['core']:.1f} strategy={meta['consensus']} cost={meta['cost_r']:.2f}R room={meta['room_r']:.2f}R limit={mod.MAX_ENTRY_DRIFT_R:.2f}R"); return original_execute(result,paper_trade)
            finally: mod.MAX_ENTRY_DRIFT_R = old_limit

    def diagnostic_state():
        state = original_diagnostic(); state.update({"version":"7.8.6-live-range-scalper","capital_efficiency_overlay":True,"compound_notional":COMPOUND_NOTIONAL,"tiny_two_slice":TINY_TWO_SLICE,"elite_chase":ELITE_CHASE,"elite_max_drift_r":ELITE_MAX_DRIFT_R,"baseline_max_entry_drift_r":mod.MAX_ENTRY_DRIFT_R,"fake_breakout_guard":FAKE_BREAKOUT_GUARD,"fake_breakout_mode":FAKE_BREAKOUT_MODE,"fake_breakout_block_score":FAKE_BREAKOUT_BLOCK_SCORE,"range_scalper_enabled":RANGE_ENABLED,"range_symbols":RANGE_SYMBOLS,"range_scan_seconds":RANGE_SCAN_SECONDS,"range_normal_leverage":RANGE_NORMAL_LEVERAGE,"range_elite_leverage":RANGE_ELITE_LEVERAGE,"range_normal_risk_pct":RANGE_NORMAL_RISK_PCT,"range_elite_risk_pct":RANGE_ELITE_RISK_PCT,"range_normal_notional_mult":RANGE_NORMAL_NOTIONAL_MULT,"range_elite_notional_mult":RANGE_ELITE_NOTIONAL_MULT}); return state

    mod._risk_limits = risk_limits; mod._three_way_split = three_way_split; mod._partial_market_close = partial_market_close; mod._adaptive_derisk = adaptive_derisk; mod._manage_open_trade = manage_open_trade; mod.execute_signal = execute_signal; mod.diagnostic_state = diagnostic_state; mod.V70_VERSION = "7.8.6-live-range-scalper"; mod._V786_RANGE_SCALPER_PATCHED = True; _PATCHED = True
    mod._diag(f"V7.8.6 overlay active: fake-breakout={FAKE_BREAKOUT_MODE}, LIVE range scalper symbols={','.join(RANGE_SYMBOLS)} leverage={RANGE_NORMAL_LEVERAGE}x/{RANGE_ELITE_LEVERAGE}x"); _start_range_thread(mod)


def _import(name,globals=None,locals=None,fromlist=(),level=0):
    module = _ORIGINAL_IMPORT(name,globals,locals,fromlist,level)
    if name=="live_executor_v70" or name.endswith(".live_executor_v70"):
        target = sys.modules.get("live_executor_v70")
        if target is not None: _apply(target); builtins.__import__ = _ORIGINAL_IMPORT
    return module


builtins.__import__ = _import
print("[V7DIAG] V7.8.6 live range-scalper overlay armed",flush=True)
