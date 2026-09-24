"""FuturesHunter V8.1.0 — 4H Swing Live Expert.

Live-mode migration:
- legacy fast Core / range / equity execution paths remain research-only;
- ONLY signals tagged SWING_4H may create new live positions;
- 1D bias + closed 4H thesis drive direction;
- 1H is confirmation/refinement only and can never veto a strong 4H thesis;
- stops sit beyond 4H structure/ATR and sizing shrinks to preserve account risk;
- micro adaptive de-risk is disabled for SWING_4H positions;
- exchange-side hard stop + TP3 and normal TP1/TP2 milestone management remain.

This module does not close or alter manual/untracked positions.
"""
import copy
import math
import os
import sys
import threading
import time

import live_executor_v70 as live

ENABLED = os.getenv("V810_SWING4H_ENABLED", "true").lower() == "true"
LIVE_ONLY_SWING = os.getenv("V810_LIVE_ONLY_SWING4H", "true").lower() == "true"
SCAN_SECONDS = max(180, int(os.getenv("V810_SWING_SCAN_SECONDS", "600")))
START_DELAY = max(20, int(os.getenv("V810_SWING_START_DELAY", "45")))
RISK_PCT = min(0.0075, max(0.0010, float(os.getenv("V810_SWING_RISK_PCT", "0.005"))))
MIN_SCORE = min(95.0, max(65.0, float(os.getenv("V810_SWING_MIN_SCORE", "78"))))
MIN_STOP_PCT = max(0.50, float(os.getenv("V810_SWING_MIN_STOP_PCT", "1.0")))
MAX_STOP_PCT = min(12.0, max(MIN_STOP_PCT, float(os.getenv("V810_SWING_MAX_STOP_PCT", "8.0"))))
ATR_STOP_MULT = min(3.0, max(1.0, float(os.getenv("V810_SWING_ATR_STOP_MULT", "1.40"))))
STRUCTURE_BUFFER_ATR = min(0.50, max(0.05, float(os.getenv("V810_SWING_STRUCTURE_BUFFER_ATR", "0.15"))))
TP1_R = min(2.5, max(1.0, float(os.getenv("V810_SWING_TP1_R", "1.5"))))
TP2_R = min(4.0, max(TP1_R, float(os.getenv("V810_SWING_TP2_R", "2.5"))))
TP3_R = min(6.0, max(TP2_R, float(os.getenv("V810_SWING_TP3_R", "4.0"))))
MAX_OPEN_SWINGS = max(1, min(3, int(os.getenv("V810_SWING_MAX_OPEN", "2"))))
MAX_SAME_BUCKET_DIRECTION = max(1, min(2, int(os.getenv("V810_SWING_MAX_SAME_BUCKET_DIRECTION", "1"))))
MAX_UNIVERSE = max(8, min(50, int(os.getenv("V810_SWING_MAX_UNIVERSE", "30"))))
RETRY_SECONDS = max(300, int(os.getenv("V810_SWING_RETRY_SECONDS", "900")))
EXPIRY_HOURS = max(24.0, min(240.0, float(os.getenv("V810_SWING_EXPIRY_HOURS", "168"))))
EXTRA_SYMBOLS = [
    x.strip().upper() for x in os.getenv(
        "V810_SWING_EXTRA_SYMBOLS",
        "BTC_USDT,ETH_USDT,SOL_USDT,XAU_USDT,XAUT_USDT,SILVER_USDT,SPX_USDT,USOIL_USDT"
    ).split(",") if x.strip()
]

_PATCHED = False
_MAIN = None
_ORIGINAL_EXECUTE = None
_ORIGINAL_ADAPTIVE = None
_ORIGINAL_DIAGNOSTIC = None
_LOCK = threading.RLock()
_LAST_ATTEMPT = {}
_LAST_SCAN_SUMMARY = 0.0


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _norm(v):
    return str(v or "").strip().upper()


def _bucket(symbol):
    s = _norm(symbol)
    if s in {
        "XAU_USDT","XAUT_USDT","SILVER_USDT","COPPER_USDT","XPT_USDT","XPD_USDT",
        "ALUMINUM_USDT","ZINC_USDT","NICKEL_USDT","LEAD_USDT"
    }:
        return "METAL"
    if s in {"SPX_USDT","NDX_USDT","DJI_USDT","DAX_USDT","FTSE_USDT","NIKKEI_USDT"}:
        return "INDEX"
    if s in {"USOIL_USDT","UKOIL_USDT","NATGAS_USDT"}:
        return "ENERGY"
    return "CRYPTO"


def _closed_frame(main, symbol, interval, min_rows):
    df = main.get_candles(symbol, interval)
    if df is None or len(df) < min_rows + 1:
        return None
    try:
        x = main.add_indicators(df.copy())
        # MEXC includes the still-forming candle at the end.
        return x.iloc[:-1].copy()
    except Exception:
        return None


def _row(df, idx=-1):
    try:
        return df.iloc[idx]
    except Exception:
        return None


def _slope(series, n=3):
    try:
        if len(series) <= n:
            return 0.0
        return _f(series.iloc[-1]) - _f(series.iloc[-1-n])
    except Exception:
        return 0.0


def _candle_parts(r):
    o,h,l,c = _f(r.get("open")),_f(r.get("high")),_f(r.get("low")),_f(r.get("close"))
    rng=max(h-l,1e-12)
    body=abs(c-o)/rng
    loc=(c-l)/rng
    return body,loc


def _direction_score(direction, d1, h4, h1, oi_score, macro, bucket):
    d = 1.0 if direction == "LONG" else -1.0
    daily = _row(d1); four = _row(h4); one = _row(h1)
    prev4 = _row(h4,-2)
    if daily is None or four is None or one is None or prev4 is None:
        return None

    score = 0.0
    reasons = []

    dc, de20, de50 = _f(daily.get("close")),_f(daily.get("ema20")),_f(daily.get("ema50"))
    h4c,h4e20,h4e50 = _f(four.get("close")),_f(four.get("ema20")),_f(four.get("ema50"))
    atr = _f(four.get("atr14") or four.get("atr"))
    if min(dc,de20,de50,h4c,h4e20,h4e50,atr) <= 0:
        return None

    # 1D structural bias — 20 points.
    ordered_daily = (dc > de20 > de50) if direction=="LONG" else (dc < de20 < de50)
    daily_side = (dc > de20) if direction=="LONG" else (dc < de20)
    ema20_slope = _slope(d1["ema20"],3)
    daily_slope_ok = ema20_slope > 0 if direction=="LONG" else ema20_slope < 0
    if ordered_daily:
        score += 14; reasons.append("1D close/EMA20/EMA50 aligned")
    elif daily_side:
        score += 8; reasons.append("1D price on trend side of EMA20")
    if daily_slope_ok:
        score += 6; reasons.append("1D EMA20 slope aligned")

    # Closed 4H trend — 20 points.
    ordered_h4 = (h4c > h4e20 > h4e50) if direction=="LONG" else (h4c < h4e20 < h4e50)
    h4_side = (h4c > h4e20) if direction=="LONG" else (h4c < h4e20)
    h4_slope = _slope(h4["ema20"],3)
    h4_slope_ok = h4_slope > 0 if direction=="LONG" else h4_slope < 0
    if ordered_h4:
        score += 14; reasons.append("4H close/EMA20/EMA50 aligned")
    elif h4_side:
        score += 8; reasons.append("4H price on trend side of EMA20")
    if h4_slope_ok:
        score += 6; reasons.append("4H EMA20 slope aligned")

    # Setup structure — mandatory, 20 points.
    prior = h4.iloc[-9:-1] if len(h4) >= 10 else h4.iloc[:-1]
    prior_high = _f(prior["high"].max())
    prior_low = _f(prior["low"].min())
    body, loc = _candle_parts(four)
    breakout = False
    pullback = False
    if direction == "LONG":
        breakout = h4c > prior_high + 0.03*atr and loc >= 0.62 and body >= 0.38
        recent_low = _f(h4.iloc[-3:]["low"].min())
        pullback = ordered_daily and h4_side and recent_low <= h4e20 + 0.35*atr and h4c > _f(prev4.get("close")) and loc >= 0.55
    else:
        breakout = h4c < prior_low - 0.03*atr and loc <= 0.38 and body >= 0.38
        recent_high = _f(h4.iloc[-3:]["high"].max())
        pullback = ordered_daily and h4_side and recent_high >= h4e20 - 0.35*atr and h4c < _f(prev4.get("close")) and loc <= 0.45

    if breakout:
        score += 20; setup="BREAKOUT"; reasons.append("closed 4H breakout through prior 8-candle structure")
    elif pullback:
        score += 17; setup="PULLBACK"; reasons.append("4H trend pullback reclaimed/continued from EMA20 structure")
    else:
        return None

    # Momentum — 15.
    rsi=_f(four.get("rsi14")); adx=_f(four.get("adx14")); macd=_f(four.get("macd_hist"))
    if direction=="LONG":
        if 52 <= rsi <= 72: score += 6; reasons.append("4H RSI constructive")
        elif rsi > 50: score += 3
        if macd > 0: score += 5; reasons.append("4H MACD histogram positive")
    else:
        if 28 <= rsi <= 48: score += 6; reasons.append("4H RSI constructive")
        elif rsi < 50: score += 3
        if macd < 0: score += 5; reasons.append("4H MACD histogram negative")
    if adx >= 22:
        score += 4; reasons.append(f"4H ADX {adx:.1f}")
    elif adx >= 17:
        score += 2

    # Candle quality — 10.
    if body >= 0.55: score += 5
    elif body >= 0.38: score += 3
    if (direction=="LONG" and loc>=0.72) or (direction=="SHORT" and loc<=0.28):
        score += 5
    elif (direction=="LONG" and loc>=0.60) or (direction=="SHORT" and loc<=0.40):
        score += 3

    # Participation — 5.
    rv=_f(four.get("relative_volume"))
    if rv >= 1.15: score += 5; reasons.append(f"4H RV {rv:.2f}x")
    elif rv >= 0.85: score += 3

    # 1H confirmation only — 5. Never a hard veto.
    onec,onee20,onee50=_f(one.get("close")),_f(one.get("ema20")),_f(one.get("ema50"))
    if direction=="LONG" and onec>onee20>onee50: score += 5; reasons.append("1H aligned")
    elif direction=="SHORT" and onec<onee20<onee50: score += 5; reasons.append("1H aligned")
    elif (direction=="LONG" and onec>onee20) or (direction=="SHORT" and onec<onee20): score += 2

    # OI is supporting evidence, never a veto — 5.
    oi=_f(oi_score)
    score += min(5.0, max(0.0, oi/3.0))
    if oi >= 10: reasons.append(f"OI support {oi:.0f}/15")

    # Macro: scheduled high-impact risk can delay entry. Directional adjustment
    # is small and applies only where broad risk-on/off mapping is sensible.
    event=_norm((macro or {}).get("event_risk"))
    if event in {"HIGH","EXTREME"}:
        return {"blocked": True, "block_reason": f"macro event risk {event}", "score": score, "setup": setup}
    regime=_norm((macro or {}).get("regime"))
    if bucket in {"CRYPTO","INDEX"}:
        if (direction=="LONG" and regime=="RISK_ON") or (direction=="SHORT" and regime=="RISK_OFF"):
            score += 5; reasons.append(f"macro {regime} aligned")
        elif (direction=="LONG" and regime=="RISK_OFF") or (direction=="SHORT" and regime=="RISK_ON"):
            score -= 5; reasons.append(f"macro {regime} conflicts")

    return {
        "blocked": False,
        "score": max(0.0,min(100.0,score)),
        "setup": setup,
        "atr": atr,
        "reasons": reasons,
        "body": body,
        "close_location": loc,
        "rsi": rsi,
        "adx": adx,
        "rv": rv,
    }


def _risk_plan(direction, h4, reference_entry):
    four=_row(h4)
    atr=_f(four.get("atr14") or four.get("atr"))
    if atr<=0 or reference_entry<=0:
        return None
    recent=h4.iloc[-7:]
    if direction=="LONG":
        structure=_f(recent["low"].min()) - STRUCTURE_BUFFER_ATR*atr
        vol_stop=reference_entry - ATR_STOP_MULT*atr
        stop=min(structure,vol_stop)
        sign=1.0
    else:
        structure=_f(recent["high"].max()) + STRUCTURE_BUFFER_ATR*atr
        vol_stop=reference_entry + ATR_STOP_MULT*atr
        stop=max(structure,vol_stop)
        sign=-1.0
    risk=abs(reference_entry-stop)
    stop_pct=risk/reference_entry*100.0
    if stop<=0 or stop_pct<MIN_STOP_PCT or stop_pct>MAX_STOP_PCT:
        return None
    return {
        "entry":reference_entry,
        "stop":stop,
        "stop_pct":stop_pct,
        "risk":risk,
        "tp1":reference_entry + sign*TP1_R*risk,
        "tp2":reference_entry + sign*TP2_R*risk,
        "tp3":reference_entry + sign*TP3_R*risk,
    }


def _candidate(main, scan_row):
    symbol=_norm(scan_row.get("symbol"))
    if not symbol:
        return None
    d1=_closed_frame(main,symbol,"Day1",55)
    time.sleep(0.05)
    h4=_closed_frame(main,symbol,"Hour4",70)
    time.sleep(0.05)
    h1=_closed_frame(main,symbol,"Min60",55)
    if d1 is None or h4 is None or h1 is None:
        return None

    latest4=_row(h4)
    reference=_f(latest4.get("close"))
    candle_ts=_f(latest4.get("time"))
    if reference<=0 or candle_ts<=0:
        return None

    try:
        macro=main.get_macro_snapshot() if hasattr(main,"get_macro_snapshot") else {}
    except Exception:
        macro={}
    oi=_f(scan_row.get("oi_score"))
    bucket=_bucket(symbol)
    long=_direction_score("LONG",d1,h4,h1,oi,macro,bucket)
    short=_direction_score("SHORT",d1,h4,h1,oi,macro,bucket)
    choices=[]
    for direction,meta in (("LONG",long),("SHORT",short)):
        if not meta or meta.get("blocked"):
            continue
        if _f(meta.get("score")) < MIN_SCORE:
            continue
        plan=_risk_plan(direction,h4,reference)
        if not plan:
            continue
        choices.append((meta["score"],direction,meta,plan))
    if not choices:
        return None
    choices.sort(key=lambda x:x[0],reverse=True)
    score,direction,meta,plan=choices[0]

    return {
        "symbol":symbol,
        "direction":direction,
        "price":reference,
        "best_score":round(score,1),
        "raw_score":round(score,1),
        "weighted_score":round(score,1),
        "oi_score":oi,
        "signal_state":"ENTRY",
        "regime":f"SWING_{meta['setup']}",
        "selected_regime":f"SWING_{meta['setup']}",
        "risk_plan":plan,
        "live_strategy_tag":"SWING_4H",
        "live_risk_pct_override":RISK_PCT,
        "live_expiry_ts":time.time()+EXPIRY_HOURS*3600.0,
        "v810_swing":{
            "version":"8.1.0",
            "bucket":bucket,
            "candle_ts":candle_ts,
            "setup":meta["setup"],
            "score":round(score,1),
            "reasons":meta["reasons"],
            "rsi4h":round(_f(meta.get("rsi")),1),
            "adx4h":round(_f(meta.get("adx")),1),
            "rv4h":round(_f(meta.get("rv")),2),
            "macro_regime":_norm(macro.get("regime")),
            "macro_event_risk":_norm(macro.get("event_risk")),
            "timeframes":"1D bias / 4H thesis / 1H confirm",
        },
    }


def _universe(main):
    rows=list(getattr(main,"V681_LAST_SCAN_RESULTS",[]) or [])
    by={}
    for r in rows:
        if not isinstance(r,dict): continue
        s=_norm(r.get("symbol"))
        if s: by[s]=r
    for s in EXTRA_SYMBOLS:
        by.setdefault(s,{"symbol":s,"oi_score":0.0,"turnover":0.0})
    vals=list(by.values())
    vals.sort(key=lambda r:_f(r.get("turnover")),reverse=True)
    return vals[:MAX_UNIVERSE]


def _open_context():
    try:
        snap=live.live_risk_snapshot(24*60) or {}
        return list(snap.get("open_positions") or [])
    except Exception:
        return []


def _portfolio_allows(candidate, opens):
    if len(opens) >= MAX_OPEN_SWINGS:
        return False,f"portfolio {len(opens)}/{MAX_OPEN_SWINGS}"
    b=_bucket(candidate["symbol"]); d=candidate["direction"]
    same=0
    for p in opens:
        if _bucket(p.get("symbol"))==b and _norm(p.get("direction"))==d:
            same+=1
    if same >= MAX_SAME_BUCKET_DIRECTION:
        return False,f"{b} {d} correlation {same}/{MAX_SAME_BUCKET_DIRECTION}"
    return True,""


def _signal_id(c):
    meta=c.get("v810_swing") or {}
    return f"s4h_{int(_f(meta.get('candle_ts')))}_{c['symbol']}_{c['direction']}"


def _scan_once(main):
    global _LAST_SCAN_SUMMARY
    candidates=[]
    for row in _universe(main):
        try:
            c=_candidate(main,row)
            if c: candidates.append(c)
        except Exception as exc:
            live._diag(f"V8.1 SWING scan {row.get('symbol')} warning: {type(exc).__name__}: {exc}")
        time.sleep(0.06)

    candidates.sort(key=lambda x:_f(x.get("best_score")),reverse=True)
    if time.time()-_LAST_SCAN_SUMMARY>=SCAN_SECONDS:
        _LAST_SCAN_SUMMARY=time.time()
        preview=", ".join(
            f"{c['symbol']} {c['direction']} {c['best_score']:.1f} {c['regime']}"
            for c in candidates[:5]
        ) or "no qualified 4H setup"
        live._diag(f"V8.1 SWING scan qualified={len(candidates)} top={preview}")

    opens=_open_context()
    for c in candidates:
        ok,why=_portfolio_allows(c,opens)
        if not ok:
            live._diag(f"V8.1 SWING SKIP {c['symbol']} {c['direction']}: {why}")
            continue
        sid=_signal_id(c)
        last=_LAST_ATTEMPT.get(sid,0.0)
        if time.time()-last<RETRY_SECONDS:
            continue
        _LAST_ATTEMPT[sid]=time.time()
        plan=c["risk_plan"]; meta=c["v810_swing"]
        live._diag(
            f"V8.1 SWING LIVE CANDIDATE {c['symbol']} {c['direction']} "
            f"score={c['best_score']:.1f} setup={meta['setup']} ref={c['price']:.8g} "
            f"stop={plan['stop']:.8g} ({plan['stop_pct']:.2f}%) "
            f"TP={TP1_R:.1f}/{TP2_R:.1f}/{TP3_R:.1f}R risk={RISK_PCT*100:.2f}% "
            f"macro={meta['macro_regime']}/{meta['macro_event_risk']}"
        )
        outcome=live.execute_signal(c,{"signal_id":sid})
        if isinstance(outcome,dict) and outcome.get("executed"):
            opens.append({"symbol":c["symbol"],"direction":c["direction"]})
            try:
                live._msg(
                    f"🕓 V8.1 4H SWING LIVE\n{c['symbol']} {c['direction']} | {meta['setup']}\n"
                    f"Score {c['best_score']:.1f}/100 | 4H RSI {meta['rsi4h']:.1f} | "
                    f"ADX {meta['adx4h']:.1f} | RV {meta['rv4h']:.2f}x\n"
                    f"Entry ref {c['price']:.8g} | Stop {plan['stop']:.8g} ({plan['stop_pct']:.2f}%)\n"
                    f"TP1 {plan['tp1']:.8g} | TP2 {plan['tp2']:.8g} | TP3 {plan['tp3']:.8g}\n"
                    f"Risk budget {RISK_PCT*100:.2f}% equity. 1D/4H thesis owns the trade; micro de-risk disabled."
                )
            except Exception:
                pass
        else:
            live._diag(
                f"V8.1 SWING NO-FILL {c['symbol']} {c['direction']}: "
                f"{(outcome or {}).get('reason') if isinstance(outcome,dict) else outcome}"
            )
        if len(opens)>=MAX_OPEN_SWINGS:
            break


def _loop(main):
    time.sleep(START_DELAY)
    live._diag(
        f"V8.1 4H SWING loop started scan={SCAN_SECONDS}s universe<={MAX_UNIVERSE} "
        f"risk={RISK_PCT*100:.2f}% max_open={MAX_OPEN_SWINGS}"
    )
    while ENABLED:
        try:
            if not getattr(live,"ENABLED",False) or not getattr(live,"ARMED",False):
                time.sleep(SCAN_SECONDS); continue
            halted,_=live.halt_status()
            if halted:
                time.sleep(SCAN_SECONDS); continue
            _scan_once(main)
        except Exception as exc:
            live._diag(f"V8.1 SWING loop warning: {type(exc).__name__}: {exc}")
        time.sleep(SCAN_SECONDS)


def _strategy_tag(signal_id):
    try:
        row=live._db("SELECT payload FROM fh_live_trades WHERE signal_id=%s",(str(signal_id),),"one")
        p=row[0] if row and isinstance(row[0],dict) else {}
        return _norm(p.get("strategyTag"))
    except Exception:
        return ""


def _patch(main):
    global _PATCHED,_MAIN,_ORIGINAL_EXECUTE,_ORIGINAL_ADAPTIVE,_ORIGINAL_DIAGNOSTIC
    with _LOCK:
        if _PATCHED:
            return True
        needed=("get_candles","add_indicators","V681_LAST_SCAN_RESULTS")
        if any(not hasattr(main,x) for x in needed):
            return False
        if not all(hasattr(live,x) for x in ("execute_signal","_adaptive_derisk","diagnostic_state")):
            return False

        _MAIN=main
        _ORIGINAL_EXECUTE=live.execute_signal
        _ORIGINAL_ADAPTIVE=live._adaptive_derisk
        _ORIGINAL_DIAGNOSTIC=live.diagnostic_state

        def execute_signal(result,paper_trade=None):
            tag=_norm((result or {}).get("live_strategy_tag") or "CORE")
            if LIVE_ONLY_SWING and tag!="SWING_4H":
                return {
                    "executed":False,
                    "reason":f"V8.1 live mode is 4H SWING only; {tag or 'CORE'} remains research/shadow"
                }
            return _ORIGINAL_EXECUTE(result,paper_trade)

        def adaptive_derisk(row,targets,px,tp1_vol):
            try:
                if row and _strategy_tag(row[0])=="SWING_4H":
                    return False
            except Exception:
                pass
            return _ORIGINAL_ADAPTIVE(row,targets,px,tp1_vol)

        def diagnostic_state():
            state=_ORIGINAL_DIAGNOSTIC()
            state.update({
                "version":"8.1.0-4h-swing-live",
                "live_entry_mode":"SWING_4H_ONLY" if LIVE_ONLY_SWING else "MIXED",
                "swing4h_enabled":ENABLED,
                "swing4h_risk_pct":RISK_PCT,
                "swing4h_min_score":MIN_SCORE,
                "swing4h_stop_pct_range":[MIN_STOP_PCT,MAX_STOP_PCT],
                "swing4h_tp_r":[TP1_R,TP2_R,TP3_R],
                "swing4h_max_open":MAX_OPEN_SWINGS,
                "swing4h_micro_adaptive":False,
            })
            return state

        live.execute_signal=execute_signal
        live._adaptive_derisk=adaptive_derisk
        live.diagnostic_state=diagnostic_state
        live.V70_VERSION="8.1.0-4h-swing-live"
        _PATCHED=True

        threading.Thread(target=_loop,args=(main,),name="V810Swing4H",daemon=True).start()
        live._diag(
            f"V8.1 4H SWING LIVE armed live_only={LIVE_ONLY_SWING} "
            f"1D+4H closed-candle expert risk={RISK_PCT*100:.2f}% "
            f"stop={MIN_STOP_PCT:.1f}-{MAX_STOP_PCT:.1f}% TP={TP1_R:.1f}/{TP2_R:.1f}/{TP3_R:.1f}R "
            f"max_open={MAX_OPEN_SWINGS} micro_adaptive=OFF legacy_live_entries=BLOCKED"
        )
        return True


def _bootstrap():
    deadline=time.time()+360
    while time.time()<deadline:
        main=sys.modules.get("__main__")
        try:
            if main is not None and _patch(main):
                return
        except Exception as exc:
            try: live._diag(f"V8.1 bootstrap retry: {type(exc).__name__}: {exc}")
            except Exception: pass
        time.sleep(0.25)
    live._diag("V8.1 bootstrap gave up; 4H Swing Live not armed")


if ENABLED:
    threading.Thread(target=_bootstrap,name="V810Bootstrap",daemon=True).start()
    live._diag("V8.1 4H Swing Live bootstrap armed")
