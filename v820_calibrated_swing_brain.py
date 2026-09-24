"""FuturesHunter V8.2 — calibrated atomic swing judgments (SHADOW ONLY).

Inspired by the useful architecture in buberlo/jev-trader:
    deterministic state -> atomic judgments -> code-owned policy -> hard risk -> outcome calibration

This module NEVER changes V8.1 eligibility, sizing, leverage, stops, targets, exits or orders.
It wraps the 4H candidate function only to observe qualified swing setups, persists compact
state/decision/outcome triples, settles a first-barrier research outcome from later CLOSED
4H candles, measures Brier/log-loss/ECE, and fits Platt scaling once enough settled samples
exist.

The research question is explicit:
    does calibrated abstention improve the outcome distribution versus taking every
    otherwise-qualified V8.1 4H setup?
"""
import json
import math
import os
import statistics
import sys
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import live_executor_v70 as live
import v810_swing4h_live as swing

ENABLED = os.getenv("V820_CALIBRATED_SWING_ENABLED", "true").lower() == "true"
SHADOW_ONLY = True
SYNC_SECONDS = max(120, int(os.getenv("V820_SYNC_SECONDS", "600")))
HORIZON_HOURS = max(12.0, min(120.0, float(os.getenv("V820_HORIZON_HOURS", "48"))))
MIN_PLATT_SAMPLE = max(20, int(os.getenv("V820_MIN_PLATT_SAMPLE", "30")))
TAKE_PROB = min(0.80, max(0.50, float(os.getenv("V820_TAKE_PROB", "0.60"))))
WATCH_PROB = min(TAKE_PROB, max(0.45, float(os.getenv("V820_WATCH_PROB", "0.52"))))
MAX_TAKE_DISAGREEMENT = min(0.40, max(0.05, float(os.getenv("V820_MAX_TAKE_DISAGREEMENT", "0.20"))))
REPORT_HOURS = sorted({
    max(0, min(23, int(x.strip())))
    for x in os.getenv("V820_REPORT_HOURS", "3,15").split(",") if x.strip()
})
REPORT_MINUTE = max(0, min(59, int(os.getenv("V820_REPORT_MINUTE", "0"))))
REPORT_TZ = os.getenv("V820_REPORT_TZ", "Europe/Madrid")

_LOCK = threading.RLock()
_PATCHED = False
_SCHEMA_READY = False
_MAIN = None
_ORIGINAL_CANDIDATE = None
_LAST_REPORT_SLOT = None


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _norm(v):
    return str(v or "").strip().upper()


def _clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, float(v)))


def _json(v):
    return live.Jsonb(v) if live.Jsonb else json.dumps(v)


def _sigmoid(x):
    if x >= 0:
        z = math.exp(-min(60.0, x))
        return 1.0 / (1.0 + z)
    z = math.exp(max(-60.0, x))
    return z / (1.0 + z)


def _logit(p):
    p = _clamp(p, 1e-6, 1.0 - 1e-6)
    return math.log(p / (1.0 - p))


def _source_key(c):
    meta = (c or {}).get("v810_swing") or {}
    return f"s4h_{int(_f(meta.get('candle_ts')))}_{_norm(c.get('symbol'))}_{_norm(c.get('direction'))}"


def _bucket(c):
    try:
        return str(((c or {}).get("v810_swing") or {}).get("bucket") or swing._bucket(c.get("symbol")))
    except Exception:
        return "UNKNOWN"


def _judgment(score, confidence, evidence):
    return {
        "score": round(_clamp(score / 100.0) * 100.0, 1),
        "confidence": round(_clamp(confidence), 3),
        "evidence": list(evidence or [])[:5],
    }


def _trend_judgment(c):
    reasons = [str(x) for x in (((c or {}).get("v810_swing") or {}).get("reasons") or [])]
    text = " | ".join(reasons)
    score = 20.0
    ev = []
    pairs = [
        ("1D close/EMA20/EMA50 aligned", 28),
        ("1D price on trend side of EMA20", 17),
        ("1D EMA20 slope aligned", 12),
        ("4H close/EMA20/EMA50 aligned", 28),
        ("4H price on trend side of EMA20", 17),
        ("4H EMA20 slope aligned", 12),
        ("1H aligned", 8),
    ]
    for needle, pts in pairs:
        if needle in text:
            score += pts
            ev.append(needle)
    return _judgment(min(100, score), 0.90 if len(ev) >= 4 else 0.75, ev)


def _setup_judgment(c):
    meta = (c or {}).get("v810_swing") or {}
    setup = _norm(meta.get("setup"))
    adx = _f(meta.get("adx4h"))
    score = 76.0 if setup == "BREAKOUT" else 72.0 if setup == "PULLBACK" else 55.0
    ev = [f"setup={setup or 'UNKNOWN'}"]
    if adx >= 30:
        score += 12; ev.append(f"4H ADX {adx:.1f}")
    elif adx >= 22:
        score += 7; ev.append(f"4H ADX {adx:.1f}")
    elif adx < 17:
        score -= 8; ev.append(f"soft 4H ADX {adx:.1f}")
    return _judgment(score, 0.85, ev)


def _momentum_judgment(c):
    meta = (c or {}).get("v810_swing") or {}
    direction = _norm((c or {}).get("direction"))
    rsi = _f(meta.get("rsi4h"), 50.0)
    adx = _f(meta.get("adx4h"))
    if direction == "LONG":
        rsi_q = 100.0 - min(100.0, abs(rsi - 61.0) * 4.0)
        if rsi > 76:
            rsi_q -= 18
    else:
        rsi_q = 100.0 - min(100.0, abs(rsi - 39.0) * 4.0)
        if rsi < 24:
            rsi_q -= 18
    adx_q = _clamp((adx - 12.0) / 28.0) * 100.0
    score = 0.60 * max(0.0, rsi_q) + 0.40 * adx_q
    return _judgment(score, 0.82, [f"4H RSI {rsi:.1f}", f"4H ADX {adx:.1f}"])


def _participation_judgment(c):
    meta = (c or {}).get("v810_swing") or {}
    rv = _f(meta.get("rv4h"), 0.0)
    oi = _f((c or {}).get("oi_score"), 0.0)
    rv_q = _clamp(rv / 1.6) * 100.0
    oi_q = _clamp(oi / 15.0) * 100.0
    score = 0.58 * rv_q + 0.42 * oi_q
    return _judgment(score, 0.78, [f"4H relative volume {rv:.2f}x", f"OI {oi:.0f}/15"])


def _macro_judgment(c):
    meta = (c or {}).get("v810_swing") or {}
    direction = _norm((c or {}).get("direction"))
    bucket = _norm(meta.get("bucket"))
    regime = _norm(meta.get("macro_regime"))
    event = _norm(meta.get("macro_event_risk"))
    score = 62.0
    ev = [f"macro={regime or 'UNKNOWN'}", f"event={event or 'UNKNOWN'}"]
    if event == "EXTREME":
        score = 5.0
    elif event == "HIGH":
        score -= 28
    elif event == "MEDIUM":
        score -= 12
    if bucket in {"CRYPTO", "INDEX"}:
        aligned = (direction == "LONG" and regime == "RISK_ON") or (direction == "SHORT" and regime == "RISK_OFF")
        conflict = (direction == "LONG" and regime == "RISK_OFF") or (direction == "SHORT" and regime == "RISK_ON")
        if aligned:
            score += 25; ev.append("macro direction aligned")
        elif conflict:
            score -= 25; ev.append("macro direction conflict")
    else:
        # The current macro engine is broad risk-on/off, not a reliable direct
        # directional model for every metal/energy contract. Keep it modest.
        score = min(score, 68.0)
    return _judgment(score, 0.68 if bucket in {"METAL", "ENERGY"} else 0.80, ev)


def _risk_geometry_judgment(c):
    plan = (c or {}).get("risk_plan") or {}
    stop_pct = _f(plan.get("stop_pct"))
    score = 82.0
    ev = [f"structural stop {stop_pct:.2f}%"]
    if 1.25 <= stop_pct <= 5.0:
        score += 10
    elif stop_pct < 1.0:
        score -= 30
    elif stop_pct > 6.5:
        score -= 18
    elif stop_pct > 5.0:
        score -= 8
    # Reward asymmetric target geometry without assuming it creates edge.
    try:
        r1 = abs(_f(plan.get("tp1")) - _f(c.get("price"))) / max(1e-12, abs(_f(c.get("price")) - _f(plan.get("stop"))))
        if r1 >= 1.4:
            score += 5; ev.append(f"TP1 {r1:.2f}R")
    except Exception:
        pass
    return _judgment(score, 0.92, ev)


def _atomic_judgments(c):
    return {
        "trend_quality": _trend_judgment(c),
        "setup_validity": _setup_judgment(c),
        "momentum_quality": _momentum_judgment(c),
        "participation_quality": _participation_judgment(c),
        "macro_alignment": _macro_judgment(c),
        "risk_geometry": _risk_geometry_judgment(c),
    }


def _raw_probability(c, judgments):
    # Code-owned composition. No single judgment may dominate the result.
    weights = {
        "trend_quality": 0.23,
        "setup_validity": 0.22,
        "momentum_quality": 0.18,
        "participation_quality": 0.12,
        "macro_alignment": 0.10,
        "risk_geometry": 0.15,
    }
    q = sum(weights[k] * (_f(judgments[k]["score"]) / 100.0) for k in weights)
    base_score = _f((c or {}).get("best_score"), 78.0)
    # Conservative prior: qualified V8.1 setups start around the middle and
    # calibration data must earn any stronger probability claim.
    logit = -0.35 + 2.1 * (q - 0.55) + 0.025 * (base_score - 78.0)
    return round(_clamp(_sigmoid(logit), 0.30, 0.80), 6)


def _disagreement(judgments):
    vals = [_f(v.get("score")) / 100.0 for v in judgments.values()]
    if len(vals) < 2:
        return 0.0
    return round(min(1.0, statistics.pstdev(vals) / 0.35), 6)


def _overall_confidence(judgments, disagreement):
    conf = sum(_f(v.get("confidence")) for v in judgments.values()) / max(1, len(judgments))
    conf *= (1.0 - 0.45 * disagreement)
    return round(_clamp(conf), 6)


def _ensure_schema():
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return True
    try:
        live._db(
            """CREATE TABLE IF NOT EXISTS fh_v82_swing_judgments (
                source_key TEXT PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                candle_ts DOUBLE PRECISION NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                setup TEXT,
                bucket TEXT,
                v81_score DOUBLE PRECISION,
                entry DOUBLE PRECISION,
                stop DOUBLE PRECISION,
                tp1 DOUBLE PRECISION,
                raw_probability DOUBLE PRECISION,
                calibrated_probability DOUBLE PRECISION,
                calibration_n INTEGER,
                disagreement DOUBLE PRECISION,
                confidence DOUBLE PRECISION,
                shadow_decision TEXT,
                judgments JSONB NOT NULL,
                state_snapshot JSONB NOT NULL,
                outcome_status TEXT,
                outcome_r DOUBLE PRECISION,
                outcome_price DOUBLE PRECISION,
                outcome_ts TIMESTAMPTZ,
                outcome_source TEXT,
                y_positive INTEGER,
                brier DOUBLE PRECISION,
                log_loss DOUBLE PRECISION,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"""
        )
        live._db("CREATE INDEX IF NOT EXISTS idx_v82_swing_time ON fh_v82_swing_judgments(observed_at DESC)")
        live._db("CREATE INDEX IF NOT EXISTS idx_v82_swing_settle ON fh_v82_swing_judgments(outcome_ts,shadow_decision)")
        live._db(
            """CREATE TABLE IF NOT EXISTS fh_v82_calibration (
                calibration_key TEXT PRIMARY KEY,
                fitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sample_n INTEGER NOT NULL,
                intercept DOUBLE PRECISION NOT NULL,
                slope DOUBLE PRECISION NOT NULL,
                brier_raw DOUBLE PRECISION,
                brier_calibrated DOUBLE PRECISION,
                log_loss_raw DOUBLE PRECISION,
                log_loss_calibrated DOUBLE PRECISION,
                ece_raw DOUBLE PRECISION,
                ece_calibrated DOUBLE PRECISION,
                payload JSONB NOT NULL
            )"""
        )
        _SCHEMA_READY = True
        return True
    except Exception as exc:
        live._diag(f"V8.2 schema retry: {type(exc).__name__}: {exc}")
        return False


def _settled_pairs(limit=5000):
    rows = live._db(
        """SELECT raw_probability,y_positive
           FROM fh_v82_swing_judgments
           WHERE outcome_ts IS NOT NULL
             AND raw_probability IS NOT NULL
             AND y_positive IN (0,1)
           ORDER BY outcome_ts DESC LIMIT %s""",
        (int(limit),), "all"
    ) or []
    return [(_f(p), int(y)) for p,y in rows if p is not None and y is not None]


def _platt_fit(pairs):
    n = len(pairs)
    if n < MIN_PLATT_SAMPLE:
        return 0.0, 1.0, n
    xs = [_logit(p) for p,_ in pairs]
    ys = [float(y) for _,y in pairs]
    a, b = 0.0, 1.0
    # Small deterministic batch gradient descent; bounded for stability.
    lr = 0.08
    reg = 0.002
    for _ in range(350):
        ga = 0.0
        gb = 0.0
        for x,y in zip(xs,ys):
            pred = _sigmoid(a + b*x)
            err = pred - y
            ga += err
            gb += err*x
        ga = ga/n + reg*a
        gb = gb/n + reg*(b-1.0)
        a -= lr*ga
        b -= lr*gb
        a = max(-3.0, min(3.0, a))
        b = max(0.15, min(4.0, b))
    return a,b,n


def _apply_calibration(raw_p):
    pairs = _settled_pairs()
    a,b,n = _platt_fit(pairs)
    if n < MIN_PLATT_SAMPLE:
        return raw_p,n,a,b
    return round(_clamp(_sigmoid(a + b*_logit(raw_p)),0.02,0.98),6),n,a,b


def _shadow_policy(prob, disagreement, confidence, calibration_n):
    # This policy is research-only. V8.1 execution never reads it.
    if prob >= TAKE_PROB and disagreement <= MAX_TAKE_DISAGREEMENT and confidence >= 0.60:
        return "TAKE"
    if prob >= WATCH_PROB:
        return "WATCH"
    return "ABSTAIN"


def _state_snapshot(c):
    meta=(c or {}).get("v810_swing") or {}
    plan=(c or {}).get("risk_plan") or {}
    return {
        "v81_score": round(_f(c.get("best_score")),2),
        "oi": round(_f(c.get("oi_score")),2),
        "setup": _norm(meta.get("setup")),
        "bucket": _norm(meta.get("bucket")),
        "rsi4h": round(_f(meta.get("rsi4h")),2),
        "adx4h": round(_f(meta.get("adx4h")),2),
        "rv4h": round(_f(meta.get("rv4h")),3),
        "macro_regime": _norm(meta.get("macro_regime")),
        "macro_event_risk": _norm(meta.get("macro_event_risk")),
        "entry": _f(c.get("price")),
        "stop": _f(plan.get("stop")),
        "stop_pct": round(_f(plan.get("stop_pct")),4),
        "tp1": _f(plan.get("tp1")),
        "tp2": _f(plan.get("tp2")),
        "tp3": _f(plan.get("tp3")),
        "timeframes": str(meta.get("timeframes") or "1D/4H/1H"),
    }


def _observe(c):
    if not ENABLED or not isinstance(c,dict):
        return c
    try:
        if not _ensure_schema():
            return c
        key=_source_key(c)
        judgments=_atomic_judgments(c)
        raw=_raw_probability(c,judgments)
        dis=_disagreement(judgments)
        conf=_overall_confidence(judgments,dis)
        calibrated,n,a,b=_apply_calibration(raw)
        decision=_shadow_policy(calibrated,dis,conf,n)
        meta=c.get("v810_swing") or {}
        plan=c.get("risk_plan") or {}
        state=_state_snapshot(c)
        payload={
            "version":"8.2.0-calibrated-atomic-shadow",
            "shadow_only":True,
            "policy_owner":"code",
            "risk_owner":"V8.1/executor",
            "calibration":{"n":n,"intercept":round(a,6),"slope":round(b,6)},
            "question":"Does calibrated abstention improve qualified V8.1 4H setup outcomes?",
        }
        live._db(
            """INSERT INTO fh_v82_swing_judgments(
                source_key,candle_ts,symbol,direction,setup,bucket,v81_score,
                entry,stop,tp1,raw_probability,calibrated_probability,calibration_n,
                disagreement,confidence,shadow_decision,judgments,state_snapshot,payload
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(source_key) DO UPDATE SET
                raw_probability=EXCLUDED.raw_probability,
                calibrated_probability=EXCLUDED.calibrated_probability,
                calibration_n=EXCLUDED.calibration_n,
                disagreement=EXCLUDED.disagreement,
                confidence=EXCLUDED.confidence,
                shadow_decision=EXCLUDED.shadow_decision,
                judgments=EXCLUDED.judgments,
                state_snapshot=EXCLUDED.state_snapshot,
                payload=EXCLUDED.payload,
                updated_at=NOW()""",
            (
                key,_f(meta.get("candle_ts")),str(c.get("symbol")),_norm(c.get("direction")),
                _norm(meta.get("setup")),_norm(meta.get("bucket")),_f(c.get("best_score")),
                _f(c.get("price")),_f(plan.get("stop")),_f(plan.get("tp1")),
                raw,calibrated,n,dis,conf,decision,_json(judgments),_json(state),_json(payload)
            )
        )
        c["v820_shadow"]={
            "prob_positive_first_barrier":calibrated,
            "raw_probability":raw,
            "calibration_n":n,
            "disagreement":dis,
            "confidence":conf,
            "decision":decision,
            "judgments":judgments,
        }
        live._diag(
            f"V8.2 SHADOW {c.get('symbol')} {_norm(c.get('direction'))} "
            f"V81={_f(c.get('best_score')):.1f} p_raw={raw:.3f} p_cal={calibrated:.3f} "
            f"n={n} disagree={dis:.2f} conf={conf:.2f} -> {decision}"
        )
    except Exception as exc:
        # Research must never break the live candidate path.
        try: live._diag(f"V8.2 observe fail-open: {type(exc).__name__}: {exc}")
        except Exception: pass
    return c


def _closed_h4(main,symbol):
    try:
        df=main.get_candles(symbol,"Hour4")
        if df is None or len(df)<3:
            return None
        return df.iloc[:-1].copy()
    except Exception:
        return None


def _settle_one(main,row):
    key,candle_ts,symbol,direction,entry,stop,tp1,observed_at=row
    df=_closed_h4(main,symbol)
    if df is None or len(df)<2:
        return False
    future=df[df["time"].astype(float) > _f(candle_ts)].copy()
    if future.empty:
        return False
    direction=_norm(direction)
    risk=abs(_f(entry)-_f(stop))
    if risk<=0:
        return False

    for _,bar in future.iterrows():
        hi=_f(bar.get("high")); lo=_f(bar.get("low")); close=_f(bar.get("close"))
        ts=_f(bar.get("time"))
        if direction=="LONG":
            stop_hit=lo<=_f(stop)
            tp_hit=hi>=_f(tp1)
        else:
            stop_hit=hi>=_f(stop)
            tp_hit=lo<=_f(tp1)
        # Conservative ambiguity rule: if both occur in one 4H candle, count STOP.
        if stop_hit:
            status="STOP_FIRST" if not tp_hit else "BOTH_SAME_BAR_STOP_FIRST"
            final_r=-1.0
            px=_f(stop)
        elif tp_hit:
            status="TP1_FIRST"
            final_r=abs(_f(tp1)-_f(entry))/risk
            px=_f(tp1)
        else:
            continue
        y=1 if final_r>0 else 0
        prow=live._db(
            "SELECT calibrated_probability FROM fh_v82_swing_judgments WHERE source_key=%s",
            (str(key),),"one"
        )
        p=_f(prow[0]) if prow and prow[0] is not None else 0.5
        brier=(p-y)**2
        ll=-(y*math.log(max(1e-9,p))+(1-y)*math.log(max(1e-9,1-p)))
        live._db(
            """UPDATE fh_v82_swing_judgments
               SET outcome_status=%s,outcome_r=%s,outcome_price=%s,
                   outcome_ts=%s,outcome_source='CLOSED_4H_FIRST_BARRIER',
                   y_positive=%s,brier=%s,log_loss=%s,updated_at=NOW()
               WHERE source_key=%s""",
            (status,final_r,px,datetime.fromtimestamp(ts,tz=timezone.utc),
             y,round(brier,6),round(ll,6),str(key))
        )
        return True

    age_h=(datetime.now(timezone.utc)-observed_at).total_seconds()/3600.0 if observed_at else 0.0
    if age_h < HORIZON_HOURS:
        return False

    last=future.iloc[-1]
    px=_f(last.get("close"))
    sign=1.0 if direction=="LONG" else -1.0
    final_r=sign*(px-_f(entry))/risk
    final_r=max(-1.0,min(1.5,final_r))
    y=1 if final_r>0 else 0
    prow=live._db(
        "SELECT calibrated_probability FROM fh_v82_swing_judgments WHERE source_key=%s",
        (str(key),),"one"
    )
    p=_f(prow[0]) if prow and prow[0] is not None else 0.5
    brier=(p-y)**2
    ll=-(y*math.log(max(1e-9,p))+(1-y)*math.log(max(1e-9,1-p)))
    live._db(
        """UPDATE fh_v82_swing_judgments
           SET outcome_status='HORIZON_MARK',outcome_r=%s,outcome_price=%s,
               outcome_ts=NOW(),outcome_source='CLOSED_4H_HORIZON_MARK',
               y_positive=%s,brier=%s,log_loss=%s,updated_at=NOW()
           WHERE source_key=%s""",
        (final_r,px,y,round(brier,6),round(ll,6),str(key))
    )
    return True


def _sync_outcomes(main):
    if not _ensure_schema():
        return 0
    rows=live._db(
        """SELECT source_key,candle_ts,symbol,direction,entry,stop,tp1,observed_at
           FROM fh_v82_swing_judgments
           WHERE outcome_ts IS NULL
           ORDER BY observed_at
           LIMIT 120""",
        (),"all"
    ) or []
    n=0
    for row in rows:
        try:
            if _settle_one(main,row):
                n+=1
        except Exception as exc:
            live._diag(f"V8.2 settle {row[2] if len(row)>2 else '?'} warning: {type(exc).__name__}: {exc}")
        time.sleep(0.04)
    return n


def _metrics(pairs, calibrated=False, a=0.0, b=1.0):
    if not pairs:
        return {"n":0,"brier":None,"log_loss":None,"ece":None}
    obs=[]
    for raw,y in pairs:
        p=_sigmoid(a+b*_logit(raw)) if calibrated else raw
        p=_clamp(p,1e-6,1-1e-6)
        obs.append((p,float(y)))
    brier=sum((p-y)**2 for p,y in obs)/len(obs)
    ll=sum(-(y*math.log(p)+(1-y)*math.log(1-p)) for p,y in obs)/len(obs)
    ece=0.0
    bins=5
    for j in range(bins):
        lo=j/bins; hi=(j+1)/bins
        bucket=[(p,y) for p,y in obs if (lo<=p<hi) or (j==bins-1 and p==1.0)]
        if not bucket: continue
        avgp=sum(p for p,_ in bucket)/len(bucket)
        avgy=sum(y for _,y in bucket)/len(bucket)
        ece += (len(bucket)/len(obs))*abs(avgp-avgy)
    return {"n":len(obs),"brier":brier,"log_loss":ll,"ece":ece}


def _save_calibration():
    pairs=_settled_pairs()
    a,b,n=_platt_fit(pairs)
    raw=_metrics(pairs)
    cal=_metrics(pairs,True,a,b)
    payload={"version":"8.2.0","method":"Platt(logit(raw_p))","min_sample":MIN_PLATT_SAMPLE}
    live._db(
        """INSERT INTO fh_v82_calibration(
             calibration_key,sample_n,intercept,slope,brier_raw,brier_calibrated,
             log_loss_raw,log_loss_calibrated,ece_raw,ece_calibrated,payload
           ) VALUES('GLOBAL',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT(calibration_key) DO UPDATE SET
             fitted_at=NOW(),sample_n=EXCLUDED.sample_n,intercept=EXCLUDED.intercept,
             slope=EXCLUDED.slope,brier_raw=EXCLUDED.brier_raw,
             brier_calibrated=EXCLUDED.brier_calibrated,
             log_loss_raw=EXCLUDED.log_loss_raw,
             log_loss_calibrated=EXCLUDED.log_loss_calibrated,
             ece_raw=EXCLUDED.ece_raw,ece_calibrated=EXCLUDED.ece_calibrated,
             payload=EXCLUDED.payload""",
        (
            n,a,b,raw.get("brier"),cal.get("brier"),raw.get("log_loss"),cal.get("log_loss"),
            raw.get("ece"),cal.get("ece"),_json(payload)
        )
    )
    return a,b,n,raw,cal


def _decision_stats():
    rows=live._db(
        """SELECT shadow_decision,COUNT(*),AVG(outcome_r),SUM(outcome_r),
                  AVG(CASE WHEN y_positive=1 THEN 1.0 ELSE 0.0 END)
           FROM fh_v82_swing_judgments
           WHERE outcome_ts IS NOT NULL
           GROUP BY shadow_decision ORDER BY shadow_decision""",
        (),"all"
    ) or []
    return [
        {"decision":str(d),"n":int(n or 0),"avg_r":_f(ar),"sum_r":_f(sr),"positive_rate":_f(pr)}
        for d,n,ar,sr,pr in rows
    ]


def _report(slot):
    try:
        a,b,n,raw,cal=_save_calibration()
        stats=_decision_stats()
        pieces=[
            f"V8.2 CALIBRATION {slot}",
            f"settled={n} platt={'ACTIVE' if n>=MIN_PLATT_SAMPLE else 'WARMING'} a={a:+.3f} b={b:.3f}",
        ]
        if raw.get("brier") is not None:
            pieces.append(
                f"Brier raw/cal={raw['brier']:.3f}/{cal['brier']:.3f} "
                f"logloss={raw['log_loss']:.3f}/{cal['log_loss']:.3f} "
                f"ECE={raw['ece']:.3f}/{cal['ece']:.3f}"
            )
        for s in stats:
            pieces.append(
                f"{s['decision']}: n={s['n']} avg={s['avg_r']:+.2f}R "
                f"sum={s['sum_r']:+.2f}R positive={s['positive_rate']*100:.0f}%"
            )
        live._diag(" | ".join(pieces))
    except Exception as exc:
        live._diag(f"V8.2 report warning: {type(exc).__name__}: {exc}")


def _report_slot_now():
    try:
        now=datetime.now(ZoneInfo(REPORT_TZ))
    except Exception:
        now=datetime.now(timezone.utc)
    for hour in REPORT_HOURS:
        if now.hour==hour and now.minute>=REPORT_MINUTE and now.minute<REPORT_MINUTE+15:
            return f"{now.date().isoformat()}-{hour:02d}"
    return None


def _loop(main):
    global _LAST_REPORT_SLOT
    time.sleep(35)
    while ENABLED:
        try:
            settled=_sync_outcomes(main)
            if settled:
                live._diag(f"V8.2 settled {settled} swing research outcome(s)")
            _save_calibration()
            slot=_report_slot_now()
            if slot and slot!=_LAST_REPORT_SLOT:
                _LAST_REPORT_SLOT=slot
                _report(slot)
        except Exception as exc:
            live._diag(f"V8.2 loop warning: {type(exc).__name__}: {exc}")
        time.sleep(SYNC_SECONDS)


def _patch():
    global _PATCHED,_ORIGINAL_CANDIDATE
    with _LOCK:
        if _PATCHED:
            return True
        if not hasattr(swing,"_candidate"):
            return False
        _ORIGINAL_CANDIDATE=swing._candidate
        def candidate(main,scan_row):
            c=_ORIGINAL_CANDIDATE(main,scan_row)
            if c is not None:
                return _observe(c)
            return c
        swing._candidate=candidate
        _PATCHED=True
        live._diag(
            f"V8.2 Calibrated Swing Brain armed SHADOW_ONLY=True "
            f"atomic=6 policy=TAKE/WATCH/ABSTAIN calibration=Brier+LogLoss+ECE+Platt "
            f"take_p>={TAKE_PROB:.2f} disagreement<={MAX_TAKE_DISAGREEMENT:.2f} "
            f"min_platt_n={MIN_PLATT_SAMPLE} live_gate_unchanged=True"
        )
        return True


def _bootstrap():
    global _MAIN
    deadline=time.time()+360
    while time.time()<deadline:
        main=sys.modules.get("__main__")
        try:
            if main is not None and hasattr(main,"get_candles") and _patch():
                _MAIN=main
                _ensure_schema()
                threading.Thread(target=_loop,args=(main,),name="V820Calibration",daemon=True).start()
                return
        except Exception as exc:
            try: live._diag(f"V8.2 bootstrap retry: {type(exc).__name__}: {exc}")
            except Exception: pass
        time.sleep(0.5)
    try: live._diag("V8.2 bootstrap gave up; research brain not armed")
    except Exception: pass


if ENABLED:
    threading.Thread(target=_bootstrap,name="V820Bootstrap",daemon=True).start()
    live._diag("V8.2 Calibrated Swing Brain bootstrap armed")
