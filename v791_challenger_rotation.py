"""FuturesHunter V7.9.1 challenger rotation + missed-trade auditor.

This overlay patches the main V7.1 live-candidate gate *after* FuturesHunter_Render
has finished defining it. It does two things:

1) Conservative crypto slot rotation. If a truly exceptional new crypto LONG/SHORT
   is blocked only by the one-crypto-direction slot (plus the existing weak-BTC /
   macro conflict soft veto), compare it with the bot-owned incumbent. Rotate only
   when the incumbent is old and objectively degraded by its live supervisor.

2) Durable counterfactual audit of skipped live candidates. The existing
   fh_v71_challenger table is extended with shadow entry/stop/TP tracking so live-only
   skips can settle even when no paper trade exists. This lets filter changes be
   judged from a sample instead of hindsight on one missed winner.

RANGE_SCALPER is untouched. Manual/untracked exchange positions are never closed.
"""
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone

import live_executor_v70 as live

ENABLED = os.getenv("V791_CHALLENGER_ENABLED", "true").lower() == "true"
ROTATION_MODE = os.getenv("V791_ROTATION_MODE", "live").strip().lower()
MIN_CHALLENGER_SCORE = max(85.0, float(os.getenv("V791_ROTATION_MIN_SCORE", "90")))
MIN_CHALLENGER_RAW = max(70.0, float(os.getenv("V791_ROTATION_MIN_RAW", "78")))
MIN_CHALLENGER_OI = max(6.0, float(os.getenv("V791_ROTATION_MIN_OI", "10")))
MIN_SCORE_DELTA = max(5.0, float(os.getenv("V791_ROTATION_MIN_SCORE_DELTA", "10")))
MAX_INCUMBENT_HEALTH = min(70.0, float(os.getenv("V791_ROTATION_MAX_INCUMBENT_HEALTH", "50")))
MIN_INCUMBENT_AGE_MIN = max(10.0, float(os.getenv("V791_ROTATION_MIN_INCUMBENT_AGE_MIN", "30")))
ROTATION_COOLDOWN_MIN = max(15.0, float(os.getenv("V791_ROTATION_COOLDOWN_MIN", "60")))
AUDIT_ENABLED = os.getenv("V791_MISSED_AUDIT_ENABLED", "true").lower() == "true"
AUDIT_SECONDS = max(30, int(os.getenv("V791_MISSED_AUDIT_SECONDS", "60")))
AUDIT_EXPIRY_HOURS = max(1.0, float(os.getenv("V791_MISSED_AUDIT_EXPIRY_HOURS", "24")))

_PATCH_LOCK = threading.RLock()
_PATCHED = False
_LAST_ROTATION_TS = 0.0
_MAIN = None


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _norm(v):
    return str(v or "").strip().upper()


def _utcnow():
    return datetime.now(timezone.utc)


def _minutes_since(ts):
    if ts is None:
        return 1e9
    try:
        if isinstance(ts, (int, float)):
            return max(0.0, (time.time() - float(ts)) / 60.0)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return max(0.0, (_utcnow() - ts).total_seconds() / 60.0)
    except Exception:
        return 1e9


def _json(v):
    if live.Jsonb:
        return live.Jsonb(v)
    return json.dumps(v)


def _candidate_metrics(result):
    return {
        "symbol": str((result or {}).get("symbol") or ""),
        "direction": _norm((result or {}).get("direction")),
        "score": _f((result or {}).get("best_score")),
        "raw": _f((result or {}).get("raw_score")),
        "oi": _f((result or {}).get("oi_score")),
        "regime": _norm((result or {}).get("regime")),
        "state": _norm((result or {}).get("signal_state") or (result or {}).get("state")),
    }


def _is_correlation_block(reasons):
    text = "; ".join(str(x) for x in (reasons or [])).lower()
    return "live correlation guard" in text and "crypto" in text


def _has_disqualifying_blocker(reasons):
    """Only correlation + weak-BTC/macro soft conflict may be overridden."""
    allowed = (
        "live correlation guard",
        "crypto regime conflict",
        "weak btc",
        "macro conflict",
    )
    hard_words = (
        "spread", "execution drag", "resistance", "support", "drift", "stop too wide",
        "stale", "invalidation", "event risk", "extreme", "degraded-regime",
        "strategy confirmation required", "selector threshold", "daily", "halt",
    )
    for reason in (reasons or []):
        s = str(reason).lower()
        if any(w in s for w in hard_words):
            return True
        if not any(a in s for a in allowed):
            return True
    return False


def _latest_scan(symbol):
    row = live._db(
        """SELECT scan_time,selected_direction,signal_state,selected_regime,
                  best_score,raw_score,weighted_score,oi_score
           FROM fh_research_scans WHERE symbol=%s
           ORDER BY scan_time DESC LIMIT 1""",
        (symbol,), "one"
    )
    if not row:
        return None
    return {
        "scan_time": row[0], "direction": _norm(row[1]), "state": _norm(row[2]),
        "regime": _norm(row[3]), "score": _f(row[4]), "raw": _f(row[5]),
        "weighted": _f(row[6]), "oi": _f(row[7]),
    }


def _supervisor(signal_id):
    try:
        row = live._db(
            """SELECT current_state,current_health,last_eval_time
               FROM fh_trade_supervisor WHERE signal_id=%s""",
            (signal_id,), "one"
        )
    except Exception:
        row = None
    if not row:
        return {"state": "", "health": 100.0, "last_eval": None}
    return {"state": _norm(row[0]), "health": _f(row[1], 100.0), "last_eval": row[2]}


def _open_bot_positions(direction):
    rows = live._db(
        """SELECT signal_id,symbol,direction,position_id,opened_at,payload,peak_favorable_r
           FROM fh_live_trades
           WHERE status='OPEN' AND direction=%s
           ORDER BY opened_at ASC""",
        (_norm(direction),), "all"
    ) or []
    out = []
    for r in rows:
        payload = r[5] if isinstance(r[5], dict) else {}
        if _norm(payload.get("strategyTag") or "CORE") != "CORE":
            continue
        out.append({
            "signal_id": str(r[0]), "symbol": str(r[1]), "direction": _norm(r[2]),
            "position_id": int(r[3] or 0), "opened_at": r[4], "payload": payload,
            "peak_r": _f(r[6]),
        })
    return out


def _choose_incumbent(direction, candidate_symbol):
    choices = []
    for p in _open_bot_positions(direction):
        if p["symbol"] == candidate_symbol:
            continue
        snap = _latest_scan(p["symbol"]) or {}
        sup = _supervisor(p["signal_id"])
        age = _minutes_since(p["opened_at"])
        weak_state = sup["state"] in {"DEFENSIVE", "EXIT_WARNING"}
        if age < MIN_INCUMBENT_AGE_MIN:
            continue
        if not (sup["health"] <= MAX_INCUMBENT_HEALTH or weak_state):
            continue
        choices.append((sup["health"], _f(snap.get("score"), 0.0), -age, p, snap, sup))
    if not choices:
        return None
    choices.sort(key=lambda x: (x[0], x[1], x[2]))
    _, _, _, p, snap, sup = choices[0]
    return p, snap, sup


def _rotation_decision(result, gate):
    global _LAST_ROTATION_TS
    if not ENABLED or ROTATION_MODE not in {"live", "shadow"}:
        return None
    reasons = list((gate or {}).get("reasons") or [])
    if (gate or {}).get("eligible") or not _is_correlation_block(reasons):
        return None
    if _has_disqualifying_blocker(reasons):
        return None
    if time.time() - _LAST_ROTATION_TS < ROTATION_COOLDOWN_MIN * 60.0:
        return None

    c = _candidate_metrics(result)
    if c["direction"] not in {"LONG", "SHORT"}:
        return None
    if c["regime"] not in {"BREAKOUT", "TREND_CONTINUATION"}:
        return None
    if c["score"] < MIN_CHALLENGER_SCORE or c["raw"] < MIN_CHALLENGER_RAW or c["oi"] < MIN_CHALLENGER_OI:
        return None

    found = _choose_incumbent(c["direction"], c["symbol"])
    if not found:
        return None
    incumbent, snap, sup = found
    incumbent_score = _f(snap.get("score"), 0.0)
    delta = c["score"] - incumbent_score
    if delta < MIN_SCORE_DELTA:
        return None

    return {
        "candidate": c,
        "incumbent": incumbent,
        "incumbent_scan": snap,
        "incumbent_supervisor": sup,
        "score_delta": delta,
        "reasons": reasons,
    }


def _close_incumbent(decision):
    global _LAST_ROTATION_TS
    p = decision["incumbent"]
    c = decision["candidate"]
    pos = live._active_position(p["symbol"], p["position_id"])
    if not pos:
        live._diag(f"V7.9.1 ROTATION skipped: tracked incumbent {p['symbol']} not active on MEXC")
        return False
    vol = _f(pos.get("holdVol") or pos.get("vol") or pos.get("positionVol"))
    if vol <= 0:
        return False

    if ROTATION_MODE == "shadow":
        live._diag(
            f"V7.9.1 ROTATION SHADOW {p['symbol']}->{c['symbol']} {c['direction']} "
            f"delta={decision['score_delta']:+.1f} incumbent_health={decision['incumbent_supervisor']['health']:.1f}"
        )
        return False

    ok, detail = live._partial_market_close(
        p["symbol"], p["direction"], p["position_id"], vol, p["signal_id"], "ROT1"
    )
    if not ok:
        live._diag(f"V7.9.1 ROTATION close failed {p['symbol']}: {detail}")
        return False

    _LAST_ROTATION_TS = time.time()
    patch = {
        "rotationExit": True,
        "rotationTs": _LAST_ROTATION_TS,
        "rotationChallenger": c["symbol"],
        "rotationChallengerScore": c["score"],
        "rotationIncumbentScore": _f(decision["incumbent_scan"].get("score")),
        "rotationIncumbentHealth": _f(decision["incumbent_supervisor"].get("health")),
    }
    try:
        live._db(
            """UPDATE fh_live_trades SET payload=COALESCE(payload,'{}'::jsonb) || %s,updated_at=NOW()
               WHERE signal_id=%s""",
            (_json(patch), p["signal_id"])
        )
    except Exception:
        pass

    live._diag(
        f"V7.9.1 ROTATION LIVE {p['symbol']}->{c['symbol']} {c['direction']} "
        f"candidate={c['score']:.1f}/{c['regime']} OI={c['oi']:.0f} "
        f"incumbent={_f(decision['incumbent_scan'].get('score')):.1f} "
        f"health={decision['incumbent_supervisor']['health']:.1f} "
        f"state={decision['incumbent_supervisor']['state']} delta={decision['score_delta']:+.1f}"
    )
    live._msg(
        f"🔄 V7.9.1 CHALLENGER ROTATION\n"
        f"Closed stale {p['symbol']} {p['direction']} to free the crypto slot.\n"
        f"Challenger: {c['symbol']} {c['direction']} {c['score']:.1f} ({c['regime']}) OI {c['oi']:.0f}/15\n"
        f"Incumbent health {decision['incumbent_supervisor']['health']:.0f}/100; "
        f"Core {_f(decision['incumbent_scan'].get('score')):.1f}; quality delta {decision['score_delta']:+.1f}."
    )
    return True


def _ensure_audit_schema():
    if not AUDIT_ENABLED:
        return False
    try:
        ddls = (
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_entry DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_stop DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_risk DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_tp1 DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_tp2 DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_tp3 DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_tracking_start DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_expiry_ts DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_last_checked DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_status TEXT",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_tp1_hit BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_tp2_hit BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_tp3_hit BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_closed_time DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_final_r DOUBLE PRECISION",
            "ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_skip_class TEXT",
        )
        for ddl in ddls:
            live._db(ddl)
        live._db("CREATE INDEX IF NOT EXISTS idx_fh_v71_audit_open ON fh_v71_challenger(audit_status,audit_tracking_start)")
        return True
    except Exception:
        return False


def _stub_levels(main, result, source_key):
    try:
        stub = main._v71_live_stub_trade(result, source_key) or {}
    except Exception:
        stub = {}
    entry = _f(stub.get("entry") or stub.get("paper_entry") or (result or {}).get("entry"))
    stop = _f(stub.get("stop") or stub.get("stop_price") or (result or {}).get("stop"))
    tp1 = _f(stub.get("tp1") or stub.get("tp1_price") or (result or {}).get("tp1"))
    tp2 = _f(stub.get("tp2") or stub.get("tp2_price") or (result or {}).get("tp2"))
    tp3 = _f(stub.get("tp3") or stub.get("tp3_price") or (result or {}).get("tp3"))
    risk = abs(entry - stop) if entry and stop else 0.0
    return stub, entry, stop, risk, tp1, tp2, tp3


def _audit_skip_class(reasons):
    text = "; ".join(str(x) for x in (reasons or [])).lower()
    if "live correlation guard" in text:
        return "CORRELATION_SLOT"
    if "macro" in text or "btc" in text:
        return "REGIME"
    if "execution drag" in text or "spread" in text:
        return "COST"
    if "resistance" in text or "support" in text:
        return "STRUCTURE"
    if "degraded" in text or "strategy confirmation" in text:
        return "QUALITY"
    return "OTHER"


def _arm_missed_candidate(main, result, source_key, outcome):
    if not AUDIT_ENABLED or not isinstance(outcome, dict) or outcome.get("executed"):
        return
    gate = (result or {}).get("v70_live_gate") or {}
    if gate.get("eligible"):
        return
    c = _candidate_metrics(result)
    if c["state"] and c["state"] != "ENTRY":
        return
    reasons = list(gate.get("reasons") or [])
    stub, entry, stop, risk, tp1, tp2, tp3 = _stub_levels(main, result, source_key)
    if not (entry > 0 and stop > 0 and risk > 0 and tp1 > 0 and tp2 > 0 and tp3 > 0):
        return
    tracking_start = (int(time.time() // 60) + 1) * 60
    expiry = tracking_start + AUDIT_EXPIRY_HOURS * 3600.0
    try:
        live._db(
            """UPDATE fh_v71_challenger SET
                 audit_entry=%s,audit_stop=%s,audit_risk=%s,audit_tp1=%s,audit_tp2=%s,audit_tp3=%s,
                 audit_tracking_start=%s,audit_expiry_ts=%s,audit_last_checked=%s,audit_status='OPEN',
                 audit_tp1_hit=FALSE,audit_tp2_hit=FALSE,audit_tp3_hit=FALSE,audit_closed_time=NULL,
                 audit_final_r=NULL,audit_skip_class=%s
               WHERE source_key=%s AND decision='SKIP' AND (audit_status IS NULL OR audit_status='')""",
            (entry, stop, risk, tp1, tp2, tp3, tracking_start, expiry, tracking_start - 1,
             _audit_skip_class(reasons), str(source_key))
        )
        live._diag(
            f"V7.9.1 MISSED AUDIT armed {c['symbol']} {c['direction']} score={c['score']:.1f} "
            f"class={_audit_skip_class(reasons)} entry={entry:g} stop={stop:g} tp3={tp3:g}"
        )
    except Exception as exc:
        live._diag(f"V7.9.1 MISSED AUDIT arm error {c['symbol']}: {type(exc).__name__}: {exc}")


def _open_audits():
    rows = live._db(
        """SELECT source_key,symbol,direction,audit_entry,audit_stop,audit_risk,audit_tp1,audit_tp2,audit_tp3,
                  audit_tracking_start,audit_expiry_ts,audit_last_checked,audit_tp1_hit,audit_tp2_hit,audit_tp3_hit
           FROM fh_v71_challenger WHERE decision='SKIP' AND audit_status='OPEN'
           ORDER BY audit_tracking_start""",
        (), "all"
    ) or []
    keys = ["source_key","symbol","direction","entry","stop","risk","tp1","tp2","tp3",
            "tracking_start","expiry_ts","last_checked","tp1_hit","tp2_hit","tp3_hit"]
    return [dict(zip(keys, r)) for r in rows]


def _touches(direction, high, low, row):
    if _norm(direction) == "LONG":
        return {
            "stop": low <= _f(row["stop"]),
            "tp1": high >= _f(row["tp1"]),
            "tp2": high >= _f(row["tp2"]),
            "tp3": high >= _f(row["tp3"]),
        }
    return {
        "stop": high >= _f(row["stop"]),
        "tp1": low <= _f(row["tp1"]),
        "tp2": low <= _f(row["tp2"]),
        "tp3": low <= _f(row["tp3"]),
    }


def _mark_audit(row, status, final_r, last_checked, tp1, tp2, tp3, closed=None):
    live._db(
        """UPDATE fh_v71_challenger SET audit_status=%s,audit_final_r=%s,audit_last_checked=%s,
                  audit_tp1_hit=%s,audit_tp2_hit=%s,audit_tp3_hit=%s,audit_closed_time=%s
           WHERE source_key=%s""",
        (status, final_r, last_checked, bool(tp1), bool(tp2), bool(tp3), closed, row["source_key"])
    )


def _settle_one(main, row):
    df = main.get_candles(row["symbol"], "Min1")
    if df is None or len(df) == 0:
        return None
    latest_price = _f(df.iloc[-1].get("close"))
    last_checked = _f(row.get("last_checked"), _f(row.get("tracking_start")) - 1)
    t1, t2, t3 = bool(row.get("tp1_hit")), bool(row.get("tp2_hit")), bool(row.get("tp3_hit"))
    terminal = None
    closed = None
    for _, candle in df.iterrows():
        ts = _f(main.normalize_candle_time(candle.get("time")))
        if ts <= last_checked or ts < _f(row.get("tracking_start")):
            continue
        last_checked = max(last_checked, ts)
        hit = _touches(row["direction"], _f(candle.get("high")), _f(candle.get("low")), row)
        if hit["stop"] and (hit["tp1"] or hit["tp2"] or hit["tp3"]):
            terminal = ("AMBIGUOUS", None); closed = ts; break
        if hit["stop"]:
            if t2:
                final_r = 1.25
            elif t1:
                final_r = 0.25
            else:
                final_r = -1.0
            terminal = ("STOP", final_r); closed = ts; break
        if hit["tp3"]:
            t1 = t2 = t3 = True
            terminal = ("TP3", 2.25)
            closed = ts; break
        if hit["tp2"]:
            t1 = t2 = True
        elif hit["tp1"]:
            t1 = True

    if terminal:
        _mark_audit(row, terminal[0], terminal[1], last_checked, t1, t2, t3, closed)
        live._diag(
            f"V7.9.1 MISSED AUDIT settled {row['symbol']} {row['direction']} -> {terminal[0]} "
            f"R={terminal[1]} TP={int(t1)}/{int(t2)}/{int(t3)}"
        )
        return terminal[0]

    if time.time() >= _f(row.get("expiry_ts")):
        if row["risk"] and latest_price:
            if _norm(row["direction"]) == "LONG":
                r = (latest_price - _f(row["entry"])) / _f(row["risk"])
            else:
                r = (_f(row["entry"]) - latest_price) / _f(row["risk"])
            r = round(max(-1.0, min(2.25, r)), 4)
        else:
            r = 0.0
        _mark_audit(row, "EXPIRED", r, last_checked, t1, t2, t3, time.time())
        return "EXPIRED"

    _mark_audit(row, "OPEN", None, last_checked, t1, t2, t3, None)
    return None


def _audit_summary():
    try:
        row = live._db(
            """SELECT COUNT(*),
                      COUNT(*) FILTER(WHERE audit_status IS NOT NULL),
                      COUNT(*) FILTER(WHERE audit_status='OPEN'),
                      COUNT(*) FILTER(WHERE audit_status IS NOT NULL AND audit_status<>'OPEN'),
                      COUNT(*) FILTER(WHERE audit_tp1_hit),
                      COUNT(*) FILTER(WHERE audit_tp2_hit),
                      COUNT(*) FILTER(WHERE audit_tp3_hit),
                      COALESCE(SUM(audit_final_r) FILTER(WHERE audit_final_r IS NOT NULL),0)
               FROM fh_v71_challenger WHERE decision='SKIP'""",
            (), "one"
        )
    except Exception:
        row = None
    return row


def _audit_loop(main):
    while True:
        try:
            if _ensure_audit_schema():
                settled = 0
                for row in _open_audits():
                    try:
                        if _settle_one(main, row):
                            settled += 1
                    except Exception as exc:
                        live._diag(f"V7.9.1 MISSED AUDIT settle error {row.get('symbol')}: {type(exc).__name__}: {exc}")
                    time.sleep(0.05)
                if settled:
                    s = _audit_summary()
                    if s:
                        live._diag(
                            f"V7.9.1 MISSED AUDIT summary skipped={int(s[0] or 0)} tracked={int(s[1] or 0)} "
                            f"open={int(s[2] or 0)} settled={int(s[3] or 0)} TP1/2/3={int(s[4] or 0)}/{int(s[5] or 0)}/{int(s[6] or 0)} "
                            f"counterfactual_R={_f(s[7]):+.2f}"
                        )
        except Exception as exc:
            live._diag(f"V7.9.1 MISSED AUDIT loop error: {type(exc).__name__}: {exc}")
        time.sleep(AUDIT_SECONDS)


def _patch_main(main):
    global _PATCHED, _MAIN
    with _PATCH_LOCK:
        if _PATCHED:
            return True
        if not hasattr(main, "_v71_live_candidate_gate") or not hasattr(main, "_v71_evaluate_live_without_paper"):
            return False
        original_gate = main._v71_live_candidate_gate
        original_eval = main._v71_evaluate_live_without_paper

        def gate_wrapper(result, trades):
            gate = original_gate(result, trades)
            try:
                d = _rotation_decision(result, gate)
                if d:
                    c = d["candidate"]
                    p = d["incumbent"]
                    if ROTATION_MODE == "shadow":
                        _close_incumbent(d)
                    elif _close_incumbent(d):
                        gate = dict(gate or {})
                        gate["eligible"] = True
                        gate["rotation_override"] = True
                        gate["rotation_from"] = p["symbol"]
                        gate["rotation_score_delta"] = round(d["score_delta"], 2)
                        gate["reasons"] = [
                            f"V7.9.1 rotated degraded {p['symbol']} into superior {c['symbol']} challenger"
                        ]
            except Exception as exc:
                live._diag(f"V7.9.1 ROTATION wrapper fail-closed: {type(exc).__name__}: {exc}")
            return gate

        def eval_wrapper(result, trades, source_key):
            out = original_eval(result, trades, source_key)
            try:
                _arm_missed_candidate(main, result, source_key, out if isinstance(out, dict) else {"executed": bool(out)})
            except Exception as exc:
                live._diag(f"V7.9.1 MISSED AUDIT wrapper error: {type(exc).__name__}: {exc}")
            return out

        main._v71_live_candidate_gate = gate_wrapper
        main._v71_evaluate_live_without_paper = eval_wrapper
        _MAIN = main
        _PATCHED = True
        live.V70_VERSION = "7.9.1-challenger-rotation-auditor"
        live._diag(
            f"V7.9.1 challenger patch armed rotation={ROTATION_MODE} "
            f"score>={MIN_CHALLENGER_SCORE:.0f} raw>={MIN_CHALLENGER_RAW:.0f} OI>={MIN_CHALLENGER_OI:.0f} "
            f"delta>={MIN_SCORE_DELTA:.0f} incumbent_health<={MAX_INCUMBENT_HEALTH:.0f} age>={MIN_INCUMBENT_AGE_MIN:.0f}m "
            f"missed_audit={AUDIT_ENABLED}"
        )
        if AUDIT_ENABLED:
            threading.Thread(target=_audit_loop, args=(main,), name="V791MissedAudit", daemon=True).start()
        return True


def _bootstrap():
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            main = sys.modules.get("__main__")
            if main is not None and _patch_main(main):
                return
        except Exception as exc:
            live._diag(f"V7.9.1 bootstrap retry: {type(exc).__name__}: {exc}")
        time.sleep(0.25)
    live._diag("V7.9.1 bootstrap gave up after 300s; no allocator patch applied")


if ENABLED:
    threading.Thread(target=_bootstrap, name="V791Bootstrap", daemon=True).start()
    live._diag("V7.9.1 challenger rotation + missed-trade auditor bootstrap armed")
