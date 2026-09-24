"""FuturesHunter V8.0.3 Research Brain — SHADOW ONLY.

Purpose:
- observe the FINAL live-selector decision after all V7 overlays,
- attach a structured thesis/bear-case/invalidation/probability snapshot,
- persist exact source-key decisions from fh_v71_challenger,
- settle against existing paper/missed-trade outcomes,
- measure calibration (Brier), expectancy and skipped-opportunity cost,
- produce twice-daily evidence-based proposals with blocker attribution.

This module has NO authority to alter eligibility, size, leverage, stops, exits,
orders, or code. It never auto-ships parameter changes.
"""
import json
import math
import os
import sys
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import live_executor_v70 as live
import v791_challenger_rotation as v791
import v792_second_crypto_slot as v792
import v794_trust_breakout as v794

ENABLED = os.getenv("V800_RESEARCH_BRAIN_ENABLED", "true").lower() == "true"
SHADOW_ONLY = True
SYNC_SECONDS = max(30, int(os.getenv("V800_SYNC_SECONDS", "90")))
REPORT_HOURS = sorted({
    max(0, min(23, int(x.strip())))
    for x in os.getenv("V800_REPORT_HOURS", "3,15").split(",")
    if x.strip()
})
REPORT_MINUTE = max(0, min(59, int(os.getenv("V800_REPORT_MINUTE", "0"))))
REPORT_TZ = os.getenv("V800_REPORT_TZ", "Europe/Madrid")
REPORT_TELEGRAM = os.getenv("V800_REPORT_TELEGRAM", "true").lower() == "true"
MIN_FILTER_SAMPLE = max(5, int(os.getenv("V800_MIN_FILTER_SAMPLE", "8")))
LOOKBACK_DAYS = max(3, int(os.getenv("V800_LOOKBACK_DAYS", "30")))

_PATCHED = False
_SCHEMA_READY = False
_MAIN = None
_PATCH_LOCK = threading.RLock()
_LAST_REPORT_SLOT = None


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _i(v, default=0):
    try:
        return int(v if v is not None else default)
    except Exception:
        return int(default)


def _norm(v):
    return str(v or "").strip().upper()


def _json(v):
    return live.Jsonb(v) if live.Jsonb else json.dumps(v)


def _clamp(v, lo, hi):
    return max(lo, min(hi, float(v)))


def _strategy_bias(name):
    return {
        "STRONG_CONFIRM": 0.075,
        "CONFIRM": 0.045,
        "MIXED": 0.0,
        "WAIT": -0.045,
        "AVOID": -0.095,
        "NO_DATA": -0.035,
    }.get(_norm(name), -0.02)


def _regime_bias(name):
    return {
        "BREAKOUT": 0.035,
        "TREND_CONTINUATION": 0.025,
        "TREND": 0.005,
    }.get(_norm(name), -0.015)


def _research_probability(result, gate):
    """Probability of a positive managed-R outcome, deliberately conservative.

    This is a calibration PRIOR, not a learned probability and not a trade gate.
    It is scored later with Brier loss and may only be promoted after evidence.
    """
    core = _f((result or {}).get("best_score"))
    raw = _f((result or {}).get("raw_score"))
    oi = _f((result or {}).get("oi_score"))
    strategy = _norm((gate or {}).get("strategy_consensus"))
    cost_r = max(0.0, _f((gate or {}).get("estimated_cost_r")))
    p = 0.50
    p += (core - 70.0) * 0.0055
    p += (raw - 70.0) * 0.0025
    p += (oi - 7.5) * 0.0070
    p += _strategy_bias(strategy)
    p += _regime_bias((result or {}).get("regime"))
    p -= min(0.12, cost_r * 0.12)
    if bool((gate or {}).get("btc_weak")):
        p -= 0.035
    if bool((gate or {}).get("macro_conflict")):
        p -= 0.040
    if _norm((gate or {}).get("risk_decision")) == "REDUCE":
        p -= 0.035
    if _norm((gate or {}).get("risk_decision")) == "BLOCK":
        p -= 0.12
    # Do not allow an uncalibrated prior to claim extreme certainty.
    return round(_clamp(p, 0.32, 0.78), 4)


def _support_room(gate):
    sr = (gate or {}).get("support_resistance") or {}
    return round(_f(sr.get("nearest_room_r"), 99.0), 4)


def _risk_levels(result):
    plan = (result or {}).get("risk_plan") or {}
    return {
        "entry": _f((result or {}).get("price") or plan.get("entry")),
        "stop": _f(plan.get("stop")),
        "tp1": _f(plan.get("tp1")),
        "tp2": _f(plan.get("tp2")),
        "tp3": _f(plan.get("tp3")),
    }


def _thesis_text(result, gate):
    symbol = str((result or {}).get("symbol") or "")
    direction = _norm((result or {}).get("direction"))
    regime = _norm((result or {}).get("regime"))
    core = _f((result or {}).get("best_score"))
    raw = _f((result or {}).get("raw_score"))
    oi = _f((result or {}).get("oi_score"))
    strategy = _norm((gate or {}).get("strategy_consensus"))
    return (
        f"{symbol} {direction} {regime}: Core {core:.1f}, Raw {raw:.0f}, "
        f"OI {oi:.0f}/15, strategy {strategy}; selector {_f((gate or {}).get('selector_score')):.1f}."
    )


def _bear_case_text(result, gate):
    concerns = []
    for r in list((gate or {}).get("reasons") or []):
        if r:
            concerns.append(str(r))
    if not concerns:
        if bool((gate or {}).get("btc_weak")):
            concerns.append("BTC not aligned")
        if bool((gate or {}).get("macro_conflict")):
            concerns.append("macro regime conflict")
        cost = _f((gate or {}).get("estimated_cost_r"))
        if cost >= 0.20:
            concerns.append(f"execution drag {cost:.2f}R")
        room = _support_room(gate)
        if room < 1.25:
            concerns.append(f"opposing structure only {room:.2f}R away")
    if not concerns:
        concerns.append("No major live-selector objection; failure case is ordinary signal invalidation/reversal.")
    return "; ".join(concerns[:5])


def _invalidation_text(result, gate):
    lv = _risk_levels(result)
    direction = _norm((result or {}).get("direction"))
    if lv["stop"] > 0:
        return (
            f"Hard invalidation: {direction} stop {lv['stop']:.12g}. "
            "Soft invalidation: loss of ENTRY state / material thesis deterioration before proof."
        )
    return "Soft invalidation: loss of ENTRY state, opposing hard structure, or thesis deterioration; live risk-plan stop remains authoritative."


def _brain_snapshot(result, gate):
    lv = _risk_levels(result)
    p = _research_probability(result, gate)
    return {
        "version": "8.0.3-shadow-research-brain",
        "shadow_only": True,
        "symbol": str((result or {}).get("symbol") or ""),
        "direction": _norm((result or {}).get("direction")),
        "state": _norm((result or {}).get("signal_state") or (result or {}).get("state")),
        "regime": _norm((result or {}).get("regime")),
        "core_score": round(_f((result or {}).get("best_score")), 4),
        "raw_score": round(_f((result or {}).get("raw_score")), 4),
        "weighted_score": round(_f((result or {}).get("weighted_score")), 4),
        "oi_score": round(_f((result or {}).get("oi_score")), 4),
        "selector_score": round(_f((gate or {}).get("selector_score")), 4),
        "strategy_consensus": _norm((gate or {}).get("strategy_consensus")),
        "risk_decision": _norm((gate or {}).get("risk_decision")),
        "estimated_cost_r": round(_f((gate or {}).get("estimated_cost_r")), 4),
        "btc_weak": bool((gate or {}).get("btc_weak")),
        "macro_conflict": bool((gate or {}).get("macro_conflict")),
        "support_room_r": _support_room(gate),
        "live_exposure_count": _i((gate or {}).get("live_exposure_count")),
        "decision": _norm((gate or {}).get("decision")),
        "prob_positive_r": p,
        "confidence_bucket": "HIGH" if p >= 0.66 else ("MEDIUM" if p >= 0.52 else "LOW"),
        "thesis": _thesis_text(result, gate),
        "bear_case": _bear_case_text(result, gate),
        "invalidation": _invalidation_text(result, gate),
        "levels": lv,
        "reasons": list((gate or {}).get("reasons") or []),
        "notes": list((gate or {}).get("notes") or []),
        "observed_ts": time.time(),
    }


def _ensure_schema():
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return True
    try:
        live._db("ALTER TABLE fh_v71_challenger ADD COLUMN IF NOT EXISTS audit_skip_class TEXT")
        live._db(
            """CREATE TABLE IF NOT EXISTS fh_v80_brain (
                source_key TEXT PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                state TEXT,
                regime TEXT,
                core_score DOUBLE PRECISION,
                raw_score DOUBLE PRECISION,
                weighted_score DOUBLE PRECISION,
                oi_score DOUBLE PRECISION,
                selector_score DOUBLE PRECISION,
                strategy_consensus TEXT,
                risk_decision TEXT,
                estimated_cost_r DOUBLE PRECISION,
                btc_weak BOOLEAN,
                macro_conflict BOOLEAN,
                support_room_r DOUBLE PRECISION,
                live_exposure_count INTEGER,
                live_decision TEXT,
                skip_class TEXT,
                prob_positive_r DOUBLE PRECISION,
                confidence_bucket TEXT,
                thesis TEXT,
                bear_case TEXT,
                invalidation TEXT,
                levels JSONB,
                reasons JSONB,
                notes JSONB,
                outcome_status TEXT,
                final_r DOUBLE PRECISION,
                outcome_source TEXT,
                brier DOUBLE PRECISION,
                settled_at TIMESTAMPTZ,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"""
        )
        live._db("CREATE INDEX IF NOT EXISTS idx_fh_v80_brain_time ON fh_v80_brain(observed_at DESC)")
        live._db("CREATE INDEX IF NOT EXISTS idx_fh_v80_brain_settled ON fh_v80_brain(settled_at,live_decision)")
        live._db(
            """CREATE TABLE IF NOT EXISTS fh_v80_nightly_reports (
                report_date DATE PRIMARY KEY,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sample_n INTEGER NOT NULL,
                settled_n INTEGER NOT NULL,
                brier DOUBLE PRECISION,
                avg_r DOUBLE PRECISION,
                allow_avg_r DOUBLE PRECISION,
                skip_avg_r DOUBLE PRECISION,
                skip_counterfactual_r DOUBLE PRECISION,
                live_closed_n INTEGER,
                live_net_r DOUBLE PRECISION,
                filter_stats JSONB NOT NULL,
                proposals JSONB NOT NULL,
                payload JSONB NOT NULL
            )"""
        )
        live._db(
            """CREATE TABLE IF NOT EXISTS fh_v803_reports (
                report_key TEXT PRIMARY KEY,
                report_date DATE NOT NULL,
                report_hour INTEGER NOT NULL,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                sample_n INTEGER NOT NULL,
                settled_n INTEGER NOT NULL,
                brier DOUBLE PRECISION,
                avg_r DOUBLE PRECISION,
                allow_avg_r DOUBLE PRECISION,
                skip_avg_r DOUBLE PRECISION,
                skip_counterfactual_r DOUBLE PRECISION,
                live_closed_n INTEGER,
                live_net_r DOUBLE PRECISION,
                filter_stats JSONB NOT NULL,
                proposals JSONB NOT NULL,
                payload JSONB NOT NULL
            )"""
        )
        _SCHEMA_READY = True
        return True
    except Exception as exc:
        live._diag(f"V8.0 schema retry: {type(exc).__name__}: {exc}")
        return False


def _backfill_legacy_rows():
    """Backfill pre-V8 selector outcomes for FILTER research only.

    Historical V7 rows do not contain the exact V8 feature snapshot, so they
    deliberately receive no probability and no Brier score. This prevents
    reconstructed history from pretending to be calibrated V8 predictions.
    """
    rows = live._db(
        """SELECT c.source_key,c.evaluated_time,c.symbol,c.direction,c.decision,
                  c.core_score,c.risk_decision,c.strategy_consensus,c.btc_weak,c.macro_conflict,
                  c.estimated_cost_r,c.selector_score,COALESCE(c.audit_skip_class,'OTHER'),
                  c.reasons,c.payload
           FROM fh_v71_challenger c
           LEFT JOIN fh_v80_brain b ON b.source_key=c.source_key
           WHERE b.source_key IS NULL
             AND c.evaluated_time >= NOW()-(%s || ' days')::interval
           ORDER BY c.evaluated_time
           LIMIT 500""",
        (str(LOOKBACK_DAYS),), "all"
    ) or []
    inserted = 0
    for row in rows:
        (
            source_key,evaluated_time,symbol,direction,decision,core,risk_decision,strategy,
            btc_weak,macro_conflict,cost_r,selector_score,skip_class,reasons,payload
        ) = row
        gate_payload = payload if isinstance(payload, dict) else {}
        reasons_list = list(reasons or [])
        thesis = (
            f"Historical V7 selector decision: {symbol} {_norm(direction)}, "
            f"Core {_f(core):.1f}, strategy {_norm(strategy)}, selector {_f(selector_score):.1f}."
        )
        bear_case = "; ".join(str(x) for x in reasons_list[:5]) or "No stored selector objection."
        historical_payload = {
            "version":"8.0.3-legacy-filter-backfill",
            "shadow_only":True,
            "backfilled_legacy":True,
            "calibration_eligible":False,
            "source_payload":gate_payload,
        }
        live._db(
            """INSERT INTO fh_v80_brain(
                source_key,observed_at,symbol,direction,state,regime,core_score,raw_score,weighted_score,oi_score,
                selector_score,strategy_consensus,risk_decision,estimated_cost_r,btc_weak,macro_conflict,
                support_room_r,live_exposure_count,live_decision,skip_class,prob_positive_r,confidence_bucket,
                thesis,bear_case,invalidation,levels,reasons,notes,payload
            ) VALUES(
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s
            ) ON CONFLICT(source_key) DO NOTHING""",
            (
                str(source_key),evaluated_time,str(symbol),_norm(direction),"ENTRY","",
                _f(core),None,None,None,_f(selector_score),_norm(strategy),_norm(risk_decision),
                _f(cost_r),bool(btc_weak),bool(macro_conflict),99.0,0,_norm(decision),
                str(skip_class or "OTHER"),None,"LEGACY",
                thesis,bear_case,
                "Historical backfill: exact V8 feature snapshot/risk-plan invalidation was not recorded.",
                _json({}),_json(reasons_list),_json([]),_json(historical_payload)
            )
        )
        inserted += 1
    return inserted


def _import_new_rows():
    """Materialize exact source-key decisions already persisted by V7.1.

    The final gate wrapper stores v800_shadow inside challenger.payload, so
    source identity is inherited from the production decision path.
    """
    rows = live._db(
        """SELECT c.source_key,c.evaluated_time,c.symbol,c.direction,c.decision,
                  COALESCE(c.audit_skip_class,'OTHER'),c.payload
           FROM fh_v71_challenger c
           LEFT JOIN fh_v80_brain b ON b.source_key=c.source_key
           WHERE b.source_key IS NULL
             AND c.payload ? 'v800_shadow'
           ORDER BY c.evaluated_time
           LIMIT 250""",
        (), "all"
    ) or []
    inserted = 0
    for source_key, evaluated_time, symbol, direction, decision, skip_class, payload in rows:
        p = payload if isinstance(payload, dict) else {}
        b = p.get("v800_shadow") or {}
        if not isinstance(b, dict):
            continue
        live._db(
            """INSERT INTO fh_v80_brain(
                source_key,observed_at,symbol,direction,state,regime,core_score,raw_score,weighted_score,oi_score,
                selector_score,strategy_consensus,risk_decision,estimated_cost_r,btc_weak,macro_conflict,
                support_room_r,live_exposure_count,live_decision,skip_class,prob_positive_r,confidence_bucket,
                thesis,bear_case,invalidation,levels,reasons,notes,payload
            ) VALUES(
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s
            ) ON CONFLICT(source_key) DO NOTHING""",
            (
                str(source_key), evaluated_time, str(symbol), _norm(direction), b.get("state"), b.get("regime"),
                _f(b.get("core_score")), _f(b.get("raw_score")), _f(b.get("weighted_score")), _f(b.get("oi_score")),
                _f(b.get("selector_score")), b.get("strategy_consensus"), b.get("risk_decision"),
                _f(b.get("estimated_cost_r")), bool(b.get("btc_weak")), bool(b.get("macro_conflict")),
                _f(b.get("support_room_r"),99.0), _i(b.get("live_exposure_count")), _norm(decision),
                str(skip_class or "OTHER"), _f(b.get("prob_positive_r")), b.get("confidence_bucket"),
                b.get("thesis"), b.get("bear_case"), b.get("invalidation"), _json(b.get("levels") or {}),
                _json(b.get("reasons") or []), _json(b.get("notes") or []), _json(b)
            )
        )
        inserted += 1
    return inserted


def _sync_outcomes():
    """Settle from the existing research ledger; never infer a result."""
    rows = live._db(
        """SELECT b.source_key,c.actual_status,c.actual_final_r,c.settled_at,
                  c.audit_status,c.audit_final_r,c.audit_closed_time
           FROM fh_v80_brain b
           JOIN fh_v71_challenger c ON c.source_key=b.source_key
           WHERE b.settled_at IS NULL
             AND (
                 (c.settled_at IS NOT NULL AND c.actual_final_r IS NOT NULL)
                 OR (c.audit_status IS NOT NULL AND c.audit_status NOT IN ('','OPEN')
                     AND c.audit_final_r IS NOT NULL)
             )
           LIMIT 250""",
        (), "all"
    ) or []
    settled = 0
    for source_key, actual_status, actual_r, actual_settled, audit_status, audit_r, audit_closed in rows:
        if actual_settled is not None and actual_r is not None:
            status = str(actual_status or "SETTLED")
            final_r = _f(actual_r)
            source = "PAPER_CONTROL"
            settled_at = actual_settled
        else:
            status = str(audit_status or "SETTLED")
            final_r = _f(audit_r)
            source = "MISSED_AUDIT"
            try:
                settled_at = datetime.fromtimestamp(_f(audit_closed), tz=timezone.utc)
            except Exception:
                settled_at = datetime.now(timezone.utc)
        prow = live._db("SELECT prob_positive_r FROM fh_v80_brain WHERE source_key=%s", (str(source_key),), "one")
        if prow and prow[0] is not None:
            prob = _f(prow[0])
            y = 1.0 if final_r > 0 else 0.0
            brier = round((prob - y) ** 2, 6)
        else:
            brier = None
        live._db(
            """UPDATE fh_v80_brain SET outcome_status=%s,final_r=%s,outcome_source=%s,
                      brier=%s,settled_at=%s,updated_at=NOW()
               WHERE source_key=%s""",
            (status, final_r, source, brier, settled_at, str(source_key))
        )
        settled += 1
    return settled


def _skip_tags(reasons):
    """Multi-label attribution from the actual selector reason text.

    Historical audit_skip_class was often OTHER. Re-derive blocker tags from
    stored reasons so old settled decisions become useful without rewriting
    or guessing outcomes.
    """
    vals = []
    if isinstance(reasons, (list, tuple)):
        vals = [str(x or "") for x in reasons]
    elif reasons:
        vals = [str(reasons)]
    text = " ; ".join(vals).lower()
    tags = []

    def add(tag, *needles):
        if any(n in text for n in needles) and tag not in tags:
            tags.append(tag)

    add("STRUCTURE_SR", "major support", "major resistance", "crowded structural path", "structural room")
    add("CORRELATION", "correlation guard", "same-direction", "same direction")
    add("EXECUTION_COST", "execution drag", "estimated cost", "cost ")
    add("DEGRADED_CORE", "degraded-regime core threshold", "core threshold")
    add("STRATEGY_CONFIRM", "strategy confirmation required", "strategy confirm")
    add("SELECTOR_THRESHOLD", "selector threshold")
    add("MACRO_BTC", "weak btc", "btc not aligned", "macro conflict", "crypto regime conflict")
    add("COOLDOWN_LOSS_CLUSTER", "cooldown", "loss-cluster", "loss cluster", "recent stop")
    add("EVENT_RISK", "event risk")
    add("PORTFOLIO_CAPACITY", "portfolio full", "max positions", "position(s) already open")
    add("ENTRY_CHASE", "entry chase", "drift block", "entry drift")
    add("LIVE_RISK_BLOCK", "live risk", "risk block", "risk decision block")

    return tags or ["OTHER"]


def _filter_stats():
    rows = live._db(
        """SELECT reasons,final_r
           FROM fh_v80_brain
           WHERE live_decision='SKIP' AND final_r IS NOT NULL
             AND observed_at >= NOW()-(%s || ' days')::interval""",
        (str(LOOKBACK_DAYS),), "all"
    ) or []

    agg = {}
    for reasons, final_r in rows:
        r = _f(final_r)
        for tag in _skip_tags(reasons):
            s = agg.setdefault(tag, {"n":0, "sum_r":0.0, "positive_n":0})
            s["n"] += 1
            s["sum_r"] += r
            if r > 0:
                s["positive_n"] += 1

    out = {}
    for tag, s in agg.items():
        n = max(1, _i(s["n"]))
        out[tag] = {
            "n": n,
            "avg_r": round(_f(s["sum_r"]) / n, 4),
            "sum_r": round(_f(s["sum_r"]), 4),
            "positive_rate": round(_f(s["positive_n"]) / n, 4),
            "attribution": "multi_label_reason_text",
        }
    return dict(sorted(out.items(), key=lambda kv: (-_i(kv[1].get("n")), kv[0])))

def _proposals(stats):
    proposals = []
    for cls, s in sorted(stats.items()):
        n = _i(s.get("n"))
        avg_r = _f(s.get("avg_r"))
        sum_r = _f(s.get("sum_r"))
        positive = _f(s.get("positive_rate"))
        if n < MIN_FILTER_SAMPLE:
            continue

        # R-multiple strategies can have positive expectancy with <50% winners,
        # so expectancy is primary. Positive-rate is descriptive, not a veto.
        if avg_r >= 0.20 and sum_r > 0:
            action = "REVIEW_FOR_LOOSENING"
            note = (
                "Skipped counterfactual expectancy is materially positive. "
                "Test a narrow shadow relaxation before any live change."
            )
        elif avg_r >= 0.10 and sum_r >= 1.0:
            action = "WATCH_FOR_LOOSENING"
            note = (
                "Skipped expectancy is positive but not yet strong enough for a "
                "parameter change; collect more category-specific outcomes."
            )
        elif avg_r <= -0.20 and sum_r < 0:
            action = "KEEP_OR_STRENGTHEN"
            note = "This blocker is avoiding negative expectancy; do not loosen without contrary evidence."
        else:
            action = "HOLD"
            note = "Evidence is mixed; collect more outcomes."

        proposals.append({
            "filter": cls,
            "action": action,
            "evidence": {
                "n": n,
                "avg_r": round(avg_r,3),
                "sum_r": round(sum_r,3),
                "positive_rate": round(positive,3),
            },
            "note": note,
        })

    priority = {
        "REVIEW_FOR_LOOSENING": 0,
        "WATCH_FOR_LOOSENING": 1,
        "KEEP_OR_STRENGTHEN": 2,
        "HOLD": 3,
    }
    proposals.sort(
        key=lambda x: (
            priority.get(x.get("action"), 9),
            -abs(_f((x.get("evidence") or {}).get("sum_r"))),
            -_i((x.get("evidence") or {}).get("n")),
        )
    )
    return proposals

def _report_payload():
    row = live._db(
        """SELECT COUNT(*),COUNT(*) FILTER(WHERE final_r IS NOT NULL),
                  AVG(brier) FILTER(WHERE brier IS NOT NULL),
                  AVG(final_r) FILTER(WHERE final_r IS NOT NULL),
                  AVG(final_r) FILTER(WHERE live_decision='ALLOW' AND final_r IS NOT NULL),
                  AVG(final_r) FILTER(WHERE live_decision='SKIP' AND final_r IS NOT NULL),
                  SUM(final_r) FILTER(WHERE live_decision='SKIP' AND final_r IS NOT NULL)
           FROM fh_v80_brain
           WHERE observed_at >= NOW()-(%s || ' days')::interval""",
        (str(LOOKBACK_DAYS),), "one"
    ) or (0,0,None,None,None,None,None)

    live_row = live._db(
        """SELECT COUNT(*),COALESCE(SUM(net_r),0),COALESCE(AVG(net_r),0)
           FROM fh_live_trades
           WHERE status='CLOSED' AND net_r IS NOT NULL
             AND COALESCE(payload->>'strategyTag','CORE')='CORE'
             AND closed_at >= NOW()-INTERVAL '24 hours'""",
        (), "one"
    ) or (0,0,0)

    stats = _filter_stats()
    proposals = _proposals(stats)
    return {
        "lookback_days": LOOKBACK_DAYS,
        "sample_n": _i(row[0]), "settled_n": _i(row[1]),
        "brier": round(_f(row[2]),4) if row[2] is not None else None,
        "avg_r": round(_f(row[3]),4) if row[3] is not None else None,
        "allow_avg_r": round(_f(row[4]),4) if row[4] is not None else None,
        "skip_avg_r": round(_f(row[5]),4) if row[5] is not None else None,
        "skip_counterfactual_r": round(_f(row[6]),4) if row[6] is not None else None,
        "live_24h_closed_n": _i(live_row[0]),
        "live_24h_net_r": round(_f(live_row[1]),4),
        "live_24h_avg_net_r": round(_f(live_row[2]),4),
        "filter_stats": stats,
        "proposals": proposals,
    }


def _format_report(p):
    brier = "N/A" if p.get("brier") is None else f"{p['brier']:.3f}"
    avg = "N/A" if p.get("avg_r") is None else f"{p['avg_r']:+.2f}R"
    skip = "N/A" if p.get("skip_avg_r") is None else f"{p['skip_avg_r']:+.2f}R"
    cf = "N/A" if p.get("skip_counterfactual_r") is None else f"{p['skip_counterfactual_r']:+.2f}R"
    lines = [
        "🧠 FUTURESHUNTER V8.0.3 RESEARCH BRAIN — SHADOW",
        f"Observed/settled: {p['sample_n']}/{p['settled_n']} | Brier {brier}",
        f"Research expectancy: {avg} | skipped avg {skip} | skipped total {cf}",
        f"Live CORE last 24h: {p['live_24h_closed_n']} closed | {p['live_24h_net_r']:+.2f}R net",
    ]
    proposals = p.get("proposals") or []
    if proposals:
        lines.append("Evidence proposals:")
        for x in proposals[:4]:
            e=x.get("evidence") or {}
            lines.append(
                f"• {x.get('filter')}: {x.get('action')} "
                f"(n={e.get('n')}, avg={_f(e.get('avg_r')):+.2f}R, "
                f"sum={_f(e.get('sum_r')):+.2f}R, positive={100*_f(e.get('positive_rate')):.0f}%)"
            )
    else:
        lines.append("Evidence proposals: not enough settled samples yet.")
    lines.append("No live parameter changes were made by V8.")
    return "\n".join(lines)


def _report_slot(now):
    """Return the latest due report slot (date, hour, key), or None."""
    due_hours = [h for h in REPORT_HOURS if (now.hour > h or (now.hour == h and now.minute >= REPORT_MINUTE))]
    if not due_hours:
        return None
    hour = max(due_hours)
    key = f"{now.date().isoformat()}-{hour:02d}"
    return now.date(), hour, key


def _save_report(report_date=None, report_hour=None, report_key=None):
    now = datetime.now(ZoneInfo(REPORT_TZ))
    if report_date is None:
        report_date = now.date()
    if report_hour is None:
        report_hour = now.hour
    if report_key is None:
        report_key = f"{report_date.isoformat()}-{int(report_hour):02d}"

    p = _report_payload()
    p["report_key"] = report_key
    p["report_hour"] = int(report_hour)
    p["report_tz"] = REPORT_TZ

    live._db(
        """INSERT INTO fh_v803_reports(
            report_key,report_date,report_hour,sample_n,settled_n,brier,avg_r,
            allow_avg_r,skip_avg_r,skip_counterfactual_r,live_closed_n,live_net_r,
            filter_stats,proposals,payload
        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(report_key) DO UPDATE SET
            generated_at=NOW(),sample_n=EXCLUDED.sample_n,settled_n=EXCLUDED.settled_n,
            brier=EXCLUDED.brier,avg_r=EXCLUDED.avg_r,allow_avg_r=EXCLUDED.allow_avg_r,
            skip_avg_r=EXCLUDED.skip_avg_r,skip_counterfactual_r=EXCLUDED.skip_counterfactual_r,
            live_closed_n=EXCLUDED.live_closed_n,live_net_r=EXCLUDED.live_net_r,
            filter_stats=EXCLUDED.filter_stats,proposals=EXCLUDED.proposals,payload=EXCLUDED.payload""",
        (
            report_key,report_date,int(report_hour),p["sample_n"],p["settled_n"],p.get("brier"),
            p.get("avg_r"),p.get("allow_avg_r"),p.get("skip_avg_r"),p.get("skip_counterfactual_r"),
            p["live_24h_closed_n"],p["live_24h_net_r"],_json(p["filter_stats"]),
            _json(p["proposals"]),_json(p)
        )
    )

    # Keep legacy daily table updated for backward compatibility with existing tools.
    live._db(
        """INSERT INTO fh_v80_nightly_reports(
            report_date,sample_n,settled_n,brier,avg_r,allow_avg_r,skip_avg_r,skip_counterfactual_r,
            live_closed_n,live_net_r,filter_stats,proposals,payload
        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(report_date) DO UPDATE SET
            generated_at=NOW(),sample_n=EXCLUDED.sample_n,settled_n=EXCLUDED.settled_n,brier=EXCLUDED.brier,
            avg_r=EXCLUDED.avg_r,allow_avg_r=EXCLUDED.allow_avg_r,skip_avg_r=EXCLUDED.skip_avg_r,
            skip_counterfactual_r=EXCLUDED.skip_counterfactual_r,live_closed_n=EXCLUDED.live_closed_n,
            live_net_r=EXCLUDED.live_net_r,filter_stats=EXCLUDED.filter_stats,
            proposals=EXCLUDED.proposals,payload=EXCLUDED.payload""",
        (
            report_date,p["sample_n"],p["settled_n"],p.get("brier"),p.get("avg_r"),p.get("allow_avg_r"),
            p.get("skip_avg_r"),p.get("skip_counterfactual_r"),p["live_24h_closed_n"],p["live_24h_net_r"],
            _json(p["filter_stats"]),_json(p["proposals"]),_json(p)
        )
    )

    msg = _format_report(p)
    label = f"{int(report_hour):02d}:00 {REPORT_TZ}"
    live._diag("V8.0.3 JOURNAL " + label + " | " + msg.replace("\n"," | "))
    if REPORT_TELEGRAM:
        try:
            live._msg(f"🕒 Research journal {label}\n" + msg)
        except Exception:
            pass
    return p


def _sync_loop():
    global _LAST_REPORT_SLOT
    tz = ZoneInfo(REPORT_TZ)
    while True:
        try:
            if _ensure_schema():
                backfilled = _backfill_legacy_rows()
                inserted = _import_new_rows()
                settled = _sync_outcomes()
                if backfilled or inserted or settled:
                    live._diag(
                        f"V8.0.3 RESEARCH sync backfilled={backfilled} new={inserted} settled={settled}"
                    )

                now = datetime.now(tz)
                slot = _report_slot(now)
                if slot:
                    report_date, report_hour, report_key = slot
                    already = live._db(
                        "SELECT 1 FROM fh_v803_reports WHERE report_key=%s",
                        (report_key,), "one"
                    )
                    if not already and _LAST_REPORT_SLOT != report_key:
                        _save_report(report_date, report_hour, report_key)
                        _LAST_REPORT_SLOT = report_key
        except Exception as exc:
            live._diag(f"V8.0.3 research loop warning: {type(exc).__name__}: {exc}")
        time.sleep(SYNC_SECONDS)

def _patch(main):
    global _PATCHED, _MAIN
    with _PATCH_LOCK:
        if _PATCHED:
            return True
        if not hasattr(main, "_v71_live_candidate_gate"):
            return False
        # Wait until the currently deployed decision overlays have finished so
        # V8 observes the FINAL decision rather than an intermediate gate.
        if not bool(getattr(v791, "_PATCHED", False)):
            return False
        if not bool(getattr(v792, "_PATCHED", False)):
            return False
        if not bool(getattr(v794, "_PATCHED", False)):
            return False
        if not bool(getattr(main, "V71_CHALLENGER_DB_READY", False)):
            return False

        original_gate = main._v71_live_candidate_gate

        def gate_wrapper(result, trades):
            gate = original_gate(result, trades)
            try:
                state = _norm((result or {}).get("signal_state") or (result or {}).get("state"))
                if isinstance(gate, dict) and (not state or state == "ENTRY"):
                    out = dict(gate)
                    out["v800_shadow"] = _brain_snapshot(result, out)
                    gate = out
            except Exception as exc:
                live._diag(f"V8.0 snapshot warning: {type(exc).__name__}: {exc}")
            return gate

        main._v71_live_candidate_gate = gate_wrapper

        if hasattr(main, "handle_telegram_command"):
            prev_command = main.handle_telegram_command

            def handle_telegram_command(chat_id, text):
                command = ((text or "").strip().split() or [""])[0].lower()
                if command in {"/brain","/v8","/research"}:
                    try:
                        main.send_to_chat(chat_id, _format_report(_report_payload()))
                    except Exception as exc:
                        main.send_to_chat(chat_id, f"V8 Research Brain unavailable: {type(exc).__name__}")
                    return
                return prev_command(chat_id, text)

            main.handle_telegram_command = handle_telegram_command

        _MAIN = main
        _PATCHED = True
        threading.Thread(target=_sync_loop, name="V800ResearchBrain", daemon=True).start()
        live._diag(
            f"V8.0.3 Research Brain armed SHADOW_ONLY=True live_gate_unchanged=True "
            f"sync={SYNC_SECONDS}s journals={','.join(f'{h:02d}:{REPORT_MINUTE:02d}' for h in REPORT_HOURS)} {REPORT_TZ} "
            f"skip_attribution=reason_text_multi_label lookback={LOOKBACK_DAYS}d auto_ship=False"
        )
        return True


def _bootstrap():
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            main = sys.modules.get("__main__")
            if main is not None and _patch(main):
                return
        except Exception as exc:
            live._diag(f"V8.0 bootstrap retry: {type(exc).__name__}: {exc}")
        time.sleep(0.25)
    live._diag("V8.0 bootstrap gave up after 300s; Research Brain not patched")


if ENABLED:
    threading.Thread(target=_bootstrap, name="V800Bootstrap", daemon=True).start()
    live._diag("V8.0.3 Research Brain bootstrap armed")
