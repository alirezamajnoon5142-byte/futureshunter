"""FuturesHunter V8.5 — isolated MACRO_BTC veto challenger (SHADOW ONLY).

Prospective policy experiment. It enrolls only NEW V8.0 rows whose production
decision is SKIP with primary skip_class MACRO_BTC and macro_conflict=True.
Production control remains the veto (0R / no trade). The challenger records the
same candidate's later research outcome as if that veto alone were ignored.
No order path, selector, sizing, leverage, stop, target or live gate is modified.
"""
import os
import threading
import time
from datetime import datetime, timezone

import live_executor_v70 as live

ENABLED=os.getenv("V850_MACRO_BTC_CHALLENGER_ENABLED","true").lower()=="true"
SHADOW_ONLY=True
SYNC_SECONDS=max(60,int(os.getenv("V850_SYNC_SECONDS","180")))
MIN_REVIEW_N=max(30,int(os.getenv("V850_MIN_REVIEW_N","30")))
_SCHEMA_READY=False
_STARTED_AT=datetime.now(timezone.utc)


def _f(v,default=0.0):
    try: return float(v if v is not None else default)
    except Exception: return float(default)


def _ensure_schema():
    global _SCHEMA_READY
    if _SCHEMA_READY: return True
    try:
        live._db("""CREATE TABLE IF NOT EXISTS fh_v85_macro_btc_challenger(
            source_key TEXT PRIMARY KEY,
            enrolled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            source_observed_at TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            production_decision TEXT NOT NULL DEFAULT 'SKIP',
            production_skip_class TEXT NOT NULL DEFAULT 'MACRO_BTC',
            control_policy_r DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            challenger_status TEXT NOT NULL DEFAULT 'OPEN',
            challenger_r DOUBLE PRECISION,
            outcome_status TEXT,
            settled_at TIMESTAMPTZ,
            experiment_version TEXT NOT NULL DEFAULT 'v85-macro-btc-veto-prospective-v1'
        )""")
        live._db("CREATE INDEX IF NOT EXISTS idx_v85_macro_btc_status ON fh_v85_macro_btc_challenger(challenger_status,enrolled_at)")
        _SCHEMA_READY=True
        return True
    except Exception as exc:
        live._diag(f"V8.5 schema retry: {type(exc).__name__}: {exc}")
        return False


def _enroll():
    # Strictly prospective: rows observed before this process started are excluded.
    return live._db(
        """INSERT INTO fh_v85_macro_btc_challenger(
             source_key,source_observed_at,symbol,direction)
           SELECT b.source_key,b.observed_at,b.symbol,b.direction
           FROM fh_v80_brain b
           WHERE b.observed_at >= %s
             AND b.live_decision='SKIP'
             AND b.skip_class='MACRO_BTC'
             AND b.macro_conflict IS TRUE
           ON CONFLICT(source_key) DO NOTHING""",
        (_STARTED_AT,)
    )


def _settle():
    return live._db(
        """UPDATE fh_v85_macro_btc_challenger c
           SET challenger_status='SETTLED',
               challenger_r=b.final_r,
               outcome_status=b.outcome_status,
               settled_at=b.settled_at
           FROM fh_v80_brain b
           WHERE c.source_key=b.source_key
             AND c.challenger_status='OPEN'
             AND b.settled_at IS NOT NULL
             AND b.final_r IS NOT NULL"""
    )


def _report():
    row=live._db(
        """SELECT COUNT(*),COUNT(*) FILTER(WHERE challenger_status='SETTLED'),
                  AVG(challenger_r) FILTER(WHERE challenger_status='SETTLED'),
                  SUM(challenger_r) FILTER(WHERE challenger_status='SETTLED'),
                  AVG(CASE WHEN challenger_r>0 THEN 1.0 ELSE 0.0 END)
                    FILTER(WHERE challenger_status='SETTLED')
           FROM fh_v85_macro_btc_challenger""",(),"one")
    if not row: return
    enrolled,n,avg_r,total_r,win=row
    ready=int(n or 0)>=MIN_REVIEW_N
    live._diag(
        f"V8.5 MACRO_BTC SHADOW prospective enrolled={int(enrolled or 0)} "
        f"settled={int(n or 0)} challenger_avg={_f(avg_r):+.2f}R "
        f"challenger_sum={_f(total_r):+.2f}R positive={_f(win)*100:.1f}% "
        f"control_veto=0.00R policy_delta={_f(avg_r):+.2f}R "
        f"readiness={'REVIEW' if ready else 'COLLECT_DATA'} min_n={MIN_REVIEW_N} "
        f"live_gate_unchanged=True"
    )


def _loop():
    time.sleep(45)
    ticks=0
    while ENABLED:
        try:
            if _ensure_schema():
                _enroll(); _settle()
                if ticks%10==0: _report()
        except Exception as exc:
            try: live._diag(f"V8.5 loop warning: {type(exc).__name__}: {exc}")
            except Exception: pass
        ticks+=1
        time.sleep(SYNC_SECONDS)


if ENABLED:
    threading.Thread(target=_loop,name="V850MacroBTCChallenger",daemon=True).start()
    try:
        live._diag("V8.5 MACRO_BTC challenger armed SHADOW_ONLY=True prospective_only=True live_gate_unchanged=True")
    except Exception:
        pass
