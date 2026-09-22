"""FuturesHunter V7.8.9 post-entry thesis-failure guard.

For CORE trades that entered on BREAKOUT/TREND_CONTINUATION, compare the live
position with the latest persisted scanner thesis. If the thesis materially
collapses before the trade ever proves itself, reduce exposure before the hard
stop. RANGE_SCALPER trades are explicitly excluded.

Stage 1: CUT50 on a severe thesis failure after a grace period.
Stage 2: EXIT_REMAINDER only if a *newer* scanner snapshot remains severely bad.
This module never adds exposure and never widens protection.
"""
import os
import time
from datetime import datetime, timezone

import live_executor_v70 as live

_ORIGINAL_INIT_DB = live.init_db
_ORIGINAL_MANAGE_OPEN_TRADE = live._manage_open_trade
_ORIGINAL_EXECUTE_SIGNAL = live.execute_signal
_ORIGINAL_DIAGNOSTIC_STATE = live.diagnostic_state

ENABLED = os.getenv("V789_THESIS_GUARD_ENABLED", "true").lower() == "true"
MODE = os.getenv("V789_THESIS_GUARD_MODE", "live").strip().lower()
GRACE_MINUTES = max(3.0, float(os.getenv("V789_THESIS_GUARD_GRACE_MINUTES", "8")))
MAX_SCAN_AGE_MINUTES = max(5.0, float(os.getenv("V789_THESIS_GUARD_MAX_SCAN_AGE_MINUTES", "20")))
MAX_UNPROVEN_MFE_R = min(0.75, max(0.10, float(os.getenv("V789_THESIS_GUARD_MAX_MFE_R", "0.40"))))
CUT_RISK_SCORE = max(4, int(os.getenv("V789_THESIS_GUARD_CUT_SCORE", "6")))
EXIT_RISK_SCORE = max(CUT_RISK_SCORE, int(os.getenv("V789_THESIS_GUARD_EXIT_SCORE", "7")))
CUT_FRACTION = min(0.75, max(0.25, float(os.getenv("V789_THESIS_GUARD_CUT_FRACTION", "0.50"))))
MIN_EXIT_DELAY_MINUTES = max(1.0, float(os.getenv("V789_THESIS_GUARD_MIN_EXIT_DELAY_MINUTES", "3")))
ENTRY_REGIMES = {"BREAKOUT", "TREND_CONTINUATION"}


def _f(value, default=0.0):
    try:
        return float(value if value is not None else default)
    except Exception:
        return float(default)


def _utcnow():
    return datetime.now(timezone.utc)


def _patched_init_db():
    ok = _ORIGINAL_INIT_DB()
    if not ok:
        return ok
    for ddl in (
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS thesis_guard_stage TEXT NOT NULL DEFAULT 'NONE'",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS thesis_guard_time TIMESTAMPTZ",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS thesis_guard_scan_time TIMESTAMPTZ",
    ):
        live._db(ddl)
    return ok


def _persist_exact_entry_context(result, execution):
    if not isinstance(execution, dict) or not execution.get("executed"):
        return
    signal_id = str(execution.get("signal_id") or "")
    if not signal_id:
        return
    patch = {
        "thesisEntryScore": _f((result or {}).get("best_score")),
        "thesisEntryRaw": _f((result or {}).get("raw_score")),
        "thesisEntryWeighted": _f((result or {}).get("weighted_score")),
        "thesisEntryOi": _f((result or {}).get("oi_score")),
        "thesisEntryRegime": str((result or {}).get("regime") or "").upper(),
        "thesisEntryDirection": str((result or {}).get("direction") or "").upper(),
        "thesisEntryTs": time.time(),
    }
    try:
        live._db(
            """UPDATE fh_live_trades
               SET payload=COALESCE(payload,'{}'::jsonb) || %s, updated_at=NOW()
               WHERE signal_id=%s""",
            (live.Jsonb(patch) if live.Jsonb else patch, signal_id),
        )
    except Exception as exc:
        live._diag(f"THESIS GUARD entry-context persist skipped signal={signal_id}: {type(exc).__name__}: {exc}")


def _patched_execute_signal(result, paper_trade=None):
    out = _ORIGINAL_EXECUTE_SIGNAL(result, paper_trade)
    try:
        _persist_exact_entry_context(result, out)
    except Exception as exc:
        live._diag(f"THESIS GUARD entry-context wrapper error: {type(exc).__name__}: {exc}")
    return out


def _ledger_guard_context(signal_id):
    row = live._db(
        """SELECT payload,thesis_guard_stage,thesis_guard_time,thesis_guard_scan_time
           FROM fh_live_trades WHERE signal_id=%s""",
        (signal_id,), "one"
    )
    if not row:
        return {}, "NONE", None, None
    payload = row[0] if isinstance(row[0], dict) else {}
    return payload or {}, str(row[1] or "NONE").upper(), row[2], row[3]


def _entry_snapshot(symbol, direction, opened_at, payload):
    exact_score = _f(payload.get("thesisEntryScore"), -1.0)
    exact_regime = str(payload.get("thesisEntryRegime") or "").upper()
    exact_oi = _f(payload.get("thesisEntryOi"), -1.0)
    if exact_score >= 0 and exact_regime:
        return {
            "score": exact_score,
            "oi": max(0.0, exact_oi),
            "regime": exact_regime,
            "direction": str(payload.get("thesisEntryDirection") or direction).upper(),
            "source": "ledger",
        }
    if opened_at is None:
        return None
    row = live._db(
        """SELECT scan_time,selected_direction,signal_state,selected_regime,
                  best_score,raw_score,weighted_score,oi_score
           FROM fh_research_scans
           WHERE symbol=%s
             AND scan_time BETWEEN %s - INTERVAL '12 minutes' AND %s + INTERVAL '12 minutes'
           ORDER BY ABS(EXTRACT(EPOCH FROM (scan_time - %s))) ASC
           LIMIT 1""",
        (symbol, opened_at, opened_at, opened_at), "one"
    )
    if not row:
        return None
    return {
        "scan_time": row[0],
        "direction": str(row[1] or direction).upper(),
        "state": str(row[2] or "").upper(),
        "regime": str(row[3] or "").upper(),
        "score": _f(row[4]),
        "raw": _f(row[5]),
        "weighted": _f(row[6]),
        "oi": _f(row[7]),
        "source": "research",
    }


def _latest_snapshot(symbol):
    row = live._db(
        """SELECT scan_time,selected_direction,signal_state,selected_regime,
                  best_score,raw_score,weighted_score,oi_score
           FROM fh_research_scans
           WHERE symbol=%s
           ORDER BY scan_time DESC LIMIT 1""",
        (symbol,), "one"
    )
    if not row:
        return None
    return {
        "scan_time": row[0],
        "direction": str(row[1] or "").upper(),
        "state": str(row[2] or "").upper(),
        "regime": str(row[3] or "").upper(),
        "score": _f(row[4]),
        "raw": _f(row[5]),
        "weighted": _f(row[6]),
        "oi": _f(row[7]),
    }


def _minutes_since(ts):
    if ts is None:
        return 1e9
    try:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return max(0.0, (_utcnow() - ts).total_seconds() / 60.0)
    except Exception:
        return 1e9


def _risk_assessment(direction, entry, current, peak_r, current_r):
    score_drop = max(0.0, _f(entry.get("score")) - _f(current.get("score")))
    entry_oi = _f(entry.get("oi"))
    current_oi = _f(current.get("oi"))
    current_score = _f(current.get("score"))
    current_state = str(current.get("state") or "").upper()
    current_regime = str(current.get("regime") or "").upper()
    current_direction = str(current.get("direction") or "").upper()
    entry_regime = str(entry.get("regime") or "").upper()
    risk = 0
    reasons = []

    if current_direction and current_direction != str(direction).upper():
        risk += 4
        reasons.append(f"direction flipped {direction}->{current_direction}")

    if current_state == "REJECT":
        risk += 3; reasons.append("state REJECT")
    elif current_state == "WATCH":
        risk += 2; reasons.append("state WATCH")
    elif current_state == "ARMED":
        risk += 1; reasons.append("state ARMED")

    if entry_regime in ENTRY_REGIMES and current_regime not in ENTRY_REGIMES:
        add = 3 if current_regime in {"NORMAL", ""} else 2
        risk += add
        reasons.append(f"regime {entry_regime}->{current_regime or 'NONE'}")

    if score_drop >= 25:
        risk += 3; reasons.append(f"score -{score_drop:.1f}")
    elif score_drop >= 15:
        risk += 2; reasons.append(f"score -{score_drop:.1f}")
    elif score_drop >= 10:
        risk += 1; reasons.append(f"score -{score_drop:.1f}")

    if entry_oi >= 8:
        if current_oi <= 3:
            risk += 2; reasons.append(f"OI {entry_oi:.0f}->{current_oi:.0f}")
        elif current_oi <= 6:
            risk += 1; reasons.append(f"OI {entry_oi:.0f}->{current_oi:.0f}")

    if current_score < 68:
        risk += 2; reasons.append(f"score low {current_score:.1f}")
    elif current_score < 75:
        risk += 1; reasons.append(f"score soft {current_score:.1f}")

    if peak_r < 0.30:
        risk += 1; reasons.append(f"MFE only {peak_r:.2f}R")
    if current_r <= -0.35:
        risk += 1; reasons.append(f"current {current_r:.2f}R")

    return {
        "risk": risk,
        "score_drop": score_drop,
        "reasons": reasons,
        "entry_score": _f(entry.get("score")),
        "current_score": current_score,
        "entry_oi": entry_oi,
        "current_oi": current_oi,
        "entry_regime": entry_regime,
        "current_regime": current_regime,
        "current_state": current_state,
    }


def _guard_open_trade(row, exchange_pos):
    if not ENABLED:
        return False
    signal_id, symbol, direction, position_id = row[0], row[1], row[2], int(row[3] or 0)
    opened_at = row[9]
    peak_r = _f(row[21] if len(row) > 21 else 0.0)

    payload, stage, guard_time, guard_scan_time = _ledger_guard_context(signal_id)
    if str(payload.get("strategyTag") or "CORE").upper() != "CORE":
        return False

    age_minutes = _minutes_since(opened_at)
    if age_minutes < GRACE_MINUTES:
        return False

    entry = _entry_snapshot(symbol, direction, opened_at, payload)
    if not entry or str(entry.get("regime") or "").upper() not in ENTRY_REGIMES:
        return False

    current = _latest_snapshot(symbol)
    if not current or _minutes_since(current.get("scan_time")) > MAX_SCAN_AGE_MINUTES:
        return False

    # Once the trade has meaningfully proven itself, existing adaptive MFE/giveback
    # management owns the position. This guard is specifically for failed theses.
    if peak_r > MAX_UNPROVEN_MFE_R:
        return False

    targets = live._management_targets(row)
    if not targets:
        return False
    px = _f((exchange_pos or {}).get("fairPrice") or (exchange_pos or {}).get("markPrice")
            or (exchange_pos or {}).get("lastPrice"))
    if px <= 0:
        px = live._fair_price(symbol)
    current_r = live._trade_r(direction, px, targets["entry"], targets["risk"])
    a = _risk_assessment(direction, entry, current, peak_r, current_r)

    if a["risk"] >= max(4, CUT_RISK_SCORE - 1):
        live._diag(
            f"THESIS GUARD EVAL {symbol} {direction} stage={stage} risk={a['risk']} "
            f"entry={a['entry_score']:.1f}/{a['entry_regime']} oi={a['entry_oi']:.0f} "
            f"now={a['current_score']:.1f}/{a['current_regime']} state={a['current_state']} "
            f"oi={a['current_oi']:.0f} peak={peak_r:.2f}R current={current_r:.2f}R "
            f"reasons={'; '.join(a['reasons'])}"
        )

    if MODE == "shadow":
        return False

    if stage == "NONE" and a["risk"] >= CUT_RISK_SCORE:
        pnow = live._active_position(symbol, position_id)
        if not pnow:
            return True
        before = _f(pnow.get("holdVol") or pnow.get("vol") or pnow.get("positionVol"))
        close_vol = live._mexc_vol(symbol, before * CUT_FRACTION)
        if close_vol <= 0 or close_vol >= before:
            # If the exchange lot size cannot represent a safe half-cut, flattening
            # is safer than silently claiming the guard acted.
            close_vol = before
        ok, detail = live._partial_market_close(symbol, direction, position_id, close_vol, signal_id, "TG1")
        if not ok:
            live.halt(f"thesis guard CUT50 failed/unconfirmed on {symbol}: {detail}")
            return True
        live._db(
            """UPDATE fh_live_trades
               SET thesis_guard_stage='CUT50',thesis_guard_time=NOW(),
                   thesis_guard_scan_time=%s,updated_at=NOW()
               WHERE signal_id=%s""",
            (current.get("scan_time"), signal_id)
        )
        live._diag(
            f"THESIS GUARD CUT50 {symbol} {direction} risk={a['risk']} "
            f"closed={close_vol:g}/{before:g} score={a['entry_score']:.1f}->{a['current_score']:.1f} "
            f"OI={a['entry_oi']:.0f}->{a['current_oi']:.0f}"
        )
        live._msg(
            f"✂️ V7.8.9 THESIS FAILURE CUT\n{symbol} {direction}\n"
            f"Closed ~{CUT_FRACTION*100:.0f}% after thesis deterioration. "
            f"Score {a['entry_score']:.1f} → {a['current_score']:.1f}; "
            f"OI {a['entry_oi']:.0f} → {a['current_oi']:.0f}; "
            f"MFE {peak_r:.2f}R. Remaining exchange protection stays live."
        )
        return True

    if stage == "CUT50" and a["risk"] >= EXIT_RISK_SCORE:
        scan_time = current.get("scan_time")
        # Never fully exit merely because the same stale bad scan was observed
        # repeatedly by the 10-second reconciler. Require a newer scanner snapshot.
        if guard_scan_time is not None and scan_time is not None and scan_time <= guard_scan_time:
            return False
        if _minutes_since(guard_time) < MIN_EXIT_DELAY_MINUTES:
            return False
        pnow = live._active_position(symbol, position_id)
        if not pnow:
            return True
        remaining = _f(pnow.get("holdVol") or pnow.get("vol") or pnow.get("positionVol"))
        if remaining <= 0:
            return True
        ok, detail = live._partial_market_close(symbol, direction, position_id, remaining, signal_id, "TGX")
        if not ok:
            live.halt(f"thesis guard EXIT failed/unconfirmed on {symbol}: {detail}")
            return True
        live._db(
            """UPDATE fh_live_trades
               SET thesis_guard_stage='EXIT_SENT',thesis_guard_time=NOW(),
                   thesis_guard_scan_time=%s,updated_at=NOW()
               WHERE signal_id=%s""",
            (scan_time, signal_id)
        )
        live._diag(
            f"THESIS GUARD EXIT {symbol} {direction} risk={a['risk']} remaining={remaining:g} "
            f"score={a['entry_score']:.1f}->{a['current_score']:.1f}"
        )
        live._msg(
            f"🧯 V7.8.9 THESIS FAILURE EXIT\n{symbol} {direction}\n"
            f"New scanner evidence stayed invalid after the 50% cut. "
            f"Closed the remaining position instead of waiting for the original hard stop."
        )
        return True

    return False


def _patched_manage_open_trade(row, exchange_pos):
    try:
        if _guard_open_trade(row, exchange_pos):
            return
    except Exception as exc:
        # Data/assessment failures must not create an unprotected management gap.
        # Fall back to the proven adaptive/TP manager.
        live._diag(f"THESIS GUARD fail-open {row[1] if len(row)>1 else '?'}: {type(exc).__name__}: {exc}")
    return _ORIGINAL_MANAGE_OPEN_TRADE(row, exchange_pos)


def _patched_diagnostic_state():
    state = _ORIGINAL_DIAGNOSTIC_STATE()
    state.update({
        "version": "7.8.9-thesis-failure-guard",
        "thesis_guard_enabled": ENABLED,
        "thesis_guard_mode": MODE,
        "thesis_guard_grace_minutes": GRACE_MINUTES,
        "thesis_guard_cut_score": CUT_RISK_SCORE,
        "thesis_guard_exit_score": EXIT_RISK_SCORE,
        "thesis_guard_max_unproven_mfe_r": MAX_UNPROVEN_MFE_R,
        "thesis_guard_cut_fraction": CUT_FRACTION,
    })
    return state


live.init_db = _patched_init_db
live.execute_signal = _patched_execute_signal
live._manage_open_trade = _patched_manage_open_trade
live.diagnostic_state = _patched_diagnostic_state
live.V70_VERSION = "7.8.9-thesis-failure-guard"

print(
    f"[V7DIAG] V7.8.9 thesis-failure guard armed mode={MODE} grace={GRACE_MINUTES:g}m "
    f"cut_score={CUT_RISK_SCORE} exit_score={EXIT_RISK_SCORE} max_unproven_mfe={MAX_UNPROVEN_MFE_R:.2f}R",
    flush=True,
)
