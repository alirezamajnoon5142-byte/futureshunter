"""FuturesHunter V7.9.3 fee-aware RANGE_SCALPER manager.

Fixes the failure mode where a very tight range stop makes gross-R look large
while round-trip fees are several R. Range trades now:
- fail closed when estimated friction is too large versus initial stop distance,
- do NOT use the generic Core adaptive 25% banker,
- use a fee-aware break-even stop after TP1,
- may ratchet to fee-aware break-even before TP1 only after positive NET-R proof,
- cool down a symbol after a gross-positive / net-negative fee-dominated close.

Core, equity, and non-range live trades are untouched.
"""
import json
import os
import threading
import time

import live_executor_v70 as live
import v786_range_overlay as base
import v787_metals_range_economics as econ

ENABLED = os.getenv("V793_RANGE_FEE_AWARE_ENABLED", "true").lower() == "true"
MAX_COST_TO_STOP_R = max(0.10, float(os.getenv("V793_RANGE_MAX_COST_TO_STOP_R", "0.75")))
NET_BE_EXTRA_BPS = max(0.0, float(os.getenv("V793_RANGE_NET_BE_EXTRA_BPS", "2.0")))
FEE_LOSS_COOLDOWN_HOURS = max(0.0, float(os.getenv("V793_RANGE_FEE_LOSS_COOLDOWN_HOURS", "6.0")))
NET_PROOF_MFE_R = max(0.25, float(os.getenv("V793_RANGE_NET_PROOF_MFE_R", "0.75")))
NET_PROOF_GIVEBACK_R = max(0.10, float(os.getenv("V793_RANGE_NET_PROOF_GIVEBACK_R", "0.35")))
NET_PROOF_CURRENT_R = max(0.0, float(os.getenv("V793_RANGE_NET_PROOF_CURRENT_R", "0.25")))

_PREVIOUS_RANGE_CANDIDATE = base._range_candidate
_PATCH_LOCK = threading.RLock()
_PATCHED = False
_STRATEGY_CACHE = {}
_LAST_REJECT_LOG = {}


def _f(v, default=0.0):
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _diag_throttled(key, text, seconds=180):
    now = time.time()
    if now - _LAST_REJECT_LOG.get(key, 0.0) >= seconds:
        _LAST_REJECT_LOG[key] = now
        print(f"[V7DIAG] {text}", flush=True)


def _strategy_payload(signal_id):
    hit = _STRATEGY_CACHE.get(str(signal_id))
    if hit and time.time() - hit[0] < 300:
        return hit[1]
    try:
        row = live._db("SELECT payload FROM fh_live_trades WHERE signal_id=%s", (str(signal_id),), "one")
        payload = row[0] if row and isinstance(row[0], dict) else {}
    except Exception:
        payload = {}
    _STRATEGY_CACHE[str(signal_id)] = (time.time(), payload)
    return payload


def _is_range_signal(signal_id):
    return str(_strategy_payload(signal_id).get("strategyTag") or "").upper() == "RANGE_SCALPER"


def _fee_dominated_recent(symbol):
    if FEE_LOSS_COOLDOWN_HOURS <= 0:
        return None
    try:
        row = live._db(
            """SELECT gross_pnl,net_pnl,
                      COALESCE(entry_fee,0)+COALESCE(exit_fee,0),
                      EXTRACT(EPOCH FROM (NOW()-closed_at))
               FROM fh_live_trades
               WHERE symbol=%s AND status='CLOSED'
                 AND payload->>'strategyTag'='RANGE_SCALPER'
                 AND closed_at IS NOT NULL
               ORDER BY closed_at DESC LIMIT 1""",
            (str(symbol),), "one"
        )
    except Exception:
        row = None
    if not row:
        return None
    gross, net, fees, age = map(_f, row)
    if age <= FEE_LOSS_COOLDOWN_HOURS * 3600.0 and gross > 0 and net < 0 and fees >= gross:
        return {"gross": gross, "net": net, "fees": fees, "age": age}
    return None


def _range_candidate(symbol):
    result = _PREVIOUS_RANGE_CANDIDATE(symbol)
    if not result or not ENABLED:
        return result

    recent = _fee_dominated_recent(symbol)
    if recent:
        _diag_throttled(
            f"fee_cooldown:{symbol}",
            f"V7.9.3 RANGE FEE COOLDOWN {symbol}: last scalp gross={recent['gross']:+.4f} "
            f"net={recent['net']:+.4f} fees={recent['fees']:.4f}; "
            f"cooldown={FEE_LOSS_COOLDOWN_HOURS:.1f}h",
            300,
        )
        return None

    plan = result.get("risk_plan") or {}
    entry = abs(_f(result.get("price") or plan.get("entry")))
    stop = _f(plan.get("stop"))
    if entry <= 0 or stop <= 0:
        return None

    meta = result.setdefault("v786_range_meta", {})
    risk_bps = abs(stop - entry) / entry * 10000.0
    cost_bps = max(0.0, _f(meta.get("estimated_cost_bps")))
    if cost_bps <= 0:
        spread = max(0.0, _f(meta.get("spread_bps")))
        cost_bps = (
            2.0 * _f(getattr(econ, "API_TAKER_BPS_PER_SIDE", 8.0), 8.0)
            + 2.0 * _f(getattr(econ, "SLIPPAGE_BPS_PER_SIDE", 2.0), 2.0)
            + spread
        )
    if risk_bps <= 0:
        return None

    cost_r = cost_bps / risk_bps
    meta["initial_risk_bps"] = round(risk_bps, 3)
    meta["cost_to_stop_r"] = round(cost_r, 3)
    meta["v793_fee_aware"] = True

    if cost_r > MAX_COST_TO_STOP_R:
        meta["v793_economic_reject"] = True
        _diag_throttled(
            f"cost_r:{symbol}",
            f"V7.9.3 RANGE ECON REJECT {symbol} {result.get('direction')}: "
            f"cost={cost_bps:.2f}bps risk={risk_bps:.2f}bps cost={cost_r:.2f}R "
            f"> {MAX_COST_TO_STOP_R:.2f}R",
        )
        return None
    return result


def _persist_range_economics(signal_id, result):
    if not signal_id:
        return
    meta = (result or {}).get("v786_range_meta") or {}
    patch = {
        "rangeEstimatedCostBps": round(max(0.0, _f(meta.get("estimated_cost_bps"))), 4),
        "rangeInitialRiskBps": round(max(0.0, _f(meta.get("initial_risk_bps"))), 4),
        "rangeCostToStopR": round(max(0.0, _f(meta.get("cost_to_stop_r"))), 4),
        "rangeFeeAwareVersion": "7.9.3",
    }
    try:
        live._db(
            "UPDATE fh_live_trades SET payload=COALESCE(payload,'{}'::jsonb) || %s,updated_at=NOW() WHERE signal_id=%s",
            (live.Jsonb(patch) if live.Jsonb else json.dumps(patch), str(signal_id)),
        )
        _STRATEGY_CACHE.pop(str(signal_id), None)
    except Exception as exc:
        live._diag(f"V7.9.3 range economics persistence warning {signal_id}: {type(exc).__name__}: {exc}")


def _range_cost_bps(signal_id):
    p = _strategy_payload(signal_id)
    cost = max(0.0, _f(p.get("rangeEstimatedCostBps")))
    if cost > 0:
        return cost
    return (
        2.0 * _f(getattr(econ, "API_TAKER_BPS_PER_SIDE", 8.0), 8.0)
        + 2.0 * _f(getattr(econ, "SLIPPAGE_BPS_PER_SIDE", 2.0), 2.0)
    )


def _apply():
    global _PATCHED
    with _PATCH_LOCK:
        if _PATCHED or not ENABLED:
            return True
        required = ("execute_signal", "_adaptive_derisk", "_management_targets", "_change_position_protection")
        if any(not hasattr(live, x) for x in required):
            return False

        old_execute = live.execute_signal
        old_adaptive = live._adaptive_derisk
        old_targets = live._management_targets

        def execute_signal(result, paper_trade=None):
            out = old_execute(result, paper_trade)
            try:
                if (
                    isinstance(result, dict)
                    and str(result.get("live_strategy_tag") or "").upper() == "RANGE_SCALPER"
                    and isinstance(out, dict)
                    and out.get("executed")
                ):
                    sid = (paper_trade or {}).get("signal_id") if isinstance(paper_trade, dict) else None
                    _persist_range_economics(sid, result)
            except Exception as exc:
                live._diag(f"V7.9.3 post-entry persistence warning: {type(exc).__name__}: {exc}")
            return out

        def management_targets(row):
            t = old_targets(row)
            if not t or not row or not _is_range_signal(row[0]):
                return t
            cost_bps = _range_cost_bps(row[0]) + NET_BE_EXTRA_BPS
            direction = str(row[2] or "").upper()
            sign = 1.0 if direction == "LONG" else -1.0
            fee_be = t["entry"] * (1.0 + sign * cost_bps / 10000.0)
            t = dict(t)
            t["be"] = fee_be
            if direction == "LONG":
                t["lock2"] = max(_f(t.get("lock2")), fee_be)
            else:
                t["lock2"] = min(_f(t.get("lock2")), fee_be)
            return t

        def adaptive_derisk(row, targets, px, tp1_vol):
            if not row or not _is_range_signal(row[0]):
                return old_adaptive(row, targets, px, tp1_vol)

            signal_id, symbol, direction, position_id = row[0], row[1], row[2], row[3]
            tp1_done = bool(row[17])
            managed_stop = _f(row[10])
            initial_stop = _f(row[11])
            extra = list(row[20:]) if len(row) > 20 else []
            stored_peak = _f(extra[1]) if len(extra) > 1 else 0.0
            stage = str(extra[2] or "NONE").upper() if len(extra) > 2 else "NONE"

            gross_r = live._trade_r(direction, px, targets["entry"], targets["risk"])
            peak_gross_r = max(stored_peak, gross_r)
            if peak_gross_r > stored_peak + 1e-9:
                live._db(
                    "UPDATE fh_live_trades SET peak_favorable_r=%s,updated_at=NOW() WHERE signal_id=%s",
                    (peak_gross_r, signal_id),
                )

            risk_bps = abs(targets["risk"]) / max(1e-12, abs(targets["entry"])) * 10000.0
            cost_r = _range_cost_bps(signal_id) / max(1e-9, risk_bps)
            net_r = gross_r - cost_r
            net_peak_r = peak_gross_r - cost_r
            giveback_r = max(0.0, net_peak_r - net_r)

            # RANGE_SCALPER never uses Core's early 25% bank. Before TP1, the
            # only discretionary action is a fee-aware net-BE ratchet after the
            # trade has demonstrated enough NET excursion and then gives back.
            if (
                not tp1_done
                and stage not in {"RANGE_NET_BE", "BANKED_25", "LOCKED_POSITIVE"}
                and net_peak_r >= NET_PROOF_MFE_R
                and giveback_r >= NET_PROOF_GIVEBACK_R
                and net_r >= NET_PROOF_CURRENT_R
            ):
                candidate = _f(targets.get("be"))
                current_stop = managed_stop or initial_stop
                if live._stop_is_tighter(direction, current_stop, candidate):
                    ok, detail = live._change_position_protection(
                        symbol, position_id, candidate, targets["tp3"]
                    )
                    if not ok:
                        pnow = live._active_position(symbol, position_id)
                        if pnow:
                            live._emergency_close(
                                symbol, direction, position_id,
                                _f(pnow.get("holdVol") or pnow.get("vol") or pnow.get("positionVol")),
                                signal_id,
                            )
                        live.halt(
                            f"V7.9.3 range net-BE ratchet not confirmed on {symbol}: "
                            f"{detail}; remaining position flattened"
                        )
                        return True
                    live._db(
                        """UPDATE fh_live_trades SET stop_price=%s,managed_stop_price=%s,
                           adaptive_derisk_stage='RANGE_NET_BE',adaptive_derisk_time=NOW(),
                           updated_at=NOW() WHERE signal_id=%s""",
                        (candidate, candidate, signal_id),
                    )
                    live._diag(
                        f"V7.9.3 RANGE NET-BE {symbol} gross_peak={peak_gross_r:.2f}R "
                        f"net_peak={net_peak_r:.2f}R net_now={net_r:.2f}R "
                        f"cost={cost_r:.2f}R stop={candidate:.12g}"
                    )
                    live._msg(
                        f"🧠 V7.9.3 RANGE NET-BE\n{symbol} {direction}\n"
                        f"Net MFE {net_peak_r:.2f}R → now {net_r:.2f}R after estimated friction.\n"
                        f"No early size cut. Stop moved to fee-aware breakeven {candidate:.10g}; "
                        f"TP1/TP2/TP3 remain active."
                    )
                    return True
            return False

        live.execute_signal = execute_signal
        live._management_targets = management_targets
        live._adaptive_derisk = adaptive_derisk
        live.V70_VERSION = "7.9.3-range-fee-aware"
        live._diag(
            f"V7.9.3 range fee-aware manager armed max_cost_to_stop={MAX_COST_TO_STOP_R:.2f}R "
            f"net_be_extra={NET_BE_EXTRA_BPS:.1f}bps fee_loss_cooldown={FEE_LOSS_COOLDOWN_HOURS:.1f}h "
            f"generic_range_bank=OFF"
        )
        _PATCHED = True
        return True


base._range_candidate = _range_candidate


def _bootstrap():
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            if _apply():
                return
        except Exception as exc:
            try:
                live._diag(f"V7.9.3 bootstrap retry: {type(exc).__name__}: {exc}")
            except Exception:
                pass
        time.sleep(0.25)


if ENABLED:
    threading.Thread(target=_bootstrap, name="V793RangeFeeAware", daemon=True).start()
    print("[V7DIAG] V7.9.3 fee-aware range manager bootstrap armed", flush=True)
