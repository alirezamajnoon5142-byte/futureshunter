"""FuturesHunter V7.8.7 metals range economics layer.

Production-only additions for RANGE_SCALPER:
- current MEXC API taker-fee aware economics (8 bps/side by default),
- spread + two-sided slippage + adverse funding settlement cost,
- minimum positive net edge before a range can execute,
- trading-session guard for exchange-traded base metals with daily suspensions.

Core trend/breakout logic and existing positions are untouched.
"""
import math
import os
import time
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

import v786_range_overlay as base
import v786_range_hardening as hard

PREVIOUS_RANGE_CANDIDATE = base._range_candidate

API_TAKER_BPS_PER_SIDE = max(0.0, float(os.getenv("V787_API_TAKER_BPS_PER_SIDE", "8.0")))
SLIPPAGE_BPS_PER_SIDE = max(0.0, float(os.getenv("V787_SLIPPAGE_BPS_PER_SIDE", "2.0")))
UNKNOWN_FUNDING_BUFFER_BPS = max(0.0, float(os.getenv("V787_UNKNOWN_FUNDING_BUFFER_BPS", "5.0")))
MIN_TP1_COST_MULT = max(1.0, float(os.getenv("V787_MIN_TP1_COST_MULT", "1.15")))
MIN_TP3_COST_MULT = max(MIN_TP1_COST_MULT, float(os.getenv("V787_MIN_TP3_COST_MULT", "1.75")))
MIN_NET_TP1_BPS = max(0.0, float(os.getenv("V787_MIN_NET_TP1_BPS", "5.0")))
MIN_NET_TP3_BPS = max(MIN_NET_TP1_BPS, float(os.getenv("V787_MIN_NET_TP3_BPS", "12.0")))
MAX_FUNDING_COST_BPS = max(5.0, float(os.getenv("V787_MAX_FUNDING_COST_BPS", "100.0")))

KNOWN_FUNDING_CYCLE_HOURS = {"COPPER_USDT": 4.0}
DEFAULT_FUNDING_CYCLE_HOURS = 8.0
SESSION_BASE_METALS = {"ALUMINUM_USDT", "ZINC_USDT", "NICKEL_USDT"}


def _row_for_symbol(data, symbol):
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return next((x for x in data if isinstance(x, dict) and str(x.get("symbol") or "").upper() == symbol), None)
    return None


def _funding_snapshot(symbol):
    symbol = str(symbol or "").upper()
    row = None
    errors = []
    try:
        row = _row_for_symbol(base._http_json(f"{base.RANGE_REST}/api/v1/contract/funding_rate/{symbol}"), symbol)
    except Exception as exc:
        errors.append(f"funding_endpoint:{type(exc).__name__}")
    if row is None:
        try:
            row = _row_for_symbol(base._http_json(f"{base.RANGE_REST}/api/v1/contract/ticker", {"symbol": symbol}), symbol)
        except Exception as exc:
            errors.append(f"ticker_fallback:{type(exc).__name__}")
    if not isinstance(row, dict):
        return {"ok": False, "errors": errors}

    rate = None
    for key in ("fundingRate", "funding_rate", "rate"):
        if row.get(key) not in (None, ""):
            rate = base._f(row.get(key))
            break
    if rate is None:
        return {"ok": False, "errors": errors + ["rate_missing"]}
    if abs(rate) > 0.10:
        rate = rate / 100.0

    cycle = None
    for key in ("collectCycle", "fundingIntervalHours", "fundingRateCycle", "cycle"):
        val = base._f(row.get(key))
        if val > 0:
            cycle = val
            break
    if cycle is None:
        cycle = KNOWN_FUNDING_CYCLE_HOURS.get(symbol, DEFAULT_FUNDING_CYCLE_HOURS)

    next_ts = 0.0
    for key in ("nextSettleTime", "nextSettlementTime", "nextSettleTimestamp", "nextFundingTime"):
        val = base._f(row.get(key))
        if val > 0:
            next_ts = val / 1000.0 if val > 1e11 else val
            break

    return {
        "ok": True,
        "rate": rate,
        "rate_bps": rate * 10000.0,
        "cycle_hours": cycle,
        "next_settle_ts": next_ts,
        "errors": errors,
    }


def _adverse_funding_cost_bps(result):
    symbol = str(result.get("symbol") or "").upper()
    direction = str(result.get("direction") or "").upper()
    end_ts = base._f(result.get("live_expiry_ts"), time.time())
    now = time.time()
    horizon_seconds = max(0.0, end_ts - now)
    snap = _funding_snapshot(symbol)
    if not snap.get("ok"):
        return UNKNOWN_FUNDING_BUFFER_BPS, {"known": False, "crossings": None, "buffer_bps": UNKNOWN_FUNDING_BUFFER_BPS}

    rate = base._f(snap.get("rate"))
    payer = (direction == "LONG" and rate > 0) or (direction == "SHORT" and rate < 0)
    cycle_seconds = max(1.0, base._f(snap.get("cycle_hours"), DEFAULT_FUNDING_CYCLE_HOURS) * 3600.0)
    next_ts = base._f(snap.get("next_settle_ts"))

    crossings = 0
    if horizon_seconds > 0:
        if next_ts > now:
            if end_ts >= next_ts:
                crossings = 1 + int(max(0.0, end_ts - next_ts) // cycle_seconds)
        else:
            crossings = int(math.ceil(horizon_seconds / cycle_seconds)) if horizon_seconds >= 0.50 * cycle_seconds else 0

    cost_bps = abs(rate) * 10000.0 * crossings if payer else 0.0
    cost_bps = min(MAX_FUNDING_COST_BPS, max(0.0, cost_bps))
    return cost_bps, {
        "known": True,
        "rate_bps": round(rate * 10000.0, 4),
        "cycle_hours": round(base._f(snap.get("cycle_hours")), 3),
        "next_settle_ts": next_ts,
        "crossings": crossings,
        "payer": bool(payer),
    }


def _v787_economic_edge_ok(result):
    if not result:
        return False
    entry = abs(base._f(result.get("price")))
    plan = result.get("risk_plan") or {}
    if entry <= 0:
        return False

    tp1 = base._f(plan.get("tp1")); tp3 = base._f(plan.get("tp3"))
    meta = result.setdefault("v786_range_meta", {})
    spread_bps = max(0.0, base._f(meta.get("spread_bps")))
    trading_fee_bps = 2.0 * API_TAKER_BPS_PER_SIDE
    slippage_bps = 2.0 * SLIPPAGE_BPS_PER_SIDE
    funding_bps, funding_meta = _adverse_funding_cost_bps(result)
    total_cost_bps = trading_fee_bps + spread_bps + slippage_bps + funding_bps

    tp1_edge_bps = abs(tp1 - entry) / entry * 10000.0
    tp3_edge_bps = abs(tp3 - entry) / entry * 10000.0
    tp1_net_bps = tp1_edge_bps - total_cost_bps
    tp3_net_bps = tp3_edge_bps - total_cost_bps

    meta.update({
        "cost_model": "V7.8.7_API_TAKER+SPREAD+SLIPPAGE+ADVERSE_FUNDING",
        "api_taker_bps_per_side": API_TAKER_BPS_PER_SIDE,
        "trading_fee_roundtrip_bps": round(trading_fee_bps, 2),
        "slippage_roundtrip_bps": round(slippage_bps, 2),
        "funding_cost_bps": round(funding_bps, 4),
        "funding": funding_meta,
        "estimated_cost_bps": round(total_cost_bps, 2),
        "tp1_edge_bps": round(tp1_edge_bps, 2),
        "tp3_edge_bps": round(tp3_edge_bps, 2),
        "tp1_net_bps": round(tp1_net_bps, 2),
        "tp3_net_bps": round(tp3_net_bps, 2),
    })

    ok = (
        tp1_edge_bps >= total_cost_bps * MIN_TP1_COST_MULT
        and tp3_edge_bps >= total_cost_bps * MIN_TP3_COST_MULT
        and tp1_net_bps >= MIN_NET_TP1_BPS
        and tp3_net_bps >= MIN_NET_TP3_BPS
    )
    if not ok:
        meta["economic_reject"] = True
    return ok


def _session_open(symbol, now_ts=None):
    symbol = str(symbol or "").upper()
    if symbol not in SESSION_BASE_METALS:
        return True
    now_utc = datetime.fromtimestamp(now_ts or time.time(), tz=timezone.utc)
    if now_utc.weekday() >= 5:
        return False

    summer = True
    if ZoneInfo is not None:
        try:
            ny = now_utc.astimezone(ZoneInfo("America/New_York"))
            summer = bool(ny.dst() and ny.dst().total_seconds())
        except Exception:
            pass

    minute = now_utc.hour * 60 + now_utc.minute
    close_min = (17 * 60 + 30) if summer else (18 * 60 + 30)
    if symbol == "NICKEL_USDT":
        open_min = 30 if summer else 90
    else:
        open_min = 90 if summer else 150
    return open_min <= minute < close_min


def _v787_range_candidate(symbol):
    if not _session_open(symbol):
        return None
    result = PREVIOUS_RANGE_CANDIDATE(symbol)
    if result is None:
        return None
    return result if _v787_economic_edge_ok(result) else None


hard._economic_edge_ok = _v787_economic_edge_ok
base._range_candidate = _v787_range_candidate

print(
    "[V7DIAG] V7.8.7 METALS RANGE ECONOMICS armed: 16bps API round-trip fee + spread + slippage + adverse funding; session-aware",
    flush=True,
)
