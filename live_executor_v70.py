"""FuturesHunter V7.0 Live Pilot — fail-closed MEXC futures execution layer.

Disabled by default. It never needs Telegram secrets and never logs API secrets.
"""
import os, time, json, hmac, hashlib, math, threading
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests

try:
    import psycopg
    from psycopg.types.json import Jsonb
except Exception:
    psycopg = None
    Jsonb = None

V70_VERSION = "7.7.3-mexc-precision"
API_BASE = os.getenv("MEXC_FUTURES_API_BASE", "https://api.mexc.com").rstrip("/")
ACCESS_KEY = os.getenv("MEXC_ACCESS_KEY", "").strip()
SECRET_KEY = os.getenv("MEXC_SECRET_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
ENABLED = os.getenv("V70_LIVE_ENABLED", "false").lower() == "true"
ARMED = os.getenv("V70_LIVE_ARMED", "false").lower() == "true"
DRY_RUN = os.getenv("V70_DRY_RUN", "false").lower() == "true"
PILOT_START_BALANCE = float(os.getenv("V70_PILOT_START_BALANCE", os.getenv("V70_START_BALANCE", "222.14387438")))
RISK_PCT = min(0.01, max(0.0001, float(os.getenv("V70_RISK_PCT", "0.01"))))
DAILY_LOSS_PCT = min(0.03, max(0.0001, float(os.getenv("V70_DAILY_LOSS_PCT", "0.03"))))
EQUITY_KILL_DRAWDOWN_PCT = min(0.90, max(0.01, float(os.getenv("V70_EQUITY_KILL_PCT", "0.10"))))
MAX_NOTIONAL_CAP = float(os.getenv("V70_MAX_NOTIONAL_CAP", str(PILOT_START_BALANCE)))
MAX_LEVERAGE = min(2, int(os.getenv("V70_MAX_LEVERAGE", "2")))
MAX_POSITIONS = min(3, max(1, int(os.getenv("V70_MAX_POSITIONS", "3"))))
RECONCILE_SECONDS = max(5, int(os.getenv("V70_RECONCILE_SECONDS", "10")))
FILL_TIMEOUT = max(3, int(os.getenv("V70_FILL_TIMEOUT", "15")))
EXPIRY_HOURS = float(os.getenv("V70_EXPIRY_HOURS", "24"))
RECV_WINDOW = min(30, max(5, int(os.getenv("V70_RECV_WINDOW", "10"))))

# Live position management. Core entry qualification/risk sizing is unchanged.
MULTI_TP_ENABLED = os.getenv("V70_MULTI_TP_ENABLED", "true").lower() == "true"
TP1_FRACTION = min(0.45, max(0.05, float(os.getenv("V70_TP1_FRACTION", "0.25"))))
TP2_FRACTION = min(0.45, max(0.05, float(os.getenv("V70_TP2_FRACTION", "0.25"))))
BE_BUFFER_BPS = min(50.0, max(0.0, float(os.getenv("V70_BE_BUFFER_BPS", "10"))))
TP2_LOCK_R = min(1.5, max(0.0, float(os.getenv("V70_TP2_LOCK_R", "1.0"))))

_lock = threading.RLock()
_halted_memory = False
_halt_reason = ""
_notify = print


def configure(notify=None):
    global _notify
    if notify:
        _notify = notify


def diagnostic_state():
    """Secret-safe runtime state for deployment diagnostics."""
    return {
        "version": V70_VERSION,
        "enabled": ENABLED,
        "armed": ARMED,
        "dry_run": DRY_RUN,
        "credentials_present": bool(ACCESS_KEY and SECRET_KEY),
        "database_configured": bool(DATABASE_URL),
        "pilot_start_balance": PILOT_START_BALANCE,
        "risk_pct": RISK_PCT,
        "daily_loss_pct": DAILY_LOSS_PCT,
        "equity_kill_drawdown_pct": EQUITY_KILL_DRAWDOWN_PCT,
        "max_notional_cap": MAX_NOTIONAL_CAP,
        "max_leverage": MAX_LEVERAGE,
        "max_positions": MAX_POSITIONS,
        "multi_tp_enabled": MULTI_TP_ENABLED,
        "tp1_fraction": TP1_FRACTION,
        "tp2_fraction": TP2_FRACTION,
        "be_buffer_bps": BE_BUFFER_BPS,
        "tp2_lock_r": TP2_LOCK_R,
    }


def _diag(text):
    # Render-only diagnostic path. Never include credentials or signed requests.
    print(f"[V7DIAG] {text}", flush=True)


def _msg(text):
    try: _notify(text)
    except Exception: print(text)


def _clean_params(params):
    return {k: v for k, v in (params or {}).items() if v is not None}


def _signed(method, path, params=None, timeout=10):
    method = method.upper()
    # Transport-level safety interlock: dry-run may read MEXC but can NEVER mutate it.
    if DRY_RUN and method not in {"GET"}:
        raise RuntimeError(f"V7DRYRUN WRITE BLOCKED: {method} {path}")
    if not ACCESS_KEY or not SECRET_KEY:
        raise RuntimeError("MEXC live API credentials are not configured")
    params = _clean_params(params)
    ts = str(int(time.time() * 1000))
    if method in {"GET", "DELETE"}:
        items = sorted(params.items(), key=lambda x: x[0])
        param_string = urlencode(items)
        body = None
    else:
        # compact JSON must be exactly the string used for signing and transport
        param_string = json.dumps(params, separators=(",", ":"), ensure_ascii=False)
        body = param_string
    sig = hmac.new(SECRET_KEY.encode(), (ACCESS_KEY + ts + param_string).encode(), hashlib.sha256).hexdigest()
    headers = {"ApiKey": ACCESS_KEY, "Request-Time": ts, "Signature": sig, "Recv-Window": str(RECV_WINDOW), "Content-Type": "application/json", "Language": "English"}
    url = API_BASE + path
    r = requests.request(method, url, params=(params if method in {"GET","DELETE"} else None), data=body, headers=headers, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    if not payload.get("success"):
        raise RuntimeError(f"MEXC API error code={payload.get('code')} message={payload.get('message')}")
    return payload.get("data")


def _public(path, params=None):
    r = requests.get(API_BASE + path, params=_clean_params(params), timeout=10)
    r.raise_for_status(); p = r.json()
    if not p.get("success"): raise RuntimeError(f"MEXC public API error: {p.get('code')} {p.get('message')}")
    return p.get("data")


def _db(sql, params=(), fetch=None):
    if not DATABASE_URL or psycopg is None: return None
    with psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=6) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch == "one": return cur.fetchone()
            if fetch == "all": return cur.fetchall()
    return True


def init_db():
    if not DATABASE_URL or psycopg is None:
        if ENABLED: halt("DATABASE_URL/psycopg unavailable — live mode requires durable ledger")
        return False
    _db("""CREATE TABLE IF NOT EXISTS fh_live_state (key TEXT PRIMARY KEY, value JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
    _db("""CREATE TABLE IF NOT EXISTS fh_live_trades (
      signal_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, direction TEXT NOT NULL, status TEXT NOT NULL,
      external_oid TEXT UNIQUE NOT NULL, entry_order_id TEXT, position_id BIGINT,
      paper_entry DOUBLE PRECISION, requested_notional DOUBLE PRECISION, actual_notional DOUBLE PRECISION,
      contracts DOUBLE PRECISION, contract_size DOUBLE PRECISION, leverage INTEGER,
      stop_price DOUBLE PRECISION, tp1_price DOUBLE PRECISION, tp2_price DOUBLE PRECISION, tp3_price DOUBLE PRECISION, stop_pct DOUBLE PRECISION,
      initial_stop_price DOUBLE PRECISION, managed_stop_price DOUBLE PRECISION,
      tp1_vol DOUBLE PRECISION, tp2_vol DOUBLE PRECISION, tp1_done BOOLEAN NOT NULL DEFAULT FALSE, tp2_done BOOLEAN NOT NULL DEFAULT FALSE,
      tp1_time TIMESTAMPTZ, tp2_time TIMESTAMPTZ, management_stage TEXT NOT NULL DEFAULT 'INITIAL',
      actual_entry DOUBLE PRECISION, actual_exit DOUBLE PRECISION, slippage_usdt DOUBLE PRECISION,
      slippage_bps DOUBLE PRECISION, entry_fee DOUBLE PRECISION DEFAULT 0, exit_fee DOUBLE PRECISION DEFAULT 0,
      funding DOUBLE PRECISION DEFAULT 0, gross_pnl DOUBLE PRECISION, net_pnl DOUBLE PRECISION,
      gross_r DOUBLE PRECISION, net_r DOUBLE PRECISION, paper_status TEXT, paper_r DOUBLE PRECISION,
      protection_confirmed BOOLEAN NOT NULL DEFAULT FALSE, opened_at TIMESTAMPTZ, expiry_ts TIMESTAMPTZ, closed_at TIMESTAMPTZ,
      halt_reason TEXT, payload JSONB, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
    # Forward-compatible migration for deployments created before the live manager.
    for ddl in (
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp1_price DOUBLE PRECISION",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp2_price DOUBLE PRECISION",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS initial_stop_price DOUBLE PRECISION",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS managed_stop_price DOUBLE PRECISION",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp1_vol DOUBLE PRECISION",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp2_vol DOUBLE PRECISION",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp1_done BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp2_done BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp1_time TIMESTAMPTZ",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS tp2_time TIMESTAMPTZ",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS management_stage TEXT NOT NULL DEFAULT 'INITIAL'",
        "ALTER TABLE fh_live_trades ADD COLUMN IF NOT EXISTS expiry_ts TIMESTAMPTZ",
    ):
        _db(ddl)
    # Adopt positions opened by the pre-multi-TP executor. Those rows already own the
    # MEXC position but do not have the manager's baseline-stop columns populated.
    # The original stop_price is the authoritative initial risk reference.
    _db("""UPDATE fh_live_trades
           SET initial_stop_price=COALESCE(initial_stop_price, stop_price),
               managed_stop_price=COALESCE(managed_stop_price, stop_price),
               management_stage=COALESCE(NULLIF(management_stage,''), 'INITIAL'),
               updated_at=NOW()
           WHERE status='OPEN'""")
    _db("CREATE INDEX IF NOT EXISTS idx_fh_live_status ON fh_live_trades(status, opened_at DESC)")
    return True


def _state_get(key):
    row = _db("SELECT value FROM fh_live_state WHERE key=%s", (key,), "one")
    return row[0] if row else None


def _state_set(key, value):
    _db("""INSERT INTO fh_live_state(key,value,updated_at) VALUES(%s,%s,NOW()) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()""", (key, Jsonb(value) if Jsonb else json.dumps(value)))


def halt(reason):
    global _halted_memory, _halt_reason
    _halted_memory = True; _halt_reason = str(reason)[:500]
    try: _state_set("v70_halt", {"halted": True, "reason": _halt_reason, "ts": time.time()})
    except Exception: pass
    _msg(f"🛑 FUTURESHUNTER V7.0 LIVE HALT\n{_halt_reason}")


def halt_status():
    if _halted_memory: return True, _halt_reason
    try:
        s = _state_get("v70_halt") or {}
        return bool(s.get("halted")), str(s.get("reason") or "")
    except Exception:
        return (True, "cannot read durable halt state") if ENABLED else (False, "")


def _is_transient_reconcile_halt(reason):
    """Only stale transport/read failures are auto-recoverable after a full fresh reconcile."""
    r = str(reason or "").lower()
    return (
        r.startswith("reconciliation failure:")
        and any(x in r for x in ("readtimeout", "connecttimeout", "connectionerror", "timeout"))
    )


def _clear_transient_reconcile_halt_after_success():
    global _halted_memory, _halt_reason
    halted, reason = halt_status()
    if not halted or not _is_transient_reconcile_halt(reason):
        return False
    # This is called only after positions, ledger ownership/protection, account equity,
    # and breakers have all been read successfully in the same reconciliation pass.
    _halted_memory = False
    _halt_reason = ""
    _state_set("v70_halt", {"halted": False, "reason": "", "recovered_from": str(reason)[:500], "ts": time.time()})
    _diag("auto-cleared stale transient reconciliation HALT after successful fresh reconciliation")
    _msg("✅ FUTURESHUNTER V7 LIVE RECOVERED\nFresh MEXC reconciliation succeeded; stale transient API-timeout HALT cleared. Hard safety halts remain durable.")
    return True


def _is_closed_position_race_halt(reason):
    r = str(reason or "").lower()
    return "code=2009" in r and "position is nonexistent or closed" in r

def _clear_closed_position_race_halt_after_success():
    """Clear only the known harmless race halt after the vanished position has been audited closed."""
    global _halted_memory, _halt_reason
    halted, reason = halt_status()
    if not halted or not _is_closed_position_race_halt(reason):
        return False
    try:
        ex = positions() or []
        row = _db("SELECT COUNT(*) FROM fh_live_trades WHERE status='OPEN'", fetch="one") or (0,)
        if ex or int(row[0] or 0) != 0:
            return False
        _halted_memory = False
        _halt_reason = ""
        _state_set("v70_halt", {"halted": False, "reason": "", "recovered_from": str(reason)[:500], "ts": time.time()})
        _diag("auto-cleared closed-position race HALT after exchange/ledger both confirmed flat")
        _msg("✅ FUTURESHUNTER V7 LIVE RECOVERED\nClosed-position race was audited; exchange and live ledger are both flat. Live gate restored.")
        return True
    except Exception:
        return False



def _clear_verified_ratchet_halt_after_success(exchange_positions, dbrows):
    """Clear only the historical TP ratchet halt once its failed position is flat
    and every still-open owned position has independently verified protection.
    """
    global _halted_memory, _halt_reason
    halted, reason = halt_status()
    r = str(reason or "")
    rl = r.lower()
    if not halted or "stop ratchet was not confirmed" not in rl or "remaining position flattened" not in rl:
        return False
    import re
    m = re.search(r"filled on ([A-Z0-9_]+)", r, re.I)
    failed_symbol = (m.group(1).upper() if m else "")
    live_symbols = {str(p.get("symbol") or p.get("contractCode") or "").upper() for p in (exchange_positions or []) if float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0) > 0}
    if not failed_symbol or failed_symbol in live_symbols:
        return False
    # Every remaining exchange position must be owned by the ledger and its
    # current managed stop + TP3 must be verifiably present on MEXC.
    by_pid = {int(rw[3] or 0): rw for rw in (dbrows or [])}
    for p in (exchange_positions or []):
        if float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0) <= 0:
            continue
        pid = int(p.get("positionId") or p.get("id") or 0)
        rw = by_pid.get(pid)
        if not rw:
            return False
        tr = _db("SELECT stop_price,tp3_price FROM fh_live_trades WHERE signal_id=%s", (rw[0],), "one")
        if not tr or not _confirm_protection(rw[1], pid, float(tr[0]), float(tr[1])):
            return False
    _halted_memory = False
    _halt_reason = ""
    _state_set("v70_halt", {"halted": False, "reason": "", "recovered_from": r[:500], "ts": time.time()})
    _diag(f"auto-cleared verified TP ratchet HALT after {failed_symbol} was flat and all remaining live protection verified")
    _msg(f"✅ FUTURESHUNTER V7 LIVE RECOVERED\n{failed_symbol} ratchet failure is fully contained; failed position is flat and every remaining live position has verified exchange protection. Live gate restored.")
    return True

def asset(): return _signed("GET", "/api/v1/private/account/asset/USDT")
def positions(symbol=None): return _signed("GET", "/api/v1/private/position/open_positions", {"symbol": symbol}) or []

def contract(symbol):
    data = _public("/api/v1/contract/detail/country", {"symbol": symbol})
    if isinstance(data, list):
        data = next((x for x in data if x.get("symbol") == symbol), None)
    if not data: raise RuntimeError(f"contract metadata unavailable for {symbol}")
    return data


def order_by_external(symbol, oid): return _signed("GET", f"/api/v1/private/order/external/{symbol}/{oid}")
def open_stops(symbol): return _signed("GET", "/api/v1/private/stoporder/open_orders", {"symbol": symbol}) or []

def _fair_price(symbol):
    data = _public("/api/v1/contract/ticker", {"symbol": symbol})
    if isinstance(data, list):
        data = next((x for x in data if x.get("symbol") == symbol), None)
    if not isinstance(data, dict):
        raise RuntimeError(f"ticker unavailable for {symbol}")
    px = float(data.get("fairPrice") or data.get("lastPrice") or data.get("indexPrice") or 0)
    if px <= 0:
        raise RuntimeError(f"invalid ticker price for {symbol}")
    return px

def _active_position(symbol, position_id):
    for p in positions(symbol):
        if int(p.get("positionId") or 0) == int(position_id) and float(p.get("holdVol") or 0) > 0:
            return p
    return None

def _active_protection(symbol, position_id):
    rows=[]
    for x in open_stops(symbol):
        if int(x.get("positionId") or 0) == int(position_id) and int(x.get("state") or 0) == 1:
            rows.append(x)
    return rows

def _floor_step(value, step):
    if step <= 0: return value
    return math.floor((value + 1e-12) / step) * step

def _decimal_places(step):
    d=Decimal(str(step)).normalize()
    return max(0, -d.as_tuple().exponent)

def _mexc_step_value(value, step, rounding=ROUND_HALF_UP):
    """Return an API-safe numeric value aligned exactly to a MEXC tick/volume step."""
    d=Decimal(str(value)); q=Decimal(str(step))
    if q <= 0:
        return float(d)
    units=(d/q).to_integral_value(rounding=rounding)
    out=units*q
    places=_decimal_places(q)
    # Round again to the tick's decimal width so binary float noise cannot add precision.
    out=out.quantize(Decimal(1).scaleb(-places)) if places else out.quantize(Decimal(1))
    return int(out) if places == 0 else float(format(out, f'.{places}f'))

def _mexc_price(symbol, value):
    c=contract(symbol)
    unit=float(c.get("priceUnit") or 0)
    if unit <= 0:
        scale=c.get("priceScale")
        unit=10.0 ** (-int(scale)) if scale is not None else 1e-8
    return _mexc_step_value(value, unit, ROUND_HALF_UP)

def _mexc_vol(symbol, value):
    c=contract(symbol)
    step=float(c.get("volUnit") or 1)
    return _mexc_step_value(value, step, ROUND_DOWN)

def _three_way_split(contracts, step, min_vol):
    """Prefer 25%/25%/50%; fall back to thirds if exchange granularity requires it."""
    c=float(contracts); step=max(float(step or 0), 1e-12); min_vol=max(float(min_vol or step), step)
    v1=_floor_step(c*TP1_FRACTION, step); v2=_floor_step(c*TP2_FRACTION, step); v3=_floor_step(c-v1-v2, step)
    if v1 >= min_vol and v2 >= min_vol and v3 >= min_vol:
        return v1,v2,v3
    v1=_floor_step(c/3.0, step); v2=_floor_step((c-v1)/2.0, step); v3=_floor_step(c-v1-v2, step)
    if v1 >= min_vol and v2 >= min_vol and v3 >= min_vol:
        return v1,v2,v3
    return None

def _oid(signal_id, suffix="E"):
    digest = hashlib.sha1(f"{signal_id}|{suffix}".encode()).hexdigest()[:24]
    return f"fh70_{suffix.lower()}_{digest}"


def _daily_net_loss():
    row = _db("SELECT COALESCE(SUM(net_pnl),0) FROM fh_live_trades WHERE closed_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'", fetch="one")
    pnl = float(row[0] or 0) if row else 0.0
    return max(0.0, -pnl)


def _risk_limits(equity):
    """Balance-aware limits. Safety caps win; leverage is handled separately."""
    equity = max(0.0, float(equity or 0))
    risk_usdt = equity * RISK_PCT
    max_notional = min(equity, MAX_NOTIONAL_CAP)
    equity_kill = PILOT_START_BALANCE * (1.0 - EQUITY_KILL_DRAWDOWN_PCT)
    return risk_usdt, max_notional, equity_kill

def _day_start_equity(current_equity):
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"v711_day_start_equity_{day}"
    state = _state_get(key)
    if isinstance(state, dict) and state.get("equity") is not None:
        return float(state["equity"])
    eq = max(0.0, float(current_equity or 0))
    _state_set(key, {"equity": eq, "day": day})
    return eq

def _dynamic_limits(asset_row):
    eq = float(asset_row.get("equity") or 0)
    risk_usdt, max_notional, equity_kill = _risk_limits(eq)
    day_start = _day_start_equity(eq)
    daily_loss_limit = day_start * DAILY_LOSS_PCT
    return {"equity": eq, "risk_usdt": risk_usdt, "max_notional": max_notional,
            "equity_kill": equity_kill, "day_start_equity": day_start,
            "daily_loss_limit": daily_loss_limit}

def preflight():
    if not ENABLED: return False, "V70_LIVE_ENABLED=false"
    if not ARMED: return False, "V70_LIVE_ARMED=false"
    if not DATABASE_URL: return False, "DATABASE_URL missing"
    halted, why = halt_status()
    if halted: return False, f"HALTED: {why}"
    a = asset(); limits = _dynamic_limits(a); eq = limits["equity"]; avail = float(a.get("availableOpen") or a.get("availableBalance") or 0)
    if eq < limits["equity_kill"]:
        halt(f"equity kill-switch: {eq:.4f} < {limits['equity_kill']:.2f} USDT"); return False, "equity kill"
    pos = positions()
    if len(pos) >= MAX_POSITIONS: return False, "maximum live positions already open"
    if _daily_net_loss() >= limits["daily_loss_limit"]:
        halt(f"daily loss breaker reached: {_daily_net_loss():.4f} >= {limits['daily_loss_limit']:.4f} USDT"); return False, "daily loss breaker"
    if avail <= 0: return False, "no available USDT"
    return True, {"asset": a, "limits": limits, "positions": pos}



def run_zero_order_dry_run(symbol="BTC_USDT"):
    """Exercise account/risk/contract/order construction without any MEXC write.

    Requires ARMED=false and V70_DRY_RUN=true. GETs are real; all non-GETs are
    blocked in _signed before requests.request is reached.
    """
    checks=[]
    def ck(name, ok, detail=""):
        checks.append((name, bool(ok), detail))
        _diag(f"V7DRYRUN {'PASS' if ok else 'FAIL'} {name}" + (f" | {detail}" if detail else ""))
    if not DRY_RUN:
        _diag("V7DRYRUN SKIP V70_DRY_RUN=false"); return False
    if ARMED:
        _diag("V7DRYRUN FAIL ARMED must remain false"); return False
    try:
        a=asset(); limits=_dynamic_limits(a); pos=positions(); c=contract(symbol)
        eq=float(a.get("equity") or 0); avail=float(a.get("availableOpen") or a.get("availableBalance") or 0)
        ck("account_read", eq>0, f"equity={eq:.4f} available={avail:.4f}")
        ck("zero_positions", len(pos)==0, f"positions={len(pos)}")
        halted,why=halt_status(); ck("not_halted", not halted, why)
        ck("equity_floor", eq>=limits["equity_kill"], f"equity={eq:.4f} floor={limits['equity_kill']:.2f}")
        ck("daily_breaker", _daily_net_loss()<limits["daily_loss_limit"], f"loss={_daily_net_loss():.4f} limit={limits['daily_loss_limit']:.4f}")
        ck("api_allowed", bool(c.get("apiAllowed",False)), f"symbol={symbol}")
        ck("isolated_supported", int(c.get("positionOpenType",0)) in {1,3}, f"positionOpenType={c.get('positionOpenType')}")
        # Use exchange metadata price when present; this is payload construction only, never submission.
        entry=float(c.get("fairPrice") or c.get("indexPrice") or c.get("lastPrice") or 100.0)
        if entry<=0: entry=100.0
        contract_size=float(c["contractSize"]); step=float(c.get("volUnit") or 1); min_vol=float(c.get("minVol") or step)
        stop_pct=1.0; stop_long=entry*(1-stop_pct/100); tp_long=entry*(1+3*stop_pct/100)
        requested=min(limits["max_notional"], limits["risk_usdt"]/(stop_pct/100))
        contracts=_floor_step(requested/(entry*contract_size),step)
        actual=contracts*contract_size*entry
        risk=actual*stop_pct/100
        base_ok=contracts>=min_vol and actual<=limits["max_notional"]+1e-8 and risk<=limits["risk_usdt"]+1e-6
        for direction in ("LONG","SHORT"):
            stop=stop_long if direction=="LONG" else entry*(1+stop_pct/100)
            tp3=tp_long if direction=="LONG" else entry*(1-3*stop_pct/100)
            payload={"symbol":symbol,"price":entry,"vol":contracts,"leverage":min(MAX_LEVERAGE,int(c.get("maxLeverage") or MAX_LEVERAGE)),"side":1 if direction=="LONG" else 3,"type":5,"openType":1,"externalOid":_oid(f"dryrun_{direction}","E"),"stopLossPrice":stop,"takeProfitPrice":tp3,"lossTrend":2,"profitTrend":2,"positionMode":1}
            ck(f"{direction.lower()}_sizing",base_ok,f"notional={actual:.4f} risk={risk:.4f} contracts={contracts}")
            ck(f"{direction.lower()}_payload",payload["openType"]==1 and payload["leverage"]<=2 and payload["side"] in {1,3},f"isolated=True leverage={payload['leverage']} side={payload['side']}")
        # Deliberately oversized intent must fail the same hard cap invariant.
        oversized=limits["max_notional"]*2.0
        ck("oversized_rejected", not (oversized<=limits["max_notional"]+1e-8), f"requested={oversized:.2f} cap={limits['max_notional']:.2f}")
        # Prove the transport interlock itself blocks a write before HTTP transport.
        blocked=False
        try: _signed("POST","/__v7_dryrun_write_probe__",{"probe":True})
        except RuntimeError as e: blocked="V7DRYRUN WRITE BLOCKED" in str(e)
        ck("transport_write_guard",blocked,"non-GET blocked before network")
        ok=all(x[1] for x in checks)
        _diag(f"V7DRYRUN RESULT={'PASS' if ok else 'FAIL'} symbol={symbol} checks={sum(x[1] for x in checks)}/{len(checks)} MEXC_WRITE_REQUESTS=0")
        return ok
    except Exception as e:
        _diag(f"V7DRYRUN RESULT=FAIL exception={type(e).__name__}: {e}")
        return False


def _position_direction(position):
    ptype = int(position.get("positionType") or 0)
    if ptype == 1:
        return "LONG"
    if ptype == 2:
        return "SHORT"
    return str(position.get("direction") or "").upper()


def live_risk_snapshot(lookback_minutes=180):
    """Read-only live portfolio context for the V7 selector.

    Exchange positions are authoritative for current exposure. Recent realized
    outcomes come only from the durable live ledger, never the paper portfolio.
    A live loss <= -0.75R is exposed as stop-like for cooldown/cluster logic;
    this avoids treating fee-only/near-breakeven closes as full stop-outs.
    """
    minutes = max(1, min(24 * 60, int(lookback_minutes or 180)))
    ex = positions() or []
    open_rows = []
    for row in ex:
        if not isinstance(row, dict):
            continue
        hold = float(row.get("holdVol") or row.get("vol") or row.get("positionVol") or 0)
        if hold <= 0:
            continue
        open_rows.append({
            "symbol": str(row.get("symbol") or row.get("contractCode") or ""),
            "direction": _position_direction(row),
            "position_id": int(row.get("positionId") or row.get("id") or 0),
            "hold_vol": hold,
        })
    closed = _db(
        """SELECT symbol,direction,closed_at,net_r,signal_id FROM fh_live_trades
           WHERE status='CLOSED' AND closed_at IS NOT NULL
             AND closed_at >= NOW() - (%s * INTERVAL '1 minute')
           ORDER BY closed_at DESC LIMIT 100""",
        (minutes,), fetch="all"
    ) or []
    closed_rows = []
    for symbol, direction, closed_at, net_r, signal_id in closed:
        try:
            closed_ts = closed_at.timestamp() if closed_at is not None else 0.0
        except Exception:
            closed_ts = 0.0
        r = float(net_r or 0.0)
        closed_rows.append({
            "symbol": str(symbol or ""),
            "direction": str(direction or "").upper(),
            "closed_time": closed_ts,
            "net_r": r,
            "stop_like": r <= -0.75,
            "signal_id": str(signal_id or ""),
        })
    return {
        "source": "MEXC_OPEN_POSITIONS+FH_LIVE_LEDGER",
        "lookback_minutes": minutes,
        "open_positions": open_rows,
        "recent_closed": closed_rows,
    }


def _signal_risk_pct(result):
    """Per-signal risk override may only REDUCE the global account-risk cap."""
    try:
        requested = float((result or {}).get("live_risk_pct_override", RISK_PCT))
    except Exception:
        requested = RISK_PCT
    return min(RISK_PCT, max(0.0001, requested))


def preview_signal(result):
    """Read-only sizing/preflight preview for a candidate. No MEXC write request."""
    if not ENABLED:
        return {"eligible": False, "reason": "live executor disabled"}
    with _lock:
        ok, state = preflight()
        if not ok:
            return {"eligible": False, "reason": str(state)}
        try:
            plan = result["risk_plan"]; symbol = result["symbol"]; direction = result["direction"]
            for p in state.get("positions") or []:
                if str(p.get("symbol") or p.get("contractCode") or "") == str(symbol):
                    return {"eligible": False, "reason": "live position already open for symbol"}
            c = contract(symbol)
            if not c.get("apiAllowed", False): return {"eligible": False, "reason": "contract API trading not allowed"}
            if int(c.get("state", 1)) != 0: return {"eligible": False, "reason": "contract not enabled"}
            if int(c.get("positionOpenType", 0)) not in {1,3}: return {"eligible": False, "reason": "isolated margin unsupported"}
            entry=float(result["price"]); stop=float(plan["stop"]); tp1=float(plan.get("tp1") or 0); tp2=float(plan.get("tp2") or 0); tp3=float(plan["tp3"])
            if entry <= 0 or stop <= 0 or tp3 <= 0:
                return {"eligible": False, "reason": "invalid price geometry"}
            if MULTI_TP_ENABLED and (tp1 <= 0 or tp2 <= 0):
                return {"eligible": False, "reason": "multi-TP requires valid TP1/TP2"}
            stop_pct=abs(entry-stop)/entry*100.0
            if stop_pct <= 0: return {"eligible": False, "reason": "invalid stop distance"}
            limits=state["limits"]; effective_risk_pct=_signal_risk_pct(result)
            risk_usdt=float(limits["equity"])*effective_risk_pct
            max_notional=float(limits["max_notional"]); risk_notional=risk_usdt/(stop_pct/100.0)
            requested_notional=min(max_notional,risk_notional)
            contract_size=float(c["contractSize"]); step=float(c.get("volUnit") or 1); min_vol=float(c.get("minVol") or step)
            contracts=_floor_step(requested_notional/(entry*contract_size),step)
            if contracts < min_vol: return {"eligible": False, "reason": "pilot size below exchange minimum"}
            split=_three_way_split(contracts,step,min_vol) if MULTI_TP_ENABLED else None
            if MULTI_TP_ENABLED and not split:
                return {"eligible": False, "reason": "position too small for three exchange-valid TP slices"}
            actual_notional=contracts*contract_size*entry
            actual_risk=actual_notional*stop_pct/100.0
            if actual_notional > max_notional + 1e-8:
                return {"eligible": False, "reason": "notional cap calculation failed"}
            if actual_risk > risk_usdt + 1e-6:
                return {"eligible": False, "reason": "risk cap calculation failed"}
            leverage=min(MAX_LEVERAGE,int(c.get("maxLeverage") or MAX_LEVERAGE))
            a=state["asset"]; available=float(a.get("availableOpen") or a.get("availableBalance") or 0)
            margin=actual_notional/max(1,leverage)
            if margin > available:
                return {"eligible": False, "reason": "insufficient margin"}
            v1,v2,v3=split if split else (0.0,0.0,contracts)
            return {
                "eligible": True, "reason": "read-only preview passed", "symbol": symbol, "direction": direction,
                "entry": entry, "stop": stop, "stop_pct": stop_pct, "effective_risk_pct": effective_risk_pct,
                "risk_budget": risk_usdt, "requested_notional": requested_notional, "actual_notional": actual_notional,
                "actual_risk": actual_risk, "contracts": contracts, "contract_size": contract_size, "leverage": leverage,
                "margin_required": margin, "tp1_vol": v1, "tp2_vol": v2, "tp3_vol": v3,
                "exchange_open_positions": len(state.get("positions") or []), "max_positions": MAX_POSITIONS,
            }
        except Exception as e:
            return {"eligible": False, "reason": f"preview exception: {type(e).__name__}: {e}"}


def execute_signal(result, paper_trade=None):
    """Attempt one live mirror of a Core ENTRY. Any uncertainty fails closed."""
    if not ENABLED: return {"executed": False, "reason": "disabled"}
    with _lock:
        ok, state = preflight()
        if not ok: return {"executed": False, "reason": state}
        plan = result["risk_plan"]; symbol = result["symbol"]; direction = result["direction"]
        for p in state.get("positions") or []:
            if str(p.get("symbol") or p.get("contractCode") or "") == str(symbol):
                return {"executed": False, "reason": "live position already open for symbol"}
        signal_id = (paper_trade or {}).get("signal_id") or f"{int(time.time())}_{symbol}_{direction}"
        existing = _db("SELECT status,entry_order_id FROM fh_live_trades WHERE signal_id=%s", (signal_id,), "one")
        if existing: return {"executed": False, "reason": f"idempotent duplicate ({existing[0]})"}
        c = contract(symbol)
        if not c.get("apiAllowed", False): return {"executed": False, "reason": "contract API trading not allowed"}
        if int(c.get("state", 1)) != 0: return {"executed": False, "reason": "contract not enabled"}
        if int(c.get("positionOpenType", 0)) not in {1,3}: return {"executed": False, "reason": "isolated margin unsupported"}
        entry = float(result["price"]); stop = float(plan["stop"]); tp1 = float(plan.get("tp1") or 0); tp2 = float(plan.get("tp2") or 0); tp3 = float(plan["tp3"])
        if MULTI_TP_ENABLED and (tp1 <= 0 or tp2 <= 0):
            return {"executed": False, "reason": "multi-TP requires valid TP1/TP2 from Core risk plan"}
        stop_pct = abs(entry-stop)/entry*100.0
        if stop_pct <= 0: return {"executed": False, "reason": "invalid stop distance"}
        limits = state["limits"]
        effective_risk_pct = _signal_risk_pct(result)
        risk_usdt = float(limits["equity"]) * effective_risk_pct
        max_notional = limits["max_notional"]
        risk_notional = risk_usdt / (stop_pct/100.0)
        requested_notional = min(max_notional, risk_notional)
        contract_size = float(c["contractSize"]); step = float(c.get("volUnit") or 1); min_vol = float(c.get("minVol") or step)
        contracts = _floor_step(requested_notional / (entry * contract_size), step)
        if contracts < min_vol: return {"executed": False, "reason": "pilot size below exchange minimum"}
        split = _three_way_split(contracts, step, min_vol) if MULTI_TP_ENABLED else None
        if MULTI_TP_ENABLED and not split:
            return {"executed": False, "reason": "position too small for three exchange-valid TP slices"}
        tp1_vol,tp2_vol,tp3_vol = split if split else (0.0,0.0,contracts)
        actual_notional = contracts * contract_size * entry
        if actual_notional > max_notional + 1e-8: return {"executed": False, "reason": "notional cap calculation failed"}
        actual_risk = actual_notional * stop_pct/100.0
        if actual_risk > risk_usdt + 1e-6: return {"executed": False, "reason": "risk cap calculation failed"}
        leverage = min(MAX_LEVERAGE, int(c.get("maxLeverage") or MAX_LEVERAGE))
        a = state["asset"]; available = float(a.get("availableOpen") or a.get("availableBalance") or 0)
        if actual_notional/leverage > available: return {"executed": False, "reason": "insufficient margin"}
        oid = _oid(signal_id, "E")
        expiry_ts = None
        try:
            raw_expiry = float(result.get("live_expiry_ts") or 0)
            if raw_expiry > 0:
                expiry_ts = datetime.fromtimestamp(raw_expiry, tz=timezone.utc)
        except Exception:
            expiry_ts = None
        entry_api=_mexc_price(symbol, entry); stop_api=_mexc_price(symbol, stop); tp3_api=_mexc_price(symbol, tp3); contracts_api=_mexc_vol(symbol, contracts)
        payload = {"symbol":symbol,"price":entry_api,"vol":contracts_api,"leverage":leverage,"side":1 if direction=="LONG" else 3,"type":5,"openType":1,"externalOid":oid,"stopLossPrice":stop_api,"takeProfitPrice":tp3_api,"lossTrend":2,"profitTrend":2,"positionMode":1}
        ledger_payload = dict(payload)
        ledger_payload["effectiveRiskPct"] = effective_risk_pct
        ledger_payload["strategyTag"] = str(result.get("live_strategy_tag") or "CORE")
        if expiry_ts is not None:
            ledger_payload["expiryTs"] = expiry_ts.isoformat()
        _db("""INSERT INTO fh_live_trades(signal_id,symbol,direction,status,external_oid,paper_entry,requested_notional,actual_notional,contracts,contract_size,leverage,stop_price,tp1_price,tp2_price,tp3_price,initial_stop_price,managed_stop_price,tp1_vol,tp2_vol,stop_pct,management_stage,expiry_ts,payload) VALUES(%s,%s,%s,'SUBMITTING',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'INITIAL',%s,%s)""",
            (signal_id,symbol,direction,oid,entry,requested_notional,actual_notional,contracts,contract_size,leverage,stop,tp1,tp2,tp3,stop,stop,tp1_vol,tp2_vol,stop_pct,expiry_ts,Jsonb(ledger_payload) if Jsonb else json.dumps(ledger_payload)))
        try:
            created = _signed("POST", "/api/v1/private/order/create", payload)
            order_id = str(created.get("orderId"))
            _db("UPDATE fh_live_trades SET status='ENTRY_SENT',entry_order_id=%s,updated_at=NOW() WHERE signal_id=%s", (order_id,signal_id))
            deadline = time.time()+FILL_TIMEOUT; order=None
            while time.time() < deadline:
                order = order_by_external(symbol, oid)
                if order and int(order.get("state") or 0) == 3: break
                time.sleep(0.7)
            if not order or int(order.get("state") or 0) != 3:
                # We started with zero exchange positions, so any matching position now is ours.
                rescue = positions(symbol)
                for p in rescue:
                    if (int(p.get("positionType") or 0) == (1 if direction=="LONG" else 2)):
                        _emergency_close(symbol, direction, int(p.get("positionId") or 0), float(p.get("holdVol") or contracts), signal_id)
                halt(f"entry fill not confirmed for {symbol} {oid}; rescue flatten attempted"); return {"executed": False, "reason": "fill unconfirmed; rescue flatten attempted"}
            fill = float(order.get("dealAvgPrice") or 0); position_id = int(order.get("positionId") or 0)
            if fill <= 0 or position_id <= 0:
                halt(f"filled order missing fill/position id for {symbol}"); return {"executed": False, "reason": "bad fill response"}
            fee = abs(float(order.get("totalFee") or 0) or (float(order.get("takerFee") or 0)+float(order.get("makerFee") or 0)))
            slip = (fill-entry) * contracts * contract_size * (1 if direction=="LONG" else -1)
            slip_bps = ((fill-entry)/entry*10000.0) * (1 if direction=="LONG" else -1)
            _db("""UPDATE fh_live_trades SET status='PROTECTING',position_id=%s,actual_entry=%s,entry_fee=%s,slippage_usdt=%s,slippage_bps=%s,opened_at=NOW(),updated_at=NOW() WHERE signal_id=%s""", (position_id,fill,fee,slip,slip_bps,signal_id))
            if not _confirm_protection(symbol, position_id, stop, tp3):
                # Attached TP/SL may take a moment to materialize; place explicit position TP/SL once.
                _signed("POST", "/api/v1/private/stoporder/place", {"lossTrend":2,"profitTrend":2,"positionId":position_id,"vol":_mexc_vol(symbol, contracts),"stopLossPrice":_mexc_price(symbol, stop),"takeProfitPrice":_mexc_price(symbol, tp3),"priceProtect":0,"profitLossVolType":"SAME","volType":2,"takeProfitType":0,"takeProfitOrderPrice":0,"stopLossType":0,"stopLossOrderPrice":0})
                time.sleep(1.0)
            if not _confirm_protection(symbol, position_id, stop, tp3):
                _emergency_close(symbol, direction, position_id, contracts, signal_id)
                halt(f"protective stop/TP3 could not be confirmed for {symbol}; position flattened")
                return {"executed": False, "reason": "protection failed; flattened"}
            _db("UPDATE fh_live_trades SET status='OPEN',protection_confirmed=TRUE,updated_at=NOW() WHERE signal_id=%s", (signal_id,))
            _msg(f"🔴 V7.0 LIVE PILOT OPEN\n{symbol} {direction}\nFill: {fill}\nNotional: {actual_notional:.2f} USDT | Risk: {actual_risk:.3f} USDT ({effective_risk_pct*100:.2f}% cap) | {leverage}x isolated\nStop: {stop} | TP1: {tp1} ({tp1_vol:g}) | TP2: {tp2} ({tp2_vol:g}) | TP3: {tp3} ({tp3_vol:g})\nManager: {'25/25/50 preferred' if MULTI_TP_ENABLED else 'disabled'} | Protection: CONFIRMED")
            return {"executed": True, "signal_id": signal_id, "fill": fill, "position_id": position_id}
        except Exception as e:
            # If an exception occurs after submission, never assume no fill. Check the exchange and flatten a matching position.
            try:
                for p in positions(symbol):
                    if int(p.get("positionType") or 0) == (1 if direction=="LONG" else 2):
                        _emergency_close(symbol, direction, int(p.get("positionId") or 0), float(p.get("holdVol") or contracts), signal_id)
            except Exception:
                pass
            halt(f"live execution exception for {symbol}: {type(e).__name__}: {e}; rescue flatten attempted")
            return {"executed": False, "reason": str(e)}


def _confirm_protection(symbol, position_id, stop, tp3):
    for _ in range(4):
        try:
            rows = open_stops(symbol)
            for x in rows:
                if int(x.get("positionId") or 0) == int(position_id) and int(x.get("state") or 0) == 1:
                    sl=float(x.get("stopLossPrice") or 0); tp=float(x.get("takeProfitPrice") or 0)
                    if sl>0 and tp>0 and abs(sl-stop) <= max(1e-10,abs(stop)*1e-6) and abs(tp-tp3) <= max(1e-10,abs(tp3)*1e-6): return True
        except Exception: pass
        time.sleep(0.5)
    return False


def _emergency_close(symbol, direction, position_id, contracts, signal_id):
    payload={"symbol":symbol,"price":0,"vol":_mexc_vol(symbol, contracts),"side":4 if direction=="LONG" else 2,"type":5,"openType":1,"externalOid":_oid(signal_id,"X"),"positionId":position_id,"positionMode":1}
    return _signed("POST", "/api/v1/private/order/create", payload)


def _wait_partial_close(symbol, direction, position_id, before_vol, close_vol, oid):
    """Confirm a partial close from order state or authoritative remaining position size."""
    deadline=time.time()+FILL_TIMEOUT
    last=None
    while time.time() < deadline:
        try:
            order=order_by_external(symbol, oid)
            if order and int(order.get("state") or 0) == 3:
                return True, order
        except Exception:
            pass
        try:
            p=_active_position(symbol, position_id)
            remaining=float(p.get("holdVol") or 0) if p else 0.0
            last=remaining
            if remaining <= max(0.0, before_vol-close_vol) + 1e-9:
                return True, None
        except Exception:
            pass
        time.sleep(0.5)
    return False, last

def _partial_market_close(symbol, direction, position_id, close_vol, signal_id, stage):
    p=_active_position(symbol, position_id)
    if not p:
        return False, "position no longer open"
    before=float(p.get("holdVol") or 0)
    vol=min(float(close_vol), before)
    if vol <= 0:
        return False, "no closable volume"
    oid=_oid(signal_id, stage)
    vol=_mexc_vol(symbol, vol)
    payload={"symbol":symbol,"price":0,"vol":vol,"side":4 if direction=="LONG" else 2,"type":5,"openType":1,"externalOid":oid,"positionId":position_id,"positionMode":1}
    try:
        _signed("POST", "/api/v1/private/order/create", payload)
    except Exception as e:
        # If submit response is uncertain, do not repeat blindly; confirm by OID/position.
        ok,detail=_wait_partial_close(symbol,direction,position_id,before,vol,oid)
        return (True,detail) if ok else (False,f"submit/confirm failed: {type(e).__name__}: {e}")
    ok,detail=_wait_partial_close(symbol,direction,position_id,before,vol,oid)
    return (True,detail) if ok else (False,f"partial close unconfirmed; remaining={detail}")

def emergency_close_symbol(symbol):
    """Owner-triggered emergency close for exactly one actual MEXC symbol.

    Exchange positions are authoritative. Other symbols are never submitted for close.
    """
    if not ENABLED:
        return {"ok": False, "closed": False, "reason": "live pilot disabled"}
    if DRY_RUN:
        return {"ok": False, "closed": False, "reason": "dry-run write interlock active"}
    if not ACCESS_KEY or not SECRET_KEY:
        return {"ok": False, "closed": False, "reason": "MEXC credentials unavailable"}
    target=str(symbol or "").strip().upper().replace("/", "_").replace("-", "_")
    if target and "_" not in target:
        target += "_USDT"
    if not target:
        return {"ok": False, "closed": False, "reason": "missing symbol"}
    with _lock:
        try:
            ex=positions() or []
            ex=[p for p in ex if isinstance(p,dict) and float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)>0]
            matches=[p for p in ex if str(p.get("symbol") or p.get("contractCode") or "").upper()==target]
            if not matches:
                return {"ok": False, "closed": False, "symbol": target, "reason": "no open MEXC position for symbol"}
            closed=[]; errors=[]; nonce=int(time.time()*1000)
            for p in matches:
                pid=int(p.get("positionId") or p.get("id") or 0)
                vol=float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)
                direction=_position_direction(p)
                if pid<=0 or vol<=0 or direction not in {"LONG","SHORT"}:
                    errors.append(f"unrecognized {target} position: id={pid} vol={vol} direction={direction}")
                    continue
                ok,detail=_partial_market_close(target,direction,pid,vol,f"KILLSYM_{pid}_{nonce}","K")
                if ok: closed.append({"symbol":target,"direction":direction,"position_id":pid,"contracts":vol})
                else: errors.append(f"{target} {direction}: {detail}")
            remaining=[]
            for _ in range(8):
                now=positions() or []
                remaining=[p for p in now if isinstance(p,dict) and str(p.get("symbol") or p.get("contractCode") or "").upper()==target and float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)>0]
                if not remaining: break
                time.sleep(0.5)
            try: reconcile_once()
            except Exception as e: errors.append(f"post-close reconcile: {type(e).__name__}: {e}")
            done=not remaining
            return {"ok": done and not errors, "closed": done, "symbol":target, "fills":closed,
                    "remaining":[{"symbol":target,"contracts":float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)} for p in remaining],
                    "errors":errors}
        except Exception as e:
            return {"ok":False,"closed":False,"symbol":target,"reason":f"{type(e).__name__}: {e}"}


def emergency_flatten_all():
    """Owner-triggered emergency kill switch: flatten every actual MEXC position.

    This is deliberately separate from strategy/statistics logic. It does not
    create signals or paper trades. Exchange positions are authoritative and
    every close is confirmed before the function can report FLAT.
    """
    if not ENABLED:
        return {"ok": False, "flat": False, "reason": "live pilot disabled"}
    if DRY_RUN:
        return {"ok": False, "flat": False, "reason": "dry-run write interlock active"}
    if not ACCESS_KEY or not SECRET_KEY:
        return {"ok": False, "flat": False, "reason": "MEXC credentials unavailable"}
    with _lock:
        try:
            ex = positions() or []
            ex = [p for p in ex if isinstance(p, dict) and float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0) > 0]
            if not ex:
                try: reconcile_once()
                except Exception: pass
                return {"ok": True, "flat": True, "closed": [], "reason": "already flat"}
            closed=[]; errors=[]
            nonce=int(time.time()*1000)
            for p in ex:
                symbol=str(p.get("symbol") or p.get("contractCode") or "")
                pid=int(p.get("positionId") or p.get("id") or 0)
                vol=float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)
                direction=_position_direction(p)
                if not symbol or pid <= 0 or vol <= 0 or direction not in {"LONG","SHORT"}:
                    errors.append(f"unrecognized position: symbol={symbol or '?'} id={pid} vol={vol} direction={direction}")
                    continue
                kill_id=f"KILL_{pid}_{nonce}"
                ok,detail=_partial_market_close(symbol,direction,pid,vol,kill_id,"K")
                if ok:
                    closed.append({"symbol":symbol,"direction":direction,"position_id":pid,"contracts":vol})
                else:
                    errors.append(f"{symbol} {direction}: {detail}")
            # Exchange is the only authority for declaring success.
            remaining=[]
            for _ in range(8):
                remaining=positions() or []
                remaining=[p for p in remaining if isinstance(p,dict) and float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)>0]
                if not remaining: break
                time.sleep(0.5)
            try: reconcile_once()
            except Exception as e: errors.append(f"post-close reconcile: {type(e).__name__}: {e}")
            flat=not remaining
            return {"ok": flat and not errors, "flat": flat, "closed": closed,
                    "remaining": [{"symbol":str(p.get("symbol") or "?"),"contracts":float(p.get("holdVol") or 0)} for p in remaining],
                    "errors": errors}
        except Exception as e:
            return {"ok": False, "flat": False, "reason": f"{type(e).__name__}: {e}"}


def _change_position_protection(symbol, position_id, new_stop, tp3):
    """Ratchet the existing exchange-side entire-position TP/SL without removing protection."""
    rows=_active_protection(symbol, position_id)
    if len(rows) != 1:
        return False, f"expected exactly one active TP/SL, found {len(rows)}"
    x=rows[0]
    stop_id=int(x.get("id") or x.get("stopPlanOrderId") or 0)
    if stop_id <= 0:
        return False, "active TP/SL has no stopPlanOrderId"
    # MEXC can report an attached whole-position TP/SL as SAME initially and
    # SEPARATE after a partial position reduction. The change_plan_price API is
    # keyed by stopPlanOrderId and does not require profitLossVolType. Do not
    # reject a live protective order merely because MEXC changed this metadata;
    # safety is established by verifying the requested SL/TP prices afterwards.
    mode=str(x.get("profitLossVolType") or "").upper()
    if mode not in {"", "SAME", "SEPARATE"}:
        _diag(f"protection ratchet observed unfamiliar profitLossVolType={mode} on {symbol}; attempting verified price update")
    elif mode == "SEPARATE":
        _diag(f"protection ratchet accepted MEXC SEPARATE mode on {symbol} position {position_id}; verification required")
    new_stop_api=_mexc_price(symbol, new_stop)
    tp3_api=_mexc_price(symbol, tp3)
    _diag(f"MEXC precision normalized {symbol} ratchet stop {new_stop:.12g}->{new_stop_api} tp3 {tp3:.12g}->{tp3_api}")
    _signed("POST", "/api/v1/private/stoporder/change_plan_price", {"stopPlanOrderId":stop_id,"stopLossPrice":new_stop_api,"takeProfitPrice":tp3_api})
    if _confirm_protection(symbol, position_id, new_stop_api, tp3_api):
        return True, stop_id
    return False, "updated TP/SL not confirmed"

def _management_targets(row):
    # row layout is documented in reconcile_once below. Backfill older rows defensively.
    direction=row[2]; actual_entry=float(row[7] or row[6] or 0); initial_stop=float(row[11] or row[10] or 0)
    if actual_entry <= 0 or initial_stop <= 0:
        return None
    risk=abs(actual_entry-initial_stop)
    sign=1.0 if direction=="LONG" else -1.0
    tp1=float(row[12] or (actual_entry+sign*1.5*risk))
    tp2=float(row[13] or (actual_entry+sign*2.0*risk))
    tp3=float(row[14] or (actual_entry+sign*3.0*risk))
    be=actual_entry*(1.0 + sign*BE_BUFFER_BPS/10000.0)
    lock2=actual_entry+sign*TP2_LOCK_R*risk
    return {"entry":actual_entry,"risk":risk,"tp1":tp1,"tp2":tp2,"tp3":tp3,"be":be,"lock2":lock2}

def _target_reached(direction, price, target):
    return price >= target if direction=="LONG" else price <= target

def _manage_open_trade(row, exchange_pos):
    """Scale out at TP1/TP2 and ratchet the exchange-side stop. TP3 remains server-side."""
    if not MULTI_TP_ENABLED:
        return
    signal_id,symbol,direction,position_id,contracts,contract_size,paper_entry,actual_entry,stop_pct,opened_at,managed_stop,initial_stop,tp1,tp2,tp3,tp1_vol,tp2_vol,tp1_done,tp2_done,stage,*_extra=row
    targets=_management_targets(row)
    if not targets:
        halt(f"live manager cannot reconstruct targets for {symbol}")
        return
    # Backfill targets for old/new rows so restarts are deterministic.
    if not tp1 or not tp2 or not initial_stop:
        _db("""UPDATE fh_live_trades SET tp1_price=%s,tp2_price=%s,initial_stop_price=COALESCE(initial_stop_price,stop_price),managed_stop_price=COALESCE(managed_stop_price,stop_price),updated_at=NOW() WHERE signal_id=%s""",(targets["tp1"],targets["tp2"],signal_id))
    c=contract(symbol); step=float(c.get("volUnit") or 1); min_vol=float(c.get("minVol") or step)
    if not tp1_vol or not tp2_vol:
        split=_three_way_split(float(contracts),step,min_vol)
        if not split:
            halt(f"live manager cannot split {symbol} into three valid TP slices")
            return
        tp1_vol,tp2_vol,_=split
        _db("UPDATE fh_live_trades SET tp1_vol=%s,tp2_vol=%s,updated_at=NOW() WHERE signal_id=%s",(tp1_vol,tp2_vol,signal_id))
    px=_fair_price(symbol)
    if not tp1_done and _target_reached(direction,px,targets["tp1"]):
        ok,detail=_partial_market_close(symbol,direction,position_id,float(tp1_vol),signal_id,"P1")
        if not ok:
            if "position no longer open" in str(detail).lower() or "code=2009" in str(detail).lower():
                return
            halt(f"TP1 partial close failed/unconfirmed on {symbol}: {detail}")
            return
        _db("UPDATE fh_live_trades SET tp1_done=TRUE,tp1_time=NOW(),management_stage='TP1_FILLED',updated_at=NOW() WHERE signal_id=%s",(signal_id,))
        pnow=_active_position(symbol,position_id)
        if not pnow:
            return
        ok2,detail2=_change_position_protection(symbol,position_id,targets["be"],targets["tp3"])
        if not ok2:
            pnow=_active_position(symbol,position_id)
            if not pnow:
                return
            _emergency_close(symbol,direction,position_id,float(pnow.get("holdVol") or 0),signal_id)
            halt(f"TP1 filled on {symbol} but breakeven stop ratchet was not confirmed: {detail2}; remaining position flattened")
            return
        _db("UPDATE fh_live_trades SET stop_price=%s,managed_stop_price=%s,management_stage='TP1_LOCKED',updated_at=NOW() WHERE signal_id=%s",(targets["be"],targets["be"],signal_id))
        _msg(f"💰 V7 LIVE TP1 BANKED\n{symbol} {direction} | closed {float(tp1_vol):g} contracts near {px}\nRemaining stop → breakeven zone {targets['be']:.10g} | TP3 stays {targets['tp3']:.10g}")
        return
    # Retry a stop ratchet after restart/transient failure if TP1 was filled but lock not persisted.
    if tp1_done and not tp2_done and str(stage or '').upper() == 'TP1_FILLED':
        ok2,detail2=_change_position_protection(symbol,position_id,targets["be"],targets["tp3"])
        if ok2:
            _db("UPDATE fh_live_trades SET stop_price=%s,managed_stop_price=%s,management_stage='TP1_LOCKED',updated_at=NOW() WHERE signal_id=%s",(targets["be"],targets["be"],signal_id))
        return
    if tp1_done and not tp2_done and _target_reached(direction,px,targets["tp2"]):
        ok,detail=_partial_market_close(symbol,direction,position_id,float(tp2_vol),signal_id,"P2")
        if not ok:
            if "position no longer open" in str(detail).lower() or "code=2009" in str(detail).lower():
                return
            halt(f"TP2 partial close failed/unconfirmed on {symbol}: {detail}")
            return
        _db("UPDATE fh_live_trades SET tp2_done=TRUE,tp2_time=NOW(),management_stage='TP2_FILLED',updated_at=NOW() WHERE signal_id=%s",(signal_id,))
        pnow=_active_position(symbol,position_id)
        if not pnow:
            return
        ok2,detail2=_change_position_protection(symbol,position_id,targets["lock2"],targets["tp3"])
        if not ok2:
            pnow=_active_position(symbol,position_id)
            if not pnow:
                return
            _emergency_close(symbol,direction,position_id,float(pnow.get("holdVol") or 0),signal_id)
            halt(f"TP2 filled on {symbol} but +{TP2_LOCK_R:.2f}R stop ratchet was not confirmed: {detail2}; remaining position flattened")
            return
        _db("UPDATE fh_live_trades SET stop_price=%s,managed_stop_price=%s,management_stage='TP2_LOCKED',updated_at=NOW() WHERE signal_id=%s",(targets["lock2"],targets["lock2"],signal_id))
        _msg(f"💰💰 V7 LIVE TP2 BANKED\n{symbol} {direction} | closed {float(tp2_vol):g} contracts near {px}\nRemaining stop → +{TP2_LOCK_R:.2f}R ({targets['lock2']:.10g}) | TP3 stays {targets['tp3']:.10g}")
        return
    if tp2_done and str(stage or '').upper() == 'TP2_FILLED':
        ok2,detail2=_change_position_protection(symbol,position_id,targets["lock2"],targets["tp3"])
        if ok2:
            _db("UPDATE fh_live_trades SET stop_price=%s,managed_stop_price=%s,management_stage='TP2_LOCKED',updated_at=NOW() WHERE signal_id=%s",(targets["lock2"],targets["lock2"],signal_id))

def _history_for(symbol, position_id):
    """Best-effort closed-position lookup. MEXC can return code 2009 during close races."""
    attempts = [
        {"symbol":symbol,"page_num":1,"page_size":100},
        {"page_num":1,"page_size":100},
    ]
    for params in attempts:
        try:
            data=_signed("GET","/api/v1/private/position/list/history_positions",params) or {}
        except Exception as e:
            msg=str(e).lower()
            if "code=2009" in msg or "position is nonexistent or closed" in msg:
                continue
            raise
        rows=data.get("resultList",[]) if isinstance(data,dict) else (data or [])
        hit=next((x for x in rows if int(x.get("positionId") or 0)==int(position_id)),None)
        if hit:
            return hit
    return None

def _close_orders_for_position(symbol, position_id, direction):
    """Fallback audit source when position-history lags or returns code 2009."""
    try:
        data=_signed("GET","/api/v1/private/order/list/history_orders",{
            "symbol":symbol,"states":"3","page_num":1,"page_size":100
        }) or {}
    except Exception:
        return []
    rows=data.get("resultList",[]) if isinstance(data,dict) else (data or [])
    close_side = 4 if direction=="LONG" else 2
    out=[]
    for x in rows:
        try:
            if int(x.get("positionId") or 0)==int(position_id) and int(x.get("side") or 0)==close_side and int(x.get("state") or 0)==3:
                out.append(x)
        except Exception:
            pass
    return out

def _settle_missing_position(row):
    """Audit and close a ledger row only when MEXC confirms the position itself is gone."""
    signal_id,symbol,direction,position_id,contracts,contract_size,paper_entry,actual_entry,stop_pct = row[:9]
    hist=_history_for(symbol,position_id)
    orders=_close_orders_for_position(symbol,position_id,direction)
    if not hist and not orders:
        return False
    exitp=0.0; gross=0.0; fees=0.0; funding=0.0
    if hist:
        exitp=float(hist.get("closeAvgPrice") or hist.get("newCloseAvgPrice") or 0)
        gross=float(hist.get("closeProfitLoss") or hist.get("realised") or 0)
        fees=abs(float(hist.get("totalFee") or hist.get("fee") or 0))
        funding=float(hist.get("holdFee") or 0)
    if orders:
        dealt=[]
        order_profit=0.0; order_fees=0.0
        for x in orders:
            vol=float(x.get("dealVol") or 0); px=float(x.get("dealAvgPrice") or 0)
            if vol>0 and px>0: dealt.append((vol,px))
            order_profit += float(x.get("profit") or 0)
            order_fees += abs(float(x.get("takerFee") or 0)) + abs(float(x.get("makerFee") or 0))
        if exitp<=0 and dealt:
            tv=sum(v for v,_ in dealt); exitp=sum(v*px for v,px in dealt)/tv if tv>0 else 0.0
        if abs(gross) < 1e-12 and abs(order_profit) > 0:
            gross=order_profit
        if order_fees > fees:
            fees=order_fees
    net=gross-fees+funding
    entry=float(actual_entry or paper_entry or 0)
    risk_usdt=max(1e-9, float(contracts or 0) * float(contract_size or 0) * entry * (float(stop_pct or 0) / 100.0))
    _db("""UPDATE fh_live_trades SET status='CLOSED',actual_exit=%s,exit_fee=GREATEST(0,%s-entry_fee),funding=%s,gross_pnl=%s,net_pnl=%s,gross_r=%s,net_r=%s,closed_at=COALESCE(closed_at,NOW()),updated_at=NOW() WHERE signal_id=%s""",
        (exitp,fees,funding,gross,net,gross/risk_usdt,net/risk_usdt,signal_id))
    _msg(f"⚪ V7.4 LIVE CLOSED\n{symbol} {direction}\nExit: {exitp or 'audited via close orders'}\nGross: {gross:+.4f} USDT | Fees: -{fees:.4f} | Funding: {funding:+.4f}\nNet: {net:+.4f} USDT ({net/risk_usdt:+.2f}R)")
    return True


def reconcile_once():
    if not ENABLED: return
    with _lock:
        try:
            halted,_=halt_status(); ex=positions(); dbrows=_db("""SELECT signal_id,symbol,direction,position_id,contracts,contract_size,paper_entry,actual_entry,stop_pct,opened_at,managed_stop_price,initial_stop_price,tp1_price,tp2_price,tp3_price,tp1_vol,tp2_vol,tp1_done,tp2_done,management_stage,expiry_ts FROM fh_live_trades WHERE status='OPEN'""",fetch="all") or []
            exids={int(x.get("positionId") or 0):x for x in ex}; dbids={int(r[3] or 0):r for r in dbrows}
            unknown=[p for pid,p in exids.items() if pid not in dbids]
            if unknown:
                halt("restart reconciliation found exchange position not owned by V7 ledger; refusing new entries")
                return
            for pid,row in dbids.items():
                if pid in exids:
                    # Crypto defaults to the global 24h expiry. Strategies such as
                    # the equity cash-session ORB can supply an earlier hard expiry.
                    opened_at = row[9]
                    expiry_override = row[20] if len(row) > 20 else None
                    expired = False
                    expiry_label = None
                    if expiry_override is not None:
                        expired = datetime.now(timezone.utc) >= expiry_override
                        expiry_label = expiry_override.isoformat()
                    elif opened_at is not None:
                        age = (datetime.now(timezone.utc) - opened_at).total_seconds()
                        expired = age >= EXPIRY_HOURS * 3600
                        expiry_label = f"{EXPIRY_HOURS:g}h"
                    if expired:
                        _emergency_close(row[1], row[2], pid, float((exids[pid] or {}).get("holdVol") or row[4]), row[0])
                        _msg(f"⏰ V7 LIVE expiry close sent for {row[1]} at {expiry_label}")
                        continue
                    # Manage TP1/TP2 first. TP3 and stop remain exchange-side throughout.
                    _manage_open_trade(row, exids[pid])
                    # A server-side TP3 or manual/exchange close can race this pass. If the
                    # position vanished while we were managing it, do not touch stale TP/SL.
                    p_after=_active_position(row[1],pid)
                    if not p_after:
                        continue
                    tr=_db("SELECT stop_price,tp3_price FROM fh_live_trades WHERE signal_id=%s",(row[0],),"one")
                    if tr and not _confirm_protection(row[1],pid,float(tr[0]),float(tr[1])):
                        _emergency_close(row[1],row[2],pid,float(p_after.get("holdVol") or row[4]),row[0]); halt(f"protection disappeared on {row[1]}; flattened")
                    continue
                if not _settle_missing_position(row):
                    halt(f"exchange position disappeared but closure history is not yet auditable for {row[1]} position {pid}")
                    return
            a=asset(); limits=_dynamic_limits(a); eq=limits["equity"]
            if eq < limits["equity_kill"] and not halted: halt(f"equity kill-switch: {eq:.4f} < {limits['equity_kill']:.2f} USDT")
            if _daily_net_loss() >= limits["daily_loss_limit"] and not halted: halt(f"daily loss breaker reached: {_daily_net_loss():.4f} USDT")
            # Recover only narrowly-audited stale halts. Unknown positions, missing
            # protection, equity kill, daily breaker, etc. remain durable.
            _clear_transient_reconcile_halt_after_success()
            _clear_closed_position_race_halt_after_success()
            _clear_verified_ratchet_halt_after_success(ex, dbrows)
        except Exception as e:
            halt(f"reconciliation failure: {type(e).__name__}: {e}")


def startup_reconcile():
    _diag(f"startup entered version={V70_VERSION} enabled={ENABLED} armed={ARMED} db={bool(DATABASE_URL)} creds={bool(ACCESS_KEY and SECRET_KEY)}")
    if not ENABLED:
        _diag("startup skipped: V70_LIVE_ENABLED=false")
        return True
    if not init_db():
        _diag("startup failed: live ledger DB initialization failed")
        return False
    _diag("live ledger DB initialized/preserved")
    if not ACCESS_KEY or not SECRET_KEY:
        _diag("startup failed: MEXC credentials missing")
        halt("live enabled but MEXC credentials missing"); return False
    try:
        _diag("MEXC reconciliation beginning")
        reconcile_once(); a=asset(); p=positions()
        _limits=_dynamic_limits(a)
        halted, reason = halt_status()
        _diag(f"MEXC reconciliation complete equity={_limits['equity']:.4f} positions={len(p)} risk={_limits['risk_usdt']:.3f} max_notional={_limits['max_notional']:.2f} daily_breaker={_limits['daily_loss_limit']:.3f} equity_kill={_limits['equity_kill']:.2f} leverage_cap={MAX_LEVERAGE}x halted={halted} armed={ARMED}")
        if halted:
            _diag(f"durable HALT active: {reason}")
        _msg(f"🧪 V7.1.1 BALANCE-AWARE PILOT STARTUP\nArmed: {ARMED}\nEquity: {_limits['equity']:.4f} USDT\nExchange positions: {len(p)}\nRisk now: {_limits['risk_usdt']:.3f} | Max notional now: {_limits['max_notional']:.2f} | Daily breaker: {_limits['daily_loss_limit']:.3f} | Equity kill: {_limits['equity_kill']:.2f} | Max leverage: {MAX_LEVERAGE}x")
        return not halted
    except Exception as e:
        _diag(f"startup reconciliation exception: {type(e).__name__}: {e}")
        halt(f"startup reconciliation failed: {type(e).__name__}: {e}"); return False


def start_reconciler():
    if not ENABLED:
        _diag("background reconciler not started: live disabled")
        return None
    def loop():
        while True:
            reconcile_once(); time.sleep(RECONCILE_SECONDS)
    t=threading.Thread(target=loop,name="FH-V70-Reconciler",daemon=True); t.start()
    _diag(f"background reconciler started interval={RECONCILE_SECONDS}s")
    return t


def live_position_text():
    """Read-only MEXC + ledger snapshot for Telegram /liveposition.

    This function performs GETs only. It never submits, cancels, amends, closes,
    or changes leverage/margin. Exchange data is treated as source of truth for
    whether a position currently exists; the PostgreSQL ledger is shown beside it.
    """
    if not ENABLED:
        return "V7 Live Position: DISABLED"
    try:
        ex = positions() or []
        if isinstance(ex, dict):
            ex = ex.get("data") or ex.get("positions") or [ex]
        ex = [x for x in ex if isinstance(x, dict) and float(x.get("holdVol") or x.get("vol") or x.get("positionVol") or 0) > 0]
        halted, why = halt_status()
        gate = "HALTED — " + why if halted else ("ARMED" if ARMED else "SAFE/DISARMED")

        dbrows = _db("""SELECT signal_id,symbol,direction,status,actual_entry,contracts,leverage,
                               stop_price,tp1_price,tp2_price,tp3_price,protection_confirmed,opened_at,
                               tp1_done,tp2_done,management_stage,tp1_vol,tp2_vol
                        FROM fh_live_trades WHERE status IN ('SUBMITTING','ENTRY_SENT','PROTECTING','OPEN')
                        ORDER BY updated_at DESC LIMIT 5""", fetch="all") or []

        lines = ["V7 LIVE POSITION", f"Exchange open positions: {len(ex)}", f"Live gate: {gate}"]
        if not ex:
            lines.append("MEXC: FLAT — no open futures position reported.")
        for i, pos in enumerate(ex, 1):
            symbol = str(pos.get("symbol") or pos.get("contractCode") or "?")
            ptype = pos.get("positionType")
            direction = "LONG" if str(ptype) == "1" else ("SHORT" if str(ptype) == "2" else str(pos.get("direction") or "UNKNOWN").upper())
            vol = pos.get("holdVol", pos.get("vol", pos.get("positionVol", "?")))
            entry = pos.get("openAvgPrice", pos.get("avgPrice", pos.get("entryPrice", "?")))
            mark = pos.get("markPrice", pos.get("fairPrice", pos.get("lastPrice", "?")))
            lev = pos.get("leverage", "?")
            upl = pos.get("unrealisedPnl", pos.get("unrealizedPnl", pos.get("unrealizedProfit", "?")))
            pid = pos.get("positionId", pos.get("id", "?"))
            lines += [f"#{i} {symbol} {direction}", f"Entry: {entry} | Mark: {mark}", f"Contracts: {vol} | Leverage: {lev}x", f"Unrealized P&L: {upl} USDT | Position ID: {pid}"]

        if dbrows:
            lines.append("Ledger active rows:")
            for r in dbrows:
                # psycopg rows are tuples in this module
                sig,sym,direction,status,entry,contracts,lev,stop,tp1,tp2,tp3,protected,opened,p1,p2,stage,v1,v2 = r
                lines.append(f"• {sym} {direction} | {status} | entry={entry or '?'} | contracts={contracts or '?'} | {lev or '?'}x | STOP={stop or '?'} | TP1={tp1 or '?'} ({'DONE' if p1 else v1 or '?'}) | TP2={tp2 or '?'} ({'DONE' if p2 else v2 or '?'}) | TP3={tp3 or '?'} | stage={stage or '?'} | protected={'YES' if protected else 'NO'}")
        else:
            lines.append("Ledger active rows: 0")

        # A mismatch is important enough to surface loudly, but this read-only
        # command deliberately does not mutate state or halt trading by itself.
        if len(ex) != len([r for r in dbrows if str(r[3]).upper() == 'OPEN']):
            lines.append("⚠️ Exchange/ledger count mismatch — reconciler should be checked.")
        else:
            lines.append("Exchange/ledger position count: MATCH")
        lines.append("Read-only check: no order/write request sent.")
        return "\n".join(lines)
    except Exception as e:
        return f"V7 Live Position unavailable: {type(e).__name__}: {e}"


def live_pnl_text():
    if not ENABLED: return "V7.0 Live Pilot: DISABLED"
    try:
        a=asset(); eq=float(a.get("equity") or 0)
        row=_db("SELECT COALESCE(SUM(gross_pnl),0),COALESCE(SUM(entry_fee+exit_fee),0),COALESCE(SUM(funding),0),COALESCE(SUM(slippage_usdt),0),COALESCE(SUM(net_pnl),0),COUNT(*) FILTER(WHERE status='OPEN'),COUNT(*) FILTER(WHERE status='CLOSED') FROM fh_live_trades",fetch="one") or (0,0,0,0,0,0,0)
        halted,why=halt_status()
        limits=_dynamic_limits(a)
        return (f"V7.1.1 LIVE PILOT\nPilot starting balance: {PILOT_START_BALANCE:.2f} USDT\nCurrent equity: {eq:.2f} USDT\nRisk now: {limits['risk_usdt']:.3f} USDT | Daily breaker: {limits['daily_loss_limit']:.3f} USDT | Equity kill: {limits['equity_kill']:.2f} USDT\nMax notional now: {limits['max_notional']:.2f} USDT\nGross strategy P&L: {float(row[0]):+.2f}\nTrading fees: -{float(row[1]):.2f}\nFunding: {float(row[2]):+.2f}\nEntry slippage cost: {float(row[3]):+.2f}\nNet closed P&L: {float(row[4]):+.2f} USDT\nOpen / closed: {int(row[5])} / {int(row[6])}\nLive gate: {'HALTED — '+why if halted else ('ARMED' if ARMED else 'SAFE/DISARMED')}")
    except Exception as e: return f"V7.0 Live Pilot status unavailable: {e}"
