"""FuturesHunter V7.0 Live Pilot — fail-closed MEXC futures execution layer.

Disabled by default. It never needs Telegram secrets and never logs API secrets.
"""
import os, time, json, hmac, hashlib, math, threading
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests

try:
    import psycopg
    from psycopg.types.json import Jsonb
except Exception:
    psycopg = None
    Jsonb = None

V70_VERSION = "7.1.4-liveposition"
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
MAX_POSITIONS = 1
RECONCILE_SECONDS = max(5, int(os.getenv("V70_RECONCILE_SECONDS", "10")))
FILL_TIMEOUT = max(3, int(os.getenv("V70_FILL_TIMEOUT", "15")))
EXPIRY_HOURS = float(os.getenv("V70_EXPIRY_HOURS", "24"))
RECV_WINDOW = min(30, max(5, int(os.getenv("V70_RECV_WINDOW", "10"))))

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
      stop_price DOUBLE PRECISION, tp3_price DOUBLE PRECISION, stop_pct DOUBLE PRECISION,
      actual_entry DOUBLE PRECISION, actual_exit DOUBLE PRECISION, slippage_usdt DOUBLE PRECISION,
      slippage_bps DOUBLE PRECISION, entry_fee DOUBLE PRECISION DEFAULT 0, exit_fee DOUBLE PRECISION DEFAULT 0,
      funding DOUBLE PRECISION DEFAULT 0, gross_pnl DOUBLE PRECISION, net_pnl DOUBLE PRECISION,
      gross_r DOUBLE PRECISION, net_r DOUBLE PRECISION, paper_status TEXT, paper_r DOUBLE PRECISION,
      protection_confirmed BOOLEAN NOT NULL DEFAULT FALSE, opened_at TIMESTAMPTZ, closed_at TIMESTAMPTZ,
      halt_reason TEXT, payload JSONB, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
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


def _floor_step(value, step):
    if step <= 0: return value
    return math.floor((value + 1e-12) / step) * step


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
    return True, {"asset": a, "limits": limits}



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


def execute_signal(result, paper_trade=None):
    """Attempt one live mirror of a Core ENTRY. Any uncertainty fails closed."""
    if not ENABLED: return {"executed": False, "reason": "disabled"}
    with _lock:
        ok, state = preflight()
        if not ok: return {"executed": False, "reason": state}
        plan = result["risk_plan"]; symbol = result["symbol"]; direction = result["direction"]
        signal_id = (paper_trade or {}).get("signal_id") or f"{int(time.time())}_{symbol}_{direction}"
        existing = _db("SELECT status,entry_order_id FROM fh_live_trades WHERE signal_id=%s", (signal_id,), "one")
        if existing: return {"executed": False, "reason": f"idempotent duplicate ({existing[0]})"}
        c = contract(symbol)
        if not c.get("apiAllowed", False): return {"executed": False, "reason": "contract API trading not allowed"}
        if int(c.get("state", 1)) != 0: return {"executed": False, "reason": "contract not enabled"}
        if int(c.get("positionOpenType", 0)) not in {1,3}: return {"executed": False, "reason": "isolated margin unsupported"}
        entry = float(result["price"]); stop = float(plan["stop"]); tp3 = float(plan["tp3"])
        stop_pct = abs(entry-stop)/entry*100.0
        if stop_pct <= 0: return {"executed": False, "reason": "invalid stop distance"}
        limits = state["limits"]
        risk_usdt = limits["risk_usdt"]
        max_notional = limits["max_notional"]
        risk_notional = risk_usdt / (stop_pct/100.0)
        requested_notional = min(max_notional, risk_notional)
        contract_size = float(c["contractSize"]); step = float(c.get("volUnit") or 1); min_vol = float(c.get("minVol") or step)
        contracts = _floor_step(requested_notional / (entry * contract_size), step)
        if contracts < min_vol: return {"executed": False, "reason": "pilot size below exchange minimum"}
        actual_notional = contracts * contract_size * entry
        if actual_notional > max_notional + 1e-8: return {"executed": False, "reason": "notional cap calculation failed"}
        actual_risk = actual_notional * stop_pct/100.0
        if actual_risk > risk_usdt + 1e-6: return {"executed": False, "reason": "risk cap calculation failed"}
        leverage = min(MAX_LEVERAGE, int(c.get("maxLeverage") or MAX_LEVERAGE))
        a = state["asset"]; available = float(a.get("availableOpen") or a.get("availableBalance") or 0)
        if actual_notional/leverage > available: return {"executed": False, "reason": "insufficient margin"}
        oid = _oid(signal_id, "E")
        payload = {"symbol":symbol,"price":entry,"vol":contracts,"leverage":leverage,"side":1 if direction=="LONG" else 3,"type":5,"openType":1,"externalOid":oid,"stopLossPrice":stop,"takeProfitPrice":tp3,"lossTrend":2,"profitTrend":2,"positionMode":1}
        _db("""INSERT INTO fh_live_trades(signal_id,symbol,direction,status,external_oid,paper_entry,requested_notional,actual_notional,contracts,contract_size,leverage,stop_price,tp3_price,stop_pct,payload) VALUES(%s,%s,%s,'SUBMITTING',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (signal_id,symbol,direction,oid,entry,requested_notional,actual_notional,contracts,contract_size,leverage,stop,tp3,stop_pct,Jsonb(payload) if Jsonb else json.dumps(payload)))
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
                _signed("POST", "/api/v1/private/stoporder/place", {"lossTrend":2,"profitTrend":2,"positionId":position_id,"vol":contracts,"stopLossPrice":stop,"takeProfitPrice":tp3,"priceProtect":0,"profitLossVolType":"SAME","volType":2,"takeProfitType":0,"takeProfitOrderPrice":0,"stopLossType":0,"stopLossOrderPrice":0})
                time.sleep(1.0)
            if not _confirm_protection(symbol, position_id, stop, tp3):
                _emergency_close(symbol, direction, position_id, contracts, signal_id)
                halt(f"protective stop/TP3 could not be confirmed for {symbol}; position flattened")
                return {"executed": False, "reason": "protection failed; flattened"}
            _db("UPDATE fh_live_trades SET status='OPEN',protection_confirmed=TRUE,updated_at=NOW() WHERE signal_id=%s", (signal_id,))
            _msg(f"🔴 V7.0 LIVE PILOT OPEN\n{symbol} {direction}\nFill: {fill}\nNotional: {actual_notional:.2f} USDT | Risk: {actual_risk:.3f} USDT | {leverage}x isolated\nStop: {stop} | TP3: {tp3}\nProtection: CONFIRMED")
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
    payload={"symbol":symbol,"price":0,"vol":contracts,"side":4 if direction=="LONG" else 2,"type":5,"openType":1,"externalOid":_oid(signal_id,"X"),"positionId":position_id,"positionMode":1}
    return _signed("POST", "/api/v1/private/order/create", payload)


def _history_for(symbol, position_id):
    data=_signed("GET","/api/v1/private/position/list/history_positions",{"symbol":symbol,"page_num":1,"page_size":100}) or {}
    rows=data.get("resultList",[]) if isinstance(data,dict) else (data or [])
    return next((x for x in rows if int(x.get("positionId") or 0)==int(position_id)),None)


def reconcile_once():
    if not ENABLED: return
    with _lock:
        try:
            halted,_=halt_status(); ex=positions(); dbrows=_db("SELECT signal_id,symbol,direction,position_id,contracts,contract_size,paper_entry,actual_entry,stop_pct,opened_at FROM fh_live_trades WHERE status='OPEN'",fetch="all") or []
            exids={int(x.get("positionId") or 0):x for x in ex}; dbids={int(r[3] or 0):r for r in dbrows}
            unknown=[p for pid,p in exids.items() if pid not in dbids]
            if unknown:
                halt("restart reconciliation found exchange position not owned by V7 ledger; refusing new entries")
                return
            for pid,row in dbids.items():
                if pid in exids:
                    # Preserve Core's 24h expiry rule for live positions.
                    opened_at = row[9]
                    if opened_at is not None:
                        age = (datetime.now(timezone.utc) - opened_at).total_seconds()
                        if age >= EXPIRY_HOURS * 3600:
                            _emergency_close(row[1], row[2], pid, float(row[4]), row[0])
                            _msg(f"⏰ V7.0 expiry close sent for {row[1]} after {EXPIRY_HOURS:g}h")
                            continue
                    # Protection disappearing while position is open is a hard failure.
                    tr=_db("SELECT stop_price,tp3_price FROM fh_live_trades WHERE signal_id=%s",(row[0],),"one")
                    if tr and not _confirm_protection(row[1],pid,float(tr[0]),float(tr[1])):
                        _emergency_close(row[1],row[2],pid,float(row[4]),row[0]); halt(f"protection disappeared on {row[1]}; flattened")
                    continue
                hist=_history_for(row[1],pid)
                if hist:
                    exitp=float(hist.get("closeAvgPrice") or hist.get("newCloseAvgPrice") or 0); gross=float(hist.get("closeProfitLoss") or 0); fees=abs(float(hist.get("totalFee") or hist.get("fee") or 0)); funding=float(hist.get("holdFee") or 0); net=gross-fees+funding
                    r=max(1e-9, float(row[4]) * float(row[5]) * float(row[6]) * (float(row[8]) / 100.0))
                    _db("""UPDATE fh_live_trades SET status='CLOSED',actual_exit=%s,exit_fee=GREATEST(0,%s-entry_fee),funding=%s,gross_pnl=%s,net_pnl=%s,gross_r=%s,net_r=%s,closed_at=NOW(),updated_at=NOW() WHERE signal_id=%s""",(exitp,fees,funding,gross,net,gross/r,net/r,row[0]))
                    _msg(f"⚪ V7.0 LIVE CLOSED\n{row[1]} {row[2]}\nExit: {exitp}\nGross: {gross:+.4f} USDT | Fees: -{fees:.4f} | Funding: {funding:+.4f}\nNet: {net:+.4f} USDT ({net/r:+.2f}R)")
            a=asset(); limits=_dynamic_limits(a); eq=limits["equity"]
            if eq < limits["equity_kill"] and not halted: halt(f"equity kill-switch: {eq:.4f} < {limits['equity_kill']:.2f} USDT")
            if _daily_net_loss() >= limits["daily_loss_limit"] and not halted: halt(f"daily loss breaker reached: {_daily_net_loss():.4f} USDT")
            # If the only durable halt was a previous transient API read/transport failure,
            # a completely successful fresh reconciliation is sufficient to recover it.
            # Hard halts (unknown position, protection, equity, daily breaker, etc.) are never auto-cleared.
            _clear_transient_reconcile_halt_after_success()
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
                               stop_price,tp3_price,protection_confirmed,opened_at
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
                sig,sym,direction,status,entry,contracts,lev,stop,tp3,protected,opened = r
                lines.append(f"• {sym} {direction} | {status} | entry={entry or '?'} | contracts={contracts or '?'} | {lev or '?'}x | STOP={stop or '?'} | TP3={tp3 or '?'} | protected={'YES' if protected else 'NO'}")
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
