"""V8.7 BTC EMA crossover funding filter — independent prospective PAPER experiment.
Original, transparent rules; no third-party strategy code copied.
No orders, position sizing, or live risk controls are touched.
"""
import os,sys,time,threading,math
import live_executor_v70 as live

ENABLED=os.getenv("V870_BTC_PAPER_ENABLED","true").lower()=="true"
SCAN=max(300,int(os.getenv("V870_BTC_SCAN_SECONDS","900")))
FEE_BPS=max(0,float(os.getenv("V870_BTC_FEE_BPS","4")))
SLIP_BPS=max(0,float(os.getenv("V870_BTC_SLIPPAGE_BPS","4")))
_LOCK=threading.Lock()
_PATCHED=False
SYMBOL="BTC_USDT"

def db(sql,args=(),mode=None):
    return live._db(sql,args,mode)

def schema():
    db("""CREATE TABLE IF NOT EXISTS fh_v87_btc_paper(
      source_key TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
      candle_ts DOUBLE PRECISION NOT NULL, direction TEXT NOT NULL,
      entry DOUBLE PRECISION NOT NULL, stop DOUBLE PRECISION NOT NULL,
      target DOUBLE PRECISION NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
      final_r DOUBLE PRECISION, exit_ts DOUBLE PRECISION,
      strategy TEXT NOT NULL DEFAULT 'EMA200_600_FUNDING_H1',
      model_version TEXT NOT NULL DEFAULT 'v870-btc-paper-v1')""")

def candles(main):
    d=main.get_candles(SYMBOL,"Min60")
    if d is None or len(d)<650:return None
    return d.iloc[:-1].copy().sort_values("time").reset_index(drop=True)

def funding_ok():
    """Public funding snapshot, fail closed if absent or malformed."""
    import requests
    try:
        response=requests.get("https://contract.mexc.com/api/v1/contract/funding_rate/BTC_USDT",timeout=8)
        response.raise_for_status()
        payload=response.json()
        if not payload.get("success"):return False
        data=payload.get("data") or {}
        rate=float(data.get("fundingRate"))
        return math.isfinite(rate) and rate<=0.0001
    except Exception:return False

def candidate(d):
    x=d.close.astype(float)
    fast=x.ewm(span=200,adjust=False).mean()
    slow=x.ewm(span=600,adjust=False).mean()
    previous=float(fast.iloc[-2]-slow.iloc[-2])
    current=float(fast.iloc[-1]-slow.iloc[-1])
    # Only new bullish crosses; no shorts or synthetic historical backfill.
    if not (previous<=0 and current>0 and funding_ok()):return None
    hi=d.high.astype(float);lo=d.low.astype(float)
    import pandas as pd
    tr=pd.concat([hi-lo,(hi-x.shift()).abs(),(lo-x.shift()).abs()],axis=1).max(axis=1)
    atr=float(tr.rolling(14).mean().iloc[-1])
    if not math.isfinite(atr) or atr<=0:return None
    close=float(x.iloc[-1]);stop=close-2*atr;target=close+4*atr
    return (str(int(float(d.time.iloc[-1])))+":LONG",float(d.time.iloc[-1]),"LONG",close,stop,target)

def settle(main,d):
    rows=db("SELECT source_key,candle_ts,direction,entry,stop,target FROM fh_v87_btc_paper WHERE status='OPEN'",(), "all") or []
    count=0
    for key,when,direction,entry,stop,target in rows:
        risk=abs(float(entry)-float(stop))
        if risk<=0:continue
        future=d[d.time.astype(float)>float(when)]
        for _,bar in future.iterrows():
            hi=float(bar.high);lo=float(bar.low)
            loss=lo<=stop if direction=="LONG" else hi>=stop
            win=hi>=target if direction=="LONG" else lo<=target
            if not(loss or win):continue
            # Stop first on ambiguous candle. Approximate round-trip costs in R.
            gross=-1.0 if loss else abs(float(target)-float(entry))/risk
            cost=(FEE_BPS+SLIP_BPS)*1e-4*(2*float(entry))/risk
            net=gross-cost
            db("UPDATE fh_v87_btc_paper SET status='SETTLED',final_r=%s,exit_ts=%s WHERE source_key=%s AND status='OPEN'",(net,float(bar.time),key))
            count+=1
            break
    return count

def stats():
    row=db("""SELECT COUNT(*),COUNT(*) FILTER(WHERE status='SETTLED'),
      AVG(final_r) FILTER(WHERE status='SETTLED'),
      SUM(final_r) FILTER(WHERE status='SETTLED'),
      AVG(CASE WHEN final_r>0 THEN 1.0 ELSE 0.0 END) FILTER(WHERE status='SETTLED')
      FROM fh_v87_btc_paper""",(),"one")
    if not row:return "₿ BTC EMA PAPER: database unavailable"
    total,n,avg,totalr,win=row
    return ("₿ BTC EMA50/200 H1 PULLBACK — PAPER ONLY\n"
            f"Signals: {int(total or 0)} | Settled: {int(n or 0)} | Open: {int(total or 0)-int(n or 0)}\n"
            + (f"Net expectancy: {float(avg):+.3f}R | Total: {float(totalr):+.2f}R | Win: {float(win)*100:.1f}%" if n else "Net expectancy: awaiting prospective settlements")
            +"\nStop-first ambiguous bars; approximate fees/slippage; no funding. NOT live fills.")

def scan(main):
    d=candles(main)
    if d is None:return
    settle(main,d)
    item=candidate(d)
    if item:
        key,when,direction,entry,stop,target=item
        # Claim before alert; avoids duplicate signals on repeated scans.
        inserted=db("""INSERT INTO fh_v87_btc_paper(source_key,candle_ts,direction,entry,stop,target)
          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING source_key""",
          (key,when,direction,entry,stop,target),"one")
        if inserted:
            main.send_telegram(f"₿ BTC EMA PAPER SIGNAL (NOT LIVE)\n{SYMBOL} {direction} H1\nReference {entry:.2f} | SL {stop:.2f} | TP {target:.2f}\nEMA200/600 crossover + funding gate\nPaper reference only; execution costs estimated.")
def loop(main):
    time.sleep(100)
    while ENABLED:
        try:scan(main)
        except Exception as exc:
            try:live._diag(f"V8.6 gold research warning: {type(exc).__name__}: {exc}")
            except Exception:pass
        time.sleep(SCAN)
def patch(main):
    global _PATCHED
    with _LOCK:
        if _PATCHED:return True
        if not all(hasattr(main,k) for k in ("get_candles","telegram_stats_message","handle_telegram_command","send_telegram")):return False
        schema()
        oldstats=main.telegram_stats_message;oldcmd=main.handle_telegram_command
        def report():return str(oldstats()).rstrip()+"\n\n"+stats()
        def command(chat,text):
            cmd=((text or "").strip().split() or [""])[0].lower().split("@")[0]
            if cmd in ("/btc","/btcpaper","/v87"):
                main.send_to_chat(chat,stats());return
            return oldcmd(chat,text)
        main.telegram_stats_message=report;main.handle_telegram_command=command
        _PATCHED=True
        threading.Thread(target=loop,args=(main,),daemon=True,name="V870BTCPaper").start()
        live._diag("V8.7 BTC PAPER armed; live execution unchanged")
        return True
def bootstrap():
    for _ in range(720):
        try:
            main=sys.modules.get("__main__")
            if main is not None and patch(main):return
        except Exception:pass
        time.sleep(.5)
if ENABLED:threading.Thread(target=bootstrap,daemon=True,name="V860Bootstrap").start()
