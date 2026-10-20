"""V8.6 gold EMA pullback — independent prospective PAPER experiment.
Original, transparent rules; no third-party strategy code copied.
No orders, position sizing, or live risk controls are touched.
"""
import os,sys,time,threading,math
import live_executor_v70 as live

ENABLED=os.getenv("V860_GOLD_PAPER_ENABLED","true").lower()=="true"
SCAN=max(300,int(os.getenv("V860_GOLD_SCAN_SECONDS","900")))
FEE_BPS=max(0,float(os.getenv("V860_GOLD_FEE_BPS","4")))
SLIP_BPS=max(0,float(os.getenv("V860_GOLD_SLIPPAGE_BPS","4")))
_LOCK=threading.Lock()
_PATCHED=False
_LAST_SCAN_LOG=0.0
SYMBOL="XAU_USDT"

def db(sql,args=(),mode=None):
    return live._db(sql,args,mode)

def schema():
    db("""CREATE TABLE IF NOT EXISTS fh_v86_gold_paper(
      source_key TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
      candle_ts DOUBLE PRECISION NOT NULL, direction TEXT NOT NULL,
      entry DOUBLE PRECISION NOT NULL, stop DOUBLE PRECISION NOT NULL,
      target DOUBLE PRECISION NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
      final_r DOUBLE PRECISION, exit_ts DOUBLE PRECISION,
      strategy TEXT NOT NULL DEFAULT 'EMA50_200_ATR_PULLBACK_H1',
      model_version TEXT NOT NULL DEFAULT 'v860-gold-paper-v1')""")

def candles(main):
    d=main.get_candles(SYMBOL,"Min60")
    if d is None or len(d)<215:return None
    return d.iloc[:-1].copy().sort_values("time").reset_index(drop=True)

def candidate(d):
    import pandas as pd
    x=d.close.astype(float); hi=d.high.astype(float); lo=d.low.astype(float)
    e50=x.ewm(span=50,adjust=False).mean();e200=x.ewm(span=200,adjust=False).mean()
    tr=pd.concat([(hi-lo),(hi-x.shift()).abs(),(lo-x.shift()).abs()],axis=1).max(axis=1)
    atr=tr.rolling(14).mean().iloc[-1]
    if not math.isfinite(atr) or atr<=0:return None
    close=float(x.iloc[-1]); prior=float(x.iloc[-2]); ma=float(e50.iloc[-1]); slow=float(e200.iloc[-1])
    direction=None
    # Closed-bar trend pullback/reclaim. No lookahead; next-bar fills NOT claimed.
    if ma>slow and prior<float(e50.iloc[-2]) and close>ma and close>prior:direction="LONG"
    if ma<slow and prior>float(e50.iloc[-2]) and close<ma and close<prior:direction="SHORT"
    if not direction:return None
    sign=1 if direction=="LONG" else -1
    stop=close-sign*1.5*atr; target=close+sign*2.0*abs(close-stop)
    return (str(int(float(d.time.iloc[-1])))+":"+direction,float(d.time.iloc[-1]),direction,close,stop,target)

def settle(main,d):
    rows=db("SELECT source_key,candle_ts,direction,entry,stop,target FROM fh_v86_gold_paper WHERE status='OPEN'",(), "all") or []
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
            db("UPDATE fh_v86_gold_paper SET status='SETTLED',final_r=%s,exit_ts=%s WHERE source_key=%s AND status='OPEN'",(net,float(bar.time),key))
            count+=1
            break
    return count

def stats():
    row=db("""SELECT COUNT(*),COUNT(*) FILTER(WHERE status='SETTLED'),
      AVG(final_r) FILTER(WHERE status='SETTLED'),
      SUM(final_r) FILTER(WHERE status='SETTLED'),
      AVG(CASE WHEN final_r>0 THEN 1.0 ELSE 0.0 END) FILTER(WHERE status='SETTLED')
      FROM fh_v86_gold_paper""",(),"one")
    if not row:return "🥇 GOLD EMA PAPER: database unavailable"
    total,n,avg,totalr,win=row
    return ("🥇 GOLD EMA50/200 H1 PULLBACK — PAPER ONLY\n"
            f"Signals: {int(total or 0)} | Settled: {int(n or 0)} | Open: {int(total or 0)-int(n or 0)}\n"
            + (f"Simulated avg: {float(avg):+.3f}R | Total: {float(totalr):+.2f}R | Win: {float(win)*100:.1f}%" if n else "Simulated avg: awaiting prospective settlements")
            +"\nReference-close entries, stop-first OHLC, estimated fees/slippage; funding not charged; NOT exchange fills.")

def scan(main):
    global _LAST_SCAN_LOG
    d=candles(main)
    if time.time()-_LAST_SCAN_LOG>=21600:
        try:
            live._diag(f"V8.6 GOLD PAPER candle_health: {'unavailable/short (<215 raw)' if d is None else str(len(d))+' closed H1 bars; last_closed_time='+str(d.time.iloc[-1])}")
        except Exception:
            pass
        _LAST_SCAN_LOG=time.time()
    if d is None:return
    settle(main,d)
    item=candidate(d)
    if item:
        key,when,direction,entry,stop,target=item
        # Claim before alert; avoids duplicate signals on repeated scans.
        inserted=db("""INSERT INTO fh_v86_gold_paper(source_key,candle_ts,direction,entry,stop,target)
          VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING source_key""",
          (key,when,direction,entry,stop,target),"one")
        if inserted:
            main.send_telegram(f"🥇 GOLD EMA PAPER SIGNAL (NOT LIVE)\n{SYMBOL} {direction} H1\nReference {entry:.2f} | SL {stop:.2f} | TP {target:.2f}\nEMA50/200 trend + closed-bar reclaim\nPaper reference only; execution costs estimated.")
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
            if cmd in ("/gold","/goldpaper","/v86"):
                main.send_to_chat(chat,stats());return
            return oldcmd(chat,text)
        main.telegram_stats_message=report;main.handle_telegram_command=command
        _PATCHED=True
        threading.Thread(target=loop,args=(main,),daemon=True,name="V860GoldPaper").start()
        live._diag("V8.6 GOLD PAPER armed; live execution unchanged")
        return True
def bootstrap():
    for _ in range(720):
        try:
            main=sys.modules.get("__main__")
            if main is not None and patch(main):return
        except Exception:pass
        time.sleep(.5)
if ENABLED:threading.Thread(target=bootstrap,daemon=True,name="V860Bootstrap").start()
