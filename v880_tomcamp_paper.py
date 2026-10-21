"""V8.8 Tom Camp-inspired 4H sweep/engulfing prospective PAPER experiment. No orders."""
import os,sys,time,threading,math
import pandas as pd
import live_executor_v70 as live
ENABLED=os.getenv("V880_PAPER_ENABLED","true").lower()=="true"
SYMBOLS=("BTC_USDT","ETH_USDT","SOL_USDT","AAVE_USDT")
TARGETS=(.75,1.,1.5,2.)
FEE_BPS=4.;SLIP_BPS=4.
_PATCHED=False
_LOCK=threading.Lock()
def db(q,args=(),mode=None):return live._db(q,args,mode)
def schema():
    db("""CREATE TABLE IF NOT EXISTS fh_v880_signals(
        key TEXT PRIMARY KEY,symbol TEXT NOT NULL,bar_ts DOUBLE PRECISION NOT NULL,
        direction TEXT NOT NULL,entry DOUBLE PRECISION NOT NULL,stop DOUBLE PRECISION NOT NULL,
        fvg BOOLEAN NOT NULL,room_r DOUBLE PRECISION,created_at TIMESTAMPTZ DEFAULT NOW())""")
    db("""CREATE TABLE IF NOT EXISTS fh_v880_exits(
        key TEXT NOT NULL REFERENCES fh_v880_signals(key),target_r DOUBLE PRECISION NOT NULL,
        status TEXT NOT NULL DEFAULT 'OPEN',net_r DOUBLE PRECISION,exit_ts DOUBLE PRECISION,
        PRIMARY KEY(key,target_r))""")
def candles(main,symbol):
    d=main.get_candles(symbol,"Min240")
    if d is None or len(d)<65:return None
    d=d.iloc[:-1].copy().sort_values("time").reset_index(drop=True)
    for col in ("time","open","high","low","close"):d[col]=pd.to_numeric(d[col],errors="coerce")
    d=d.dropna(subset=["time","open","high","low","close"])
    return d if len(d)>=60 else None
def candidate(d):
    n=len(d);p=d.iloc[-2];c=d.iloc[-1]
    lows=[float(d.low.iloc[i]) for i in range(2,n-3) if float(d.low.iloc[i])<min(d.low.iloc[i-2],d.low.iloc[i-1],d.low.iloc[i+1],d.low.iloc[i+2])]
    highs=[float(d.high.iloc[i]) for i in range(2,n-3) if float(d.high.iloc[i])>max(d.high.iloc[i-2],d.high.iloc[i-1],d.high.iloc[i+1],d.high.iloc[i+2])]
    if not lows or not highs:return None
    bull=(p.low<lows[-1] and p.close>lows[-1] and c.close>c.open and c.open<=p.close and c.close>p.open and c.close>p.high)
    bear=(p.high>highs[-1] and p.close<highs[-1] and c.close<c.open and c.open>=p.close and c.close<p.open and c.close<p.low)
    if bull==bear:return None
    atr=pd.concat([d.high-d.low,(d.high-d.close.shift()).abs(),(d.low-d.close.shift()).abs()],axis=1).max(axis=1).rolling(14).mean().iloc[-1]
    if not math.isfinite(atr) or atr<=0:return None
    direction="LONG" if bull else "SHORT";entry=float(c.close)
    stop=float(min(p.low,c.low)-.1*atr if bull else max(p.high,c.high)+.1*atr)
    risk=abs(entry-stop)
    if risk<=0:return None
    fvg=bool(d.high.iloc[-3]<c.low) if bull else bool(d.low.iloc[-3]>c.high)
    opposing=[v for v in highs if v>entry] if bull else [v for v in lows if v<entry]
    room=((min(opposing)-entry)/risk if bull else (entry-max(opposing))/risk) if opposing else None
    return direction,entry,stop,risk,fvg,room,float(c.time)
def scan(main):
    for symbol in SYMBOLS:
        try:
            d=candles(main,symbol)
            if d is None:continue
            rows=db("""SELECT s.key,s.bar_ts,s.direction,s.entry,s.stop,e.target_r
                       FROM fh_v880_signals s JOIN fh_v880_exits e USING(key)
                       WHERE s.symbol=%s AND e.status='OPEN'""",(symbol,),"all") or []
            for key,when,direction,entry,stop,tr in rows:
                risk=abs(float(entry)-float(stop));tr=float(tr)
                if risk<=0:continue
                target=float(entry)+(1 if direction=="LONG" else -1)*risk*tr
                for _,bar in d[d.time>float(when)].iterrows():
                    hit_stop=(bar.low<=stop if direction=="LONG" else bar.high>=stop)
                    hit_tp=(bar.high>=target if direction=="LONG" else bar.low<=target)
                    if not(hit_stop or hit_tp):continue
                    net=(-1. if hit_stop else tr)-2*float(entry)*(FEE_BPS+SLIP_BPS)*1e-4/risk
                    db("UPDATE fh_v880_exits SET status='SETTLED',net_r=%s,exit_ts=%s WHERE key=%s AND target_r=%s AND status='OPEN'",(net,float(bar.time),key,tr))
                    break
            item=candidate(d)
            if item is None:continue
            direction,entry,stop,risk,fvg,room,when=item
            key=f"{symbol}:{int(when)}:{direction}:v880"
            created=db("""INSERT INTO fh_v880_signals(key,symbol,bar_ts,direction,entry,stop,fvg,room_r)
                       VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING key""",
                       (key,symbol,when,direction,entry,stop,fvg,room),"one")
            if created:
                for tr in TARGETS:db("INSERT INTO fh_v880_exits(key,target_r) VALUES(%s,%s) ON CONFLICT DO NOTHING",(key,tr))
                live._diag(f"V8.8 PAPER {symbol} {direction} entry={entry:g} stop={stop:g} fvg={fvg} roomR={room}")
        except Exception as e:live._diag(f"V8.8 paper scan {symbol}: {type(e).__name__}: {e}")
def stats():
    rows=db("""SELECT target_r,COUNT(*),COUNT(*) FILTER(WHERE status='SETTLED'),
               AVG(net_r) FILTER(WHERE status='SETTLED'),
               AVG(CASE WHEN net_r>0 THEN 1.0 ELSE 0.0 END) FILTER(WHERE status='SETTLED')
               FROM fh_v880_exits GROUP BY target_r ORDER BY target_r""",(),"all") or []
    lines=["V8.8 TOM CAMP-INSPIRED 4H — PAPER ONLY","Close-reference entries; stop-first OHLC; estimated costs; no exchange fills."]
    if not rows:lines.append("No prospective signals yet.")
    for tr,n,settled,avg,win in rows:
        lines.append(f"TP {float(tr):.2f}R: signals={n} settled={settled} avg_net={float(avg):+.3f}R win={float(win)*100:.1f}%" if settled else f"TP {float(tr):.2f}R: signals={n} settled=0")
    return "\n".join(lines)
def patch(main):
    global _PATCHED
    with _LOCK:
        if _PATCHED:return True
        if not all(hasattr(main,x) for x in ("get_candles","telegram_stats_message","handle_telegram_command","send_to_chat")):return False
        schema()
        old_stats=main.telegram_stats_message;old_cmd=main.handle_telegram_command
        def report():return str(old_stats()).rstrip()+"\n\n"+stats()
        def command(chat,text):
            cmd=((text or "").strip().split() or [""])[0].lower().split("@")[0]
            if cmd in ("/tomcamp","/v88"):main.send_to_chat(chat,stats());return
            return old_cmd(chat,text)
        main.telegram_stats_message=report;main.handle_telegram_command=command
        _PATCHED=True
        threading.Thread(target=loop,args=(main,),daemon=True,name="V880Paper").start()
        live._diag("V8.8 PAPER armed; existing live execution unchanged")
        return True
def loop(main):
    time.sleep(120)
    while ENABLED:
        scan(main)
        time.sleep(900)
def bootstrap():
    for _ in range(720):
        try:
            main=sys.modules.get("__main__")
            if main is not None and patch(main):return
        except Exception as e:
            try:live._diag(f"V8.8 bootstrap: {type(e).__name__}: {e}")
            except Exception:pass
        time.sleep(.5)
if ENABLED:threading.Thread(target=bootstrap,daemon=True,name="V880Bootstrap").start()
