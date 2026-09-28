"""FuturesHunter V8.4 ICT/DOL — prospective shadow research only.

Bias: closed 4H/1H structure + EMA alignment.
DOL: nearest external 1H/4H pivot liquidity in bias direction.
Entry: 15m liquidity sweep/reclaim -> 5m MSS displacement -> FVG retest.
Outcome: closed-5m first barrier, structural stop vs DOL, with cost estimate.
No live eligibility, sizing, leverage, stops, exits, or orders are modified.
"""
import json, os, sys, threading, time
from datetime import datetime, timezone
import live_executor_v70 as live
import v810_swing4h_live as swing

ENABLED=os.getenv("V840_ICT_DOL_ENABLED","true").lower()=="true"
SCAN=max(180,int(os.getenv("V840_ICT_DOL_SCAN_SECONDS","300")))
MAX_UNIVERSE=max(4,min(24,int(os.getenv("V840_ICT_DOL_MAX_UNIVERSE","12"))))
MIN_RR=max(1.0,float(os.getenv("V840_ICT_DOL_MIN_RR","1.5")))
HORIZON=max(6.0,float(os.getenv("V840_ICT_DOL_HORIZON_HOURS","24")))
FEE_BPS=max(0.0,float(os.getenv("V840_ICT_DOL_FEE_BPS","2")))
SLIP_BPS=max(0.0,float(os.getenv("V840_ICT_DOL_SLIP_BPS","3")))
START_TS=time.time()
_LOCK=threading.RLock(); _PATCHED=False; _SCHEMA=False

def f(v,d=0.0):
    try:return float(v if v is not None else d)
    except:return float(d)
def norm(v): return str(v or "").strip().upper()
def ts(v):
    try:
        if hasattr(v,"timestamp"): return float(v.timestamp())
        x=float(v); return x/1000 if x>1e12 else x
    except:return 0.0
def js(v): return live.Jsonb(v) if getattr(live,"Jsonb",None) else json.dumps(v)

def frame(main,sym,iv,n): return swing._closed_frame(main,sym,iv,n)
def atr(df,n=14):
    h,l,c=df.high.astype(float),df.low.astype(float),df.close.astype(float); p=c.shift(1)
    q=(h-l).to_frame("a"); q["b"]=(h-p).abs(); q["c"]=(l-p).abs()
    return f(q.max(axis=1).tail(n).mean())
def pivots(df,r=2,n=100):
    x=df.tail(n).reset_index(drop=True); out=[]
    for i in range(r,len(x)-r):
        w=x.iloc[i-r:i+r+1]; hi=f(x.iloc[i].high); lo=f(x.iloc[i].low); t=ts(x.iloc[i].time)
        if hi>=f(w.high.max()) and hi>f(x.iloc[i-1].high): out.append(("H",hi,t))
        if lo<=f(w.low.min()) and lo<f(x.iloc[i-1].low): out.append(("L",lo,t))
    return out
def tfscore(df):
    c=df.close.astype(float); e20=c.ewm(span=20,adjust=False).mean(); e50=c.ewm(span=50,adjust=False).mean(); s=0
    if f(c.iloc[-1])>f(e20.iloc[-1])>f(e50.iloc[-1]): s+=2
    elif f(c.iloc[-1])<f(e20.iloc[-1])<f(e50.iloc[-1]): s-=2
    s += 1 if f(e20.iloc[-1])>f(e20.iloc[-4]) else -1
    ps=pivots(df); hs=[x for x in ps if x[0]=="H"]; ls=[x for x in ps if x[0]=="L"]
    if len(hs)>1 and len(ls)>1:
        if hs[-1][1]>hs[-2][1] and ls[-1][1]>ls[-2][1]: s+=2
        elif hs[-1][1]<hs[-2][1] and ls[-1][1]<ls[-2][1]: s-=2
    return s
def bias(h4,h1):
    s=2*tfscore(h4)+tfscore(h1)
    return ("LONG",s) if s>=4 else (("SHORT",s) if s<=-4 else ("NEUTRAL",s))
def dol(direction,entry,h1,h4):
    a=[]
    for src,df,w,n in (("1H",h1,1,120),("4H",h4,2,80)):
        for k,p,t in pivots(df,2,n):
            if direction=="LONG" and k=="H" and p>entry:a.append((p-entry,-w,p,src,t))
            if direction=="SHORT" and k=="L" and p<entry:a.append((entry-p,-w,p,src,t))
    if not a:return None
    _,nw,p,src,t=sorted(a)[0]; return {"price":p,"source":src,"pivot_ts":t,"weight":-nw}
def sweep15(direction,m15):
    x=m15.reset_index(drop=True)
    for i in range(len(x)-1,max(19,len(x)-9),-1):
        pre=x.iloc[i-20:i]; row=x.iloc[i]
        if direction=="LONG":
            q=f(pre.low.min())
            if f(row.low)<q and f(row.close)>q:return {"ts":ts(row.time),"level":q,"extreme":f(row.low)}
        else:
            q=f(pre.high.max())
            if f(row.high)>q and f(row.close)<q:return {"ts":ts(row.time),"level":q,"extreme":f(row.high)}
    return None
def entry5(direction,m5,sw):
    x=m5.reset_index(drop=True); a=atr(x); med=f((x.close-x.open).abs().tail(30).median())
    ids=[i for i in range(len(x)) if ts(x.iloc[i].time)>=sw["ts"]]
    if not ids or a<=0:return None
    s0=ids[0]; pre=x.iloc[max(0,s0-10):s0]
    if len(pre)<6:return None
    level=f(pre.high.max()) if direction=="LONG" else f(pre.low.min()); mi=None
    for i in range(s0,len(x)):
        r=x.iloc[i]; o,h,l,c=map(f,(r.open,r.high,r.low,r.close)); rng=max(1e-9,h-l); body=abs(c-o)
        disp=body>=max(.55*a,1.2*med) and ((c>o and (h-c)/rng<=.3) if direction=="LONG" else (c<o and (c-l)/rng<=.3))
        if disp and ((c>level) if direction=="LONG" else (c<level)):mi=i;break
    if mi is None:return None
    gap=None
    for i in range(max(2,mi),min(len(x),mi+5)):
        if direction=="LONG" and f(x.iloc[i].low)>f(x.iloc[i-2].high): gap=(i,f(x.iloc[i-2].high),f(x.iloc[i].low),ts(x.iloc[i].time));break
        if direction=="SHORT" and f(x.iloc[i].high)<f(x.iloc[i-2].low): gap=(i,f(x.iloc[i].high),f(x.iloc[i-2].low),ts(x.iloc[i].time));break
    if not gap:return None
    gi,glo,ghi,gt=gap; mid=(glo+ghi)/2
    for j in range(gi+1,len(x)):
        r=x.iloc[j]; touch=f(r.low)<=ghi and f(r.high)>=glo; c=f(r.close); o=f(r.open)
        hold=c>=mid if direction=="LONG" else c<=mid; directional=c>o if direction=="LONG" else c<o
        if touch and hold and directional:return {"entry":c,"entry_ts":ts(r.time),"mss_ts":ts(x.iloc[mi].time),"mss_level":level,"fvg_ts":gt,"fvg_low":glo,"fvg_high":ghi,"atr":a}
    return None

def candidate(main,row):
    sym=norm(row.get("symbol"));
    if not sym:return None
    h4=frame(main,sym,"Hour4",55); time.sleep(.03); h1=frame(main,sym,"Min60",70); time.sleep(.03); m15=frame(main,sym,"Min15",70); time.sleep(.03); m5=frame(main,sym,"Min5",100)
    if any(x is None for x in (h4,h1,m15,m5)):return None
    direction,bs=bias(h4,h1)
    if direction=="NEUTRAL":return None
    sw=sweep15(direction,m15)
    if not sw:return None
    e=entry5(direction,m5,sw)
    if not e or e["entry_ts"]+300<START_TS:return None
    stop=sw["extreme"]-.15*e["atr"] if direction=="LONG" else sw["extreme"]+.15*e["atr"]; risk=abs(e["entry"]-stop)
    target=dol(direction,e["entry"],h1,h4)
    if not target or risk<=0:return None
    rr=abs(target["price"]-e["entry"])/risk
    if rr<MIN_RR:return None
    key=f"ictdol_{sym}_{direction}_{int(sw['ts'])}_{int(e['mss_ts'])}_{int(e['fvg_ts'])}"
    return {"key":key,"symbol":sym,"direction":direction,"bias":bs,"sweep":sw,"e":e,"stop":stop,"dol":target,"rr":rr}

def schema():
    global _SCHEMA
    if _SCHEMA:return True
    try:
        live._db("""CREATE TABLE IF NOT EXISTS fh_v84_ict_dol_shadow(source_key TEXT PRIMARY KEY,symbol TEXT NOT NULL,direction TEXT NOT NULL,bias_score DOUBLE PRECISION,sweep_ts DOUBLE PRECISION,sweep_level DOUBLE PRECISION,sweep_extreme DOUBLE PRECISION,mss_ts DOUBLE PRECISION,mss_level DOUBLE PRECISION,fvg_ts DOUBLE PRECISION,fvg_low DOUBLE PRECISION,fvg_high DOUBLE PRECISION,entry_ts DOUBLE PRECISION,entry DOUBLE PRECISION,stop DOUBLE PRECISION,dol DOUBLE PRECISION,dol_source TEXT,planned_rr DOUBLE PRECISION,status TEXT NOT NULL DEFAULT 'OPEN',outcome TEXT,exit_price DOUBLE PRECISION,gross_r DOUBLE PRECISION,cost_r DOUBLE PRECISION,final_r DOUBLE PRECISION,settled_at TIMESTAMPTZ,payload JSONB,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")
        _SCHEMA=True; return True
    except Exception as ex: live._diag(f"V8.4 schema warning {type(ex).__name__}: {ex}"); return False
def insert(c):
    if not schema():return False
    e,sw,d=c["e"],c["sweep"],c["dol"]
    r=live._db("""INSERT INTO fh_v84_ict_dol_shadow(source_key,symbol,direction,bias_score,sweep_ts,sweep_level,sweep_extreme,mss_ts,mss_level,fvg_ts,fvg_low,fvg_high,entry_ts,entry,stop,dol,dol_source,planned_rr,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(source_key) DO NOTHING RETURNING source_key""",(c["key"],c["symbol"],c["direction"],c["bias"],sw["ts"],sw["level"],sw["extreme"],e["mss_ts"],e["mss_level"],e["fvg_ts"],e["fvg_low"],e["fvg_high"],e["entry_ts"],e["entry"],c["stop"],d["price"],d["source"],c["rr"],js({"version":"8.4.0","shadow_only":True,"model":"HTF bias -> DOL -> sweep -> MSS -> FVG retest"})),"one")
    if r: live._diag(f"V8.4 ICT/DOL NEW {c['symbol']} {c['direction']} entry={e['entry']:.8g} stop={c['stop']:.8g} DOL={d['price']:.8g} rr={c['rr']:.2f}")
    return bool(r)
def settle(main):
    if not schema():return 0
    rows=live._db("SELECT source_key,symbol,direction,entry_ts,entry,stop,dol FROM fh_v84_ict_dol_shadow WHERE status='OPEN' ORDER BY created_at LIMIT 100",(),"all") or []; n=0; now=time.time()
    for key,sym,direction,et,entry,stop,target in rows:
        entry,stop,target=map(f,(entry,stop,target)); risk=abs(entry-stop); m5=frame(main,sym,"Min5",30)
        if risk<=0 or m5 is None:continue
        sign=1 if direction=="LONG" else -1; out=px=gross=ot=None
        for _,r in m5.iterrows():
            t=ts(r.time)
            if t<=f(et):continue
            sh=f(r.low)<=stop if sign==1 else f(r.high)>=stop; th=f(r.high)>=target if sign==1 else f(r.low)<=target
            if sh:out,px,gross,ot="STOP",stop,-1.0,t;break
            if th:out,px,gross,ot="DOL_HIT",target,abs(target-entry)/risk,t;break
        if out is None and now-f(et)>=HORIZON*3600:
            r=m5.iloc[-1]; px=f(r.close); ot=ts(r.time); out="HORIZON"; gross=sign*(px-entry)/risk
        if out is None:continue
        cost=(entry*2*(FEE_BPS+SLIP_BPS)/10000)/risk; final=f(gross)-cost
        live._db("UPDATE fh_v84_ict_dol_shadow SET status='SETTLED',outcome=%s,exit_price=%s,gross_r=%s,cost_r=%s,final_r=%s,settled_at=%s,updated_at=NOW() WHERE source_key=%s AND status='OPEN'",(out,px,gross,cost,final,datetime.fromtimestamp(ot or now,tz=timezone.utc),key)); n+=1
    return n
def stats():
    if not schema():return "🧭 V8.4 ICT/DOL SHADOW\nDatabase unavailable."
    a=live._db("SELECT COUNT(*),COUNT(*) FILTER(WHERE status='OPEN'),COUNT(*) FILTER(WHERE status='SETTLED'),COUNT(*) FILTER(WHERE outcome='DOL_HIT'),COALESCE(AVG(final_r) FILTER(WHERE status='SETTLED'),0),COALESCE(SUM(final_r) FILTER(WHERE status='SETTLED'),0),MIN(created_at) FROM fh_v84_ict_dol_shadow",(),"one")
    latest=live._db("SELECT symbol,direction,entry,stop,dol,planned_rr,status,outcome,final_r FROM fh_v84_ict_dol_shadow ORDER BY created_at DESC LIMIT 3",(),"all") or []
    if not a:return "🧭 V8.4 ICT/DOL SHADOW\nWarming."
    total,op,res,hit,avg,sr,started=a; total,op,res,hit=map(lambda x:int(x or 0),(total,op,res,hit))
    lines=["🧭 V8.4 ICT/DOL SHADOW — prospective",f"Signals: {total} | Open: {op} | Resolved: {res}",f"DOL hit: {100*hit/res:.1f}% | Avg: {f(avg):+.2f}R | Total: {f(sr):+.2f}R" if res else "DOL hit / expectancy: warming",f"Cohort: {started}" if started else "Cohort starts with first signal","No backfill. Closed-candle shadow only. Live execution unchanged."]
    for s,d,e,st,t,rr,status_,out,fr in latest: lines.append(f"{s} {d} | E {f(e):.8g} S {f(st):.8g} DOL {f(t):.8g} | {f(rr):.2f}R | {status_}"+(f" {out} {f(fr):+.2f}R" if fr is not None else ""))
    return "\n".join(lines)
def scan(main):
    rows=list(swing._universe(main)); by={norm(r.get("symbol")):r for r in rows if isinstance(r,dict) and norm(r.get("symbol"))}
    for s in ("XAU_USDT","BTC_USDT","ETH_USDT"):by.setdefault(s,{"symbol":s,"turnover":0})
    vals=sorted(by.values(),key=lambda r:f(r.get("turnover")),reverse=True)[:MAX_UNIVERSE]; added=0
    for r in vals:
        try:
            c=candidate(main,r); added += 1 if c and insert(c) else 0
        except Exception as ex: live._diag(f"V8.4 scan {r.get('symbol')} warning {type(ex).__name__}: {ex}")
        time.sleep(.05)
    live._diag(f"V8.4 ICT/DOL scan universe={len(vals)} new={added}")
def loop(main):
    time.sleep(75)
    while ENABLED:
        try:
            n=settle(main); scan(main)
            if n:live._diag(f"V8.4 settled {n} ICT/DOL trade(s)")
        except Exception as ex: live._diag(f"V8.4 loop warning {type(ex).__name__}: {ex}")
        time.sleep(SCAN)
def patch(main):
    global _PATCHED
    with _LOCK:
        if _PATCHED:return True
        needed=("get_candles","handle_telegram_command","telegram_stats_message","send_to_chat")
        if any(not hasattr(main,x) for x in needed):return False
        pc,ps=main.handle_telegram_command,main.telegram_stats_message
        def cmd(chat,text):
            c=((text or "").strip().split() or [""])[0].lower().split("@")[0]
            if c in {"/ict","/dol","/v84","/biasbeacon"}:main.send_to_chat(chat,stats());return
            return pc(chat,text)
        def smsg():return str(ps()).rstrip()+"\n\n"+stats()
        main.handle_telegram_command=cmd; main.telegram_stats_message=smsg; _PATCHED=True; schema()
        threading.Thread(target=loop,args=(main,),name="V840ICTDOL",daemon=True).start()
        live._diag(f"V8.4 ICT/DOL armed SHADOW_ONLY=True scan={SCAN}s universe<={MAX_UNIVERSE} min_rr={MIN_RR:.2f} live_gate_unchanged=True no_backfill=True")
        return True
def bootstrap():
    end=time.time()+360
    while time.time()<end:
        try:
            main=sys.modules.get("__main__")
            if main is not None and patch(main):return
        except Exception as ex:
            try:live._diag(f"V8.4 bootstrap retry {type(ex).__name__}: {ex}")
            except:pass
        time.sleep(.5)
if ENABLED:
    threading.Thread(target=bootstrap,name="V840Bootstrap",daemon=True).start()
    try:live._diag("V8.4 ICT/DOL bootstrap armed")
    except:pass
