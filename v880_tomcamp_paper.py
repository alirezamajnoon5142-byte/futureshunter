"""V8.8 Tom Camp-inspired 4H sweep/engulfing prospective PAPER experiment. No orders."""
import os,sys,time,threading,math
import pandas as pd
import live_executor_v70 as live
ENABLED=os.getenv("V880_PAPER_ENABLED","true").lower()=="true"
# MEXC-listed proxies: SP500_USDT tracks S&P 500 exposure, NOT CME ES futures.
# Every instrument remains research-only; unavailable feeds are reported rather than substituted.
UNIVERSE=(
    ("NAS100_USDT","NASDAQ"),
    ("SP500_USDT","ES proxy"),
    ("US30_USDT","DOW"),
    ("XAU_USDT","XAU"),
    ("ONDO_USDT","ONDO"),
    ("BTC_USDT","BTC"),
    ("ETH_USDT","ETH"),
    ("SOL_USDT","SOL"),
    ("AAVE_USDT","AAVE"),
    ("BNB_USDT","BNB"),
    ("XRP_USDT","XRP"),
    ("LINK_USDT","LINK"),
    ("DOGE_USDT","DOGE"),
    ("ADA_USDT","ADA"),
)
SYMBOLS=tuple(symbol for symbol,_label in UNIVERSE)
LABELS=dict(UNIVERSE)
HEALTH={}
START_TS=time.time()
BAR_SECONDS=4*3600
SCAN_SECONDS=max(300,int(os.getenv("V880_SCAN_SECONDS","900")))
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
    # MEXC API documents Hour4, not Min240. Reject forming/invalid candles.
    d=main.get_candles(symbol,"Hour4")
    if d is None or len(d)<65:return None
    d=d.copy().sort_values("time").reset_index(drop=True)
    for col in ("time","open","high","low","close"):
        d[col]=pd.to_numeric(d[col],errors="coerce")
    d=d.dropna(subset=["time","open","high","low","close"])
    d=d[(d.time+BAR_SECONDS)<=time.time()-60].reset_index(drop=True)
    return d if len(d)>=60 else None
def candidate(d):
    # The last two CLOSED bars are manipulation and engulfing confirmation.
    # Pivot levels must be confirmed BEFORE the manipulation candle.
    n=len(d);p=d.iloc[-2];c=d.iloc[-1];base=d.iloc[:-2]
    lows=[float(base.low.iloc[i]) for i in range(2,len(base)-2)
          if float(base.low.iloc[i])<min(base.low.iloc[i-2],base.low.iloc[i-1],base.low.iloc[i+1],base.low.iloc[i+2])]
    highs=[float(base.high.iloc[i]) for i in range(2,len(base)-2)
           if float(base.high.iloc[i])>max(base.high.iloc[i-2],base.high.iloc[i-1],base.high.iloc[i+1],base.high.iloc[i+2])]
    if len(lows)<2 or len(highs)<2:return None
    up=highs[-1]>highs[-2] and lows[-1]>lows[-2]
    down=highs[-1]<highs[-2] and lows[-1]<lows[-2]
    bull=(up and p.low<lows[-1] and p.close>lows[-1]
          and c.close>c.open and c.open<=p.close and c.close>p.open and c.close>p.high)
    bear=(down and p.high>highs[-1] and p.close<highs[-1]
          and c.close<c.open and c.open>=p.close and c.close<p.open and c.close<p.low)
    if bull==bear:return None
    atr=pd.concat([d.high-d.low,(d.high-d.close.shift()).abs(),(d.low-d.close.shift()).abs()],
                  axis=1).max(axis=1).rolling(14).mean().iloc[-1]
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
            if d is None:
                HEALTH[symbol]="no 4H candles"
                continue
            recent_close=float(d.time.iloc[-1])+BAR_SECONDS
            age=time.time()-recent_close
            HEALTH[symbol]=("fresh" if age<=BAR_SECONDS+900 else f"stale {age/3600:.0f}h")
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
            # Never mint a retrospective paper setup on restart or across index market closure.
            if recent_close<START_TS or age>BAR_SECONDS+900:continue
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
        except Exception as e:
            HEALTH[symbol]=f"error {type(e).__name__}"
            live._diag(f"V8.8 paper scan {symbol}: {type(e).__name__}: {e}")
def stats(detail=False):
    # Show every configured market even when it has zero signals or no usable feed.
    try:
        rows=db("""SELECT s.symbol,e.target_r,COUNT(*),
                          COUNT(*) FILTER(WHERE e.status='SETTLED'),
                          AVG(e.net_r) FILTER(WHERE e.status='SETTLED'),
                          AVG(CASE WHEN e.net_r>0 THEN 1.0 ELSE 0.0 END)
                              FILTER(WHERE e.status='SETTLED')
                   FROM fh_v880_signals s JOIN fh_v880_exits e USING(key)
                   GROUP BY s.symbol,e.target_r""",(),"all") or []
    except Exception as exc:
        return f"V8.8 TOM CAMP 4H PAPER: database unavailable ({type(exc).__name__})"
    by={(str(sym),float(tp)):(int(total),int(settled),avg,win)
        for sym,tp,total,settled,avg,win in rows}
    lines=["V8.8 TOM CAMP-INSPIRED | 4H | PAPER ONLY",
           "Nasdaq=NAS100; ES=S&P500 proxy (NOT CME ES); all markets MEXC USDT perpetuals.",
           "Cost-estimated reference fills; ambiguous OHLC bars stop-first."]
    def part(item):
        if item is None:return "0/0 -"
        total,settled,avg,win=item
        return f"{settled}/{total} {float(avg):+.2f}R {float(win)*100:.0f}%" if settled else f"0/{total} -"
    lines.append("Markets (settled/signals, net avg R, net win):")
    for sym,label in UNIVERSE:
        one=by.get((sym,1.0))
        state=HEALTH.get(sym,"awaiting scan")
        lines.append(f"{label} {part(one)} | {state}")
    if detail:
        lines.append("TP variants per market: .75R | 1R | 1.5R | 2R")
        for sym,label in UNIVERSE:
            values=" / ".join(part(by.get((sym,tp))) for tp in TARGETS)
            lines.append(f"{label}: {values}")
        lines.append("Paper-only and non-exchange fills. No historical setup backfill; wait for settled samples.")
    else:
        lines.append("Detailed four-target comparison: /tomcamp")
    return "\n".join(lines)
def patch(main):
    global _PATCHED
    with _LOCK:
        if _PATCHED:return True
        if not all(hasattr(main,x) for x in ("get_candles","telegram_stats_message","handle_telegram_command","send_to_chat")):return False
        schema()
        old_stats=main.telegram_stats_message;old_cmd=main.handle_telegram_command
        def report():return str(old_stats()).rstrip()+"\n\n"+stats(detail=False)
        def command(chat,text):
            cmd=((text or "").strip().split() or [""])[0].lower().split("@")[0]
            if cmd=="/stats":
                # Telegram rejects >4096-character messages; keep legacy stats and
                # the required all-symbol 4H summary as TWO independent messages.
                main.send_to_chat(chat,str(old_stats()))
                main.send_to_chat(chat,stats(detail=False))
                return
            if cmd in ("/tomcamp","/v88"):main.send_to_chat(chat,stats(detail=True));return
            # Retired ICT commands explicitly point to the replacement paper research.
            if cmd in ("/ict","/dol","/v84","/biasbeacon"):
                main.send_to_chat(chat,"ICT/DOL research retired from active reporting. Use /tomcamp for V8.8 4H.")
                return
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
        time.sleep(SCAN_SECONDS)
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
