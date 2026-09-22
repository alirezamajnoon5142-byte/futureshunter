from pathlib import Path

PATH = Path("FuturesHunter_Render.py")
text = PATH.read_text(encoding="utf-8")

if '"version":"7.8.3-breakout-aware-sr"' in text:
    print("V7.8.3 breakout-aware S/R already applied")
    raise SystemExit(0)


def replace_once(old, new, label):
    global text
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one anchor, found {count}")
    text = text.replace(old, new, 1)


old_constants = '''V71_SR_CAUTION_MIN_CORE = float(os.getenv("V71_SR_CAUTION_MIN_CORE", "90.0"))
V71_SR_CONTEXT_CACHE = {}
'''
new_constants = '''V71_SR_CAUTION_MIN_CORE = float(os.getenv("V71_SR_CAUTION_MIN_CORE", "90.0"))
# Breakout-aware refinement: a nearby zone may be treated as consumed only when
# CLOSED candles prove the market actually broke/held it. A wick never qualifies.
V71_SR_BREAKOUT_AWARE = os.getenv("V71_SR_BREAKOUT_AWARE", "true").lower() == "true"
V71_SR_BREAKOUT_15M_RV = max(1.0, float(os.getenv("V71_SR_BREAKOUT_15M_RV", "1.15")))
V71_SR_BREAKOUT_1H_RV = max(1.0, float(os.getenv("V71_SR_BREAKOUT_1H_RV", "1.05")))
V71_SR_BREAKOUT_SINGLE_15M_RV = max(V71_SR_BREAKOUT_15M_RV, float(os.getenv("V71_SR_BREAKOUT_SINGLE_15M_RV", "1.35")))
V71_SR_BREAKOUT_RETEST_TOL = max(0.05, float(os.getenv("V71_SR_BREAKOUT_RETEST_TOL", "0.20")))
V71_SR_CONTEXT_CACHE = {}
'''
replace_once(old_constants, new_constants, "S/R constants")


helper_anchor = '''def _v71_structural_room(result):
    """Map major 1h/4h S/R zones and express nearest opposing structure in R."""
'''
helpers = '''def _v71_sr_candle_stats(closed, offset=-1):
    """Return scale-free quality/volume stats for one CLOSED candle."""
    if closed is None or len(closed) < 3 or abs(int(offset)) > len(closed):
        return None
    pos = len(closed) + int(offset) if int(offset) < 0 else int(offset)
    if pos < 0 or pos >= len(closed):
        return None
    row = closed.iloc[pos]
    high=num(row.get("high")); low=num(row.get("low")); open_=num(row.get("open")); close=num(row.get("close"))
    rng=max(high-low,1e-12)
    prior=closed.iloc[max(0,pos-20):pos]
    avg_vol=num(prior["volume"].mean()) if len(prior) else 0.0
    volume=num(row.get("volume"))
    rv=volume/avg_vol if avg_vol > 0 else 0.0
    return {
        "open":open_,"high":high,"low":low,"close":close,"rv":rv,
        "body":(close-open_)/rng,"close_location":(close-low)/rng,
        "time":normalize_candle_time(row.get("time")),
    }


def _v71_sr_breakout_evidence(direction, zone, df15, df1, entry, tolerance):
    """Decide whether a major opposing zone has been freshly broken and held.

    Evidence is deliberately strict and uses CLOSED candles only. Confirmations:
      * decisive 1h close beyond the far edge with directional body + volume; or
      * two consecutive 15m closes beyond the far edge with participation; or
      * a 15m breakout followed by a closed retest/hold; or
      * an exceptional single 15m expansion candle with much stronger volume.
    Live price must still hold at least the breakout half of the zone. A wick or
    a failed move back through the zone center cannot consume structure.
    """
    if not V71_SR_BREAKOUT_AWARE or df15 is None or df1 is None:
        return {"confirmed":False,"reason":"breakout-aware S/R disabled/unavailable"}
    closed15=df15.iloc[:-1]
    closed1=df1.iloc[:-1]
    if len(closed15) < 25 or len(closed1) < 25:
        return {"confirmed":False,"reason":"insufficient closed candles for breakout confirmation"}

    latest15=_v71_sr_candle_stats(closed15,-1)
    prev15=_v71_sr_candle_stats(closed15,-2)
    latest1=_v71_sr_candle_stats(closed1,-1)
    if not latest15 or not prev15 or not latest1:
        return {"confirmed":False,"reason":"breakout candle metrics unavailable"}

    direction=str(direction or "").upper()
    low=num(zone.get("low")); high=num(zone.get("high")); center=(low+high)/2.0
    margin=max(0.0,0.03*num(tolerance))
    retest_band=max(0.0,V71_SR_BREAKOUT_RETEST_TOL*num(tolerance))

    if direction == "LONG":
        edge=high
        beyond=lambda c: num(c.get("close")) > edge + margin
        quality15=lambda c,body=0.30,loc=0.62: num(c.get("body")) >= body and num(c.get("close_location")) >= loc
        quality1=lambda c: num(c.get("body")) >= 0.25 and num(c.get("close_location")) >= 0.62
        live_hold=entry >= center
        retest_touch=num(latest15.get("low")) <= edge + retest_band
        retest_close=num(latest15.get("close")) > edge
    else:
        edge=low
        beyond=lambda c: num(c.get("close")) < edge - margin
        quality15=lambda c,body=0.30,loc=0.38: num(c.get("body")) <= -body and num(c.get("close_location")) <= loc
        quality1=lambda c: num(c.get("body")) <= -0.25 and num(c.get("close_location")) <= 0.38
        live_hold=entry <= center
        retest_touch=num(latest15.get("high")) >= edge - retest_band
        retest_close=num(latest15.get("close")) < edge

    decisive_1h=(
        beyond(latest1) and quality1(latest1)
        and num(latest1.get("rv")) >= V71_SR_BREAKOUT_1H_RV
    )
    two_15m=(
        beyond(prev15) and beyond(latest15)
        and num(latest15.get("close_location")) >= 0.58 if direction == "LONG" else
        beyond(prev15) and beyond(latest15) and num(latest15.get("close_location")) <= 0.42
    )
    two_15m=bool(two_15m and max(num(prev15.get("rv")),num(latest15.get("rv"))) >= V71_SR_BREAKOUT_15M_RV)
    retest_hold=bool(
        beyond(prev15) and quality15(prev15,0.25,0.60 if direction == "LONG" else 0.40)
        and num(prev15.get("rv")) >= V71_SR_BREAKOUT_15M_RV
        and retest_touch and retest_close
        and (num(latest15.get("close_location")) >= 0.55 if direction == "LONG" else num(latest15.get("close_location")) <= 0.45)
    )
    exceptional_15m=bool(
        beyond(latest15) and quality15(latest15,0.45,0.72 if direction == "LONG" else 0.28)
        and num(latest15.get("rv")) >= V71_SR_BREAKOUT_SINGLE_15M_RV
    )
    confirmed=bool(live_hold and (decisive_1h or two_15m or retest_hold or exceptional_15m))
    modes=[]
    if decisive_1h: modes.append("1H_DECISIVE")
    if two_15m: modes.append("2X15M_CLOSE")
    if retest_hold: modes.append("15M_RETEST_HOLD")
    if exceptional_15m: modes.append("15M_EXPANSION")
    return {
        "confirmed":confirmed,
        "modes":modes,
        "edge":edge,"zone_center":center,"live_hold":bool(live_hold),
        "latest_15m_close":num(latest15.get("close")),"latest_15m_rv":round(num(latest15.get("rv")),3),
        "latest_1h_close":num(latest1.get("close")),"latest_1h_rv":round(num(latest1.get("rv")),3),
        "reason":" + ".join(modes) if confirmed else ("break evidence failed/price no longer holding" if modes else "no closed-candle breakout confirmation"),
    }


def _v71_structural_room(result):
    """Map major 1h/4h S/R zones and express nearest opposing structure in R."""
'''
replace_once(helper_anchor, helpers, "breakout helper insertion")


old_fetch = '''        try:
            df1=get_candles(symbol,"Min60")
            time.sleep(0.05)
            df4=get_candles(symbol,"Hour4")
            if df1 is None or df4 is None or len(df1) < 60 or len(df4) < 45:
                return {"ok":False,"reason":"insufficient 1h/4h structure history"}
            atr1=_v71_sr_atr(df1)
'''
new_fetch = '''        try:
            df15=get_candles(symbol,"Min15")
            time.sleep(0.05)
            df1=get_candles(symbol,"Min60")
            time.sleep(0.05)
            df4=get_candles(symbol,"Hour4")
            if df15 is None or df1 is None or df4 is None or len(df15) < 60 or len(df1) < 60 or len(df4) < 45:
                return {"ok":False,"reason":"insufficient 15m/1h/4h structure history"}
            atr1=_v71_sr_atr(df1)
'''
replace_once(old_fetch, new_fetch, "S/R timeframe fetch")

old_context = '''            context={"atr1h":atr1,"tolerance":tolerance,"supports":supports,"resistances":resistances}
            V71_SR_CONTEXT_CACHE[symbol]={"ts":now,"context":context}
'''
new_context = '''            context={"atr1h":atr1,"tolerance":tolerance,"supports":supports,"resistances":resistances,
                     "df15":df15,"df1":df1}
            V71_SR_CONTEXT_CACHE[symbol]={"ts":now,"context":context}
'''
replace_once(old_context, new_context, "S/R cached context")

old_opposing = '''    zones=context["resistances"] if direction=="LONG" else context["supports"]
    opposing=[]
    for z in zones:
        low=num(z.get("low")); high=num(z.get("high"))
        if direction=="LONG":
            if high <= entry:
                continue
            distance=max(0.0,low-entry)
        else:
            if low >= entry:
                continue
            distance=max(0.0,entry-high)
        zr=distance/risk
        item=dict(z); item["distance"]=distance; item["room_r"]=zr
        opposing.append(item)
    opposing.sort(key=lambda z:num(z.get("room_r")))
    nearest=opposing[0] if opposing else None
'''
new_opposing = '''    zones=context["resistances"] if direction=="LONG" else context["supports"]
    opposing=[]
    for z in zones:
        low=num(z.get("low")); high=num(z.get("high"))
        if direction=="LONG":
            if high <= entry:
                continue
            distance=max(0.0,low-entry)
        else:
            if low >= entry:
                continue
            distance=max(0.0,entry-high)
        zr=distance/risk
        item=dict(z); item["distance"]=distance; item["room_r"]=zr
        item["breakout"]=_v71_sr_breakout_evidence(
            direction,item,context.get("df15"),context.get("df1"),entry,num(context.get("tolerance"))
        )
        opposing.append(item)
    opposing.sort(key=lambda z:num(z.get("room_r")))
    raw_nearest=opposing[0] if opposing else None
    broken=[z for z in opposing if bool((z.get("breakout") or {}).get("confirmed"))]
    active=[z for z in opposing if not bool((z.get("breakout") or {}).get("confirmed"))]
    nearest=active[0] if active else None
'''
replace_once(old_opposing, new_opposing, "S/R breakout classification")

old_return = '''        "ok":True,
        "nearest_zone":nearest,
        "nearest_room_r":num(nearest.get("room_r")) if nearest else 99.0,
        "nearest_strength":num(nearest.get("strength")) if nearest else 0.0,
        "tp1_requires_break":before_target(nearest,tp1),
        "tp2_requires_break":before_target(nearest,tp2),
        "tp3_requires_break":before_target(nearest,tp3),
        "major_opposing_zones":opposing[:5],
        "atr1h":num(context.get("atr1h")),
        "zone_tolerance":num(context.get("tolerance")),
    }
'''
new_return = '''        "ok":True,
        "nearest_zone":nearest,
        "raw_nearest_zone":raw_nearest,
        "nearest_room_r":num(nearest.get("room_r")) if nearest else 99.0,
        "nearest_strength":num(nearest.get("strength")) if nearest else 0.0,
        "tp1_requires_break":before_target(nearest,tp1),
        "tp2_requires_break":before_target(nearest,tp2),
        "tp3_requires_break":before_target(nearest,tp3),
        "major_opposing_zones":opposing[:5],
        "breakout_confirmed_zones":broken[:3],
        "breakout_adjusted":bool(broken),
        "atr1h":num(context.get("atr1h")),
        "zone_tolerance":num(context.get("tolerance")),
    }
'''
replace_once(old_return, new_return, "S/R breakout return payload")

old_gate = '''        room=num(sr.get("nearest_room_r"))
        nearest=sr.get("nearest_zone") or {}
        if nearest:
            notes.append(
                f"nearest {'resistance' if result.get('direction')=='LONG' else 'support'} "
                f"{num(nearest.get('low')):.8g}-{num(nearest.get('high')):.8g} "
                f"strength={num(nearest.get('strength')):.1f} room={room:.2f}R"
            )
        if room < V71_SR_HARD_BLOCK_R:
'''
new_gate = '''        room=num(sr.get("nearest_room_r"))
        nearest=sr.get("nearest_zone") or {}
        raw_nearest=sr.get("raw_nearest_zone") or {}
        breakout_zones=sr.get("breakout_confirmed_zones") or []
        core_score=num(result.get("best_score"))
        strong_strategy=sc == "STRONG_CONFIRM"
        breakout_override=bool(
            V71_SR_BREAKOUT_AWARE and breakout_zones
            and core_score >= V71_SR_CAUTION_MIN_CORE and strong_strategy
        )
        if breakout_zones and breakout_override:
            consumed=breakout_zones[0]
            evidence=consumed.get("breakout") or {}
            notes.append(
                f"breakout-aware S/R: consumed "
                f"{'resistance' if result.get('direction')=='LONG' else 'support'} "
                f"{num(consumed.get('low')):.8g}-{num(consumed.get('high')):.8g} "
                f"via {','.join(evidence.get('modes') or ['CONFIRMED'])}; checking next zone"
            )
        elif breakout_zones:
            # Market evidence alone cannot bypass structure. Require the same elite
            # signal quality used by the existing crowded-path exception.
            nearest=raw_nearest
            room=num(raw_nearest.get("room_r")) if raw_nearest else 99.0
            notes.append(
                f"breakout evidence present but no structural override: Core {core_score:.1f}, Strategy {sc}"
            )
        if nearest:
            notes.append(
                f"nearest {'resistance' if result.get('direction')=='LONG' else 'support'} "
                f"{num(nearest.get('low')):.8g}-{num(nearest.get('high')):.8g} "
                f"strength={num(nearest.get('strength')):.1f} room={room:.2f}R"
            )
        if room < V71_SR_HARD_BLOCK_R:
'''
replace_once(old_gate, new_gate, "S/R gate breakout override")

# Remove the duplicate local core/strategy assignment from the caution branch;
# it is now computed once above for both hard-block and breakout-aware paths.
old_caution = '''        elif room < V71_SR_CAUTION_R:
            core_score=num(result.get("best_score"))
            strong_strategy=sc == "STRONG_CONFIRM"
            if core_score < V71_SR_CAUTION_MIN_CORE or not strong_strategy:
'''
new_caution = '''        elif room < V71_SR_CAUTION_R:
            if core_score < V71_SR_CAUTION_MIN_CORE or not strong_strategy:
'''
replace_once(old_caution, new_caution, "S/R caution cleanup")

replace_once(
    '"evaluated_ts":time.time(),"version":"7.8.2-structural-sr-gate"',
    '"evaluated_ts":time.time(),"version":"7.8.3-breakout-aware-sr"',
    "selector version bump",
)

PATH.write_text(text, encoding="utf-8")
print("Applied V7.8.3 breakout-aware S/R refinement")
