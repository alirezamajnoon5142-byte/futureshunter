"""Cryptonary 11-asset MEXC futures watcher.

Read-only Telegram command patch for the exact Cryptonary levels discussed on
2026-09-28/29. Uses FuturesHunter's existing MEXC candle fetcher, so output is
based on current MEXC USDT perpetual candles rather than browser snapshots.

No live trading logic, sizing, leverage, entries, exits, or order paths are
modified.
"""
import sys
import threading
import time

ASSETS = {
    "BTC_USDT": {"label": "BTC", "kind": "btc", "level": 80840.0, "targets": [82800.0]},
    "ETH_USDT": {"label": "ETH", "kind": "eth", "level": 2633.0, "targets": [2464.0]},
    "SOL_USDT": {"label": "SOL", "kind": "sol", "level": 120.0, "targets": [122.80, 148.41], "alt": 115.80},
    "HYPE_USDT": {"label": "HYPE", "kind": "hype", "level": 89.96, "targets": [84.89, 75.20], "alt": 86.76},
    "AURA_USDT": {"label": "AURA", "kind": "aura", "level": 0.0093, "targets": [0.0126, 0.0137, 0.0182]},
    "ZEC_USDT": {"label": "ZEC", "kind": "zec", "level": 1444.0, "targets": [1320.0]},
    "XMR_USDT": {"label": "XMR", "kind": "xmr", "level": 541.0, "targets": [484.30, 449.61], "alt": 548.50},
    "DELTA_USDT": {"label": "DELTA", "kind": "delta", "level": 0.0238, "targets": [0.0380], "alt": 0.0181},
    "PLUME_USDT": {"label": "PLUME", "kind": "plume", "level": 0.0174, "targets": [0.0187, 0.0200, 0.0207]},
    "GRAM_USDT": {"label": "GRAM", "kind": "gram", "level": 1.548, "targets": [1.841, 2.10]},
    "ZRO_USDT": {"label": "ZRO", "kind": "zro", "level": 1.43, "targets": [1.646, 1.725, 2.13]},
}

_PATCHED = False
_LOCK = threading.RLock()


def _f(v):
    try:
        return float(v)
    except Exception:
        return None


def _fmt(x):
    if x is None:
        return "n/a"
    ax = abs(float(x))
    if ax >= 1000:
        return f"{x:,.2f}"
    if ax >= 100:
        return f"{x:.3f}"
    if ax >= 1:
        return f"{x:.4f}"
    return f"{x:.6f}"


def _closed(df):
    if df is None or len(df) < 2:
        return None
    # FuturesHunter get_candles returns newest material; use penultimate row as
    # the last safely completed candle and preserve a few completed bars.
    try:
        return df.iloc[:-1].tail(12).copy()
    except Exception:
        return None


def _get(main, symbol, interval, n=40):
    try:
        df = main.get_candles(symbol, interval)
        return _closed(df).tail(n)
    except Exception:
        return None


def _row_vals(row):
    return {
        "o": _f(row.get("open")),
        "h": _f(row.get("high")),
        "l": _f(row.get("low")),
        "c": _f(row.get("close")),
        "t": row.get("time"),
    }


def _fresh_price(main, symbol):
    # Prefer the most recent 5m candle close from MEXC. This is intentionally
    # exchange-native and avoids search/indexed web snapshots.
    try:
        df = main.get_candles(symbol, "Min5")
        if df is None or len(df) == 0:
            return None
        return _f(df.iloc[-1].get("close"))
    except Exception:
        return None


def _reclaim_after_sweep(bars, level, direction="long"):
    if bars is None or len(bars) < 2:
        return False, None
    vals = [_row_vals(r) for _, r in bars.iterrows()]
    for i in range(1, len(vals)):
        prev, cur = vals[i-1], vals[i]
        if None in (prev["l"], prev["h"], prev["c"], cur["c"]):
            continue
        if direction == "long":
            if prev["l"] < level and prev["c"] < level and cur["c"] > level:
                return True, cur
        else:
            if prev["h"] > level and prev["c"] > level and cur["c"] < level:
                return True, cur
    return False, None


def _accept_below(bars, level):
    if bars is None or len(bars) < 3:
        return False
    closes = [_f(x) for x in bars["close"].tail(3)]
    return all(x is not None and x < level for x in closes)


def _accept_above(bars, level):
    if bars is None or len(bars) < 3:
        return False
    closes = [_f(x) for x in bars["close"].tail(3)]
    return all(x is not None and x > level for x in closes)


def _latest_close(bars):
    if bars is None or len(bars) == 0:
        return None
    return _f(bars.iloc[-1].get("close"))


def _status(main, symbol, cfg):
    p = _fresh_price(main, symbol)
    m5, m15 = _get(main, symbol, "Min5"), _get(main, symbol, "Min15")
    d1 = _get(main, symbol, "Day1", 8)
    kind, lv = cfg["kind"], cfg["level"]
    status, note = "WATCH", ""

    if p is None:
        return {"symbol": symbol, "price": None, "status": "UNVERIFIED", "note": "MEXC candle fetch unavailable", "m5": None, "m15": None}

    if kind in {"btc", "eth"}:
        hit15, _ = _reclaim_after_sweep(m15, lv, "long")
        hit5, _ = _reclaim_after_sweep(m5, lv, "long")
        if hit15 or hit5:
            status, note = "LONG CONFIRMED", f"sweep below {_fmt(lv)} then reclaim on {'15m' if hit15 else '5m'}"
        else:
            note = f"needs sweep below {_fmt(lv)} + reclaim"

    elif kind == "sol":
        if _accept_above(m15, 120.0):
            status, note = "LONG PATH ACTIVE", "15m acceptance above 120; next 122.80"
        elif _latest_close(d1) is not None and _latest_close(d1) < 115.80:
            status, note = "PULLBACK-BUY WATCH", "daily closed below 115.80; 107-110 region in focus"
        else:
            note = "between 115.80 and 120 decision levels"

    elif kind == "hype":
        if p < 89.96:
            status, note = "SHORT PATH ACTIVE", "below 89.96; downside 84.89 then ~75.20"
        else:
            status, note = "WATCH", "reclaimed 89.96; pullback momentum weakened"

    elif kind == "aura":
        if _accept_above(m15, lv):
            status, note = "LONG PATH ACTIVE", "accepted above 0.0093"
        else:
            note = "needs reclaim/strength above 0.0093"

    elif kind == "zec":
        if _accept_above(m15, 1444.0):
            status, note = "RECLAIMED", "holding above 1444 reduces deeper-pullback probability"
        elif p < 1444.0:
            status, note = "BEARISH PATH ACTIVE", "below 1444; ~1320 remains mapped"

    elif kind == "xmr":
        if _accept_below(m15, 541.0):
            status, note = "SHORT PATH ACTIVE", "15m acceptance below 541; watch failed reclaim"
        elif p < 541.0:
            status, note = "DECISION / BELOW 541", "below 541 but acceptance not yet established"
        elif p > 548.50:
            status, note = "BEARISH THESIS WEAKER", "above 548.50"
        else:
            note = "541-548.50 decision zone"

    elif kind == "delta":
        if _accept_above(m15, 0.0238):
            status, note = "LONG PATH ACTIVE", "15m acceptance above 0.0238; 0.038 mapped"
        else:
            note = "needs strength/acceptance above 0.0238"

    elif kind == "plume":
        dc = _latest_close(d1)
        if dc is not None and dc < 0.0174:
            status, note = "INVALIDATED", "daily close below 0.0174"
        elif p > 0.0174:
            status, note = "BULLISH STRUCTURE ACTIVE", "above 0.0174; fresh re-entry confirmation still required"
        else:
            status, note = "DANGER", "below 0.0174 intraday"

    elif kind == "gram":
        if _accept_above(m15, 1.548):
            status, note = "LONG CONFIRMATION", "15m acceptance/reclaim above 1.548"
        elif p < 1.548:
            status, note = "WAIT FOR RECLAIM", "below 1.548; no bullish confirmation yet"
        else:
            note = "around weekly 50% area; needs solid bullish setup"

    elif kind == "zro":
        if p < 1.43:
            status, note = "STRUCTURE WARNING", "below 1.43; bullish pathway damaged"
        elif _accept_above(m15, 1.43):
            status, note = "BULLISH PATH ACTIVE", "holding above 1.43; fresh re-entry confirmation still required"
        else:
            note = "above 1.43 but needs clearer hold/confirmation"

    return {"symbol": symbol, "price": p, "status": status, "note": note, "m5": _latest_close(m5), "m15": _latest_close(m15)}


def build_text(main):
    lines = ["🧭 CRYPTONARY 11 — LIVE MEXC USDT PERPS", "Read-only check from FuturesHunter MEXC candles", ""]
    for sym, cfg in ASSETS.items():
        r = _status(main, sym, cfg)
        tg = " → ".join(_fmt(x) for x in cfg.get("targets", []))
        lines += [
            f"{cfg['label']} ({sym}) — {r['status']}",
            f"Now {_fmt(r['price'])} | last closed 5m {_fmt(r['m5'])} | 15m {_fmt(r['m15'])}",
            f"{r['note']}" + (f" | mapped: {tg}" if tg else ""),
            "",
        ]
    lines.append("No orders are sent by this command. It only reads MEXC futures candles.")
    return "\n".join(lines)


def patch(main):
    global _PATCHED
    with _LOCK:
        if _PATCHED:
            return True
        if not hasattr(main, "get_candles") or not hasattr(main, "handle_telegram_command") or not hasattr(main, "send_to_chat"):
            return False
        prev = main.handle_telegram_command

        def cmd(chat_id, text):
            c = (((text or "").strip().split() or [""])[0].lower().split("@")[0])
            if c in {"/cryptonary", "/crypto11", "/c11"}:
                report = build_text(main)
                print("[C11 SNAPSHOT]\n" + report, flush=True)
                main.send_to_chat(chat_id, report)
                return
            return prev(chat_id, text)

        main.handle_telegram_command = cmd
        _PATCHED = True
        try:
            print("[CRYPTONARY11] read-only MEXC candle command armed: /cryptonary /crypto11 /c11", flush=True)
        except Exception:
            pass
        return True


def bootstrap():
    end = time.time() + 360
    while time.time() < end:
        try:
            main = sys.modules.get("__main__")
            if main is not None and patch(main):
                return
        except Exception:
            pass
        time.sleep(0.5)


threading.Thread(target=bootstrap, name="Cryptonary11Bootstrap", daemon=True).start()
