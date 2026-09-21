
import asyncio
import csv
import json
import os
import time
import threading
import hashlib
import re
import xml.etree.ElementTree as ET
import html as html_lib
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import numpy as np
import pandas as pd
import requests
import websockets
from dotenv import load_dotenv


# ============================================================
# LOAD SECRETS
# ============================================================

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
EMERGENCY_CLOSE_SECRET = os.getenv("EMERGENCY_CLOSE_SECRET", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # admin / original subscriber
WEBSITE_URL = os.getenv("WEBSITE_URL", "https://futureshunter-v66.onrender.com")


# ============================================================
# SETTINGS
# ============================================================

MEXC_WS = "wss://contract.mexc.com/edge"
MEXC_REST = "https://contract.mexc.com"

TOP_N = 20
MIN_24H_TURNOVER = 10_000_000

FULL_SCAN_INTERVAL = 5 * 60
OI_UPDATE_INTERVAL = 60

ALERT_COOLDOWN = 60 * 60
ALERT_SCORE_IMPROVEMENT = 5

MIN_SCORE = 82
MAX_SPREAD_PCT = 0.03
MIN_ADX_1H = 20
MIN_RV_15M = 0.80
MAX_EMA20_DISTANCE_ATR = 1.50
MAX_LIVE_DISTANCE_ATR = 0.75
MIN_STOP_PCT = 0.10
MAX_STOP_PCT = 2.50
MIN_OI_SCORE = 8

HISTORY_RETENTION = 6 * 60 * 60

# Paper trade expires after 24 hours if neither TP3 nor stop is reached.
TRADE_EXPIRY_HOURS = 24

BASE_DIR = Path(__file__).resolve().parent

HISTORY_FILE = BASE_DIR / "oi_history.json"
ALERT_STATE_FILE = BASE_DIR / "alert_state.json"
SIGNAL_LOG_FILE = BASE_DIR / "paper_signals.csv"

TRADES_FILE = BASE_DIR / "paper_trades_v5.json"
RESULTS_FILE = BASE_DIR / "paper_trade_results.csv"
SUBSCRIBERS_FILE = BASE_DIR / "telegram_subscribers.json"
LATEST_SIGNAL_FILE = BASE_DIR / "latest_signal.json"
FACTOR_LOG_FILE = BASE_DIR / "factor_signals_v6.csv"
STARTED_AT = time.time()
SUBSCRIBER_LOCK = threading.RLock()


# ============================================================
# BASIC HELPERS
# ============================================================

def num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def pct_change(current, previous):
    if previous is None or previous == 0:
        return None

    return ((current - previous) / previous) * 100


def direction_matches(trend, direction):
    if direction == "LONG":
        return "BULLISH" in trend

    if direction == "SHORT":
        return "BEARISH" in trend

    return False


def format_optional(value):
    if value is None:
        return "N/A"

    return f"{value:+.2f}%"


def local_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def make_source_key(
    timestamp_text,
    symbol,
    direction
):
    # Minute-level key prevents duplicate imports after restarts.
    minute_text = str(timestamp_text)[:16]

    return (
        f"{minute_text}|"
        f"{symbol}|"
        f"{direction}"
    )


def normalize_candle_time(value):
    timestamp = num(value)

    # MEXC kline timestamps may be seconds or milliseconds.
    if timestamp > 10_000_000_000:
        timestamp /= 1000

    return timestamp


# ============================================================
# TELEGRAM — MULTI-SUBSCRIBER BROADCAST BOT
# ============================================================

def telegram_ready():
    return bool(TELEGRAM_BOT_TOKEN)


def _normalize_chat_id(value):
    if value is None:
        return None
    return str(value).strip()


def load_subscribers():
    subscribers = set()

    if SUBSCRIBERS_FILE.exists():
        try:
            data = json.loads(
                SUBSCRIBERS_FILE.read_text(encoding="utf-8")
            )
            if isinstance(data, list):
                subscribers.update(
                    _normalize_chat_id(item)
                    for item in data
                    if _normalize_chat_id(item)
                )
        except Exception as error:
            print(f"Subscriber file error: {error}")

    # Preserve the original owner's chat even before /start is pressed.
    admin_id = _normalize_chat_id(TELEGRAM_CHAT_ID)
    if admin_id:
        subscribers.add(admin_id)

    return subscribers


def save_subscribers(subscribers):
    with SUBSCRIBER_LOCK:
        SUBSCRIBERS_FILE.write_text(
            json.dumps(sorted(subscribers), indent=2),
            encoding="utf-8"
        )


SUBSCRIBERS = load_subscribers()


def _safe_error(error):
    value = str(error)
    if TELEGRAM_BOT_TOKEN:
        value = value.replace(str(TELEGRAM_BOT_TOKEN), "<REDACTED>")
    return value


def send_to_chat(chat_id, message):
    if not telegram_ready():
        print("Telegram bot token missing.")
        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    try:
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
                "disable_web_page_preview": True
            },
            timeout=15
        )
        response.raise_for_status()
        return bool(response.json().get("ok"))

    except Exception as error:
        print(f"Telegram send error ({chat_id}): {_safe_error(error)}")
        return False


def broadcast_telegram(message):
    with SUBSCRIBER_LOCK:
        targets = list(SUBSCRIBERS)

    if not targets:
        print("No Telegram subscribers yet.")
        return False

    delivered = 0
    stale = []

    for chat_id in targets:
        if send_to_chat(chat_id, message):
            delivered += 1
        else:
            # Do not immediately delete users on transient network errors.
            pass

    return delivered > 0


def send_telegram(message):
    # Backwards-compatible name: every V5 signal/outcome is now broadcast.
    return broadcast_telegram(message)


def build_welcome_message():
    return (
        "🐆 Welcome to FuturesHunter\n\n"
        "AI-powered MEXC USDT perpetual futures scanner.\n"
        "You’ll receive qualifying LONG/SHORT setups automatically.\n"
        "No forced trades — if nothing qualifies, FuturesHunter waits.\n\n"
        f"🌐 Dashboard: {WEBSITE_URL}\n\n"
        "Commands: /latest /stats /status /website /stop"
    )


def telegram_status_message():
    uptime = max(0, int(time.time() - STARTED_AT))
    hours, rem = divmod(uptime, 3600)
    minutes, _ = divmod(rem, 60)
    with SUBSCRIBER_LOCK:
        subscriber_count = len(SUBSCRIBERS)
    return (
        "✅ FuturesHunter V6 is online.\n\n"
        f"Subscribers: {subscriber_count}\n"
        f"Universe: top {TOP_N} liquid MEXC USDT perpetuals\n"
        f"Scan interval: {FULL_SCAN_INTERVAL // 60} min\n"
        f"Scoring model: 50 factors\n"
        f"Uptime: {hours}h {minutes}m"
    )


def telegram_stats_message():
    trades = load_json(TRADES_FILE, [])
    stats = calculate_stats(trades)
    return (
        "📊 FuturesHunter paper stats\n\n"
        f"Signals: {stats['total']}\n"
        f"Open: {stats['open']}\n"
        f"Resolved: {stats['resolved']}\n"
        f"Win rate: {stats['win_rate']:.1f}%\n"
        f"Cumulative R: {stats['cumulative_r']:+.2f}R\n"
        f"Average R: {stats['average_r']:+.2f}R"
    )


def save_latest_signal(message, result):
    payload = {
        "time": time.time(),
        "time_text": local_time(),
        "message": message,
        "symbol": result.get("symbol"),
        "direction": result.get("direction"),
        "score": result.get("best_score")
    }
    LATEST_SIGNAL_FILE.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )


def _telegram_api(method, payload=None, timeout=35):
    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/{method}"
    )
    response = requests.post(
        url,
        json=payload or {},
        timeout=timeout
    )
    response.raise_for_status()
    return response.json()


def handle_telegram_command(chat_id, text):
    command = (text or "").strip().split()[0].lower()

    if command in {"/start", "/subscribe"}:
        with SUBSCRIBER_LOCK:
            SUBSCRIBERS.add(str(chat_id))
            save_subscribers(SUBSCRIBERS)
        send_to_chat(chat_id, build_welcome_message())
        return

    if command in {"/stop", "/unsubscribe"}:
        with SUBSCRIBER_LOCK:
            SUBSCRIBERS.discard(str(chat_id))
            save_subscribers(SUBSCRIBERS)
        send_to_chat(
            chat_id,
            "🔕 FuturesHunter alerts stopped. Send /start anytime to subscribe again."
        )
        return

    if command == "/website":
        send_to_chat(chat_id, f"🌐 FuturesHunter dashboard:\n{WEBSITE_URL}")
        return

    if command == "/status":
        send_to_chat(chat_id, telegram_status_message())
        return

    if command == "/stats":
        send_to_chat(chat_id, telegram_stats_message())
        return

    if command == "/latest":
        if LATEST_SIGNAL_FILE.exists():
            try:
                payload = json.loads(
                    LATEST_SIGNAL_FILE.read_text(encoding="utf-8")
                )
                message = payload.get("message")
            except Exception:
                message = None
        else:
            message = None

        send_to_chat(
            chat_id,
            message or "No qualifying signal has been broadcast yet."
        )
        return

    if command in {"/help", "/commands"}:
        send_to_chat(chat_id, build_welcome_message())


def telegram_command_loop():
    if not telegram_ready():
        return

    # getUpdates cannot run while a webhook is active. This bot is dedicated
    # to FuturesHunter, so force long-polling mode without dropping updates.
    try:
        _telegram_api("deleteWebhook", {"drop_pending_updates": False}, timeout=15)
    except Exception as error:
        print(f"Telegram webhook cleanup warning: {_safe_error(error)}")

    offset = None

    while True:
        try:
            payload = {"timeout": 25}
            if offset is not None:
                payload["offset"] = offset

            result = _telegram_api("getUpdates", payload, timeout=35)

            for update in result.get("result", []):
                offset = update.get("update_id", 0) + 1
                message = update.get("message") or {}
                chat = message.get("chat") or {}
                chat_id = chat.get("id")
                text = message.get("text", "")

                if chat_id is not None and text.startswith("/"):
                    handle_telegram_command(chat_id, text)

        except Exception as error:
            print(f"Telegram command loop error: {_safe_error(error)}")
            time.sleep(5)


def start_telegram_command_thread():
    thread = threading.Thread(
        target=telegram_command_loop,
        name="FuturesHunterTelegram",
        daemon=True
    )
    thread.start()
    return thread


# ============================================================
# JSON STORAGE
# ============================================================

def load_json(path, default):
    if not path.exists():
        return default

    try:
        with open(
            path,
            "r",
            encoding="utf-8"
        ) as file:
            return json.load(file)

    except Exception:
        return default


def save_json(path, data):
    with open(
        path,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            data,
            file,
            indent=2
        )


# ============================================================
# OI HISTORY
# ============================================================

def find_old_snapshot(
    snapshots,
    now,
    seconds_ago,
    tolerance
):
    target = now - seconds_ago

    candidates = []

    for snapshot in snapshots:
        timestamp = num(
            snapshot.get("time")
        )

        difference = abs(
            timestamp - target
        )

        if difference <= tolerance:
            candidates.append(
                (
                    difference,
                    snapshot
                )
            )

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: item[0]
    )

    return candidates[0][1]


def update_oi_history(
    history,
    details
):
    now = time.time()
    metrics = {}

    for symbol, ticker in details.items():
        current_oi = num(
            ticker.get("holdVol")
        )

        current_price = num(
            ticker.get("lastPrice")
        )

        snapshots = history.get(
            symbol,
            []
        )

        old5 = find_old_snapshot(
            snapshots,
            now,
            5 * 60,
            120
        )

        old15 = find_old_snapshot(
            snapshots,
            now,
            15 * 60,
            180
        )

        oi5 = None
        price5 = None
        oi15 = None
        price15 = None

        if old5 is not None:
            oi5 = pct_change(
                current_oi,
                num(old5.get("oi"))
            )

            price5 = pct_change(
                current_price,
                num(old5.get("price"))
            )

        if old15 is not None:
            oi15 = pct_change(
                current_oi,
                num(old15.get("oi"))
            )

            price15 = pct_change(
                current_price,
                num(old15.get("price"))
            )

        metrics[symbol] = {
            "oi5": oi5,
            "price5": price5,
            "oi15": oi15,
            "price15": price15
        }

        snapshots.append({
            "time": now,
            "oi": current_oi,
            "price": current_price
        })

        cutoff = (
            now
            - HISTORY_RETENTION
        )

        snapshots = [
            snapshot
            for snapshot in snapshots
            if num(
                snapshot.get("time")
            ) >= cutoff
        ]

        history[symbol] = snapshots

    save_json(
        HISTORY_FILE,
        history
    )

    return metrics


# ============================================================
# FIND TOP MARKETS
# ============================================================

async def get_top_symbols():
    async with websockets.connect(
        MEXC_WS
    ) as ws:

        await ws.send(
            json.dumps({
                "method": "sub.tickers",
                "param": {},
                "gzip": False
            })
        )

        while True:
            raw = await ws.recv()
            message = json.loads(raw)

            if (
                message.get("channel")
                != "push.tickers"
            ):
                continue

            tickers = message.get(
                "data",
                []
            )

            if not isinstance(
                tickers,
                list
            ):
                continue

            markets = []

            for ticker in tickers:
                symbol = ticker.get(
                    "symbol",
                    ""
                )

                if not symbol.endswith(
                    "_USDT"
                ):
                    continue

                turnover = num(
                    ticker.get("amount24")
                )

                if (
                    turnover
                    < MIN_24H_TURNOVER
                ):
                    continue

                markets.append({
                    "symbol": symbol,
                    "turnover": turnover
                })

            markets.sort(
                key=lambda item:
                item["turnover"],
                reverse=True
            )

            return [
                item["symbol"]
                for item
                in markets[:TOP_N]
            ]


# ============================================================
# DETAILED MARKET DATA
# ============================================================

async def get_detailed_tickers(symbols):
    details = {}

    async with websockets.connect(
        MEXC_WS
    ) as ws:

        for symbol in symbols:
            await ws.send(
                json.dumps({
                    "method": "sub.ticker",
                    "param": {
                        "symbol": symbol
                    },
                    "gzip": False
                })
            )

            await asyncio.sleep(0.04)

        deadline = (
            time.time()
            + 20
        )

        while (
            len(details)
            < len(symbols)
        ):

            if time.time() > deadline:
                break

            try:
                raw = (
                    await asyncio.wait_for(
                        ws.recv(),
                        timeout=5
                    )
                )

            except asyncio.TimeoutError:
                continue

            message = json.loads(raw)

            if (
                message.get("channel")
                != "push.ticker"
            ):
                continue

            data = message.get(
                "data",
                {}
            )

            if not isinstance(
                data,
                dict
            ):
                continue

            symbol = data.get(
                "symbol"
            )

            if symbol in symbols:
                details[symbol] = data

    return details


# ============================================================
# HISTORICAL CANDLES
# ============================================================

def get_candles(
    symbol,
    interval
):
    url = (
        f"{MEXC_REST}"
        f"/api/v1/contract/"
        f"kline/{symbol}"
    )

    params = {
        "interval": interval
    }

    for attempt in range(3):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=10
            )

            response.raise_for_status()
            result = response.json()

            if not result.get(
                "success"
            ):
                raise RuntimeError(
                    f"MEXC returned failure "
                    f"for {symbol} {interval}"
                )

            data = result["data"]

            df = pd.DataFrame({
                "time": data["time"],
                "open": data["open"],
                "high": data["high"],
                "low": data["low"],
                "close": data["close"],
                "volume": data["vol"]
            })

            for column in [
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume"
            ]:
                df[column] = pd.to_numeric(
                    df[column],
                    errors="coerce"
                )

            return (
                df
                .dropna()
                .reset_index(
                    drop=True
                )
            )

        except Exception as error:
            if attempt == 2:
                print(
                    f"Candle error "
                    f"{symbol} "
                    f"{interval}: "
                    f"{error}"
                )

                return None

            time.sleep(0.7)

    return None


# ============================================================
# INDICATORS
# ============================================================

def add_indicators(df):
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    for span in (20, 50, 200):
        df[f"ema{span}"] = close.ewm(span=span, adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))

    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs()
        ],
        axis=1
    ).max(axis=1)
    df["atr14"] = true_range.ewm(alpha=1 / 14, adjust=False).mean()

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=df.index
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=df.index
    )
    atr = true_range.ewm(alpha=1 / 14, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / 14, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / 14, adjust=False).mean() / atr.replace(0, np.nan)
    denominator = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / denominator
    df["adx14"] = dx.ewm(alpha=1 / 14, adjust=False).mean()

    df["volume_avg20"] = volume.shift(1).rolling(20).mean()
    df["relative_volume"] = volume / df["volume_avg20"].replace(0, np.nan)

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    # Momentum / oscillators
    df["roc10"] = close.pct_change(10) * 100
    lowest14 = low.rolling(14).min()
    highest14 = high.rolling(14).max()
    range14 = (highest14 - lowest14).replace(0, np.nan)
    df["stoch_k"] = 100 * (close - lowest14) / range14
    df["williams_r"] = -100 * (highest14 - close) / range14

    typical = (high + low + close) / 3
    typical_sma = typical.rolling(20).mean()
    mean_dev = typical.rolling(20).apply(
        lambda x: np.mean(np.abs(x - np.mean(x))), raw=True
    )
    df["cci20"] = (typical - typical_sma) / (0.015 * mean_dev.replace(0, np.nan))

    raw_money = typical * volume
    positive_flow = raw_money.where(typical.diff() > 0, 0.0)
    negative_flow = raw_money.where(typical.diff() < 0, 0.0).abs()
    money_ratio = positive_flow.rolling(14).sum() / negative_flow.rolling(14).sum().replace(0, np.nan)
    df["mfi14"] = 100 - (100 / (1 + money_ratio))

    # Bollinger bands
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std(ddof=0)
    df["bb_mid"] = bb_mid
    df["bb_upper"] = bb_mid + 2 * bb_std
    df["bb_lower"] = bb_mid - 2 * bb_std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / bb_mid.replace(0, np.nan)
    df["bb_position"] = (close - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"]).replace(0, np.nan)

    # OBV
    signed_volume = np.sign(close.diff()).fillna(0) * volume
    df["obv"] = signed_volume.cumsum()

    # Rolling VWAP approximation over 20 candles.
    pv = typical * volume
    df["vwap20"] = pv.rolling(20).sum() / volume.rolling(20).sum().replace(0, np.nan)

    # Structure and regime helpers.
    df["high20_prev"] = high.shift(1).rolling(20).max()
    df["low20_prev"] = low.shift(1).rolling(20).min()
    df["atr_pct"] = df["atr14"] / close.replace(0, np.nan) * 100
    df["atr_pctile"] = df["atr_pct"].rolling(100).rank(pct=True)
    df["realized_vol20"] = close.pct_change().rolling(20).std() * np.sqrt(20) * 100

    candle_range = (high - low).replace(0, np.nan)
    df["body_pct"] = (close - df["open"]) / candle_range
    df["close_location"] = (close - low) / candle_range

    return df

# ============================================================
# TREND CLASSIFICATION
# ============================================================

def describe_trend(row):
    price = row["close"]

    if (
        price
        > row["ema20"]
        > row["ema50"]
        > row["ema200"]
    ):
        return "STRONG BULLISH"

    if (
        price
        < row["ema20"]
        < row["ema50"]
        < row["ema200"]
    ):
        return "STRONG BEARISH"

    if (
        price
        > row["ema50"]
    ):
        return "BULLISH"

    if (
        price
        < row["ema50"]
    ):
        return "BEARISH"

    return "NEUTRAL"


def analyse_timeframe(
    symbol,
    interval
):
    df = get_candles(symbol, interval)

    if df is None or len(df) < 250:
        return None

    df = add_indicators(df)

    latest = df.iloc[-2]          # most recent CLOSED candle
    previous = df.iloc[-3]
    closed = df.iloc[:-1]
    recent = closed.tail(12)

    # Slopes are normalized as percentage changes to make them comparable.
    def slope_pct(column, lookback=5):
        series = closed[column].dropna()
        if len(series) <= lookback:
            return 0.0
        current = num(series.iloc[-1])
        older = num(series.iloc[-1 - lookback])
        if older == 0:
            return 0.0
        return (current - older) / abs(older) * 100

    # OBV and volume trend are scale-free directional features.
    obv_now = num(closed["obv"].iloc[-1])
    obv_old = num(closed["obv"].iloc[-6])
    obv_slope = obv_now - obv_old

    vol_now = num(closed["volume"].tail(5).mean())
    vol_old = num(closed["volume"].iloc[-10:-5].mean())
    volume_trend = pct_change(vol_now, vol_old)
    if volume_trend is None:
        volume_trend = 0.0

    swing_high = num(recent["high"].max())
    swing_low = num(recent["low"].min())

    return {
        "close": num(latest["close"]),
        "open": num(latest["open"]),
        "high": num(latest["high"]),
        "low": num(latest["low"]),
        "ema20": num(latest["ema20"]),
        "ema50": num(latest["ema50"]),
        "ema200": num(latest["ema200"]),
        "ema20_slope": slope_pct("ema20"),
        "ema50_slope": slope_pct("ema50"),
        "rsi": num(latest["rsi14"]),
        "atr": num(latest["atr14"]),
        "atr_pct": num(latest["atr_pct"]),
        "atr_pctile": num(latest["atr_pctile"]),
        "realized_vol": num(latest["realized_vol20"]),
        "adx": num(latest["adx14"]),
        "rv": num(latest["relative_volume"]),
        "macd_hist": num(latest["macd_hist"]),
        "macd_hist_prev": num(previous["macd_hist"]),
        "roc": num(latest["roc10"]),
        "stoch": num(latest["stoch_k"]),
        "cci": num(latest["cci20"]),
        "williams_r": num(latest["williams_r"]),
        "mfi": num(latest["mfi14"]),
        "bb_position": num(latest["bb_position"]),
        "bb_width": num(latest["bb_width"]),
        "bb_width_prev": num(previous["bb_width"]),
        "vwap": num(latest["vwap20"]),
        "obv_slope": obv_slope,
        "volume_trend": num(volume_trend),
        "body_pct": num(latest["body_pct"]),
        "close_location": num(latest["close_location"]),
        "high20_prev": num(latest["high20_prev"]),
        "low20_prev": num(latest["low20_prev"]),
        "trend": describe_trend(latest),
        "swing_high": swing_high,
        "swing_low": swing_low
    }

# ============================================================
# OPEN INTEREST SCORE
# ============================================================

def oi_score(
    direction,
    metrics
):
    if not metrics:
        return 0

    oi5 = metrics.get(
        "oi5"
    )

    price5 = metrics.get(
        "price5"
    )

    oi15 = metrics.get(
        "oi15"
    )

    price15 = metrics.get(
        "price15"
    )

    if any(
        value is None
        for value in [
            oi5,
            price5,
            oi15,
            price15
        ]
    ):
        return 0

    score = 0

    if direction == "LONG":
        aligned5 = (
            price5 > 0
        )

        aligned15 = (
            price15 > 0
        )

    else:
        aligned5 = (
            price5 < 0
        )

        aligned15 = (
            price15 < 0
        )

    if (
        aligned15
        and oi15 > 0
    ):
        if oi15 >= 0.50:
            score += 10

        elif oi15 >= 0.15:
            score += 8

        else:
            score += 6

    elif aligned15:
        score += 2

    if (
        aligned5
        and oi5 > 0
    ):
        if oi5 >= 0.25:
            score += 5

        elif oi5 >= 0.10:
            score += 4

        else:
            score += 3

    elif aligned5:
        score += 1

    return min(
        score,
        15
    )


# ============================================================
# SETUP SCORING
# ============================================================

def _dir_value(direction):
    return 1 if direction == "LONG" else -1


def _factor(direction, bullish_condition, bearish_condition, strong=False):
    """Return 0/1/2 points for one directional factor."""
    aligned = bullish_condition if direction == "LONG" else bearish_condition
    if not aligned:
        return 0
    return 2 if strong else 1


def score_direction(
    direction,
    tf5,
    tf15,
    tf1h,
    funding,
    spread,
    oi_metrics
):
    """
    V6: exactly 50 independent factors, each scored 0/1/2.
    Final score therefore remains a natural 0-100 scale.
    The factor list is intentionally transparent and logged in result['parts'].
    """
    parts = {}

    def add(name, value):
        parts[name] = int(max(0, min(2, value)))

    # 1-3: broad trend state
    for name, tf in (("1H trend", tf1h), ("15m trend", tf15), ("5m trend", tf5)):
        aligned = direction_matches(tf["trend"], direction)
        add(name, 2 if aligned and "STRONG" in tf["trend"] else (1 if aligned else 0))

    # 4-12: price relative to core EMAs across three timeframes
    idx = 4
    for label, tf in (("1H", tf1h), ("15m", tf15), ("5m", tf5)):
        for ema_name in ("ema20", "ema50", "ema200"):
            bullish = tf["close"] > tf[ema_name]
            bearish = tf["close"] < tf[ema_name]
            distance = abs(tf["close"] - tf[ema_name]) / tf["atr"] if tf["atr"] > 0 else 0
            add(f"{label} price vs {ema_name.upper()}", _factor(direction, bullish, bearish, strong=distance >= 0.25))
            idx += 1

    # 13-17: EMA slopes
    slope_specs = [
        ("1H EMA20 slope", tf1h["ema20_slope"]),
        ("1H EMA50 slope", tf1h["ema50_slope"]),
        ("15m EMA20 slope", tf15["ema20_slope"]),
        ("15m EMA50 slope", tf15["ema50_slope"]),
        ("5m EMA20 slope", tf5["ema20_slope"]),
    ]
    for name, value in slope_specs:
        add(name, _factor(direction, value > 0, value < 0, strong=abs(value) >= 0.15))

    # 18-20: RSI regime on 3 timeframes
    for name, tf in (("1H RSI", tf1h), ("15m RSI", tf15), ("5m RSI", tf5)):
        rsi = tf["rsi"]
        if direction == "LONG":
            score = 2 if 52 <= rsi <= 68 else (1 if 48 <= rsi <= 72 else 0)
        else:
            score = 2 if 32 <= rsi <= 48 else (1 if 28 <= rsi <= 52 else 0)
        add(name, score)

    # 21-23: MACD histogram direction and expansion
    for name, tf in (("1H MACD", tf1h), ("15m MACD", tf15), ("5m MACD", tf5)):
        hist = tf["macd_hist"]
        prev = tf["macd_hist_prev"]
        bullish = hist > 0
        bearish = hist < 0
        expanding = abs(hist) > abs(prev)
        add(name, _factor(direction, bullish, bearish, strong=expanding))

    # 24-26: rate of change
    for name, tf in (("1H ROC", tf1h), ("15m ROC", tf15), ("5m ROC", tf5)):
        roc = tf["roc"]
        add(name, _factor(direction, roc > 0, roc < 0, strong=abs(roc) >= 0.5))

    # 27-28: stochastic
    for name, tf in (("15m Stochastic", tf15), ("5m Stochastic", tf5)):
        stoch = tf["stoch"]
        if direction == "LONG":
            value = 2 if 50 <= stoch <= 85 else (1 if 35 <= stoch < 90 else 0)
        else:
            value = 2 if 15 <= stoch <= 50 else (1 if 10 < stoch <= 65 else 0)
        add(name, value)

    # 29-30: CCI
    for name, tf in (("15m CCI", tf15), ("5m CCI", tf5)):
        cci = tf["cci"]
        if direction == "LONG":
            value = 2 if 25 <= cci <= 150 else (1 if cci > -25 else 0)
        else:
            value = 2 if -150 <= cci <= -25 else (1 if cci < 25 else 0)
        add(name, value)

    # 31: Williams %R (15m)
    wr = tf15["williams_r"]
    if direction == "LONG":
        add("15m Williams %R", 2 if -50 <= wr <= -15 else (1 if -70 <= wr < -15 else 0))
    else:
        add("15m Williams %R", 2 if -85 <= wr <= -50 else (1 if -85 < wr <= -30 else 0))

    # 32: MFI (15m)
    mfi = tf15["mfi"]
    if direction == "LONG":
        add("15m MFI", 2 if 52 <= mfi <= 75 else (1 if 45 <= mfi <= 80 else 0))
    else:
        add("15m MFI", 2 if 25 <= mfi <= 48 else (1 if 20 <= mfi <= 55 else 0))

    # 33-34: trend strength
    add("1H ADX", 2 if tf1h["adx"] >= 25 else (1 if tf1h["adx"] >= 20 else 0))
    add("15m ADX", 2 if tf15["adx"] >= 25 else (1 if tf15["adx"] >= 18 else 0))

    # 35-36: relative volume
    add("15m Relative volume", 2 if tf15["rv"] >= 1.2 else (1 if tf15["rv"] >= 0.8 else 0))
    add("5m Relative volume", 2 if tf5["rv"] >= 1.3 else (1 if tf5["rv"] >= 0.8 else 0))

    # 37: OBV slope direction
    add("15m OBV slope", _factor(direction, tf15["obv_slope"] > 0, tf15["obv_slope"] < 0, strong=abs(tf15["obv_slope"]) > 0))

    # 38: recent volume trend
    add("15m Volume trend", 2 if tf15["volume_trend"] >= 20 else (1 if tf15["volume_trend"] >= 0 else 0))

    # 39: candle body direction/conviction
    body = tf15["body_pct"]
    add("15m Candle body", _factor(direction, body > 0, body < 0, strong=abs(body) >= 0.55))

    # 40: close location in candle
    cl = tf15["close_location"]
    if direction == "LONG":
        add("15m Close location", 2 if cl >= 0.70 else (1 if cl >= 0.50 else 0))
    else:
        add("15m Close location", 2 if cl <= 0.30 else (1 if cl <= 0.50 else 0))

    # 41-42: 20-bar breakout pressure
    for name, tf in (("15m Breakout", tf15), ("5m Breakout", tf5)):
        if direction == "LONG":
            strong = tf["close"] > tf["high20_prev"] > 0
            near = tf["high20_prev"] > 0 and tf["close"] >= tf["high20_prev"] - 0.25 * tf["atr"]
        else:
            strong = 0 < tf["low20_prev"] and tf["close"] < tf["low20_prev"]
            near = tf["low20_prev"] > 0 and tf["close"] <= tf["low20_prev"] + 0.25 * tf["atr"]
        add(name, 2 if strong else (1 if near else 0))

    # 43: swing structure position
    if direction == "LONG":
        swing_mid = (tf15["swing_high"] + tf15["swing_low"]) / 2
        add("15m Swing structure", 2 if tf15["close"] > swing_mid else (1 if tf15["close"] > tf15["swing_low"] else 0))
    else:
        swing_mid = (tf15["swing_high"] + tf15["swing_low"]) / 2
        add("15m Swing structure", 2 if tf15["close"] < swing_mid else (1 if tf15["close"] < tf15["swing_high"] else 0))

    # 44: ATR regime — avoid dead and hyper-chaotic tails.
    atr_pctile = tf15["atr_pctile"]
    add("15m ATR regime", 2 if 0.25 <= atr_pctile <= 0.80 else (1 if 0.10 <= atr_pctile <= 0.90 else 0))

    # 45: Bollinger position
    bbp = tf15["bb_position"]
    if direction == "LONG":
        add("15m Bollinger position", 2 if 0.55 <= bbp <= 0.90 else (1 if 0.45 <= bbp <= 1.0 else 0))
    else:
        add("15m Bollinger position", 2 if 0.10 <= bbp <= 0.45 else (1 if 0.0 <= bbp <= 0.55 else 0))

    # 46: Bollinger bandwidth expansion
    width = tf15["bb_width"]
    prev_width = tf15["bb_width_prev"]
    add("15m BB expansion", 2 if width > prev_width * 1.05 else (1 if width >= prev_width else 0))

    # 47: price relative to rolling VWAP
    add("15m VWAP side", _factor(direction, tf15["close"] > tf15["vwap"], tf15["close"] < tf15["vwap"], strong=abs(tf15["close"] - tf15["vwap"]) >= 0.20 * tf15["atr"]))

    # 48: funding — contrarian preference, but not a hard requirement
    if direction == "LONG":
        add("Funding", 2 if funding <= 0 else (1 if funding <= 0.0005 else 0))
    else:
        add("Funding", 2 if funding >= 0 else (1 if funding >= -0.0005 else 0))

    # 49: executable spread quality
    add("Spread", 2 if spread <= 0.01 else (1 if spread <= 0.03 else 0))

    # 50: multi-horizon OI confirmation
    oi_points = oi_score(direction, oi_metrics)
    add("OI confirmation", 2 if oi_points >= 10 else (1 if oi_points >= 6 else 0))

    if len(parts) != 50:
        raise RuntimeError(f"Expected 50 factors, got {len(parts)}")

    score = int(sum(parts.values()))
    return score, parts


# ============================================================
# RISK PLAN
# ============================================================

def build_risk_plan(
    direction,
    live_price,
    tf5
):
    atr = tf5["atr"]

    if atr <= 0:
        return None

    entry = live_price

    if direction == "LONG":
        entry_low = (
            entry
            - 0.15 * atr
        )

        entry_high = (
            entry
            + 0.05 * atr
        )

        structure_stop = (
            tf5["swing_low"]
            - 0.15 * atr
        )

        minimum_stop = (
            entry
            - 0.80 * atr
        )

        stop = min(
            structure_stop,
            minimum_stop
        )

        risk = (
            entry
            - stop
        )

        if risk <= 0:
            return None

        tp1 = (
            entry
            + 1.5 * risk
        )

        tp2 = (
            entry
            + 2.0 * risk
        )

        tp3 = (
            entry
            + 3.0 * risk
        )

    else:
        entry_low = (
            entry
            - 0.05 * atr
        )

        entry_high = (
            entry
            + 0.15 * atr
        )

        structure_stop = (
            tf5["swing_high"]
            + 0.15 * atr
        )

        minimum_stop = (
            entry
            + 0.80 * atr
        )

        stop = max(
            structure_stop,
            minimum_stop
        )

        risk = (
            stop
            - entry
        )

        if risk <= 0:
            return None

        tp1 = (
            entry
            - 1.5 * risk
        )

        tp2 = (
            entry
            - 2.0 * risk
        )

        tp3 = (
            entry
            - 3.0 * risk
        )

    stop_pct = (
        risk
        / entry
    ) * 100

    return {
        "entry": entry,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "stop_pct": stop_pct,
        "risk": risk,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3
    }


# ============================================================
# HARD QUALIFICATION
# ============================================================

def qualify_setup(result):
    reasons = []

    direction = result[
        "direction"
    ]

    tf5 = result["5m"]
    tf15 = result["15m"]
    tf1h = result["1h"]

    plan = result[
        "risk_plan"
    ]

    if (
        result["best_score"]
        < MIN_SCORE
    ):
        reasons.append(
            f"score below {MIN_SCORE}"
        )

    if not direction_matches(
        tf1h["trend"],
        direction
    ):
        reasons.append(
            "1H trend not aligned"
        )

    if not direction_matches(
        tf15["trend"],
        direction
    ):
        reasons.append(
            "15m trend not aligned"
        )

    if (
        tf1h["adx"]
        < MIN_ADX_1H
    ):
        reasons.append(
            "1H ADX too weak"
        )

    if (
        tf15["rv"]
        < MIN_RV_15M
    ):
        reasons.append(
            "15m volume too weak"
        )

    if (
        result["spread"]
        > MAX_SPREAD_PCT
    ):
        reasons.append(
            "spread too wide"
        )

    if (
        result["oi_score"]
        < MIN_OI_SCORE
    ):
        reasons.append(
            "OI confirmation too weak"
        )

    if tf15["atr"] > 0:
        ema_distance = (
            abs(
                tf15["close"]
                - tf15["ema20"]
            )
            / tf15["atr"]
        )

    else:
        ema_distance = 999

    if (
        ema_distance
        > MAX_EMA20_DISTANCE_ATR
    ):
        reasons.append(
            "price too stretched "
            "from 15m EMA20"
        )

    if tf5["atr"] > 0:
        live_distance = (
            abs(
                result["price"]
                - tf5["close"]
            )
            / tf5["atr"]
        )

    else:
        live_distance = 999

    if (
        live_distance
        > MAX_LIVE_DISTANCE_ATR
    ):
        reasons.append(
            "live price moved too far "
            "from latest 5m close"
        )

    if plan is None:
        reasons.append(
            "invalid risk plan"
        )

    else:
        if (
            plan["stop_pct"]
            < MIN_STOP_PCT
        ):
            reasons.append(
                "stop too tight"
            )

        if (
            plan["stop_pct"]
            > MAX_STOP_PCT
        ):
            reasons.append(
                "stop too wide"
            )

    return (
        len(reasons) == 0,
        reasons
    )


# ============================================================
# ANALYSE ONE MARKET
# ============================================================

def analyse_market(
    symbol,
    ticker,
    oi_metrics
):
    print(
        f"   Analysing "
        f"{symbol}..."
    )

    tf5 = analyse_timeframe(
        symbol,
        "Min5"
    )

    time.sleep(0.15)

    tf15 = analyse_timeframe(
        symbol,
        "Min15"
    )

    time.sleep(0.15)

    tf1h = analyse_timeframe(
        symbol,
        "Min60"
    )

    time.sleep(0.15)

    if (
        tf5 is None
        or tf15 is None
        or tf1h is None
    ):
        return None

    bid = num(
        ticker.get("bid1")
    )

    ask = num(
        ticker.get("ask1")
    )

    funding = num(
        ticker.get(
            "fundingRate"
        )
    )

    open_interest = num(
        ticker.get(
            "holdVol"
        )
    )

    turnover = num(
        ticker.get(
            "amount24"
        )
    )

    live_price = num(
        ticker.get(
            "lastPrice"
        )
    )

    spread = 999

    if (
        bid > 0
        and ask > 0
    ):
        midpoint = (
            bid + ask
        ) / 2

        spread = (
            (
                ask - bid
            )
            / midpoint
        ) * 100

    long_score, long_parts = (
        score_direction(
            "LONG",
            tf5,
            tf15,
            tf1h,
            funding,
            spread,
            oi_metrics
        )
    )

    short_score, short_parts = (
        score_direction(
            "SHORT",
            tf5,
            tf15,
            tf1h,
            funding,
            spread,
            oi_metrics
        )
    )

    if (
        long_score
        >= short_score
    ):
        direction = "LONG"
        best_score = long_score
        best_parts = long_parts

    else:
        direction = "SHORT"
        best_score = short_score
        best_parts = short_parts

    risk_plan = build_risk_plan(
        direction,
        live_price,
        tf5
    )

    result = {
        "symbol": symbol,
        "price": live_price,
        "turnover": turnover,
        "oi": open_interest,
        "funding": funding,
        "spread": spread,
        "oi_metrics": oi_metrics,
        "5m": tf5,
        "15m": tf15,
        "1h": tf1h,
        "long_score": long_score,
        "short_score": short_score,
        "direction": direction,
        "best_score": best_score,
        "parts": best_parts,
        "oi_score": oi_score(direction, oi_metrics),
        "risk_plan": risk_plan
    }

    qualified, reasons = qualify_setup(
        result
    )

    result[
        "qualified"
    ] = qualified

    result[
        "reject_reasons"
    ] = reasons

    return result


# ============================================================
# PAPER SIGNAL CSV LOG
# ============================================================

def log_signal(result):
    plan = result[
        "risk_plan"
    ]

    oi = result[
        "oi_metrics"
    ]

    file_exists = (
        SIGNAL_LOG_FILE.exists()
    )

    fields = [
        "timestamp",
        "symbol",
        "direction",
        "score",
        "oi_score",
        "live_price",
        "entry_low",
        "entry_high",
        "stop",
        "stop_pct",
        "tp1",
        "tp2",
        "tp3",
        "funding_pct",
        "spread_pct",
        "open_interest",
        "oi5_pct",
        "price5_pct",
        "oi15_pct",
        "price15_pct",
        "rsi15",
        "adx1h",
        "rv15"
    ]

    with open(
        SIGNAL_LOG_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fields
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "timestamp": local_time(),
            "symbol": result["symbol"],
            "direction": result["direction"],
            "score": result["best_score"],
            "oi_score": result["oi_score"],
            "live_price": result["price"],
            "entry_low": plan["entry_low"],
            "entry_high": plan["entry_high"],
            "stop": plan["stop"],
            "stop_pct": plan["stop_pct"],
            "tp1": plan["tp1"],
            "tp2": plan["tp2"],
            "tp3": plan["tp3"],
            "funding_pct":
            result["funding"] * 100,
            "spread_pct":
            result["spread"],
            "open_interest":
            result["oi"],
            "oi5_pct":
            oi.get("oi5"),
            "price5_pct":
            oi.get("price5"),
            "oi15_pct":
            oi.get("oi15"),
            "price15_pct":
            oi.get("price15"),
            "rsi15":
            result["15m"]["rsi"],
            "adx1h":
            result["1h"]["adx"],
            "rv15":
            result["15m"]["rv"]
        })


# ============================================================
# V6 50-FACTOR SIGNAL LOG
# ============================================================

def log_factor_signal(result):
    parts = result.get("parts", {})
    if len(parts) != 50:
        return

    file_exists = FACTOR_LOG_FILE.exists()
    factor_names = list(parts.keys())
    fields = [
        "timestamp", "symbol", "direction", "score",
        "oi_score", "live_price"
    ] + factor_names

    with open(
        FACTOR_LOG_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        if not file_exists:
            writer.writeheader()

        row = {
            "timestamp": local_time(),
            "symbol": result["symbol"],
            "direction": result["direction"],
            "score": result["best_score"],
            "oi_score": result["oi_score"],
            "live_price": result["price"]
        }
        row.update(parts)
        writer.writerow(row)


# ============================================================
# V5 PAPER TRADE LEDGER
# ============================================================

def create_paper_trade(
    result,
    trades
):
    plan = result[
        "risk_plan"
    ]

    now = time.time()
    signal_time_text = local_time()

    signal_id = (
        f"{int(now)}_"
        f"{result['symbol']}_"
        f"{result['direction']}"
    )

    tracking_start = (
        int(now // 60) + 1
    ) * 60

    trade = {
        "signal_id": signal_id,
        "source_key": make_source_key(
            signal_time_text,
            result["symbol"],
            result["direction"]
        ),
        "signal_time": now,
        "signal_time_text": signal_time_text,
        "tracking_start": tracking_start,
        "symbol": result["symbol"],
        "direction": result["direction"],
        "score": result["best_score"],
        "oi_score": result["oi_score"],
        "entry": result["price"],
        "entry_low": plan["entry_low"],
        "entry_high": plan["entry_high"],
        "stop": plan["stop"],
        "tp1": plan["tp1"],
        "tp2": plan["tp2"],
        "tp3": plan["tp3"],
        "risk": plan["risk"],
        "stop_pct": plan["stop_pct"],
        "status": "OPEN",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "tp1_time": None,
        "tp2_time": None,
        "tp3_time": None,
        "closed_time": None,
        "closed_time_text": None,
        "final_r": None,
        "last_checked": tracking_start - 1,
        "result_logged": False
    }

    trades.append(
        trade
    )

    save_json(
        TRADES_FILE,
        trades
    )

    return trade



def import_recent_logged_signals(
    trades
):
    """
    One-time bridge from older V4/V5 rows already present in
    paper_signals.csv into the V5 paper-trade ledger.

    Only imports signals from the last TRADE_EXPIRY_HOURS so that
    1-minute candle history is still sufficient for reconstruction.
    """
    if not SIGNAL_LOG_FILE.exists():
        return 0

    existing_keys = {
        trade.get("source_key")
        for trade in trades
        if trade.get("source_key")
    }

    imported = 0
    now = time.time()

    try:
        with open(
            SIGNAL_LOG_FILE,
            "r",
            newline="",
            encoding="utf-8"
        ) as file:
            reader = csv.DictReader(
                file
            )

            for row in reader:
                timestamp_text = row.get(
                    "timestamp",
                    ""
                )

                symbol = row.get(
                    "symbol",
                    ""
                )

                direction = row.get(
                    "direction",
                    ""
                )

                if (
                    not timestamp_text
                    or not symbol
                    or direction
                    not in {
                        "LONG",
                        "SHORT"
                    }
                ):
                    continue

                try:
                    signal_dt = datetime.strptime(
                        timestamp_text,
                        "%Y-%m-%d %H:%M:%S"
                    )

                    signal_time = (
                        signal_dt.timestamp()
                    )

                except Exception:
                    continue

                age = (
                    now
                    - signal_time
                )

                if (
                    age < 0
                    or age
                    > TRADE_EXPIRY_HOURS
                    * 60
                    * 60
                ):
                    continue

                source_key = make_source_key(
                    timestamp_text,
                    symbol,
                    direction
                )

                if source_key in existing_keys:
                    continue

                entry = num(
                    row.get(
                        "live_price"
                    )
                )

                stop = num(
                    row.get(
                        "stop"
                    )
                )

                tp1 = num(
                    row.get(
                        "tp1"
                    )
                )

                tp2 = num(
                    row.get(
                        "tp2"
                    )
                )

                tp3 = num(
                    row.get(
                        "tp3"
                    )
                )

                if (
                    entry <= 0
                    or stop <= 0
                    or tp1 <= 0
                    or tp2 <= 0
                    or tp3 <= 0
                ):
                    continue

                risk = abs(
                    stop - entry
                )

                if risk <= 0:
                    continue

                tracking_start = (
                    int(
                        signal_time
                        // 60
                    )
                    + 1
                ) * 60

                signal_id = (
                    f"{int(signal_time)}_"
                    f"{symbol}_"
                    f"{direction}"
                )

                trade = {
                    "signal_id":
                    signal_id,

                    "source_key":
                    source_key,

                    "signal_time":
                    signal_time,

                    "signal_time_text":
                    timestamp_text,

                    "tracking_start":
                    tracking_start,

                    "symbol":
                    symbol,

                    "direction":
                    direction,

                    "score":
                    int(
                        num(
                            row.get(
                                "score"
                            )
                        )
                    ),

                    "oi_score":
                    int(
                        num(
                            row.get(
                                "oi_score"
                            )
                        )
                    ),

                    "entry":
                    entry,

                    "entry_low":
                    num(
                        row.get(
                            "entry_low"
                        )
                    ),

                    "entry_high":
                    num(
                        row.get(
                            "entry_high"
                        )
                    ),

                    "stop":
                    stop,

                    "tp1":
                    tp1,

                    "tp2":
                    tp2,

                    "tp3":
                    tp3,

                    "risk":
                    risk,

                    "stop_pct":
                    num(
                        row.get(
                            "stop_pct"
                        )
                    ),

                    "status":
                    "OPEN",

                    "tp1_hit":
                    False,

                    "tp2_hit":
                    False,

                    "tp3_hit":
                    False,

                    "tp1_time":
                    None,

                    "tp2_time":
                    None,

                    "tp3_time":
                    None,

                    "closed_time":
                    None,

                    "closed_time_text":
                    None,

                    "final_r":
                    None,

                    "last_checked":
                    tracking_start - 1,

                    "result_logged":
                    False
                }

                trades.append(
                    trade
                )

                existing_keys.add(
                    source_key
                )

                imported += 1

    except Exception as error:
        print(
            f"Signal import warning: "
            f"{error}"
        )

    if imported:
        save_json(
            TRADES_FILE,
            trades
        )

    return imported


def has_open_trade_for_symbol(
    trades,
    symbol
):
    return any(
        trade.get("status") == "OPEN"
        and trade.get("symbol") == symbol
        for trade in trades
    )


def current_r_for_price(
    trade,
    price
):
    risk = num(
        trade.get("risk")
    )

    if risk <= 0:
        return 0.0

    entry = num(
        trade.get("entry")
    )

    if (
        trade.get("direction")
        == "LONG"
    ):
        return (
            price - entry
        ) / risk

    return (
        entry - price
    ) / risk


def trade_touches(
    trade,
    high,
    low
):
    direction = trade[
        "direction"
    ]

    stop = num(
        trade["stop"]
    )

    tp1 = num(
        trade["tp1"]
    )

    tp2 = num(
        trade["tp2"]
    )

    tp3 = num(
        trade["tp3"]
    )

    if direction == "LONG":
        return {
            "stop":
            low <= stop,

            "tp1":
            high >= tp1,

            "tp2":
            high >= tp2,

            "tp3":
            high >= tp3
        }

    return {
        "stop":
        high >= stop,

        "tp1":
        low <= tp1,

        "tp2":
        low <= tp2,

        "tp3":
        low <= tp3
    }


def evaluate_trade(
    trade
):
    if (
        trade.get("status")
        != "OPEN"
    ):
        return []

    df = get_candles(
        trade["symbol"],
        "Min1"
    )

    if (
        df is None
        or len(df) == 0
    ):
        return []

    events = []

    # Normalize timestamps.
    df = df.copy()

    df["time_norm"] = (
        df["time"].apply(
            normalize_candle_time
        )
    )

    tracking_start = num(
        trade.get(
            "tracking_start",
            (
                int(
                    trade["signal_time"]
                    // 60
                )
                + 1
            ) * 60
        )
    )

    last_checked = num(
        trade.get(
            "last_checked",
            tracking_start - 1
        )
    )

    relevant = df[
        (
            df["time_norm"]
            > last_checked
        )
        &
        (
            df["time_norm"]
            >= tracking_start
        )
    ]

    latest_price = num(
        df.iloc[-1]["close"]
    )

    latest_seen_time = last_checked

    for _, candle in relevant.iterrows():
        candle_time = num(
            candle["time_norm"]
        )

        high = num(
            candle["high"]
        )

        low = num(
            candle["low"]
        )

        latest_seen_time = max(
            latest_seen_time,
            candle_time
        )

        touched = trade_touches(
            trade,
            high,
            low
        )

        # If stop and TP3 are both inside the same 1m candle,
        # intrabar order is unknowable from OHLC data.
        if (
            touched["stop"]
            and touched["tp3"]
        ):
            trade["status"] = "AMBIGUOUS"
            trade["closed_time"] = candle_time
            trade["closed_time_text"] = datetime.fromtimestamp(
                candle_time
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            trade["final_r"] = None

            events.append({
                "type": "AMBIGUOUS",
                "trade": trade
            })

            break

        # A stop closes the full paper position at -1R.
        # TP1/TP2 touched in the exact same 1m candle are not
        # credited because we do not know whether they happened
        # before or after the stop.
        if touched["stop"]:
            trade["status"] = "STOP"
            trade["closed_time"] = candle_time
            trade["closed_time_text"] = datetime.fromtimestamp(
                candle_time
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            trade["final_r"] = -1.0

            events.append({
                "type": "STOP",
                "trade": trade
            })

            break

        # If TP3 is reached, close at +3R and send one event.
        if touched["tp3"]:
            trade["tp1_hit"] = True
            trade["tp2_hit"] = True
            trade["tp3_hit"] = True

            if trade["tp1_time"] is None:
                trade["tp1_time"] = candle_time

            if trade["tp2_time"] is None:
                trade["tp2_time"] = candle_time

            trade["tp3_time"] = candle_time
            trade["status"] = "TP3"

            trade["closed_time"] = candle_time
            trade["closed_time_text"] = datetime.fromtimestamp(
                candle_time
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            trade["final_r"] = 3.0

            events.append({
                "type": "TP3",
                "trade": trade
            })

            break

        # If TP2 is reached, TP1 is implicitly reached too.
        # Send only one notification for the highest milestone
        # reached inside that candle.
        if (
            touched["tp2"]
            and not trade[
                "tp2_hit"
            ]
        ):
            trade["tp1_hit"] = True
            trade["tp2_hit"] = True

            if trade["tp1_time"] is None:
                trade["tp1_time"] = candle_time

            trade["tp2_time"] = candle_time

            events.append({
                "type": "TP2",
                "trade": trade
            })

        elif (
            touched["tp1"]
            and not trade[
                "tp1_hit"
            ]
        ):
            trade["tp1_hit"] = True
            trade["tp1_time"] = candle_time

            events.append({
                "type": "TP1",
                "trade": trade
            })

    trade["last_checked"] = max(
        latest_seen_time,
        trade.get(
            "last_checked",
            0
        )
    )

    # Time-based exit if still open.
    if (
        trade["status"]
        == "OPEN"
    ):
        age = (
            time.time()
            - trade["signal_time"]
        )

        if (
            age
            >= TRADE_EXPIRY_HOURS
            * 60
            * 60
        ):
            final_r = current_r_for_price(
                trade,
                latest_price
            )

            # By construction, stop and TP3 should already have
            # been caught. Clamp for gap/data anomalies.
            final_r = max(
                -1.0,
                min(
                    3.0,
                    final_r
                )
            )

            trade["status"] = "EXPIRED"
            trade["closed_time"] = time.time()
            trade["closed_time_text"] = local_time()
            trade["final_r"] = round(
                final_r,
                4
            )

            events.append({
                "type": "EXPIRED",
                "trade": trade
            })

    return events


def log_trade_result(
    trade
):
    if trade.get(
        "result_logged"
    ):
        return

    file_exists = (
        RESULTS_FILE.exists()
    )

    fields = [
        "signal_id",
        "signal_time",
        "closed_time",
        "symbol",
        "direction",
        "score",
        "oi_score",
        "entry",
        "stop",
        "tp1",
        "tp2",
        "tp3",
        "status",
        "final_r",
        "tp1_hit",
        "tp2_hit",
        "tp3_hit"
    ]

    with open(
        RESULTS_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fields
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "signal_id":
            trade["signal_id"],

            "signal_time":
            trade[
                "signal_time_text"
            ],

            "closed_time":
            trade.get(
                "closed_time_text"
            ),

            "symbol":
            trade["symbol"],

            "direction":
            trade["direction"],

            "score":
            trade["score"],

            "oi_score":
            trade["oi_score"],

            "entry":
            trade["entry"],

            "stop":
            trade["stop"],

            "tp1":
            trade["tp1"],

            "tp2":
            trade["tp2"],

            "tp3":
            trade["tp3"],

            "status":
            trade["status"],

            "final_r":
            trade.get(
                "final_r"
            ),

            "tp1_hit":
            trade["tp1_hit"],

            "tp2_hit":
            trade["tp2_hit"],

            "tp3_hit":
            trade["tp3_hit"]
        })

    trade[
        "result_logged"
    ] = True


def build_trade_event_message(
    event_type,
    trade
):
    symbol = trade[
        "symbol"
    ]

    direction = trade[
        "direction"
    ]

    if event_type == "TP1":
        return (
            f"✅ {symbol} {direction}\n\n"
            f"TP1 HIT — +1.5R level reached.\n"
            f"Trade remains open for evaluation."
        )

    if event_type == "TP2":
        return (
            f"✅ {symbol} {direction}\n\n"
            f"TP2 HIT — +2.0R level reached.\n"
            f"Trade remains open for evaluation."
        )

    if event_type == "TP3":
        return (
            f"🏆 {symbol} {direction}\n\n"
            f"TP3 HIT — paper trade closed at +3R.\n"
            f"Signal score: {trade['score']}/100"
        )

    if event_type == "STOP":
        return (
            f"❌ {symbol} {direction}\n\n"
            f"STOP HIT — paper trade closed at -1R.\n"
            f"Signal score: {trade['score']}/100"
        )

    if event_type == "EXPIRED":
        return (
            f"⏱ {symbol} {direction}\n\n"
            f"Paper trade expired after "
            f"{TRADE_EXPIRY_HOURS}h.\n"
            f"Final result: "
            f"{trade['final_r']:+.2f}R"
        )

    return (
        f"⚠️ {symbol} {direction}\n\n"
        f"Outcome marked AMBIGUOUS because "
        f"the stop and TP3 were both touched "
        f"inside the same 1-minute candle.\n"
        f"It is excluded from expectancy stats."
    )


def track_open_trades(
    trades
):
    open_trades = [
        trade
        for trade in trades
        if trade.get("status") == "OPEN"
    ]

    if not open_trades:
        return

    print(
        f"Tracking "
        f"{len(open_trades)} "
        f"open paper trade(s)..."
    )

    any_change = False

    for trade in open_trades:
        try:
            events = evaluate_trade(
                trade
            )

        except Exception as error:
            print(
                f"Trade tracker error "
                f"{trade.get('symbol')}: "
                f"{error}"
            )

            continue

        if events:
            any_change = True

        for event in events:
            event_type = event[
                "type"
            ]

            print(
                f"   {trade['symbol']} "
                f"{event_type}"
            )

            send_telegram(
                build_trade_event_message(
                    event_type,
                    trade
                )
            )

        if (
            trade["status"]
            != "OPEN"
        ):
            log_trade_result(
                trade
            )

            any_change = True

    if any_change:
        save_json(
            TRADES_FILE,
            trades
        )


# ============================================================
# PERFORMANCE STATISTICS
# ============================================================

def calculate_stats(
    trades
):
    total = len(trades)

    open_count = sum(
        1
        for trade in trades
        if trade.get("status")
        == "OPEN"
    )

    ambiguous = sum(
        1
        for trade in trades
        if trade.get("status")
        == "AMBIGUOUS"
    )

    resolved = [
        trade
        for trade in trades
        if trade.get("status")
        in {
            "TP3",
            "STOP",
            "EXPIRED"
        }
        and trade.get(
            "final_r"
        ) is not None
    ]

    tp1_hits = sum(
        1
        for trade in trades
        if trade.get(
            "tp1_hit"
        )
    )

    tp2_hits = sum(
        1
        for trade in trades
        if trade.get(
            "tp2_hit"
        )
    )

    tp3_hits = sum(
        1
        for trade in trades
        if trade.get(
            "tp3_hit"
        )
    )

    cumulative_r = sum(
        num(
            trade.get(
                "final_r"
            )
        )
        for trade in resolved
    )

    average_r = (
        cumulative_r
        / len(resolved)
        if resolved
        else 0.0
    )

    wins = sum(
        1
        for trade in resolved
        if num(
            trade.get(
                "final_r"
            )
        ) > 0
    )

    win_rate = (
        wins
        / len(resolved)
        * 100
        if resolved
        else 0.0
    )

    return {
        "total": total,
        "open": open_count,
        "resolved": len(
            resolved
        ),
        "ambiguous": ambiguous,
        "tp1_hits": tp1_hits,
        "tp2_hits": tp2_hits,
        "tp3_hits": tp3_hits,
        "cumulative_r": cumulative_r,
        "average_r": average_r,
        "win_rate": win_rate
    }


def print_stats(
    trades
):
    stats = calculate_stats(
        trades
    )

    print()
    print(
        "PAPER PERFORMANCE"
    )

    print(
        "-" * 60
    )

    print(
        f"Signals:       "
        f"{stats['total']}"
    )

    print(
        f"Open:          "
        f"{stats['open']}"
    )

    print(
        f"Resolved:      "
        f"{stats['resolved']}"
    )

    print(
        f"Ambiguous:     "
        f"{stats['ambiguous']}"
    )

    print(
        f"TP1 reached:   "
        f"{stats['tp1_hits']}"
    )

    print(
        f"TP2 reached:   "
        f"{stats['tp2_hits']}"
    )

    print(
        f"TP3 reached:   "
        f"{stats['tp3_hits']}"
    )

    print(
        f"Win rate:      "
        f"{stats['win_rate']:.1f}%"
    )

    print(
        f"Average R:     "
        f"{stats['average_r']:+.2f}R"
    )

    print(
        f"Cumulative R:  "
        f"{stats['cumulative_r']:+.2f}R"
    )


# ============================================================
# DUPLICATE ALERT PROTECTION
# ============================================================

def should_send_alert(
    result,
    alert_state,
    trades
):
    # Do not stack another paper trade on the same market
    # while one is still open.
    if has_open_trade_for_symbol(
        trades,
        result["symbol"]
    ):
        return False

    key = (
        f"{result['symbol']}_"
        f"{result['direction']}"
    )

    previous = alert_state.get(
        key
    )

    if previous is None:
        return True

    now = time.time()

    last_time = num(
        previous.get("time")
    )

    last_score = num(
        previous.get("score")
    )

    age = (
        now - last_time
    )

    if (
        age
        >= ALERT_COOLDOWN
    ):
        return True

    if (
        result["best_score"]
        >= (
            last_score
            + ALERT_SCORE_IMPROVEMENT
        )
    ):
        return True

    return False


def record_alert_state(
    result,
    alert_state
):
    key = (
        f"{result['symbol']}_"
        f"{result['direction']}"
    )

    alert_state[key] = {
        "time": time.time(),
        "score":
        result["best_score"],
        "price":
        result["price"]
    }

    save_json(
        ALERT_STATE_FILE,
        alert_state
    )


# ============================================================
# TELEGRAM SIGNAL MESSAGE
# ============================================================

def build_alert_message(
    result
):
    plan = result[
        "risk_plan"
    ]

    oi = result[
        "oi_metrics"
    ]

    side_icon = (
        "🟢"
        if result[
            "direction"
        ] == "LONG"
        else "🔴"
    )

    factor_values = list(result.get("parts", {}).values())
    supportive = sum(1 for value in factor_values if value > 0)
    strong = sum(1 for value in factor_values if value == 2)

    return (
        f"🚨 FUTURES HUNTER V6 — 50 FACTOR\n"
        f"\n"
        f"{side_icon} "
        f"{result['symbol']} "
        f"{result['direction']}\n"
        f"\n"
        f"50-factor score: "
        f"{result['best_score']}/100\n"
        f"Factors aligned: {supportive}/50 "
        f"({strong} strong)\n"
        f"OI confirmation: "
        f"{result['oi_score']}/15\n"
        f"\n"
        f"Paper entry: "
        f"{result['price']}\n"
        f"Entry zone: "
        f"{plan['entry_low']:.8f}"
        f" → "
        f"{plan['entry_high']:.8f}\n"
        f"\n"
        f"Stop: "
        f"{plan['stop']:.8f}\n"
        f"Stop distance: "
        f"{plan['stop_pct']:.2f}%\n"
        f"\n"
        f"TP1: "
        f"{plan['tp1']:.8f} "
        f"(1.5R)\n"
        f"TP2: "
        f"{plan['tp2']:.8f} "
        f"(2R)\n"
        f"TP3: "
        f"{plan['tp3']:.8f} "
        f"(3R)\n"
        f"\n"
        f"OI 5m: "
        f"{format_optional(oi.get('oi5'))}\n"
        f"Price 5m: "
        f"{format_optional(oi.get('price5'))}\n"
        f"OI 15m: "
        f"{format_optional(oi.get('oi15'))}\n"
        f"Price 15m: "
        f"{format_optional(oi.get('price15'))}\n"
        f"\n"
        f"15m RSI: "
        f"{result['15m']['rsi']:.1f}\n"
        f"1h ADX: "
        f"{result['1h']['adx']:.1f}\n"
        f"15m RV: "
        f"{result['15m']['rv']:.2f}x\n"
        f"Funding: "
        f"{result['funding'] * 100:.4f}%\n"
        f"Spread: "
        f"{result['spread']:.4f}%\n"
        f"\n"
        f"⚠️ Paper-trading signal only."
    )


# ============================================================
# RUN FULL TECHNICAL SCAN
# ============================================================

def run_full_scan(
    symbols,
    details,
    oi_metrics
):
    print()
    print(
        "=" * 80
    )

    print(
        f"FULL SCAN — "
        f"{local_time()}"
    )

    print(
        "=" * 80
    )

    results = []

    for symbol in symbols:
        ticker = details.get(
            symbol
        )

        if ticker is None:
            continue

        try:
            result = analyse_market(
                symbol,
                ticker,
                oi_metrics.get(
                    symbol,
                    {}
                )
            )

            if result is not None:
                results.append(
                    result
                )

        except Exception as error:
            print(
                f"ERROR "
                f"{symbol}: "
                f"{error}"
            )

    if not results:
        print(
            "No markets analysed."
        )

        return None

    results.sort(
        key=lambda item:
        (
            item["qualified"],
            item["best_score"]
        ),
        reverse=True
    )

    print()
    print(
        "TOP 5 CURRENT SETUPS"
    )

    print(
        "-" * 80
    )

    for result in results[:5]:
        status = (
            "PASS"
            if result[
                "qualified"
            ]
            else "NO"
        )

        print(
            f"{result['symbol']:<18}"
            f"{result['direction']:<7}"
            f"Score "
            f"{result['best_score']:>3} "
            f"OI "
            f"{result['oi_score']:>2}/15 "
            f"{status}"
        )

    qualified = [
        result
        for result in results
        if result[
            "qualified"
        ]
    ]

    if not qualified:
        print()
        print(
            "Decision: NO TRADE"
        )

        return None

    best = qualified[0]

    print()
    print(
        "Best qualifying setup:"
    )

    print(
        f"{best['symbol']} "
        f"{best['direction']} "
        f"{best['best_score']}/100"
    )

    return best


# ============================================================
# CONTINUOUS ENGINE
# ============================================================

async def main():
    print()
    print(
        "=" * 80
    )

    print(
        "MEXC FUTURES HUNTER V6 — 50 FACTORS"
    )

    print(
        "=" * 80
    )

    print()
    print(
        "Telegram: "
        + (
            "CONNECTED"
            if telegram_ready()
            else "NOT CONFIGURED"
        )
    )

    if not telegram_ready():
        print()
        print(
            "Check your .env file."
        )

        return

    start_telegram_command_thread()
    print("Telegram subscriber command listener: ACTIVE")

    history = load_json(
        HISTORY_FILE,
        {}
    )

    alert_state = load_json(
        ALERT_STATE_FILE,
        {}
    )

    trades = load_json(
        TRADES_FILE,
        []
    )

    imported = import_recent_logged_signals(
        trades
    )

    if imported:
        print()
        print(
            f"Imported {imported} recent "
            f"paper signal(s) into V5 tracking."
        )

    print()
    print(
        "OI snapshots: every 60 seconds"
    )

    print(
        "Full scan: every 5 minutes"
    )

    print(
        "Trade outcome check: every 60 seconds"
    )

    print(
        f"Paper trade expiry: "
        f"{TRADE_EXPIRY_HOURS} hours"
    )

    print(
        "Press Ctrl+C to stop."
    )

    send_telegram(
        "✅ Futures Hunter V6 is online.\n\n"
        "50-factor scanner + automatic "
        "paper-trade outcome tracking active."
    )

    next_full_scan = 0

    while True:
        cycle_start = (
            time.time()
        )

        try:
            print()
            print(
                f"[{local_time()}] "
                f"Updating market + OI data..."
            )

            symbols = await get_top_symbols()

            details = await get_detailed_tickers(
                symbols
            )

            oi_metrics = update_oi_history(
                history,
                details
            )

            # First update all existing paper trades.
            track_open_trades(
                trades
            )

            now = time.time()

            if now >= next_full_scan:
                best = run_full_scan(
                    symbols,
                    details,
                    oi_metrics
                )

                next_full_scan = (
                    now
                    + FULL_SCAN_INTERVAL
                )

                if best is not None:
                    if should_send_alert(
                        best,
                        alert_state,
                        trades
                    ):
                        message = build_alert_message(
                            best
                        )

                        sent = send_telegram(
                            message
                        )

                        if sent:
                            print()
                            print(
                                "✅ Telegram alert broadcast."
                            )

                            save_latest_signal(message, best)

                            log_signal(
                                best
                            )

                            create_paper_trade(
                                best,
                                trades
                            )

                            record_alert_state(
                                best,
                                alert_state
                            )

                        else:
                            print()
                            print(
                                "⚠️ Telegram alert failed."
                            )

                    else:
                        print()
                        print(
                            "Qualifying setup found, "
                            "but duplicate/open-trade "
                            "alert suppressed."
                        )

                print_stats(
                    trades
                )

        except Exception as error:
            print()
            print(
                f"MAIN LOOP ERROR: "
                f"{error}"
            )

        elapsed = (
            time.time()
            - cycle_start
        )

        sleep_for = max(
            5,
            OI_UPDATE_INTERVAL
            - elapsed
        )

        await asyncio.sleep(
            sleep_for
        )



# ============================================================
# V6.6 ADAPTIVE MOMENTUM / BREAKOUT OVERRIDES
# ============================================================
#
# Design goals:
# - keep the original 50 transparent factors
# - stop treating every factor as equally important
# - remove OI/RSI-style vetoes that can suppress strong trends
# - explicitly recognize breakout / continuation regimes
# - surface WATCH -> ARMED -> ENTRY states
# - keep shadow-paper records of rejected/developing setups
# - make /why SYMBOL diagnostics available in Telegram
#
# IMPORTANT: this improves opportunity capture and measurement;
# it does not guarantee profitability. Validate on paper data first.

# Universe / cadence
TOP_N = int(os.getenv("TOP_N", "25"))
MIN_24H_TURNOVER = float(os.getenv("MIN_24H_TURNOVER", "5000000"))
FULL_SCAN_INTERVAL = int(os.getenv("FULL_SCAN_INTERVAL", str(3 * 60)))
MOMENTUM_EXTRA_N = int(os.getenv("MOMENTUM_EXTRA_N", "5"))
FOCUS_SYMBOLS = {
    item.strip().upper()
    for item in os.getenv("FOCUS_SYMBOLS", "ZEC_USDT").split(",")
    if item.strip()
}

# Adaptive state thresholds
WATCH_SCORE_NORMAL = 64
ARMED_SCORE_NORMAL = 70
ENTRY_SCORE_NORMAL = 76

WATCH_SCORE_TREND = 60
ARMED_SCORE_TREND = 66
ENTRY_SCORE_TREND = 72

WATCH_SCORE_CONTINUATION = 58
ARMED_SCORE_CONTINUATION = 64
ENTRY_SCORE_CONTINUATION = 70

WATCH_SCORE_BREAKOUT = 58
ARMED_SCORE_BREAKOUT = 63
ENTRY_SCORE_BREAKOUT = 68

# Hard safety / execution gates. These are deliberately few.
V66_MIN_ADX_1H = 16
V66_MAX_SPREAD_PCT = 0.03
V66_MIN_STOP_PCT = 0.08
V66_MAX_STOP_PCT = 3.00
V66_MAX_LIVE_DISTANCE_ATR = 1.25
V66_MAX_EMA_DISTANCE_ATR = 2.00
V66_MAX_BREAKOUT_EMA_DISTANCE_ATR = 2.75
V66_EXTREME_FUNDING = 0.0020

# Watch / diagnostics / shadow research
LATEST_SCAN_FILE = BASE_DIR / "latest_scan_v66.json"
MARKET_STATE_FILE = BASE_DIR / "market_states_v66.json"
SHADOW_TRADES_FILE = BASE_DIR / "shadow_trades_v66.json"
SHADOW_RESULTS_FILE = BASE_DIR / "shadow_trade_results_v66.csv"
SHADOW_MIN_SCORE = 58
SHADOW_RECAPTURE_COOLDOWN = 60 * 60
WATCH_ALERT_COOLDOWN = 45 * 60
WATCH_SCORE_IMPROVEMENT = 4

# Optional sizing info shown in alerts; this does NOT place orders.
ACCOUNT_SIZE = float(os.getenv("ACCOUNT_SIZE", "0"))
RISK_PCT = float(os.getenv("RISK_PCT", "0.50"))


def _clean_json_value(value):
    """Convert numpy/NaN values into JSON-safe primitives."""
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        if not np.isfinite(value):
            return None
        return value
    if isinstance(value, dict):
        return {str(k): _clean_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean_json_value(v) for v in value]
    return value


def _normalize_symbol_query(value):
    value = (value or "").strip().upper().replace("/", "_").replace("-", "_")
    if not value:
        return ""
    if not value.endswith("_USDT"):
        value = f"{value}_USDT"
    return value


def _signal_thresholds(regime):
    if regime == "BREAKOUT":
        return WATCH_SCORE_BREAKOUT, ARMED_SCORE_BREAKOUT, ENTRY_SCORE_BREAKOUT
    if regime == "TREND_CONTINUATION":
        return (
            WATCH_SCORE_CONTINUATION,
            ARMED_SCORE_CONTINUATION,
            ENTRY_SCORE_CONTINUATION,
        )
    if regime == "TREND":
        return WATCH_SCORE_TREND, ARMED_SCORE_TREND, ENTRY_SCORE_TREND
    return WATCH_SCORE_NORMAL, ARMED_SCORE_NORMAL, ENTRY_SCORE_NORMAL


def _factor_group(name):
    name_upper = name.upper()

    if (
        " TREND" in name_upper
        or "PRICE VS EMA" in name_upper
        or "EMA20 SLOPE" in name_upper
        or "EMA50 SLOPE" in name_upper
        or "VWAP SIDE" in name_upper
    ):
        return "trend"

    if any(
        token in name_upper
        for token in ("RSI", "MACD", " ROC", "STOCHASTIC", "CCI", "WILLIAMS", "MFI")
    ):
        return "momentum"

    if any(
        token in name_upper
        for token in ("RELATIVE VOLUME", "OBV SLOPE", "VOLUME TREND")
    ):
        return "volume"

    if any(
        token in name_upper
        for token in ("BREAKOUT", "SWING STRUCTURE", "BOLLINGER POSITION", "BB EXPANSION")
    ):
        return "breakout"

    if any(
        token in name_upper
        for token in ("ADX", "CANDLE BODY", "CLOSE LOCATION")
    ):
        return "strength"

    if "ATR REGIME" in name_upper:
        return "regime"

    if name_upper in {"FUNDING", "SPREAD"}:
        return "execution"

    if "OI CONFIRMATION" in name_upper:
        return "oi"

    return "trend"


V66_GROUP_WEIGHTS = {
    "trend": 25.0,
    "momentum": 15.0,
    "strength": 10.0,
    "volume": 15.0,
    "breakout": 15.0,
    "regime": 5.0,
    "execution": 5.0,
    "oi": 10.0,
}


def weighted_factor_score(parts):
    grouped = {name: [] for name in V66_GROUP_WEIGHTS}

    for name, value in parts.items():
        group = _factor_group(name)
        grouped[group].append(float(value))

    breakdown = {}
    total = 0.0

    for group, weight in V66_GROUP_WEIGHTS.items():
        values = grouped[group]
        if not values:
            pct = 0.0
        else:
            pct = sum(values) / (2.0 * len(values))

        contribution = pct * weight
        breakdown[group] = {
            "pct": round(pct * 100, 1),
            "contribution": round(contribution, 2),
            "weight": weight,
        }
        total += contribution

    return round(max(0.0, min(100.0, total)), 1), breakdown


def detect_market_regime(direction, tf5, tf15, tf1h):
    trend_ok = (
        direction_matches(tf1h["trend"], direction)
        and direction_matches(tf15["trend"], direction)
    )

    if direction == "LONG":
        direct_15 = tf15["high20_prev"] > 0 and tf15["close"] > tf15["high20_prev"]
        direct_5 = tf5["high20_prev"] > 0 and tf5["close"] > tf5["high20_prev"]
        near_15 = (
            tf15["high20_prev"] > 0
            and tf15["atr"] > 0
            and tf15["close"] >= tf15["high20_prev"] - 0.35 * tf15["atr"]
        )
        near_5 = (
            tf5["high20_prev"] > 0
            and tf5["atr"] > 0
            and tf5["close"] >= tf5["high20_prev"] - 0.35 * tf5["atr"]
        )
    else:
        direct_15 = tf15["low20_prev"] > 0 and tf15["close"] < tf15["low20_prev"]
        direct_5 = tf5["low20_prev"] > 0 and tf5["close"] < tf5["low20_prev"]
        near_15 = (
            tf15["low20_prev"] > 0
            and tf15["atr"] > 0
            and tf15["close"] <= tf15["low20_prev"] + 0.35 * tf15["atr"]
        )
        near_5 = (
            tf5["low20_prev"] > 0
            and tf5["atr"] > 0
            and tf5["close"] <= tf5["low20_prev"] + 0.35 * tf5["atr"]
        )

    volume_ok = (
        tf15["rv"] >= 0.75
        or tf5["rv"] >= 0.90
        or tf15["volume_trend"] >= 5
    )
    strength_ok = tf1h["adx"] >= 18 and tf15["adx"] >= 15

    if trend_ok and strength_ok and volume_ok and (direct_15 or direct_5):
        return "BREAKOUT"

    if trend_ok and strength_ok and (near_15 or near_5):
        return "TREND_CONTINUATION"

    if trend_ok:
        return "TREND"

    return "NORMAL"


def regime_bonus(regime, direction, tf5, tf15, oi_points):
    bonus = 0.0

    if regime == "BREAKOUT":
        bonus += 7.0
    elif regime == "TREND_CONTINUATION":
        bonus += 4.0
    elif regime == "TREND":
        bonus += 2.0

    # OI helps, but it no longer vetoes a good setup.
    if oi_points >= 10:
        bonus += 2.0
    elif oi_points >= 6:
        bonus += 1.0

    # Reward decisive closes in the direction of the move.
    if direction == "LONG" and tf15["close_location"] >= 0.70:
        bonus += 1.0
    elif direction == "SHORT" and tf15["close_location"] <= 0.30:
        bonus += 1.0

    return bonus


async def get_top_symbols():
    """
    V6.6 universe:
    - top liquid markets
    - explicit focus symbols (ZEC_USDT by default)
    - a few high-24h-momentum markets
    """
    async with websockets.connect(MEXC_WS) as ws:
        await ws.send(
            json.dumps({
                "method": "sub.tickers",
                "param": {},
                "gzip": False,
            })
        )

        while True:
            raw = await ws.recv()
            message = json.loads(raw)

            if message.get("channel") != "push.tickers":
                continue

            tickers = message.get("data", [])
            if not isinstance(tickers, list):
                continue

            eligible = []
            all_symbols = set()

            # Refresh once per cache window before crypto filtering so newly
            # listed MEXC stock futures cannot leak into the crypto Core.
            equity_refresh = globals().get("_v73_refresh_equity_symbol_cache")
            if callable(equity_refresh):
                try:
                    equity_refresh()
                except Exception as _equity_refresh_error:
                    print(f"V7.5 equity-universe refresh warning: {type(_equity_refresh_error).__name__}: {_equity_refresh_error}")

            for ticker in tickers:
                symbol = ticker.get("symbol", "")
                if not symbol.endswith("_USDT"):
                    continue
                # V7.5 hard-separates stock/index futures from the crypto Core.
                # Equity contracts are evaluated only by the dedicated NY cash-open ORB lab.
                equity_detector = globals().get("_v73_is_equity_symbol")
                if callable(equity_detector) and equity_detector(symbol):
                    continue

                all_symbols.add(symbol)
                turnover = num(ticker.get("amount24"))
                change = abs(num(ticker.get("riseFallRate")))

                if turnover >= MIN_24H_TURNOVER:
                    eligible.append({
                        "symbol": symbol,
                        "turnover": turnover,
                        "change": change,
                    })

            eligible.sort(key=lambda item: item["turnover"], reverse=True)
            selected = [item["symbol"] for item in eligible[:TOP_N]]

            # Add strongest movers that already clear the liquidity floor.
            movers = sorted(
                [item for item in eligible if item["change"] > 0],
                key=lambda item: item["change"],
                reverse=True,
            )
            for item in movers[:MOMENTUM_EXTRA_N]:
                if item["symbol"] not in selected:
                    selected.append(item["symbol"])

            # Never silently ignore an explicit focus market.
            for symbol in sorted(FOCUS_SYMBOLS):
                if symbol in all_symbols and symbol not in selected:
                    selected.append(symbol)

            return selected


def qualify_setup(result):
    """
    V6.6: only genuine safety/execution constraints are hard gates.
    OI, RSI, Stochastic, CCI, MFI etc. influence the score but do not
    independently veto a strong breakout.
    """
    hard_reasons = []

    direction = result["direction"]
    tf5 = result["5m"]
    tf15 = result["15m"]
    tf1h = result["1h"]
    plan = result["risk_plan"]
    regime = result.get("regime", "NORMAL")
    score = result["best_score"]

    watch_score, armed_score, entry_score = _signal_thresholds(regime)
    result["watch_threshold"] = watch_score
    result["armed_threshold"] = armed_score
    result["entry_threshold"] = entry_score

    if not direction_matches(tf1h["trend"], direction):
        hard_reasons.append("1H trend not aligned")

    if not direction_matches(tf15["trend"], direction):
        hard_reasons.append("15m trend not aligned")

    if tf1h["adx"] < V66_MIN_ADX_1H:
        hard_reasons.append("1H ADX too weak")

    if result["spread"] > V66_MAX_SPREAD_PCT:
        hard_reasons.append("spread too wide")

    # Only veto truly crowded funding; moderate positive funding in a
    # long breakout is no longer treated as disqualifying.
    if direction == "LONG" and result["funding"] > V66_EXTREME_FUNDING:
        hard_reasons.append("long funding extremely crowded")
    if direction == "SHORT" and result["funding"] < -V66_EXTREME_FUNDING:
        hard_reasons.append("short funding extremely crowded")

    if tf15["atr"] > 0:
        ema_distance = abs(tf15["close"] - tf15["ema20"]) / tf15["atr"]
    else:
        ema_distance = 999.0

    ema_limit = (
        V66_MAX_BREAKOUT_EMA_DISTANCE_ATR
        if regime in {"BREAKOUT", "TREND_CONTINUATION"}
        else V66_MAX_EMA_DISTANCE_ATR
    )
    result["ema20_distance_atr"] = ema_distance

    if ema_distance > ema_limit:
        hard_reasons.append(
            f"price stretched {ema_distance:.2f} ATR from 15m EMA20"
        )

    if tf5["atr"] > 0:
        live_distance = abs(result["price"] - tf5["close"]) / tf5["atr"]
    else:
        live_distance = 999.0

    result["live_distance_atr"] = live_distance

    if live_distance > V66_MAX_LIVE_DISTANCE_ATR:
        hard_reasons.append(
            f"live price {live_distance:.2f} ATR from latest 5m close"
        )

    if plan is None:
        hard_reasons.append("invalid risk plan")
    else:
        if plan["stop_pct"] < V66_MIN_STOP_PCT:
            hard_reasons.append("stop too tight")
        if plan["stop_pct"] > V66_MAX_STOP_PCT:
            hard_reasons.append("stop too wide")

    # Truly dead volume can still veto a non-breakout setup.
    if (
        regime not in {"BREAKOUT", "TREND_CONTINUATION"}
        and tf15["rv"] < 0.40
        and tf15["volume_trend"] < -30
    ):
        hard_reasons.append("volume regime too weak")

    if hard_reasons:
        state = "REJECT"
    elif score >= entry_score:
        state = "ENTRY"
    elif score >= armed_score:
        state = "ARMED"
    elif score >= watch_score:
        state = "WATCH"
    else:
        state = "IGNORE"

    result["signal_state"] = state
    result["hard_reject_reasons"] = hard_reasons

    reasons = list(hard_reasons)
    if not hard_reasons and state != "ENTRY":
        reasons.append(
            f"score {score:.1f} below {entry_score} ENTRY threshold"
        )

    return state == "ENTRY", reasons


def analyse_market(symbol, ticker, oi_metrics):
    print(f"   Analysing {symbol}...")

    tf5 = analyse_timeframe(symbol, "Min5")
    time.sleep(0.15)
    tf15 = analyse_timeframe(symbol, "Min15")
    time.sleep(0.15)
    tf1h = analyse_timeframe(symbol, "Min60")
    time.sleep(0.15)

    if tf5 is None or tf15 is None or tf1h is None:
        return None

    bid = num(ticker.get("bid1"))
    ask = num(ticker.get("ask1"))
    funding = num(ticker.get("fundingRate"))
    open_interest = num(ticker.get("holdVol"))
    turnover = num(ticker.get("amount24"))
    live_price = num(ticker.get("lastPrice"))

    spread = 999.0
    if bid > 0 and ask > 0:
        midpoint = (bid + ask) / 2
        spread = ((ask - bid) / midpoint) * 100

    long_raw, long_parts = score_direction(
        "LONG", tf5, tf15, tf1h, funding, spread, oi_metrics
    )
    short_raw, short_parts = score_direction(
        "SHORT", tf5, tf15, tf1h, funding, spread, oi_metrics
    )

    long_weighted, long_breakdown = weighted_factor_score(long_parts)
    short_weighted, short_breakdown = weighted_factor_score(short_parts)

    long_oi = oi_score("LONG", oi_metrics)
    short_oi = oi_score("SHORT", oi_metrics)

    long_regime = detect_market_regime("LONG", tf5, tf15, tf1h)
    short_regime = detect_market_regime("SHORT", tf5, tf15, tf1h)

    long_effective = min(
        100.0,
        long_weighted
        + regime_bonus(long_regime, "LONG", tf5, tf15, long_oi),
    )
    short_effective = min(
        100.0,
        short_weighted
        + regime_bonus(short_regime, "SHORT", tf5, tf15, short_oi),
    )

    if long_effective >= short_effective:
        direction = "LONG"
        raw_score = long_raw
        weighted_score = long_weighted
        best_score = round(long_effective, 1)
        best_parts = long_parts
        breakdown = long_breakdown
        regime = long_regime
        selected_oi = long_oi
    else:
        direction = "SHORT"
        raw_score = short_raw
        weighted_score = short_weighted
        best_score = round(short_effective, 1)
        best_parts = short_parts
        breakdown = short_breakdown
        regime = short_regime
        selected_oi = short_oi

    risk_plan = build_risk_plan(direction, live_price, tf5)

    result = {
        "symbol": symbol,
        "price": live_price,
        "turnover": turnover,
        "oi": open_interest,
        "funding": funding,
        "spread": spread,
        "oi_metrics": oi_metrics,
        "5m": tf5,
        "15m": tf15,
        "1h": tf1h,
        "long_score": round(long_effective, 1),
        "short_score": round(short_effective, 1),
        "long_raw_score": long_raw,
        "short_raw_score": short_raw,
        "direction": direction,
        "best_score": best_score,
        "raw_score": raw_score,
        "weighted_score": weighted_score,
        "parts": best_parts,
        "score_breakdown": breakdown,
        "oi_score": selected_oi,
        "regime": regime,
        "risk_plan": risk_plan,
    }

    qualified, reasons = qualify_setup(result)
    result["qualified"] = qualified
    result["reject_reasons"] = reasons

    return result


def _snapshot_result(result):
    plan = result.get("risk_plan") or {}
    return _clean_json_value({
        "time": time.time(),
        "time_text": local_time(),
        "symbol": result.get("symbol"),
        "direction": result.get("direction"),
        "state": result.get("signal_state"),
        "regime": result.get("regime"),
        "score": result.get("best_score"),
        "raw_score": result.get("raw_score"),
        "weighted_score": result.get("weighted_score"),
        "entry_threshold": result.get("entry_threshold"),
        "armed_threshold": result.get("armed_threshold"),
        "watch_threshold": result.get("watch_threshold"),
        "oi_score": result.get("oi_score"),
        "price": result.get("price"),
        "funding": result.get("funding"),
        "spread": result.get("spread"),
        "reject_reasons": result.get("reject_reasons", []),
        "hard_reject_reasons": result.get("hard_reject_reasons", []),
        "parts": result.get("parts", {}),
        "score_breakdown": result.get("score_breakdown", {}),
        "entry": plan.get("entry"),
        "stop": plan.get("stop"),
        "stop_pct": plan.get("stop_pct"),
        "tp1": plan.get("tp1"),
        "tp2": plan.get("tp2"),
        "tp3": plan.get("tp3"),
        "rsi15": result.get("15m", {}).get("rsi"),
        "rv15": result.get("15m", {}).get("rv"),
        "adx1h": result.get("1h", {}).get("adx"),
        "trend1h": result.get("1h", {}).get("trend"),
        "trend15m": result.get("15m", {}).get("trend"),
        "ema20_distance_atr": result.get("ema20_distance_atr"),
        "live_distance_atr": result.get("live_distance_atr"),
    })


def save_scan_snapshot(results):
    payload = {
        "generated_at": local_time(),
        "results": [_snapshot_result(result) for result in results],
    }
    save_json(LATEST_SCAN_FILE, payload)


def build_why_message(symbol_query):
    symbol = _normalize_symbol_query(symbol_query)

    if not symbol:
        return "Usage: /why ZEC  (or /why ZEC_USDT)"

    data = load_json(LATEST_SCAN_FILE, {})
    results = data.get("results", [])

    result = next(
        (item for item in results if item.get("symbol") == symbol),
        None,
    )

    if result is None:
        available = ", ".join(
            item.get("symbol", "")
            for item in results[:8]
            if item.get("symbol")
        )
        return (
            f"No recent scan result for {symbol}.\n"
            f"Recent universe starts with: {available or 'none'}"
        )

    parts = result.get("parts", {})
    strongest = [
        name for name, value in parts.items() if num(value) >= 2
    ][:6]
    weakest = [
        name for name, value in parts.items() if num(value) <= 0
    ][:6]

    reasons = result.get("reject_reasons") or []
    reason_text = (
        "\n".join(f"• {reason}" for reason in reasons)
        if reasons
        else "• No hard blocker"
    )

    return (
        f"🔎 WHY {symbol}\n\n"
        f"State: {result.get('state')}\n"
        f"Regime: {result.get('regime')}\n"
        f"Adaptive score: {num(result.get('score')):.1f}/100\n"
        f"Raw 50-factor score: {num(result.get('raw_score')):.0f}/100\n"
        f"ENTRY threshold: {num(result.get('entry_threshold')):.0f}\n"
        f"OI: {num(result.get('oi_score')):.0f}/15\n"
        f"1H trend: {result.get('trend1h')}\n"
        f"15m trend: {result.get('trend15m')}\n"
        f"15m RSI: {num(result.get('rsi15')):.1f}\n"
        f"15m RV: {num(result.get('rv15')):.2f}x\n"
        f"1H ADX: {num(result.get('adx1h')):.1f}\n\n"
        f"Blockers / next requirement:\n{reason_text}\n\n"
        f"Strong factors: {', '.join(strongest) or 'none'}\n"
        f"Weak factors: {', '.join(weakest) or 'none'}"
    )


def build_watch_message():
    data = load_json(LATEST_SCAN_FILE, {})
    results = data.get("results", [])
    active = [
        item
        for item in results
        if item.get("state") in {"WATCH", "ARMED", "ENTRY"}
    ]
    active.sort(key=lambda item: num(item.get("score")), reverse=True)

    if not active:
        return "No WATCH/ARMED/ENTRY setup in the latest scan."

    lines = ["👀 FuturesHunter V6.6 watchlist", ""]
    for item in active[:5]:
        lines.append(
            f"{item.get('state'):>5}  "
            f"{item.get('symbol')} {item.get('direction')}  "
            f"{num(item.get('score')):.1f}/100  "
            f"{item.get('regime')}"
        )

    return "\n".join(lines)


def _shadow_stats_message():
    trades = load_json(SHADOW_TRADES_FILE, [])
    stats = calculate_stats(trades)
    return (
        "🧪 V6.6 shadow-trade stats\n\n"
        f"Candidates: {stats['total']}\n"
        f"Open: {stats['open']}\n"
        f"Resolved: {stats['resolved']}\n"
        f"Win rate: {stats['win_rate']:.1f}%\n"
        f"Average R: {stats['average_r']:+.2f}R\n"
        f"Cumulative R: {stats['cumulative_r']:+.2f}R"
    )


def build_welcome_message():
    return (
        "🐆 Welcome to FuturesHunter V6.6\n\n"
        "Adaptive MEXC USDT perpetual scanner.\n"
        "States: WATCH → ARMED → ENTRY.\n"
        "Breakout / trend-continuation mode is active.\n\n"
        f"🌐 Dashboard: {WEBSITE_URL}\n\n"
        "Commands: /latest /watch /why ZEC /shadow /stats /status /website /stop"
    )


def telegram_status_message():
    uptime = max(0, int(time.time() - STARTED_AT))
    hours, rem = divmod(uptime, 3600)
    minutes, _ = divmod(rem, 60)
    with SUBSCRIBER_LOCK:
        subscriber_count = len(SUBSCRIBERS)

    focus = ", ".join(sorted(FOCUS_SYMBOLS)) or "none"
    return (
        "✅ FuturesHunter V6.7 MacroHunter is online.\n\n"
        f"Subscribers: {subscriber_count}\n"
        f"Base universe: top {TOP_N} liquid contracts\n"
        f"Focus: {focus}\n"
        f"Scan interval: {FULL_SCAN_INTERVAL // 60} min\n"
        "Model: weighted 50 factors + adaptive regime\n"
        f"Uptime: {hours}h {minutes}m"
    )


def handle_telegram_command(chat_id, text):
    chunks = (text or "").strip().split()
    if not chunks:
        return

    command = chunks[0].lower()
    argument = chunks[1] if len(chunks) > 1 else ""

    if command in {"/start", "/subscribe"}:
        with SUBSCRIBER_LOCK:
            SUBSCRIBERS.add(str(chat_id))
            save_subscribers(SUBSCRIBERS)
        send_to_chat(chat_id, build_welcome_message())
        return

    if command in {"/stop", "/unsubscribe"}:
        with SUBSCRIBER_LOCK:
            SUBSCRIBERS.discard(str(chat_id))
            save_subscribers(SUBSCRIBERS)
        send_to_chat(
            chat_id,
            "🔕 FuturesHunter alerts stopped. Send /start anytime to subscribe again.",
        )
        return

    if command == "/website":
        send_to_chat(chat_id, f"🌐 FuturesHunter dashboard:\n{WEBSITE_URL}")
        return

    if command == "/status":
        send_to_chat(chat_id, telegram_status_message())
        return

    if command == "/stats":
        send_to_chat(chat_id, telegram_stats_message())
        return

    if command == "/shadow":
        send_to_chat(chat_id, _shadow_stats_message())
        return

    if command == "/watch":
        send_to_chat(chat_id, build_watch_message())
        return

    if command == "/why":
        send_to_chat(chat_id, build_why_message(argument))
        return

    if command == "/latest":
        if LATEST_SIGNAL_FILE.exists():
            try:
                payload = json.loads(
                    LATEST_SIGNAL_FILE.read_text(encoding="utf-8")
                )
                message = payload.get("message")
            except Exception:
                message = None
        else:
            message = None

        send_to_chat(
            chat_id,
            message or "No ENTRY signal has been broadcast yet. Try /watch.",
        )
        return

    if command in {"/help", "/commands"}:
        send_to_chat(chat_id, build_welcome_message())


def _make_shadow_trade(result):
    plan = result.get("risk_plan")
    if not plan:
        return None

    now = time.time()
    signal_time_text = local_time()
    tracking_start = (int(now // 60) + 1) * 60

    return {
        "signal_id": f"shadow_{int(now)}_{result['symbol']}_{result['direction']}",
        "source_key": make_source_key(
            signal_time_text, result["symbol"], result["direction"]
        ),
        "signal_time": now,
        "signal_time_text": signal_time_text,
        "tracking_start": tracking_start,
        "symbol": result["symbol"],
        "direction": result["direction"],
        "score": result["best_score"],
        "raw_score": result.get("raw_score"),
        "weighted_score": result.get("weighted_score"),
        "oi_score": result["oi_score"],
        "regime": result.get("regime"),
        "signal_state": result.get("signal_state"),
        "reject_reasons": result.get("reject_reasons", []),
        "entry": result["price"],
        "entry_low": plan["entry_low"],
        "entry_high": plan["entry_high"],
        "stop": plan["stop"],
        "tp1": plan["tp1"],
        "tp2": plan["tp2"],
        "tp3": plan["tp3"],
        "risk": plan["risk"],
        "stop_pct": plan["stop_pct"],
        "status": "OPEN",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "tp1_time": None,
        "tp2_time": None,
        "tp3_time": None,
        "closed_time": None,
        "closed_time_text": None,
        "final_r": None,
        "last_checked": tracking_start - 1,
        "result_logged": False,
    }


def record_shadow_candidates(results):
    trades = load_json(SHADOW_TRADES_FILE, [])
    now = time.time()
    added = 0

    for result in results:
        if result.get("signal_state") == "ENTRY":
            continue
        if num(result.get("best_score")) < SHADOW_MIN_SCORE:
            continue
        if result.get("risk_plan") is None:
            continue

        symbol = result["symbol"]
        direction = result["direction"]

        if any(
            trade.get("status") == "OPEN"
            and trade.get("symbol") == symbol
            and trade.get("direction") == direction
            for trade in trades
        ):
            continue

        recent_same = any(
            trade.get("symbol") == symbol
            and trade.get("direction") == direction
            and now - num(trade.get("signal_time")) < SHADOW_RECAPTURE_COOLDOWN
            for trade in trades
        )
        if recent_same:
            continue

        trade = _make_shadow_trade(result)
        if trade is not None:
            trades.append(trade)
            added += 1

    if added:
        save_json(SHADOW_TRADES_FILE, trades)
        print(f"Shadow research: captured {added} rejected/developing setup(s).")


def _log_shadow_result(trade):
    if trade.get("result_logged"):
        return

    file_exists = SHADOW_RESULTS_FILE.exists()
    fields = [
        "signal_id",
        "signal_time",
        "closed_time",
        "symbol",
        "direction",
        "signal_state",
        "regime",
        "score",
        "raw_score",
        "weighted_score",
        "oi_score",
        "entry",
        "stop",
        "tp1",
        "tp2",
        "tp3",
        "status",
        "final_r",
        "tp1_hit",
        "tp2_hit",
        "tp3_hit",
        "reject_reasons",
    ]

    with open(
        SHADOW_RESULTS_FILE,
        "a",
        newline="",
        encoding="utf-8",
    ) as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "signal_id": trade.get("signal_id"),
            "signal_time": trade.get("signal_time_text"),
            "closed_time": trade.get("closed_time_text"),
            "symbol": trade.get("symbol"),
            "direction": trade.get("direction"),
            "signal_state": trade.get("signal_state"),
            "regime": trade.get("regime"),
            "score": trade.get("score"),
            "raw_score": trade.get("raw_score"),
            "weighted_score": trade.get("weighted_score"),
            "oi_score": trade.get("oi_score"),
            "entry": trade.get("entry"),
            "stop": trade.get("stop"),
            "tp1": trade.get("tp1"),
            "tp2": trade.get("tp2"),
            "tp3": trade.get("tp3"),
            "status": trade.get("status"),
            "final_r": trade.get("final_r"),
            "tp1_hit": trade.get("tp1_hit"),
            "tp2_hit": trade.get("tp2_hit"),
            "tp3_hit": trade.get("tp3_hit"),
            "reject_reasons": " | ".join(trade.get("reject_reasons") or []),
        })

    trade["result_logged"] = True


def track_shadow_trades():
    trades = load_json(SHADOW_TRADES_FILE, [])
    open_trades = [
        trade for trade in trades if trade.get("status") == "OPEN"
    ]

    if not open_trades:
        return

    any_change = False

    for trade in open_trades:
        try:
            events = evaluate_trade(trade)
        except Exception as error:
            print(
                f"Shadow tracker error {trade.get('symbol')}: {error}"
            )
            continue

        if events:
            any_change = True

        if trade.get("status") != "OPEN":
            _log_shadow_result(trade)
            any_change = True

    if any_change:
        save_json(SHADOW_TRADES_FILE, trades)


def _state_rank(state):
    return {
        "IGNORE": 0,
        "REJECT": 0,
        "WATCH": 1,
        "ARMED": 2,
        "ENTRY": 3,
    }.get(state, 0)


def emit_watch_updates(results):
    """
    Notify only on meaningful WATCH/ARMED upgrades.
    ENTRY is left to the normal signal alert path.
    """
    state_book = load_json(MARKET_STATE_FILE, {})
    now = time.time()

    candidates = [
        result
        for result in results
        if result.get("signal_state") in {"WATCH", "ARMED"}
    ]
    candidates.sort(key=lambda item: num(item.get("best_score")), reverse=True)

    notified = 0
    for result in candidates[:3]:
        key = f"{result['symbol']}_{result['direction']}"
        previous = state_book.get(key, {})
        previous_state = previous.get("state", "IGNORE")
        previous_score = num(previous.get("score"))
        previous_time = num(previous.get("time"))

        state_upgraded = (
            _state_rank(result["signal_state"]) > _state_rank(previous_state)
        )
        improved = (
            result["best_score"] >= previous_score + WATCH_SCORE_IMPROVEMENT
            and now - previous_time >= WATCH_ALERT_COOLDOWN
        )

        if state_upgraded or improved:
            reasons = result.get("reject_reasons") or []
            next_text = reasons[0] if reasons else "waiting for threshold confirmation"
            message = (
                f"👀 {result['signal_state']} — FuturesHunter V6.6\n\n"
                f"{result['symbol']} {result['direction']}\n"
                f"Regime: {result['regime']}\n"
                f"Adaptive score: {result['best_score']:.1f}/100\n"
                f"ENTRY threshold: {result['entry_threshold']}\n"
                f"OI: {result['oi_score']}/15\n"
                f"Next: {next_text}\n\n"
                f"Use /why {result['symbol'].replace('_USDT', '')} for diagnostics."
            )
            send_telegram(message)
            notified += 1

        state_book[key] = {
            "state": result["signal_state"],
            "score": result["best_score"],
            "time": now,
        }

    # Update the rest silently too. This lets a later re-upgrade from
    # REJECT/IGNORE/WATCH become visible instead of being compared with a stale state.
    candidate_keys = {
        f"{result['symbol']}_{result['direction']}"
        for result in candidates[:3]
    }
    for result in results:
        key = f"{result['symbol']}_{result['direction']}"
        if key in candidate_keys:
            continue
        state_book[key] = {
            "state": result.get("signal_state"),
            "score": result.get("best_score"),
            "time": now,
        }

    save_json(MARKET_STATE_FILE, state_book)
    return notified


def build_alert_message(result):
    plan = result["risk_plan"]
    oi = result["oi_metrics"]

    side_icon = "🟢" if result["direction"] == "LONG" else "🔴"
    factor_values = list(result.get("parts", {}).values())
    supportive = sum(1 for value in factor_values if value > 0)
    strong = sum(1 for value in factor_values if value == 2)

    risk_cash = ACCOUNT_SIZE * (RISK_PCT / 100.0)
    notional = 0.0
    if ACCOUNT_SIZE > 0 and plan["stop_pct"] > 0:
        notional = risk_cash / (plan["stop_pct"] / 100.0)

    sizing_text = ""
    if ACCOUNT_SIZE > 0:
        sizing_text = (
            f"Risk reference ({RISK_PCT:.2f}% of ${ACCOUNT_SIZE:.0f}): "
            f"${risk_cash:.2f} max planned risk\n"
            f"Indicative notional from stop: ${notional:.2f}\n\n"
        )

    return (
        "🚨 FUTURES HUNTER V6.6 — ENTRY\n\n"
        f"{side_icon} {result['symbol']} {result['direction']}\n"
        f"Regime: {result['regime']}\n\n"
        f"Adaptive score: {result['best_score']:.1f}/100\n"
        f"Raw 50-factor score: {result['raw_score']}/100\n"
        f"Factors aligned: {supportive}/50 ({strong} strong)\n"
        f"OI confirmation: {result['oi_score']}/15\n\n"
        f"Paper entry: {result['price']}\n"
        f"Entry zone: {plan['entry_low']:.8f} → {plan['entry_high']:.8f}\n"
        f"Stop: {plan['stop']:.8f} ({plan['stop_pct']:.2f}%)\n"
        f"TP1: {plan['tp1']:.8f} (1.5R)\n"
        f"TP2: {plan['tp2']:.8f} (2R)\n"
        f"TP3: {plan['tp3']:.8f} (3R)\n\n"
        f"15m RSI: {result['15m']['rsi']:.1f}\n"
        f"1h ADX: {result['1h']['adx']:.1f}\n"
        f"15m RV: {result['15m']['rv']:.2f}x\n"
        f"OI 5m: {format_optional(oi.get('oi5'))}\n"
        f"OI 15m: {format_optional(oi.get('oi15'))}\n"
        f"Funding: {result['funding'] * 100:.4f}%\n"
        f"Spread: {result['spread']:.4f}%\n\n"
        f"{sizing_text}"
        "⚠️ Paper-trading signal. Validate V6.6 before live use."
    )


def run_full_scan(symbols, details, oi_metrics):
    print()
    print("=" * 90)
    print(f"V6.6 ADAPTIVE FULL SCAN — {local_time()}")
    print("=" * 90)

    results = []

    for symbol in symbols:
        ticker = details.get(symbol)
        if ticker is None:
            continue

        try:
            result = analyse_market(
                symbol,
                ticker,
                oi_metrics.get(symbol, {}),
            )
            if result is not None:
                results.append(result)
                # V6.50F defined this logger but never called it.
                log_factor_signal(result)
        except Exception as error:
            print(f"ERROR {symbol}: {error}")

    if not results:
        print("No markets analysed.")
        return None

    save_scan_snapshot(results)
    record_shadow_candidates(results)
    emit_watch_updates(results)

    results.sort(
        key=lambda item: (
            _state_rank(item.get("signal_state")),
            num(item.get("best_score")),
        ),
        reverse=True,
    )

    print()
    print("TOP 8 CURRENT SETUPS")
    print("-" * 90)

    for result in results[:8]:
        print(
            f"{result['symbol']:<18}"
            f"{result['direction']:<7}"
            f"{result['signal_state']:<8}"
            f"{result['regime']:<20}"
            f"Score {result['best_score']:>5.1f} "
            f"Raw {result['raw_score']:>3} "
            f"OI {result['oi_score']:>2}/15"
        )

        if result["signal_state"] == "REJECT":
            print(
                "   ↳ "
                + "; ".join(result.get("hard_reject_reasons", [])[:2])
            )

    qualified = [
        result
        for result in results
        if result.get("signal_state") == "ENTRY"
    ]

    if not qualified:
        active = [
            result
            for result in results
            if result.get("signal_state") in {"WATCH", "ARMED"}
        ]
        print()
        if active:
            lead = active[0]
            print(
                f"Decision: NO ENTRY — best developing setup "
                f"{lead['symbol']} {lead['signal_state']} "
                f"{lead['best_score']:.1f}/100"
            )
        else:
            print("Decision: NO ENTRY")
        return None

    best = qualified[0]
    print()
    print(
        f"Best ENTRY: {best['symbol']} {best['direction']} "
        f"{best['best_score']:.1f}/100 ({best['regime']})"
    )
    return best


async def main():
    print()
    print("=" * 90)
    print("MEXC FUTURES HUNTER V6.7 — MACRO + ADAPTIVE 50 FACTORS")
    print("=" * 90)

    print()
    print(
        "Telegram: "
        + ("CONNECTED" if telegram_ready() else "NOT CONFIGURED")
    )

    if not telegram_ready():
        print()
        print("Check your .env file.")
        return

    start_telegram_command_thread()
    print("Telegram subscriber command listener: ACTIVE")

    history = load_json(HISTORY_FILE, {})
    alert_state = load_json(ALERT_STATE_FILE, {})
    trades = load_json(TRADES_FILE, [])

    imported = import_recent_logged_signals(trades)
    if imported:
        print()
        print(
            f"Imported {imported} recent paper signal(s) into tracking."
        )

    print()
    print("OI snapshots: every 60 seconds")
    print(f"Adaptive full scan: every {FULL_SCAN_INTERVAL // 60} minutes")
    print("States: WATCH → ARMED → ENTRY")
    print(f"Focus symbols: {', '.join(sorted(FOCUS_SYMBOLS)) or 'none'}")
    print("Shadow rejected-trade research: ACTIVE")
    print(f"Paper trade expiry: {TRADE_EXPIRY_HOURS} hours")
    print("Press Ctrl+C to stop.")

    send_telegram(
        "✅ FuturesHunter V6.7 MacroHunter is online.\n\n"
        "Weighted 50-factor scoring + breakout mode + "
        "WATCH/ARMED/ENTRY + macro/crypto-news risk layer + shadow tracking active.\n\n"
        "Try /macro, /news, /calendar, /watch or /why ZEC."
    )

    next_full_scan = 0

    while True:
        cycle_start = time.time()

        try:
            print()
            print(f"[{local_time()}] Updating market + OI data...")

            symbols = await get_top_symbols()
            details = await get_detailed_tickers(symbols)
            oi_metrics = update_oi_history(history, details)

            # Track both actual paper signals and rejected/developing shadow trades.
            track_open_trades(trades)
            settled_now = _v681_settle_risk_challenger(trades)
            if settled_now:
                print(f"V6.8.1 Risk Lab: settled {settled_now} challenger outcome(s).")
            track_shadow_trades()

            now = time.time()

            if now >= next_full_scan:
                best = run_full_scan(
                    symbols,
                    details,
                    oi_metrics,
                )

                next_full_scan = now + FULL_SCAN_INTERVAL

                if best is not None:
                    if should_send_alert(
                        best,
                        alert_state,
                        trades,
                    ):
                        message = build_alert_message(best)
                        sent = send_telegram(message)

                        if sent:
                            print()
                            print("✅ Telegram ENTRY alert broadcast.")
                            save_latest_signal(message, best)
                            log_signal(best)
                            create_paper_trade(best, trades)
                            record_alert_state(best, alert_state)
                        else:
                            print()
                            print("⚠️ Telegram alert failed.")
                    else:
                        print()
                        print(
                            "ENTRY setup found, but duplicate/open-trade "
                            "alert suppressed."
                        )

                print_stats(trades)

        except Exception as error:
            print()
            print(f"MAIN LOOP ERROR: {error}")

        elapsed = time.time() - cycle_start
        sleep_for = max(5, OI_UPDATE_INTERVAL - elapsed)
        await asyncio.sleep(sleep_for)




# ============================================================
# V6.7 MACRO + CRYPTO NEWS INTELLIGENCE
# ============================================================
#
# Purpose:
# - monitor macro / central-bank / economic-release headlines
# - monitor crypto-specific headlines and exchange incidents
# - maintain a lightweight high-impact economic calendar
# - classify headlines with transparent deterministic rules
# - use macro as a RISK / CONFIRMATION layer, never as a standalone entry trigger
# - expose /macro /news /calendar /risk /sources in Telegram
#
# This is intentionally lightweight so it can run on Render Free without
# a paid LLM/API.  Failed news sources fail open and never stop the scanner.

V67_VERSION = "6.7"
NEWS_REFRESH_SECONDS = int(os.getenv("NEWS_REFRESH_SECONDS", "180"))
NEWS_LOOKBACK_HOURS = float(os.getenv("NEWS_LOOKBACK_HOURS", "8"))
NEWS_MAX_ITEMS = int(os.getenv("NEWS_MAX_ITEMS", "80"))
NEWS_ALERT_MIN_IMPACT = int(os.getenv("NEWS_ALERT_MIN_IMPACT", "3"))
NEWS_ALERT_COOLDOWN = int(os.getenv("NEWS_ALERT_COOLDOWN", "600"))
CALENDAR_URL = os.getenv(
    "ECON_CALENDAR_URL",
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
)

MACRO_NEWS_FILE = BASE_DIR / "macro_news_v67.json"
MACRO_RUNTIME_FILE = BASE_DIR / "macro_runtime_v67.json"
MACRO_LOCK = threading.RLock()
NEWS_SESSION = requests.Session()
NEWS_SESSION.headers.update({
    "User-Agent": (
        "FuturesHunter/6.7 (+market-risk-monitor; "
        "contact=local-render-service)"
    )
})

# Official feeds first; crypto media feeds are supplemental.
V67_RSS_SOURCES = [
    {
        "name": "Federal Reserve Monetary Policy",
        "url": "https://www.federalreserve.gov/feeds/press_monetary.xml",
        "kind": "macro",
        "priority": 1.30,
    },
    {
        "name": "Federal Reserve Speeches",
        "url": "https://www.federalreserve.gov/feeds/speeches.xml",
        "kind": "macro",
        "priority": 1.10,
    },
    {
        "name": "US BLS",
        "url": "https://www.bls.gov/feed/bls_latest.rss",
        "kind": "macro",
        "priority": 1.25,
    },
    {
        "name": "ECB",
        "url": "https://www.ecb.europa.eu/rss/press.html",
        "kind": "macro",
        "priority": 1.25,
    },
    {
        "name": "Bank of England",
        "url": "https://www.bankofengland.co.uk/rss/news",
        "kind": "macro",
        "priority": 1.20,
    },
    {
        "name": "Coinbase Blog",
        "url": "https://www.coinbase.com/blog/rss.xml",
        "kind": "crypto",
        "priority": 1.10,
    },
    {
        "name": "Coinbase Status",
        "url": "https://status.coinbase.com/history.rss",
        "kind": "crypto",
        "priority": 1.25,
    },
    {
        "name": "CoinDesk",
        "url": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "kind": "crypto",
        "priority": 0.95,
    },
    {
        "name": "Cointelegraph",
        "url": "https://cointelegraph.com/rss",
        "kind": "crypto",
        "priority": 0.90,
    },
    {
        "name": "Decrypt",
        "url": "https://decrypt.co/feed",
        "kind": "crypto",
        "priority": 0.90,
    },
]

# Additional RSS feeds can be supplied from Render as comma-separated URLs.
for _extra_url in os.getenv("NEWS_SOURCES_EXTRA", "").split(","):
    _extra_url = _extra_url.strip()
    if _extra_url:
        V67_RSS_SOURCES.append({
            "name": urlparse(_extra_url).netloc or "Extra source",
            "url": _extra_url,
            "kind": "crypto",
            "priority": 0.80,
        })

# GDELT is used as a free broad-news discovery layer, including coverage that
# mentions CoinGecko, Cryptonary, Coinbase, BoJ and other sources/entities.
V67_GDELT_QUERIES = [
    {
        "name": "GDELT Macro",
        "kind": "macro",
        "priority": 0.85,
        "query": (
            '("Federal Reserve" OR FOMC OR "Bank of Japan" OR BOJ OR ECB OR '
            '"Bank of England" OR CPI OR inflation OR payrolls OR PCE OR '
            'tariffs OR sanctions OR "bond yields" OR recession)'
        ),
    },
    {
        "name": "GDELT Crypto",
        "kind": "crypto",
        "priority": 0.82,
        "query": (
            '(bitcoin OR ethereum OR cryptocurrency OR crypto OR stablecoin OR '
            'Coinbase OR CoinGecko OR Cryptonary OR Binance OR MEXC OR '
            '"spot ETF" OR hack OR exploit OR liquidation)'
        ),
    },
    {
        "name": "GDELT CoinGecko/Cryptonary/Coinbase",
        "kind": "crypto",
        "priority": 0.88,
        "query": (
            '(domain:coingecko.com OR domain:cryptonary.com OR domain:coinbase.com)'
        ),
    },
]

# Deterministic headline language. Positive = broadly risk-on / crypto-positive;
# negative = broadly risk-off / crypto-negative. Macro is never allowed to
# create a technical entry on its own.
V67_MACRO_PHRASES = {
    "rate cut": 12,
    "cuts rates": 12,
    "cut rates": 10,
    "dovish": 9,
    "quantitative easing": 12,
    "stimulus": 8,
    "liquidity injection": 9,
    "easing": 6,
    "inflation cools": 7,
    "inflation falls": 7,
    "cpi falls": 7,
    "soft landing": 5,
    "ceasefire": 6,
    "rate hike": -12,
    "hikes rates": -12,
    "hike rates": -10,
    "hawkish": -9,
    "higher for longer": -10,
    "quantitative tightening": -8,
    "inflation accelerates": -8,
    "inflation rises": -7,
    "hotter than expected": -7,
    "tariff": -5,
    "sanctions": -6,
    "war": -7,
    "attack": -7,
    "missile": -7,
    "recession": -6,
    "bank failure": -10,
    "banking crisis": -11,
    "emergency meeting": -8,
}

V67_CRYPTO_PHRASES = {
    "etf approved": 13,
    "etf approval": 13,
    "spot etf approval": 14,
    "record inflows": 9,
    "institutional adoption": 8,
    "strategic reserve": 9,
    "crypto reserve": 9,
    "buys bitcoin": 7,
    "bitcoin purchase": 6,
    "network upgrade": 4,
    "partnership": 3,
    "listing": 3,
    "hack": -14,
    "hacked": -14,
    "exploit": -13,
    "security breach": -13,
    "outage": -9,
    "withdrawals halted": -12,
    "withdrawal suspended": -12,
    "insolvency": -15,
    "bankruptcy": -14,
    "liquidation cascade": -10,
    "mass liquidations": -9,
    "delisting": -7,
    "delist": -6,
    "ban crypto": -12,
    "crypto ban": -12,
    "lawsuit": -7,
    "enforcement action": -8,
    "sec charges": -8,
    "fraud": -9,
    "rug pull": -12,
}

V67_HIGH_IMPACT_TERMS = (
    "fomc", "federal reserve", "fed chair", "powell", "interest rate",
    "rate decision", "bank of japan", "boj", "ecb", "bank of england",
    "cpi", "consumer price index", "pce", "payroll", "nonfarm",
    "unemployment", "gdp", "inflation", "emergency", "war", "attack",
    "hack", "exploit", "security breach", "outage", "etf approved",
    "etf approval", "insolvency", "bankruptcy", "sanctions",
)

V67_CENTRAL_BANK_TERMS = (
    "federal reserve", "fomc", "fed chair", "powell",
    "bank of japan", "boj", "ecb", "european central bank",
    "bank of england", "boe", "interest rate", "policy rate",
)

V67_RELEVANT_CALENDAR_CURRENCIES = {
    item.strip().upper()
    for item in os.getenv("MACRO_CURRENCIES", "USD,JPY,EUR,GBP,CNY").split(",")
    if item.strip()
}

# In-memory snapshot.  The news thread mutates it under MACRO_LOCK.
MACRO_STATE = {
    "updated_at": 0.0,
    "macro_score": 0.0,
    "crypto_score": 0.0,
    "combined_score": 0.0,
    "regime": "NEUTRAL",
    "confidence": 0,
    "event_risk": "LOW",
    "headlines": [],
    "upcoming_events": [],
    "source_status": {},
    "market_reaction": 0.0,
    "market_reaction_text": "WAITING FOR MARKET DATA",
    "last_error": None,
}


def _strip_html(value):
    value = html_lib.unescape(str(value or ""))
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _parse_news_time(value):
    if not value:
        return time.time()

    text = str(value).strip()
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        pass

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return time.time()


def _entry_child_text(node, local_names):
    wanted = {name.lower() for name in local_names}
    for child in list(node):
        local = child.tag.split("}")[-1].lower()
        if local in wanted and child.text:
            return child.text.strip()
    return ""


def _entry_link(node):
    for child in list(node):
        local = child.tag.split("}")[-1].lower()
        if local != "link":
            continue
        href = child.attrib.get("href")
        if href:
            return href.strip()
        if child.text:
            return child.text.strip()
    return ""


def _fetch_rss_source(source):
    response = NEWS_SESSION.get(source["url"], timeout=14)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    items = []

    for node in root.iter():
        local = node.tag.split("}")[-1].lower()
        if local not in {"item", "entry"}:
            continue

        title = _strip_html(_entry_child_text(node, {"title"}))
        if not title:
            continue

        summary = _strip_html(
            _entry_child_text(node, {"description", "summary", "content"})
        )
        published = _entry_child_text(
            node,
            {"pubdate", "published", "updated", "date"},
        )
        link = _entry_link(node)

        items.append({
            "title": title,
            "summary": summary[:700],
            "link": link,
            "source": source["name"],
            "kind": source["kind"],
            "priority": source["priority"],
            "published_ts": _parse_news_time(published),
        })

        if len(items) >= 12:
            break

    return items


def _fetch_gdelt_source(source):
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": source["query"],
        "mode": "ArtList",
        "maxrecords": 35,
        "format": "json",
        "sort": "HybridRel",
        "formatdatetime": "true",
    }
    response = NEWS_SESSION.get(url, params=params, timeout=18)
    response.raise_for_status()
    payload = response.json()
    articles = payload.get("articles", []) if isinstance(payload, dict) else []

    items = []
    for article in articles[:35]:
        if not isinstance(article, dict):
            continue
        title = _strip_html(article.get("title"))
        if not title:
            continue
        domain = article.get("domain") or urlparse(article.get("url", "")).netloc
        source_name = domain or source["name"]
        items.append({
            "title": title,
            "summary": "",
            "link": article.get("url", ""),
            "source": source_name,
            "source_group": source["name"],
            "kind": source["kind"],
            "priority": source["priority"],
            "published_ts": _parse_news_time(
                article.get("seendate") or article.get("date")
            ),
        })
    return items


def _headline_key(item):
    raw = f"{item.get('title','')}|{item.get('link','')}".lower().encode(
        "utf-8", errors="ignore"
    )
    return hashlib.sha1(raw).hexdigest()[:20]


def _classify_headline(item):
    text = (
        f"{item.get('title','')} {item.get('summary','')} "
        f"{item.get('source','')}"
    ).lower()

    macro_raw = 0
    crypto_raw = 0
    matched = []

    for phrase, value in V67_MACRO_PHRASES.items():
        if phrase in text:
            macro_raw += value
            matched.append(phrase)

    for phrase, value in V67_CRYPTO_PHRASES.items():
        if phrase in text:
            crypto_raw += value
            matched.append(phrase)

    # Source/topic context gets influence but no forced directional opinion.
    is_central_bank = any(term in text for term in V67_CENTRAL_BANK_TERMS)
    is_high_impact = any(term in text for term in V67_HIGH_IMPACT_TERMS)

    impact = 1
    max_abs = max(abs(macro_raw), abs(crypto_raw))
    if max_abs >= 12 or (is_central_bank and is_high_impact):
        impact = 3
    elif max_abs >= 6 or is_high_impact:
        impact = 2

    if "unexpected" in text or "surprise" in text or "emergency" in text:
        impact = 3

    # Coinbase status incidents deserve extra attention even if the title is terse.
    if "coinbase status" in item.get("source", "").lower():
        if any(word in text for word in ("incident", "degraded", "outage", "delayed")):
            crypto_raw -= 8
            impact = max(impact, 3)

    return {
        "macro_raw": macro_raw,
        "crypto_raw": crypto_raw,
        "impact": impact,
        "matched": matched[:8],
        "central_bank": is_central_bank,
    }


def _dedupe_news(items):
    best = {}
    for item in items:
        key = _headline_key(item)
        item["key"] = key
        previous = best.get(key)
        if previous is None or num(item.get("priority")) > num(previous.get("priority")):
            best[key] = item
    return list(best.values())


def _fetch_calendar():
    try:
        response = NEWS_SESSION.get(CALENDAR_URL, timeout=14)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            return [], "unexpected calendar format"

        events = []
        now_ts = time.time()
        for event in payload:
            if not isinstance(event, dict):
                continue

            currency = str(
                event.get("country")
                or event.get("currency")
                or ""
            ).upper().strip()
            impact = str(event.get("impact") or "").strip().lower()
            title = str(event.get("title") or event.get("event") or "").strip()
            if not title:
                continue
            if currency and currency not in V67_RELEVANT_CALENDAR_CURRENCIES:
                continue
            if impact not in {"high", "medium"}:
                continue

            event_ts = _parse_news_time(event.get("date") or event.get("datetime"))
            minutes = (event_ts - now_ts) / 60.0
            if -45 <= minutes <= 24 * 60:
                events.append({
                    "title": title,
                    "currency": currency or "GLOBAL",
                    "impact": impact.upper(),
                    "event_ts": event_ts,
                    "minutes": minutes,
                    "forecast": event.get("forecast"),
                    "previous": event.get("previous"),
                })

        events.sort(key=lambda item: item["event_ts"])
        return events[:20], None
    except Exception as error:
        return [], str(error)


def _age_decay(age_hours):
    # News loses most influence after a few hours; official event headlines still
    # remain visible in /news even when their score contribution has decayed.
    if age_hours <= 0.5:
        return 1.0
    if age_hours <= 2:
        return 0.80
    if age_hours <= 4:
        return 0.55
    if age_hours <= 8:
        return 0.30
    return 0.10


def _derive_macro_state(items, upcoming_events):
    now_ts = time.time()
    macro_total = 0.0
    crypto_total = 0.0
    evidence = 0.0
    recent_count = 0

    for item in items:
        age_hours = max(0.0, (now_ts - num(item.get("published_ts"))) / 3600.0)
        if age_hours > NEWS_LOOKBACK_HOURS:
            continue
        classification = item.get("classification") or {}
        decay = _age_decay(age_hours)
        priority = max(0.5, num(item.get("priority")) or 1.0)
        impact_weight = 0.65 + 0.25 * num(classification.get("impact", 1))
        weight = decay * priority * impact_weight
        macro_total += num(classification.get("macro_raw")) * weight
        crypto_total += num(classification.get("crypto_raw")) * weight
        evidence += abs(num(classification.get("macro_raw"))) * weight
        evidence += abs(num(classification.get("crypto_raw"))) * weight
        if classification.get("impact", 1) >= 2:
            recent_count += 1

    macro_score = max(-100.0, min(100.0, macro_total * 1.8))
    crypto_score = max(-100.0, min(100.0, crypto_total * 1.8))
    combined = max(-100.0, min(100.0, macro_score * 0.60 + crypto_score * 0.40))

    if combined >= 30:
        regime = "RISK_ON"
    elif combined <= -30:
        regime = "RISK_OFF"
    elif macro_score * crypto_score < 0 and abs(macro_score) >= 20 and abs(crypto_score) >= 20:
        regime = "MIXED"
    else:
        regime = "NEUTRAL"

    event_risk = "LOW"
    for event in upcoming_events:
        minutes = num(event.get("minutes"))
        impact = str(event.get("impact", "")).upper()
        if impact == "HIGH" and -15 <= minutes <= 15:
            event_risk = "EXTREME"
            break
        if impact == "HIGH" and -30 <= minutes <= 75:
            event_risk = "HIGH"
        elif event_risk == "LOW" and impact == "MEDIUM" and -15 <= minutes <= 45:
            event_risk = "MEDIUM"

    confidence = min(95, int(25 + min(50, evidence) + min(20, recent_count * 3)))
    if not items:
        confidence = 10

    return {
        "macro_score": round(macro_score, 1),
        "crypto_score": round(crypto_score, 1),
        "combined_score": round(combined, 1),
        "regime": regime,
        "event_risk": event_risk,
        "confidence": confidence,
    }


def _format_age(timestamp):
    seconds = max(0, int(time.time() - num(timestamp)))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    return f"{hours}h"


def _news_bias_label(item):
    cls = item.get("classification") or {}
    macro = num(cls.get("macro_raw"))
    crypto = num(cls.get("crypto_raw"))
    score = macro + crypto
    if score >= 7:
        return "🟢 +"
    if score <= -7:
        return "🔴 -"
    return "⚪"


def get_macro_snapshot():
    with MACRO_LOCK:
        return json.loads(json.dumps(_clean_json_value(MACRO_STATE)))


def _update_market_confirmation(oi_metrics):
    values = []
    labels = []
    for symbol in ("BTC_USDT", "ETH_USDT"):
        metric = oi_metrics.get(symbol, {}) if isinstance(oi_metrics, dict) else {}
        p5 = metric.get("price5")
        p15 = metric.get("price15")
        if p5 is not None:
            values.append(max(-3.0, min(3.0, num(p5))))
            labels.append(f"{symbol.split('_')[0]} 5m {num(p5):+.2f}%")
        elif p15 is not None:
            values.append(max(-3.0, min(3.0, num(p15) * 0.6)))
            labels.append(f"{symbol.split('_')[0]} 15m {num(p15):+.2f}%")

    reaction = sum(values) / len(values) if values else 0.0
    text = ", ".join(labels) if labels else "warming up OI/price history"

    with MACRO_LOCK:
        MACRO_STATE["market_reaction"] = round(reaction, 3)
        MACRO_STATE["market_reaction_text"] = text


def _macro_gate(result):
    state = get_macro_snapshot()
    direction = result.get("direction")
    event_risk = state.get("event_risk", "LOW")
    combined = num(state.get("combined_score"))
    crypto = num(state.get("crypto_score"))
    reaction = num(state.get("market_reaction"))

    reasons = []
    action = "ALLOW"

    # Scheduled event risk is the strongest gate because price discovery can be
    # discontinuous around CPI/rate decisions.  Macro never creates an entry.
    if event_risk == "EXTREME":
        action = "BLOCK"
        reasons.append("high-impact macro event inside the ±15m danger window")
    elif event_risk == "HIGH":
        action = "CAUTION"
        reasons.append("high-impact macro event is close")

    # Strong headline conflict only blocks when BTC/ETH price reaction broadly
    # confirms the conflict.  This avoids blindly trading headline semantics.
    long_conflict = direction == "LONG" and (combined <= -42 or crypto <= -50)
    short_conflict = direction == "SHORT" and (combined >= 42 or crypto >= 50)
    market_confirms_long_conflict = reaction <= -0.20
    market_confirms_short_conflict = reaction >= 0.20

    if long_conflict and market_confirms_long_conflict:
        action = "BLOCK"
        reasons.append("risk-off news and BTC/ETH reaction conflict with LONG")
    elif short_conflict and market_confirms_short_conflict:
        action = "BLOCK"
        reasons.append("risk-on news and BTC/ETH reaction conflict with SHORT")
    elif long_conflict or short_conflict:
        if action != "BLOCK":
            action = "CAUTION"
        reasons.append("headline regime conflicts, but market reaction is not confirming")

    return {
        "action": action,
        "reasons": reasons,
        "snapshot": state,
    }


def _build_macro_message():
    state = get_macro_snapshot()
    score = num(state.get("combined_score"))
    arrow = "🟢" if score >= 30 else "🔴" if score <= -30 else "⚪"
    events = state.get("upcoming_events", [])
    next_event = None
    for event in events:
        if num(event.get("minutes")) >= -15:
            next_event = event
            break

    next_text = "No high/medium event found in the next 24h"
    if next_event:
        mins = num(next_event.get("minutes"))
        if mins >= 0:
            when = f"in {int(mins)}m"
        else:
            when = f"{abs(int(mins))}m ago / cooling window"
        next_text = (
            f"{next_event.get('currency')} {next_event.get('title')} "
            f"({next_event.get('impact')}) — {when}"
        )

    return (
        "🌍 FUTURESHUNTER V6.7 MACRO\n\n"
        f"{arrow} Regime: {state.get('regime')}\n"
        f"Combined macro/crypto bias: {score:+.1f}/100\n"
        f"Macro score: {num(state.get('macro_score')):+.1f}\n"
        f"Crypto-news score: {num(state.get('crypto_score')):+.1f}\n"
        f"Confidence: {int(num(state.get('confidence')))}%\n"
        f"Event risk: {state.get('event_risk')}\n"
        f"BTC/ETH reaction: {state.get('market_reaction_text')}\n\n"
        f"Next event: {next_text}\n\n"
        "Macro can delay/block an entry; it never creates one by itself."
    )


def _build_news_message(limit=8):
    state = get_macro_snapshot()
    headlines = state.get("headlines", [])[:limit]
    if not headlines:
        return "📰 No macro/crypto headlines cached yet. The news engine may still be warming up."

    lines = ["📰 FUTURESHUNTER NEWS", ""]
    for item in headlines:
        lines.append(
            f"{_news_bias_label(item)} [{item.get('source')}] "
            f"{item.get('title')} ({_format_age(item.get('published_ts'))})"
        )
    lines.extend([
        "",
        "Sources include official central-bank/BLS feeds, Coinbase, crypto media, and GDELT discovery.",
    ])
    return "\n".join(lines)


def _build_calendar_message(limit=8):
    state = get_macro_snapshot()
    events = state.get("upcoming_events", [])[:limit]
    if not events:
        return "📅 No relevant high/medium-impact events found in the cached weekly calendar."

    lines = [f"📅 MACRO CALENDAR — risk {state.get('event_risk')}", ""]
    for event in events:
        mins = num(event.get("minutes"))
        if mins >= 0:
            timing = f"in {int(mins)}m"
        else:
            timing = f"{abs(int(mins))}m ago"
        lines.append(
            f"• {event.get('currency')} | {event.get('impact')} | "
            f"{event.get('title')} — {timing}"
        )
    return "\n".join(lines)


def _build_risk_message():
    state = get_macro_snapshot()
    return (
        "🛡️ FUTURESHUNTER RISK LAYER\n\n"
        f"Macro regime: {state.get('regime')}\n"
        f"Event risk: {state.get('event_risk')}\n"
        f"Combined score: {num(state.get('combined_score')):+.1f}/100\n"
        f"Market confirmation: {state.get('market_reaction_text')}\n\n"
        "EXTREME event risk blocks new entries. Strong headline conflict only blocks when "
        "BTC/ETH price reaction confirms the conflict."
    )


def _build_sources_message():
    state = get_macro_snapshot()
    statuses = state.get("source_status", {})
    if not statuses:
        return "📡 News sources have not completed the first refresh yet."
    lines = ["📡 NEWS SOURCE STATUS", ""]
    for name, info in sorted(statuses.items()):
        ok = bool(info.get("ok"))
        icon = "✅" if ok else "⚠️"
        count = int(num(info.get("count")))
        suffix = f"{count} items" if ok else str(info.get("error", "failed"))[:80]
        lines.append(f"{icon} {name}: {suffix}")
    return "\n".join(lines[:20])


def _build_news_alert(item):
    cls = item.get("classification") or {}
    bias = _news_bias_label(item)
    tags = ", ".join(cls.get("matched", [])[:4]) or "high-impact topic"
    return (
        "⚡ FUTURESHUNTER NEWS SHOCK\n\n"
        f"{bias} {item.get('title')}\n"
        f"Source: {item.get('source')}\n"
        f"Impact: {int(num(cls.get('impact', 1)))}/3\n"
        f"Macro impulse: {num(cls.get('macro_raw')):+.0f}\n"
        f"Crypto impulse: {num(cls.get('crypto_raw')):+.0f}\n"
        f"Matched: {tags}\n\n"
        "No trade is created from news alone; technical + price/OI confirmation still required."
    )


def _send_calendar_warnings(events, runtime):
    now_ts = time.time()
    sent = runtime.setdefault("calendar_alerts", {})
    changed = False

    for event in events:
        if str(event.get("impact", "")).upper() != "HIGH":
            continue
        minutes = num(event.get("minutes"))
        if not (0 <= minutes <= 60):
            continue
        bucket = "15" if minutes <= 15 else "60"
        key = hashlib.sha1(
            f"{event.get('event_ts')}|{event.get('title')}|{bucket}".encode("utf-8")
        ).hexdigest()[:18]
        if sent.get(key):
            continue
        message = (
            "⚠️ MACRO EVENT WARNING\n\n"
            f"{event.get('currency')} {event.get('title')}\n"
            f"Impact: {event.get('impact')}\n"
            f"Expected in ~{max(0, int(minutes))} minutes\n\n"
            + (
                "New FuturesHunter entries are gated inside the ±15m danger window."
                if minutes <= 15
                else "FuturesHunter is increasing event-risk caution."
            )
        )
        send_telegram(message)
        sent[key] = now_ts
        changed = True

    # Trim old alert ids.
    cutoff = now_ts - 7 * 24 * 3600
    runtime["calendar_alerts"] = {
        key: ts for key, ts in sent.items() if num(ts) >= cutoff
    }
    return changed


def refresh_macro_news(initial=False):
    all_items = []
    source_status = {}

    for source in V67_RSS_SOURCES:
        try:
            items = _fetch_rss_source(source)
            all_items.extend(items)
            source_status[source["name"]] = {"ok": True, "count": len(items)}
        except Exception as error:
            source_status[source["name"]] = {
                "ok": False,
                "count": 0,
                "error": str(error)[:160],
            }

    for source in V67_GDELT_QUERIES:
        try:
            items = _fetch_gdelt_source(source)
            all_items.extend(items)
            source_status[source["name"]] = {"ok": True, "count": len(items)}
        except Exception as error:
            source_status[source["name"]] = {
                "ok": False,
                "count": 0,
                "error": str(error)[:160],
            }

    items = _dedupe_news(all_items)
    for item in items:
        item["classification"] = _classify_headline(item)

    items.sort(key=lambda item: num(item.get("published_ts")), reverse=True)
    cutoff = time.time() - max(NEWS_LOOKBACK_HOURS * 3600, 2 * 3600)
    items = [item for item in items if num(item.get("published_ts")) >= cutoff]
    items = items[:NEWS_MAX_ITEMS]

    events, calendar_error = _fetch_calendar()
    derived = _derive_macro_state(items, events)

    runtime = load_json(MACRO_RUNTIME_FILE, {})
    seen = set(runtime.get("seen_news", []))
    first_success = not bool(runtime.get("initialized"))
    new_high_impact = []

    for item in items:
        key = item.get("key")
        cls = item.get("classification") or {}
        if key and key not in seen and int(num(cls.get("impact"))) >= NEWS_ALERT_MIN_IMPACT:
            new_high_impact.append(item)
        if key:
            seen.add(key)

    runtime["initialized"] = True
    runtime["seen_news"] = list(seen)[-1200:]
    _send_calendar_warnings(events, runtime)
    save_json(MACRO_RUNTIME_FILE, runtime)

    with MACRO_LOCK:
        market_reaction = MACRO_STATE.get("market_reaction", 0.0)
        market_text = MACRO_STATE.get("market_reaction_text", "WAITING FOR MARKET DATA")
        MACRO_STATE.update(derived)
        MACRO_STATE["updated_at"] = time.time()
        MACRO_STATE["headlines"] = items
        MACRO_STATE["upcoming_events"] = events
        MACRO_STATE["source_status"] = source_status
        MACRO_STATE["market_reaction"] = market_reaction
        MACRO_STATE["market_reaction_text"] = market_text
        MACRO_STATE["last_error"] = calendar_error

    save_json(MACRO_NEWS_FILE, get_macro_snapshot())

    # Do not dump old headlines into Telegram on first startup.  Subsequent
    # genuinely new high-impact items can alert, capped to avoid spam.
    if not initial and not first_success:
        for item in new_high_impact[:3]:
            send_telegram(_build_news_alert(item))

    print(
        f"Macro/news refresh: {derived['regime']} "
        f"{derived['combined_score']:+.1f}, event risk {derived['event_risk']}, "
        f"{len(items)} recent headlines, {len(events)} calendar events"
    )
    return get_macro_snapshot()


def macro_news_loop():
    # Initial baseline prevents a flood of historical headlines.
    try:
        refresh_macro_news(initial=True)
    except Exception as error:
        print(f"Initial macro/news refresh error: {error}")

    while True:
        time.sleep(max(60, NEWS_REFRESH_SECONDS))
        try:
            refresh_macro_news(initial=False)
        except Exception as error:
            print(f"Macro/news refresh error: {error}")
            with MACRO_LOCK:
                MACRO_STATE["last_error"] = str(error)[:200]


def start_macro_news_thread():
    thread = threading.Thread(
        target=macro_news_loop,
        name="FuturesHunterMacroNews",
        daemon=True,
    )
    thread.start()
    return thread


# -------------------- V6.7 wrappers around V6.6 --------------------

_V66_HANDLE_TELEGRAM_COMMAND = handle_telegram_command
_V66_BUILD_ALERT_MESSAGE = build_alert_message
_V66_RUN_FULL_SCAN = run_full_scan
_V66_BUILD_WHY_MESSAGE = build_why_message
_V66_BUILD_WELCOME_MESSAGE = build_welcome_message
_V66_TELEGRAM_STATUS_MESSAGE = telegram_status_message


def build_welcome_message():
    return (
        "🐆 Welcome to FuturesHunter V6.7 MacroHunter\n\n"
        "Adaptive 50-factor MEXC scanner + macro/crypto-news risk layer.\n"
        "News never creates a trade by itself; price, volume, OI and technicals must still confirm.\n\n"
        f"🌐 Health: {WEBSITE_URL}\n\n"
        "Commands: /latest /watch /why ZEC /macro /news /calendar /risk /sources /shadow /stats /status /website /stop"
    )


def telegram_status_message():
    base = _V66_TELEGRAM_STATUS_MESSAGE()
    if "V6.7" not in base:
        base = base.replace("V6", "V6.7", 1)
    state = get_macro_snapshot()
    return (
        base
        + "\n"
        + f"Macro: {state.get('regime')} ({num(state.get('combined_score')):+.1f})\n"
        + f"Event risk: {state.get('event_risk')}\n"
        + f"News refresh: every {max(60, NEWS_REFRESH_SECONDS) // 60} min"
    )


def build_why_message(symbol_query):
    base = _V66_BUILD_WHY_MESSAGE(symbol_query)
    state = get_macro_snapshot()
    return (
        base
        + "\n\n🌍 V6.7 MACRO LAYER\n"
        + f"Regime: {state.get('regime')} | event risk {state.get('event_risk')}\n"
        + f"Macro {num(state.get('macro_score')):+.1f} | crypto news {num(state.get('crypto_score')):+.1f}\n"
        + f"BTC/ETH: {state.get('market_reaction_text')}"
    )


def handle_telegram_command(chat_id, text):
    parts = (text or "").strip().split()
    command = parts[0].lower() if parts else ""

    if command == "/macro":
        send_to_chat(chat_id, _build_macro_message())
        return
    if command == "/news":
        send_to_chat(chat_id, _build_news_message())
        return
    if command == "/calendar":
        send_to_chat(chat_id, _build_calendar_message())
        return
    if command == "/risk":
        send_to_chat(chat_id, _build_risk_message())
        return
    if command == "/sources":
        send_to_chat(chat_id, _build_sources_message())
        return
    if command == "/refreshnews":
        # Useful for the owner; the refresh runs in this command thread and can
        # take several seconds, so it is intentionally manual rather than spammy.
        send_to_chat(chat_id, "🔄 Refreshing macro + crypto news now...")
        try:
            refresh_macro_news(initial=False)
            send_to_chat(chat_id, _build_macro_message())
        except Exception as error:
            send_to_chat(chat_id, f"News refresh failed: {error}")
        return

    return _V66_HANDLE_TELEGRAM_COMMAND(chat_id, text)


def build_alert_message(result):
    base = _V66_BUILD_ALERT_MESSAGE(result).replace(
        "FUTURES HUNTER V6.6", "FUTURES HUNTER V6.7 MACROHUNTER"
    )
    gate = result.get("macro_gate") or _macro_gate(result)
    state = gate.get("snapshot", get_macro_snapshot())
    reasons = gate.get("reasons") or []
    reason_text = "; ".join(reasons[:2]) if reasons else "no macro conflict detected"
    return (
        base
        + "\n\n🌍 MACRO / NEWS LAYER\n"
        + f"Regime: {state.get('regime')} ({num(state.get('combined_score')):+.1f}/100)\n"
        + f"Event risk: {state.get('event_risk')}\n"
        + f"Crypto news: {num(state.get('crypto_score')):+.1f}\n"
        + f"BTC/ETH reaction: {state.get('market_reaction_text')}\n"
        + f"Gate: {gate.get('action')} — {reason_text}"
    )


def run_full_scan(symbols, details, oi_metrics):
    _update_market_confirmation(oi_metrics)
    best = _V66_RUN_FULL_SCAN(symbols, details, oi_metrics)
    if best is None:
        return None

    gate = _macro_gate(best)
    best["macro_gate"] = gate

    if gate["action"] == "BLOCK":
        print()
        print(
            f"MACRO GATE: {best['symbol']} {best['direction']} ENTRY held as ARMED — "
            + "; ".join(gate.get("reasons", []))
        )
        # Important: macro/news can suppress/delay a technical entry, but never
        # promote a non-entry into an entry.
        return None

    if gate["action"] == "CAUTION":
        print(
            f"MACRO CAUTION: {best['symbol']} {best['direction']} — "
            + "; ".join(gate.get("reasons", []))
        )

    return best


# ============================================================
# V6.8 DURABLE PERSISTENCE + RESEARCH LAB
# ============================================================
#
# Goals:
# - persist scanner state outside Render's ephemeral filesystem
# - persist every ENTRY before Telegram delivery (at-least-once delivery)
# - retry unsent Telegram ENTRY alerts after transient failures/restarts
# - preserve paper-trade / alert / shadow state across restarts
# - preserve OI continuity from durable minute samples
# - farm every full-scan setup in a compact research table
# - record live market samples for later horizon-return / MFE / MAE analysis
# - detect downtime and backfill candle closes for research continuity
#
# This layer deliberately does NOT auto-change the trading rules. It gathers
# evidence so later versions can be promoted only after shadow/out-of-sample
# validation.

try:
    import psycopg
    from psycopg.types.json import Jsonb
except Exception:
    psycopg = None
    Jsonb = None

V68_DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
V68_DB_LOCK = threading.RLock()
V68_DB_CONN = None
V68_DB_READY = False
V68_DB_LAST_ERROR = ""
V68_RESEARCH_VERSION = "6.8.1"
V68_DOWNTIME_THRESHOLD_SECONDS = int(
    os.getenv("V68_DOWNTIME_THRESHOLD_SECONDS", "180")
)
V68_BACKFILL_MAX_HOURS = float(os.getenv("V68_BACKFILL_MAX_HOURS", "12"))
V68_RETRY_UNSENT_SECONDS = int(os.getenv("V68_RETRY_UNSENT_SECONDS", "300"))
V68_RESEARCH_LOOKBACK_HOURS = int(os.getenv("V68_RESEARCH_LOOKBACK_HOURS", "24"))


def _v68_json(value):
    if Jsonb is None:
        return value
    return Jsonb(_clean_json_value(value))


def _v68_db_connect():
    global V68_DB_CONN, V68_DB_LAST_ERROR

    if not V68_DATABASE_URL or psycopg is None:
        return None

    with V68_DB_LOCK:
        try:
            if V68_DB_CONN is None or getattr(V68_DB_CONN, "closed", True):
                V68_DB_CONN = psycopg.connect(
                    V68_DATABASE_URL,
                    autocommit=True,
                    connect_timeout=6,
                )
            return V68_DB_CONN
        except Exception as error:
            V68_DB_LAST_ERROR = str(error)[:240]
            V68_DB_CONN = None
            return None


def _v68_db_execute(sql, params=None, fetch=None):
    global V68_DB_CONN, V68_DB_LAST_ERROR

    params = params or ()
    conn = _v68_db_connect()
    if conn is None:
        return None

    try:
        with V68_DB_LOCK:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
        V68_DB_LAST_ERROR = ""
        return True
    except Exception as error:
        V68_DB_LAST_ERROR = str(error)[:240]
        try:
            conn.close()
        except Exception:
            pass
        V68_DB_CONN = None
        print(f"V6.8 database warning: {V68_DB_LAST_ERROR}")
        return None


def _v68_db_executemany(sql, rows):
    global V68_DB_CONN, V68_DB_LAST_ERROR

    if not rows:
        return True

    conn = _v68_db_connect()
    if conn is None:
        return None

    try:
        with V68_DB_LOCK:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        V68_DB_LAST_ERROR = ""
        return True
    except Exception as error:
        V68_DB_LAST_ERROR = str(error)[:240]
        try:
            conn.close()
        except Exception:
            pass
        V68_DB_CONN = None
        print(f"V6.8 database batch warning: {V68_DB_LAST_ERROR}")
        return None


def v68_init_database():
    global V68_DB_READY, V68_DB_LAST_ERROR

    if not V68_DATABASE_URL:
        V68_DB_READY = False
        V68_DB_LAST_ERROR = "DATABASE_URL is not configured"
        print("V6.8 persistence: DATABASE_URL missing — running in local fallback mode.")
        return False

    if psycopg is None:
        V68_DB_READY = False
        V68_DB_LAST_ERROR = "psycopg is not installed"
        print("V6.8 persistence: psycopg missing — check requirements.txt.")
        return False

    statements = [
        """
        CREATE TABLE IF NOT EXISTS fh_state (
            key TEXT PRIMARY KEY,
            value JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fh_signal_queue (
            signal_key TEXT PRIMARY KEY,
            signal_ts DOUBLE PRECISION NOT NULL,
            signal_time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            score DOUBLE PRECISION,
            message TEXT NOT NULL,
            payload JSONB NOT NULL,
            telegram_sent BOOLEAN NOT NULL DEFAULT FALSE,
            telegram_sent_at TIMESTAMPTZ,
            telegram_attempts INTEGER NOT NULL DEFAULT 0,
            side_effects_done BOOLEAN NOT NULL DEFAULT FALSE,
            last_error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_signal_queue_unsent
        ON fh_signal_queue (telegram_sent, signal_time)
        """,
        """
        CREATE TABLE IF NOT EXISTS fh_research_scans (
            id BIGSERIAL PRIMARY KEY,
            scan_ts BIGINT NOT NULL,
            scan_time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            selected_direction TEXT,
            signal_state TEXT,
            selected_regime TEXT,
            best_score DOUBLE PRECISION,
            long_score DOUBLE PRECISION,
            short_score DOUBLE PRECISION,
            long_raw SMALLINT,
            short_raw SMALLINT,
            raw_score SMALLINT,
            weighted_score DOUBLE PRECISION,
            oi_score SMALLINT,
            price DOUBLE PRECISION,
            open_interest DOUBLE PRECISION,
            funding DOUBLE PRECISION,
            spread DOUBLE PRECISION,
            turnover DOUBLE PRECISION,
            watch_threshold DOUBLE PRECISION,
            armed_threshold DOUBLE PRECISION,
            entry_threshold DOUBLE PRECISION,
            long_factors SMALLINT[],
            short_factors SMALLINT[],
            selected_factors SMALLINT[],
            long_group JSONB,
            short_group JSONB,
            metrics JSONB,
            reject_reasons TEXT[],
            hard_reject_reasons TEXT[],
            macro JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (scan_ts, symbol)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_research_time
        ON fh_research_scans (scan_time DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_research_symbol_time
        ON fh_research_scans (symbol, scan_time DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_research_state_time
        ON fh_research_scans (signal_state, scan_time DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS fh_market_samples (
            sample_ts BIGINT NOT NULL,
            sample_time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            price DOUBLE PRECISION,
            open_interest DOUBLE PRECISION,
            funding DOUBLE PRECISION,
            turnover DOUBLE PRECISION,
            source TEXT NOT NULL DEFAULT 'LIVE',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (sample_ts, symbol, source)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_market_symbol_time
        ON fh_market_samples (symbol, sample_ts DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS fh_downtime_gaps (
            id BIGSERIAL PRIMARY KEY,
            last_seen_ts DOUBLE PRECISION,
            resumed_ts DOUBLE PRECISION NOT NULL,
            gap_seconds DOUBLE PRECISION NOT NULL,
            backfill_status TEXT NOT NULL DEFAULT 'PENDING',
            note TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (resumed_ts)
        )
        """,
    ]

    for statement in statements:
        if _v68_db_execute(statement) is None:
            V68_DB_READY = False
            return False

    V68_DB_READY = True
    V68_DB_LAST_ERROR = ""
    _v68_state_set("schema_version", {"version": V68_RESEARCH_VERSION})
    print("V6.8 persistence: POSTGRES CONNECTED + RESEARCH LAB ACTIVE")
    return True


def _v68_state_set(key, value):
    if not V68_DB_READY and key != "schema_version":
        return False

    result = _v68_db_execute(
        """
        INSERT INTO fh_state (key, value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        (str(key), _v68_json(value)),
    )
    return bool(result)


def _v68_state_get(key, default=None):
    if not V68_DB_READY:
        return default

    row = _v68_db_execute(
        "SELECT value FROM fh_state WHERE key = %s",
        (str(key),),
        fetch="one",
    )
    if not row:
        return default
    return row[0]


# Mirror the important JSON state files into Postgres. OI history is handled
# separately through fh_market_samples so we don't rewrite a huge JSON blob
# every minute.
_V68_ORIGINAL_LOAD_JSON = load_json
_V68_ORIGINAL_SAVE_JSON = save_json

V68_MIRRORED_JSON_NAMES = set()
for _v68_name in (
    "ALERT_STATE_FILE",
    "TRADES_FILE",
    "MARKET_STATE_FILE",
    "SHADOW_TRADES_FILE",
    "MACRO_RUNTIME_FILE",
):
    _v68_path = globals().get(_v68_name)
    if isinstance(_v68_path, Path):
        V68_MIRRORED_JSON_NAMES.add(_v68_path.name)


def load_json(path, default):
    path_obj = Path(path)

    if path_obj.exists():
        return _V68_ORIGINAL_LOAD_JSON(path_obj, default)

    if V68_DB_READY and path_obj.name in V68_MIRRORED_JSON_NAMES:
        restored = _v68_state_get(f"file:{path_obj.name}", None)
        if restored is not None:
            return restored

    return default


def save_json(path, data):
    path_obj = Path(path)
    _V68_ORIGINAL_SAVE_JSON(path_obj, data)

    if V68_DB_READY and path_obj.name in V68_MIRRORED_JSON_NAMES:
        _v68_state_set(f"file:{path_obj.name}", data)


_V68_ORIGINAL_SAVE_SUBSCRIBERS = save_subscribers


def save_subscribers(subscribers):
    _V68_ORIGINAL_SAVE_SUBSCRIBERS(subscribers)
    if V68_DB_READY:
        _v68_state_set("telegram_subscribers", sorted(str(x) for x in subscribers))


def _v68_restore_subscribers():
    if not V68_DB_READY:
        return 0

    saved = _v68_state_get("telegram_subscribers", []) or []
    restored = 0
    with SUBSCRIBER_LOCK:
        for item in saved:
            normalized = _normalize_chat_id(item)
            if normalized and normalized not in SUBSCRIBERS:
                SUBSCRIBERS.add(normalized)
                restored += 1
    return restored


_V68_ORIGINAL_SAVE_LATEST_SIGNAL = save_latest_signal


def save_latest_signal(message, result):
    _V68_ORIGINAL_SAVE_LATEST_SIGNAL(message, result)
    if V68_DB_READY:
        _v68_state_set(
            "latest_signal",
            {
                "time": time.time(),
                "time_text": local_time(),
                "message": message,
                "symbol": result.get("symbol"),
                "direction": result.get("direction"),
                "score": result.get("best_score"),
            },
        )


def _v68_restore_latest_signal_file():
    if LATEST_SIGNAL_FILE.exists() or not V68_DB_READY:
        return
    payload = _v68_state_get("latest_signal", None)
    if payload:
        try:
            LATEST_SIGNAL_FILE.write_text(
                json.dumps(payload, indent=2),
                encoding="utf-8",
            )
        except Exception as error:
            print(f"Latest-signal restore warning: {error}")


def _v68_persist_market_samples(details, source="LIVE", timestamp=None):
    if not V68_DB_READY or not details:
        return False

    now = float(timestamp if timestamp is not None else time.time())
    sample_ts = int(now // 60) * 60
    sample_dt = datetime.fromtimestamp(sample_ts, tz=timezone.utc)
    rows = []

    for symbol, ticker in details.items():
        if not isinstance(ticker, dict):
            continue
        rows.append((
            sample_ts,
            sample_dt,
            str(symbol),
            num(ticker.get("lastPrice")),
            num(ticker.get("holdVol")) if ticker.get("holdVol") is not None else None,
            num(ticker.get("fundingRate")) if ticker.get("fundingRate") is not None else None,
            num(ticker.get("amount24")) if ticker.get("amount24") is not None else None,
            source,
        ))

    return bool(_v68_db_executemany(
        """
        INSERT INTO fh_market_samples (
            sample_ts, sample_time, symbol, price,
            open_interest, funding, turnover, source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sample_ts, symbol, source)
        DO UPDATE SET
            price = EXCLUDED.price,
            open_interest = EXCLUDED.open_interest,
            funding = EXCLUDED.funding,
            turnover = EXCLUDED.turnover
        """,
        rows,
    ))


def _v68_restore_oi_history():
    if not V68_DB_READY:
        return None

    cutoff = time.time() - HISTORY_RETENTION
    rows = _v68_db_execute(
        """
        SELECT sample_ts, symbol, open_interest, price
        FROM fh_market_samples
        WHERE sample_ts >= %s
          AND source = 'LIVE'
          AND open_interest IS NOT NULL
          AND price IS NOT NULL
        ORDER BY sample_ts ASC
        """,
        (int(cutoff),),
        fetch="all",
    )

    if rows is None:
        return None

    history = {}
    for sample_ts, symbol, open_interest, price in rows:
        history.setdefault(symbol, []).append({
            "time": float(sample_ts),
            "oi": float(open_interest),
            "price": float(price),
        })

    return history


def _v68_get_candles_range(symbol, interval, start_ts, end_ts):
    url = f"{MEXC_REST}/api/v1/contract/kline/{symbol}"
    params = {
        "interval": interval,
        "start": int(start_ts),
        "end": int(end_ts),
    }

    try:
        response = requests.get(url, params=params, timeout=12)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            return None
        data = payload.get("data") or {}
        if not data.get("time"):
            return None
        frame = pd.DataFrame({
            "time": data.get("time", []),
            "open": data.get("open", []),
            "high": data.get("high", []),
            "low": data.get("low", []),
            "close": data.get("close", []),
            "volume": data.get("vol", []),
        })
        for column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        return frame.dropna().reset_index(drop=True)
    except Exception as error:
        print(f"Backfill candle warning {symbol}: {error}")
        return None


def _v68_backfill_gap_market_samples(symbols, last_seen_ts, resumed_ts):
    if not V68_DB_READY or not last_seen_ts:
        return 0

    gap = float(resumed_ts) - float(last_seen_ts)
    if gap <= V68_DOWNTIME_THRESHOLD_SECONDS:
        return 0

    max_seconds = V68_BACKFILL_MAX_HOURS * 3600
    start_ts = max(float(last_seen_ts), float(resumed_ts) - max_seconds)
    end_ts = float(resumed_ts)
    inserted = 0

    print(
        f"V6.8 downtime backfill: {gap / 60:.1f} min gap; "
        f"rebuilding candle samples for {len(symbols)} market(s)."
    )

    for symbol in symbols:
        frame = _v68_get_candles_range(symbol, "Min1", start_ts, end_ts)
        if frame is None or frame.empty:
            continue

        rows = []
        for _, candle in frame.iterrows():
            ts = int(num(candle["time"]))
            rows.append((
                ts,
                datetime.fromtimestamp(ts, tz=timezone.utc),
                symbol,
                num(candle["close"]),
                None,
                None,
                None,
                "BACKFILL_KLINE",
            ))

        result = _v68_db_executemany(
            """
            INSERT INTO fh_market_samples (
                sample_ts, sample_time, symbol, price,
                open_interest, funding, turnover, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sample_ts, symbol, source) DO NOTHING
            """,
            rows,
        )
        if result:
            inserted += len(rows)
        time.sleep(0.11)

    _v68_db_execute(
        """
        UPDATE fh_downtime_gaps
        SET backfill_status = %s,
            note = %s
        WHERE resumed_ts = %s
        """,
        (
            "DONE" if inserted else "NO_DATA",
            f"{inserted} one-minute candle samples; OI/funding unavailable in backfill",
            float(resumed_ts),
        ),
    )
    return inserted


def _v68_record_downtime(last_seen_ts, resumed_ts):
    if not V68_DB_READY or not last_seen_ts:
        return 0.0

    gap = float(resumed_ts) - float(last_seen_ts)
    if gap <= V68_DOWNTIME_THRESHOLD_SECONDS:
        return 0.0

    _v68_db_execute(
        """
        INSERT INTO fh_downtime_gaps (
            last_seen_ts, resumed_ts, gap_seconds, backfill_status, note
        )
        VALUES (%s, %s, %s, 'PENDING', %s)
        ON CONFLICT (resumed_ts) DO NOTHING
        """,
        (
            float(last_seen_ts),
            float(resumed_ts),
            gap,
            "Research candle backfill pending; live OI/funding cannot be reconstructed exactly",
        ),
    )
    return gap


_V68_ORIGINAL_SAVE_SCAN_SNAPSHOT = save_scan_snapshot


def _v68_compact_macro_snapshot():
    state = get_macro_snapshot() or {}
    return {
        "regime": state.get("regime"),
        "combined_score": state.get("combined_score"),
        "macro_score": state.get("macro_score"),
        "crypto_score": state.get("crypto_score"),
        "confidence": state.get("confidence"),
        "event_risk": state.get("event_risk"),
        "market_reaction": state.get("market_reaction"),
        "market_reaction_text": state.get("market_reaction_text"),
        "updated_at": state.get("updated_at"),
    }


def _v68_compact_metrics(result):
    tf5 = result.get("5m", {}) or {}
    tf15 = result.get("15m", {}) or {}
    tf1h = result.get("1h", {}) or {}
    return {
        "rsi5": tf5.get("rsi"),
        "rsi15": tf15.get("rsi"),
        "rsi1h": tf1h.get("rsi"),
        "adx15": tf15.get("adx"),
        "adx1h": tf1h.get("adx"),
        "rv5": tf5.get("rv"),
        "rv15": tf15.get("rv"),
        "atr15_pctile": tf15.get("atr_pctile"),
        "roc5": tf5.get("roc"),
        "roc15": tf15.get("roc"),
        "trend5": tf5.get("trend"),
        "trend15": tf15.get("trend"),
        "trend1h": tf1h.get("trend"),
        "ema20_distance_atr": result.get("ema20_distance_atr"),
        "live_distance_atr": result.get("live_distance_atr"),
        "oi_metrics": result.get("oi_metrics", {}),
        "risk_plan": result.get("risk_plan", {}),
    }


def _v68_store_research_results(results):
    if not V68_DB_READY or not results:
        return 0

    scan_ts = int(time.time() // max(60, FULL_SCAN_INTERVAL)) * max(60, FULL_SCAN_INTERVAL)
    scan_dt = datetime.fromtimestamp(scan_ts, tz=timezone.utc)
    macro_snapshot = _v68_compact_macro_snapshot()
    rows = []
    factor_names_saved = False

    for result in results:
        try:
            tf5 = result.get("5m", {})
            tf15 = result.get("15m", {})
            tf1h = result.get("1h", {})
            funding = num(result.get("funding"))
            spread = num(result.get("spread"))
            oi_metrics = result.get("oi_metrics", {}) or {}

            long_raw, long_parts = score_direction(
                "LONG", tf5, tf15, tf1h, funding, spread, oi_metrics
            )
            short_raw, short_parts = score_direction(
                "SHORT", tf5, tf15, tf1h, funding, spread, oi_metrics
            )
            long_weighted, long_group = weighted_factor_score(long_parts)
            short_weighted, short_group = weighted_factor_score(short_parts)

            factor_names = list(long_parts.keys())
            if not factor_names_saved:
                _v68_state_set(
                    "research_factor_schema",
                    {
                        "version": "v6_50_factor_order",
                        "names": factor_names,
                    },
                )
                factor_names_saved = True

            rows.append((
                scan_ts,
                scan_dt,
                result.get("symbol"),
                result.get("direction"),
                result.get("signal_state"),
                result.get("regime"),
                num(result.get("best_score")),
                num(result.get("long_score")),
                num(result.get("short_score")),
                int(long_raw),
                int(short_raw),
                int(num(result.get("raw_score"))),
                num(result.get("weighted_score")),
                int(num(result.get("oi_score"))),
                num(result.get("price")),
                num(result.get("oi")),
                funding,
                spread,
                num(result.get("turnover")),
                num(result.get("watch_threshold")),
                num(result.get("armed_threshold")),
                num(result.get("entry_threshold")),
                [int(long_parts[name]) for name in factor_names],
                [int(short_parts[name]) for name in factor_names],
                [int(result.get("parts", {}).get(name, 0)) for name in factor_names],
                _v68_json(long_group),
                _v68_json(short_group),
                _v68_json(_v68_compact_metrics(result)),
                [str(x) for x in result.get("reject_reasons", [])],
                [str(x) for x in result.get("hard_reject_reasons", [])],
                _v68_json(macro_snapshot),
            ))
        except Exception as error:
            print(f"Research row warning {result.get('symbol')}: {error}")

    if not rows:
        return 0

    stored = _v68_db_executemany(
        """
        INSERT INTO fh_research_scans (
            scan_ts, scan_time, symbol, selected_direction,
            signal_state, selected_regime, best_score,
            long_score, short_score, long_raw, short_raw,
            raw_score, weighted_score, oi_score, price,
            open_interest, funding, spread, turnover,
            watch_threshold, armed_threshold, entry_threshold,
            long_factors, short_factors, selected_factors,
            long_group, short_group, metrics,
            reject_reasons, hard_reject_reasons, macro
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (scan_ts, symbol)
        DO UPDATE SET
            selected_direction = EXCLUDED.selected_direction,
            signal_state = EXCLUDED.signal_state,
            selected_regime = EXCLUDED.selected_regime,
            best_score = EXCLUDED.best_score,
            long_score = EXCLUDED.long_score,
            short_score = EXCLUDED.short_score,
            long_raw = EXCLUDED.long_raw,
            short_raw = EXCLUDED.short_raw,
            raw_score = EXCLUDED.raw_score,
            weighted_score = EXCLUDED.weighted_score,
            oi_score = EXCLUDED.oi_score,
            price = EXCLUDED.price,
            open_interest = EXCLUDED.open_interest,
            funding = EXCLUDED.funding,
            spread = EXCLUDED.spread,
            turnover = EXCLUDED.turnover,
            long_factors = EXCLUDED.long_factors,
            short_factors = EXCLUDED.short_factors,
            selected_factors = EXCLUDED.selected_factors,
            long_group = EXCLUDED.long_group,
            short_group = EXCLUDED.short_group,
            metrics = EXCLUDED.metrics,
            reject_reasons = EXCLUDED.reject_reasons,
            hard_reject_reasons = EXCLUDED.hard_reject_reasons,
            macro = EXCLUDED.macro
        """,
        rows,
    )

    return len(rows) if stored else 0


def save_scan_snapshot(results):
    # V6.8.1 keeps the full in-memory scan available to the shadow risk challenger
    # so it can assess BTC market health without changing the core signal engine.
    global V681_LAST_SCAN_RESULTS
    V681_LAST_SCAN_RESULTS = list(results or [])

    _V68_ORIGINAL_SAVE_SCAN_SNAPSHOT(results)
    stored = _v68_store_research_results(results)
    if stored:
        print(f"V6.8 Research Lab: persisted {stored} full-scan setup(s).")


def _v68_signal_key(result, signal_ts=None):
    signal_ts = float(signal_ts if signal_ts is not None else time.time())
    minute = int(signal_ts // 60)
    raw = (
        f"{minute}|{result.get('symbol')}|{result.get('direction')}|"
        f"{num(result.get('price')):.12g}|{num(result.get('best_score')):.1f}"
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:28]


def _v68_queue_signal(result, message):
    if not V68_DB_READY:
        return None

    signal_ts = time.time()
    key = _v68_signal_key(result, signal_ts)
    signal_dt = datetime.fromtimestamp(signal_ts, tz=timezone.utc)

    _v68_db_execute(
        """
        INSERT INTO fh_signal_queue (
            signal_key, signal_ts, signal_time, symbol,
            direction, score, message, payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (signal_key) DO NOTHING
        """,
        (
            key,
            signal_ts,
            signal_dt,
            result.get("symbol"),
            result.get("direction"),
            num(result.get("best_score")),
            message,
            _v68_json(result),
        ),
    )

    row = _v68_db_execute(
        """
        SELECT signal_key, signal_ts, message, payload,
               telegram_sent, side_effects_done
        FROM fh_signal_queue
        WHERE signal_key = %s
        """,
        (key,),
        fetch="one",
    )
    return row


def _v68_mark_signal_side_effects(signal_key):
    _v68_db_execute(
        """
        UPDATE fh_signal_queue
        SET side_effects_done = TRUE
        WHERE signal_key = %s
        """,
        (signal_key,),
    )


def _v68_mark_signal_delivery(signal_key, sent, error_text=""):
    _v68_db_execute(
        """
        UPDATE fh_signal_queue
        SET telegram_attempts = telegram_attempts + 1,
            telegram_sent = CASE WHEN %s THEN TRUE ELSE telegram_sent END,
            telegram_sent_at = CASE WHEN %s THEN NOW() ELSE telegram_sent_at END,
            last_error = %s
        WHERE signal_key = %s
        """,
        (
            bool(sent),
            bool(sent),
            None if sent else str(error_text or "Telegram delivery failed")[:240],
            signal_key,
        ),
    )


def _v68_apply_signal_side_effects(signal_key, message, result, trades, alert_state):
    if not isinstance(result, dict):
        return False

    row = _v68_db_execute(
        "SELECT side_effects_done FROM fh_signal_queue WHERE signal_key = %s",
        (signal_key,),
        fetch="one",
    )
    if row and bool(row[0]):
        return True

    try:
        # Durable bookkeeping happens before Telegram delivery. If Telegram is
        # temporarily unavailable, the setup still exists and is tracked.
        save_latest_signal(message, result)
        log_signal(result)

        if not has_open_trade_for_symbol(trades, result.get("symbol")):
            trade = create_paper_trade(result, trades)
            trade["v68_signal_key"] = signal_key
            save_json(TRADES_FILE, trades)
        else:
            # V7.4.3 live-bridge fix: paper bookkeeping must never suppress a
            # legitimate live evaluation. The paper engine intentionally keeps
            # only one open paper trade per symbol, but the live selector has
            # its own risk, cooldown, position and exchange preflight gates.
            # Evaluate the durable Core ENTRY even when an older/opposite paper
            # trade on the same symbol is still open.
            live_bridge = globals().get("_v71_evaluate_live_without_paper")
            if callable(live_bridge):
                live_bridge(result, trades, signal_key)

        record_alert_state(result, alert_state)
        _v68_mark_signal_side_effects(signal_key)
        return True
    except Exception as error:
        print(f"V6.8 signal bookkeeping warning: {error}")
        return False


def _v68_retry_unsent_signals(trades, alert_state, max_count=12):
    if not V68_DB_READY:
        return 0

    rows = _v68_db_execute(
        """
        SELECT signal_key, signal_ts, message, payload,
               telegram_sent, side_effects_done
        FROM fh_signal_queue
        WHERE telegram_sent = FALSE
        ORDER BY signal_time ASC
        LIMIT %s
        """,
        (int(max_count),),
        fetch="all",
    )
    if not rows:
        return 0

    delivered = 0
    for signal_key, signal_ts, message, payload, sent, side_done in rows:
        result = payload if isinstance(payload, dict) else {}
        if not side_done:
            _v68_apply_signal_side_effects(
                signal_key,
                message,
                result,
                trades,
                alert_state,
            )

        age_minutes = max(0.0, (time.time() - float(signal_ts)) / 60.0)
        outgoing = message
        if age_minutes >= 10:
            outgoing = (
                f"♻️ RECOVERED UNSENT FUTURESHUNTER ALERT\n"
                f"Original setup was ~{int(age_minutes)}m ago. Do not treat this as a fresh entry.\n\n"
                + message
            )

        ok = send_telegram(outgoing)
        _v68_mark_signal_delivery(
            signal_key,
            ok,
            "" if ok else "retry failed",
        )
        if ok:
            delivered += 1
            print(f"V6.8 durable queue: recovered Telegram signal {signal_key}.")
        else:
            # Avoid hammering Telegram if connectivity is down.
            break
        time.sleep(0.25)

    return delivered


def _v68_research_summary_message():
    if not V68_DB_READY:
        return (
            "🧪 FuturesHunter V6.8 Research Lab\n\n"
            "Database: OFFLINE / not configured\n"
            f"Reason: {V68_DB_LAST_ERROR or 'DATABASE_URL missing'}"
        )

    hours = max(1, V68_RESEARCH_LOOKBACK_HOURS)
    row = _v68_db_execute(
        f"""
        SELECT
            COUNT(*) AS scans,
            COUNT(*) FILTER (WHERE selected_direction = 'LONG') AS longs,
            COUNT(*) FILTER (WHERE selected_direction = 'SHORT') AS shorts,
            COUNT(*) FILTER (WHERE signal_state = 'ENTRY') AS entries,
            COUNT(*) FILTER (
                WHERE signal_state = 'ENTRY' AND selected_direction = 'LONG'
            ) AS long_entries,
            COUNT(*) FILTER (
                WHERE signal_state = 'ENTRY' AND selected_direction = 'SHORT'
            ) AS short_entries,
            AVG(long_score),
            AVG(short_score)
        FROM fh_research_scans
        WHERE scan_time >= NOW() - INTERVAL '{hours} hours'
        """,
        fetch="one",
    )

    queue = _v68_db_execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE telegram_sent = FALSE),
            COUNT(*)
        FROM fh_signal_queue
        """,
        fetch="one",
    ) or (0, 0)

    gaps = _v68_db_execute(
        "SELECT COUNT(*) FROM fh_downtime_gaps",
        fetch="one",
    ) or (0,)

    if not row:
        return "🧪 Research Lab connected, but no full-scan rows have been stored yet."

    scans, longs, shorts, entries, long_entries, short_entries, avg_long, avg_short = row
    return (
        "🧪 FUTURESHUNTER V6.8 RESEARCH LAB\n\n"
        f"Window: last {hours}h\n"
        f"Farmed setup rows: {int(scans or 0)}\n"
        f"Selected LONG / SHORT: {int(longs or 0)} / {int(shorts or 0)}\n"
        f"ENTRY states LONG / SHORT: {int(long_entries or 0)} / {int(short_entries or 0)}\n"
        f"Avg LONG score: {num(avg_long):.1f}\n"
        f"Avg SHORT score: {num(avg_short):.1f}\n"
        f"Durable signals total: {int(queue[1] or 0)}\n"
        f"Unsent Telegram queue: {int(queue[0] or 0)}\n"
        f"Recorded downtime gaps: {int(gaps[0] or 0)}\n\n"
        "Research data is observational; V6.8 does not auto-change thresholds or weights."
    )


def _v68_db_status_message():
    status = "CONNECTED" if V68_DB_READY and _v68_db_connect() is not None else "DEGRADED"
    queue = (0, 0)
    scans = 0
    samples = 0
    if V68_DB_READY:
        queue_row = _v68_db_execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE telegram_sent = FALSE) FROM fh_signal_queue",
            fetch="one",
        )
        scan_row = _v68_db_execute("SELECT COUNT(*) FROM fh_research_scans", fetch="one")
        sample_row = _v68_db_execute("SELECT COUNT(*) FROM fh_market_samples", fetch="one")
        if queue_row:
            queue = queue_row
        if scan_row:
            scans = int(scan_row[0] or 0)
        if sample_row:
            samples = int(sample_row[0] or 0)

    return (
        "🗄️ FUTURESHUNTER V6.8 PERSISTENCE\n\n"
        f"Postgres: {status}\n"
        f"Signals stored: {int(queue[0] or 0)}\n"
        f"Signals waiting for Telegram: {int(queue[1] or 0)}\n"
        f"Research rows: {scans}\n"
        f"Market samples: {samples}\n"
        f"Last DB error: {V68_DB_LAST_ERROR or 'none'}"
    )


# Add Research Lab commands on top of V6.7's Telegram command set.
_V68_V67_HANDLE_TELEGRAM_COMMAND = handle_telegram_command
_V68_V67_BUILD_WELCOME_MESSAGE = build_welcome_message
_V68_V67_TELEGRAM_STATUS_MESSAGE = telegram_status_message


def build_welcome_message():
    base = _V68_V67_BUILD_WELCOME_MESSAGE()
    if "/research" not in base:
        base += "\n\nV6.8: /research /dbstatus"
    return base.replace("V6.7 MacroHunter", "V6.8 Research Lab")


def telegram_status_message():
    base = _V68_V67_TELEGRAM_STATUS_MESSAGE().replace("V6.7", "V6.8")
    db_text = "CONNECTED" if V68_DB_READY else "LOCAL FALLBACK"
    return base + f"\nPersistence: {db_text}\nResearch Lab: {'ACTIVE' if V68_DB_READY else 'WAITING FOR DATABASE_URL'}"


def handle_telegram_command(chat_id, text):
    parts = (text or "").strip().split()
    command = parts[0].lower() if parts else ""

    if command in {"/research", "/lab", "/bias"}:
        send_to_chat(chat_id, _v68_research_summary_message())
        return

    if command in {"/dbstatus", "/persistence"}:
        send_to_chat(chat_id, _v68_db_status_message())
        return

    return _V68_V67_HANDLE_TELEGRAM_COMMAND(chat_id, text)



# ============================================================
# V6.8.1 SHADOW PORTFOLIO / REGIME RISK CHALLENGER
# ============================================================
#
# This challenger is intentionally SHADOW-ONLY. It observes every new paper
# ENTRY that the unchanged V6.7 decision engine would take, assigns an exposure
# multiplier (1.0 / 0.5 / 0.0), and stores the hypothetical result beside the
# actual control outcome. No live/paper ENTRY is suppressed by this layer yet.
#
# The first target is correlated regime failure: many same-direction crypto
# positions can all be technically valid and still fail together when BTC and
# the broader macro/news regime deteriorate. We want evidence before promotion.

V681_LAST_SCAN_RESULTS = []
V681_RISK_LOOKBACK_HOURS = int(os.getenv("V681_RISK_LOOKBACK_HOURS", "24"))
V681_MACRO_CONFLICT = float(os.getenv("V681_MACRO_CONFLICT", "35"))
V681_STRONG_MACRO_CONFLICT = float(os.getenv("V681_STRONG_MACRO_CONFLICT", "42"))
V681_BTC_ENTRY_STRENGTH = float(os.getenv("V681_BTC_ENTRY_STRENGTH", "68"))
V681_MAX_CORRELATED_OPEN = int(os.getenv("V681_MAX_CORRELATED_OPEN", "3"))
V681_HARD_CORRELATED_OPEN = int(os.getenv("V681_HARD_CORRELATED_OPEN", "5"))
V681_STOP_CLUSTER_COUNT = int(os.getenv("V681_STOP_CLUSTER_COUNT", "2"))
V681_STOP_CLUSTER_MINUTES = int(os.getenv("V681_STOP_CLUSTER_MINUTES", "60"))


def _v681_asset_bucket(symbol):
    symbol = str(symbol or "").upper()
    # V7.3 can dynamically recognize plain-ticker MEXC Stock Futures (for
    # example PLTR_USDT / QQQ_USDT) without routing them through BTC-specific
    # crypto regime logic.  The helper is defined later in the file but is
    # available by the time the scanner starts.
    try:
        equity_detector = globals().get("_v73_is_equity_symbol")
        if equity_detector is not None and equity_detector(symbol):
            return "EQUITY"
    except Exception:
        pass
    if any(token in symbol for token in ("STOCK", "SOXL", "SOXS", "TQQQ", "SQQQ")):
        return "EQUITY"
    if symbol.startswith(("XAU", "XAUT", "SILVER", "GOLD")):
        return "METAL"
    if symbol.startswith(("USOIL", "UKOIL", "WTI", "BRENT")):
        return "ENERGY"
    return "CRYPTO"


def _v681_btc_scan_snapshot():
    for result in V681_LAST_SCAN_RESULTS or []:
        if str(result.get("symbol")) == "BTC_USDT":
            return {
                "direction": result.get("direction"),
                "state": result.get("signal_state"),
                "score": num(result.get("best_score")),
                "long_score": num(result.get("long_score")),
                "short_score": num(result.get("short_score")),
                "oi_score": int(num(result.get("oi_score"))),
                "regime": result.get("regime"),
            }
    return {
        "direction": None,
        "state": None,
        "score": 0.0,
        "long_score": 0.0,
        "short_score": 0.0,
        "oi_score": 0,
        "regime": None,
    }


def _v681_same_bucket_open(trades, symbol, direction):
    bucket = _v681_asset_bucket(symbol)
    count = 0
    symbols = []
    for trade in trades or []:
        if trade.get("status") != "OPEN":
            continue
        if trade.get("direction") != direction:
            continue
        if _v681_asset_bucket(trade.get("symbol")) != bucket:
            continue
        count += 1
        symbols.append(str(trade.get("symbol")))
    return count, symbols


def _v681_recent_stop_cluster(trades, symbol, direction, now_ts=None):
    bucket = _v681_asset_bucket(symbol)
    now_ts = float(now_ts or time.time())
    cutoff = now_ts - max(1, V681_STOP_CLUSTER_MINUTES) * 60
    stops = []
    for trade in trades or []:
        if trade.get("status") != "STOP":
            continue
        if trade.get("direction") != direction:
            continue
        if _v681_asset_bucket(trade.get("symbol")) != bucket:
            continue
        closed = num(trade.get("closed_time"))
        if closed >= cutoff:
            stops.append({
                "symbol": trade.get("symbol"),
                "closed_time": closed,
            })
    return len(stops), stops


def _v681_directional_macro_conflict(direction, bucket, combined_score):
    # Risk-off/risk-on is meaningful for crypto and equity proxies. Metals and
    # energy have different macro transmission, so only event risk applies to
    # them in this first challenger version.
    if bucket not in {"CRYPTO", "EQUITY"}:
        return False, False

    if direction == "LONG":
        return (
            combined_score <= -V681_MACRO_CONFLICT,
            combined_score <= -V681_STRONG_MACRO_CONFLICT,
        )
    if direction == "SHORT":
        return (
            combined_score >= V681_MACRO_CONFLICT,
            combined_score >= V681_STRONG_MACRO_CONFLICT,
        )
    return False, False


def _v681_btc_is_weak_for(direction, bucket, btc):
    if bucket != "CRYPTO":
        return False
    if not btc or not btc.get("direction"):
        return True

    state = str(btc.get("state") or "")
    score = num(btc.get("score"))
    aligned = btc.get("direction") == direction
    strong_state = state in {"ENTRY", "ARMED"}
    return not (aligned and strong_state and score >= V681_BTC_ENTRY_STRENGTH)


def _v681_evaluate_risk_challenger(result, trades):
    now_ts = time.time()
    direction = result.get("direction")
    symbol = result.get("symbol")
    bucket = _v681_asset_bucket(symbol)
    macro = get_macro_snapshot() or {}
    combined = num(macro.get("combined_score"))
    event_risk = str(macro.get("event_risk") or "LOW").upper()
    btc = _v681_btc_scan_snapshot()

    open_count, open_symbols = _v681_same_bucket_open(
        trades, symbol, direction
    )
    stop_count, recent_stops = _v681_recent_stop_cluster(
        trades, symbol, direction, now_ts
    )

    macro_conflict, strong_macro_conflict = _v681_directional_macro_conflict(
        direction, bucket, combined
    )
    btc_weak = _v681_btc_is_weak_for(direction, bucket, btc)

    multiplier = 1.0
    reasons = []

    if event_risk == "EXTREME":
        multiplier = 0.0
        reasons.append("EXTREME scheduled-event danger window")
    elif event_risk == "HIGH":
        multiplier = min(multiplier, 0.5)
        reasons.append("HIGH scheduled-event risk")

    if macro_conflict:
        reasons.append(
            f"{bucket} {direction} conflicts with macro/news regime {combined:+.1f}"
        )
        if strong_macro_conflict:
            multiplier = min(multiplier, 0.5)

    if btc_weak:
        reasons.append(
            "BTC is not aligned at ENTRY-strength "
            f"({btc.get('direction') or 'N/A'} {btc.get('state') or 'N/A'} {num(btc.get('score')):.1f})"
        )

    if open_count >= V681_MAX_CORRELATED_OPEN:
        multiplier = min(multiplier, 0.5)
        reasons.append(
            f"{open_count} correlated {bucket} {direction} positions already open"
        )

    if open_count >= V681_HARD_CORRELATED_OPEN:
        multiplier = 0.0
        reasons.append(
            f"hard correlated-exposure cap reached ({open_count})"
        )

    # Combined failure pattern that hurt the control sample: macro conflict +
    # weakening BTC while several same-direction crypto positions were already
    # open. Shadow-block it so we can measure whether that improves expectancy.
    if bucket == "CRYPTO" and macro_conflict and btc_weak and open_count >= V681_MAX_CORRELATED_OPEN:
        multiplier = 0.0
        reasons.append("macro conflict + weak BTC + crowded same-direction crypto book")
    elif bucket == "CRYPTO" and macro_conflict and btc_weak:
        multiplier = min(multiplier, 0.5)
        reasons.append("macro conflict confirmed by weak BTC structure")

    if stop_count >= V681_STOP_CLUSTER_COUNT:
        multiplier = 0.0
        reasons.append(
            f"cooldown: {stop_count} same-bucket {direction} stops in last {V681_STOP_CLUSTER_MINUTES}m"
        )
    elif stop_count == 1 and macro_conflict:
        multiplier = min(multiplier, 0.5)
        reasons.append("recent stop + macro conflict")

    if multiplier <= 0:
        decision = "BLOCK"
        multiplier = 0.0
    elif multiplier < 1.0:
        decision = "REDUCE"
    elif reasons:
        decision = "CAUTION"
    else:
        decision = "ALLOW"

    return {
        "version": "6.8.1-shadow",
        "evaluated_ts": now_ts,
        "decision": decision,
        "size_multiplier": multiplier,
        "reasons": reasons,
        "symbol": symbol,
        "direction": direction,
        "asset_bucket": bucket,
        "signal_score": num(result.get("best_score")),
        "signal_regime": result.get("regime"),
        "macro_score": combined,
        "event_risk": event_risk,
        "btc": btc,
        "btc_weak": btc_weak,
        "macro_conflict": macro_conflict,
        "strong_macro_conflict": strong_macro_conflict,
        "open_correlated": open_count,
        "open_correlated_symbols": open_symbols,
        "recent_stop_count": stop_count,
        "recent_stops": recent_stops,
    }


def v681_init_risk_lab():
    if not V68_DB_READY:
        return False

    statements = [
        """
        CREATE TABLE IF NOT EXISTS fh_risk_challenger (
            id BIGSERIAL PRIMARY KEY,
            evaluated_ts DOUBLE PRECISION NOT NULL,
            evaluated_time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            asset_bucket TEXT,
            signal_score DOUBLE PRECISION,
            signal_regime TEXT,
            macro_score DOUBLE PRECISION,
            event_risk TEXT,
            btc_direction TEXT,
            btc_state TEXT,
            btc_score DOUBLE PRECISION,
            btc_weak BOOLEAN,
            macro_conflict BOOLEAN,
            open_correlated INTEGER,
            recent_stop_count INTEGER,
            decision TEXT NOT NULL,
            size_multiplier DOUBLE PRECISION NOT NULL,
            reasons TEXT[],
            payload JSONB NOT NULL,
            actual_status TEXT,
            actual_final_r DOUBLE PRECISION,
            challenger_final_r DOUBLE PRECISION,
            settled_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_risk_challenger_time
        ON fh_risk_challenger (evaluated_time DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_risk_challenger_decision
        ON fh_risk_challenger (decision, evaluated_time DESC)
        """,
    ]
    for statement in statements:
        if not _v68_db_execute(statement):
            return False
    _v68_state_set("risk_challenger_version", {
        "version": "6.8.1-shadow",
        "mode": "SHADOW_ONLY",
    })
    return True


def _v681_store_risk_decision(risk):
    if not V68_DB_READY or not risk:
        return None
    ts = float(risk.get("evaluated_ts") or time.time())
    row = _v68_db_execute(
        """
        INSERT INTO fh_risk_challenger (
            evaluated_ts, evaluated_time, symbol, direction,
            asset_bucket, signal_score, signal_regime,
            macro_score, event_risk, btc_direction, btc_state,
            btc_score, btc_weak, macro_conflict, open_correlated,
            recent_stop_count, decision, size_multiplier, reasons, payload
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        RETURNING id
        """,
        (
            ts,
            datetime.fromtimestamp(ts, tz=timezone.utc),
            risk.get("symbol"),
            risk.get("direction"),
            risk.get("asset_bucket"),
            num(risk.get("signal_score")),
            risk.get("signal_regime"),
            num(risk.get("macro_score")),
            risk.get("event_risk"),
            risk.get("btc", {}).get("direction"),
            risk.get("btc", {}).get("state"),
            num(risk.get("btc", {}).get("score")),
            bool(risk.get("btc_weak")),
            bool(risk.get("macro_conflict")),
            int(num(risk.get("open_correlated"))),
            int(num(risk.get("recent_stop_count"))),
            risk.get("decision"),
            num(risk.get("size_multiplier")),
            [str(x) for x in risk.get("reasons", [])],
            _v68_json(risk),
        ),
        fetch="one",
    )
    if not row:
        return None
    return int(row[0])


# Preserve risk-challenger metadata inside each control paper trade so the
# outcome can be settled later without fuzzy symbol/time matching.
_V681_ORIGINAL_CREATE_PAPER_TRADE = create_paper_trade


def create_paper_trade(result, trades):
    trade = _V681_ORIGINAL_CREATE_PAPER_TRADE(result, trades)
    risk = result.get("risk_challenger") if isinstance(result, dict) else None
    if risk:
        trade["risk_challenger"] = _clean_json_value(risk)
        save_json(TRADES_FILE, trades)
    return trade


def _v681_settle_risk_challenger(trades):
    if not V68_DB_READY:
        return 0
    changed = False
    settled = 0

    for trade in trades or []:
        if trade.get("status") == "OPEN":
            continue
        if trade.get("v681_risk_settled"):
            continue

        risk = trade.get("risk_challenger") or {}
        row_id = int(num(risk.get("row_id")))
        if row_id <= 0:
            continue

        actual_r = trade.get("final_r")
        if actual_r is None:
            # Ambiguous OHLC outcomes stay unscored rather than being guessed.
            challenger_r = None
        else:
            challenger_r = num(actual_r) * num(risk.get("size_multiplier", 1.0))

        ok = _v68_db_execute(
            """
            UPDATE fh_risk_challenger
            SET actual_status = %s,
                actual_final_r = %s,
                challenger_final_r = %s,
                settled_at = NOW()
            WHERE id = %s
            """,
            (
                trade.get("status"),
                None if actual_r is None else num(actual_r),
                challenger_r,
                row_id,
            ),
        )
        if ok:
            trade["v681_risk_settled"] = True
            changed = True
            settled += 1

    if changed:
        save_json(TRADES_FILE, trades)
    return settled


def _v681_risklab_summary_message():
    if not V68_DB_READY:
        return "🧯 V6.8.1 Risk Lab is waiting for Postgres."

    hours = max(1, V681_RISK_LOOKBACK_HOURS)
    row = _v68_db_execute(
        f"""
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE decision = 'ALLOW'),
            COUNT(*) FILTER (WHERE decision = 'CAUTION'),
            COUNT(*) FILTER (WHERE decision = 'REDUCE'),
            COUNT(*) FILTER (WHERE decision = 'BLOCK'),
            COUNT(*) FILTER (WHERE actual_status IS NOT NULL),
            COALESCE(SUM(actual_final_r) FILTER (WHERE actual_status IS NOT NULL), 0),
            COALESCE(SUM(challenger_final_r) FILTER (WHERE actual_status IS NOT NULL), 0)
        FROM fh_risk_challenger
        WHERE evaluated_time >= NOW() - INTERVAL '{hours} hours'
        """,
        fetch="one",
    )

    latest = _v68_db_execute(
        """
        SELECT symbol, direction, decision, size_multiplier,
               macro_score, btc_direction, btc_state, btc_score,
               open_correlated, recent_stop_count, reasons
        FROM fh_risk_challenger
        ORDER BY evaluated_time DESC
        LIMIT 1
        """,
        fetch="one",
    )

    if not row or int(row[0] or 0) == 0:
        return (
            "🧯 FUTURESHUNTER V6.8.1 RISK LAB\n\n"
            "Mode: SHADOW ONLY\n"
            "No new ENTRY has been evaluated since deployment yet."
        )

    total, allow, caution, reduce, block, settled, actual_r, shadow_r = row
    edge = num(shadow_r) - num(actual_r)
    latest_text = ""
    if latest:
        symbol, direction, decision, mult, macro_score, btc_dir, btc_state, btc_score, open_corr, stops, reasons = latest
        reason_text = "; ".join((reasons or [])[:2]) or "no risk flags"
        latest_text = (
            "\n\nLatest challenger decision:\n"
            f"{symbol} {direction} → {decision} ({num(mult):.1f}x)\n"
            f"Macro {num(macro_score):+.1f} | BTC {btc_dir or 'N/A'} {btc_state or 'N/A'} {num(btc_score):.1f}\n"
            f"Correlated open {int(open_corr or 0)} | recent stops {int(stops or 0)}\n"
            f"Why: {reason_text}"
        )

    return (
        "🧯 FUTURESHUNTER V6.8.1 RISK LAB\n\n"
        "Mode: SHADOW ONLY — control signals remain unchanged\n"
        f"Window: last {hours}h\n"
        f"Decisions A/C/R/B: {int(allow or 0)}/{int(caution or 0)}/{int(reduce or 0)}/{int(block or 0)}\n"
        f"Settled comparisons: {int(settled or 0)}\n"
        f"Control R: {num(actual_r):+.2f}R\n"
        f"Shadow risk R: {num(shadow_r):+.2f}R\n"
        f"Shadow delta: {edge:+.2f}R\n\n"
        "Do not promote this layer from shadow mode until the sample is materially larger."
        + latest_text
    )


# Add /risklab without disturbing the V6.8/V6.7 command stack.
_V681_V68_HANDLE_TELEGRAM_COMMAND = handle_telegram_command


def handle_telegram_command(chat_id, text):
    parts = (text or "").strip().split()
    command = parts[0].lower() if parts else ""
    if command in {"/risklab", "/challenger", "/portfolio"}:
        send_to_chat(chat_id, _v681_risklab_summary_message())
        return
    return _V681_V68_HANDLE_TELEGRAM_COMMAND(chat_id, text)


# ============================================================
# FUTURESHUNTER V6.9 — SHADOW STRATEGY ENSEMBLE / PLAYBOOK LAB
# ============================================================
# This layer does NOT alter the V6.8.1 control entries. It evaluates a set of
# explicit, falsifiable trading playbooks using CLOSED candles only, stores the
# decisions in Postgres, and later settles each playbook against the control
# trade outcome. Instagram/social-media ideas are treated as hypotheses, not
# truth. Promotion requires out-of-sample evidence.

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

V69_STRATEGY_VERSION = "6.9-shadow-ensemble"
V69_STRATEGY_LOOKBACK_HOURS = int(os.getenv("V69_STRATEGY_LOOKBACK_HOURS", "72"))
V69_CONTEXT_CACHE_SECONDS = int(os.getenv("V69_CONTEXT_CACHE_SECONDS", "180"))
V69_PLAYBOOK_NAMES = (
    "MTF_15M_CLOSE",
    "MTF_1H_CLOSE",
    "MTF_4H_CLOSE",
    "BOX_15M_BREAKOUT",
    "BREAKOUT_RETEST_15M",
    "EMA20_RECLAIM_15M",
    "VOL_COMPRESSION_EXPANSION_15M",
    "ORB_5M_SESSION",
)
V69_CONTEXT_CACHE = {}


def _v69_status(status, reason, **extra):
    payload = {"status": status, "reason": reason}
    payload.update(_clean_json_value(extra))
    return payload


def _v69_closed_frame(symbol, interval):
    df = get_candles(symbol, interval)
    if df is None or len(df) < 40:
        return None
    try:
        return add_indicators(df.copy())
    except Exception as error:
        print(f"V6.9 strategy context error {symbol} {interval}: {error}")
        return None


def _v69_market_context(symbol):
    now = time.time()
    cached = V69_CONTEXT_CACHE.get(symbol)
    if cached and now - num(cached.get("ts")) < V69_CONTEXT_CACHE_SECONDS:
        return cached.get("context")

    context = {}
    for key, interval in (("5m", "Min5"), ("15m", "Min15"), ("1h", "Min60"), ("4h", "Hour4")):
        df = _v69_closed_frame(symbol, interval)
        if df is None:
            return None
        context[key] = df
        time.sleep(0.08)

    V69_CONTEXT_CACHE[symbol] = {"ts": now, "context": context}
    return context


def _v69_rows(df):
    # MEXC includes the still-forming candle at the end. All playbooks use
    # closed candles only to prevent accidental look-ahead.
    if df is None or len(df) < 5:
        return None, None, None
    closed = df.iloc[:-1]
    return closed, closed.iloc[-1], closed.iloc[-2]


def _v69_directional_close(direction, latest, previous, label):
    close = num(latest.get("close"))
    prev_high = num(previous.get("high"))
    prev_low = num(previous.get("low"))
    body_pct = num(latest.get("body_pct"))
    close_location = num(latest.get("close_location"))

    if direction == "LONG":
        confirmed = close > prev_high and close_location >= 0.60
        wrong_way = close < prev_low and close_location <= 0.40
        trigger = prev_high
    else:
        confirmed = close < prev_low and close_location <= 0.40
        wrong_way = close > prev_high and close_location >= 0.60
        trigger = prev_low

    if confirmed:
        return _v69_status(
            "CONFIRM",
            f"{label} closed beyond the previous candle in signal direction",
            trigger=trigger,
            close=close,
            body_pct=body_pct,
            close_location=close_location,
        )
    if wrong_way:
        return _v69_status(
            "REJECT",
            f"{label} closed beyond the previous candle in the opposite direction",
            trigger=trigger,
            close=close,
        )
    return _v69_status(
        "WAIT",
        f"{label} has not produced a directional close confirmation yet",
        trigger=trigger,
        close=close,
    )


def _v69_box_breakout(direction, df15):
    closed, latest, _ = _v69_rows(df15)
    if closed is None or len(closed) < 14:
        return _v69_status("N/A", "insufficient 15m history")

    prior = closed.iloc[-13:-1]
    box_high = num(prior["high"].max())
    box_low = num(prior["low"].min())
    close = num(latest["close"])
    rv = num(latest.get("relative_volume"))
    body = num(latest.get("body_pct"))
    loc = num(latest.get("close_location"))

    if direction == "LONG":
        if close > box_high and body >= 0.45 and loc >= 0.60:
            return _v69_status("CONFIRM", "15m closed above the 12-candle box", box_high=box_high, box_low=box_low, rv=rv)
        if close < box_low:
            return _v69_status("REJECT", "15m closed below the box while signal is LONG", box_high=box_high, box_low=box_low)
    else:
        if close < box_low and body >= 0.45 and loc <= 0.40:
            return _v69_status("CONFIRM", "15m closed below the 12-candle box", box_high=box_high, box_low=box_low, rv=rv)
        if close > box_high:
            return _v69_status("REJECT", "15m closed above the box while signal is SHORT", box_high=box_high, box_low=box_low)

    return _v69_status("WAIT", "price is still inside/around the 15m box", box_high=box_high, box_low=box_low, rv=rv)


def _v69_breakout_retest(direction, df15):
    closed, latest, previous = _v69_rows(df15)
    if closed is None or len(closed) < 16:
        return _v69_status("N/A", "insufficient 15m history")

    # Box is defined using candles before the previous candle. The previous
    # candle is the candidate breakout; latest is the candidate retest/hold.
    base = closed.iloc[-14:-2]
    box_high = num(base["high"].max())
    box_low = num(base["low"].min())
    atr = max(num(latest.get("atr14")), num(latest.get("atr")), 1e-12)
    tolerance = 0.20 * atr

    pclose = num(previous["close"])
    close = num(latest["close"])
    high = num(latest["high"])
    low = num(latest["low"])

    if direction == "LONG":
        breakout = pclose > box_high
        retest = low <= box_high + tolerance and close > box_high
        if breakout and retest:
            return _v69_status("CONFIRM", "prior 15m breakout retested the box top and held", level=box_high, tolerance=tolerance)
        if close < box_low:
            return _v69_status("REJECT", "retest failed back through the opposite side of the box", level=box_high)
    else:
        breakout = pclose < box_low
        retest = high >= box_low - tolerance and close < box_low
        if breakout and retest:
            return _v69_status("CONFIRM", "prior 15m breakdown retested the box bottom and held", level=box_low, tolerance=tolerance)
        if close > box_high:
            return _v69_status("REJECT", "retest failed back through the opposite side of the box", level=box_low)

    return _v69_status("WAIT", "breakout + retest sequence is not complete", box_high=box_high, box_low=box_low)


def _v69_ema20_reclaim(direction, df15, df1h):
    _, latest, _ = _v69_rows(df15)
    _, h1, _ = _v69_rows(df1h)
    if latest is None or h1 is None:
        return _v69_status("N/A", "missing 15m/1h data")

    close = num(latest["close"])
    open_ = num(latest["open"])
    high = num(latest["high"])
    low = num(latest["low"])
    ema20 = num(latest.get("ema20"))
    atr = max(num(latest.get("atr14")), num(latest.get("atr")), 1e-12)
    trend1h = describe_trend(h1)
    touch = 0.22 * atr

    if direction == "LONG":
        if trend1h == "BEARISH":
            return _v69_status("REJECT", "1h trend is bearish for a LONG pullback play")
        if low <= ema20 + touch and close > ema20 and close > open_:
            return _v69_status("CONFIRM", "15m pulled into EMA20 and reclaimed it with 1h non-bearish", ema20=ema20, trend1h=trend1h)
        if close < ema20 - 0.50 * atr:
            return _v69_status("REJECT", "15m lost EMA20 by more than 0.5 ATR", ema20=ema20)
    else:
        if trend1h == "BULLISH":
            return _v69_status("REJECT", "1h trend is bullish for a SHORT pullback play")
        if high >= ema20 - touch and close < ema20 and close < open_:
            return _v69_status("CONFIRM", "15m pulled into EMA20 and rejected it with 1h non-bullish", ema20=ema20, trend1h=trend1h)
        if close > ema20 + 0.50 * atr:
            return _v69_status("REJECT", "15m reclaimed EMA20 by more than 0.5 ATR against SHORT", ema20=ema20)

    return _v69_status("WAIT", "waiting for an EMA20 pullback/reclaim sequence", ema20=ema20, trend1h=trend1h)


def _v69_vol_compression_expansion(direction, df15):
    closed, latest, previous = _v69_rows(df15)
    if closed is None or len(closed) < 55:
        return _v69_status("N/A", "insufficient history for compression percentile")

    widths = closed["bb_width"].dropna().iloc[-50:]
    if len(widths) < 30:
        return _v69_status("N/A", "insufficient Bollinger width history")

    threshold = float(widths.quantile(0.35))
    pre_width = num(previous.get("bb_width"))
    latest_width = num(latest.get("bb_width"))
    rv = num(latest.get("relative_volume"))
    atr = max(num(latest.get("atr14")), 1e-12)
    candle_range = num(latest["high"]) - num(latest["low"])
    loc = num(latest.get("close_location"))
    expansion = latest_width > pre_width and candle_range >= 1.05 * atr and rv >= 1.10
    compressed = pre_width <= threshold

    if not compressed:
        return _v69_status("N/A", "no preceding volatility compression", pre_width=pre_width, threshold=threshold)

    if direction == "LONG" and expansion and loc >= 0.65:
        return _v69_status("CONFIRM", "15m volatility compression expanded upward with volume", rv=rv, pre_width=pre_width, latest_width=latest_width)
    if direction == "SHORT" and expansion and loc <= 0.35:
        return _v69_status("CONFIRM", "15m volatility compression expanded downward with volume", rv=rv, pre_width=pre_width, latest_width=latest_width)

    return _v69_status("WAIT", "compression exists but directional expansion is not confirmed", rv=rv, pre_width=pre_width, latest_width=latest_width)


def _v69_session_open_utc_ts(bucket, reference_ts):
    dt_utc = datetime.fromtimestamp(reference_ts, tz=timezone.utc)

    if bucket == "CRYPTO":
        # Deliberately named/treated as a hypothesis: crypto has no exchange
        # open. We test the first 5m candle of the UTC trading day because this
        # is a common social-media rule, not because it is assumed valid.
        return datetime(dt_utc.year, dt_utc.month, dt_utc.day, tzinfo=timezone.utc).timestamp(), "UTC_DAY_00:00"

    if ZoneInfo is None:
        return None, None

    ny = ZoneInfo("America/New_York")
    dt_ny = dt_utc.astimezone(ny)
    if bucket == "EQUITY":
        local = datetime(dt_ny.year, dt_ny.month, dt_ny.day, 9, 30, tzinfo=ny)
        return local.astimezone(timezone.utc).timestamp(), "NY_09:30"
    if bucket == "METAL":
        # 08:20 New York is tested as a gold/metals session hypothesis.
        local = datetime(dt_ny.year, dt_ny.month, dt_ny.day, 8, 20, tzinfo=ny)
        return local.astimezone(timezone.utc).timestamp(), "NY_METALS_08:20"

    return None, None


def _v69_orb_5m(direction, symbol, df5):
    closed, latest, _ = _v69_rows(df5)
    if closed is None or len(closed) < 10:
        return _v69_status("N/A", "insufficient 5m history")

    bucket = _v681_asset_bucket(symbol)
    session_ts, session_name = _v69_session_open_utc_ts(bucket, num(latest["time"]))
    if session_ts is None:
        return _v69_status("N/A", f"5m ORB not defined for {bucket}")

    candidates = closed[(closed["time"] >= session_ts) & (closed["time"] < session_ts + 300)]
    if candidates.empty:
        return _v69_status("N/A", f"session opening 5m candle unavailable ({session_name})")

    opening = candidates.iloc[0]
    orb_high = num(opening["high"])
    orb_low = num(opening["low"])
    close = num(latest["close"])
    loc = num(latest.get("close_location"))

    if num(latest["time"]) <= num(opening["time"]):
        return _v69_status("WAIT", "opening 5m range has only just formed", session=session_name, orb_high=orb_high, orb_low=orb_low)

    if direction == "LONG":
        if close > orb_high and loc >= 0.55:
            return _v69_status("CONFIRM", "price closed above the session first-5m range", session=session_name, orb_high=orb_high, orb_low=orb_low)
        if close < orb_low and loc <= 0.40:
            return _v69_status("REJECT", "price closed below the session first-5m range against LONG", session=session_name, orb_high=orb_high, orb_low=orb_low)
    else:
        if close < orb_low and loc <= 0.45:
            return _v69_status("CONFIRM", "price closed below the session first-5m range", session=session_name, orb_high=orb_high, orb_low=orb_low)
        if close > orb_high and loc >= 0.60:
            return _v69_status("REJECT", "price closed above the session first-5m range against SHORT", session=session_name, orb_high=orb_high, orb_low=orb_low)

    return _v69_status("WAIT", "price has not closed outside the first-5m range in signal direction", session=session_name, orb_high=orb_high, orb_low=orb_low)


def _v69_evaluate_strategy_ensemble(result):
    symbol = result.get("symbol")
    direction = result.get("direction")
    context = _v69_market_context(symbol)
    if context is None:
        return {
            "version": V69_STRATEGY_VERSION,
            "evaluated_ts": time.time(),
            "symbol": symbol,
            "direction": direction,
            "consensus": "NO_DATA",
            "confirm_count": 0,
            "wait_count": 0,
            "reject_count": 0,
            "applicable_count": 0,
            "playbooks": {},
        }

    _, m15, p15 = _v69_rows(context["15m"])
    _, h1, p1h = _v69_rows(context["1h"])
    _, h4, p4h = _v69_rows(context["4h"])

    playbooks = {
        "MTF_15M_CLOSE": _v69_directional_close(direction, m15, p15, "15m"),
        "MTF_1H_CLOSE": _v69_directional_close(direction, h1, p1h, "1h"),
        "MTF_4H_CLOSE": _v69_directional_close(direction, h4, p4h, "4h"),
        "BOX_15M_BREAKOUT": _v69_box_breakout(direction, context["15m"]),
        "BREAKOUT_RETEST_15M": _v69_breakout_retest(direction, context["15m"]),
        "EMA20_RECLAIM_15M": _v69_ema20_reclaim(direction, context["15m"], context["1h"]),
        "VOL_COMPRESSION_EXPANSION_15M": _v69_vol_compression_expansion(direction, context["15m"]),
        "ORB_5M_SESSION": _v69_orb_5m(direction, symbol, context["5m"]),
    }

    statuses = [p.get("status") for p in playbooks.values() if p.get("status") != "N/A"]
    confirm = statuses.count("CONFIRM")
    wait = statuses.count("WAIT")
    reject = statuses.count("REJECT")
    applicable = len(statuses)

    if applicable == 0:
        consensus = "NO_DATA"
    elif reject >= 2 and reject > confirm:
        consensus = "AVOID"
    elif confirm >= 3 and reject == 0:
        consensus = "STRONG_CONFIRM"
    elif confirm >= 2 and confirm > reject:
        consensus = "CONFIRM"
    elif wait >= max(confirm, reject):
        consensus = "WAIT"
    else:
        consensus = "MIXED"

    return {
        "version": V69_STRATEGY_VERSION,
        "evaluated_ts": time.time(),
        "symbol": symbol,
        "direction": direction,
        "asset_bucket": _v681_asset_bucket(symbol),
        "signal_score": num(result.get("best_score")),
        "signal_regime": result.get("regime"),
        "consensus": consensus,
        "confirm_count": confirm,
        "wait_count": wait,
        "reject_count": reject,
        "applicable_count": applicable,
        "playbooks": playbooks,
    }


def v69_init_strategy_lab():
    if not V68_DB_READY:
        return False
    statements = [
        """
        CREATE TABLE IF NOT EXISTS fh_strategy_ensemble (
            id BIGSERIAL PRIMARY KEY,
            ensemble_key TEXT UNIQUE NOT NULL,
            evaluated_ts DOUBLE PRECISION NOT NULL,
            evaluated_time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            asset_bucket TEXT,
            signal_score DOUBLE PRECISION,
            signal_regime TEXT,
            consensus TEXT,
            confirm_count INTEGER,
            wait_count INTEGER,
            reject_count INTEGER,
            applicable_count INTEGER,
            playbooks JSONB NOT NULL,
            payload JSONB NOT NULL,
            actual_status TEXT,
            actual_final_r DOUBLE PRECISION,
            settled_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_strategy_ensemble_time
        ON fh_strategy_ensemble (evaluated_time DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_strategy_ensemble_consensus
        ON fh_strategy_ensemble (consensus, evaluated_time DESC)
        """,
    ]
    for statement in statements:
        if not _v68_db_execute(statement):
            return False
    _v68_state_set("strategy_ensemble_version", {
        "version": V69_STRATEGY_VERSION,
        "mode": "SHADOW_ONLY",
        "playbooks": list(V69_PLAYBOOK_NAMES),
    })
    return True


def _v69_ensemble_key(result, ensemble):
    ts_minute = int(num(ensemble.get("evaluated_ts")) // 60)
    raw = f"{ts_minute}|{result.get('symbol')}|{result.get('direction')}|{num(result.get('price')):.12g}|{num(result.get('best_score')):.3f}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _v69_store_ensemble(result, ensemble):
    if not V68_DB_READY or not ensemble:
        return None
    ts = float(ensemble.get("evaluated_ts") or time.time())
    key = _v69_ensemble_key(result, ensemble)
    row = _v68_db_execute(
        """
        INSERT INTO fh_strategy_ensemble (
            ensemble_key, evaluated_ts, evaluated_time, symbol, direction,
            asset_bucket, signal_score, signal_regime, consensus,
            confirm_count, wait_count, reject_count, applicable_count,
            playbooks, payload
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (ensemble_key) DO NOTHING
        RETURNING id
        """,
        (
            key, ts, datetime.fromtimestamp(ts, tz=timezone.utc),
            result.get("symbol"), result.get("direction"),
            ensemble.get("asset_bucket"), num(ensemble.get("signal_score")),
            ensemble.get("signal_regime"), ensemble.get("consensus"),
            int(num(ensemble.get("confirm_count"))), int(num(ensemble.get("wait_count"))),
            int(num(ensemble.get("reject_count"))), int(num(ensemble.get("applicable_count"))),
            _v68_json(ensemble.get("playbooks", {})), _v68_json(ensemble),
        ),
        fetch="one",
    )
    if row:
        return int(row[0])
    existing = _v68_db_execute(
        "SELECT id FROM fh_strategy_ensemble WHERE ensemble_key = %s",
        (key,), fetch="one"
    )
    return int(existing[0]) if existing else None


_V69_V681_CREATE_PAPER_TRADE = create_paper_trade


def create_paper_trade(result, trades):
    trade = _V69_V681_CREATE_PAPER_TRADE(result, trades)
    ensemble = result.get("strategy_ensemble") if isinstance(result, dict) else None
    if ensemble:
        trade["strategy_ensemble"] = _clean_json_value(ensemble)
        save_json(TRADES_FILE, trades)
    return trade


def _v69_settle_strategy_ensemble(trades):
    if not V68_DB_READY:
        return 0
    settled = 0
    changed = False
    for trade in trades or []:
        if trade.get("status") == "OPEN" or trade.get("v69_strategy_settled"):
            continue
        ensemble = trade.get("strategy_ensemble") or {}
        row_id = int(num(ensemble.get("row_id")))
        if row_id <= 0:
            continue
        actual_r = trade.get("final_r")
        ok = _v68_db_execute(
            """
            UPDATE fh_strategy_ensemble
            SET actual_status = %s,
                actual_final_r = %s,
                settled_at = NOW()
            WHERE id = %s
            """,
            (
                trade.get("status"),
                None if actual_r is None else num(actual_r),
                row_id,
            ),
        )
        if ok:
            trade["v69_strategy_settled"] = True
            changed = True
            settled += 1
    if changed:
        save_json(TRADES_FILE, trades)
    return settled


def _v69_strategy_lab_summary_message():
    if not V68_DB_READY:
        return "🧠 V6.9 Strategy Lab is waiting for Postgres."

    hours = max(1, V69_STRATEGY_LOOKBACK_HOURS)
    rows = _v68_db_execute(
        f"""
        SELECT consensus, playbooks, actual_final_r
        FROM fh_strategy_ensemble
        WHERE evaluated_time >= NOW() - INTERVAL '{hours} hours'
        ORDER BY evaluated_time DESC
        """,
        fetch="all",
    ) or []

    if not rows:
        return (
            "🧠 FUTURESHUNTER V6.9 STRATEGY LAB\n\n"
            "Mode: SHADOW ONLY\n"
            "No control ENTRY has been evaluated by the playbook ensemble yet."
        )

    settled_rows = [r for r in rows if r[2] is not None]
    control_r = sum(num(r[2]) for r in settled_rows)
    consensus_counts = {}
    for consensus, _, _ in rows:
        consensus_counts[str(consensus)] = consensus_counts.get(str(consensus), 0) + 1

    perf = {name: {"confirm": 0, "settled": 0, "r": 0.0} for name in V69_PLAYBOOK_NAMES}
    for _, playbooks, actual_r in rows:
        playbooks = playbooks or {}
        for name in V69_PLAYBOOK_NAMES:
            pb = playbooks.get(name) or {}
            if pb.get("status") == "CONFIRM":
                perf[name]["confirm"] += 1
                if actual_r is not None:
                    perf[name]["settled"] += 1
                    perf[name]["r"] += num(actual_r)

    ranked = sorted(
        perf.items(),
        key=lambda item: (item[1]["settled"], item[1]["r"]),
        reverse=True,
    )
    lines = []
    for name, p in ranked:
        lines.append(
            f"{name}: confirms {p['confirm']} | settled {p['settled']} | filter R {p['r']:+.1f}"
        )

    ctext = ", ".join(f"{k} {v}" for k, v in sorted(consensus_counts.items()))
    return (
        "🧠 FUTURESHUNTER V6.9 STRATEGY LAB\n\n"
        "Mode: SHADOW ONLY — ZERO control-entry changes\n"
        f"Window: last {hours}h\n"
        f"Signals evaluated: {len(rows)} | settled: {len(settled_rows)}\n"
        f"Control R on settled: {control_r:+.2f}R\n"
        f"Consensus: {ctext}\n\n"
        + "\n".join(lines)
        + "\n\nFilter R means: take the original control trade only when that playbook already CONFIRMED. WAIT is not yet simulated as a delayed re-entry."
    )


# Add Strategy Lab commands without disturbing Risk Lab / Research Lab commands.
_V69_V681_HANDLE_TELEGRAM_COMMAND = handle_telegram_command


def handle_telegram_command(chat_id, text):
    parts = (text or "").strip().split()
    command = parts[0].lower() if parts else ""
    if command in {"/strategylab", "/playbooks", "/ensemble"}:
        send_to_chat(chat_id, _v69_strategy_lab_summary_message())
        return
    return _V69_V681_HANDLE_TELEGRAM_COMMAND(chat_id, text)



# ============================================================
# FUTURESHUNTER V6.9.1 — LIVE TRADE SUPERVISOR (SHADOW ONLY)
# ============================================================
# The scanner, Risk Lab and Strategy Lab no longer stop thinking after ENTRY.
# Every OPEN control paper trade is re-evaluated throughout its lifetime using:
#   1) Core scanner trajectory (latest full-scan directional score/state)
#   2) Portfolio/regime risk (macro, BTC, correlated book, stop clusters)
#   3) Strategy ensemble (5m/15m/1h/4h closed-candle playbooks)
#   4) Fast tape telemetry (price/OI + current R)
#
# IMPORTANT: this is advisory/research SHADOW logic. It NEVER closes, resizes,
# moves the stop of, or otherwise modifies the control paper trade. The first
# confirmed EXIT_WARNING is stored as a counterfactual shadow exit so we can
# later compare supervisor management with the original STOP/TP3/expiry logic.

V691_SUPERVISOR_VERSION = "6.9.1-shadow-live-supervisor+profit-protect-7.1.5"
V691_SUPERVISOR_INTERVAL_SECONDS = int(os.getenv("V691_SUPERVISOR_INTERVAL_SECONDS", "60"))
V691_STRATEGY_REFRESH_SECONDS = int(os.getenv("V691_STRATEGY_REFRESH_SECONDS", "300"))
V691_SNAPSHOT_SECONDS = int(os.getenv("V691_SNAPSHOT_SECONDS", "180"))
V691_SUPERVISOR_LOOKBACK_HOURS = int(os.getenv("V691_SUPERVISOR_LOOKBACK_HOURS", "72"))
V691_STATE_CONFIRMATIONS = max(1, int(os.getenv("V691_STATE_CONFIRMATIONS", "2")))
V691_EXIT_CONFIRMATIONS = max(1, int(os.getenv("V691_EXIT_CONFIRMATIONS", "2")))
# V7.1.5 research-only profit-protection challenger. It NEVER touches live or paper exits.
V691_PROFIT_PROTECT_MIN_R = float(os.getenv("V691_PROFIT_PROTECT_MIN_R", "1.5"))
V691_PROFIT_PROTECT_CORE_HEALTH_MAX = float(os.getenv("V691_PROFIT_PROTECT_CORE_HEALTH_MAX", "55"))
V691_STRATEGY_RUNTIME_CACHE = {}
V691_SEVERITY = {
    "STRONG_HOLD": 0,
    "HOLD": 1,
    "CAUTION": 2,
    "DEFENSIVE": 3,
    "EXIT_WARNING": 4,
}


def _v691_clamp(value, low=0.0, high=100.0):
    return max(low, min(high, num(value)))


def _v691_latest_scan(symbol):
    symbol = str(symbol or "").upper()
    for result in V681_LAST_SCAN_RESULTS or []:
        if str(result.get("symbol") or "").upper() == symbol:
            return result

    if not V68_DB_READY:
        return None

    row = _v68_db_execute(
        """
        SELECT selected_direction, signal_state, selected_regime,
               best_score, long_score, short_score, oi_score, price,
               entry_threshold, metrics, scan_ts
        FROM fh_research_scans
        WHERE symbol = %s
        ORDER BY scan_time DESC
        LIMIT 1
        """,
        (symbol,),
        fetch="one",
    )
    if not row:
        return None

    direction, state, regime, best, long_score, short_score, oi_score, price, entry_threshold, metrics, scan_ts = row
    return {
        "symbol": symbol,
        "direction": direction,
        "signal_state": state,
        "regime": regime,
        "best_score": num(best),
        "long_score": num(long_score),
        "short_score": num(short_score),
        "oi_score": int(num(oi_score)),
        "price": num(price),
        "entry_threshold": num(entry_threshold),
        "metrics": metrics or {},
        "scan_ts": num(scan_ts),
    }


def _v691_core_view(trade):
    direction = str(trade.get("direction") or "")
    scan = _v691_latest_scan(trade.get("symbol"))
    if not scan:
        return {
            "health": 55.0,
            "directional_score": 0.0,
            "selected_direction": None,
            "state": "NO_DATA",
            "regime": None,
            "score_delta": 0.0,
            "reasons": ["no recent core full-scan snapshot"],
            "hard_flags": [],
        }

    directional_score = num(
        scan.get("long_score") if direction == "LONG" else scan.get("short_score")
    )
    if directional_score <= 0 and scan.get("direction") == direction:
        directional_score = num(scan.get("best_score"))

    selected_direction = scan.get("direction")
    state = str(scan.get("signal_state") or "IGNORE")
    same_direction = selected_direction == direction
    health = directional_score if directional_score > 0 else 50.0
    reasons = []
    hard_flags = []

    if same_direction:
        health += {
            "ENTRY": 6.0,
            "ARMED": 3.0,
            "WATCH": -3.0,
            "REJECT": -7.0,
            "IGNORE": -9.0,
        }.get(state, 0.0)
    else:
        health -= 8.0
        reasons.append(f"core scanner currently prefers {selected_direction or 'no direction'}")
        if state == "ENTRY":
            health -= 10.0
            hard_flags.append("core scanner flipped to opposite ENTRY")
        elif state == "ARMED":
            health -= 5.0

    entry_score = num(trade.get("score"))
    score_delta = directional_score - entry_score if directional_score > 0 else 0.0
    if score_delta <= -25:
        health -= 10.0
        reasons.append(f"directional score deteriorated {score_delta:.1f} points from entry")
    elif score_delta <= -15:
        health -= 5.0
        reasons.append(f"directional score is {abs(score_delta):.1f} points below entry")
    elif score_delta >= 10:
        health += 4.0

    if directional_score and directional_score < 45:
        reasons.append(f"directional core score is weak at {directional_score:.1f}")
        if not same_direction:
            hard_flags.append("trade-direction core score collapsed while scanner prefers opposite side")

    return {
        "health": _v691_clamp(health),
        "directional_score": directional_score,
        "selected_direction": selected_direction,
        "state": state,
        "regime": scan.get("regime"),
        "score_delta": score_delta,
        "oi_score": int(num(scan.get("oi_score"))),
        "scan_price": num(scan.get("price")),
        "reasons": reasons,
        "hard_flags": hard_flags,
    }


def _v691_other_correlated_open(trades, trade):
    bucket = _v681_asset_bucket(trade.get("symbol"))
    direction = trade.get("direction")
    signal_id = trade.get("signal_id")
    symbols = []
    for other in trades or []:
        if other.get("status") != "OPEN":
            continue
        if signal_id and other.get("signal_id") == signal_id:
            continue
        if other.get("direction") != direction:
            continue
        if _v681_asset_bucket(other.get("symbol")) != bucket:
            continue
        symbols.append(str(other.get("symbol")))
    return len(symbols), symbols


def _v691_management_risk_view(trade, trades):
    direction = trade.get("direction")
    symbol = trade.get("symbol")
    bucket = _v681_asset_bucket(symbol)
    macro = get_macro_snapshot() or {}
    combined = num(macro.get("combined_score"))
    event_risk = str(macro.get("event_risk") or "LOW").upper()
    btc = _v681_btc_scan_snapshot()
    macro_conflict, strong_macro_conflict = _v681_directional_macro_conflict(
        direction, bucket, combined
    )
    btc_weak = _v681_btc_is_weak_for(direction, bucket, btc)
    open_count, open_symbols = _v691_other_correlated_open(trades, trade)
    stop_count, recent_stops = _v681_recent_stop_cluster(
        trades, symbol, direction, time.time()
    )

    health = 100.0
    reasons = []
    hard_flags = []

    if event_risk == "EXTREME":
        health -= 55.0
        reasons.append("EXTREME event-risk window")
        hard_flags.append("extreme scheduled-event risk")
    elif event_risk == "HIGH":
        health -= 28.0
        reasons.append("HIGH event-risk window")

    if macro_conflict:
        health -= 24.0 if strong_macro_conflict else 14.0
        reasons.append(f"{bucket} {direction} conflicts with macro/news {combined:+.1f}")

    if btc_weak:
        health -= 18.0
        reasons.append(
            "BTC not aligned at entry strength "
            f"({btc.get('direction') or 'N/A'} {btc.get('state') or 'N/A'} {num(btc.get('score')):.1f})"
        )

    if open_count >= 6:
        health -= 18.0
        reasons.append(f"{open_count} other correlated {bucket} {direction} positions open")
    elif open_count >= 3:
        health -= 10.0
        reasons.append(f"{open_count} other correlated {bucket} {direction} positions open")
    elif open_count >= 1:
        health -= 4.0

    if stop_count >= 2:
        health -= 18.0
        reasons.append(f"{stop_count} same-bucket {direction} stops in the recent cluster window")
    elif stop_count == 1:
        health -= 7.0

    # Portfolio stress is not, by itself, an exit thesis. It becomes a hard
    # invalidation only when broad conditions and the core trade thesis both fail.
    if strong_macro_conflict and btc_weak and bucket == "CRYPTO":
        hard_flags.append("strong macro conflict + weak BTC")

    health = _v691_clamp(health)
    if health >= 78:
        decision = "ALLOW"
    elif health >= 58:
        decision = "CAUTION"
    else:
        decision = "DEFENSIVE"

    return {
        "health": health,
        "decision": decision,
        "macro_score": combined,
        "event_risk": event_risk,
        "macro_regime": macro.get("regime"),
        "macro_conflict": macro_conflict,
        "strong_macro_conflict": strong_macro_conflict,
        "btc": btc,
        "btc_weak": btc_weak,
        "open_correlated": open_count,
        "open_correlated_symbols": open_symbols,
        "recent_stop_count": stop_count,
        "recent_stops": recent_stops,
        "reasons": reasons,
        "hard_flags": hard_flags,
    }


def _v691_strategy_view(trade, core):
    symbol = str(trade.get("symbol") or "")
    direction = str(trade.get("direction") or "")
    cache_key = f"{symbol}|{direction}"
    now_ts = time.time()
    cached = V691_STRATEGY_RUNTIME_CACHE.get(cache_key)
    ensemble = None

    if cached and now_ts - num(cached.get("ts")) < V691_STRATEGY_REFRESH_SECONDS:
        ensemble = cached.get("ensemble")

    if not ensemble:
        synthetic = {
            "symbol": symbol,
            "direction": direction,
            "best_score": num(core.get("directional_score")) or num(trade.get("score")),
            "regime": core.get("regime") or (trade.get("strategy_ensemble") or {}).get("signal_regime") or (trade.get("risk_challenger") or {}).get("signal_regime") or "UNKNOWN",
        }
        try:
            ensemble = _v69_evaluate_strategy_ensemble(synthetic)
            V691_STRATEGY_RUNTIME_CACHE[cache_key] = {
                "ts": now_ts,
                "ensemble": ensemble,
            }
        except Exception as error:
            print(f"V6.9.1 strategy supervisor error {symbol}: {error}")
            ensemble = cached.get("ensemble") if cached else None

    if not ensemble:
        return {
            "health": 55.0,
            "consensus": "NO_DATA",
            "confirm_count": 0,
            "wait_count": 0,
            "reject_count": 0,
            "applicable_count": 0,
            "statuses": {},
            "reasons": ["strategy ensemble unavailable"],
            "hard_flags": [],
        }

    playbooks = ensemble.get("playbooks") or {}
    statuses = {}
    values = []
    reasons = []
    rejected_names = []
    weights = {"CONFIRM": 100.0, "WAIT": 60.0, "REJECT": 20.0}
    for name, payload in playbooks.items():
        status = str((payload or {}).get("status") or "N/A")
        statuses[name] = status
        if status in weights:
            values.append(weights[status])
        if status == "REJECT":
            rejected_names.append(name)
            reason = str((payload or {}).get("reason") or "rejected")
            reasons.append(f"{name}: {reason}")

    health = sum(values) / len(values) if values else 55.0
    consensus = str(ensemble.get("consensus") or "NO_DATA")
    health += {
        "STRONG_CONFIRM": 6.0,
        "CONFIRM": 3.0,
        "WAIT": 0.0,
        "MIXED": -4.0,
        "AVOID": -12.0,
    }.get(consensus, 0.0)

    reject_count = int(num(ensemble.get("reject_count")))
    hard_flags = []
    if reject_count >= 3:
        hard_flags.append(f"{reject_count} strategy playbooks reject the open thesis")
    if consensus == "AVOID":
        hard_flags.append("strategy ensemble consensus is AVOID")

    return {
        "health": _v691_clamp(health),
        "consensus": consensus,
        "confirm_count": int(num(ensemble.get("confirm_count"))),
        "wait_count": int(num(ensemble.get("wait_count"))),
        "reject_count": reject_count,
        "applicable_count": int(num(ensemble.get("applicable_count"))),
        "statuses": statuses,
        "reasons": reasons,
        "hard_flags": hard_flags,
    }


def _v691_tape_view(trade, details, oi_metrics):
    symbol = trade.get("symbol")
    ticker = (details or {}).get(symbol, {}) or {}
    price = num(ticker.get("lastPrice")) or num(ticker.get("fairPrice")) or num(trade.get("entry"))
    metrics = (oi_metrics or {}).get(symbol, {}) or {}
    oi5 = metrics.get("oi5")
    oi15 = metrics.get("oi15")
    price5 = metrics.get("price5")
    price15 = metrics.get("price15")
    direction = trade.get("direction")
    current_r = current_r_for_price(trade, price) if price > 0 else 0.0

    health = 60.0
    reasons = []

    def apply_window(pchg, ochg, weight, label):
        nonlocal health
        if pchg is None or ochg is None:
            return
        p = num(pchg)
        o = num(ochg)
        directional_p = p if direction == "LONG" else -p
        if directional_p > 0 and o > 0:
            health += 10.0 * weight
        elif directional_p < 0 and o > 0:
            health -= 14.0 * weight
            reasons.append(f"{label} price moving against trade while OI expands")
        elif directional_p > 0 and o < 0:
            health -= 3.0 * weight
            reasons.append(f"{label} favorable price move is occurring with contracting OI")
        elif directional_p < 0 and o < 0:
            health -= 5.0 * weight

    apply_window(price5, oi5, 0.7, "5m")
    apply_window(price15, oi15, 1.0, "15m")

    if current_r >= 2.0:
        health += 8.0
    elif current_r >= 1.0:
        health += 5.0
    elif current_r <= -0.85:
        health -= 24.0
        reasons.append(f"trade is close to original stop at {current_r:+.2f}R")
    elif current_r <= -0.50:
        health -= 10.0
        reasons.append(f"trade is under pressure at {current_r:+.2f}R")

    if trade.get("tp2_hit"):
        health += 6.0
    elif trade.get("tp1_hit"):
        health += 3.0

    return {
        "health": _v691_clamp(health),
        "price": price,
        "current_r": current_r,
        "oi5": oi5,
        "oi15": oi15,
        "price5": price5,
        "price15": price15,
        "funding": num(ticker.get("fundingRate")),
        "reasons": reasons,
        "hard_flags": [],
    }


def _v691_evaluate_open_trade(trade, trades, details, oi_metrics):
    core = _v691_core_view(trade)
    risk = _v691_management_risk_view(trade, trades)
    strategy = _v691_strategy_view(trade, core)
    tape = _v691_tape_view(trade, details, oi_metrics)

    health = _v691_clamp(
        0.40 * num(core.get("health"))
        + 0.30 * num(strategy.get("health"))
        + 0.20 * num(risk.get("health"))
        + 0.10 * num(tape.get("health"))
    )

    hard_flags = []
    hard_flags.extend(core.get("hard_flags") or [])
    hard_flags.extend(strategy.get("hard_flags") or [])

    # A broad-risk warning becomes an exit-quality invalidation only when the
    # trade's own core thesis is also weak. This prevents "BLOCK new entries"
    # from being misread as "dump a technically healthy existing position".
    if risk.get("hard_flags") and num(core.get("health")) < 50:
        hard_flags.extend(risk.get("hard_flags") or [])

    if (
        strategy.get("consensus") == "AVOID"
        and num(core.get("health")) < 52
    ):
        hard_flags.append("core + strategy thesis failure")

    opposite_entry = (
        core.get("selected_direction")
        and core.get("selected_direction") != trade.get("direction")
        and core.get("state") == "ENTRY"
    )
    immediate_exit = bool(opposite_entry and num(core.get("directional_score")) < 45)

    if immediate_exit or len(set(hard_flags)) >= 2:
        target_state = "EXIT_WARNING"
    elif health >= 82:
        target_state = "STRONG_HOLD"
    elif health >= 68:
        target_state = "HOLD"
    elif health >= 54:
        target_state = "CAUTION"
    elif health >= 40:
        target_state = "DEFENSIVE"
    else:
        target_state = "EXIT_WARNING"

    reasons = []
    for group in (core, risk, strategy, tape):
        for reason in group.get("reasons") or []:
            if reason not in reasons:
                reasons.append(reason)
    for flag in hard_flags:
        if flag not in reasons:
            reasons.append(flag)

    entry_regime = (
        (trade.get("strategy_ensemble") or {}).get("signal_regime")
        or (trade.get("risk_challenger") or {}).get("signal_regime")
        or trade.get("entry_regime")
        or "UNKNOWN"
    )

    return {
        "version": V691_SUPERVISOR_VERSION,
        "evaluated_ts": time.time(),
        "signal_id": trade.get("signal_id"),
        "symbol": trade.get("symbol"),
        "direction": trade.get("direction"),
        "entry_regime": entry_regime,
        "current_regime": core.get("regime"),
        "health": round(health, 2),
        "target_state": target_state,
        "immediate_exit": immediate_exit,
        "hard_flags": list(dict.fromkeys(hard_flags)),
        "reasons": reasons[:8],
        "core": core,
        "risk": risk,
        "strategy": strategy,
        "tape": tape,
    }


def _v691_apply_state_machine(trade, snapshot):
    now_ts = num(snapshot.get("evaluated_ts")) or time.time()
    supervisor = trade.get("supervisor") or {}
    previous_state = supervisor.get("state")
    target = snapshot.get("target_state") or "HOLD"
    health = num(snapshot.get("health"))
    previous_health = num(supervisor.get("health")) if supervisor.get("health") is not None else health
    initialized = bool(supervisor.get("initialized"))
    transitioned = False
    old_state = previous_state

    if not initialized or previous_state not in V691_SEVERITY:
        supervisor.update({
            "initialized": True,
            "state": target,
            "pending_state": None,
            "pending_count": 0,
            "first_seen_ts": now_ts,
        })
        previous_state = target
        old_state = None
        # Avoid a restart flood. Initial severe conditions are still surfaced.
        transitioned = target in {"DEFENSIVE", "EXIT_WARNING"}
    elif target == previous_state:
        supervisor["pending_state"] = None
        supervisor["pending_count"] = 0
    else:
        if supervisor.get("pending_state") == target:
            supervisor["pending_count"] = int(num(supervisor.get("pending_count"))) + 1
        else:
            supervisor["pending_state"] = target
            supervisor["pending_count"] = 1

        worsening = V691_SEVERITY.get(target, 1) > V691_SEVERITY.get(previous_state, 1)
        required = V691_STATE_CONFIRMATIONS
        if target == "EXIT_WARNING":
            required = V691_EXIT_CONFIRMATIONS
            if snapshot.get("immediate_exit") or len(snapshot.get("hard_flags") or []) >= 2:
                required = 1
        elif not worsening:
            required = V691_STATE_CONFIRMATIONS

        if int(num(supervisor.get("pending_count"))) >= required:
            supervisor["state"] = target
            supervisor["pending_state"] = None
            supervisor["pending_count"] = 0
            transitioned = True

    current_state = supervisor.get("state") or target
    tape = snapshot.get("tape") or {}
    current_r = num(tape.get("current_r"))
    mfe = supervisor.get("mfe_r")
    mae = supervisor.get("mae_r")
    supervisor["mfe_r"] = current_r if mfe is None else max(num(mfe), current_r)
    supervisor["mae_r"] = current_r if mae is None else min(num(mae), current_r)
    supervisor["previous_health"] = previous_health
    supervisor["health"] = health
    supervisor["last_eval_ts"] = now_ts
    supervisor["target_state"] = target
    supervisor["components"] = {
        "core": round(num((snapshot.get("core") or {}).get("health")), 2),
        "strategy": round(num((snapshot.get("strategy") or {}).get("health")), 2),
        "risk": round(num((snapshot.get("risk") or {}).get("health")), 2),
        "tape": round(num((snapshot.get("tape") or {}).get("health")), 2),
    }
    supervisor["current_price"] = num(tape.get("price"))
    supervisor["current_r"] = current_r
    supervisor["reasons"] = list(snapshot.get("reasons") or [])[:6]
    supervisor["hard_flags"] = list(snapshot.get("hard_flags") or [])
    supervisor["core"] = {
        "directional_score": num((snapshot.get("core") or {}).get("directional_score")),
        "selected_direction": (snapshot.get("core") or {}).get("selected_direction"),
        "state": (snapshot.get("core") or {}).get("state"),
        "regime": (snapshot.get("core") or {}).get("regime"),
        "score_delta": num((snapshot.get("core") or {}).get("score_delta")),
    }
    supervisor["risk"] = {
        "decision": (snapshot.get("risk") or {}).get("decision"),
        "macro_score": num((snapshot.get("risk") or {}).get("macro_score")),
        "event_risk": (snapshot.get("risk") or {}).get("event_risk"),
        "btc_direction": ((snapshot.get("risk") or {}).get("btc") or {}).get("direction"),
        "btc_state": ((snapshot.get("risk") or {}).get("btc") or {}).get("state"),
        "btc_score": num(((snapshot.get("risk") or {}).get("btc") or {}).get("score")),
        "open_correlated": int(num((snapshot.get("risk") or {}).get("open_correlated"))),
        "recent_stop_count": int(num((snapshot.get("risk") or {}).get("recent_stop_count"))),
    }
    supervisor["strategy"] = {
        "consensus": (snapshot.get("strategy") or {}).get("consensus"),
        "confirm_count": int(num((snapshot.get("strategy") or {}).get("confirm_count"))),
        "wait_count": int(num((snapshot.get("strategy") or {}).get("wait_count"))),
        "reject_count": int(num((snapshot.get("strategy") or {}).get("reject_count"))),
        "statuses": (snapshot.get("strategy") or {}).get("statuses") or {},
    }
    supervisor["tape"] = {
        "oi5": (snapshot.get("tape") or {}).get("oi5"),
        "oi15": (snapshot.get("tape") or {}).get("oi15"),
        "price5": (snapshot.get("tape") or {}).get("price5"),
        "price15": (snapshot.get("tape") or {}).get("price15"),
    }

    # Capture only the first confirmed EXIT_WARNING. It is the shadow strategy's
    # counterfactual management exit; the control trade intentionally remains OPEN.
    if current_state == "EXIT_WARNING" and supervisor.get("shadow_exit_r") is None:
        supervisor["shadow_exit_ts"] = now_ts
        supervisor["shadow_exit_price"] = num(tape.get("price"))
        supervisor["shadow_exit_r"] = round(current_r, 4)
        supervisor["shadow_exit_reason"] = "; ".join((snapshot.get("reasons") or [])[:3])

    # V7.1.5 PROFIT-PROTECTION CHALLENGER (research-only):
    # Once >= +1.5R has actually been reached, capture a counterfactual profitable
    # exit only when the *confirmed/persistent* supervisor state is DEFENSIVE or
    # EXIT_WARNING AND Core itself has weakened. A transient downgrade cannot fire
    # this because current_state only changes after the existing confirmation state
    # machine. The control paper trade and any real MEXC position remain untouched.
    core_now = snapshot.get("core") or {}
    core_direction = core_now.get("selected_direction")
    core_state = str(core_now.get("state") or "").upper()
    core_health = num(core_now.get("health"))
    opposite_core = bool(core_direction and core_direction != trade.get("direction"))
    core_weakened = bool(
        opposite_core
        or core_state == "REJECT"
        or core_health <= V691_PROFIT_PROTECT_CORE_HEALTH_MAX
    )
    profit_protect_eligible = bool(
        current_r >= V691_PROFIT_PROTECT_MIN_R
        and current_state in {"DEFENSIVE", "EXIT_WARNING"}
        and core_weakened
    )
    supervisor["profit_protect_eligible"] = profit_protect_eligible
    if profit_protect_eligible and supervisor.get("profit_protect_exit_r") is None:
        supervisor["profit_protect_exit_ts"] = now_ts
        supervisor["profit_protect_exit_price"] = num(tape.get("price"))
        supervisor["profit_protect_exit_r"] = round(current_r, 4)
        supervisor["profit_protect_exit_state"] = current_state
        supervisor["profit_protect_exit_reason"] = (
            f">={V691_PROFIT_PROTECT_MIN_R:.2f}R + persistent {current_state} + Core weakened "
            f"(health={core_health:.1f}, state={core_state or 'N/A'}, direction={core_direction or 'N/A'})"
        )
        print(
            f"V7.1.5 PROFIT-PROTECT SHADOW: {trade.get('symbol')} {trade.get('direction')} "
            f"would exit {current_r:+.2f}R | {supervisor['profit_protect_exit_reason']}"
        )

    supervisor["state_changed"] = transitioned
    supervisor["old_state"] = old_state
    trade["supervisor"] = supervisor
    return transitioned, old_state, current_state


def v691_init_supervisor_lab():
    if not V68_DB_READY:
        return False

    statements = [
        """
        CREATE TABLE IF NOT EXISTS fh_trade_supervisor (
            signal_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_time TIMESTAMPTZ,
            entry_price DOUBLE PRECISION,
            entry_regime TEXT,
            current_state TEXT,
            current_health DOUBLE PRECISION,
            last_eval_time TIMESTAMPTZ,
            first_exit_warning_time TIMESTAMPTZ,
            shadow_exit_price DOUBLE PRECISION,
            shadow_exit_r DOUBLE PRECISION,
            actual_status TEXT,
            actual_final_r DOUBLE PRECISION,
            supervisor_final_r DOUBLE PRECISION,
            edge_r DOUBLE PRECISION,
            settled_at TIMESTAMPTZ,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_trade_supervisor_state
        ON fh_trade_supervisor (current_state, updated_at DESC)
        """,
        "ALTER TABLE fh_trade_supervisor ADD COLUMN IF NOT EXISTS profit_protect_exit_time TIMESTAMPTZ",
        "ALTER TABLE fh_trade_supervisor ADD COLUMN IF NOT EXISTS profit_protect_exit_price DOUBLE PRECISION",
        "ALTER TABLE fh_trade_supervisor ADD COLUMN IF NOT EXISTS profit_protect_exit_r DOUBLE PRECISION",
        "ALTER TABLE fh_trade_supervisor ADD COLUMN IF NOT EXISTS profit_protect_exit_state TEXT",
        "ALTER TABLE fh_trade_supervisor ADD COLUMN IF NOT EXISTS profit_protect_exit_reason TEXT",
        "ALTER TABLE fh_trade_supervisor ADD COLUMN IF NOT EXISTS profit_protect_final_r DOUBLE PRECISION",
        "ALTER TABLE fh_trade_supervisor ADD COLUMN IF NOT EXISTS profit_protect_edge_r DOUBLE PRECISION",
        """
        CREATE TABLE IF NOT EXISTS fh_trade_supervisor_snapshots (
            id BIGSERIAL PRIMARY KEY,
            snapshot_key TEXT UNIQUE NOT NULL,
            snapshot_ts BIGINT NOT NULL,
            snapshot_time TIMESTAMPTZ NOT NULL,
            signal_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            supervisor_state TEXT,
            target_state TEXT,
            health_score DOUBLE PRECISION,
            core_health DOUBLE PRECISION,
            strategy_health DOUBLE PRECISION,
            risk_health DOUBLE PRECISION,
            tape_health DOUBLE PRECISION,
            current_price DOUBLE PRECISION,
            current_r DOUBLE PRECISION,
            mfe_r DOUBLE PRECISION,
            mae_r DOUBLE PRECISION,
            entry_regime TEXT,
            current_regime TEXT,
            core_direction TEXT,
            core_signal_state TEXT,
            core_directional_score DOUBLE PRECISION,
            strategy_consensus TEXT,
            confirm_count INTEGER,
            wait_count INTEGER,
            reject_count INTEGER,
            macro_score DOUBLE PRECISION,
            event_risk TEXT,
            btc_direction TEXT,
            btc_state TEXT,
            btc_score DOUBLE PRECISION,
            oi5 DOUBLE PRECISION,
            oi15 DOUBLE PRECISION,
            price5 DOUBLE PRECISION,
            price15 DOUBLE PRECISION,
            state_changed BOOLEAN NOT NULL DEFAULT FALSE,
            reasons TEXT[],
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_trade_supervisor_snapshots_trade_time
        ON fh_trade_supervisor_snapshots (signal_id, snapshot_time DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_fh_trade_supervisor_snapshots_state_time
        ON fh_trade_supervisor_snapshots (supervisor_state, snapshot_time DESC)
        """,
    ]
    for statement in statements:
        if not _v68_db_execute(statement):
            return False

    _v68_state_set("trade_supervisor_version", {
        "version": V691_SUPERVISOR_VERSION,
        "mode": "SHADOW_ONLY",
        "interval_seconds": V691_SUPERVISOR_INTERVAL_SECONDS,
        "strategy_refresh_seconds": V691_STRATEGY_REFRESH_SECONDS,
        "snapshot_seconds": V691_SNAPSHOT_SECONDS,
    })
    return True


def _v691_compact_payload(snapshot, supervisor):
    strategy = snapshot.get("strategy") or {}
    core = snapshot.get("core") or {}
    risk = snapshot.get("risk") or {}
    tape = snapshot.get("tape") or {}
    return {
        "version": V691_SUPERVISOR_VERSION,
        "target_state": snapshot.get("target_state"),
        "hard_flags": snapshot.get("hard_flags") or [],
        "core": {
            "score": core.get("directional_score"),
            "direction": core.get("selected_direction"),
            "state": core.get("state"),
            "regime": core.get("regime"),
            "score_delta": core.get("score_delta"),
        },
        "risk": {
            "decision": risk.get("decision"),
            "macro_score": risk.get("macro_score"),
            "event_risk": risk.get("event_risk"),
            "btc": risk.get("btc"),
            "open_correlated": risk.get("open_correlated"),
            "recent_stop_count": risk.get("recent_stop_count"),
        },
        "strategy": {
            "consensus": strategy.get("consensus"),
            "confirm": strategy.get("confirm_count"),
            "wait": strategy.get("wait_count"),
            "reject": strategy.get("reject_count"),
            "statuses": strategy.get("statuses") or {},
        },
        "tape": {
            "current_r": tape.get("current_r"),
            "oi5": tape.get("oi5"),
            "oi15": tape.get("oi15"),
            "price5": tape.get("price5"),
            "price15": tape.get("price15"),
        },
        "reasons": (snapshot.get("reasons") or [])[:6],
        "shadow_exit_r": supervisor.get("shadow_exit_r"),
        "profit_protect_exit_r": supervisor.get("profit_protect_exit_r"),
        "profit_protect_exit_state": supervisor.get("profit_protect_exit_state"),
        "profit_protect_exit_reason": supervisor.get("profit_protect_exit_reason"),
    }


def _v691_store_supervisor(trade, snapshot, force_snapshot=False):
    if not V68_DB_READY:
        return False

    supervisor = trade.get("supervisor") or {}
    ts = int(num(snapshot.get("evaluated_ts")) or time.time())
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    entry_ts = num(trade.get("signal_time"))
    entry_dt = datetime.fromtimestamp(entry_ts, tz=timezone.utc) if entry_ts else None
    shadow_exit_ts = num(supervisor.get("shadow_exit_ts"))
    shadow_exit_dt = datetime.fromtimestamp(shadow_exit_ts, tz=timezone.utc) if shadow_exit_ts else None
    payload = _v691_compact_payload(snapshot, supervisor)

    _v68_db_execute(
        """
        INSERT INTO fh_trade_supervisor (
            signal_id, symbol, direction, entry_time, entry_price, entry_regime,
            current_state, current_health, last_eval_time,
            first_exit_warning_time, shadow_exit_price, shadow_exit_r, payload, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (signal_id) DO UPDATE SET
            current_state = EXCLUDED.current_state,
            current_health = EXCLUDED.current_health,
            last_eval_time = EXCLUDED.last_eval_time,
            first_exit_warning_time = COALESCE(fh_trade_supervisor.first_exit_warning_time, EXCLUDED.first_exit_warning_time),
            shadow_exit_price = COALESCE(fh_trade_supervisor.shadow_exit_price, EXCLUDED.shadow_exit_price),
            shadow_exit_r = COALESCE(fh_trade_supervisor.shadow_exit_r, EXCLUDED.shadow_exit_r),
            payload = EXCLUDED.payload,
            updated_at = NOW()
        """,
        (
            trade.get("signal_id"), trade.get("symbol"), trade.get("direction"),
            entry_dt, num(trade.get("entry")), snapshot.get("entry_regime"),
            supervisor.get("state"), num(supervisor.get("health")), dt,
            shadow_exit_dt, supervisor.get("shadow_exit_price"), supervisor.get("shadow_exit_r"),
            _v68_json(payload),
        ),
    )

    pp_ts = num(supervisor.get("profit_protect_exit_ts"))
    pp_dt = datetime.fromtimestamp(pp_ts, tz=timezone.utc) if pp_ts else None
    if supervisor.get("profit_protect_exit_r") is not None:
        _v68_db_execute(
            """UPDATE fh_trade_supervisor
               SET profit_protect_exit_time=COALESCE(profit_protect_exit_time,%s),
                   profit_protect_exit_price=COALESCE(profit_protect_exit_price,%s),
                   profit_protect_exit_r=COALESCE(profit_protect_exit_r,%s),
                   profit_protect_exit_state=COALESCE(profit_protect_exit_state,%s),
                   profit_protect_exit_reason=COALESCE(profit_protect_exit_reason,%s),
                   updated_at=NOW()
               WHERE signal_id=%s""",
            (pp_dt, supervisor.get("profit_protect_exit_price"), supervisor.get("profit_protect_exit_r"),
             supervisor.get("profit_protect_exit_state"), supervisor.get("profit_protect_exit_reason"), trade.get("signal_id")),
        )

    last_snapshot_ts = num(supervisor.get("last_snapshot_ts"))
    due = (ts - last_snapshot_ts) >= V691_SNAPSHOT_SECONDS
    state_changed = bool(supervisor.get("state_changed"))
    if not (force_snapshot or due or state_changed):
        return True

    bucket_ts = int(ts // max(60, V691_SNAPSHOT_SECONDS)) * max(60, V691_SNAPSHOT_SECONDS)
    key_material = f"{trade.get('signal_id')}|{bucket_ts}|{supervisor.get('state')}"
    snapshot_key = hashlib.sha1(key_material.encode("utf-8")).hexdigest()
    core = snapshot.get("core") or {}
    risk = snapshot.get("risk") or {}
    strategy = snapshot.get("strategy") or {}
    tape = snapshot.get("tape") or {}
    btc = risk.get("btc") or {}

    stored = _v68_db_execute(
        """
        INSERT INTO fh_trade_supervisor_snapshots (
            snapshot_key, snapshot_ts, snapshot_time, signal_id, symbol, direction,
            supervisor_state, target_state, health_score,
            core_health, strategy_health, risk_health, tape_health,
            current_price, current_r, mfe_r, mae_r,
            entry_regime, current_regime,
            core_direction, core_signal_state, core_directional_score,
            strategy_consensus, confirm_count, wait_count, reject_count,
            macro_score, event_risk, btc_direction, btc_state, btc_score,
            oi5, oi15, price5, price15,
            state_changed, reasons, payload
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (snapshot_key) DO NOTHING
        """,
        (
            snapshot_key, ts, dt, trade.get("signal_id"), trade.get("symbol"), trade.get("direction"),
            supervisor.get("state"), snapshot.get("target_state"), num(snapshot.get("health")),
            num(core.get("health")), num(strategy.get("health")), num(risk.get("health")), num(tape.get("health")),
            num(tape.get("price")), num(tape.get("current_r")), num(supervisor.get("mfe_r")), num(supervisor.get("mae_r")),
            snapshot.get("entry_regime"), snapshot.get("current_regime"),
            core.get("selected_direction"), core.get("state"), num(core.get("directional_score")),
            strategy.get("consensus"), int(num(strategy.get("confirm_count"))), int(num(strategy.get("wait_count"))), int(num(strategy.get("reject_count"))),
            num(risk.get("macro_score")), risk.get("event_risk"), btc.get("direction"), btc.get("state"), num(btc.get("score")),
            tape.get("oi5"), tape.get("oi15"), tape.get("price5"), tape.get("price15"),
            state_changed, [str(x) for x in (snapshot.get("reasons") or [])[:8]], _v68_json(payload),
        ),
    )
    if stored:
        supervisor["last_snapshot_ts"] = ts
    return bool(stored)


def _v691_supervisor_alert_message(trade, old_state, new_state):
    sup = trade.get("supervisor") or {}
    comp = sup.get("components") or {}
    risk = sup.get("risk") or {}
    strategy = sup.get("strategy") or {}
    reasons = sup.get("reasons") or []
    icon = {
        "STRONG_HOLD": "🟢",
        "HOLD": "✅",
        "CAUTION": "⚠️",
        "DEFENSIVE": "🟠",
        "EXIT_WARNING": "🚨",
    }.get(new_state, "🤖")

    if old_state:
        transition = f"{old_state} → {new_state}"
    else:
        transition = f"Initial state: {new_state}"

    if new_state == "EXIT_WARNING":
        view = "Shadow exit thesis triggered. Control paper trade remains unchanged."
    elif new_state == "DEFENSIVE":
        view = "Material deterioration. Supervisor would manage defensively in shadow."
    elif new_state == "CAUTION":
        view = "Trade thesis is weakening; monitor confirmation closely."
    elif new_state == "HOLD":
        view = "Trade thesis has recovered / remains intact."
    else:
        view = "Trade thesis is strongly supported."

    reason_text = "\n".join(f"• {x}" for x in reasons[:3]) or "• no major negative flag"
    shadow_line = ""
    if sup.get("shadow_exit_r") is not None:
        shadow_line = f"\nShadow exit captured: {num(sup.get('shadow_exit_r')):+.2f}R"

    return (
        f"{icon} FUTURESHUNTER V6.9.1 LIVE SUPERVISOR — SHADOW\n\n"
        f"{trade.get('symbol')} {trade.get('direction')}\n"
        f"{transition}\n"
        f"Health: {num(sup.get('health')):.0f}/100 | Current: {num(sup.get('current_r')):+.2f}R\n"
        f"Core {num(comp.get('core')):.0f} | Strategy {num(comp.get('strategy')):.0f} | Risk {num(comp.get('risk')):.0f} | Tape {num(comp.get('tape')):.0f}\n"
        f"Strategy: {strategy.get('consensus') or 'N/A'} "
        f"C/W/R {int(num(strategy.get('confirm_count')))}/{int(num(strategy.get('wait_count')))}/{int(num(strategy.get('reject_count')))}\n"
        f"Macro {num(risk.get('macro_score')):+.1f} | BTC {risk.get('btc_direction') or 'N/A'} {risk.get('btc_state') or 'N/A'} {num(risk.get('btc_score')):.1f}\n\n"
        f"{view}\n{reason_text}{shadow_line}\n\n"
        "Research advisory only — original paper stop/TP/expiry logic is untouched."
    )


def _v691_should_notify(old_state, new_state):
    important = {"CAUTION", "DEFENSIVE", "EXIT_WARNING"}
    if new_state in important or old_state in important:
        return True
    return False


def _v691_monitor_open_trades(trades, details, oi_metrics):
    now_ts = time.time()
    open_trades = [t for t in (trades or []) if t.get("status") == "OPEN"]
    if not open_trades:
        return 0

    evaluated = 0
    persist_needed = False
    for trade in open_trades:
        sup = trade.get("supervisor") or {}
        if now_ts - num(sup.get("last_eval_ts")) < V691_SUPERVISOR_INTERVAL_SECONDS:
            continue
        try:
            snapshot = _v691_evaluate_open_trade(trade, trades, details, oi_metrics)
            transitioned, old_state, new_state = _v691_apply_state_machine(trade, snapshot)
            sup = trade.get("supervisor") or {}
            force_snapshot = not bool(sup.get("last_snapshot_ts"))
            _v691_store_supervisor(trade, snapshot, force_snapshot=force_snapshot)
            evaluated += 1

            if transitioned and _v691_should_notify(old_state, new_state):
                send_telegram(_v691_supervisor_alert_message(trade, old_state, new_state))
                print(
                    f"V6.9.1 SUPERVISOR: {trade.get('symbol')} {trade.get('direction')} "
                    f"{old_state or 'INIT'} → {new_state} | health {num(sup.get('health')):.1f}"
                )
            persist_needed = True
        except Exception as error:
            print(f"V6.9.1 supervisor error {trade.get('symbol')}: {error}")

    if persist_needed:
        save_json(TRADES_FILE, trades)
    return evaluated


def _v691_settle_supervisor(trades):
    if not V68_DB_READY:
        return 0
    settled = 0
    changed = False
    for trade in trades or []:
        if trade.get("status") == "OPEN" or trade.get("v691_supervisor_settled"):
            continue
        supervisor = trade.get("supervisor") or {}
        if not supervisor.get("initialized"):
            continue

        actual_r = trade.get("final_r")
        if actual_r is None:
            supervisor_r = None
            edge_r = None
        else:
            shadow_exit_r = supervisor.get("shadow_exit_r")
            supervisor_r = num(shadow_exit_r) if shadow_exit_r is not None else num(actual_r)
            edge_r = supervisor_r - num(actual_r)

        ok = _v68_db_execute(
            """
            UPDATE fh_trade_supervisor
            SET actual_status = %s,
                actual_final_r = %s,
                supervisor_final_r = %s,
                edge_r = %s,
                settled_at = NOW(),
                updated_at = NOW()
            WHERE signal_id = %s
            """,
            (
                trade.get("status"),
                None if actual_r is None else num(actual_r),
                supervisor_r,
                edge_r,
                trade.get("signal_id"),
            ),
        )
        if ok:
            pp_exit_r = supervisor.get("profit_protect_exit_r")
            pp_final_r = num(pp_exit_r) if pp_exit_r is not None else (None if actual_r is None else num(actual_r))
            pp_edge_r = None if (pp_final_r is None or actual_r is None) else pp_final_r - num(actual_r)
            _v68_db_execute(
                """UPDATE fh_trade_supervisor
                   SET profit_protect_final_r=%s, profit_protect_edge_r=%s, updated_at=NOW()
                   WHERE signal_id=%s""",
                (pp_final_r, pp_edge_r, trade.get("signal_id")),
            )
            supervisor["profit_protect_final_r"] = pp_final_r
            supervisor["profit_protect_edge_r"] = pp_edge_r
            supervisor["actual_status"] = trade.get("status")
            supervisor["actual_final_r"] = actual_r
            supervisor["supervisor_final_r"] = supervisor_r
            supervisor["edge_r"] = edge_r
            trade["supervisor"] = supervisor
            trade["v691_supervisor_settled"] = True
            changed = True
            settled += 1

    if changed:
        save_json(TRADES_FILE, trades)
    return settled


def _v691_trade_summary_line(trade):
    sup = trade.get("supervisor") or {}
    state = sup.get("state") or "WARMING_UP"
    return (
        f"{trade.get('symbol')} {trade.get('direction')} | {state} "
        f"{num(sup.get('health')):.0f}/100 | {num(sup.get('current_r')):+.2f}R"
    )


def _v691_supervisor_summary_message():
    trades = load_json(TRADES_FILE, [])
    open_trades = [t for t in trades if t.get("status") == "OPEN"]
    open_trades.sort(
        key=lambda t: (
            -V691_SEVERITY.get((t.get("supervisor") or {}).get("state"), 1),
            num((t.get("supervisor") or {}).get("health")),
        )
    )

    lines = [_v691_trade_summary_line(t) for t in open_trades[:15]]
    stats_text = ""
    if V68_DB_READY:
        row = _v68_db_execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE actual_status IS NOT NULL),
                   COUNT(*) FILTER (WHERE shadow_exit_r IS NOT NULL),
                   COALESCE(SUM(actual_final_r) FILTER (WHERE actual_status IS NOT NULL), 0),
                   COALESCE(SUM(supervisor_final_r) FILTER (WHERE actual_status IS NOT NULL), 0),
                   COALESCE(SUM(edge_r) FILTER (WHERE actual_status IS NOT NULL), 0)
            FROM fh_trade_supervisor
            """,
            fetch="one",
        )
        if row:
            tracked, settled, exit_warnings, control_r, supervisor_r, edge_r = row
            stats_text = (
                f"\nTracked: {int(tracked or 0)} | settled: {int(settled or 0)} | "
                f"shadow exits: {int(exit_warnings or 0)}\n"
                f"Settled control {num(control_r):+.2f}R vs supervisor {num(supervisor_r):+.2f}R "
                f"(Δ {num(edge_r):+.2f}R)\n"
            )

    return (
        "🤖 FUTURESHUNTER V6.9.1 LIVE TRADE SUPERVISOR\n\n"
        "Mode: SHADOW / ADVISORY — control trades are NEVER altered\n"
        f"Open trades: {len(open_trades)}\n"
        + stats_text
        + ("\n" + "\n".join(lines) if lines else "\nNo open control paper trades.")
        + "\n\nUse /trade HYPE for the detailed live thesis on one market."
    )


def _v691_trade_detail_message(query):
    query = str(query or "").strip().upper()
    if query and not query.endswith("_USDT"):
        query += "_USDT"
    trades = load_json(TRADES_FILE, [])
    candidates = trades
    if query:
        candidates = [t for t in trades if str(t.get("symbol") or "").upper() == query]
    if not candidates:
        return f"🤖 No tracked paper trade found for {query or 'that symbol'}."

    candidates.sort(
        key=lambda t: (t.get("status") == "OPEN", num(t.get("signal_time"))),
        reverse=True,
    )
    trade = candidates[0]
    sup = trade.get("supervisor") or {}
    if not sup.get("initialized"):
        return (
            f"🤖 {trade.get('symbol')} {trade.get('direction')} is tracked, but V6.9.1 "
            "has not produced its first supervisor evaluation yet."
        )

    comp = sup.get("components") or {}
    core = sup.get("core") or {}
    risk = sup.get("risk") or {}
    strategy = sup.get("strategy") or {}
    tape = sup.get("tape") or {}
    reasons = sup.get("reasons") or []
    reason_text = "\n".join(f"• {x}" for x in reasons[:5]) or "• no major negative flags"
    shadow = "Not triggered"
    if sup.get("shadow_exit_r") is not None:
        shadow = f"{num(sup.get('shadow_exit_r')):+.2f}R @ {num(sup.get('shadow_exit_price')):.8g}"

    return (
        f"🤖 V6.9.1 TRADE SUPERVISOR — {trade.get('symbol')}\n\n"
        f"Control: {trade.get('direction')} {trade.get('status')} | entry {num(trade.get('entry')):.8g}\n"
        f"Supervisor: {sup.get('state')} | target {sup.get('target_state')} | health {num(sup.get('health')):.0f}/100\n"
        f"Current: {num(sup.get('current_price')):.8g} | {num(sup.get('current_r')):+.2f}R | "
        f"MFE {num(sup.get('mfe_r')):+.2f}R | MAE {num(sup.get('mae_r')):+.2f}R\n\n"
        f"Core {num(comp.get('core')):.0f}: {core.get('selected_direction') or 'N/A'} {core.get('state') or 'N/A'} "
        f"dir-score {num(core.get('directional_score')):.1f}\n"
        f"Strategy {num(comp.get('strategy')):.0f}: {strategy.get('consensus') or 'N/A'} "
        f"C/W/R {int(num(strategy.get('confirm_count')))}/{int(num(strategy.get('wait_count')))}/{int(num(strategy.get('reject_count')))}\n"
        f"Risk {num(comp.get('risk')):.0f}: {risk.get('decision') or 'N/A'} | macro {num(risk.get('macro_score')):+.1f} | "
        f"BTC {risk.get('btc_direction') or 'N/A'} {risk.get('btc_state') or 'N/A'} {num(risk.get('btc_score')):.1f}\n"
        f"Tape {num(comp.get('tape')):.0f}: OI5 {format_optional(tape.get('oi5'))} | "
        f"OI15 {format_optional(tape.get('oi15'))}\n\n"
        f"Why:\n{reason_text}\n\n"
        f"First shadow EXIT: {shadow}\n"
        "Original paper STOP/TP/expiry remains untouched."
    )


def _v715_profit_protect_summary_message():
    if not V68_DB_READY:
        return "V7.1.5 profit-protect: database unavailable"
    row = _v68_db_execute(
        """SELECT
             COUNT(*) FILTER (WHERE profit_protect_exit_r IS NOT NULL),
             COUNT(*) FILTER (WHERE profit_protect_exit_r IS NOT NULL AND profit_protect_edge_r IS NOT NULL),
             COALESCE(AVG(profit_protect_exit_r) FILTER (WHERE profit_protect_exit_r IS NOT NULL),0),
             COALESCE(SUM(profit_protect_edge_r) FILTER (WHERE profit_protect_edge_r IS NOT NULL),0),
             COALESCE(AVG(profit_protect_edge_r) FILTER (WHERE profit_protect_edge_r IS NOT NULL),0),
             COALESCE(SUM(actual_final_r) FILTER (WHERE profit_protect_exit_r IS NOT NULL AND profit_protect_edge_r IS NOT NULL),0),
             COALESCE(SUM(profit_protect_final_r) FILTER (WHERE profit_protect_exit_r IS NOT NULL AND profit_protect_edge_r IS NOT NULL),0)
           FROM fh_trade_supervisor""", fetch="one")
    if not row:
        return "V7.1.5 profit-protect: no data"
    return (
        "V7.1.5 PROFIT-PROTECT SHADOW\n"
        f"Triggered: {int(row[0] or 0)} | Settled triggers: {int(row[1] or 0)}\n"
        f"Average shadow exit: {num(row[2]):+.2f}R\n"
        f"Control R on settled triggers: {num(row[5]):+.2f}R\n"
        f"Shadow R on settled triggers: {num(row[6]):+.2f}R\n"
        f"Net edge vs control: {num(row[3]):+.2f}R | Avg edge/trigger: {num(row[4]):+.2f}R\n"
        "Research-only: original paper/live exits remain untouched."
    )


# Add live-supervisor commands without disturbing V6.9 / Risk Lab / Research Lab.
_V691_V69_HANDLE_TELEGRAM_COMMAND = handle_telegram_command


def handle_telegram_command(chat_id, text):
    parts = (text or "").strip().split()
    command = parts[0].lower() if parts else ""
    if command in {"/supervisor", "/livesupervisor", "/tradesupervisor"}:
        send_to_chat(chat_id, _v691_supervisor_summary_message())
        return
    if command in {"/profitprotect", "/ppshadow", "/shadow15"}:
        send_to_chat(chat_id, _v715_profit_protect_summary_message())
        return
    if command in {"/trade", "/tradehealth"}:
        query = parts[1] if len(parts) > 1 else ""
        if not query:
            send_to_chat(chat_id, "Usage: /trade HYPE")
        else:
            send_to_chat(chat_id, _v691_trade_detail_message(query))
        return
    return _V691_V69_HANDLE_TELEGRAM_COMMAND(chat_id, text)


# V6.8 main: same live decision engine as V6.7, with durable state / signal queue
# and Research Lab farming around it.
async def main():
    print()
    print("=" * 90)
    print("MEXC FUTURES HUNTER V6.9.1 — RESEARCH + RISK + STRATEGY + LIVE TRADE SUPERVISOR")
    print("=" * 90)

    if not V68_DB_READY:
        v68_init_database()
    v681_init_risk_lab()
    v69_init_strategy_lab()
    v691_init_supervisor_lab()
    _v73_init_equity_lab()
    _v75_init_equity_live_shadow()
    _v76_init_equity_agent_lab()
    _v77_init_optimizer_lab()
    _v68_restore_subscribers()
    _v68_restore_latest_signal_file()

    print()
    print("Telegram: " + ("CONNECTED" if telegram_ready() else "NOT CONFIGURED"))

    if not telegram_ready():
        print("Check your Telegram environment variables.")
        return

    start_telegram_command_thread()
    print("Telegram subscriber command listener: ACTIVE")

    history = _v68_restore_oi_history()
    if history is None:
        history = _V68_ORIGINAL_LOAD_JSON(HISTORY_FILE, {})
    elif history:
        print(f"V6.8 OI restore: recovered durable history for {len(history)} market(s).")

    alert_state = load_json(ALERT_STATE_FILE, {})
    trades = load_json(TRADES_FILE, [])

    imported = import_recent_logged_signals(trades)
    if imported:
        print(f"Imported {imported} recent local paper signal(s) into tracking.")

    if V77_DB_READY:
        v77_backfilled = _v77_sync_control_history(trades)
        print(f"V7.7 optimizer lab: historical control rows synchronized={v77_backfilled}")

    # Recover any setup that reached the durable queue but crashed before all
    # local side effects / Telegram delivery completed.
    _v68_retry_unsent_signals(trades, alert_state, max_count=25)

    resumed_ts = time.time()
    last_seen_ts = _v68_state_get("last_cycle_ts", None) if V68_DB_READY else None
    gap_seconds = _v68_record_downtime(last_seen_ts, resumed_ts)

    print()
    print("OI snapshots: every 60 seconds + durable market samples")
    print(f"Adaptive full scan: every {FULL_SCAN_INTERVAL // 60} minutes")
    print("States: WATCH → ARMED → ENTRY")
    print(f"Focus symbols: {', '.join(sorted(FOCUS_SYMBOLS)) or 'none'}")
    print("Research Lab: EVERY full-scan setup + LONG/SHORT factor vectors")
    print("Risk challenger: SHADOW ONLY — portfolio/regime throttling is being measured")
    print("Strategy ensemble: SHADOW ONLY — 8 explicit playbooks on closed candles")
    print("Live trade supervisor: SHADOW ONLY — continuously re-evaluates every OPEN trade")
    print(f"Equity Agent Ensemble V7.6: SHADOW research — top {V73_EQUITY_TOP_N} liquid stock/index futures, 6 specialist agents + regime-aware meta-agent")
    print(f"Equity Live V7.5 gate: {'ENABLED' if V75_EQUITY_LIVE_ENABLED else 'SHADOW FIRST'} — {V75_EQUITY_RISK_PCT*100:.2f}% risk if enabled, max {V75_EQUITY_MAX_OPEN} equity position")
    print(f"V7.7 Crypto/Metals Optimizer: SHADOW ONLY — max {V77_MAX_OPEN} portfolio positions, correlation/cost/managed-exit challengers")
    print("Durable signal queue: persist BEFORE Telegram")
    print("Paper/shadow/alert state: mirrored to Postgres")
    print(f"Paper trade expiry: {TRADE_EXPIRY_HOURS} hours")

    send_telegram(
        "✅ FuturesHunter V6.9.1 Research + Risk + Strategy + Live Supervisor is online.\n\n"
        "V6.7 trading logic is unchanged. Durable signal queue + persistent "
        "paper/shadow state + research farming are active.\n"
        "The V6.8.1 risk challenger, V6.9 strategy ensemble and V6.9.1 live trade supervisor are SHADOW-ONLY.\n"
        f"V7.6 Equity Agent Ensemble scans the top {V73_EQUITY_TOP_N} liquid stock/index futures with six specialist strategies and a regime-aware meta-agent.\n"
        f"V7.5 equity execution gate is {'ENABLED' if V75_EQUITY_LIVE_ENABLED else 'SHADOW-FIRST/OFF'}; if later enabled it risks at most {V75_EQUITY_RISK_PCT*100:.2f}% per equity trade.\n"
        f"V7.7 Crypto/Metals Optimizer is SHADOW-ONLY: max {V77_MAX_OPEN} realistic positions, correlation guard, cost-adjusted R, 25/25/50 managed exits and metals cross-confirmation.\n"
        "The supervisor watches OPEN trades but never closes/resizes the control paper position.\n\n"
        "Try /optimizer, /agents, /equitylive, /equity, /supervisor, /trade HYPE, /research, /risklab, /strategylab, /dbstatus or /macro."
    )

    if gap_seconds > V68_DOWNTIME_THRESHOLD_SECONDS:
        send_telegram(
            "♻️ FuturesHunter recovered from downtime.\n\n"
            f"Detected gap: ~{gap_seconds / 60:.1f} minutes.\n"
            "Existing queued signals/state were preserved. Candle closes will be "
            "backfilled for research; historical OI/funding cannot be reconstructed exactly."
        )

    next_full_scan = 0.0
    next_unsent_retry = time.time() + V68_RETRY_UNSENT_SECONDS
    gap_backfilled = not bool(gap_seconds)

    while True:
        cycle_start = time.time()

        try:
            print()
            print(f"[{local_time()}] Updating market + OI data...")

            scan_symbols = await get_top_symbols()
            detail_symbols = list(scan_symbols)
            for _open_trade in trades:
                if _open_trade.get("status") != "OPEN":
                    continue
                _sym = str(_open_trade.get("symbol") or "")
                if _sym and _sym not in detail_symbols:
                    detail_symbols.append(_sym)
            symbols = detail_symbols
            details = await get_detailed_tickers(detail_symbols)

            if not gap_backfilled and last_seen_ts:
                _v68_backfill_gap_market_samples(
                    symbols,
                    float(last_seen_ts),
                    resumed_ts,
                )
                gap_backfilled = True

            _v68_persist_market_samples(details, source="LIVE")
            oi_metrics = update_oi_history(history, details)

            now = time.time()
            if V68_DB_READY:
                _v68_state_set("last_cycle_ts", now)
                _v68_state_set("last_symbols", list(symbols))

            # Track both actual paper signals and rejected/developing shadow trades.
            track_open_trades(trades)

            settled_risk = _v681_settle_risk_challenger(trades)
            if settled_risk:
                print(f"V6.8.1 Risk Lab: settled {settled_risk} challenger outcome(s).")

            settled_strategy = _v69_settle_strategy_ensemble(trades)
            if settled_strategy:
                print(f"V6.9 Strategy Lab: settled {settled_strategy} ensemble outcome(s).")

            settled_supervisor = _v691_settle_supervisor(trades)
            if settled_supervisor:
                print(f"V6.9.1 Live Supervisor: settled {settled_supervisor} management outcome(s).")

            supervised = _v691_monitor_open_trades(trades, details, oi_metrics)
            if supervised:
                print(f"V6.9.1 Live Supervisor: refreshed {supervised} open trade(s).")

            v77_changed = _v77_sync_optimizer_shadow(trades)
            if v77_changed:
                print(f"V7.7 Optimizer Lab: updated {v77_changed} exact portfolio shadow(s).")

            track_shadow_trades()

            settled_equity = _v73_settle_equity_shadow()
            if settled_equity:
                print(f"V7.3 Equity Lab: settled {settled_equity} shadow outcome(s).")
            _v75_sync_equity_candidate_outcomes()
            _v76_sync_agent_outcomes()

            if now >= next_unsent_retry:
                _v68_retry_unsent_signals(
                    trades,
                    alert_state,
                    max_count=6,
                )
                next_unsent_retry = now + V68_RETRY_UNSENT_SECONDS

            if now >= next_full_scan:
                equity_stats = _v76_scan_equity_agents()
                if equity_stats.get("agent_signals") or equity_stats.get("meta_candidates"):
                    print(
                        f"V7.6 Equity Agents: {equity_stats.get('agent_signals', 0)} new agent shadow(s), "
                        f"{equity_stats.get('meta_candidates', 0)} meta candidate(s), "
                        f"{equity_stats.get('would_take', 0)} WOULD-TAKE"
                    )

                best = run_full_scan(
                    scan_symbols,
                    details,
                    oi_metrics,
                )

                next_full_scan = now + FULL_SCAN_INTERVAL
                if V68_DB_READY:
                    _v68_state_set("last_full_scan_ts", now)

                if best is not None:
                    if should_send_alert(best, alert_state, trades):
                        # Shadow-only portfolio/regime challenger. The control trade
                        # is still taken exactly as V6.8 would take it.
                        risk_shadow = _v681_evaluate_risk_challenger(best, trades)
                        risk_row_id = _v681_store_risk_decision(risk_shadow)
                        if risk_row_id:
                            risk_shadow["row_id"] = risk_row_id
                        best["risk_challenger"] = risk_shadow
                        print(
                            f"V6.8.1 RISK SHADOW: {best['symbol']} {best['direction']} "
                            f"→ {risk_shadow['decision']} ({risk_shadow['size_multiplier']:.1f}x)"
                            + (
                                " — " + "; ".join(risk_shadow.get("reasons", [])[:2])
                                if risk_shadow.get("reasons") else ""
                            )
                        )

                        strategy_shadow = _v69_evaluate_strategy_ensemble(best)
                        strategy_row_id = _v69_store_ensemble(best, strategy_shadow)
                        if strategy_row_id:
                            strategy_shadow["row_id"] = strategy_row_id
                        best["strategy_ensemble"] = strategy_shadow
                        print(
                            f"V6.9 STRATEGY SHADOW: {best['symbol']} {best['direction']} "
                            f"→ {strategy_shadow.get('consensus')} "
                            f"C/W/R {strategy_shadow.get('confirm_count', 0)}/"
                            f"{strategy_shadow.get('wait_count', 0)}/"
                            f"{strategy_shadow.get('reject_count', 0)}"
                        )

                        message = (
                            build_alert_message(best)
                            .replace("V6.7 MACROHUNTER", "V6.9.1 RESEARCH LAB")
                            .replace("Validate V6.6", "Validate V6.9.1")
                        )

                        if V68_DB_READY:
                            queued = _v68_queue_signal(best, message)
                            if queued:
                                signal_key, signal_ts, stored_message, payload, already_sent, side_done = queued
                                if not side_done:
                                    _v68_apply_signal_side_effects(
                                        signal_key,
                                        stored_message,
                                        payload if isinstance(payload, dict) else best,
                                        trades,
                                        alert_state,
                                    )

                                if not already_sent:
                                    sent = send_telegram(stored_message)
                                    _v68_mark_signal_delivery(
                                        signal_key,
                                        sent,
                                        "" if sent else "initial Telegram delivery failed",
                                    )
                                    if sent:
                                        print("✅ Durable ENTRY persisted + Telegram broadcast.")
                                    else:
                                        print("⚠️ ENTRY persisted; Telegram failed and will be retried.")
                                else:
                                    print("Durable signal already delivered; duplicate Telegram suppressed.")
                            else:
                                # Database had a transient issue. Preserve old V6.7 behavior
                                # rather than dropping a valid setup.
                                sent = send_telegram(message)
                                if sent:
                                    save_latest_signal(message, best)
                                    log_signal(best)
                                    create_paper_trade(best, trades)
                                    record_alert_state(best, alert_state)
                                print("⚠️ DB degraded during signal; local fallback path used.")
                        else:
                            # Graceful fallback if DATABASE_URL isn't configured yet.
                            sent = send_telegram(message)
                            if sent:
                                save_latest_signal(message, best)
                                log_signal(best)
                                create_paper_trade(best, trades)
                                record_alert_state(best, alert_state)
                                print("✅ Telegram ENTRY broadcast (local fallback mode).")
                            else:
                                print("⚠️ Telegram alert failed in local fallback mode.")
                    else:
                        # Paper notification/ledger suppression must not silently
                        # suppress V7 live evaluation. Only bridge when the block
                        # is specifically an already-open paper trade; ordinary
                        # alert cooldown/duplicate suppression remains unchanged.
                        paper_blocked = has_open_trade_for_symbol(trades, best.get("symbol"))
                        bridge_due = False
                        bridge_due_fn = globals().get("_v71_live_bridge_due")
                        if paper_blocked and callable(bridge_due_fn):
                            bridge_due = bool(bridge_due_fn(best))

                        if paper_blocked and bridge_due:
                            attach_context = globals().get("_v71_attach_live_context")
                            live_bridge = globals().get("_v71_evaluate_live_without_paper")
                            if callable(attach_context) and callable(live_bridge):
                                attach_context(best, trades)
                                bridge_key = _v68_signal_key(best, now)
                                print(
                                    f"V7.4.3 LIVE BRIDGE: paper trade blocks {best.get('symbol')} "
                                    "paper alert, but live candidate will still be evaluated"
                                )
                                live_bridge(best, trades, bridge_key)
                                mark_bridge = globals().get("_v71_mark_live_bridge")
                                if callable(mark_bridge):
                                    mark_bridge(best)
                        else:
                            print(
                                "ENTRY setup found, but duplicate/open-trade "
                                "alert suppressed."
                            )

                print_stats(trades)

        except Exception as error:
            print()
            print(f"MAIN LOOP ERROR: {error}")

        elapsed = time.time() - cycle_start
        sleep_for = max(5, OI_UPDATE_INTERVAL - elapsed)
        await asyncio.sleep(sleep_for)




# ============================================================
# V7.3 EQUITY / STOCK FUTURES LAB — RESEARCH FEED FOR V7.5
# ============================================================
# Purpose:
# - keep V6.9.1 crypto/control logic untouched
# - discover MEXC stock/index futures separately
# - test a stock-specific 5m cash-open ORB model anchored to 09:30 New York
# - record/settle its own paper outcomes in a dedicated Postgres table
# - V7.3 never executes directly; qualifying signals are handed to the separate V7.5 gate
#
# MEXC Stock Futures can trade outside the underlying US cash session.  This
# research model intentionally uses the underlying 09:30 ET cash open as a
# price-discovery anchor, then expires the test at 16:00 ET the same day.

V73_EQUITY_VERSION = "7.3-equity-orb-shadow-1"
V73_EQUITY_SHADOW_ENABLED = os.getenv("V73_EQUITY_SHADOW_ENABLED", "true").lower() == "true"
V73_EQUITY_TOP_N = int(os.getenv("V73_EQUITY_TOP_N", "30"))
V73_EQUITY_MIN_TURNOVER = float(os.getenv("V73_EQUITY_MIN_TURNOVER", "500000"))
V73_EQUITY_MAX_SPREAD_PCT = float(os.getenv("V73_EQUITY_MAX_SPREAD_PCT", "0.25"))
V73_EQUITY_ORB_WINDOW_MINUTES = int(os.getenv("V73_EQUITY_ORB_WINDOW_MINUTES", "180"))
V73_EQUITY_MIN_SCORE = float(os.getenv("V73_EQUITY_MIN_SCORE", "72"))
V73_EQUITY_MIN_RV = float(os.getenv("V73_EQUITY_MIN_RV", "1.00"))
V73_EQUITY_MIN_ORB_ATR = float(os.getenv("V73_EQUITY_MIN_ORB_ATR", "0.30"))
V73_EQUITY_MAX_ORB_ATR = float(os.getenv("V73_EQUITY_MAX_ORB_ATR", "2.25"))
V73_EQUITY_MAX_BREAKOUT_EXTENSION_ATR = float(os.getenv("V73_EQUITY_MAX_BREAKOUT_EXTENSION_ATR", "0.80"))
V73_EQUITY_MAX_RISK_PCT = float(os.getenv("V73_EQUITY_MAX_RISK_PCT", "3.50"))
V73_EQUITY_NOTIFY = os.getenv("V73_EQUITY_NOTIFY", "false").lower() == "true"
V73_EQUITY_DB_READY = False
V73_EQUITY_META_CACHE_SECONDS = int(os.getenv("V73_EQUITY_META_CACHE_SECONDS", "3600"))
V73_EQUITY_SYMBOL_CACHE = set()
V73_EQUITY_META_CACHE = {}
V73_EQUITY_META_CACHE_TS = 0.0

# Exact bases are a fail-safe fallback. Primary discovery comes from MEXC
# contract metadata so newly-listed stock futures do not require a code deploy.
# The STOCK suffix remains auto-detected, and operators can extend the set via
# V73_EQUITY_SYMBOLS="AAPL_USDT,NVDA_USDT,..." without a code deploy.
V73_EQUITY_BASES = {
    "AAPL","MSFT","NVDA","TSLA","AMZN","META","GOOG","GOOGL","AMD","AVGO",
    "NFLX","PLTR","COIN","MSTR","HOOD","RDDT","BABA","TSM","QCOM","INTC",
    "MU","ARM","SMCI","ORCL","CRM","ADBE","CSCO","SNOW","CRWD","IONQ",
    "RKLB","ONDS","CVNA","MELI","JPM","BAC","GS","V","MA","PYPL","SQ",
    "XOM","CVX","COP","MCD","DIS","LLY","UNH","NKE","WMT","COST","GE",
    "ACN","IBM","UBER","ABNB","SHOP","SPOT","PANW","APP","DELL","MRVL",
    "QQQ","SPY","IWM","SOXL","SOXS","TQQQ","SQQQ","NAS100","US30","SP500",
    "VST","UPST","FCX","BLK","AXP","CCL","CMCSA","MAR","FOXA","BX","CRDO","AXTI",
    "RTX","NIO","AVAV","SMR","AEHR","TSLL","NVDL","RAM","POET","SPCX","DRAM","XLK",
    "BRKB","HD","CCJ","DDOG",
}


def _v73_refresh_equity_symbol_cache(force=False):
    global V73_EQUITY_SYMBOL_CACHE, V73_EQUITY_META_CACHE, V73_EQUITY_META_CACHE_TS
    now = time.time()
    if (not force and V73_EQUITY_SYMBOL_CACHE and
            now - V73_EQUITY_META_CACHE_TS < max(60, V73_EQUITY_META_CACHE_SECONDS)):
        return V73_EQUITY_SYMBOL_CACHE
    try:
        response = requests.get(f"{MEXC_REST}/api/v1/contract/detail", timeout=12)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data") or [] if payload.get("success") else []
        discovered = set()
        meta = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").upper().strip()
            if not symbol.endswith("_USDT"):
                continue
            plates = {str(x or "").lower() for x in (row.get("conceptPlate") or [])}
            base = symbol[:-5]
            if "mc-trade-zone-stock" in plates or "STOCK" in base:
                discovered.add(symbol)
                meta[symbol] = {
                    "display_name": str(row.get("displayNameEn") or row.get("displayName") or symbol),
                    "base_name": str(row.get("baseCoinName") or row.get("baseCoin") or base),
                    "api_allowed": bool(row.get("apiAllowed", False)),
                    "state": int(row.get("state") or 0),
                }
        discovered.update(_v73_extra_equity_symbols())
        if discovered:
            V73_EQUITY_SYMBOL_CACHE = discovered
            V73_EQUITY_META_CACHE = meta
            V73_EQUITY_META_CACHE_TS = now
            print(f"V7.5 Equity universe: discovered {len(discovered)} USDT stock/index future(s) from MEXC metadata")
    except Exception as error:
        print(f"V7.5 Equity metadata discovery warning: {type(error).__name__}: {error}")
    return V73_EQUITY_SYMBOL_CACHE


def _v73_extra_equity_symbols():
    raw = os.getenv("V73_EQUITY_SYMBOLS", "")
    values = set()
    for token in raw.split(","):
        token = token.strip().upper()
        if not token:
            continue
        if not token.endswith("_USDT"):
            token = token + "_USDT"
        values.add(token)
    return values


def _v73_is_equity_symbol(symbol):
    symbol = str(symbol or "").upper().strip()
    if not symbol.endswith("_USDT"):
        return False
    if symbol in _v73_extra_equity_symbols() or symbol in V73_EQUITY_SYMBOL_CACHE:
        return True
    base = symbol[:-5]
    if "STOCK" in base:
        return True
    return base in V73_EQUITY_BASES


def _v73_init_equity_lab():
    global V73_EQUITY_DB_READY
    if not V68_DB_READY or not V73_EQUITY_SHADOW_ENABLED:
        V73_EQUITY_DB_READY = False
        return False
    table_sql = """
    CREATE TABLE IF NOT EXISTS fh_v73_equity_shadow (
        id BIGSERIAL PRIMARY KEY,
        source_key TEXT UNIQUE NOT NULL,
        strategy_version TEXT NOT NULL,
        symbol TEXT NOT NULL,
        session_date TEXT NOT NULL,
        direction TEXT NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        entry DOUBLE PRECISION NOT NULL,
        stop DOUBLE PRECISION NOT NULL,
        risk DOUBLE PRECISION NOT NULL,
        tp1 DOUBLE PRECISION NOT NULL,
        tp2 DOUBLE PRECISION NOT NULL,
        tp3 DOUBLE PRECISION NOT NULL,
        signal_time DOUBLE PRECISION NOT NULL,
        tracking_start DOUBLE PRECISION NOT NULL,
        expiry_ts DOUBLE PRECISION NOT NULL,
        last_checked DOUBLE PRECISION NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'OPEN',
        tp1_hit BOOLEAN NOT NULL DEFAULT FALSE,
        tp2_hit BOOLEAN NOT NULL DEFAULT FALSE,
        tp3_hit BOOLEAN NOT NULL DEFAULT FALSE,
        tp1_time DOUBLE PRECISION,
        tp2_time DOUBLE PRECISION,
        tp3_time DOUBLE PRECISION,
        closed_time DOUBLE PRECISION,
        final_r DOUBLE PRECISION,
        orb_high DOUBLE PRECISION,
        orb_low DOUBLE PRECISION,
        orb_atr DOUBLE PRECISION,
        relative_volume DOUBLE PRECISION,
        spread_pct DOUBLE PRECISION,
        event_risk TEXT,
        reasons JSONB,
        payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        settled_at TIMESTAMPTZ
    )
    """
    ok = _v68_db_execute(table_sql)
    if ok:
        _v68_db_execute("CREATE INDEX IF NOT EXISTS idx_fh_v73_equity_shadow_status ON fh_v73_equity_shadow(status)")
        _v68_db_execute("CREATE INDEX IF NOT EXISTS idx_fh_v73_equity_shadow_symbol_date ON fh_v73_equity_shadow(symbol, session_date)")
    V73_EQUITY_DB_READY = bool(ok)
    return V73_EQUITY_DB_READY


def _v73_fetch_equity_tickers():
    """Return the top liquid MEXC USDT stock/index futures, isolated from crypto Core."""
    try:
        _v73_refresh_equity_symbol_cache()
        response = requests.get(f"{MEXC_REST}/api/v1/contract/ticker", timeout=10)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            return []
        data = payload.get("data") or []
        if isinstance(data, dict):
            data = [data]
        rows = []
        for ticker in data:
            symbol = str(ticker.get("symbol") or "").upper()
            if not _v73_is_equity_symbol(symbol):
                continue
            turnover = num(ticker.get("amount24"))
            if turnover < V73_EQUITY_MIN_TURNOVER:
                continue
            bid = num(ticker.get("bid1")); ask = num(ticker.get("ask1"))
            spread = 999.0
            if bid > 0 and ask > 0:
                spread = ((ask - bid) / ((ask + bid) / 2.0)) * 100.0
            row = dict(ticker)
            row["_turnover"] = turnover
            row["_spread_pct"] = spread
            rows.append(row)
        rows.sort(key=lambda x: x.get("_turnover", 0.0), reverse=True)
        return rows[:max(1, V73_EQUITY_TOP_N)]
    except Exception as error:
        print(f"V7.3 Equity Lab ticker discovery warning: {type(error).__name__}: {error}")
        return []


def _v73_session_bounds(reference_ts):
    if ZoneInfo is None:
        return None
    ny = ZoneInfo("America/New_York")
    dt_ny = datetime.fromtimestamp(reference_ts, tz=timezone.utc).astimezone(ny)
    # No new US cash-open ORB on Saturday/Sunday. MEXC may still quote the
    # contract, but that is deliberately outside this first stock strategy.
    if dt_ny.weekday() >= 5:
        return None
    open_ny = datetime(dt_ny.year, dt_ny.month, dt_ny.day, 9, 30, tzinfo=ny)
    close_ny = datetime(dt_ny.year, dt_ny.month, dt_ny.day, 16, 0, tzinfo=ny)
    open_ts = open_ny.astimezone(timezone.utc).timestamp()
    close_ts = close_ny.astimezone(timezone.utc).timestamp()
    return {
        "session_date": open_ny.strftime("%Y-%m-%d"),
        "open_ts": open_ts,
        "entry_start_ts": open_ts + 5 * 60,
        "entry_end_ts": open_ts + max(5, V73_EQUITY_ORB_WINDOW_MINUTES) * 60,
        "close_ts": close_ts,
    }


def _v73_equity_signal(symbol, ticker):
    context = _v69_market_context(symbol)
    if context is None:
        return None

    closed5, latest5, _ = _v69_rows(context.get("5m"))
    closed15, latest15, _ = _v69_rows(context.get("15m"))
    closed1h, latest1h, _ = _v69_rows(context.get("1h"))
    if closed5 is None or latest5 is None or latest15 is None or latest1h is None:
        return None

    latest_start = normalize_candle_time(latest5.get("time"))
    latest_close_ts = latest_start + 5 * 60
    bounds = _v73_session_bounds(latest_close_ts)
    if bounds is None:
        return None
    if not (bounds["entry_start_ts"] <= latest_close_ts <= bounds["entry_end_ts"]):
        return None

    opening = closed5[
        (closed5["time"] >= bounds["open_ts"]) &
        (closed5["time"] < bounds["open_ts"] + 5 * 60)
    ]
    if opening.empty:
        return None
    opening = opening.iloc[0]
    orb_high = num(opening.get("high")); orb_low = num(opening.get("low"))
    orb_range = orb_high - orb_low
    atr5 = max(num(latest5.get("atr14")), 1e-12)
    orb_atr = orb_range / atr5
    close5 = num(latest5.get("close"))
    if close5 > orb_high:
        direction = "LONG"
        extension_atr = (close5 - orb_high) / atr5
    elif close5 < orb_low:
        direction = "SHORT"
        extension_atr = (orb_low - close5) / atr5
    else:
        return None

    spread = num(ticker.get("_spread_pct"))
    rv5 = num(latest5.get("relative_volume"))
    loc5 = num(latest5.get("close_location"))
    rsi15 = num(latest15.get("rsi14"))
    adx15 = num(latest15.get("adx14"))
    macd5 = num(latest5.get("macd_hist"))
    close15 = num(latest15.get("close")); vwap15 = num(latest15.get("vwap20")); ema15 = num(latest15.get("ema20"))
    close1h = num(latest1h.get("close")); ema20_1h = num(latest1h.get("ema20")); ema50_1h = num(latest1h.get("ema50"))

    hard = []
    if spread > V73_EQUITY_MAX_SPREAD_PCT:
        hard.append(f"spread {spread:.3f}% > {V73_EQUITY_MAX_SPREAD_PCT:.3f}%")
    if rv5 < V73_EQUITY_MIN_RV:
        hard.append(f"breakout relative volume {rv5:.2f} < {V73_EQUITY_MIN_RV:.2f}")
    if not (V73_EQUITY_MIN_ORB_ATR <= orb_atr <= V73_EQUITY_MAX_ORB_ATR):
        hard.append(f"opening range {orb_atr:.2f} ATR outside {V73_EQUITY_MIN_ORB_ATR:.2f}-{V73_EQUITY_MAX_ORB_ATR:.2f}")
    if extension_atr > V73_EQUITY_MAX_BREAKOUT_EXTENSION_ATR:
        hard.append(f"breakout extended {extension_atr:.2f} ATR beyond ORB")

    event_risk = str((get_macro_snapshot() or {}).get("event_risk") or "LOW").upper()
    if event_risk == "EXTREME":
        hard.append("EXTREME scheduled-event risk")

    # Full opposite-side ORB invalidation.  This is deliberately conservative
    # for the first research version; later data can test midpoint/ATR stops.
    entry = close5
    stop = orb_low if direction == "LONG" else orb_high
    risk = abs(entry - stop)
    risk_pct = risk / max(entry, 1e-12) * 100.0
    if risk <= 0 or risk_pct > V73_EQUITY_MAX_RISK_PCT:
        hard.append(f"ORB stop risk {risk_pct:.2f}% > {V73_EQUITY_MAX_RISK_PCT:.2f}%")
    if hard:
        return None

    score = 35.0  # valid ORB break is mandatory
    reasons = ["5m close broke the 09:30 ET cash-session opening range"]

    if rv5 >= 1.50:
        score += 12; reasons.append(f"strong breakout volume {rv5:.2f}x")
    elif rv5 >= 1.20:
        score += 9; reasons.append(f"healthy breakout volume {rv5:.2f}x")
    else:
        score += 5

    if direction == "LONG":
        if close15 > vwap15: score += 10; reasons.append("15m above VWAP")
        if close15 > ema15: score += 8; reasons.append("15m above EMA20")
        if close1h > ema20_1h and ema20_1h >= ema50_1h: score += 12; reasons.append("1h trend aligned")
        if 52 <= rsi15 <= 78: score += 8; reasons.append(f"15m RSI supportive {rsi15:.0f}")
        if macd5 > 0: score += 7
        if loc5 >= 0.65: score += 5
    else:
        if close15 < vwap15: score += 10; reasons.append("15m below VWAP")
        if close15 < ema15: score += 8; reasons.append("15m below EMA20")
        if close1h < ema20_1h and ema20_1h <= ema50_1h: score += 12; reasons.append("1h trend aligned")
        if 22 <= rsi15 <= 48: score += 8; reasons.append(f"15m RSI supportive {rsi15:.0f}")
        if macd5 < 0: score += 7
        if loc5 <= 0.35: score += 5

    if adx15 >= 18: score += 3
    if spread <= 0.10: score += 2
    score = min(100.0, round(score, 1))
    if score < V73_EQUITY_MIN_SCORE:
        return None

    if direction == "LONG":
        tp1 = entry + risk; tp2 = entry + 2 * risk; tp3 = entry + 3 * risk
    else:
        tp1 = entry - risk; tp2 = entry - 2 * risk; tp3 = entry - 3 * risk

    source_key = hashlib.sha256(
        f"{V73_EQUITY_VERSION}|{symbol}|{bounds['session_date']}".encode("utf-8")
    ).hexdigest()[:40]
    payload = {
        "version": V73_EQUITY_VERSION,
        "symbol": symbol,
        "session_date": bounds["session_date"],
        "direction": direction,
        "score": score,
        "entry": entry,
        "stop": stop,
        "risk": risk,
        "risk_pct": risk_pct,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "signal_time": latest_close_ts,
        "tracking_start": latest_close_ts,
        "expiry_ts": bounds["close_ts"],
        "orb_high": orb_high,
        "orb_low": orb_low,
        "orb_atr": orb_atr,
        "extension_atr": extension_atr,
        "relative_volume": rv5,
        "spread_pct": spread,
        "event_risk": event_risk,
        "reasons": reasons,
    }
    payload["source_key"] = source_key
    return payload


def _v73_store_equity_signal(signal):
    if not V73_EQUITY_DB_READY or not signal:
        return False
    row = _v68_db_execute(
        """INSERT INTO fh_v73_equity_shadow
        (source_key,strategy_version,symbol,session_date,direction,score,entry,stop,risk,tp1,tp2,tp3,
         signal_time,tracking_start,expiry_ts,last_checked,status,orb_high,orb_low,orb_atr,
         relative_volume,spread_pct,event_risk,reasons,payload)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT(source_key) DO NOTHING RETURNING id""",
        (
            signal["source_key"],V73_EQUITY_VERSION,signal["symbol"],signal["session_date"],signal["direction"],
            signal["score"],signal["entry"],signal["stop"],signal["risk"],signal["tp1"],signal["tp2"],signal["tp3"],
            signal["signal_time"],signal["tracking_start"],signal["expiry_ts"],signal["tracking_start"]-1,
            signal["orb_high"],signal["orb_low"],signal["orb_atr"],signal["relative_volume"],signal["spread_pct"],
            signal["event_risk"],json.dumps(signal.get("reasons") or []),json.dumps(_clean_json_value(signal)),
        ), fetch="one"
    )
    return bool(row)


def _v73_scan_equity_shadow():
    if not V73_EQUITY_SHADOW_ENABLED or not V73_EQUITY_DB_READY:
        return 0
    tickers = _v73_fetch_equity_tickers()
    created = 0
    if tickers:
        print(f"V7.3 Equity Lab: scanning {len(tickers)} liquid stock/index future(s) — SHADOW ONLY")
    for ticker in tickers:
        symbol = str(ticker.get("symbol") or "")
        try:
            signal = _v73_equity_signal(symbol, ticker)
            if signal:
                is_new = _v73_store_equity_signal(signal)
                if is_new:
                    created += 1
                if not is_new:
                    time.sleep(0.05)
                    continue
                msg = (
                    f"📈 V7.3 EQUITY SHADOW\n\n"
                    f"{signal['symbol']} {signal['direction']}\n"
                    f"Cash-open ORB score: {signal['score']:.0f}/100\n"
                    f"Entry {signal['entry']:.6g} | Stop {signal['stop']:.6g}\n"
                    f"TP1 {signal['tp1']:.6g} | TP2 {signal['tp2']:.6g} | TP3 {signal['tp3']:.6g}\n"
                    f"ORB {signal['orb_atr']:.2f} ATR | RV {signal['relative_volume']:.2f}x | spread {signal['spread_pct']:.3f}%\n\n"
                    "V7.3 control signal — V7.5 evaluates any live action separately."
                )
                print(msg.replace("\n", " | "))
                if V73_EQUITY_NOTIFY:
                    send_telegram(msg)
        except Exception as error:
            print(f"V7.3 Equity Lab {symbol} warning: {type(error).__name__}: {error}")
        time.sleep(0.05)
    return created


def _v73_open_rows():
    if not V73_EQUITY_DB_READY:
        return []
    rows = _v68_db_execute(
        """SELECT id,source_key,symbol,direction,entry,stop,risk,tp1,tp2,tp3,signal_time,
        tracking_start,expiry_ts,last_checked,status,tp1_hit,tp2_hit,tp3_hit,tp1_time,tp2_time,tp3_time
        FROM fh_v73_equity_shadow WHERE status='OPEN' ORDER BY signal_time""", fetch="all"
    ) or []
    keys = ["id","source_key","symbol","direction","entry","stop","risk","tp1","tp2","tp3","signal_time",
            "tracking_start","expiry_ts","last_checked","status","tp1_hit","tp2_hit","tp3_hit","tp1_time","tp2_time","tp3_time"]
    return [dict(zip(keys,row)) for row in rows]


def _v73_update_trade(trade):
    return _v68_db_execute(
        """UPDATE fh_v73_equity_shadow SET
        last_checked=%s,status=%s,tp1_hit=%s,tp2_hit=%s,tp3_hit=%s,tp1_time=%s,tp2_time=%s,tp3_time=%s,
        closed_time=%s,final_r=%s,settled_at=CASE WHEN %s='OPEN' THEN settled_at ELSE NOW() END
        WHERE id=%s""",
        (trade.get("last_checked"),trade.get("status"),trade.get("tp1_hit"),trade.get("tp2_hit"),trade.get("tp3_hit"),
         trade.get("tp1_time"),trade.get("tp2_time"),trade.get("tp3_time"),trade.get("closed_time"),trade.get("final_r"),
         trade.get("status"),trade.get("id"))
    )


def _v73_settle_one_equity(trade):
    df = get_candles(trade["symbol"], "Min1")
    if df is None or len(df) == 0:
        return None
    df = df.copy()
    df["time_norm"] = df["time"].apply(normalize_candle_time)
    relevant = df[(df["time_norm"] > num(trade.get("last_checked"))) &
                  (df["time_norm"] >= num(trade.get("tracking_start")))]
    latest_price = num(df.iloc[-1]["close"])
    latest_seen = num(trade.get("last_checked"))
    event = None
    for _, candle in relevant.iterrows():
        candle_time = num(candle["time_norm"])
        latest_seen = max(latest_seen, candle_time)
        touched = trade_touches(trade, num(candle["high"]), num(candle["low"]))
        if touched["stop"] and touched["tp3"]:
            trade["status"] = "AMBIGUOUS"; trade["closed_time"] = candle_time; trade["final_r"] = None; event = "AMBIGUOUS"; break
        if touched["stop"]:
            trade["status"] = "STOP"; trade["closed_time"] = candle_time; trade["final_r"] = -1.0; event = "STOP"; break
        if touched["tp3"]:
            trade["tp1_hit"] = trade["tp2_hit"] = trade["tp3_hit"] = True
            if not trade.get("tp1_time"): trade["tp1_time"] = candle_time
            if not trade.get("tp2_time"): trade["tp2_time"] = candle_time
            trade["tp3_time"] = candle_time; trade["status"] = "TP3"; trade["closed_time"] = candle_time; trade["final_r"] = 3.0; event = "TP3"; break
        if touched["tp2"] and not trade.get("tp2_hit"):
            trade["tp1_hit"] = True; trade["tp2_hit"] = True
            if not trade.get("tp1_time"): trade["tp1_time"] = candle_time
            trade["tp2_time"] = candle_time
        elif touched["tp1"] and not trade.get("tp1_hit"):
            trade["tp1_hit"] = True; trade["tp1_time"] = candle_time
    trade["last_checked"] = latest_seen
    if trade.get("status") == "OPEN" and time.time() >= num(trade.get("expiry_ts")):
        final_r = current_r_for_price(trade, latest_price)
        trade["final_r"] = round(max(-1.0, min(3.0, final_r)), 4)
        trade["status"] = "EXPIRED"; trade["closed_time"] = time.time(); event = "EXPIRED"
    _v73_update_trade(trade)
    return event


def _v73_settle_equity_shadow():
    if not V73_EQUITY_DB_READY:
        return 0
    settled = 0
    for trade in _v73_open_rows():
        try:
            event = _v73_settle_one_equity(trade)
            if event:
                settled += 1
                print(f"V7.3 Equity Lab settled {trade['symbol']} {trade['direction']} → {event} ({trade.get('final_r')})")
        except Exception as error:
            print(f"V7.3 Equity settlement warning {trade.get('symbol')}: {type(error).__name__}: {error}")
        time.sleep(0.05)
    return settled


def _v73_equity_summary_text():
    if not V73_EQUITY_SHADOW_ENABLED:
        return "V7.3 Equity Lab is disabled."
    if not V73_EQUITY_DB_READY:
        return "V7.3 Equity Lab is waiting for Postgres."
    row = _v68_db_execute(
        """SELECT COUNT(*),
        COUNT(*) FILTER(WHERE status='OPEN'),
        COUNT(*) FILTER(WHERE final_r IS NOT NULL),
        COUNT(*) FILTER(WHERE final_r>0),
        COALESCE(SUM(final_r) FILTER(WHERE final_r IS NOT NULL),0),
        COALESCE(AVG(final_r) FILTER(WHERE final_r IS NOT NULL),0),
        COUNT(*) FILTER(WHERE tp1_hit),COUNT(*) FILTER(WHERE tp2_hit),COUNT(*) FILTER(WHERE tp3_hit),
        COUNT(*) FILTER(WHERE status='STOP'),COUNT(*) FILTER(WHERE status='AMBIGUOUS')
        FROM fh_v73_equity_shadow""", fetch="one"
    )
    if not row:
        return "V7.3 Equity Lab: no data yet."
    total,open_n,settled,wins,total_r,avg_r,tp1,tp2,tp3,stops,amb = row
    win_rate = (100.0 * num(wins) / num(settled)) if num(settled) else 0.0
    latest = _v68_db_execute(
        """SELECT symbol,direction,score,status,final_r,session_date
        FROM fh_v73_equity_shadow ORDER BY signal_time DESC LIMIT 5""", fetch="all"
    ) or []
    lines = [
        "📈 V7.3 EQUITY / STOCK FUTURES LAB — SHADOW ONLY",
        f"Signals: {int(total or 0)} | Open: {int(open_n or 0)} | Settled: {int(settled or 0)}",
        f"Win rate: {win_rate:.1f}% | Avg: {num(avg_r):+.2f}R | Cumulative: {num(total_r):+.2f}R",
        f"TP1/TP2/TP3: {int(tp1 or 0)}/{int(tp2 or 0)}/{int(tp3 or 0)} | Stops: {int(stops or 0)} | Ambiguous: {int(amb or 0)}",
        f"Model: 5m 09:30 ET cash-open ORB + volume/VWAP/EMA/1h trend; score ≥ {V73_EQUITY_MIN_SCORE:.0f}",
        "V7.3 itself never executes; qualifying signals are passed to the separate V7.5 shadow/live gate.",
    ]
    if latest:
        lines.append("\nLatest:")
        for sym,direction,score,status,final_r,session_date in latest:
            rtxt = "OPEN" if final_r is None else f"{num(final_r):+.2f}R"
            lines.append(f"• {session_date} {sym} {direction} {num(score):.0f} — {status} {rtxt}")
    return "\n".join(lines)


_V73_PREV_COMMAND = handle_telegram_command
def handle_telegram_command(chat_id, text):
    command = ((text or "").strip().split() or [""])[0].lower()
    if command in {"/equity","/stocklab","/stocks","/orb"}:
        send_to_chat(chat_id, _v73_equity_summary_text())
        return
    return _V73_PREV_COMMAND(chat_id, text)

# ============================================================
# V7.0 LIVE PILOT — FAIL-CLOSED EXECUTION WRAPPER
# ============================================================
try:
    import live_executor_v70 as V70_LIVE
except Exception as _v70_import_error:
    V70_LIVE = None
    print(f"V7.0 live module import warning: {_v70_import_error}")


# ============================================================
# V7.5 EQUITY LIVE CANDIDATE — SHADOW-FIRST, LIVE-CAPABLE
# ============================================================
# The dedicated equity path is built now, but live execution is OFF by default.
# This lets several NY cash sessions accumulate auditable "would take / would skip"
# outcomes before the operator explicitly enables stock-futures live trading.
V75_EQUITY_VERSION = "7.5.0-equity-live-shadow-ready"
V75_EQUITY_LIVE_ENABLED = os.getenv("V75_EQUITY_LIVE_ENABLED", "false").lower() == "true"
V75_EQUITY_RISK_PCT = min(0.005, max(0.001, float(os.getenv("V75_EQUITY_RISK_PCT", "0.005"))))
V75_EQUITY_MIN_SCORE = float(os.getenv("V75_EQUITY_MIN_SCORE", "80"))
V75_EQUITY_MIN_RV = float(os.getenv("V75_EQUITY_MIN_RV", "1.20"))
V75_EQUITY_MAX_SPREAD_PCT = float(os.getenv("V75_EQUITY_MAX_SPREAD_PCT", "0.15"))
V75_EQUITY_MAX_STOP_PCT = float(os.getenv("V75_EQUITY_MAX_STOP_PCT", "2.50"))
V75_EQUITY_MAX_EXTENSION_ATR = float(os.getenv("V75_EQUITY_MAX_EXTENSION_ATR", "0.60"))
V75_EQUITY_MAX_ENTRY_DRIFT_R = float(os.getenv("V75_EQUITY_MAX_ENTRY_DRIFT_R", "0.20"))
V75_EQUITY_MAX_ADVERSE_DRIFT_R = float(os.getenv("V75_EQUITY_MAX_ADVERSE_DRIFT_R", "0.35"))
V75_EQUITY_MAX_SIGNAL_AGE_SECONDS = int(os.getenv("V75_EQUITY_MAX_SIGNAL_AGE_SECONDS", "480"))
V75_EQUITY_MAX_OPEN = min(1, max(1, int(os.getenv("V75_EQUITY_MAX_OPEN", "1"))))
V75_EQUITY_SAME_SYMBOL_COOLDOWN_MINUTES = int(os.getenv("V75_EQUITY_SAME_SYMBOL_COOLDOWN_MINUTES", "120"))
V75_EQUITY_LOSS_CLUSTER_COUNT = int(os.getenv("V75_EQUITY_LOSS_CLUSTER_COUNT", "2"))
V75_EQUITY_LOSS_CLUSTER_MINUTES = int(os.getenv("V75_EQUITY_LOSS_CLUSTER_MINUTES", "390"))
V75_EQUITY_MIN_SHADOW_SESSIONS = int(os.getenv("V75_EQUITY_MIN_SHADOW_SESSIONS", "3"))
V75_EQUITY_MIN_SETTLED_FOR_REVIEW = int(os.getenv("V75_EQUITY_MIN_SETTLED_FOR_REVIEW", "20"))
V75_EQUITY_NOTIFY = os.getenv("V75_EQUITY_NOTIFY", "true").lower() == "true"
V75_EQUITY_DB_READY = False


def _v75_init_equity_live_shadow():
    global V75_EQUITY_DB_READY
    if not V68_DB_READY:
        V75_EQUITY_DB_READY = False
        return False
    ok = _v68_db_execute("""
    CREATE TABLE IF NOT EXISTS fh_v75_equity_live_candidates (
        source_key TEXT PRIMARY KEY,
        version TEXT NOT NULL,
        symbol TEXT NOT NULL,
        session_date TEXT NOT NULL,
        direction TEXT NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        decision TEXT NOT NULL,
        reasons JSONB,
        notes JSONB,
        current_price DOUBLE PRECISION,
        drift_r DOUBLE PRECISION,
        live_stop_pct DOUBLE PRECISION,
        risk_fraction DOUBLE PRECISION,
        live_enabled BOOLEAN NOT NULL DEFAULT FALSE,
        executed BOOLEAN NOT NULL DEFAULT FALSE,
        execution_reason TEXT,
        actual_status TEXT,
        actual_final_r DOUBLE PRECISION,
        shadow_entry DOUBLE PRECISION, shadow_stop DOUBLE PRECISION, shadow_risk DOUBLE PRECISION,
        shadow_tp1 DOUBLE PRECISION, shadow_tp2 DOUBLE PRECISION, shadow_tp3 DOUBLE PRECISION,
        shadow_tracking_start DOUBLE PRECISION, shadow_expiry_ts DOUBLE PRECISION, shadow_last_checked DOUBLE PRECISION,
        shadow_tp1_hit BOOLEAN NOT NULL DEFAULT FALSE, shadow_tp2_hit BOOLEAN NOT NULL DEFAULT FALSE, shadow_tp3_hit BOOLEAN NOT NULL DEFAULT FALSE,
        shadow_tp1_time DOUBLE PRECISION, shadow_tp2_time DOUBLE PRECISION, shadow_tp3_time DOUBLE PRECISION,
        shadow_closed_time DOUBLE PRECISION,
        payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        settled_at TIMESTAMPTZ
    )
    """)
    if ok:
        for ddl in (
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_entry DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_stop DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_risk DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp1 DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp2 DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp3 DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tracking_start DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_expiry_ts DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_last_checked DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp1_hit BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp2_hit BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp3_hit BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp1_time DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp2_time DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_tp3_time DOUBLE PRECISION",
            "ALTER TABLE fh_v75_equity_live_candidates ADD COLUMN IF NOT EXISTS shadow_closed_time DOUBLE PRECISION",
        ):
            _v68_db_execute(ddl)
        _v68_db_execute("CREATE INDEX IF NOT EXISTS idx_fh_v75_equity_decision ON fh_v75_equity_live_candidates(decision, created_at DESC)")
        _v68_db_execute("CREATE INDEX IF NOT EXISTS idx_fh_v75_equity_shadow_status ON fh_v75_equity_live_candidates(actual_status, shadow_tracking_start)")
    V75_EQUITY_DB_READY = bool(ok)
    return V75_EQUITY_DB_READY


def _v75_current_ticker(symbol):
    response = requests.get(f"{MEXC_REST}/api/v1/contract/ticker", params={"symbol": symbol}, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError(f"MEXC ticker failure {payload.get('code')} {payload.get('message')}")
    data = payload.get("data")
    if isinstance(data, list):
        data = next((x for x in data if str(x.get("symbol") or "") == str(symbol)), None)
    if not isinstance(data, dict):
        raise RuntimeError("equity ticker unavailable")
    return data


def _v75_equity_live_gate(signal):
    reasons = []
    notes = []
    symbol = str((signal or {}).get("symbol") or "")
    direction = str((signal or {}).get("direction") or "").upper()
    if not signal or not _v73_is_equity_symbol(symbol):
        return {"decision":"SKIP","eligible":False,"reasons":["not a recognized equity/index future"],"notes":notes}
    if V70_LIVE is None or not getattr(V70_LIVE, "ENABLED", False):
        return {"decision":"SKIP","eligible":False,"reasons":["live executor unavailable/disabled"],"notes":notes}

    # Real MEXC positions + real live ledger only. Paper positions are never used.
    try:
        snapshot = V70_LIVE.live_risk_snapshot(V75_EQUITY_LOSS_CLUSTER_MINUTES)
    except Exception as error:
        return {"decision":"SKIP","eligible":False,"reasons":[f"live risk snapshot failed: {type(error).__name__}: {error}"],"notes":notes}

    open_equities = [p for p in (snapshot.get("open_positions") or []) if _v73_is_equity_symbol(p.get("symbol"))]
    if any(str(p.get("symbol") or "") == symbol for p in open_equities):
        reasons.append("same equity symbol already live")
    if len(open_equities) >= V75_EQUITY_MAX_OPEN:
        reasons.append(f"equity live-position cap reached ({len(open_equities)}/{V75_EQUITY_MAX_OPEN})")

    recent_losses = [
        x for x in (snapshot.get("recent_closed") or [])
        if bool(x.get("stop_like"))
        and _v73_is_equity_symbol(x.get("symbol"))
        and str(x.get("direction") or "").upper() == direction
    ]
    if len(recent_losses) >= V75_EQUITY_LOSS_CLUSTER_COUNT:
        reasons.append(f"equity LIVE loss-cluster breaker: {len(recent_losses)} same-direction stop-like losses/{V75_EQUITY_LOSS_CLUSTER_MINUTES}m")
    same_symbol_losses = [
        x for x in recent_losses
        if str(x.get("symbol") or "") == symbol
        and time.time() - num(x.get("closed_time")) <= V75_EQUITY_SAME_SYMBOL_COOLDOWN_MINUTES * 60
    ]
    if same_symbol_losses:
        age_m = (time.time() - max(num(x.get("closed_time")) for x in same_symbol_losses)) / 60.0
        reasons.append(f"same-symbol equity LIVE loss cooldown ({age_m:.0f}m ago)")

    signal_age = max(0.0, time.time() - num(signal.get("signal_time")))
    if signal_age > V75_EQUITY_MAX_SIGNAL_AGE_SECONDS:
        reasons.append(f"equity signal stale {signal_age:.0f}s > {V75_EQUITY_MAX_SIGNAL_AGE_SECONDS}s")

    event_risk = str(signal.get("event_risk") or "LOW").upper()
    if event_risk in {"HIGH", "EXTREME"}:
        reasons.append(f"scheduled-event risk {event_risk}")
    macro = get_macro_snapshot() or {}
    macro_conflict, strong_macro_conflict = _v681_directional_macro_conflict(direction, "EQUITY", num(macro.get("combined_score")))
    if strong_macro_conflict:
        reasons.append(f"strong macro conflict {num(macro.get('combined_score')):+.1f}")
    elif macro_conflict:
        notes.append(f"moderate macro conflict {num(macro.get('combined_score')):+.1f}")

    setup_type = str(signal.get("setup_type") or signal.get("agent_name") or "ORB").upper()
    score = num(signal.get("score"))
    rv = num(signal.get("relative_volume"))
    spread = num(signal.get("spread_pct"))
    extension = num(signal.get("extension_atr"))
    if score < V75_EQUITY_MIN_SCORE:
        reasons.append(f"live score {score:.1f} < {V75_EQUITY_MIN_SCORE:.1f}")
    if rv < V75_EQUITY_MIN_RV:
        reasons.append(f"live RV {rv:.2f} < {V75_EQUITY_MIN_RV:.2f}")
    if spread > V75_EQUITY_MAX_SPREAD_PCT:
        reasons.append(f"live spread {spread:.3f}% > {V75_EQUITY_MAX_SPREAD_PCT:.3f}%")
    # Extension is a hard chase filter only for breakout-family setups.  Mean-
    # reversion/reversal agents intentionally begin from large VWAP displacement.
    if setup_type in {"ORB", "GAP_GO"} and extension > V75_EQUITY_MAX_EXTENSION_ATR:
        reasons.append(f"live breakout extension {extension:.2f} ATR > {V75_EQUITY_MAX_EXTENSION_ATR:.2f}")

    current_price = 0.0
    drift_r = 999.0
    live_stop_pct = 999.0
    try:
        ticker = _v75_current_ticker(symbol)
        current_price = num(ticker.get("fairPrice") or ticker.get("lastPrice") or ticker.get("indexPrice"))
        if current_price <= 0:
            raise RuntimeError("invalid live ticker price")
        bid = num(ticker.get("bid1")); ask = num(ticker.get("ask1"))
        if bid > 0 and ask > 0:
            current_spread = ((ask - bid) / max(1e-12, (ask + bid) / 2.0)) * 100.0
            if current_spread > V75_EQUITY_MAX_SPREAD_PCT:
                reasons.append(f"current spread {current_spread:.3f}% > {V75_EQUITY_MAX_SPREAD_PCT:.3f}%")
        original_entry = num(signal.get("entry")); original_risk = max(num(signal.get("risk")), 1e-12)
        sign = 1.0 if direction == "LONG" else -1.0
        drift_r = ((current_price - original_entry) * sign) / original_risk
        if drift_r > V75_EQUITY_MAX_ENTRY_DRIFT_R:
            reasons.append(f"entry chase {drift_r:.2f}R > {V75_EQUITY_MAX_ENTRY_DRIFT_R:.2f}R")
        if drift_r < -V75_EQUITY_MAX_ADVERSE_DRIFT_R:
            reasons.append(f"setup deteriorated {drift_r:.2f}R < -{V75_EQUITY_MAX_ADVERSE_DRIFT_R:.2f}R")
        validation_mode = str(signal.get("validation_mode") or "NONE").upper()
        validation_level = num(signal.get("validation_level"))
        if validation_mode == "ABOVE" and validation_level > 0 and current_price <= validation_level:
            reasons.append(f"live price lost required support/trigger {validation_level:.6g}")
        elif validation_mode == "BELOW" and validation_level > 0 and current_price >= validation_level:
            reasons.append(f"live price lost required resistance/trigger {validation_level:.6g}")
        stop_px = num(signal.get("stop"))
        if direction == "LONG" and current_price <= stop_px:
            reasons.append("live price already crossed long invalidation stop")
        if direction == "SHORT" and current_price >= stop_px:
            reasons.append("live price already crossed short invalidation stop")
        live_stop_pct = abs(current_price - stop_px) / current_price * 100.0
        if live_stop_pct <= 0 or live_stop_pct > V75_EQUITY_MAX_STOP_PCT:
            reasons.append(f"live ORB stop {live_stop_pct:.2f}% > {V75_EQUITY_MAX_STOP_PCT:.2f}%")
        c = V70_LIVE.contract(symbol)
        if not c.get("apiAllowed", False):
            reasons.append("MEXC API trading not allowed for contract")
        if int(c.get("state", 1)) != 0:
            reasons.append("MEXC contract not enabled")
        if int(c.get("positionOpenType", 0)) not in {1, 3}:
            reasons.append("isolated margin unsupported")
    except Exception as error:
        reasons.append(f"live market/contract validation failed: {type(error).__name__}: {error}")

    preview = {}
    # Shadow decisions should mirror the real executor as closely as possible:
    # total-position cap, exchange min size, 0.5% risk sizing, available margin,
    # and the 25/25/50 contract split are checked without sending an order.
    if not reasons and V70_LIVE is not None:
        preview_fn = getattr(V70_LIVE, "preview_signal", None)
        if callable(preview_fn):
            try:
                preview_result = _v75_result_from_signal(signal, {"current_price": current_price})
                preview = preview_fn(preview_result) or {}
                if not preview.get("eligible"):
                    reasons.append(str(preview.get("reason") or "executor preview rejected"))
            except Exception as error:
                reasons.append(f"executor preview failed: {type(error).__name__}: {error}")
        else:
            reasons.append("executor lacks read-only preview_signal")

    return {
        "version": V75_EQUITY_VERSION,
        "decision": "ALLOW" if not reasons else "SKIP",
        "eligible": not reasons,
        "reasons": reasons,
        "notes": notes,
        "current_price": current_price,
        "drift_r": drift_r,
        "live_stop_pct": live_stop_pct,
        "live_equity_open": len(open_equities),
        "recent_live_equity_losses": len(recent_losses),
        "risk_fraction": V75_EQUITY_RISK_PCT,
        "live_enabled": V75_EQUITY_LIVE_ENABLED,
        "preview": preview,
    }


def _v75_result_from_signal(signal, gate):
    entry = num(gate.get("current_price")) or num(signal.get("entry"))
    stop = num(signal.get("stop"))
    risk = abs(entry - stop)
    sign = 1.0 if signal.get("direction") == "LONG" else -1.0
    return {
        "symbol": signal["symbol"],
        "direction": signal["direction"],
        "price": entry,
        "best_score": num(signal.get("score")),
        "selected_regime": str(signal.get("regime") or signal.get("setup_type") or "EQUITY_AGENT"),
        "risk_plan": {
            "stop": stop,
            "tp1": entry + sign * risk,
            "tp2": entry + sign * 2.0 * risk,
            "tp3": entry + sign * 3.0 * risk,
        },
        "live_risk_pct_override": V75_EQUITY_RISK_PCT,
        "live_expiry_ts": num(signal.get("expiry_ts")),
        "live_strategy_tag": str(signal.get("live_strategy_tag") or f"EQUITY_{signal.get('setup_type') or signal.get('agent_name') or 'AGENT'}"),
    }


def _v75_store_equity_candidate(signal, gate, outcome):
    if not V75_EQUITY_DB_READY:
        return False
    outcome = outcome or {}
    return bool(_v68_db_execute("""
    INSERT INTO fh_v75_equity_live_candidates
    (source_key,version,symbol,session_date,direction,score,decision,reasons,notes,current_price,drift_r,
     live_stop_pct,risk_fraction,live_enabled,executed,execution_reason,payload,updated_at)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
    ON CONFLICT(source_key) DO UPDATE SET
      version=EXCLUDED.version,symbol=EXCLUDED.symbol,session_date=EXCLUDED.session_date,direction=EXCLUDED.direction,
      score=EXCLUDED.score,decision=EXCLUDED.decision,reasons=EXCLUDED.reasons,notes=EXCLUDED.notes,current_price=EXCLUDED.current_price,
      drift_r=EXCLUDED.drift_r,live_stop_pct=EXCLUDED.live_stop_pct,risk_fraction=EXCLUDED.risk_fraction,
      live_enabled=EXCLUDED.live_enabled,executed=(fh_v75_equity_live_candidates.executed OR EXCLUDED.executed),
      execution_reason=EXCLUDED.execution_reason,payload=EXCLUDED.payload,updated_at=NOW()
    RETURNING source_key
    """, (
        signal.get("source_key"),V75_EQUITY_VERSION,signal.get("symbol"),signal.get("session_date"),signal.get("direction"),
        num(signal.get("score")),gate.get("decision"),json.dumps(gate.get("reasons") or []),json.dumps(gate.get("notes") or []),
        num(gate.get("current_price")),num(gate.get("drift_r")),num(gate.get("live_stop_pct")),V75_EQUITY_RISK_PCT,
        V75_EQUITY_LIVE_ENABLED,bool(outcome.get("executed")),str(outcome.get("reason") or ("submitted/confirmed" if outcome.get("executed") else "shadow-only")),
        json.dumps(_clean_json_value({"signal":signal,"gate":gate,"outcome":outcome})),
    ), fetch="one"))


def _v75_process_equity_candidate(signal):
    # Once a V7.5 shadow/live candidate has been accepted, keep its original
    # execution-time geometry immutable. Subsequent scans must not move the
    # hypothetical entry/stop/targets or submit another real order.
    existing = None
    if V75_EQUITY_DB_READY:
        existing = _v68_db_execute(
            "SELECT decision,actual_status,executed,score FROM fh_v75_equity_live_candidates WHERE source_key=%s",
            (signal.get("source_key"),), fetch="one"
        )
    if existing and (bool(existing[2]) or str(existing[0] or "").upper() == "ALLOW"):
        return {"gate": {"decision": "LOCKED", "eligible": False}, "outcome": {"executed": bool(existing[2]), "reason": "candidate already accepted"}}

    gate = _v75_equity_live_gate(signal)
    outcome = {"executed": False, "reason": "shadow-only; V75_EQUITY_LIVE_ENABLED=false"}
    result = None
    if gate.get("eligible"):
        result = _v75_result_from_signal(signal, gate)
        if V75_EQUITY_LIVE_ENABLED:
            trade = {
                "signal_id": f"equity_{signal.get('source_key')}",
                "source_key": signal.get("source_key"),
                "symbol": signal.get("symbol"),
                "direction": signal.get("direction"),
                "score": num(signal.get("score")),
                "signal_time": num(signal.get("signal_time")),
                "status": "LIVE_CANDIDATE",
            }
            outcome = V70_LIVE.execute_signal(result, trade)
            if not isinstance(outcome, dict):
                outcome = {"executed": bool(outcome)}

    _v75_store_equity_candidate(signal, gate, outcome)

    # Start a separate V7.5 shadow trade at the exact live-gate geometry. This
    # is deliberately independent of V7.3's earlier control entry, otherwise
    # our few-day validation would measure the wrong trade.
    if gate.get("eligible") and V75_EQUITY_DB_READY and result is not None:
        plan = result.get("risk_plan") or {}
        entry = num(result.get("price")); stop = num(plan.get("stop")); risk = abs(entry-stop)
        # Start from the next full 1-minute candle. This avoids pretending we
        # know the intraminute high/low ordering before the shadow decision.
        tracking_start = (int(time.time() // 60) + 1) * 60
        _v68_db_execute("""UPDATE fh_v75_equity_live_candidates SET
            actual_status='OPEN',actual_final_r=NULL,settled_at=NULL,
            shadow_entry=%s,shadow_stop=%s,shadow_risk=%s,shadow_tp1=%s,shadow_tp2=%s,shadow_tp3=%s,
            shadow_tracking_start=%s,shadow_expiry_ts=%s,shadow_last_checked=%s,
            shadow_tp1_hit=FALSE,shadow_tp2_hit=FALSE,shadow_tp3_hit=FALSE,
            shadow_tp1_time=NULL,shadow_tp2_time=NULL,shadow_tp3_time=NULL,shadow_closed_time=NULL,updated_at=NOW()
            WHERE source_key=%s""", (
            entry,stop,risk,num(plan.get("tp1")),num(plan.get("tp2")),num(plan.get("tp3")),
            tracking_start,num(result.get("live_expiry_ts")),tracking_start-1,signal.get("source_key")
        ))

    mode = "LIVE" if V75_EQUITY_LIVE_ENABLED else "SHADOW"
    detail = "; ".join(gate.get("reasons") or gate.get("notes") or ["all live-pilot gates passed"])
    preview = gate.get("preview") or {}
    print(
        f"V7.5 EQUITY {mode}: {signal.get('symbol')} {signal.get('direction')} {gate.get('decision')} "
        f"score={num(signal.get('score')):.1f} px={num(gate.get('current_price')):.6g} "
        f"stop={num(gate.get('live_stop_pct')):.2f}% drift={num(gate.get('drift_r')):.2f}R "
        f"riskcap={V75_EQUITY_RISK_PCT*100:.2f}% notional={num(preview.get('actual_notional')):.2f} — {detail}"
    )
    if V75_EQUITY_NOTIFY and gate.get("eligible"):
        action = "LIVE ORDER PATH ENABLED" if V75_EQUITY_LIVE_ENABLED else "WOULD TAKE — SHADOW ONLY"
        send_telegram(
            f"🏛️ V7.5 EQUITY {action}\n\n"
            f"{signal.get('symbol')} {signal.get('direction')} | score {num(signal.get('score')):.1f}\n"
            f"Live check {num(gate.get('current_price')):.6g} | stop risk {num(gate.get('live_stop_pct')):.2f}% | drift {num(gate.get('drift_r')):.2f}R\n"
            f"Risk cap if live: {V75_EQUITY_RISK_PCT*100:.2f}% of equity\n"
            f"Session expiry: 16:00 New York"
        )
    return {"gate": gate, "outcome": outcome}


def _v75_shadow_stop_r(trade, stage):
    entry=num(trade.get("entry")); risk=max(num(trade.get("risk")),1e-12)
    direction=str(trade.get("direction") or "").upper()
    if stage <= 0:
        return -1.0
    if stage == 1:
        bps = num(getattr(V70_LIVE, "BE_BUFFER_BPS", 10.0) if V70_LIVE else 10.0)
        stop_px = entry * (1.0 + bps/10000.0) if direction == "LONG" else entry * (1.0 - bps/10000.0)
        return current_r_for_price({"entry":entry,"risk":risk,"direction":direction}, stop_px)
    return 1.0


def _v75_shadow_final_r_at_stop(trade, stage):
    if stage <= 0:
        return -1.0
    if stage == 1:
        # 25% banked at +1R; 75% exits at the BE-buffer stop.
        return 0.25 + 0.75 * _v75_shadow_stop_r(trade, stage)
    # 25% at +1R + 25% at +2R + remaining 50% at +1R stop.
    return 0.25 + 0.50 + 0.50


def _v75_shadow_current_final_r(trade, current_r):
    stage = 2 if trade.get("tp2_hit") else (1 if trade.get("tp1_hit") else 0)
    if stage == 0:
        return current_r
    if stage == 1:
        return 0.25 + 0.75 * current_r
    return 0.75 + 0.50 * current_r


def _v75_open_shadow_rows():
    if not V75_EQUITY_DB_READY:
        return []
    rows=_v68_db_execute("""SELECT source_key,symbol,direction,shadow_entry,shadow_stop,shadow_risk,
        shadow_tp1,shadow_tp2,shadow_tp3,shadow_tracking_start,shadow_expiry_ts,shadow_last_checked,
        shadow_tp1_hit,shadow_tp2_hit,shadow_tp3_hit,shadow_tp1_time,shadow_tp2_time,shadow_tp3_time
        FROM fh_v75_equity_live_candidates WHERE decision='ALLOW' AND actual_status='OPEN'
        ORDER BY shadow_tracking_start""",fetch="all") or []
    keys=["source_key","symbol","direction","entry","stop","risk","tp1","tp2","tp3","tracking_start","expiry_ts","last_checked",
          "tp1_hit","tp2_hit","tp3_hit","tp1_time","tp2_time","tp3_time"]
    return [dict(zip(keys,row)) for row in rows]


def _v75_update_shadow_trade(trade):
    return _v68_db_execute("""UPDATE fh_v75_equity_live_candidates SET
        shadow_last_checked=%s,actual_status=%s,actual_final_r=%s,
        shadow_tp1_hit=%s,shadow_tp2_hit=%s,shadow_tp3_hit=%s,
        shadow_tp1_time=%s,shadow_tp2_time=%s,shadow_tp3_time=%s,shadow_closed_time=%s,
        settled_at=CASE WHEN %s='OPEN' THEN settled_at ELSE COALESCE(settled_at,NOW()) END,updated_at=NOW()
        WHERE source_key=%s""",(
        trade.get("last_checked"),trade.get("status"),trade.get("final_r"),
        trade.get("tp1_hit"),trade.get("tp2_hit"),trade.get("tp3_hit"),
        trade.get("tp1_time"),trade.get("tp2_time"),trade.get("tp3_time"),trade.get("closed_time"),
        trade.get("status"),trade.get("source_key")
    ))


def _v75_settle_one_shadow(trade):
    df=get_candles(trade["symbol"],"Min1")
    if df is None or len(df)==0:
        return None
    df=df.copy(); df["time_norm"]=df["time"].apply(normalize_candle_time)
    relevant=df[(df["time_norm"]>num(trade.get("last_checked"))) & (df["time_norm"]>=num(trade.get("tracking_start")))]
    latest_price=num(df.iloc[-1]["close"]); latest_seen=num(trade.get("last_checked")); event=None
    direction=str(trade.get("direction") or "").upper()
    for _,candle in relevant.iterrows():
        t=num(candle["time_norm"]); latest_seen=max(latest_seen,t)
        high=num(candle["high"]); low=num(candle["low"])
        stage=2 if trade.get("tp2_hit") else (1 if trade.get("tp1_hit") else 0)
        stop_r=_v75_shadow_stop_r(trade,stage)
        entry=num(trade.get("entry")); risk=max(num(trade.get("risk")),1e-12)
        managed_stop=(entry + stop_r*risk) if direction=="LONG" else (entry - stop_r*risk)
        stop_touched = low <= managed_stop if direction=="LONG" else high >= managed_stop
        next_target_touched = False
        if not trade.get("tp1_hit"):
            next_target_touched = high >= num(trade.get("tp1")) if direction=="LONG" else low <= num(trade.get("tp1"))
        elif not trade.get("tp2_hit"):
            next_target_touched = high >= num(trade.get("tp2")) if direction=="LONG" else low <= num(trade.get("tp2"))
        else:
            next_target_touched = high >= num(trade.get("tp3")) if direction=="LONG" else low <= num(trade.get("tp3"))
        # If both the active stop and a fresh target are inside the same 1m bar,
        # sequence is unknowable from OHLC; exclude it from expectancy.
        if stop_touched and next_target_touched:
            trade["status"]="AMBIGUOUS"; trade["final_r"]=None; trade["closed_time"]=t; event="AMBIGUOUS"; break
        if stop_touched:
            trade["status"]="STOP"; trade["final_r"]=round(_v75_shadow_final_r_at_stop(trade,stage),4); trade["closed_time"]=t; event="STOP"; break
        tp3_touched = high >= num(trade.get("tp3")) if direction=="LONG" else low <= num(trade.get("tp3"))
        tp2_touched = high >= num(trade.get("tp2")) if direction=="LONG" else low <= num(trade.get("tp2"))
        tp1_touched = high >= num(trade.get("tp1")) if direction=="LONG" else low <= num(trade.get("tp1"))
        if tp3_touched:
            trade["tp1_hit"]=trade["tp2_hit"]=trade["tp3_hit"]=True
            if not trade.get("tp1_time"): trade["tp1_time"]=t
            if not trade.get("tp2_time"): trade["tp2_time"]=t
            trade["tp3_time"]=t; trade["status"]="TP3"; trade["final_r"]=2.25; trade["closed_time"]=t; event="TP3"; break
        if tp2_touched and not trade.get("tp2_hit"):
            trade["tp1_hit"]=True; trade["tp2_hit"]=True
            if not trade.get("tp1_time"): trade["tp1_time"]=t
            trade["tp2_time"]=t
        elif tp1_touched and not trade.get("tp1_hit"):
            trade["tp1_hit"]=True; trade["tp1_time"]=t
    trade["last_checked"]=latest_seen
    trade.setdefault("status","OPEN")
    if trade.get("status")=="OPEN" and time.time()>=num(trade.get("expiry_ts")):
        current_r=current_r_for_price(trade,latest_price)
        trade["final_r"]=round(max(-1.0,min(2.25,_v75_shadow_current_final_r(trade,current_r))),4)
        trade["status"]="EXPIRED"; trade["closed_time"]=time.time(); event="EXPIRED"
    _v75_update_shadow_trade(trade)
    return event


def _v75_sync_equity_candidate_outcomes():
    if not V75_EQUITY_DB_READY:
        return 0
    settled=0
    for trade in _v75_open_shadow_rows():
        try:
            event=_v75_settle_one_shadow(trade)
            if event:
                settled+=1
                print(f"V7.5 Equity shadow settled {trade['symbol']} {trade['direction']} → {event} ({trade.get('final_r')})")
        except Exception as error:
            print(f"V7.5 Equity shadow settlement warning {trade.get('symbol')}: {type(error).__name__}: {error}")
        time.sleep(0.03)
    return settled


def _v75_equity_live_summary_text():
    if not V75_EQUITY_DB_READY:
        return "V7.5 Equity Live Shadow is waiting for Postgres."
    row = _v68_db_execute("""
    SELECT COUNT(*),
           COUNT(*) FILTER(WHERE decision='ALLOW'),
           COUNT(*) FILTER(WHERE decision='SKIP'),
           COUNT(*) FILTER(WHERE actual_final_r IS NOT NULL),
           COALESCE(AVG(actual_final_r) FILTER(WHERE decision='ALLOW' AND actual_final_r IS NOT NULL),0),
           COALESCE(SUM(actual_final_r) FILTER(WHERE decision='ALLOW' AND actual_final_r IS NOT NULL),0),
           COUNT(*) FILTER(WHERE executed),
           COUNT(DISTINCT session_date) FILTER(WHERE decision='ALLOW'),
           COUNT(*) FILTER(WHERE decision='ALLOW' AND actual_final_r IS NOT NULL),
           COUNT(*) FILTER(WHERE decision='ALLOW' AND actual_final_r>0),
           COUNT(*) FILTER(WHERE decision='ALLOW' AND actual_status='AMBIGUOUS'),
           COUNT(*) FILTER(WHERE decision='ALLOW' AND actual_status='OPEN')
      FROM fh_v75_equity_live_candidates
    """, fetch="one")
    if not row:
        return "V7.5 Equity Live Shadow: no candidates yet."
    sessions=int(row[7] or 0); settled_allows=int(row[8] or 0); wins=int(row[9] or 0); ambiguous=int(row[10] or 0); open_n=int(row[11] or 0)
    win_rate=100.0*wins/max(1,settled_allows)
    evidence_ready=(sessions>=V75_EQUITY_MIN_SHADOW_SESSIONS and settled_allows>=V75_EQUITY_MIN_SETTLED_FOR_REVIEW and num(row[4])>0)
    return (
        "🏛️ V7.5 EQUITY LIVE SHADOW\n"
        f"Universe: up to top {V73_EQUITY_TOP_N} liquid MEXC USDT stock/index futures\n"
        f"Live execution: {'ENABLED' if V75_EQUITY_LIVE_ENABLED else 'OFF — SHADOW FIRST'}\n"
        f"Candidate risk if enabled: {V75_EQUITY_RISK_PCT*100:.2f}% | Max equity positions: {V75_EQUITY_MAX_OPEN} | Global max: {getattr(V70_LIVE,'MAX_POSITIONS','?') if V70_LIVE else '?'}\n"
        f"Candidates: {int(row[0] or 0)} | WOULD-TAKE {int(row[1] or 0)} | SKIP {int(row[2] or 0)} | Open shadow {open_n} | Sessions {sessions}\n"
        f"Settled WOULD-TAKEs: {settled_allows} | Win {win_rate:.1f}% | Avg {num(row[4]):+.2f}R | Total {num(row[5]):+.2f}R | Ambiguous {ambiguous} | Actual live {int(row[6] or 0)}\n"
        f"Evidence target: {V75_EQUITY_MIN_SHADOW_SESSIONS} sessions + {V75_EQUITY_MIN_SETTLED_FOR_REVIEW} settled + positive avg R → {'READY FOR MANUAL REVIEW' if evidence_ready else 'COLLECTING'}\n"
        f"Live gates: score≥{V75_EQUITY_MIN_SCORE:.0f}, RV≥{V75_EQUITY_MIN_RV:.2f}x, spread≤{V75_EQUITY_MAX_SPREAD_PCT:.2f}%, stop≤{V75_EQUITY_MAX_STOP_PCT:.2f}%, chase≤{V75_EQUITY_MAX_ENTRY_DRIFT_R:.2f}R. Promotion is manual only."
    )


# ============================================================
# V7.6 EQUITY AGENT ENSEMBLE — SIX SPECIALISTS + META-AGENT
# ============================================================
# Shadow-first research architecture.  Each specialist owns a distinct setup,
# records its own counterfactual outcome, and contributes to a regime-aware
# meta-agent.  Only the meta-agent can hand a candidate to the V7.5 risk/execution
# gate, so several agents agreeing on the same move never create duplicate orders.
V76_EQUITY_VERSION = "7.6.0-equity-agent-ensemble-shadow"
V76_AGENT_NAMES = (
    "ORB",
    "VWAP_MOMENTUM",
    "BREAKOUT_RETEST",
    "GAP_GO",
    "MEAN_REVERSION",
    "LATE_REVERSAL",
)
V76_AGENT_MIN_SCORE = float(os.getenv("V76_AGENT_MIN_SCORE", "72"))
V76_META_MIN_SCORE = float(os.getenv("V76_META_MIN_SCORE", "80"))
V76_META_STRONG_SINGLE_SCORE = float(os.getenv("V76_META_STRONG_SINGLE_SCORE", "90"))
V76_META_MIN_DIRECTION_EDGE = float(os.getenv("V76_META_MIN_DIRECTION_EDGE", "1.20"))
V76_AGENT_MAX_STOP_PCT = float(os.getenv("V76_AGENT_MAX_STOP_PCT", "3.50"))
V76_WEIGHT_CACHE_SECONDS = int(os.getenv("V76_WEIGHT_CACHE_SECONDS", "900"))
V76_NOTIFY = os.getenv("V76_NOTIFY", "true").lower() == "true"
V76_DB_READY = False
V76_WEIGHT_CACHE = {}
V76_PRIOR_CLOSE_CACHE = {}


def _v76_init_equity_agent_lab():
    global V76_DB_READY
    if not V68_DB_READY:
        V76_DB_READY = False
        return False
    agent_ok = _v68_db_execute("""
    CREATE TABLE IF NOT EXISTS fh_v76_equity_agent_shadow (
        source_key TEXT PRIMARY KEY,
        version TEXT NOT NULL,
        agent_name TEXT NOT NULL,
        symbol TEXT NOT NULL,
        session_date TEXT NOT NULL,
        direction TEXT NOT NULL,
        regime TEXT NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        entry DOUBLE PRECISION NOT NULL,
        stop DOUBLE PRECISION NOT NULL,
        risk DOUBLE PRECISION NOT NULL,
        tp1 DOUBLE PRECISION NOT NULL,
        tp2 DOUBLE PRECISION NOT NULL,
        tp3 DOUBLE PRECISION NOT NULL,
        signal_time DOUBLE PRECISION NOT NULL,
        tracking_start DOUBLE PRECISION NOT NULL,
        expiry_ts DOUBLE PRECISION NOT NULL,
        last_checked DOUBLE PRECISION NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'OPEN',
        tp1_hit BOOLEAN NOT NULL DEFAULT FALSE,
        tp2_hit BOOLEAN NOT NULL DEFAULT FALSE,
        tp3_hit BOOLEAN NOT NULL DEFAULT FALSE,
        tp1_time DOUBLE PRECISION,
        tp2_time DOUBLE PRECISION,
        tp3_time DOUBLE PRECISION,
        closed_time DOUBLE PRECISION,
        final_r DOUBLE PRECISION,
        mfe_r DOUBLE PRECISION NOT NULL DEFAULT 0,
        mae_r DOUBLE PRECISION NOT NULL DEFAULT 0,
        reasons JSONB,
        payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        settled_at TIMESTAMPTZ
    )
    """)
    meta_ok = _v68_db_execute("""
    CREATE TABLE IF NOT EXISTS fh_v76_equity_meta (
        source_key TEXT PRIMARY KEY,
        version TEXT NOT NULL,
        symbol TEXT NOT NULL,
        session_date TEXT NOT NULL,
        direction TEXT NOT NULL,
        regime TEXT NOT NULL,
        meta_score DOUBLE PRECISION NOT NULL,
        primary_agent TEXT NOT NULL,
        supporting_agents JSONB,
        conflicting_agents JSONB,
        weights JSONB,
        v75_decision TEXT,
        v75_reasons JSONB,
        executed BOOLEAN NOT NULL DEFAULT FALSE,
        payload JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """)
    if agent_ok:
        _v68_db_execute("CREATE INDEX IF NOT EXISTS idx_fh_v76_agent_name_status ON fh_v76_equity_agent_shadow(agent_name,status)")
        _v68_db_execute("CREATE INDEX IF NOT EXISTS idx_fh_v76_agent_symbol_session ON fh_v76_equity_agent_shadow(symbol,session_date)")
    if meta_ok:
        _v68_db_execute("CREATE INDEX IF NOT EXISTS idx_fh_v76_meta_session ON fh_v76_equity_meta(session_date,meta_score DESC)")
    V76_DB_READY = bool(agent_ok and meta_ok)
    return V76_DB_READY


def _v76_previous_cash_close(symbol, bounds):
    key = (str(symbol), str(bounds.get("session_date")))
    if key in V76_PRIOR_CLOSE_CACHE:
        return V76_PRIOR_CLOSE_CACHE[key]
    if ZoneInfo is None:
        return None
    ny = ZoneInfo("America/New_York")
    open_dt = datetime.fromtimestamp(num(bounds.get("open_ts")), tz=timezone.utc).astimezone(ny)
    prev = open_dt - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    start_ny = datetime(prev.year, prev.month, prev.day, 15, 40, tzinfo=ny)
    end_ny = datetime(prev.year, prev.month, prev.day, 16, 5, tzinfo=ny)
    try:
        response = requests.get(
            f"{MEXC_REST}/api/v1/contract/kline/{symbol}",
            params={
                "interval": "Min5",
                "start": int(start_ny.astimezone(timezone.utc).timestamp()),
                "end": int(end_ny.astimezone(timezone.utc).timestamp()),
            },
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or {}
        times = data.get("time") or []
        closes = data.get("close") or []
        cutoff = datetime(prev.year, prev.month, prev.day, 16, 0, tzinfo=ny).astimezone(timezone.utc).timestamp()
        candidates = []
        for t, c in zip(times, closes):
            ts = normalize_candle_time(t)
            if ts <= cutoff and num(c) > 0:
                candidates.append((ts, num(c)))
        value = max(candidates, key=lambda x: x[0])[1] if candidates else None
        V76_PRIOR_CLOSE_CACHE[key] = value
        return value
    except Exception as error:
        print(f"V7.6 prior cash-close warning {symbol}: {type(error).__name__}: {error}")
        V76_PRIOR_CLOSE_CACHE[key] = None
        return None


def _v76_market_snapshot(symbol, ticker):
    context = _v69_market_context(symbol)
    if context is None:
        return None
    closed5, latest5, previous5 = _v69_rows(context.get("5m"))
    closed15, latest15, previous15 = _v69_rows(context.get("15m"))
    closed1h, latest1h, previous1h = _v69_rows(context.get("1h"))
    if any(x is None for x in (closed5, latest5, previous5, latest15, previous15, latest1h, previous1h)):
        return None
    latest_start = normalize_candle_time(latest5.get("time"))
    latest_close_ts = latest_start + 5 * 60
    bounds = _v73_session_bounds(latest_close_ts)
    if bounds is None:
        return None
    # Agents only research the underlying U.S. cash session.
    if latest_close_ts < bounds["entry_start_ts"] or latest_close_ts > bounds["close_ts"]:
        return None
    opening = closed5[(closed5["time"] >= bounds["open_ts"]) & (closed5["time"] < bounds["open_ts"] + 5 * 60)]
    if opening.empty:
        return None
    opening = opening.iloc[0]
    session5 = closed5[(closed5["time"] >= bounds["open_ts"]) & (closed5["time"] <= latest_start)]
    if session5.empty:
        return None
    close5 = num(latest5.get("close")); atr5 = max(num(latest5.get("atr14")), 1e-12)
    close15 = num(latest15.get("close")); atr15 = max(num(latest15.get("atr14")), 1e-12)
    open_px = num(opening.get("open")); orb_high = num(opening.get("high")); orb_low = num(opening.get("low"))
    session_high = max(num(x) for x in session5["high"].tolist())
    session_low = min(num(x) for x in session5["low"].tolist())
    rv5 = num(latest5.get("relative_volume")); spread = num(ticker.get("_spread_pct"))
    vwap5 = num(latest5.get("vwap20")); vwap15 = num(latest15.get("vwap20"))
    ema5 = num(latest5.get("ema20")); ema15 = num(latest15.get("ema20"))
    ema20_1h = num(latest1h.get("ema20")); ema50_1h = num(latest1h.get("ema50"))
    adx15 = num(latest15.get("adx14")); rsi15 = num(latest15.get("rsi14"))
    mins = (latest_close_ts - bounds["open_ts"]) / 60.0
    orb_atr = (orb_high - orb_low) / atr5
    dist_vwap_atr = (close15 - vwap15) / atr15 if atr15 > 0 else 0.0
    if mins >= 270:
        regime = "LATE"
    elif mins <= 120 and rv5 >= 1.50 and orb_atr >= 0.75:
        regime = "EXPANSION"
    elif adx15 >= 24 and ((close15 > ema15 and ema20_1h >= ema50_1h) or (close15 < ema15 and ema20_1h <= ema50_1h)):
        regime = "TREND"
    elif adx15 <= 18:
        regime = "RANGE"
    else:
        regime = "MIXED"
    return {
        "symbol": symbol, "ticker": ticker, "context": context,
        "closed5": closed5, "latest5": latest5, "previous5": previous5,
        "latest15": latest15, "previous15": previous15,
        "latest1h": latest1h, "previous1h": previous1h,
        "bounds": bounds, "session5": session5, "latest_start": latest_start, "latest_close_ts": latest_close_ts,
        "minutes_since_open": mins, "session_date": bounds["session_date"], "session_open": open_px,
        "orb_high": orb_high, "orb_low": orb_low, "orb_atr": orb_atr,
        "session_high": session_high, "session_low": session_low,
        "close5": close5, "close15": close15, "atr5": atr5, "atr15": atr15,
        "vwap5": vwap5, "vwap15": vwap15, "ema5": ema5, "ema15": ema15,
        "ema20_1h": ema20_1h, "ema50_1h": ema50_1h,
        "rv5": rv5, "spread_pct": spread, "rsi15": rsi15, "adx15": adx15,
        "macd5": num(latest5.get("macd_hist")), "macd5_prev": num(previous5.get("macd_hist")),
        "loc5": num(latest5.get("close_location")), "body5": num(latest5.get("body_pct")),
        "dist_vwap_atr": dist_vwap_atr, "event_risk": str((get_macro_snapshot() or {}).get("event_risk") or "LOW").upper(),
        "regime": regime,
    }


def _v76_candidate(ctx, agent_name, direction, score, entry, stop, reasons,
                   validation_mode="NONE", validation_level=None, extension_atr=0.0, extra=None):
    entry = num(entry); stop = num(stop); score = min(100.0, max(0.0, num(score)))
    if entry <= 0 or stop <= 0 or direction not in {"LONG", "SHORT"}:
        return None
    risk = abs(entry - stop)
    risk_pct = risk / max(entry, 1e-12) * 100.0
    if risk <= 0 or risk_pct > V76_AGENT_MAX_STOP_PCT or score < V76_AGENT_MIN_SCORE:
        return None
    sign = 1.0 if direction == "LONG" else -1.0
    source_key = hashlib.sha256(
        f"{V76_EQUITY_VERSION}|{agent_name}|{ctx['symbol']}|{ctx['session_date']}|{direction}".encode("utf-8")
    ).hexdigest()[:40]
    out = {
        "version": V76_EQUITY_VERSION,
        "agent_name": agent_name,
        "setup_type": agent_name,
        "symbol": ctx["symbol"], "session_date": ctx["session_date"], "direction": direction,
        "regime": ctx["regime"], "score": round(score, 1), "entry": entry, "stop": stop, "risk": risk,
        "risk_pct": risk_pct, "tp1": entry + sign * risk, "tp2": entry + sign * 2 * risk, "tp3": entry + sign * 3 * risk,
        "signal_time": ctx["latest_close_ts"], "expiry_ts": ctx["bounds"]["close_ts"],
        "relative_volume": ctx["rv5"], "spread_pct": ctx["spread_pct"], "extension_atr": num(extension_atr),
        "orb_high": ctx["orb_high"], "orb_low": ctx["orb_low"], "orb_atr": ctx["orb_atr"],
        "event_risk": ctx["event_risk"], "reasons": list(reasons or []),
        "validation_mode": validation_mode, "validation_level": num(validation_level) if validation_level is not None else None,
        "source_key": source_key,
    }
    if extra:
        out.update(_clean_json_value(extra))
    return out


def _v76_agent_orb(ctx):
    if not (5 <= ctx["minutes_since_open"] <= V73_EQUITY_ORB_WINDOW_MINUTES):
        return None
    close = ctx["close5"]; atr = ctx["atr5"]
    if close > ctx["orb_high"]:
        direction = "LONG"; extension = (close - ctx["orb_high"]) / atr
    elif close < ctx["orb_low"]:
        direction = "SHORT"; extension = (ctx["orb_low"] - close) / atr
    else:
        return None
    if ctx["spread_pct"] > 0.25 or ctx["rv5"] < 1.0 or not (0.30 <= ctx["orb_atr"] <= 2.25) or extension > 0.80:
        return None
    score = 48.0
    reasons = ["5m cash-open range break"]
    if ctx["rv5"] >= 1.5: score += 12; reasons.append("strong breakout volume")
    elif ctx["rv5"] >= 1.2: score += 9
    else: score += 5
    if direction == "LONG":
        if ctx["close15"] > ctx["vwap15"]: score += 10
        if ctx["close15"] > ctx["ema15"]: score += 8
        if ctx["ema20_1h"] >= ctx["ema50_1h"]: score += 10
        if ctx["loc5"] >= 0.65: score += 5
        stop = ctx["orb_low"]; mode = "ABOVE"; level = ctx["orb_high"]
    else:
        if ctx["close15"] < ctx["vwap15"]: score += 10
        if ctx["close15"] < ctx["ema15"]: score += 8
        if ctx["ema20_1h"] <= ctx["ema50_1h"]: score += 10
        if ctx["loc5"] <= 0.35: score += 5
        stop = ctx["orb_high"]; mode = "BELOW"; level = ctx["orb_low"]
    if ctx["adx15"] >= 18: score += 4
    return _v76_candidate(ctx, "ORB", direction, score, close, stop, reasons, mode, level, extension)


def _v76_agent_vwap_momentum(ctx):
    if not (15 <= ctx["minutes_since_open"] <= 360) or ctx["rv5"] < 0.90 or ctx["adx15"] < 18:
        return None
    close = ctx["close5"]; atr = ctx["atr5"]
    long_ok = (close > ctx["vwap5"] and ctx["close15"] > ctx["vwap15"] and ctx["close15"] > ctx["ema15"] and ctx["ema20_1h"] >= ctx["ema50_1h"] and ctx["loc5"] >= 0.58)
    short_ok = (close < ctx["vwap5"] and ctx["close15"] < ctx["vwap15"] and ctx["close15"] < ctx["ema15"] and ctx["ema20_1h"] <= ctx["ema50_1h"] and ctx["loc5"] <= 0.42)
    if long_ok == short_ok:
        return None
    direction = "LONG" if long_ok else "SHORT"
    distance = abs(close - ctx["vwap5"]) / atr
    if distance > 0.90:
        return None
    reasons = ["5m/15m VWAP momentum alignment", "1h EMA trend aligned"]
    score = 56 + min(12, max(0, (ctx["adx15"] - 18) * 0.8)) + min(10, max(0, (ctx["rv5"] - 0.9) * 12))
    if direction == "LONG":
        if ctx["macd5"] > 0: score += 8
        if 50 <= ctx["rsi15"] <= 75: score += 8
        stop = min(ctx["vwap5"], ctx["ema5"], num(ctx["latest5"].get("low"))) - 0.10 * atr
        mode = "ABOVE"; level = ctx["vwap5"]
    else:
        if ctx["macd5"] < 0: score += 8
        if 25 <= ctx["rsi15"] <= 50: score += 8
        stop = max(ctx["vwap5"], ctx["ema5"], num(ctx["latest5"].get("high"))) + 0.10 * atr
        mode = "BELOW"; level = ctx["vwap5"]
    return _v76_candidate(ctx, "VWAP_MOMENTUM", direction, score, close, stop, reasons, mode, level, distance)


def _v76_agent_breakout_retest(ctx):
    if not (10 <= ctx["minutes_since_open"] <= 330):
        return None
    past = ctx["session5"].iloc[:-1]
    if past.empty:
        return None
    close = ctx["close5"]; atr = ctx["atr5"]
    latest_low = num(ctx["latest5"].get("low")); latest_high = num(ctx["latest5"].get("high"))
    broke_long = bool((past["close"] > ctx["orb_high"]).any())
    broke_short = bool((past["close"] < ctx["orb_low"]).any())
    long_ok = broke_long and latest_low <= ctx["orb_high"] + 0.18 * atr and close > ctx["orb_high"] and ctx["loc5"] >= 0.55
    short_ok = broke_short and latest_high >= ctx["orb_low"] - 0.18 * atr and close < ctx["orb_low"] and ctx["loc5"] <= 0.45
    if long_ok == short_ok:
        return None
    direction = "LONG" if long_ok else "SHORT"
    score = 62 + min(10, max(0, (ctx["rv5"] - 0.8) * 10))
    reasons = ["opening-range breakout retested and held"]
    if direction == "LONG":
        if ctx["close15"] > ctx["vwap15"]: score += 8
        if ctx["ema20_1h"] >= ctx["ema50_1h"]: score += 7
        if ctx["macd5"] >= ctx["macd5_prev"]: score += 5
        stop = min(latest_low, ctx["orb_high"] - 0.18 * atr) - 0.05 * atr
        mode = "ABOVE"; level = ctx["orb_high"]
    else:
        if ctx["close15"] < ctx["vwap15"]: score += 8
        if ctx["ema20_1h"] <= ctx["ema50_1h"]: score += 7
        if ctx["macd5"] <= ctx["macd5_prev"]: score += 5
        stop = max(latest_high, ctx["orb_low"] + 0.18 * atr) + 0.05 * atr
        mode = "BELOW"; level = ctx["orb_low"]
    return _v76_candidate(ctx, "BREAKOUT_RETEST", direction, score, close, stop, reasons, mode, level, 0.0)


def _v76_agent_gap_go(ctx):
    if not (5 <= ctx["minutes_since_open"] <= 120):
        return None
    prior_close = _v76_previous_cash_close(ctx["symbol"], ctx["bounds"])
    if not prior_close or prior_close <= 0:
        return None
    gap_pct = (ctx["session_open"] / prior_close - 1.0) * 100.0
    if abs(gap_pct) < 0.50 or abs(gap_pct) > 10.0 or ctx["rv5"] < 1.0:
        return None
    close = ctx["close5"]
    if gap_pct > 0:
        direction = "LONG"
        if close <= ctx["session_open"] or close <= ctx["vwap5"] or ctx["loc5"] < 0.55:
            return None
        stop = min(ctx["orb_low"], ctx["session_open"] - 0.10 * ctx["atr5"])
        mode = "ABOVE"; level = ctx["session_open"]
    else:
        direction = "SHORT"
        if close >= ctx["session_open"] or close >= ctx["vwap5"] or ctx["loc5"] > 0.45:
            return None
        stop = max(ctx["orb_high"], ctx["session_open"] + 0.10 * ctx["atr5"])
        mode = "BELOW"; level = ctx["session_open"]
    score = 54 + min(14, abs(gap_pct) * 3) + min(12, max(0, (ctx["rv5"] - 1.0) * 10))
    if (direction == "LONG" and ctx["ema20_1h"] >= ctx["ema50_1h"]) or (direction == "SHORT" and ctx["ema20_1h"] <= ctx["ema50_1h"]):
        score += 8
    reasons = [f"cash-open gap {gap_pct:+.2f}% continuing with VWAP support"]
    return _v76_candidate(ctx, "GAP_GO", direction, score, close, stop, reasons, mode, level, 0.0, {"gap_pct": gap_pct, "prior_cash_close": prior_close})


def _v76_agent_mean_reversion(ctx):
    if not (30 <= ctx["minutes_since_open"] <= 330) or ctx["adx15"] > 23:
        return None
    dist = ctx["dist_vwap_atr"]
    close = ctx["close5"]; atr = ctx["atr5"]
    latest_open = num(ctx["latest5"].get("open")); latest_low = num(ctx["latest5"].get("low")); latest_high = num(ctx["latest5"].get("high"))
    long_ok = dist <= -1.00 and ctx["rsi15"] <= 38 and close > latest_open and ctx["loc5"] >= 0.58
    short_ok = dist >= 1.00 and ctx["rsi15"] >= 62 and close < latest_open and ctx["loc5"] <= 0.42
    if long_ok == short_ok:
        return None
    direction = "LONG" if long_ok else "SHORT"
    score = 56 + min(14, max(0, abs(dist) - 1.0) * 12) + min(8, max(0, 23 - ctx["adx15"]) * 0.7)
    reasons = [f"mean-reversion stretch {dist:+.2f} ATR from 15m VWAP", "low-trend regime"]
    if direction == "LONG":
        if ctx["macd5"] > ctx["macd5_prev"]: score += 8
        stop = min(ctx["session_low"], latest_low) - 0.15 * atr
        mode = "BELOW"; level = ctx["vwap15"]
    else:
        if ctx["macd5"] < ctx["macd5_prev"]: score += 8
        stop = max(ctx["session_high"], latest_high) + 0.15 * atr
        mode = "ABOVE"; level = ctx["vwap15"]
    return _v76_candidate(ctx, "MEAN_REVERSION", direction, score, close, stop, reasons, mode, level, abs(dist))


def _v76_agent_late_reversal(ctx):
    if not (300 <= ctx["minutes_since_open"] <= 380):
        return None
    close = ctx["close5"]; open_px = ctx["session_open"]; atr = ctx["atr5"]
    if open_px <= 0:
        return None
    move_pct = (close / open_px - 1.0) * 100.0
    latest_open = num(ctx["latest5"].get("open"))
    long_ok = move_pct <= -1.20 and ctx["dist_vwap_atr"] <= -0.80 and close > latest_open and ctx["loc5"] >= 0.63 and ctx["rsi15"] <= 45
    short_ok = move_pct >= 1.20 and ctx["dist_vwap_atr"] >= 0.80 and close < latest_open and ctx["loc5"] <= 0.37 and ctx["rsi15"] >= 55
    if long_ok == short_ok:
        return None
    direction = "LONG" if long_ok else "SHORT"
    score = 58 + min(15, max(0, abs(move_pct) - 1.2) * 5) + min(10, max(0, abs(ctx["dist_vwap_atr"]) - 0.8) * 8)
    if direction == "LONG":
        if ctx["macd5"] > ctx["macd5_prev"]: score += 8
        stop = ctx["session_low"] - 0.15 * atr
    else:
        if ctx["macd5"] < ctx["macd5_prev"]: score += 8
        stop = ctx["session_high"] + 0.15 * atr
    reasons = [f"late-session reversal after {move_pct:+.2f}% cash-session move"]
    return _v76_candidate(ctx, "LATE_REVERSAL", direction, score, close, stop, reasons, "NONE", None, abs(ctx["dist_vwap_atr"]), {"session_move_pct": move_pct})


V76_AGENT_FUNCTIONS = (
    _v76_agent_orb,
    _v76_agent_vwap_momentum,
    _v76_agent_breakout_retest,
    _v76_agent_gap_go,
    _v76_agent_mean_reversion,
    _v76_agent_late_reversal,
)


def _v76_store_agent_shadow(candidate):
    if not V76_DB_READY or not candidate:
        return False
    tracking_start = (int(time.time() // 60) + 1) * 60
    row = _v68_db_execute("""
    INSERT INTO fh_v76_equity_agent_shadow
    (source_key,version,agent_name,symbol,session_date,direction,regime,score,entry,stop,risk,tp1,tp2,tp3,
     signal_time,tracking_start,expiry_ts,last_checked,status,reasons,payload)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s::jsonb,%s::jsonb)
    ON CONFLICT(source_key) DO NOTHING RETURNING source_key
    """, (
        candidate["source_key"],V76_EQUITY_VERSION,candidate["agent_name"],candidate["symbol"],candidate["session_date"],candidate["direction"],candidate["regime"],
        candidate["score"],candidate["entry"],candidate["stop"],candidate["risk"],candidate["tp1"],candidate["tp2"],candidate["tp3"],
        candidate["signal_time"],tracking_start,candidate["expiry_ts"],tracking_start-1,
        json.dumps(candidate.get("reasons") or []),json.dumps(_clean_json_value(candidate)),
    ), fetch="one")
    return bool(row)


def _v76_agent_history_weight(agent_name, symbol, regime):
    base_by_regime = {
        "EXPANSION": {"ORB":1.25,"VWAP_MOMENTUM":1.15,"BREAKOUT_RETEST":1.15,"GAP_GO":1.30,"MEAN_REVERSION":0.55,"LATE_REVERSAL":0.60},
        "TREND": {"ORB":1.10,"VWAP_MOMENTUM":1.30,"BREAKOUT_RETEST":1.25,"GAP_GO":1.10,"MEAN_REVERSION":0.60,"LATE_REVERSAL":0.70},
        "RANGE": {"ORB":0.70,"VWAP_MOMENTUM":0.75,"BREAKOUT_RETEST":0.90,"GAP_GO":0.70,"MEAN_REVERSION":1.35,"LATE_REVERSAL":1.05},
        "LATE": {"ORB":0.55,"VWAP_MOMENTUM":0.85,"BREAKOUT_RETEST":0.80,"GAP_GO":0.45,"MEAN_REVERSION":1.10,"LATE_REVERSAL":1.40},
        "MIXED": {name:1.0 for name in V76_AGENT_NAMES},
    }
    base = num(base_by_regime.get(regime, base_by_regime["MIXED"]).get(agent_name, 1.0))
    if not V76_DB_READY:
        return base, {"base":base,"history":1.0,"n":0}
    key=(agent_name,symbol,regime)
    cached=V76_WEIGHT_CACHE.get(key)
    if cached and time.time()-num(cached.get("ts")) < V76_WEIGHT_CACHE_SECONDS:
        return cached["weight"], cached["detail"]
    row=_v68_db_execute("""
    SELECT COUNT(*) FILTER(WHERE final_r IS NOT NULL AND status<>'AMBIGUOUS'),
           COALESCE(AVG(final_r) FILTER(WHERE final_r IS NOT NULL AND status<>'AMBIGUOUS'),0),
           COUNT(*) FILTER(WHERE final_r IS NOT NULL AND status<>'AMBIGUOUS' AND symbol=%s),
           COALESCE(AVG(final_r) FILTER(WHERE final_r IS NOT NULL AND status<>'AMBIGUOUS' AND symbol=%s),0),
           COUNT(*) FILTER(WHERE final_r IS NOT NULL AND status<>'AMBIGUOUS' AND regime=%s),
           COALESCE(AVG(final_r) FILTER(WHERE final_r IS NOT NULL AND status<>'AMBIGUOUS' AND regime=%s),0)
      FROM fh_v76_equity_agent_shadow WHERE agent_name=%s
    """,(symbol,symbol,regime,regime,agent_name),fetch="one")
    n,avg_r,sn,savg,rn,ravg = row if row else (0,0,0,0,0,0)
    history = 1.0
    history += 0.18 * np.tanh(num(avg_r)) * min(1.0, num(n)/30.0)
    history += 0.12 * np.tanh(num(savg)) * min(1.0, num(sn)/10.0)
    history += 0.08 * np.tanh(num(ravg)) * min(1.0, num(rn)/15.0)
    history = min(1.30,max(0.70,history))
    weight = min(1.60,max(0.45,base*history))
    detail={"base":round(base,3),"history":round(history,3),"n":int(n or 0),"avg_r":round(num(avg_r),3),"symbol_n":int(sn or 0),"symbol_avg_r":round(num(savg),3),"regime_n":int(rn or 0),"regime_avg_r":round(num(ravg),3)}
    V76_WEIGHT_CACHE[key]={"ts":time.time(),"weight":weight,"detail":detail}
    return weight,detail


def _v76_meta_candidate(ctx, candidates):
    if not candidates:
        return None
    weighted=[]
    evidence={"LONG":0.0,"SHORT":0.0}
    for c in candidates:
        weight,detail=_v76_agent_history_weight(c["agent_name"],c["symbol"],ctx["regime"])
        utility=weight*max(0.0,num(c["score"])-50.0)
        evidence[c["direction"]]+=utility
        weighted.append((c,weight,detail,utility))
    winner="LONG" if evidence["LONG"]>=evidence["SHORT"] else "SHORT"
    loser="SHORT" if winner=="LONG" else "LONG"
    if evidence[winner] <= 0:
        return None
    if evidence[loser] > 0 and evidence[winner] / max(evidence[loser],1e-12) < V76_META_MIN_DIRECTION_EDGE:
        return None
    agree=[x for x in weighted if x[0]["direction"]==winner]
    conflict=[x for x in weighted if x[0]["direction"]!=winner]
    denom=sum(x[1] for x in agree) or 1.0
    weighted_score=sum(num(x[0]["score"])*x[1] for x in agree)/denom
    consensus_bonus=min(8.0,max(0,len(agree)-1)*3.0)
    conflict_penalty=min(10.0,len(conflict)*3.0)
    meta_score=min(100.0,max(0.0,weighted_score+consensus_bonus-conflict_penalty))
    primary=max(agree,key=lambda x:num(x[0]["score"])*x[1])
    primary_c=primary[0]
    if meta_score < V76_META_MIN_SCORE:
        return None
    if len(agree)<2 and num(primary_c["score"])<V76_META_STRONG_SINGLE_SCORE:
        return None
    source_key=hashlib.sha256(f"{V76_EQUITY_VERSION}|META|{ctx['symbol']}|{ctx['session_date']}".encode("utf-8")).hexdigest()[:40]
    weights_payload={x[0]["agent_name"]:{"weight":round(x[1],3),**x[2]} for x in weighted}
    signal=dict(primary_c)
    signal.update({
        "version":V76_EQUITY_VERSION,"agent_name":"META","setup_type":primary_c["agent_name"],
        "primary_agent":primary_c["agent_name"],"score":round(meta_score,1),"regime":ctx["regime"],
        "source_key":source_key,"supporting_agents":[x[0]["agent_name"] for x in agree],
        "conflicting_agents":[x[0]["agent_name"] for x in conflict],"agent_weights":weights_payload,
        "meta_evidence":{k:round(v,3) for k,v in evidence.items()},
        "reasons":[f"meta-agent {len(agree)} supporting specialist(s), regime {ctx['regime']}",f"primary {primary_c['agent_name']} score {num(primary_c['score']):.1f}"] + list(primary_c.get("reasons") or []),
        "live_strategy_tag":f"EQUITY_META_{primary_c['agent_name']}",
    })
    return signal


def _v76_store_meta(meta, v75_result):
    if not V76_DB_READY or not meta:
        return False
    gate=(v75_result or {}).get("gate") or {}
    outcome=(v75_result or {}).get("outcome") or {}
    return bool(_v68_db_execute("""
    INSERT INTO fh_v76_equity_meta
    (source_key,version,symbol,session_date,direction,regime,meta_score,primary_agent,supporting_agents,conflicting_agents,weights,v75_decision,v75_reasons,executed,payload,updated_at)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s,%s::jsonb,%s,%s::jsonb,NOW())
    ON CONFLICT(source_key) DO UPDATE SET direction=EXCLUDED.direction,regime=EXCLUDED.regime,meta_score=EXCLUDED.meta_score,
      primary_agent=EXCLUDED.primary_agent,supporting_agents=EXCLUDED.supporting_agents,conflicting_agents=EXCLUDED.conflicting_agents,
      weights=EXCLUDED.weights,v75_decision=EXCLUDED.v75_decision,v75_reasons=EXCLUDED.v75_reasons,
      executed=(fh_v76_equity_meta.executed OR EXCLUDED.executed),payload=EXCLUDED.payload,updated_at=NOW()
    RETURNING source_key
    """,(
        meta["source_key"],V76_EQUITY_VERSION,meta["symbol"],meta["session_date"],meta["direction"],meta["regime"],num(meta["score"]),meta["primary_agent"],
        json.dumps(meta.get("supporting_agents") or []),json.dumps(meta.get("conflicting_agents") or []),json.dumps(meta.get("agent_weights") or {}),
        gate.get("decision"),json.dumps(gate.get("reasons") or []),bool(outcome.get("executed")),json.dumps(_clean_json_value({"meta":meta,"v75":v75_result})),
    ),fetch="one"))


def _v76_open_agent_rows():
    if not V76_DB_READY:
        return []
    rows=_v68_db_execute("""SELECT source_key,agent_name,symbol,direction,regime,entry,stop,risk,tp1,tp2,tp3,tracking_start,expiry_ts,last_checked,
        tp1_hit,tp2_hit,tp3_hit,tp1_time,tp2_time,tp3_time,mfe_r,mae_r FROM fh_v76_equity_agent_shadow WHERE status='OPEN' ORDER BY tracking_start""",fetch="all") or []
    keys=["source_key","agent_name","symbol","direction","regime","entry","stop","risk","tp1","tp2","tp3","tracking_start","expiry_ts","last_checked",
          "tp1_hit","tp2_hit","tp3_hit","tp1_time","tp2_time","tp3_time","mfe_r","mae_r"]
    return [dict(zip(keys,row)) for row in rows]


def _v76_update_agent_trade(trade):
    return _v68_db_execute("""UPDATE fh_v76_equity_agent_shadow SET last_checked=%s,status=%s,final_r=%s,mfe_r=%s,mae_r=%s,
        tp1_hit=%s,tp2_hit=%s,tp3_hit=%s,tp1_time=%s,tp2_time=%s,tp3_time=%s,closed_time=%s,
        settled_at=CASE WHEN %s='OPEN' THEN settled_at ELSE COALESCE(settled_at,NOW()) END WHERE source_key=%s""",(
        trade.get("last_checked"),trade.get("status"),trade.get("final_r"),trade.get("mfe_r"),trade.get("mae_r"),
        trade.get("tp1_hit"),trade.get("tp2_hit"),trade.get("tp3_hit"),trade.get("tp1_time"),trade.get("tp2_time"),trade.get("tp3_time"),trade.get("closed_time"),
        trade.get("status"),trade.get("source_key")
    ))


def _v76_settle_agent_trade(trade):
    df=get_candles(trade["symbol"],"Min1")
    if df is None or len(df)==0:
        return None
    df=df.copy(); df["time_norm"]=df["time"].apply(normalize_candle_time)
    relevant=df[(df["time_norm"]>num(trade.get("last_checked"))) & (df["time_norm"]>=num(trade.get("tracking_start")))]
    latest_price=num(df.iloc[-1]["close"]); latest_seen=num(trade.get("last_checked")); event=None
    direction=str(trade.get("direction") or "").upper(); entry=num(trade.get("entry")); risk=max(num(trade.get("risk")),1e-12)
    mfe=num(trade.get("mfe_r")); mae=num(trade.get("mae_r"))
    for _,candle in relevant.iterrows():
        t=num(candle["time_norm"]); latest_seen=max(latest_seen,t); high=num(candle["high"]); low=num(candle["low"])
        if direction=="LONG":
            mfe=max(mfe,(high-entry)/risk); mae=min(mae,(low-entry)/risk)
        else:
            mfe=max(mfe,(entry-low)/risk); mae=min(mae,(entry-high)/risk)
        stage=2 if trade.get("tp2_hit") else (1 if trade.get("tp1_hit") else 0)
        stop_r=_v75_shadow_stop_r(trade,stage)
        managed_stop=(entry+stop_r*risk) if direction=="LONG" else (entry-stop_r*risk)
        stop_touched=low<=managed_stop if direction=="LONG" else high>=managed_stop
        if not trade.get("tp1_hit"):
            next_target=high>=num(trade.get("tp1")) if direction=="LONG" else low<=num(trade.get("tp1"))
        elif not trade.get("tp2_hit"):
            next_target=high>=num(trade.get("tp2")) if direction=="LONG" else low<=num(trade.get("tp2"))
        else:
            next_target=high>=num(trade.get("tp3")) if direction=="LONG" else low<=num(trade.get("tp3"))
        if stop_touched and next_target:
            trade["status"]="AMBIGUOUS"; trade["final_r"]=None; trade["closed_time"]=t; event="AMBIGUOUS"; break
        if stop_touched:
            trade["status"]="STOP"; trade["final_r"]=round(_v75_shadow_final_r_at_stop(trade,stage),4); trade["closed_time"]=t; event="STOP"; break
        tp3=high>=num(trade.get("tp3")) if direction=="LONG" else low<=num(trade.get("tp3"))
        tp2=high>=num(trade.get("tp2")) if direction=="LONG" else low<=num(trade.get("tp2"))
        tp1=high>=num(trade.get("tp1")) if direction=="LONG" else low<=num(trade.get("tp1"))
        if tp3:
            trade["tp1_hit"]=trade["tp2_hit"]=trade["tp3_hit"]=True
            if not trade.get("tp1_time"): trade["tp1_time"]=t
            if not trade.get("tp2_time"): trade["tp2_time"]=t
            trade["tp3_time"]=t; trade["status"]="TP3"; trade["final_r"]=2.25; trade["closed_time"]=t; event="TP3"; break
        if tp2 and not trade.get("tp2_hit"):
            trade["tp1_hit"]=trade["tp2_hit"]=True
            if not trade.get("tp1_time"): trade["tp1_time"]=t
            trade["tp2_time"]=t
        elif tp1 and not trade.get("tp1_hit"):
            trade["tp1_hit"]=True; trade["tp1_time"]=t
    trade["last_checked"]=latest_seen; trade["mfe_r"]=round(mfe,4); trade["mae_r"]=round(mae,4); trade.setdefault("status","OPEN")
    if trade.get("status")=="OPEN" and time.time()>=num(trade.get("expiry_ts")):
        current_r=current_r_for_price(trade,latest_price)
        trade["final_r"]=round(max(-1.0,min(2.25,_v75_shadow_current_final_r(trade,current_r))),4)
        trade["status"]="EXPIRED"; trade["closed_time"]=time.time(); event="EXPIRED"
    _v76_update_agent_trade(trade)
    if event:
        V76_WEIGHT_CACHE.clear()
    return event


def _v76_sync_agent_outcomes():
    if not V76_DB_READY:
        return 0
    settled=0
    for trade in _v76_open_agent_rows():
        try:
            event=_v76_settle_agent_trade(trade)
            if event:
                settled+=1
                print(f"V7.6 agent settled {trade['agent_name']} {trade['symbol']} {trade['direction']} → {event} ({trade.get('final_r')})")
        except Exception as error:
            print(f"V7.6 agent settlement warning {trade.get('agent_name')} {trade.get('symbol')}: {type(error).__name__}: {error}")
        time.sleep(0.02)
    return settled


def _v76_scan_equity_agents():
    stats={"agent_signals":0,"meta_candidates":0,"would_take":0,"symbols":0}
    if not V76_DB_READY:
        return stats
    tickers=_v73_fetch_equity_tickers()
    if tickers:
        print(f"V7.6 Equity Agents: scanning {len(tickers)} liquid stock/index future(s) — 6 AGENTS + META, SHADOW FIRST")
    for ticker in tickers:
        symbol=str(ticker.get("symbol") or "")
        try:
            ctx=_v76_market_snapshot(symbol,ticker)
            if ctx is None:
                continue
            stats["symbols"]+=1
            candidates=[]
            for fn in V76_AGENT_FUNCTIONS:
                try:
                    c=fn(ctx)
                    if c:
                        candidates.append(c)
                        if _v76_store_agent_shadow(c):
                            stats["agent_signals"]+=1
                            print(f"V7.6 AGENT {c['agent_name']}: {symbol} {c['direction']} score={c['score']:.1f} regime={ctx['regime']}")
                except Exception as agent_error:
                    print(f"V7.6 agent warning {fn.__name__} {symbol}: {type(agent_error).__name__}: {agent_error}")
            meta=_v76_meta_candidate(ctx,candidates)
            if meta:
                stats["meta_candidates"]+=1
                v75=_v75_process_equity_candidate(meta)
                if ((v75 or {}).get("gate") or {}).get("eligible"):
                    stats["would_take"]+=1
                _v76_store_meta(meta,v75)
                print(
                    f"V7.6 META: {symbol} {meta['direction']} score={meta['score']:.1f} regime={meta['regime']} "
                    f"primary={meta['primary_agent']} support={','.join(meta.get('supporting_agents') or [])} "
                    f"→ {((v75 or {}).get('gate') or {}).get('decision','N/A')}"
                )
        except Exception as error:
            print(f"V7.6 Equity Ensemble {symbol} warning: {type(error).__name__}: {error}")
        time.sleep(0.04)
    return stats


def _v76_equity_agent_summary_text():
    if not V76_DB_READY:
        return "V7.6 Equity Agent Ensemble is waiting for Postgres."
    rows=_v68_db_execute("""
      SELECT agent_name,COUNT(*),COUNT(*) FILTER(WHERE final_r IS NOT NULL),
             COALESCE(AVG(final_r) FILTER(WHERE final_r IS NOT NULL),0),
             COALESCE(SUM(final_r) FILTER(WHERE final_r IS NOT NULL),0),
             COUNT(*) FILTER(WHERE final_r>0),COUNT(*) FILTER(WHERE status='OPEN')
      FROM fh_v76_equity_agent_shadow GROUP BY agent_name ORDER BY agent_name
    """,fetch="all") or []
    meta=_v68_db_execute("""SELECT COUNT(*),COUNT(*) FILTER(WHERE v75_decision='ALLOW'),COUNT(*) FILTER(WHERE executed) FROM fh_v76_equity_meta""",fetch="one") or (0,0,0)
    lines=[
        "🧠 V7.6 EQUITY AGENT ENSEMBLE",
        f"Universe: top {V73_EQUITY_TOP_N} MEXC USDT stock/index futures | Meta threshold {V76_META_MIN_SCORE:.0f}",
        f"Meta candidates: {int(meta[0] or 0)} | WOULD-TAKE: {int(meta[1] or 0)} | Actual live: {int(meta[2] or 0)}",
        "",
    ]
    seen=set()
    for agent,total,settled,avg_r,total_r,wins,open_n in rows:
        seen.add(str(agent)); wr=100.0*num(wins)/max(1,num(settled))
        lines.append(f"{agent}: {int(total)} signals | {int(settled)} settled | {wr:.1f}% win | {num(avg_r):+.2f}R avg | {num(total_r):+.2f}R | open {int(open_n)}")
    for agent in V76_AGENT_NAMES:
        if agent not in seen:
            lines.append(f"{agent}: no signals yet")
    lines.append("Weights use regime priors plus shrinkage toward each agent's settled global/symbol/regime expectancy; promotion remains manual.")
    return "\n".join(lines)

_V70_PREV_COMMAND = handle_telegram_command

def handle_telegram_command(chat_id, text):
    parts = (text or "").strip().split()
    command = parts[0].lower() if parts else ""
    if command in {"/livepnl", "/live", "/livepilot"}:
        if V70_LIVE is None:
            send_to_chat(chat_id, "V7.0 Live Pilot module unavailable.")
        else:
            send_to_chat(chat_id, V70_LIVE.live_pnl_text())
        return
    if command in {"/liveposition", "/livepos", "/positionlive"}:
        if V70_LIVE is None:
            send_to_chat(chat_id, "V7 Live Pilot module unavailable.")
        else:
            send_to_chat(chat_id, V70_LIVE.live_position_text())
        return
    if command in {"/equitylive", "/stocklive", "/orblive"}:
        send_to_chat(chat_id, _v75_equity_live_summary_text())
        return
    if command in {"/agents", "/equityagents", "/ensemble", "/v76"}:
        send_to_chat(chat_id, _v76_equity_agent_summary_text())
        return
    return _V70_PREV_COMMAND(chat_id, text)

_V70_PAPER_CREATE = create_paper_trade

# V7.1 SELECTION + CHALLENGER LAYER
# V6.9.1 remains the immutable paper control. This layer only decides whether
# a Core ENTRY is eligible for the tiny live pilot and records counterfactuals.
V70_SELECTIVE_GATE = os.getenv("V70_SELECTIVE_GATE", "true").lower() == "true"
V70_SAME_SYMBOL_COOLDOWN_MINUTES = int(os.getenv("V70_SAME_SYMBOL_COOLDOWN_MINUTES", "120"))
V71_REGIME_GATE = os.getenv("V71_REGIME_GATE", "true").lower() == "true"
V71_LOSS_CLUSTER_GATE = os.getenv("V71_LOSS_CLUSTER_GATE", "true").lower() == "true"
V71_LOSS_CLUSTER_MINUTES = int(os.getenv("V71_LOSS_CLUSTER_MINUTES", "180"))
V71_LOSS_CLUSTER_COUNT = int(os.getenv("V71_LOSS_CLUSTER_COUNT", "3"))
V71_LIVE_CORRELATION_GATE = os.getenv("V71_LIVE_CORRELATION_GATE", "true").lower() == "true"
V71_MAX_SAME_BUCKET_DIRECTION = min(3, max(1, int(os.getenv("V71_MAX_SAME_BUCKET_DIRECTION", "1"))))
V71_COST_GATE = os.getenv("V71_COST_GATE", "true").lower() == "true"
V71_MAX_COST_FRACTION_R = float(os.getenv("V71_MAX_COST_FRACTION_R", "0.35"))
V71_EST_TAKER_FEE_BPS = float(os.getenv("V71_EST_TAKER_FEE_BPS", "5.0"))
V71_EST_SLIPPAGE_BPS = float(os.getenv("V71_EST_SLIPPAGE_BPS", "4.0"))
V71_STRUCTURAL_RESET_SCORE_DELTA = float(os.getenv("V71_STRUCTURAL_RESET_SCORE_DELTA", "8.0"))
# V7.2 adaptive quality gate: when the recent tape/regime is degraded, demand a
# stronger Core score plus Strategy confirmation. V6.9.1 paper control is untouched.
V72_ADAPTIVE_GATE = os.getenv("V72_ADAPTIVE_GATE", "true").lower() == "true"
V72_DEGRADED_MIN_CORE = float(os.getenv("V72_DEGRADED_MIN_CORE", "80.0"))
V72_DEGRADED_MIN_SELECTOR = float(os.getenv("V72_DEGRADED_MIN_SELECTOR", "68.0"))
V71_CHALLENGER_DB_READY = False

def _v71_init_challenger():
    global V71_CHALLENGER_DB_READY
    # V7 startup runs before async main(); ensure the shared V6.8 PostgreSQL
    # layer is initialized here rather than falsely degrading to local-only.
    if not V68_DB_READY:
        if not v68_init_database():
            print(f"[V7DIAG] challenger DB bootstrap failed: {V68_DB_LAST_ERROR or 'unknown database error'}", flush=True)
            return False
    ok = _v68_db_execute("""CREATE TABLE IF NOT EXISTS fh_v71_challenger (
        source_key TEXT PRIMARY KEY, evaluated_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        symbol TEXT NOT NULL, direction TEXT NOT NULL, core_score DOUBLE PRECISION,
        risk_decision TEXT, strategy_consensus TEXT, btc_weak BOOLEAN,
        macro_conflict BOOLEAN, recent_stop_count INTEGER, estimated_cost_r DOUBLE PRECISION,
        selector_score DOUBLE PRECISION, decision TEXT NOT NULL, reasons JSONB NOT NULL,
        payload JSONB NOT NULL, actual_status TEXT, actual_final_r DOUBLE PRECISION,
        settled_at TIMESTAMPTZ
    )""")
    V71_CHALLENGER_DB_READY = bool(ok)
    return V71_CHALLENGER_DB_READY

def _v71_recent_same_symbol_stop(trades, symbol, direction):
    now_ts=time.time(); cutoff=now_ts-max(1,V70_SAME_SYMBOL_COOLDOWN_MINUTES)*60
    candidates=[]
    for t in trades or []:
        if t.get("status") == "STOP" and t.get("symbol") == symbol and t.get("direction") == direction:
            closed=num(t.get("closed_time"))
            if closed >= cutoff:
                candidates.append(t)
    return max(candidates, key=lambda x:num(x.get("closed_time")), default=None)

def _v71_bucket_stop_cluster(trades, result):
    now=time.time(); cutoff=now-max(1,V71_LOSS_CLUSTER_MINUTES)*60
    bucket=((result.get("risk_challenger") or {}).get("asset_bucket") or _v681_asset_bucket(result.get("symbol")))
    direction=result.get("direction")
    hits=[]
    for t in trades or []:
        if t.get("status") != "STOP" or t.get("direction") != direction: continue
        if num(t.get("closed_time")) < cutoff: continue
        if _v681_asset_bucket(t.get("symbol")) == bucket: hits.append(t.get("symbol"))
    return len(hits), hits

def _v71_estimated_cost_r(result):
    plan=result.get("risk_plan") or {}; entry=max(num(result.get("price")),1e-12)
    stop_pct=abs(entry-num(plan.get("stop")))/entry if plan.get("stop") is not None else 0
    if stop_pct <= 0: return 999.0
    notional=min(50.0, 0.50/stop_pct)
    # conservative round trip: taker on entry + exit, plus slippage on both legs.
    cost=notional*(2*V71_EST_TAKER_FEE_BPS+2*V71_EST_SLIPPAGE_BPS)/10000.0
    actual_risk=min(0.50,notional*stop_pct)
    return cost/max(actual_risk,1e-9)

def _v71_structural_reset(result, prior_stop):
    if not prior_stop: return False
    score_delta=num(result.get("best_score"))-num(prior_stop.get("score"))
    risk=result.get("live_risk_challenger") or result.get("risk_challenger") or {}; strategy=result.get("strategy_ensemble") or {}
    # A cooldown can be overridden only by a materially stronger context, not time alone.
    regime_ok=(not bool(risk.get("btc_weak")) and not bool(risk.get("macro_conflict")))
    strategy_ok=str(strategy.get("consensus") or "").upper() in {"CONFIRM","STRONG_CONFIRM"}
    return score_delta >= V71_STRUCTURAL_RESET_SCORE_DELTA and regime_ok and strategy_ok

def _v71_live_risk_snapshot():
    """Fetch fail-closed live exposure/loss context from the executor.

    This deliberately does not use paper trades. If the live account/ledger
    cannot be read, the live selector must skip rather than assume zero risk.
    """
    if V70_LIVE is None or not getattr(V70_LIVE, "ENABLED", False):
        return {"ok": False, "reason": "live executor unavailable/disabled"}
    fn = getattr(V70_LIVE, "live_risk_snapshot", None)
    if not callable(fn):
        return {"ok": False, "reason": "live executor lacks live_risk_snapshot"}
    try:
        snap = fn(max(V71_LOSS_CLUSTER_MINUTES, V70_SAME_SYMBOL_COOLDOWN_MINUTES))
        if not isinstance(snap, dict):
            return {"ok": False, "reason": "invalid live risk snapshot"}
        snap["ok"] = True
        return snap
    except Exception as e:
        return {"ok": False, "reason": f"live risk snapshot failed: {type(e).__name__}: {e}"}


def _v71_live_same_bucket_open(snapshot, symbol, direction):
    bucket = _v681_asset_bucket(symbol)
    count = 0; symbols = []
    for p in (snapshot or {}).get("open_positions") or []:
        if str(p.get("direction") or "").upper() != str(direction or "").upper():
            continue
        psym = str(p.get("symbol") or "")
        if _v681_asset_bucket(psym) != bucket:
            continue
        count += 1; symbols.append(psym)
    return count, symbols


def _v71_live_recent_losses(snapshot, result, minutes=None, same_symbol=False):
    direction = str(result.get("direction") or "").upper()
    symbol = str(result.get("symbol") or "")
    bucket = _v681_asset_bucket(symbol)
    cutoff = time.time() - max(1, int(minutes or V71_LOSS_CLUSTER_MINUTES)) * 60
    hits = []
    for row in (snapshot or {}).get("recent_closed") or []:
        if not bool(row.get("stop_like")):
            continue
        if str(row.get("direction") or "").upper() != direction:
            continue
        if num(row.get("closed_time")) < cutoff:
            continue
        rsym = str(row.get("symbol") or "")
        if same_symbol:
            if rsym != symbol:
                continue
        elif _v681_asset_bucket(rsym) != bucket:
            continue
        hits.append(row)
    return hits


def _v71_build_live_risk(result, snapshot):
    """Recompute the Risk Lab view from actual live exposure and live losses.

    Macro/BTC regime logic is preserved. Only portfolio exposure and recent
    stop/loss history are sourced from the real MEXC account + live ledger.
    """
    now_ts = time.time()
    direction = result.get("direction")
    symbol = result.get("symbol")
    bucket = _v681_asset_bucket(symbol)
    macro = get_macro_snapshot() or {}
    combined = num(macro.get("combined_score"))
    event_risk = str(macro.get("event_risk") or "LOW").upper()
    btc = _v681_btc_scan_snapshot()
    open_count, open_symbols = _v71_live_same_bucket_open(snapshot, symbol, direction)
    live_losses = _v71_live_recent_losses(snapshot, result, V681_STOP_CLUSTER_MINUTES)
    stop_count = len(live_losses)
    macro_conflict, strong_macro_conflict = _v681_directional_macro_conflict(direction, bucket, combined)
    btc_weak = _v681_btc_is_weak_for(direction, bucket, btc)
    multiplier = 1.0; reasons = []
    if event_risk == "EXTREME":
        multiplier = 0.0; reasons.append("EXTREME scheduled-event danger window")
    elif event_risk == "HIGH":
        multiplier = min(multiplier, 0.5); reasons.append("HIGH scheduled-event risk")
    if macro_conflict:
        reasons.append(f"{bucket} {direction} conflicts with macro/news regime {combined:+.1f}")
        if strong_macro_conflict:
            multiplier = min(multiplier, 0.5)
    if btc_weak:
        reasons.append(
            "BTC is not aligned at ENTRY-strength "
            f"({btc.get('direction') or 'N/A'} {btc.get('state') or 'N/A'} {num(btc.get('score')):.1f})"
        )
    if open_count >= V681_MAX_CORRELATED_OPEN:
        multiplier = min(multiplier, 0.5)
        reasons.append(f"{open_count} correlated LIVE {bucket} {direction} positions already open")
    if open_count >= V681_HARD_CORRELATED_OPEN:
        multiplier = 0.0; reasons.append(f"hard LIVE correlated-exposure cap reached ({open_count})")
    if bucket == "CRYPTO" and macro_conflict and btc_weak and open_count >= V681_MAX_CORRELATED_OPEN:
        multiplier = 0.0; reasons.append("macro conflict + weak BTC + crowded same-direction LIVE crypto book")
    elif bucket == "CRYPTO" and macro_conflict and btc_weak:
        multiplier = min(multiplier, 0.5); reasons.append("macro conflict confirmed by weak BTC structure")
    if stop_count >= V681_STOP_CLUSTER_COUNT:
        multiplier = 0.0
        reasons.append(f"cooldown: {stop_count} LIVE same-bucket {direction} losses in last {V681_STOP_CLUSTER_MINUTES}m")
    elif stop_count == 1 and macro_conflict:
        multiplier = min(multiplier, 0.5); reasons.append("recent LIVE loss + macro conflict")
    if multiplier <= 0:
        decision = "BLOCK"; multiplier = 0.0
    elif multiplier < 1.0:
        decision = "REDUCE"
    elif reasons:
        decision = "CAUTION"
    else:
        decision = "ALLOW"
    return {
        "version": "7.4.4-live-risk", "evaluated_ts": now_ts, "decision": decision,
        "size_multiplier": multiplier, "reasons": reasons, "symbol": symbol,
        "direction": direction, "asset_bucket": bucket, "macro_score": combined,
        "event_risk": event_risk, "btc_weak": btc_weak, "macro_conflict": macro_conflict,
        "open_count": open_count, "open_symbols": open_symbols, "recent_stop_count": stop_count,
        "recent_stop_symbols": [x.get("symbol") for x in live_losses],
        "exposure_source": (snapshot or {}).get("source") or "LIVE",
    }


def _v71_selector_score(result, cost_r, cluster_count):
    risk=result.get("live_risk_challenger") or result.get("risk_challenger") or {}; strategy=result.get("strategy_ensemble") or {}
    score=num(result.get("best_score"))
    rd=str(risk.get("decision") or "NO_DATA").upper(); sc=str(strategy.get("consensus") or "NO_DATA").upper()
    score += {"ALLOW":8,"CAUTION":3,"REDUCE":-4,"BLOCK":-25}.get(rd,0)
    score += {"STRONG_CONFIRM":8,"CONFIRM":5,"MIXED":0,"WAIT":-4,"AVOID":-10}.get(sc,0)
    if risk.get("btc_weak"): score-=8
    if risk.get("macro_conflict"): score-=6
    score-=min(12,cluster_count*4)
    score-=min(12,cost_r*10)
    return round(score,2)

def _v71_live_candidate_gate(result, trades):
    if not V70_SELECTIVE_GATE:
        return {"eligible":True,"decision":"ALLOW","reasons":["selector disabled"],"selector_score":num(result.get("best_score"))}
    live_snapshot = _v71_live_risk_snapshot()
    if not live_snapshot.get("ok"):
        return {"eligible":False,"decision":"SKIP","reasons":[live_snapshot.get("reason") or "live risk context unavailable"],
                "selector_score":0.0,"evaluated_ts":time.time(),"version":"7.4.4-live-risk"}
    risk = _v71_build_live_risk(result, live_snapshot)
    result["live_risk_challenger"] = risk
    strategy=result.get("strategy_ensemble") or {}
    rd=str(risk.get("decision") or "NO_DATA").upper(); sc=str(strategy.get("consensus") or "NO_DATA").upper()
    reasons=[]; notes=[
        f"live exposure source={risk.get('exposure_source')}",
        f"live same-direction bucket positions={risk.get('open_count',0)}",
    ]
    if rd == "BLOCK": reasons.append("Risk Lab BLOCK")
    if rd == "BLOCK" and sc in {"WAIT","AVOID"}: reasons.append(f"Risk BLOCK + Strategy {sc}")
    if (
        V71_LIVE_CORRELATION_GATE
        and risk.get("asset_bucket") == "CRYPTO"
        and int(risk.get("open_count") or 0) >= V71_MAX_SAME_BUCKET_DIRECTION
    ):
        reasons.append(
            f"live correlation guard: {int(risk.get('open_count') or 0)} "
            f"CRYPTO {result.get('direction')} position(s) already open "
            f"(max {V71_MAX_SAME_BUCKET_DIRECTION})"
        )
    if V71_REGIME_GATE and (risk.get("asset_bucket") == "CRYPTO") and bool(risk.get("btc_weak")) and bool(risk.get("macro_conflict")):
        reasons.append("crypto regime conflict: weak BTC + macro conflict")
    same_symbol_losses = _v71_live_recent_losses(live_snapshot, result, V70_SAME_SYMBOL_COOLDOWN_MINUTES, same_symbol=True)
    prior = max(same_symbol_losses, key=lambda x:num(x.get("closed_time")), default=None)
    if prior:
        # Live ledger rows do not yet persist the original Core score, so do not
        # invent a structural-reset comparison. A real same-symbol stop-like
        # loss keeps its cooldown until the configured time window expires.
        age=(time.time()-num(prior.get("closed_time")))/60
        reasons.append(f"same-symbol LIVE loss cooldown ({age:.0f}m ago)")
    cluster_rows = _v71_live_recent_losses(live_snapshot, result, V71_LOSS_CLUSTER_MINUTES)
    cluster_count = len(cluster_rows); cluster_symbols = [x.get("symbol") for x in cluster_rows]
    if V71_LOSS_CLUSTER_GATE and cluster_count >= V71_LOSS_CLUSTER_COUNT:
        reasons.append(f"LIVE loss-cluster breaker: {cluster_count} same-bucket {result.get('direction')} losses/{V71_LOSS_CLUSTER_MINUTES}m")
    cost_r=_v71_estimated_cost_r(result)
    if V71_COST_GATE and cost_r > V71_MAX_COST_FRACTION_R:
        reasons.append(f"estimated execution drag {cost_r:.2f}R > {V71_MAX_COST_FRACTION_R:.2f}R")
    selector_score=_v71_selector_score(result,cost_r,cluster_count)
    degraded = bool(
        cluster_count > 0
        or risk.get("btc_weak")
        or risk.get("macro_conflict")
        or rd in {"CAUTION", "REDUCE"}
        or sc in {"MIXED", "WAIT", "AVOID", "NO_DATA"}
    )
    if V72_ADAPTIVE_GATE and degraded:
        core_score = num(result.get("best_score"))
        strategy_confirmed = sc in {"CONFIRM", "STRONG_CONFIRM"}
        if core_score < V72_DEGRADED_MIN_CORE:
            reasons.append(f"adaptive degraded-regime Core threshold: {core_score:.1f} < {V72_DEGRADED_MIN_CORE:.1f}")
        if not strategy_confirmed:
            reasons.append(f"adaptive degraded-regime Strategy confirmation required ({sc})")
        if selector_score < V72_DEGRADED_MIN_SELECTOR:
            reasons.append(f"adaptive selector threshold: {selector_score:.1f} < {V72_DEGRADED_MIN_SELECTOR:.1f}")
    return {"eligible":not reasons,"decision":"ALLOW" if not reasons else "SKIP","reasons":reasons,
            "notes":notes,"risk_decision":rd,"strategy_consensus":sc,"btc_weak":bool(risk.get("btc_weak")),
            "macro_conflict":bool(risk.get("macro_conflict")),"recent_stop_count":cluster_count,
            "recent_stop_symbols":cluster_symbols,"estimated_cost_r":round(cost_r,4),"selector_score":selector_score,
            "evaluated_ts":time.time(),"version":"7.4.4-live-risk","live_exposure_count":risk.get("open_count",0),"live_exposure_symbols":risk.get("open_symbols",[]),"exposure_source":risk.get("exposure_source")}

def _v71_store_challenger(trade, gate):
    if not V71_CHALLENGER_DB_READY or not trade: return
    try:
        _v68_db_execute("""INSERT INTO fh_v71_challenger
        (source_key,symbol,direction,core_score,risk_decision,strategy_consensus,btc_weak,macro_conflict,
         recent_stop_count,estimated_cost_r,selector_score,decision,reasons,payload)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT(source_key) DO NOTHING""",(
            trade.get("source_key"),trade.get("symbol"),trade.get("direction"),num(trade.get("score")),
            gate.get("risk_decision"),gate.get("strategy_consensus"),gate.get("btc_weak"),gate.get("macro_conflict"),
            gate.get("recent_stop_count"),gate.get("estimated_cost_r"),gate.get("selector_score"),gate.get("decision"),
            json.dumps(gate.get("reasons") or []),json.dumps(gate)))
    except Exception as e: print(f"V7.1 challenger store warning: {e}")

def _v71_settle_challenger(trades):
    if not V71_CHALLENGER_DB_READY: return
    for t in trades or []:
        if t.get("status") == "OPEN" or t.get("final_r") is None: continue
        _v68_db_execute("""UPDATE fh_v71_challenger SET actual_status=%s,actual_final_r=%s,settled_at=NOW()
        WHERE source_key=%s AND settled_at IS NULL""",(t.get("status"),num(t.get("final_r")),t.get("source_key")))

def _v71_challenger_text():
    if not V71_CHALLENGER_DB_READY: return "V7.1 Challenger: database unavailable"
    row=_v68_db_execute("""SELECT COUNT(*),COUNT(*) FILTER(WHERE decision='ALLOW'),
      COUNT(*) FILTER(WHERE decision='SKIP'),COUNT(*) FILTER(WHERE settled_at IS NOT NULL),
      COALESCE(AVG(actual_final_r) FILTER(WHERE decision='ALLOW' AND settled_at IS NOT NULL),0),
      COALESCE(AVG(actual_final_r) FILTER(WHERE decision='SKIP' AND settled_at IS NOT NULL),0),
      COALESCE(SUM(actual_final_r) FILTER(WHERE decision='ALLOW' AND settled_at IS NOT NULL),0)
      FROM fh_v71_challenger""",fetch="one")
    if not row: return "V7.1 Challenger: no data"
    return (f"V7.1 SELECTION CHALLENGER\nObserved: {row[0]} | ALLOW {row[1]} | SKIP {row[2]} | settled {row[3]}\n"
            f"ALLOW expectancy: {num(row[4]):+.2f}R\nSkipped-trade expectancy: {num(row[5]):+.2f}R\n"
            f"Counterfactual ALLOW total: {num(row[6]):+.2f}R")

# Add challenger command without disturbing existing Telegram command tree.
_V71_PREV_COMMAND = handle_telegram_command
def handle_telegram_command(chat_id, text):
    command=((text or "").strip().split() or [""])[0].lower()
    if command in {"/v7","/v71","/challenger","/selector"}:
        send_to_chat(chat_id,_v71_challenger_text()); return
    return _V71_PREV_COMMAND(chat_id,text)

_V70_PAPER_CREATE = create_paper_trade

# Separate cadence for live-only bridge evaluations. This mirrors the normal
# alert cooldown/score-improvement behavior without mutating paper alert state.
V743_BRIDGE_VERSION = "7.7.1-crypto-metals-optimizer-shadow"
V71_LIVE_BRIDGE_STATE = {}

def _v71_live_bridge_due(result):
    key = f"{result.get('symbol')}_{result.get('direction')}"
    previous = V71_LIVE_BRIDGE_STATE.get(key)
    if previous is None:
        return True
    age = time.time() - num(previous.get("time"))
    if age >= ALERT_COOLDOWN:
        return True
    return num(result.get("best_score")) >= num(previous.get("score")) + ALERT_SCORE_IMPROVEMENT

def _v71_mark_live_bridge(result):
    key = f"{result.get('symbol')}_{result.get('direction')}"
    V71_LIVE_BRIDGE_STATE[key] = {
        "time": time.time(),
        "score": num(result.get("best_score")),
        "price": num(result.get("price")),
    }

def _v71_attach_live_context(result, trades):
    """Attach Risk/Strategy context even when paper alert creation is blocked."""
    if not isinstance(result.get("risk_challenger"), dict):
        risk_shadow = _v681_evaluate_risk_challenger(result, trades)
        risk_row_id = _v681_store_risk_decision(risk_shadow)
        if risk_row_id:
            risk_shadow["row_id"] = risk_row_id
        result["risk_challenger"] = risk_shadow
        print(
            f"V6.8.1 RISK SHADOW: {result.get('symbol')} {result.get('direction')} "
            f"→ {risk_shadow.get('decision')} ({num(risk_shadow.get('size_multiplier')):.1f}x)"
            + (" — " + "; ".join(risk_shadow.get("reasons", [])[:2]) if risk_shadow.get("reasons") else "")
        )
    if not isinstance(result.get("strategy_ensemble"), dict):
        strategy_shadow = _v69_evaluate_strategy_ensemble(result)
        strategy_row_id = _v69_store_ensemble(result, strategy_shadow)
        if strategy_row_id:
            strategy_shadow["row_id"] = strategy_row_id
        result["strategy_ensemble"] = strategy_shadow
        print(
            f"V6.9 STRATEGY SHADOW: {result.get('symbol')} {result.get('direction')} "
            f"→ {strategy_shadow.get('consensus')} "
            f"C/W/R {strategy_shadow.get('confirm_count', 0)}/"
            f"{strategy_shadow.get('wait_count', 0)}/"
            f"{strategy_shadow.get('reject_count', 0)}"
        )
    return result

def _v71_live_stub_trade(result, source_key):
    """Build a deterministic live-only identity when paper creation is suppressed."""
    symbol = str(result.get("symbol") or "UNKNOWN")
    direction = str(result.get("direction") or "UNKNOWN")
    stable_key = str(source_key or _v68_signal_key(result))
    digest = hashlib.sha1(stable_key.encode("utf-8")).hexdigest()[:16]
    return {
        "signal_id": f"live_{digest}_{symbol}_{direction}",
        "source_key": stable_key,
        "symbol": symbol,
        "direction": direction,
        "score": num(result.get("best_score")),
        "signal_time": time.time(),
        "status": "LIVE_CANDIDATE",
    }

def _v71_execute_live_gate(result, trade, gate, bridge_mode=False):
    """Run the live executor once and make ALLOW/SKIP outcomes auditable."""
    if V70_LIVE is None:
        return {"executed": False, "reason": "live module unavailable"}
    try:
        lr = result.get("live_risk_challenger") or {}
        if lr:
            print(
                f"V7.4.4 LIVE RISK: {result.get('symbol')} {result.get('direction')} "
                f"→ {lr.get('decision')} live_open={lr.get('open_count',0)} "
                f"live_losses={lr.get('recent_stop_count',0)} btc_weak={bool(lr.get('btc_weak'))}"
            )
        if gate.get("eligible"):
            outcome = V70_LIVE.execute_signal(result, trade)
            outcome = outcome if isinstance(outcome, dict) else {"executed": bool(outcome)}
            print(
                f"V7.1 LIVE SELECTOR: {result.get('symbol')} {result.get('direction')} "
                f"ALLOW — selector={gate.get('selector_score')} "
                f"bridge={'LIVE_ONLY' if bridge_mode else 'PAPER_CREATED'} "
                f"executed={bool(outcome.get('executed'))} "
                f"reason={outcome.get('reason') or 'submitted/confirmed'}"
            )
            return outcome
        print(
            f"V7.1 LIVE SELECTOR: {result.get('symbol')} {result.get('direction')} SKIP — "
            + "; ".join(gate.get("reasons") or [])
            + (" | bridge=LIVE_ONLY" if bridge_mode else "")
        )
        return {"executed": False, "reason": "; ".join(gate.get("reasons") or ["selector skip"])}
    except Exception as error:
        try:
            V70_LIVE.halt(f"live hook exception: {type(error).__name__}: {error}")
        except Exception:
            pass
        return {"executed": False, "reason": f"live hook exception: {type(error).__name__}: {error}"}

def _v71_evaluate_live_without_paper(result, trades, source_key):
    """Evaluate a durable Core ENTRY for live use even if paper ledger blocks it."""
    _v71_settle_challenger(trades)
    gate = _v71_live_candidate_gate(result, trades)
    result["v70_live_gate"] = gate
    trade = _v71_live_stub_trade(result, source_key)
    _v71_store_challenger(trade, gate)
    print(
        f"V7.4.3 LIVE BRIDGE: {result.get('symbol')} {result.get('direction')} "
        "evaluating despite existing paper trade on symbol"
    )
    return _v71_execute_live_gate(result, trade, gate, bridge_mode=True)

def create_paper_trade(result, trades):
    _v71_settle_challenger(trades)
    gate=_v71_live_candidate_gate(result,trades)
    result["v70_live_gate"]=gate
    trade=_V70_PAPER_CREATE(result,trades)
    _v71_store_challenger(trade,gate)
    _v71_execute_live_gate(result, trade, gate, bridge_mode=False)
    return trade


# ============================================================
# V7.7 CRYPTO + METALS OPTIMIZER LAB (SHADOW ONLY)
# ============================================================
# Purpose:
#   1) Maintain a realistic max-2 portfolio shadow with correlation control.
#   2) Measure gross R versus execution-cost-adjusted R.
#   3) Measure the live-style 25/25/50 managed exit path independently.
#   4) Backfill the existing control-paper ledger into analyzable score/bucket/
#      strategy cohorts without changing the control ledger or live executor.
#   5) Give metals their own cross-metal context instead of BTC regime logic.
#
# This entire block is research-only. It NEVER submits, closes, resizes or
# modifies a real MEXC position and it never changes the original paper trade.

V77_VERSION = "7.7.1-crypto-metals-optimizer-shadow"
V77_DB_READY = False
V77_MAX_OPEN = min(2, max(1, int(os.getenv("V77_MAX_OPEN", "2"))))
V77_MAX_SAME_BUCKET_DIRECTION = min(2, max(1, int(os.getenv("V77_MAX_SAME_BUCKET_DIRECTION", "1"))))
V77_LOSS_CLUSTER_COUNT = int(os.getenv("V77_LOSS_CLUSTER_COUNT", str(V71_LOSS_CLUSTER_COUNT)))
V77_LOSS_CLUSTER_MINUTES = int(os.getenv("V77_LOSS_CLUSTER_MINUTES", str(V71_LOSS_CLUSTER_MINUTES)))
V77_SAME_SYMBOL_COOLDOWN_MINUTES = int(os.getenv("V77_SAME_SYMBOL_COOLDOWN_MINUTES", str(V70_SAME_SYMBOL_COOLDOWN_MINUTES)))
V77_MAX_COST_R = float(os.getenv("V77_MAX_COST_R", str(V71_MAX_COST_FRACTION_R)))
V77_DEGRADED_MIN_CORE = float(os.getenv("V77_DEGRADED_MIN_CORE", str(V72_DEGRADED_MIN_CORE)))
V77_DEGRADED_MIN_SELECTOR = float(os.getenv("V77_DEGRADED_MIN_SELECTOR", str(V72_DEGRADED_MIN_SELECTOR)))
V77_BE_BUFFER_BPS = float(os.getenv("V77_BE_BUFFER_BPS", "10"))
V77_TRACK_LIMIT = max(2, int(os.getenv("V77_TRACK_LIMIT", "50")))
V77_MIN_COHORT_N = max(3, int(os.getenv("V77_MIN_COHORT_N", "5")))


def _v77_score_bucket(score):
    score = num(score)
    if score >= 90:
        return "90+"
    if score >= 85:
        return "85-89"
    if score >= 80:
        return "80-84"
    if score >= 70:
        return "70-79"
    return "<70"


def _v77_cost_r_from_geometry(entry, stop):
    entry = abs(num(entry)); stop = num(stop)
    if entry <= 0:
        return 0.0
    stop_fraction = abs(entry - stop) / entry
    if stop_fraction <= 0:
        return 0.0
    fee_bps = num(globals().get("V71_EST_TAKER_FEE_BPS", 2.0))
    slip_bps = num(globals().get("V71_EST_SLIPPAGE_BPS", 2.0))
    round_trip_fraction = (2.0 * fee_bps + 2.0 * slip_bps) / 10000.0
    return round(round_trip_fraction / stop_fraction, 4)


def _v77_be_buffer_r(entry, risk):
    entry = abs(num(entry)); risk = abs(num(risk))
    if entry <= 0 or risk <= 0:
        return 0.0
    buffer_price = entry * V77_BE_BUFFER_BPS / 10000.0
    return max(0.0, buffer_price / risk)


def _v77_managed_r_from_control_trade(trade):
    """Approximate live 25/25/50 management from an already-settled control row.

    Exact V7.7 candidates are tracked candle-by-candle below. This helper exists
    only to backfill the 130+ historical control trades using milestones already
    recorded in the durable paper ledger.
    """
    if trade.get("final_r") is None:
        return None
    status = str(trade.get("status") or "").upper()
    gross = num(trade.get("final_r"))
    tp1 = bool(trade.get("tp1_hit")); tp2 = bool(trade.get("tp2_hit")); tp3 = bool(trade.get("tp3_hit"))
    if tp3 or status == "TP3":
        return 2.375  # .25*1.5 + .25*2 + .50*3
    if status == "STOP":
        if tp2:
            return 1.375  # .25*1.5 + .25*2 + .50*1
        if tp1:
            be_r = _v77_be_buffer_r(trade.get("entry"), trade.get("risk"))
            return round(0.375 + 0.75 * be_r, 4)
        return -1.0
    if status == "EXPIRED":
        if tp2:
            return round(0.875 + 0.50 * gross, 4)
        if tp1:
            return round(0.375 + 0.75 * gross, 4)
        return round(gross, 4)
    return round(gross, 4)


def _v77_control_row(trade):
    if not trade or trade.get("status") == "OPEN" or trade.get("final_r") is None:
        return None
    risk_meta = trade.get("risk_challenger") or {}
    strategy = trade.get("strategy_ensemble") or {}
    gross = num(trade.get("final_r"))
    cost_r = _v77_cost_r_from_geometry(trade.get("entry"), trade.get("stop"))
    managed = _v77_managed_r_from_control_trade(trade)
    supervisor = trade.get("supervisor") or {}
    pp_triggered = supervisor.get("profit_protect_exit_r") is not None
    pp_r = num(supervisor.get("profit_protect_exit_r")) if pp_triggered else gross
    bucket = str(risk_meta.get("asset_bucket") or _v681_asset_bucket(trade.get("symbol")))
    strategy_consensus = str(strategy.get("consensus") or "NO_DATA").upper()
    risk_decision = str(risk_meta.get("decision") or "NO_DATA").upper()
    regime = str(risk_meta.get("signal_regime") or trade.get("entry_regime") or "UNKNOWN")
    return (
        str(trade.get("signal_id")), str(trade.get("source_key") or ""), str(trade.get("symbol") or ""),
        str(trade.get("direction") or ""), bucket, num(trade.get("score")), _v77_score_bucket(trade.get("score")),
        strategy_consensus, risk_decision, regime, str(trade.get("status") or ""), gross, cost_r,
        round(gross - cost_r, 4), managed, None if managed is None else round(managed - cost_r, 4),
        bool(pp_triggered), pp_r, num(trade.get("signal_time")), num(trade.get("closed_time")),
    )


def _v77_init_optimizer_lab():
    global V77_DB_READY
    if not V68_DB_READY:
        V77_DB_READY = False
        return False
    statements = [
        """
        CREATE TABLE IF NOT EXISTS fh_v77_control_history (
            signal_id TEXT PRIMARY KEY,
            source_key TEXT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            asset_bucket TEXT NOT NULL,
            core_score DOUBLE PRECISION,
            score_bucket TEXT,
            strategy_consensus TEXT,
            risk_decision TEXT,
            entry_regime TEXT,
            actual_status TEXT,
            gross_r DOUBLE PRECISION,
            estimated_cost_r DOUBLE PRECISION,
            estimated_net_r DOUBLE PRECISION,
            managed_estimate_r DOUBLE PRECISION,
            managed_net_estimate_r DOUBLE PRECISION,
            profit_protect_triggered BOOLEAN NOT NULL DEFAULT FALSE,
            profit_protect_strategy_r DOUBLE PRECISION,
            signal_ts DOUBLE PRECISION,
            closed_ts DOUBLE PRECISION,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_v77_control_bucket ON fh_v77_control_history(asset_bucket, direction, score_bucket)",
        """
        CREATE TABLE IF NOT EXISTS fh_v77_optimizer_candidates (
            source_key TEXT PRIMARY KEY,
            version TEXT NOT NULL,
            signal_ts DOUBLE PRECISION NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            asset_bucket TEXT NOT NULL,
            core_score DOUBLE PRECISION,
            score_bucket TEXT,
            strategy_consensus TEXT,
            risk_decision TEXT,
            selector_score DOUBLE PRECISION,
            estimated_cost_r DOUBLE PRECISION,
            decision TEXT NOT NULL,
            reasons JSONB NOT NULL,
            notes JSONB NOT NULL,
            entry DOUBLE PRECISION,
            stop DOUBLE PRECISION,
            tp1 DOUBLE PRECISION,
            tp2 DOUBLE PRECISION,
            tp3 DOUBLE PRECISION,
            risk DOUBLE PRECISION,
            tracking_start DOUBLE PRECISION,
            last_checked DOUBLE PRECISION,
            control_status TEXT NOT NULL DEFAULT 'NOT_TRACKED',
            managed_status TEXT NOT NULL DEFAULT 'NOT_TRACKED',
            control_tp1_hit BOOLEAN NOT NULL DEFAULT FALSE,
            control_tp2_hit BOOLEAN NOT NULL DEFAULT FALSE,
            control_tp3_hit BOOLEAN NOT NULL DEFAULT FALSE,
            managed_tp1_hit BOOLEAN NOT NULL DEFAULT FALSE,
            managed_tp2_hit BOOLEAN NOT NULL DEFAULT FALSE,
            managed_tp3_hit BOOLEAN NOT NULL DEFAULT FALSE,
            control_final_r DOUBLE PRECISION,
            managed_final_r DOUBLE PRECISION,
            managed_net_r DOUBLE PRECISION,
            reference_control_r DOUBLE PRECISION,
            control_closed_ts DOUBLE PRECISION,
            managed_closed_ts DOUBLE PRECISION,
            closed_ts DOUBLE PRECISION,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_v77_candidate_open ON fh_v77_optimizer_candidates(decision, managed_status, signal_ts)",
        "CREATE INDEX IF NOT EXISTS idx_v77_candidate_bucket ON fh_v77_optimizer_candidates(asset_bucket, direction, decision)",
        "ALTER TABLE fh_v77_optimizer_candidates ADD COLUMN IF NOT EXISTS control_closed_ts DOUBLE PRECISION",
        "ALTER TABLE fh_v77_optimizer_candidates ADD COLUMN IF NOT EXISTS managed_closed_ts DOUBLE PRECISION",
    ]
    for statement in statements:
        if _v68_db_execute(statement) is None:
            V77_DB_READY = False
            return False
    V77_DB_READY = True
    print("V7.7 optimizer lab: POSTGRES READY — portfolio/cost/managed-exit/metals shadows active")
    return True


def _v77_sync_control_history(trades):
    if not V77_DB_READY:
        return 0
    rows = [row for row in (_v77_control_row(t) for t in (trades or [])) if row]
    if not rows:
        return 0
    ok = _v68_db_executemany(
        """
        INSERT INTO fh_v77_control_history
        (signal_id,source_key,symbol,direction,asset_bucket,core_score,score_bucket,strategy_consensus,risk_decision,
         entry_regime,actual_status,gross_r,estimated_cost_r,estimated_net_r,managed_estimate_r,managed_net_estimate_r,
         profit_protect_triggered,profit_protect_strategy_r,signal_ts,closed_ts)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(signal_id) DO UPDATE SET
          source_key=EXCLUDED.source_key,symbol=EXCLUDED.symbol,direction=EXCLUDED.direction,asset_bucket=EXCLUDED.asset_bucket,
          core_score=EXCLUDED.core_score,score_bucket=EXCLUDED.score_bucket,strategy_consensus=EXCLUDED.strategy_consensus,
          risk_decision=EXCLUDED.risk_decision,entry_regime=EXCLUDED.entry_regime,actual_status=EXCLUDED.actual_status,
          gross_r=EXCLUDED.gross_r,estimated_cost_r=EXCLUDED.estimated_cost_r,estimated_net_r=EXCLUDED.estimated_net_r,
          managed_estimate_r=EXCLUDED.managed_estimate_r,managed_net_estimate_r=EXCLUDED.managed_net_estimate_r,
          profit_protect_triggered=EXCLUDED.profit_protect_triggered,profit_protect_strategy_r=EXCLUDED.profit_protect_strategy_r,
          signal_ts=EXCLUDED.signal_ts,closed_ts=EXCLUDED.closed_ts,updated_at=NOW()
        """, rows)
    if ok:
        # Link exact V7.7 candidates back to the control book where both share a durable source key.
        _v68_db_execute("""
          UPDATE fh_v77_optimizer_candidates c
          SET reference_control_r=h.gross_r, updated_at=NOW()
          FROM fh_v77_control_history h
          WHERE c.source_key=h.source_key AND c.reference_control_r IS DISTINCT FROM h.gross_r
        """)
        return len(rows)
    return 0


def _v77_metals_context(result):
    """Cross-metal confirmation for XAU/XAUT/SILVER; deliberately no BTC proxy."""
    symbol = str(result.get("symbol") or "")
    direction = str(result.get("direction") or "").upper()
    aligned = []; opposed = []
    for row in V681_LAST_SCAN_RESULTS or []:
        other = str(row.get("symbol") or "")
        if other == symbol or _v681_asset_bucket(other) != "METAL":
            continue
        state = str(row.get("signal_state") or "").upper()
        score = num(row.get("best_score"))
        if state not in {"ENTRY", "ARMED", "WATCH"} or score < 68:
            continue
        odir = str(row.get("direction") or "").upper()
        item = {"symbol": other, "direction": odir, "state": state, "score": round(score, 1)}
        if odir == direction:
            aligned.append(item)
        elif odir in {"LONG", "SHORT"}:
            opposed.append(item)
    adjustment = min(8.0, 4.0 * len(aligned)) - min(12.0, 6.0 * len(opposed))
    return {"aligned": aligned, "opposed": opposed, "score_adjustment": adjustment}


def _v77_open_portfolio_rows():
    if not V77_DB_READY:
        return []
    rows = _v68_db_execute("""
      SELECT source_key,symbol,direction,asset_bucket,signal_ts
      FROM fh_v77_optimizer_candidates
      WHERE decision='ALLOW' AND managed_status='OPEN'
      ORDER BY signal_ts
    """, fetch="all") or []
    return [
        {"source_key":r[0],"symbol":r[1],"direction":r[2],"asset_bucket":r[3],"signal_ts":num(r[4])}
        for r in rows
    ]


def _v77_recent_shadow_losses(symbol, direction, bucket, minutes, same_symbol=False):
    if not V77_DB_READY:
        return []
    cutoff = time.time() - max(1, int(minutes)) * 60
    rows = _v68_db_execute("""
      SELECT symbol,direction,asset_bucket,managed_final_r,managed_closed_ts
      FROM fh_v77_optimizer_candidates
      WHERE decision='ALLOW' AND managed_status NOT IN ('OPEN','NOT_TRACKED','AMBIGUOUS')
        AND managed_final_r IS NOT NULL AND managed_final_r <= -0.75 AND managed_closed_ts >= %s
      ORDER BY managed_closed_ts DESC
    """, (cutoff,), fetch="all") or []
    hits=[]
    for r in rows:
        rsym, rdir, rbucket, rr, rclosed = r
        if str(rdir).upper() != str(direction).upper():
            continue
        if same_symbol:
            if str(rsym) != str(symbol):
                continue
        elif str(rbucket) != str(bucket):
            continue
        hits.append({"symbol":rsym,"direction":rdir,"asset_bucket":rbucket,"final_r":num(rr),"closed_time":num(rclosed)})
    return hits


def _v77_shadow_gate(result):
    symbol = str(result.get("symbol") or "")
    direction = str(result.get("direction") or "").upper()
    bucket = _v681_asset_bucket(symbol)
    if bucket == "EQUITY":
        return {"eligible":False,"decision":"SKIP","reasons":["equity handled by V7.6 ensemble"],"notes":[],"selector_score":0.0,"asset_bucket":bucket}

    strategy = result.get("strategy_ensemble") or {}
    sc = str(strategy.get("consensus") or "NO_DATA").upper()
    macro = get_macro_snapshot() or {}
    combined = num(macro.get("combined_score"))
    event_risk = str(macro.get("event_risk") or "LOW").upper()
    btc = _v681_btc_scan_snapshot()
    macro_conflict, strong_macro_conflict = _v681_directional_macro_conflict(direction, bucket, combined)
    btc_weak = _v681_btc_is_weak_for(direction, bucket, btc) if bucket == "CRYPTO" else False
    open_rows = _v77_open_portfolio_rows()
    same_bucket_dir = [r for r in open_rows if r.get("asset_bucket")==bucket and str(r.get("direction")).upper()==direction]
    same_symbol_open = [r for r in open_rows if r.get("symbol")==symbol]
    same_symbol_losses = _v77_recent_shadow_losses(symbol,direction,bucket,V77_SAME_SYMBOL_COOLDOWN_MINUTES,True)
    cluster = _v77_recent_shadow_losses(symbol,direction,bucket,V77_LOSS_CLUSTER_MINUTES,False)
    cost_r = _v71_estimated_cost_r(result)

    risk_decision = "ALLOW"
    reasons=[]; notes=[]
    if event_risk == "EXTREME":
        risk_decision="BLOCK"; reasons.append("EXTREME scheduled-event danger window")
    elif event_risk == "HIGH":
        risk_decision="CAUTION"; notes.append("HIGH scheduled-event risk")
    if bucket == "CRYPTO" and btc_weak:
        notes.append(f"BTC not ENTRY-aligned ({btc.get('direction') or 'N/A'} {btc.get('state') or 'N/A'} {num(btc.get('score')):.1f})")
    if macro_conflict:
        notes.append(f"{bucket} {direction} conflicts with macro/news score {combined:+.1f}")
        if strong_macro_conflict and risk_decision == "ALLOW":
            risk_decision="CAUTION"
    if bucket == "CRYPTO" and btc_weak and macro_conflict:
        risk_decision="BLOCK"; reasons.append("crypto regime conflict: weak BTC + macro conflict")
    if len(open_rows) >= V77_MAX_OPEN:
        reasons.append(f"V7.7 portfolio full ({len(open_rows)}/{V77_MAX_OPEN})")
    if same_symbol_open:
        reasons.append("V7.7 same-symbol position already open")
    if len(same_bucket_dir) >= V77_MAX_SAME_BUCKET_DIRECTION:
        reasons.append(f"correlation guard: {len(same_bucket_dir)} {bucket} {direction} already open")
    if same_symbol_losses:
        age=(time.time()-num(same_symbol_losses[0].get("closed_time")))/60.0
        reasons.append(f"same-symbol V7.7 loss cooldown ({age:.0f}m ago)")
    if len(cluster) >= V77_LOSS_CLUSTER_COUNT:
        risk_decision="BLOCK"; reasons.append(f"V7.7 loss-cluster breaker: {len(cluster)} {bucket} {direction} losses/{V77_LOSS_CLUSTER_MINUTES}m")
    if cost_r > V77_MAX_COST_R:
        reasons.append(f"estimated execution drag {cost_r:.2f}R > {V77_MAX_COST_R:.2f}R")

    metal_context = None
    metal_adjustment = 0.0
    if bucket == "METAL":
        metal_context = _v77_metals_context(result)
        metal_adjustment = num(metal_context.get("score_adjustment"))
        if metal_context.get("aligned"):
            notes.append("cross-metal alignment: " + ",".join(x["symbol"] for x in metal_context["aligned"]))
        if metal_context.get("opposed"):
            notes.append("cross-metal opposition: " + ",".join(x["symbol"] for x in metal_context["opposed"]))

    score = num(result.get("best_score"))
    score += {"ALLOW":8,"CAUTION":3,"REDUCE":-4,"BLOCK":-25}.get(risk_decision,0)
    score += {"STRONG_CONFIRM":8,"CONFIRM":5,"MIXED":0,"WAIT":-4,"AVOID":-10}.get(sc,0)
    if btc_weak: score -= 8
    if macro_conflict: score -= 6
    score -= min(12.0, len(cluster)*4.0)
    score -= min(12.0, cost_r*10.0)
    score += metal_adjustment
    selector_score=round(max(0.0,min(100.0,score)),2)

    degraded = bool(
        len(cluster)>0 or btc_weak or macro_conflict or event_risk in {"HIGH","EXTREME"}
        or risk_decision in {"CAUTION","REDUCE","BLOCK"} or sc in {"MIXED","WAIT","AVOID","NO_DATA"}
        or (bucket=="METAL" and metal_context and len(metal_context.get("opposed") or [])>0)
    )
    if degraded:
        if num(result.get("best_score")) < V77_DEGRADED_MIN_CORE:
            reasons.append(f"degraded-regime Core threshold: {num(result.get('best_score')):.1f} < {V77_DEGRADED_MIN_CORE:.1f}")
        if sc not in {"CONFIRM","STRONG_CONFIRM"}:
            reasons.append(f"degraded-regime Strategy confirmation required ({sc})")
        if selector_score < V77_DEGRADED_MIN_SELECTOR:
            reasons.append(f"degraded-regime selector threshold: {selector_score:.1f} < {V77_DEGRADED_MIN_SELECTOR:.1f}")
    if bucket == "METAL" and metal_context and len(metal_context.get("opposed") or []) >= 2 and selector_score < 80:
        reasons.append("metals cross-market conflict")

    return {
        "eligible": not reasons,
        "decision": "ALLOW" if not reasons else "SKIP",
        "reasons": reasons,
        "notes": notes,
        "asset_bucket": bucket,
        "risk_decision": risk_decision,
        "strategy_consensus": sc,
        "selector_score": selector_score,
        "estimated_cost_r": round(cost_r,4),
        "btc_weak": bool(btc_weak),
        "macro_conflict": bool(macro_conflict),
        "event_risk": event_risk,
        "portfolio_open": len(open_rows),
        "same_bucket_direction_open": len(same_bucket_dir),
        "recent_loss_count": len(cluster),
        "metal_context": metal_context,
        "version": V77_VERSION,
    }


def _v77_process_candidate(result, source_key=None):
    if not V77_DB_READY or not isinstance(result, dict):
        return None
    symbol=str(result.get("symbol") or "")
    if _v681_asset_bucket(symbol) == "EQUITY":
        return None
    source_key=str(source_key or _v68_signal_key(result))
    # Lock the first evaluation for a durable signal. Repeated bridge scans cannot
    # rewrite history after portfolio state or price has changed.
    existing=_v68_db_execute("SELECT decision FROM fh_v77_optimizer_candidates WHERE source_key=%s",(source_key,),fetch="one")
    if existing:
        return {"decision":existing[0],"existing":True}
    gate=_v77_shadow_gate(result)
    plan=result.get("risk_plan") or {}
    entry=num(result.get("price")); stop=num(plan.get("stop")); risk=abs(num(plan.get("risk")) or (entry-stop))
    tracking_start=(int(time.time()//60)+1)*60
    tracked = bool(gate.get("eligible") and entry>0 and risk>0)
    control_status="OPEN" if tracked else "NOT_TRACKED"
    managed_status="OPEN" if tracked else "NOT_TRACKED"
    payload={"gate":gate,"core_score":num(result.get("best_score")),"regime":result.get("regime"),"risk_plan":plan}
    row=_v68_db_execute("""
      INSERT INTO fh_v77_optimizer_candidates
      (source_key,version,signal_ts,symbol,direction,asset_bucket,core_score,score_bucket,strategy_consensus,risk_decision,
       selector_score,estimated_cost_r,decision,reasons,notes,entry,stop,tp1,tp2,tp3,risk,tracking_start,last_checked,
       control_status,managed_status,payload)
      VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
      ON CONFLICT(source_key) DO NOTHING RETURNING source_key
    """,(
      source_key,V77_VERSION,time.time(),symbol,str(result.get("direction") or ""),gate.get("asset_bucket"),num(result.get("best_score")),
      _v77_score_bucket(result.get("best_score")),gate.get("strategy_consensus"),gate.get("risk_decision"),gate.get("selector_score"),gate.get("estimated_cost_r"),
      gate.get("decision"),json.dumps(gate.get("reasons") or []),json.dumps(gate.get("notes") or []),entry,stop,num(plan.get("tp1")),num(plan.get("tp2")),num(plan.get("tp3")),risk,
      tracking_start,tracking_start-1,control_status,managed_status,json.dumps(_clean_json_value(payload))
    ),fetch="one")
    if row:
        print(
            f"V7.7 OPTIMIZER: {symbol} {result.get('direction')} → {gate.get('decision')} "
            f"selector={gate.get('selector_score'):.1f} cost={gate.get('estimated_cost_r'):.2f}R "
            f"bucket={gate.get('asset_bucket')} portfolio={gate.get('portfolio_open')}/{V77_MAX_OPEN}"
            + (" — " + "; ".join(gate.get("reasons")[:2]) if gate.get("reasons") else "")
        )
    return gate


def _v77_open_tracked_rows():
    if not V77_DB_READY:
        return []
    rows=_v68_db_execute("""
      SELECT source_key,symbol,direction,entry,stop,tp1,tp2,tp3,risk,tracking_start,last_checked,
             control_status,managed_status,control_tp1_hit,control_tp2_hit,control_tp3_hit,
             managed_tp1_hit,managed_tp2_hit,managed_tp3_hit,estimated_cost_r,signal_ts
      FROM fh_v77_optimizer_candidates
      WHERE decision='ALLOW' AND (control_status='OPEN' OR managed_status='OPEN')
      ORDER BY signal_ts LIMIT %s
    """,(V77_TRACK_LIMIT,),fetch="all") or []
    keys=["source_key","symbol","direction","entry","stop","tp1","tp2","tp3","risk","tracking_start","last_checked",
          "control_status","managed_status","control_tp1_hit","control_tp2_hit","control_tp3_hit",
          "managed_tp1_hit","managed_tp2_hit","managed_tp3_hit","estimated_cost_r","signal_ts"]
    return [dict(zip(keys,r)) for r in rows]


def _v77_managed_stop_r(row):
    if row.get("managed_tp2_hit"):
        return 1.0
    if row.get("managed_tp1_hit"):
        return _v77_be_buffer_r(row.get("entry"), row.get("risk"))
    return -1.0


def _v77_managed_realized_on_stop(row):
    if row.get("managed_tp2_hit"):
        return 1.375
    if row.get("managed_tp1_hit"):
        return round(0.375 + 0.75 * _v77_be_buffer_r(row.get("entry"), row.get("risk")),4)
    return -1.0


def _v77_managed_realized_at_r(row, current_r):
    if row.get("managed_tp2_hit"):
        return round(0.875 + 0.50 * current_r,4)
    if row.get("managed_tp1_hit"):
        return round(0.375 + 0.75 * current_r,4)
    return round(current_r,4)


def _v77_touch(direction, level, high, low, is_target=True):
    direction=str(direction or "").upper()
    if direction=="LONG":
        return high >= level if is_target else low <= level
    return low <= level if is_target else high >= level


def _v77_settle_row(row):
    df=get_candles(row["symbol"],"Min1")
    if df is None or len(df)==0:
        return False
    df=df.copy(); df["time_norm"]=df["time"].apply(normalize_candle_time)
    relevant=df[(df["time_norm"]>num(row.get("last_checked"))) & (df["time_norm"]>=num(row.get("tracking_start")))]
    latest_price=num(df.iloc[-1]["close"]); latest_seen=num(row.get("last_checked"))
    direction=str(row.get("direction") or "").upper(); entry=num(row.get("entry")); risk=max(abs(num(row.get("risk"))),1e-12)
    changed=False; closed_ts=None; control_closed_ts=None; managed_closed_ts=None
    for _,c in relevant.iterrows():
        t=num(c["time_norm"]); high=num(c["high"]); low=num(c["low"]); latest_seen=max(latest_seen,t)
        # Exact managed path first: dynamic stop after each prior confirmed milestone.
        if row.get("managed_status")=="OPEN":
            stop_r=_v77_managed_stop_r(row)
            managed_stop=entry + stop_r*risk if direction=="LONG" else entry - stop_r*risk
            stop_touch=_v77_touch(direction,managed_stop,high,low,is_target=False)
            next_level = num(row.get("tp3")) if row.get("managed_tp2_hit") else (num(row.get("tp2")) if row.get("managed_tp1_hit") else num(row.get("tp1")))
            target_touch=_v77_touch(direction,next_level,high,low,is_target=True)
            if stop_touch and target_touch:
                row["managed_status"]="AMBIGUOUS"; row["managed_final_r"]=None; managed_closed_ts=t; closed_ts=t; changed=True
            elif stop_touch:
                row["managed_status"]="STOP"; row["managed_final_r"]=_v77_managed_realized_on_stop(row); managed_closed_ts=t; closed_ts=t; changed=True
            else:
                tp3=_v77_touch(direction,num(row.get("tp3")),high,low,True)
                tp2=_v77_touch(direction,num(row.get("tp2")),high,low,True)
                tp1=_v77_touch(direction,num(row.get("tp1")),high,low,True)
                if tp3:
                    row["managed_tp1_hit"]=row["managed_tp2_hit"]=row["managed_tp3_hit"]=True
                    row["managed_status"]="TP3"; row["managed_final_r"]=2.375; managed_closed_ts=t; closed_ts=t; changed=True
                elif tp2 and not row.get("managed_tp2_hit"):
                    row["managed_tp1_hit"]=row["managed_tp2_hit"]=True; changed=True
                elif tp1 and not row.get("managed_tp1_hit"):
                    row["managed_tp1_hit"]=True; changed=True
        # Gross/control path stays independent and uses original fixed stop/full TP3.
        if row.get("control_status")=="OPEN":
            stop_touch=_v77_touch(direction,num(row.get("stop")),high,low,False)
            tp3=_v77_touch(direction,num(row.get("tp3")),high,low,True)
            if stop_touch and tp3:
                row["control_status"]="AMBIGUOUS"; row["control_final_r"]=None; control_closed_ts=t; closed_ts=closed_ts or t; changed=True
            elif stop_touch:
                row["control_status"]="STOP"; row["control_final_r"]=-1.0; control_closed_ts=t; closed_ts=closed_ts or t; changed=True
            elif tp3:
                row["control_tp1_hit"]=row["control_tp2_hit"]=row["control_tp3_hit"]=True
                row["control_status"]="TP3"; row["control_final_r"]=3.0; control_closed_ts=t; closed_ts=closed_ts or t; changed=True
            else:
                tp2=_v77_touch(direction,num(row.get("tp2")),high,low,True)
                tp1=_v77_touch(direction,num(row.get("tp1")),high,low,True)
                if tp2 and not row.get("control_tp2_hit"):
                    row["control_tp1_hit"]=row["control_tp2_hit"]=True; changed=True
                elif tp1 and not row.get("control_tp1_hit"):
                    row["control_tp1_hit"]=True; changed=True
    age=time.time()-num(row.get("signal_ts"))
    if age >= TRADE_EXPIRY_HOURS*3600:
        current_r=(latest_price-entry)/risk if direction=="LONG" else (entry-latest_price)/risk
        current_r=max(-1.0,min(3.0,current_r))
        if row.get("control_status")=="OPEN":
            row["control_status"]="EXPIRED"; row["control_final_r"]=round(current_r,4); control_closed_ts=time.time(); changed=True
        if row.get("managed_status")=="OPEN":
            row["managed_status"]="EXPIRED"; row["managed_final_r"]=_v77_managed_realized_at_r(row,current_r); managed_closed_ts=time.time(); changed=True
        closed_ts=closed_ts or time.time()
    row["last_checked"]=latest_seen
    managed_net=None if row.get("managed_final_r") is None else round(num(row.get("managed_final_r"))-num(row.get("estimated_cost_r")),4)
    terminal = row.get("control_status")!="OPEN" and row.get("managed_status")!="OPEN"
    _v68_db_execute("""
      UPDATE fh_v77_optimizer_candidates SET
        last_checked=%s,control_status=%s,managed_status=%s,
        control_tp1_hit=%s,control_tp2_hit=%s,control_tp3_hit=%s,
        managed_tp1_hit=%s,managed_tp2_hit=%s,managed_tp3_hit=%s,
        control_final_r=%s,managed_final_r=%s,managed_net_r=%s,
        control_closed_ts=COALESCE(control_closed_ts,%s),managed_closed_ts=COALESCE(managed_closed_ts,%s),
        closed_ts=CASE WHEN %s THEN COALESCE(closed_ts,%s) ELSE closed_ts END,updated_at=NOW()
      WHERE source_key=%s
    """,(
      row.get("last_checked"),row.get("control_status"),row.get("managed_status"),
      bool(row.get("control_tp1_hit")),bool(row.get("control_tp2_hit")),bool(row.get("control_tp3_hit")),
      bool(row.get("managed_tp1_hit")),bool(row.get("managed_tp2_hit")),bool(row.get("managed_tp3_hit")),
      row.get("control_final_r"),row.get("managed_final_r"),managed_net,control_closed_ts,managed_closed_ts,bool(terminal),closed_ts,row.get("source_key")
    ))
    return changed


def _v77_sync_optimizer_shadow(trades=None):
    if not V77_DB_READY:
        return 0
    if trades is not None:
        _v77_sync_control_history(trades)
    changed=0
    for row in _v77_open_tracked_rows():
        try:
            if _v77_settle_row(row):
                changed+=1
        except Exception as e:
            print(f"V7.7 optimizer settlement warning {row.get('symbol')}: {type(e).__name__}: {e}")
        time.sleep(0.02)
    return changed


def _v77_optimizer_summary_text():
    if not V77_DB_READY:
        return "V7.7 Optimizer Lab: database unavailable"
    control=_v68_db_execute("""
      SELECT COUNT(*),COALESCE(SUM(gross_r),0),COALESCE(AVG(gross_r),0),
             COALESCE(100.0*AVG(CASE WHEN gross_r>0 THEN 1.0 ELSE 0.0 END),0),
             COALESCE(SUM(estimated_net_r),0),COALESCE(AVG(estimated_net_r),0),
             COALESCE(SUM(managed_estimate_r),0),COALESCE(SUM(managed_net_estimate_r),0),
             COUNT(*) FILTER(WHERE profit_protect_triggered),COALESCE(SUM(profit_protect_strategy_r),0)
      FROM fh_v77_control_history
    """,fetch="one") or (0,0,0,0,0,0,0,0,0,0)
    portfolio=_v68_db_execute("""
      SELECT COUNT(*),COUNT(*) FILTER(WHERE decision='ALLOW'),COUNT(*) FILTER(WHERE decision='SKIP'),
             COUNT(*) FILTER(WHERE decision='ALLOW' AND managed_status NOT IN('OPEN','NOT_TRACKED')),
             COALESCE(SUM(control_final_r) FILTER(WHERE decision='ALLOW' AND control_final_r IS NOT NULL),0),
             COALESCE(SUM(managed_final_r) FILTER(WHERE decision='ALLOW' AND managed_final_r IS NOT NULL),0),
             COALESCE(SUM(managed_net_r) FILTER(WHERE decision='ALLOW' AND managed_net_r IS NOT NULL),0),
             COUNT(*) FILTER(WHERE decision='ALLOW' AND managed_status='OPEN')
      FROM fh_v77_optimizer_candidates
    """,fetch="one") or (0,0,0,0,0,0,0,0)
    selector_compare=_v68_db_execute("""
      SELECT decision,COUNT(*) FILTER(WHERE reference_control_r IS NOT NULL),
             COALESCE(AVG(reference_control_r) FILTER(WHERE reference_control_r IS NOT NULL),0),
             COALESCE(SUM(reference_control_r) FILTER(WHERE reference_control_r IS NOT NULL),0),
             COALESCE(100.0*AVG(CASE WHEN reference_control_r>0 THEN 1.0 ELSE 0.0 END)
                      FILTER(WHERE reference_control_r IS NOT NULL),0)
      FROM fh_v77_optimizer_candidates
      GROUP BY decision ORDER BY decision
    """,fetch="all") or []
    score_rows=_v68_db_execute("""
      SELECT score_bucket,COUNT(*),COALESCE(AVG(gross_r),0),COALESCE(SUM(gross_r),0),
             COALESCE(100.0*AVG(CASE WHEN gross_r>0 THEN 1.0 ELSE 0.0 END),0)
      FROM fh_v77_control_history GROUP BY score_bucket ORDER BY
        CASE score_bucket WHEN '<70' THEN 0 WHEN '70-79' THEN 1 WHEN '80-84' THEN 2 WHEN '85-89' THEN 3 ELSE 4 END
    """,fetch="all") or []
    bucket_rows=_v68_db_execute("""
      SELECT asset_bucket,COUNT(*),COALESCE(AVG(gross_r),0),COALESCE(SUM(gross_r),0),
             COALESCE(AVG(estimated_net_r),0)
      FROM fh_v77_control_history GROUP BY asset_bucket ORDER BY COUNT(*) DESC
    """,fetch="all") or []
    cohort_rows=_v68_db_execute("""
      SELECT asset_bucket,direction,score_bucket,strategy_consensus,COUNT(*),
             COALESCE(AVG(gross_r),0),COALESCE(AVG(estimated_net_r),0),
             COALESCE(100.0*AVG(CASE WHEN gross_r>0 THEN 1.0 ELSE 0.0 END),0)
      FROM fh_v77_control_history
      GROUP BY asset_bucket,direction,score_bucket,strategy_consensus
      HAVING COUNT(*) >= %s
      ORDER BY AVG(estimated_net_r) DESC
    """,(V77_MIN_COHORT_N,),fetch="all") or []
    pp_edge=num(control[9])-num(control[1])
    lines=[
      "🧪 V7.7 CRYPTO + METALS OPTIMIZER — SHADOW",
      f"Control settled: {int(control[0])} | win {num(control[3]):.1f}% | gross {num(control[1]):+.2f}R ({num(control[2]):+.2f}R/trade)",
      f"Cost-adjusted estimate: {num(control[4]):+.2f}R ({num(control[5]):+.2f}R/trade)",
      f"25/25/50 managed estimate: gross {num(control[6]):+.2f}R | after estimated costs {num(control[7]):+.2f}R",
      f"Profit-Protect strategy: {int(control[8])} triggers | full-book counterfactual {num(control[9]):+.2f}R | edge {pp_edge:+.2f}R",
      "",
      f"Realistic portfolio: {int(portfolio[0])} candidates | ALLOW {int(portfolio[1])} | SKIP {int(portfolio[2])} | open {int(portfolio[7])}/{V77_MAX_OPEN}",
      f"Settled ALLOW: {int(portfolio[3])} | fixed-exit {num(portfolio[4]):+.2f}R | managed {num(portfolio[5]):+.2f}R | managed net {num(portfolio[6]):+.2f}R",
      f"Correlation guard: max {V77_MAX_SAME_BUCKET_DIRECTION} same-direction position per asset bucket | cost gate {V77_MAX_COST_R:.2f}R",
      "",
      "Selector audit (linked control outcomes):",
    ]
    if selector_compare:
        for decision,n,avg_r,total_r,wr in selector_compare:
            lines.append(f"{decision}: n={int(n)} | avg {num(avg_r):+.2f}R | total {num(total_r):+.2f}R | win {num(wr):.1f}%")
    else:
        lines.append("waiting for linked outcomes")
    lines.extend(["", "Score cohorts (n | avgR | totalR | win):"])
    for b,n,avg_r,total_r,wr in score_rows:
        lines.append(f"{b}: {int(n)} | {num(avg_r):+.2f} | {num(total_r):+.2f} | {num(wr):.1f}%")
    lines.append("")
    lines.append("Asset cohorts (n | avg grossR | totalR | avg netR):")
    for b,n,avg_r,total_r,net_avg in bucket_rows:
        lines.append(f"{b}: {int(n)} | {num(avg_r):+.2f} | {num(total_r):+.2f} | {num(net_avg):+.2f}")
    lines.append("")
    lines.append(f"Adaptive cohort learner (minimum n={V77_MIN_COHORT_N}; ranked by avg net R):")
    if cohort_rows:
        best=cohort_rows[:3]
        worst=list(reversed(cohort_rows[-3:]))
        for label,rows in (("BEST",best),("WEAK",worst)):
            for b,d,sb,sc,n,avg_r,net_r,wr in rows:
                lines.append(
                    f"{label} {b} {d} score {sb} / {sc}: n={int(n)} | "
                    f"gross {num(avg_r):+.2f}R | net {num(net_r):+.2f}R | win {num(wr):.1f}%"
                )
    else:
        lines.append("waiting for enough settled trades per cohort")
    lines.append("Research-only. Learner never auto-promotes filters; control paper and live execution are untouched.")
    return "\n".join(lines)


# Wrap the final live/paper bridge only after all previous wrappers are in place.
_V77_PREV_CREATE_PAPER_TRADE = create_paper_trade

def create_paper_trade(result, trades):
    trade = _V77_PREV_CREATE_PAPER_TRADE(result, trades)
    try:
        _v77_process_candidate(result, trade.get("source_key") if trade else None)
    except Exception as e:
        print(f"V7.7 optimizer candidate warning: {type(e).__name__}: {e}")
    return trade


_V77_PREV_LIVE_NO_PAPER = _v71_evaluate_live_without_paper

def _v71_evaluate_live_without_paper(result, trades, source_key):
    outcome = _V77_PREV_LIVE_NO_PAPER(result, trades, source_key)
    try:
        _v77_process_candidate(result, source_key)
    except Exception as e:
        print(f"V7.7 live-bridge optimizer warning: {type(e).__name__}: {e}")
    return outcome


_V77_PREV_COMMAND = handle_telegram_command

def handle_telegram_command(chat_id, text):
    parts=(text or "").strip().split()
    command=(parts or [""])[0].lower()
    if command in {"/optimizer","/v77","/portfolio","/paperopt","/cryptolab"}:
        send_to_chat(chat_id,_v77_optimizer_summary_text()); return
    if command == "/close":
        owner=_normalize_chat_id(TELEGRAM_CHAT_ID)
        caller=_normalize_chat_id(chat_id)
        if not owner or caller != owner:
            send_to_chat(chat_id,"⛔ /close is restricted to the configured owner chat."); return
        if V70_LIVE is None:
            send_to_chat(chat_id,"⛔ Live executor unavailable; no order was sent."); return
        if len(parts) < 2:
            send_to_chat(chat_id,"Usage: /close SYMBOL  (example: /close HBAR_USDT)"); return
        symbol=parts[1].upper().replace("/","_").replace("-","_")
        if "_" not in symbol: symbol += "_USDT"
        confirmed=len(parts) >= 3 and parts[2].upper() == "CONFIRM"
        if not confirmed:
            try:
                snapshot=V70_LIVE.positions() or []
                matches=[p for p in snapshot if isinstance(p,dict) and str(p.get("symbol") or p.get("contractCode") or "").upper()==symbol and float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)>0]
                if not matches:
                    send_to_chat(chat_id,f"ℹ️ No open MEXC position found for {symbol}. No order sent."); return
                desc=", ".join(f"{str(p.get('symbol') or symbol)} {float(p.get('holdVol') or p.get('vol') or p.get('positionVol') or 0):g}" for p in matches)
                send_to_chat(chat_id,f"⚠️ SINGLE-SYMBOL CLOSE\nTarget: {desc}\nOther symbols will be left untouched.\nNo order sent. To market-close this symbol, send exactly:\n/close {symbol} CONFIRM")
            except Exception as e:
                send_to_chat(chat_id,f"⛔ Could not verify MEXC position; no order sent: {type(e).__name__}: {e}")
            return
        send_to_chat(chat_id,f"🛑 Closing actual MEXC position for {symbol} only and verifying exchange state...")
        result=V70_LIVE.emergency_close_symbol(symbol)
        if result.get("closed"):
            fills=result.get("fills") or []
            desc=", ".join(f"{x.get('symbol')} {x.get('direction')} {x.get('contracts'):g}" for x in fills) or symbol
            extra=("\nWarnings: "+"; ".join(result.get("errors") or [])) if result.get("errors") else ""
            send_to_chat(chat_id,f"✅ {symbol} CLOSED — CONFIRMED BY MEXC\nClosed: {desc}\nOther symbols were not targeted.{extra}")
        else:
            rem=result.get("remaining") or []
            remtxt=", ".join(f"{x.get('symbol')} {x.get('contracts'):g}" for x in rem) or "unknown"
            why=result.get("reason") or "; ".join(result.get("errors") or []) or "unconfirmed close state"
            send_to_chat(chat_id,f"🚨 {symbol} CLOSE NOT CONFIRMED\nRemaining: {remtxt}\nReason: {why}")
        return
    if command == "/closeall":
        # Emergency live-trading kill switch: owner/admin only and two-step by design.
        owner=_normalize_chat_id(TELEGRAM_CHAT_ID)
        caller=_normalize_chat_id(chat_id)
        if not owner or caller != owner:
            send_to_chat(chat_id,"⛔ /closeall is restricted to the configured owner chat."); return
        if V70_LIVE is None:
            send_to_chat(chat_id,"⛔ Live executor unavailable; no order was sent."); return
        confirmed=len(parts) >= 2 and parts[1].upper() == "CONFIRM"
        if not confirmed:
            try:
                snapshot=V70_LIVE.positions() or []
                snapshot=[p for p in snapshot if isinstance(p,dict) and float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0)>0]
                names=", ".join(str(p.get("symbol") or "?") for p in snapshot) or "none"
                send_to_chat(chat_id,f"⚠️ EMERGENCY FLATTEN\nMEXC open positions: {len(snapshot)} ({names})\nNo order sent. To market-close ALL live positions, send exactly:\n/closeall CONFIRM")
            except Exception as e:
                send_to_chat(chat_id,f"⛔ Could not verify MEXC positions; no order sent: {type(e).__name__}: {e}")
            return
        send_to_chat(chat_id,"🛑 Emergency flatten requested. Closing actual MEXC positions and verifying exchange state...")
        result=V70_LIVE.emergency_flatten_all()
        if result.get("flat"):
            closed=result.get("closed") or []
            desc=", ".join(f"{x.get('symbol')} {x.get('direction')} {x.get('contracts'):g}" for x in closed) or "already flat"
            extra=("\nWarnings: "+"; ".join(result.get("errors") or [])) if result.get("errors") else ""
            send_to_chat(chat_id,f"✅ FLAT CONFIRMED BY MEXC\nClosed: {desc}\nExchange open positions: 0{extra}")
        else:
            rem=result.get("remaining") or []
            remtxt=", ".join(f"{x.get('symbol')} {x.get('contracts'):g}" for x in rem) or "unknown"
            why=result.get("reason") or "; ".join(result.get("errors") or []) or "unconfirmed close state"
            send_to_chat(chat_id,f"🚨 NOT FLAT — DO NOT ASSUME POSITIONS ARE CLOSED\nRemaining: {remtxt}\nReason: {why}")
        return
    return _V77_PREV_COMMAND(chat_id,text)


# ============================================================
# RENDER HEALTH SERVER
# ============================================================

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in {"/", "/health", "/status"}:
            body = json.dumps({
                "ok": True,
                "service": "FuturesHunter",
                "version": "7.7.2-crypto-metals-optimizer-kill-switch",
                "equity_shadow_lab": ("active" if V73_EQUITY_DB_READY else "disabled_or_unavailable"),
                "equity_agent_ensemble": ("active" if V76_DB_READY else "disabled_or_unavailable"),
                "equity_agents": list(V76_AGENT_NAMES),
                "equity_meta_min_score": V76_META_MIN_SCORE,
                "equity_top_n": V73_EQUITY_TOP_N,
                "equity_live_path": ("enabled" if V75_EQUITY_LIVE_ENABLED else "shadow_first"),
                "equity_live_risk_pct": V75_EQUITY_RISK_PCT,
                "equity_live_max_open": V75_EQUITY_MAX_OPEN,
                "v77_optimizer_lab": ("active" if V77_DB_READY else "disabled_or_unavailable"),
                "v77_portfolio_max_open": V77_MAX_OPEN,
                "v77_same_bucket_direction_max": V77_MAX_SAME_BUCKET_DIRECTION,
                "v77_cost_gate_r": V77_MAX_COST_R,
                "v77_managed_exit_shadow": "25_25_50",
                "v77_metals_context": "cross_metal_shadow",
                "live_pilot": ("enabled" if (V70_LIVE is not None and V70_LIVE.ENABLED) else "disabled"),
                "v70_selective_gate": bool(V70_SELECTIVE_GATE),
                "v70_same_symbol_cooldown_minutes": V70_SAME_SYMBOL_COOLDOWN_MINUTES,
                "v71_selection_challenger": "active" if V71_CHALLENGER_DB_READY else "local_only",
                "macro_regime": get_macro_snapshot().get("regime"),
                "database_ready": bool(V68_DB_READY),
                "research_lab": "active" if V68_DB_READY else "local_fallback",
                "risk_challenger": "shadow" if V68_DB_READY else "local_fallback",
                "strategy_ensemble": "shadow" if V68_DB_READY else "local_fallback",
                "trade_supervisor": "shadow_live" if V68_DB_READY else "local_fallback",
                "event_risk": get_macro_snapshot().get("event_risk"),
                "uptime_seconds": int(max(0, time.time() - STARTED_AT)),
                "time": local_time(),
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def _json_response(self, status, payload):
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _emergency_authorized(self):
        # Accept either standard Bearer auth or the dedicated header used by
        # the iOS Shortcuts emergency controller. Keep the shared secret
        # server-side and compare exact values only.
        if not EMERGENCY_CLOSE_SECRET:
            return False
        auth = str(self.headers.get("Authorization") or "").strip()
        shortcut_secret = str(self.headers.get("X-Emergency-Secret") or "").strip()
        return (
            auth == f"Bearer {EMERGENCY_CLOSE_SECRET}"
            or shortcut_secret == EMERGENCY_CLOSE_SECRET
        )

    def do_POST(self):
        # Private backup control path for emergency symbol closes.
        # No MEXC credentials are accepted over HTTP; they remain server-side.
        if self.path.rstrip("/") != "/emergency/close":
            self._json_response(404, {"ok": False, "error": "not_found"})
            return
        if not self._emergency_authorized():
            self._json_response(403, {"ok": False, "error": "forbidden"})
            return
        if V70_LIVE is None:
            self._json_response(503, {"ok": False, "error": "live_executor_unavailable"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > 4096:
                self._json_response(400, {"ok": False, "error": "invalid_body"})
                return
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            symbol = str(payload.get("symbol") or "").strip().upper().replace("/", "_").replace("-", "_")
            if symbol and "_" not in symbol:
                symbol += "_USDT"
            if not symbol or not symbol.endswith("_USDT"):
                self._json_response(400, {"ok": False, "error": "invalid_symbol"})
                return
            confirm = str(payload.get("confirm") or "").strip().upper()
            snapshot = V70_LIVE.positions() or []
            matches = [p for p in snapshot if isinstance(p, dict)
                       and str(p.get("symbol") or p.get("contractCode") or "").upper() == symbol
                       and float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0) > 0]
            view = [{"symbol": symbol,
                     "contracts": float(p.get("holdVol") or p.get("vol") or p.get("positionVol") or 0),
                     "direction": V70_LIVE._position_direction(p)} for p in matches]
            if confirm != "CONFIRM":
                self._json_response(200, {"ok": True, "mode": "READ_ONLY", "symbol": symbol,
                                          "positions": view, "order_sent": False,
                                          "instruction": "Repeat with confirm=CONFIRM to market-close this symbol only."})
                return
            if not matches:
                self._json_response(409, {"ok": False, "symbol": symbol, "closed": False,
                                          "order_sent": False, "error": "no_open_position"})
                return
            result = V70_LIVE.emergency_close_symbol(symbol)
            status = 200 if result.get("closed") else 409
            self._json_response(status, result)
        except Exception as error:
            self._json_response(500, {"ok": False, "error": type(error).__name__, "detail": _safe_error(error)})

    def log_message(self, format, *args):
        # Keep Render logs focused on scanner output.
        return


def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="FuturesHunterHealth",
        daemon=True,
    )
    thread.start()
    print(f"Health endpoint: http://0.0.0.0:{port}/health")
    return server

# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    try:
        start_health_server()
        start_macro_news_thread()
        print(f"[V7DIAG] integration hook reached; main_version={globals().get('V743_BRIDGE_VERSION', 'pre-bridge')} live_module_loaded={V70_LIVE is not None}", flush=True)
        challenger_ok = _v71_init_challenger()
        print(f"[V7DIAG] V7.1 challenger init ready={challenger_ok} v68_db_ready={V68_DB_READY}", flush=True)
        if V70_LIVE is not None:
            try:
                ds = V70_LIVE.diagnostic_state()
                print("[V7DIAG] executor config " + " ".join(f"{k}={v}" for k,v in ds.items()), flush=True)
            except Exception as e:
                print(f"[V7DIAG] executor diagnostic_state unavailable: {type(e).__name__}: {e}", flush=True)
            V70_LIVE.configure(notify=send_telegram)
            startup_ok = V70_LIVE.startup_reconcile()
            print(f"[V7DIAG] startup_reconcile returned={startup_ok}", flush=True)

            if getattr(V70_LIVE, "DRY_RUN", False):
                dry_ok = V70_LIVE.run_zero_order_dry_run()
                print(f"[V7DIAG] zero_order_dry_run returned={dry_ok}", flush=True)
            V70_LIVE.start_reconciler()
        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        print()
        print(
            "Futures Hunter stopped."
        )
