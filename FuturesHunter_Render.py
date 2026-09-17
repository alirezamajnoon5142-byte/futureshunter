
import asyncio
import csv
import json
import os
import time
import threading
from datetime import datetime
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
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # admin / original subscriber
WEBSITE_URL = os.getenv("WEBSITE_URL", "https://xautguard-hq5ehhqr.manus.space/")


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
        print(f"Telegram send error ({chat_id}): {error}")
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
        print(f"Telegram webhook cleanup warning: {error}")

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
            print(f"Telegram command loop error: {error}")
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

            for ticker in tickers:
                symbol = ticker.get("symbol", "")
                if not symbol.endswith("_USDT"):
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
        "✅ FuturesHunter V6.6 is online.\n\n"
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
    print("MEXC FUTURES HUNTER V6.6 — ADAPTIVE 50 FACTORS")
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
        "✅ FuturesHunter V6.6 is online.\n\n"
        "Weighted 50-factor scoring + breakout mode + "
        "WATCH/ARMED/ENTRY + shadow-rejection tracking active.\n\n"
        "Try /watch or /why ZEC."
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
# RENDER HEALTH SERVER
# ============================================================

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in {"/", "/health", "/status"}:
            body = json.dumps({
                "ok": True,
                "service": "FuturesHunter",
                "version": "6.6",
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
        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        print()
        print(
            "Futures Hunter stopped."
        )
