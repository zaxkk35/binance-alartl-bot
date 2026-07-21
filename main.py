"""
Binance Big Order (Whale Wall) Telegram Bot
--------------------------------------------
Maintains a LIVE local order book per watched symbol via Binance's
USDT-M Futures diff-depth websocket stream on top of an initial REST
snapshot (Binance's official local order book procedure). Reports
EXACT resting buy/sell walls (no clustering/merging) within a fixed
price-point range of the current price -- on demand, on a daily
schedule, and/or as live push alerts when a new big wall appears.

Also includes:
  /liq     -- nearest big wall (>=300 base-asset units) on each side
  /ta      -- RSI / EMA / Fibonacci technicals + wall confluence
  /signal  -- simplified order-block + fib retracement entry read

Run:
    pip install -r requirements.txt
    export TELEGRAM_BOT_TOKEN="123456:ABC..."
    python main.py
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import websockets
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
log = logging.getLogger("wall-bot")

BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_WS_BASE = "wss://fstream.binance.com/stream"
SETTINGS_FILE = Path(__file__).parent / "user_settings.json"
ICT_TZ = ZoneInfo("Asia/Bangkok")  # UTC+7, used for /time

EXACT_CLUSTER_PCT = 0.0   # no merging -- raw tick-level wall data, like ATAS
LIQ_THRESHOLD = 300.0     # /liq only looks at walls >= this size (base asset units)
FIB_RATIOS = [0.236, 0.382, 0.5, 0.618, 0.786]
QUICK_SYMBOLS = ["BTC", "SOL", "ZEC", "HYPE", "BNB"]

DEFAULTS = {
    "symbol": "BTCUSDT",
    "threshold_qty": 5.0,     # min resting size, in base-asset units (e.g. BTC), to count as a wall
    "range_abs": 3000.0,      # only show orders within +/- this many price points of current price
    "daily_time": None,       # "HH:MM" 24h, Bangkok time, or None if off
    "alert_on": False,        # live push when a new wall crosses threshold
}

# ---------------------------------------------------------------------------
# Simple JSON-backed per-chat settings store
# ---------------------------------------------------------------------------
_settings_lock = asyncio.Lock()


def _load_all() -> dict:
    if SETTINGS_FILE.exists():
        try:
            return json.loads(SETTINGS_FILE.read_text())
        except json.JSONDecodeError:
            log.warning("settings file corrupt, resetting")
    return {}


def _save_all(data: dict) -> None:
    SETTINGS_FILE.write_text(json.dumps(data, indent=2))


async def get_settings(chat_id: int) -> dict:
    async with _settings_lock:
        all_settings = _load_all()
        s = all_settings.get(str(chat_id), {})
        merged = {**DEFAULTS, **s}
        return merged


async def update_setting(chat_id: int, key: str, value) -> None:
    async with _settings_lock:
        all_settings = _load_all()
        s = all_settings.get(str(chat_id), {})
        s[key] = value
        all_settings[str(chat_id)] = s
        _save_all(all_settings)


def base_asset(symbol: str) -> str:
    for quote in ("USDT", "BUSD", "USDC", "USD"):
        if symbol.endswith(quote):
            return symbol[: -len(quote)]
    return symbol


# ---------------------------------------------------------------------------
# 12-hour time parsing, for /time
# ---------------------------------------------------------------------------
TIME_RE = re.compile(r"^(\d{1,2})(?::([0-5]\d))?\s*([APap][Mm])$")


def parse_12h_time(s: str):
    m = TIME_RE.match(s.strip())
    if not m:
        raise ValueError("bad format")
    hour = int(m.group(1))
    minute = int(m.group(2) or 0)
    ampm = m.group(3).lower()
    if not (1 <= hour <= 12):
        raise ValueError("hour out of range")
    if ampm == "am":
        hour24 = 0 if hour == 12 else hour
    else:
        hour24 = 12 if hour == 12 else hour + 12
    return hour24, minute


def format_12h(hour24: int, minute: int) -> str:
    ampm = "am" if hour24 < 12 else "pm"
    h12 = hour24 % 12
    if h12 == 0:
        h12 = 12
    return f"{h12}:{minute:02d}{ampm}"


def fmt_age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return "up <1m"
    minutes = seconds // 60
    if minutes < 60:
        return f"up {minutes}m"
    hours = minutes // 60
    minutes = minutes % 60
    return f"up {hours}h {minutes}m"


# ---------------------------------------------------------------------------
# Live local order book (Binance's official sync procedure)
# ---------------------------------------------------------------------------
class LiveOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.bid_first_seen: dict[float, float] = {}
        self.ask_first_seen: dict[float, float] = {}
        self.last_update_id: int | None = None
        self.ready = asyncio.Event()
        self.lock = asyncio.Lock()
        self.task: asyncio.Task | None = None
        self.last_message_ts: float = 0.0
        self.synced_since: float = 0.0

    async def snapshot(self):
        """Returns (best_bid, best_ask, bids_copy, asks_copy, bid_fs_copy, ask_fs_copy)."""
        async with self.lock:
            if not self.bids or not self.asks:
                return None, None, {}, {}, {}, {}
            best_bid = max(self.bids)
            best_ask = min(self.asks)
            return (
                best_bid, best_ask,
                dict(self.bids), dict(self.asks),
                dict(self.bid_first_seen), dict(self.ask_first_seen),
            )

    async def _apply_snapshot(self, data: dict):
        now = time.time()
        async with self.lock:
            self.bids = {float(p): float(q) for p, q in data["bids"] if float(q) > 0}
            self.asks = {float(p): float(q) for p, q in data["asks"] if float(q) > 0}
            self.bid_first_seen = {p: now for p in self.bids}
            self.ask_first_seen = {p: now for p in self.asks}
            self.last_update_id = data["lastUpdateId"]

    async def _apply_diff(self, event: dict):
        now = time.time()
        async with self.lock:
            for p_str, q_str in event["b"]:
                p, q = float(p_str), float(q_str)
                if q == 0:
                    self.bids.pop(p, None)
                    self.bid_first_seen.pop(p, None)
                else:
                    if p not in self.bids:
                        self.bid_first_seen[p] = now
                    self.bids[p] = q
            for p_str, q_str in event["a"]:
                p, q = float(p_str), float(q_str)
                if q == 0:
                    self.asks.pop(p, None)
                    self.ask_first_seen.pop(p, None)
                else:
                    if p not in self.asks:
                        self.ask_first_seen[p] = now
                    self.asks[p] = q
            self.last_update_id = event["u"]
        self.last_message_ts = now

    async def run(self):
        """Background loop: connect, sync, apply diffs forever (with reconnect/backoff)."""
        backoff = 1
        stream_name = f"{self.symbol.lower()}@depth@500ms"
        url = f"{BINANCE_WS_BASE}?streams={stream_name}"
        while True:
            try:
                self.ready.clear()
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    log.info("ws connected for %s", self.symbol)

                    async def recv_one():
                        raw = await ws.recv()
                        return json.loads(raw)["data"]

                    first_event = await recv_one()

                    async with httpx.AsyncClient(timeout=10) as client:
                        r = await client.get(
                            f"{BINANCE_FAPI_BASE}/fapi/v1/depth",
                            params={"symbol": self.symbol, "limit": 1000},
                        )
                        r.raise_for_status()
                        snap = r.json()
                    await self._apply_snapshot(snap)
                    last_applied_u = self.last_update_id

                    started = False

                    async def process(event):
                        nonlocal started, last_applied_u
                        if event["u"] <= last_applied_u:
                            return
                        if not started:
                            if event["U"] > last_applied_u + 1:
                                raise RuntimeError("gap between snapshot and stream, resyncing")
                            started = True
                        else:
                            if event.get("pu") != last_applied_u:
                                raise RuntimeError("sequence gap detected, resyncing")
                        await self._apply_diff(event)
                        last_applied_u = event["u"]

                    await process(first_event)

                    self.synced_since = time.time()
                    self.ready.set()
                    backoff = 1
                    log.info("%s order book synced (lastUpdateId=%s)", self.symbol, last_applied_u)

                    async for raw in ws:
                        msg = json.loads(raw)
                        await process(msg["data"])

            except Exception as e:
                self.ready.clear()
                log.warning("book stream for %s dropped (%s), reconnecting in %ss", self.symbol, e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


class BookManager:
    def __init__(self):
        self.books: dict[str, LiveOrderBook] = {}
        self.lock = asyncio.Lock()

    async def ensure(self, symbol: str) -> LiveOrderBook:
        symbol = symbol.upper()
        async with self.lock:
            book = self.books.get(symbol)
            if book is None:
                book = LiveOrderBook(symbol)
                book.task = asyncio.create_task(book.run())
                self.books[symbol] = book
        return book


book_manager = BookManager()


# ---------------------------------------------------------------------------
# Wall detection -- EXACT mode, no clustering/merging (matches ATAS raw ticks)
# ---------------------------------------------------------------------------
def cluster_levels(items, first_seen_map, lower_bound, upper_bound, threshold_qty):
    """items: iterable of (price, qty) floats.
    With EXACT_CLUSTER_PCT this returns every individual price tick above
    threshold, unmerged. Returns list of (price, qty, first_seen_ts), no cap."""
    now = time.time()
    walls = []
    for p, q in items:
        if not (lower_bound <= p <= upper_bound) or q <= 0:
            continue
        if q >= threshold_qty:
            walls.append((p, q, first_seen_map.get(p, now)))
    walls.sort(key=lambda w: w[1], reverse=True)
    return walls


async def get_big_walls(symbol: str, threshold_qty: float, range_abs: float):
    """Returns (ready, mid_price, buy_walls, sell_walls).
    Each wall is (price, qty, first_seen_ts). No limit on count."""
    book = await book_manager.ensure(symbol)
    try:
        await asyncio.wait_for(book.ready.wait(), timeout=8)
    except asyncio.TimeoutError:
        return False, None, [], []

    best_bid, best_ask, bids, asks, bid_fs, ask_fs = await book.snapshot()
    if best_bid is None:
        return False, None, [], []

    mid_price = (best_bid + best_ask) / 2
    lower_bound = mid_price - range_abs
    upper_bound = mid_price + range_abs

    buy_walls = cluster_levels(bids.items(), bid_fs, lower_bound, upper_bound, threshold_qty)
    sell_walls = cluster_levels(asks.items(), ask_fs, lower_bound, upper_bound, threshold_qty)
    return True, mid_price, buy_walls, sell_walls


def zone_key(price: float, mid_price: float) -> int:
    width = max(mid_price * 0.0002, 0.0001)  # tight fixed zone width for alert de-dupe
    return round(price / width)


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def fmt_qty(n: float) -> str:
    if n >= 1000:
        return f"{n/1000:.2f}K"
    return f"{n:.3f}"


def build_report(symbol, price, buy_walls, sell_walls, threshold_qty, range_abs, show_age=False) -> str:
    asset = base_asset(symbol)
    lines = [
        f"📊 *{symbol}* live wall scan (exact)",
        f"Threshold: ≥ {fmt_qty(threshold_qty)} {asset}  |  Range: `{price - range_abs:,.0f}` – `{price + range_abs:,.0f}`",
        "",
    ]

    def wall_line(p, q, ts):
        base = f"  `{p:,.2f}`  →  {fmt_qty(q)} {asset}"
        if show_age:
            base += f"  ({fmt_age(time.time() - ts)})"
        return base

    lines.append("🔴 *Sell walls*")
    if sell_walls:
        for p, q, ts in sorted(sell_walls, key=lambda w: w[0], reverse=True):
            lines.append(wall_line(p, q, ts))
    else:
        lines.append("  none found")

    # Clearly separated current-price divider so it doesn't blend into the wall list
    lines.append("")
    lines.append(f"📍  *{price:,.2f}*  ← current price")
    lines.append("")

    lines.append("🟢 *Buy walls*")
    if buy_walls:
        for p, q, ts in sorted(buy_walls, key=lambda w: w[0], reverse=True):
            lines.append(wall_line(p, q, ts))
    else:
        lines.append("  none found")

    return "\n".join(lines)


def action_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Receive", callback_data="receive"),
                InlineKeyboardButton("🕒 Get", callback_data="get"),
            ],
            [InlineKeyboardButton("📡 Status", callback_data="status")],
            [
                InlineKeyboardButton("📐 TA", callback_data="ta"),
                InlineKeyboardButton("🎯 Signal", callback_data="signal"),
            ],
        ]
    )


def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📥 Receive", callback_data="receive"),
                InlineKeyboardButton("🕒 Get", callback_data="get"),
            ],
            [InlineKeyboardButton("📡 Status", callback_data="status")],
            [
                InlineKeyboardButton("📐 TA", callback_data="ta"),
                InlineKeyboardButton("🎯 Signal", callback_data="signal"),
            ],
            [InlineKeyboardButton("📖 Help", callback_data="help")],
        ]
    )


def alert_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("🔔 On", callback_data="alert:on"),
            InlineKeyboardButton("🔕 Off", callback_data="alert:off"),
        ]]
    )


def timeoff_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Turn off daily report", callback_data="timeoff")]])


def symbol_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(sym, callback_data=f"symbol:{sym}USDT") for sym in QUICK_SYMBOLS[:3]],
            [InlineKeyboardButton(sym, callback_data=f"symbol:{sym}USDT") for sym in QUICK_SYMBOLS[3:]]]
    rows[-1].append(InlineKeyboardButton("Others…", callback_data="symbol:others"))
    return InlineKeyboardMarkup(rows)


def signal_interval_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("1H", callback_data="signal:1h"),
            InlineKeyboardButton("4H", callback_data="signal:4h"),
            InlineKeyboardButton("8H", callback_data="signal:8h"),
        ]]
    )


# ---------------------------------------------------------------------------
# Core fetch used by /receive, /get, scheduled reports
# ---------------------------------------------------------------------------
async def _do_receive(chat_id: int, show_age: bool = False) -> str:
    s = await get_settings(chat_id)
    ready, price, buy_walls, sell_walls = await get_big_walls(s["symbol"], s["threshold_qty"], s["range_abs"])
    if not ready:
        return (
            f"⏳ Still syncing the *{s['symbol']}* order book — try again in a few seconds.\n"
            "(This only happens right after the bot starts or the symbol changes.)"
        )
    return build_report(s["symbol"], price, buy_walls, sell_walls, s["threshold_qty"], s["range_abs"], show_age)


async def _do_status(chat_id: int) -> str:
    s = await get_settings(chat_id)
    book = await book_manager.ensure(s["symbol"])
    if not book.ready.is_set():
        return f"⏳ `{s['symbol']}` order book is still syncing..."
    best_bid, best_ask, bids, asks, _, _ = await book.snapshot()
    uptime = time.time() - book.synced_since
    last_update_age = time.time() - book.last_message_ts if book.last_message_ts else None
    lines = [
        f"📡 *Live Book Status* — `{s['symbol']}`",
        f"Synced: ✅ ({fmt_age(uptime)})",
        f"Levels tracked: {len(bids):,} bids / {len(asks):,} asks",
    ]
    if last_update_age is not None:
        lines.append(f"Last update: {int(last_update_age)}s ago")
    return "\n".join(lines)


async def _do_liq(chat_id: int) -> str:
    s = await get_settings(chat_id)
    ready, price, buy_walls, sell_walls = await get_big_walls(s["symbol"], LIQ_THRESHOLD, s["range_abs"])
    if not ready:
        return f"⏳ Still syncing the *{s['symbol']}* order book — try again in a few seconds."

    asset = base_asset(s["symbol"])
    lines = [
        f"🧲 *Liquidity Magnet* — `{s['symbol']}`",
        f"Current price: `{price:,.2f}`  |  Looking for walls ≥ {fmt_qty(LIQ_THRESHOLD)} {asset}",
        "",
    ]

    if sell_walls:
        p, q, _ = min(sell_walls, key=lambda w: abs(w[0] - price))
        dist = p - price
        lines.append(f"🔴 Nearest big sell: `{p:,.2f}` → {fmt_qty(q)} {asset}  (+{dist:,.0f} pts, {dist/price*100:+.2f}%)")
    else:
        lines.append(f"🔴 No sell wall ≥ {fmt_qty(LIQ_THRESHOLD)} {asset} found in range.")

    if buy_walls:
        p, q, _ = min(buy_walls, key=lambda w: abs(w[0] - price))
        dist = price - p
        lines.append(f"🟢 Nearest big buy: `{p:,.2f}` → {fmt_qty(q)} {asset}  (-{dist:,.0f} pts, {-dist/price*100:+.2f}%)")
    else:
        lines.append(f"🟢 No buy wall ≥ {fmt_qty(LIQ_THRESHOLD)} {asset} found in range.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Klines + technical indicators (for /ta and /signal)
# ---------------------------------------------------------------------------
async def fetch_klines(symbol: str, interval: str, limit: int = 200):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{BINANCE_FAPI_BASE}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        r.raise_for_status()
        raw = r.json()
    return [
        {"open": float(k[1]), "high": float(k[2]), "low": float(k[3]), "close": float(k[4])}
        for k in raw
    ]


def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema


def fib_from_swing(highs, lows, lookback):
    lookback = min(lookback, len(highs))
    recent_highs = highs[-lookback:]
    recent_lows = lows[-lookback:]
    hi = max(recent_highs)
    hi_idx = len(highs) - lookback + recent_highs.index(hi)
    lo = min(recent_lows)
    lo_idx = len(lows) - lookback + recent_lows.index(lo)
    direction = "up" if hi_idx > lo_idx else "down"
    if direction == "up":
        levels = {r: hi - (hi - lo) * r for r in FIB_RATIOS}
    else:
        levels = {r: lo + (hi - lo) * r for r in FIB_RATIOS}
    return direction, lo, hi, levels


ENTRY_RATIOS = [0.618, 0.649]
TP_RATIO = 1.01
RISK_REWARD = 1.3  # stop distance = TP distance / 1.3


def find_fib_extension_signal(klines):
    """Trend-Based Fib Extension strategy:
    P1 -> P2 defines the impulsive swing, P3 is the pullback point the
    extension is projected from. Entry = price touching the 0.618 or 0.649
    extension level from P3; TP = the 1.01 extension level from P3;
    stop-loss distance = TP distance / 1.3 (not placed at swing low/high)."""
    n = len(klines)
    if n < 20:
        return None
    highs = [k["high"] for k in klines]
    lows = [k["low"] for k in klines]
    closes = [k["close"] for k in klines]

    swing_highs, swing_lows = [], []
    for i in range(3, n - 3):
        if highs[i] == max(highs[i - 3:i + 4]):
            swing_highs.append((i, "H", highs[i]))
        if lows[i] == min(lows[i - 3:i + 4]):
            swing_lows.append((i, "L", lows[i]))
    tagged = sorted(swing_highs + swing_lows, key=lambda t: t[0])
    if len(tagged) < 3:
        return None

    p1_idx, p1_type, p1 = tagged[-3]
    p2_idx, p2_type, p2 = tagged[-2]
    p3_idx, p3_type, p3 = tagged[-1]

    if not (p1_type != p2_type and p2_type != p3_type and p1_type == p3_type):
        return None  # must alternate H-L-H or L-H-L

    if p1_type == "L" and p2_type == "H":
        direction = "long"
        if p3 <= p1:
            return None  # P3 must be a HIGHER low than P1
        move = p2 - p1
        entry_levels = {r: p3 + move * r for r in ENTRY_RATIOS}
        tp = p3 + move * TP_RATIO
    else:
        direction = "short"
        if p3 >= p1:
            return None  # P3 must be a LOWER high than P1
        move = p1 - p2
        entry_levels = {r: p3 - move * r for r in ENTRY_RATIOS}
        tp = p3 - move * TP_RATIO

    current_price = closes[-1]
    plans = {}
    for r, entry in entry_levels.items():
        reward = abs(tp - entry)
        stop_dist = reward / RISK_REWARD
        sl = entry - stop_dist if direction == "long" else entry + stop_dist
        plans[r] = {"entry": entry, "sl": sl, "reward": reward, "stop_dist": stop_dist}

    lo_entry = min(e["entry"] for e in plans.values())
    hi_entry = max(e["entry"] for e in plans.values())
    if direction == "long":
        triggered = current_price <= hi_entry
    else:
        triggered = current_price >= lo_entry

    return {
        "direction": direction, "p1": p1, "p2": p2, "p3": p3,
        "plans": plans, "tp": tp, "current_price": current_price,
        "triggered": triggered,
    }


async def _do_ta(chat_id: int) -> str:
    s = await get_settings(chat_id)
    symbol = s["symbol"]
    try:
        klines = await fetch_klines(symbol, "1h", 200)
    except Exception as e:
        return f"❌ Couldn't load candles for `{symbol}`: {e}"

    closes = [k["close"] for k in klines]
    highs = [k["high"] for k in klines]
    lows = [k["low"] for k in klines]
    price = closes[-1]

    rsi = compute_rsi(closes, 14)
    ema9, ema21, ema50 = compute_ema(closes, 9), compute_ema(closes, 21), compute_ema(closes, 50)

    if rsi is None or ema50 is None:
        return "⏳ Not enough candle history yet for a full read — try again shortly."

    if price > ema9 > ema21 > ema50:
        trend = "🟢 bullish (price above EMA9/21/50, all rising order)"
    elif price < ema9 < ema21 < ema50:
        trend = "🔴 bearish (price below EMA9/21/50, all falling order)"
    else:
        trend = "🟡 mixed / no clean trend"

    if rsi >= 70:
        rsi_note = "overbought"
    elif rsi <= 30:
        rsi_note = "oversold"
    else:
        rsi_note = "neutral"

    direction, lo, hi, levels = fib_from_swing(highs, lows, lookback=80)

    ready, _, buy_walls, sell_walls = await get_big_walls(symbol, s["threshold_qty"], s["range_abs"])
    confluences = []
    if ready:
        all_walls = [(p, q, "buy") for p, q, _ in buy_walls] + [(p, q, "sell") for p, q, _ in sell_walls]
        for r, lvl in levels.items():
            for p, q, side in all_walls:
                if lvl > 0 and abs(p - lvl) / lvl * 100 <= 0.15:
                    confluences.append((r, lvl, p, q, side))

    asset = base_asset(symbol)
    lines = [
        f"📐 *{symbol}* — 1H Technicals",
        f"Price: `{price:,.2f}`",
        f"RSI(14): {rsi:.1f} ({rsi_note})",
        f"EMA9 `{ema9:,.2f}`  EMA21 `{ema21:,.2f}`  EMA50 `{ema50:,.2f}`",
        f"Trend: {trend}",
        "",
        f"*Fibonacci* (swing {'low → high' if direction == 'up' else 'high → low'}: `{lo:,.2f}` – `{hi:,.2f}`)",
    ]
    for r in FIB_RATIOS:
        lines.append(f"  {r*100:.1f}%  →  `{levels[r]:,.2f}`")

    if confluences:
        lines.append("")
        lines.append("🎯 *Confluence*")
        for r, lvl, p, q, side in confluences:
            icon = "🟢" if side == "buy" else "🔴"
            lines.append(f"  {icon} {r*100:.1f}% fib (`{lvl:,.2f}`) lines up with a {fmt_qty(q)} {asset} {side} wall at `{p:,.2f}`")

    lines.append("")
    lines.append("_Not financial advice — a simplified technical read, verify against your own charts._")
    return "\n".join(lines)


async def _do_signal(chat_id: int, interval: str) -> str:
    s = await get_settings(chat_id)
    symbol = s["symbol"]
    try:
        klines = await fetch_klines(symbol, interval, 200)
    except Exception as e:
        return f"❌ Couldn't load candles for `{symbol}`: {e}"

    d = find_fib_extension_signal(klines)
    if d is None:
        return f"No clean P1→P2→P3 swing pattern found on `{symbol}` {interval.upper()} right now — try a different timeframe."

    icon = "🟢" if d["direction"] == "long" else "🔴"
    status = "✅ price is inside/through the entry zone" if d["triggered"] else "⏳ price hasn't reached an entry level yet"
    lines = [
        f"🎯 *{symbol}* — {interval.upper()} Fib Extension Signal",
        f"Current price: `{d['current_price']:,.2f}`",
        f"Bias: {icon} {d['direction']}   ({status})",
        "",
        f"P1 `{d['p1']:,.2f}`  →  P2 `{d['p2']:,.2f}`  →  P3 (pullback) `{d['p3']:,.2f}`",
        "",
        "*Entry levels:*",
    ]
    for r in ENTRY_RATIOS:
        p = d["plans"][r]
        lines.append(f"  {r} → entry `{p['entry']:,.2f}`  |  SL `{p['sl']:,.2f}`  |  risk {p['stop_dist']:,.2f} pts (1.3R)")
    lines.append("")
    lines.append(f"TP (1.01 extension): `{d['tp']:,.2f}`")
    lines.append("")
    lines.append("_Trend-based fib extension strategy. Not financial advice — verify against your own charts before risking money._")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Scheduled daily report (/time)
# ---------------------------------------------------------------------------
async def send_scheduled_report(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    try:
        report = await _do_receive(chat_id, show_age=False)
    except Exception:
        log.exception("scheduled report failed for %s", chat_id)
        return
    await context.bot.send_message(chat_id=chat_id, text=report, parse_mode="Markdown", reply_markup=action_keyboard())


def schedule_daily_job(job_queue, chat_id: int, hour: int, minute: int):
    for job in job_queue.get_jobs_by_name(f"daily_{chat_id}"):
        job.schedule_removal()
    job_queue.run_daily(
        send_scheduled_report,
        time=dtime(hour=hour, minute=minute, tzinfo=ICT_TZ),
        chat_id=chat_id,
        name=f"daily_{chat_id}",
    )


def remove_daily_job(job_queue, chat_id: int):
    for job in job_queue.get_jobs_by_name(f"daily_{chat_id}"):
        job.schedule_removal()


# ---------------------------------------------------------------------------
# Live alerts (/alert on|off) -- pings when a NEW wall appears above threshold
# ---------------------------------------------------------------------------
alert_state: dict[int, dict] = {}  # chat_id -> {"primed": bool, "seen": set()}


async def alert_check(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.chat_id
    s = await get_settings(chat_id)
    if not s.get("alert_on"):
        return
    ready, price, buy_walls, sell_walls = await get_big_walls(s["symbol"], s["threshold_qty"], s["range_abs"])
    if not ready:
        return

    current = {}
    for p, q, ts in buy_walls:
        current[("buy", zone_key(p, price))] = (p, q)
    for p, q, ts in sell_walls:
        current[("sell", zone_key(p, price))] = (p, q)

    state = alert_state.setdefault(chat_id, {"primed": False, "seen": set()})
    current_keys = set(current.keys())

    if not state["primed"]:
        state["seen"] = current_keys
        state["primed"] = True
        return

    new_keys = current_keys - state["seen"]
    state["seen"] = current_keys

    if not new_keys:
        return

    asset = base_asset(s["symbol"])
    lines = [f"🚨 *New wall* — `{s['symbol']}`"]
    for side, key in new_keys:
        p, q = current[(side, key)]
        icon = "🟢" if side == "buy" else "🔴"
        lines.append(f"{icon} `{p:,.2f}`  →  {fmt_qty(q)} {asset}")
    await context.bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="Markdown")


def ensure_alert_job(job_queue, chat_id: int):
    if not job_queue.get_jobs_by_name(f"alert_{chat_id}"):
        alert_state[chat_id] = {"primed": False, "seen": set()}
        job_queue.run_repeating(alert_check, interval=45, first=10, chat_id=chat_id, name=f"alert_{chat_id}")


def remove_alert_job(job_queue, chat_id: int):
    for job in job_queue.get_jobs_by_name(f"alert_{chat_id}"):
        job.schedule_removal()
    alert_state.pop(chat_id, None)


# ---------------------------------------------------------------------------
# Telegram handlers
# ---------------------------------------------------------------------------
HELP_TEXT = (
    "*Commands*\n"
    "/symbol — quick-pick a pair, or /symbol SYM to type one\n"
    "/threshold N — min wall size, in base asset\n"
    "/range N — ± price points around current price\n"
    "/receive — snapshot now (exact walls, no cap)\n"
    "/get — snapshot now, with wall age\n"
    "/liq — nearest big wall (≥300) on each side\n"
    "/ta — RSI/EMA/Fibonacci technicals + wall confluence\n"
    "/signal — order block + fib entry read (pick 1H/4H/8H)\n"
    "/time 7:00pm — daily auto report (Bangkok time)\n"
    "/timeoff — turn off daily report\n"
    "/alert on|off — live push when a new big wall appears\n"
    "/status — live order book sync status"
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s = await get_settings(chat_id)
    asset = base_asset(s["symbol"])
    await book_manager.ensure(s["symbol"])
    text = (
        "📡 *LIVE ORDER BOOK*\n\n"
        f"Symbol: `{s['symbol']}`\n"
        f"Threshold: {fmt_qty(s['threshold_qty'])} {asset}\n"
        f"Range: ±{s['range_abs']:,.0f} price points\n\n"
        "Type /help to see all commands."
    )
    await update.message.reply_text(
        text, parse_mode="Markdown", reply_markup=start_keyboard()
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def _apply_symbol(chat_id: int, symbol: str, responder):
    symbol = symbol.upper()
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/price", params={"symbol": symbol})
            r.raise_for_status()
        except httpx.HTTPStatusError:
            await responder(f"❌ `{symbol}` not found on Futures.", parse_mode="Markdown")
            return
    await update_setting(chat_id, "symbol", symbol)
    await book_manager.ensure(symbol)
    await responder(
        f"✅ Symbol set to `{symbol}` — syncing its order book now, give it a few seconds.",
        parse_mode="Markdown",
    )


async def set_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Pick a symbol:", reply_markup=symbol_keyboard())
        return
    await _apply_symbol(chat_id, context.args[0], update.message.reply_text)


async def set_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s = await get_settings(chat_id)
    asset = base_asset(s["symbol"])
    if not context.args:
        await update.message.reply_text(f"Usage: /threshold 5  (min wall size in {asset})")
        return
    try:
        value = float(context.args[0])
        if value <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(f"Please provide a positive number, e.g. /threshold 5 (means 5 {asset})")
        return
    await update_setting(chat_id, "threshold_qty", value)
    await update.message.reply_text(f"✅ Threshold set to {fmt_qty(value)} {asset}")


async def set_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /range 3000  (± price points around current price)")
        return
    try:
        value = float(context.args[0])
        if value <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Please provide a positive number, e.g. /range 3000")
        return
    await update_setting(chat_id, "range_abs", value)
    await update.message.reply_text(f"✅ Range set to ±{value:,.0f} price points")


async def set_time_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /time 7:00pm  (daily report, Bangkok time)")
        return
    raw = " ".join(context.args)
    try:
        hour, minute = parse_12h_time(raw)
    except ValueError:
        await update.message.reply_text("Couldn't read that time. Try something like /time 7:00pm or /time 9:30am")
        return
    await update_setting(chat_id, "daily_time", f"{hour:02d}:{minute:02d}")
    schedule_daily_job(context.job_queue, chat_id, hour, minute)
    await update.message.reply_text(
        f"✅ Daily report set for *{format_12h(hour, minute)}* (Bangkok time).",
        parse_mode="Markdown",
        reply_markup=timeoff_keyboard(),
    )


async def timeoff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    remove_daily_job(context.job_queue, chat_id)
    await update_setting(chat_id, "daily_time", None)
    await update.message.reply_text("✅ Daily report turned off.")


async def alert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Live alerts:", reply_markup=alert_keyboard())
        return
    if context.args[0].lower() not in ("on", "off"):
        await update.message.reply_text("Usage: /alert on  or  /alert off")
        return
    await _apply_alert_choice(chat_id, context.args[0].lower() == "on", context.job_queue, update.message.reply_text)


async def _apply_alert_choice(chat_id: int, turn_on: bool, job_queue, responder):
    await update_setting(chat_id, "alert_on", turn_on)
    if turn_on:
        ensure_alert_job(job_queue, chat_id)
        await responder(
            "✅ Live alerts *on* — you'll get pinged when a new wall crosses your threshold.\n"
            "(Baselining current walls now, so you won't get alerted for what's already there.)",
            parse_mode="Markdown",
        )
    else:
        remove_alert_job(job_queue, chat_id)
        await responder("✅ Live alerts *off*.", parse_mode="Markdown")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = await _do_status(chat_id)
    await update.message.reply_text(text, parse_mode="Markdown")


async def liq_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = await _do_liq(chat_id)
    await update.message.reply_text(text, parse_mode="Markdown")


async def ta_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("⏳ Crunching technicals...")
    text = await _do_ta(chat_id)
    await msg.edit_text(text, parse_mode="Markdown")


async def signal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if context.args and context.args[0].lower() in ("1h", "4h", "8h"):
        msg = await update.message.reply_text("⏳ Reading structure...")
        text = await _do_signal(chat_id, context.args[0].lower())
        await msg.edit_text(text, parse_mode="Markdown")
        return
    await update.message.reply_text("Pick a timeframe:", reply_markup=signal_interval_keyboard())


async def receive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("⏳ Reading live order book...")
    try:
        report = await _do_receive(chat_id, show_age=False)
    except Exception as e:
        log.exception("receive failed")
        await msg.edit_text(f"❌ Error: {e}")
        return
    await msg.edit_text(report, parse_mode="Markdown", reply_markup=action_keyboard())


async def get_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("⏳ Reading live order book...")
    try:
        report = await _do_receive(chat_id, show_age=True)
    except Exception as e:
        log.exception("get failed")
        await msg.edit_text(f"❌ Error: {e}")
        return
    await msg.edit_text(report, parse_mode="Markdown", reply_markup=action_keyboard())


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data = query.data

    try:
        if data in ("receive", "get"):
            report = await _do_receive(chat_id, show_age=(data == "get"))
            await query.message.reply_text(report, parse_mode="Markdown", reply_markup=action_keyboard())

        elif data == "status":
            text = await _do_status(chat_id)
            await query.message.reply_text(text, parse_mode="Markdown")

        elif data == "help":
            await query.message.reply_text(HELP_TEXT, parse_mode="Markdown")

        elif data == "timeoff":
            remove_daily_job(context.job_queue, chat_id)
            await update_setting(chat_id, "daily_time", None)
            await query.message.reply_text("✅ Daily report turned off.")

        elif data.startswith("alert:"):
            turn_on = data.split(":", 1)[1] == "on"
            await _apply_alert_choice(chat_id, turn_on, context.job_queue, query.message.reply_text)

        elif data == "symbol:others":
            await query.message.reply_text("Type /symbol SYMBOL to set a custom pair, e.g. /symbol ADAUSDT")

        elif data.startswith("symbol:"):
            symbol = data.split(":", 1)[1]
            await _apply_symbol(chat_id, symbol, query.message.reply_text)

        elif data == "ta":
            msg = await query.message.reply_text("⏳ Crunching technicals...")
            text = await _do_ta(chat_id)
            await msg.edit_text(text, parse_mode="Markdown")

        elif data == "signal":
            await query.message.reply_text("Pick a timeframe:", reply_markup=signal_interval_keyboard())

        elif data.startswith("signal:"):
            interval = data.split(":", 1)[1]
            msg = await query.message.reply_text("⏳ Reading structure...")
            text = await _do_signal(chat_id, interval)
            await msg.edit_text(text, parse_mode="Markdown")

    except Exception as e:
        log.exception("button callback failed")
        await query.message.reply_text(f"❌ Error: {e}")


# ---------------------------------------------------------------------------
# Startup: restore any scheduled jobs from persisted settings
# ---------------------------------------------------------------------------
async def on_startup(application: Application):
    all_settings = _load_all()
    for chat_id_str, raw in all_settings.items():
        chat_id = int(chat_id_str)
        s = {**DEFAULTS, **raw}
        if s.get("daily_time"):
            hour, minute = map(int, s["daily_time"].split(":"))
            schedule_daily_job(application.job_queue, chat_id, hour, minute)
        if s.get("alert_on"):
            ensure_alert_job(application.job_queue, chat_id)


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN environment variable first.")

    app = Application.builder().token(token).post_init(on_startup).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("a", start))
    app.add_handler(CommandHandler("s", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("symbol", set_symbol))
    app.add_handler(CommandHandler("threshold", set_threshold))
    app.add_handler(CommandHandler("range", set_range))
    app.add_handler(CommandHandler("time", set_time_cmd))
    app.add_handler(CommandHandler("timeoff", timeoff_cmd))
    app.add_handler(CommandHandler("alert", alert_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("liq", liq_cmd))
    app.add_handler(CommandHandler("ta", ta_cmd))
    app.add_handler(CommandHandler("signal", signal_cmd))
    app.add_handler(CommandHandler("receive", receive_command))
    app.add_handler(CommandHandler("get", get_command))
    app.add_handler(CallbackQueryHandler(
        button_callback,
        pattern="^(receive|get|status|help|timeoff|alert:.*|symbol:.*|ta|signal|signal:.*)$",
    ))

    log.info("Bot starting (polling)...")
    app.run_polling()


if __name__ == "__main__":
    main()
