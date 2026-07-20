"""
Binance Big Order (Whale Wall) Telegram Bot
--------------------------------------------
Maintains a LIVE local order book per watched symbol via Binance's
USDT-M Futures diff-depth websocket stream on top of an initial REST
snapshot (Binance's official local order book procedure). Reports
clustered buy/sell walls above a user-defined size, within a fixed
price-point range of the current price -- on demand, on a daily
schedule, and/or as live push alerts when a new big wall appears.

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

CLUSTER_PRESETS = {"low": 0.02, "mid": 0.05, "high": 0.15}

DEFAULTS = {
    "symbol": "BTCUSDT",
    "threshold_qty": 5.0,     # min resting size, in base-asset units (e.g. BTC), to count as a wall
    "range_abs": 3000.0,      # only show orders within +/- this many price points of current price
    "top_n": 5,               # max walls to show per side
    "cluster_level": "mid",   # low / mid / high -> CLUSTER_PRESETS
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
# Wall detection (clustering) -- works on live book snapshot
# ---------------------------------------------------------------------------
def cluster_levels(items, first_seen_map, lower_bound, upper_bound, cluster_pct, threshold_qty, top_n):
    """items: iterable of (price, qty) floats.
    Groups levels within `cluster_pct`% of each other into one merged wall.
    Returns list of (avg_price, total_qty, oldest_first_seen_ts), largest first."""
    now = time.time()
    in_range = [(p, q) for p, q in items if lower_bound <= p <= upper_bound and q > 0]
    if not in_range:
        return []
    in_range.sort(key=lambda x: x[0])

    clusters = []  # [weighted_price_sum, total_qty, oldest_ts]
    for p, q in in_range:
        ts = first_seen_map.get(p, now)
        if clusters:
            last = clusters[-1]
            cluster_ref_price = last[0] / last[1]
            if abs(p - cluster_ref_price) / cluster_ref_price * 100 <= cluster_pct:
                last[0] += p * q
                last[1] += q
                last[2] = min(last[2], ts)
                continue
        clusters.append([p * q, q, ts])

    walls = []
    for weighted_price_sum, total_qty, oldest_ts in clusters:
        if total_qty >= threshold_qty:
            walls.append((weighted_price_sum / total_qty, total_qty, oldest_ts))

    walls.sort(key=lambda w: w[1], reverse=True)
    return walls[:top_n]


async def get_big_walls(symbol: str, threshold_qty: float, range_abs: float, top_n: int, cluster_pct: float):
    """Returns (ready, mid_price, buy_walls, sell_walls).
    Each wall is (price, qty, first_seen_ts)."""
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

    buy_walls = cluster_levels(bids.items(), bid_fs, lower_bound, upper_bound, cluster_pct, threshold_qty, top_n)
    sell_walls = cluster_levels(asks.items(), ask_fs, lower_bound, upper_bound, cluster_pct, threshold_qty, top_n)
    return True, mid_price, buy_walls, sell_walls


def zone_key(price: float, mid_price: float, cluster_pct: float) -> int:
    width = max(mid_price * cluster_pct / 100 * 2, 0.0001)
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
        f"📊 *{symbol}* live wall scan",
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

    lines.append(f"────  `{price:,.2f}`  (current price)  ────")

    lines.append("🟢 *Buy walls*")
    if buy_walls:
        for p, q, ts in sorted(buy_walls, key=lambda w: w[0], reverse=True):
            lines.append(wall_line(p, q, ts))
    else:
        lines.append("  none found")

    return "\n".join(lines)


def action_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("📥 Receive", callback_data="receive"),
            InlineKeyboardButton("🕒 Get", callback_data="get"),
        ]]
    )


# ---------------------------------------------------------------------------
# Core fetch used by /receive, /get, scheduled reports
# ---------------------------------------------------------------------------
async def _do_receive(chat_id: int, show_age: bool = False) -> str:
    s = await get_settings(chat_id)
    cluster_pct = CLUSTER_PRESETS.get(s["cluster_level"], CLUSTER_PRESETS["mid"])
    ready, price, buy_walls, sell_walls = await get_big_walls(
        s["symbol"], s["threshold_qty"], s["range_abs"], s["top_n"], cluster_pct
    )
    if not ready:
        return (
            f"⏳ Still syncing the *{s['symbol']}* order book — try again in a few seconds.\n"
            "(This only happens right after the bot starts or the symbol changes.)"
        )
    return build_report(s["symbol"], price, buy_walls, sell_walls, s["threshold_qty"], s["range_abs"], show_age)


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
    cluster_pct = CLUSTER_PRESETS.get(s["cluster_level"], CLUSTER_PRESETS["mid"])
    ready, price, buy_walls, sell_walls = await get_big_walls(
        s["symbol"], s["threshold_qty"], s["range_abs"], s["top_n"], cluster_pct
    )
    if not ready:
        return

    current = {}
    for p, q, ts in buy_walls:
        current[("buy", zone_key(p, price, cluster_pct))] = (p, q)
    for p, q, ts in sell_walls:
        current[("sell", zone_key(p, price, cluster_pct))] = (p, q)

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
        text, parse_mode="Markdown", reply_markup=action_keyboard()
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "*Commands*\n"
        "/symbol SYM — set trading pair (e.g. BTCUSDT)\n"
        "/threshold N — min wall size, in base asset\n"
        "/range N — ± price points around current price\n"
        "/cluster low|mid|high — wall grouping tightness\n"
        "/receive — snapshot now\n"
        "/get — snapshot now, with wall age\n"
        "/time 7:00pm — daily auto report (Bangkok time)\n"
        "/timeoff — turn off daily report\n"
        "/alert on|off — live push when a new big wall appears\n"
        "/status — live order book sync status"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def set_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /symbol BTCUSDT")
        return
    symbol = context.args[0].upper()
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/price", params={"symbol": symbol})
            r.raise_for_status()
        except httpx.HTTPStatusError:
            await update.message.reply_text(f"❌ `{symbol}` not found on Futures.", parse_mode="Markdown")
            return
    await update_setting(chat_id, "symbol", symbol)
    await book_manager.ensure(symbol)
    await update.message.reply_text(
        f"✅ Symbol set to `{symbol}` — syncing its order book now, give it a few seconds.",
        parse_mode="Markdown",
    )


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


async def set_cluster(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args or context.args[0].lower() not in CLUSTER_PRESETS:
        await update.message.reply_text("Usage: /cluster low | mid | high")
        return
    level = context.args[0].lower()
    await update_setting(chat_id, "cluster_level", level)
    await update.message.reply_text(f"✅ Cluster tightness set to *{level}*", parse_mode="Markdown")


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
        f"✅ Daily report set for *{format_12h(hour, minute)}* (Bangkok time). Use /timeoff to disable.",
        parse_mode="Markdown",
    )


async def timeoff_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    remove_daily_job(context.job_queue, chat_id)
    await update_setting(chat_id, "daily_time", None)
    await update.message.reply_text("✅ Daily report turned off.")


async def alert_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args or context.args[0].lower() not in ("on", "off"):
        await update.message.reply_text("Usage: /alert on  or  /alert off")
        return
    turn_on = context.args[0].lower() == "on"
    await update_setting(chat_id, "alert_on", turn_on)
    if turn_on:
        ensure_alert_job(context.job_queue, chat_id)
        await update.message.reply_text(
            "✅ Live alerts *on* — you'll get pinged when a new wall crosses your threshold.\n"
            "(Baselining current walls now, so you won't get alerted for what's already there.)",
            parse_mode="Markdown",
        )
    else:
        remove_alert_job(context.job_queue, chat_id)
        await update.message.reply_text("✅ Live alerts *off*.", parse_mode="Markdown")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s = await get_settings(chat_id)
    book = await book_manager.ensure(s["symbol"])
    if not book.ready.is_set():
        await update.message.reply_text(f"⏳ `{s['symbol']}` order book is still syncing...", parse_mode="Markdown")
        return
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
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


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
    show_age = query.data == "get"
    try:
        report = await _do_receive(chat_id, show_age=show_age)
    except Exception as e:
        log.exception("callback receive failed")
        await query.message.reply_text(f"❌ Error: {e}")
        return
    await query.message.reply_text(
        report, parse_mode="Markdown", reply_markup=action_keyboard()
    )


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
    app.add_handler(CommandHandler("s", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("symbol", set_symbol))
    app.add_handler(CommandHandler("threshold", set_threshold))
    app.add_handler(CommandHandler("range", set_range))
    app.add_handler(CommandHandler("cluster", set_cluster))
    app.add_handler(CommandHandler("time", set_time_cmd))
    app.add_handler(CommandHandler("timeoff", timeoff_cmd))
    app.add_handler(CommandHandler("alert", alert_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("receive", receive_command))
    app.add_handler(CommandHandler("get", get_command))
    app.add_handler(CallbackQueryHandler(button_callback, pattern="^(receive|get)$"))

    log.info("Bot starting (polling)...")
    app.run_polling()


if __name__ == "__main__":
    main()
