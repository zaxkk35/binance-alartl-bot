"""
Binance Big Order (Whale Wall) Telegram Bot
--------------------------------------------
Maintains a LIVE local order book per watched symbol by syncing
Binance's USDT-M Futures diff-depth websocket stream on top of an
initial REST snapshot (Binance's official local order book
procedure). On demand (via a "Receive" button or /receive command)
it reads that in-memory book instantly and reports clustered
buy/sell walls above a user-defined size, within a fixed price-point
range of the current price.

Why a live book instead of one-shot REST calls:
Binance's REST depth endpoint only returns the ~1000 price levels
nearest the current price. A live book has no such ceiling -- as
normal market activity generates updates across the full book over
time, deep walls far from price get picked up too.

Run:
    pip install -r requirements.txt
    export TELEGRAM_BOT_TOKEN="123456:ABC..."
    python main.py
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path

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

DEFAULTS = {
    "symbol": "BTCUSDT",
    "threshold_qty": 5.0,     # min resting size, in base-asset units (e.g. BTC), to count as a wall
    "range_abs": 3000.0,      # only show orders within +/- this many price points of current price
    "top_n": 5,               # max walls to show per side
    "cluster_pct": 0.05,      # merge price levels within this % of each other into one wall
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
# Live local order book (Binance's official sync procedure)
# https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-market-streams
# ---------------------------------------------------------------------------
class LiveOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None
        self.ready = asyncio.Event()
        self.lock = asyncio.Lock()
        self.task: asyncio.Task | None = None
        self.last_message_ts: float = 0.0

    async def snapshot(self):
        """Returns (best_bid, best_ask, bids_copy, asks_copy)."""
        async with self.lock:
            if not self.bids or not self.asks:
                return None, None, {}, {}
            best_bid = max(self.bids)
            best_ask = min(self.asks)
            return best_bid, best_ask, dict(self.bids), dict(self.asks)

    async def _apply_snapshot(self, data: dict):
        async with self.lock:
            self.bids = {float(p): float(q) for p, q in data["bids"] if float(q) > 0}
            self.asks = {float(p): float(q) for p, q in data["asks"] if float(q) > 0}
            self.last_update_id = data["lastUpdateId"]

    async def _apply_diff(self, event: dict):
        async with self.lock:
            for p_str, q_str in event["b"]:
                p, q = float(p_str), float(q_str)
                if q == 0:
                    self.bids.pop(p, None)
                else:
                    self.bids[p] = q
            for p_str, q_str in event["a"]:
                p, q = float(p_str), float(q_str)
                if q == 0:
                    self.asks.pop(p, None)
                else:
                    self.asks[p] = q
            self.last_update_id = event["u"]
        self.last_message_ts = time.time()

    async def run(self):
        """Background loop: connect, sync, apply diffs forever (with reconnect/backoff)."""
        backoff = 1
        stream_name = f"{self.symbol.lower()}@depth@500ms"
        url = f"{BINANCE_WS_BASE}?streams={stream_name}"
        while True:
            try:
                self.ready.clear()
                buffered = []
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    log.info("ws connected for %s", self.symbol)

                    # 1. buffer a few events first so we don't miss the gap right after snapshot fetch
                    async def recv_one():
                        raw = await ws.recv()
                        msg = json.loads(raw)
                        return msg["data"]

                    first_event = await recv_one()
                    buffered.append(first_event)

                    # 2. fetch REST snapshot
                    async with httpx.AsyncClient(timeout=10) as client:
                        r = await client.get(
                            f"{BINANCE_FAPI_BASE}/fapi/v1/depth",
                            params={"symbol": self.symbol, "limit": 1000},
                        )
                        r.raise_for_status()
                        snap = r.json()
                    await self._apply_snapshot(snap)
                    last_applied_u = self.last_update_id

                    # 3. drop/validate buffered events against snapshot, then apply forward
                    started = False
                    async def process(event):
                        nonlocal started, last_applied_u
                        if event["u"] <= last_applied_u:
                            return  # stale, drop
                        if not started:
                            # first relevant event must straddle the snapshot's lastUpdateId
                            if event["U"] > last_applied_u + 1:
                                raise RuntimeError("gap between snapshot and stream, resyncing")
                            started = True
                        else:
                            if event.get("pu") != last_applied_u:
                                raise RuntimeError("sequence gap detected, resyncing")
                        await self._apply_diff(event)
                        last_applied_u = event["u"]

                    for ev in buffered:
                        await process(ev)

                    self.ready.set()
                    backoff = 1
                    log.info("%s order book synced (lastUpdateId=%s)", self.symbol, last_applied_u)

                    # 4. steady state: keep applying live diffs
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
def cluster_levels(level_items, lower_bound, upper_bound, cluster_pct, threshold_qty, top_n):
    """level_items: iterable of (price, qty) floats.
    Groups levels within `cluster_pct`% of each other into one merged wall."""
    in_range = [(p, q) for p, q in level_items if lower_bound <= p <= upper_bound and q > 0]
    if not in_range:
        return []

    in_range.sort(key=lambda x: x[0])

    clusters = []  # each: [weighted_price_sum, total_qty]
    for p, q in in_range:
        if clusters:
            last = clusters[-1]
            cluster_ref_price = last[0] / last[1]
            if abs(p - cluster_ref_price) / cluster_ref_price * 100 <= cluster_pct:
                last[0] += p * q
                last[1] += q
                continue
        clusters.append([p * q, q])

    walls = []
    for weighted_price_sum, total_qty in clusters:
        if total_qty >= threshold_qty:
            walls.append((weighted_price_sum / total_qty, total_qty))

    walls.sort(key=lambda w: w[1], reverse=True)
    return walls[:top_n]


async def get_big_walls(symbol: str, threshold_qty: float, range_abs: float, top_n: int, cluster_pct: float):
    """Returns (ready, mid_price, buy_walls, sell_walls)."""
    book = await book_manager.ensure(symbol)
    try:
        await asyncio.wait_for(book.ready.wait(), timeout=8)
    except asyncio.TimeoutError:
        return False, None, [], []

    best_bid, best_ask, bids, asks = await book.snapshot()
    if best_bid is None:
        return False, None, [], []

    mid_price = (best_bid + best_ask) / 2
    lower_bound = mid_price - range_abs
    upper_bound = mid_price + range_abs

    buy_walls = cluster_levels(bids.items(), lower_bound, upper_bound, cluster_pct, threshold_qty, top_n)
    sell_walls = cluster_levels(asks.items(), lower_bound, upper_bound, cluster_pct, threshold_qty, top_n)
    return True, mid_price, buy_walls, sell_walls


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def fmt_qty(n: float) -> str:
    if n >= 1000:
        return f"{n/1000:.2f}K"
    return f"{n:.3f}"


def build_report(symbol, price, buy_walls, sell_walls, threshold_qty, range_abs) -> str:
    asset = base_asset(symbol)
    lines = [
        f"📊 *{symbol}* wall scan (live book)",
        f"Current price: `{price:,.2f}`",
        f"Threshold: ≥ {fmt_qty(threshold_qty)} {asset}  |  Range: `{price - range_abs:,.0f}` – `{price + range_abs:,.0f}`",
        "",
    ]

    lines.append("🟢 *Buy walls (bids)*")
    if buy_walls:
        for p, q in buy_walls:
            lines.append(f"  `{p:,.2f}`  →  {fmt_qty(q)} {asset}")
    else:
        lines.append("  none found")

    lines.append("")
    lines.append("🔴 *Sell walls (asks)*")
    if sell_walls:
        for p, q in sell_walls:
            lines.append(f"  `{p:,.2f}`  →  {fmt_qty(q)} {asset}")
    else:
        lines.append("  none found")

    return "\n".join(lines)


def receive_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("📥 Receive", callback_data="receive")]]
    )


# ---------------------------------------------------------------------------
# Telegram handlers
# ---------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s = await get_settings(chat_id)
    asset = base_asset(s["symbol"])
    # warm up the book right away so /receive is fast later
    await book_manager.ensure(s["symbol"])
    text = (
        "👋 *Binance Whale Wall Bot* (USDT-M Futures, live order book)\n\n"
        f"Current settings:\n"
        f"  Symbol: `{s['symbol']}`\n"
        f"  Threshold: {fmt_qty(s['threshold_qty'])} {asset}\n"
        f"  Range: ±{s['range_abs']:,.0f} price points\n\n"
        "Commands:\n"
        "  /symbol BTCUSDT — set the pair\n"
        f"  /threshold 5 — min wall size in {asset} (base asset)\n"
        "  /range 3000 — ± price points around current price\n"
        "  /receive — get a snapshot now\n\n"
        "Or just tap the button below any time 👇\n\n"
        "_Note: right after the bot (re)starts, the book takes a few seconds "
        "to sync, and a bit longer to fill in on deep price levels far from "
        "the current price._"
    )
    await update.message.reply_text(
        text, parse_mode="Markdown", reply_markup=receive_keyboard()
    )


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
    await book_manager.ensure(symbol)  # start warming the book immediately
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


async def _do_receive(chat_id: int) -> str:
    s = await get_settings(chat_id)
    ready, price, buy_walls, sell_walls = await get_big_walls(
        s["symbol"], s["threshold_qty"], s["range_abs"], s["top_n"], s["cluster_pct"]
    )
    if not ready:
        return (
            f"⏳ Still syncing the *{s['symbol']}* order book — try again in a few seconds.\n"
            "(This only happens right after the bot starts or the symbol changes.)"
        )
    return build_report(s["symbol"], price, buy_walls, sell_walls, s["threshold_qty"], s["range_abs"])


async def receive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("⏳ Reading live order book...")
    try:
        report = await _do_receive(chat_id)
    except Exception as e:
        log.exception("receive failed")
        await msg.edit_text(f"❌ Error: {e}")
        return
    await msg.edit_text(report, parse_mode="Markdown", reply_markup=receive_keyboard())


async def receive_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    try:
        report = await _do_receive(chat_id)
    except Exception as e:
        log.exception("receive failed")
        await query.message.reply_text(f"❌ Error: {e}")
        return
    await query.message.reply_text(
        report, parse_mode="Markdown", reply_markup=receive_keyboard()
    )


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN environment variable first.")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("symbol", set_symbol))
    app.add_handler(CommandHandler("threshold", set_threshold))
    app.add_handler(CommandHandler("range", set_range))
    app.add_handler(CommandHandler("receive", receive_command))
    app.add_handler(CallbackQueryHandler(receive_callback, pattern="^receive$"))

    log.info("Bot starting (polling)...")
    app.run_polling()


if __name__ == "__main__":
    main()
