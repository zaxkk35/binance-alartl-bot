"""
Binance Big Order (Whale Wall) Telegram Bot
--------------------------------------------
Watches the USDT-M Futures order book for a symbol and, on demand
(via a "Receive" button or /receive command), reports resting
buy/sell orders ("walls") above a user-defined size (in base-asset
units, e.g. BTC), within a fixed price-point range of the current
price. Nearby price levels are clustered into a single wall so you
see real sitting orders instead of every raw order-book tick.

Run:
    pip install -r requirements.txt
    export TELEGRAM_BOT_TOKEN="123456:ABC..."
    python main.py
"""

import asyncio
import json
import logging
import os
from pathlib import Path

import httpx
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
_lock = asyncio.Lock()


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
    async with _lock:
        all_settings = _load_all()
        s = all_settings.get(str(chat_id), {})
        merged = {**DEFAULTS, **s}
        return merged


async def update_setting(chat_id: int, key: str, value) -> None:
    async with _lock:
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
# Binance data fetching
# ---------------------------------------------------------------------------
async def fetch_price(client: httpx.AsyncClient, symbol: str) -> float:
    r = await client.get(
        f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/price", params={"symbol": symbol}
    )
    r.raise_for_status()
    return float(r.json()["price"])


async def fetch_order_book(client: httpx.AsyncClient, symbol: str, limit: int = 1000):
    r = await client.get(
        f"{BINANCE_FAPI_BASE}/fapi/v1/depth",
        params={"symbol": symbol, "limit": limit},
    )
    r.raise_for_status()
    data = r.json()
    return data["bids"], data["asks"]  # each: list of [price_str, qty_str]


def cluster_levels(levels, lower_bound, upper_bound, cluster_pct, threshold_qty, top_n):
    """Group nearby raw order-book levels into merged 'walls'.

    Levels within `cluster_pct`% of each other in price are summed together,
    so a big resting order that Binance shows split across a few adjacent
    ticks reads as ONE wall instead of several small noisy lines.
    Returns list of (price, total_qty) sorted by size, largest first.
    """
    in_range = []
    for p_str, q_str in levels:
        p, q = float(p_str), float(q_str)
        if lower_bound <= p <= upper_bound and q > 0:
            in_range.append((p, q))
    if not in_range:
        return []

    in_range.sort(key=lambda x: x[0])

    clusters = []  # each: [sum_price_weighted, sum_qty, first_price, last_price]
    for p, q in in_range:
        if clusters:
            last = clusters[-1]
            cluster_ref_price = last[0] / last[1]  # running weighted-avg price
            if abs(p - cluster_ref_price) / cluster_ref_price * 100 <= cluster_pct:
                last[0] += p * q
                last[1] += q
                continue
        clusters.append([p * q, q, p, p])

    walls = []
    for weighted_price_sum, total_qty, _, _ in clusters:
        if total_qty >= threshold_qty:
            avg_price = weighted_price_sum / total_qty
            walls.append((avg_price, total_qty))

    walls.sort(key=lambda w: w[1], reverse=True)
    return walls[:top_n]


async def get_big_walls(symbol: str, threshold_qty: float, range_abs: float, top_n: int, cluster_pct: float):
    """Returns (current_price, buy_walls, sell_walls).
    Each wall is (price: float, qty: float) where qty is in base-asset units."""
    async with httpx.AsyncClient(timeout=10) as client:
        price, (bids, asks) = await asyncio.gather(
            fetch_price(client, symbol), fetch_order_book(client, symbol)
        )

    lower_bound = price - range_abs
    upper_bound = price + range_abs

    buy_walls = cluster_levels(bids, lower_bound, upper_bound, cluster_pct, threshold_qty, top_n)
    sell_walls = cluster_levels(asks, lower_bound, upper_bound, cluster_pct, threshold_qty, top_n)
    return price, buy_walls, sell_walls


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
        f"📊 *{symbol}* wall scan",
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
    text = (
        "👋 *Binance Whale Wall Bot* (USDT-M Futures)\n\n"
        f"Current settings:\n"
        f"  Symbol: `{s['symbol']}`\n"
        f"  Threshold: {fmt_qty(s['threshold_qty'])} {asset}\n"
        f"  Range: ±{s['range_abs']:,.0f} price points\n\n"
        "Commands:\n"
        "  /symbol BTCUSDT — set the pair\n"
        f"  /threshold 5 — min wall size in {asset} (base asset)\n"
        "  /range 3000 — ± price points around current price\n"
        "  /receive — get a snapshot now\n\n"
        "Or just tap the button below any time 👇"
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
            await fetch_price(client, symbol)
        except httpx.HTTPStatusError:
            await update.message.reply_text(f"❌ `{symbol}` not found on Futures.", parse_mode="Markdown")
            return
    await update_setting(chat_id, "symbol", symbol)
    await update.message.reply_text(f"✅ Symbol set to `{symbol}`", parse_mode="Markdown")


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


async def _do_receive(chat_id: int):
    s = await get_settings(chat_id)
    price, buy_walls, sell_walls = await get_big_walls(
        s["symbol"], s["threshold_qty"], s["range_abs"], s["top_n"], s["cluster_pct"]
    )
    return build_report(s["symbol"], price, buy_walls, sell_walls, s["threshold_qty"], s["range_abs"])


async def receive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg = await update.message.reply_text("⏳ Scanning order book...")
    try:
        report = await _do_receive(chat_id)
    except Exception as e:
        log.exception("receive failed")
        await msg.edit_text(f"❌ Error fetching data: {e}")
        return
    await msg.edit_text(report, parse_mode="Markdown", reply_markup=receive_keyboard())


async def receive_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # ack immediately so the button doesn't hang
    chat_id = query.message.chat_id
    try:
        report = await _do_receive(chat_id)
    except Exception as e:
        log.exception("receive failed")
        await query.message.reply_text(f"❌ Error fetching data: {e}")
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
