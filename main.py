"""
Binance Big Order (Whale Wall) Telegram Bot
--------------------------------------------
Watches the USDT-M Futures order book for a symbol and, on demand
(via a "Receive" button or /receive command), reports resting
buy/sell orders ("walls") above a user-defined USD size, within a
user-defined percentage range of the current price.

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
    "threshold_usd": 100_000,   # min notional value (price * qty) to count as "big"
    "range_pct": 3.0,           # only show orders within +/- this % of current price
    "top_n": 10,                # max walls to show per side
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


async def get_big_walls(symbol: str, threshold_usd: float, range_pct: float, top_n: int):
    """Returns (current_price, buy_walls, sell_walls).
    Each wall is (price: float, qty: float, notional: float)."""
    async with httpx.AsyncClient(timeout=10) as client:
        price, (bids, asks) = await asyncio.gather(
            fetch_price(client, symbol), fetch_order_book(client, symbol)
        )

    lower_bound = price * (1 - range_pct / 100)
    upper_bound = price * (1 + range_pct / 100)

    def sift(levels):
        walls = []
        for p_str, q_str in levels:
            p, q = float(p_str), float(q_str)
            if not (lower_bound <= p <= upper_bound):
                continue
            notional = p * q
            if notional >= threshold_usd:
                walls.append((p, q, notional))
        walls.sort(key=lambda w: w[2], reverse=True)
        return walls[:top_n]

    buy_walls = sift(bids)
    sell_walls = sift(asks)
    return price, buy_walls, sell_walls


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------
def fmt_usd(n: float) -> str:
    if n >= 1_000_000:
        return f"${n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n/1_000:.1f}K"
    return f"${n:.0f}"


def build_report(symbol, price, buy_walls, sell_walls, threshold_usd, range_pct) -> str:
    lines = [
        f"📊 *{symbol}* big-order scan",
        f"Current price: `{price:,.2f}`",
        f"Threshold: ≥ {fmt_usd(threshold_usd)}  |  Range: ±{range_pct}%",
        "",
    ]

    lines.append("🟢 *Buy walls (bids)*")
    if buy_walls:
        for p, q, notional in buy_walls:
            lines.append(f"  `{p:,.2f}`  qty `{q:,.4f}`  ≈ {fmt_usd(notional)}")
    else:
        lines.append("  none found")

    lines.append("")
    lines.append("🔴 *Sell walls (asks)*")
    if sell_walls:
        for p, q, notional in sell_walls:
            lines.append(f"  `{p:,.2f}`  qty `{q:,.4f}`  ≈ {fmt_usd(notional)}")
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
    text = (
        "👋 *Binance Whale Wall Bot* (USDT-M Futures)\n\n"
        f"Current settings:\n"
        f"  Symbol: `{s['symbol']}`\n"
        f"  Threshold: {fmt_usd(s['threshold_usd'])}\n"
        f"  Range: ±{s['range_pct']}%\n\n"
        "Commands:\n"
        "  /symbol BTCUSDT — set the pair\n"
        "  /threshold 100000 — set min order size in USD\n"
        "  /range 3 — set % range around current price\n"
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
    # validate against Binance
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
    if not context.args:
        await update.message.reply_text("Usage: /threshold 100000  (min USD order size)")
        return
    try:
        value = float(context.args[0])
        if value <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Please provide a positive number, e.g. /threshold 100000")
        return
    await update_setting(chat_id, "threshold_usd", value)
    await update.message.reply_text(f"✅ Threshold set to {fmt_usd(value)}")


async def set_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: /range 3  (percent around current price)")
        return
    try:
        value = float(context.args[0])
        if not (0 < value <= 50):
            raise ValueError
    except ValueError:
        await update.message.reply_text("Please provide a percent between 0 and 50, e.g. /range 3")
        return
    await update_setting(chat_id, "range_pct", value)
    await update.message.reply_text(f"✅ Range set to ±{value}%")


async def _do_receive(chat_id: int):
    s = await get_settings(chat_id)
    price, buy_walls, sell_walls = await get_big_walls(
        s["symbol"], s["threshold_usd"], s["range_pct"], s["top_n"]
    )
    return build_report(s["symbol"], price, buy_walls, sell_walls, s["threshold_usd"], s["range_pct"])


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
