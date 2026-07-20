# Binance Whale Wall Telegram Bot

Watches the Binance **USDT-M Futures** order book and, on demand, reports
large resting buy/sell orders ("walls") near the current price.

## 1. Create your Telegram bot
1. Open Telegram, message **@BotFather**
2. Send `/newbot`, follow the prompts
3. Copy the token it gives you (looks like `123456789:AAExample...`)

## 2. Install & run

```bash
cd binance-wall-bot
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

export TELEGRAM_BOT_TOKEN="paste-your-token-here"   # Windows: set TELEGRAM_BOT_TOKEN=...
python main.py
```

The bot uses Binance's **public** REST endpoints only — no Binance API key needed.

## 3. Use it
In Telegram, open a chat with your bot and send `/start`.

- `/symbol BTCUSDT` — set which pair to watch (any USDT-M Futures symbol)
- `/threshold 100000` — minimum order size in USD to count as "big" (default 100,000)
- `/range 3` — only show orders within ±3% of the current price (default 3)
- `/receive` or the **📥 Receive** button — get a fresh snapshot

Settings are saved per chat in `user_settings.json` next to `main.py`, so they
persist across restarts.

## Notes / things you may want to extend
- This is **on-demand (pull)**, not a live stream — every click/command fetches
  a fresh order-book snapshot from Binance. Simple and reliable, but it won't
  alert you automatically when a new wall appears.
- If you want **continuous auto-alerts** instead (bot pings you the moment a
  wall crosses your threshold, without clicking anything), that needs a
  background task using Binance's websocket depth stream (`wss://fstream.binance.com`)
  plus a running loop that diffs order-book state — happy to build that next
  if it's what you actually want.
- Order book snapshots are limited to the top 1000 levels per side (Binance's
  max for this endpoint), which comfortably covers realistic ± % ranges for
  major pairs.
- Deploy anywhere that can run a long-lived Python process (a small VPS,
  Railway, Fly.io, a Raspberry Pi, etc.) — `python main.py` just needs to keep
  running.
