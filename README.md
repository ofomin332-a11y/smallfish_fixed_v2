# Smallfish — Public Signal Mode

Signal-only deployment for Railway.

- Uses MEXC public market data.
- No MEXC API key or secret is required.
- Does not place, cancel, or manage orders.
- Sends qualifying signals to Telegram.
- Default symbols: SOLUSDT, SUIUSDT, XRPUSDT, DOGEUSDT, ADAUSDT.
- Multi-timeframe inputs: 1h, 15m, 5m, 1m.
- Default polling: 30 seconds.

Railway variables:
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID

Optional:
SYMBOLS
POLL_SECONDS
MIN_SCORE
ALERT_COOLDOWN_SECONDS
