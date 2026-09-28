# Smallfish Public Signal Mode v5

Railway-ready, signal-only MEXC Futures scanner. It uses public market data only: no MEXC API key/secret and no order placement.

## v5 entry logic
- 1H direction establishes the main bias.
- 15m structure confirms the bias.
- 10m is now a mandatory entry-timeframe filter.
- 10m requires bearish breakdown/retest for SHORT or bullish breakout/retest for LONG; fresh breaks are accepted only when not excessively extended.
- Strong opposite 10m reversal candles veto the old direction.
- 5m momentum/RSI/volume/order-book provide confirmation.
- SHORT entries are blocked near the recent 5m LOW or when there is insufficient downside room.
- LONG entries are blocked near the recent 5m HIGH or when there is insufficient upside room.
- Entries too far from 5m EMA21 in ATR are blocked.
- At least two independent entry confirmations are required.
- 1m pullback -> break trigger is still required for timing.
- Entry price is taken from the latest CLOSED 1m candle, not the older 5m close.
- Stops are structural, based on the 10m broken level plus an ATR buffer, rather than a blind fixed percentage.
- All timeframe calculations use CLOSED candles; the currently forming candle is ignored.
- The 10m candles are safely derived from closed 5m candles, so the scanner does not depend on an undocumented 10m API interval.
- Every scan logs a WAIT reason for every symbol.

## MEXC
The Futures public API domain is `https://api.mexc.com`. No account credentials are used.

## Railway
No custom start command is required. Docker runs `python app.py`.

Recommended Railway Variables:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `SYMBOLS`
- `POLL_SECONDS=30`
- `MIN_SCORE=6`
- `MIN_ENTRY_TRIGGERS=2`
- `DIAGNOSTICS=true`
