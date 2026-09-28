# Smallfish Public Signal Mode v6

Railway-ready MEXC Futures signal scanner. Public market data only: no MEXC API key/secret and no order placement.

## V6 logic
The scanner is designed around the pattern visible in the reference LINK setup:

**Liquidity sweep → 10m CHoCH → 1H/15m bias → 5m confirmation → 1m trigger**

### LONG
- 1H and 15m must support bullish bias.
- A fresh **sell-side liquidity sweep** must be detected on derived 10m candles: price takes a recent low and closes back above it.
- After the sweep, a **10m CHoCH** must occur.
- 5m must confirm bullish momentum without requiring an already exhausted move.
- 1m must show a fresh reclaim/break trigger.
- Order-book imbalance should support bids and volume must not be dead.
- Entry is rejected if price is already too far above the sweep or too close to the recent high.

### SHORT
Mirror logic:
- 1H/15m bearish bias.
- Buy-side liquidity sweep.
- 10m bearish CHoCH.
- 5m bearish confirmation.
- 1m rejection/break trigger.
- Ask-side order-book imbalance and usable volume.
- Reject late entries near the recent low or too far below the sweep.

## Important
This is a pattern scanner, not a guarantee of profit. It tries to detect the same *sequence* of market events rather than simply scoring trend indicators after a move has already happened.

## Railway
Docker runs `python app.py`. Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in Railway. No MEXC credentials are required.
