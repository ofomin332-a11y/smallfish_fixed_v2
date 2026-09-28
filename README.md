# Smallfish Public Signal Mode v9
Railway-ready, MEXC public futures data only. No MEXC API key/secret and no orders.

Pattern: liquidity sweep -> 10m CHoCH -> fresh bias -> EARLY setup -> 5m/1m trigger -> final signal.

V9 adds an EARLY SETUP Telegram alert when a fresh sweep+CHoCH is detected, instead of waiting until price is already several ATR away. Final SIGNAL still requires 5m + 1m confirmation and location filters.
