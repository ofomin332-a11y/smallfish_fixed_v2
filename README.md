# Smallfish Public Signal V8

Railway-ready, signal-only MEXC Futures scanner. No MEXC API key/secret and no orders.

Pattern: liquidity sweep -> 10m CHoCH -> fresh 1H/15m bias -> 5m confirmation -> 1m trigger -> entry zone.

V8 fixes:
- never labels an extended setup as LONG/SHORT when location fails; it is WAIT;
- uses an entry zone instead of treating the current market price as the only entry;
- separates fresh setup age from older structural sweeps;
- caches 1m/5m/15m/1h candles and order book to reduce MEXC request pressure;
- retries API calls and uses cached candles on transient empty responses;
- requires fresh sweep, higher-timeframe alignment, 5m confirmation, 1m trigger, order-book and volume confirmation for alerts;
- blocks entries that are too close to local extremes or too far from the sweep.
