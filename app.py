import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger("smallfish-public")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "SYMBOLS", "SOLUSDT,SUIUSDT,XRPUSDT,DOGEUSDT,ADAUSDT"
).split(",") if s.strip()]
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
MIN_SCORE = float(os.getenv("MIN_SCORE", "6"))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
LOCATION_LOOKBACK = int(os.getenv("LOCATION_LOOKBACK", "36"))
NEAR_EDGE_PCT = float(os.getenv("NEAR_EDGE_PCT", "0.16"))
MAX_EXTENSION_ATR = float(os.getenv("MAX_EXTENSION_ATR", "1.35"))
MIN_ENTRY_TRIGGERS = int(os.getenv("MIN_ENTRY_TRIGGERS", "2"))
DIAGNOSTICS = os.getenv("DIAGNOSTICS", "true").lower() in {"1", "true", "yes", "on"}

# Public MEXC futures data only. No account endpoints and no orders.
MEXC_PUBLIC = "https://api.mexc.com"
MEXC_CONTRACT = "https://api.mexc.com"
FUTURES_KLINE = "/api/v1/contract/kline"
FUTURES_DEPTH = "/api/v1/contract/depth"

last_alert = {}


def ema(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    x = sum(values[:period]) / period
    for v in values[period:]:
        x = v * k + x * (1 - k)
    return x


def rsi(values, period=14):
    if len(values) < period + 1:
        return 50.0
    gains, losses = [], []
    for a, b in zip(values[-period - 1:-1], values[-period:]):
        d = b - a
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains) / period
    al = sum(losses) / period
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def atr(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    trs = []
    prev = candles[-period - 1][4]
    for c in candles[-period:]:
        h, l = c[2], c[3]
        trs.append(max(h - l, abs(h - prev), abs(l - prev)))
        prev = c[4]
    return sum(trs) / len(trs)


def parse_candles(data):
    if isinstance(data, list):
        out = []
        for x in data:
            if isinstance(x, list) and len(x) >= 6:
                out.append([int(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])])
        return out
    if isinstance(data, dict):
        d = data.get("data")
        if isinstance(d, dict):
            ts = d.get("time") or d.get("timestamp")
            opens = d.get("open") or d.get("opens")
            highs = d.get("high") or d.get("highs")
            lows = d.get("low") or d.get("lows")
            closes = d.get("close") or d.get("closes")
            vols = d.get("vol") or d.get("volume") or d.get("volumes")
            if all(isinstance(v, list) for v in [ts, opens, highs, lows, closes, vols]):
                n = min(map(len, [ts, opens, highs, lows, closes, vols]))
                return [[
                    int(ts[i]) * 1000 if int(ts[i]) < 10**12 else int(ts[i]),
                    float(opens[i]), float(highs[i]), float(lows[i]),
                    float(closes[i]), float(vols[i])
                ] for i in range(n)]
        if isinstance(d, list):
            return parse_candles(d)
    return []


async def get_json(session, path, params, base=MEXC_PUBLIC):
    url = base + path
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=12)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} {path}")
        return await r.json(content_type=None)


async def candles(session, symbol, interval):
    contract_interval = {"Min1": "Min1", "Min5": "Min5", "Min15": "Min15", "Hour1": "Min60"}[interval]
    fs = symbol.replace("USDT", "_USDT")
    d = await get_json(session, f"{FUTURES_KLINE}/{fs}", {"interval": contract_interval}, base=MEXC_CONTRACT)
    c = parse_candles(d)
    interval_ms = {"Min1": 60_000, "Min5": 300_000, "Min15": 900_000, "Hour1": 3_600_000}[interval]
    c = closed_candles(c, interval_ms)
    if len(c) < 60:
        raise RuntimeError(f"no usable futures candles for {symbol} {interval}: {len(c)}")
    return c[-180:]


async def depth(session, symbol):
    fs = symbol.replace("USDT", "_USDT")
    d = await get_json(session, f"{FUTURES_DEPTH}/{fs}", {"limit": 20}, base=MEXC_CONTRACT)
    data = d.get("data", d) if isinstance(d, dict) else d
    bids = data.get("bids", []) if isinstance(data, dict) else []
    asks = data.get("asks", []) if isinstance(data, dict) else []
    if not bids or not asks:
        raise RuntimeError(f"empty futures order book for {symbol}")
    return bids, asks


def orderbook_imbalance(bids, asks):
    bidvol = sum(float(x[1]) for x in bids[:10])
    askvol = sum(float(x[1]) for x in asks[:10])
    total = bidvol + askvol
    return (bidvol - askvol) / total if total else 0.0


def closed_candles(candles, interval_ms):
    """Drop the currently forming candle so signals are based on closed bars only."""
    if not candles:
        return candles
    now_ms = int(time.time() * 1000)
    out = [c for c in candles if c[0] + interval_ms <= now_ms]
    return out if len(out) >= 20 else candles[:-1] if len(candles) > 20 else candles


def aggregate_5m_to_10m(c5):
    """Build reliable 10m candles from closed 5m candles; avoids relying on an undocumented interval."""
    if len(c5) < 4:
        return []
    buckets = {}
    for c in c5:
        bucket = (int(c[0]) // 600000) * 600000
        buckets.setdefault(bucket, []).append(c)
    out = []
    for ts in sorted(buckets):
        rows = sorted(buckets[ts], key=lambda x: x[0])
        if len(rows) < 2:
            continue
        out.append([
            ts, rows[0][1], max(x[2] for x in rows), min(x[3] for x in rows),
            rows[-1][4], sum(x[5] for x in rows)
        ])
    return out


def ten_min_confirmation(c10, side):
    """Mandatory 10m structure confirmation: trend + recent break/retest, not a raw impulse."""
    if len(c10) < 12:
        return False, False, "not enough 10m confirmation candles"
    closes = [x[4] for x in c10]
    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    if e9 is None or e21 is None:
        return False, False, "10m EMA unavailable"
    last = c10[-1]
    prev = c10[-2]
    atr10 = atr(c10)
    if atr10 <= 0:
        return False, False, "10m ATR unavailable"

    # The most recent 3 bars define the short-term structure level.
    prior = c10[-6:-3]
    level_low = min(x[3] for x in prior)
    level_high = max(x[2] for x in prior)
    recent = c10[-3:]
    short_break = any(x[4] < level_low for x in recent)
    long_break = any(x[4] > level_high for x in recent)

    # Retest means price interacted with the broken level and then closed back
    # in the direction of the break. This prevents chasing a vertical candle.
    short_retest = short_break and last[2] >= level_low * 0.999 and last[4] < level_low
    long_retest = long_break and last[3] <= level_high * 1.001 and last[4] > level_high

    # A fresh breakdown/breakout is allowed if it is not already excessively
    # extended from the 10m EMA. This keeps the scanner from missing clean breaks.
    fresh_short_break = short_break and last[4] < level_low and (level_low - last[4]) <= atr10 * 0.55
    fresh_long_break = long_break and last[4] > level_high and (last[4] - level_high) <= atr10 * 0.55

    trend_short = last[4] < e9 < e21
    trend_long = last[4] > e9 > e21

    # Strong opposite 10m candle = do not chase the old direction.
    body = abs(last[4] - last[1])
    span = max(last[2] - last[3], last[4] * 1e-8)
    opposite_bull = last[4] > last[1] and body / span >= 0.55 and last[4] > prev[2]
    opposite_bear = last[4] < last[1] and body / span >= 0.55 and last[4] < prev[3]

    if side == "SHORT":
        if opposite_bull:
            return False, False, "10m bullish reversal"
        ok = trend_short and (short_retest or fresh_short_break)
        if ok:
            return True, short_retest, "10m bearish breakdown/retest"
        if not trend_short:
            return False, False, "10m trend not bearish"
        return False, False, "10m waiting for breakdown/retest"
    else:
        if opposite_bear:
            return False, False, "10m bearish reversal"
        ok = trend_long and (long_retest or fresh_long_break)
        if ok:
            return True, long_retest, "10m bullish breakout/retest"
        if not trend_long:
            return False, False, "10m trend not bullish"
        return False, False, "10m waiting for breakout/retest"


def retest_trigger(c1m, side):
    """Approximate pullback -> break trigger from the last 8 one-minute candles."""
    if len(c1m) < 8:
        return False, "not enough 1m trigger candles"
    w = c1m[-8:]
    bodies = [x[4] - x[1] for x in w]
    if side == "SHORT":
        pullback = any(b > 0 for b in bodies[-5:-2])
        trigger = w[-1][4] < min(x[3] for x in w[-3:-1])
        return pullback and trigger, "pullback → bearish break" if pullback and trigger else "waiting for pullback/break"
    pullback = any(b < 0 for b in bodies[-5:-2])
    trigger = w[-1][4] > max(x[2] for x in w[-3:-1])
    return pullback and trigger, "pullback → bullish break" if pullback and trigger else "waiting for pullback/break"


def setup_metrics(c1h, c15, c5, c10, c1m, bids, asks):
    close5 = c5[-1][4]
    ten_ok_long, ten_retest_long, ten_text_long = ten_min_confirmation(c10, "LONG")
    ten_ok_short, ten_retest_short, ten_text_short = ten_min_confirmation(c10, "SHORT")
    e20h = ema([x[4] for x in c1h], 20)
    e50h = ema([x[4] for x in c1h], 50)
    e20_15 = ema([x[4] for x in c15], 20)
    e50_15 = ema([x[4] for x in c15], 50)
    e9_5 = ema([x[4] for x in c5], 9)
    e21_5 = ema([x[4] for x in c5], 21)
    r = rsi([x[4] for x in c5])
    a = atr(c5)
    vols = [x[5] for x in c5[-21:-1]]
    vol_avg = sum(vols) / len(vols) if vols else 0
    vr = c5[-1][5] / vol_avg if vol_avg else 1.0
    obi = orderbook_imbalance(bids, asks)

    look = c5[-LOCATION_LOOKBACK:]
    local_low = min(x[3] for x in look)
    local_high = max(x[2] for x in look)
    rng = max(local_high - local_low, close5 * 1e-8)
    pos = (close5 - local_low) / rng
    room_to_low_pct = (close5 - local_low) / close5 * 100
    room_to_high_pct = (local_high - close5) / close5 * 100
    dist_ema_atr = ((close5 - e21_5) / a) if e21_5 and a else 0.0

    long_score = 0.0
    short_score = 0.0
    long_reasons, short_reasons = [], []

    if e20h and e50h:
        if c1h[-1][4] > e20h > e50h:
            long_score += 2; long_reasons.append("1H trend bullish")
        if c1h[-1][4] < e20h < e50h:
            short_score += 2; short_reasons.append("1H trend bearish")
    if e20_15 and e50_15:
        if c15[-1][4] > e20_15 > e50_15:
            long_score += 2; long_reasons.append("15m structure bullish")
        if c15[-1][4] < e20_15 < e50_15:
            short_score += 2; short_reasons.append("15m structure bearish")
    # 10m is the mandatory entry-timeframe confirmation. It can add score,
    # but more importantly it can veto a signal when structure is reversing.
    if ten_ok_long:
        long_score += 2; long_reasons.append(ten_text_long)
    if ten_ok_short:
        short_score += 2; short_reasons.append(ten_text_short)
    if e9_5 and e21_5:
        if e9_5 > e21_5 and close5 > e9_5:
            long_score += 1; long_reasons.append("5m momentum")
        if e9_5 < e21_5 and close5 < e9_5:
            short_score += 1; short_reasons.append("5m momentum")

    # RSI confirms momentum, but extreme values are not rewarded because
    # they often mean the move is already extended.
    if 55 <= r <= 68:
        long_score += 1; long_reasons.append("RSI supports long")
    elif 32 <= r <= 45:
        short_score += 1; short_reasons.append("RSI supports short")

    if vr >= 1.10:
        if close5 >= c5[-1][1]:
            long_score += 1; long_reasons.append("volume expansion")
        else:
            short_score += 1; short_reasons.append("volume expansion")

    if obi >= 0.10:
        long_score += 1; long_reasons.append("order-book bid imbalance")
    elif obi <= -0.10:
        short_score += 1; short_reasons.append("order-book ask imbalance")

    if len(c1m) >= 6:
        mret = (c1m[-1][4] / c1m[-6][4] - 1) * 100
        if mret > 0.10:
            long_score += 1; long_reasons.append("1m impulse")
        elif mret < -0.10:
            short_score += 1; short_reasons.append("1m impulse")

    long_trigger, long_trigger_text = retest_trigger(c1m, "LONG")
    short_trigger, short_trigger_text = retest_trigger(c1m, "SHORT")

    # Independent entry confirmations. Trend alignment alone can no longer
    # generate a signal; this prevents late entries after an extended move.
    long_triggers = int((e9_5 is not None and e21_5 is not None and e9_5 > e21_5 and close5 > e9_5))
    long_triggers += int(obi >= 0.10)
    long_triggers += int(vr >= 1.10 and close5 >= c5[-1][1])
    long_triggers += int(55 <= r <= 68)
    long_triggers += int(long_trigger)
    long_triggers += int(ten_ok_long)

    short_triggers = int((e9_5 is not None and e21_5 is not None and e9_5 < e21_5 and close5 < e9_5))
    short_triggers += int(obi <= -0.10)
    short_triggers += int(vr >= 1.10 and close5 <= c5[-1][1])
    short_triggers += int(32 <= r <= 45)
    short_triggers += int(short_trigger)
    short_triggers += int(ten_ok_short)

    # A closed 5m rejection in the trade direction is an additional entry-quality check.
    c5last = c5[-1]
    c5range = max(c5last[2] - c5last[3], c5last[4] * 1e-8)
    short_rejection = c5last[4] < c5last[1] and (c5last[2] - c5last[4]) / c5range >= 0.35
    long_rejection = c5last[4] > c5last[1] and (c5last[4] - c5last[3]) / c5range >= 0.35
    short_triggers += int(short_rejection)
    long_triggers += int(long_rejection)

    near_low = pos <= NEAR_EDGE_PCT
    near_high = pos >= 1 - NEAR_EDGE_PCT
    extended_short = dist_ema_atr < -MAX_EXTENSION_ATR
    extended_long = dist_ema_atr > MAX_EXTENSION_ATR
    # Minimum room to target-side structure. A short at the floor or a long
    # under the ceiling is rejected even when trend indicators agree.
    room_too_small_short = room_to_low_pct < 0.35
    room_too_small_long = room_to_high_pct < 0.35

    side = "LONG" if long_score > short_score else "SHORT"
    score = max(long_score, short_score)
    triggers = long_triggers if side == "LONG" else short_triggers
    reasons = long_reasons if side == "LONG" else short_reasons
    trigger_text = long_trigger_text if side == "LONG" else short_trigger_text

    if side == "SHORT":
        if near_low or room_too_small_short:
            wait_reason = "price too close to local LOW / insufficient downside room"
        elif not ten_ok_short:
            wait_reason = ten_text_short
        elif extended_short:
            wait_reason = "short is extended from 5m EMA"
        elif triggers < MIN_ENTRY_TRIGGERS:
            wait_reason = f"only {triggers}/{MIN_ENTRY_TRIGGERS} entry confirmations"
        elif not short_trigger:
            wait_reason = "waiting for pullback/retest trigger"
        else:
            wait_reason = "ready"
    else:
        if near_high or room_too_small_long:
            wait_reason = "price too close to local HIGH / insufficient upside room"
        elif not ten_ok_long:
            wait_reason = ten_text_long
        elif extended_long:
            wait_reason = "long is extended from 5m EMA"
        elif triggers < MIN_ENTRY_TRIGGERS:
            wait_reason = f"only {triggers}/{MIN_ENTRY_TRIGGERS} entry confirmations"
        elif not long_trigger:
            wait_reason = "waiting for pullback/retest trigger"
        else:
            wait_reason = "ready"

    blocked = (
        (side == "SHORT" and (near_low or room_too_small_short or extended_short or not ten_ok_short)) or
        (side == "LONG" and (near_high or room_too_small_long or extended_long or not ten_ok_long)) or
        triggers < MIN_ENTRY_TRIGGERS or
        not (short_trigger if side == "SHORT" else long_trigger)
    )

    entry_price = c1m[-1][4]
    return {
        "side": side, "score": score, "reasons": reasons, "entry": entry_price,
        "rsi": r, "obi": obi, "volume": vr, "atr": a,
        "local_low": local_low, "local_high": local_high,
        "pos": pos, "room_low": room_to_low_pct, "room_high": room_to_high_pct,
        "dist_ema_atr": dist_ema_atr, "triggers": triggers,
        "trigger_text": trigger_text, "blocked": blocked, "wait_reason": wait_reason,
        "ten_ok": ten_ok_long if side == "LONG" else ten_ok_short,
        "ten_text": ten_text_long if side == "LONG" else ten_text_short,
        "ten_retest": ten_retest_long if side == "LONG" else ten_retest_short,
        "ten_level": (min(x[3] for x in c10[-6:-3]) if side == "SHORT" else max(x[2] for x in c10[-6:-3])),
        "ten_atr": atr(c10),
    }


def fmt_signal(symbol, m):
    entry = m["entry"]
    # Structural stop: beyond the 10m broken level, with a small ATR buffer.
    # This is safer than a fixed percentage stop placed inside the retest zone.
    if m["side"] == "LONG":
        structural_sl = m["ten_level"] - m["ten_atr"] * 0.18
        risk = max(entry - structural_sl, entry * 0.0018)
        sl = entry - risk
        target = max(risk * 1.55, entry * 0.0045)
        tp = entry + target
    else:
        structural_sl = m["ten_level"] + m["ten_atr"] * 0.18
        risk = max(structural_sl - entry, entry * 0.0018)
        sl = entry + risk
        target = max(risk * 1.55, entry * 0.0045)
        tp = entry - target
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"🐟 SMALLFISH SIGNAL\n\n"
        f"{'🟢' if m['side']=='LONG' else '🔴'} {m['side']} {symbol}\n"
        f"Score: {m['score']:.0f}/10\n"
        f"Entry: {entry:.8g}\nSL: {sl:.8g}\nTP: {tp:.8g}\n\n"
        f"RSI(5m): {m['rsi']:.1f}\nOBI: {m['obi']:+.2f}\nVolume: {m['volume']:.2f}x\n"
        f"Entry confirmations: {m['triggers']}\n"
        f"Trigger: {m['trigger_text']}\n"
        f"Why: {', '.join(m['reasons'][:5])}\n\n"
        f"⚠️ Signal-only. No orders are placed.\n{now}"
    )


def fmt_diag(symbol, m):
    edge = "low" if m["side"] == "SHORT" else "high"
    return (
        f"{symbol} | {m['side']} | score={m['score']:.0f}/10 | "
        f"triggers={m['triggers']}/{MIN_ENTRY_TRIGGERS} | "
        f"RSI={m['rsi']:.1f} | OBI={m['obi']:+.2f} | V={m['volume']:.2f}x | "
        f"range-pos={m['pos']:.2f} | room-to-{edge}="
        f"{m['room_low'] if edge=='low' else m['room_high']:.2f}% | "
        f"EMA21={m['dist_ema_atr']:+.2f}ATR | 10m={m['ten_text']} | {m['wait_reason']}"
    )


async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=12)) as r:
        if r.status != 200:
            body = await r.text()
            raise RuntimeError(f"Telegram HTTP {r.status}: {body[:300]}")


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    LOG.info("SMALLFISH PUBLIC SIGNAL MODE v5 started | %d symbols | poll=%ss", len(SYMBOLS), POLL_SECONDS)
    LOG.info("MEXC public market data only | no API key/secret | no orders")
    LOG.info("Entry filter | min_score=%s | min_triggers=%s | near_edge=%s | max_extension=%s ATR", MIN_SCORE, MIN_ENTRY_TRIGGERS, NEAR_EDGE_PCT, MAX_EXTENSION_ATR)
    async with aiohttp.ClientSession(headers={"User-Agent": "smallfish-public-signal/5.0"}) as session:
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            try:
                await send_telegram(session, "🐟 SMALLFISH PUBLIC SIGNAL MODE v5 ONLINE\nMEXC public data only. No orders.\nEntry filter: 1H + 15m + mandatory 10m structure + 5m + 1m trigger + location.")
            except Exception as e:
                LOG.warning("Telegram startup message failed: %s", e)

        while True:
            started = time.monotonic()
            signals = 0
            for symbol in SYMBOLS:
                try:
                    c1h, c15, c5, c1m = await asyncio.gather(
                        candles(session, symbol, "Hour1"),
                        candles(session, symbol, "Min15"),
                        candles(session, symbol, "Min5"),
                        candles(session, symbol, "Min1"),
                    )
                    c10 = aggregate_5m_to_10m(c5)
                    if len(c10) < 25:
                        raise RuntimeError(f"not enough derived 10m candles: {len(c10)}")
                    bids, asks = await depth(session, symbol)
                    m = setup_metrics(c1h, c15, c5, c10, c1m, bids, asks)

                    if DIAGNOSTICS:
                        LOG.info("%s", fmt_diag(symbol, m))

                    key = f"{symbol}:{m['side']}"
                    now = time.time()
                    if m["score"] < MIN_SCORE or m["blocked"]:
                        continue
                    if now - last_alert.get(key, 0) < ALERT_COOLDOWN:
                        continue

                    msg = fmt_signal(symbol, m)
                    LOG.info("SIGNAL %s %s score=%.0f entry=%s", m["side"], symbol, m["score"], m["entry"])
                    await send_telegram(session, msg)
                    last_alert[key] = now
                    signals += 1
                except Exception as e:
                    LOG.warning("%s scan failed: %s", symbol, e)

            elapsed = time.monotonic() - started
            LOG.info("scan complete | %.1fs | signals=%d", elapsed, signals)
            await asyncio.sleep(max(1, POLL_SECONDS - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
