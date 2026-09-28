import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger("smallfish-public-v6")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "SYMBOLS",
    "LINKUSDT,BTCUSDT,ETHUSDT,NEARUSDT,PYTHUSDT,ADAUSDT,SOLUSDT,SUIUSDT,DOGEUSDT"
).split(",") if s.strip()]
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
MIN_SCORE = float(os.getenv("MIN_SCORE", "7"))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
SWEEP_LOOKBACK = int(os.getenv("SWEEP_LOOKBACK", "12"))
SWEEP_MAX_AGE = int(os.getenv("SWEEP_MAX_AGE", "5"))
MAX_ENTRY_EXTENSION_ATR = float(os.getenv("MAX_ENTRY_EXTENSION_ATR", "1.15"))
MIN_ROOM_PCT = float(os.getenv("MIN_ROOM_PCT", "0.45"))
DIAGNOSTICS = os.getenv("DIAGNOSTICS", "true").lower() in {"1", "true", "yes", "on"}

MEXC = "https://api.mexc.com"
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
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag, al = sum(gains) / period, sum(losses) / period
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


def closed_candles(candles, interval_ms):
    if not candles:
        return []
    now_ms = int(time.time() * 1000)
    out = [c for c in candles if c[0] + interval_ms <= now_ms]
    return out if len(out) >= 20 else candles[:-1] if len(candles) > 20 else candles


def parse_candles(data):
    if isinstance(data, list):
        return [[int(x[0]), float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])]
                for x in data if isinstance(x, list) and len(x) >= 6]
    if not isinstance(data, dict):
        return []
    d = data.get("data")
    if isinstance(d, list):
        return parse_candles(d)
    if isinstance(d, dict):
        ts = d.get("time") or d.get("timestamp")
        op = d.get("open") or d.get("opens")
        hi = d.get("high") or d.get("highs")
        lo = d.get("low") or d.get("lows")
        cl = d.get("close") or d.get("closes")
        vol = d.get("vol") or d.get("volume") or d.get("volumes")
        if all(isinstance(v, list) for v in (ts, op, hi, lo, cl, vol)):
            n = min(map(len, (ts, op, hi, lo, cl, vol)))
            return [[int(ts[i]) * 1000 if int(ts[i]) < 10**12 else int(ts[i]),
                     float(op[i]), float(hi[i]), float(lo[i]), float(cl[i]), float(vol[i])]
                    for i in range(n)]
    return []


async def get_json(session, path, params):
    async with session.get(MEXC + path, params=params,
                           timeout=aiohttp.ClientTimeout(total=12)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} {path}")
        return await r.json(content_type=None)


async def candles(session, symbol, interval):
    fs = symbol.replace("USDT", "_USDT")
    api_interval = {"1m": "Min1", "5m": "Min5", "15m": "Min15", "1h": "Min60"}[interval]
    d = await get_json(session, f"{FUTURES_KLINE}/{fs}", {"interval": api_interval})
    c = parse_candles(d)
    ms = {"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000}[interval]
    c = closed_candles(c, ms)
    if len(c) < 70:
        raise RuntimeError(f"not enough {interval} candles: {len(c)}")
    return c[-240:]


async def depth(session, symbol):
    fs = symbol.replace("USDT", "_USDT")
    d = await get_json(session, f"{FUTURES_DEPTH}/{fs}", {"limit": 20})
    data = d.get("data", d) if isinstance(d, dict) else d
    bids = data.get("bids", []) if isinstance(data, dict) else []
    asks = data.get("asks", []) if isinstance(data, dict) else []
    if not bids or not asks:
        raise RuntimeError("empty order book")
    return bids, asks


def obi(bids, asks):
    bv = sum(float(x[1]) for x in bids[:10])
    av = sum(float(x[1]) for x in asks[:10])
    return (bv - av) / (bv + av) if bv + av else 0.0


def aggregate_10m(c5):
    buckets = {}
    for c in c5:
        ts = (c[0] // 600000) * 600000
        buckets.setdefault(ts, []).append(c)
    out = []
    for ts, rows in sorted(buckets.items()):
        rows = sorted(rows)
        if len(rows) < 2:
            continue
        out.append([ts, rows[0][1], max(x[2] for x in rows), min(x[3] for x in rows),
                    rows[-1][4], sum(x[5] for x in rows)])
    return out


def body_ratio(c):
    span = max(c[2] - c[3], c[4] * 1e-9)
    return abs(c[4] - c[1]) / span


def liquidity_sweep(c10, side):
    """Detect the key pattern: sweep of liquidity, then close back through the level."""
    if len(c10) < SWEEP_LOOKBACK + 5:
        return None
    end = min(SWEEP_MAX_AGE, len(c10) - 2)
    for age in range(1, end + 1):
        i = len(c10) - 1 - age
        if i < SWEEP_LOOKBACK:
            continue
        cur = c10[i]
        before = c10[i - SWEEP_LOOKBACK:i]
        level = min(x[3] for x in before) if side == "LONG" else max(x[2] for x in before)
        a = atr(c10[:i + 1])
        if a <= 0:
            continue
        if side == "LONG":
            swept = cur[3] < level and cur[4] > level
            wick = (min(cur[1], cur[4]) - cur[3]) / max(cur[2] - cur[3], cur[4] * 1e-9)
            if swept and wick >= 0.30:
                return {"index": i, "level": level, "extreme": cur[3], "atr": a, "age": age}
        else:
            swept = cur[2] > level and cur[4] < level
            wick = (cur[2] - max(cur[1], cur[4])) / max(cur[2] - cur[3], cur[4] * 1e-9)
            if swept and wick >= 0.30:
                return {"index": i, "level": level, "extreme": cur[2], "atr": a, "age": age}
    return None


def choch_after_sweep(c10, sweep, side):
    start = sweep["index"] + 1
    if start >= len(c10):
        return False, None
    window = c10[start:]
    if not window:
        return False, None
    prior = c10[max(0, sweep["index"] - 5):sweep["index"]]
    if not prior:
        return False, None
    if side == "LONG":
        level = max(x[2] for x in prior)
        hit = [x for x in window if x[4] > level]
    else:
        level = min(x[3] for x in prior)
        hit = [x for x in window if x[4] < level]
    return bool(hit), level


def micro_trigger(c1m, side):
    if len(c1m) < 10:
        return False, "not enough 1m trigger data"
    w = c1m[-8:]
    last = w[-1]
    prev = w[-2]
    if side == "LONG":
        local_high = max(x[2] for x in w[-4:-1])
        pullback = min(x[3] for x in w[-5:-2]) < max(x[2] for x in w[-5:-2])
        trigger = last[4] > local_high and last[4] > last[1]
        return bool(pullback and trigger), "1m reclaim + break" if pullback and trigger else "waiting 1m reclaim/break"
    local_low = min(x[3] for x in w[-4:-1])
    pullback = max(x[2] for x in w[-5:-2]) > min(x[3] for x in w[-5:-2])
    trigger = last[4] < local_low and last[4] < last[1]
    return bool(pullback and trigger), "1m reject + break" if pullback and trigger else "waiting 1m reject/break"


def bias(c1h, c15, side):
    h = [x[4] for x in c1h]
    m = [x[4] for x in c15]
    e20h, e50h = ema(h, 20), ema(h, 50)
    e20m, e50m = ema(m, 20), ema(m, 50)
    if not all(v is not None for v in (e20h, e50h, e20m, e50m)):
        return False
    if side == "LONG":
        return h[-1] > e20h > e50h and m[-1] > e20m > e50m
    return h[-1] < e20h < e50h and m[-1] < e20m < e50m


def five_min_confirmation(c5, side):
    closes = [x[4] for x in c5]
    e9, e21 = ema(closes, 9), ema(closes, 21)
    if e9 is None or e21 is None:
        return False
    r = rsi(closes)
    if side == "LONG":
        return e9 > e21 and closes[-1] > e9 and 48 <= r <= 72
    return e9 < e21 and closes[-1] < e9 and 28 <= r <= 52


def location_filter(c5, side, sweep, entry):
    a = atr(c5)
    if not a:
        return False, "ATR unavailable"
    lows = [x[3] for x in c5[-36:]]
    highs = [x[2] for x in c5[-36:]]
    lo, hi = min(lows), max(highs)
    if side == "LONG":
        room = (hi - entry) / entry * 100
        extension = (entry - sweep["extreme"]) / a
        if room < MIN_ROOM_PCT:
            return False, f"too close to local HIGH ({room:.2f}% room)"
        if extension > MAX_ENTRY_EXTENSION_ATR:
            return False, f"entry too far above sweep ({extension:.2f} ATR)"
    else:
        room = (entry - lo) / entry * 100
        extension = (sweep["extreme"] - entry) / a
        if room < MIN_ROOM_PCT:
            return False, f"too close to local LOW ({room:.2f}% room)"
        if extension > MAX_ENTRY_EXTENSION_ATR:
            return False, f"entry too far below sweep ({extension:.2f} ATR)"
    return True, "location acceptable"


def analyze(c1h, c15, c5, c1m, c10, bids, asks):
    entry = c1m[-1][4]
    order_imb = obi(bids, asks)
    best = None
    diagnostics = []

    for side in ("LONG", "SHORT"):
        sw = liquidity_sweep(c10, side)
        if not sw:
            diagnostics.append(f"{side}: no fresh liquidity sweep")
            continue
        ch_ok, ch_level = choch_after_sweep(c10, sw, side)
        if not ch_ok:
            diagnostics.append(f"{side}: sweep found, waiting CHoCH")
            continue
        b = bias(c1h, c15, side)
        f5 = five_min_confirmation(c5, side)
        micro, micro_text = micro_trigger(c1m, side)
        loc_ok, loc_text = location_filter(c5, side, sw, entry)
        ob_ok = order_imb >= 0.08 if side == "LONG" else order_imb <= -0.08
        vol = c5[-1][5]
        avg = sum(x[5] for x in c5[-21:-1]) / 20
        vr = vol / avg if avg else 1.0
        volume_ok = vr >= 0.85
        score = 0
        reasons = [f"{side}-side liquidity sweep", "10m CHoCH"]
        score += 3
        if b:
            score += 2; reasons.append("1H + 15m bias aligned")
        if f5:
            score += 1; reasons.append("5m confirmation")
        if ob_ok:
            score += 1; reasons.append("order-book reversal imbalance")
        if volume_ok:
            score += 1; reasons.append("volume supports move")
        if micro:
            score += 1; reasons.append(micro_text)
        if sw["age"] == 1:
            score += 1; reasons.append("fresh sweep")
        if not loc_ok:
            score -= 3
        candidate = {
            "side": side, "entry": entry, "score": score, "sweep": sw,
            "choch_level": ch_level, "bias": b, "five": f5, "micro": micro,
            "micro_text": micro_text, "loc_ok": loc_ok, "loc_text": loc_text,
            "obi": order_imb, "volume": vr, "reasons": reasons,
            "ready": b and f5 and micro and ob_ok and volume_ok and loc_ok and score >= MIN_SCORE,
        }
        if best is None or candidate["score"] > best["score"]:
            best = candidate

    if best is None:
        return {"ready": False, "wait": "no sweep + CHoCH setup", "diagnostics": diagnostics, "entry": entry}
    best["wait"] = "ready" if best["ready"] else best["loc_text"] if not best["loc_ok"] else \
        "waiting for 1H/15m + 5m + 1m confirmation"
    best["diagnostics"] = diagnostics
    return best


def fmt_signal(symbol, m):
    entry = m["entry"]
    a = atr_from = m["sweep"]["atr"]
    if m["side"] == "LONG":
        sl = min(m["sweep"]["extreme"] - a * 0.12, entry - a * 0.65)
        risk = max(entry - sl, entry * 0.002)
        tp = entry + max(risk * 2.0, entry * 0.006)
    else:
        sl = max(m["sweep"]["extreme"] + a * 0.12, entry + a * 0.65)
        risk = max(sl - entry, entry * 0.002)
        tp = entry - max(risk * 2.0, entry * 0.006)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        "🐟 SMALLFISH V6 SIGNAL\n\n"
        f"{'🟢' if m['side']=='LONG' else '🔴'} {m['side']} {symbol}\n"
        f"Score: {m['score']:.0f}/10\n"
        f"Entry: {entry:.8g}\nSL: {sl:.8g}\nTP: {tp:.8g}\n\n"
        f"Liquidity sweep: {'SELL-SIDE' if m['side']=='LONG' else 'BUY-SIDE'} ✓\n"
        f"10m CHoCH: ✓\n5m confirmation: {'✓' if m['five'] else '—'}\n"
        f"1m trigger: {'✓' if m['micro'] else '—'}\n"
        f"OBI: {m['obi']:+.2f}\nVolume: {m['volume']:.2f}x\n\n"
        f"Why: {', '.join(m['reasons'][:6])}\n\n"
        "⚠️ Signal-only. No orders are placed.\n" + now
    )


def fmt_diag(symbol, m):
    if "side" not in m:
        return f"{symbol} | WAIT | {m['wait']}"
    return (
        f"{symbol} | {m['side']} | score={m['score']:.0f}/10 | "
        f"sweep_age={m['sweep']['age']} | CHoCH={'Y' if m['choch_level'] else 'N'} | "
        f"5m={'Y' if m['five'] else 'N'} | 1m={'Y' if m['micro'] else 'N'} | "
        f"OBI={m['obi']:+.2f} | V={m['volume']:.2f}x | {m['wait']}"
    )


async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    async with session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                            timeout=aiohttp.ClientTimeout(total=12)) as r:
        if r.status != 200:
            body = await r.text()
            raise RuntimeError(f"Telegram HTTP {r.status}: {body[:300]}")


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    LOG.info("SMALLFISH PUBLIC SIGNAL MODE v6 started | %d symbols | poll=%ss", len(SYMBOLS), POLL_SECONDS)
    LOG.info("MEXC public market data only | no API key/secret | no orders")
    LOG.info("V6 pattern: liquidity sweep -> 10m CHoCH -> 1H/15m bias -> 5m -> 1m trigger")
    async with aiohttp.ClientSession(headers={"User-Agent": "smallfish-public-signal/6.0"}) as session:
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            try:
                await send_telegram(session, "🐟 SMALLFISH V6 ONLINE\nLiquidity sweep → 10m CHoCH → 1H/15m bias → 5m → 1m trigger.\nMEXC public data only. No orders.")
            except Exception as e:
                LOG.warning("Telegram startup failed: %s", e)
        while True:
            started = time.monotonic()
            signals = 0
            for symbol in SYMBOLS:
                try:
                    c1h, c15, c5, c1m = await asyncio.gather(
                        candles(session, symbol, "1h"),
                        candles(session, symbol, "15m"),
                        candles(session, symbol, "5m"),
                        candles(session, symbol, "1m"),
                    )
                    c10 = aggregate_10m(c5)
                    if len(c10) < 25:
                        raise RuntimeError(f"not enough derived 10m candles: {len(c10)}")
                    bids, asks = await depth(session, symbol)
                    m = analyze(c1h, c15, c5, c1m, c10, bids, asks)
                    if DIAGNOSTICS:
                        LOG.info("%s", fmt_diag(symbol, m))
                    if not m.get("ready"):
                        continue
                    key = f"{symbol}:{m['side']}"
                    now = time.time()
                    if now - last_alert.get(key, 0) < ALERT_COOLDOWN:
                        continue
                    await send_telegram(session, fmt_signal(symbol, m))
                    last_alert[key] = now
                    signals += 1
                    LOG.info("SIGNAL %s %s score=%s entry=%s", m["side"], symbol, m["score"], m["entry"])
                except Exception as e:
                    LOG.warning("%s scan failed: %s", symbol, e)
            elapsed = time.monotonic() - started
            LOG.info("scan complete | %.1fs | signals=%d", elapsed, signals)
            await asyncio.sleep(max(1, POLL_SECONDS - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
