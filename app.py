
import asyncio
import logging
import math
import os
import statistics
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
MIN_SCORE = float(os.getenv("MIN_SCORE", "7"))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))

# This is deliberately signal-only: no account endpoints, no orders,
# no API key/secret and no exchange credentials are required.
MEXC_PUBLIC = "https://api.mexc.com"
MEXC_CONTRACT = "https://contract.mexc.com"
FUTURES_KLINE = "/api/v1/contract/kline"
SPOT_KLINE = "/api/v3/klines"
FUTURES_DEPTH = "/api/v1/contract/depth"
SPOT_DEPTH = "/api/v3/depth"
FUTURES_TICKER = "/api/v1/contract/ticker"
SPOT_TICKER = "/api/v3/ticker/24hr"

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
    for a, b in zip(values[-period-1:-1], values[-period:]):
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
    prev = candles[-period-1][4]
    for c in candles[-period:]:
        h, l = c[2], c[3]
        trs.append(max(h-l, abs(h-prev), abs(l-prev)))
        prev = c[4]
    return sum(trs) / len(trs)

def parse_candles(data):
    # Futures response is normally an object containing arrays; spot is a list.
    if isinstance(data, list):
        rows = data
        out=[]
        for x in rows:
            if isinstance(x, list) and len(x) >= 6:
                out.append([int(x[0]), float(x[1]), float(x[2]), float(x[3]),
                            float(x[4]), float(x[5])])
        return out
    if isinstance(data, dict):
        d=data.get("data")
        if isinstance(d, dict):
            ts=d.get("time") or d.get("timestamp")
            opens=d.get("open") or d.get("opens")
            highs=d.get("high") or d.get("highs")
            lows=d.get("low") or d.get("lows")
            closes=d.get("close") or d.get("closes")
            vols=d.get("vol") or d.get("volume") or d.get("volumes")
            if all(isinstance(v, list) for v in [ts,opens,highs,lows,closes,vols]):
                n=min(map(len,[ts,opens,highs,lows,closes,vols]))
                return [[int(ts[i])*1000 if int(ts[i]) < 10**12 else int(ts[i]),
                         float(opens[i]),float(highs[i]),float(lows[i]),
                         float(closes[i]),float(vols[i])] for i in range(n)]
        if isinstance(d, list):
            return parse_candles(d)
    return []

async def get_json(session, path, params, base=MEXC_PUBLIC):
    url=base+path
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=12)) as r:
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} {path}")
        return await r.json(content_type=None)

async def candles(session, symbol, interval):
    # MEXC perpetual-futures market data uses contract.mexc.com, not api.mexc.com.
    # The futures API expects Min1/Min5/Min15/Min60 rather than 1m/5m/15m/1h.
    contract_interval = {
        "Min1": "Min1",
        "Min5": "Min5",
        "Min15": "Min15",
        "Hour1": "Min60",
    }[interval]
    fs = symbol.replace("USDT", "_USDT")
    d = await get_json(
        session,
        f"{FUTURES_KLINE}/{fs}",
        {"interval": contract_interval},
        base=MEXC_CONTRACT,
    )
    c = parse_candles(d)
    if len(c) < 30:
        raise RuntimeError(f"no usable futures candles for {symbol} {interval}: {len(c)}")
    return c[-120:]

async def depth(session, symbol):
    fs=symbol.replace("USDT","_USDT")
    d=await get_json(session, f"{FUTURES_DEPTH}/{fs}", {"limit":20}, base=MEXC_CONTRACT)
    data=d.get("data",d) if isinstance(d,dict) else d
    bids=data.get("bids",[]) if isinstance(data,dict) else []
    asks=data.get("asks",[]) if isinstance(data,dict) else []
    if not bids or not asks:
        raise RuntimeError(f"empty futures order book for {symbol}")
    return bids,asks

def score_setup(c1h,c15,c5,c1m,bids,asks):
    close1=c1h[-1][4]
    e20h=ema([x[4] for x in c1h],20)
    e50h=ema([x[4] for x in c1h],50)
    e20_15=ema([x[4] for x in c15],20)
    e50_15=ema([x[4] for x in c15],50)
    e9_5=ema([x[4] for x in c5],9)
    e21_5=ema([x[4] for x in c5],21)
    r=rsi([x[4] for x in c5])
    vols=[x[5] for x in c5[-21:-1]]
    vol_avg=sum(vols)/len(vols) if vols else 0
    vol_ratio=c5[-1][5]/vol_avg if vol_avg else 1
    mid=(float(bids[0][0])+float(asks[0][0]))/2 if bids and asks else c5[-1][4]
    bidvol=sum(float(x[1]) for x in bids[:10])
    askvol=sum(float(x[1]) for x in asks[:10])
    obi=(bidvol-askvol)/(bidvol+askvol) if bidvol+askvol else 0

    long_score=0
    short_score=0
    reasons_l=[]
    reasons_s=[]

    if e20h and e50h:
        if close1>e20h>e50h: long_score+=2; reasons_l.append("1H trend bullish")
        if close1<e20h<e50h: short_score+=2; reasons_s.append("1H trend bearish")
    if e20_15 and e50_15:
        if c15[-1][4]>e20_15>e50_15: long_score+=2; reasons_l.append("15m structure bullish")
        if c15[-1][4]<e20_15<e50_15: short_score+=2; reasons_s.append("15m structure bearish")
    if e9_5 and e21_5:
        if e9_5>e21_5 and c5[-1][4]>e9_5: long_score+=1; reasons_l.append("5m momentum")
        if e9_5<e21_5 and c5[-1][4]<e9_5: short_score+=1; reasons_s.append("5m momentum")
    if r>=55 and r<=72: long_score+=1; reasons_l.append("RSI supports long")
    if r<=45 and r>=28: short_score+=1; reasons_s.append("RSI supports short")
    if vol_ratio>=1.15:
        if c5[-1][4]>=c5[-1][1]: long_score+=1; reasons_l.append("volume expansion")
        else: short_score+=1; reasons_s.append("volume expansion")
    if obi>=0.12: long_score+=1; reasons_l.append("order-book bid imbalance")
    if obi<=-0.12: short_score+=1; reasons_s.append("order-book ask imbalance")
    if len(c1m)>=6:
        mret=(c1m[-1][4]/c1m[-6][4]-1)*100
        if mret>0.12: long_score+=1; reasons_l.append("1m impulse")
        if mret<-0.12: short_score+=1; reasons_s.append("1m impulse")

    side="LONG" if long_score>short_score else "SHORT"
    score=max(long_score,short_score)
    reasons=reasons_l if side=="LONG" else reasons_s
    return side, score, reasons, mid, r, obi, vol_ratio

async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload={"chat_id":TELEGRAM_CHAT_ID,"text":text}
    async with session.post(url,json=payload,timeout=aiohttp.ClientTimeout(total=12)) as r:
        if r.status != 200:
            body=await r.text()
            raise RuntimeError(f"Telegram HTTP {r.status}: {body[:300]}")

def fmt_signal(symbol, side, score, entry, atrv, reasons, rsi_v, obi, vr):
    # Signal-only target is intentionally modest: about 0.6%, with a wider
    # volatility-aware stop. No order is submitted by this program.
    risk=max(atrv*0.8, entry*0.0025)
    target=max(entry*0.006, risk*1.5)
    if side=="LONG":
        sl=entry-risk; tp=entry+target
    else:
        sl=entry+risk; tp=entry-target
    now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (
        f"🐟 SMALLFISH SIGNAL\n\n"
        f"{'🟢' if side=='LONG' else '🔴'} {side} {symbol}\n"
        f"Score: {score:.0f}/10\n"
        f"Entry: {entry:.8g}\n"
        f"SL: {sl:.8g}\n"
        f"TP: {tp:.8g}\n\n"
        f"RSI(5m): {rsi_v:.1f}\n"
        f"OBI: {obi:+.2f}\n"
        f"Volume: {vr:.2f}x\n"
        f"Why: {', '.join(reasons[:5])}\n\n"
        f"⚠️ Signal-only. No orders are placed.\n"
        f"{now}"
    )

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )
    LOG.info("SMALLFISH PUBLIC SIGNAL MODE started | %d symbols | poll=%ss",
             len(SYMBOLS), POLL_SECONDS)
    LOG.info("MEXC public market data only | no API key/secret | no orders")
    async with aiohttp.ClientSession(headers={"User-Agent":"smallfish-public-signal/1.0"}) as session:
        # Validate Telegram once.
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            try:
                await send_telegram(session, "🐟 SMALLFISH PUBLIC SIGNAL MODE ONLINE\nNo MEXC API key/secret required.\nNo orders are placed.")
            except Exception as e:
                LOG.warning("Telegram startup message failed: %s", e)

        while True:
            started=time.monotonic()
            signals=0
            for symbol in SYMBOLS:
                try:
                    c1h,c15,c5,c1m = await asyncio.gather(
                        candles(session,symbol,"Hour1"),
                        candles(session,symbol,"Min15"),
                        candles(session,symbol,"Min5"),
                        candles(session,symbol,"Min1"),
                    )
                    bids,asks=await depth(session,symbol)
                    side,score,reasons,entry,rsi_v,obi,vr=score_setup(
                        c1h,c15,c5,c1m,bids,asks
                    )
                    if score < MIN_SCORE:
                        continue
                    key=f"{symbol}:{side}"
                    now=time.time()
                    if now-last_alert.get(key,0) < ALERT_COOLDOWN:
                        continue
                    msg=fmt_signal(symbol,side,score,entry,atr(c5),reasons,rsi_v,obi,vr)
                    LOG.info("SIGNAL %s %s score=%.0f entry=%s",side,symbol,score,entry)
                    await send_telegram(session,msg)
                    last_alert[key]=now
                    signals+=1
                except Exception as e:
                    LOG.warning("%s scan failed: %s",symbol,e)
            elapsed=time.monotonic()-started
            LOG.info("scan complete | %.1fs | signals=%d",elapsed,signals)
            await asyncio.sleep(max(1,POLL_SECONDS-elapsed))

if __name__=="__main__":
    asyncio.run(main())
