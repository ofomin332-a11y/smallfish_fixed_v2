import asyncio, logging, os, time
from datetime import datetime, timezone
from collections import defaultdict
import aiohttp
from dotenv import load_dotenv

load_dotenv()
LOG = logging.getLogger("smallfish-public-v8")
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "LINKUSDT,BTCUSDT,ETHUSDT,NEARUSDT,PYTHUSDT,ADAUSDT,SOLUSDT,SUIUSDT,DOGEUSDT").split(",") if s.strip()]
POLL = int(os.getenv("POLL_SECONDS", "30"))
MIN_SCORE = float(os.getenv("MIN_SCORE", "7"))
COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
SWEEP_LOOKBACK = int(os.getenv("SWEEP_LOOKBACK", "12"))
SWEEP_MAX_AGE = int(os.getenv("SWEEP_MAX_AGE", "12"))
FRESH_SWEEP_AGE = int(os.getenv("FRESH_SWEEP_AGE", "4"))
MAX_EXTENSION_ATR = float(os.getenv("MAX_EXTENSION_ATR", "2.20"))
MIN_ROOM_PCT = float(os.getenv("MIN_ROOM_PCT", "0.35"))
DIAG = os.getenv("DIAGNOSTICS", "true").lower() in {"1", "true", "yes", "on"}
MEXC = "https://api.mexc.com"
KLINE = "/api/v1/contract/kline"
DEPTH = "/api/v1/contract/depth"
last_alert = {}
cache = {}

TTL = {"1m": 12, "5m": 25, "15m": 90, "1h": 300}
MS = {"1m": 60000, "5m": 300000, "15m": 900000, "1h": 3600000}
INT = {"1m": "Min1", "5m": "Min5", "15m": "Min15", "1h": "Min60"}

def ema(v, p):
    if len(v) < p: return None
    k = 2/(p+1); x = sum(v[:p])/p
    for a in v[p:]: x = a*k + x*(1-k)
    return x

def rsi(v, p=14):
    if len(v) < p+1: return 50.0
    g=l=0.0
    for a,b in zip(v[-p-1:-1],v[-p:]):
        d=b-a; g += max(d,0); l += max(-d,0)
    return 100.0 if l == 0 else 100 - 100/(1+g/l)

def atr(c,p=14):
    if len(c)<p+1:return 0.0
    out=[]; prev=c[-p-1][4]
    for x in c[-p:]:
        out.append(max(x[2]-x[3],abs(x[2]-prev),abs(x[3]-prev))); prev=x[4]
    return sum(out)/len(out)

def parse(d):
    if isinstance(d,list):
        return [[int(x[0]),float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5])] for x in d if isinstance(x,list) and len(x)>=6]
    if not isinstance(d,dict): return []
    d=d.get("data",d)
    if isinstance(d,list): return parse(d)
    ts=d.get("time") or d.get("timestamp"); op=d.get("open") or d.get("opens"); hi=d.get("high") or d.get("highs"); lo=d.get("low") or d.get("lows"); cl=d.get("close") or d.get("closes"); vol=d.get("vol") or d.get("volume") or d.get("volumes")
    if all(isinstance(x,list) for x in (ts,op,hi,lo,cl,vol)):
        n=min(map(len,(ts,op,hi,lo,cl,vol))); out=[]
        for i in range(n):
            t=int(ts[i]); t=t*1000 if t<10**12 else t
            out.append([t,float(op[i]),float(hi[i]),float(lo[i]),float(cl[i]),float(vol[i])])
        return out
    return []

def closed(c,ms):
    now=int(time.time()*1000)
    z=[x for x in c if x[0]+ms<=now]
    return z if len(z)>=25 else c[:-1] if len(c)>25 else c

async def request(session,path,params,retries=5):
    last=None
    for i in range(retries):
        try:
            async with session.get(MEXC+path,params=params,timeout=aiohttp.ClientTimeout(total=8)) as r:
                text=await r.text()
                if r.status!=200: raise RuntimeError(f"HTTP {r.status}")
                d=__import__('json').loads(text)
                if isinstance(d,dict) and d.get("success") is False: raise RuntimeError(str(d))
                return d
        except Exception as e:
            last=e
            await asyncio.sleep(min(3.0,0.35*(2**i)))
    raise last

async def candles(session,symbol,interval):
    key=(symbol,interval); now=time.time(); old=cache.get(key)
    if old and now-old[0] < TTL[interval]: return old[1]
    fs=symbol.replace("USDT","_USDT")
    c=parse(await request(session,f"{KLINE}/{fs}",{"interval":INT[interval]}))
    c=closed(c,MS[interval])
    if len(c)<70:
        if old and len(old[1])>=70:
            LOG.warning("%s %s empty/short response; using cached candles",symbol,interval)
            return old[1]
        raise RuntimeError(f"not enough {interval} candles: {len(c)}")
    c=c[-300:]; cache[key]=(now,c); return c

async def book(session,symbol):
    key=(symbol,"book"); now=time.time(); old=cache.get(key)
    if old and now-old[0] < 10:return old[1]
    fs=symbol.replace("USDT","_USDT")
    d=await request(session,f"{DEPTH}/{fs}",{"limit":20}); d=d.get("data",d) if isinstance(d,dict) else d
    b=d.get("bids",[]) if isinstance(d,dict) else []; a=d.get("asks",[]) if isinstance(d,dict) else []
    if not b or not a:
        if old:return old[1]
        raise RuntimeError("empty order book")
    cache[key]=(now,(b,a)); return b,a

def obi(b,a):
    bv=sum(float(x[1]) for x in b[:10]); av=sum(float(x[1]) for x in a[:10])
    return (bv-av)/(bv+av) if bv+av else 0.0

def agg10(c5):
    d=defaultdict(list)
    for x in c5:d[(x[0]//600000)*600000].append(x)
    out=[]
    for ts,r in sorted(d.items()):
        r=sorted(r)
        if len(r)>=2: out.append([ts,r[0][1],max(x[2] for x in r),min(x[3] for x in r),r[-1][4],sum(x[5] for x in r)])
    return out

def sweep(c,side):
    if len(c)<SWEEP_LOOKBACK+6:return None
    start=max(SWEEP_LOOKBACK,len(c)-SWEEP_MAX_AGE-2); end=len(c)-2
    for i in range(end,start-1,-1):
        before=c[i-SWEEP_LOOKBACK:i]; cur=c[i]; a=atr(c[:i+1])
        if not a:continue
        if side=="LONG":
            level=min(x[3] for x in before)
            if cur[3]>=level:continue
            reclaim=None
            for k in range(i,min(i+4,len(c))):
                if c[k][4]>level: reclaim=k; break
            if reclaim is not None:
                return {"index":i,"confirm_index":reclaim,"level":level,"extreme":min(x[3] for x in c[i:reclaim+1]),"atr":a,"age":len(c)-1-reclaim}
        else:
            level=max(x[2] for x in before)
            if cur[2]<=level:continue
            reclaim=None
            for k in range(i,min(i+4,len(c))):
                if c[k][4]<level: reclaim=k; break
            if reclaim is not None:
                return {"index":i,"confirm_index":reclaim,"level":level,"extreme":max(x[2] for x in c[i:reclaim+1]),"atr":a,"age":len(c)-1-reclaim}
    return None

def choch(c,sw,side):
    i=sw["confirm_index"]; prior=c[max(0,sw["index"]-6):sw["index"]]
    if len(prior)<2:return False,None
    level=max(x[2] for x in prior) if side=="LONG" else min(x[3] for x in prior)
    for k in range(i+1,len(c)):
        if (c[k][4]>level if side=="LONG" else c[k][4]<level):return True,level
    return False,level

def bias(c1h,c15,side):
    h=[x[4] for x in c1h]; m=[x[4] for x in c15]
    e20h,e50h,e20m,e50m=ema(h,20),ema(h,50),ema(m,20),ema(m,50)
    if None in (e20h,e50h,e20m,e50m):return 0
    if side=="LONG":return sum([h[-1]>e20h,e20h>e50h,m[-1]>e20m,e20m>e50m])
    return sum([h[-1]<e20h,e20h<e50h,m[-1]<e20m,e20m<e50m])

def five(c,side):
    cl=[x[4] for x in c]; e9,e21=ema(cl,9),ema(cl,21); rr=rsi(cl)
    if e9 is None:return False
    return (e9>e21 and cl[-1]>e9 and 45<=rr<=78) if side=="LONG" else (e9<e21 and cl[-1]<e9 and 22<=rr<=55)

def micro(c,side):
    if len(c)<10:return False
    w=c[-8:]; last=w[-1]; ph=max(x[2] for x in w[-4:-1]); pl=min(x[3] for x in w[-4:-1])
    return (last[4]>ph and last[4]>last[1]) if side=="LONG" else (last[4]<pl and last[4]<last[1])

def location(c5,side,sw,entry):
    a=atr(c5)
    if not a:return False,"ATR unavailable",0
    lo=min(x[3] for x in c5[-36:]); hi=max(x[2] for x in c5[-36:])
    if side=="LONG":
        room=(hi-entry)/entry*100; ext=(entry-sw["extreme"])/a
        if room<MIN_ROOM_PCT:return False,f"too close to local HIGH ({room:.2f}% room)",ext
    else:
        room=(entry-lo)/entry*100; ext=(sw["extreme"]-entry)/a
        if room<MIN_ROOM_PCT:return False,f"too close to local LOW ({room:.2f}% room)",ext
    if ext>MAX_EXTENSION_ATR:return False,f"market entry {ext:.2f} ATR from sweep; wait for retest",ext
    return True,"location acceptable",ext

def zone(sw,chl,side):
    if side=="LONG": return min(sw["extreme"],sw["level"]), max(sw["level"],chl)
    return min(chl,sw["level"]), max(sw["extreme"],sw["level"])

def analyze(h,m15,c5,c1m,c10,b,a):
    entry=c1m[-1][4]; oi=obi(b,a); best=None
    for side in ("LONG","SHORT"):
        sw=sweep(c10,side)
        if not sw:continue
        ch,chl=choch(c10,sw,side)
        if not ch:continue
        bs=bias(h,m15,side); f=five(c5,side); mic=micro(c1m,side)
        loc,lt,ext=location(c5,side,sw,entry)
        ob=(oi>=0.08) if side=="LONG" else (oi<=-0.08)
        vol=c5[-1][5]; av=sum(x[5] for x in c5[-21:-1])/20; vr=vol/av if av else 1
        vs=vr>=0.80
        fresh=sw["age"]<=FRESH_SWEEP_AGE
        score=2+min(bs,3)+int(f)+int(mic)+int(ob)+int(vs)+int(fresh)
        zone_lo,zone_hi=zone(sw,chl,side)
        ready=fresh and bs>=2 and f and mic and ob and vs and loc and score>=MIN_SCORE
        # A setup with a valid pattern but an extended market is never labeled LONG/SHORT; it is WAIT.
        wait=lt if not loc else ("sweep is not fresh" if not fresh else "waiting for confirmations")
        x={"ready":ready,"side":side,"entry":entry,"score":score,"sweep":sw,"choch":chl,"bias":bs,"five":f,"micro":mic,"obi":oi,"volume":vr,"loc":loc,"loc_text":lt,"ext":ext,"zone_lo":zone_lo,"zone_hi":zone_hi,"wait":wait,"fresh":fresh}
        if best is None or x["score"]>best["score"]:best=x
    return best or {"ready":False,"wait":"no sweep + CHoCH setup","entry":entry}

def signal(sym,m):
    e=m["entry"]; sw=m["sweep"]; a=sw["atr"]
    if m["side"]=="LONG":
        sl=sw["extreme"]-a*0.20; risk=max(e-sl,e*.002); tp=e+max(risk*2.2,e*.008)
        direction="🟢 LONG"; liq="SELL-SIDE"
    else:
        sl=sw["extreme"]+a*0.20; risk=max(sl-e,e*.002); tp=e-max(risk*2.2,e*.008)
        direction="🔴 SHORT"; liq="BUY-SIDE"
    now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (f"🐟 SMALLFISH V8 SIGNAL\n\n{direction} {sym}\n\n"
            f"Entry zone: {m['zone_lo']:.8g} – {m['zone_hi']:.8g}\n"
            f"Market: {e:.8g}\nSL: {sl:.8g}\nTP: {tp:.8g}\n\n"
            f"Liquidity sweep: {liq} ✓\n10m CHoCH: ✓\n1H/15m bias: {m['bias']}/4\n"
            f"5m confirmation: ✓\n1m trigger: ✓\nOBI: {m['obi']:+.2f}\nVolume: {m['volume']:.2f}x\n\n"
            f"Why: fresh liquidity sweep, 10m CHoCH, aligned higher-timeframe bias, 5m confirmation, 1m trigger, order-book confirmation.\n\n"
            f"⚠️ Signal-only. No orders are placed.\n{now}")

def diag(sym,m):
    if "side" not in m:return f"{sym} | WAIT | {m['wait']}"
    return (f"{sym} | {'SIGNAL' if m['ready'] else 'WAIT'} {m['side']} | score={m['score']:.0f}/10 | "
            f"age={m['sweep']['age']} | bias={m['bias']}/4 | 5m={'Y' if m['five'] else 'N'} | 1m={'Y' if m['micro'] else 'N'} | "
            f"OBI={m['obi']:+.2f} | V={m['volume']:.2f}x | ext={m['ext']:.2f}ATR | {m['wait']}")

async def telegram(s,text):
    if not TOKEN or not CHAT:return
    async with s.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",json={"chat_id":CHAT,"text":text},timeout=aiohttp.ClientTimeout(total=10)) as r:
        if r.status!=200:raise RuntimeError(f"Telegram HTTP {r.status}")

async def main():
    logging.basicConfig(level=logging.INFO,format="%(asctime)s | %(levelname)s | %(message)s")
    LOG.info("SMALLFISH PUBLIC SIGNAL MODE v8 started | %d symbols | poll=%ss",len(SYMBOLS),POLL)
    LOG.info("MEXC public market data only | no API key/secret | no orders")
    LOG.info("V8 pattern: sweep -> 10m CHoCH -> fresh bias -> 5m -> 1m trigger -> entry zone")
    async with aiohttp.ClientSession(headers={"User-Agent":"smallfish-public-signal/8.0"}) as s:
        if TOKEN and CHAT:
            try: await telegram(s,"🐟 SMALLFISH V8 ONLINE\nLiquidity sweep → 10m CHoCH → 1H/15m bias → 5m → 1m trigger → entry zone.\nMEXC public data only. No orders.")
            except Exception as e: LOG.warning("Telegram startup failed: %s",e)
        while True:
            started=time.monotonic(); signals=0
            for sym in SYMBOLS:
                try:
                    h,m15,c5,c1=await asyncio.gather(candles(s,sym,"1h"),candles(s,sym,"15m"),candles(s,sym,"5m"),candles(s,sym,"1m"))
                    c10=agg10(c5)
                    if len(c10)<25:raise RuntimeError(f"not enough derived 10m candles: {len(c10)}")
                    b,a=await book(s,sym); m=analyze(h,m15,c5,c1,c10,b,a)
                    if DIAG:LOG.info("%s",diag(sym,m))
                    if not m.get("ready"):continue
                    key=f"{sym}:{m['side']}"; now=time.time()
                    if now-last_alert.get(key,0)<COOLDOWN:continue
                    await telegram(s,signal(sym,m)); last_alert[key]=now; signals+=1
                    LOG.info("SIGNAL %s %s score=%s zone=%s-%s",m["side"],sym,m["score"],m["zone_lo"],m["zone_hi"])
                except Exception as e:LOG.warning("%s scan failed: %s",sym,e)
            elapsed=time.monotonic()-started; LOG.info("scan complete | %.1fs | signals=%s",elapsed,signals)
            await asyncio.sleep(max(1,POLL-elapsed))

if __name__=="__main__":asyncio.run(main())
