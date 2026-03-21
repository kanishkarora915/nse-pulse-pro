#!/usr/bin/env python3
"""
NSE PULSE PRO v5.0 — Multi-User Institutional Terminal
Built by Kanishk Arora
Run:  python3 server.py
Open: http://localhost:3000
Supports 20+ concurrent users, each with their own Zerodha keys.
"""
import os, json, csv, io, time, hashlib, math, traceback, statistics, uuid, threading, gzip
from datetime import datetime, timedelta
import tornado.ioloop, tornado.web, tornado.gen
import requests
from concurrent.futures import ThreadPoolExecutor

PORT = int(os.environ.get("PORT", 3000))
KITE = "https://api.kite.trade"
DIR  = os.path.dirname(os.path.abspath(__file__))
pool = ThreadPoolExecutor(max_workers=20)

# ═══════════════════════════════════════════════════════════
#  SERVER-SIDE RESPONSE CACHE WITH TTL
# ═══════════════════════════════════════════════════════════
_cache = {}       # key -> {"data": ..., "ts": time.time()}
_cache_lock = threading.Lock()

CACHE_TTL_QUOTE      = 15   # seconds
CACHE_TTL_HISTORICAL = 60
CACHE_TTL_SCREENER   = 30

def cache_get(key):
    """Return cached value if still valid, else None."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry["ts"]) < entry["ttl"]:
            return entry["data"]
        # Expired — remove it
        _cache.pop(key, None)
        return None

def cache_set(key, data, ttl):
    """Store value with TTL."""
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time(), "ttl": ttl}

def cache_evict_expired():
    """Remove all expired entries (called periodically)."""
    now = time.time()
    with _cache_lock:
        expired = [k for k, v in _cache.items() if (now - v["ts"]) >= v["ttl"]]
        for k in expired:
            del _cache[k]

# Periodic cache cleanup every 120 seconds
def _schedule_cache_cleanup():
    cache_evict_expired()
    tornado.ioloop.IOLoop.current().call_later(120, _schedule_cache_cleanup)

# ═══════════════════════════════════════════════════════════
#  GZIP COMPRESSION HELPER
# ═══════════════════════════════════════════════════════════
GZIP_MIN_SIZE = 1024  # Only compress responses larger than 1KB

def gzip_compress(data_bytes):
    """Compress bytes with gzip."""
    return gzip.compress(data_bytes, compresslevel=6)

# ═══════════════════════════════════════════════════════════
#  LICENSE KEY SYSTEM — By Kanishk Arora
# ═══════════════════════════════════════════════════════════
LICENSE_FILE = os.path.join(DIR, "licenses.json")
MASTER_KEY = "KANISHK-MASTER-2026"  # Admin key to manage licenses

def load_licenses():
    """Load license keys from file"""
    if os.path.isfile(LICENSE_FILE):
        try:
            with open(LICENSE_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    # Default: 20 pre-generated keys + master
    default = {
        "keys": {
            "PULSE-PRO-001": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-002": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-003": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-004": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-005": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-006": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-007": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-008": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-009": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-010": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-011": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-012": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-013": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-014": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-015": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-016": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-017": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-018": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-019": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
            "PULSE-PRO-020": {"active": True, "user": "", "created": "2026-03-21", "uses": 0},
        }
    }
    save_licenses(default)
    return default

def save_licenses(data):
    """Save license keys to file"""
    try:
        with open(LICENSE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"  License save error: {e}")

def verify_license(key):
    """Check if a license key is valid and active"""
    if key == MASTER_KEY:
        return True, "Master key — full access"
    data = load_licenses()
    info = data["keys"].get(key)
    if not info:
        return False, "Invalid license key"
    if not info.get("active", False):
        return False, "License key has been deactivated"
    # Update usage
    info["uses"] = info.get("uses", 0) + 1
    info["last_used"] = datetime.now().isoformat()
    save_licenses(data)
    return True, "Valid license"

# ═══════════════════════════════════════════════════════════
#  MULTI-USER SESSION MANAGEMENT
# ═══════════════════════════════════════════════════════════
MAX_SESSIONS = 30
sessions = {}  # session_id -> {api_key, secret, token, created, last_active, instruments_loaded}
sessions_lock = threading.Lock()

# Shared instruments cache (downloaded once, shared across users)
SYM = {}     # "RELIANCE" -> {"token": 738561, "name": "..."}
TOK = {}     # 738561 -> "RELIANCE"
NFO = []     # NFO instruments for option chain
instruments_loaded = False
instruments_loading = False

def create_session(api_key, secret="", token=""):
    """Create a new session and return session_id"""
    with sessions_lock:
        # Clean up expired sessions (>12 hours)
        now = time.time()
        expired = [sid for sid, s in sessions.items() if now - s["last_active"] > 43200]
        for sid in expired:
            del sessions[sid]
        # Check capacity
        if len(sessions) >= MAX_SESSIONS:
            # Remove oldest inactive session
            oldest = min(sessions.items(), key=lambda x: x[1]["last_active"])
            del sessions[oldest[0]]
        sid = uuid.uuid4().hex[:16]
        sessions[sid] = {
            "api_key": api_key, "secret": secret, "token": token,
            "created": now, "last_active": now,
        }
        return sid

def get_session(sid):
    """Get session by ID, update last_active"""
    with sessions_lock:
        s = sessions.get(sid)
        if s:
            s["last_active"] = time.time()
        return s

def delete_session(sid):
    with sessions_lock:
        sessions.pop(sid, None)

# ─── INDEX COMPOSITIONS ───────────────────────────────────
NIFTY50 = [
    "RELIANCE","TCS","HDFCBANK","ICICIBANK","INFY","BHARTIARTL","ITC","SBIN",
    "HINDUNILVR","BAJFINANCE","LT","AXISBANK","KOTAKBANK","TITAN","ASIANPAINT",
    "MARUTI","WIPRO","HCLTECH","ULTRACEMCO","SUNPHARMA","TATAMOTORS","NTPC",
    "POWERGRID","NESTLEIND","ADANIPORTS","TECHM","M&M","BAJAJ-AUTO","TATASTEEL",
    "JSWSTEEL","CIPLA","DIVISLAB","GRASIM","HINDALCO","INDUSINDBK","BPCL",
    "ADANIENT","COALINDIA","BEL","SHRIRAMFIN","ONGC","TATACONSUM","HEROMOTOCO",
    "DRREDDY","APOLLOHOSP","EICHERMOT","BAJAJFINSV","SBILIFE","HDFCLIFE","BRITANNIA"
]

BANKNIFTY = [
    "HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK","INDUSINDBK",
    "BANKBARODA","CANBK","FEDERALBNK","IDFCFIRSTB","AUBANK","PNB"
]

NIFTYIT = ["TCS","INFY","WIPRO","HCLTECH","TECHM","LTIM","MPHASIS","COFORGE","PERSISTENT","LTTS"]

NIFTYPHARMA = ["SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP","AUROPHARMA",
               "BIOCON","TORNTPHARM","LUPIN","ALKEM"]

NIFTYAUTO = ["TATAMOTORS","MARUTI","M&M","BAJAJ-AUTO","EICHERMOT","HEROMOTOCO",
             "ASHOKLEY","BALKRISIND","BHARATFORG","BOSCHLTD","TVS MOTOR","EXIDEIND"]

NIFTYMETAL = ["TATASTEEL","JSWSTEEL","HINDALCO","VEDL","NMDC","SAIL",
              "NATIONALUM","JINDALSTEL","APLAPOLLO","RATNAMANI"]

NIFTYENERGY = ["RELIANCE","NTPC","POWERGRID","ONGC","BPCL","TATAPOWER",
               "IOC","GAIL","ADANIGREEN","COALINDIA"]

NIFTYFMCG = ["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","TATACONSUM","GODREJCP",
             "DABUR","MARICO","COLPAL","VBL"]

NIFTYREALTY = ["DLF","GODREJPROP","OBEROIRLTY","PHOENIXLTD","PRESTIGE",
               "BRIGADE","SOBHA","LODHA","MAHLIFE","SUNTECK"]

NIFTYMIDCAP = ["PERSISTENT","COFORGE","MPHASIS","LTTS","AUBANK","FEDERALBNK",
               "BALKRISIND","ASHOKLEY","AUROPHARMA","TORNTPHARM","LUPIN","ALKEM",
               "JINDALSTEL","SAIL","NATIONALUM","VEDL","TATAPOWER","IOC","GAIL",
               "DLF","GODREJPROP","OBEROIRLTY","VBL","DABUR","MARICO","COLPAL"]

INDICES = {
    "NIFTY50": NIFTY50, "BANKNIFTY": BANKNIFTY, "NIFTYIT": NIFTYIT,
    "NIFTYPHARMA": NIFTYPHARMA, "NIFTYAUTO": NIFTYAUTO, "NIFTYMETAL": NIFTYMETAL,
    "NIFTYENERGY": NIFTYENERGY, "NIFTYFMCG": NIFTYFMCG, "NIFTYREALTY": NIFTYREALTY,
    "NIFTYMIDCAP": NIFTYMIDCAP,
}

# Stock → Sector mapping
STOCK_SECTOR = {}
_sector_map = {
    "Banking": BANKNIFTY, "IT": NIFTYIT, "Pharma": NIFTYPHARMA,
    "Auto": NIFTYAUTO, "Metal": NIFTYMETAL, "Energy": NIFTYENERGY,
    "FMCG": NIFTYFMCG, "Realty": NIFTYREALTY,
}
for _sec, _syms in _sector_map.items():
    for _s in _syms:
        if _s not in STOCK_SECTOR:
            STOCK_SECTOR[_s] = _sec
for _s in NIFTY50:
    if _s not in STOCK_SECTOR:
        STOCK_SECTOR[_s] = "Diversified"


def kh_for(session):
    """Build Kite headers for a specific user session"""
    return {"Authorization": f"token {session['api_key']}:{session['token']}", "X-Kite-Version": "3"}

def download_instruments(session):
    """Download instruments using a session's credentials (shared cache)"""
    global instruments_loaded, instruments_loading
    if instruments_loaded or instruments_loading:
        return
    instruments_loading = True
    try:
        print("  Downloading Kite instruments...")
        r = requests.get(f"{KITE}/instruments", headers=kh_for(session), timeout=60)
        if r.status_code != 200:
            print(f"  Instruments failed: HTTP {r.status_code}")
            instruments_loading = False
            return
        reader = csv.DictReader(io.StringIO(r.text))
        nc = nf = 0
        for row in reader:
            try:
                ex = row.get("exchange","")
                it = row.get("instrument_type","")
                if ex == "NSE" and it == "EQ":
                    s = row["tradingsymbol"]
                    t = int(row["instrument_token"])
                    SYM[s] = {"token": t, "name": row.get("name", s)}
                    TOK[t] = s
                    nc += 1
                elif ex == "NFO":
                    NFO.append({
                        "token": int(row["instrument_token"]),
                        "symbol": row["tradingsymbol"],
                        "name": row.get("name",""),
                        "expiry": row.get("expiry",""),
                        "strike": float(row.get("strike",0)),
                        "type": it,
                        "lot": int(row.get("lot_size",1))
                    })
                    nf += 1
            except: continue
        instruments_loaded = True
        instruments_loading = False
        print(f"  Loaded {nc} NSE equities + {nf} NFO instruments")
    except Exception as e:
        instruments_loading = False
        print(f"  Instrument download error: {e}")

# ═══════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS (Institutional Grade)
# ═══════════════════════════════════════════════════════════

def calc_sma(data, period):
    if len(data) < period: return []
    return [sum(data[i-period:i])/period for i in range(period, len(data)+1)]

def calc_ema(data, period):
    if len(data) < period: return []
    k = 2/(period+1)
    ema = [sum(data[:period])/period]
    for i in range(period, len(data)):
        ema.append(data[i]*k + ema[-1]*(1-k))
    return ema

def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d,0)); losses.append(max(-d,0))
    if len(gains) < period: return None
    ag = sum(gains[:period])/period
    al = sum(losses[:period])/period
    for i in range(period, len(gains)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
    if al == 0: return 100.0
    return round(100 - 100/(1+ag/al), 1)

def calc_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow + signal: return None, None, None
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)
    diff = len(ema_fast) - len(ema_slow)
    macd_line = [ema_fast[diff+i] - ema_slow[i] for i in range(len(ema_slow))]
    if len(macd_line) < signal: return None, None, None
    sig_line = calc_ema(macd_line, signal)
    hist = macd_line[-1] - sig_line[-1] if sig_line else 0
    return round(macd_line[-1], 2), round(sig_line[-1], 2), round(hist, 2)

def calc_bollinger(closes, period=20, num_std=2):
    if len(closes) < period: return None, None, None
    sma = sum(closes[-period:]) / period
    std = statistics.stdev(closes[-period:]) if len(closes[-period:]) > 1 else 0
    return round(sma, 2), round(sma + num_std*std, 2), round(sma - num_std*std, 2)

def calc_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1: return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    if len(trs) < period: return None
    atr = sum(trs[:period])/period
    for i in range(period, len(trs)):
        atr = (atr*(period-1) + trs[i])/period
    return round(atr, 2)

def calc_volatility(closes):
    if len(closes) < 5: return 0
    rets = [(closes[i]/closes[i-1]-1) for i in range(1,len(closes)) if closes[i-1]>0]
    if not rets: return 0
    m = sum(rets)/len(rets)
    v = sum((r-m)**2 for r in rets)/len(rets)
    return round(math.sqrt(v)*math.sqrt(252)*100, 1)

def calc_beta(stock_returns, market_returns):
    if len(stock_returns) < 5 or len(market_returns) < 5: return None
    n = min(len(stock_returns), len(market_returns))
    sr = stock_returns[-n:]
    mr = market_returns[-n:]
    m_mean = sum(mr)/n
    s_mean = sum(sr)/n
    cov = sum((sr[i]-s_mean)*(mr[i]-m_mean) for i in range(n))/n
    var = sum((mr[i]-m_mean)**2 for i in range(n))/n
    if var == 0: return None
    return round(cov/var, 2)

def calc_max_drawdown(closes):
    if not closes: return 0, 0, 0
    peak = closes[0]
    max_dd = 0
    peak_idx = 0
    trough_idx = 0
    for i, c in enumerate(closes):
        if c > peak:
            peak = c
            peak_idx = i
        dd = (peak - c) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
            trough_idx = i
    return round(max_dd, 2), peak_idx, trough_idx

def calc_sharpe(returns, risk_free_annual=0.06):
    if len(returns) < 5: return None
    rf_daily = risk_free_annual / 252
    excess = [r - rf_daily for r in returns]
    mean_excess = sum(excess)/len(excess)
    if len(excess) < 2: return None
    std = statistics.stdev(excess)
    if std == 0: return None
    return round(mean_excess/std * math.sqrt(252), 2)

def calc_adx(highs, lows, closes, period=14):
    if len(closes) < period*2: return None
    plus_dm = []; minus_dm = []; tr_list = []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        plus_dm.append(up if up > down and up > 0 else 0)
        minus_dm.append(down if down > up and down > 0 else 0)
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        tr_list.append(tr)
    if len(tr_list) < period: return None
    atr = sum(tr_list[:period])/period
    plus_di = sum(plus_dm[:period])/period
    minus_di = sum(minus_dm[:period])/period
    for i in range(period, len(tr_list)):
        atr = (atr*(period-1)+tr_list[i])/period
        plus_di = (plus_di*(period-1)+plus_dm[i])/period
        minus_di = (minus_di*(period-1)+minus_dm[i])/period
    if atr == 0: return None
    plus_di_val = plus_di/atr*100
    minus_di_val = minus_di/atr*100
    dx = abs(plus_di_val-minus_di_val)/(plus_di_val+minus_di_val)*100 if (plus_di_val+minus_di_val) > 0 else 0
    return round(dx, 1)

def calc_vwap_deviation(ltp, avg_price):
    if avg_price <= 0: return 0
    return round((ltp - avg_price) / avg_price * 100, 2)

def calc_relative_strength(stock_change, index_change):
    return round(stock_change - index_change, 2)

def detect_divergence(closes, period=14):
    if len(closes) < period * 3: return "none"
    mid = len(closes) // 2
    rsi1 = calc_rsi(closes[:mid+5], period)
    rsi2 = calc_rsi(closes, period)
    if rsi1 is None or rsi2 is None: return "none"
    price_higher = closes[-1] > closes[mid]
    rsi_higher = rsi2 > rsi1
    if price_higher and not rsi_higher: return "bearish"
    if not price_higher and rsi_higher: return "bullish"
    return "none"

def calc_support_resistance(highs, lows, closes, num_levels=3):
    if not closes: return [], []
    h = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    l = min(lows[-20:]) if len(lows) >= 20 else min(lows)
    c = closes[-1]
    pivot = (h + l + c) / 3
    r1 = 2*pivot - l; s1 = 2*pivot - h
    r2 = pivot + (h - l); s2 = pivot - (h - l)
    r3 = h + 2*(pivot - l); s3 = l - 2*(h - pivot)
    return ([round(r1,2),round(r2,2),round(r3,2)], [round(s1,2),round(s2,2),round(s3,2)])

def fetch_historical_for(session, sym, from_date, to_date, interval="day"):
    info = SYM.get(sym)
    if not info: return None, f"{sym}: not found"
    # Check cache first
    ck = f"hist:{session['api_key']}:{sym}:{from_date}:{to_date}:{interval}"
    cached = cache_get(ck)
    if cached is not None:
        return cached, None
    try:
        r = requests.get(
            f"{KITE}/instruments/historical/{info['token']}/{interval}",
            params={"from": f"{from_date} 09:15:00", "to": f"{to_date} 15:30:00"},
            headers=kh_for(session), timeout=15)
        r.raise_for_status()
        candles = r.json().get("data", {}).get("candles", [])
        if not candles: return None, f"{sym}: no data"
        cache_set(ck, candles, CACHE_TTL_HISTORICAL)
        return candles, None
    except Exception as e:
        return None, f"{sym}: {str(e)[:80]}"

def parse_candles(candles):
    o = [c[1] for c in candles]; h = [c[2] for c in candles]
    l = [c[3] for c in candles]; cl = [c[4] for c in candles]
    v = [c[5] for c in candles]; ts = [c[0] for c in candles]
    return o, h, l, cl, v, ts

def calc_returns(closes):
    return [(closes[i]/closes[i-1]-1) for i in range(1, len(closes)) if closes[i-1] > 0]

# ═══════════════════════════════════════════════════════════
#  HANDLERS (Multi-User)
# ═══════════════════════════════════════════════════════════
class Base(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin","*")
        self.set_header("Access-Control-Allow-Headers","*")
        self.set_header("Access-Control-Allow-Methods","*")
        self.set_header("Content-Type","application/json")
    def options(self, *a): self.set_status(204); self.finish()
    def err(self, code, msg):
        self.set_status(code); self.write({"error": msg})
    def body(self):
        try: return json.loads(self.request.body or "{}")
        except: return {}
    def get_user_session(self):
        """Extract session from cookie or header"""
        sid = self.get_cookie("pp_session") or self.request.headers.get("X-Session-Id","")
        if not sid: return None
        return get_session(sid)
    def require_session(self):
        """Get session or send 401"""
        s = self.get_user_session()
        if not s or not s.get("token"):
            self.err(401, "Not authenticated. Please login.")
            return None
        return s
    def write_json_gzip(self, data):
        """Write JSON response with gzip compression for large payloads."""
        raw = json.dumps(data) if not isinstance(data, (str, bytes)) else data
        if isinstance(raw, str):
            raw_bytes = raw.encode("utf-8")
        else:
            raw_bytes = raw
        accept_enc = self.request.headers.get("Accept-Encoding", "")
        if len(raw_bytes) >= GZIP_MIN_SIZE and "gzip" in accept_enc:
            compressed = gzip_compress(raw_bytes)
            self.set_header("Content-Encoding", "gzip")
            self.set_header("Content-Length", str(len(compressed)))
            self.write(compressed)
        else:
            self.write(raw_bytes)

class IndexPage(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type","text/html")
        with open(os.path.join(DIR,"nse_terminal_pro.html"),"r",encoding="utf-8") as f:
            self.write(f.read())

class StaticFileHandler(tornado.web.RequestHandler):
    """Serve PWA static files"""
    MIME = {".json":"application/json",".js":"application/javascript",".png":"image/png",".ico":"image/x-icon"}
    def get(self, path):
        fpath = os.path.join(DIR, path)
        if not os.path.isfile(fpath):
            self.set_status(404); self.write("Not found"); return
        ext = os.path.splitext(path)[1]
        self.set_header("Content-Type", self.MIME.get(ext, "application/octet-stream"))
        if ext == ".png":
            self.set_header("Cache-Control", "public, max-age=86400")
            with open(fpath, "rb") as f: self.write(f.read())
        else:
            with open(fpath, "r", encoding="utf-8") as f: self.write(f.read())

# ── Health Endpoint (Render keep-alive) ───────────────────

class HealthHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type","application/json")
        self.write(json.dumps({"status":"ok","uptime":round(time.time()-_server_start,1),"sessions":len(sessions),"instruments":len(SYM)}))

# ── License Key Verification ──────────────────────────────

class LicenseVerifyHandler(Base):
    def post(self):
        b = self.body()
        key = b.get("license_key", "").strip().upper()
        if not key:
            return self.err(400, "License key required")
        valid, msg = verify_license(key)
        if valid:
            self.set_cookie("pp_license", key, expires_days=30, httponly=True, samesite="Lax")
            self.write({"valid": True, "message": msg})
        else:
            self.write({"valid": False, "error": msg})

class LicenseAdminHandler(Base):
    """Admin endpoint to manage license keys — requires master key"""
    def post(self):
        b = self.body()
        master = b.get("master_key", "").strip()
        if master != MASTER_KEY:
            return self.err(403, "Invalid master key")
        action = b.get("action", "")
        data = load_licenses()
        if action == "list":
            self.write({"keys": data["keys"]})
        elif action == "add":
            new_key = b.get("key", f"PULSE-PRO-{uuid.uuid4().hex[:6].upper()}")
            data["keys"][new_key] = {
                "active": True, "user": b.get("user", ""),
                "created": datetime.now().strftime("%Y-%m-%d"), "uses": 0
            }
            save_licenses(data)
            self.write({"ok": True, "key": new_key})
        elif action == "deactivate":
            key = b.get("key", "")
            if key in data["keys"]:
                data["keys"][key]["active"] = False
                save_licenses(data)
                self.write({"ok": True, "deactivated": key})
            else:
                self.err(404, "Key not found")
        elif action == "activate":
            key = b.get("key", "")
            if key in data["keys"]:
                data["keys"][key]["active"] = True
                save_licenses(data)
                self.write({"ok": True, "activated": key})
            else:
                self.err(404, "Key not found")
        elif action == "delete":
            key = b.get("key", "")
            if key in data["keys"]:
                del data["keys"][key]
                save_licenses(data)
                self.write({"ok": True, "deleted": key})
            else:
                self.err(404, "Key not found")
        else:
            self.err(400, "Invalid action. Use: list, add, deactivate, activate, delete")

class LicenseCheckHandler(Base):
    """Check if current session has valid license"""
    def get(self):
        key = self.get_cookie("pp_license", "")
        if not key:
            self.write({"licensed": False})
            return
        valid, msg = verify_license(key)
        self.write({"licensed": valid, "key": key[:10] + "..."})

# ── Auth (Multi-User) ────────────────────────────────────

class AuthHandler(Base):
    def post(self):
        b = self.body()
        # Check license first
        license_key = self.get_cookie("pp_license", "")
        if not license_key:
            license_key = b.get("license_key", "").strip().upper()
        if license_key:
            valid, _ = verify_license(license_key)
            if not valid:
                return self.err(403, "Invalid or expired license key. Contact Kanishk Arora.")
        else:
            return self.err(403, "License key required. Contact Kanishk Arora to get one.")
        ak = b.get("api_key","").strip()
        at = b.get("access_token","").strip()
        if not ak or not at:
            return self.err(400, "Need api_key and access_token")
        try:
            r = requests.get(f"{KITE}/quote", params={"i":"NSE:RELIANCE"},
                             headers={"Authorization":f"token {ak}:{at}","X-Kite-Version":"3"}, timeout=10)
            if r.status_code == 200:
                sid = create_session(ak, token=at)
                self.set_cookie("pp_session", sid, expires_days=1, httponly=True, samesite="Lax")
                session = get_session(sid)
                if not instruments_loaded:
                    tornado.ioloop.IOLoop.current().run_in_executor(pool, download_instruments, session)
                self.write({"valid": True, "session": sid})
            else:
                try: msg = r.json().get("message","Auth failed")
                except: msg = f"HTTP {r.status_code}"
                self.write({"valid": False, "error": msg})
        except Exception as e:
            self.write({"valid": False, "error": str(e)})

class LoginRedirect(tornado.web.RequestHandler):
    def get(self):
        # Check license
        license_key = self.get_cookie("pp_license", "")
        if not license_key:
            self.set_status(403)
            self.set_header("Content-Type", "text/html")
            self.write("<h2>License Required</h2><p>Please enter a valid license key first.</p>")
            return
        valid, _ = verify_license(license_key)
        if not valid:
            self.set_status(403)
            self.set_header("Content-Type", "text/html")
            self.write("<h2>Invalid License</h2><p>Your license key is invalid or expired. Contact Kanishk Arora.</p>")
            return
        ak = self.get_argument("api_key","")
        sec = self.get_argument("api_secret","")
        if not ak or not sec:
            self.set_status(400); self.write("Need api_key and api_secret"); return
        # Store pending OAuth in a temporary session
        sid = create_session(ak, secret=sec)
        self.set_cookie("pp_pending", sid, expires_days=1, httponly=True, samesite="Lax")
        self.redirect(f"https://kite.zerodha.com/connect/login?v=3&api_key={ak}")

class CallbackHandler(tornado.web.RequestHandler):
    def get(self):
        rt = self.get_argument("request_token","")
        pending_sid = self.get_cookie("pp_pending")
        if not rt or not pending_sid:
            self.set_header("Content-Type","text/html")
            self.write("<h2>Error</h2><p>Missing credentials. Please try again.</p>"); return
        session = get_session(pending_sid)
        if not session or not session["api_key"] or not session["secret"]:
            self.set_header("Content-Type","text/html")
            self.write("<h2>Error</h2><p>Session expired. Please try again.</p>"); return
        cs = hashlib.sha256(f"{session['api_key']}{rt}{session['secret']}".encode()).hexdigest()
        try:
            r = requests.post(f"{KITE}/session/token", data={
                "api_key": session["api_key"], "request_token": rt, "checksum": cs
            })
            d = r.json()
            if d.get("status") == "success" and d.get("data",{}).get("access_token"):
                session["token"] = d["data"]["access_token"]
                self.set_cookie("pp_session", pending_sid, expires_days=1, httponly=True, samesite="Lax")
                self.clear_cookie("pp_pending")
                if not instruments_loaded:
                    tornado.ioloop.IOLoop.current().run_in_executor(pool, download_instruments, session)
                self.redirect("/")
            else:
                delete_session(pending_sid)
                self.set_header("Content-Type","text/html")
                self.write(f"<h2>Login failed</h2><pre>{json.dumps(d,indent=2)}</pre>")
        except Exception as e:
            self.set_header("Content-Type","text/html")
            self.write(f"<h2>Error</h2><pre>{e}</pre>")

class LogoutHandler(Base):
    def post(self):
        sid = self.get_cookie("pp_session")
        if sid: delete_session(sid)
        self.clear_cookie("pp_session")
        self.write({"ok": True})

class StatusHandler(Base):
    def get(self):
        s = self.get_user_session()
        self.write(json.dumps({
            "ok": bool(s and s.get("token")),
            "instruments": len(SYM), "nfo": len(NFO),
            "indices": list(INDICES.keys()),
            "activeSessions": len(sessions),
            "maxSessions": MAX_SESSIONS,
        }))

# ── Live Market Data ──────────────────────────────────────

class QuoteHandler(Base):
    def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        syms = b.get("symbols",[])
        if not syms: return self.err(400,"No symbols")
        # Cache key based on user + sorted symbols
        ck = f"quote:{s['api_key']}:{','.join(sorted(syms))}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip({"data": cached})
            return
        try:
            all_data = {}
            for i in range(0, len(syms), 100):
                batch = syms[i:i+100]
                params = [("i",f"NSE:{sym}") for sym in batch]
                r = requests.get(f"{KITE}/quote", params=params, headers=kh_for(s), timeout=15)
                r.raise_for_status()
                d = r.json().get("data",{})
                all_data.update(d)
            cache_set(ck, all_data, CACHE_TTL_QUOTE)
            self.write_json_gzip({"data": all_data})
        except Exception as e:
            self.err(500, str(e))

class IndicesHandler(Base):
    def get(self):
        s = self.require_session()
        if not s: return
        ck = f"indices:{s['api_key']}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return
        try:
            idx = ["NIFTY 50","NIFTY BANK","INDIA VIX","NIFTY IT",
                   "NIFTY FIN SERVICE","NIFTY MIDCAP 50","NIFTY NEXT 50",
                   "NIFTY PHARMA","NIFTY AUTO","NIFTY METAL","NIFTY ENERGY",
                   "NIFTY FMCG","NIFTY REALTY"]
            params = [("i",f"NSE:{i}") for i in idx]
            r = requests.get(f"{KITE}/quote", params=params, headers=kh_for(s), timeout=15)
            r.raise_for_status()
            result = r.json()
            cache_set(ck, result, CACHE_TTL_QUOTE)
            self.write_json_gzip(result)
        except Exception as e:
            self.err(500, str(e))

class HistHandler(Base):
    def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        sym = b.get("symbol",""); fr = b.get("from",""); to = b.get("to","")
        interval = b.get("interval","day")
        if not sym or not fr or not to: return self.err(400,"Need symbol, from, to")
        info = SYM.get(sym)
        if not info: return self.err(404,f"Unknown: {sym}")
        ck = f"histapi:{s['api_key']}:{sym}:{fr}:{to}:{interval}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return
        try:
            r = requests.get(
                f"{KITE}/instruments/historical/{info['token']}/{interval}",
                params={"from":f"{fr} 09:15:00","to":f"{to} 15:30:00"},
                headers=kh_for(s), timeout=15)
            r.raise_for_status()
            result = r.json()
            cache_set(ck, result, CACHE_TTL_HISTORICAL)
            self.write_json_gzip(result)
        except Exception as e:
            self.err(500, str(e))

# ── INSTITUTIONAL SCREENER ────────────────────────────────

class ScreenerHandler(Base):
    async def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        syms = b.get("symbols",[])
        fr = b.get("from",""); to = b.get("to","")
        index_name = b.get("index","NIFTY50")
        if not syms and index_name:
            syms = INDICES.get(index_name, NIFTY50)
        if not fr or not to: return self.err(400,"Need from and to dates")

        # Check screener-level cache
        ck = f"screener:{s['api_key']}:{index_name}:{','.join(sorted(syms))}:{fr}:{to}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return

        loop = tornado.ioloop.IOLoop.current()
        nifty_candles, _ = await loop.run_in_executor(pool, fetch_historical_for, s, "NIFTY 50", fr, to)
        nifty_returns = []; nifty_change = 0
        if nifty_candles:
            _, _, _, ncl, _, _ = parse_candles(nifty_candles)
            nifty_returns = calc_returns(ncl)
            if ncl and ncl[0] > 0: nifty_change = (ncl[-1] - ncl[0]) / ncl[0] * 100

        results, errors = [], []

        def fetch_and_analyze(sym):
            candles, err = fetch_historical_for(s, sym, fr, to)
            if err: return None, err
            o, h, l, cl, v, ts = parse_candles(candles)
            if not cl or len(cl) < 2: return None, f"{sym}: insufficient data"
            sp, ep = o[0], cl[-1]
            chg = ((ep - sp) / sp * 100) if sp > 0 else 0
            period_high = max(h); period_low = min(l)
            dd_high = ((period_high - ep) / period_high * 100) if period_high > 0 else 0
            rec_low = ((ep - period_low) / period_low * 100) if period_low > 0 else 0
            rsi = calc_rsi(cl)
            macd_val, macd_sig, macd_hist = calc_macd(cl)
            bb_mid, bb_upper, bb_lower = calc_bollinger(cl)
            atr = calc_atr(h, l, cl); vol = calc_volatility(cl); adx = calc_adx(h, l, cl)
            max_dd, _, _ = calc_max_drawdown(cl)
            stock_returns = calc_returns(cl)
            sharpe = calc_sharpe(stock_returns)
            beta = calc_beta(stock_returns, nifty_returns) if nifty_returns else None
            rel_strength = calc_relative_strength(chg, nifty_change) if nifty_change else None
            divergence = detect_divergence(cl)
            resistances, supports = calc_support_resistance(h, l, cl)
            sma20 = sum(cl[-20:])/min(len(cl),20) if len(cl)>=1 else ep
            sma50 = sum(cl[-50:])/min(len(cl),50) if len(cl)>=1 else ep
            sma200 = sum(cl[-200:])/min(len(cl),200) if len(cl)>=1 else ep
            avg_vol = round(sum(v)/len(v)) if v else 0
            vol_trend = 0
            if len(v) >= 10:
                recent = sum(v[-5:])/5; older = sum(v[:-5])/max(len(v)-5,1)
                vol_trend = round((recent/older-1)*100,1) if older>0 else 0
            abs_change = abs(ep-sp)
            path_length = sum(abs(cl[i]-cl[i-1]) for i in range(1,len(cl)))
            efficiency = round(abs_change/path_length*100,1) if path_length>0 else 0
            return {
                "symbol":sym,"sector":STOCK_SECTOR.get(sym,"Other"),
                "startPrice":round(sp,2),"endPrice":round(ep,2),"change":round(chg,2),
                "periodHigh":round(period_high,2),"periodLow":round(period_low,2),
                "drawdownFromHigh":round(dd_high,2),"recoveryFromLow":round(rec_low,2),
                "rsi":rsi,"macd":macd_val,"macdSignal":macd_sig,"macdHist":macd_hist,
                "bbMid":bb_mid,"bbUpper":bb_upper,"bbLower":bb_lower,
                "atr":atr,"adx":adx,"volatility":vol,"maxDrawdown":max_dd,
                "sharpe":sharpe,"beta":beta,"relativeStrength":rel_strength,
                "divergence":divergence,"supports":supports,"resistances":resistances,
                "aboveSMA20":ep>sma20,"aboveSMA50":ep>sma50 if len(cl)>=50 else None,
                "aboveSMA200":ep>sma200 if len(cl)>=200 else None,
                "avgVolume":avg_vol,"totalVolume":sum(v),"volumeTrend":vol_trend,
                "efficiency":efficiency,"dataPoints":len(candles),
                "sparkline":cl[-30:] if len(cl)>=30 else cl,
            }, None

        for i in range(0, len(syms), 5):
            batch = syms[i:i+5]
            futs = [loop.run_in_executor(pool, fetch_and_analyze, sym) for sym in batch]
            ress = await tornado.gen.multi(futs)
            for r, e in ress:
                if r: results.append(r)
                if e: errors.append(e)
            if i+5 < len(syms): await tornado.gen.sleep(0.15)

        changes = [r["change"] for r in results]
        avg_change = round(sum(changes)/len(changes),2) if changes else 0
        advances = sum(1 for c in changes if c>0.05)
        declines = sum(1 for c in changes if c<-0.05)
        response = {
            "count":len(results),"fromDate":fr,"toDate":to,"indexChange":avg_change,
            "advances":advances,"declines":declines,"unchanged":len(changes)-advances-declines,
            "stocks":results,"errors":errors,
        }
        cache_set(ck, response, CACHE_TTL_SCREENER)
        self.write_json_gzip(response)

# ── SMART MONEY FLOW ──────────────────────────────────────

class SmartMoneyHandler(Base):
    def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        syms = b.get("symbols", NIFTY50)
        ck = f"smartmoney:{s['api_key']}:{','.join(sorted(syms))}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return
        try:
            all_data = {}
            for i in range(0, len(syms), 100):
                batch = syms[i:i+100]
                params = [("i",f"NSE:{sym}") for sym in batch]
                r = requests.get(f"{KITE}/quote", params=params, headers=kh_for(s), timeout=15)
                r.raise_for_status()
                d = r.json().get("data",{})
                all_data.update(d)
            results = []; total_buy = total_sell = 0
            for key, info in all_data.items():
                sym = key.replace("NSE:","")
                ltp = info.get("last_price",0); vol = info.get("volume",0)
                avg_price = info.get("average_price",0)
                ohlc = info.get("ohlc",{}); prev = ohlc.get("close",0)
                buy_qty = info.get("buy_quantity",0); sell_qty = info.get("sell_quantity",0)
                oi = info.get("oi",0)
                chg = ((ltp-prev)/prev*100) if prev>0 else 0
                vwap_dev = calc_vwap_deviation(ltp, avg_price)
                total_buy += buy_qty; total_sell += sell_qty
                bs_ratio = round(buy_qty/sell_qty,2) if sell_qty>0 else 0
                if chg>0 and vwap_dev>0 and bs_ratio>1.2: signal="STRONG_ACCUMULATION"
                elif chg>0 and bs_ratio>1: signal="ACCUMULATION"
                elif chg<0 and vwap_dev<0 and bs_ratio<0.8: signal="STRONG_DISTRIBUTION"
                elif chg<0 and bs_ratio<1: signal="DISTRIBUTION"
                else: signal="NEUTRAL"
                score = 0
                score += min(max((bs_ratio-1)*30,-30),30)
                score += min(max(vwap_dev*5,-30),30)
                score += min(max(chg*3,-20),20)
                if vol>0 and avg_price>0: score += min(max((ltp/avg_price-1)*50,-20),20)
                depth = info.get("depth",{})
                buy_depth = depth.get("buy",[])
                sell_depth = depth.get("sell",[])
                buy_wall = sum(b.get("quantity",0) for b in buy_depth[:3]) if buy_depth else 0
                sell_wall = sum(sd.get("quantity",0) for sd in sell_depth[:3]) if sell_depth else 0
                wall_ratio = round(buy_wall/sell_wall,2) if sell_wall>0 else 0
                results.append({
                    "symbol":sym,"sector":STOCK_SECTOR.get(sym,"Other"),
                    "ltp":ltp,"change":round(chg,2),"volume":vol,"avgPrice":round(avg_price,2),
                    "vwapDeviation":vwap_dev,"buyQty":buy_qty,"sellQty":sell_qty,
                    "bsRatio":bs_ratio,"oi":oi,"signal":signal,"smartScore":round(score,1),
                    "buyWall":buy_wall,"sellWall":sell_wall,"wallRatio":wall_ratio,
                    "high":ohlc.get("high",0),"low":ohlc.get("low",0),
                    "open":ohlc.get("open",0),"prevClose":prev,
                })
            market_bs = round(total_buy/total_sell,2) if total_sell>0 else 0
            response = {"stocks":results,"marketBSRatio":market_bs,
                "totalBuyQty":total_buy,"totalSellQty":total_sell}
            cache_set(ck, response, CACHE_TTL_QUOTE)
            self.write_json_gzip(response)
        except Exception as e:
            self.err(500, str(e))

# ── MOMENTUM SCANNER ──────────────────────────────────────

class MomentumHandler(Base):
    async def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        syms = b.get("symbols", NIFTY50)
        loop = tornado.ioloop.IOLoop.current()
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now()-timedelta(days=365)).strftime("%Y-%m-%d")

        # Check cache
        ck = f"momentum:{s['api_key']}:{','.join(sorted(syms))}:{from_date}:{to_date}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return

        results, errors = [], []

        def analyze_momentum(sym):
            candles, err = fetch_historical_for(s, sym, from_date, to_date)
            if err: return None, err
            o, h, l, cl, v, ts = parse_candles(candles)
            if len(cl)<20: return None, f"{sym}: insufficient data"
            ep = cl[-1]; high_52w = max(h); low_52w = min(l)
            dist_52h = round((high_52w-ep)/high_52w*100,2) if high_52w>0 else 0
            dist_52l = round((ep-low_52w)/low_52w*100,2) if low_52w>0 else 0
            rsi = calc_rsi(cl)
            macd_val, macd_sig, macd_hist = calc_macd(cl)
            adx = calc_adx(h, l, cl)
            sma20 = sum(cl[-20:])/20 if len(cl)>=20 else ep
            sma50 = sum(cl[-50:])/50 if len(cl)>=50 else ep
            sma200 = sum(cl[-200:])/200 if len(cl)>=200 else ep
            cross_signal = "none"
            if len(cl)>=200:
                prev_sma50 = sum(cl[-51:-1])/50; prev_sma200 = sum(cl[-201:-1])/200
                if prev_sma50<prev_sma200 and sma50>sma200: cross_signal="golden_cross"
                elif prev_sma50>prev_sma200 and sma50<sma200: cross_signal="death_cross"
            gap=0; gap_type="none"
            if len(o)>=2 and len(cl)>=2:
                gap = round((o[-1]-cl[-2])/cl[-2]*100,2)
                if gap>1: gap_type="gap_up"
                elif gap<-1: gap_type="gap_down"
            trend_score = 0
            if rsi and rsi>50: trend_score+=20
            if rsi and rsi>60: trend_score+=10
            if ep>sma20: trend_score+=15
            if len(cl)>=50 and ep>sma50: trend_score+=20
            if len(cl)>=200 and ep>sma200: trend_score+=25
            if macd_hist and macd_hist>0: trend_score+=10
            if adx and adx>25: trend_score+=10
            if rsi and rsi<40: trend_score-=20
            if rsi and rsi<30: trend_score-=10
            if ep<sma20: trend_score-=15
            if len(cl)>=50 and ep<sma50: trend_score-=20
            if len(cl)>=200 and ep<sma200: trend_score-=25
            chg_1w = round((ep/cl[-6]-1)*100,2) if len(cl)>=6 else 0
            chg_1m = round((ep/cl[-22]-1)*100,2) if len(cl)>=22 else 0
            chg_3m = round((ep/cl[-66]-1)*100,2) if len(cl)>=66 else 0
            chg_6m = round((ep/cl[-132]-1)*100,2) if len(cl)>=132 else 0
            chg_1y = round((ep/cl[0]-1)*100,2) if cl[0]>0 else 0
            divergence = detect_divergence(cl)
            return {
                "symbol":sym,"sector":STOCK_SECTOR.get(sym,"Other"),"ltp":round(ep,2),
                "high52w":round(high_52w,2),"low52w":round(low_52w,2),
                "distFrom52wHigh":dist_52h,"distFrom52wLow":dist_52l,
                "rsi":rsi,"macd":macd_val,"macdHist":macd_hist,"adx":adx,
                "trendScore":trend_score,"aboveSMA20":ep>sma20,
                "aboveSMA50":ep>sma50 if len(cl)>=50 else None,
                "aboveSMA200":ep>sma200 if len(cl)>=200 else None,
                "sma20":round(sma20,2),"sma50":round(sma50,2) if len(cl)>=50 else None,
                "sma200":round(sma200,2) if len(cl)>=200 else None,
                "crossSignal":cross_signal,"gap":gap,"gapType":gap_type,
                "divergence":divergence,
                "chg1w":chg_1w,"chg1m":chg_1m,"chg3m":chg_3m,"chg6m":chg_6m,"chg1y":chg_1y,
            }, None

        for i in range(0, len(syms), 5):
            batch = syms[i:i+5]
            futs = [loop.run_in_executor(pool, analyze_momentum, sym) for sym in batch]
            ress = await tornado.gen.multi(futs)
            for r, e in ress:
                if r: results.append(r)
                if e: errors.append(e)
            if i+5 < len(syms): await tornado.gen.sleep(0.15)

        response = {"count":len(results),"stocks":results,"errors":errors}
        cache_set(ck, response, CACHE_TTL_SCREENER)
        self.write_json_gzip(response)

# ── SECTOR ROTATION ───────────────────────────────────────

class SectorRotationHandler(Base):
    async def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        fr = b.get("from",(datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d"))
        to = b.get("to",datetime.now().strftime("%Y-%m-%d"))

        ck = f"sectors:{s['api_key']}:{fr}:{to}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return

        loop = tornado.ioloop.IOLoop.current()
        sector_data = {}
        for sector, syms in _sector_map.items():
            sector_results = []
            for i in range(0, len(syms), 5):
                batch = syms[i:i+5]
                futs = [loop.run_in_executor(pool, fetch_historical_for, s, sym, fr, to) for sym in batch]
                ress = await tornado.gen.multi(futs)
                for idx, (candles, err) in enumerate(ress):
                    sym = batch[idx]
                    if candles:
                        o, h, l, cl, v, ts = parse_candles(candles)
                        if cl and cl[0]>0:
                            chg = (cl[-1]-cl[0])/cl[0]*100
                            vol_avg = sum(v)/len(v) if v else 0
                            sector_results.append({"symbol":sym,"change":round(chg,2),"ltp":round(cl[-1],2),"avgVolume":round(vol_avg)})
                if i+5 < len(syms): await tornado.gen.sleep(0.15)
            if sector_results:
                avg_chg = sum(r["change"] for r in sector_results)/len(sector_results)
                adv = sum(1 for r in sector_results if r["change"]>0)
                dec = sum(1 for r in sector_results if r["change"]<0)
                breadth = round(adv/(adv+dec)*100,1) if (adv+dec)>0 else 50
                sector_data[sector] = {"avgChange":round(avg_chg,2),"advances":adv,"declines":dec,"breadth":breadth,
                    "stocks":sorted(sector_results, key=lambda x: x["change"], reverse=True)}
        sorted_sectors = sorted(sector_data.items(), key=lambda x: x[1]["avgChange"], reverse=True)
        response = {"fromDate":fr,"toDate":to,"sectors":{k:v for k,v in sorted_sectors}}
        cache_set(ck, response, CACHE_TTL_SCREENER)
        self.write_json_gzip(response)

# ── CORRELATION MATRIX ────────────────────────────────────

class CorrelationHandler(Base):
    async def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        syms = b.get("symbols", NIFTY50[:15])[:20]
        fr = b.get("from",(datetime.now()-timedelta(days=90)).strftime("%Y-%m-%d"))
        to = b.get("to",datetime.now().strftime("%Y-%m-%d"))

        ck = f"corr:{s['api_key']}:{','.join(sorted(syms))}:{fr}:{to}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return

        loop = tornado.ioloop.IOLoop.current()
        returns_map = {}
        for i in range(0, len(syms), 5):
            batch = syms[i:i+5]
            futs = [loop.run_in_executor(pool, fetch_historical_for, s, sym, fr, to) for sym in batch]
            ress = await tornado.gen.multi(futs)
            for idx, (candles, err) in enumerate(ress):
                sym = batch[idx]
                if candles:
                    _, _, _, cl, _, _ = parse_candles(candles)
                    returns_map[sym] = calc_returns(cl)
            if i+5 < len(syms): await tornado.gen.sleep(0.15)
        valid_syms = [sym for sym in syms if sym in returns_map]
        matrix = []
        for s1 in valid_syms:
            row = []
            r1 = returns_map[s1]
            for s2 in valid_syms:
                r2 = returns_map[s2]
                n = min(len(r1),len(r2))
                if n<5: row.append(0); continue
                a, b_r = r1[-n:], r2[-n:]
                ma, mb = sum(a)/n, sum(b_r)/n
                cov = sum((a[i]-ma)*(b_r[i]-mb) for i in range(n))/n
                sa = math.sqrt(sum((a[i]-ma)**2 for i in range(n))/n)
                sb = math.sqrt(sum((b_r[i]-mb)**2 for i in range(n))/n)
                corr = cov/(sa*sb) if sa*sb>0 else 0
                row.append(round(corr,3))
            matrix.append(row)
        response = {"symbols":valid_syms,"matrix":matrix}
        cache_set(ck, response, CACHE_TTL_SCREENER)
        self.write_json_gzip(response)

# ── BREADTH ───────────────────────────────────────────────

class BreadthHandler(Base):
    def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        syms = b.get("symbols", NIFTY50)
        ck = f"breadth:{s['api_key']}:{','.join(sorted(syms))}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return
        try:
            all_data = {}
            for i in range(0, len(syms), 100):
                batch = syms[i:i+100]
                params = [("i",f"NSE:{sym}") for sym in batch]
                r = requests.get(f"{KITE}/quote", params=params, headers=kh_for(s), timeout=15)
                r.raise_for_status()
                d = r.json().get("data",{})
                all_data.update(d)
            adv=dec=unch=0; stocks=[]; sector_breadth={}
            for key, info in all_data.items():
                sym = key.replace("NSE:","")
                ltp = info.get("last_price",0)
                ohlc = info.get("ohlc",{}); prev = ohlc.get("close",0)
                vol = info.get("volume",0)
                buy_qty = info.get("buy_quantity",0); sell_qty = info.get("sell_quantity",0)
                chg = ((ltp-prev)/prev*100) if prev>0 else 0
                if chg>0.05: adv+=1
                elif chg<-0.05: dec+=1
                else: unch+=1
                sector = STOCK_SECTOR.get(sym,"Other")
                if sector not in sector_breadth: sector_breadth[sector]={"adv":0,"dec":0,"unch":0}
                if chg>0.05: sector_breadth[sector]["adv"]+=1
                elif chg<-0.05: sector_breadth[sector]["dec"]+=1
                else: sector_breadth[sector]["unch"]+=1
                stocks.append({"symbol":sym,"sector":sector,"ltp":ltp,"prevClose":prev,
                    "open":ohlc.get("open",0),"high":ohlc.get("high",0),
                    "low":ohlc.get("low",0),"change":round(chg,2),
                    "volume":vol,"buyQty":buy_qty,"sellQty":sell_qty})
            total = adv+dec+unch
            response = {"advances":adv,"declines":dec,"unchanged":unch,"total":total,
                "adRatio":round(adv/max(dec,1),2),"breadthPct":round(adv/max(total,1)*100,1),
                "sectorBreadth":sector_breadth,"stocks":sorted(stocks, key=lambda x: x["change"])}
            cache_set(ck, response, CACHE_TTL_QUOTE)
            self.write_json_gzip(response)
        except Exception as e:
            self.err(500, str(e))

# ── RISK MATRIX ───────────────────────────────────────────

class RiskHandler(Base):
    async def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        syms = b.get("symbols", NIFTY50[:20])[:30]
        loop = tornado.ioloop.IOLoop.current()
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now()-timedelta(days=365)).strftime("%Y-%m-%d")

        ck = f"risk:{s['api_key']}:{','.join(sorted(syms))}:{from_date}:{to_date}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return

        nifty_candles, _ = await loop.run_in_executor(pool, fetch_historical_for, s, "NIFTY 50", from_date, to_date)
        nifty_rets = []
        if nifty_candles:
            _, _, _, ncl, _, _ = parse_candles(nifty_candles)
            nifty_rets = calc_returns(ncl)
        results, errors, all_returns = [], [], {}

        def analyze_risk(sym):
            candles, err = fetch_historical_for(s, sym, from_date, to_date)
            if err: return None, err, None
            o, h, l, cl, v, ts = parse_candles(candles)
            if len(cl)<20: return None, f"{sym}: insufficient data", None
            returns = calc_returns(cl); vol = calc_volatility(cl)
            max_dd, _, _ = calc_max_drawdown(cl)
            beta = calc_beta(returns, nifty_rets) if nifty_rets else None
            sharpe = calc_sharpe(returns)
            sorted_rets = sorted(returns)
            var_idx = int(len(sorted_rets)*0.05)
            var_95 = round(sorted_rets[var_idx]*100,2) if var_idx<len(sorted_rets) else 0
            worst = sorted_rets[:max(var_idx,1)]
            cvar = round(sum(worst)/len(worst)*100,2) if worst else 0
            neg_rets = [r for r in returns if r<0]
            downside_dev = math.sqrt(sum(r**2 for r in neg_rets)/len(neg_rets)) if neg_rets else 0
            avg_ret = sum(returns)/len(returns) if returns else 0
            sortino = round(avg_ret/downside_dev*math.sqrt(252),2) if downside_dev>0 else None
            annual_ret = avg_ret*252*100
            calmar = round(annual_ret/max_dd,2) if max_dd>0 else None
            return {"symbol":sym,"sector":STOCK_SECTOR.get(sym,"Other"),"ltp":round(cl[-1],2),
                "annualReturn":round(annual_ret,2),"volatility":vol,"maxDrawdown":max_dd,
                "beta":beta,"sharpe":sharpe,"sortino":sortino,"calmar":calmar,
                "var95":var_95,"cvar95":cvar,"dataPoints":len(candles)}, None, returns

        for i in range(0, len(syms), 5):
            batch = syms[i:i+5]
            futs = [loop.run_in_executor(pool, analyze_risk, sym) for sym in batch]
            ress = await tornado.gen.multi(futs)
            for r, e, rets in ress:
                if r: results.append(r)
                if e: errors.append(e)
                if rets and r: all_returns[r["symbol"]] = rets
            if i+5 < len(syms): await tornado.gen.sleep(0.15)

        portfolio_var = 0
        if all_returns and len(all_returns)>1:
            n_stocks = len(all_returns)
            min_len = min(len(v) for v in all_returns.values())
            port_returns = []
            for i in range(min_len):
                daily = sum(list(all_returns.values())[j][-(min_len-i)] for j in range(n_stocks))/n_stocks
                port_returns.append(daily)
            sorted_port = sorted(port_returns)
            var_idx = int(len(sorted_port)*0.05)
            portfolio_var = round(sorted_port[var_idx]*100,2) if var_idx<len(sorted_port) else 0
        response = {"count":len(results),"stocks":results,"portfolioVaR95":portfolio_var,"errors":errors}
        cache_set(ck, response, CACHE_TTL_SCREENER)
        self.write_json_gzip(response)

# ── OPTION CHAIN ──────────────────────────────────────────

class OptionChainHandler(Base):
    def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        underlying = b.get("symbol","NIFTY"); expiry = b.get("expiry","")
        if not NFO: return self.err(503,"NFO instruments not loaded. Retry in a few seconds.")
        opts = [o for o in NFO if o["name"]==underlying and o["type"] in ("CE","PE")]
        if not opts: return self.err(404,f"No options for {underlying}")
        expiries = sorted(set(o["expiry"] for o in opts))
        if not expiry and expiries: expiry = expiries[0]
        chain = [o for o in opts if o["expiry"]==expiry]
        if not chain: return self.err(404,f"No options for expiry {expiry}")
        chain = sorted(chain, key=lambda x: x["strike"])
        if len(chain)>100:
            strikes = sorted(set(c["strike"] for c in chain))
            mid = len(strikes)//2
            keep = set(strikes[max(0,mid-25):mid+25])
            chain = [c for c in chain if c["strike"] in keep]
        ck = f"optchain:{s['api_key']}:{underlying}:{expiry}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return
        try:
            params = [("i",f"NFO:{o['symbol']}") for o in chain[:100]]
            r = requests.get(f"{KITE}/quote", params=params, headers=kh_for(s), timeout=15)
            r.raise_for_status()
            data = r.json().get("data",{})
            sm = {}
            for o in chain:
                key = f"NFO:{o['symbol']}"; info = data.get(key,{})
                strike = o["strike"]
                if strike not in sm: sm[strike]={"strike":strike}
                p = "ce" if o["type"]=="CE" else "pe"
                sm[strike][f"{p}_ltp"]=info.get("last_price",0)
                sm[strike][f"{p}_oi"]=info.get("oi",0)
                sm[strike][f"{p}_vol"]=info.get("volume",0)
                sm[strike][f"{p}_chg"]=info.get("net_change",0)
            result = sorted(sm.values(), key=lambda x: x["strike"])
            tce = sum(s.get("ce_oi",0) for s in result)
            tpe = sum(s.get("pe_oi",0) for s in result)
            pcr = tpe/max(tce,1)
            mp = 0; mpv = float('inf')
            for st in result:
                pain = 0
                for o in result:
                    if o["strike"]<st["strike"]: pain+=o.get("ce_oi",0)*(st["strike"]-o["strike"])
                    elif o["strike"]>st["strike"]: pain+=o.get("pe_oi",0)*(o["strike"]-st["strike"])
                if pain<mpv: mpv=pain; mp=st["strike"]
            ce_oi_list = [(st["strike"],st.get("ce_oi",0)) for st in result]
            pe_oi_list = [(st["strike"],st.get("pe_oi",0)) for st in result]
            max_ce_oi = max(ce_oi_list, key=lambda x: x[1]) if ce_oi_list else (0,0)
            max_pe_oi = max(pe_oi_list, key=lambda x: x[1]) if pe_oi_list else (0,0)
            response = {
                "underlying":underlying,"expiry":expiry,"expiries":expiries[:12],"chain":result,
                "totalCEOI":tce,"totalPEOI":tpe,"pcr":round(pcr,3),"maxPain":mp,
                "maxCEOIStrike":max_ce_oi[0],"maxPEOIStrike":max_pe_oi[0],
                "maxCEOI":max_ce_oi[1],"maxPEOI":max_pe_oi[1],
            }
            cache_set(ck, response, CACHE_TTL_QUOTE)
            self.write_json_gzip(response)
        except Exception as e:
            self.err(500, str(e))

# ── INDEX CONSTITUENTS ────────────────────────────────────

class IndexConstituentsHandler(Base):
    def get(self):
        index = self.get_argument("index","NIFTY50")
        syms = INDICES.get(index, NIFTY50)
        self.write(json.dumps({"index":index,"symbols":syms,"indices":list(INDICES.keys())}))

# ── AI ANALYSIS DASHBOARD ────────────────────────────────

CACHE_TTL_AI = 10  # seconds — meant to be called frequently

class AIAnalysisHandler(Base):
    async def post(self):
        s = self.require_session()
        if not s: return

        ck = f"aianalysis:{s['api_key']}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return

        loop = tornado.ioloop.IOLoop.current()
        response = {"timestamp": datetime.now().isoformat()}

        # ── Helper: fetch quotes in batch ──
        def _fetch_quotes(params_list, session):
            """Fetch quotes from Kite given list of (i, instrument_string) params."""
            all_data = {}
            for i in range(0, len(params_list), 100):
                batch = params_list[i:i+100]
                try:
                    r = requests.get(f"{KITE}/quote", params=batch, headers=kh_for(session), timeout=15)
                    r.raise_for_status()
                    d = r.json().get("data", {})
                    all_data.update(d)
                except:
                    pass
            return all_data

        # ── 1. Live Index Data ──
        def fetch_indices(session):
            idx_names = ["NIFTY 50", "NIFTY BANK", "INDIA VIX", "NIFTY FIN SERVICE",
                         "NIFTY MIDCAP 50", "NIFTY NEXT 50"]
            params = [("i", f"NSE:{n}") for n in idx_names]
            params.append(("i", "BSE:SENSEX"))
            return _fetch_quotes(params, session)

        # ── 2. Option chain data for NIFTY / BANKNIFTY ──
        def fetch_option_data(underlying, session):
            """Return PCR stats for a given underlying from NFO option chain."""
            if not NFO:
                return None
            opts = [o for o in NFO if o["name"] == underlying and o["type"] in ("CE", "PE")]
            if not opts:
                return None
            expiries = sorted(set(o["expiry"] for o in opts))
            if not expiries:
                return None
            expiry = expiries[0]  # nearest expiry
            chain = [o for o in opts if o["expiry"] == expiry]
            chain = sorted(chain, key=lambda x: x["strike"])
            # Keep top 50 strikes (25 around ATM)
            if len(chain) > 100:
                strikes = sorted(set(c["strike"] for c in chain))
                mid = len(strikes) // 2
                keep = set(strikes[max(0, mid - 25):mid + 25])
                chain = [c for c in chain if c["strike"] in keep]
            # Fetch quotes for these option contracts
            params = [("i", f"NFO:{o['symbol']}") for o in chain[:100]]
            try:
                r = requests.get(f"{KITE}/quote", params=params, headers=kh_for(session), timeout=15)
                r.raise_for_status()
                data = r.json().get("data", {})
            except:
                return None
            total_ce_buy = total_pe_buy = 0
            total_ce_sell = total_pe_sell = 0
            net_ce_oi = net_pe_oi = 0
            strike_pain = {}
            for o in chain:
                key = f"NFO:{o['symbol']}"
                info = data.get(key, {})
                oi_val = info.get("oi", 0)
                buy_qty = info.get("buy_quantity", 0)
                sell_qty = info.get("sell_quantity", 0)
                strike = o["strike"]
                if strike not in strike_pain:
                    strike_pain[strike] = {"ce_oi": 0, "pe_oi": 0}
                if o["type"] == "CE":
                    total_ce_buy += buy_qty
                    total_ce_sell += sell_qty
                    net_ce_oi += oi_val
                    strike_pain[strike]["ce_oi"] = oi_val
                else:
                    total_pe_buy += buy_qty
                    total_pe_sell += sell_qty
                    net_pe_oi += oi_val
                    strike_pain[strike]["pe_oi"] = oi_val
            pcr = round(net_pe_oi / max(net_ce_oi, 1), 3)
            # Max Pain calculation
            mp = 0
            mpv = float('inf')
            strikes_list = sorted(strike_pain.keys())
            for st in strikes_list:
                pain = 0
                for ost in strikes_list:
                    if ost < st:
                        pain += strike_pain[ost]["ce_oi"] * (st - ost)
                    elif ost > st:
                        pain += strike_pain[ost]["pe_oi"] * (ost - st)
                if pain < mpv:
                    mpv = pain
                    mp = st
            return {
                "totalCEBuyQty": total_ce_buy, "totalPEBuyQty": total_pe_buy,
                "totalCESellQty": total_ce_sell, "totalPESellQty": total_pe_sell,
                "pcr": pcr, "maxPain": mp,
                "netCEOI": net_ce_oi, "netPEOI": net_pe_oi,
                "expiry": expiry,
            }

        # ── 3 & 4. Stock quotes for NIFTY50 (predictions + premarket) ──
        def fetch_nifty50_quotes(session):
            params = [("i", f"NSE:{sym}") for sym in NIFTY50]
            return _fetch_quotes(params, session)

        # ── 5. Gold vs Nifty historical ──
        def fetch_gold_vs_nifty(session):
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
            result = {"dates": [], "nifty": [], "gold": []}
            # NIFTY 50 is index (token 256265), GOLDBEES is NSE equity
            nifty_token = 256265  # NIFTY 50 index token
            try:
                r = requests.get(
                    f"{KITE}/instruments/historical/{nifty_token}/day",
                    params={"from": f"{from_date} 09:15:00", "to": f"{to_date} 15:30:00"},
                    headers=kh_for(session), timeout=15)
                r.raise_for_status()
                nifty_candles = r.json().get("data", {}).get("candles", [])
            except:
                nifty_candles = []
            gold_candles, _ = fetch_historical_for(session, "GOLDBEES", from_date, to_date)
            if not gold_candles:
                gold_candles = []
            if nifty_candles and gold_candles:
                nc = nifty_candles[-30:]
                gc = gold_candles[-30:]
                n_base = nc[0][4] if nc else 1
                g_base = gc[0][4] if gc else 1
                min_len = min(len(nc), len(gc))
                for i in range(min_len):
                    result["dates"].append(nc[i][0][:10] if isinstance(nc[i][0], str) else str(nc[i][0])[:10])
                    result["nifty"].append(round((nc[i][4] / n_base - 1) * 100, 2) if n_base > 0 else 0)
                    result["gold"].append(round((gc[i][4] / g_base - 1) * 100, 2) if g_base > 0 else 0)
            return result

        # ── Parallel fetch: indices, nifty options, banknifty options, nifty50 quotes, gold vs nifty ──
        try:
            fut_indices = loop.run_in_executor(pool, fetch_indices, s)
            fut_nifty_opts = loop.run_in_executor(pool, fetch_option_data, "NIFTY", s)
            fut_bn_opts = loop.run_in_executor(pool, fetch_option_data, "BANKNIFTY", s)
            fut_stocks = loop.run_in_executor(pool, fetch_nifty50_quotes, s)
            fut_gold = loop.run_in_executor(pool, fetch_gold_vs_nifty, s)

            indices_data, nifty_opts, bn_opts, stocks_data, gold_data = await tornado.gen.multi([
                fut_indices, fut_nifty_opts, fut_bn_opts, fut_stocks, fut_gold
            ])
        except Exception as e:
            indices_data = {}
            nifty_opts = None
            bn_opts = None
            stocks_data = {}
            gold_data = {"dates": [], "nifty": [], "gold": []}

        # ── 1. Indices result ──
        try:
            idx_result = {}
            for key, info in indices_data.items():
                name = key.replace("NSE:", "").replace("BSE:", "")
                idx_result[name] = {
                    "last_price": info.get("last_price", 0),
                    "change": info.get("net_change", 0),
                    "change_pct": round(info.get("net_change", 0) / max(info.get("ohlc", {}).get("close", 1), 1) * 100, 2),
                    "ohlc": info.get("ohlc", {}),
                }
            response["indices"] = idx_result
        except:
            response["indices"] = {}

        # ── 2. Options data ──
        response["niftyOptions"] = nifty_opts or {
            "totalCEBuyQty": 0, "totalPEBuyQty": 0, "totalCESellQty": 0,
            "totalPESellQty": 0, "pcr": 0, "maxPain": 0, "netCEOI": 0, "netPEOI": 0
        }
        response["bankniftyOptions"] = bn_opts or {
            "totalCEBuyQty": 0, "totalPEBuyQty": 0, "totalCESellQty": 0,
            "totalPESellQty": 0, "pcr": 0, "maxPain": 0, "netCEOI": 0, "netPEOI": 0
        }

        # ── 3. Stock Predictions (math-based) ──
        likely_rise = []
        likely_fall = []
        all_stocks_live = []
        pre_market_positive = []
        total_buy_qty = 0
        total_sell_qty = 0
        advances = declines = unchanged = 0
        total_volume = 0
        change_sum = 0
        stock_count = 0

        try:
            for key, info in stocks_data.items():
                sym = key.replace("NSE:", "")
                ltp = info.get("last_price", 0)
                avg_price = info.get("average_price", 0)
                buy_qty = info.get("buy_quantity", 0)
                sell_qty = info.get("sell_quantity", 0)
                volume = info.get("volume", 0)
                oi = info.get("oi", 0)
                ohlc = info.get("ohlc", {})
                prev_close = ohlc.get("close", 0)
                day_open = ohlc.get("open", 0)
                day_high = ohlc.get("high", 0)
                day_low = ohlc.get("low", 0)
                chg_pct = ((ltp - prev_close) / prev_close * 100) if prev_close > 0 else 0

                total_buy_qty += buy_qty
                total_sell_qty += sell_qty
                total_volume += volume
                change_sum += chg_pct
                stock_count += 1

                if chg_pct > 0.05:
                    advances += 1
                elif chg_pct < -0.05:
                    declines += 1
                else:
                    unchanged += 1

                # ── Prediction score calculation ──
                score = 0
                reasons = []

                # VWAP deviation
                if avg_price > 0:
                    vwap_dev = (ltp - avg_price) / avg_price * 100
                    if vwap_dev > 0.3:
                        score += min(vwap_dev * 8, 25)
                        reasons.append("above VWAP")
                    elif vwap_dev < -0.3:
                        score -= min(abs(vwap_dev) * 8, 25)
                        reasons.append("below VWAP")
                else:
                    vwap_dev = 0

                # Buy/Sell ratio
                bs_ratio = buy_qty / sell_qty if sell_qty > 0 else 1
                if bs_ratio > 1.2:
                    score += min((bs_ratio - 1) * 40, 25)
                    reasons.append("strong buying")
                elif bs_ratio < 0.8:
                    score -= min((1 - bs_ratio) * 40, 25)
                    reasons.append("strong selling")

                # Price vs open (intraday momentum)
                if day_open > 0:
                    open_dev = (ltp - day_open) / day_open * 100
                    if open_dev > 0.2:
                        score += min(open_dev * 6, 20)
                        reasons.append("above open")
                    elif open_dev < -0.2:
                        score -= min(abs(open_dev) * 6, 20)
                        reasons.append("below open")

                # Day's range position: (price-low)/(high-low)
                day_range = day_high - day_low
                if day_range > 0:
                    range_pos = (ltp - day_low) / day_range
                    if range_pos > 0.7:
                        score += 15
                        reasons.append("near day high")
                    elif range_pos < 0.3:
                        score -= 15
                        reasons.append("near day low")
                else:
                    range_pos = 0.5

                # Change from previous close
                if chg_pct > 0.5:
                    score += min(chg_pct * 3, 15)
                elif chg_pct < -0.5:
                    score -= min(abs(chg_pct) * 3, 15)

                score = round(score, 1)

                stock_entry = {
                    "symbol": sym, "sector": STOCK_SECTOR.get(sym, "Other"),
                    "ltp": ltp, "change": round(chg_pct, 2),
                    "open": day_open, "high": day_high, "low": day_low,
                    "prevClose": prev_close, "volume": volume,
                    "buyQty": buy_qty, "sellQty": sell_qty,
                    "bsRatio": round(bs_ratio, 2),
                    "vwapDev": round(vwap_dev, 2),
                    "predictionScore": score,
                    "reasons": reasons,
                }
                all_stocks_live.append(stock_entry)

                if score > 10:
                    likely_rise.append(stock_entry)
                elif score < -10:
                    likely_fall.append(stock_entry)

                # ── 4. Pre-Market Positive ──
                if prev_close > 0 and chg_pct > 1.0:
                    pre_market_positive.append({
                        "symbol": sym, "ltp": ltp, "prevClose": prev_close,
                        "change": round(chg_pct, 2),
                    })

            likely_rise.sort(key=lambda x: x["predictionScore"], reverse=True)
            likely_fall.sort(key=lambda x: x["predictionScore"])
        except:
            pass

        response["predictions"] = {
            "likely_to_rise": likely_rise,
            "likely_to_fall": likely_fall,
        }
        response["preMarketPositive"] = sorted(pre_market_positive, key=lambda x: x["change"], reverse=True)
        response["allStocksLive"] = all_stocks_live

        # ── 5. Gold vs Nifty ──
        response["goldVsNifty"] = gold_data

        # ── 6. Real-time Signals ──
        signals = []
        try:
            nifty_pcr = nifty_opts["pcr"] if nifty_opts else 0
            vix_data = indices_data.get("NSE:INDIA VIX", {})
            vix = vix_data.get("last_price", 0) if vix_data else 0
            market_bs = round(total_buy_qty / max(total_sell_qty, 1), 2)

            if nifty_pcr > 1 and vix < 15 and advances > declines:
                signals.append({"signal": "NIFTY bullish", "type": "bullish",
                                "detail": f"PCR={nifty_pcr}, VIX={vix}, A/D={advances}/{declines}"})
            if nifty_pcr < 0.7 and vix > 20:
                signals.append({"signal": "NIFTY bearish", "type": "bearish",
                                "detail": f"PCR={nifty_pcr}, VIX={vix}"})
            if vix > 20:
                signals.append({"signal": "High volatility warning", "type": "warning",
                                "detail": f"VIX={vix}"})
            if market_bs > 1.2:
                signals.append({"signal": "Institutional buying", "type": "bullish",
                                "detail": f"Market Buy/Sell ratio={market_bs}"})
            if market_bs < 0.8:
                signals.append({"signal": "Institutional selling", "type": "bearish",
                                "detail": f"Market Buy/Sell ratio={market_bs}"})

            # Check individual stocks for strong buy candidates
            for st in all_stocks_live:
                is_accumulation = st.get("bsRatio", 1) > 1.2 and st.get("vwapDev", 0) > 0
                is_oversold = st.get("change", 0) < -2  # proxy for RSI oversold without historical data
                if is_accumulation and is_oversold:
                    signals.append({"signal": f"{st['symbol']} strong buy candidate", "type": "strong_buy",
                                    "detail": f"Smart money accumulation + oversold (chg={st['change']}%)"})
        except:
            pass

        response["signals"] = signals

        # ── 7. Market Internals ──
        avg_change = round(change_sum / max(stock_count, 1), 2)
        response["marketInternals"] = {
            "advances": advances, "declines": declines, "unchanged": unchanged,
            "adRatio": round(advances / max(declines, 1), 2),
            "totalVolume": total_volume, "avgChange": avg_change,
            "totalBuyQty": total_buy_qty, "totalSellQty": total_sell_qty,
            "marketBSRatio": round(total_buy_qty / max(total_sell_qty, 1), 2),
        }

        cache_set(ck, response, CACHE_TTL_AI)
        self.write_json_gzip(response)

# ── STOCK DETAIL (Single Stock Deep Analysis) ─────────────

CACHE_TTL_STOCKDETAIL = 15  # seconds

class StockDetailHandler(Base):
    async def post(self):
        s = self.require_session()
        if not s: return
        b = self.body()
        symbol = b.get("symbol", "").strip().upper()
        if not symbol:
            return self.err(400, "Need symbol")
        if symbol not in SYM:
            return self.err(404, f"Unknown symbol: {symbol}")

        ck = f"stockdetail:{s['api_key']}:{symbol}"
        cached = cache_get(ck)
        if cached is not None:
            self.write_json_gzip(cached)
            return

        loop = tornado.ioloop.IOLoop.current()
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date_1y = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

        # ── Parallel fetches: quote, 1-year historical, NIFTY 50 historical ──
        def fetch_quote(session, sym):
            try:
                r = requests.get(f"{KITE}/quote", params=[("i", f"NSE:{sym}")],
                                 headers=kh_for(session), timeout=15)
                r.raise_for_status()
                return r.json().get("data", {}).get(f"NSE:{sym}", {})
            except:
                return {}

        try:
            fut_quote = loop.run_in_executor(pool, fetch_quote, s, symbol)
            fut_hist = loop.run_in_executor(pool, fetch_historical_for, s, symbol, from_date_1y, to_date)
            fut_nifty = loop.run_in_executor(pool, lambda: self._fetch_nifty_hist(s, from_date_1y, to_date))

            quote_data, (candles, hist_err), nifty_candles = await tornado.gen.multi([
                fut_quote, fut_hist, fut_nifty
            ])
        except Exception as e:
            return self.err(500, f"Fetch error: {str(e)[:100]}")

        if not quote_data:
            return self.err(502, f"Could not fetch quote for {symbol}")
        if hist_err or not candles or len(candles) < 20:
            return self.err(502, f"Could not fetch historical data for {symbol}: {hist_err or 'insufficient data'}")

        # ── Parse candles ──
        opens, highs, lows, closes, volumes, timestamps = parse_candles(candles)
        ltp = quote_data.get("last_price", closes[-1])
        ep = ltp  # effective price

        # ── 1. Live Quote Data ──
        ohlc = quote_data.get("ohlc", {})
        depth = quote_data.get("depth", {})
        buy_depth = depth.get("buy", [])
        sell_depth = depth.get("sell", [])
        buy_quantity = quote_data.get("buy_quantity", 0)
        sell_quantity = quote_data.get("sell_quantity", 0)
        avg_price = quote_data.get("average_price", 0)
        today_volume = quote_data.get("volume", 0)
        prev_close = ohlc.get("close", 0)

        quote_result = {
            "ltp": ltp,
            "open": ohlc.get("open", 0),
            "high": ohlc.get("high", 0),
            "low": ohlc.get("low", 0),
            "close": ohlc.get("close", 0),
            "prev_close": prev_close,
            "volume": today_volume,
            "avg_price": avg_price,
            "buy_quantity": buy_quantity,
            "sell_quantity": sell_quantity,
            "oi": quote_data.get("oi", 0),
            "ohlc": ohlc,
            "depth": depth,
        }

        # ── 2. Historical Analysis (1 year) ──
        rsi = calc_rsi(closes)
        macd_val, macd_sig, macd_hist = calc_macd(closes)
        bb_mid, bb_upper, bb_lower = calc_bollinger(closes)
        atr = calc_atr(highs, lows, closes)
        adx = calc_adx(highs, lows, closes)
        volatility = calc_volatility(closes)
        max_dd, _, _ = calc_max_drawdown(closes)
        stock_returns = calc_returns(closes)
        sharpe = calc_sharpe(stock_returns)

        # Beta vs NIFTY 50
        nifty_returns = []
        if nifty_candles:
            _, _, _, ncl, _, _ = parse_candles(nifty_candles)
            nifty_returns = calc_returns(ncl)
        beta = calc_beta(stock_returns, nifty_returns) if nifty_returns else None

        # SMA values
        sma20 = round(sum(closes[-20:]) / min(len(closes), 20), 2) if len(closes) >= 20 else round(ep, 2)
        sma50 = round(sum(closes[-50:]) / 50, 2) if len(closes) >= 50 else None
        sma200 = round(sum(closes[-200:]) / 200, 2) if len(closes) >= 200 else None

        sma_result = {
            "sma20": sma20,
            "sma50": sma50,
            "sma200": sma200,
            "aboveSMA20": ep > sma20 if sma20 else None,
            "aboveSMA50": ep > sma50 if sma50 else None,
            "aboveSMA200": ep > sma200 if sma200 else None,
        }

        technicals = {
            "rsi": rsi,
            "macd": macd_val,
            "macdSignal": macd_sig,
            "macdHist": macd_hist,
            "bbMid": bb_mid,
            "bbUpper": bb_upper,
            "bbLower": bb_lower,
            "atr": atr,
            "adx": adx,
            "volatility": volatility,
            "sharpe": sharpe,
            "beta": beta,
            "maxDrawdown": max_dd,
        }

        # Support / Resistance
        resistances, supports = calc_support_resistance(highs, lows, closes)

        # 52-week high/low
        high_52w = round(max(highs), 2)
        low_52w = round(min(lows), 2)
        dist_52h = round((high_52w - ep) / high_52w * 100, 2) if high_52w > 0 else 0
        dist_52l = round((ep - low_52w) / low_52w * 100, 2) if low_52w > 0 else 0

        levels = {
            "supports": supports,
            "resistances": resistances,
            "52wHigh": high_52w,
            "52wLow": low_52w,
            "distFrom52wHigh": dist_52h,
            "distFrom52wLow": dist_52l,
        }

        # Returns
        chg_1w = round((ep / closes[-6] - 1) * 100, 2) if len(closes) >= 6 else 0
        chg_1m = round((ep / closes[-22] - 1) * 100, 2) if len(closes) >= 22 else 0
        chg_3m = round((ep / closes[-66] - 1) * 100, 2) if len(closes) >= 66 else 0
        chg_6m = round((ep / closes[-132] - 1) * 100, 2) if len(closes) >= 132 else 0
        chg_1y = round((ep / closes[0] - 1) * 100, 2) if closes[0] > 0 else 0

        returns_result = {
            "chg1w": chg_1w, "chg1m": chg_1m, "chg3m": chg_3m,
            "chg6m": chg_6m, "chg1y": chg_1y,
        }

        # ── 3. AI Targets & Stop Loss ──
        atr_val = atr if atr else 0
        s1 = supports[0] if supports else ep * 0.97
        r2 = resistances[1] if len(resistances) > 1 else ep * 1.05

        target1 = round(ep + 1 * atr_val, 2)
        target2 = round(ep + 2 * atr_val, 2)
        target3 = round(r2, 2)
        stop_loss = round(max(s1, ep - 1.5 * atr_val), 2)
        risk = ep - stop_loss
        reward = target1 - ep
        risk_reward = round(reward / risk, 2) if risk > 0 else 0

        targets = {
            "target1": target1,
            "target2": target2,
            "target3": target3,
            "stopLoss": stop_loss,
            "riskReward": risk_reward,
        }

        # ── 4. AI Buy/Sell Timing ──
        rsi_val = rsi if rsi else 50
        macd_h = macd_hist if macd_hist else 0
        above_sma200 = (ep > sma200) if sma200 else False
        above_sma50 = (ep > sma50) if sma50 else False
        above_sma20 = ep > sma20

        # Detect MACD histogram turning positive
        macd_turning_positive = False
        if len(closes) > 36:
            _, _, prev_hist = calc_macd(closes[:-1])
            if prev_hist is not None and prev_hist < 0 and macd_h > 0:
                macd_turning_positive = True

        # Buy signal
        if rsi_val < 35 and ep <= s1 * 1.02 and macd_turning_positive:
            buy_signal = "NOW - Strong Buy Zone"
            buy_confidence = 85
        elif rsi_val < 45 and above_sma200:
            buy_signal = "Accumulate on dips"
            buy_confidence = 65
        elif rsi_val > 60 and above_sma20 and above_sma50 and above_sma200:
            buy_signal = "Trending - buy on pullbacks to SMA20"
            buy_confidence = 55
        else:
            buy_signal = "Wait for better entry"
            buy_confidence = 30

        # Sell signal
        divergence = detect_divergence(closes)
        if rsi_val > 80 and divergence == "bearish":
            sell_signal = "Exit - overbought with divergence"
            sell_confidence = 85
        elif rsi_val > 75:
            sell_signal = "Book partial profits"
            sell_confidence = 70
        elif not above_sma50 and macd_h < 0:
            sell_signal = "Exit - trend broken"
            sell_confidence = 75
        else:
            sell_signal = f"Hold with trailing SL at {stop_loss}"
            sell_confidence = 40

        confidence = max(buy_confidence, sell_confidence)

        timing = {
            "buySignal": buy_signal,
            "sellSignal": sell_signal,
            "confidence": confidence,
        }

        # ── 5. AI Reasons (bullish/bearish) ──
        bullish_reasons = []
        bearish_reasons = []
        net_score = 0

        # RSI
        if rsi_val < 30:
            bullish_reasons.append("RSI oversold (<30) - reversal likely")
            net_score += 15
        elif rsi_val < 40:
            bullish_reasons.append("RSI approaching oversold zone")
            net_score += 8
        elif rsi_val > 70:
            bearish_reasons.append("RSI overbought (>70) - correction risk")
            net_score -= 15
        elif rsi_val > 60:
            bullish_reasons.append("RSI in bullish zone (>60)")
            net_score += 5

        # MACD
        if macd_h > 0:
            bullish_reasons.append(f"MACD histogram positive ({macd_h})")
            net_score += 10
        elif macd_h < 0:
            bearish_reasons.append(f"MACD histogram negative ({macd_h})")
            net_score -= 10

        if macd_val is not None and macd_sig is not None:
            if macd_val > macd_sig:
                bullish_reasons.append("MACD above signal line - bullish crossover")
                net_score += 8
            else:
                bearish_reasons.append("MACD below signal line - bearish crossover")
                net_score -= 8

        # Price vs SMAs
        if above_sma20:
            bullish_reasons.append(f"Price above SMA20 ({sma20})")
            net_score += 5
        else:
            bearish_reasons.append(f"Price below SMA20 ({sma20})")
            net_score -= 5

        if sma50 is not None:
            if above_sma50:
                bullish_reasons.append(f"Price above SMA50 ({sma50})")
                net_score += 8
            else:
                bearish_reasons.append(f"Price below SMA50 ({sma50})")
                net_score -= 8

        if sma200 is not None:
            if above_sma200:
                bullish_reasons.append(f"Price above SMA200 ({sma200}) - long-term uptrend")
                net_score += 12
            else:
                bearish_reasons.append(f"Price below SMA200 ({sma200}) - long-term downtrend")
                net_score -= 12

        # Volume trend
        if len(volumes) >= 10:
            recent_vol = sum(volumes[-5:]) / 5
            older_vol = sum(volumes[-10:-5]) / 5 if len(volumes) >= 10 else recent_vol
            if older_vol > 0:
                vol_trend = (recent_vol / older_vol - 1) * 100
                if vol_trend > 20:
                    bullish_reasons.append(f"Volume surging (+{round(vol_trend, 1)}%)")
                    net_score += 7
                elif vol_trend < -20:
                    bearish_reasons.append(f"Volume declining ({round(vol_trend, 1)}%)")
                    net_score -= 5

        # Bollinger Band position
        if bb_upper and bb_lower:
            if ep > bb_upper:
                bearish_reasons.append("Price above upper Bollinger Band - overextended")
                net_score -= 8
            elif ep < bb_lower:
                bullish_reasons.append("Price below lower Bollinger Band - oversold bounce likely")
                net_score += 8

        # Divergence
        if divergence == "bullish":
            bullish_reasons.append("Bullish divergence detected (price down, RSI up)")
            net_score += 12
        elif divergence == "bearish":
            bearish_reasons.append("Bearish divergence detected (price up, RSI down)")
            net_score -= 12

        # Beta
        if beta is not None:
            if beta > 1.3:
                bearish_reasons.append(f"High beta ({beta}) - amplified market risk")
                net_score -= 3
            elif beta < 0.7:
                bullish_reasons.append(f"Low beta ({beta}) - defensive stock")
                net_score += 3

        # Buy/Sell quantity from live quote
        if buy_quantity > 0 and sell_quantity > 0:
            bs_ratio = buy_quantity / sell_quantity
            if bs_ratio > 1.3:
                bullish_reasons.append(f"Strong buy-side demand (B/S ratio: {round(bs_ratio, 2)})")
                net_score += 6
            elif bs_ratio < 0.7:
                bearish_reasons.append(f"Strong sell-side pressure (B/S ratio: {round(bs_ratio, 2)})")
                net_score -= 6

        # Determine verdict
        if net_score >= 30:
            verdict = "STRONG BUY"
        elif net_score >= 10:
            verdict = "BUY"
        elif net_score >= -10:
            verdict = "HOLD"
        elif net_score >= -30:
            verdict = "SELL"
        else:
            verdict = "STRONG SELL"

        ai_verdict = {
            "verdict": verdict,
            "score": net_score,
            "bullishReasons": bullish_reasons,
            "bearishReasons": bearish_reasons,
        }

        # ── 6. Last 7 Days Trading Data ──
        last7_candles = candles[-7:] if len(candles) >= 7 else candles
        last7_data = []
        last7_total_vol = 0
        for i, c in enumerate(last7_candles):
            dt = c[0][:10] if isinstance(c[0], str) else str(c[0])[:10]
            c_open, c_high, c_low, c_close, c_vol = c[1], c[2], c[3], c[4], c[5]
            prev_c = last7_candles[i - 1][4] if i > 0 else (candles[-8][4] if len(candles) > 7 else c_open)
            day_chg = round((c_close / prev_c - 1) * 100, 2) if prev_c > 0 else 0
            buy_pressure = c_vol if c_close > c_open else -c_vol
            last7_total_vol += c_vol
            last7_data.append({
                "date": dt,
                "open": round(c_open, 2),
                "high": round(c_high, 2),
                "low": round(c_low, 2),
                "close": round(c_close, 2),
                "volume": c_vol,
                "change": day_chg,
                "buyPressure": buy_pressure,
            })

        avg_7d_vol = round(last7_total_vol / max(len(last7_candles), 1))

        # ── 7. Delivery/Institutional Participation Estimate ──
        bs_ratio_live = round(buy_quantity / sell_quantity, 2) if sell_quantity > 0 else 0
        vwap_dev = calc_vwap_deviation(ltp, avg_price) if avg_price > 0 else 0
        buy_wall = sum(b_item.get("quantity", 0) for b_item in buy_depth[:3]) if buy_depth else 0
        sell_wall = sum(s_item.get("quantity", 0) for s_item in sell_depth[:3]) if sell_depth else 0
        wall_ratio = round(buy_wall / sell_wall, 2) if sell_wall > 0 else 0

        if bs_ratio_live > 1.3:
            inst_assessment = "Strong institutional buying suspected"
        elif bs_ratio_live > 1.1:
            inst_assessment = "Moderate institutional interest"
        elif bs_ratio_live < 0.7:
            inst_assessment = "Institutional selling suspected"
        else:
            inst_assessment = "Neutral institutional activity"

        # Promoter interest heuristic based on index membership
        is_nifty50 = symbol in NIFTY50
        promoter_interest = "High (NIFTY50 large cap)" if is_nifty50 else "Unknown"

        institutional_activity = {
            "bsRatio": bs_ratio_live,
            "vwapDev": vwap_dev,
            "buyWall": buy_wall,
            "sellWall": sell_wall,
            "wallRatio": wall_ratio,
            "assessment": inst_assessment,
            "promoterInterest": promoter_interest,
        }

        # ── 8. Block Deal Indicator ──
        vol_ratio = round(today_volume / avg_7d_vol, 2) if avg_7d_vol > 0 else 0
        block_deal_flag = vol_ratio > 5
        block_deal_days = []
        for day_data in last7_data:
            if avg_7d_vol > 0 and day_data["volume"] > 3 * avg_7d_vol:
                block_deal_days.append({
                    "date": day_data["date"],
                    "volume": day_data["volume"],
                    "ratio": round(day_data["volume"] / avg_7d_vol, 2),
                })

        volume_analysis = {
            "avg7DayVol": avg_7d_vol,
            "todayVol": today_volume,
            "volRatio": vol_ratio,
            "blockDealFlag": block_deal_flag,
            "blockDealDays": block_deal_days,
        }

        # ── 9. Sparkline data ──
        sparkline = [round(c, 2) for c in (closes[-30:] if len(closes) >= 30 else closes)]

        # ── Build response ──
        stock_name = SYM.get(symbol, {}).get("name", symbol)
        sector = STOCK_SECTOR.get(symbol, "Other")

        response = {
            "symbol": symbol,
            "name": stock_name,
            "sector": sector,
            "ltp": ltp,
            "quote": quote_result,
            "technicals": technicals,
            "sma": sma_result,
            "levels": levels,
            "returns": returns_result,
            "targets": targets,
            "timing": timing,
            "aiVerdict": ai_verdict,
            "last7Days": last7_data,
            "volumeAnalysis": volume_analysis,
            "institutionalActivity": institutional_activity,
            "sparkline": sparkline,
        }

        cache_set(ck, response, CACHE_TTL_STOCKDETAIL)
        self.write_json_gzip(response)

    def _fetch_nifty_hist(self, session, from_date, to_date):
        """Fetch NIFTY 50 index historical data using token 256265."""
        ck = f"hist:{session['api_key']}:NIFTY50_IDX:{from_date}:{to_date}:day"
        cached = cache_get(ck)
        if cached is not None:
            return cached
        try:
            r = requests.get(
                f"{KITE}/instruments/historical/256265/day",
                params={"from": f"{from_date} 09:15:00", "to": f"{to_date} 15:30:00"},
                headers=kh_for(session), timeout=15)
            r.raise_for_status()
            candles = r.json().get("data", {}).get("candles", [])
            if candles:
                cache_set(ck, candles, CACHE_TTL_HISTORICAL)
                return candles
            return None
        except:
            return None


# ═══════════════════════════════════════════════════════════
#  APP
# ═══════════════════════════════════════════════════════════
_server_start = time.time()

def make_app():
    return tornado.web.Application([
        (r"/", IndexPage),
        (r"/(manifest\.json)", StaticFileHandler),
        (r"/(sw\.js)", StaticFileHandler),
        (r"/(icon-192\.png)", StaticFileHandler),
        (r"/(icon-512\.png)", StaticFileHandler),
        (r"/callback", CallbackHandler),
        (r"/api/health", HealthHandler),
        (r"/api/license/verify", LicenseVerifyHandler),
        (r"/api/license/admin", LicenseAdminHandler),
        (r"/api/license/check", LicenseCheckHandler),
        (r"/api/auth", AuthHandler),
        (r"/api/login", LoginRedirect),
        (r"/api/logout", LogoutHandler),
        (r"/api/status", StatusHandler),
        (r"/api/quote", QuoteHandler),
        (r"/api/indices", IndicesHandler),
        (r"/api/historical", HistHandler),
        (r"/api/screener", ScreenerHandler),
        (r"/api/smartmoney", SmartMoneyHandler),
        (r"/api/momentum", MomentumHandler),
        (r"/api/sectors", SectorRotationHandler),
        (r"/api/correlation", CorrelationHandler),
        (r"/api/breadth", BreadthHandler),
        (r"/api/risk", RiskHandler),
        (r"/api/optionchain", OptionChainHandler),
        (r"/api/constituents", IndexConstituentsHandler),
        (r"/api/aianalysis", AIAnalysisHandler),
        (r"/api/stockdetail", StockDetailHandler),
    ], cookie_secret=hashlib.sha256(os.urandom(32)).hexdigest(), debug=True)

if __name__ == "__main__":
    app = make_app()
    app.listen(PORT)
    # Schedule periodic cache cleanup
    tornado.ioloop.IOLoop.current().call_later(120, _schedule_cache_cleanup)
    print(f"\n{'='*55}")
    print(f"  NSE PULSE PRO v5.0 — Multi-User Institutional Terminal")
    print(f"  Built by Kanishk Arora")
    print(f"  Supports up to {MAX_SESSIONS} concurrent users")
    print(f"  Performance: cache TTLs quote={CACHE_TTL_QUOTE}s hist={CACHE_TTL_HISTORICAL}s screener={CACHE_TTL_SCREENER}s")
    print(f"  Batch size: 5 | Delay: 0.15s | Gzip: >{GZIP_MIN_SIZE}B | Workers: 20")
    print(f"  http://localhost:{PORT}")
    print(f"{'='*55}\n")
    tornado.ioloop.IOLoop.current().start()
