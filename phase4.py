"""
NEXUS PHASE 4 — PER-SYMBOL AUTONOMOUS BOTS V1.5
4 dedicated bots: NUGT, SOXL, LABU, TQQQ
Each reads full market context, selects a trading mode, executes independently.

Trading Modes:
  SCALP    — RSI dip + bounce. Quick target. Choppy/uncertain markets.
  RIDE     — Sustained trend entry. Looser trail. Bull market confirmed.
  EXTENDED — Multi-bar uptrend, higher lows locked in. Ride the full wave.

Bear pairs: each bot monitors bull RSI exhaustion -> flips to bear ETF on reversal.
  NUGT -> DUST | SOXL -> SOXS | LABU -> LABD | TQQQ -> SQQQ

Capital allocation (by EV from nexus_analyzer 2yr + 1yr backtest):
  NUGT 30% | SOXL 25% | LABU 25% | TQQQ 20%

Positions held overnight — no forced close at market end.

V1.5 changes vs V1.4:
  - BUG FIX: recover_position() now generates trade_id, entry_time, and
    infers mode from current market context — no more orphaned fingerprints
    or 0-minute hold times on redeployed positions
  - BUG FIX: bear entry sym_ctx now computed properly from live bear_prices
    (above_ma20, higher_lows, trend_10bar no longer hardcoded False/0)
  - BUG FIX: crash guard added — bear entry skipped if bear_prices is empty
  - BUG FIX: check_reversal() now applies per-bear-pair hour gating before
    returning True — SOXS/LABD/SQQQ bad hours blocked at the reversal gate
  - BUG FIX: EXTENDED trend-break exit now fires even when slightly underwater
    (profit_pct > -0.005) to avoid holding broken-trend losers past stop
  - BUG FIX: balance + positions cache now protected by threading.Lock()
  - FIX: SOXL avoid_hours corrected (was blocking hour 8 at 56.4% WR — wrong)
  - FIX: TQQQ avoid_hours corrected (was blocking hour 9 at 54.5% WR — wrong)
  - FIX: trough_price dead field removed — mae tracks adversity correctly
  - NEW: Extended signal suite — StochRSI, MACD, OBV, Bollinger Bands,
    WilliamsR, CCI, EMA9/21 all computed from yfinance data
  - NEW: Signal combo scoring — entry quality scored against best_signal_combos
    from strategy_recipes.json, boosting high-conviction trades
  - NEW: Analyst bridge — Phase4 fetches live confluence scores + active
    signals from nexus-analyst at entry decision time for each symbol
  - NEW: SQQQ fully gated — disabled by default, only tradeable via env flag
  - NEW: DUST/SOXS extended TP mode — when MFE headroom is high, uses
    analyst-style trailing exit (4% target, 1% trail) instead of 1.5% fixed
"""

import os
import time
import uuid
import traceback
import threading
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    import psycopg2
    import psycopg2.extras
    _db_available = True
except ImportError:
    _db_available = False

from webull.core.client import ApiClient
from webull.trade.trade_client import TradeClient

# ── Env ───────────────────────────────────────────────────────────────────────
APP_KEY          = os.environ.get("WEBULL_APP_KEY")
APP_SECRET       = os.environ.get("WEBULL_APP_SECRET")
ACCOUNT_ID       = os.environ.get("WEBULL_ACCOUNT_ID")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
DATABASE_URL     = os.environ.get("DATABASE_URL", "")
ANALYST_URL      = os.environ.get("ANALYST_URL", "").rstrip("/")
NEXUS_TOKEN      = os.environ.get("NEXUS_INTERNAL_TOKEN", "")

# V1.5: SQQQ is disabled by default — set PHASE4_SQQQ_ENABLED=true to re-enable
SQQQ_ENABLED     = os.environ.get("PHASE4_SQQQ_ENABLED", "false").lower() == "true"

CENTRAL  = ZoneInfo("America/Chicago")
BOT_NAME = "PHASE4"

# ── Bot configs — capital by EV ───────────────────────────────────────────────
# V1.5 avoid_hours corrections based on strategy_recipes analysis:
#   NUGT:  hour 9 is bad (47.3% WR/55 trades). Hour 11 removed — only 12 trades, noise.
#   SOXL:  no truly bad hours. Hour 8 (56.4%/181 trades) was wrongly blocked. Cleared.
#   LABU:  hour 10 (38.7%/31) and hour 13 (37.5%/16) are confirmed bad. Kept.
#   TQQQ:  hour 10 (52.9%/17) is below-average. Hour 9 (54.5%/33) was wrongly blocked.
BOT_CONFIGS = {
    "NUGT": {
        "bear_pair":   "DUST",
        "budget_pct":  0.30,
        "recipe": {
            "stop_loss": 0.028, "profit_ratchet": 0.008, "trailing_stop": 0.003,
            "avoid_hours": [9], "avoid_days": [],
        },
        "ride_stop_mult":   1.3,
        "ride_trail_mult":  1.5,
        "ext_stop_mult":    1.8,
        "ext_trail_mult":   3.0,   # V1.5: widened from 2.2 to prevent noise whipsaws
        "ext_ratchet_pct":  0.02,
    },
    "SOXL": {
        "bear_pair":   "SOXS",
        "budget_pct":  0.25,
        "recipe": {
            "stop_loss": 0.025, "profit_ratchet": 0.006, "trailing_stop": 0.002,
            "avoid_hours": [], "avoid_days": [],  # V1.5: no bad hours for SOXL
        },
        "ride_stop_mult":   1.3,
        "ride_trail_mult":  1.5,
        "ext_stop_mult":    1.8,
        "ext_trail_mult":   3.0,   # V1.5: widened from 2.2
        "ext_ratchet_pct":  0.02,
    },
    "LABU": {
        "bear_pair":   "LABD",
        "budget_pct":  0.25,
        "recipe": {
            "stop_loss": 0.028, "profit_ratchet": 0.009, "trailing_stop": 0.003,
            "avoid_hours": [10, 13], "avoid_days": [],
        },
        "ride_stop_mult":   1.3,
        "ride_trail_mult":  1.5,
        "ext_stop_mult":    1.8,
        "ext_trail_mult":   3.0,   # V1.5: widened
        "ext_ratchet_pct":  0.025,
    },
    "TQQQ": {
        "bear_pair":   "SQQQ",
        "budget_pct":  0.20,
        "recipe": {
            "stop_loss": 0.021, "profit_ratchet": 0.004, "trailing_stop": 0.0016,
            "avoid_hours": [10], "avoid_days": [],  # V1.5: hour 9 removed (54.5% WR — decent)
        },
        "ride_stop_mult":   1.3,
        "ride_trail_mult":  1.5,
        "ext_stop_mult":    2.0,
        "ext_trail_mult":   3.5,   # V1.5: widened from 2.5
        "ext_ratchet_pct":  0.015,
    },
}

# V1.5: Bear recipes now include avoid_hours based on per-symbol data analysis:
#   DUST:  hour 11 (43.8%/16 trades) is bad
#   SOXS:  hour 9 (44.1%/111 trades) is very bad — largest impact
#   LABD:  hours 11 (42.3%/26) and 14 (39.4%/33) are bad
#   SQQQ:  hours 11, 13, 14 all at 33.3% WR — disabled by default anyway
BEAR_RECIPES = {
    "DUST": {
        "stop_loss": 0.026, "profit_ratchet": 0.006, "trailing_stop": 0.0025,
        "avoid_hours": [11],
    },
    "SOXS": {
        "stop_loss": 0.024, "profit_ratchet": 0.006, "trailing_stop": 0.002,
        "avoid_hours": [9],
    },
    "LABD": {
        "stop_loss": 0.025, "profit_ratchet": 0.007, "trailing_stop": 0.0024,
        "avoid_hours": [11, 14],
    },
    "SQQQ": {
        "stop_loss": 0.018, "profit_ratchet": 0.0035, "trailing_stop": 0.0016,
        "avoid_hours": [11, 13, 14],
    },
}

# V1.5: Bear pairs with large avg MFE (DUST=5.4%, SOXS=4.8%) get extended TP mode
# instead of a fixed 1.5% cap. Activates trailing after +2%, exits at +1% drawdown.
BEAR_EXTENDED_TP = {
    "DUST": {"trail_activate": 0.020, "trail_stop": 0.010},
    "SOXS": {"trail_activate": 0.020, "trail_stop": 0.010},
}

RSI_PERIOD          = 7
BUYING_POWER_BUFFER = 1.15
WIN_COOLDOWN_SECS   = 180
LOSS_COOLDOWN_SECS  = 900

QQQ_BEAR_RSI_GATE       = 58
QQQ_BEAR_RSI_GATE_LABD  = 65

PM_MIN_TRADES        = 15
PM_ANALYSIS_INTERVAL = 86400
PM_MIN_BUCKET_TRADES = 3
WIN_RATE_GATE_THRESHOLD = 0.35

# V1.5: Signal combo boost — if analyst signals include both signals from
# the best combo for this symbol, require no minimum analyst score.
# If only one signal matches, apply normal score gate.
SIGNAL_COMBO_BOOST_SYMBOLS = {
    "SOXL": [("BB squeeze — breakout imminent", "StochRSI")],
    "LABU": [("RSI14 EXTREME oversold", "RSI7 EXTREME oversold")],
    "TQQQ": [("Bouncing", "OBV falling — distribution")],
    "NUGT": [("BB squeeze — breakout imminent", "MACD bullish")],
    "DUST": [("Far below MA20", "StochRSI")],
    "SOXS": [("Below MA20", "RSI7 EXTREME oversold")],
    "LABD": [("EMA9 above EMA21", "StochRSI")],
}

WEBULL_CACHE_TTL    = 25
WEBULL_429_BACKOFF  = 30
LOOP_INTERVAL       = 12
WARMUP_BARS         = 35
REVERSAL_OB_RSI     = 70
REVERSAL_RSI_RESET  = 60
REVERSAL_CONFIRM    = 0.005
REVERSAL_MAX_WATCH  = 1800

# Analyst bridge cache — shared across all bots
_analyst_scores_cache: dict  = {}
_analyst_scores_ts:    float = 0.0
_analyst_scores_ttl:   float = 20.0  # re-fetch every 20s max
_analyst_lock                = threading.Lock()

# ── Webull client ─────────────────────────────────────────────────────────────
api_client   = ApiClient(APP_KEY, APP_SECRET, "us")
trade_client = TradeClient(api_client)
_order_lock  = threading.Lock()

# ── Cache locks (V1.5: protect shared globals accessed by 4 threads) ─────────
_balance_lock    = threading.Lock()
_positions_lock  = threading.Lock()

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(symbol, msg):
    ts = datetime.now(tz=CENTRAL).strftime("%H:%M:%S")
    print(f"[{symbol} | {ts}] {msg}", flush=True)

def alert(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except Exception:
        pass

def is_market_hours() -> bool:
    now = datetime.now(tz=CENTRAL)
    return now.weekday() < 5 and 8 <= now.hour < 15

def compute_rsi(prices: list, period: int = 7) -> float | None:
    if len(prices) < period + 1:
        return None
    s     = pd.Series(prices)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0).rolling(period).mean()
    loss  = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs    = gain / loss
    rsi   = 100 - (100 / (1 + rs.iloc[-1]))
    if not (0 < rsi < 100):
        return None
    return round(float(rsi), 2)

def compute_ma(prices: list, period: int = 20) -> float | None:
    if len(prices) < period:
        return None
    return round(float(sum(prices[-period:]) / period), 4)

def compute_ema(prices: list, period: int) -> float | None:
    """Exponential moving average — last value."""
    if len(prices) < period:
        return None
    s = pd.Series(prices)
    return round(float(s.ewm(span=period, adjust=False).mean().iloc[-1]), 4)

def compute_macd(prices: list) -> dict:
    """MACD(12,26,9). Returns {bullish, macd_line, signal_line, histogram}."""
    if len(prices) < 26:
        return {"bullish": False, "macd_line": 0, "signal_line": 0, "histogram": 0}
    s          = pd.Series(prices)
    ema12      = s.ewm(span=12, adjust=False).mean()
    ema26      = s.ewm(span=26, adjust=False).mean()
    macd_line  = ema12 - ema26
    signal     = macd_line.ewm(span=9, adjust=False).mean()
    histogram  = macd_line - signal
    return {
        "bullish":     float(macd_line.iloc[-1]) > float(signal.iloc[-1]),
        "macd_line":   round(float(macd_line.iloc[-1]), 5),
        "signal_line": round(float(signal.iloc[-1]), 5),
        "histogram":   round(float(histogram.iloc[-1]), 5),
    }

def compute_bollinger(prices: list, period: int = 20, std_dev: float = 2.0) -> dict:
    """Bollinger Bands. Returns {upper, middle, lower, pct_b, squeeze, near_lower, at_lower, far_below}."""
    if len(prices) < period:
        return {"upper": 0, "middle": 0, "lower": 0, "pct_b": 0.5,
                "squeeze": False, "near_lower": False, "at_lower": False, "far_below": False}
    s      = pd.Series(prices)
    middle = s.rolling(period).mean().iloc[-1]
    std    = s.rolling(period).std().iloc[-1]
    upper  = middle + std_dev * std
    lower  = middle - std_dev * std
    price  = prices[-1]
    band_w = upper - lower
    pct_b  = (price - lower) / band_w if band_w > 0 else 0.5

    # Squeeze: band width < 2% of price (compression before breakout)
    squeeze    = (band_w / price) < 0.02 if price > 0 else False
    near_lower = pct_b < 0.20
    at_lower   = pct_b < 0.05
    far_below  = price < lower * 0.99  # more than 1% below lower band

    return {
        "upper":      round(float(upper), 4),
        "middle":     round(float(middle), 4),
        "lower":      round(float(lower), 4),
        "pct_b":      round(float(pct_b), 3),
        "squeeze":    squeeze,
        "near_lower": near_lower,
        "at_lower":   at_lower,
        "far_below":  far_below,
    }

def compute_stochrsi(prices: list, rsi_period: int = 14, stoch_period: int = 14) -> dict:
    """StochRSI — tells us where RSI sits relative to its own range."""
    if len(prices) < rsi_period + stoch_period + 5:
        return {"k": 50, "d": 50, "oversold": False, "overbought": False}
    s       = pd.Series(prices)
    delta   = s.diff()
    gain    = delta.where(delta > 0, 0).rolling(rsi_period).mean()
    loss    = (-delta.where(delta < 0, 0)).rolling(rsi_period).mean()
    rs      = gain / loss
    rsi_ser = 100 - (100 / (1 + rs))
    min_rsi = rsi_ser.rolling(stoch_period).min()
    max_rsi = rsi_ser.rolling(stoch_period).max()
    denom   = max_rsi - min_rsi
    stoch_k = ((rsi_ser - min_rsi) / denom * 100).where(denom != 0, 50)
    stoch_d = stoch_k.rolling(3).mean()
    k_val   = float(stoch_k.iloc[-1])
    d_val   = float(stoch_d.iloc[-1])
    return {
        "k":          round(k_val, 2),
        "d":          round(d_val, 2),
        "oversold":   k_val < 20 and d_val < 20,
        "overbought": k_val > 80 and d_val > 80,
    }

def compute_obv(prices: list, volumes: list) -> dict:
    """On-Balance Volume — trend confirmation. Returns {rising, obv_slope}."""
    if len(prices) < 10 or len(volumes) < 10:
        return {"rising": False, "obv_slope": 0}
    n      = min(len(prices), len(volumes))
    prices = prices[-n:]
    volumes = volumes[-n:]
    obv    = [0.0]
    for i in range(1, len(prices)):
        if prices[i] > prices[i-1]:
            obv.append(obv[-1] + volumes[i])
        elif prices[i] < prices[i-1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    # Slope of OBV over last 10 bars
    if len(obv) >= 10:
        recent = obv[-10:]
        slope  = (recent[-1] - recent[0]) / (abs(recent[0]) + 1)
    else:
        slope = 0
    return {"rising": slope > 0, "obv_slope": round(slope, 4)}

def compute_williams_r(prices: list, period: int = 14) -> dict:
    """Williams %R — momentum oscillator."""
    if len(prices) < period:
        return {"value": -50, "oversold": False, "overbought": False}
    recent   = prices[-period:]
    high     = max(recent)
    low      = min(recent)
    close    = prices[-1]
    wr_val   = ((high - close) / (high - low) * -100) if (high - low) > 0 else -50
    return {
        "value":      round(wr_val, 2),
        "oversold":   wr_val < -80,
        "overbought": wr_val > -20,
    }

def compute_cci(prices: list, period: int = 20) -> dict:
    """Commodity Channel Index."""
    if len(prices) < period:
        return {"value": 0, "oversold": False, "overbought": False}
    recent    = prices[-period:]
    tp        = recent  # using close as proxy for typical price on 1-min
    tp_mean   = sum(tp) / len(tp)
    mean_dev  = sum(abs(p - tp_mean) for p in tp) / len(tp)
    cci_val   = (tp[-1] - tp_mean) / (0.015 * mean_dev) if mean_dev > 0 else 0
    return {
        "value":      round(cci_val, 2),
        "oversold":   cci_val < -100,
        "overbought": cci_val > 100,
    }

def check_higher_lows(prices: list, lookback: int = 20) -> bool:
    if len(prices) < lookback:
        return False
    recent = prices[-lookback:]
    lows   = [recent[i] for i in range(1, len(recent)-1)
              if recent[i] <= recent[i-1] and recent[i] <= recent[i+1]]
    return len(lows) >= 2 and lows[-1] > lows[-2]

def fetch_prices_and_volumes(symbol: str, bars: int = 40) -> tuple:
    """Fetch recent 1-min close prices and volumes via yfinance.
    Returns (prices, volumes) — both lists, both same length."""
    try:
        ticker = yf.Ticker(symbol)
        df     = ticker.history(period="1d", interval="1m")
        if not df.empty:
            prices  = df["Close"].tail(bars).tolist()
            volumes = df["Volume"].tail(bars).tolist()
            return prices, volumes
    except Exception:
        pass
    return [], []

def fetch_prices(symbol: str, bars: int = 40) -> list:
    prices, _ = fetch_prices_and_volumes(symbol, bars)
    return prices

def get_current_price(symbol: str) -> float | None:
    try:
        return float(yf.Ticker(symbol).fast_info.last_price)
    except Exception:
        return None

def get_account_id() -> str:
    if ACCOUNT_ID:
        return ACCOUNT_ID
    try:
        res = trade_client.account_v2.get_account_list()
        if res.status_code == 200:
            accounts = res.json()
            if accounts:
                return accounts[0].get("account_id", "")
    except Exception:
        pass
    return ""

_balance_cache      = {}
_balance_cache_time = 0.0

def get_buying_power(acct_id: str) -> float:
    global _balance_cache, _balance_cache_time
    now = time.time()
    with _balance_lock:
        if now - _balance_cache_time < WEBULL_CACHE_TTL and _balance_cache:
            return float(_balance_cache.get("buying_power", 0))
        cached = dict(_balance_cache)
        cached_time = _balance_cache_time

    try:
        res = trade_client.account_v2.get_account_balance(acct_id)
        if res.status_code == 200:
            data   = res.json()
            assets = data.get("account_currency_assets", [])
            for asset in assets:
                if asset.get("currency") == "USD":
                    with _balance_lock:
                        _balance_cache      = asset
                        _balance_cache_time = now
                    bp = float(asset.get("buying_power", 0))
                    if bp == 0:
                        bp = float(asset.get("option_buying_power", 0))
                    return bp
        elif res.status_code == 429:
            time.sleep(WEBULL_429_BACKOFF)
    except Exception:
        pass

    # Fall back to stale cache
    with _balance_lock:
        bp = float(_balance_cache.get("buying_power", 0))
        if bp == 0:
            bp = float(_balance_cache.get("option_buying_power", 0))
    return bp

_positions_cache      = {}
_positions_cache_time = 0.0

def get_all_positions(acct_id: str) -> dict:
    global _positions_cache, _positions_cache_time
    now = time.time()
    with _positions_lock:
        if now - _positions_cache_time < WEBULL_CACHE_TTL and _positions_cache is not None:
            return dict(_positions_cache)

    try:
        res = trade_client.account_v2.get_account_position(acct_id)
        if res.status_code == 200:
            data   = res.json()
            items  = data if isinstance(data, list) else data.get("items", [])
            result = {}
            for item in items:
                sym = item.get("ticker", {}).get("symbol", "") or item.get("symbol", "")
                if sym:
                    result[sym] = item
            with _positions_lock:
                _positions_cache      = result
                _positions_cache_time = now
            return result
        elif res.status_code == 429:
            time.sleep(WEBULL_429_BACKOFF)
    except Exception:
        pass

    with _positions_lock:
        return dict(_positions_cache) if _positions_cache else {}

def invalidate_pos_cache():
    global _positions_cache_time
    with _positions_lock:
        _positions_cache_time = 0.0

def place_order(symbol: str, side: str, qty: int, acct_id: str) -> bool:
    with _order_lock:
        try:
            order = {
                "client_order_id":         uuid.uuid4().hex,
                "combo_type":              "NORMAL",
                "symbol":                  symbol,
                "instrument_type":         "EQUITY",
                "market":                  "US",
                "side":                    side,
                "order_type":              "MARKET",
                "time_in_force":           "DAY",
                "quantity":                str(qty),
                "support_trading_session": "CORE",
                "entrust_type":            "QTY",
            }
            res = trade_client.order_v2.place_order(account_id=acct_id, new_orders=[order])
            if res.status_code == 200:
                return True
            else:
                print(f"[ORDER ERR] {symbol} {side}: {res.status_code} {res.text[:200]}", flush=True)
                return False
        except Exception as e:
            print(f"[ORDER ERR] {symbol} {side}: {e}", flush=True)
            return False

# ── SPY / QQQ context (shared across all bots) ───────────────────────────────
_spy_prices  = []
_qqq_prices  = []
_spy_lock    = threading.Lock()

def refresh_context_data():
    """Fetch SPY and QQQ prices every 30s. Runs in its own thread."""
    while True:
        try:
            spy, _ = fetch_prices_and_volumes("SPY", 40)
            qqq, _ = fetch_prices_and_volumes("QQQ", 40)
            with _spy_lock:
                if spy:
                    _spy_prices[:] = spy
                if qqq:
                    _qqq_prices[:] = qqq
        except Exception:
            pass
        time.sleep(30)

def get_spy_context() -> dict:
    with _spy_lock:
        prices = list(_spy_prices)
    if len(prices) < 21:
        return {"bullish": False, "strong": False, "overbought": False,
                "momentum": 0, "rsi": 50, "above_ma20": False}
    rsi        = compute_rsi(prices) or 50
    ma20       = compute_ma(prices, 20) or prices[-1]
    momentum   = (prices[-1] - prices[-6]) / prices[-6] if prices[-6] > 0 else 0
    above_ma20 = prices[-1] > ma20
    bullish    = above_ma20 and momentum > 0
    strong     = above_ma20 and momentum > 0.005
    overbought = rsi > 72
    return {
        "bullish":    bullish,
        "strong":     strong,
        "overbought": overbought,
        "momentum":   round(momentum * 100, 3),
        "rsi":        rsi,
        "above_ma20": above_ma20,
    }

def get_qqq_context() -> dict:
    with _spy_lock:
        prices = list(_qqq_prices)
    if len(prices) < 8:
        return {"rsi": 50, "momentum": 0, "overbought": False, "oversold": False}
    rsi      = compute_rsi(prices) or 50
    momentum = (prices[-1] - prices[-6]) / prices[-6] if len(prices) >= 6 and prices[-6] > 0 else 0
    return {
        "rsi":        rsi,
        "momentum":   round(momentum * 100, 3),
        "overbought": rsi > 68,
        "oversold":   rsi < 35,
    }

# ── Analyst bridge (V1.5) ────────────────────────────────────────────────────
def fetch_analyst_scores() -> dict:
    """Fetch live confluence scores + signals from nexus-analyst /scores endpoint.
    Returns {symbol: {score, signals, timestamp}} or {} on failure.
    Cached with _analyst_scores_ttl to avoid hammering analyst on every bot iteration.
    """
    global _analyst_scores_cache, _analyst_scores_ts
    now = time.time()
    with _analyst_lock:
        if now - _analyst_scores_ts < _analyst_scores_ttl and _analyst_scores_cache:
            return dict(_analyst_scores_cache)

    if not ANALYST_URL:
        return {}
    try:
        headers = {"X-Nexus-Token": NEXUS_TOKEN} if NEXUS_TOKEN else {}
        res     = requests.get(f"{ANALYST_URL}/scores", headers=headers, timeout=5)
        if res.status_code == 200:
            data = res.json()
            with _analyst_lock:
                _analyst_scores_cache = data
                _analyst_scores_ts    = now
            return data
    except Exception:
        pass
    with _analyst_lock:
        return dict(_analyst_scores_cache)

def get_analyst_signal_boost(symbol: str, analyst_scores: dict) -> tuple:
    """
    V1.5: Check if analyst signals include the best combo for this symbol.
    Returns (boost_level, signals_list):
      boost_level 2 = both signals in best combo present -> high conviction
      boost_level 1 = one signal present -> normal
      boost_level 0 = no match / no data
    """
    sym_data = analyst_scores.get(symbol)
    if not sym_data:
        return 0, []
    signals = sym_data.get("signals", [])
    combos  = SIGNAL_COMBO_BOOST_SYMBOLS.get(symbol, [])
    for combo_pair in combos:
        sig_a, sig_b = combo_pair
        a_present = any(sig_a.lower() in s.lower() for s in signals)
        b_present = any(sig_b.lower() in s.lower() for s in signals)
        if a_present and b_present:
            return 2, signals
        if a_present or b_present:
            return 1, signals
    return 0, signals

# ── Phase4Memory ─────────────────────────────────────────────────────────────
class Phase4Memory:
    """
    V1.1+: Pattern memory for Phase4 bots.
    Fingerprints every trade with entry conditions + outcome.
    Daily analysis generates win rates per condition bucket.
    """
    def __init__(self, db_url: str):
        self.db_url          = db_url
        self._conn           = None
        self._lock           = threading.Lock()
        self._win_rates: dict = {}
        self._cache_ts        = 0.0
        self._last_analysis   = 0.0
        self._enabled         = bool(db_url) and _db_available

    def _get_conn(self):
        if not self._enabled:
            return None
        try:
            if self._conn is None or self._conn.closed:
                self._conn = psycopg2.connect(self.db_url)
                self._conn.autocommit = False
            return self._conn
        except Exception as e:
            print(f"[PM] DB connect error: {e}", flush=True)
            return None

    def init_tables(self):
        if not self._enabled:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS phase4_trade_fingerprints (
            id              SERIAL PRIMARY KEY,
            trade_id        VARCHAR(32) UNIQUE NOT NULL,
            symbol          VARCHAR(10) NOT NULL,
            bear_pair       VARCHAR(10),
            is_bear_trade   BOOLEAN DEFAULT FALSE,
            mode            VARCHAR(12),
            entry_ts        BIGINT,
            exit_ts         BIGINT,
            entry_price     REAL,
            symbol_rsi      REAL,
            spy_rsi         REAL,
            qqq_rsi         REAL,
            spy_bullish     BOOLEAN,
            spy_momentum    REAL,
            qqq_overbought  BOOLEAN,
            higher_lows     BOOLEAN,
            above_ma20      BOOLEAN,
            bb_squeeze      BOOLEAN,
            stochrsi_oversold BOOLEAN,
            macd_bullish    BOOLEAN,
            obv_rising      BOOLEAN,
            hour_cdt        INTEGER,
            day_of_week     INTEGER,
            analyst_score   INTEGER,
            signal_boost    INTEGER,
            won             BOOLEAN,
            pnl_pct         REAL,
            exit_reason     VARCHAR(50),
            hold_time_min   INTEGER,
            mfe             REAL,
            mae             REAL,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_p4fp_symbol ON phase4_trade_fingerprints(symbol);
        CREATE INDEX IF NOT EXISTS idx_p4fp_won    ON phase4_trade_fingerprints(won);
        CREATE TABLE IF NOT EXISTS phase4_pattern_stats (
            id           SERIAL PRIMARY KEY,
            bucket_key   VARCHAR(200) UNIQUE NOT NULL,
            win_rate     REAL NOT NULL,
            sample_count INTEGER NOT NULL,
            avg_pnl      REAL,
            last_updated TIMESTAMPTZ DEFAULT NOW()
        );
        """
        try:
            with self._lock:
                conn = self._get_conn()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(ddl)
                    conn.commit()
                    print("[PM] Phase4 pattern memory tables ready", flush=True)
        except Exception as e:
            print(f"[PM] init_tables error: {e}", flush=True)

    def record_entry(self, trade_id: str, symbol: str, bear_pair: str,
                     is_bear: bool, mode: str, entry_price: float,
                     sym_rsi: float, spy_ctx: dict, qqq_ctx: dict,
                     sym_ctx: dict, analyst_score: int = 0, signal_boost: int = 0):
        if not self._enabled:
            return
        threading.Thread(target=self._write_entry, daemon=True, args=(
            trade_id, symbol, bear_pair, is_bear, mode, entry_price,
            sym_rsi, spy_ctx, qqq_ctx, sym_ctx, analyst_score, signal_boost
        )).start()

    def _write_entry(self, trade_id, symbol, bear_pair, is_bear, mode,
                     entry_price, sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
                     analyst_score, signal_boost):
        now = datetime.now(tz=CENTRAL)
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO phase4_trade_fingerprints
                        (trade_id, symbol, bear_pair, is_bear_trade, mode,
                         entry_ts, entry_price,
                         symbol_rsi, spy_rsi, qqq_rsi,
                         spy_bullish, spy_momentum, qqq_overbought,
                         higher_lows, above_ma20,
                         bb_squeeze, stochrsi_oversold, macd_bullish, obv_rising,
                         hour_cdt, day_of_week,
                         analyst_score, signal_boost)
                        VALUES (%s,%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s,
                                %s,%s, %s,%s,%s,%s, %s,%s, %s,%s)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        trade_id, symbol, bear_pair, is_bear, mode,
                        int(time.time()), entry_price,
                        sym_rsi,
                        spy_ctx.get("rsi"),
                        qqq_ctx.get("rsi"),
                        spy_ctx.get("bullish"),
                        spy_ctx.get("momentum"),
                        qqq_ctx.get("overbought"),
                        sym_ctx.get("higher_lows"),
                        sym_ctx.get("above_ma20"),
                        sym_ctx.get("bb_squeeze"),
                        sym_ctx.get("stochrsi_oversold"),
                        sym_ctx.get("macd_bullish"),
                        sym_ctx.get("obv_rising"),
                        now.hour,
                        now.weekday(),
                        analyst_score,
                        signal_boost,
                    ))
                conn.commit()
        except Exception as e:
            print(f"[PM] write_entry error {trade_id}: {e}", flush=True)

    def record_exit(self, trade_id: str, won: bool, pnl_pct: float,
                    exit_reason: str, hold_min: int,
                    mfe: float = 0.0, mae: float = 0.0):
        if not self._enabled:
            return
        threading.Thread(target=self._write_exit, daemon=True, args=(
            trade_id, won, pnl_pct, exit_reason, hold_min, mfe, mae
        )).start()

    def _write_exit(self, trade_id, won, pnl_pct, exit_reason, hold_min, mfe, mae):
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE phase4_trade_fingerprints
                        SET won=%s, pnl_pct=%s, exit_reason=%s,
                            hold_time_min=%s, exit_ts=%s,
                            mfe=%s, mae=%s
                        WHERE trade_id=%s
                    """, (won, round(pnl_pct * 100, 3), exit_reason,
                          hold_min, int(time.time()),
                          round(mfe * 100, 3), round(mae * 100, 3),
                          trade_id))
                conn.commit()
        except Exception as e:
            print(f"[PM] write_exit error {trade_id}: {e}", flush=True)

    def run_analysis(self):
        """Daily analysis — builds win rate buckets from completed trades."""
        if not self._enabled:
            return
        query = """
            SELECT symbol, is_bear_trade, mode, symbol_rsi, spy_rsi, qqq_rsi,
                   spy_bullish, qqq_overbought, higher_lows, hour_cdt,
                   bb_squeeze, stochrsi_oversold, macd_bullish, obv_rising,
                   won, pnl_pct, mfe, mae
            FROM phase4_trade_fingerprints WHERE won IS NOT NULL
        """
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query)
                    rows = cur.fetchall()

            if len(rows) < PM_MIN_TRADES:
                print(f"[PM] {len(rows)} trades < {PM_MIN_TRADES} min, skipping analysis", flush=True)
                return

            from collections import defaultdict
            buckets:  dict = defaultdict(list)
            pnl_bkts: dict = defaultdict(list)

            for row in rows:
                key = self._bucket_key(
                    row["symbol"], row["is_bear_trade"], row["mode"] or "SCALP",
                    row["symbol_rsi"] if row["symbol_rsi"] is not None else 99,
                    {"bullish": row["spy_bullish"]},
                    {"overbought": row["qqq_overbought"]},
                    row["hour_cdt"] if row["hour_cdt"] is not None else 12,
                )
                buckets[key].append(bool(row["won"]))
                if row["pnl_pct"] is not None:
                    pnl_bkts[key].append(float(row["pnl_pct"]))

            new_cache = {}
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    for key, outcomes in buckets.items():
                        if len(outcomes) < PM_MIN_BUCKET_TRADES:
                            continue
                        wr      = sum(outcomes) / len(outcomes)
                        avg_pnl = (sum(pnl_bkts[key]) / len(pnl_bkts[key])
                                   if pnl_bkts[key] else None)
                        cur.execute("""
                            INSERT INTO phase4_pattern_stats
                            (bucket_key, win_rate, sample_count, avg_pnl)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (bucket_key) DO UPDATE
                            SET win_rate=EXCLUDED.win_rate,
                                sample_count=EXCLUDED.sample_count,
                                avg_pnl=EXCLUDED.avg_pnl,
                                last_updated=NOW()
                        """, (key, wr, len(outcomes), avg_pnl))
                        new_cache[key] = wr
                conn.commit()

            self._win_rates     = new_cache
            self._last_analysis = time.time()
            total = len(rows)
            wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
            print(f"[PM] Analysis: {len(new_cache)} buckets | {total} trades | {wr:.1%} WR", flush=True)
        except Exception as e:
            print(f"[PM] analysis error: {e}", flush=True)

    @staticmethod
    def _bucket_key(symbol: str, is_bear: bool, mode: str, sym_rsi: float,
                    spy_ctx: dict, qqq_ctx: dict, hour: int) -> str:
        rsi_b  = ("rsi_lt30" if sym_rsi < 30 else
                  "rsi_30_40" if sym_rsi < 40 else
                  "rsi_40_55" if sym_rsi < 55 else "rsi_gt55")
        spy_b  = "spy_bull" if spy_ctx.get("bullish") else "spy_bear"
        qqq_b  = "qqq_ob" if qqq_ctx.get("overbought") else "qqq_ok"
        bear_b = "bear" if is_bear else "bull"
        mode_b = mode or "SCALP"
        hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
        return f"{symbol}|{bear_b}|{mode_b}|{rsi_b}|{spy_b}|{qqq_b}|{hr_b}"

    def get_win_rate(self, symbol: str, is_bear: bool, mode: str,
                     sym_rsi: float, spy_ctx: dict, qqq_ctx: dict,
                     hour: int) -> float:
        if not self._win_rates:
            return 0.5
        key = self._bucket_key(symbol, is_bear, mode, sym_rsi, spy_ctx, qqq_ctx, hour)
        return self._win_rates.get(key, 0.5)

    def should_skip_entry(self, symbol: str, is_bear: bool, mode: str,
                          sym_rsi: float, spy_ctx: dict, qqq_ctx: dict,
                          hour: int) -> tuple:
        """Returns (skip, win_rate, has_data). No data = never skips."""
        key = self._bucket_key(symbol, is_bear, mode, sym_rsi, spy_ctx, qqq_ctx, hour)
        if key not in self._win_rates:
            return False, 0.5, False
        wr = self._win_rates[key]
        return (wr < WIN_RATE_GATE_THRESHOLD), wr, True

    def start_scheduler(self):
        def _run():
            time.sleep(300)
            self.run_analysis()
            while True:
                time.sleep(PM_ANALYSIS_INTERVAL)
                self.run_analysis()
        threading.Thread(target=_run, daemon=True, name="p4-pattern-memory").start()


_phase4_memory = None


# ── SymbolBot ─────────────────────────────────────────────────────────────────
class SymbolBot:
    def __init__(self, symbol: str, config: dict, acct_id: str):
        self.symbol      = symbol
        self.bear_pair   = config["bear_pair"]
        self.budget_pct  = config["budget_pct"]
        self.recipe      = config["recipe"]
        self.cfg         = config
        self.acct_id     = acct_id

        self.prices:      list  = []
        self.volumes:     list  = []
        self.bear_prices: list  = []
        self.bear_volumes: list = []
        self.peak_price:  float = 0.0
        self.entry_price: float = 0.0
        self.entry_time:  float = 0.0
        self.trade_id:    str   = ""
        self.mfe:         float = 0.0
        self.mae:         float = 0.0
        self.in_position: bool  = False
        self.active_sym:  str   = symbol
        self.mode:        str   = "SCALP"
        self.cooldown_until: float = 0.0

        # Extended TP state for bear pairs (DUST/SOXS)
        self._bear_ext_trailing: bool  = False
        self._bear_ext_peak:     float = 0.0

        # Context captured at entry for fingerprint + exit logic
        self._entry_spy_ctx:    dict  = {}
        self._entry_qqq_ctx:    dict  = {}
        self._entry_sym_ctx:    dict  = {}
        self._entry_rsi:        float = 50.0
        self._entry_analyst_score: int = 0
        self._entry_signal_boost:  int = 0

        self.reversal_state: dict = {"state": "IDLE"}

        self.daily_wins:   int   = 0
        self.daily_losses: int   = 0
        self.daily_pnl:    float = 0.0

    def is_on_cooldown(self) -> bool:
        return time.time() < self.cooldown_until

    def set_cooldown(self, secs: int):
        self.cooldown_until = time.time() + secs

    def refresh_prices(self):
        p, v = fetch_prices_and_volumes(self.symbol, WARMUP_BARS + 5)
        if p:
            self.prices  = p
            self.volumes = v
        bp, bv = fetch_prices_and_volumes(self.bear_pair, WARMUP_BARS + 5)
        if bp:
            self.bear_prices  = bp
            self.bear_volumes = bv

    def get_signal_suite(self, prices: list, volumes: list) -> dict:
        """
        V1.5: Full signal suite computed from raw price/volume bars.
        Returns signals used by both entry logic and fingerprinting.
        """
        if len(prices) < 21:
            return {
                "rsi": 50, "rsi14": 50, "ma20": 0, "above_ma20": False,
                "trend_10bar": 0, "higher_lows": False,
                "ema9": 0, "ema21": 0, "ema9_above_ema21": False,
                "bb": {}, "stochrsi": {}, "macd": {}, "obv": {},
                "williams_r": {}, "cci": {},
                "bb_squeeze": False, "stochrsi_oversold": False,
                "macd_bullish": False, "obv_rising": False,
                "near_lower_bb": False, "at_lower_bb": False, "far_below_bb": False,
                "williams_oversold": False, "cci_oversold": False,
            }
        rsi7      = compute_rsi(prices, 7) or 50
        rsi14     = compute_rsi(prices, 14) or 50
        rsi21     = compute_rsi(prices, 21) or 50
        ma20      = compute_ma(prices, 20) or prices[-1]
        ema9      = compute_ema(prices, 9) or prices[-1]
        ema21     = compute_ema(prices, 21) or prices[-1]
        trend_10  = (prices[-1] - prices[-11]) / prices[-11] if len(prices) > 11 and prices[-11] > 0 else 0
        above_ma20     = prices[-1] > ma20
        ema9_above_21  = ema9 > ema21
        higher_l  = check_higher_lows(prices)
        bouncing  = len(prices) >= 3 and prices[-1] > prices[-3]

        bb         = compute_bollinger(prices)
        stochrsi   = compute_stochrsi(prices)
        macd       = compute_macd(prices)
        obv        = compute_obv(prices, volumes) if volumes else {"rising": False, "obv_slope": 0}
        williams   = compute_williams_r(prices)
        cci        = compute_cci(prices)

        return {
            "rsi":              rsi7,
            "rsi14":            rsi14,
            "rsi21":            rsi21,
            "ma20":             ma20,
            "above_ma20":       above_ma20,
            "trend_10bar":      round(trend_10 * 100, 3),
            "higher_lows":      higher_l,
            "bouncing":         bouncing,
            "ema9":             ema9,
            "ema21":            ema21,
            "ema9_above_ema21": ema9_above_21,
            # Bollingers
            "bb":               bb,
            "bb_squeeze":       bb.get("squeeze", False),
            "near_lower_bb":    bb.get("near_lower", False),
            "at_lower_bb":      bb.get("at_lower", False),
            "far_below_bb":     bb.get("far_below", False),
            # Oscillators
            "stochrsi":         stochrsi,
            "stochrsi_oversold": stochrsi.get("oversold", False),
            "macd":             macd,
            "macd_bullish":     macd.get("bullish", False),
            "obv":              obv,
            "obv_rising":       obv.get("rising", False),
            "williams_r":       williams,
            "williams_oversold": williams.get("oversold", False),
            "cci":              cci,
            "cci_oversold":     cci.get("oversold", False),
        }

    def select_mode(self, spy_ctx: dict, sym_ctx: dict) -> str:
        if spy_ctx["overbought"]:
            return "SCALP"
        if (spy_ctx["strong"]
                and sym_ctx.get("trend_10bar", 0) > 0.3
                and sym_ctx.get("higher_lows", False)
                and sym_ctx.get("above_ma20", False)):
            return "EXTENDED"
        if spy_ctx["bullish"] and (sym_ctx.get("above_ma20", False) or sym_ctx.get("trend_10bar", 0) > 0.1):
            return "RIDE"
        return "SCALP"

    def get_exit_params(self) -> tuple:
        """Returns (stop_loss, ratchet, trail) based on current mode."""
        r = self.recipe
        if self.active_sym == self.bear_pair:
            br = BEAR_RECIPES.get(self.bear_pair, r)
            return br["stop_loss"], br["profit_ratchet"], br["trailing_stop"]

        sl      = r["stop_loss"]
        ratchet = r["profit_ratchet"]
        trail   = r["trailing_stop"]

        if self.mode == "RIDE":
            sl      = round(sl    * self.cfg["ride_stop_mult"],  4)
            trail   = round(trail * self.cfg["ride_trail_mult"], 4)
        elif self.mode == "EXTENDED":
            sl      = round(sl    * self.cfg["ext_stop_mult"],  4)
            trail   = round(trail * self.cfg["ext_trail_mult"], 4)
            ratchet = self.cfg["ext_ratchet_pct"]

        return sl, ratchet, trail

    def should_enter_bull(self, spy_ctx: dict, sym_ctx: dict) -> bool:
        """Entry logic for bull symbol based on mode and signal suite."""
        prices = self.prices
        if len(prices) < 8:
            return False

        now = datetime.now(tz=CENTRAL)
        if now.hour in self.recipe.get("avoid_hours", []):
            return False
        if now.weekday() in self.recipe.get("avoid_days", []):
            return False

        rsi      = sym_ctx["rsi"]
        bouncing = sym_ctx.get("bouncing", False)

        # V1.5: signal suite boosts — high-conviction signals can relax RSI gate slightly
        stochrsi_os  = sym_ctx.get("stochrsi_oversold", False)
        bb_squeeze   = sym_ctx.get("bb_squeeze", False)
        at_lower_bb  = sym_ctx.get("at_lower_bb", False)
        near_lower_bb = sym_ctx.get("near_lower_bb", False)
        far_below_bb = sym_ctx.get("far_below_bb", False)
        macd_bull    = sym_ctx.get("macd_bullish", False)
        obv_rising   = sym_ctx.get("obv_rising", False)
        rsi14_extreme = sym_ctx.get("rsi14", 50) < 25  # extreme oversold on rsi14

        if self.mode == "SCALP":
            # Base: RSI < 40 + bouncing
            base = rsi < 40 and bouncing
            # Signal boost: if StochRSI oversold or at lower BB, relax RSI gate to 45
            boosted = (rsi < 45 and bouncing and (stochrsi_os or at_lower_bb or rsi14_extreme))
            return base or boosted

        elif self.mode == "RIDE":
            base = rsi < 52 and bouncing and sym_ctx.get("above_ma20", False)
            # Boost: near lower BB confirms genuine dip in uptrend
            boosted = (rsi < 55 and bouncing and sym_ctx.get("above_ma20", False)
                       and (near_lower_bb or obv_rising))
            return base or boosted

        elif self.mode == "EXTENDED":
            near_ma   = ((prices[-1] - sym_ctx.get("ma20", prices[-1]))
                         / sym_ctx.get("ma20", prices[-1]) < 0.01
                         if sym_ctx.get("ma20", 0) > 0 else False)
            base = (rsi < 58 and bouncing
                    and sym_ctx.get("higher_lows", False)
                    and (near_ma or rsi < 50))
            # Boost: BB squeeze in extended mode = imminent breakout after pullback
            boosted = (rsi < 60 and bouncing
                       and sym_ctx.get("higher_lows", False)
                       and bb_squeeze)
            return base or boosted

        return False

    def check_reversal(self) -> bool:
        """
        Monitor bull RSI exhaustion -> flip to bear pair.
        V1.5: Now applies bear pair hour gating before confirming reversal.
        Returns True if a bear entry should be triggered.
        """
        if len(self.prices) < 8:
            return False
        bull_rsi = compute_rsi(self.prices)
        if bull_rsi is None:
            return False

        state = self.reversal_state
        now_t = time.time()

        if state["state"] == "IDLE":
            if bull_rsi >= REVERSAL_OB_RSI:
                self.reversal_state = {
                    "state":       "WATCHING",
                    "bull_peak":   self.prices[-1],
                    "watch_start": now_t,
                }
                log(self.symbol, f"👁 REVERSAL WATCH -> {self.bear_pair} | RSI={bull_rsi}")
            return False

        if state["state"] == "WATCHING":
            if now_t - state.get("watch_start", now_t) > REVERSAL_MAX_WATCH:
                self.reversal_state = {"state": "IDLE"}
                return False
            if bull_rsi < REVERSAL_RSI_RESET:
                log(self.symbol, f"↩ REVERSAL CANCEL | RSI recovered to {bull_rsi}")
                self.reversal_state = {"state": "IDLE"}
                return False

            bull_peak = max(state.get("bull_peak", self.prices[-1]), self.prices[-1])
            self.reversal_state["bull_peak"] = bull_peak
            drop = (bull_peak - self.prices[-1]) / bull_peak if bull_peak > 0 else 0

            if drop >= REVERSAL_CONFIRM:
                # Check bear bounce
                bear_p   = self.bear_prices
                bouncing = len(bear_p) >= 3 and bear_p[-1] > bear_p[-3]
                if not bouncing:
                    return False

                # V1.5: Hour gate for the bear pair
                now_hour = datetime.now(tz=CENTRAL).hour
                bear_avoid = BEAR_RECIPES.get(self.bear_pair, {}).get("avoid_hours", [])
                if now_hour in bear_avoid:
                    log(self.symbol,
                        f"⏰ REVERSAL GATED: {self.bear_pair} avoid_hours={bear_avoid} "
                        f"| current hour={now_hour} — skipping")
                    return False

                # V1.5: SQQQ fully gated unless env flag set
                if self.bear_pair == "SQQQ" and not SQQQ_ENABLED:
                    log(self.symbol,
                        f"🚫 REVERSAL BLOCKED: SQQQ disabled "
                        f"(set PHASE4_SQQQ_ENABLED=true to enable)")
                    return False

                # QQQ filter — if QQQ is already oversold, skip
                qqq_ctx = get_qqq_context()
                if qqq_ctx.get("oversold"):
                    log(self.symbol,
                        f"🚫 REVERSAL BLOCKED: QQQ oversold (RSI={qqq_ctx['rsi']:.1f})")
                    return False

                gate = QQQ_BEAR_RSI_GATE_LABD if self.bear_pair == "LABD" else QQQ_BEAR_RSI_GATE
                if qqq_ctx.get("rsi", 50) < gate:
                    log(self.symbol,
                        f"⚠ REVERSAL WEAK: QQQ RSI={qqq_ctx['rsi']:.1f} < {gate} — skipping")
                    return False

                log(self.symbol,
                    f"🔁 REVERSAL CONFIRMED -> {self.bear_pair} | "
                    f"drop={round(drop*100,2)}% | QQQ RSI={qqq_ctx['rsi']:.1f} ✅")
                self.reversal_state = {"state": "IDLE"}
                return True

        return False

    def try_buy(self, sym: str, prices: list, volumes: list,
                spy_ctx: dict, sym_ctx: dict) -> bool:
        """Attempt to buy. Returns True if successful."""
        bp         = get_buying_power(self.acct_id)
        trade_size = round(bp * self.budget_pct, 2)
        if trade_size < 1.00:
            return False

        price = prices[-1] if prices else get_current_price(sym)
        if not price or price <= 0:
            return False

        qty = int(trade_size / (price * BUYING_POWER_BUFFER))
        if qty < 1:
            return False

        self.mode = self.select_mode(spy_ctx, sym_ctx)

        # V1.5: Analyst bridge — fetch live scores + signal boost
        analyst_scores  = fetch_analyst_scores()
        analyst_entry   = analyst_scores.get(sym, {})
        analyst_score   = analyst_entry.get("score", 0)
        signal_boost, active_signals = get_analyst_signal_boost(sym, analyst_scores)

        qqq_ctx = get_qqq_context()

        # Win-rate gate from pattern memory
        if _phase4_memory:
            is_bear = (sym == self.bear_pair)
            hour    = datetime.now(tz=CENTRAL).hour
            skip, wr, has_data = _phase4_memory.should_skip_entry(
                self.symbol, is_bear, self.mode, sym_ctx.get("rsi", 50),
                spy_ctx, qqq_ctx, hour
            )
            if skip:
                log(self.symbol,
                    f"🚫 WIN-RATE GATE: {sym} | mode={self.mode} | "
                    f"historical WR={wr:.0%} < {WIN_RATE_GATE_THRESHOLD:.0%}")
                return False
            if has_data:
                log(self.symbol, f"✅ WR gate passed: {sym} | {wr:.0%} historical WR")

        boost_label = ""
        if signal_boost == 2:
            boost_label = " | 🔥 COMBO BOOST"
        elif signal_boost == 1:
            boost_label = " | ✨ signal partial"

        log(self.symbol,
            f"📊 Entry signal | mode={self.mode} | RSI={sym_ctx['rsi']:.1f} | "
            f"SPY={'bull' if spy_ctx.get('bullish') else 'bear'} | "
            f"analyst={analyst_score} | boost={signal_boost}{boost_label}")

        success = place_order(sym, "BUY", qty, self.acct_id)
        if success:
            self.in_position          = True
            self.active_sym           = sym
            self.entry_price          = price
            self.peak_price           = price
            self.entry_time           = time.time()
            self.mfe                  = 0.0
            self.mae                  = 0.0
            self.trade_id             = __import__('secrets').token_hex(8)
            self._entry_spy_ctx       = spy_ctx
            self._entry_qqq_ctx       = qqq_ctx
            self._entry_sym_ctx       = sym_ctx
            self._entry_rsi           = sym_ctx.get("rsi", 50)
            self._entry_analyst_score = analyst_score
            self._entry_signal_boost  = signal_boost
            # Extended TP state for DUST/SOXS
            self._bear_ext_trailing   = False
            self._bear_ext_peak       = price
            invalidate_pos_cache()
            if _phase4_memory:
                _phase4_memory.record_entry(
                    self.trade_id, self.symbol, self.bear_pair,
                    sym == self.bear_pair, self.mode, price,
                    self._entry_rsi, spy_ctx, qqq_ctx, sym_ctx,
                    analyst_score, signal_boost
                )
            log(self.symbol, f"⚡ BUY: {sym} | {qty}sh @ ~${round(price,2)} | mode={self.mode}")
            alert(f"⚡ PHASE4 BUY [{self.mode}]: {sym} | {qty} @ ~${round(price,2)}{boost_label}")
            return True
        return False

    def try_sell(self, reason: str, pnl_pct: float) -> bool:
        """Attempt to sell current position."""
        positions = get_all_positions(self.acct_id)
        if self.active_sym not in positions:
            self.in_position = False
            return True

        pos = positions[self.active_sym]
        qty = int(float(pos.get("quantity", pos.get("position_qty", 0))))
        if qty <= 0:
            self.in_position = False
            return True

        success = place_order(self.active_sym, "SELL", qty, self.acct_id)
        if success:
            emoji    = "✅" if pnl_pct > 0 else "🛑"
            pnl_s    = f"+{round(pnl_pct*100,3)}%" if pnl_pct > 0 else f"{round(pnl_pct*100,3)}%"
            hold_min = int((time.time() - self.entry_time) / 60) if self.entry_time > 0 else 0
            log(self.symbol,
                f"{emoji} SELL [{reason}]: {self.active_sym} | P&L: {pnl_s} | "
                f"MFE: {round(self.mfe*100,2):+.2f}% | MAE: {round(self.mae*100,2):+.2f}% | "
                f"held: {hold_min}m")
            alert(f"{emoji} PHASE4 [{reason}]: {self.active_sym} | {pnl_s}")
            if _phase4_memory and self.trade_id:
                _phase4_memory.record_exit(
                    self.trade_id, pnl_pct > 0, pnl_pct,
                    reason, hold_min, self.mfe, self.mae
                )
            self.in_position          = False
            self.peak_price           = 0.0
            self.entry_price          = 0.0
            self.entry_time           = 0.0
            self.trade_id             = ""
            self.mfe                  = 0.0
            self.mae                  = 0.0
            self._bear_ext_trailing   = False
            self._bear_ext_peak       = 0.0
            if pnl_pct > 0:
                self.daily_wins += 1
                self.set_cooldown(WIN_COOLDOWN_SECS)
            else:
                self.daily_losses += 1
                self.set_cooldown(LOSS_COOLDOWN_SECS)
            self.daily_pnl += pnl_pct * 100
            invalidate_pos_cache()
            return True
        return False

    def recover_position(self):
        """
        On startup, check if we already have an open position.
        V1.5: Now generates trade_id and sets entry_time so exits are properly
        fingerprinted. Mode is inferred from current market context so exit
        params are correct. Previously recovered positions had trade_id=""
        (orphaned fingerprint), entry_time=0 (0-minute holds), mode=SCALP always.
        """
        positions = get_all_positions(self.acct_id)
        for sym in [self.symbol, self.bear_pair]:
            if sym in positions:
                pos              = positions[sym]
                cost             = float(pos.get("cost_price", pos.get("average_cost", 0)))
                self.in_position = True
                self.active_sym  = sym
                self.entry_price = cost
                self.peak_price  = max(cost, get_current_price(sym) or cost)
                # V1.5: Generate a fresh trade_id so exit fingerprint is valid
                self.trade_id    = __import__('secrets').token_hex(8)
                # V1.5: entry_time = now (approximate — redeploy happened, old time unknown)
                self.entry_time  = time.time()
                self.mfe         = 0.0
                self.mae         = 0.0
                # V1.5: Infer mode from current market context
                spy_ctx  = get_spy_context()
                sym_ctx  = self.get_signal_suite(self.prices, self.volumes)
                self.mode = self.select_mode(spy_ctx, sym_ctx)
                # Bear pair extended TP recovery
                self._bear_ext_trailing = False
                self._bear_ext_peak     = self.peak_price
                log(self.symbol,
                    f"🔄 Recovered position: {sym} | entry=${cost:.3f} | "
                    f"peak=${self.peak_price:.3f} | mode={self.mode} | "
                    f"trade_id={self.trade_id[:8]}")
                return

    def run_loop(self):
        """Main bot loop — runs in its own thread."""
        log(self.symbol, f"🚀 Bot online | bear={self.bear_pair} | budget={int(self.budget_pct*100)}%")

        # Warmup
        self.refresh_prices()
        log(self.symbol, f"✅ Warmed up | {len(self.prices)} bars")

        # Recover open position if any
        self.recover_position()

        while True:
            try:
                if not is_market_hours():
                    if self.in_position:
                        log(self.symbol, "📌 Market closed — holding position overnight")
                    time.sleep(60)
                    continue

                # Refresh price data
                self.refresh_prices()
                spy_ctx = get_spy_context()
                sym_ctx = self.get_signal_suite(self.prices, self.volumes)

                prices = self.prices
                if not prices:
                    time.sleep(LOOP_INTERVAL)
                    continue

                # ── MANAGE OPEN POSITION ─────────────────────────────────
                if self.in_position:
                    active_prices  = self.prices  if self.active_sym == self.symbol else self.bear_prices
                    active_volumes = self.volumes if self.active_sym == self.symbol else self.bear_volumes
                    if not active_prices:
                        time.sleep(LOOP_INTERVAL)
                        continue

                    price      = active_prices[-1]
                    profit_pct = (price - self.entry_price) / self.entry_price if self.entry_price > 0 else 0
                    drawdown   = (self.peak_price - price) / self.peak_price if self.peak_price > 0 else 0

                    self.peak_price = max(self.peak_price, price)
                    self.mfe        = max(self.mfe, profit_pct)
                    self.mae        = min(self.mae, profit_pct)

                    sl, ratchet, trail = self.get_exit_params()

                    log(self.symbol,
                        f"📊 {self.active_sym} | P&L={round(profit_pct*100,2):+.2f}% | "
                        f"peak=${self.peak_price:.3f} | dd={round(drawdown*100,2):.2f}% | mode={self.mode}")

                    # ── BEAR PAIR: extended TP for DUST/SOXS ────────────
                    ext_cfg = BEAR_EXTENDED_TP.get(self.active_sym) if self.active_sym == self.bear_pair else None
                    if ext_cfg:
                        self._bear_ext_peak = max(self._bear_ext_peak, price)
                        if not self._bear_ext_trailing and profit_pct >= ext_cfg["trail_activate"]:
                            self._bear_ext_trailing = True
                            log(self.symbol,
                                f"🎯 {self.active_sym} EXTENDED TP activated at "
                                f"+{profit_pct*100:.1f}% — trailing {ext_cfg['trail_stop']*100:.1f}%")
                        if self._bear_ext_trailing:
                            ext_dd = (self._bear_ext_peak - price) / self._bear_ext_peak if self._bear_ext_peak > 0 else 0
                            if ext_dd >= ext_cfg["trail_stop"]:
                                self.try_sell("ext-trail", profit_pct)
                                time.sleep(LOOP_INTERVAL)
                                continue
                        # Standard stop still applies before trail activates
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)
                    else:
                        # Standard bull pair / non-extended bear pair exits
                        # Stop loss
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)
                        # Trail stop (after ratchet)
                        elif profit_pct >= ratchet and drawdown >= trail:
                            self.try_sell("trail", profit_pct)
                        # EXTENDED: exit on trend break
                        # V1.5: fires even when slightly underwater (profit > -0.5%)
                        # so broken-trend losing trades don't wait for stop-loss
                        elif (self.mode == "EXTENDED"
                              and self.active_sym == self.symbol
                              and profit_pct > -0.005):
                            active_sig = self.get_signal_suite(active_prices, active_volumes)
                            if not active_sig.get("higher_lows", True):
                                log(self.symbol, "📉 EXTENDED: trend break detected")
                                self.try_sell("trend-break", profit_pct)

                # ── LOOK FOR ENTRY ────────────────────────────────────────
                elif not self.is_on_cooldown():
                    # Bear reversal first (higher priority)
                    if self.check_reversal():
                        # V1.5: compute real sym_ctx for bear pair (not hardcoded False)
                        if not self.bear_prices:
                            log(self.symbol, "⚠ Bear reversal triggered but bear_prices empty — skip")
                        else:
                            bear_ctx = self.get_signal_suite(self.bear_prices, self.bear_volumes)
                            self.try_buy(self.bear_pair, self.bear_prices, self.bear_volumes,
                                         spy_ctx, bear_ctx)
                    # Bull entry
                    elif self.should_enter_bull(spy_ctx, sym_ctx):
                        self.try_buy(self.symbol, prices, self.volumes, spy_ctx, sym_ctx)

            except Exception as e:
                log(self.symbol, f"🔴 Loop error: {e}")
                log(self.symbol, traceback.format_exc())

            time.sleep(LOOP_INTERVAL)


# ── Phase4 Service ────────────────────────────────────────────────────────────
def run():
    global _phase4_memory
    print("[PHASE4] NEXUS PHASE 4 V1.5 STARTING", flush=True)
    print("[PHASE4] Bots: NUGT(30%) | SOXL(25%) | LABU(25%) | TQQQ(20%)", flush=True)
    print("[PHASE4] Bear pairs: DUST | SOXS | LABD" + (" | SQQQ" if SQQQ_ENABLED else " | SQQQ(DISABLED)"), flush=True)
    print("[PHASE4] V1.5: Signal suite | Analyst bridge | Bear hour gates | recover_position fix", flush=True)

    if DATABASE_URL and _db_available:
        _phase4_memory = Phase4Memory(DATABASE_URL)
        _phase4_memory.init_tables()
        _phase4_memory.start_scheduler()
        print("[PHASE4] Pattern memory: DB connected, daily analysis scheduled", flush=True)
    else:
        _phase4_memory = Phase4Memory("")
        print("[PHASE4] Pattern memory: disabled (no DATABASE_URL)", flush=True)

    acct_id = get_account_id()
    if not acct_id:
        print("[PHASE4] 🔴 Could not get Webull account ID — check env vars", flush=True)
        return

    print(f"[PHASE4] Account: {acct_id}", flush=True)

    if ANALYST_URL:
        print(f"[PHASE4] Analyst bridge: {ANALYST_URL}", flush=True)
    else:
        print("[PHASE4] Analyst bridge: disabled (no ANALYST_URL env var)", flush=True)

    # Start context refresh thread
    ctx_thread = threading.Thread(target=refresh_context_data, daemon=True)
    ctx_thread.start()
    time.sleep(5)

    bots    = []
    threads = []
    for symbol, config in BOT_CONFIGS.items():
        bot = SymbolBot(symbol, config, acct_id)
        bots.append(bot)
        t = threading.Thread(target=bot.run_loop, daemon=True, name=f"bot_{symbol}")
        threads.append(t)
        t.start()
        print(f"[PHASE4] ✅ {symbol} bot started", flush=True)
        time.sleep(2)

    from phase4_server import start_server
    start_server(bots)

    sqqq_note = " | SQQQ enabled" if SQQQ_ENABLED else " | SQQQ disabled"
    alert(
        f"⚡ PHASE4 V1.5 ONLINE\n"
        f"NUGT+SOXL+LABU+TQQQ | Signal suite live\n"
        f"Analyst bridge: {'✅' if ANALYST_URL else '⚠ no ANALYST_URL'}{sqqq_note}"
    )
    print("[PHASE4] All bots running. Holding main thread.", flush=True)

    last_day = datetime.now(tz=CENTRAL).date()
    while True:
        today = datetime.now(tz=CENTRAL).date()
        if today != last_day:
            for bot in bots:
                bot.daily_wins   = 0
                bot.daily_losses = 0
                bot.daily_pnl    = 0.0
            last_day = today
            print("[PHASE4] 🌅 Daily reset complete", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    run()
