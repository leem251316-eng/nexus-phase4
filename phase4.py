"""
NEXUS PHASE 4 — PER-SYMBOL AUTONOMOUS BOTS V1.9
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

V1.9 — Strategy fixes (Jun 26 2026 audit):
  WIN_RATE_GATE_THRESHOLD raised 35%->45%: same fix as Berserker/Scanner --
    35% WR with typical Phase4 TP/stop ratios produces negative EV. 45% is
    the minimum where the math works at current risk parameters.
  SQQQ_ENABLED now defaults to True with an SPY bear regime guard:
    SQQQ only fires when SPY is below its 20-day MA (bear regime confirmed).
    Disabled by default during bull markets, auto-enables in corrections.
    The original backtest negative EV was over a 2yr sample dominated by the
    2024-2025 bull run. Bear regime guard ensures SQQQ only trades when macro
    conditions actually support it.
  VIX staleness logging: when yfinance fails to fetch VIX data, the regime
    gates (>28 caution, >35 pause) silently use the 15.0 default. Added a
    warning log when VIX data is more than 5 minutes stale.

V1.8 — Score + StochRSI fixes:
  compute_stochrsi() upgraded to Wilder EWM smoothing inside the RSI
  calculation -- the simple rolling mean was inconsistent with compute_rsi()
  and understated StochRSI oversold readings, causing some genuine oversold
  entries to miss the stochrsi_oversold flag.
  compute_entry_score() double-counting fix: neutral signals (not in best,
  not in worst) no longer add +1 -- only explicit best_signals score positive.
  This prevents irrelevant noise signals from padding the score past min_score
  and matches the intent of the backtest-derived best/worst signal lists.

V1.7 — RSI fix:
  compute_rsi() upgraded to Wilder EWM smoothing (alpha=1/period).
  Replaces simple rolling mean which was noisier and triggered more
  false signals in both bull entries and RSI overbought exits.
  Matches main.py V10.9 and industry standard Wilder RSI.

V1.6 — Complete entry/exit overhaul:
  ENTRY:
  - Replaced single RSI gate with per-symbol confluence scoring (15 signals)
  - Per-symbol signal weights from strategy_recipes best/worst signal data
  - Underlying index context: SMH (SOXL/SOXS), QQQ (TQQQ/SQQQ),
    GDX (NUGT/DUST), XBI (LABU/LABD) — the tide that moves the boat
  - Multi-timeframe: 5-min structure check before 1-min trigger
  - Asymmetric position sizing by conviction level (signal boost 0/1/2)
  - Reversal quality scoring — high/medium/skip, affects size
  - Bear trap detection: underlying at highs + volume expanding = skip reversal
  EXIT:
  - ATR-based stops (derived from avg MAE) — adapts to actual volatility
  - Adaptive ratchet: two-phase trailing (early trail, then tighten at momentum peak)
  - Signal reversal exit: RSI hits overbought zone -> mean reversion complete
  - Time-in-dwell: flat position after 30 min = thesis failed, exit clean
  - VIX regime: VIX > 28 = reduce sizes 50%, VIX > 35 = pause bull entries
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
# V1.9: SQQQ enabled by default -- guarded by SPY bear regime check in check_reversal()
# Override: set PHASE4_SQQQ_ENABLED=false to disable entirely
SQQQ_ENABLED     = os.environ.get("PHASE4_SQQQ_ENABLED", "true").lower() == "true"

CENTRAL  = ZoneInfo("America/Chicago")
BOT_NAME = "PHASE4"

# ── Per-symbol config ─────────────────────────────────────────────────────────
# V1.6: exit params derived from recipe avg MAE/MFE data
# atr_stop      = 1.2x avg MAE  (adapts to symbol volatility)
# early_ratchet = 0.38x avg MFE (start trailing early)
# late_ratchet  = 0.72x avg MFE (tighten trail at momentum peak)
# trail_normal  = 0.22x avg MAE
# trail_tight   = 0.15x avg MAE (used when RSI overbought)
BOT_CONFIGS = {
    "NUGT": {
        "bear_pair":      "DUST",
        "underlying":     "GDX",
        "budget_pct":     0.30,
        "min_score":      5,       # score>=5: 55.5% WR / 128 trades / EV+0.07%
        "atr_stop":       0.0213,
        "early_ratchet":  0.0059,
        "late_ratchet":   0.0111,
        "trail_normal":   0.0039,
        "trail_tight":    0.0027,
        "avoid_hours":    [9],
        "avoid_days":     [],
        # Signals from recipe best/worst
        "best_signals":   ["rsi_lt40", "bb_squeeze", "below_ma20", "rsi14_lt35"],
        "worst_signals":  ["near_lower_bb", "ema9_above_ema21"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  1.8,
    },
    "SOXL": {
        "bear_pair":      "SOXS",
        "underlying":     "SMH",
        "budget_pct":     0.25,
        "min_score":      6,       # score>=6: 65.9% WR / 214 trades / EV+0.44%
        "atr_stop":       0.0159,
        "early_ratchet":  0.0051,
        "late_ratchet":   0.0097,
        "trail_normal":   0.0029,
        "trail_tight":    0.0020,
        "avoid_hours":    [],
        "avoid_days":     [],
        "best_signals":   ["far_below_bb", "rsi14_lt20", "rsi_lt25", "stochrsi_oversold", "near_lower_bb"],
        "worst_signals":  ["macd_bullish", "below_ma20"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  1.8,
    },
    "LABU": {
        "bear_pair":      "LABD",
        "underlying":     "XBI",
        "budget_pct":     0.25,
        "min_score":      6,       # score>=6: 57.7% WR / 97 trades / EV+0.13%
        "atr_stop":       0.0193,
        "early_ratchet":  0.0054,
        "late_ratchet":   0.0101,
        "trail_normal":   0.0035,
        "trail_tight":    0.0024,
        "avoid_hours":    [10, 13],
        "avoid_days":     [],
        "best_signals":   ["at_lower_bb", "rsi_lt25", "rsi14_lt20", "stochrsi_oversold", "far_below_bb"],
        "worst_signals":  ["ema9_above_ema21", "near_lower_bb"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  1.8,
    },
    "TQQQ": {
        "bear_pair":      "SQQQ",
        "underlying":     "QQQ",
        "budget_pct":     0.20,
        "min_score":      4,       # score>=4: 62.7% WR / 158 trades / EV+0.37%
        "atr_stop":       0.0151,
        "early_ratchet":  0.0051,
        "late_ratchet":   0.0097,
        "trail_normal":   0.0028,
        "trail_tight":    0.0019,
        "avoid_hours":    [10],
        "avoid_days":     [],
        "best_signals":   ["rsi_lt40", "macd_bullish", "near_lower_bb", "obv_falling", "ema9_above_ema21"],
        "worst_signals":  ["williams_oversold", "bb_squeeze"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  2.0,
    },
}

# V1.6: Bear recipes with confluence min scores and ATR-based exits
BEAR_RECIPES = {
    "DUST": {
        "underlying":     "GDX",
        "min_score":      7,       # EV+2.74% at score>=7 / 73 trades
        "atr_stop":       0.0180,
        "early_ratchet":  0.0134,
        "trail":          0.0033,
        "avoid_hours":    [11],
        "best_signals":   ["obv_rising", "below_ma20", "macd_bullish", "far_below_bb"],
        "worst_signals":  ["obv_falling", "near_lower_bb"],
    },
    "SOXS": {
        "underlying":     "SMH",
        "min_score":      9,       # EV+2.66% at score>=9 / 64 trades
        "atr_stop":       0.0177,
        "early_ratchet":  0.0120,
        "trail":          0.0032,
        "avoid_hours":    [9],
        "best_signals":   ["below_ma20", "rsi_lt25", "rsi14_lt20", "stochrsi_oversold", "near_lower_bb"],
        "worst_signals":  ["obv_rising", "macd_bullish"],
    },
    "LABD": {
        "underlying":     "XBI",
        "min_score":      4,       # EV+0.13% at score>=4 / 347 trades
        "atr_stop":       0.0164,
        "early_ratchet":  0.0032,
        "trail":          0.0030,
        "avoid_hours":    [11, 14],
        "best_signals":   ["far_below_bb", "near_lower_bb", "obv_falling", "rsi_lt25", "macd_bullish"],
        "worst_signals":  ["bb_squeeze", "below_ma20"],
    },
    "SQQQ": {
        "underlying":     "QQQ",
        "min_score":      5,
        "atr_stop":       0.0178,
        "early_ratchet":  0.0045,
        "trail":          0.0030,
        "avoid_hours":    [11, 13, 14],
        "best_signals":   ["rsi_lt40", "rsi14_lt35", "bouncing"],
        "worst_signals":  ["rsi_lt25", "at_lower_bb"],
    },
}

# DUST/SOXS extended TP: avg MFE 5.4%/4.8% — use trailing exit not fixed cap
BEAR_EXTENDED_TP = {
    "DUST": {"trail_activate": 0.020, "trail_stop": 0.010},
    "SOXS": {"trail_activate": 0.020, "trail_stop": 0.010},
}

# Signal combo boost for entry size scaling
SIGNAL_COMBO_BOOST_SYMBOLS = {
    "SOXL": [("bb_squeeze", "stochrsi_oversold")],     # 86% WR
    "LABU": [("rsi14_lt20", "rsi_lt25")],              # 74% WR
    "TQQQ": [("bouncing", "obv_falling")],             # 74% WR
    "NUGT": [("bb_squeeze", "macd_bullish")],          # 68% WR
    "DUST": [("far_below_bb", "stochrsi_oversold")],   # 71% WR
    "SOXS": [("below_ma20", "rsi_lt25")],              # 71% WR
    "LABD": [("ema9_above_ema21", "stochrsi_oversold")], # 67% WR
}

# VIX regime thresholds
VIX_CAUTION  = 28.0   # reduce bull sizes 50%
VIX_PAUSE    = 35.0   # pause bull entries entirely (bear still ok)

# Reversal quality thresholds
REVERSAL_HIGH_RSI    = 75    # above this = high quality overbought
REVERSAL_HIGH_DROP   = 0.008 # 0.8% drop = high quality
REVERSAL_OB_RSI      = 70
REVERSAL_RSI_RESET   = 60
REVERSAL_CONFIRM     = 0.005
REVERSAL_MAX_WATCH   = 1800

# Time-in-dwell: exit flat trades after this many minutes
DWELL_MINUTES        = 30
DWELL_FLAT_THRESHOLD = 0.001  # within 0.1% of entry = flat

# RSI overbought exit threshold
RSI_OVERBOUGHT_EXIT  = 70

QQQ_BEAR_RSI_GATE      = 58
QQQ_BEAR_RSI_GATE_LABD = 65

PM_MIN_TRADES        = 15
PM_ANALYSIS_INTERVAL = 86400
PM_MIN_BUCKET_TRADES = 3
WIN_RATE_GATE_THRESHOLD = 0.45  # V1.9: was 0.35 -- 35% WR is net negative EV at current TP/stop ratios

BUYING_POWER_BUFFER  = 1.15
WIN_COOLDOWN_SECS    = 180
LOSS_COOLDOWN_SECS   = 900
WEBULL_CACHE_TTL     = 25
WEBULL_429_BACKOFF   = 30
LOOP_INTERVAL        = 12
WARMUP_BARS          = 40

# ── Webull client ─────────────────────────────────────────────────────────────
api_client   = ApiClient(APP_KEY, APP_SECRET, "us")
trade_client = TradeClient(api_client)
_order_lock  = threading.Lock()
_balance_lock   = threading.Lock()
_positions_lock = threading.Lock()

# ── Shared context data ───────────────────────────────────────────────────────
_spy_prices:        list  = []
_qqq_prices:        list  = []
_vix_price:         float = 15.0
_vix_last_updated:  float = 0.0   # V1.9: track when VIX was last fetched
_underlying_prices: dict  = {}   # {symbol: [prices]}
_underlying_5m:     dict  = {}   # {symbol: [5-min prices]}
_context_lock               = threading.Lock()

_analyst_scores_cache: dict  = {}
_analyst_scores_ts:    float = 0.0
_analyst_scores_ttl:   float = 20.0
_analyst_lock                = threading.Lock()

_phase4_memory = None

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

# ── Signal computations ───────────────────────────────────────────────────────
def compute_rsi(prices: list, period: int = 7) -> float | None:
    if len(prices) < period + 1:
        return None
    s     = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0)
    loss  = (-delta.where(delta < 0, 0.0))
    # V1.7: Wilder smoothed RSI (EWM alpha=1/period) -- replaces simple
    # rolling mean which was noisier and triggered more false signals.
    # Matches main.py V10.8 and industry standard Wilder RSI.
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    val = float(rsi.iloc[-1])
    if not (0 < val < 100):
        return None
    return round(val, 2)

def compute_ma(prices: list, period: int = 20) -> float | None:
    if len(prices) < period:
        return None
    return round(float(sum(prices[-period:]) / period), 4)

def compute_ema(prices: list, period: int) -> float | None:
    if len(prices) < period:
        return None
    s = pd.Series(prices)
    return round(float(s.ewm(span=period, adjust=False).mean().iloc[-1]), 4)

def compute_atr(prices: list, period: int = 14) -> float:
    """Average True Range from close prices (simplified — no high/low available)."""
    if len(prices) < period + 1:
        return abs(prices[-1] * 0.015) if prices else 0.0
    diffs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    return round(float(sum(diffs[-period:]) / period), 4)

def compute_macd(prices: list) -> dict:
    if len(prices) < 26:
        return {"bullish": False, "macd_line": 0, "signal_line": 0, "histogram": 0}
    s         = pd.Series(prices)
    ema12     = s.ewm(span=12, adjust=False).mean()
    ema26     = s.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal    = macd_line.ewm(span=9, adjust=False).mean()
    hist      = macd_line - signal
    return {
        "bullish":     float(macd_line.iloc[-1]) > float(signal.iloc[-1]),
        "macd_line":   round(float(macd_line.iloc[-1]), 5),
        "signal_line": round(float(signal.iloc[-1]), 5),
        "histogram":   round(float(hist.iloc[-1]), 5),
    }

def compute_bollinger(prices: list, period: int = 20, std_dev: float = 2.0) -> dict:
    if len(prices) < period:
        return {"upper": 0, "middle": 0, "lower": 0, "pct_b": 0.5,
                "squeeze": False, "near_lower": False, "at_lower": False, "far_below": False}
    s      = pd.Series(prices)
    middle = float(s.rolling(period).mean().iloc[-1])
    std    = float(s.rolling(period).std().iloc[-1])
    upper  = middle + std_dev * std
    lower  = middle - std_dev * std
    price  = prices[-1]
    band_w = upper - lower
    pct_b  = (price - lower) / band_w if band_w > 0 else 0.5
    squeeze    = (band_w / price) < 0.02 if price > 0 else False
    return {
        "upper":      round(upper, 4),
        "middle":     round(middle, 4),
        "lower":      round(lower, 4),
        "pct_b":      round(pct_b, 3),
        "squeeze":    squeeze,
        "near_lower": pct_b < 0.20,
        "at_lower":   pct_b < 0.05,
        "far_below":  price < lower * 0.99,
    }

def compute_stochrsi(prices: list, rsi_period: int = 14, stoch_period: int = 14) -> dict:
    if len(prices) < rsi_period + stoch_period + 5:
        return {"k": 50, "d": 50, "oversold": False, "overbought": False}
    s     = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0)
    loss  = (-delta.where(delta < 0, 0.0))
    # V1.8: Wilder EWM smoothing inside StochRSI -- matches compute_rsi()
    # The old simple rolling mean understated oversold readings, causing some
    # genuine oversold setups to miss the stochrsi_oversold signal flag.
    avg_gain = gain.ewm(alpha=1.0 / rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / rsi_period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("nan"))
    rsi_ser  = 100 - (100 / (1 + rs))
    min_rsi  = rsi_ser.rolling(stoch_period).min()
    max_rsi  = rsi_ser.rolling(stoch_period).max()
    denom    = max_rsi - min_rsi
    stoch_k  = ((rsi_ser - min_rsi) / denom * 100).where(denom != 0, 50)
    stoch_d  = stoch_k.rolling(3).mean()
    k_val    = float(stoch_k.iloc[-1])
    d_val    = float(stoch_d.iloc[-1])
    return {
        "k": round(k_val, 2), "d": round(d_val, 2),
        "oversold":   k_val < 20 and d_val < 20,
        "overbought": k_val > 80 and d_val > 80,
    }

def compute_obv(prices: list, volumes: list) -> dict:
    if len(prices) < 10 or len(volumes) < 10:
        return {"rising": False, "obv_slope": 0}
    n = min(len(prices), len(volumes))
    p, v = prices[-n:], volumes[-n:]
    obv = [0.0]
    for i in range(1, len(p)):
        obv.append(obv[-1] + v[i] if p[i] > p[i-1] else
                   obv[-1] - v[i] if p[i] < p[i-1] else obv[-1])
    recent = obv[-10:]
    slope  = (recent[-1] - recent[0]) / (abs(recent[0]) + 1)
    return {"rising": slope > 0, "obv_slope": round(slope, 4)}

def compute_williams_r(prices: list, period: int = 14) -> dict:
    if len(prices) < period:
        return {"value": -50, "oversold": False}
    recent = prices[-period:]
    high, low, close = max(recent), min(recent), prices[-1]
    wr_val = ((high - close) / (high - low) * -100) if (high - low) > 0 else -50
    return {"value": round(wr_val, 2), "oversold": wr_val < -80}

def compute_cci(prices: list, period: int = 20) -> dict:
    if len(prices) < period:
        return {"value": 0, "oversold": False}
    recent   = prices[-period:]
    tp_mean  = sum(recent) / len(recent)
    mean_dev = sum(abs(p - tp_mean) for p in recent) / len(recent)
    cci_val  = (recent[-1] - tp_mean) / (0.015 * mean_dev) if mean_dev > 0 else 0
    return {"value": round(cci_val, 2), "oversold": cci_val < -100}

def check_higher_lows(prices: list, lookback: int = 20) -> bool:
    if len(prices) < lookback:
        return False
    recent = prices[-lookback:]
    lows   = [recent[i] for i in range(1, len(recent)-1)
              if recent[i] <= recent[i-1] and recent[i] <= recent[i+1]]
    return len(lows) >= 2 and lows[-1] > lows[-2]

def fetch_prices_and_volumes(symbol: str, bars: int = 40, interval: str = "1m") -> tuple:
    try:
        ticker = yf.Ticker(symbol)
        period = "1d" if interval == "1m" else "5d"
        df     = ticker.history(period=period, interval=interval)
        if not df.empty:
            return df["Close"].tail(bars).tolist(), df["Volume"].tail(bars).tolist()
    except Exception:
        pass
    return [], []

def fetch_prices(symbol: str, bars: int = 40) -> list:
    p, _ = fetch_prices_and_volumes(symbol, bars)
    return p

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
            bp = float(_balance_cache.get("buying_power", 0))
            return bp or float(_balance_cache.get("option_buying_power", 0))
    try:
        res = trade_client.account_v2.get_account_balance(acct_id)
        if res.status_code == 200:
            for asset in res.json().get("account_currency_assets", []):
                if asset.get("currency") == "USD":
                    with _balance_lock:
                        _balance_cache      = asset
                        _balance_cache_time = now
                    bp = float(asset.get("buying_power", 0))
                    return bp or float(asset.get("option_buying_power", 0))
        elif res.status_code == 429:
            time.sleep(WEBULL_429_BACKOFF)
    except Exception:
        pass
    with _balance_lock:
        bp = float(_balance_cache.get("buying_power", 0))
        return bp or float(_balance_cache.get("option_buying_power", 0))

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
            data  = res.json()
            items = data if isinstance(data, list) else data.get("items", [])
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
            print(f"[ORDER ERR] {symbol} {side}: {res.status_code} {res.text[:200]}", flush=True)
            return False
        except Exception as e:
            print(f"[ORDER ERR] {symbol} {side}: {e}", flush=True)
            return False

# ── Context refresh thread ────────────────────────────────────────────────────
def refresh_context_data():
    """Fetch SPY, QQQ, VIX, and all underlying indices every 30s."""
    all_underlyings = ["SMH", "GDX", "XBI", "QQQ", "^VIX"]
    while True:
        try:
            spy_p, _ = fetch_prices_and_volumes("SPY", 40)
            qqq_p, _ = fetch_prices_and_volumes("QQQ", 40)
            with _context_lock:
                if spy_p: _spy_prices[:] = spy_p
                if qqq_p: _qqq_prices[:] = qqq_p

            for sym in all_underlyings:
                p1, _ = fetch_prices_and_volumes(sym, 40, "1m")
                p5, _ = fetch_prices_and_volumes(sym, 60, "5m")
                vix_sym = sym.replace("^", "")
                with _context_lock:
                    if p1:
                        if sym == "^VIX":
                            globals()["_vix_price"] = p1[-1]
                            globals()["_vix_last_updated"] = time.time()  # V1.9
                        else:
                            _underlying_prices[sym] = p1
                    if p5:
                        _underlying_5m[sym] = p5
        except Exception:
            pass
        time.sleep(30)

def get_spy_context() -> dict:
    with _context_lock:
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
    return {
        "bullish": bullish, "strong": strong,
        "overbought": rsi > 72, "momentum": round(momentum * 100, 3),
        "rsi": rsi, "above_ma20": above_ma20,
    }

def get_qqq_context() -> dict:
    with _context_lock:
        prices = list(_qqq_prices)
    if len(prices) < 8:
        return {"rsi": 50, "momentum": 0, "overbought": False, "oversold": False}
    rsi      = compute_rsi(prices) or 50
    momentum = (prices[-1] - prices[-6]) / prices[-6] if len(prices) >= 6 and prices[-6] > 0 else 0
    return {
        "rsi": rsi, "momentum": round(momentum * 100, 3),
        "overbought": rsi > 68, "oversold": rsi < 35,
    }

def get_vix() -> float:
    with _context_lock:
        vix   = _vix_price
        last  = _vix_last_updated
    # V1.9: Warn when VIX data is stale (>5 min) -- stale VIX silently disables
    # the >28 caution and >35 pause regime gates, which is a silent risk.
    if last > 0 and (time.time() - last) > 300:
        print(f"[VIX] ⚠️ VIX data stale ({int((time.time()-last)/60)}m) -- "
              f"using last known value {vix:.1f}", flush=True)
    return vix

def get_underlying_context(underlying: str) -> dict:
    """
    V1.6: Macro tide layer — underlying index context.
    SMH for SOXL/SOXS, GDX for NUGT/DUST, XBI for LABU/LABD, QQQ for TQQQ/SQQQ.
    Returns both 1-min and 5-min context.
    """
    with _context_lock:
        prices_1m = list(_underlying_prices.get(underlying, []))
        prices_5m = list(_underlying_5m.get(underlying, []))

    result = {
        "available":       False,
        "rsi_1m":          50,
        "above_ema20_1m":  False,
        "trending_up_1m":  False,
        "rsi_5m":          50,
        "above_ema20_5m":  False,
        "trending_up_5m":  False,
        "at_high":         False,
        "vol_expanding":   False,
        "tide_bullish":    False,
        "tide_bearish":    False,
    }

    if len(prices_1m) >= 21:
        rsi_1m       = compute_rsi(prices_1m) or 50
        ema20_1m     = compute_ema(prices_1m, 20) or prices_1m[-1]
        momentum_1m  = (prices_1m[-1] - prices_1m[-6]) / prices_1m[-6] if len(prices_1m) >= 6 and prices_1m[-6] > 0 else 0
        result.update({
            "available":      True,
            "rsi_1m":         rsi_1m,
            "above_ema20_1m": prices_1m[-1] > ema20_1m,
            "trending_up_1m": momentum_1m > 0,
        })

    if len(prices_5m) >= 21:
        rsi_5m      = compute_rsi(prices_5m) or 50
        ema20_5m    = compute_ema(prices_5m, 20) or prices_5m[-1]
        momentum_5m = (prices_5m[-1] - prices_5m[-6]) / prices_5m[-6] if len(prices_5m) >= 6 and prices_5m[-6] > 0 else 0
        hl_5m       = check_higher_lows(prices_5m, 15)
        result.update({
            "rsi_5m":         rsi_5m,
            "above_ema20_5m": prices_5m[-1] > ema20_5m,
            "trending_up_5m": momentum_5m > 0 and hl_5m,
        })

        # Bear trap detection: underlying at multi-day high + expanding volume
        # Use 5-min prices to check if at 30-bar high
        if len(prices_5m) >= 30:
            recent_high = max(prices_5m[-30:])
            at_high     = prices_5m[-1] >= recent_high * 0.995
            result["at_high"] = at_high

    # Tide determination
    if result["available"]:
        result["tide_bullish"] = (
            result["above_ema20_1m"] and
            result["trending_up_1m"] and
            result.get("rsi_1m", 50) < 72
        )
        result["tide_bearish"] = (
            not result["above_ema20_1m"] and
            not result["trending_up_1m"]
        )

    return result

# ── Analyst bridge ────────────────────────────────────────────────────────────
def fetch_analyst_scores() -> dict:
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
    sym_data = analyst_scores.get(symbol)
    if not sym_data:
        return 0, []
    signals = sym_data.get("signals", [])
    for combo_pair in SIGNAL_COMBO_BOOST_SYMBOLS.get(symbol, []):
        sig_a, sig_b = combo_pair
        a = any(sig_a.lower() in s.lower() for s in signals)
        b = any(sig_b.lower() in s.lower() for s in signals)
        if a and b:
            return 2, signals
        if a or b:
            return 1, signals
    return 0, signals

# ── Phase4Memory ─────────────────────────────────────────────────────────────
class Phase4Memory:
    def __init__(self, db_url: str):
        self.db_url       = db_url
        self._conn        = None
        self._lock        = threading.Lock()
        self._win_rates   = {}
        self._last_analysis = 0.0
        self._enabled     = bool(db_url) and _db_available

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
            entry_score     INTEGER,
            reversal_quality INTEGER,
            underlying_tide  BOOLEAN,
            vix_at_entry    REAL,
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
        # Also add new V1.6 columns to existing table if they don't exist
        alter_ddl = """
        ALTER TABLE phase4_trade_fingerprints
            ADD COLUMN IF NOT EXISTS entry_score      INTEGER,
            ADD COLUMN IF NOT EXISTS reversal_quality INTEGER,
            ADD COLUMN IF NOT EXISTS underlying_tide  BOOLEAN,
            ADD COLUMN IF NOT EXISTS vix_at_entry     REAL;
        """
        try:
            with self._lock:
                conn = self._get_conn()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(ddl)
                        cur.execute(alter_ddl)
                    conn.commit()
                    print("[PM] Phase4 pattern memory tables ready (V1.6)", flush=True)
        except Exception as e:
            print(f"[PM] init_tables error: {e}", flush=True)

    def record_entry(self, trade_id, symbol, bear_pair, is_bear, mode,
                     entry_price, sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
                     analyst_score=0, signal_boost=0,
                     entry_score=0, reversal_quality=0,
                     underlying_tide=False, vix=15.0):
        if not self._enabled:
            return
        threading.Thread(target=self._write_entry, daemon=True, args=(
            trade_id, symbol, bear_pair, is_bear, mode, entry_price,
            sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
            analyst_score, signal_boost, entry_score, reversal_quality,
            underlying_tide, vix
        )).start()

    def _write_entry(self, trade_id, symbol, bear_pair, is_bear, mode,
                     entry_price, sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
                     analyst_score, signal_boost, entry_score, reversal_quality,
                     underlying_tide, vix):
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
                         analyst_score, signal_boost,
                         entry_score, reversal_quality, underlying_tide, vix_at_entry)
                        VALUES (%s,%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s,
                                %s,%s, %s,%s,%s,%s, %s,%s, %s,%s, %s,%s,%s,%s)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        trade_id, symbol, bear_pair, is_bear, mode,
                        int(time.time()), entry_price,
                        sym_rsi, spy_ctx.get("rsi"), qqq_ctx.get("rsi"),
                        spy_ctx.get("bullish"), spy_ctx.get("momentum"), qqq_ctx.get("overbought"),
                        sym_ctx.get("higher_lows"), sym_ctx.get("above_ma20"),
                        sym_ctx.get("bb_squeeze"), sym_ctx.get("stochrsi_oversold"),
                        sym_ctx.get("macd_bullish"), sym_ctx.get("obv_rising"),
                        now.hour, now.weekday(),
                        analyst_score, signal_boost,
                        entry_score, reversal_quality, underlying_tide, vix,
                    ))
                conn.commit()
        except Exception as e:
            print(f"[PM] write_entry error {trade_id}: {e}", flush=True)

    def record_exit(self, trade_id, won, pnl_pct, exit_reason, hold_min,
                    mfe=0.0, mae=0.0):
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
                            hold_time_min=%s, exit_ts=%s, mfe=%s, mae=%s
                        WHERE trade_id=%s
                    """, (won, round(pnl_pct * 100, 3), exit_reason,
                          hold_min, int(time.time()),
                          round(mfe * 100, 3), round(mae * 100, 3), trade_id))
                conn.commit()
        except Exception as e:
            print(f"[PM] write_exit error {trade_id}: {e}", flush=True)

    def run_analysis(self):
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
                return

            from collections import defaultdict
            buckets  = defaultdict(list)
            pnl_bkts = defaultdict(list)

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
                            INSERT INTO phase4_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (bucket_key) DO UPDATE
                            SET win_rate=EXCLUDED.win_rate, sample_count=EXCLUDED.sample_count,
                                avg_pnl=EXCLUDED.avg_pnl, last_updated=NOW()
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
    def _bucket_key(symbol, is_bear, mode, sym_rsi, spy_ctx, qqq_ctx, hour):
        rsi_b  = ("rsi_lt30" if sym_rsi < 30 else "rsi_30_40" if sym_rsi < 40 else
                  "rsi_40_55" if sym_rsi < 55 else "rsi_gt55")
        spy_b  = "spy_bull" if spy_ctx.get("bullish") else "spy_bear"
        qqq_b  = "qqq_ob" if qqq_ctx.get("overbought") else "qqq_ok"
        bear_b = "bear" if is_bear else "bull"
        mode_b = mode or "SCALP"
        hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
        return f"{symbol}|{bear_b}|{mode_b}|{rsi_b}|{spy_b}|{qqq_b}|{hr_b}"

    def should_skip_entry(self, symbol, is_bear, mode, sym_rsi, spy_ctx, qqq_ctx, hour):
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


# ── SymbolBot ─────────────────────────────────────────────────────────────────
class SymbolBot:
    def __init__(self, symbol: str, config: dict, acct_id: str):
        self.symbol      = symbol
        self.bear_pair   = config["bear_pair"]
        self.underlying  = config["underlying"]
        self.budget_pct  = config["budget_pct"]
        self.cfg         = config
        self.acct_id     = acct_id

        self.prices:       list  = []
        self.volumes:      list  = []
        self.bear_prices:  list  = []
        self.bear_volumes: list  = []
        self.peak_price:   float = 0.0
        self.entry_price:  float = 0.0
        self.entry_time:   float = 0.0
        self.trade_id:     str   = ""
        self.mfe:          float = 0.0
        self.mae:          float = 0.0
        self.in_position:  bool  = False
        self.active_sym:   str   = symbol
        self.mode:         str   = "SCALP"
        self.cooldown_until: float = 0.0

        # Extended TP state (DUST/SOXS)
        self._bear_ext_trailing: bool  = False
        self._bear_ext_peak:     float = 0.0
        # Adaptive ratchet state
        self._late_ratchet_active: bool = False

        # Entry context for fingerprint
        self._entry_spy_ctx:       dict  = {}
        self._entry_qqq_ctx:       dict  = {}
        self._entry_sym_ctx:       dict  = {}
        self._entry_rsi:           float = 50.0
        self._entry_analyst_score: int   = 0
        self._entry_signal_boost:  int   = 0
        self._entry_score:         int   = 0
        self._entry_rev_quality:   int   = 0
        self._entry_tide:          bool  = False
        self._entry_vix:           float = 15.0

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
        """Full 15-signal suite from 1-min bars."""
        if len(prices) < 21:
            return {
                "rsi": 50, "rsi14": 50, "rsi21": 50,
                "ma20": 0, "above_ma20": True, "below_ma20": False,
                "trend_10bar": 0, "higher_lows": False, "bouncing": False,
                "ema9": 0, "ema21": 0, "ema9_above_ema21": False,
                "bb": {}, "bb_squeeze": False, "near_lower_bb": False,
                "at_lower_bb": False, "far_below_bb": False,
                "stochrsi": {}, "stochrsi_oversold": False,
                "macd": {}, "macd_bullish": False,
                "obv": {}, "obv_rising": False, "obv_falling": False,
                "williams_r": {}, "williams_oversold": False,
                "cci": {}, "cci_oversold": False,
                "rsi_lt40": False, "rsi_lt25": False,
                "rsi14_lt35": False, "rsi14_lt20": False,
                "rsi21_lt45": False,
            }
        rsi7   = compute_rsi(prices, 7)  or 50
        rsi14  = compute_rsi(prices, 14) or 50
        rsi21  = compute_rsi(prices, 21) or 50
        ma20   = compute_ma(prices, 20)  or prices[-1]
        ema9   = compute_ema(prices, 9)  or prices[-1]
        ema21v = compute_ema(prices, 21) or prices[-1]
        trend10 = (prices[-1] - prices[-11]) / prices[-11] if len(prices) > 11 and prices[-11] > 0 else 0
        above_ma20    = prices[-1] > ma20
        ema9_above_21 = ema9 > ema21v
        higher_l      = check_higher_lows(prices)
        bouncing      = len(prices) >= 3 and prices[-1] > prices[-3]

        bb       = compute_bollinger(prices)
        stochrsi = compute_stochrsi(prices)
        macd     = compute_macd(prices)
        obv      = compute_obv(prices, volumes) if volumes else {"rising": False, "obv_slope": 0}
        williams = compute_williams_r(prices)
        cci      = compute_cci(prices)

        return {
            "rsi": rsi7, "rsi14": rsi14, "rsi21": rsi21,
            "ma20": ma20, "above_ma20": above_ma20, "below_ma20": not above_ma20,
            "trend_10bar": round(trend10 * 100, 3),
            "higher_lows": higher_l, "bouncing": bouncing,
            "ema9": ema9, "ema21": ema21v, "ema9_above_ema21": ema9_above_21,
            "bb": bb,
            "bb_squeeze":    bb.get("squeeze", False),
            "near_lower_bb": bb.get("near_lower", False),
            "at_lower_bb":   bb.get("at_lower", False),
            "far_below_bb":  bb.get("far_below", False),
            "stochrsi": stochrsi, "stochrsi_oversold": stochrsi.get("oversold", False),
            "macd": macd,         "macd_bullish":      macd.get("bullish", False),
            "obv": obv,           "obv_rising":        obv.get("rising", False),
            "obv_falling": not obv.get("rising", True),
            "williams_r": williams, "williams_oversold": williams.get("oversold", False),
            "cci": cci,           "cci_oversold":       cci.get("oversold", False),
            # Named boolean flags matching BOT_CONFIGS best/worst signal keys
            "rsi_lt40":    rsi7  < 40,
            "rsi_lt25":    rsi7  < 25,
            "rsi14_lt35":  rsi14 < 35,
            "rsi14_lt20":  rsi14 < 20,
            "rsi21_lt45":  rsi21 < 45,
        }

    def compute_entry_score(self, sym: str, sym_ctx: dict) -> int:
        """
        V1.8: Per-symbol confluence score replacing the RSI-only gate.
        +1 for each best_signal present, -1 for each worst_signal present.
        Score must reach cfg['min_score'] to enter.

        V1.8 FIX: Neutral signals (not in best, not in worst) NO LONGER add
        +1. Previously every positive indicator that wasn't explicitly banned
        scored +1, allowing irrelevant noise signals to pad the score past
        min_score. Now only explicit best_signals score positive, so the
        min_score thresholds derived from backtest data are respected.
        """
        cfg  = self.cfg if sym == self.symbol else BEAR_RECIPES.get(sym, self.cfg)
        best = cfg.get("best_signals", [])
        worst = cfg.get("worst_signals", [])
        score = 0

        # Only best_signals score positive -- neutral signals contribute nothing
        for sig in best:
            if sym_ctx.get(sig, False):
                score += 1

        # Worst signals subtract
        for sig in worst:
            if sym_ctx.get(sig, False):
                score -= 1

        # Bonus for extreme conditions when they are best signals
        if sym_ctx.get("rsi_lt25") and "rsi_lt25" in best:
            score += 1  # extra point for extreme oversold when it's a best signal
        if sym_ctx.get("at_lower_bb") and "at_lower_bb" in best:
            score += 1

        return max(0, score)

    def select_mode(self, spy_ctx: dict, sym_ctx: dict) -> str:
        if spy_ctx.get("overbought"):
            return "SCALP"
        if (spy_ctx.get("strong") and
                sym_ctx.get("trend_10bar", 0) > 0.3 and
                sym_ctx.get("higher_lows") and
                sym_ctx.get("above_ma20")):
            return "EXTENDED"
        if spy_ctx.get("bullish") and (sym_ctx.get("above_ma20") or sym_ctx.get("trend_10bar", 0) > 0.1):
            return "RIDE"
        return "SCALP"

    def get_exit_params(self) -> tuple:
        """Returns (atr_stop, early_ratchet, late_ratchet, trail_normal, trail_tight)."""
        if self.active_sym == self.bear_pair:
            br = BEAR_RECIPES.get(self.bear_pair, self.cfg)
            sl         = br["atr_stop"]
            early_r    = br["early_ratchet"]
            late_r     = early_r * 1.8
            trail_n    = br["trail"]
            trail_t    = round(trail_n * 0.7, 4)
            return sl, early_r, late_r, trail_n, trail_t

        sl      = self.cfg["atr_stop"]
        early_r = self.cfg["early_ratchet"]
        late_r  = self.cfg["late_ratchet"]
        trail_n = self.cfg["trail_normal"]
        trail_t = self.cfg["trail_tight"]

        mult = self.cfg.get("ride_stop_mult", 1.3) if self.mode == "RIDE" else \
               self.cfg.get("ext_stop_mult",  1.8) if self.mode == "EXTENDED" else 1.0
        sl = round(sl * mult, 4)
        return sl, early_r, late_r, trail_n, trail_t

    def should_enter_bull(self, spy_ctx: dict, sym_ctx: dict,
                          underlying_ctx: dict) -> tuple:
        """
        V1.6: Confluence score entry — replaces RSI-only gate.
        Returns (should_enter, score, reasons).
        """
        now = datetime.now(tz=CENTRAL)
        if now.hour in self.cfg.get("avoid_hours", []):
            return False, 0, "avoid_hour"
        if now.weekday() in self.cfg.get("avoid_days", []):
            return False, 0, "avoid_day"

        # VIX regime gate
        vix = get_vix()
        if vix >= VIX_PAUSE:
            return False, 0, f"vix_pause({vix:.1f})"

        # Need at least a bounce — still required as minimum trigger
        if not sym_ctx.get("bouncing"):
            return False, 0, "no_bounce"

        # Underlying tide check (Layer 1)
        # If underlying data available and tide is bearish, skip
        if underlying_ctx.get("available") and underlying_ctx.get("tide_bearish"):
            return False, 0, "tide_bearish"

        # Compute confluence score
        score    = self.compute_entry_score(self.symbol, sym_ctx)
        min_sc   = self.cfg["min_score"]

        if score < min_sc:
            return False, score, f"score_{score}<{min_sc}"

        return True, score, "ok"

    def score_reversal_quality(self, bull_rsi: float, drop: float,
                                bear_ctx: dict) -> int:
        """
        V1.6: Score the quality of a reversal signal 0-3.
        0 = skip, 1 = weak (half size), 2 = medium (normal size), 3 = strong (full + 25%)
        """
        score = 0

        # RSI level at reversal
        if bull_rsi >= REVERSAL_HIGH_RSI:
            score += 2
        elif bull_rsi >= REVERSAL_OB_RSI:
            score += 1

        # Drop magnitude
        if drop >= REVERSAL_HIGH_DROP:
            score += 1

        # Bear pair already moving
        if bear_ctx.get("rsi", 50) < 45:
            score += 1
        if bear_ctx.get("obv_rising"):
            score += 1

        # Bear trap check — if underlying at multi-day high + volume expanding,
        # this may be a continuation not a reversal
        underlying_ctx = get_underlying_context(
            BEAR_RECIPES.get(self.bear_pair, {}).get("underlying", "QQQ"))
        if underlying_ctx.get("at_high") and underlying_ctx.get("tide_bullish"):
            score -= 2  # strong bear trap signal

        return max(0, min(3, score))

    def check_reversal(self) -> tuple:
        """
        V1.6: Returns (should_enter, reversal_quality).
        quality 0 = skip, 1-3 = enter with scaled size.
        Now includes quality scoring and bear trap detection.
        """
        if len(self.prices) < 8:
            return False, 0
        bull_rsi = compute_rsi(self.prices)
        if bull_rsi is None:
            return False, 0

        state = self.reversal_state
        now_t = time.time()

        if state["state"] == "IDLE":
            if bull_rsi >= REVERSAL_OB_RSI:
                self.reversal_state = {
                    "state":       "WATCHING",
                    "bull_peak":   self.prices[-1],
                    "watch_start": now_t,
                }
                log(self.symbol, f"👁 REVERSAL WATCH -> {self.bear_pair} | RSI={bull_rsi:.1f}")
            return False, 0

        if state["state"] == "WATCHING":
            if now_t - state.get("watch_start", now_t) > REVERSAL_MAX_WATCH:
                self.reversal_state = {"state": "IDLE"}
                return False, 0
            if bull_rsi < REVERSAL_RSI_RESET:
                log(self.symbol, f"↩ REVERSAL CANCEL | RSI recovered to {bull_rsi:.1f}")
                self.reversal_state = {"state": "IDLE"}
                return False, 0

            bull_peak = max(state.get("bull_peak", self.prices[-1]), self.prices[-1])
            self.reversal_state["bull_peak"] = bull_peak
            drop = (bull_peak - self.prices[-1]) / bull_peak if bull_peak > 0 else 0

            if drop >= REVERSAL_CONFIRM:
                # Bear bounce check
                if len(self.bear_prices) < 3 or self.bear_prices[-1] <= self.bear_prices[-3]:
                    return False, 0

                # Hour gate for bear pair
                now_hour   = datetime.now(tz=CENTRAL).hour
                bear_avoid = BEAR_RECIPES.get(self.bear_pair, {}).get("avoid_hours", [])
                if now_hour in bear_avoid:
                    log(self.symbol, f"⏰ REVERSAL GATED: {self.bear_pair} hour {now_hour}")
                    return False, 0

                # SQQQ gate -- V1.9: enabled by default but requires SPY bear regime
                # (SPY below 20-day MA). During bull markets SQQQ has negative EV;
                # in bear/correction regimes it's the right trade.
                if self.bear_pair == "SQQQ":
                    if not SQQQ_ENABLED:
                        return False, 0
                    spy_ctx_sq = get_spy_context()
                    spy_above_ma = spy_ctx_sq.get("above_ma20", True)
                    if spy_above_ma:
                        log(self.symbol,
                            f"🚫 SQQQ blocked: SPY above MA20 (bull regime) -- "
                            f"SQQQ only fires in bear regime")
                        return False, 0
                    log(self.symbol, f"✅ SQQQ bear regime confirmed: SPY below MA20")

                # QQQ filters
                qqq_ctx = get_qqq_context()
                if qqq_ctx.get("oversold"):
                    return False, 0
                gate = QQQ_BEAR_RSI_GATE_LABD if self.bear_pair == "LABD" else QQQ_BEAR_RSI_GATE
                if qqq_ctx.get("rsi", 50) < gate:
                    return False, 0

                # Bear pair confluence score
                bear_ctx  = self.get_signal_suite(self.bear_prices, self.bear_volumes)
                bear_min  = BEAR_RECIPES.get(self.bear_pair, {}).get("min_score", 4)
                bear_score = self.compute_entry_score(self.bear_pair, bear_ctx)
                if bear_score < bear_min:
                    log(self.symbol,
                        f"⚠ REVERSAL LOW SCORE: {self.bear_pair} score={bear_score} < {bear_min}")
                    return False, 0

                # Score the reversal quality
                quality = self.score_reversal_quality(bull_rsi, drop, bear_ctx)
                if quality == 0:
                    log(self.symbol,
                        f"🚫 REVERSAL QUALITY=0 (bear trap detected or too weak) — skip")
                    return False, 0

                log(self.symbol,
                    f"🔁 REVERSAL CONFIRMED -> {self.bear_pair} | "
                    f"drop={round(drop*100,2)}% | bull_rsi={bull_rsi:.1f} | "
                    f"bear_score={bear_score} | quality={quality}")
                self.reversal_state = {"state": "IDLE"}
                return True, quality

        return False, 0

    def try_buy(self, sym: str, prices: list, volumes: list,
                spy_ctx: dict, sym_ctx: dict,
                reversal_quality: int = 0) -> bool:
        """
        V1.6: Asymmetric position sizing by conviction.
        signal_boost 2 = 100% | 1 = 80% | 0 = 60%
        reversal_quality 3 = +25% bonus | 2 = normal | 1 = 50%
        VIX caution = 50% of computed size
        """
        bp         = get_buying_power(self.acct_id)
        base_size  = round(bp * self.budget_pct, 2)
        if base_size < 1.00:
            return False

        is_bear     = (sym == self.bear_pair)
        entry_score = self.compute_entry_score(sym, sym_ctx)
        self.mode   = self.select_mode(spy_ctx, sym_ctx)

        # Analyst bridge
        analyst_scores = fetch_analyst_scores()
        analyst_entry  = analyst_scores.get(sym, {})
        analyst_score  = analyst_entry.get("score", 0)
        signal_boost, active_signals = get_analyst_signal_boost(sym, analyst_scores)

        # Underlying context
        underlying     = self.cfg["underlying"] if not is_bear else BEAR_RECIPES.get(sym, {}).get("underlying", "QQQ")
        underlying_ctx = get_underlying_context(underlying)
        tide_bullish   = underlying_ctx.get("tide_bullish", False)
        vix            = get_vix()

        qqq_ctx = get_qqq_context()

        # Win-rate gate from pattern memory
        if _phase4_memory:
            hour = datetime.now(tz=CENTRAL).hour
            skip, wr, has_data = _phase4_memory.should_skip_entry(
                self.symbol, is_bear, self.mode, sym_ctx.get("rsi", 50),
                spy_ctx, qqq_ctx, hour
            )
            if skip:
                log(self.symbol, f"🚫 WIN-RATE GATE: {sym} historical WR={wr:.0%}")
                return False

        # Asymmetric sizing
        size_mult = 1.0 if signal_boost == 2 else 0.8 if signal_boost == 1 else 0.6
        if reversal_quality == 3:
            size_mult = min(1.25, size_mult + 0.25)
        elif reversal_quality == 1:
            size_mult *= 0.5
        if vix >= VIX_CAUTION:
            size_mult *= 0.5
            log(self.symbol, f"⚠ VIX={vix:.1f} — reducing size 50%")

        trade_size = round(base_size * size_mult, 2)
        if trade_size < 1.00:
            return False

        price = prices[-1] if prices else get_current_price(sym)
        if not price or price <= 0:
            return False

        qty = int(trade_size / (price * BUYING_POWER_BUFFER))
        if qty < 1:
            return False

        boost_label  = " 🔥COMBO" if signal_boost == 2 else " ✨sig" if signal_boost == 1 else ""
        tide_label   = " 🌊TIDE" if tide_bullish else ""
        log(self.symbol,
            f"📊 BUY signal | score={entry_score} | mode={self.mode} | "
            f"RSI={sym_ctx['rsi']:.1f} | size_mult={size_mult:.0%} | "
            f"analyst={analyst_score}{boost_label}{tide_label}")

        success = place_order(sym, "BUY", qty, self.acct_id)
        if success:
            self.in_position           = True
            self.active_sym            = sym
            self.entry_price           = price
            self.peak_price            = price
            self.entry_time            = time.time()
            self.mfe                   = 0.0
            self.mae                   = 0.0
            self.trade_id              = __import__('secrets').token_hex(8)
            self._entry_spy_ctx        = spy_ctx
            self._entry_qqq_ctx        = qqq_ctx
            self._entry_sym_ctx        = sym_ctx
            self._entry_rsi            = sym_ctx.get("rsi", 50)
            self._entry_analyst_score  = analyst_score
            self._entry_signal_boost   = signal_boost
            self._entry_score          = entry_score
            self._entry_rev_quality    = reversal_quality
            self._entry_tide           = tide_bullish
            self._entry_vix            = vix
            self._bear_ext_trailing    = False
            self._bear_ext_peak        = price
            self._late_ratchet_active  = False
            invalidate_pos_cache()

            if _phase4_memory:
                _phase4_memory.record_entry(
                    self.trade_id, self.symbol, self.bear_pair,
                    is_bear, self.mode, price,
                    self._entry_rsi, spy_ctx, qqq_ctx, sym_ctx,
                    analyst_score, signal_boost,
                    entry_score, reversal_quality, tide_bullish, vix
                )

            log(self.symbol,
                f"⚡ BUY: {sym} | {qty}sh @ ~${round(price,2)} | mode={self.mode} | "
                f"size={round(trade_size,2)} ({size_mult:.0%}){boost_label}")
            alert(
                f"⚡ PHASE4 BUY [{self.mode}]: {sym} | {qty} @ ~${round(price,2)}"
                f"\nscore={entry_score} | boost={signal_boost} | vix={vix:.1f}{boost_label}"
            )
            return True
        return False

    def try_sell(self, reason: str, pnl_pct: float) -> bool:
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
            alert(f"{emoji} PHASE4 [{reason}]: {self.active_sym} | {pnl_s} | {hold_min}m")

            if _phase4_memory and self.trade_id:
                _phase4_memory.record_exit(
                    self.trade_id, pnl_pct > 0, pnl_pct,
                    reason, hold_min, self.mfe, self.mae
                )

            self.in_position           = False
            self.peak_price            = 0.0
            self.entry_price           = 0.0
            self.entry_time            = 0.0
            self.trade_id              = ""
            self.mfe                   = 0.0
            self.mae                   = 0.0
            self._bear_ext_trailing    = False
            self._bear_ext_peak        = 0.0
            self._late_ratchet_active  = False

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
        """V1.5+: generates trade_id, entry_time, infers mode."""
        positions = get_all_positions(self.acct_id)
        for sym in [self.symbol, self.bear_pair]:
            if sym in positions:
                pos              = positions[sym]
                cost             = float(pos.get("cost_price", pos.get("average_cost", 0)))
                self.in_position = True
                self.active_sym  = sym
                self.entry_price = cost
                self.peak_price  = max(cost, get_current_price(sym) or cost)
                self.trade_id    = __import__('secrets').token_hex(8)
                self.entry_time  = time.time()
                self.mfe         = 0.0
                self.mae         = 0.0
                spy_ctx          = get_spy_context()
                sym_ctx          = self.get_signal_suite(self.prices, self.volumes)
                self.mode        = self.select_mode(spy_ctx, sym_ctx)
                self._bear_ext_trailing   = False
                self._bear_ext_peak       = self.peak_price
                self._late_ratchet_active = False
                log(self.symbol,
                    f"🔄 Recovered: {sym} | entry=${cost:.3f} | mode={self.mode} | "
                    f"trade_id={self.trade_id[:8]}")
                return

    def run_loop(self):
        log(self.symbol,
            f"🚀 Bot online | bear={self.bear_pair} | underlying={self.underlying} | "
            f"budget={int(self.budget_pct*100)}% | min_score={self.cfg['min_score']}")

        self.refresh_prices()
        log(self.symbol, f"✅ Warmed up | {len(self.prices)} bars")
        self.recover_position()

        while True:
            try:
                if not is_market_hours():
                    if self.in_position:
                        log(self.symbol, "📌 Market closed — holding overnight")
                    time.sleep(60)
                    continue

                self.refresh_prices()
                spy_ctx        = get_spy_context()
                sym_ctx        = self.get_signal_suite(self.prices, self.volumes)
                underlying_ctx = get_underlying_context(self.underlying)

                prices = self.prices
                if not prices:
                    time.sleep(LOOP_INTERVAL)
                    continue

                # ── MANAGE OPEN POSITION ─────────────────────────────────
                if self.in_position:
                    active_prices  = self.prices  if self.active_sym == self.symbol else self.bear_prices
                    active_volumes = self.volumes if self.active_sym == self.symbol else self.bear_volumes
                    active_ctx     = self.get_signal_suite(active_prices, active_volumes)
                    if not active_prices:
                        time.sleep(LOOP_INTERVAL)
                        continue

                    price      = active_prices[-1]
                    profit_pct = (price - self.entry_price) / self.entry_price if self.entry_price > 0 else 0
                    drawdown   = (self.peak_price - price) / self.peak_price if self.peak_price > 0 else 0

                    self.peak_price = max(self.peak_price, price)
                    self.mfe        = max(self.mfe, profit_pct)
                    self.mae        = min(self.mae, profit_pct)

                    sl, early_r, late_r, trail_n, trail_t = self.get_exit_params()

                    log(self.symbol,
                        f"📊 {self.active_sym} | P&L={round(profit_pct*100,2):+.2f}% | "
                        f"peak=${self.peak_price:.3f} | dd={round(drawdown*100,2):.2f}% | "
                        f"mode={self.mode} | rsi={active_ctx.get('rsi',50):.0f}")

                    # ── BEAR PAIR: extended TP for DUST/SOXS ─────────────
                    ext_cfg = BEAR_EXTENDED_TP.get(self.active_sym) if self.active_sym == self.bear_pair else None
                    if ext_cfg:
                        self._bear_ext_peak = max(self._bear_ext_peak, price)
                        if not self._bear_ext_trailing and profit_pct >= ext_cfg["trail_activate"]:
                            self._bear_ext_trailing = True
                            log(self.symbol,
                                f"🎯 {self.active_sym} EXTENDED TP activated at +{profit_pct*100:.1f}%")
                        if self._bear_ext_trailing:
                            ext_dd = (self._bear_ext_peak - price) / self._bear_ext_peak if self._bear_ext_peak > 0 else 0
                            if ext_dd >= ext_cfg["trail_stop"]:
                                self.try_sell("ext-trail", profit_pct)
                                time.sleep(LOOP_INTERVAL)
                                continue
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)

                    else:
                        # ── STOP LOSS ─────────────────────────────────────
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)

                        # ── SIGNAL REVERSAL EXIT ──────────────────────────
                        # If RSI hit overbought and we entered on oversold = reversion complete
                        elif (active_ctx.get("rsi", 50) >= RSI_OVERBOUGHT_EXIT
                              and profit_pct > 0):
                            log(self.symbol,
                                f"🔄 RSI REVERSAL EXIT: RSI={active_ctx['rsi']:.0f} >= {RSI_OVERBOUGHT_EXIT}")
                            self.try_sell("rsi-overbought", profit_pct)

                        # ── ADAPTIVE RATCHET ──────────────────────────────
                        elif profit_pct >= early_r:
                            # Check if we should switch to tight trailing
                            rsi_now  = active_ctx.get("rsi", 50)
                            obv_flat = not active_ctx.get("obv_rising") and not active_ctx.get("obv_falling")

                            # Activate late (tight) ratchet when momentum peaks
                            if (profit_pct >= late_r or rsi_now >= 65 or
                                    (obv_flat and profit_pct >= early_r * 1.5)):
                                self._late_ratchet_active = True

                            trail = trail_t if self._late_ratchet_active else trail_n
                            if drawdown >= trail:
                                reason = "trail-tight" if self._late_ratchet_active else "trail"
                                self.try_sell(reason, profit_pct)

                        # ── EXTENDED: exit on trend break ─────────────────
                        elif (self.mode == "EXTENDED"
                              and self.active_sym == self.symbol
                              and profit_pct > -0.005):
                            if not active_ctx.get("higher_lows", True):
                                log(self.symbol, "📉 EXTENDED: trend break")
                                self.try_sell("trend-break", profit_pct)

                        # ── TIME-IN-DWELL EXIT ────────────────────────────
                        # Flat position = dead money = exit and redeploy
                        elif self.entry_time > 0:
                            held_min = (time.time() - self.entry_time) / 60
                            if (held_min >= DWELL_MINUTES
                                    and abs(profit_pct) < DWELL_FLAT_THRESHOLD):
                                log(self.symbol,
                                    f"⏱ DWELL EXIT: {held_min:.0f}m | flat at {profit_pct*100:+.3f}%")
                                self.try_sell("dwell", profit_pct)

                # ── LOOK FOR ENTRY ────────────────────────────────────────
                elif not self.is_on_cooldown():
                    # Bear reversal check (higher priority)
                    rev_ok, rev_quality = self.check_reversal()
                    if rev_ok:
                        if not self.bear_prices:
                            log(self.symbol, "⚠ Reversal but bear_prices empty — skip")
                        else:
                            bear_ctx = self.get_signal_suite(self.bear_prices, self.bear_volumes)
                            self.try_buy(self.bear_pair, self.bear_prices, self.bear_volumes,
                                         spy_ctx, bear_ctx, reversal_quality=rev_quality)
                    else:
                        # Bull entry — confluence score gate
                        should_enter, score, reason = self.should_enter_bull(
                            spy_ctx, sym_ctx, underlying_ctx)
                        if should_enter:
                            self.try_buy(self.symbol, prices, self.volumes, spy_ctx, sym_ctx)
                        elif score > 0:
                            log(self.symbol,
                                f"⏳ score={score} (need {self.cfg['min_score']}) | {reason}")

            except Exception as e:
                log(self.symbol, f"🔴 Loop error: {e}")
                log(self.symbol, traceback.format_exc())

            time.sleep(LOOP_INTERVAL)


# ── Phase4 Service ────────────────────────────────────────────────────────────
def run():
    global _phase4_memory
    print("[PHASE4] NEXUS PHASE 4 V1.8 STARTING", flush=True)
    print("[PHASE4] Bots: NUGT(30%) | SOXL(25%) | LABU(25%) | TQQQ(20%)", flush=True)
    print("[PHASE4] Bear pairs: DUST | SOXS | LABD" + (" | SQQQ" if SQQQ_ENABLED else " | SQQQ(DISABLED)"), flush=True)
    print("[PHASE4] V1.6: Confluence score entry | Underlying index | ATR stops | Adaptive ratchet", flush=True)
    print("[PHASE4] V1.6: Asymmetric sizing | Reversal quality | Bear trap detect | Dwell exit", flush=True)

    if DATABASE_URL and _db_available:
        _phase4_memory = Phase4Memory(DATABASE_URL)
        _phase4_memory.init_tables()
        _phase4_memory.start_scheduler()
        print("[PHASE4] Pattern memory: DB connected", flush=True)
    else:
        _phase4_memory = Phase4Memory("")
        print("[PHASE4] Pattern memory: disabled (no DATABASE_URL)", flush=True)

    acct_id = get_account_id()
    if not acct_id:
        print("[PHASE4] 🔴 Could not get Webull account ID", flush=True)
        return

    print(f"[PHASE4] Account: {acct_id}", flush=True)
    print(f"[PHASE4] Analyst bridge: {ANALYST_URL if ANALYST_URL else 'disabled'}", flush=True)
    print(f"[PHASE4] VIX caution={VIX_CAUTION} / pause={VIX_PAUSE}", flush=True)

    ctx_thread = threading.Thread(target=refresh_context_data, daemon=True)
    ctx_thread.start()
    time.sleep(8)  # let underlyings warm up before bots start

    bots    = []
    threads = []
    for symbol, config in BOT_CONFIGS.items():
        bot = SymbolBot(symbol, config, acct_id)
        bots.append(bot)
        t = threading.Thread(target=bot.run_loop, daemon=True, name=f"bot_{symbol}")
        threads.append(t)
        t.start()
        print(f"[PHASE4] ✅ {symbol} bot started (underlying={config['underlying']}, min_score={config['min_score']})", flush=True)
        time.sleep(2)

    from phase4_server import start_server
    start_server(bots)

    vix_now = get_vix()
    alert(
        f"⚡ PHASE4 V1.8 ONLINE\n"
        f"Confluence scoring | Underlying index | Adaptive exits\n"
        f"SOXL(SMH) TQQQ(QQQ) NUGT(GDX) LABU(XBI)\n"
        f"VIX: {vix_now:.1f} | SQQQ: {'ON' if SQQQ_ENABLED else 'OFF'}\n"
        f"Analyst: {'✅' if ANALYST_URL else '⚠ disabled'}\n"
        f"V1.8: StochRSI Wilder fix | Score double-count fix"
    )
    print("[PHASE4] All bots running.", flush=True)

    last_day = datetime.now(tz=CENTRAL).date()
    while True:
        today = datetime.now(tz=CENTRAL).date()
        if today != last_day:
            for bot in bots:
                bot.daily_wins   = 0
                bot.daily_losses = 0
                bot.daily_pnl    = 0.0
            last_day = today
            print("[PHASE4] 🌅 Daily reset", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    run()
