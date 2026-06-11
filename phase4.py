"""
NEXUS PHASE 4 — PER-SYMBOL AUTONOMOUS BOTS V1.3
4 dedicated bots: NUGT, SOXL, LABU, TQQQ
Each reads full market context, selects a trading mode, executes independently.

Trading Modes:
  SCALP    — RSI dip + bounce. Quick target. Choppy/uncertain markets.
  RIDE     — Sustained trend entry. Looser trail. Bull market confirmed.
  EXTENDED — Multi-bar uptrend, higher lows locked in. Ride the full wave.

Bear pairs: each bot monitors bull RSI exhaustion → flips to bear ETF on reversal.
  NUGT → DUST | SOXL → SOXS | LABU → LABD | TQQQ → SQQQ

Capital allocation (by EV from nexus_analyzer 2yr + 1yr backtest):
  NUGT 30% | SOXL 25% | LABU 25% | TQQQ 20%

Positions held overnight — no forced close at market end.
"""

import os
import time
import uuid
import traceback
import threading
import requests
import pandas as pd
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
DATABASE_URL     = os.environ.get("DATABASE_URL", "")  # V1.1: pattern memory

CENTRAL  = ZoneInfo("America/Chicago")
BOT_NAME = "PHASE4"

# ── Bot configs — capital by EV ───────────────────────────────────────────────
BOT_CONFIGS = {
    "NUGT": {
        "bear_pair":   "DUST",
        "budget_pct":  0.30,   # 30% — highest EV (+0.114%)
        "recipe": {
            "stop_loss": 0.028, "profit_ratchet": 0.008, "trailing_stop": 0.003,
            "avoid_hours": [13], "avoid_days": [],
        },
        "ride_stop_mult":     1.3,   # loosen stop in RIDE mode
        "ride_trail_mult":    1.5,
        "ext_stop_mult":      1.8,   # even looser in EXTENDED
        "ext_trail_mult":     2.2,
        "ext_ratchet_pct":    0.02,  # trail only activates after +2% in EXTENDED
    },
    "SOXL": {
        "bear_pair":   "SOXS",
        "budget_pct":  0.25,   # 25% — EV +0.049%
        "recipe": {
            "stop_loss": 0.025, "profit_ratchet": 0.006, "trailing_stop": 0.002,
            "avoid_hours": [], "avoid_days": [],
        },
        "ride_stop_mult":     1.3,
        "ride_trail_mult":    1.5,
        "ext_stop_mult":      1.8,
        "ext_trail_mult":     2.2,
        "ext_ratchet_pct":    0.02,
    },
    "LABU": {
        "bear_pair":   "LABD",
        "budget_pct":  0.25,   # 25% — EV +0.042%
        "recipe": {
            "stop_loss": 0.028, "profit_ratchet": 0.009, "trailing_stop": 0.003,
            "avoid_hours": [], "avoid_days": [],
        },
        "ride_stop_mult":     1.3,
        "ride_trail_mult":    1.5,
        "ext_stop_mult":      1.8,
        "ext_trail_mult":     2.2,
        "ext_ratchet_pct":    0.025,
    },
    "TQQQ": {
        "bear_pair":   "SQQQ",
        "budget_pct":  0.20,   # 20% — EV +0.034%
        "recipe": {
            "stop_loss": 0.021, "profit_ratchet": 0.004, "trailing_stop": 0.0016,
            "avoid_hours": [], "avoid_days": [],
        },
        "ride_stop_mult":     1.3,
        "ride_trail_mult":    1.5,
        "ext_stop_mult":      2.0,
        "ext_trail_mult":     2.5,
        "ext_ratchet_pct":    0.015,
    },
}

BEAR_RECIPES = {
    "DUST": {"stop_loss": 0.026, "profit_ratchet": 0.006, "trailing_stop": 0.0025},
    "SOXS": {"stop_loss": 0.024, "profit_ratchet": 0.006, "trailing_stop": 0.002},
    "LABD": {"stop_loss": 0.025, "profit_ratchet": 0.007, "trailing_stop": 0.0024},
    "SQQQ": {"stop_loss": 0.018, "profit_ratchet": 0.0035,"trailing_stop": 0.0016},
}

RSI_PERIOD          = 7
BUYING_POWER_BUFFER = 1.15
WIN_COOLDOWN_SECS   = 180
LOSS_COOLDOWN_SECS  = 900

# V1.1: QQQ filter for bear entries
QQQ_BEAR_RSI_GATE       = 58   # QQQ RSI must be above this for bear entries
QQQ_BEAR_RSI_GATE_LABD  = 65   # LABD specifically needs stronger QQQ overbought
                                # Biotech more volatile, fake reversals common

# V1.1: Pattern memory
PM_MIN_TRADES       = 15   # min completed trades before analysis runs
PM_ANALYSIS_INTERVAL = 86400  # daily
WEBULL_CACHE_TTL         = 25    # positions cache TTL (seconds)
WEBULL_BALANCE_CACHE_TTL = 300   # V1.3: balance cache TTL — 5 min to avoid 429s
WEBULL_429_BACKOFF  = 60   # V1.3: increased backoff on rate limit
LOOP_INTERVAL       = 12   # seconds between bot iterations
WARMUP_BARS         = 30
REVERSAL_OB_RSI     = 70
REVERSAL_RSI_RESET  = 60
REVERSAL_CONFIRM    = 0.005
REVERSAL_MAX_WATCH  = 1800

# ── Webull client ─────────────────────────────────────────────────────────────
api_client   = ApiClient(APP_KEY, APP_SECRET, "us")
trade_client = TradeClient(api_client)
_order_lock  = threading.Lock()

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(symbol, msg):
    ts = datetime.now(tz=CENTRAL).strftime("%H:%M:%S")
    print(f"[{symbol} | {ts}] {msg}", flush=True)

def alert(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except:
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

def check_higher_lows(prices: list, lookback: int = 20) -> bool:
    """Returns True if recent price structure shows higher lows (uptrend)."""
    if len(prices) < lookback:
        return False
    recent = prices[-lookback:]
    lows   = [recent[i] for i in range(1, len(recent)-1)
              if recent[i] <= recent[i-1] and recent[i] <= recent[i+1]]
    return len(lows) >= 2 and lows[-1] > lows[-2]

def fetch_prices(symbol: str, bars: int = 35) -> list:
    """Fetch recent 1-min close prices via yfinance."""
    try:
        ticker = yf.Ticker(symbol)
        df     = ticker.history(period="1d", interval="1m")
        if not df.empty:
            return df["Close"].tail(bars).tolist()
    except:
        pass
    return []

def get_current_price(symbol: str) -> float | None:
    try:
        return float(yf.Ticker(symbol).fast_info.last_price)
    except:
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
    except:
        pass
    return ""

_balance_cache      = {}
_balance_cache_time = 0.0
_balance_last_good  = 0.0   # V1.2: last successfully resolved buying power value

# V1.2: All known Webull field names for buying power, in priority order.
# Webull API returns different field names across SDK versions and account types.
_BP_FIELD_CANDIDATES = [
    "buying_power",     # snake_case (what we originally assumed)
    "buyingPower",      # camelCase v1 API
    "cashBalance",      # some account types
    "cash_balance",     # snake variant
    "usableCash",       # seen in some Webull responses
    "usable_cash",
    "settledFunds",     # settled cash variant
    "settled_funds",
    "totalCash",        # fallback total
    "total_cash",
    "netLiquidation",   # last resort
]

def _extract_bp(obj: dict) -> float | None:
    """
    V1.2: Try every known Webull field name for buying power.
    Returns the first non-zero positive float found, or None if nothing found.
    Handles string values ("123.45"), None values, and zero values gracefully.
    """
    for field in _BP_FIELD_CANDIDATES:
        val = obj.get(field)
        if val is None:
            continue
        try:
            f = float(val)
            if f > 0:
                return f
        except (TypeError, ValueError):
            continue
    return None

def _resolve_balance_from_response(data) -> float | None:
    """
    V1.2: Handle all known Webull response nesting structures.
    Returns buying power float or None if unresolvable.

    Known structures:
      A) {"account_currency_assets": [{"currency": "USD", "buyingPower": 123}]}
      B) {"data": {"account_currency_assets": [...]}}
      C) [{"currency": "USD", "buyingPower": 123}]   -- list directly
      D) {"buyingPower": 123}                          -- flat dict
      E) {"currency": "USD", "buyingPower": 123}       -- single asset dict
    """
    if data is None:
        return None

    # Structure C: response is a list directly
    if isinstance(data, list):
        assets = data
    # Structure B: wrapped in "data" key
    elif isinstance(data, dict) and "data" in data:
        inner = data["data"]
        if isinstance(inner, dict):
            assets = inner.get("account_currency_assets", [inner])
        elif isinstance(inner, list):
            assets = inner
        else:
            assets = [data]
    # Structure A: account_currency_assets at top level
    elif isinstance(data, dict) and "account_currency_assets" in data:
        assets = data["account_currency_assets"]
    # Structure D/E: flat dict — treat it as a single asset
    elif isinstance(data, dict):
        assets = [data]
    else:
        return None

    if not isinstance(assets, list):
        assets = [assets]

    # Pass 1: prefer the USD-denominated asset
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        currency = asset.get("currency", "")
        if str(currency).upper() in ("USD", "US"):
            val = _extract_bp(asset)
            if val is not None:
                return val

    # Pass 2: if no currency match (field absent or different value), try all assets
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        val = _extract_bp(asset)
        if val is not None:
            return val

    return None

def get_buying_power(acct_id: str) -> float:
    """
    V1.2: Ironclad buying power fetch with multi-field, multi-structure handling.
    Falls back to last known good value rather than 0 on transient failures.
    Logs raw response on failure for diagnosis.
    """
    global _balance_cache, _balance_cache_time, _balance_last_good
    now = time.time()

    # Guard: empty acct_id will always fail
    if not acct_id:
        print("[PHASE4] ⚠️ get_buying_power: empty acct_id", flush=True)
        return _balance_last_good

    # Return cached value if fresh
    if now - _balance_cache_time < WEBULL_BALANCE_CACHE_TTL and _balance_cache:
        cached = _extract_bp(_balance_cache)
        if cached is not None:
            return cached
        # Cache exists but couldn't extract — fall through to re-fetch

    try:
        res = trade_client.account_v2.get_account_balance(acct_id)

        if res.status_code == 200:
            try:
                data = res.json()
            except Exception as e:
                print(f"[PHASE4] ⚠️ balance JSON parse error: {e}", flush=True)
                return _balance_last_good

            val = _resolve_balance_from_response(data)

            if val is not None:
                # Success — update cache and last known good
                _balance_cache      = data if isinstance(data, dict) else {"_raw": data}
                _balance_cache_time = now
                _balance_last_good  = val
                return val
            else:
                # Got 200 but couldn't find buying power in response
                # Log raw response for diagnosis (truncated)
                raw_str = str(data)[:400]
                print(f"[PHASE4] ⚠️ buying power not found in response. Raw: {raw_str}", flush=True)
                return _balance_last_good

        elif res.status_code == 429:
            print("[PHASE4] ⚠️ balance API rate limited (429) — backing off", flush=True)
            time.sleep(WEBULL_429_BACKOFF)
            return _balance_last_good

        elif res.status_code in (401, 403):
            print(f"[PHASE4] 🔴 balance API auth error ({res.status_code}) — token may be expired", flush=True)
            return _balance_last_good

        else:
            print(f"[PHASE4] ⚠️ balance API unexpected status {res.status_code}: {res.text[:200]}", flush=True)
            return _balance_last_good

    except Exception as e:
        err_str = str(e)
        if "429" in err_str or "TOO_MANY_REQUESTS" in err_str:
            print(f"[PHASE4] ⚠️ balance API rate limited (429 exception) — using last good value ${_balance_last_good:.2f}", flush=True)
        elif "401" in err_str or "403" in err_str or "token" in err_str.lower():
            print(f"[PHASE4] 🔴 balance API auth exception — token may be expired", flush=True)
        else:
            print(f"[PHASE4] ⚠️ get_buying_power exception: {e}", flush=True)
        return _balance_last_good

_positions_cache      = {}
_positions_cache_time = 0.0

def get_all_positions(acct_id: str) -> dict:
    global _positions_cache, _positions_cache_time
    now = time.time()
    if now - _positions_cache_time < WEBULL_CACHE_TTL and _positions_cache is not None:
        return _positions_cache
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
            _positions_cache      = result
            _positions_cache_time = now
            return result
        elif res.status_code == 429:
            time.sleep(WEBULL_429_BACKOFF)
    except:
        pass
    return _positions_cache or {}

def invalidate_pos_cache():
    global _positions_cache_time
    _positions_cache_time = 0.0

def place_order(symbol: str, side: str, qty: int, acct_id: str) -> bool:
    with _order_lock:
        try:
            order = {
                "client_order_id":      uuid.uuid4().hex,
                "combo_type":           "NORMAL",
                "symbol":               symbol,
                "instrument_type":      "EQUITY",
                "market":               "US",
                "side":                 side,
                "order_type":           "MARKET",
                "time_in_force":        "DAY",
                "quantity":             str(qty),
                "support_trading_session": "CORE",
                "entrust_type":         "QTY",
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

# ── SPY context (shared across all bots) ─────────────────────────────────────
_spy_prices  = []
_qqq_prices  = []
_spy_lock    = threading.Lock()

def refresh_context_data():
    """Fetch SPY and QQQ prices. Called by context refresh thread."""
    global _spy_prices, _qqq_prices
    while True:
        try:
            spy = fetch_prices("SPY", 35)
            qqq = fetch_prices("QQQ", 35)
            with _spy_lock:
                if spy:
                    _spy_prices = spy
                if qqq:
                    _qqq_prices = qqq
        except:
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
    """V1.1: QQQ context for bear pair entry filter."""
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

# ── Phase4Memory ─────────────────────────────────────────────────────────────
class Phase4Memory:
    """
    V1.1: Pattern memory for Phase4 bots.
    Fingerprints every trade with entry conditions + outcome.
    Daily analysis generates win rates per condition bucket.
    Separate tables from crypto to avoid interference.
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
            hour_cdt        INTEGER,
            day_of_week     INTEGER,
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
                     sym_ctx: dict):
        if not self._enabled:
            return
        threading.Thread(target=self._write_entry, daemon=True, args=(
            trade_id, symbol, bear_pair, is_bear, mode, entry_price,
            sym_rsi, spy_ctx, qqq_ctx, sym_ctx
        )).start()

    def _write_entry(self, trade_id, symbol, bear_pair, is_bear, mode,
                     entry_price, sym_rsi, spy_ctx, qqq_ctx, sym_ctx):
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
                         hour_cdt, day_of_week)
                        VALUES (%s,%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s, %s,%s, %s,%s)
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
                        now.hour,
                        now.weekday(),
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
        """Daily analysis -- builds win rate buckets from completed trades."""
        if not self._enabled:
            return
        query = """
            SELECT symbol, is_bear_trade, mode, symbol_rsi, spy_rsi, qqq_rsi,
                   spy_bullish, qqq_overbought, higher_lows, hour_cdt,
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
            buckets: dict  = defaultdict(list)
            pnl_bkts: dict = defaultdict(list)

            for row in rows:
                rsi_b = ("rsi_lt30" if (row["symbol_rsi"] or 99) < 30 else
                          "rsi_30_40" if (row["symbol_rsi"] or 99) < 40 else
                          "rsi_40_55" if (row["symbol_rsi"] or 99) < 55 else "rsi_gt55")
                spy_b = "spy_bull" if row["spy_bullish"] else "spy_bear"
                qqq_b = "qqq_ob" if row["qqq_overbought"] else "qqq_ok"
                bear_b = "bear" if row["is_bear_trade"] else "bull"
                mode_b = row["mode"] or "SCALP"
                hr_b   = ("hr_open" if (row["hour_cdt"] or 12) < 10 else
                           "hr_mid"  if (row["hour_cdt"] or 12) < 13 else "hr_late")
                key = f"{row['symbol']}|{bear_b}|{mode_b}|{rsi_b}|{spy_b}|{qqq_b}|{hr_b}"
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
                        if len(outcomes) < 3:
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
            print(f"[PM] Analysis complete: {len(new_cache)} buckets | {total} trades | {wr:.1%} WR", flush=True)
        except Exception as e:
            print(f"[PM] analysis error: {e}", flush=True)

    def get_win_rate(self, symbol: str, is_bear: bool, mode: str,
                     sym_rsi: float, spy_ctx: dict, qqq_ctx: dict,
                     hour: int) -> float:
        """Look up historical win rate for current conditions. Returns 0.5 if unknown."""
        if not self._win_rates:
            return 0.5
        rsi_b = ("rsi_lt30" if sym_rsi < 30 else
                  "rsi_30_40" if sym_rsi < 40 else
                  "rsi_40_55" if sym_rsi < 55 else "rsi_gt55")
        spy_b  = "spy_bull" if spy_ctx.get("bullish") else "spy_bear"
        qqq_b  = "qqq_ob" if qqq_ctx.get("overbought") else "qqq_ok"
        bear_b = "bear" if is_bear else "bull"
        mode_b = mode or "SCALP"
        hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
        key    = f"{symbol}|{bear_b}|{mode_b}|{rsi_b}|{spy_b}|{qqq_b}|{hr_b}"
        return self._win_rates.get(key, 0.5)

    def start_scheduler(self):
        def _run():
            time.sleep(300)
            self.run_analysis()
            while True:
                time.sleep(PM_ANALYSIS_INTERVAL)
                self.run_analysis()
        threading.Thread(target=_run, daemon=True, name="p4-pattern-memory").start()


# ── Shared pattern memory instance ───────────────────────────────────────────
_phase4_memory = None   # Phase4Memory -- initialized in run()


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
        self.bear_prices: list  = []
        self.peak_price:  float = 0.0
        self.trough_price:float = 0.0   # V1.1: MAE tracking
        self.entry_price: float = 0.0
        self.entry_time:  float = 0.0   # V1.1: hold time tracking
        self.trade_id:    str   = ""    # V1.1: fingerprint ID
        self.mfe:         float = 0.0   # V1.1: max favorable excursion
        self.mae:         float = 0.0   # V1.1: max adverse excursion
        self.in_position: bool  = False
        self.active_sym:  str   = symbol  # switches to bear_pair during reversal
        self.mode:        str   = "SCALP"
        self.cooldown_until: float = 0.0
        # V1.1: context captured at entry for fingerprint
        self._entry_spy_ctx: dict = {}
        self._entry_qqq_ctx: dict = {}
        self._entry_sym_ctx: dict = {}
        self._entry_rsi:     float = 50.0

        self.reversal_state: dict = {"state": "IDLE"}

        self.daily_wins:   int   = 0
        self.daily_losses: int   = 0
        self.daily_pnl:    float = 0.0

    def is_on_cooldown(self) -> bool:
        return time.time() < self.cooldown_until

    def set_cooldown(self, secs: int):
        self.cooldown_until = time.time() + secs

    def refresh_prices(self):
        prices = fetch_prices(self.symbol, WARMUP_BARS + 5)
        if prices:
            self.prices = prices
        bear_p = fetch_prices(self.bear_pair, WARMUP_BARS + 5)
        if bear_p:
            self.bear_prices = bear_p

    def get_symbol_context(self) -> dict:
        prices = self.prices
        if len(prices) < 21:
            return {"rsi": 50, "ma20": 0, "above_ma20": False,
                    "trend_10bar": 0, "vol_ratio": 1.0, "higher_lows": False}
        rsi        = compute_rsi(prices) or 50
        ma20       = compute_ma(prices, 20) or prices[-1]
        trend_10   = (prices[-1] - prices[-11]) / prices[-11] if len(prices) > 11 and prices[-11] > 0 else 0
        above_ma20 = prices[-1] > ma20
        higher_l   = check_higher_lows(prices)
        return {
            "rsi":        rsi,
            "ma20":       ma20,
            "above_ma20": above_ma20,
            "trend_10bar": round(trend_10 * 100, 3),
            "higher_lows": higher_l,
        }

    def select_mode(self, spy_ctx: dict, sym_ctx: dict) -> str:
        """
        EXTENDED: SPY strong + symbol trending up + higher lows confirmed
        RIDE:     SPY bullish + symbol above MA20 or trending
        SCALP:    everything else
        """
        if spy_ctx["overbought"]:
            return "SCALP"
        if (spy_ctx["strong"]
                and sym_ctx["trend_10bar"] > 0.3
                and sym_ctx["higher_lows"]
                and sym_ctx["above_ma20"]):
            return "EXTENDED"
        if spy_ctx["bullish"] and (sym_ctx["above_ma20"] or sym_ctx["trend_10bar"] > 0.1):
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
            sl      = round(sl      * self.cfg["ride_stop_mult"],  4)
            trail   = round(trail   * self.cfg["ride_trail_mult"], 4)
        elif self.mode == "EXTENDED":
            sl      = round(sl      * self.cfg["ext_stop_mult"],   4)
            trail   = round(trail   * self.cfg["ext_trail_mult"],  4)
            ratchet = self.cfg["ext_ratchet_pct"]

        return sl, ratchet, trail

    def should_enter_bull(self, spy_ctx: dict, sym_ctx: dict) -> bool:
        """Entry logic for bull symbol based on mode."""
        prices = self.prices
        if len(prices) < 8:
            return False

        now    = datetime.now(tz=CENTRAL)
        if now.hour in self.recipe.get("avoid_hours", []):
            return False
        if now.weekday() in self.recipe.get("avoid_days", []):
            return False

        rsi = sym_ctx["rsi"]

        if self.mode == "SCALP":
            # Classic RSI dip + bounce
            bouncing = len(prices) >= 3 and prices[-1] > prices[-3]
            return rsi < 40 and bouncing

        elif self.mode == "RIDE":
            # Shallow dip in uptrend
            bouncing = len(prices) >= 3 and prices[-1] > prices[-3]
            return rsi < 52 and bouncing and sym_ctx["above_ma20"]

        elif self.mode == "EXTENDED":
            # Pullback to MA in strong trend
            bouncing = len(prices) >= 3 and prices[-1] > prices[-3]
            near_ma  = (prices[-1] - sym_ctx["ma20"]) / sym_ctx["ma20"] < 0.01 if sym_ctx["ma20"] > 0 else False
            return rsi < 58 and bouncing and sym_ctx["higher_lows"] and (near_ma or rsi < 50)

        return False

    def check_reversal(self) -> bool:
        """
        Monitor bull RSI for exhaustion → flip to bear pair.
        Returns True if a bear entry was triggered.
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
                log(self.symbol, f"👁️ REVERSAL WATCH → {self.bear_pair} | RSI={bull_rsi}")
            return False

        if state["state"] == "WATCHING":
            if now_t - state.get("watch_start", now_t) > REVERSAL_MAX_WATCH:
                self.reversal_state = {"state": "IDLE"}
                return False
            if bull_rsi < REVERSAL_RSI_RESET:
                log(self.symbol, f"↩️ REVERSAL CANCEL | RSI recovered to {bull_rsi}")
                self.reversal_state = {"state": "IDLE"}
                return False

            bull_peak = max(state.get("bull_peak", self.prices[-1]), self.prices[-1])
            self.reversal_state["bull_peak"] = bull_peak
            drop = (bull_peak - self.prices[-1]) / bull_peak if bull_peak > 0 else 0

            if drop >= REVERSAL_CONFIRM:
                bear_p    = self.bear_prices
                bouncing  = len(bear_p) >= 3 and bear_p[-1] > bear_p[-3]
                if not bouncing:
                    return False

                # V1.1: QQQ filter -- if QQQ is already oversold, reversal
                # is weaker (semis may be catching down to QQQ, not reversing)
                qqq_ctx = get_qqq_context()
                if qqq_ctx.get("oversold"):
                    log(self.symbol,
                        f"🚫 REVERSAL BLOCKED: QQQ already oversold (RSI={qqq_ctx['rsi']:.1f}) "
                        f"-- bear entry too risky")
                    return False

                # V1.1: Additional confirmation -- QQQ should be overbought
                # LABD uses a stricter gate since biotech has more fake reversals
                gate = QQQ_BEAR_RSI_GATE_LABD if self.bear_pair == "LABD" else QQQ_BEAR_RSI_GATE
                if qqq_ctx.get("rsi", 50) < gate:
                    log(self.symbol,
                        f"⚠️ REVERSAL WEAK: QQQ RSI={qqq_ctx['rsi']:.1f} < {gate} "
                        f"({'LABD strict gate' if self.bear_pair == 'LABD' else 'standard gate'}) "
                        f"-- skipping bear entry")
                    return False

                log(self.symbol,
                    f"🔁 REVERSAL CONFIRMED → {self.bear_pair} | "
                    f"drop={round(drop*100,2)}% | QQQ RSI={qqq_ctx['rsi']:.1f} ✅")
                self.reversal_state = {"state": "IDLE"}
                return True

        return False

    def try_buy(self, sym: str, prices: list, spy_ctx: dict, sym_ctx: dict) -> bool:
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
        log(self.symbol, f"📊 Entry signal | mode={self.mode} | RSI={sym_ctx['rsi']} | SPY={'bull' if spy_ctx['bullish'] else 'bear'}")

        success = place_order(sym, "BUY", qty, self.acct_id)
        if success:
            self.in_position    = True
            self.active_sym     = sym
            self.entry_price    = price
            self.peak_price     = price
            self.trough_price   = price
            self.entry_time     = time.time()
            self.mfe            = 0.0
            self.mae            = 0.0
            self.trade_id       = __import__('secrets').token_hex(8)
            # V1.1: capture context for fingerprint
            self._entry_spy_ctx = spy_ctx
            self._entry_qqq_ctx = get_qqq_context()
            self._entry_sym_ctx = sym_ctx
            self._entry_rsi     = sym_ctx.get("rsi", 50)
            invalidate_pos_cache()
            # V1.1: fingerprint entry
            if _phase4_memory:
                _phase4_memory.record_entry(
                    self.trade_id, self.symbol, self.bear_pair,
                    sym == self.bear_pair, self.mode, price,
                    self._entry_rsi, spy_ctx,
                    self._entry_qqq_ctx, sym_ctx
                )
            log(self.symbol, f"⚡ BUY: {sym} | {qty} shares @ ~${round(price,2)} | mode={self.mode}")
            alert(f"⚡ PHASE4 BUY [{self.mode}]: {sym} | {qty} @ ~${round(price,2)}")
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
            # V1.1: fingerprint exit
            if _phase4_memory and self.trade_id:
                _phase4_memory.record_exit(
                    self.trade_id, pnl_pct > 0, pnl_pct,
                    reason, hold_min, self.mfe, self.mae
                )
            self.in_position  = False
            self.peak_price   = 0.0
            self.trough_price = 0.0
            self.entry_price  = 0.0
            self.entry_time   = 0.0
            self.trade_id     = ""
            self.mfe          = 0.0
            self.mae          = 0.0
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
        """On startup, check if we already have an open position."""
        positions = get_all_positions(self.acct_id)
        for sym in [self.symbol, self.bear_pair]:
            if sym in positions:
                pos              = positions[sym]
                cost             = float(pos.get("cost_price", pos.get("average_cost", 0)))
                self.in_position = True
                self.active_sym  = sym
                self.entry_price = cost
                self.peak_price  = max(cost, get_current_price(sym) or cost)
                log(self.symbol, f"🔄 Recovered position: {sym} | entry=${cost:.3f} | peak=${self.peak_price:.3f}")
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
                sym_ctx = self.get_symbol_context()

                prices = self.prices
                if not prices:
                    time.sleep(LOOP_INTERVAL)
                    continue

                current_price = prices[-1]

                # ── MANAGE OPEN POSITION ─────────────────────────────────
                if self.in_position:
                    active_prices = self.prices if self.active_sym == self.symbol else self.bear_prices
                    if not active_prices:
                        time.sleep(LOOP_INTERVAL)
                        continue

                    price = active_prices[-1]
                    self.peak_price   = max(self.peak_price, price)
                    self.trough_price = min(self.trough_price if self.trough_price > 0 else price, price)
                    profit_pct = (price - self.entry_price) / self.entry_price if self.entry_price > 0 else 0
                    drawdown   = (self.peak_price - price) / self.peak_price if self.peak_price > 0 else 0

                    # V1.1: Update MFE/MAE in real time
                    self.mfe = max(self.mfe, profit_pct)
                    self.mae = min(self.mae, profit_pct)

                    sl, ratchet, trail = self.get_exit_params()

                    # Status log every 60s
                    log(self.symbol,
                        f"📊 {self.active_sym} | P&L={round(profit_pct*100,2):+.2f}% | "
                        f"peak=${self.peak_price:.3f} | dd={round(drawdown*100,2):.2f}% | mode={self.mode}")

                    # Stop loss
                    if profit_pct <= -sl:
                        self.try_sell("stop-loss", profit_pct)

                    # Trail stop (after ratchet)
                    elif profit_pct >= ratchet and drawdown >= trail:
                        self.try_sell("trail", profit_pct)

                    # EXTENDED mode: exit on trend break (lower low)
                    elif self.mode == "EXTENDED" and self.active_sym == self.symbol:
                        if check_higher_lows(active_prices) is False and profit_pct > 0:
                            log(self.symbol, "📉 EXTENDED: trend break detected")
                            self.try_sell("trend-break", profit_pct)

                # ── LOOK FOR ENTRY ────────────────────────────────────────
                elif not self.is_on_cooldown():
                    # Check bear reversal first (higher priority if bull exhausted)
                    if self.check_reversal():
                        self.try_buy(self.bear_pair, self.bear_prices, spy_ctx,
                                     {"rsi": compute_rsi(self.bear_prices) or 50,
                                      "ma20": compute_ma(self.bear_prices) or self.bear_prices[-1],
                                      "above_ma20": False, "trend_10bar": 0, "higher_lows": False})
                    # Otherwise check bull entry
                    elif self.should_enter_bull(spy_ctx, sym_ctx):
                        self.try_buy(self.symbol, prices, spy_ctx, sym_ctx)

            except Exception as e:
                log(self.symbol, f"🔴 Loop error: {e}")
                log(self.symbol, traceback.format_exc())

            time.sleep(LOOP_INTERVAL)


# ── Phase4 Service ────────────────────────────────────────────────────────────
def run():
    global _phase4_memory
    print("[PHASE4] NEXUS PHASE 4 V1.3 STARTING", flush=True)
    print("[PHASE4] Bots: NUGT(30%) | SOXL(25%) | LABU(25%) | TQQQ(20%)", flush=True)
    print("[PHASE4] Bear pairs: DUST | SOXS | LABD | SQQQ", flush=True)
    print("[PHASE4] V1.1: QQQ filter | Pattern memory | MFE/MAE tracking", flush=True)

    # V1.1: Initialize pattern memory
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

    # Start context refresh thread (SPY/QQQ)
    ctx_thread = threading.Thread(target=refresh_context_data, daemon=True)
    ctx_thread.start()
    time.sleep(5)  # let SPY/QQQ warm up

    # Start each bot in its own thread
    bots    = []
    threads = []
    for symbol, config in BOT_CONFIGS.items():
        bot    = SymbolBot(symbol, config, acct_id)
        bots.append(bot)
        t = threading.Thread(target=bot.run_loop, daemon=True, name=f"bot_{symbol}")
        threads.append(t)
        t.start()
        print(f"[PHASE4] ✅ {symbol} bot started", flush=True)
        time.sleep(2)  # stagger starts

    from phase4_server import start_server
    start_server(bots)
    alert("⚡ PHASE4 V1.3 ONLINE | NUGT+SOXL+LABU+TQQQ | QQQ filter active | Pattern memory live")
    print("[PHASE4] All bots running. Holding main thread.", flush=True)

    # Keep main thread alive + daily reset at 8am
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
