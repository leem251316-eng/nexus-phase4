import time
import os
import json
import threading
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

import shared_state
import trade_log

try:
    import psycopg2
    _psycopg2_ok = True
except ImportError:
    _psycopg2_ok = False

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ==============================================================================
# SCANNER V2.2 - DYNAMIC MOMENTUM HUNTER
# ✅ Startup alert removed — no more double T-Bone message on deploy
# ✅ Added: paused_scanner + buys_disabled bot_state checks
# ✅ V1.9: Per-symbol volume spike multipliers from nexus_analyzer.py 2yr backtest
# ✅ V2.0: Pattern memory + win-rate gate (mirrors Berserker/Phase4/Options)
# ✅ V2.0: Recovery entry_time fix -- max-hold timer no longer resets on redeploy
# ✅ V2.0: Pruned negative-EV symbols from universe + vol-mult table
# ✅ V2.2: WIN_RATE_GATE_THRESHOLD raised 35%->45% (same fix as Berserker/Phase4)
# ✅ V2.2: Time exit now only fires when position is NEGATIVE at 60 minutes --
#          previously fired on flat/sideways positions that might have recovered.
#          Stop loss still fires immediately regardless of direction.
# ✅ V2.1: DB transaction abort fix -- broken transactions now rolled back before
#          retry so subsequent queries don't die with "current transaction aborted"
# ✅ V2.1: np.float64 bug fix -- pnl_pct/mfe/mae cast to plain float before
#          passing to psycopg2 (np.float64 in SQL string causes literal "np.float64(x)"
#          to appear in the statement instead of a numeric value)
# ==============================================================================

API_KEY    = os.environ.get("ALPACA_API_KEY")
SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")
IS_PAPER   = False

BOT_NAME = "SCANNER"
CENTRAL  = ZoneInfo("America/Chicago")

SCANNER_UNIVERSE = [
    "SPY", "QQQ", "IWM", "DIA", "VXX", "UVXY", "SVXY",
    "XLF", "XLE", "XLK", "XLV", "XLI", "ARKK",
    "GLD", "SLV", "USO", "UNG",
    "UPRO", "TMF", "TNA", "TZA", "NAIL", "WANT",
    "MSTU", "MSTZ", "NVDL", "NVDS",
    "NFLX", "BABA", "UBER", "SNAP", "RIVN",
    "HOOD", "SOFI", "UPST", "RBLX",
    "IONQ", "RGTI", "QUBT", "JOBY", "ACHR",
    "AI", "BBAI", "SOUN",
]

TRAILING_STOP_PHASE1  = 0.015
TRAILING_STOP_PHASE2  = 0.004
RATCHET_THRESHOLD     = 0.03
STOP_LOSS_PCT         = 0.015
MAX_HOLD_MINUTES      = 60
VOLUME_SPIKE_MULT     = 1.5   # default fallback
PRICE_MOVE_MIN        = 0.015
SCAN_INTERVAL_SECS    = 45
MIN_TRADE_AMT         = 2.00
TRADE_PCT_OF_BUDGET   = 0.75
CIRCUIT_BREAKER_COUNT = 6
CASH_LOG_INTERVAL     = 60

# V2.0: Pattern memory
PM_MIN_TRADES        = 15     # min completed trades before analysis runs
PM_MIN_BUCKET_TRADES = 3       # min samples per bucket before it's used
PM_ANALYSIS_INTERVAL = 86400   # daily

# V2.0: Win-rate gate -- skip entries whose exact historical bucket has a
# win rate below this, once it has >= PM_MIN_BUCKET_TRADES samples (see
# ScannerMemory.should_skip_entry). "No data" never blocks a trade.
WIN_RATE_GATE_THRESHOLD = 0.45  # V2.2: was 0.35 -- 35% WR is net negative EV at current stop/TP ratio


# ── Per-symbol volume spike multipliers — nexus_analyzer.py 2yr backtest ─────
# V2.0: removed 14 entries for symbols not in SCANNER_UNIVERSE (dead config --
# SOXL/SOXS/TQQQ/LABD/LABU/FNGU/FAS/FAZ/ERX/DUST/SDOW/UDOW/SPXL/SPXU are
# Phase4 leveraged-ETF symbols, never part of Scanner's universe). The
# remaining entries ARE live; MSTZ/NVDS were flagged negative-EV by the
# original backtest but are left for the V2.0 win-rate gate to evaluate
# with real data rather than hand-pruning on a stale 2yr snapshot.
SCANNER_VOL_MULT = {
    "TNA":  3.0,  # EV +0.075%/trade
    "TZA":  2.5,  # EV -0.030% (best of bad options)
    "MSTU": 1.5,  # EV +0.063%/trade
    "MSTZ": 1.5,  # EV -0.069% (negative at all levels — let gate evaluate)
    "NVDL": 2.5,  # EV +0.086%/trade
    "NVDS": 1.5,  # EV -0.046% (negative — let gate evaluate)
}


trading_client    = TradingClient(API_KEY, SECRET_KEY, paper=IS_PAPER)
stock_data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

scanner_positions: dict = {}
_dropping_count:   int  = 0

# V2.0: persist entry_time (and fingerprint trade_id) across redeploys so
# MAX_HOLD_MINUTES and pattern memory don't reset on every restart.
SCANNER_POSITIONS_FILE = "/app/data/scanner_positions.json"

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
_last_cash_log   = 0.0

# ✅ Per-symbol trade tracker — feeds /wins command in main.py
_today_symbol_trades: dict = {}

def log_symbol_trade(symbol: str, pnl: float):
    """Track per-symbol win/loss for /wins command. Resets daily."""
    if symbol not in _today_symbol_trades:
        _today_symbol_trades[symbol] = {"bot": "SCANNER", "wins": 0, "losses": 0, "pnl": 0.0}
    if pnl > 0:
        _today_symbol_trades[symbol]["wins"] += 1
    else:
        _today_symbol_trades[symbol]["losses"] += 1
    _today_symbol_trades[symbol]["pnl"] = round(
        _today_symbol_trades[symbol]["pnl"] + pnl * 100, 3
    )

def reset_daily_symbol_trades():
    """Called by main.py reset_daily_state() each morning at 8am."""
    global _today_symbol_trades
    _today_symbol_trades.clear()

def log(msg):
    print(f"[SCANNER | {datetime.now(tz=CENTRAL).strftime('%H:%M:%S')}] {msg}", flush=True)

def alert(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except:
        pass


# ==============================================================================
# SCANNER MEMORY -- V2.0
# Mirrors BerserkerMemory/Phase4Memory/OptionsMemory: separate fingerprint
# table, threaded writes, daily bucket analysis, win-rate gate on entry.
#
# Scanner's signal shape differs from the others (no RSI -- entries are
# vol-spike + price-move based), so the bucket key uses:
#   symbol | vol_ratio bucket | price_move bucket | spy trend | hour bucket
# ==============================================================================
class ScannerMemory:
    def __init__(self, db_url: str):
        self.db_url         = db_url
        self._conn          = None
        self._lock          = threading.Lock()
        self._win_rates     = {}
        self._last_analysis = 0.0
        self._enabled       = bool(db_url) and _psycopg2_ok

    def _get_conn(self):
        if not self._enabled:
            return None
        try:
            if self._conn is None or self._conn.closed:
                self._conn = psycopg2.connect(self.db_url)
                self._conn.autocommit = False
                return self._conn
            # V2.1: If the connection is in a failed transaction state, roll it
            # back before returning so the next query doesn't immediately die
            # with "current transaction is aborted, commands ignored...".
            # This was the root cause of the DB errors seen in today's logs.
            try:
                self._conn.rollback()
            except Exception:
                self._conn = None
                self._conn = psycopg2.connect(self.db_url)
                self._conn.autocommit = False
            return self._conn
        except Exception as e:
            log(f"[SM] DB connect error: {e}")
            return None

    def init_tables(self):
        if not self._enabled:
            log("[SM] Scanner pattern memory: disabled (no DATABASE_URL or psycopg2)")
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS scanner_trade_fingerprints (
            id              SERIAL PRIMARY KEY,
            trade_id        VARCHAR(32) UNIQUE NOT NULL,
            symbol          VARCHAR(10) NOT NULL,
            entry_ts        BIGINT,
            exit_ts         BIGINT,
            entry_price     REAL,
            vol_ratio       REAL,
            price_move_pct  REAL,
            spy_bullish     BOOLEAN,
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
        CREATE INDEX IF NOT EXISTS idx_stf_symbol ON scanner_trade_fingerprints(symbol);
        CREATE INDEX IF NOT EXISTS idx_stf_won    ON scanner_trade_fingerprints(won);
        CREATE TABLE IF NOT EXISTS scanner_pattern_stats (
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
                    log("[SM] Scanner pattern memory tables ready")
        except Exception as e:
            log(f"[SM] init_tables error: {e}")

    # -- writes --------------------------------------------------------------
    def record_entry(self, trade_id: str, symbol: str, entry_price: float,
                      vol_ratio: float, price_move_pct: float, spy_bullish: bool):
        if not self._enabled:
            return
        threading.Thread(target=self._write_entry, daemon=True, args=(
            trade_id, symbol, entry_price, vol_ratio, price_move_pct, spy_bullish
        )).start()

    def _write_entry(self, trade_id, symbol, entry_price, vol_ratio,
                      price_move_pct, spy_bullish):
        now = datetime.now(tz=CENTRAL)
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO scanner_trade_fingerprints
                        (trade_id, symbol, entry_ts, entry_price,
                         vol_ratio, price_move_pct, spy_bullish,
                         hour_cdt, day_of_week)
                        VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        trade_id, symbol, int(time.time()), float(entry_price),
                        float(vol_ratio), float(price_move_pct), bool(spy_bullish),
                        now.hour, now.weekday(),
                    ))
                conn.commit()
        except Exception as e:
            log(f"[SM] write_entry error: {e}")
            try:
                if self._conn:
                    self._conn.rollback()
            except Exception:
                self._conn = None

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
            # V2.1: Cast numpy scalar types to plain Python floats before passing
            # to psycopg2. np.float64 values stringify as "np.float64(x)" which
            # lands literally in the SQL statement and aborts the transaction.
            pnl_val = float(round(float(pnl_pct) * 100, 3))
            mfe_val = float(round(float(mfe) * 100, 3))
            mae_val = float(round(float(mae) * 100, 3))
            hold_val = int(hold_min)
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE scanner_trade_fingerprints
                        SET won=%s, pnl_pct=%s, exit_reason=%s,
                            hold_time_min=%s, exit_ts=%s, mfe=%s, mae=%s
                        WHERE trade_id=%s
                    """, (bool(won), pnl_val, exit_reason,
                          hold_val, int(time.time()),
                          mfe_val, mae_val,
                          trade_id))
                conn.commit()
        except Exception as e:
            log(f"[SM] write_exit error: {e}")
            try:
                if self._conn:
                    self._conn.rollback()
            except Exception:
                self._conn = None

    # -- bucket key + gate -----------------------------------------------------
    @staticmethod
    def _bucket_key(symbol: str, vol_ratio: float, price_move_pct: float,
                    spy_bullish: bool, hour: int) -> str:
        """Shared bucket-key builder -- used by both should_skip_entry()
        (lookup) and run_analysis() (population) so the two never drift."""
        vol_b  = ("vol_lt2x" if vol_ratio < 2.0 else
                  "vol_2_3x" if vol_ratio < 3.0 else "vol_gt3x")
        move_b = ("move_lt2pct" if price_move_pct < 2.0 else
                  "move_2_4pct" if price_move_pct < 4.0 else "move_gt4pct")
        spy_b  = "spy_bull" if spy_bullish else "spy_bear"
        hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
        return f"{symbol}|{vol_b}|{move_b}|{spy_b}|{hr_b}"

    def should_skip_entry(self, symbol: str, vol_ratio: float,
                           price_move_pct: float, spy_bullish: bool,
                           hour: int) -> tuple:
        """
        V2.0: Win-rate gate. Returns (skip, win_rate, has_data).

        has_data is True only if this exact bucket has >= PM_MIN_BUCKET_TRADES
        historical samples -- run_analysis() only writes buckets that meet
        that threshold, so presence in _win_rates IS the sample-size check.
        "No data" never blocks a trade (skip=False, win_rate=0.5).
        """
        key = self._bucket_key(symbol, vol_ratio, price_move_pct, spy_bullish, hour)
        if key not in self._win_rates:
            return False, 0.5, False
        wr = self._win_rates[key]
        return (wr < WIN_RATE_GATE_THRESHOLD), wr, True

    # -- analysis --------------------------------------------------------------
    def run_analysis(self):
        if not self._enabled:
            return
        query = """
            SELECT symbol, vol_ratio, price_move_pct, spy_bullish, hour_cdt,
                   won, pnl_pct
            FROM scanner_trade_fingerprints WHERE won IS NOT NULL
        """
        # V2.1: Fetch is separated from processing so a transaction abort on
        # fetch can be rolled back cleanly without eating the whole analysis.
        rows = []
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                import psycopg2.extras as _pg_extras
                with _pg_extras.RealDictCursor(conn) as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
        except Exception as e:
            log(f"[SM] run_analysis fetch error: {e}")
            try:
                if self._conn:
                    self._conn.rollback()
            except Exception:
                self._conn = None
            return

        if len(rows) < PM_MIN_TRADES:
            log(f"[SM] {len(rows)} trades < {PM_MIN_TRADES} min, skipping")
            return

        try:
            from collections import defaultdict
            buckets  = defaultdict(list)
            pnl_bkts = defaultdict(list)

            for row in rows:
                key = self._bucket_key(
                    row["symbol"],
                    row["vol_ratio"] if row["vol_ratio"] is not None else 1.5,
                    row["price_move_pct"] if row["price_move_pct"] is not None else 1.5,
                    row["spy_bullish"],
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
                            INSERT INTO scanner_pattern_stats
                            (bucket_key, win_rate, sample_count, avg_pnl)
                            VALUES (%s,%s,%s,%s)
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
            log(f"[SM] Analysis: {len(new_cache)} buckets | {total} trades | {wr:.1%} WR")
        except Exception as e:
            log(f"[SM] analysis error: {e}")
            try:
                if self._conn:
                    self._conn.rollback()
            except Exception:
                self._conn = None

    def start_scheduler(self):
        def _run():
            time.sleep(300)
            self.run_analysis()
            while True:
                time.sleep(PM_ANALYSIS_INTERVAL)
                self.run_analysis()
        threading.Thread(target=_run, daemon=True, name="ScannerMemory-Analysis").start()


_scanner_memory: ScannerMemory = None


def init_scanner_memory():
    """Called once from run() at startup."""
    global _scanner_memory
    _scanner_memory = ScannerMemory(DATABASE_URL)
    _scanner_memory.init_tables()
    _scanner_memory.start_scheduler()


# V2.0: persist entry_time/trade_id/peak/trough across redeploys -----------
def save_scanner_positions():
    """Persist scanner_positions so MAX_HOLD_MINUTES and fingerprint
    trade_ids survive a restart. Never raises -- logs on failure."""
    try:
        os.makedirs(os.path.dirname(SCANNER_POSITIONS_FILE), exist_ok=True)
        serializable = {}
        for sym, pos in scanner_positions.items():
            d = dict(pos)
            if isinstance(d.get("entry_time"), datetime):
                d["entry_time"] = d["entry_time"].isoformat()
            serializable[sym] = d
        tmp = SCANNER_POSITIONS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(serializable, f, indent=2)
        os.replace(tmp, SCANNER_POSITIONS_FILE)
    except Exception as e:
        log(f"⚠️ save_scanner_positions error: {e}")


def load_scanner_positions() -> dict:
    """Load persisted positions (entry_time as datetime). Returns {} on
    any failure or missing file -- never raises."""
    try:
        if not os.path.exists(SCANNER_POSITIONS_FILE):
            return {}
        with open(SCANNER_POSITIONS_FILE, "r") as f:
            data = json.load(f)
        for sym, pos in data.items():
            if isinstance(pos.get("entry_time"), str):
                pos["entry_time"] = datetime.fromisoformat(pos["entry_time"])
        return data
    except Exception as e:
        log(f"⚠️ load_scanner_positions error: {e}")
        return {}


def is_market_hours() -> bool:
    now = datetime.now(tz=CENTRAL)
    return now.weekday() < 5 and 8 <= now.hour < 15

def get_alpaca_cash() -> float:
    try:
        return float(trading_client.get_account().cash)
    except Exception as e:
        log(f"⚠️ Cash fetch error: {e}")
        return 0.0

def get_price(symbol: str) -> float | None:
    try:
        t = stock_data_client.get_stock_latest_trade(
            StockLatestTradeRequest(symbol_or_symbols=symbol, feed=DataFeed.IEX))
        return float(t[symbol].price)
    except:
        return None

def get_bars(symbol: str, limit: int = 20) -> pd.DataFrame | None:
    try:
        bars = stock_data_client.get_stock_bars(StockBarsRequest(
            symbol_or_symbols=symbol, timeframe=TimeFrame.Minute,
            limit=limit, feed=DataFeed.IEX))
        df = bars.df
        if hasattr(df.index, "levels"):
            df = df.xs(symbol, level=0)
        return df if len(df) >= 5 else None
    except:
        return None

# V2.0: SPY trend cache for the win-rate gate's bucket key. Refreshed at most
# once every 60s -- this is informational context for bucketing, not a
# trading signal, so a slightly stale read is fine and saves API calls.
_spy_bullish_cache: dict = {"value": True, "ts": 0.0}

def get_spy_bullish() -> bool:
    now = time.time()
    if now - _spy_bullish_cache["ts"] < 60:
        return _spy_bullish_cache["value"]
    try:
        df = get_bars("SPY", limit=20)
        if df is not None and len(df) >= 11:
            bullish = bool(df["close"].iloc[-1] > df["close"].iloc[-11])
            _spy_bullish_cache["value"] = bullish
            _spy_bullish_cache["ts"]    = now
            return bullish
    except Exception:
        pass
    return _spy_bullish_cache["value"]

def check_circuit_breaker() -> bool:
    global _dropping_count
    sample   = SCANNER_UNIVERSE[:20]
    dropping = 0
    for sym in sample:
        df = get_bars(sym, limit=6)
        if df is not None and len(df) >= 2:
            if df["close"].iloc[-1] < df["close"].iloc[-5]:
                dropping += 1
    _dropping_count = dropping
    triggered = dropping >= CIRCUIT_BREAKER_COUNT
    if triggered:
        log(f"⚡ Circuit breaker: {dropping}/{len(sample)} symbols dropping — pausing buys")
    return triggered

def scan_for_entry(symbol: str) -> dict | None:
    df = get_bars(symbol, limit=20)
    if df is None or len(df) < 11:
        return None
    avg_vol    = df["volume"].iloc[-11:-1].mean()
    latest_vol = df["volume"].iloc[-1]
    vol_ratio  = (latest_vol / avg_vol) if avg_vol > 0 else 0
    price_10m  = df["close"].iloc[-11]
    price_now  = df["close"].iloc[-1]
    price_move = (price_now - price_10m) / price_10m if price_10m > 0 else 0
    vol_mult   = SCANNER_VOL_MULT.get(symbol, VOLUME_SPIKE_MULT)
    if vol_ratio >= vol_mult and price_move >= PRICE_MOVE_MIN:
        return {"symbol": symbol, "price": price_now,
                "vol_ratio": round(vol_ratio, 2), "price_move": round(price_move * 100, 2)}
    return None

def position_exists_alpaca(symbol: str) -> bool:
    try:
        trading_client.get_open_position(symbol)
        return True
    except:
        return False

def buy(symbol: str, amount: float) -> bool:
    try:
        order = MarketOrderRequest(symbol=symbol, notional=round(amount, 2),
                                   side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
        trading_client.submit_order(order)
        log(f"🎯 SCANNER BUY: {symbol} | ${round(amount,2)}")
        alert(f"🎯 SCANNER BUY: {symbol} | ${round(amount,2)}")
        return True
    except Exception as e:
        log(f"⚠️ Buy error [{symbol}]: {e}")
        return False

def sell(symbol: str, reason: str, pnl_pct: float, pos_data: dict = None):
    try:
        if not position_exists_alpaca(symbol):
            shared_state.release(symbol)
            scanner_positions.pop(symbol, None)
            save_scanner_positions()
            return
        trading_client.close_position(symbol)
        shared_state.release(symbol)
        emoji     = "✅" if pnl_pct > 0 else "🛑"
        pnl_label = f"+{round(pnl_pct*100,2)}%" if pnl_pct > 0 else f"{round(pnl_pct*100,2)}%"
        log(f"{emoji} SCANNER EXIT [{reason}]: {symbol} | P&L: {pnl_label}")
        alert(f"{emoji} SCANNER EXIT [{reason}]: {symbol} | P&L: {pnl_label}")
        trade_log.record_trade("SCANNER", symbol, pnl_pct, reason)
        log_symbol_trade(symbol, pnl_pct)   # ✅ /wins hook

        # V2.0: record exit fingerprint (no-op if pos_data missing trade_id,
        # e.g. positions recovered before V2.0's record_entry existed)
        if _scanner_memory and pos_data and pos_data.get("trade_id"):
            entry_price = pos_data["entry_price"]
            peak_price  = pos_data.get("peak_price", entry_price)
            trough_price = pos_data.get("trough_price", entry_price)
            mfe = (peak_price - entry_price) / entry_price if entry_price > 0 else 0
            mae = (trough_price - entry_price) / entry_price if entry_price > 0 else 0
            hold_min = int((datetime.now(tz=CENTRAL) - pos_data["entry_time"]).total_seconds() / 60)
            _scanner_memory.record_exit(
                pos_data["trade_id"], won=bool(pnl_pct > 0), pnl_pct=pnl_pct,
                exit_reason=reason, hold_min=hold_min, mfe=mfe, mae=mae,
            )

        scanner_positions.pop(symbol, None)
        save_scanner_positions()
        shared_state.set_cooldown(symbol, 1800)
    except Exception as e:
        log(f"⚠️ Sell error [{symbol}]: {e}")
        shared_state.release(symbol)
        scanner_positions.pop(symbol, None)
        save_scanner_positions()

def manage_scanner_exits():
    for symbol in list(scanner_positions.keys()):
        pos_data    = scanner_positions[symbol]
        price       = get_price(symbol)
        if not price:
            continue
        entry_price = pos_data["entry_price"]
        peak_price  = pos_data.get("peak_price", entry_price)
        trough_price = pos_data.get("trough_price", entry_price)
        entry_time  = pos_data["entry_time"]
        profit_pct  = (price - entry_price) / entry_price
        if price > peak_price:
            scanner_positions[symbol]["peak_price"] = price
            peak_price = price
        if price < trough_price:
            scanner_positions[symbol]["trough_price"] = price
            trough_price = price
        drawdown = (peak_price - price) / peak_price if peak_price > 0 else 0
        trailing = TRAILING_STOP_PHASE2 if profit_pct >= RATCHET_THRESHOLD else TRAILING_STOP_PHASE1
        if profit_pct <= -STOP_LOSS_PCT:
            sell(symbol, "stop-loss", profit_pct, pos_data)
        elif drawdown >= trailing:
            sell(symbol, "trail-tight" if profit_pct >= RATCHET_THRESHOLD else "trail", profit_pct, pos_data)
        elif (datetime.now(tz=CENTRAL) - entry_time).total_seconds() >= MAX_HOLD_MINUTES * 60:
            # V2.2: Only time-exit when position is negative or flat.
            # If profit_pct > 0.005 (+0.5%), let the trail handle it --
            # a runner shouldn't be forced out just because it held 60 minutes.
            if profit_pct <= 0.005:
                sell(symbol, "max-hold", profit_pct, pos_data)

def recover_open_positions():
    persisted = load_scanner_positions()
    try:
        raw       = trading_client.get_all_positions()
        recovered = 0
        for pos in raw:
            sym = pos.symbol
            if sym not in SCANNER_UNIVERSE or sym in scanner_positions:
                continue
            if shared_state.owner(sym) is not None:
                log(f"⏭️ Recovery skipping {sym} — already owned by {shared_state.owner(sym)}")
                continue
            entry_price   = float(pos.avg_entry_price)
            try:
                current_price = float(pos.current_price) if pos.current_price else entry_price
            except:
                current_price = entry_price
            if not shared_state.claim(sym, BOT_NAME):
                continue

            # V2.0: restore entry_time/trade_id/peak/trough from disk if we
            # have them -- without this, every redeploy reset entry_time to
            # now(), so MAX_HOLD_MINUTES (60min) effectively never fired on
            # positions that survived a restart. Falls back to now() only
            # if nothing was persisted for this symbol (e.g. first deploy
            # after V2.0, or position opened by a previous code version).
            old = persisted.get(sym, {})
            entry_time = old.get("entry_time") if isinstance(old.get("entry_time"), datetime) else datetime.now(tz=CENTRAL)
            scanner_positions[sym] = {
                "entry_price":  entry_price,
                "peak_price":   max(entry_price, current_price, old.get("peak_price", entry_price)),
                "trough_price": min(entry_price, current_price, old.get("trough_price", entry_price)),
                "entry_time":   entry_time,
                "trade_id":     old.get("trade_id"),
                "vol_ratio":    old.get("vol_ratio"),
                "price_move":   old.get("price_move"),
            }
            recovered += 1
            held_min = int((datetime.now(tz=CENTRAL) - entry_time).total_seconds() / 60)
            pnl_pct = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
            log(f"🔄 Recovered: {sym} | entry=${entry_price:.2f} | now=${current_price:.2f} | "
                f"P&L: {pnl_pct:+.2f}% | held {held_min}m")
        if recovered:
            log(f"🔄 Scanner recovered {recovered} open position(s) from Alpaca")
            alert(f"🔄 SCANNER: Recovered {recovered} open position(s) on restart")
            save_scanner_positions()
        else:
            log("✅ Scanner: no open positions to recover")
    except Exception as e:
        log(f"⚠️ Position recovery error: {e}")

def run(bot_state=None):
    global _last_cash_log
    log(f"SCANNER V2.1 ONLINE | Universe: {len(SCANNER_UNIVERSE)} symbols | "
        f"Entry: per-symbol vol mult + move≥{int(PRICE_MOVE_MIN*100)}% | "
        f"Per-trade: {int(TRADE_PCT_OF_BUDGET*100)}% of scanner budget")

    init_scanner_memory()
    recover_open_positions()
    last_scan = 0.0

    while True:
        try:
            if not is_market_hours():
                time.sleep(60)
                continue
            if bot_state and (bot_state.get("paused") or bot_state.get("paused_scanner")):
                time.sleep(10)
                continue

            now = time.time()
            if scanner_positions:
                manage_scanner_exits()

            if now - last_scan >= SCAN_INTERVAL_SECS:
                last_scan = now
                pdt_slots = shared_state.get_pdt_slots()
                if pdt_slots == 0:
                    log("🔒 Scanner: 0 PDT slots remaining — skipping new buys")
                    time.sleep(30)
                    continue
                if bot_state and bot_state.get("buys_disabled"):
                    time.sleep(30)
                    continue

                total_cash     = get_alpaca_cash()
                scanner_budget = shared_state.get_budgets(total_cash)["scanner"]

                if now - _last_cash_log >= CASH_LOG_INTERVAL:
                    log(f"💵 Scanner budget: ${scanner_budget:.2f} | open: {len(scanner_positions)} | PDT slots: {pdt_slots}/3")
                    _last_cash_log = now

                if scanner_budget < MIN_TRADE_AMT:
                    time.sleep(10)
                    continue
                if check_circuit_breaker():
                    time.sleep(10)
                    continue

                for symbol in SCANNER_UNIVERSE:
                    if shared_state.is_on_cooldown(symbol) or shared_state.owner(symbol) is not None:
                        continue
                    signal = scan_for_entry(symbol)
                    if not signal:
                        continue
                    log(f"📡 Signal [{symbol}] vol={signal['vol_ratio']}x | move=+{signal['price_move']}%")

                    # V2.0: Win-rate gate -- skip entries where this exact
                    # historical setup (symbol|vol-ratio bucket|price-move
                    # bucket|SPY trend|hour) has a win rate below
                    # WIN_RATE_GATE_THRESHOLD, once enough samples exist
                    # (>= PM_MIN_BUCKET_TRADES). "No data" never blocks.
                    spy_bullish = get_spy_bullish()
                    hour        = datetime.now(tz=CENTRAL).hour
                    if _scanner_memory:
                        skip, wr, has_data = _scanner_memory.should_skip_entry(
                            symbol, signal["vol_ratio"], signal["price_move"],
                            spy_bullish, hour
                        )
                        if skip:
                            log(f"🚫 WIN-RATE GATE [{symbol}]: historical WR={wr:.0%} "
                                f"< {WIN_RATE_GATE_THRESHOLD:.0%} -- skipping entry")
                            continue
                        if has_data:
                            log(f"✅ Win-rate check passed [{symbol}]: historical WR={wr:.0%}")

                    if not shared_state.claim(symbol, BOT_NAME):
                        continue
                    trade_amount = round(scanner_budget * TRADE_PCT_OF_BUDGET, 2)
                    if trade_amount < MIN_TRADE_AMT:
                        shared_state.release(symbol)
                        continue
                    success = buy(symbol, trade_amount)
                    if success:
                        import secrets as _sec
                        trade_id = _sec.token_hex(8)
                        scanner_positions[symbol] = {
                            "entry_price":  signal["price"],
                            "peak_price":   signal["price"],
                            "trough_price": signal["price"],
                            "entry_time":   datetime.now(tz=CENTRAL),
                            "trade_id":     trade_id,
                            "vol_ratio":    signal["vol_ratio"],
                            "price_move":   signal["price_move"],
                        }
                        if _scanner_memory:
                            _scanner_memory.record_entry(
                                trade_id, symbol, signal["price"],
                                signal["vol_ratio"], signal["price_move"], spy_bullish,
                            )
                        save_scanner_positions()
                        break
                    else:
                        shared_state.release(symbol)

            time.sleep(5)

        except Exception as e:
            log(f"🔴 Scanner loop error: {e}")
            time.sleep(10)
