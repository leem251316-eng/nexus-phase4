"""
NEXUS PHASE 4 — PER-SYMBOL AUTONOMOUS BOTS V1.0
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

from webull.core.client import ApiClient
from webull.trade.trade_client import TradeClient

# ── Env ───────────────────────────────────────────────────────────────────────
APP_KEY          = os.environ.get("WEBULL_APP_KEY")
APP_SECRET       = os.environ.get("WEBULL_APP_SECRET")
ACCOUNT_ID       = os.environ.get("WEBULL_ACCOUNT_ID")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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
WEBULL_CACHE_TTL    = 25
WEBULL_429_BACKOFF  = 30
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

def get_buying_power(acct_id: str) -> float:
    global _balance_cache, _balance_cache_time
    now = time.time()
    if now - _balance_cache_time < WEBULL_CACHE_TTL and _balance_cache:
        return float(_balance_cache.get("buying_power", 0))
    try:
        res = trade_client.account_v2.get_account_balance(acct_id)
        if res.status_code == 200:
            data   = res.json()
            assets = data.get("account_currency_assets", [])
            for asset in assets:
                if asset.get("currency") == "USD":
                    _balance_cache      = asset
                    _balance_cache_time = now
                    return float(asset.get("buying_power", 0))
        elif res.status_code == 429:
            time.sleep(WEBULL_429_BACKOFF)
    except:
        pass
    return float(_balance_cache.get("buying_power", 0)) if _balance_cache else 0.0

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
        self.entry_price: float = 0.0
        self.in_position: bool  = False
        self.active_sym:  str   = symbol  # switches to bear_pair during reversal
        self.mode:        str   = "SCALP"
        self.cooldown_until: float = 0.0

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
                log(self.symbol, f"🔁 REVERSAL CONFIRMED → {self.bear_pair} | drop={round(drop*100,2)}%")
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
            self.in_position  = True
            self.active_sym   = sym
            self.entry_price  = price
            self.peak_price   = price
            invalidate_pos_cache()
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
            emoji = "✅" if pnl_pct > 0 else "🛑"
            pnl_s = f"+{round(pnl_pct*100,3)}%" if pnl_pct > 0 else f"{round(pnl_pct*100,3)}%"
            log(self.symbol, f"{emoji} SELL [{reason}]: {self.active_sym} | P&L: {pnl_s} | mode={self.mode}")
            alert(f"{emoji} PHASE4 [{reason}]: {self.active_sym} | {pnl_s}")
            self.in_position = False
            self.peak_price  = 0.0
            self.entry_price = 0.0
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
                    self.peak_price = max(self.peak_price, price)
                    profit_pct = (price - self.entry_price) / self.entry_price if self.entry_price > 0 else 0
                    drawdown   = (self.peak_price - price) / self.peak_price if self.peak_price > 0 else 0
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
    print("[PHASE4] NEXUS PHASE 4 V1.0 STARTING", flush=True)
    print("[PHASE4] Bots: NUGT(30%) | SOXL(25%) | LABU(25%) | TQQQ(20%)", flush=True)
    print("[PHASE4] Bear pairs: DUST | SOXS | LABD | SQQQ", flush=True)

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
    alert("⚡ PHASE4 V1.0 ONLINE | NUGT+SOXL+LABU+TQQQ | Bear pairs active")
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
