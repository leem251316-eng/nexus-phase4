"""
NEXUS PHASE 4 — PER-SYMBOL AUTONOMOUS BOTS V2.18

V2.18 — Regime shadow (Aug 2 2026, pre-registered):
  B1+B3 joint rule observed, never enforced: every real entry signal and
  every EOD overnight hold logs regime (ETF-MA20), the rule's action
  (ALLOW / CARRY_ONLY / WOULD_CLOSE / WOULD_CARRY), and price to
  phase4_shadow_signals via dedicated autocommit connections. Zero order
  paths read shadow state. Evaluation at 30 sessions per
  PHASE4_REGIME_SHADOW_PREREG.md; counterfactuals computed from bars at
  verdict time.
V2.17 — Phase B dashboard plumbing (Jul 22 2026):
  alert() dual-writes to nexus_events; 60s heartbeat publishes per-bot
  state to service_status. Fire-and-forget, no bot logic changed.

V2.16 — Manual discretionary buy (Jul 15 2026):
  BotState.manual_open(sym, usd): /buyphase4 from T-Bone. Bypasses entry
  scoring ON PURPOSE; position is fully bot-managed (stops, ratchets,
  MFE/MAE, external-close detection, fingerprints). mode set to SCALP
  for exits; pattern memory records mode='MANUAL' for bucket isolation.
  One position per bot, bull or bear leg, unchanged.

V2.15 — External-close detection + fill-anchored exits (Jul 8 2026):
  - place_sell_all now distinguishes 'gone' (account doesn't hold the
    symbol -- closed by /close, manually, or a broker sweep) from a
    transient failure. try_sell clears the bot's state on 'gone' and
    records a '{reason}-external' exit fingerprint instead of retrying a
    dead sell every 12s until redeploy (only /close_all had the phantom-
    state clear before; the main loop was blind to it).
  - Exits are FILL-ANCHORED: try_sell polls the close order's actual
    average fill price and records that PnL. Entries got this in V2.8;
    exits still booked the last bar price -- on 3x ETFs the bar-vs-fill
    gap was flowing into fingerprints, Win Follower weights, and daily
    tallies. Mirrors Berserker V10.37.
  - _daily_limit_hit now actually tracked (P4_DAILY_LOSS_LIMIT_PCT env,
    default 3.0 pct pts) -- /think reported the field since V2.0 but
    nothing ever set it. Still NOT enforced; arming is a separate call.

V2.14 — Reversal drop-confirm before RSI reset + loud gate rejections (Jul 7 2026):
  ✅ WATCHING checked "RSI < 60 -> reset" BEFORE the >=0.5% drop-confirm.
     Violent flushes take RSI below 60 on the same bars that produce the
     drop, so fast cracks reset the watch instead of firing it (Jul 7:
     SOXL armed 6 watches at the top over 2h, RSI 84->24 flush fired
     nothing). Drop-confirm now runs first; RSI collapsing WITH the crack
     is confirmation. Slow-grind resets unchanged. Same wrong-order family
     as V2.1's exit-priority fix.
  ✅ Every reversal gate rejection now logs its reason (🚫 REVERSAL
     REJECTED | gate=...) -- all paths were silent, making misses
     undiagnosable without log forensics.

V2.13 — DUST/SOXS bear gates were mathematically unreachable (Jul 6 2026):
  ✅ DUST min_score 7 with a max achievable score of 4; SOXS min_score 9
     with a max of 6. Neither bear pair has EVER been able to enter --
     with SQQQ flag-disabled, 3 of 4 reversal pairs were dead and every
     NUGT/SOXL reversal watch was decorative. Clamped to the maximum
     achievable score (ALL best signals required = perfect-setup-only,
     the most selective reachable gate). The backtester runs both at 3;
     re-derive from evidence on the next backtest pass.

V2.12 — TTL fetch cache: stop burning the shared Alpaca budget (Jul 6 2026):
  ✅ V2.10's loud logging exposed Alpaca 429s. One API key serves ALL
     NEXUS services (~200 req/min) -- Berserker's exit-check sweeps compete
     with Phase4's fetches. Cache: 1m bars 10s TTL (dedupes bot vs context
     thread), 5m bars 240s TTL (they can't change for 5 min anyway).
     Cuts Phase4's API call volume ~60-70%. Non-empty results only.

V2.11 — Fallback threshold 21 -> 35 (Jul 6 2026):
  ✅ NUGT trapped at exactly 21 1m bars: cleared the 5m-fallback check but
     failed warmup's 40. Also, 21-34 sparse IEX bars compute StochRSI/ADX
     as defaults (both need 33) with gap-distorted values. Now: < 35 real
     1m bars -> 5m stopgap; >= 35 -> full 1m resolution. Auto-revert logic
     unchanged.

V2.10 — 5-min fetch was BROKEN since V2.0 (Jul 6 2026):
  ✅ TimeFrame(5, "Min") raises AttributeError inside alpaca-py -- it needs
     TimeFrameUnit.Minute, not a raw string. The bare except swallowed it,
     so every 5m Alpaca fetch fell back to rate-limited yfinance and
     usually returned NOTHING. Consequences, live since the Alpaca
     migration: _underlying_5m context was empty (trending_up_5m, at_high,
     and the 5m half of tide logic ran on defaults), and V2.9's 5m warmup
     stopgap never engaged (NUGT/LABU still stuck at 18/40 this morning
     while Berserker fired). Constructor fixed; Alpaca fetch failures now
     log once per symbol/interval instead of dying silently.

V2.9 — Best-available data: 5-min stopgap for thin 1-min tape (Jul 6 2026):
  ✅ refresh_prices() and the SPY/QQQ context fetch now fall back to 5-min
     bars whenever the 1-min IEX tape is too thin to compute signals
     (< 21 bars). Jul 6 boot evidence: NUGT and LABU sat at 12-14/40 bars
     for 6+ minutes -- blind through exactly the oversold-open window this
     strategy is built for. The 5m stopgap is NOT a mode: every refresh
     tries 1m first and reverts automatically the moment 1m depth is
     adequate. Transitions are logged (📶). Price-level exits
     (stops/trails/ratchets) are unaffected -- only indicator cadence is
     coarser while the stopgap is active.

V2.8 — Fill-anchored entries + true recovery times (Jul 6 2026):
  ✅ Entry price now anchored to the ACTUAL Alpaca fill (brief poll after
     submit), not the last 1-min bar. On 3x leveraged ETFs, intrabar moves
     of 0.2-0.5% are routine -- stops, ratchets, MFE/MAE, and fingerprints
     all key off entry_price, so the bar anchor was shifting every exit
     level. Same fix class as Berserker V10.35. place_order() now returns
     the order object (drop-in: callers only tested truthiness).
  ✅ recover_position() recovers the real entry time from the last filled
     BUY order -- the dwell-exit timer and hold stats were restarting on
     every redeploy.

V2.7 — Killswitch integration (Jul 4 2026):
  - New per-bot kill_paused flag: blocks NEW entries (try_buy guard) while
    exit management keeps running. Set/cleared via phase4_server V2.1's
    new POST /close_all and POST /resume endpoints.
  - main.py V10.35 /killswitch calls /close_all (set PHASE4_URL on the
    Fleet Commander service to the Railway internal URL to arm it), and
    /resume clears the pause. Closes the "Phase4: manual close required"
    gap. /close_all closes ONLY the bots' tracked positions — Phase4
    shares the Alpaca account with Berserker, so a blanket account close
    from here would clobber Berserker's stocks.
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

V2.2 — Cross-service capital coordination (Jun 30 2026):
  Confirmed: Phase4 and Berserker (main.py, separate Railway service/process,
  nexus-commander/rare-perception) trade against the SAME live Alpaca account
  -- one account was ever created; paper trading was layered on top of it,
  never a second account. Each service was independently calling Alpaca's
  buying_power and sizing trades with zero knowledge of the other's
  outstanding orders. Added capital_coordinator.py, coordinating through
  Postgres (both services already share DATABASE_URL). try_buy() now clamps
  trade_size against get_available() before sizing, reserves immediately
  before order submission, releases in a finally block immediately after --
  whether the order succeeds or fails. Fails open throughout: any DB/
  connection problem falls back to raw buying_power, uncoordinated, exactly
  the pre-V2.2 behavior, rather than blocking a trade. Identical fix applied
  to main.py (V10.25).

V2.1 — Exit priority fix (Jun 30 2026):
  rsi-overbought was checked BEFORE the profit ratchet. On a mean-reversion
  entry (buy oversold, RSI climbs toward overbought as the thesis plays out),
  that ordering let rsi-overbought cut almost every winner the instant profit
  ticked positive -- before the position ever reached early_ratchet.
  Backtest evidence (phase4_backtester.py V1.2, 2yr replay): 298/321 wins
  (93%) were rsi-overbought exits averaging ~+0.24%, while the 61 stop-losses
  averaged ~-1.7% (atr_stop) -- a ~7:1 win/loss size mismatch that made a
  73% WR system net PnL-negative (avg -0.031%/trade) in both training and
  out-of-sample. Ratchet is now checked first; rsi-overbought is the fallback
  for trades that hit RSI=70 without yet clearing early_ratchet -- i.e. take
  the small win/scratch now rather than risk it reversing to red. No change
  to stop-loss priority (still checked first, unconditionally) or to any
  entry-side logic, sizing, or gating.

V2.0 — Full Alpaca migration (Jun 29 2026):
  Webull broker layer completely removed.
  Alpaca TradingClient handles all orders, positions, buying power.
  Alpaca StockHistoricalDataClient replaces yfinance for all price bars.
  yfinance kept as fallback only for ^VIX (not available on Alpaca IEX).
  acct_id param removed throughout — Alpaca uses API key auth, not account IDs.
  Fractional shares supported via Alpaca notional orders.

V1.9 — Alpaca data feed replaces yfinance for price bars.
V1.8 — Score + StochRSI fixes.
V1.7 — RSI Wilder EWM fix.
V1.6 — Complete entry/exit overhaul.
"""

import os
import time
import json
import threading
import traceback
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from capital_coordinator import CapitalCoordinator, NO_COORDINATION

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce
    _alpaca_trade_ok = True
except ImportError:
    _alpaca_trade_ok = False

try:
    from alpaca.data import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    _alpaca_data_ok = True
except ImportError:
    _alpaca_data_ok = False

try:
    import psycopg2
    import psycopg2.extras
    _db_available = True
except ImportError:
    _db_available = False

# ── Env ───────────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")

# V2.7: killswitch pause lives as a PER-BOT attribute (bot.kill_paused),
# set/cleared by phase4_server V2.1's /close_all and /resume. Not a module
# global: this file runs as __main__ while phase4_server does `import
# phase4`, which creates a second module instance -- a global set there
# would be invisible to the bots. Bot objects are shared by reference.
ANALYST_URL       = os.environ.get("ANALYST_URL", "").rstrip("/")
NEXUS_TOKEN       = os.environ.get("NEXUS_INTERNAL_TOKEN", "")
SQQQ_ENABLED      = os.environ.get("PHASE4_SQQQ_ENABLED", "false").lower() == "true"
ALPACA_API_KEY    = os.environ.get("APCA_API_KEY_ID", "")
ALPACA_API_SECRET = os.environ.get("APCA_API_SECRET_KEY", "")
PAPER_MODE        = os.environ.get("APCA_PAPER", "false").lower() == "true"

CENTRAL  = ZoneInfo("America/Chicago")
BOT_NAME = "PHASE4"

# ── Alpaca clients ────────────────────────────────────────────────────────────
_trade_client = None
_data_client  = None

if _alpaca_trade_ok and ALPACA_API_KEY and ALPACA_API_SECRET:
    try:
        _trade_client = TradingClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_API_SECRET,
            paper=PAPER_MODE,
        )
        print(f"[PHASE4] Alpaca trading client ready (paper={PAPER_MODE})", flush=True)
    except Exception as _e:
        print(f"[PHASE4] ⚠ Alpaca trading client error: {_e}", flush=True)

if _alpaca_data_ok and ALPACA_API_KEY and ALPACA_API_SECRET:
    try:
        _data_client = StockHistoricalDataClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_API_SECRET,
        )
    except Exception as _e:
        print(f"[PHASE4] ⚠ Alpaca data client error: {_e}", flush=True)

# ── Per-symbol config ─────────────────────────────────────────────────────────
BOT_CONFIGS = {
    "NUGT": {
        "bear_pair":      "DUST",
        "underlying":     "GDX",
        "budget_pct":     0.30,
        "min_score":      5,
        "atr_stop":       0.0213,
        "early_ratchet":  0.0059,
        "late_ratchet":   0.0111,
        "trail_normal":   0.0039,
        "trail_tight":    0.0027,
        "avoid_hours":    [9],
        "avoid_days":     [],
        "best_signals":   ["rsi_lt40", "bb_squeeze", "below_ma20", "rsi14_lt35"],
        "worst_signals":  ["near_lower_bb", "ema9_above_ema21"],
        "ride_stop_mult": 1.3,
        "ext_stop_mult":  1.8,
    },
    "SOXL": {
        "bear_pair":      "SOXS",
        "underlying":     "SMH",
        "budget_pct":     0.25,
        "min_score":      6,
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
        "min_score":      6,
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
        "min_score":      4,
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

BEAR_RECIPES = {
    "DUST": {
        "underlying":     "GDX",
        # V2.13: was 7 -- MATHEMATICALLY UNREACHABLE. Max achievable score
        # = 4 best_signals, no bonus signals in list. DUST could never
        # fire; every NUGT reversal watch was theater. 4 = ALL signals
        # required (perfect-setup-only), the most selective reachable gate.
        # Backtester runs this at 3; re-derive from evidence when re-run.
        "min_score":      4,
        "atr_stop":       0.0180,
        "early_ratchet":  0.0134,
        "trail":          0.0033,
        "avoid_hours":    [11],
        "best_signals":   ["obv_rising", "below_ma20", "macd_bullish", "far_below_bb"],
        "worst_signals":  ["obv_falling", "near_lower_bb"],
    },
    "SOXS": {
        "underlying":     "SMH",
        # V2.13: was 9 -- MATHEMATICALLY UNREACHABLE. Max achievable score
        # = 5 best_signals + rsi_lt25 bonus = 6. Same fix as DUST:
        # 6 = ALL signals + bonus required. Backtester uses 3.
        "min_score":      6,
        "atr_stop":       0.0177,
        "early_ratchet":  0.0120,
        "trail":          0.0032,
        "avoid_hours":    [9],
        "best_signals":   ["below_ma20", "rsi_lt25", "rsi14_lt20", "stochrsi_oversold", "near_lower_bb"],
        "worst_signals":  ["obv_rising", "macd_bullish"],
    },
    "LABD": {
        "underlying":     "XBI",
        "min_score":      4,
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

BEAR_EXTENDED_TP = {
    "DUST": {"trail_activate": 0.020, "trail_stop": 0.010},
    "SOXS": {"trail_activate": 0.020, "trail_stop": 0.010},
}

SIGNAL_COMBO_BOOST_SYMBOLS = {
    "SOXL": [("bb_squeeze", "stochrsi_oversold")],
    "LABU": [("rsi14_lt20", "rsi_lt25")],
    "TQQQ": [("bouncing", "obv_falling")],
    "NUGT": [("bb_squeeze", "macd_bullish")],
    "DUST": [("far_below_bb", "stochrsi_oversold")],
    "SOXS": [("below_ma20", "rsi_lt25")],
    "LABD": [("ema9_above_ema21", "stochrsi_oversold")],
}

VIX_CAUTION  = 28.0
VIX_PAUSE    = 35.0

REVERSAL_HIGH_RSI    = 75
REVERSAL_HIGH_DROP   = 0.008
REVERSAL_OB_RSI      = 70
REVERSAL_RSI_RESET   = 60
REVERSAL_CONFIRM     = 0.005
REVERSAL_MAX_WATCH   = 1800

DWELL_MINUTES        = 30
DWELL_FLAT_THRESHOLD = 0.001
RSI_OVERBOUGHT_EXIT  = 70
QQQ_BEAR_RSI_GATE      = 58
QQQ_BEAR_RSI_GATE_LABD = 65

PM_MIN_TRADES           = 15
PM_ANALYSIS_INTERVAL    = 86400
PM_MIN_BUCKET_TRADES    = 3
WIN_RATE_GATE_THRESHOLD = 0.35

BUYING_POWER_BUFFER  = 1.05
WIN_COOLDOWN_SECS    = 180
LOSS_COOLDOWN_SECS   = 900
LOOP_INTERVAL        = 12

# V2.15: daily loss limit (percent points of daily_pnl) -- TRACKED ONLY,
# not enforced. /think surfaces _daily_limit_hit; arming entry-blocking on
# it is a separate, deliberate decision.
DAILY_LOSS_LIMIT_PCT = float(os.environ.get("P4_DAILY_LOSS_LIMIT_PCT", "3.0"))
WARMUP_BARS          = 40

# ── Shared context data ───────────────────────────────────────────────────────
_spy_prices:        list  = []
_qqq_prices:        list  = []
_vix_price:         float = 15.0
_underlying_prices: dict  = {}
_underlying_5m:     dict  = {}
_context_lock               = threading.Lock()
_analyst_scores_cache: dict  = {}
_analyst_scores_ts:    float = 0.0
_analyst_scores_ttl:   float = 20.0
_analyst_lock                = threading.Lock()
_alpaca_fetch_warned: set    = set()   # V2.10: one-time Alpaca fetch failure log keys
_phase4_memory = None
_capital_coordinator = None  # CapitalCoordinator -- initialized in run(), see capital_coordinator.py

# ==============================================================================
# V2.4: PHASE4 WIN FOLLOWER -- performance-weighted budget reallocation.
# Same follow-the-wins model as Berserker V10.29 / Scanner V2.8 / crypto V5.3,
# adapted for Phase4's structure: instead of benching (each bot already has a
# daily loss limit), the FIXED budget split (30/25/25/20) becomes DYNAMIC.
# Every hour, each bot's rolling 14-day WR (bull + bear pair combined, from
# phase4_trade_fingerprints) shifts its budget share up to +/-8pts, clamped
# to [10%, 40%] and renormalized to 100%. Winners get more capital, losers
# get less -- but every bot keeps >= 10% so it never stops generating the
# data needed to earn its way back up. T-Bone alert whenever any bot's
# weight moves >= 2pts from the last alerted state.
# ==============================================================================
WF_LOOKBACK_DAYS  = 14
WF_REFRESH_SECS   = 3600
WF_MIN_TRADES     = 5        # rolling trades needed before a bot's WR moves its weight
WF_MAX_SHIFT      = 0.08     # max budget shift up or down from base
WF_WEIGHT_FLOOR   = 0.10     # no bot ever below 10% -- keeps data flowing
WF_WEIGHT_CEIL    = 0.40
WF_ALERT_DELTA    = 0.02     # alert when any weight moves >= 2pts

_win_follower = None   # Phase4WinFollower -- initialized in run()


class Phase4WinFollower:
    """V2.4: follow-the-wins budget allocator across the 4 bots."""

    def __init__(self, db_url: str):
        self.db_url        = db_url
        self._conn         = None
        self._lock         = threading.Lock()
        self._enabled      = bool(db_url) and _db_available
        self._weights      = {b: c["budget_pct"] for b, c in BOT_CONFIGS.items()}
        self._stats        = {}
        self._last_alerted = dict(self._weights)
        self._last_refresh = 0.0
        # bear ETF -> owning bot (DUST->NUGT etc.) so both sides of a bot's
        # trades count toward the same performance record
        self._sym_to_bot = {}
        for bot, cfg in BOT_CONFIGS.items():
            self._sym_to_bot[bot] = bot
            self._sym_to_bot[cfg["bear_pair"]] = bot

    def _get_conn(self):
        if not self._enabled:
            return None
        try:
            if self._conn is None or self._conn.closed:
                self._conn = psycopg2.connect(self.db_url, connect_timeout=5)
                self._conn.autocommit = False
            else:
                try:
                    self._conn.rollback()
                except Exception:
                    self._conn = psycopg2.connect(self.db_url, connect_timeout=5)
                    self._conn.autocommit = False
            return self._conn
        except Exception as e:
            log("WF", f"DB connect error: {e}")
            return None

    def refresh(self):
        if not self._enabled:
            return
        cutoff = int(time.time()) - WF_LOOKBACK_DAYS * 86400
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT symbol, won, pnl_pct
                        FROM phase4_trade_fingerprints
                        WHERE won IS NOT NULL AND exit_ts >= %s
                          AND trade_id NOT LIKE 'bt_%%'
                    """, (cutoff,))
                    rows = cur.fetchall()
                conn.commit()
        except Exception as e:
            log("WF", f"refresh query: {e}")
            return

        per_bot = {b: [] for b in BOT_CONFIGS}
        for symbol, won, pnl in rows:
            bot = self._sym_to_bot.get(symbol)
            if bot:
                per_bot[bot].append((bool(won), float(pnl or 0)))

        raw = {}
        stats = {}
        for bot, cfg in BOT_CONFIGS.items():
            trades = per_bot[bot]
            n   = len(trades)
            wr  = (sum(1 for w, _ in trades if w) / n) if n else 0.0
            pnl = sum(p for _, p in trades)
            stats[bot] = {"trades": n, "wr": round(wr, 3),
                          "pnl_sum": round(pnl, 2)}   # V2.6: DB stores percent already
            base = cfg["budget_pct"]
            if n >= WF_MIN_TRADES:
                # WR 70% -> +8pts, WR 30% -> -8pts, linear between, clamped
                shift = max(-WF_MAX_SHIFT, min(WF_MAX_SHIFT, (wr - 0.50) * 0.4))
            else:
                shift = 0.0   # not enough data -> base weight
            raw[bot] = max(WF_WEIGHT_FLOOR, min(WF_WEIGHT_CEIL, base + shift))

        total = sum(raw.values())
        new_weights = {b: round(w / total, 4) for b, w in raw.items()}

        changed = any(abs(new_weights[b] - self._last_alerted.get(b, 0)) >= WF_ALERT_DELTA
                      for b in new_weights)
        self._weights      = new_weights
        self._stats        = stats
        self._last_refresh = time.time()

        wtxt = " | ".join(f"{b} {new_weights[b]*100:.0f}%" for b in BOT_CONFIGS)
        log("WF", f"weights: {wtxt}")
        if changed:
            self._last_alerted = dict(new_weights)
            lines = ["⚖️ WIN FOLLOWER [PHASE4] -- budgets reweighted"]
            for b in BOT_CONFIGS:
                s    = stats[b]
                base = BOT_CONFIGS[b]["budget_pct"]
                d    = (new_weights[b] - base) * 100
                lines.append(f"{b}: {new_weights[b]*100:.0f}% ({d:+.0f} vs base) — "
                             f"WR {s['wr']:.0%}/{s['trades']}t {s['pnl_sum']:+.1f}%")
            lines.append(f"({WF_LOOKBACK_DAYS}d rolling | floor {WF_WEIGHT_FLOOR:.0%} "
                         f"keeps every bot alive)")
            alert("\n".join(lines))

    def get_weight(self, bot_symbol: str, fallback: float) -> float:
        """Budget share for this bot. Falls back to the static budget_pct
        if disabled or not yet refreshed -- exact pre-V2.4 behavior."""
        if not self._enabled or not self._last_refresh:
            return fallback
        return self._weights.get(bot_symbol, fallback)

    def get_status(self) -> dict:
        return {"weights": self._weights, "stats": self._stats,
                "base": {b: c["budget_pct"] for b, c in BOT_CONFIGS.items()},
                "last_refresh_min_ago": (round((time.time() - self._last_refresh) / 60, 1)
                                         if self._last_refresh else None)}

    def start_scheduler(self):
        if not self._enabled:
            log("WF", "disabled (no DATABASE_URL) -- static budget split")
            return
        def _run():
            time.sleep(120)
            while True:
                try:
                    self.refresh()
                except Exception as e:
                    log("WF", f"loop: {e}")
                time.sleep(WF_REFRESH_SECS)
        threading.Thread(target=_run, daemon=True, name="phase4-wf").start()
        log("WF", f"scheduler started (refresh {WF_REFRESH_SECS//60}m, "
                  f"lookback {WF_LOOKBACK_DAYS}d, shift ±{WF_MAX_SHIFT:.0%})")

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(symbol, msg):
    ts = datetime.now(tz=CENTRAL).strftime("%H:%M:%S")
    print(f"[{symbol} | {ts}] {msg}", flush=True)

# =============================================================================
# V2.17 PHASE B: dashboard event feed + service heartbeat (mirrors main V10.56)
# Dedicated short-lived autocommit connections, fire-and-forget. A DB hiccup
# can never touch the bots.
# =============================================================================
_phaseb_ddl_done = False

def _phaseb_write(sql, params=()):
    global _phaseb_ddl_done
    try:
        import psycopg2 as _pg
        _c = _pg.connect(os.environ["DATABASE_URL"], connect_timeout=5)
        _c.autocommit = True
        with _c.cursor() as _cur:
            if not _phaseb_ddl_done:
                _cur.execute("""
                    CREATE TABLE IF NOT EXISTS nexus_events (
                        id BIGSERIAL PRIMARY KEY, ts BIGINT NOT NULL,
                        service VARCHAR(16) NOT NULL,
                        severity VARCHAR(8) DEFAULT 'info', text TEXT NOT NULL);
                """)
                _cur.execute("""
                    CREATE TABLE IF NOT EXISTS service_status (
                        service VARCHAR(16) PRIMARY KEY, ts BIGINT NOT NULL,
                        state JSONB NOT NULL);
                """)
                _phaseb_ddl_done = True
            _cur.execute(sql, params)
        _c.close()
    except Exception:
        try:
            _c.close()
        except Exception:
            pass

def _phaseb_event(text, severity="info"):
    _phaseb_write(
        "INSERT INTO nexus_events (ts, service, severity, text) VALUES (%s,%s,%s,%s)",
        (int(time.time()), "phase4", severity, str(text)[:2000]))

# =============================================================================
# V2.18 REGIME SHADOW (B1+B3 joint rule, pre-registered Aug 2 2026)
# OBSERVATION ONLY — no order path reads any of this. Logs what the joint
# rule (R1: trend entries ok / no overnight; R2: chop entries only as
# designated carries) WOULD do at each real entry signal and each EOD hold.
# Counterfactual P&L is computed at verdict time by the evaluation script
# from bar data — nothing here updates rows later. Same dedicated
# short-lived autocommit pattern as Phase B: a DB hiccup never touches bots.
# =============================================================================
_shadow_ddl_done = False

def _shadow_event(kind, symbol, active_sym, regime, rule_action, actual_action, price, note=""):
    global _shadow_ddl_done
    try:
        import psycopg2 as _pg
        _c = _pg.connect(os.environ["DATABASE_URL"], connect_timeout=5)
        _c.autocommit = True
        with _c.cursor() as _cur:
            if not _shadow_ddl_done:
                _cur.execute("""
                    CREATE TABLE IF NOT EXISTS phase4_shadow_signals (
                        id BIGSERIAL PRIMARY KEY, ts BIGINT NOT NULL,
                        kind VARCHAR(16) NOT NULL,
                        symbol VARCHAR(8) NOT NULL, active_sym VARCHAR(8),
                        regime VARCHAR(8), rule_action VARCHAR(16),
                        actual_action VARCHAR(16), price REAL, note TEXT);
                """)
                _shadow_ddl_done = True
            _cur.execute(
                "INSERT INTO phase4_shadow_signals "
                "(ts, kind, symbol, active_sym, regime, rule_action, actual_action, price, note) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (int(time.time()), kind, symbol, active_sym, regime,
                 rule_action, actual_action, round(float(price), 4), str(note)[:500]))
        _c.close()
    except Exception:
        try:
            _c.close()
        except Exception:
            pass

def _phaseb_heartbeat_start(bot_list):
    """60s heartbeat: per-bot state for the dashboard."""
    def _hb():
        while True:
            try:
                st = {"version": "V2.18", "bots": {}}
                for b in bot_list:
                    try:
                        st["bots"][b.symbol] = {
                            "in_position": bool(b.in_position),
                            "active_sym":  b.active_sym if b.in_position else None,
                            "mode":        getattr(b, "mode", None) if b.in_position else None,
                            "entry_price": getattr(b, "entry_price", None) if b.in_position else None,
                        }
                    except Exception:
                        pass
                _phaseb_write(
                    "INSERT INTO service_status (service, ts, state) VALUES (%s,%s,%s) "
                    "ON CONFLICT (service) DO UPDATE SET ts=EXCLUDED.ts, state=EXCLUDED.state",
                    ("phase4", int(time.time()), json.dumps(st, default=str)))
            except Exception:
                pass
            time.sleep(60)
    threading.Thread(target=_hb, daemon=True, name="phaseb-heartbeat").start()

def alert(msg):
    try:
        _phaseb_event(msg)
    except Exception:
        pass
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
    except Exception:
        pass

def is_market_hours() -> bool:
    now = datetime.now(tz=CENTRAL)
    return now.weekday() < 5 and 8 <= now.hour < 15

# ── Alpaca broker functions ───────────────────────────────────────────────────
def get_buying_power() -> float:
    """Get available buying power from Alpaca account."""
    if _trade_client is None:
        return 0.0
    try:
        acct = _trade_client.get_account()
        return float(acct.buying_power)
    except Exception as e:
        print(f"[P4 BROKER] buying_power error: {e}", flush=True)
        return 0.0

def get_all_positions() -> dict:
    """Returns {symbol: position_object} for all open positions."""
    if _trade_client is None:
        return {}
    try:
        positions = _trade_client.get_all_positions()
        return {p.symbol: p for p in positions}
    except Exception as e:
        print(f"[P4 BROKER] get_positions error: {e}", flush=True)
        return {}

def place_order(symbol: str, side: str, notional: float):
    """
    Place a notional market order via Alpaca.
    V2.0: Uses notional (dollar amount) instead of qty — supports fractional shares.
    V2.8: Returns the submitted order object (truthy) so callers can poll
    the actual fill price, or None on failure. All existing callers only
    tested truthiness, so this is drop-in.
    side: 'BUY' or 'SELL'
    """
    if _trade_client is None:
        print(f"[P4 BROKER] No trade client — order skipped {symbol} {side}", flush=True)
        return None
    try:
        req = MarketOrderRequest(
            symbol=symbol,
            notional=round(notional, 2),
            side=OrderSide.BUY if side == "BUY" else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        return _trade_client.submit_order(req)
    except Exception as e:
        print(f"[P4 BROKER] order error {symbol} {side} ${notional}: {e}", flush=True)
        return None


def get_fill_price(order, retries: int = 3, delay: float = 1.0) -> float:
    """
    V2.8: Poll a submitted order briefly for its actual average fill price.
    Returns 0.0 if unavailable (caller falls back to the bar price).
    Mirrors Berserker's V10.35 fill-anchor fix -- on 3x leveraged ETFs,
    intrabar moves of 0.2-0.5% are routine, so anchoring stops/ratchets/
    fingerprints to the last 1-min bar instead of the fill materially
    shifts every exit level.
    """
    try:
        oid = str(getattr(order, "id", "") or "")
        if not oid or _trade_client is None:
            return 0.0
        for _ in range(retries):
            o = _trade_client.get_order_by_id(oid)
            fap = getattr(o, "filled_avg_price", None) if o is not None else None
            if fap:
                return float(fap)
            time.sleep(delay)
    except Exception as e:
        print(f"[P4 BROKER] fill poll failed: {e}", flush=True)
    return 0.0

def place_sell_all(symbol: str) -> tuple:
    """
    Close entire position in symbol via Alpaca.
    V2.15: returns (status, order) where status is:
      'ok'   -- close submitted; order is the Alpaca order object (poll it
                with get_fill_price for the actual exit fill)
      'gone' -- the account doesn't hold this symbol (closed externally:
                /close from Fleet Commander, manual, broker sweep). Caller
                must clear its state instead of retrying forever -- the old
                bool return made an externally-closed position a permanent
                12s retry loop until redeploy (same blind spot Scanner
                fixed in V2.13; only /close_all had the phantom-state
                clear before this).
      'fail' -- transient error; caller keeps the position and retries.
    """
    if _trade_client is None:
        return ("fail", None)
    try:
        order = _trade_client.close_position(symbol)
        return ("ok", order)
    except Exception as e:
        msg = str(e).lower()
        if "does not exist" in msg or "position not found" in msg or "404" in msg:
            print(f"[P4 BROKER] {symbol} not held (closed externally) -- clearing state", flush=True)
            return ("gone", None)
        print(f"[P4 BROKER] close_position error {symbol}: {e}", flush=True)
        return ("fail", None)

# ── Price data (Alpaca + yfinance fallback) ───────────────────────────────────
# V2.12: TTL fetch cache. The V2.10 loud logging exposed Alpaca 429s
# ("too many requests") -- every NEXUS service shares one API key and its
# ~200 req/min budget, and Berserker's price sweeps (exit checks!) compete
# with Phase4's fetches. Two kinds of waste this kills: (1) 5m bars can't
# change for 5 minutes but were re-fetched every 12s loop; (2) the same
# symbol (e.g. SMH) is fetched by both a bot thread and the context
# thread within seconds. Non-empty results only; thread-safe.
_fetch_cache: dict = {}
_fetch_cache_lock  = threading.Lock()
_FETCH_TTL         = {"1m": 10, "5m": 240}   # seconds

def fetch_prices_and_volumes(symbol: str, bars: int = 40, interval: str = "1m") -> tuple:
    alpaca_sym = symbol.replace("^", "")

    # V2.12: serve from cache when fresh
    _ck  = (symbol, interval, bars)
    _now = time.time()
    with _fetch_cache_lock:
        _hit = _fetch_cache.get(_ck)
        if _hit and _now - _hit[0] < _FETCH_TTL.get(interval, 10):
            return list(_hit[1]), list(_hit[2])

    if _data_client is not None and not symbol.startswith("^"):
        try:
            # V2.10: TimeFrame(5, "Min") raised AttributeError inside alpaca-py
            # (needs the TimeFrameUnit enum, not a raw string) -- swallowed by
            # this except since the V2.0 migration, so EVERY 5m Alpaca fetch
            # silently fell to rate-limited yfinance and usually returned
            # nothing: _underlying_5m context ran on defaults, and V2.9's
            # 5m warmup stopgap never actually engaged.
            tf       = TimeFrame.Minute if interval == "1m" else TimeFrame(5, TimeFrameUnit.Minute)
            lookback = timedelta(days=2) if interval == "1m" else timedelta(days=7)
            req      = StockBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=tf,
                start=datetime.now(timezone.utc) - lookback,
                feed="iex",
            )
            df = _data_client.get_stock_bars(req).df
            if df is not None and not df.empty:
                if isinstance(df.index, pd.MultiIndex):
                    df = df.xs(alpaca_sym, level="symbol")
                prices  = df["close"].tail(bars).tolist()
                volumes = df["volume"].tail(bars).tolist()
                if prices:
                    with _fetch_cache_lock:
                        _fetch_cache[_ck] = (_now, list(prices), list(volumes))
                    return prices, volumes
        except Exception as _fe:
            # V2.10: this except hid the broken 5m constructor for 6 days of
            # live trading. Log once per (symbol, interval) so Alpaca-path
            # failures are never invisible again.
            _k = f"{symbol}:{interval}"
            if _k not in _alpaca_fetch_warned:
                _alpaca_fetch_warned.add(_k)
                print(f"[P4 DATA] \u26a0 Alpaca {interval} fetch failed for {symbol}: "
                      f"{type(_fe).__name__}: {_fe} -- falling back to yfinance", flush=True)

    # Fallback: yfinance (used for ^VIX and when Alpaca fails)
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        period = "1d" if interval == "1m" else "5d"
        df     = ticker.history(period=period, interval=interval)
        if not df.empty:
            _p = df["Close"].tail(bars).tolist()
            _v = df["Volume"].tail(bars).tolist()
            with _fetch_cache_lock:
                _fetch_cache[_ck] = (_now, list(_p), list(_v))
            return _p, _v
    except Exception:
        pass
    return [], []

def fetch_prices(symbol: str, bars: int = 40) -> list:
    p, _ = fetch_prices_and_volumes(symbol, bars)
    return p


# V2.9: BEST-AVAILABLE DATA -- 1-min bars first; if the 1m tape is too thin
# (< MIN_LIVE_BARS -- routine on NUGT/LABU on IEX right after boot or the
# open), fall back to 5-min bars so the bot can trade instead of sitting
# blind through exactly the oversold-open window it's built for. This is a
# STOPGAP, not a mode: every refresh tries 1m first, so the bot reverts to
# full 1-min resolution automatically the moment 1m depth reaches
# MIN_LIVE_BARS. Price-level exits (stops/trails/ratchets) are unaffected;
# only indicator cadence is coarser while the stopgap is active.
# V2.11: raised 21 -> 35. NUGT sat at exactly 21 1-min bars: enough to
# skip the 5m fallback, not enough to pass warmup (40) -- trapped between
# thresholds. And 21 sparse IEX bars on a thin ticker are WORSE data than
# 45 clean 5m bars: StochRSI and ADX need 33 bars (returning defaults
# below that), and sparse 1m bars have missing minutes that distort every
# indicator. 35 = full indicator suite on real 1m data, or 5m stopgap.
MIN_LIVE_BARS = 35

def fetch_prices_with_fallback(symbol: str, bars: int) -> tuple:
    """Returns (prices, volumes, timeframe_label). 1m preferred, 5m stopgap."""
    p, v = fetch_prices_and_volumes(symbol, bars, "1m")
    if len(p) >= MIN_LIVE_BARS:
        return p, v, "1m"
    p5, v5 = fetch_prices_and_volumes(symbol, bars, "5m")
    if len(p5) > len(p):
        return p5, v5, "5m"
    return p, v, "1m"

def get_current_price(symbol: str) -> float | None:
    alpaca_sym = symbol.replace("^", "")
    if _data_client is not None and not symbol.startswith("^"):
        try:
            req = StockBarsRequest(
                symbol_or_symbols=alpaca_sym,
                timeframe=TimeFrame.Minute,
                start=datetime.now(timezone.utc) - timedelta(minutes=5),
                feed="iex",
            )
            df = _data_client.get_stock_bars(req).df
            if df is not None and not df.empty:
                if isinstance(df.index, pd.MultiIndex):
                    df = df.xs(alpaca_sym, level="symbol")
                if not df.empty:
                    return float(df["close"].iloc[-1])
        except Exception:
            pass
    try:
        import yfinance as yf
        return float(yf.Ticker(symbol).fast_info.last_price)
    except Exception:
        return None

# ── Signal computations ───────────────────────────────────────────────────────
def compute_rsi(prices: list, period: int = 7) -> float | None:
    if len(prices) < period + 1:
        return None
    s     = pd.Series(prices, dtype=float)
    delta = s.diff()
    gain  = delta.where(delta > 0, 0.0)
    loss  = (-delta.where(delta < 0, 0.0))
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
    squeeze = (band_w / price) < 0.02 if price > 0 else False
    return {
        "upper": round(upper, 4), "middle": round(middle, 4), "lower": round(lower, 4),
        "pct_b": round(pct_b, 3), "squeeze": squeeze,
        "near_lower": pct_b < 0.20, "at_lower": pct_b < 0.05,
        "far_below": price < lower * 0.99,
    }

def compute_stochrsi(prices: list, rsi_period: int = 14, stoch_period: int = 14) -> dict:
    if len(prices) < rsi_period + stoch_period + 5:
        return {"k": 50, "d": 50, "oversold": False, "overbought": False}
    s        = pd.Series(prices, dtype=float)
    delta    = s.diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
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
    n    = min(len(prices), len(volumes))
    p, v = prices[-n:], volumes[-n:]
    obv  = [0.0]
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

# V2.3: ADX regime filter + volume confirmation. Was claimed live in the
# V2.2 boot log ("ADX regime filter | Vol confirmation") but never actually
# implemented -- only existed in phase4_backtester.py, which is what the
# 298/321-win backtest evidence cited in this file's header was measuring.
# Ported directly from phase4_backtester.py's compute_adx/
# check_volume_confirmation, unchanged, so live finally matches what was
# already backtested instead of drifting from it.
ADX_TREND        = 20.0     # ADX < 20 = ranging = SCALP only
VOL_CONFIRM_MULT = 1.2      # volume gate

def compute_adx(prices: list, period: int = 14) -> float:
    if len(prices) < period * 2 + 5:
        return 25.0
    try:
        s      = pd.Series(prices)
        pos_dm = s.diff().clip(lower=0)
        neg_dm = (-s.diff()).clip(lower=0)
        tr     = s.diff().abs()
        atr_s  = tr.ewm(alpha=1.0 / period, adjust=False).mean()
        pdm_s  = pos_dm.ewm(alpha=1.0 / period, adjust=False).mean()
        ndm_s  = neg_dm.ewm(alpha=1.0 / period, adjust=False).mean()
        pdi    = 100 * pdm_s / atr_s.replace(0, float("nan"))
        ndi    = 100 * ndm_s / atr_s.replace(0, float("nan"))
        dx     = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, float("nan"))
        adx    = dx.ewm(alpha=1.0 / period, adjust=False).mean()
        val    = float(adx.iloc[-1])
        return round(val, 2) if not (pd.isna(val) or val < 0) else 25.0
    except Exception:
        return 25.0

def check_volume_confirmation(volumes: list, lookback: int = 10) -> bool:
    if len(volumes) < lookback + 1:
        return True
    recent_vol = volumes[-1]
    avg_vol    = sum(volumes[-lookback-1:-1]) / lookback
    return avg_vol <= 0 or recent_vol >= avg_vol * VOL_CONFIRM_MULT

# ── Context refresh thread ────────────────────────────────────────────────────
def refresh_context_data():
    all_underlyings = ["SMH", "GDX", "XBI", "QQQ", "^VIX"]
    while True:
        try:
            spy_p, _sv, _stf = fetch_prices_with_fallback("SPY", 40)
            qqq_p, _qv, _qtf = fetch_prices_with_fallback("QQQ", 40)
            with _context_lock:
                if spy_p: _spy_prices[:] = spy_p
                if qqq_p: _qqq_prices[:] = qqq_p
            for sym in all_underlyings:
                p1, _ = fetch_prices_and_volumes(sym, 40, "1m")
                p5, _ = fetch_prices_and_volumes(sym, 60, "5m")
                with _context_lock:
                    if p1:
                        if sym == "^VIX":
                            globals()["_vix_price"] = p1[-1]
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
        return _vix_price

def get_underlying_context(underlying: str) -> dict:
    with _context_lock:
        prices_1m = list(_underlying_prices.get(underlying, []))
        prices_5m = list(_underlying_5m.get(underlying, []))
    result = {
        "available": False, "rsi_1m": 50, "above_ema20_1m": False,
        "trending_up_1m": False, "rsi_5m": 50, "above_ema20_5m": False,
        "trending_up_5m": False, "at_high": False, "vol_expanding": False,
        "tide_bullish": False, "tide_bearish": False,
    }
    if len(prices_1m) >= 21:
        rsi_1m      = compute_rsi(prices_1m) or 50
        ema20_1m    = compute_ema(prices_1m, 20) or prices_1m[-1]
        momentum_1m = (prices_1m[-1] - prices_1m[-6]) / prices_1m[-6] if len(prices_1m) >= 6 and prices_1m[-6] > 0 else 0
        result.update({
            "available": True, "rsi_1m": rsi_1m,
            "above_ema20_1m": prices_1m[-1] > ema20_1m,
            "trending_up_1m": momentum_1m > 0,
        })
    if len(prices_5m) >= 21:
        rsi_5m      = compute_rsi(prices_5m) or 50
        ema20_5m    = compute_ema(prices_5m, 20) or prices_5m[-1]
        momentum_5m = (prices_5m[-1] - prices_5m[-6]) / prices_5m[-6] if len(prices_5m) >= 6 and prices_5m[-6] > 0 else 0
        hl_5m       = check_higher_lows(prices_5m, 15)
        result.update({
            "rsi_5m": rsi_5m,
            "above_ema20_5m": prices_5m[-1] > ema20_5m,
            "trending_up_5m": momentum_5m > 0 and hl_5m,
        })
        if len(prices_5m) >= 30:
            recent_high   = max(prices_5m[-30:])
            result["at_high"] = prices_5m[-1] >= recent_high * 0.995
    if result["available"]:
        result["tide_bullish"] = (
            result["above_ema20_1m"] and result["trending_up_1m"] and
            result.get("rsi_1m", 50) < 72
        )
        result["tide_bearish"] = (
            not result["above_ema20_1m"] and not result["trending_up_1m"]
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
        self.db_url           = db_url
        self._conn            = None
        self._lock            = threading.Lock()
        self._win_rates       = {}
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
                    print("[PM] Phase4 pattern memory tables ready (V2.0)", flush=True)
        except Exception as e:
            print(f"[PM] init_tables error: {e}", flush=True)

    def record_entry(self, trade_id, symbol, bear_pair, is_bear, mode,
                     entry_price, sym_rsi, spy_ctx, qqq_ctx, sym_ctx,
                     analyst_score=0, signal_boost=0, entry_score=0,
                     reversal_quality=0, underlying_tide=False, vix=15.0):
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
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT symbol, is_bear_trade, mode, symbol_rsi,
                               spy_bullish, qqq_overbought, hour_cdt,
                               won, pnl_pct
                        FROM phase4_trade_fingerprints WHERE won IS NOT NULL
                    """)
                    rows = cur.fetchall()

            if len(rows) < PM_MIN_TRADES:
                return

            from collections import defaultdict
            buckets  = defaultdict(list)
            pnl_bkts = defaultdict(list)

            for row in rows:
                key = Phase4Memory._bucket_key(
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
                        avg_pnl = sum(pnl_bkts[key]) / len(pnl_bkts[key]) if pnl_bkts[key] else None
                        cur.execute("""
                            INSERT INTO phase4_pattern_stats (bucket_key, win_rate, sample_count, avg_pnl)
                            VALUES (%s,%s,%s,%s)
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
        qqq_b  = "qqq_ob"   if qqq_ctx.get("overbought") else "qqq_ok"
        hr_b   = "hr_open"  if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
        return f"{symbol}|{'bear' if is_bear else 'bull'}|{mode or 'SCALP'}|{rsi_b}|{spy_b}|{qqq_b}|{hr_b}"

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
    def __init__(self, symbol: str, config: dict):
        self.symbol      = symbol
        self.bear_pair   = config["bear_pair"]
        self.underlying  = config["underlying"]
        self.budget_pct  = config["budget_pct"]
        self.cfg         = config

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
        # V2.15: daily-limit flag TRACKED ONLY (still not enforced -- boot
        # banner says so; arming is Matthew's call). phase4_server /think
        # has reported getattr(bot, "_daily_limit_hit", False) since V2.0
        # but nothing ever SET it -- the field was permanently False. Set
        # when daily_pnl (percent points) breaches DAILY_LOSS_LIMIT_PCT.
        self._daily_limit_hit: bool = False

        self._bear_ext_trailing:   bool  = False
        self._bear_ext_peak:       float = 0.0
        self._late_ratchet_active: bool  = False

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
        # V2.9: best-available data -- 5m stopgap when the 1m tape is thin,
        # auto-revert to 1m as soon as it's deep enough. Transitions logged.
        p, v, tf = fetch_prices_with_fallback(self.symbol, WARMUP_BARS + 5)
        if p:
            self.prices  = p
            self.volumes = v
            if tf != getattr(self, "_data_tf", "1m"):
                log(self.symbol, f"📶 {self.symbol} data: {tf} bars "
                    f"({'1m tape thin -- 5m stopgap' if tf == '5m' else 'back to full 1m resolution'})")
                self._data_tf = tf
        bp, bv, btf = fetch_prices_with_fallback(self.bear_pair, WARMUP_BARS + 5)
        if bp:
            self.bear_prices  = bp
            self.bear_volumes = bv
            if btf != getattr(self, "_bear_data_tf", "1m"):
                log(self.symbol, f"📶 {self.bear_pair} data: {btf} bars "
                    f"({'1m tape thin -- 5m stopgap' if btf == '5m' else 'back to full 1m resolution'})")
                self._bear_data_tf = btf

    def get_signal_suite(self, prices: list, volumes: list) -> dict:
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
                "rsi14_lt35": False, "rsi14_lt20": False, "rsi21_lt45": False,
                "vol_confirmed": True, "adx": 25.0,
            }
        rsi7   = compute_rsi(prices, 7)  or 50
        rsi14  = compute_rsi(prices, 14) or 50
        rsi21  = compute_rsi(prices, 21) or 50
        ma20   = compute_ma(prices, 20)  or prices[-1]
        ema9   = compute_ema(prices, 9)  or prices[-1]
        ema21v = compute_ema(prices, 21) or prices[-1]
        trend10       = (prices[-1] - prices[-11]) / prices[-11] if len(prices) > 11 and prices[-11] > 0 else 0
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
        adx      = compute_adx(prices)
        vol_ok   = check_volume_confirmation(volumes) if volumes else True
        return {
            "rsi": rsi7, "rsi14": rsi14, "rsi21": rsi21,
            "ma20": ma20, "above_ma20": above_ma20, "below_ma20": not above_ma20,
            "trend_10bar": round(trend10 * 100, 3),
            "higher_lows": higher_l, "bouncing": bouncing,
            "ema9": ema9, "ema21": ema21v, "ema9_above_ema21": ema9_above_21,
            "bb": bb, "bb_squeeze": bb.get("squeeze", False),
            "near_lower_bb": bb.get("near_lower", False),
            "at_lower_bb":   bb.get("at_lower", False),
            "far_below_bb":  bb.get("far_below", False),
            "stochrsi": stochrsi, "stochrsi_oversold": stochrsi.get("oversold", False),
            "macd": macd,         "macd_bullish":      macd.get("bullish", False),
            "obv": obv,           "obv_rising":        obv.get("rising", False),
            "obv_falling": not obv.get("rising", True),
            "williams_r": williams, "williams_oversold": williams.get("oversold", False),
            "cci": cci,           "cci_oversold":       cci.get("oversold", False),
            "rsi_lt40":   rsi7  < 40, "rsi_lt25":   rsi7  < 25,
            "rsi14_lt35": rsi14 < 35, "rsi14_lt20": rsi14 < 20,
            "rsi21_lt45": rsi21 < 45,
            "vol_confirmed": vol_ok, "adx": adx,
        }

    def compute_entry_score(self, sym: str, sym_ctx: dict) -> int:
        cfg   = self.cfg if sym == self.symbol else BEAR_RECIPES.get(sym, self.cfg)
        best  = cfg.get("best_signals", [])
        worst = cfg.get("worst_signals", [])
        score = 0
        for sig in best:
            if sym_ctx.get(sig, False):
                score += 1
        for sig in worst:
            if sym_ctx.get(sig, False):
                score -= 1
        if sym_ctx.get("rsi_lt25") and "rsi_lt25" in best:
            score += 1
        if sym_ctx.get("at_lower_bb") and "at_lower_bb" in best:
            score += 1
        return max(0, score)

    def select_mode(self, spy_ctx: dict, sym_ctx: dict) -> str:
        # V2.3: ranging market (ADX < 20) forces SCALP regardless of SPY
        # context -- checked first, matching phase4_backtester.py's order.
        if sym_ctx.get("adx", 25.0) < ADX_TREND:
            return "SCALP"
        if spy_ctx.get("overbought"):
            return "SCALP"
        if (spy_ctx.get("strong") and sym_ctx.get("trend_10bar", 0) > 0.3 and
                sym_ctx.get("higher_lows") and sym_ctx.get("above_ma20")):
            return "EXTENDED"
        if spy_ctx.get("bullish") and (sym_ctx.get("above_ma20") or sym_ctx.get("trend_10bar", 0) > 0.1):
            return "RIDE"
        return "SCALP"

    def get_exit_params(self) -> tuple:
        if self.active_sym == self.bear_pair:
            br      = BEAR_RECIPES.get(self.bear_pair, self.cfg)
            sl      = br["atr_stop"]
            early_r = br["early_ratchet"]
            late_r  = early_r * 1.8
            trail_n = br["trail"]
            trail_t = round(trail_n * 0.7, 4)
            return sl, early_r, late_r, trail_n, trail_t
        sl      = self.cfg["atr_stop"]
        early_r = self.cfg["early_ratchet"]
        late_r  = self.cfg["late_ratchet"]
        trail_n = self.cfg["trail_normal"]
        trail_t = self.cfg["trail_tight"]
        mult    = (self.cfg.get("ride_stop_mult", 1.3) if self.mode == "RIDE" else
                   self.cfg.get("ext_stop_mult",  1.8) if self.mode == "EXTENDED" else 1.0)
        return round(sl * mult, 4), early_r, late_r, trail_n, trail_t

    def should_enter_bull(self, spy_ctx: dict, sym_ctx: dict, underlying_ctx: dict) -> tuple:
        now = datetime.now(tz=CENTRAL)
        if now.hour in self.cfg.get("avoid_hours", []):
            return False, 0, "avoid_hour"
        if now.weekday() in self.cfg.get("avoid_days", []):
            return False, 0, "avoid_day"
        vix = get_vix()
        if vix >= VIX_PAUSE:
            return False, 0, f"vix_pause({vix:.1f})"
        if not sym_ctx.get("bouncing"):
            return False, 0, "no_bounce"
        if underlying_ctx.get("available") and underlying_ctx.get("tide_bearish"):
            return False, 0, "tide_bearish"
        if not sym_ctx.get("vol_confirmed", True):
            return False, 0, "vol_not_confirmed"
        score  = self.compute_entry_score(self.symbol, sym_ctx)
        min_sc = self.cfg["min_score"]
        if score < min_sc:
            return False, score, f"score_{score}<{min_sc}"
        return True, score, "ok"

    def score_reversal_quality(self, bull_rsi: float, drop: float, bear_ctx: dict) -> int:
        score = 0
        if bull_rsi >= REVERSAL_HIGH_RSI:
            score += 2
        elif bull_rsi >= REVERSAL_OB_RSI:
            score += 1
        if drop >= REVERSAL_HIGH_DROP:
            score += 1
        if bear_ctx.get("rsi", 50) < 45:
            score += 1
        if bear_ctx.get("obv_rising"):
            score += 1
        underlying_ctx = get_underlying_context(
            BEAR_RECIPES.get(self.bear_pair, {}).get("underlying", "QQQ"))
        if underlying_ctx.get("at_high") and underlying_ctx.get("tide_bullish"):
            score -= 2
        return max(0, min(3, score))

    def check_reversal(self) -> tuple:
        if len(self.prices) < 8:
            return False, 0
        bull_rsi = compute_rsi(self.prices)
        if bull_rsi is None:
            return False, 0
        state = self.reversal_state
        now_t = time.time()
        if state["state"] == "IDLE":
            if bull_rsi >= REVERSAL_OB_RSI:
                self.reversal_state = {"state": "WATCHING", "bull_peak": self.prices[-1], "watch_start": now_t}
                log(self.symbol, f"👁 REVERSAL WATCH -> {self.bear_pair} | RSI={bull_rsi:.1f}")
            return False, 0
        if state["state"] == "WATCHING":
            if now_t - state.get("watch_start", now_t) > REVERSAL_MAX_WATCH:
                self.reversal_state = {"state": "IDLE"}
                return False, 0
            # V2.14: DROP-CONFIRM BEFORE THE RSI RESET. A violent flush
            # drives RSI below 60 on the SAME bars that produce the >=0.5%
            # drop -- the old order reset the watch before the drop check
            # ever ran, so fast cracks (the profitable kind) systematically
            # KILLED the watch instead of firing it, and re-arming needs
            # RSI>=70 which a collapsing price never revisits. Jul 7
            # evidence: SOXL armed 6 watches over 2h at the top, then RSI
            # 84->24 midday flush fired NOTHING. Slow grinds still reset
            # normally via the check below; RSI collapsing WITH the drop is
            # the confirmation, not a reason to stand down. Same
            # wrong-order family as the V2.1 exit-priority fix.
            bull_peak = max(state.get("bull_peak", self.prices[-1]), self.prices[-1])
            self.reversal_state["bull_peak"] = bull_peak
            drop = (bull_peak - self.prices[-1]) / bull_peak if bull_peak > 0 else 0
            if drop >= REVERSAL_CONFIRM:
                # V2.14: every rejection now says WHY -- these paths were
                # all silent, which made today's miss undiagnosable without
                # log forensics.
                reject = None
                bear_ctx = None
                if len(self.bear_prices) < 3 or self.bear_prices[-1] <= self.bear_prices[-3]:
                    reject = "bear_not_bid"
                if reject is None:
                    now_hour   = datetime.now(tz=CENTRAL).hour
                    bear_avoid = BEAR_RECIPES.get(self.bear_pair, {}).get("avoid_hours", [])
                    if now_hour in bear_avoid:
                        reject = f"bear_avoid_hour_{now_hour}"
                if reject is None and self.bear_pair == "SQQQ" and not SQQQ_ENABLED:
                    reject = "sqqq_disabled"
                if reject is None:
                    qqq_ctx = get_qqq_context()
                    if qqq_ctx.get("oversold"):
                        reject = "qqq_oversold"
                    else:
                        gate = QQQ_BEAR_RSI_GATE_LABD if self.bear_pair == "LABD" else QQQ_BEAR_RSI_GATE
                        if qqq_ctx.get("rsi", 50) < gate:
                            reject = f"qqq_rsi_{qqq_ctx.get('rsi', 50):.0f}<{gate}"
                if reject is None:
                    bear_ctx   = self.get_signal_suite(self.bear_prices, self.bear_volumes)
                    bear_min   = BEAR_RECIPES.get(self.bear_pair, {}).get("min_score", 4)
                    bear_score = self.compute_entry_score(self.bear_pair, bear_ctx)
                    if bear_score < bear_min:
                        reject = f"bear_score_{bear_score}<{bear_min}"
                    elif not bear_ctx.get("vol_confirmed", True):
                        reject = "vol_not_confirmed"
                if reject is None:
                    quality = self.score_reversal_quality(bull_rsi, drop, bear_ctx)
                    if quality == 0:
                        reject = "quality_0"
                if reject is None:
                    log(self.symbol,
                        f"🔁 REVERSAL -> {self.bear_pair} | drop={round(drop*100,2)}% | "
                        f"bull_rsi={bull_rsi:.1f} | bear_score={bear_score} | quality={quality}")
                    self.reversal_state = {"state": "IDLE"}
                    return True, quality
                log(self.symbol,
                    f"🚫 REVERSAL REJECTED -> {self.bear_pair} | drop={round(drop*100,2)}% | "
                    f"bull_rsi={bull_rsi:.1f} | gate={reject}")
                # fall through to the RSI reset check below -- a rejected
                # crack with collapsed RSI should end the watch normally
            if bull_rsi < REVERSAL_RSI_RESET:
                self.reversal_state = {"state": "IDLE"}
                return False, 0
        return False, 0

    def try_buy(self, sym: str, prices: list, volumes: list,
                spy_ctx: dict, sym_ctx: dict, reversal_quality: int = 0) -> bool:
        # V2.7: killswitch pause -- phase4_server /close_all sets
        # bot.kill_paused on every bot; /resume clears it. Per-bot attribute
        # (NOT a module global) because this file runs as __main__ while the
        # server does `import phase4` -- two separate module instances, so a
        # module-level flag set by the server would never be seen here. The
        # bot objects are shared by reference; attributes on them are the
        # one reliable channel. Blocks NEW entries only; exits keep running.
        if getattr(self, "kill_paused", False):
            return False
        bp        = get_buying_power()
        # V2.4: Win Follower dynamic budget share -- falls back to the static
        # budget_pct until the first refresh completes (or if DB unavailable)
        _share    = _win_follower.get_weight(self.symbol, self.budget_pct) if _win_follower else self.budget_pct
        base_size = round(bp * _share, 2)
        if base_size < 1.00:
            return False

        is_bear     = (sym == self.bear_pair)
        entry_score = self.compute_entry_score(sym, sym_ctx)
        self.mode   = self.select_mode(spy_ctx, sym_ctx)

        analyst_scores = fetch_analyst_scores()
        analyst_entry  = analyst_scores.get(sym, {})
        analyst_score  = analyst_entry.get("score", 0)
        signal_boost, _ = get_analyst_signal_boost(sym, analyst_scores)

        underlying     = self.cfg["underlying"] if not is_bear else BEAR_RECIPES.get(sym, {}).get("underlying", "QQQ")
        underlying_ctx = get_underlying_context(underlying)
        tide_bullish   = underlying_ctx.get("tide_bullish", False)
        vix            = get_vix()
        qqq_ctx        = get_qqq_context()

        if _phase4_memory:
            hour = datetime.now(tz=CENTRAL).hour
            skip, wr, has_data = _phase4_memory.should_skip_entry(
                self.symbol, is_bear, self.mode, sym_ctx.get("rsi", 50),
                spy_ctx, qqq_ctx, hour
            )
            if skip:
                log(self.symbol, f"🚫 WIN-RATE GATE: {sym} historical WR={wr:.0%}")
                return False

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

        # V2.2: Cross-service capital coordination -- Berserker (main.py)
        # trades against this SAME Alpaca account from a separate process.
        # Clamp our intended spend against what's actually available once
        # Berserker's outstanding reservations (if any) are accounted for.
        # Fails open: if the coordinator can't reach the DB, available falls
        # back to bp (raw buying power) unchanged.
        if _capital_coordinator:
            available = _capital_coordinator.get_available(bp)
            if trade_size > available:
                if available < 1.00:
                    log(self.symbol, f"💰 CAPITAL COORD: ${available:.2f} available "
                        f"(Berserker holding the rest) — skipping {sym}")
                    return False
                log(self.symbol, f"💰 CAPITAL COORD: trimmed ${trade_size:.2f} -> "
                    f"${available:.2f} (Berserker reservation active)")
                trade_size = round(available, 2)

        price = prices[-1] if prices else get_current_price(sym)
        if not price or price <= 0:
            return False

        boost_label = " 🔥COMBO" if signal_boost == 2 else " ✨sig" if signal_boost == 1 else ""
        tide_label  = " 🌊TIDE"  if tide_bullish else ""
        log(self.symbol,
            f"📊 BUY signal | score={entry_score} | mode={self.mode} | "
            f"RSI={sym_ctx['rsi']:.1f} | size_mult={size_mult:.0%} | "
            f"analyst={analyst_score}{boost_label}{tide_label}")

        # V2.2: Reserve against the shared account immediately before
        # submitting, release immediately after -- closes the race window
        # where Berserker (separate process, same Alpaca account) could read
        # stale buying_power between now and Alpaca settling this order.
        _res_id = _capital_coordinator.reserve(trade_size, symbol=sym) if _capital_coordinator else None
        try:
            _order  = place_order(sym, "BUY", trade_size)
            success = _order is not None
        finally:
            if _capital_coordinator:
                _capital_coordinator.release(_res_id)
        if success:
            import secrets
            # V2.8: anchor to the ACTUAL fill, not the last 1-min bar --
            # stops, ratchets, MFE/MAE, and the fingerprint all key off
            # entry_price. Bar-price anchoring was the same disease
            # Berserker fixed in V10.35.
            _fill = get_fill_price(_order)
            entry_anchor = _fill if _fill > 0 else price
            if _fill > 0 and abs(_fill - price) / price > 0.001:
                log(self.symbol, f"⚓ Fill anchor: bar=${price:.3f} -> fill=${_fill:.3f}")
            self.in_position           = True
            self.active_sym            = sym
            self.entry_price           = entry_anchor
            self.peak_price            = entry_anchor
            self.entry_time            = time.time()
            self.mfe                   = 0.0
            self.mae                   = 0.0
            self.trade_id              = secrets.token_hex(8)
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
            self._bear_ext_peak        = entry_anchor
            self._late_ratchet_active  = False

            if _phase4_memory:
                _phase4_memory.record_entry(
                    self.trade_id, self.symbol, self.bear_pair,
                    is_bear, self.mode, entry_anchor,
                    self._entry_rsi, spy_ctx, qqq_ctx, sym_ctx,
                    analyst_score, signal_boost,
                    entry_score, reversal_quality, tide_bullish, vix
                )

            log(self.symbol,
                f"⚡ BUY: {sym} | ${trade_size:.2f} notional @ ~${round(price,2)} | "
                f"mode={self.mode}{boost_label}")
            alert(
                f"⚡ PHASE4 BUY [{self.mode}]: {sym} | ${trade_size:.2f} @ ~${round(price,2)}"
                f"\nscore={entry_score} | boost={signal_boost} | vix={vix:.1f}{boost_label}"
            )
            return True
        return False

    def manual_open(self, sym: str, usd: float) -> dict:
        """
        V2.16: discretionary buy from /buyphase4 (Fleet Commander). Bypasses
        entry scoring/reversal gates ON PURPOSE -- Matthew's judgment call --
        but the position lands in this bot's normal state machine, so stops,
        ratchets, MFE/MAE, external-close detection, and fingerprints all
        apply identically to a scored entry. sym must be this bot's bull
        symbol or its bear pair; one position per bot, same as always.
        self.mode is set to SCALP (the conservative exit profile); the
        pattern-memory row records mode='MANUAL' so these trades are
        isolated in the buckets and auditable later.
        """
        sym = sym.upper().strip()
        if self.in_position:
            return {"ok": False,
                    "error": f"{self.symbol} bot already holds {self.active_sym}"}
        if sym not in (self.symbol, self.bear_pair):
            return {"ok": False,
                    "error": f"{sym} not managed by the {self.symbol} bot"}
        if usd <= 0:
            return {"ok": False, "error": "amount must be positive"}
        try:
            price = get_current_price(sym) or 0.0
        except Exception:
            price = 0.0
        if price <= 0:
            return {"ok": False, "error": f"no price for {sym}"}

        is_bear = (sym == self.bear_pair)
        _res_id = _capital_coordinator.reserve(usd, symbol=sym) if _capital_coordinator else None
        try:
            _order  = place_order(sym, "BUY", usd)
            success = _order is not None
        finally:
            if _capital_coordinator:
                _capital_coordinator.release(_res_id)
        if not success:
            return {"ok": False, "error": f"order rejected for {sym}"}

        import secrets
        _fill = get_fill_price(_order)
        entry_anchor = _fill if _fill > 0 else price
        self.in_position           = True
        self.active_sym            = sym
        self.entry_price           = entry_anchor
        self.peak_price            = entry_anchor
        self.entry_time            = time.time()
        self.mfe                   = 0.0
        self.mae                   = 0.0
        self.trade_id              = secrets.token_hex(8)
        self.mode                  = "SCALP"       # conservative exit profile
        self._entry_spy_ctx        = {}
        self._entry_qqq_ctx        = {}
        self._entry_sym_ctx        = {}
        self._entry_rsi            = 50.0
        self._entry_analyst_score  = 0
        self._entry_signal_boost   = 0
        self._entry_score          = 0
        self._entry_rev_quality    = 0
        self._entry_tide           = False
        self._entry_vix            = 0.0
        self._bear_ext_trailing    = False
        self._bear_ext_peak        = entry_anchor
        self._late_ratchet_active  = False

        if _phase4_memory:
            _phase4_memory.record_entry(
                self.trade_id, self.symbol, self.bear_pair,
                is_bear, "MANUAL", entry_anchor,
                50.0, {}, {}, {}, 0, 0, 0, 0, False, 0.0
            )

        log(self.symbol,
            f"⚡ MANUAL BUY: {sym} | ${usd:.2f} notional @ ~${entry_anchor:.3f} "
            f"(fill-anchored) | exits=SCALP")
        alert(
            f"⚡ PHASE4 MANUAL BUY: {sym} | ${usd:.2f} @ ~${round(entry_anchor, 2)}\n"
            f"Bot-managed (stops/ratchets/EOD apply). /close {sym} to exit."
        )
        return {"ok": True,
                "msg": f"{sym} ${usd:.2f} @ ${entry_anchor:.3f}"}

    def try_sell(self, reason: str, pnl_pct: float) -> bool:
        status, _order = place_sell_all(self.active_sym)

        # V2.15: EXTERNAL-CLOSE DETECTION. The position is already gone
        # (Fleet Commander /close, manual, broker sweep). Record the exit
        # with the quote PnL we were tracking, tag it -external so Friday
        # digests can separate these, clear state, and DON'T count it
        # against the win/loss cooldowns -- we didn't make this exit.
        if status == "gone":
            hold_min = int((time.time() - self.entry_time) / 60) if self.entry_time > 0 else 0
            log(self.symbol,
                f"🤝 {self.active_sym} closed externally | last P&L "
                f"{round(pnl_pct*100,3):+.3f}% | held {hold_min}m -- clearing state")
            alert(f"🤝 PHASE4: {self.active_sym} was closed externally | "
                  f"{round(pnl_pct*100,3):+.3f}% | state cleared")
            if _phase4_memory and self.trade_id:
                _phase4_memory.record_exit(
                    self.trade_id, pnl_pct > 0, pnl_pct,
                    f"{reason}-external", hold_min, self.mfe, self.mae
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
            self._late_ratchet_active = False
            self.daily_pnl += pnl_pct * 100
            if self.daily_pnl <= -DAILY_LOSS_LIMIT_PCT and not self._daily_limit_hit:
                self._daily_limit_hit = True
                log(self.symbol, f"⚠ Daily loss limit BREACHED (tracked, not enforced): {self.daily_pnl:+.2f}%")
            return True

        success = (status == "ok")
        if success:
            # V2.15: FILL-ANCHORED EXIT -- entries got the fill anchor in
            # V2.8 but exits still recorded the last bar price. On 3x ETFs
            # intrabar moves of 0.2-0.5% are routine; the recorded PnL,
            # fingerprints, Win Follower weights, and daily tallies were
            # all inheriting the bar-vs-fill gap. Mirrors Berserker V10.37.
            _fill = get_fill_price(_order)
            if _fill > 0 and self.entry_price > 0:
                _fill_pnl = (_fill - self.entry_price) / self.entry_price
                if abs(_fill_pnl - pnl_pct) > 0.0005:
                    log(self.symbol,
                        f"⚓ Exit fill anchor: bar P&L {round(pnl_pct*100,3):+.3f}% "
                        f"-> fill {round(_fill_pnl*100,3):+.3f}%")
                pnl_pct = _fill_pnl
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

            self.in_position          = False
            self.peak_price           = 0.0
            self.entry_price          = 0.0
            self.entry_time           = 0.0
            self.trade_id             = ""
            self.mfe                  = 0.0
            self.mae                  = 0.0
            self._bear_ext_trailing   = False
            self._bear_ext_peak       = 0.0
            self._late_ratchet_active = False

            if pnl_pct > 0:
                self.daily_wins += 1
                self.set_cooldown(WIN_COOLDOWN_SECS)
            else:
                self.daily_losses += 1
                self.set_cooldown(LOSS_COOLDOWN_SECS)
            self.daily_pnl += pnl_pct * 100
            if self.daily_pnl <= -DAILY_LOSS_LIMIT_PCT and not self._daily_limit_hit:
                self._daily_limit_hit = True
                log(self.symbol, f"⚠ Daily loss limit BREACHED (tracked, not enforced): {self.daily_pnl:+.2f}%")
            return True
        return False

    def recover_position(self):
        positions = get_all_positions()
        for sym in [self.symbol, self.bear_pair]:
            if sym in positions:
                import secrets
                pos              = positions[sym]
                cost             = float(pos.avg_entry_price or 0)
                self.in_position = True
                self.active_sym  = sym
                self.entry_price = cost
                self.peak_price  = max(cost, get_current_price(sym) or cost)
                self.trade_id    = secrets.token_hex(8)
                # V2.8: recover the REAL entry time from the last filled BUY
                # order instead of resetting to now -- the dwell-exit timer
                # and hold-time stats were restarting on every redeploy.
                # Falls back to now if the order lookup fails.
                self.entry_time  = time.time()
                try:
                    from alpaca.trading.requests import GetOrdersRequest
                    from alpaca.trading.enums import QueryOrderStatus
                    _req = GetOrdersRequest(status=QueryOrderStatus.CLOSED,
                                            symbols=[sym], limit=10)
                    for _o in (_trade_client.get_orders(filter=_req) or []):
                        _side = str(getattr(_o, "side", "")).lower()
                        _fat  = getattr(_o, "filled_at", None)
                        if _side.endswith("buy") and _fat is not None:
                            self.entry_time = _fat.timestamp()
                            log(self.symbol, f"🕐 Recovered entry time: "
                                f"{datetime.fromtimestamp(self.entry_time, tz=CENTRAL).strftime('%m/%d %I:%M %p')}")
                            break
                except Exception as _te:
                    log(self.symbol, f"⚠ entry-time recovery failed ({_te}) -- using now")
                self.mfe         = 0.0
                self.mae         = 0.0
                spy_ctx          = get_spy_context()
                sym_ctx          = self.get_signal_suite(self.prices, self.volumes)
                self.mode        = self.select_mode(spy_ctx, sym_ctx)
                self._bear_ext_trailing   = False
                self._bear_ext_peak       = self.peak_price
                self._late_ratchet_active = False
                log(self.symbol,
                    f"🔄 Recovered: {sym} | entry=${cost:.3f} | mode={self.mode}")
                return

    def run_loop(self):
        log(self.symbol,
            f"🚀 Bot online | bear={self.bear_pair} | underlying={self.underlying} | "
            f"budget={int(self.budget_pct*100)}% | min_score={self.cfg['min_score']}")

        # Warmup — keep retrying until we have bars
        for attempt in range(30):
            self.refresh_prices()
            if len(self.prices) >= WARMUP_BARS:
                log(self.symbol, f"✅ Warmed up | {len(self.prices)} bars")
                break
            log(self.symbol, f"⏳ Warming up: {len(self.prices)}/{WARMUP_BARS} bars")
            time.sleep(12)
        else:
            log(self.symbol, f"⚠ Warmup timeout — continuing with {len(self.prices)} bars")

        self.recover_position()

        while True:
            try:
                if not is_market_hours():
                    if self.in_position:
                        log(self.symbol, "📌 Market closed — holding overnight")
                        # V2.18 SHADOW: log the joint rule's EOD branch once/day.
                        # R1 trend -> WOULD_CLOSE at this price; R2 chop -> WOULD_CARRY.
                        try:
                            _today = datetime.now(CENTRAL).strftime("%Y-%m-%d")
                            if getattr(self, "_shadow_eod_date", None) != _today:
                                self._shadow_eod_date = _today
                                _ap = self.prices if self.active_sym == self.symbol else self.bear_prices
                                _av = self.volumes if self.active_sym == self.symbol else self.bear_volumes
                                _ctx = self.get_signal_suite(_ap, _av) if _ap else {}
                                _regime = "TREND" if _ctx.get("above_ma20") else "CHOP"
                                _rule   = "WOULD_CLOSE" if _regime == "TREND" else "WOULD_CARRY"
                                _shadow_event("eod_hold", self.symbol, self.active_sym,
                                              _regime, _rule, "HELD",
                                              _ap[-1] if _ap else 0.0,
                                              f"entry={self.entry_price:.4f}")
                        except Exception:
                            pass
                    time.sleep(60)
                    continue

                self.refresh_prices()
                spy_ctx        = get_spy_context()
                sym_ctx        = self.get_signal_suite(self.prices, self.volumes)
                underlying_ctx = get_underlying_context(self.underlying)
                prices         = self.prices

                if not prices:
                    time.sleep(LOOP_INTERVAL)
                    continue

                if self.in_position:
                    active_prices  = self.prices  if self.active_sym == self.symbol else self.bear_prices
                    active_volumes = self.volumes  if self.active_sym == self.symbol else self.bear_volumes
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

                    ext_cfg = BEAR_EXTENDED_TP.get(self.active_sym) if self.active_sym == self.bear_pair else None
                    if ext_cfg:
                        self._bear_ext_peak = max(self._bear_ext_peak, price)
                        if not self._bear_ext_trailing and profit_pct >= ext_cfg["trail_activate"]:
                            self._bear_ext_trailing = True
                            log(self.symbol, f"🎯 {self.active_sym} EXTENDED TP activated at +{profit_pct*100:.1f}%")
                        if self._bear_ext_trailing:
                            ext_dd = (self._bear_ext_peak - price) / self._bear_ext_peak if self._bear_ext_peak > 0 else 0
                            if ext_dd >= ext_cfg["trail_stop"]:
                                self.try_sell("ext-trail", profit_pct)
                                time.sleep(LOOP_INTERVAL)
                                continue
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)
                    else:
                        # V2.1 FIX (Jun 30 2026): ratchet now checked BEFORE
                        # rsi-overbought. See module docstring for the full
                        # rationale and backtest evidence -- in short, the old
                        # order let rsi-overbought cut nearly every winner the
                        # instant profit ticked positive (mean-reversion
                        # entries naturally push RSI toward overbought as the
                        # thesis plays out), so winners almost never reached
                        # early_ratchet. 93% of wins were rsi-overbought exits
                        # averaging ~+0.24%, against stop-losses averaging
                        # ~-1.7% -- a ~7:1 mismatch that made a 73% WR system
                        # net PnL-negative. Stop-loss priority is unchanged.
                        if profit_pct <= -sl:
                            self.try_sell("stop-loss", profit_pct)
                        elif profit_pct >= early_r:
                            rsi_now  = active_ctx.get("rsi", 50)
                            obv_flat = not active_ctx.get("obv_rising") and not active_ctx.get("obv_falling")
                            if (profit_pct >= late_r or rsi_now >= 65 or
                                    (obv_flat and profit_pct >= early_r * 1.5)):
                                self._late_ratchet_active = True
                            trail  = trail_t if self._late_ratchet_active else trail_n
                            if drawdown >= trail:
                                reason = "trail-tight" if self._late_ratchet_active else "trail"
                                self.try_sell(reason, profit_pct)
                        elif active_ctx.get("rsi", 50) >= RSI_OVERBOUGHT_EXIT and profit_pct > 0:
                            log(self.symbol, f"🔄 RSI REVERSAL EXIT: RSI={active_ctx['rsi']:.0f}")
                            self.try_sell("rsi-overbought", profit_pct)
                        elif (self.mode == "EXTENDED" and self.active_sym == self.symbol and
                              profit_pct > -0.005 and not active_ctx.get("higher_lows", True)):
                            log(self.symbol, "📉 EXTENDED: trend break")
                            self.try_sell("trend-break", profit_pct)
                        elif self.entry_time > 0:
                            held_min = (time.time() - self.entry_time) / 60
                            if held_min >= DWELL_MINUTES and abs(profit_pct) < DWELL_FLAT_THRESHOLD:
                                log(self.symbol, f"⏱ DWELL EXIT: {held_min:.0f}m | flat at {profit_pct*100:+.3f}%")
                                self.try_sell("dwell", profit_pct)

                elif not self.is_on_cooldown():
                    rev_ok, rev_quality = self.check_reversal()
                    if rev_ok:
                        if not self.bear_prices:
                            log(self.symbol, "⚠ Reversal but bear_prices empty — skip")
                        else:
                            bear_ctx = self.get_signal_suite(self.bear_prices, self.bear_volumes)
                            # V2.18 SHADOW: annotate what the joint rule would do
                            try:
                                _regime = "TREND" if bear_ctx.get("above_ma20") else "CHOP"
                                _rule   = "ALLOW" if _regime == "TREND" else "CARRY_ONLY"
                                _shadow_event("entry_signal", self.symbol, self.bear_pair,
                                              _regime, _rule, "ENTERED",
                                              self.bear_prices[-1], f"bear rev_q={rev_quality}")
                            except Exception:
                                pass
                            self.try_buy(self.bear_pair, self.bear_prices, self.bear_volumes,
                                         spy_ctx, bear_ctx, reversal_quality=rev_quality)
                    else:
                        should_enter, score, reason = self.should_enter_bull(
                            spy_ctx, sym_ctx, underlying_ctx)
                        if should_enter:
                            # V2.18 SHADOW: annotate what the joint rule would do
                            try:
                                _regime = "TREND" if sym_ctx.get("above_ma20") else "CHOP"
                                _rule   = "ALLOW" if _regime == "TREND" else "CARRY_ONLY"
                                _shadow_event("entry_signal", self.symbol, self.symbol,
                                              _regime, _rule, "ENTERED",
                                              prices[-1], f"bull score={score}")
                            except Exception:
                                pass
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
    global _phase4_memory, _capital_coordinator, _win_follower
    print("[PHASE4] NEXUS PHASE 4 V2.18 STARTING — Alpaca Edition", flush=True)
    print("[PHASE4] Broker: Alpaca | Fractional shares | Real-time IEX feed", flush=True)
    print(f"[PHASE4] Bots: NUGT(30%) | SOXL(25%) | LABU(25%) | TQQQ(20%) base — V2.4 reweights hourly by rolling WR", flush=True)
    print(f"[PHASE4] Bear pairs: DUST | SOXS | LABD" + (" | SQQQ" if SQQQ_ENABLED else " | SQQQ(DISABLED)"), flush=True)
    print(f"[PHASE4] V2.3: Capital coordination | Exit priority fix | ADX regime filter (live) | Vol confirmation (live) | Underlying exit | Daily limits tracked (not yet enforced)", flush=True)

    # Auth check
    print(f"[PHASE4] Auth check: API key={'SET (' + ALPACA_API_KEY[:6] + ')' if ALPACA_API_KEY else 'MISSING'}", flush=True)
    print(f"[PHASE4] Auth check: Secret={'SET' if ALPACA_API_SECRET else 'MISSING'}", flush=True)
    print(f"[PHASE4] Alpaca clients ready (paper={PAPER_MODE})", flush=True)

    if DATABASE_URL and _db_available:
        _phase4_memory = Phase4Memory(DATABASE_URL)
        _phase4_memory.init_tables()
        _phase4_memory.start_scheduler()
        print("[PHASE4] Pattern memory: DB connected", flush=True)
    else:
        _phase4_memory = Phase4Memory("")
        print("[PHASE4] Pattern memory: disabled (no DATABASE_URL)", flush=True)

    # V2.4: Win Follower budget allocator
    _win_follower = Phase4WinFollower(DATABASE_URL if _db_available else "")
    _win_follower.start_scheduler()

    # V2.2: Capital coordinator -- Phase4 trades against the SAME live Alpaca
    # account as Berserker (main.py, separate Railway service/process) with
    # zero prior coordination. See capital_coordinator.py for full rationale.
    _capital_coordinator = CapitalCoordinator(DATABASE_URL, service_name="phase4")
    _capital_coordinator.init_table()

    # Verify Alpaca account
    if _trade_client:
        try:
            acct = _trade_client.get_account()
            print(f"[PHASE4] Alpaca account: buying_power=${float(acct.buying_power):.2f} | paper={PAPER_MODE}", flush=True)
        except Exception as e:
            print(f"[PHASE4] ⚠ Account check failed: {e}", flush=True)
    else:
        print("[PHASE4] ⚠ No Alpaca trade client — orders disabled", flush=True)

    print(f"[PHASE4] Analyst bridge: {ANALYST_URL if ANALYST_URL else 'disabled'}", flush=True)
    print(f"[PHASE4] VIX caution={VIX_CAUTION} / pause={VIX_PAUSE}", flush=True)

    ctx_thread = threading.Thread(target=refresh_context_data, daemon=True)
    ctx_thread.start()
    print("[PHASE4] Context refresh thread started — waiting 15s for data warmup...", flush=True)
    time.sleep(15)

    spy_status  = "✅" if len(_spy_prices) > 0 else "⚠ EMPTY"
    soxl_status = "✅" if "SMH" in _underlying_prices else "⚠ EMPTY"
    print(f"[PHASE4] Data check: SPY={spy_status} | SOXL={soxl_status}", flush=True)

    bots    = []
    threads = []
    for symbol, config in BOT_CONFIGS.items():
        bot = SymbolBot(symbol, config)
        bots.append(bot)
        t = threading.Thread(target=bot.run_loop, daemon=True, name=f"bot_{symbol}")
        threads.append(t)
        t.start()
        print(f"[PHASE4] ✅ {symbol} bot started (underlying={config['underlying']}, min_score={config['min_score']})", flush=True)
        time.sleep(2)

    from phase4_server import start_server
    start_server(bots)
    _phaseb_heartbeat_start(bots)   # V2.17 Phase B

    vix_now = get_vix()
    alert(
        f"⚡ PHASE4 V2.18 ONLINE — Alpaca Edition\n"
        f"Win Follower: budgets reweight hourly by 14d WR (±8pts, 10% floor)\n"
        f"SOXL(SMH) TQQQ(QQQ) NUGT(GDX) LABU(XBI)\n"
        f"VIX: {vix_now:.1f} | SQQQ: {'ON' if SQQQ_ENABLED else 'OFF'}\n"
        f"Analyst: {'✅' if ANALYST_URL else '⚠ disabled'}\n"
        f"V2.18: Regime shadow (B1+B3, no orders) | V2.17 dashboard feed | V2.16 manual buy\n"
        f"V2.2: Capital coordination (shared Alpaca account w/ Berserker)\n"
        f"V2.1: Exit priority fix — ratchet before rsi-overbought"
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
                bot._daily_limit_hit = False   # V2.15
            last_day = today
            print("[PHASE4] 🌅 Daily reset", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    run()
