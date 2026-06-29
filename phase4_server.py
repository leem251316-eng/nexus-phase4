"""
phase4_server.py — Thin Flask control server for nexus-phase4.
Runs as a daemon thread alongside the bot worker threads.
Exposes /think and /health so nexus-commander can query live bot state.

V2.0: Updated for Alpaca migration — removed acct_id, bot.prices/bear_prices
      references. Uses shared _latest_prices cache and Alpaca position data.
"""
import os
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify

CENTRAL     = ZoneInfo("America/Chicago")
_flask_app  = Flask("phase4_server")
_bots       = []
_start_time = time.time()


def _bot_state_dict(bot) -> dict:
    """Serializable state from a SymbolBot instance — V2.0 Alpaca version."""
    now = time.time()
    d   = {
        "symbol":       bot.symbol,
        "bear_pair":    bot.bear_pair,
        "in_position":  bot.in_position,
        "mode":         bot.mode,
        "daily_wins":   bot.daily_wins,
        "daily_losses": bot.daily_losses,
        "daily_pnl":    round(bot.daily_pnl * 100, 3),
        "daily_limit_hit": bot._daily_limit_hit,
    }

    if bot.in_position:
        try:
            from phase4 import get_current_price
            current_price = get_current_price(bot.active_sym) or bot.entry_price
        except Exception:
            current_price = bot.entry_price

        peak     = bot.peak_price or bot.entry_price
        pnl_pct  = ((current_price - bot.entry_price) / bot.entry_price * 100
                    if bot.entry_price > 0 else 0)
        peak_pct = ((peak - bot.entry_price) / bot.entry_price * 100
                    if bot.entry_price > 0 else 0)
        held_min = int((now - bot.entry_time) / 60) if bot.entry_time > 0 else 0

        try:
            sl, early_r, late_r, trail_n, trail_t = bot.get_exit_params()
        except Exception:
            sl, early_r, late_r, trail_n, trail_t = 0, 0, 0, 0, 0

        d.update({
            "active_sym":    bot.active_sym,
            "entry_price":   round(bot.entry_price, 4),
            "current_price": round(current_price, 4),
            "peak_price":    round(peak, 4),
            "pnl_pct":       round(pnl_pct, 3),
            "peak_pct":      round(peak_pct, 3),
            "held_minutes":  held_min,
            "stop_pct":      round(sl, 4),
            "mfe":           round(bot.mfe * 100, 3),
            "mae":           round(bot.mae * 100, 3),
            "mode":          bot.mode,
        })
    else:
        try:
            from phase4 import compute_rsi, get_prices
            prices, _ = get_prices(bot.symbol)
            rsi = compute_rsi(prices) if prices else None
        except Exception:
            rsi = None

        cooldown_remaining = max(0, bot.cooldown_until - now) if bot.cooldown_until > now else 0
        d.update({
            "rsi":                     round(rsi, 2) if rsi is not None else None,
            "reversal_state":          bot.reversal_state.get("state", "IDLE"),
            "cooldown_remaining_secs": round(cooldown_remaining),
            "idle_minutes":            0,
        })

    return d


@_flask_app.route("/health")
def health():
    return jsonify({
        "ok":         True,
        "version":    "phase4-v2.0",
        "uptime_min": int((time.time() - _start_time) / 60),
    })


@_flask_app.route("/think")
def think():
    try:
        buying_power = 0.0
        total_value  = 0.0
        try:
            from phase4 import get_buying_power, get_all_positions, get_current_price
            buying_power = round(get_buying_power(), 2)
            positions    = get_all_positions()
            for sym, pos in positions.items():
                qty    = float(pos.qty)
                price  = get_current_price(sym) or float(pos.avg_entry_price)
                total_value += qty * price
            total_value = round(total_value + buying_power, 2)
        except Exception:
            pass

        bots_dict = {}
        for bot in _bots:
            try:
                bots_dict[bot.symbol] = _bot_state_dict(bot)
            except Exception as e:
                bots_dict[bot.symbol] = {"symbol": bot.symbol, "error": str(e)}

        now = datetime.now(tz=CENTRAL)
        return jsonify({
            "online":       True,
            "version":      "V2.0",
            "timestamp":    now.strftime("%H:%M:%S CDT"),
            "bots":         bots_dict,
            "buying_power": buying_power,
            "total_value":  total_value,
        })
    except Exception as e:
        return jsonify({"online": True, "error": str(e), "bots": {}}), 500


def start_server(bots: list, port: int = None):
    global _bots
    _bots = bots
    if port is None:
        port = int(os.environ.get("PORT", 8081))

    def _run():
        print(f"[PHASE4-SERVER] Flask /think server starting on port {port}", flush=True)
        _flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

    t = threading.Thread(target=_run, daemon=True, name="Phase4FlaskServer")
    t.start()
    print(f"[PHASE4-SERVER] ✅ /think endpoint live on port {port}", flush=True)
    return t
