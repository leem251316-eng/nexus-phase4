"""
phase4_server.py — Thin Flask control server for nexus-phase4.
Runs as a daemon thread alongside the bot worker threads.
Exposes /think and /health so nexus-commander can query live bot state.

Add to phase4.py run() function:
    from phase4_server import start_server
    start_server(bots)

Add to Railway nexus-phase4 env vars:
    PORT=8081  (or any free port — set PHASE4_URL in nexus-commander to match)
"""
import os
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify

CENTRAL    = ZoneInfo("America/Chicago")
_flask_app = Flask("phase4_server")
_bots      = []   # populated by start_server()
_start_time = time.time()


def _bot_state_dict(bot) -> dict:
    """
    Extract serializable state from a SymbolBot instance.
    Returns everything /think and the watchdog need.
    """
    now = time.time()
    d   = {
        "symbol":       bot.symbol,
        "bear_pair":    bot.bear_pair,
        "in_position":  bot.in_position,
        "mode":         bot.mode,
        "daily_wins":   bot.daily_wins,
        "daily_losses": bot.daily_losses,
        "daily_pnl":    round(bot.daily_pnl, 3),
    }

    if bot.in_position:
        # Current price from cached prices list
        prices        = (bot.bear_prices if bot.active_sym == bot.bear_pair
                         else bot.prices)
        current_price = prices[-1] if prices else bot.entry_price
        peak          = bot.peak_price or bot.entry_price
        pnl_pct       = ((current_price - bot.entry_price) / bot.entry_price * 100
                         if bot.entry_price > 0 else 0)
        peak_pct      = ((peak - bot.entry_price) / bot.entry_price * 100
                         if bot.entry_price > 0 else 0)

        # Held time — estimate from cooldown state (no entry_time stored on bot)
        # Use a reasonable fallback
        held_min = 0
        if hasattr(bot, '_entry_ts') and bot._entry_ts:
            held_min = int((now - bot._entry_ts) / 60)

        # Exit params
        try:
            sl, ratchet, trail = bot.get_exit_params()
        except Exception:
            sl, ratchet, trail = 0, 0, 0

        d.update({
            "active_sym":   bot.active_sym,
            "entry_price":  round(bot.entry_price, 4),
            "current_price": round(current_price, 4),
            "peak_price":   round(peak, 4),
            "pnl_pct":      round(pnl_pct, 3),
            "peak_pct":     round(peak_pct, 3),
            "held_minutes": held_min,
            "stop_pct":     round(sl, 4),
            "ratchet_pct":  round(ratchet, 4),
            "trail_pct":    round(trail, 4),
        })
    else:
        # Idle bot — include RSI and reversal state
        rsi = None
        try:
            from phase4 import compute_rsi
            rsi = compute_rsi(bot.prices)
        except Exception:
            pass

        cooldown_remaining = max(0, bot.cooldown_until - now) if bot.cooldown_until > now else 0

        # Idle time estimation
        idle_min = 0
        if hasattr(bot, '_last_active_ts') and bot._last_active_ts:
            idle_min = int((now - bot._last_active_ts) / 60)

        d.update({
            "rsi":                    round(rsi, 2) if rsi is not None else None,
            "reversal_state":         bot.reversal_state.get("state", "IDLE"),
            "cooldown_remaining_secs": round(cooldown_remaining),
            "idle_minutes":           idle_min,
        })

    return d


@_flask_app.route("/health")
def health():
    return jsonify({"ok": True, "version": "phase4-v1.0", "uptime_min": int((time.time() - _start_time) / 60)})


@_flask_app.route("/think")
def think():
    """
    Full live state for all 4 bots.
    Used by nexus-commander /think command and watchdog thread.
    """
    try:
        # Buying power — pull from the first bot's acct_id
        buying_power = 0.0
        total_value  = 0.0
        if _bots:
            try:
                from phase4 import get_buying_power, get_all_positions
                acct_id      = _bots[0].acct_id
                buying_power = round(get_buying_power(acct_id), 2)
                positions    = get_all_positions(acct_id)
                # Estimate total value
                from phase4 import get_current_price
                for sym, pos in positions.items():
                    qty    = float(pos.get("quantity", pos.get("position_qty", 0)))
                    price  = get_current_price(sym) or 0
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
            "online":        True,
            "version":       "V1.0",
            "timestamp":     now.strftime("%H:%M:%S CDT"),
            "bots":          bots_dict,
            "buying_power":  buying_power,
            "total_value":   total_value,
        })
    except Exception as e:
        return jsonify({"online": True, "error": str(e), "bots": {}}), 500


def start_server(bots: list, port: int = None):
    """
    Launch the Flask server in a daemon thread.
    Call this from phase4.py run() after bots are created.

    Args:
        bots: list of SymbolBot instances
        port: port to listen on (defaults to PORT env var or 8081)
    """
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
