"""
phase4_server.py — Thin Flask control server for nexus-phase4.
V2.2: TOKEN LOCKDOWN — all routes except /health require X-Nexus-Token
      == NEXUS_INTERNAL_TOKEN. /close_all and /resume were publicly
      reachable with zero auth. Fail-open only when the env var is unset.
V2.0: Updated for Alpaca migration.
- Removed: bot.prices, bot.bear_prices, bot.acct_id references
- Uses: get_current_price(), get_buying_power(), get_all_positions() (no args)
- Added: daily_limit_hit, entry_notional, proper entry_time tracking
"""
import os
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request

CENTRAL     = ZoneInfo("America/Chicago")
_flask_app  = Flask("phase4_server")
_bots       = []
_start_time = time.time()

# V2.2: TOKEN LOCKDOWN. This server answers on a PUBLIC up.railway.app
# domain and exposes POST /close_all (flattens every Phase4 position) and
# POST /resume -- previously open to anyone who found the URL. Every
# request except /health must carry X-Nexus-Token matching
# NEXUS_INTERNAL_TOKEN. Fail-open ONLY if the env var is unset (loud boot
# warning) so a missed variable degrades to pre-V2.2 behavior instead of
# breaking the killswitch. Fleet Commander's nexus_client already sends
# the header on every call.
_NEXUS_TOKEN  = os.environ.get("NEXUS_INTERNAL_TOKEN", "")
_PUBLIC_PATHS = {"/health"}

@_flask_app.before_request
def _require_nexus_token():
    if request.path in _PUBLIC_PATHS:
        return None
    if not _NEXUS_TOKEN:
        return None   # unset var = fail-open (warned at server start)
    if request.headers.get("X-Nexus-Token", "") != _NEXUS_TOKEN:
        return jsonify({"error": "unauthorized"}), 401


def _bot_state_dict(bot) -> dict:
    now = time.time()
    d   = {
        "symbol":          bot.symbol,
        "bear_pair":       bot.bear_pair,
        "in_position":     bot.in_position,
        "mode":            bot.mode,
        "daily_wins":      bot.daily_wins,
        "daily_losses":    bot.daily_losses,
        "daily_pnl":       round(bot.daily_pnl * 100, 3),
        "daily_limit_hit": getattr(bot, "_daily_limit_hit", False),
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
            "mfe":           round(getattr(bot, "mfe", 0) * 100, 3),
            "mae":           round(getattr(bot, "mae", 0) * 100, 3),
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
        })

    return d


@_flask_app.route("/health")
def health():
    return jsonify({
        "ok":         True,
        "version":    "phase4-v2.2",
        "uptime_min": int((time.time() - _start_time) / 60),
    })


@_flask_app.route("/close_all", methods=["POST"])
def close_all():
    """
    V2.1: Killswitch hook. Called by main.py /killswitch (PHASE4_URL env).
    Closes ONLY the bots' tracked positions -- Phase4 shares the Alpaca
    account with Berserker, so a blanket close-all-account-positions here
    would clobber Berserker's stocks. Sets bot.kill_paused so no bot
    re-enters; exit management keeps running for anything that survives.
    """
    import phase4
    closed, failed = [], []
    for bot in _bots:
        bot.kill_paused = True
        try:
            if not bot.in_position:
                continue
            try:
                cp = phase4.get_current_price(bot.active_sym) or bot.entry_price
            except Exception:
                cp = bot.entry_price
            pnl = ((cp - bot.entry_price) / bot.entry_price
                   if bot.entry_price > 0 else 0.0)
            if bot.try_sell("killswitch", pnl):
                closed.append(bot.active_sym)
            else:
                # Position may already be gone (closed manually or by a
                # broker-level close). If the account doesn't hold it,
                # clear the bot's phantom state instead of failing.
                try:
                    held = phase4.get_all_positions()
                except Exception:
                    held = {}
                if bot.active_sym not in held:
                    bot.in_position = False
                    bot.entry_price = 0.0
                    bot.entry_time  = 0.0
                    bot.trade_id    = ""
                    closed.append(f"{bot.active_sym}(already-flat)")
                else:
                    failed.append(bot.active_sym)
        except Exception as e:
            failed.append(f"{bot.symbol}:{e}")
    print(f"[PHASE4-SERVER] KILLSWITCH close_all: closed={closed} failed={failed} -- entries PAUSED", flush=True)
    return jsonify({"ok": len(failed) == 0, "closed": len(closed),
                    "closed_syms": closed, "failed": failed, "paused": True})


@_flask_app.route("/resume", methods=["POST"])
def resume():
    """V2.1: clears the killswitch pause (called by main.py /resume)."""
    for bot in _bots:
        bot.kill_paused = False
    print("[PHASE4-SERVER] Killswitch pause CLEARED -- entries re-enabled", flush=True)
    return jsonify({"ok": True, "paused": False})


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
                qty   = float(pos.qty)
                price = get_current_price(sym) or float(pos.avg_entry_price)
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

    if not _NEXUS_TOKEN:
        print("[PHASE4-SERVER] \u26a0 NEXUS_INTERNAL_TOKEN not set -- "
              "/close_all and /resume are UNAUTHENTICATED (fail-open)", flush=True)

    def _run():
        print(f"[PHASE4-SERVER] Flask /think server starting on port {port}", flush=True)
        _flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

    t = threading.Thread(target=_run, daemon=True, name="Phase4FlaskServer")
    t.start()
    print(f"[PHASE4-SERVER] ✅ /think endpoint live on port {port}", flush=True)
    return t
