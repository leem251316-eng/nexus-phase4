# -*- coding: utf-8 -*-
import time
import os
import json
import secrets
import threading
import requests
import pandas as pd
from collections import deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.data.enums import DataFeed

import shared_state
import trade_log
from capital_coordinator import CapitalCoordinator, NO_COORDINATION

try:
    import yfinance as yf
    _yfinance_ok = True
except ImportError:
    _yfinance_ok = False

try:
    import psycopg2
    import psycopg2.extras as _pg_extras
    _psycopg2_ok = True
except ImportError:
    _psycopg2_ok = False

try:
    import scanner
    _scanner_ok  = True
    _scanner_err = ""
except Exception as e:
    _scanner_ok  = False
    _scanner_err = str(e)

try:
    import options_engine
    _options_ok  = True
    _options_err = ""
except Exception as e:
    _options_ok  = False
    _options_err = str(e)

import nexus_client

# ==============================================================================
# NEXUS TRADING SYSTEM V10.35
# V10.35: PatternMemory hygiene + fill-anchored fingerprints + Phase4 kill hook (Jul 4 2026)
#   ✅ Berserker PatternMemory run_analysis() was completely UNFILTERED --
#      the win-rate buckets that block live entries (and V10.19 dynamic-TP
#      stats) were fed by the aggressive paper twin's deliberately ungated
#      entries plus pre-go-live masquerade rows. Same failure mode as the
#      crypto threshold ratchet fixed in V5.5. Now applies the V10.34
#      strategist rule: bt_ bulk stays (deliberate food), live rows must be
#      entry_ts >= GATE_LAUNCH_DATE, paper-twin rows never feed the gate.
#   ✅ Fingerprint entry price is now the ACTUAL Alpaca fill
#      (get_order_by_id poll after submit, quote fallback) instead of the
#      last polled IEX quote. TP/SL math was already fill-true via
#      pos.avg_entry_price; this fixes the recorded anchor Exit Autopsy
#      reconstructs exit prices from.
#   ✅ /killswitch now closes Phase4 too: POSTs to Phase4 V2.7's new
#      /close_all endpoint when PHASE4_URL env is set on this service
#      (use the Railway internal URL). Unset = old manual-close note.
# V10.34: Strategist data-hygiene (Jul 4 2026)
#   ✅ /run_strategist's pull now applies the go-live floor: backtest (bt_)
#      rows remain the intended bulk food, but pre-Jun-30 paper rows that
#      were backfilled is_paper=FALSE no longer masquerade as live history.
#      Same fix class as the V10.31 Win Follower pass. Pipeline verified
#      end-to-end: writes /app/data/strategy_recipes.json (persistent
#      volume), /reload_recipes reads the same path, units consistent.
# V10.33: Options Engine audition program (Jul 3 2026 night)
#   ✅ options_engine V1.4 -- Tier-1 PAPER shadow entries (6h cooldown, live
#      money still requires the full 4/4 Tier-2 signal), paper universe
#      widened to MSTR/COIN + BITO/MARA (live stays MSTR/COIN), signal_tier
#      fingerprinted, and /options_live is now GRADUATION-GATED: refuses
#      until >= 15 closed paper trades at >= 55% WR with positive avg P&L.
#      '/options_live force' overrides. /options_stats shows the audition:
#      graduation progress + per-tier per-underlying paper breakdown.
# V10.32: Thorn/Autopsy phone access + equity Exit Autopsy (Jul 3 2026 night)
#   ✅ /thorn [hours] and /autopsy [days] T-Bone commands -- Thorn digest and
#      exit grades on demand from the phone instead of raw endpoint URLs.
#   ✅ equity_exit_autopsy: every Berserker AND Scanner exit (all paths --
#      TP/SL/trail/EOD, live + paper) is recorded via the shared
#      record_exit hook with capture-ratio inputs; a resolver samples the
#      latest trade AT each 15m/1h/4h horizon to grade post-exit
#      continuation. Same "leave nothing on the table" measurement crypto
#      V5.7 got. bt_ rows excluded at the source.
# V10.31: Win Follower data honesty pass (Jul 3 2026, ~3:45 PM)
#   ✅ Live classification floored at GATE_LAUNCH_DATE (Jun 30 go-live).
#      is_paper was added in V10.17 with DEFAULT FALSE, backfilling all
#      pre-V10.17 paper fingerprints as "live" -- that's where MSTR's
#      "25 live trades / WR 0%" and the GEO/CXW/SMCI HOT badges came from.
#      A live trade cannot predate live trading. Expect all benches to
#      release and HOT badges to clear on first refresh: with ~3 days of
#      real live data, honest tiers ARE mostly NEUTRAL until trades accrue.
#      WARM path (V10.17+ paper, is_paper=TRUE) unaffected and still feeds
#      symbols into rotation.
#   ✅ pnl display: berserker_trade_fingerprints stores pnl_pct as PERCENT
#      (x100 at write); WF multiplied by 100 again (GEO "+1223.6%").
# V10.30: Hotfix set (Jul 3 2026 PM)
#   ✅ /help fixed -- "/scanner_aggro <0.5-1.5>" (V10.28) broke Telegram's
#      HTML parser; the 400 was swallowed silently, so /help just never
#      arrived. Escaped, AND alert() now detects rejected sends and retries
#      as plain text -- no alert can ever be silently dropped again.
#   ✅ Unknown slash-commands now reply "try /help" instead of vanishing.
#   ✅ Win Follower excludes backtester fingerprints (trade_id 'bt_%%').
#      Backtesters stamp exit_ts with WRITE time, so Sunday runs looked like
#      thousands of fresh live trades -- NUE/SPCX were benched on backtest
#      rows. Benches self-validate each refresh and release when the clean
#      live record doesn't support them (also = natural 14d bench expiry).
#   ✅ Scanner thread launch is signature-adaptive + alerts if it dies at
#      boot (was: TypeError killed it instantly, boot logged "alive: False",
#      nobody was told, Scanner silently traded nothing).
# V10.29: Berserker Win Follower -- follow-the-wins allocation (Jul 3 2026)
#   Rolling 14-day per-symbol performance (berserker_trade_fingerprints,
#   live + paper) tiers every symbol hourly. Same architecture as crypto
#   V5.3's WinFollower -- capital and priority migrate toward what's winning.
#   ✅ HOT  (live WR >= 55%, >= 5 trades, net positive): size x1.30, win-rate
#      gate discount -7pts (floored at the 40% mathematical breakeven for
#      1.5%TP/1.0%SL -- never below), and HOT symbols scan FIRST in the buy
#      loop instead of least-recently-traded rotation. Winners get fed.
#   ✅ WARM (thin live sample, paper WR >= 55% over >= 10 paper trades):
#      gate discount -4pts. Paper-proven symbols earn their way into live.
#   ✅ COLD (live WR <= 35% over >= 6 trades -- below 40% breakeven):
#      BENCHED from live entries. Paper Berserker keeps trading it; the
#      symbol auto-returns (with alert) once its last 10 paper trades SINCE
#      benching hit >= 50% WR. Bench persists in nexus_config table --
#      survives redeploys. Exits/positions never affected.
#   ✅ get_sorted_symbols(): HOT first, benched excluded, rest keep rotation.
#      The old order was pure least-recently-traded -- a fairness rotation
#      that actively starved winners to give losers equal turns.
#   ✅ /followwins T-Bone command -- tiers across Berserker + Scanner + Crypto.
#   ✅ Buy alerts tagged [HOT]/[WARM]. T-Bone alert on every promote/bench/return.
#   ✅ Raw fingerprints unchanged -- tier effects touch gating/sizing/priority
#      only, so pattern memory data stays honest.
# V10.28: Scanner circuit breaker override + dead regime-score factor fix (Jun 30 2026)
#   ✅ Added /scanner_cb_override on|off -- Scanner's breadth-based circuit
#      breaker (15/20-of-44-symbols-dropping check) had zero remote control
#      surface at all: no T-Bone command, no flag, nothing. It tripped three
#      times today (9:57am, 10:20am, 1:09pm CDT) on a day with confirmed
#      normal VIX (17.0) and bullish SPY -- likely tied to the sample's heavy
#      weighting toward volatility products (VXX/UVXY mechanically falling as
#      VIX normalized) and quarter-end rebalancing flows hitting XLK/QQQ
#      specifically, neither a real broad selloff. Sets scanner.
#      _circuit_breaker_override directly (Scanner runs in this same process
#      via `import scanner`, confirmed at line ~38) -- does NOT touch
#      paused_scanner (separate, unrelated full-stop flag); only bypasses the
#      breadth-pause specifically, win-rate gate/position limits/capital
#      coordination all stay active. /think's Scanner line now shows
#      🟢OVERRIDE when active.
#   ✅ Found while building the above: main.py's compute_regime_score()
#      Factor 2 calls scanner.is_circuit_breaker_active() via hasattr() --
#      but that function never existed in scanner.py, so hasattr() silently
#      returned False every single time and Factor 2 has NEVER fired since
#      it was written. Added is_circuit_breaker_active() to scanner.py
#      (V2.5) so the regime score actually reflects Scanner's breadth state
#      going forward, and wired it to respect the same override so the
#      regime score and the actual buy-pausing behavior never disagree.
# V10.27: Capital coordinator gap -- Scanner's 10% wasn't covered (Jun 30 2026)
#   ✅ Caught via direct question while reviewing the V10.25 capital
#      coordination work: the Berserker<->Phase4 clamp in execute_trade()
#      was checking get_available(total_cash) -- the FULL account -- not
#      get_available(budgets["berserker"]), Berserker's actual 90% share.
#      That meant Berserker's cross-service-coordinated spend could
#      legitimately eat into Scanner's reserved 10% as long as the combined
#      Berserker+Phase4 total stayed under the whole account, because the
#      coordinator only knew about the Berserker-vs-Phase4 boundary, not the
#      Berserker-vs-Scanner one (a separate in-process convention via
#      shared_state.get_budgets() that predates and was never wired into the
#      coordinator). Fixed: now clamps against budgets["berserker"] specifically.
#   ✅ Second, related gap: Scanner itself (scanner.py) had ZERO capital
#      coordinator awareness at all -- it computed its 10% budget off raw
#      total_cash with no knowledge of Phase4's reservations, so Phase4
#      could be mid-order on a large reservation while Scanner simultaneously
#      tried to spend money that was actually already committed elsewhere in
#      the same account. Scanner now has its own CapitalCoordinator instance
#      (service_name="scanner"), clamps trade_amount against
#      get_available(scanner_budget) before buying, and reserves/releases
#      around its own order submission -- identical pattern to Berserker and
#      Phase4. All three services (Berserker, Scanner, Phase4) now correctly
#      coordinate against the same shared_reservations table, each clamped
#      to its own legitimate share of the account rather than the raw total.
#   Verified via simulation: in every reservation scenario (including Phase4
#   reserving more than Scanner's entire 10% share), the sum of all
#   available-capital checks across services never exceeds the true account
#   total -- conservative by construction, get_available() already floors
#   at 0 rather than going negative.
# V10.26: Real VIX replaces VIXY proxy (Jun 30 2026, first live trading day)
#   ✅ Caught live, first morning of trading: T-Bone showed "VIX: 32.5
#      (EXTREME)" while actual market VIX was ~17.5 (calm, normal day --
#      confirmed via Yahoo Finance/CNBC/multiple sources, all converging on
#      17.5-18.6). This wasn't a coding bug in the *= 1.5 multiplier or the
#      threshold checks -- both were correct -- the VIXY-as-proxy assumption
#      itself had drifted. VIXY is benchmarked to an index of ROLLING VIX
#      FUTURES, not spot VIX (ProShares' own docs: VIXY "can be expected to
#      perform very differently from the VIX... on a daily basis and over
#      time," partly from futures-roll/contango decay). The "$10-25 VIXY,
#      $12-40 VIX, ratio ~1.5x" relationship documented in V10.19 was a
#      snapshot of conditions at that time, not a stable constant. By Jun 30
#      VIXY was trading ~$22-24 while spot VIX sat ~17.5 -- the formula
#      computed 22-24 * 1.5 = 33-36, blowing past BOTH VIX_BLOCK_THRESHOLD
#      (25) and VIX_EXTREME_THRESHOLD (30) on a day with normal volatility.
#      This silently hard-blocked all new Berserker entries and capped
#      MAX_POSITIONS to 1 on the very first live day, and inflated
#      compute_regime_score()'s VIX factor (+1 for "VIX>25 high fear") when
#      it shouldn't have fired.
#   ✅ Fix: added _fetch_real_vix(), pulling ^VIX directly via yfinance
#      (same yf.Ticker(...).fast_info.last_price pattern already proven live
#      in phase4.py and in main.py's own earnings-calendar fetch). Cached on
#      a 90s TTL since yfinance shouldn't be hit every ~30s sweep. VIXY*1.5
#      is kept ONLY as a fallback for when yfinance is fully unavailable, so
#      the VIX gate never goes completely blind -- but it's no longer the
#      primary signal. New module-level _vix_source ("yfinance" /
#      "vixy_proxy" / "none") is now visible in /vix so a future drift or
#      fallback is immediately diagnosable instead of silently wrong again.
#      Every consumer (get_vix_status, compute_regime_score,
#      vix_max_positions, /vix, /think, /status, morning brief) reads the
#      same _vix_level_smooth global, so this one fix corrects all of them.
# V10.25: Cross-service capital coordination (Jun 30 2026)
#   ✅ Confirmed tonight: Berserker (this service) and Phase4 (separate
#      Railway service/process, nexus-phase4/glorious-achievement) trade
#      against the SAME live Alpaca account -- one account was ever created;
#      paper trading was layered on top of it, never a second account. Each
#      service was independently calling Alpaca's buying_power and sizing
#      trades with zero knowledge of the other's outstanding orders. No
#      existing safety net covered this: shared_state.py's claim()/release()
#      registry is an in-process Python module shared between main.py and
#      scanner.py (same container, same process) and cannot reach Phase4 --
#      module state doesn't cross process or service boundaries.
#   ✅ Added capital_coordinator.py -- coordinates through Postgres instead,
#      since both services already share DATABASE_URL. Before sizing a BUY,
#      execute_trade() now calls get_available() to clamp the intended spend
#      against what Phase4 might currently have reserved, then reserve()s
#      immediately before order submission and release()s in a finally block
#      immediately after -- whether the order succeeds or fails. TTL-based
#      auto-sweep (90s) means a crashed service can never permanently lock
#      out capital. Fails open throughout: any DB/connection problem falls
#      back to the exact pre-V10.25 behavior (raw buying_power, uncoordinated)
#      rather than blocking a trade. Connection timeout capped at 3s so an
#      unreachable DB can't stall the trade loop waiting on a TCP timeout.
#   ✅ Identical fix applied to phase4.py (V2.1) -- both services tag their
#      reservations with their own service name and reserve/release around
#      their respective order-submission calls.
# V10.24: T-Bone cleanup sweep (Jun 30 2026)
#   ✅ Fixed 81 mangled " ? " separators (should have been " — " em-dashes)
#      scattered across log lines, alerts, and version-history comments --
#      likely from a local editor that couldn't render the original
#      character and silently substituted "?" on save.
#   ✅ Fixed 6 mangled emoji placeholders that were leaking literal
#      bracket-text into live T-Bone messages and Railway logs:
#      "[WATCH][STEAK]" -> 👀🥩 (morning brief), "[RUN]" -> ▶️ (both
#      /resume confirmation and the recurring sweep log line -- this is
#      what was showing up as "[RUN]" in Railway logs), "[YLW]" -> 🟡
#      (/friday on confirmation and status line), and two bare "?"
#      placeholders -> ❓ (symbol-not-found message) and ⏳ (Phase4 idle
#      watchdog alert, matching the ⏳ convention used by the adjacent
#      "holding too long" watchdog alerts).
#   ✅ Fixed stale "(Webull)" labels across 6 locations -- Phase4 migrated
#      to Alpaca in its own V2.0 (confirmed live), but /think, /help,
#      /equity, /wins, /killswitch, and find_and_close() were all still
#      describing it as Webull. /equity's webull_val variable renamed to
#      phase4_val and relabeled "Alpaca (Phase4)" -- the underlying math
#      was already correct (Phase4 runs its own separate Alpaca account
#      via ALPACA_PHASE4_API_KEY, so summing it with Berserker's account
#      was never double-counting), only the broker label was wrong.
#   ✅ Flagged (not fixed -- needs a real decision) a genuine gap surfaced
#      while fixing the Webull labels: /killswitch does not actually close
#      Phase4 positions, only notes "manual close required." Help text and
#      the killswitch completion message now say this explicitly instead
#      of implying full coverage.
#   ✅ Audited /help against every implemented command (cmd ==/cmd.startswith
#      across all 36 commands) -- found zero actually missing; the earlier
#      "7 missing" read was a false positive from a too-strict comment
#      pattern (e.g. "/buys on|off" wasn't matched by a regex expecting an
#      em-dash immediately after the command name).
#   No logic, gating, or trading behavior changed in this pass -- pure
#   string/label correctness pass, verified by identical line count and
#   AST syntax check before/after.
# V10.23: PDT removal (Jun 30 2026)
#   ✅ FINRA retired the Pattern Day Trader rule effective Jun 4 2026
#      (Regulatory Notice 26-10, replacing Rule 4210's day-trading margin
#      provisions with an intraday margin standard). Alpaca implemented
#      same-day, removing pattern_day_trader/daytrade_count/
#      daytrading_buying_power from the Trading API (full field removal by
#      Jul 6 2026 per Alpaca's migration notice).
#   ✅ Removed all PDT state and logic: _pdt_info, pdt_blocked set,
#      refresh_pdt_info(), get_pdt_rolloff_dates(), get_pdt_slots_remaining(),
#      pdt_warning_message(), is_pdt_error(), handle_pdt_block().
#   ✅ Removed should_exit_now() entirely -- this function throttled all
#      three exit paths (take-profit, stop-loss, trailing-stop) to ration
#      day-trade slots that no longer exist. Exits now fire immediately on
#      their own trigger conditions with no artificial delay.
#   ✅ manage_exits() no longer skips PDT-blocked symbols -- this was a real
#      bug: positions held overnight under the old PDT logic were excluded
#      from exit management entirely (no stop-loss, no take-profit, no
#      trailing-stop checks) until the block cleared. Every open position is
#      now evaluated for exit every cycle.
#   ✅ execute_trade() no longer blocks new buys based on day-trade slot
#      count -- this was a real entry blocker, not just a display issue.
#   ✅ BerserkerMemory._bucket_key() drops the pdt_used dimension (was
#      splitting every bucket into pdt_ok/pdt_tight halves for a constraint
#      that no longer exists). get_win_rate(), should_skip_entry(), and
#      get_dynamic_tp() all updated to match. Buckets consolidate going
#      forward, improving sample size per bucket for the win-rate gate and
#      dynamic TP. The pdt_slots_used DB column is kept (written as a
#      constant 0) for historical fingerprint continuity -- not dropped, to
#      avoid a schema migration and preserve the existing 19,000+ trade
#      history.
#   ✅ Removed /pdt T-Bone command and all PDT display lines from /status,
#      /think, /pnl, /help, boot alert, morning brief, daily report, and
#      sweep logging.
# V10.22: Win-rate gate cold-start ramp (Jun 30 2026)
#   ✅ WIN_RATE_GATE_THRESHOLD now ramps over time instead of a static 57%.
#      Problem: 57% is the correct long-run breakeven gate, but pattern
#      memory needs live trades to populate meaningful bucket win rates.
#      A near-empty live dataset means almost nothing clears 57%, so
#      Berserker can sit idle for days -- starved of the exact data it
#      needs to ever pass the gate. Ramp: 45% (days 0-14) -> 50% (days
#      14-28) -> 57% (day 28+), keyed off GATE_LAUNCH_DATE (Jun 30 2026).
#      45% is still EV-positive at current 1.5%TP/1.0%SL (EV=+0.125%/trade),
#      just thinner margin than 57%. Re-evaluated daily in reset_daily_state()
#      so it actually ramps on a long-lived Railway service, not just at boot.
#      /status now shows current gate threshold for visibility.
# V10.21: MFE double-multiply fix (Jun 29 2026)
#   ✅ FIX — mfe/mae stored as pct pts in DB (e.g. 1.5 = 1.5%). run_analysis()
#      was doing float(mfe_val) * 100 → 150.0, making dynamic TP thresholds
#      (avg_mfe >= 2.0, >= 3.0) compare against 100x-inflated values. Dynamic
#      TP was effectively dead — no bucket ever cleanly met the threshold.
#      Same bug in run_strategy_pipeline() winners_mfe and sym_data mfe lists.
#      All three * 100 multiplications removed. pnl_pct in run_strategy_pipeline
#      also fixed (was * 100, same issue — pnl_pct already stored as pct pts).
# V10.20: Five-fix audit pass (Jun 29 2026)
#   ✅ FIX 1 — VIXY multiplier corrected 10.0→1.5 in _update_spy_qqq_history().
#      ×10 made vix_smooth=100-250 permanently, killing the VIX gate, regime
#      factor 5, and vix_max_positions() — all dead code since V10.19 deploy.
#      ×1.5 is correct (VIXY $10-25, VIX $12-40; backtester already used 1.5).
#   ✅ FIX 2 — Analyst scores wired into Berserker confluence as signal 5 of 5.
#      Analyst watches the same symbols 24/7; score ≥ 1 counts as confirmation.
#      Soft gate only — agreement adds confluence, absence never blocks.
#   ✅ FIX 3 — winners_mfe computation in run_strategy_pipeline() corrected.
#      Was zip(d["mfe"], rows) — misaligned (per-symbol list vs full rows).
#      Now iterates rows directly filtered by symbol+won, same pattern as
#      BerserkerMemory.run_analysis(). Dynamic TP from /run_strategist now correct.
#   ✅ PRIORITY 1 — Earnings Calendar Blackout
#      check_earnings_calendar() called once/hour (not every sweep).
#      Checks yfinance .calendar for every SYMBOL. Symbols with earnings
#      within 48h block new entries (earnings_blocked set). Symbols with
#      earnings within 24h + open position force-closed immediately.
#      /earnings T-Bone command shows current blackout status.
#      4-hour cache prevents API hammering. Alpaca news events checked
#      as primary with yfinance as fallback/confirmation.
#   ✅ PRIORITY 2 — VIX Regime Gate
#      VIXY (VIX ETF) fetched alongside SPY/QQQ each sweep via Alpaca IEX.
#      5-bar smoothed VIX stored in _vix_history deque + shared_state.
#      VIX < 20: normal | 20-25: log warning | >25: block entries |
#      >30: block entries + MAX_POSITIONS → 1. /vix added to T-Bone.
#      VIX shown in /status, /think, morning brief.
#   ✅ PRIORITY 3 — Automated Weekly Strategy Pipeline
#      /run_strategist T-Bone command triggers: DB pull → backtest_log
#      build → strategist analysis → results posted to T-Bone.
#      Also writes strategy_recipes.json to /app/data/ for persistence.
#      Berserker reads the file at boot and daily reset; merges with
#      hardcoded BERSERKER_RECIPES as fallback. /reload_recipes forces
#      a re-read and logs what changed.
#   ✅ PRIORITY 4 — Dynamic TP from MFE Distribution
#      BerserkerMemory.run_analysis() now computes avg_winner_mfe and
#      pct_reach_2/pct_reach_3 per bucket (stored in berserker_pattern_stats).
#      execute_trade() sets per-trade dynamic_tp in _berserker_fingerprints.
#      manage_exits() uses dynamic_tp if set, falling back to recipe TP.
#      Gate: bucket needs 30+ winner samples, dynamic TP never < recipe TP.
#      T-Bone exit alerts show when dynamic TP fired.
#   ✅ PRIORITY 5 — Regime Aggregator
#      compute_regime_score() sums 5 factors: CB active, scanner CB active,
#      F&G < 25, SPY below MA20, VIX > 25. Score 0-5 broadcast via
#      shared_state. 0-1: normal | 2: reduce size 25% | 3: reduce 50%
#      no scanner | 4-5: shutdown all entries + alert.
#      /regime T-Bone command shows score + factors.
# V10.18: Fresh 16,616-trade Railway backtest — recipe refresh (Jun 27 2026)
#   ✅ SMCI sl corrected 1.5%->1.0% -- strategist fresh run confirms 1.0%
#      optimal stop (V10.17 had 1.5% from prior data; new 347-trade dataset
#      says 1.0%). All other TP/SL values confirmed unchanged.
#   ✅ All avoid_hours confirmed vs fresh strategist output -- no changes.
#   ✅ AMD/META/MSFT cuts confirmed: WR 33.1%/32.6%/31.3%, all EV negative
#      even at tightest stop. Correct to exclude.
#   ✅ SPCX: 1.5% SL confirmed (35% of winners touch -1.5% -- wider stop
#      needed). Score 3 subset hits 51% WR (126 trades) -- EV positive at
#      that filter level; pattern memory gate handles the rest.
#   ✅ CLSK: TP stays 1.5% -- +2%=20% tail is real but changing TP to 2.0%
#      requires 57% breakeven WR; trailing ratchet already captures the tail.
#   ⚠️  Avg MFE/MAE = 0.0% in strategy_report.txt -- retrieve_results.py
#      not populating mfe/mae fields correctly. Does not affect TP/SL/hours
#      derivation. Fix retrieve_results.py in next session.
# V10.17: Per-symbol TP/SL + is_paper fingerprinting + gate calibration
#   ✅ Per-symbol take-profit and stop-loss in BERSERKER_RECIPES derived from
#      strategist optimal_tp/optimal_stop (milestone-based, not hardcoded).
#      manage_exits() reads recipe TP/SL per symbol; global TAKE_PROFIT_PCT
#      and STOP_LOSS_PCT are now fallbacks only.
#   ✅ is_paper column added to berserker_trade_fingerprints -- live vs paper
#      trades now distinguishable in DB. _write_entry() passes is_paper flag.
#      BerserkerMemory.init_tables() runs ALTER TABLE IF EXISTS to add column
#      to existing DB without wiping data.
#   ✅ WIN_RATE_GATE_THRESHOLD raised 45%->57% -- 45% was EV=-0.425%/trade at
#      1.5%TP/1.0%SL. 57% is the actual mathematical breakeven (SL/(TP+SL)).
#      Only buckets with proven positive EV now pass the gate.
#   ✅ scanner.py V2.2 boot string corrected (was printing V2.1)
# V10.16: Full system audit — efficiency + signal quality overhaul
#   ✅ STOP_LOSS_PCT tightened 2.0%->1.0% -- strategist recommended 1.0% for
#      13/15 symbols based on 16,339-trade backtest. At 1.5%TP/1.0%SL breakeven
#      WR drops from 57.1% to 40.0%, converting several marginal symbols to
#      positive or near-breakeven EV.
#   ✅ Removed MSFT (31% WR), AMD (33% WR), META (33% WR) -- EV negative even
#      at 1.0% stop after 650-1300 trades of evidence. Same logic that removed
#      AMZN/GOOGL/CCJ/COIN. TECH_GROWTH now: NVDA/TSLA/AAPL/SMCI/SPCX
#   ✅ Price fetching batched: 32 API calls/sweep -> 3 (all SYMBOLS + SPY/QQQ
#      in one StockLatestTradeRequest each for live, paper, and SPY/QQQ).
#      Dramatically reduces Alpaca IEX rate limit exposure.
#   ✅ MACD double-computation fixed: compute_macd() now returns line, signal,
#      AND histogram so get_signals() confluence check reuses existing values
#      instead of rebuilding the full EWM chain from scratch.
#   ✅ price_history, _paper_price_history, _spy_history, _qqq_history converted
#      from list+pop(0) to collections.deque(maxlen=N) -- O(1) vs O(n) appends,
#      consistent with crypto.py's existing pattern.
#   ✅ Dead code removed: get_webull_positions(), get_position_pnl_webull() --
#      never called anywhere, Webull is Phase4's domain.
#   ✅ Inline imports moved to top-level: secrets (was imported inside
#      execute_trade() on every buy), MarketOrderRequest (re-imported inside
#      paper_execute_buy() despite being at module level).
#   ✅ /patterns fingerprint count now live from DB instead of hardcoded 10,265.
# V10.15: BERSERKER_RECIPES full refresh from 16,339-trade Railway backtest
# V9.1: Per-bot pause commands, /buys on/off, /cooldown, /equity
#        BERSERKER position recovery + peak persistence
# V9.2: Force-close logs win/loss, pnl_dollar in snapshot, crypto snapshot V3
# V9.3: Service split — crypto and analyst are separate Railway services
# V9.8: BERSERKER_RECIPES — per-symbol hour/day gates from nexus_analyzer
# V9.9: SCALPER retired — PHASE4 per-symbol bots on Webull
# V10.0: T-Bone overhaul
#   ✅ Single clean startup message — no duplicate boot spam
#   ✅ /crypto fixed — reads JSON dict correctly from V4.2
#   ✅ /analyst fixed — reads JSON dict correctly from V2.6
#   ✅ /wins fixed — proper elif, no longer buried in /friday
#   ✅ /phase4 command — live bot state via Phase4 /think endpoint
#   ✅ /killswitch — implemented (was listed in help but never existed)
#   ✅ /think — full system diagnostic across all services
#   ✅ Morning brief — 8am market open summary
#   ✅ Watchdog thread — proactive alerts when something looks wrong
#   ✅ /performance fixed — crypto_trades endpoint removed, uses snapshot
#   ✅ Daily reset guard uses _morning_brief_sent flag, not time window
#   ✅ Startup health checks are log-only, single boot alert at end
# V10.6: SPCX + display fixes
#   ✅ Added SPCX (SpaceX, IPO'd 2026-06-12) to TECH_GROWTH + BERSERKER_RECIPES
#   ✅ /status position line was printing market_value (total $ of position)
#      mislabeled as a per-share price -- invisible for whole-share positions
#      but wrong for fractional ones (e.g. 0.36 TSLA showed as "$147" when
#      TSLA trades at $410+). Now shows current_price (per-share).
#   ✅ Buy alert clarified to "$X order" -- it's the order notional, not a
#      share price, which caused the same confusion for the TSLA buy alert.
# V10.7: Dashboard removed (revisit later)
#   ✅ Removed Flask app, all /api routes, build_snapshot(), webull/crypto
#      snapshot cache, run_flask() and its thread. T-Bone (Telegram) is now
#      the only interface -- all /think /status /crypto /analyst etc. T-Bone
#      commands call nexus_client directly and are fully unaffected.
#   ⚠️ If Railway healthcheck for this service pings an HTTP port/path,
#      update or remove it -- nothing listens on PORT anymore.
#   ✅ dashboard.html left in repo untouched for when this is revisited.
# V10.8: Bug fixes + pattern memory overhaul
#   ✅ Dead symbols removed from BERSERKER_RECIPES (CCJ, COIN, AMZN, GOOGL)
#   ✅ /analyst command condition fixed (was logically inverted)
#   ✅ RSI upgraded to Wilder EWM smoothing -- replaces noisy simple rolling
#      mean, reduces false signals, matches industry standard
#   ✅ /patterns now shows both Berserker + Crypto pattern memory combined
#   ✅ /crypto_patterns added for crypto-only pattern detail
# V10.15: BERSERKER_RECIPES full refresh from 16,339-trade Railway backtest
#   ✅ retrieve_results.py V1.2 now bridges Railway Postgres -> backtest_log.json
#   ✅ strategist.py ran on full dataset (was 8,840, now 16,339 trades)
#   ✅ All 15 symbols have real avoid_hours from data -- SPCX now has 149 trades
#   ✅ Major changes: CLSK/MARA/GEO/CXW/MSTR gates removed (data said no avoid)
#      PLTR/NVDA/AMD/TSLA/AAPL/META/MSFT/NUE/SPCX gates added or updated
# V10.14: Circuit breaker loop fix (Jun 26 2026)
#   ✅ trigger_circuit_breaker() is now idempotent -- if CB is already active
#      when triggered again, it just extends the timer silently. No duplicate
#      T-Bone alerts. Prevents the CB alert spam loop seen at market open when
#      multiple overnight positions stopped out in rapid succession.
#   ✅ consecutive_losses not incremented while CB is already active -- stops
#      fired during an active CB pause don't immediately re-trigger a new CB
#      the moment the timer expires.
# V10.13: BERSERKER_RECIPES updated from fresh strategist run (Jun 26 2026)
#   ✅ All avoid_hours derived from 8,840 trade backtest (May 2026 run)
#      Previous recipes were from nexus_analyzer 2yr 5-min backtest.
#      New strategist data is more granular (signal-aware, confluence-scored).
#   Key changes from old recipes:
#   CLSK: was [8,9,10,11,12,13] -> now [9,10] (11,12,13,14 are best hours!)
#   MARA: was [8,9,11,13] -> now [11,14]
#   PLTR: was [8,14] -> now [] (all hours viable)
#   GEO:  was [8,9,11,12,13,14] -> now [10,13] (14 is actually best hour!)
#   CXW:  was [8,10,12,13,14] -> now [14,9]
#   NUE:  was [8,11] -> now [] + removed Thursday avoid_day
#   MSTR: was [8,9,10,13] -> now [13,14] + removed Monday avoid_day
#   NVDA: was [13] -> now [] + removed Thursday avoid_day
#   AMD:  was [8,14] -> now [10,11] (14 is best hour!) + removed Friday avoid_day
#   TSLA: was [8,13] -> now [] + removed Thursday avoid_day
#   SMCI: was [8,9,10,11,13,14] -> now [11,12] (was severely over-gated)
#   ✅ Boot alert version updated to V10.13
# V10.12: Berserker strategy fixes (Jun 26 2026 overnight audit)
#   ✅ STOP_LOSS_PCT tightened 4%->2% -- reward/risk was 0.375 at 4% stop with
#      1.5% TP; breakeven required 72% WR but actual WR is 55-63%. At 2% stop
#      reward/risk is 0.75, breakeven at 57% WR -- matches actual performance.
#   ✅ WIN_RATE_GATE_THRESHOLD raised 35%->45% -- 35% WR with old 4% stop was
#      EV=-2.07%/trade (letting through mathematically losing setups). At 45%
#      with 2% stop EV turns positive.
#   ✅ Confluence signal 3 (dead BB lower-band check) replaced with MACD
#      histogram accelerating -- pct_b<0.35 almost never fires when RSI>62
#      (price can't be in lower 35% of band when RSI is momentum-elevated).
#      MACD histogram accelerating (current histogram > prior histogram) is
#      a genuine momentum confirmation that coexists with the base gate.
#   ✅ Paper Berserker RSI gate loosened to 45+ -- was using same RSI>62 gate
#      as live, meaning paper only fingerprinted high-RSI entries (same as
#      live). Paper should fingerprint mid-RSI (45-62) entries to let pattern
#      memory learn whether those setups win, not just confirm what we know.
#   ✅ Boot alert version updated to V10.12
# V10.11: Boot-time analyst health check fix
#   ✅ Analyst health check retries extended 3x6s -> 8x10s (18s -> 80s max)
#      Analyst boots slower than other services (DB init + warmup_stocks +
#      log restore); 18s was consistently too short, service declared offline
#      before it finished booting every redeploy
#   ✅ Boot alert version updated to V10.11
# V10.10: Bug fixes from full code audit
#   ✅ _update_spy_qqq_history() now called every sweep -- was defined but
#      never called, so _spy_history/_qqq_history were always empty
#   ✅ get_spy_regime_and_momentum() now called every sweep and results
#      written to _spy_regime/_spy_momentum_ok -- both stuck at boot defaults
#      (BULL/True) forever; BEAR regime limit and momentum block were dead code
#   ✅ above_ma20 added to get_signals() return dict -- was missing, causing
#      paper Berserker fingerprints to always record above_ma20=False
#   ✅ Confluence signal 1 relabelled from "OBV rising" to "price_momentum"
#      (no volume data in price_history; calling it OBV was misleading in logs)
#   ✅ /opp F&G display threshold corrected 30->25 to match OPP_FNG_FEAR gate
#   ✅ Boot alert version updated to V10.10
# ==============================================================================


API_KEY          = os.environ.get("ALPACA_API_KEY")
SECRET_KEY       = os.environ.get("ALPACA_SECRET_KEY")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
IS_PAPER         = False

BOT_NAME = "BERSERKER"

TRUMP_THEME = ["CLSK", "MARA", "PLTR", "GEO", "CXW", "NUE", "MSTR"]  # Removed: CCJ (40% WR), COIN (40% WR)
TECH_GROWTH = ["NVDA", "TSLA", "AAPL", "SMCI", "SPCX"]  # Removed: AMD (33% WR), MSFT (31% WR), META (33% WR)
SYMBOLS     = TRUMP_THEME + TECH_GROWTH

# -- Per-symbol hour/day gates + TP/SL -- 16,616-trade Railway backtest Jun 27 2026 ---
# V10.18: Fresh strategist run confirms all V10.17 values. One fix: SMCI sl 1.5%->1.0%.
#         Trade counts updated from fresh run.
# V10.17: Added per-symbol tp/sl. V10.16: AMD/MSFT/META removed.
BERSERKER_RECIPES = {
    # TRUMP THEME                               tp      sl
    "CLSK": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=45.0%  866t | +2%=20% +3%=13%
    "MARA": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=38.6% 2743t
    "PLTR": {"avoid_hours": [9, 11],  "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=35.8% 1860t
    "GEO":  {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=36.3%  647t
    "CXW":  {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=36.4%  678t
    "NUE":  {"avoid_hours": [10, 12], "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=39.0%  711t
    "MSTR": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=37.1% 1509t
    # TECH GROWTH
    "NVDA": {"avoid_hours": [8, 9],   "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=39.0% 1488t
    "TSLA": {"avoid_hours": [11, 10], "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=39.4% 1645t
    "AAPL": {"avoid_hours": [8, 13],  "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=35.8%  836t
    "SMCI": {"avoid_hours": [],       "avoid_days": [], "tp": 0.015, "sl": 0.010},  # WR=34.6%  347t | V10.18: sl 1.5%->1.0%
    "SPCX": {"avoid_hours": [9, 13],  "avoid_days": [], "tp": 0.015, "sl": 0.015},  # WR=42.7%  218t | 1.5% sl confirmed
}

MAX_POSITIONS        = 3
TRADE_FRACTION       = 0.40
TRAILING_STOP        = 0.015
RATCHET_PROFIT       = 0.015   # V10.9: was 0.03 -- tighten trail earlier
RATCHET_TRAIL_TIGHT  = 0.005   # V10.9: trail after ratchet fires (was 0.01)
TAKE_PROFIT_PCT      = 0.015   # V10.9: hard TP at +1.5% -- milestone data shows cliff after this
STOP_LOSS_PCT        = 0.01    # V10.16: 2%->1% -- strategist recommended 1% for 13/15 symbols
                               # (backtest optimal). At 1.5%TP/1.0%SL breakeven WR = 40%
                               # vs 57.1% at 2% stop. Dramatically improves EV for marginal symbols.
COOLDOWN_SECS        = 1800
CENTRAL              = ZoneInfo("America/Chicago")
RSI_PERIOD           = 9
MACD_FAST            = 12
MACD_SLOW            = 26
MACD_SIGNAL          = 9
RSI_BUY_TRIGGER      = 62
WARMUP_BARS          = 50
MIN_TRADE_AMT        = 5.00
MIN_POSITION_VALUE   = 10.00  # V10.2: force-close positions worth less than this
ALERT_COOLDOWN_SECS  = 300
DAILY_LOSS_LIMIT_PCT = 0.05
EOD_BUY_CUTOFF_HOUR   = 14
EOD_BUY_CUTOFF_MINUTE = 55
MIN_HOLD_MINUTES      = 20
DAILY_STATE_FILE      = "/app/data/nexus_daily.json"
MAX_CRITICAL_ALERTS   = 3
BERSERKER_PEAKS_FILE  = "/app/data/berserker_peaks.json"
STRATEGY_RECIPES_FILE = "/app/data/strategy_recipes.json"   # V10.19: auto-updated recipes

# V10.19: Dynamic TP bucket data (populated by BerserkerMemory.run_analysis)
# _berserker_fingerprints[symbol]["dynamic_tp"] set per-trade when bucket has evidence
_bucket_mfe_stats: dict = {}   # bucket_key -> {"avg_mfe": float, "pct_2": float, "pct_3": float, "n_winners": int}

# V10.1: Pattern memory
PM_MIN_TRADES        = 20
PM_MIN_BUCKET_TRADES = 3      # min samples per bucket before it's used (run_analysis)
PM_ANALYSIS_INTERVAL = 86400  # daily

# V10.5: Win-rate gate -- skip Berserker entries whose exact historical
# bucket has a win rate below this, once it has >= PM_MIN_BUCKET_TRADES
# samples (see BerserkerMemory.should_skip_entry).
#
# V10.22: Cold-start ramp (Jun 30 2026)
#   57% is the correct long-run breakeven gate (SL/(TP+SL)), but it requires
#   live bucket data to actually pass anything -- a near-empty pattern memory
#   means almost no live bucket clears 57%, so Berserker can sit idle for
#   days waiting on data it can only get by trading. 45% is still EV-positive
#   at 1.5%TP/1.0%SL (EV = 0.45*1.5% - 0.55*1.0% = +0.125%/trade), just
#   thinner margin than 57%. Ramping the gate up over the first few weeks
#   lets live fingerprints accumulate at a workable trade volume, then
#   tightens back to the full breakeven standard once there's enough data
#   for the gate to mean something. GATE_LAUNCH_DATE is the go-live date --
#   update it if redeployed fresh; it does NOT reset on every redeploy since
#   it's a fixed date, not "first boot."
GATE_LAUNCH_DATE     = datetime(2026, 6, 30, tzinfo=CENTRAL)
GATE_RAMP_SCHEDULE   = [
    (14, 0.45),   # days 0-14:  45% -- let backtest-seeded marginal buckets through
    (28, 0.50),   # days 14-28: 50% -- tightening as live data fills in
    (999, 0.57),  # day 28+:    57% -- full mathematical breakeven gate
]

def _current_win_rate_gate() -> float:
    """V10.22: returns the gate threshold for today based on days since launch."""
    days_live = (datetime.now(tz=CENTRAL) - GATE_LAUNCH_DATE).days
    for max_days, threshold in GATE_RAMP_SCHEDULE:
        if days_live < max_days:
            return threshold
    return GATE_RAMP_SCHEDULE[-1][1]

WIN_RATE_GATE_THRESHOLD = _current_win_rate_gate()  # V10.22: cold-start ramp -- see schedule above
                                # V10.17: was static 0.45 -- 45% is EV negative at OLD 1.5%TP/2%SL combo,
                                # but EV positive at CURRENT 1.5%TP/1%SL. 57% = SL/(TP+SL), the actual
                                # long-run breakeven. Ramp lets live data accumulate before tightening.


trading_client    = TradingClient(API_KEY, SECRET_KEY, paper=IS_PAPER)
stock_data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

# V10.2: Consecutive loss circuit breaker
CONSEC_LOSS_LIMIT    = 3      # auto-pause after this many consecutive stops
CONSEC_LOSS_PAUSE    = 7200   # pause duration in seconds (2 hours)
_consecutive_losses  = 0      # reset on any win
_circuit_break_until = 0.0    # timestamp when circuit break expires

# V10.2: SPY market regime filter
SPY_MOMENTUM_GATE    = -0.3   # % SPY must NOT be falling faster than this (30min)
SPY_BEAR_REGIME_MA   = 20     # days for SPY MA to determine bear/bull regime
SPY_BEAR_MAX_POS     = 1      # max positions in bear regime (vs normal MAX_POSITIONS=3)
_spy_regime          = "BULL" # "BULL" or "BEAR" -- updated each sweep
_spy_momentum_ok     = True   # True if SPY momentum is acceptable for entries

# V10.19: VIX regime gate
VIX_WARN_THRESHOLD   = 20.0   # log warning above this
VIX_BLOCK_THRESHOLD  = 25.0   # block new entries above this
VIX_EXTREME_THRESHOLD = 30.0  # block entries + reduce to 1 position above this
_vix_history         = deque(maxlen=20)   # 5-bar smoothed
_vix_level_raw       = 0.0
_vix_level_smooth    = 0.0    # 5-bar EMA of real ^VIX (or VIXY-proxy fallback) -- public module-level for status
_vix_history_lock    = threading.Lock()
_vix_source          = "none"   # "yfinance" or "vixy_proxy" -- visible in /vix for diagnosis
# V10.26: real ^VIX fetch cache. yfinance can't be hit every ~30s sweep without
# risking rate limits, so the real VIX is fetched on its own TTL and the last
# good value is reused between refreshes.
_real_vix_cache       = {"value": None, "ts": 0.0}
_REAL_VIX_TTL_SECS    = 90

# V10.19: Earnings calendar blackout
earnings_blocked: set = set()          # symbols blocked from new entries
_earnings_cache: dict = {}             # symbol -> {"date": str, "checked_at": float}
_earnings_last_check: float = 0.0      # timestamp of last full sweep
EARNINGS_REFRESH_SECS = 14400         # re-check every 4 hours

# V10.19: Regime aggregator
_regime_score: int  = 0
_regime_factors: list = []
_regime_shutdown_alerted: bool = False

# V10.2: Paper trading mode -- parallel Berserker on Alpaca paper account
PAPER_API_KEY    = os.environ.get("ALPACA_PAPER_API_KEY", "")
PAPER_SECRET_KEY = os.environ.get("ALPACA_PAPER_SECRET_KEY", "")
PAPER_ENABLED    = bool(PAPER_API_KEY and PAPER_SECRET_KEY)

_paper_trading_client    = TradingClient(PAPER_API_KEY, PAPER_SECRET_KEY, paper=True) if PAPER_ENABLED else None
_paper_data_client       = StockHistoricalDataClient(PAPER_API_KEY, PAPER_SECRET_KEY) if PAPER_ENABLED else None
_paper_price_history     = {sym: deque(maxlen=WARMUP_BARS) for sym in SYMBOLS}
_paper_fingerprints: dict = {}   # symbol -> {trade_id, entry_context, mfe, mae}
_paper_peak_prices: dict  = {}
_paper_entry_times: dict  = {}
_paper_stats = {"trades": 0, "wins": 0, "losses": 0}

# V10.1: SPY/QQQ price history for context scoring
_spy_history: deque = deque(maxlen=50)
_qqq_history: deque = deque(maxlen=50)
_spy_history_lock   = threading.Lock()
_spy_session_open   = 0.0   # float -- SPY price at 8am today

# V10.1: Trade fingerprints -- keyed by symbol, cleared on exit
_berserker_fingerprints: dict = {}   # symbol -> {trade_id, entry_context}

price_history         = {sym: deque(maxlen=WARMUP_BARS) for sym in SYMBOLS}
portfolio             = {"peak_prices": {}, "sector_health": "STRONG"}
daily_stats           = {"trades": 0, "wins": 0, "losses": 0, "start_equity": 0.0}
_today_symbol_trades = {}

bot_state = {
    "paused":           False,
    "paused_berserker": False,
    "paused_scanner":   False,
    "paused_crypto":    False,
    "buys_disabled":    False,
    "quiet_until":      None,
    "daily_loss_hit":   False,
}

pending_sells        = set()
last_alert_time      = {}
stayopen_symbols     = set()
position_entry_times = {}

recently_traded = []
MAX_RECENT       = 10

_critical_alert_count  = 0
_last_error_reset      = time.time()
_service_start_time    = time.time()

_eod_warning_sent        = False
_eod_second_warning_sent = False
_eod_closed              = False

# ✅ V10.0: morning brief flag — prevents duplicate 8am messages
_morning_brief_sent = False

_alert_feed       = []

# V10.23: PDT removed (Jun 30 2026) -- FINRA retired the Pattern Day Trader
# rule effective Jun 4 2026 (Reg Notice 26-10); Alpaca implemented day-one,
# removing pattern_day_trader/daytrade_count/daytrading_buying_power from
# the API entirely (full removal by Jul 6 2026). All _pdt_info state,
# pdt_blocked set, refresh_pdt_info(), get_pdt_rolloff_dates(),
# get_pdt_slots_remaining(), pdt_warning_message(), is_pdt_error(),
# handle_pdt_block(), and should_exit_now() (which throttled exits to
# conserve day-trade slots that no longer exist) have all been removed.
# The pdt_slots_used DB column and bucket-key dimension are kept for
# historical fingerprint continuity but no longer meaningfully populated
# or used to split pattern memory buckets -- see BerserkerMemory._bucket_key.
_friday_buy_enabled    = False


# ==============================================================================
# BERSERKER PEAK PERSISTENCE
# ==============================================================================
def save_berserker_peaks():
    try:
        os.makedirs(os.path.dirname(BERSERKER_PEAKS_FILE), exist_ok=True)
        with open(BERSERKER_PEAKS_FILE, "w") as f:
            json.dump(portfolio["peak_prices"], f)
    except Exception as e:
        log(f"⚠️ Could not save BERSERKER peaks: {e}")

def load_berserker_peaks() -> dict:
    try:
        if os.path.exists(BERSERKER_PEAKS_FILE):
            with open(BERSERKER_PEAKS_FILE, "r") as f:
                data = json.load(f)
            log(f"💾 Loaded BERSERKER peaks: {data}")
            return data
    except Exception as e:
        log(f"⚠️ Could not load BERSERKER peaks: {e}")
    return {}


# ==============================================================================
# BERSERKER POSITION RECOVERY
# ==============================================================================
def recover_berserker_positions():
    try:
        saved_peaks = load_berserker_peaks()
        raw         = trading_client.get_all_positions()
        recovered   = 0
        for pos in raw:
            sym = pos.symbol.replace("/USD","").replace("USD","")
            if sym not in SYMBOLS:
                continue
            if shared_state.owner(sym) is not None:
                log(f"⏭️ BERSERKER recovery skipping {sym} — owned by {shared_state.owner(sym)}")
                continue
            if not shared_state.claim(sym, BOT_NAME):
                continue
            entry_price = float(pos.avg_entry_price)
            try:
                current_price = float(pos.current_price) if pos.current_price else entry_price
            except:
                current_price = entry_price

            # V10.2: Skip dust positions on recovery
            try:
                market_value = float(pos.market_value) if pos.market_value else 0
                if 0 < market_value < MIN_POSITION_VALUE:
                    log(f"🧹 Skipping dust recovery {sym} | value=${market_value:.2f} -- force closing")
                    try:
                        trading_client.close_position(sym)
                    except Exception as _e:
                        log(f"⚠️ Could not close dust {sym}: {_e}")
                    continue
            except Exception:
                pass

            saved_peak = float(saved_peaks.get(sym, 0))
            peak_price = max(saved_peak, entry_price, current_price)
            portfolio["peak_prices"][sym] = peak_price
            position_entry_times[sym]     = datetime.now(tz=CENTRAL)
            recovered += 1
            pnl_pct   = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
            peak_note = "(from file)" if saved_peak > 0 else "(calculated)"
            log(f"🔄 BERSERKER recovered: {sym} | entry=${entry_price:.2f} | "
                f"now=${current_price:.2f} | P&L: {pnl_pct:+.2f}% | "
                f"peak=${peak_price:.2f} {peak_note}")
        if recovered:
            log(f"🔄 BERSERKER recovered {recovered} open position(s)")
        else:
            log("✅ BERSERKER: no open positions to recover")
        return recovered
    except Exception as e:
        log(f"⚠️ BERSERKER position recovery error: {e}")
        return 0





# ==============================================================================
# DAILY STATE PERSISTENCE
# ==============================================================================
def load_daily_state(current_equity: float) -> float:
    today = datetime.now(tz=CENTRAL).strftime("%Y-%m-%d")
    try:
        if os.path.exists(DAILY_STATE_FILE):
            with open(DAILY_STATE_FILE, "r") as f:
                saved = json.load(f)
            if saved.get("date") == today:
                start = saved["start_equity"]
                log(f"📊 Loaded today's start equity: ${start:.2f}")
                return start
    except Exception as e:
        log(f"⚠️ Could not load daily state: {e}")
    save_daily_state(current_equity)
    log(f"📊 New trading day — start equity set: ${current_equity:.2f}")
    return current_equity

def save_daily_state(equity: float):
    today = datetime.now(tz=CENTRAL).strftime("%Y-%m-%d")
    try:
        os.makedirs(os.path.dirname(DAILY_STATE_FILE), exist_ok=True)
        with open(DAILY_STATE_FILE, "w") as f:
            json.dump({
                "date":         today,
                "start_equity": equity,
                "saved_at":     datetime.now(tz=CENTRAL).strftime("%H:%M:%S"),
            }, f)
    except Exception as e:
        log(f"⚠️ Could not save daily state: {e}")

def reset_daily_state(equity: float):
    global _morning_brief_sent, WIN_RATE_GATE_THRESHOLD
    save_daily_state(equity)
    daily_stats.update({"trades": 0, "wins": 0, "losses": 0, "start_equity": equity})
    bot_state["daily_loss_hit"] = False
    recently_traded.clear()
    _today_symbol_trades.clear()
    if _scanner_ok and hasattr(scanner, 'reset_daily_symbol_trades'):
        scanner.reset_daily_symbol_trades()
    _morning_brief_sent    = False
    # V10.22: re-evaluate cold-start gate ramp daily (not just at boot) so it
    # actually tightens over time on a long-lived Railway service instead of
    # freezing at whatever value was set when the container last started.
    old_gate = WIN_RATE_GATE_THRESHOLD
    WIN_RATE_GATE_THRESHOLD = _current_win_rate_gate()
    if WIN_RATE_GATE_THRESHOLD != old_gate:
        log(f"📈 Win-rate gate ramp: {old_gate:.0%} -> {WIN_RATE_GATE_THRESHOLD:.0%}")
    log(f"🌅 Daily state reset — new baseline: ${equity:.2f}")


# ==============================================================================
# HELPERS
# ==============================================================================
def is_market_hours_for_buying(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    if now.hour < 8:
        return False
    if now.hour > EOD_BUY_CUTOFF_HOUR:
        return False
    if now.hour == EOD_BUY_CUTOFF_HOUR and now.minute >= EOD_BUY_CUTOFF_MINUTE:
        return False
    return True

def is_quiet() -> bool:
    qt = bot_state.get("quiet_until")
    if qt and time.time() < qt:
        return True
    if qt:
        bot_state["quiet_until"] = None
    return False

def alert(msg, critical=False):
    global _alert_feed, _critical_alert_count, _last_error_reset
    if not critical and is_quiet():
        return
    if critical and "error" in msg.lower():
        now_t = time.time()
        if now_t - _last_error_reset > 600:
            _critical_alert_count = 0
            _last_error_reset     = now_t
        _critical_alert_count += 1
        if _critical_alert_count > MAX_CRITICAL_ALERTS:
            return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=5)
        # V10.30: Telegram rejects any message with an unescaped <...> under
        # HTML parse mode (400 "can't parse entities") and this except was
        # eating it silently -- that's exactly how /help vanished after
        # "/scanner_aggro <0.5-1.5>" was added to the help text in V10.28.
        # Retry as plain text so no alert is EVER silently dropped again.
        try:
            if not r.json().get("ok", False):
                log(f"[TEL] send rejected: {str(r.json().get('description','?'))[:70]} -- retrying plain text")
                requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=5)
        except Exception:
            pass
    except:
        pass
    _alert_feed.insert(0, {
        "msg":      msg,
        "time":     datetime.now(tz=CENTRAL).strftime("%H:%M"),
        "critical": critical,
        "win":      "✅" in msg and "EXIT" in msg,
        "loss":     "🛑" in msg,
    })
    _alert_feed = _alert_feed[:20]

def alert_exit(symbol: str, msg: str):
    now  = time.time()
    last = last_alert_time.get(symbol, 0)
    if now - last < ALERT_COOLDOWN_SECS:
        log(f"🔕 T-Bone suppressed repeat alert for {symbol}")
        return
    last_alert_time[symbol] = now
    alert(msg)

def log(msg):
    print(f"[NEXUS | {datetime.now(tz=CENTRAL).strftime('%H:%M:%S')}] {msg}", flush=True)

def log_symbol_trade(bot: str, symbol: str, pnl: float):
    if symbol not in _today_symbol_trades:
        _today_symbol_trades[symbol] = {"bot": bot, "wins": 0, "losses": 0, "pnl": 0.0}
    if pnl > 0:
        _today_symbol_trades[symbol]["wins"] += 1
    else:
        _today_symbol_trades[symbol]["losses"] += 1
    _today_symbol_trades[symbol]["pnl"] = round(
        _today_symbol_trades[symbol]["pnl"] + pnl * 100, 3
    )



# ==============================================================================
# SYMBOL ROTATION
# ==============================================================================
def mark_recently_traded(symbol: str):
    global recently_traded
    if symbol in recently_traded:
        recently_traded.remove(symbol)
    recently_traded.insert(0, symbol)
    recently_traded = recently_traded[:MAX_RECENT]

def get_symbol_priority(symbol: str) -> int:
    try:
        return recently_traded.index(symbol) + 1
    except ValueError:
        return 0

def get_sorted_symbols(positions: dict) -> list:
    available = [s for s in SYMBOLS if s not in positions and
                 not shared_state.is_on_cooldown(s) and
                 shared_state.owner(s) is None and
                 not (_win_follower and _win_follower.is_benched(s))]
    # V10.29: HOT symbols scan first -- feed the winners. Everything else
    # keeps the least-recently-traded rotation. The old pure-rotation order
    # was a fairness scheme: it actively deprioritized whatever just won.
    if _win_follower:
        hot  = sorted([s for s in available if _win_follower.get_tier(s) == "HOT"],
                      key=get_symbol_priority)
        rest = sorted([s for s in available if _win_follower.get_tier(s) != "HOT"],
                      key=get_symbol_priority)
        return hot + rest
    return sorted(available, key=get_symbol_priority)


# ==============================================================================
# BROKER HELPERS
# ==============================================================================
def get_alpaca_positions() -> dict:
    try:
        raw = trading_client.get_all_positions()
        return {p.symbol.replace("/USD","").replace("USD",""): p for p in raw}
    except:
        return {}

def get_position_pnl_alpaca(pos) -> float:
    try:
        return float(pos.unrealized_plpc)
    except:
        return 0.0

def close_alpaca_position(symbol: str) -> tuple:
    try:
        pos  = trading_client.get_open_position(symbol)
        pnl  = get_position_pnl_alpaca(pos)
        trading_client.close_position(symbol)
        shared_state.release(symbol)
        pending_sells.discard(symbol)
        portfolio["peak_prices"].pop(symbol, None)
        position_entry_times.pop(symbol, None)
        if _scanner_ok:
            scanner.scanner_positions.pop(symbol, None)
        save_berserker_peaks()
        return True, pnl
    except Exception as e:
        log(f"⚠️ Alpaca close error [{symbol}]: {e}")
        return False, 0.0

def find_and_close(symbol: str) -> str:
    symbol = symbol.upper()
    if symbol in get_alpaca_positions():
        success, pnl = close_alpaca_position(symbol)
        if success:
            pnl_label = f"+{round(pnl*100,2)}%" if pnl > 0 else f"{round(pnl*100,2)}%"
            emoji     = "✅" if pnl > 0 else "🛑"
            trade_log.record_trade("BERSERKER", symbol, pnl, "manual-close")
            log_symbol_trade("BERSERKER", symbol, pnl)
            daily_stats["wins" if pnl > 0 else "losses"] += 1
            daily_stats["trades"] += 1
            return f"{emoji} Closed {symbol} (Alpaca) | P&L: {pnl_label}"
        return f"⚠️ {symbol} close failed"
    return f"❓ {symbol} not found in open Alpaca positions\nPhase4 positions are managed by that service directly (separate Alpaca account)."

def hold_time_minutes(symbol: str) -> float:
    entry = position_entry_times.get(symbol)
    if not entry:
        return 999.0
    return (datetime.now(tz=CENTRAL) - entry).total_seconds() / 60


# ==============================================================================
# EOD AUTO-CLOSE
# ==============================================================================
def is_position_still_running(symbol: str, broker: str) -> bool:
    try:
        if broker == "alpaca":
            positions = get_alpaca_positions()
            if symbol not in positions:
                return False
            pnl = get_position_pnl_alpaca(positions[symbol])
            if pnl <= 0:
                return False
            if symbol in price_history and len(price_history[symbol]) >= 6:
                return price_history[symbol][-1] > price_history[symbol][-6]
            return False
    except:
        return False

def run_eod_autoclose():
    log("⏰ EOD auto-close running...")
    closed  = []
    left    = []
    failed  = []

    for symbol, pos in get_alpaca_positions().items():
        if symbol in stayopen_symbols:
            left.append(f"  🔓 {symbol} — /stayopen override")
            continue
        if is_position_still_running(symbol, "alpaca"):
            pnl = get_position_pnl_alpaca(pos)
            left.append(f"  🚀 {symbol} +{round(pnl*100,2)}% still running")
        else:
            success, pnl = close_alpaca_position(symbol)
            if success:
                pnl_label = f"+{round(pnl*100,2)}%" if pnl > 0 else f"{round(pnl*100,2)}%"
                emoji     = "✅" if pnl > 0 else "🛑"
                closed.append(f"  {emoji} {symbol} {pnl_label}")
                trade_log.record_trade("BERSERKER", symbol, pnl, "eod-autoclose")
                log_symbol_trade("BERSERKER", symbol, pnl)
                daily_stats["wins" if pnl > 0 else "losses"] += 1
                daily_stats["trades"] += 1
            else:
                pnl_pct   = get_position_pnl_alpaca(pos)
                pnl_label = f"+{round(pnl_pct*100,2)}%" if pnl_pct > 0 else f"{round(pnl_pct*100,2)}%"
                failed.append(f"  ⚠️ {symbol} {pnl_label} — close failed, will retry")

    lines = ["🔒 EOD AUTO-CLOSE\n──────────────────"]
    lines.append("Closed:")
    lines.extend(closed if closed else ["  None"])
    if left:
        lines.append("Left running:")
        lines.extend(left)
    if failed:
        lines.append("Close failed:")
        lines.extend(failed)
    lines.append("──────────────────")
    lines.append("Have a good evening 🌙")
    alert("\n".join(lines), critical=True)
    log("⏰ EOD auto-close complete")

def check_eod(now: datetime):
    global _eod_warning_sent, _eod_second_warning_sent, _eod_closed

    if now.weekday() >= 5:
        return

    if now.hour == 14 and now.minute == 50 and not _eod_warning_sent:
        _eod_warning_sent = True
        alpaca_pos = get_alpaca_positions()
        all_pos    = [f"  {s}" for s in alpaca_pos]
        if all_pos:
            alert(
                f"⚠️ 10 min to close!\n"
                f"──────────────────\n"
                f"Open positions:\n" + "\n".join(all_pos) + "\n"
                f"──────────────────\n"
                f"Auto-closing at 2:58 PM\n"
                f"Winners still running left open\n"
                f"Use /stayopen SYMBOL to override",
                critical=True
            )

    if now.hour == 14 and now.minute == 55 and not _eod_second_warning_sent:
        _eod_second_warning_sent = True
        if get_alpaca_positions():
            alert("⏰ 3 min to auto-close!", critical=True)

    if now.hour == 14 and now.minute == 58 and not _eod_closed:
        _eod_closed = True
        run_eod_autoclose()


# ==============================================================================
# DAILY LOSS CIRCUIT BREAKER
# ==============================================================================
def check_circuit_breaker():
    """V10.2: Check if consecutive loss circuit breaker is active."""
    if time.time() < _circuit_break_until:
        return True
    return False

def trigger_circuit_breaker():
    """V10.2: Trigger the consecutive loss circuit breaker.
    V10.13: Guard against re-firing while CB is already active. Multiple
    stop-loss exits in rapid succession (e.g. overnight holds stopping out
    at open) were each incrementing _consecutive_losses and firing the CB
    alert repeatedly. Now idempotent -- extending an active pause logs only,
    no duplicate T-Bone alert.
    """
    global _circuit_break_until, _consecutive_losses
    already_active = time.time() < _circuit_break_until
    _circuit_break_until = time.time() + CONSEC_LOSS_PAUSE
    _consecutive_losses  = 0   # always reset
    if already_active:
        # CB already running -- just extend the timer silently
        log(f"[CB] CB already active -- extended to {datetime.fromtimestamp(_circuit_break_until, tz=CENTRAL).strftime('%I:%M %p')} CDT")
        return
    resume_time = datetime.fromtimestamp(_circuit_break_until, tz=CENTRAL).strftime("%I:%M %p")
    msg = (
        "⚠️ BERSERKER CIRCUIT BREAKER\n"
        "──────────────────\n"
        f"{CONSEC_LOSS_LIMIT} consecutive stops hit\n"
        "Auto-pausing for 2 hours\n"
        f"Resumes at {resume_time} CDT\n"
        "──────────────────\n"
        "Send /resume to override early"
    )
    alert(msg, critical=True)
    log(f"[CB] Circuit breaker triggered -- {CONSEC_LOSS_LIMIT} consecutive losses -- paused 2hrs")


def check_daily_loss(total_equity: float):
    start = daily_stats["start_equity"]
    if start <= 0:
        return
    loss_pct = (start - total_equity) / start
    if loss_pct >= DAILY_LOSS_LIMIT_PCT and not bot_state["daily_loss_hit"]:
        bot_state["daily_loss_hit"] = True
        log(f"🚨 Daily loss limit: -{round(loss_pct*100,2)}% equity")
        alert(
            f"🚨 DAILY LOSS LIMIT HIT\n"
            f"Down {round(loss_pct*100,2)}% today\n"
            f"Start: ${round(start,2)} — Now: ${round(total_equity,2)}\n"
            f"New buys paused for the day.",
            critical=True
        )



# ==============================================================================
# ✅ V10.0: MORNING BRIEF
# ==============================================================================
def send_morning_brief(equity: float):
    """
    Fires once at 8am on trading days.
    Replaces the old 'New trading day!' message with actual useful context.
    """
    now      = datetime.now(tz=CENTRAL)
    day_name = ["Mon", "Tue", "Wed", "Thu", "Fri"][now.weekday()]

    # Crypto context
    crypto_line = "🌙 Crypto: offline"
    snap = nexus_client.crypto_snapshot()
    if snap and snap.get("online"):
        fg        = snap.get("fear_greed", "?")
        fg_lbl    = snap.get("fear_greed_label", "")
        tier      = snap.get("hour_tier", "?")
        dom       = snap.get("btc_dominance", 0)
        cr_pos    = len(snap.get("positions", []))
        pos_note  = f" | {cr_pos} position(s) open" if cr_pos else ""
        crypto_line = f"🌙 Crypto: F&G {fg} ({fg_lbl}) | Tier: {tier} | BTC Dom: {dom:.1f}%{pos_note}"

    # Phase4 context
    p4_line = "⚡ Phase4: offline"
    p4 = nexus_client.phase4_think()
    if p4 and p4.get("online"):
        bots     = p4.get("bots", {})
        in_pos   = [s for s, b in bots.items() if b.get("in_position")]
        watching = [s for s, b in bots.items() if b.get("reversal_state") == "WATCHING"]
        parts    = []
        if in_pos:
            parts.append(f"holding {', '.join(in_pos)}")
        if watching:
            parts.append(f"watching reversal on {', '.join(watching)}")
        if not parts:
            parts.append("all bots idle, scanning")
        p4_line = f"⚡ Phase4: {' | '.join(parts)}"

    # Berserker positions from overnight
    alpaca_pos    = get_alpaca_positions()
    berserker_pos = [sym for sym in alpaca_pos if sym in SYMBOLS]
    pos_line      = f"Holding: {', '.join(berserker_pos)}" if berserker_pos else "No open positions"

    alert(
        f"🌅 {day_name} {now.strftime('%b %d')} — Market Open\n"
        f"──────────────────\n"
        f"Equity: ${equity:.2f} | Sector: {portfolio['sector_health']}\n"
        f"{pos_line}\n"
        f"──────────────────\n"
        f"{crypto_line}\n"
        f"{p4_line}\n"
        f"──────────────────\n"
        f"VIX: {get_vix_status()['emoji']} {get_vix_status()['level']} ({get_vix_status()['label']}) | "
        f"Regime: {_regime_score}/5\n"
        f"{'Earnings blocked: ' + ', '.join(sorted(earnings_blocked)) if earnings_blocked else 'No earnings blackouts'}\n"
        f"──────────────────\n"
        f"NEXUS is watching 👀🥩",
        critical=True
    )
    log("🌅 Morning brief sent")


# ==============================================================================
# ✅ V10.0: WATCHDOG THREAD
# Runs every 5 minutes during market hours.
# Proactively alerts when something looks wrong.
# ==============================================================================
def run_watchdog():
    log("[DOG] Watchdog started")
    time.sleep(300)  # let everything settle on boot

    _phase4_offline_alerted   = False
    _crypto_block_start: float = 0.0
    _last_p4_idle_alert:  dict = {}  # symbol -> time

    while True:
        try:
            now = datetime.now(tz=CENTRAL)
            # Market hours for most checks: weekdays 8am-3pm
            market_hours = now.weekday() < 5 and 8 <= now.hour < 15
            # V10.9: Extended window for Phase4 overnight position monitoring:
            # weekdays until midnight so leveraged ETF positions held overnight
            # don't go unmonitored after the 3pm watchdog cutoff.
            phase4_watch = now.weekday() < 5 and now.hour < 24

            if market_hours:

                # -- Phase4 service health ──────────────────---------------
                p4 = nexus_client.phase4_think()
                if not p4 or not p4.get("online"):
                    if not _phase4_offline_alerted:
                        _phase4_offline_alerted = True
                        alert(
                            "⚠️ WATCHDOG: Phase4 service not responding!\n"
                            "Check Railway nexus-phase4 deploy logs.",
                            critical=True
                        )
                else:
                    _phase4_offline_alerted = False
                    bots = p4.get("bots", {})

                    # -- Phase4 bot idle too long ──────────────────--------
                    for sym, b in bots.items():
                        if b.get("in_position"):
                            # Check for stale position (>5 hours)
                            held_min = b.get("held_minutes", 0)
                            if held_min > 300:
                                last_alerted = _last_p4_idle_alert.get(f"{sym}_long", 0)
                                if time.time() - last_alerted > 3600:
                                    _last_p4_idle_alert[f"{sym}_long"] = time.time()
                                    sym_active = b.get("active_sym", sym)
                                    pnl        = b.get("pnl_pct", 0)
                                    alert(
                                        f"⏳ WATCHDOG: {sym} holding {sym_active} for "
                                        f"{int(held_min)}m | P&L: {pnl:+.2f}%\n"
                                        f"Stop: {b.get('stop_pct','?')}% | "
                                        f"Mode: {b.get('mode','?')}",
                                        critical=True
                                    )
                        else:
                            # Bot idle — check if it's been idle too long
                            idle_min = b.get("idle_minutes", 0)
                            cooldown = b.get("cooldown_remaining_secs", 0)
                            if idle_min > 90 and cooldown == 0:
                                last_alerted = _last_p4_idle_alert.get(f"{sym}_idle", 0)
                                if time.time() - last_alerted > 3600:
                                    _last_p4_idle_alert[f"{sym}_idle"] = time.time()
                                    rsi = b.get("rsi", "?")
                                    alert(
                                        f"⏳ WATCHDOG: {sym} bot idle {int(idle_min)}m\n"
                                        f"RSI: {rsi} | No cooldown active\n"
                                        f"Conditions not met or avoid-hour gate.",
                                    )

                # -- Crypto confidence stuck at BLOCK ──────────────────---
                snap = nexus_client.crypto_snapshot()
                if snap and snap.get("online"):
                    tier = snap.get("hour_tier", "")
                    if tier == "peak":
                        # Check if all pairs are blocked
                        # We flag this if no buys for 2+ peak hours
                        # Simple heuristic: if wins==0, losses==0, no positions, and
                        # we're 2hrs into peak — probably worth flagging
                        pass  # Phase2: wire to /confidence endpoint when available

                # -- Berserker: position held overnight unexpectedly -------
                # V10.4: only check positions actually owned by BERSERKER --
                # previously this iterated ALL Alpaca positions (including
                # Scanner-owned ones like BBAI/SLV/AI), which aren't in
                # position_entry_times, so hold_time_minutes() fell back to
                # 999.0 and got mislabeled "BERSERKER {sym} held 999m".
                for sym, pos in get_alpaca_positions().items():
                    if shared_state.owner(sym) != BOT_NAME:
                        continue
                    held = hold_time_minutes(sym)
                    if held > 360:  # 6 hours
                        pnl = get_position_pnl_alpaca(pos)
                        last_alerted = _last_p4_idle_alert.get(f"BERSERK_{sym}", 0)
                        if time.time() - last_alerted > 7200:
                            _last_p4_idle_alert[f"BERSERK_{sym}"] = time.time()
                            alert(
                                f"⏳ WATCHDOG: BERSERKER {sym} held {int(held)}m\n"
                                f"P&L: {round(pnl*100,2)}% — exits still active",
                            )

            # V10.9: Phase4 overnight position monitor -- runs until midnight
            # on weekdays regardless of market hours. Leveraged ETFs can gap
            # hard on overnight macro events; we need eyes on them after 3pm.
            elif phase4_watch and not market_hours:
                try:
                    p4 = nexus_client.phase4_think()
                    if p4 and p4.get("online"):
                        bots = p4.get("bots", {})
                        for sym, b in bots.items():
                            if b.get("in_position"):
                                held_min   = b.get("held_minutes", 0)
                                sym_active = b.get("active_sym", sym)
                                pnl        = b.get("pnl_pct", 0)
                                last_alerted = _last_p4_idle_alert.get(f"{sym}_overnight", 0)
                                # Alert once per hour for overnight positions
                                if time.time() - last_alerted > 3600:
                                    _last_p4_idle_alert[f"{sym}_overnight"] = time.time()
                                    alert(
                                        f"🌙 WATCHDOG (overnight): {sym} holding {sym_active} | "
                                        f"{int(held_min)}m | P&L: {pnl:+.2f}%\n"
                                        f"Phase4 exits active -- position monitored.",
                                        critical=True
                                    )
                except Exception:
                    pass

        except Exception as e:
            log(f"⚠️ Watchdog error: {e}")

        time.sleep(300)  # check every 5 minutes


# ==============================================================================
# ✅ V10.0: /think — full system diagnostic
# ==============================================================================
def build_think_report() -> str:
    lines = ["🧠 NEXUS THINK REPORT\n──────────────────"]
    now   = datetime.now(tz=CENTRAL)

    # -- BERSERKER ──────────────────────────────────────────────────────------
    try:
        acct      = trading_client.get_account()
        equity    = float(acct.equity)
        cash      = float(acct.cash)
        positions = get_alpaca_positions()
        start_eq  = daily_stats["start_equity"]
        day_pnl   = equity - start_eq
        day_pct   = round(day_pnl / start_eq * 100, 2) if start_eq > 0 else 0

        lines.append(f"📊 BERSERKER (Alpaca)")
        lines.append(f"  Equity: ${equity:.2f} ({day_pct:+.2f}% today) | Cash: ${cash:.2f}")
        if positions:
            for sym, pos in positions.items():
                if sym not in SYMBOLS:
                    continue
                pnl   = get_position_pnl_alpaca(pos)
                held  = hold_time_minutes(sym)
                lines.append(f"  {'✅' if pnl>=0 else '🔴'} {sym}: {pnl*100:+.2f}% | {int(held)}m held")
        else:
            lines.append("  No open positions")

        lines.append(f"  Sector: {portfolio['sector_health']} | Trades today: {daily_stats['trades']} | {daily_stats['wins']}W {daily_stats['losses']}L")
        # V10.19: VIX + regime + earnings
        vix_st = get_vix_status()
        lines.append(f"  VIX: {vix_st['emoji']} {vix_st['level']} ({vix_st['label']}) | "
                     f"Regime: {_regime_score}/5")
        if earnings_blocked:
            lines.append(f"  ❌ Earnings blocked: {', '.join(sorted(earnings_blocked))}")
        else:
            lines.append(f"  ✅ No earnings blackouts")
    except Exception as e:
        lines.append(f"  ⚠️ Berserker read error: {e}")

    # -- PHASE4 ──────────────────────────────────────────────────────---------
    lines.append("──────────────────")
    lines.append("⚡ PHASE4 (Alpaca)")
    try:
        p4 = nexus_client.phase4_think()
        if not p4 or not p4.get("online"):
            lines.append("  ⚠️ Service offline or unreachable")
        else:
            bots = p4.get("bots", {})
            for sym, b in bots.items():
                if b.get("in_position"):
                    active  = b.get("active_sym", sym)
                    pnl     = b.get("pnl_pct", 0)
                    peak    = b.get("peak_pct", 0)
                    mode    = b.get("mode", "?")
                    held    = b.get("held_minutes", 0)
                    lines.append(
                        f"  🟢 {sym} — {active} | {pnl:+.2f}% | peak {peak:+.2f}% | "
                        f"{int(held)}m | {mode}"
                    )
                else:
                    rsi      = b.get("rsi", "?")
                    rev      = b.get("reversal_state", "IDLE")
                    cd_secs  = b.get("cooldown_remaining_secs", 0)
                    idle_min = b.get("idle_minutes", 0)
                    cd_note  = f" | cooldown {int(cd_secs//60)}m" if cd_secs > 0 else ""
                    rev_note = f" | 👁️ WATCHING reversal" if rev == "WATCHING" else ""
                    lines.append(
                        f"  ⚪ {sym}: idle {int(idle_min)}m | RSI {rsi}{cd_note}{rev_note}"
                    )
            bp = p4.get("buying_power", 0)
            lines.append(f"  Buying power: ${bp:.2f}")
    except Exception as e:
        lines.append(f"  ⚠️ Phase4 read error: {e}")

    # -- CRYPTO ──────────────────────────────────────────────────────---------
    lines.append("──────────────────")
    lines.append("🌙 CRYPTO (Coinbase)")
    try:
        snap = nexus_client.crypto_snapshot()
        if not snap or not snap.get("online"):
            lines.append("  ⚠️ Service offline or unreachable")
        else:
            bal      = snap.get("usdc_balance", 0)
            fg       = snap.get("fear_greed", "?")
            fg_lbl   = snap.get("fear_greed_label", "")
            tier     = snap.get("hour_tier", "?")
            session  = snap.get("session", "?")
            dom      = snap.get("btc_dominance", 0)
            cb_act   = snap.get("circuit_break_active", False)
            paused   = snap.get("paused", False)
            wins     = snap.get("wins", 0)
            losses   = snap.get("losses", 0)
            status   = "PAUSED" if paused else "CB ACTIVE" if cb_act else "ACTIVE"
            lines.append(f"  Balance: ${bal:.2f} | {status} | {wins}W {losses}L")
            lines.append(f"  F&G: {fg} ({fg_lbl}) | Tier: {tier} | Session: {session} | BTC Dom: {dom:.1f}%")
            positions = snap.get("positions", [])
            if positions:
                for p in positions:
                    pair  = p.get("pair", "?").replace("-USDC","")
                    pct   = p.get("pnl_pct", 0)
                    conf  = p.get("confidence", 0)
                    mode  = p.get("mode", "?")
                    lines.append(f"  {'✅' if pct>=0 else '🔴'} {pair}: {pct:+.2f}% | conf={conf} | {mode}")
            else:
                lines.append("  No open positions")
    except Exception as e:
        lines.append(f"  ⚠️ Crypto read error: {e}")

    # -- SCANNER ──────────────────────────────────────────────────────--------
    lines.append("──────────────────")
    if _scanner_ok:
        s_pos = getattr(scanner, 'scanner_positions', {})
        dropping = getattr(scanner, '_dropping_count', 0)
        cb_override = getattr(scanner, '_circuit_breaker_override', False)
        override_note = " 🟢OVERRIDE" if cb_override else ""
        lines.append(f"🔍 SCANNER: {len(s_pos)} positions | dropping: {dropping}/20{override_note}")
        for sym, d in s_pos.items():
            price  = scanner.get_price(sym) or 0
            entry  = d.get("entry_price", price)
            pnl    = (price - entry) / entry if entry > 0 else 0
            held   = int((datetime.now(tz=CENTRAL) - d["entry_time"]).seconds / 60)
            lines.append(f"  {'✅' if pnl>=0 else '🔴'} {sym}: {pnl*100:+.2f}% | {held}m held")
    else:
        lines.append(f"🔍 SCANNER: offline ({_scanner_err[:40]})")

    # -- OPTIONS ENGINE ─────────────────────────────────────────────────────--
    lines.append("──────────────────")
    if _options_ok:
        try:
            ostate = options_engine.get_state()
            omode  = "paper" if ostate.get("paper_mode", True) else "LIVE"
            if ostate.get("status") == "OPEN":
                sym   = ostate.get("contract_symbol", "?")
                entry = ostate.get("entry_price") or 0
                quote = options_engine._get_current_quote(sym)
                bid   = quote.get("bid") if quote else None
                if bid is not None and entry > 0:
                    pnl_pct = (bid - entry) / entry * 100
                    lines.append(f"🎯 OPTIONS [{omode}]: 🟢 {sym} | {pnl_pct:+.1f}%")
                else:
                    lines.append(f"🎯 OPTIONS [{omode}]: 🟢 {sym} | entry ${entry:.2f}")
            else:
                en = "ON" if ostate.get("enabled") else "off"
                lines.append(f"🎯 OPTIONS [{omode}]: idle | engine {en}")
        except Exception as e:
            lines.append(f"🎯 OPTIONS: ⚠️ read error: {e}")
    else:
        lines.append(f"🎯 OPTIONS: offline ({_options_err[:40]})")

    lines.append("──────────────────")
    lines.append(f"Generated {now.strftime('%H:%M:%S')} CDT")
    return "\n".join(lines)


# ==============================================================================
# ✅ V10.0: /phase4 — Phase4 bot status
# ==============================================================================
def build_phase4_status() -> str:
    lines = ["⚡ PHASE4 STATUS\n──────────────────"]
    try:
        p4 = nexus_client.phase4_think()
        if not p4 or not p4.get("online"):
            lines.append("⚠️ Service offline or unreachable")
            lines.append("Check Railway nexus-phase4 logs")
            return "\n".join(lines)

        bots = p4.get("bots", {})
        bp   = p4.get("buying_power", 0)

        for sym, b in bots.items():
            bear = b.get("bear_pair", "?")
            if b.get("in_position"):
                active  = b.get("active_sym", sym)
                pnl     = b.get("pnl_pct", 0)
                peak    = b.get("peak_pct", 0)
                mode    = b.get("mode", "?")
                held    = b.get("held_minutes", 0)
                sl      = b.get("stop_pct", 0)
                ratchet = b.get("ratchet_pct", 0)
                lines.append(
                    f"🟢 {sym} [{bear}] — IN POSITION: {active}\n"
                    f"   P&L: {pnl:+.2f}% | Peak: {peak:+.2f}% | Held: {int(held)}m\n"
                    f"   Mode: {mode} | Stop: {sl:.1%} | Ratchet: {ratchet:.1%}"
                )
            else:
                rsi     = b.get("rsi", "?")
                rev     = b.get("reversal_state", "IDLE")
                cd_secs = b.get("cooldown_remaining_secs", 0)
                wins    = b.get("daily_wins", 0)
                losses  = b.get("daily_losses", 0)
                cd_note = f"\n   Cooldown: {int(cd_secs//60)}m remaining" if cd_secs > 0 else ""
                rev_note= f"\n   👁️ Reversal WATCHING — {bear}" if rev == "WATCHING" else ""
                lines.append(
                    f"⚪ {sym} [{bear}] — idle\n"
                    f"   RSI: {rsi} | Today: {wins}W {losses}L{cd_note}{rev_note}"
                )
        lines.append("──────────────────")
        lines.append(f"Buying power: ${bp:.2f}")
        lines.append("Modes: SCALP / RIDE / EXTENDED")
        lines.append("Bear pairs active — reversal machine on")
    except Exception as e:
        lines.append(f"⚠️ Error: {e}")
    return "\n".join(lines)


# ==============================================================================
# TELEGRAM COMMAND HANDLER
# ==============================================================================
def get_updates(offset=None):
    try:
        url    = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        params = {"timeout": 5, "allowed_updates": ["message"]}
        if offset:
            params["offset"] = offset
        r = requests.get(url, params=params, timeout=10)
        return r.json().get("result", [])
    except:
        return []

def handle_commands():
    log("[TEL] T-Bone command listener started")
    last_update_id = None

    while True:
        try:
            updates = get_updates(offset=last_update_id)
            for update in updates:
                last_update_id = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip()
                chat_id = str(msg.get("chat", {}).get("id", ""))

                if chat_id != str(TELEGRAM_CHAT_ID):
                    continue

                cmd = text.lower()

                # -- /think — full system diagnostic ──────────────────----
                if cmd == "/think":
                    try:
                        alert(build_think_report(), critical=True)
                    except Exception as e:
                        alert(f"⚠️ Think error: {e}", critical=True)

                # -- /phase4 — phase4 bot status ──────────────────---------
                elif cmd == "/phase4":
                    try:
                        alert(build_phase4_status(), critical=True)
                    except Exception as e:
                        alert(f"⚠️ Phase4 status error: {e}", critical=True)

                # -- /status — berserker + alpaca overview -----------------
                elif cmd == "/status":
                    try:
                        acct      = trading_client.get_account()
                        cash      = float(acct.cash)
                        equity    = float(acct.equity)
                        positions = get_alpaca_positions()
                        now       = datetime.now(tz=CENTRAL)
                        start_eq  = daily_stats['start_equity']
                        day_pnl   = equity - start_eq
                        day_pct   = round(day_pnl / start_eq * 100, 2) if start_eq > 0 else 0

                        pos_lines = "\n".join([
                            f"  {sym}: "
                            f"{'✅' if float(p.unrealized_plpc)>=0 else '🔴'}"
                            f"{round(float(p.unrealized_plpc)*100,2)}% | "
                            # V10.6: was f"${round(float(p.market_value),2)}" --
                            # market_value is the TOTAL dollar value of the
                            # position (qty * price), not a per-share price.
                            # For whole-share positions qty=1 so the two are
                            # identical and the bug was invisible; for
                            # fractional positions (e.g. 0.3586 TSLA @
                            # $410.67 = $147.26 market_value) it displayed as
                            # if TSLA were trading at $147, which it isn't.
                            # current_price is the actual per-share price.
                            f"${round(float(p.current_price), 2) if p.current_price else round(float(p.avg_entry_price), 2)} | "
                            f"{int(hold_time_minutes(sym))}m"
                            for sym, p in positions.items()
                            if sym in SYMBOLS
                        ]) or "  None"

                        paused_bots = [k.replace("paused_","").upper()
                                       for k,v in bot_state.items()
                                       if k.startswith("paused_") and v]
                        status      = "⏸️ PAUSED" if bot_state["paused"] else "▶️ ACTIVE"
                        if paused_bots:
                            status += f" (⏸️ {', '.join(paused_bots)} paused)"

                        flags = []
                        if bot_state.get("buys_disabled"):
                            flags.append("🚫 Buys disabled")
                        if bot_state["daily_loss_hit"]:
                            flags.append("🚨 Daily loss limit")
                        if not is_market_hours_for_buying(now) and now.weekday() < 5 and now.hour >= 8:
                            flags.append("⏰ After buy cutoff")
                        flag_str = "\n" + "\n".join(flags) if flags else ""

                        alert(
                            f"⚡ NEXUS STATUS\n"
                            f"──────────────────\n"
                            f"🔥 BERSERKER — {status}{flag_str}\n"
                            f"Cash: ${round(cash,2)} | Equity: ${round(equity,2)} "
                            f"({'✅' if day_pnl>=0 else '🔴'}{day_pct:+.2f}% today)\n"
                            f"──────────────────\n"
                            f"Positions:\n{pos_lines}\n"
                            f"Today: {daily_stats['trades']} trades | "
                            f"{daily_stats['wins']}W {daily_stats['losses']}L\n"
                            f"Pattern gate: {WIN_RATE_GATE_THRESHOLD:.0%} WR threshold",
                            critical=True
                        )
                    except Exception as e:
                        alert(f"⚠️ Status error: {e}", critical=True)

                # -- /crypto — crypto service status ──────────────────-----
                elif cmd == "/crypto":
                    try:
                        snap = nexus_client.crypto_snapshot()
                        if not snap or not snap.get("online"):
                            alert("🌙 CRYPTO — service unreachable.", critical=True)
                        else:
                            positions = snap.get("positions", [])
                            balance   = snap.get("usdc_balance", 0.0)
                            total_val = snap.get("total_value", balance)
                            wins      = snap.get("wins", 0)
                            losses    = snap.get("losses", 0)
                            pnl       = snap.get("total_pnl", 0.0)
                            version   = snap.get("version", "V4.x")
                            paused    = snap.get("paused", False)
                            fg        = snap.get("fear_greed", "?")
                            fg_lbl    = snap.get("fear_greed_label", "")
                            session   = snap.get("session", "?")
                            tier      = snap.get("hour_tier", "?")
                            dom       = snap.get("btc_dominance", 0.0)
                            weekend   = snap.get("is_weekend", False)
                            cb_active = snap.get("circuit_break_active", False)
                            buys_off  = snap.get("buys_disabled", False)

                            if paused:       status_str = "PAUSED"
                            elif cb_active:  status_str = "CIRCUIT BREAK"
                            elif buys_off:   status_str = "BUYS OFF"
                            else:            status_str = "ACTIVE"

                            lines = [
                                f"🌙 CRYPTO {version} — {status_str}",
                                "──────────────────",
                                f"Balance: ${balance:.2f} | Total: ${total_val:.2f}",
                                f"P&L: {pnl:+.2f} | {wins}W / {losses}L",
                                f"Session: {session} | Tier: {tier} | "
                                f"{'Weekend' if weekend else 'Weekday'}",
                                f"F&G: {fg} ({fg_lbl}) | BTC Dom: {dom:.1f}%",
                            ]
                            if positions:
                                lines.append("──────────────────")
                                lines.append(f"Open ({len(positions)}):")
                                for p in positions:
                                    pair  = p.get("pair", "?").replace("-USDC", "")
                                    pct   = p.get("pnl_pct", 0.0)
                                    conf  = p.get("confidence", 0)
                                    mode  = p.get("mode", "?")
                                    emoji = "✅" if pct >= 0 else "🔴"
                                    lines.append(
                                        f"  {emoji} {pair}: {pct:+.2f}% | "
                                        f"conf={conf} | {mode}"
                                    )
                            else:
                                lines.append("No open positions")
                            alert("\n".join(lines), critical=True)
                    except Exception as e:
                        alert(f"⚠️ Crypto error: {e}", critical=True)

                # -- /analyst — analyst service status ──────────────────---
                elif cmd == "/analyst":
                    try:
                        snap = nexus_client.analyst_snapshot()
                        if not snap:
                            alert("🔭 ANALYST — service unreachable.", critical=True)
                        else:
                                v_wins   = snap.get("virtual_wins", 0)
                                v_losses = snap.get("virtual_losses", 0)
                                v_open   = snap.get("virtual_open", 0)
                                total    = v_wins + v_losses
                                wr       = round(v_wins / total * 100) if total > 0 else 0
                                events   = snap.get("events", 0)
                                alert(
                                    f"🔭 ANALYST V2.7\n"
                                    f"──────────────────\n"
                                    f"Virtual trades: {total} | {v_wins}W {v_losses}L | {wr}% WR\n"
                                    f"Open: {v_open} | Events: {events}\n"
                                    f"──────────────────\n"
                                    f"Signal Bridge active 📡",
                                    critical=True
                                )
                    except Exception as e:
                        alert(f"⚠️ Analyst error: {e}", critical=True)

                # -- /pnl — unrealized P&L ──────────────────---------------
                elif cmd == "/pnl":
                    try:
                        lines     = ["💰 UNREALIZED P&L\n──────────────────"]
                        total_pnl = 0.0
                        any_pos   = False

                        for sym, pos in get_alpaca_positions().items():
                            pnl      = get_position_pnl_alpaca(pos)
                            pnl_amt  = float(pos.unrealized_pl)
                            held     = hold_time_minutes(sym)
                            emoji    = "✅" if pnl > 0 else "🔴"
                            lines.append(
                                f"  {emoji} {sym}: {pnl*100:+.2f}% "
                                f"(${pnl_amt:+.2f}) | {int(held)}m"
                            )
                            total_pnl += pnl_amt
                            any_pos    = True

                        # Phase4 positions from snapshot
                        snap = nexus_client.crypto_snapshot()  # reuse pattern
                        p4   = nexus_client.phase4_think()
                        if p4 and p4.get("online"):
                            for sym, b in p4.get("bots", {}).items():
                                if b.get("in_position"):
                                    active = b.get("active_sym", sym)
                                    pct    = b.get("pnl_pct", 0)
                                    emoji  = "✅" if pct >= 0 else "🔴"
                                    lines.append(
                                        f"  {emoji} {active} (Phase4): {pct:+.2f}%"
                                    )
                                    any_pos = True

                        if not any_pos:
                            lines.append("  No open positions")

                        lines.append("──────────────────")
                        lines.append(f"Alpaca total: {'✅' if total_pnl>=0 else '🔴'} ${total_pnl:+.2f}")
                        alert("\n".join(lines), critical=True)
                    except Exception as e:
                        alert(f"⚠️ P&L error: {e}", critical=True)

                # -- /equity — total across all brokers ──────────────────--
                elif cmd == "/equity":
                    try:
                        acct       = trading_client.get_account()
                        alpaca_val = float(acct.equity)
                        crypto_val = 0.0
                        phase4_val = 0.0

                        snap = nexus_client.crypto_snapshot()
                        if snap and snap.get("online"):
                            crypto_val = float(snap.get("total_value",
                                               snap.get("usdc_balance", 0)))

                        p4 = nexus_client.phase4_think()
                        if p4 and p4.get("online"):
                            phase4_val = float(p4.get("total_value",
                                               p4.get("buying_power", 0)))

                        total = alpaca_val + crypto_val + phase4_val
                        alert(
                            f"💰 NEXUS EQUITY\n"
                            f"──────────────────\n"
                            f"Alpaca (Berserker): ${alpaca_val:.2f}\n"
                            f"Alpaca (Phase4):    ${phase4_val:.2f}\n"
                            f"Coinbase:           ${crypto_val:.2f}\n"
                            f"──────────────────\n"
                            f"Total: ${total:.2f}",
                            critical=True
                        )
                    except Exception as e:
                        alert(f"⚠️ Equity error: {e}", critical=True)

                # -- /wins — today's W/L per symbol ──────────────────-----
                elif cmd == "/wins":
                    today  = datetime.now(tz=CENTRAL).strftime("%b %d")
                    merged = {s: dict(d) for s, d in _today_symbol_trades.items()}
                    if _scanner_ok and hasattr(scanner, '_today_symbol_trades'):
                        for sym, d in scanner._today_symbol_trades.items():
                            if sym not in merged:
                                merged[sym] = {"bot": "SCANNER", "wins": 0, "losses": 0, "pnl": 0.0}
                            merged[sym]["wins"]   += d["wins"]
                            merged[sym]["losses"] += d["losses"]
                            merged[sym]["pnl"]    = round(merged[sym]["pnl"] + d["pnl"], 3)

                    if not merged:
                        alert(
                            f"📊 No Alpaca trades today ({today}) yet.\n"
                            f"Phase4 trades appear as T-Bone alerts.",
                            critical=True
                        )
                    else:
                        lines    = [f"📊 TODAY — {today}\n──────────────────"]
                        all_syms = sorted(merged.items(), key=lambda x: x[1]["pnl"], reverse=True)
                        for sym, d in all_syms:
                            t     = d["wins"] + d["losses"]
                            wr    = round(d["wins"] / t * 100) if t > 0 else 0
                            emoji = "✅" if d["pnl"] > 0 else "🔴"
                            lines.append(
                                f"  {emoji} {sym} [{d['bot']}]: "
                                f"{d['wins']}W {d['losses']}L | {wr}% | {d['pnl']:+.2f}%"
                            )
                        total_pnl = sum(d["pnl"] for d in merged.values())
                        total_w   = sum(d["wins"] for d in merged.values())
                        total_l   = sum(d["losses"] for d in merged.values())
                        lines.append("──────────────────")
                        lines.append(
                            f"Total: {total_w}W {total_l}L "
                            f"| {total_pnl:+.2f}%"
                        )
                        lines.append("Phase4 trades via T-Bone alerts")
                        alert("\n".join(lines), critical=True)

                # -- /cryptowins — crypto pair breakdown ──────────────────-
                elif cmd == "/cryptowins":
                    today = datetime.now(tz=CENTRAL).strftime("%b %d")
                    snap  = nexus_client.crypto_snapshot()
                    if not snap or not snap.get("online"):
                        alert("🌙 CRYPTO — service unreachable.", critical=True)
                    else:
                        pair_stats   = snap.get("pair_stats", {})
                        wins_total   = snap.get("wins", 0)
                        losses_total = snap.get("losses", 0)
                        lines        = [f"🌙 CRYPTO TODAY — {today}\n──────────────────"]
                        if not pair_stats:
                            lines.append("  No trades yet today")
                        else:
                            sorted_pairs = sorted(
                                pair_stats.items(),
                                key=lambda x: x[1]["pnl"], reverse=True
                            )
                            for pair, s in sorted_pairs:
                                if s["wins"] + s["losses"] == 0:
                                    continue
                                t     = s["wins"] + s["losses"]
                                wr    = round(s["wins"] / t * 100) if t > 0 else 0
                                emoji = "✅" if s["pnl"] > 0 else "🔴"
                                name  = pair.replace("-USDC", "")
                                lines.append(
                                    f"  {emoji} {name}: "
                                    f"{s['wins']}W {s['losses']}L | {wr}% | {s['pnl']:+.2f}%"
                                )
                        lines.append("──────────────────")
                        lines.append(f"Session: {wins_total}W {losses_total}L")
                        alert("\n".join(lines), critical=True)

                # -- /performance — weekly summary ──────────────────-------
                elif cmd == "/performance":
                    try:
                        local_text = trade_log.format_performance()
                        # Crypto session stats from snapshot (V4.2 has no /trades endpoint)
                        snap = nexus_client.crypto_snapshot()
                        cr_lines = ["──────────────────", "🌙 CRYPTO (session)"]
                        if snap and snap.get("online"):
                            wins   = snap.get("wins", 0)
                            losses = snap.get("losses", 0)
                            pnl    = snap.get("total_pnl", 0.0)
                            total  = wins + losses
                            wr     = round(wins / total * 100) if total > 0 else 0
                            cr_lines.append(
                                f"  {total} trades | {wins}W {losses}L | {wr}% WR | "
                                f"P&L: ${pnl:+.2f}"
                            )
                        else:
                            cr_lines.append("  Service unreachable")
                        alert(local_text + "\n" + "\n".join(cr_lines), critical=True)
                    except Exception as e:
                        alert(f"⚠️ Performance error: {e}", critical=True)

                # -- /pause ────────────────────────────────────------------
                elif cmd.startswith("/pause"):
                    parts  = text.split()
                    target = parts[1].upper() if len(parts) > 1 else None
                    BOT_FLAGS = {
                        "BERSERKER": "paused_berserker",
                        "SCANNER":   "paused_scanner",
                        "CRYPTO":    "paused_crypto",
                    }
                    if target and target in BOT_FLAGS:
                        bot_state[BOT_FLAGS[target]] = True
                        if target == "CRYPTO":
                            nexus_client.crypto_control({"paused_crypto": True})
                        alert(
                            f"⏸️ {target} PAUSED\n"
                            f"Other bots still running.\n"
                            f"Send /resume to unpause all.",
                            critical=True
                        )
                    elif target == "PHASE4":
                        alert(
                            "⏸️ PHASE4 is a separate Railway service.\n"
                            "To pause it: send /buys off (stops all new entries)\n"
                            "or redeploy with pause via Railway dashboard.",
                            critical=True
                        )
                    else:
                        bot_state["paused"] = True
                        nexus_client.crypto_control({"paused": True})
                        alert(
                            "⏸️ NEXUS PAUSED\n"
                            "All bots stopped.\n"
                            "Send /resume to restart.",
                            critical=True
                        )

                # -- /resume ────────────────────────────────────-----------
                elif cmd == "/resume":
                    bot_state["paused"]           = False
                    bot_state["paused_berserker"] = False
                    bot_state["paused_scanner"]   = False
                    bot_state["paused_crypto"]    = False
                    bot_state["daily_loss_hit"]   = False
                    nexus_client.crypto_control({
                        "paused":        False,
                        "paused_crypto": False,
                        "buys_disabled": False,
                    })
                    # V10.35: also clear Phase4's killswitch pause
                    _p4_resume = ""
                    _p4url = os.environ.get("PHASE4_URL", "").rstrip("/")
                    if _p4url:
                        try:
                            _p4r = requests.post(f"{_p4url}/resume", json={}, timeout=10)
                            _p4_resume = ("\nPhase4: entries re-enabled"
                                          if _p4r.status_code == 200
                                          else f"\nPhase4: resume HTTP {_p4r.status_code}")
                        except Exception as _p4e:
                            _p4_resume = f"\nPhase4: resume failed ({str(_p4e)[:60]})"
                    alert(f"▶️ NEXUS RESUMED\nAll bots back online!{_p4_resume}", critical=True)

                # -- /scanner_cb_override ────────────────────────────────--
                # V10.28: manual override for Scanner's breadth-based circuit
                # breaker (15/20-of-44-symbols-dropping check, fully local to
                # scanner.py with no prior remote control surface at all --
                # see check_circuit_breaker()/_circuit_breaker_override in
                # scanner.py for the full rationale). This does NOT touch
                # paused_scanner (the separate, unrelated full-stop flag) --
                # it only bypasses the breadth-pause specifically, so Scanner
                # keeps trading through what would otherwise be a circuit-
                # breaker pause while everything else about it (win-rate
                # gate, position limits, capital coordination) stays active.
                elif cmd.startswith("/scanner_cb_override"):
                    parts = text.lower().split()
                    if len(parts) < 2 or parts[1] not in ("on", "off"):
                        alert(
                            "Usage: /scanner_cb_override on|off\n"
                            "──────────────────\n"
                            f"Current: {'🟢 ON (breadth pause bypassed)' if (_scanner_ok and getattr(scanner, '_circuit_breaker_override', False)) else '⚪ OFF (normal behavior)'}\n"
                            f"Dropping now: {getattr(scanner, '_dropping_count', 0) if _scanner_ok else '?'}/20",
                            critical=True
                        )
                    elif not _scanner_ok:
                        alert("⚠️ Scanner module not loaded -- cannot set override", critical=True)
                    else:
                        scanner._circuit_breaker_override = (parts[1] == "on")
                        if parts[1] == "on":
                            alert(
                                "🟢 SCANNER CIRCUIT BREAKER OVERRIDE: ON\n"
                                "──────────────────\n"
                                "Scanner will trade through breadth-based pauses.\n"
                                f"Currently {getattr(scanner, '_dropping_count', 0)}/20 symbols dropping.\n"
                                "Win-rate gate, position limits, and capital\n"
                                "coordination are all still active.\n"
                                "Send /scanner_cb_override off to restore.",
                                critical=True
                            )
                        else:
                            alert(
                                "⚪ SCANNER CIRCUIT BREAKER OVERRIDE: OFF\n"
                                "Normal breadth-pause behavior restored.",
                                critical=True
                            )

                # -- /scanner_aggro ──────────────────────────────────────--
                # V2.7: live nudge for Scanner's confidence-score sizing
                # (see SIZE_PCT_*/PACE_TAPER_*/compute_confidence_score in
                # scanner.py) -- a multiplier on top of the score-based tier
                # and pace taper, not a replacement for either. Clamped
                # scanner-side too (AGGRO_MULT_MIN/MAX) so this is a second
                # line of defense, not the only one.
                elif cmd.startswith("/scanner_aggro"):
                    parts = text.lower().split()
                    if len(parts) < 2:
                        alert(
                            "Usage: /scanner_aggro <0.5-1.5>\n"
                            "──────────────────\n"
                            f"Current: {getattr(scanner, '_aggro_mult', 1.0):.2f}x\n"
                            f"Today's trades: {getattr(scanner, '_today_trade_count', '?') if _scanner_ok else '?'} "
                            f"(pace taper: 0.75x after 3, 0.5x after 6)",
                            critical=True
                        )
                    elif not _scanner_ok:
                        alert("⚠️ Scanner module not loaded -- cannot set aggro", critical=True)
                    else:
                        try:
                            val = float(parts[1])
                        except ValueError:
                            alert(f"⚠️ '{parts[1]}' isn't a number. Usage: /scanner_aggro <0.5-1.5>", critical=True)
                        else:
                            clamped = max(scanner.AGGRO_MULT_MIN, min(scanner.AGGRO_MULT_MAX, val))
                            scanner._aggro_mult = clamped
                            note = "" if clamped == val else f" (clamped from {val} -- range is {scanner.AGGRO_MULT_MIN}-{scanner.AGGRO_MULT_MAX})"
                            alert(
                                f"🎛️ SCANNER AGGRO: {clamped:.2f}x{note}\n"
                                "──────────────────\n"
                                "Applies on top of the confidence-score tier\n"
                                "and today's pace taper -- doesn't touch the\n"
                                "win-rate gate, hard entry gate, or capital\n"
                                "coordination. Send /scanner_aggro 1.0 to reset.",
                                critical=True
                            )

                # -- /buys ────────────────────────────────────-------------
                elif cmd.startswith("/buys"):
                    parts = text.lower().split()
                    if len(parts) >= 2 and parts[1] == "off":
                        bot_state["buys_disabled"] = True
                        nexus_client.crypto_control({"buys_disabled": True})
                        alert(
                            "🚫 BUYS DISABLED\n"
                            "All bots manage exits only.\n"
                            "Send /buys on to re-enable.",
                            critical=True
                        )
                    elif len(parts) >= 2 and parts[1] == "on":
                        bot_state["buys_disabled"] = False
                        nexus_client.crypto_control({"buys_disabled": False})
                        alert("✅ BUYS ENABLED\nAll bots back to normal.", critical=True)
                    else:
                        status = "OFF 🚫" if bot_state.get("buys_disabled") else "ON ✅"
                        alert(f"Buys are currently: {status}\nUse /buys on or /buys off", critical=True)

                # -- /close SYMBOL ────────────────────────────────────-----
                elif cmd.startswith("/close "):
                    parts = text.split()
                    if len(parts) >= 2:
                        result = find_and_close(parts[1].upper())
                        alert(result, critical=True)
                    else:
                        alert("Usage: /close SYMBOL", critical=True)

                # -- /closeall ────────────────────────────────────---------
                elif cmd == "/closeall":
                    try:
                        positions = trading_client.get_all_positions()
                        if not positions:
                            alert("? No open Alpaca positions.", critical=True)
                        else:
                            alert(
                                f"🚨 CLOSING ALL {len(positions)} ALPACA POSITIONS...",
                                critical=True
                            )
                            for p in positions:
                                try:
                                    close_alpaca_position(
                                        p.symbol.replace("/USD","").replace("USD","")
                                    )
                                except:
                                    pass
                            bot_state["paused"] = True
                            nexus_client.crypto_control({"paused": True})
                            alert(
                                "✅ All Alpaca positions closed.\n"
                                "NEXUS PAUSED.\n"
                                "Send /resume when ready.",
                                critical=True
                            )
                    except Exception as e:
                        alert(f"⚠️ Close all error: {e}", critical=True)

                # -- /killswitch — emergency stop everything ---------------
                elif cmd == "/killswitch":
                    try:
                        alert("🚨 KILLSWITCH ACTIVATED — closing all positions...", critical=True)

                        # V10.35: Phase4 FIRST -- it shares this Alpaca
                        # account. Its /close_all closes its own positions
                        # through its bots (clean state + fingerprinted
                        # exits) and pauses its entries. If we blanket-closed
                        # the account first, Phase4's bots would be left
                        # holding phantom state.
                        phase4_note = "manual close required (PHASE4_URL not set)"
                        _p4url = os.environ.get("PHASE4_URL", "").rstrip("/")
                        if _p4url:
                            try:
                                _p4r = requests.post(f"{_p4url}/close_all",
                                                     json={}, timeout=25)
                                _p4j = {}
                                try:
                                    _p4j = _p4r.json()
                                except Exception:
                                    pass
                                phase4_note = (f"{_p4j.get('closed', '?')} position(s) "
                                               f"closed, entries paused"
                                               if _p4r.status_code == 200
                                               else f"close_all HTTP {_p4r.status_code}")
                            except Exception as _p4e:
                                phase4_note = f"close_all FAILED: {str(_p4e)[:80]}"

                        # Close all remaining Alpaca positions (Berserker/Scanner)
                        positions = trading_client.get_all_positions()
                        for p in positions:
                            try:
                                close_alpaca_position(
                                    p.symbol.replace("/USD","").replace("USD","")
                                )
                            except:
                                pass

                        # Close all crypto positions
                        nexus_client._post(f"{nexus_client.CRYPTO_URL}/closeall", {})

                        # V10.4: Close any open options position + disable engine
                        options_note = "n/a"
                        if _options_ok:
                            try:
                                options_engine.set_enabled(False)
                                if options_engine.get_state().get("status") == "OPEN":
                                    options_note = options_engine.force_close("killswitch")
                                else:
                                    options_note = "no open position, engine disabled"
                            except Exception as _oe:
                                options_note = f"error: {_oe}"

                        # Pause everything
                        bot_state["paused"]           = True
                        bot_state["paused_berserker"] = True
                        bot_state["paused_scanner"]   = True
                        bot_state["paused_crypto"]    = True
                        bot_state["buys_disabled"]    = True
                        nexus_client.crypto_control({
                            "paused":        True,
                            "buys_disabled": True,
                        })

                        alpaca_closed = len(positions)
                        alert(
                            f"🚨 KILLSWITCH COMPLETE\n"
                            f"──────────────────\n"
                            f"Alpaca: {alpaca_closed} position(s) closed\n"
                            f"Crypto: close signal sent\n"
                            f"Options: {options_note}\n"
                            f"Phase4: {phase4_note}\n"
                            f"──────────────────\n"
                            f"ALL BOTS PAUSED\n"
                            f"Send /resume when ready.",
                            critical=True
                        )
                    except Exception as e:
                        alert(f"⚠️ Killswitch error: {e}", critical=True)

                # -- /stayopen SYMBOL ────────────────────────────────────--
                elif cmd.startswith("/stayopen "):
                    parts = text.split()
                    if len(parts) >= 2:
                        symbol = parts[1].upper()
                        stayopen_symbols.add(symbol)
                        alert(
                            f"🔓 {symbol} marked as stay-open.\n"
                            f"Won't be auto-closed at EOD.",
                            critical=True
                        )

                # -- /cooldown SYMBOL [min] ──────────────────--------------
                elif cmd.startswith("/cooldown"):
                    parts = text.split()
                    if len(parts) >= 2:
                        sym  = parts[1].upper()
                        mins = int(parts[2]) if len(parts) >= 3 else 30
                        mins = max(1, min(mins, 480))
                        shared_state.set_cooldown(sym, mins * 60)
                        alert(
                            f"⏳ Cooldown set: {sym} | {mins} min\n"
                            f"Bots won't touch it until then.",
                            critical=True
                        )
                    else:
                        alert("Usage: /cooldown SYMBOL [minutes]\nDefault: 30 min", critical=True)

                # -- /quiet [min] ────────────────────────────────────------
                elif cmd.startswith("/quiet"):
                    parts   = text.split()
                    minutes = 30
                    if len(parts) > 1:
                        try:
                            minutes = int(parts[1])
                        except ValueError:
                            pass
                    minutes = max(1, min(minutes, 480))
                    bot_state["quiet_until"] = time.time() + minutes * 60
                    alert(
                        f"🔕 T-Bone going quiet for {minutes} min\n"
                        f"Send /unquiet to cancel.",
                        critical=True
                    )

                # -- /unquiet ────────────────────────────────────----------
                elif cmd == "/unquiet":
                    bot_state["quiet_until"] = None
                    alert("🔔 T-Bone back — all alerts resumed.", critical=True)

                # -- /friday on|off ────────────────────────────────────----
                elif cmd == "/friday" or cmd.startswith("/friday "):
                    global _friday_buy_enabled
                    parts  = text.split()
                    action = parts[1].lower() if len(parts) > 1 else ""
                    if action == "on":
                        _friday_buy_enabled                = True
                        bot_state["friday_buy_enabled"]    = True
                        nexus_client.crypto_control({"friday_buy_enabled": True})
                        alert(
                            "🟡 FRIDAY BUYING ENABLED\n"
                            "──────────────────\n"
                            "Crypto params locked to CAUTIOUS:\n"
                            "  — 60% position size\n"
                            "  — Offpeak score gates\n"
                            "──────────────────\n"
                            "Send /friday off to restore full block.",
                            critical=True
                        )
                    elif action == "off":
                        _friday_buy_enabled             = False
                        bot_state["friday_buy_enabled"] = False
                        nexus_client.crypto_control({"friday_buy_enabled": False})
                        alert(
                            "🚫 FRIDAY BLOCK RESTORED\n"
                            "No new crypto entries on Friday.",
                            critical=True
                        )
                    else:
                        status = "🟡 ON (cautious)" if _friday_buy_enabled else "🚫 OFF (full block)"
                        alert(
                            f"📅 Friday buying: {status}\n"
                            f"Use /friday on or /friday off",
                            critical=True
                        )

                # -- /help ────────────────────────────────────-------------

                # ── /followwins ── V10.29: Win Follower tiers, all services ──
                elif cmd == "/followwins":
                    lines = ["🏆 WIN FOLLOWER\n──────────────────"]
                    _tier_icons = {"HOT": "🔥", "WARM": "🌤", "NEUTRAL": "➖", "COLD": "🧊"}
                    # Berserker (local)
                    lines.append("⚔️ BERSERKER")
                    if _win_follower and _win_follower._enabled:
                        _st = _win_follower.get_status()
                        for _s, _t in sorted(_st["tiers"].items(),
                                             key=lambda x: ("HOT", "WARM", "NEUTRAL", "COLD").index(x[1]["tier"])):
                            _b = " (benched)" if _s in _st["benched"] else ""
                            lines.append(f"  {_tier_icons[_t['tier']]} {_s}: "
                                         f"{_t['live_wr']:.0%}/{_t['live_trades']}t "
                                         f"{_t['live_pnl_sum']:+.1f}%{_b}")
                    else:
                        lines.append("  disabled")
                    # Scanner (same process)
                    lines.append("📡 SCANNER")
                    _swf = getattr(scanner, "_win_follower", None) if _scanner_ok else None
                    if _swf and getattr(_swf, "_enabled", False):
                        _st = _swf.get_status()
                        _sh = [s for s, t in _st["tiers"].items() if t["tier"] == "HOT"]
                        lines.append(f"  🔥 HOT: {', '.join(_sh) if _sh else 'none'}")
                        lines.append(f"  🧊 Benched: {', '.join(_st['benched']) if _st['benched'] else 'none'}")
                    else:
                        lines.append("  disabled")
                    # Crypto (HTTP)
                    lines.append("🪙 CRYPTO")
                    try:
                        _cwf = nexus_client._get(f"{nexus_client.CRYPTO_URL}/winfollower")
                        if _cwf and _cwf.get("enabled"):
                            for _p, _t in sorted(_cwf.get("tiers", {}).items(),
                                                 key=lambda x: ("HOT", "WARM", "NEUTRAL", "COLD").index(x[1]["tier"])):
                                if _t["tier"] != "NEUTRAL":
                                    lines.append(f"  {_tier_icons[_t['tier']]} {_p.replace('-USDC','')}: "
                                                 f"{_t['live_wr']:.0%}/{_t['live_trades']}t live "
                                                 f"({_t['paper_wr']:.0%}/{_t['paper_trades']}t paper)")
                            if not any(_t["tier"] != "NEUTRAL" for _t in _cwf.get("tiers", {}).values()):
                                lines.append("  all NEUTRAL (building history)")
                        else:
                            lines.append("  offline or pre-V5.3")
                    except Exception:
                        lines.append("  unreachable")
                    lines.append("──────────────────\nPhase4 reweights budgets on its own -- alerts on change")
                    alert("\n".join(lines))

                # ── /thorn [hours] ── V10.32: what the crypto walls did ──
                elif cmd.startswith("/thorn"):
                    _parts = cmd.split()
                    try:
                        _hrs = max(1, min(int(_parts[1]), 720)) if len(_parts) > 1 else 24
                    except Exception:
                        _hrs = 24
                    lines = [f"🌵 THORN ({_hrs}h)\n──────────────────"]
                    try:
                        _t = nexus_client._get(f"{nexus_client.CRYPTO_URL}/thorn?hours={_hrs}")
                        _grp = (_t or {}).get("groups") or {}
                        if not _grp:
                            lines.append("no resolved observations yet -- give it a few hours")
                        for _k, _g in sorted(_grp.items(), key=lambda x: -x[1]["n"]):
                            _a4 = f"{_g['avg_4h']:+.2f}%" if _g.get("avg_4h") is not None else "?"
                            lines.append(f"{_k}\n  n={_g['n']} | 1h {_g['avg_1h']:+.2f}% "
                                         f"({_g['up_1h_pct']:.0f}% up) | 4h {_a4}")
                        lines.append("──────────────────\nPositive avg on a BLOCK = wall may be costing money")
                    except Exception:
                        lines.append("crypto service unreachable")
                    alert("\n".join(lines))

                # ── /autopsy [days] ── V10.32: exit grades, crypto + equities ──
                elif cmd.startswith("/autopsy"):
                    _parts = cmd.split()
                    try:
                        _dys = max(1, min(int(_parts[1]), 30)) if len(_parts) > 1 else 7
                    except Exception:
                        _dys = 7
                    lines = [f"🩺 EXIT AUTOPSY ({_dys}d)\n──────────────────", "🪙 CRYPTO"]
                    try:
                        _a = nexus_client._get(f"{nexus_client.CRYPTO_URL}/autopsy?days={_dys}")
                        _ex = (_a or {}).get("exits") or {}
                        if not _ex:
                            lines.append("  no resolved exits yet")
                        for _r, _g in sorted(_ex.items(), key=lambda x: -x[1]["n"]):
                            _cap = f"{_g['capture_pct']:.0f}%" if _g.get("capture_pct") is not None else "?"
                            _p1  = f"{_g['post_1h_avg']:+.2f}%" if _g.get("post_1h_avg") is not None else "?"
                            lines.append(f"  {_r}: n={_g['n']} | kept {_cap} of peak | post-1h {_p1}")
                    except Exception:
                        lines.append("  crypto service unreachable")
                    lines.append("⚔️ EQUITIES")
                    _eq = equity_autopsy_summary(_dys)
                    if not _eq:
                        lines.append("  no resolved exits yet")
                    for _k, _g in sorted(_eq.items(), key=lambda x: -x[1]["n"]):
                        _cap = f"{_g['capture_pct']:.0f}%" if _g.get("capture_pct") is not None else "?"
                        _p1  = f"{_g['post_1h_avg']:+.2f}%" if _g.get("post_1h_avg") is not None else "?"
                        lines.append(f"  {_k}: n={_g['n']} | kept {_cap} of peak | post-1h {_p1}")
                    lines.append("──────────────────\nLow capture + positive post-1h = exiting too early")
                    alert("\n".join(lines))

                # ── /patterns ── combined Berserker + crypto pattern memory ──
                elif cmd == "/patterns":
                    lines = ["🧠 PATTERN MEMORY\n──────────────────"]

                    # -- Berserker (read from in-memory _win_rates cache) -----
                    lines.append("🔥 BERSERKER")
                    try:
                        if _berserker_memory and _berserker_memory._enabled:
                            win_rates   = _berserker_memory._win_rates
                            loaded_bkts = len(win_rates)
                            if loaded_bkts == 0:
                                lines.append("  Buckets not loaded yet (scheduler runs 5min after boot)")
                            else:
                                sorted_bkts = sorted(win_rates.items(), key=lambda x: x[1])
                                blocked     = [(k, v) for k, v in sorted_bkts if v < WIN_RATE_GATE_THRESHOLD]
                                top         = sorted(win_rates.items(), key=lambda x: -x[1])[:3]
                                # V10.16: live count from DB instead of hardcoded
                                fp_count = 0
                                try:
                                    conn = _berserker_memory._get_conn()
                                    if conn:
                                        with conn.cursor() as _cur:
                                            _cur.execute("SELECT COUNT(*) FROM berserker_trade_fingerprints WHERE won IS NOT NULL")
                                            fp_count = _cur.fetchone()[0]
                                except Exception:
                                    pass
                                lines.append(f"  Fingerprints: {fp_count:,}")
                                lines.append(f"  Buckets loaded: {loaded_bkts}")
                                lines.append(f"  Blocked (below {round(WIN_RATE_GATE_THRESHOLD*100)}% WR): {len(blocked)}")
                                if top:
                                    lines.append("  Top setups:")
                                    for k, v in top:
                                        sym = k.split("|")[0]
                                        lines.append(f"    ✅ {sym} {round(v*100)}% WR")
                                if blocked[:5]:
                                    lines.append("  Blocked setups:")
                                    for k, v in blocked[:5]:
                                        sym = k.split("|")[0]
                                        hr  = k.split("|")[5] if len(k.split("|")) > 5 else "?"
                                        lines.append(f"    🚫 {sym} {round(v*100)}% WR | {hr}")
                        else:
                            lines.append("  ⚠️ Pattern memory disabled (no DB)")
                    except Exception as e:
                        lines.append(f"  ⚠️ Error: {e}")

                    # -- Crypto (crypto service /pattern_stats endpoint) ------
                    lines.append("──────────────────")
                    lines.append("🌙 CRYPTO")
                    try:
                        r = requests.get(
                            f"{nexus_client.CRYPTO_URL}/pattern_stats",
                            headers={"X-Nexus-Token": nexus_client.TOKEN},
                            timeout=10
                        )
                        if r.status_code != 200:
                            lines.append("  ⚠️ Crypto service unreachable")
                        else:
                            d          = r.json()
                            total      = d.get("total_completed_trades", 0)
                            min_t      = d.get("min_trades_for_analysis", 10)
                            ready      = d.get("ready_for_analysis", False)
                            bkt_count  = d.get("bucket_count", 0)
                            last_ago   = d.get("last_analysis_hrs_ago")
                            buckets    = d.get("buckets", {})
                            lines.append(f"  Trades: {total} / {min_t} min | {'✅ ready' if ready else '🔴 not yet'}")
                            lines.append(f"  Buckets: {bkt_count}")
                            if last_ago is not None:
                                lines.append(f"  Last analysis: {last_ago}h ago")
                            if buckets:
                                lines.append("  Top buckets:")
                                for key, val in list(buckets.items())[:3]:
                                    wr = val.get("win_rate", 0)
                                    lines.append(f"    ✅ {round(wr*100)}% WR | {key[:35]}")
                            else:
                                lines.append(f"  Need {max(0, min_t - total)} more trades")
                    except Exception as e:
                        lines.append(f"  ⚠️ Error: {e}")

                    alert("\n".join(lines), critical=True)

                # ── /crypto_patterns ── crypto-only pattern memory ───────────
                elif cmd == "/crypto_patterns":
                    try:
                        r = requests.get(
                            f"{nexus_client.CRYPTO_URL}/pattern_stats",
                            headers={"X-Nexus-Token": nexus_client.TOKEN},
                            timeout=10
                        )
                        if r.status_code != 200:
                            alert("🧠 CRYPTO PATTERNS — service unreachable.", critical=True)
                        else:
                            d            = r.json()
                            total        = d.get("total_completed_trades", 0)
                            bucket_count = d.get("bucket_count", 0)
                            ready        = d.get("ready_for_analysis", False)
                            min_t        = d.get("min_trades_for_analysis", 10)
                            last_ago     = d.get("last_analysis_hrs_ago")
                            next_hrs     = d.get("next_analysis_hrs")
                            interval     = d.get("analysis_interval", "daily")
                            buckets      = d.get("buckets", {})
                            lines = ["🌙 CRYPTO PATTERN MEMORY", "─" * 18]
                            lines.append(f"Completed trades: {total} / {min_t} min")
                            lines.append(f"Ready for analysis: {'✅ Yes' if ready else '🔴 Not yet'}")
                            lines.append(f"Analysis: {interval}")
                            if last_ago is not None:
                                lines.append(f"Last run: {last_ago}h ago")
                            if next_hrs is not None:
                                lines.append(f"Next run: in ~{next_hrs}h")
                            lines.append(f"Active buckets: {bucket_count}")
                            lines.append("─" * 18)
                            if buckets:
                                lines.append("Top win-rate buckets:")
                                for key, val in list(buckets.items())[:8]:
                                    wr = val.get("win_rate", 0)
                                    lines.append(f"  {round(wr*100)}% WR | {key[:45]}")
                                if len(buckets) > 8:
                                    lines.append(f"  ... +{len(buckets)-8} more")
                            else:
                                lines.append("No pattern buckets yet")
                                lines.append(f"Need {max(0, min_t - total)} more trades")
                            alert("\n".join(lines), critical=True)
                    except Exception as e:
                        alert(f"⚠️ Crypto patterns error: {e}", critical=True)

                # ── /earnings ── earnings calendar blackout status ──────────
                elif cmd == "/earnings":
                    alert(build_earnings_status(), critical=True)

                # ── /vix ── VIX regime status ───────────────────────────────
                elif cmd == "/vix":
                    vix_st = get_vix_status()
                    lines  = [f"📊 VIX STATUS\n──────────────────",
                              f"{vix_st['emoji']} VIX: {vix_st['level']} ({vix_st['label']})"]
                    if vix_st['level'] == 0.0:
                        lines.append("Not yet fetched (waits for market hours)")
                    elif vix_st['blocking']:
                        lines.append(f"🚫 New entries BLOCKED (VIX > {VIX_BLOCK_THRESHOLD})")
                        if vix_st['extreme']:
                            lines.append(f"🚨 MAX_POSITIONS → 1 (VIX > {VIX_EXTREME_THRESHOLD})")
                    elif _vix_level_smooth >= VIX_WARN_THRESHOLD:
                        lines.append(f"⚠️ Elevated — monitoring (> {VIX_WARN_THRESHOLD})")
                    else:
                        lines.append("✅ Normal — full operation")
                    lines.append("──────────────────")
                    lines.append(f"Thresholds: warn {VIX_WARN_THRESHOLD} | block {VIX_BLOCK_THRESHOLD} | extreme {VIX_EXTREME_THRESHOLD}")
                    src_label = {"yfinance": "real ^VIX via yfinance",
                                 "vixy_proxy": "⚠️ VIXY proxy fallback (yfinance unavailable)",
                                 "none": "⚠️ no source available"}.get(_vix_source, _vix_source)
                    lines.append(f"Source: {src_label}, 5-bar smoothed")
                    alert("\n".join(lines), critical=True)

                # ── /regime ── regime aggregator score ─────────────────────
                elif cmd == "/regime":
                    alert(build_regime_status(), critical=True)

                # ── /run_strategist ── trigger strategy pipeline ───────────
                elif cmd == "/run_strategist":
                    alert("🧠 Running strategy pipeline... this takes ~30s", critical=True)
                    try:
                        result = run_strategy_pipeline()
                        alert(result, critical=True)
                    except Exception as e:
                        alert(f"⚠️ Strategy pipeline error: {e}", critical=True)

                # ── /reload_recipes ── reload strategy recipes from file ────
                elif cmd == "/reload_recipes":
                    old_recipes = {sym: dict(rec) for sym, rec in BERSERKER_RECIPES.items()}
                    apply_strategy_recipes_file()
                    changes = []
                    for sym in BERSERKER_RECIPES:
                        if sym not in old_recipes:
                            continue
                        old_tp = old_recipes[sym].get("tp", TAKE_PROFIT_PCT)
                        old_sl = old_recipes[sym].get("sl", STOP_LOSS_PCT)
                        new_tp = BERSERKER_RECIPES[sym].get("tp", TAKE_PROFIT_PCT)
                        new_sl = BERSERKER_RECIPES[sym].get("sl", STOP_LOSS_PCT)
                        if abs(new_tp - old_tp) > 0.001 or abs(new_sl - old_sl) > 0.001:
                            changes.append(f"  {sym}: TP {old_tp*100:.1f}%→{new_tp*100:.1f}% SL {old_sl*100:.1f}%→{new_sl*100:.1f}%")
                    if changes:
                        alert(
                            f"🔄 RECIPES RELOADED\n──────────────────\n"
                            + "\n".join(changes) + "\n──────────────────\n"
                            "Changes are live immediately.",
                            critical=True
                        )
                    else:
                        import os as _os
                        exists = _os.path.exists(STRATEGY_RECIPES_FILE)
                        alert(
                            f"🔄 Recipes reloaded — no changes.\n"
                            f"File: {'✅ exists' if exists else '⚠️ not found'}\n"
                            f"Run /run_strategist to generate.",
                            critical=True
                        )

                # ── /opp ── opportunity scanner status ─────────────────────
                elif cmd == "/opp":
                    try:
                        r = requests.get(
                            f"{nexus_client.CRYPTO_URL}/opportunity",
                            headers={"X-Nexus-Token": nexus_client.TOKEN},
                            timeout=10
                        )
                        if r.status_code != 200:
                            alert("🎯 OPP SCANNER — crypto service unreachable.", critical=True)
                        else:
                            d = r.json()
                            tier        = d.get("tier", 0)
                            met         = d.get("conditions_met", 0)
                            rsi         = d.get("btc_rsi_15m")
                            price       = d.get("btc_price")
                            fg          = d.get("fg")
                            rsi_curl    = d.get("rsi_curl", False)
                            higher_lows = d.get("higher_lows_15m", False)
                            cap_candle  = d.get("capitulation_candle", False)
                            fear        = d.get("fg_extreme_fear", False)
                            rsi_hist    = d.get("rsi_history", [])
                            t2_today    = d.get("tier2_events_today", 0)

                            tier_label = (
                                "🎯 TIER 2 — OPTIONS WORTHY" if tier == 2 else
                                "👀 TIER 1 — SETUP FORMING"  if tier == 1 else
                                "🚫 NO SETUP"
                            )
                            lines = [
                                f"🧠 OPPORTUNITY SCANNER",
                                "─" * 18,
                                f"Status: {tier_label}",
                                f"Conditions: {met}/4 met",
                                "─" * 18,
                            ]
                            if price:
                                lines.append(f"BTC: ${price:,.0f}")
                            if rsi is not None:
                                rsi_str = f"{rsi:.1f}"
                                if rsi_hist:
                                    rsi_str += f" (hist: {' → '.join(str(round(r,1)) for r in rsi_hist[-3:])})"
                                lines.append(f"RSI 15m: {rsi_str}")
                            if fg is not None:
                                lines.append(f"F&G: {fg} ({'Extreme Fear' if fear else 'Fear' if fg < 25 else 'Neutral'})")
                            lines.append("─" * 18)
                            _ok  = "✅"
                            _wait = "⏳"
                            lines.append(f"{_ok if rsi_curl    else _wait} RSI curling up from low")
                            lines.append(f"{_ok if higher_lows else _wait} Higher lows on 15m")
                            lines.append(f"{_ok if cap_candle  else _wait} Capitulation candle")
                            lines.append(f"{_ok if fear        else _wait} Extreme Fear (F&G < 15)")
                            if t2_today > 0:
                                lines.append(f"─" * 18)
                                lines.append(f"Tier 2 alerts today: {t2_today}")
                            alert("\n".join(lines), critical=True)
                    except Exception as e:
                        alert(f"⚠️ /opp error: {e}", critical=True)

                # ── /options ── options engine status ──────────────────────
                elif cmd == "/options":
                    if not _options_ok:
                        alert(f"🎯 OPTIONS ENGINE — module not loaded: {_options_err}", critical=True)
                    else:
                        try:
                            alert(options_engine.get_status_text(), critical=True)
                        except Exception as e:
                            alert(f"⚠️ /options error: {e}", critical=True)

                # ── /options_on ── enable auto-buy on Tier 2 ────────────────
                elif cmd == "/options_on":
                    if not _options_ok:
                        alert(f"🎯 OPTIONS ENGINE — module not loaded: {_options_err}", critical=True)
                    else:
                        try:
                            alert(options_engine.set_enabled(True), critical=True)
                        except Exception as e:
                            alert(f"⚠️ /options_on error: {e}", critical=True)

                # ── /options_off ── disable auto-buy ────────────────────────
                elif cmd == "/options_off":
                    if not _options_ok:
                        alert(f"🎯 OPTIONS ENGINE — module not loaded: {_options_err}", critical=True)
                    else:
                        try:
                            alert(options_engine.set_enabled(False), critical=True)
                        except Exception as e:
                            alert(f"⚠️ /options_off error: {e}", critical=True)

                # ── /options_close ── manually close open options position ──
                elif cmd == "/options_close":
                    if not _options_ok:
                        alert(f"🎯 OPTIONS ENGINE — module not loaded: {_options_err}", critical=True)
                    else:
                        try:
                            alert(options_engine.force_close(), critical=True)
                        except Exception as e:
                            alert(f"⚠️ /options_close error: {e}", critical=True)

                # ── /options_paper ── switch options engine to paper mode ──
                elif cmd == "/options_paper":
                    if not _options_ok:
                        alert(f"🎯 OPTIONS ENGINE — module not loaded: {_options_err}", critical=True)
                    else:
                        try:
                            alert(options_engine.set_paper_mode(True), critical=True)
                        except Exception as e:
                            alert(f"⚠️ /options_paper error: {e}", critical=True)

                # ── /options_live [force] ── V10.33: graduation-gated ──────
                elif cmd.startswith("/options_live"):
                    if not _options_ok:
                        alert(f"🎯 OPTIONS ENGINE — module not loaded: {_options_err}", critical=True)
                    else:
                        try:
                            # V10.33: graduation-gated; 'force' overrides
                            _force = "force" in cmd
                            alert(options_engine.set_paper_mode(False, force=_force), critical=True)
                        except Exception as e:
                            alert(f"⚠️ /options_live error: {e}", critical=True)

                # ── /options_stats ── paper vs live win-rate summary ───────
                elif cmd == "/options_stats":
                    if not _options_ok:
                        alert(f"🎯 OPTIONS ENGINE — module not loaded: {_options_err}", critical=True)
                    else:
                        try:
                            alert(options_engine.get_stats_text(), critical=True)
                        except Exception as e:
                            alert(f"⚠️ /options_stats error: {e}", critical=True)


                elif cmd == "/help":
                    alert(
                        "⚡ NEXUS COMMANDS\n"
                        "──────────────────\n"
                        "INTEL\n"
                        "/status — Berserker + Alpaca overview\n"
                        "/think — Full system diagnostic (all bots)\n"
                        "/phase4 — Phase4 bot state (Alpaca)\n"
                        "/crypto — Crypto service status\n"
                        "/analyst — Signal intelligence status\n"
                        "/pnl — Unrealized P&L across brokers\n"
                        "/equity — Total equity across all accounts\n"
                        "/wins — Today's W/L per symbol\n"
                        "/cryptowins — Today's W/L per crypto pair\n"
                        "/performance — Weekly trade summary\n"
                        "/patterns — Berserker + Crypto pattern memory\n"
                        "/crypto_patterns — Crypto pattern detail\n"
                        "/opp — Opportunity scanner (BTC setup)\n"
                        "/earnings — Earnings blackout status 🆕\n"
                        "/vix — VIX regime gate status 🆕\n"
                        "/regime — Cross-system regime score 🆕\n"
                        "/followwins — Win Follower tiers, all services 🆕\n"
                        "/thorn [hours] — What crypto's walls did (blocked-setup outcomes) 🆕\n"
                        "/autopsy [days] — Exit grades: capture ratio + post-exit runs 🆕\n"
                        "──────────────────\n"
                        "STRATEGY\n"
                        "/run_strategist — Pull DB + run strategy pipeline 🆕\n"
                        "/reload_recipes — Apply updated strategy_recipes.json 🆕\n"
                        "──────────────────\n"
                        "CONTROL\n"
                        "/pause — Pause all bots\n"
                        "/pause BERSERKER|SCANNER|CRYPTO\n"
                        "/resume — Resume all + clear pause flags\n"
                        "/buys on|off — Toggle new entries\n"
                        "/scanner_cb_override on|off — Bypass Scanner's breadth circuit breaker 🆕\n"
                        "/scanner_aggro &lt;0.5-1.5&gt; — Nudge Scanner's sizing live 🆕\n"
                        "/close SYMBOL — Close a position\n"
                        "/closeall — Close all Alpaca positions\n"
                        "/stayopen SYMBOL — Skip EOD auto-close\n"
                        "/cooldown SYMBOL [min] — Manual cooldown\n"
                        "/friday on|off — Friday crypto toggle\n"
                        "/quiet [min] — Silence alerts (default 30m)\n"
                        "/unquiet — Resume alerts\n"
                        "──────────────────\n"
                        "OPTIONS ENGINE\n"
                        "/options — Status (mode, position, P&L)\n"
                        "/options_on — Enable Tier 2 auto-buy\n"
                        "/options_off — Disable auto-buy\n"
                        "/options_paper — Switch to paper trading\n"
                        "/options_live — Switch to LIVE (real $)\n"
                        "/options_close — Manually close open position\n"
                        "/options_stats — Paper vs live win-rate\n"
                        "──────────────────\n"
                        "EMERGENCY\n"
                        "/killswitch — Close Alpaca+Crypto, pause all (Phase4 needs manual close)\n"
                        "──────────────────\n"
                        "Phase4 trade alerts appear automatically 🤖",
                        critical=True
                    )

                # V10.30: unknown slash-commands used to vanish silently
                elif cmd.startswith("/"):
                    alert(f"❓ Unknown command: {cmd.split()[0]} — try /help", critical=True)

            time.sleep(2)

        except Exception as e:
            log(f"⚠️ T-Bone listener error: {e}")
            time.sleep(5)


# ==============================================================================
# DAILY REPORT
# ==============================================================================
_last_report_date = None

def maybe_send_daily_report(total_equity: float):
    global _last_report_date
    now = datetime.now(tz=CENTRAL)
    if now.weekday() >= 5:
        return
    if now.hour == 15 and now.minute < 5:
        today = now.date()
        if _last_report_date != today:
            _last_report_date = today
            start    = daily_stats["start_equity"]
            pnl      = total_equity - start
            pnl_pct  = (pnl / start * 100) if start > 0 else 0

            # V2.6: was daily_stats["trades"/"wins"/"losses"] -- Berserker's
            # own counters, same root bug as the /wins Total line below.
            # Sum from the merged Berserker+Scanner per-symbol dict instead,
            # same pattern /wins already uses correctly for its per-symbol
            # rows, just not for its own Total line until now.
            merged = {s: dict(d) for s, d in _today_symbol_trades.items()}
            if _scanner_ok and hasattr(scanner, '_today_symbol_trades'):
                for sym, d in scanner._today_symbol_trades.items():
                    if sym not in merged:
                        merged[sym] = {"bot": "SCANNER", "wins": 0, "losses": 0, "pnl": 0.0}
                    merged[sym]["wins"]   += d["wins"]
                    merged[sym]["losses"] += d["losses"]
            wins     = sum(d["wins"] for d in merged.values())
            losses   = sum(d["losses"] for d in merged.values())
            trades   = wins + losses
            win_rate = round(wins / trades * 100) if trades > 0 else 0
            alert(
                f"📊 NEXUS DAILY REPORT\n"
                f"Date: {today.strftime('%b %d, %Y')}\n"
                f"──────────────────\n"
                f"Start: ${round(start,2)} — End: ${round(total_equity,2)}\n"
                f"P&L: {'✅' if pnl>=0 else '🔴'} ${round(pnl,2)} ({round(pnl_pct,2)}%)\n"
                f"──────────────────\n"
                f"Trades: {trades} | {wins}W {losses}L | {win_rate}% WR\n"
                f"──────────────────\n"
                f"{'📈 Good day!' if pnl>=0 else '📉 Tough day, back tomorrow!'}",
                critical=True
            )
            log("📊 T-Bone sent daily report")
            try:
                acct = trading_client.get_account()
                reset_daily_state(float(acct.equity))
            except:
                pass


# ==============================================================================
# V10.1: BERSERKER PATTERN MEMORY
# ==============================================================================
class BerserkerMemory:
    """
    Pattern memory for BERSERKER trades on Alpaca.
    Same architecture as Phase4Memory and crypto PatternMemory.
    Separate tables: berserker_trade_fingerprints, berserker_pattern_stats.
    """
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
        except Exception as e:
            log(f"[BM] DB connect error: {e}")
            return None

    def init_tables(self):
        if not self._enabled:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS berserker_trade_fingerprints (
            id              SERIAL PRIMARY KEY,
            trade_id        VARCHAR(32) UNIQUE NOT NULL,
            symbol          VARCHAR(10) NOT NULL,
            sector          VARCHAR(20),
            entry_ts        BIGINT,
            exit_ts         BIGINT,
            entry_price     REAL,
            symbol_rsi      REAL,
            macd_bullish    BOOLEAN,
            above_ma20      BOOLEAN,
            spy_rsi         REAL,
            spy_momentum    REAL,
            spy_bullish     BOOLEAN,
            qqq_rsi         REAL,
            sector_health   VARCHAR(10),
            hour_cdt        INTEGER,
            day_of_week     INTEGER,
            pdt_slots_used  INTEGER,
            is_paper        BOOLEAN DEFAULT FALSE,
            won             BOOLEAN,
            pnl_pct         REAL,
            exit_reason     VARCHAR(50),
            hold_time_min   INTEGER,
            mfe             REAL,
            mae             REAL,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_btf_symbol   ON berserker_trade_fingerprints(symbol);
        CREATE INDEX IF NOT EXISTS idx_btf_won      ON berserker_trade_fingerprints(won);
        CREATE INDEX IF NOT EXISTS idx_btf_is_paper ON berserker_trade_fingerprints(is_paper);
        -- V10.32: Exit Autopsy (shared with Scanner). Every closed equity
        -- trade gets post-exit forward prices so each exit rule can be
        -- graded on capture ratio (realized vs the trade's own MFE peak)
        -- and post-exit continuation. All *_pct columns are PERCENT.
        -- Resolution: latest-trade sampled AT each horizon (15m/1h/4h) by
        -- the resolver thread; a horizon missed while the service was down
        -- simply stays NULL. Rows prune at 30 days.
        CREATE TABLE IF NOT EXISTS equity_exit_autopsy (
            id           SERIAL PRIMARY KEY,
            ts           BIGINT NOT NULL,
            service      VARCHAR(12) NOT NULL,
            symbol       VARCHAR(12) NOT NULL,
            mode         VARCHAR(10),
            exit_reason  VARCHAR(50),
            exit_price   REAL,
            realized_pct REAL,
            mfe_pct      REAL,
            mae_pct      REAL,
            hold_min     INTEGER,
            post_15m REAL, post_1h REAL, post_4h REAL
        );
        CREATE INDEX IF NOT EXISTS idx_eea_ts ON equity_exit_autopsy(ts);
        -- V10.17: Add is_paper to existing DBs without wiping data
        ALTER TABLE berserker_trade_fingerprints
            ADD COLUMN IF NOT EXISTS is_paper BOOLEAN DEFAULT FALSE;
        CREATE TABLE IF NOT EXISTS berserker_pattern_stats (
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
                    log("[BM] Berserker pattern memory tables ready")
        except Exception as e:
            log(f"[BM] init_tables error: {e}")

    def record_entry(self, trade_id: str, symbol: str, entry_price: float,
                     symbol_rsi: float, macd_bull: bool, above_ma20: bool,
                     spy_ctx: dict, sector_health: str,
                     is_paper: bool = False):
        if not self._enabled:
            return
        threading.Thread(target=self._write_entry, daemon=True, args=(
            trade_id, symbol, entry_price, symbol_rsi, macd_bull, above_ma20,
            spy_ctx, sector_health, is_paper
        )).start()

    def _write_entry(self, trade_id, symbol, entry_price, symbol_rsi,
                     macd_bull, above_ma20, spy_ctx, sector_health,
                     is_paper=False):
        sector = "TRUMP" if symbol in TRUMP_THEME else "TECH"
        now    = datetime.now(tz=CENTRAL)
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO berserker_trade_fingerprints
                        (trade_id, symbol, sector, entry_ts, entry_price,
                         symbol_rsi, macd_bullish, above_ma20,
                         spy_rsi, spy_momentum, spy_bullish, qqq_rsi,
                         sector_health, hour_cdt, day_of_week, pdt_slots_used,
                         is_paper)
                        VALUES (%s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s)
                        ON CONFLICT (trade_id) DO NOTHING
                    """, (
                        trade_id, symbol, sector, int(time.time()), float(entry_price),
                        float(symbol_rsi) if symbol_rsi is not None else None,
                        bool(macd_bull),
                        bool(above_ma20),
                        float(spy_ctx.get("rsi")) if spy_ctx.get("rsi") is not None else None,
                        float(spy_ctx.get("momentum")) if spy_ctx.get("momentum") is not None else None,
                        bool(spy_ctx.get("bullish")) if spy_ctx.get("bullish") is not None else None,
                        float(spy_ctx.get("qqq_rsi")) if spy_ctx.get("qqq_rsi") is not None else None,
                        sector_health, now.hour, now.weekday(),
                        0,  # V10.23: pdt_slots_used retired -- column kept for historical
                            # fingerprint continuity, always 0 for new rows post-PDT removal
                        bool(is_paper),
                    ))
                conn.commit()
        except Exception as e:
            log(f"[BM] write_entry error: {e}")

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
                        UPDATE berserker_trade_fingerprints
                        SET won=%s, pnl_pct=%s, exit_reason=%s,
                            hold_time_min=%s, exit_ts=%s,
                            mfe=%s, mae=%s
                        WHERE trade_id=%s
                    """, (won, round(pnl_pct * 100, 3), exit_reason,
                          hold_min, int(time.time()),
                          round(mfe * 100, 3), round(mae * 100, 3),
                          trade_id))
                    # V10.32: Exit Autopsy row -- symbol + entry price come
                    # from the fingerprint itself so every record_exit caller
                    # is covered without touching the exit paths. exit_price
                    # is derived (entry * (1+pnl)); bt_ rows excluded.
                    cur.execute("""
                        INSERT INTO equity_exit_autopsy
                        (ts, service, symbol, mode, exit_reason, exit_price,
                         realized_pct, mfe_pct, mae_pct, hold_min)
                        SELECT %s, 'BERSERKER', symbol,
                               CASE WHEN COALESCE(is_paper, FALSE)
                                    THEN 'PAPER' ELSE 'LIVE' END,
                               %s, entry_price * (1 + %s),
                               %s, %s, %s, %s
                        FROM berserker_trade_fingerprints
                        WHERE trade_id=%s AND trade_id NOT LIKE 'bt_%%'
                    """, (int(time.time()), exit_reason, pnl_pct,
                          round(pnl_pct * 100, 3), round(mfe * 100, 3),
                          round(mae * 100, 3), hold_min, trade_id))
                conn.commit()
        except Exception as e:
            log(f"[BM] write_exit error: {e}")

    def run_analysis(self):
        if not self._enabled:
            return
        # V10.35 data-hygiene: this query was completely unfiltered -- the
        # win-rate buckets that BLOCK live entries (and the V10.19 dynamic-TP
        # MFE stats) were computed from the aggressive paper twin's
        # deliberately ungated entries plus pre-go-live masquerade rows.
        # Exact same failure mode that ratcheted crypto's thresholds 75->84
        # before V5.5. Rule (mirrors the V10.34 strategist fix): a row feeds
        # the buckets if it's backtest bulk (bt_) OR a genuinely-live trade
        # entered after launch. Paper-twin rows never feed the live gate.
        query = """
            SELECT symbol, sector, symbol_rsi, macd_bullish, spy_bullish,
                   sector_health, hour_cdt, day_of_week,
                   won, pnl_pct, mfe, mae
            FROM berserker_trade_fingerprints
            WHERE won IS NOT NULL
              AND (is_paper IS NULL OR is_paper = FALSE)
              AND (trade_id LIKE 'bt_%%' OR entry_ts >= %s)
        """
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with _pg_extras.RealDictCursor(conn) as cur:
                    cur.execute(query, (int(GATE_LAUNCH_DATE.timestamp()),))
                    rows = cur.fetchall()

            if len(rows) < PM_MIN_TRADES:
                log(f"[BM] {len(rows)} trades < {PM_MIN_TRADES} min, skipping")
                return

            from collections import defaultdict
            buckets   = defaultdict(list)
            pnl_bkts  = defaultdict(list)

            for row in rows:
                key = self._bucket_key(
                    row["symbol"],
                    row["symbol_rsi"] if row["symbol_rsi"] is not None else 50,
                    row["spy_bullish"],
                    row["sector_health"] or "STRONG",
                    row["hour_cdt"] if row["hour_cdt"] is not None else 12,
                )
                buckets[key].append(bool(row["won"]))
                if row["pnl_pct"] is not None:
                    pnl_bkts[key].append(float(row["pnl_pct"]))

            # V10.19: Also compute winner MFE distribution per bucket for dynamic TP
            mfe_bkts_winners = defaultdict(list)  # key -> [mfe_pct] for winners only
            for row in rows:
                if not bool(row["won"]):
                    continue
                mfe_val = row.get("mfe")
                if mfe_val is None:
                    continue
                key = self._bucket_key(
                    row["symbol"],
                    row["symbol_rsi"] if row["symbol_rsi"] is not None else 50,
                    row["spy_bullish"],
                    row["sector_health"] or "STRONG",
                    row["hour_cdt"] if row["hour_cdt"] is not None else 12,
                )
                mfe_bkts_winners[key].append(float(mfe_val))   # already stored as pct pts (e.g. 1.5 = 1.5%)

            new_cache = {}
            new_mfe_stats = {}
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
                            INSERT INTO berserker_pattern_stats
                            (bucket_key, win_rate, sample_count, avg_pnl)
                            VALUES (%s,%s,%s,%s)
                            ON CONFLICT (bucket_key) DO UPDATE
                            SET win_rate=EXCLUDED.win_rate,
                                sample_count=EXCLUDED.sample_count,
                                avg_pnl=EXCLUDED.avg_pnl,
                                last_updated=NOW()
                        """, (key, wr, len(outcomes), avg_pnl))
                        new_cache[key] = wr

                        # V10.19: MFE distribution for dynamic TP
                        w_mfes = mfe_bkts_winners.get(key, [])
                        if w_mfes:
                            avg_wmfe  = sum(w_mfes) / len(w_mfes)
                            pct_2     = sum(1 for m in w_mfes if m >= 2.0) / len(w_mfes)
                            pct_3     = sum(1 for m in w_mfes if m >= 3.0) / len(w_mfes)
                            new_mfe_stats[key] = {
                                "avg_mfe":   round(avg_wmfe, 3),
                                "pct_reach_2": round(pct_2, 3),
                                "pct_reach_3": round(pct_3, 3),
                                "n_winners": len(w_mfes),
                            }
                conn.commit()

            self._win_rates     = new_cache
            self._last_analysis = time.time()
            global _bucket_mfe_stats
            _bucket_mfe_stats = new_mfe_stats  # broadcast for execute_trade dynamic TP
            total = len(rows)
            wr    = sum(1 for r in rows if r["won"]) / total if total > 0 else 0
            dyn_eligible = sum(1 for s in new_mfe_stats.values() if s["n_winners"] >= 30)
            log(f"[BM] Analysis: {len(new_cache)} buckets | {total} trades | {wr:.1%} WR | "
                f"{dyn_eligible} buckets eligible for dynamic TP")
        except Exception as e:
            log(f"[BM] analysis error: {e}")

    @staticmethod
    def _bucket_key(symbol: str, rsi: float, spy_bullish: bool,
                    sector_health: str, hour: int) -> str:
        """Shared bucket-key builder -- used by both get_win_rate() (lookup)
        and run_analysis() (population) so the two never drift apart.
        V10.23: dropped the pdt_used dimension (pdt_ok/pdt_tight) -- PDT was
        eliminated by FINRA Jun 4 2026, so that split was artificially halving
        every bucket's sample size for a constraint that no longer exists.
        Existing rows still carry the historical pdt_slots_used DB column,
        but it's no longer read into the key, so old and new fingerprints
        for the same symbol/RSI/SPY/sector/hour combination now consolidate
        into one bucket instead of two.
        V10.23 also fixed RSI thresholds 70/60 -> 72/62 to match
        nexus_analyzer_1min_railway.py's run_pattern_analysis() exactly --
        the two had silently drifted (live used 70/60, backtester used
        72/62), so backtest-seeded buckets and live-seeded buckets for the
        same conditions were landing in different keys and never merging.
        62 is also RSI_BUY_TRIGGER, the actual live entry floor, so
        bucketing exactly at that line is the meaningful split anyway."""
        sector = "TRUMP" if symbol in TRUMP_THEME else "TECH"
        rsi_b  = "rsi_hi" if rsi > 72 else "rsi_mid" if rsi > 62 else "rsi_low"
        spy_b  = "spy_bull" if spy_bullish else "spy_bear"
        sec_b  = sector_health or "STRONG"
        hr_b   = "hr_open" if hour < 10 else "hr_mid" if hour < 13 else "hr_late"
        return f"{symbol}|{rsi_b}|{spy_b}|{sec_b}|{sector}|{hr_b}"

    def get_dynamic_tp(self, symbol: str, rsi: float, spy_bullish: bool,
                        sector_health: str, hour: int,
                        recipe_tp: float) -> float:
        """
        V10.19: Return dynamic TP for this bucket if evidence supports raising it.
        Rules:
          - Bucket needs >= 30 winner samples
          - avg_winner_mfe > 2.0% AND pct_reach_2 > 25% → TP = 2.0%
          - avg_winner_mfe > 3.0% AND pct_reach_3 > 15% → TP = 2.5%
          - Dynamic TP NEVER lower than recipe_tp
        Returns recipe_tp if no evidence or conditions not met.
        """
        key   = self._bucket_key(symbol, rsi, spy_bullish, sector_health, hour)
        stats = _bucket_mfe_stats.get(key)
        if not stats or stats["n_winners"] < 30:
            return recipe_tp
        avg_mfe = stats["avg_mfe"]
        pct_2   = stats["pct_reach_2"]
        pct_3   = stats["pct_reach_3"]
        if avg_mfe >= 3.0 and pct_3 >= 0.15:
            dyn_tp = 0.025
        elif avg_mfe >= 2.0 and pct_2 >= 0.25:
            dyn_tp = 0.020
        else:
            return recipe_tp
        return max(dyn_tp, recipe_tp)   # never lower than recipe

    def get_win_rate(self, symbol: str, rsi: float, spy_bullish: bool,
                     sector_health: str, hour: int) -> float:
        if not self._win_rates:
            return 0.5
        key = self._bucket_key(symbol, rsi, spy_bullish, sector_health, hour)
        return self._win_rates.get(key, 0.5)

    def should_skip_entry(self, symbol: str, rsi: float, spy_bullish: bool,
                           sector_health: str, hour: int) -> tuple:
        """
        V10.5: Win-rate gate. Returns (skip, win_rate, has_data).

        has_data is True only if this exact bucket has >= PM_MIN_BUCKET_TRADES
        historical samples -- run_analysis() only writes buckets that meet
        that threshold, so presence in _win_rates IS the sample-size check.
        "No data" never blocks a trade (skip=False, win_rate=0.5).
        """
        key = self._bucket_key(symbol, rsi, spy_bullish, sector_health, hour)
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
        threading.Thread(target=_run, daemon=True, name="berserker-memory").start()


# ==============================================================================
# V10.29: BERSERKER WIN FOLLOWER -- follow-the-wins allocation
# Rolling per-symbol performance drives priority, sizing, and gate strictness.
# Losers get benched (paper keeps trading them and earns their way back in);
# winners get scanned first, sized up, and an easier win-rate gate.
# ==============================================================================
WF_LOOKBACK_DAYS   = 14      # rolling performance window
WF_REFRESH_SECS    = 3600    # tier refresh cadence
WF_HOT_WR          = 0.55    # live WR to qualify HOT (breakeven is 40% at 1.5/1.0)
WF_HOT_MIN_TRADES  = 5
WF_COLD_WR         = 0.35    # at/below this = COLD (below breakeven)
WF_COLD_MIN_TRADES = 6
WF_HOT_SIZE_MULT   = 1.30
WF_HOT_GATE_DISC   = 0.07    # HOT: win-rate gate threshold discount
WF_WARM_GATE_DISC  = 0.04    # WARM: smaller discount
WF_GATE_FLOOR      = 0.40    # NEVER gate below mathematical breakeven
WF_WARM_MIN_PAPER  = 10
WF_WARM_PAPER_WR   = 0.55
WF_RECOVERY_TRADES = 10      # paper trades since bench needed to return
WF_RECOVERY_WR     = 0.50


class BerserkerWinFollower:
    """
    V10.29: Follow-the-wins allocator for Berserker. Reads completed trades
    from berserker_trade_fingerprints (is_paper distinguishes live vs paper),
    tiers every symbol, and feeds size multipliers / gate discounts / scan
    priority back into the live engine. Benched symbols are blocked from
    LIVE entries only -- the paper Berserker loop keeps fingerprinting them,
    which is what powers automatic recovery.
    """

    def __init__(self, db_url: str):
        self.db_url        = db_url
        self._conn         = None
        self._lock         = threading.Lock()
        self._enabled      = bool(db_url) and _psycopg2_ok
        self._tiers        = {}
        self._bench        = {}
        self._last_refresh = 0.0

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
            log(f"[WF] DB connect error: {e}")
            return None

    # ── Bench persistence (nexus_config -- shared key/value, created here) ──
    def _init_config_table(self):
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS nexus_config (
                            key   VARCHAR(100) PRIMARY KEY,
                            value TEXT NOT NULL,
                            updated_at TIMESTAMPTZ DEFAULT NOW()
                        )
                    """)
                conn.commit()
        except Exception as e:
            log(f"[WF] init_config_table: {e}")

    def _load_bench(self):
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("SELECT value FROM nexus_config WHERE key='wf_bench_berserker'")
                    row = cur.fetchone()
                conn.commit()
            if row and row[0]:
                self._bench = json.loads(row[0])
                if self._bench:
                    log(f"[WF] restored bench from DB: {list(self._bench.keys())}")
        except Exception as e:
            log(f"[WF] _load_bench: {e}")

    def _save_bench(self):
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO nexus_config (key, value, updated_at)
                        VALUES ('wf_bench_berserker', %s, NOW())
                        ON CONFLICT (key) DO UPDATE
                        SET value=EXCLUDED.value, updated_at=NOW()
                    """, (json.dumps(self._bench),))
                conn.commit()
        except Exception as e:
            log(f"[WF] _save_bench: {e}")

    # ── Core refresh ─────────────────────────────────────────────────────────
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
                        SELECT symbol, COALESCE(is_paper, FALSE), won, pnl_pct, entry_ts
                        FROM berserker_trade_fingerprints
                        WHERE won IS NOT NULL AND exit_ts >= %s
                          AND trade_id NOT LIKE 'bt_%%'
                    """, (cutoff,))

                    rows = cur.fetchall()
                conn.commit()
        except Exception as e:
            log(f"[WF] refresh query: {e}")
            return

        # V10.31: pre-go-live paper rows masqueraded as live. The is_paper
        # column was added in V10.17 with DEFAULT FALSE, which backfilled
        # every OLDER paper fingerprint as is_paper=FALSE -- so MSTR showed
        # "25 live trades" when live trading was 3 days old with max 3
        # positions. A live trade cannot predate live trading: anything
        # entered before GATE_LAUNCH_DATE (Jun 30 go-live) is dropped from
        # the live record. Paper bucket (is_paper=TRUE) is inherently
        # V10.17+ and needs no floor.
        _live_floor = int(GATE_LAUNCH_DATE.timestamp())
        live, paper = {}, {}
        for symbol, is_paper, won, pnl, entry_ts in rows:
            if is_paper:
                paper.setdefault(symbol, []).append((bool(won), float(pnl or 0)))
            elif entry_ts and int(entry_ts) >= _live_floor:
                live.setdefault(symbol, []).append((bool(won), float(pnl or 0)))
            # else: pre-go-live row with backfilled is_paper=FALSE -- ignored

        old_tiers = {s: t.get("tier") for s, t in self._tiers.items()}
        new_tiers = {}

        for symbol in SYMBOLS:
            lt, pt = live.get(symbol, []), paper.get(symbol, [])
            l_n    = len(lt)
            l_wr   = (sum(1 for w, _ in lt if w) / l_n) if l_n else 0.0
            l_pnl  = sum(p for _, p in lt)
            p_n    = len(pt)
            p_wr   = (sum(1 for w, _ in pt if w) / p_n) if p_n else 0.0

            # V10.30: bench self-validation. All four backtesters write
            # fingerprints with trade_id 'bt_...' and exit_ts = WRITE time,
            # so every Sunday backtest looked like thousands of fresh live
            # trades -- NUE (34%/113t) and SPCX (28%/72t) were benched on
            # backtest rows, not live ones. Now that bt_ rows are excluded,
            # any bench the clean live record doesn't support is released
            # here. This also gives benches a natural expiry: once the bad
            # trades age out of the 14d window, the symbol comes back.
            _clean_cold = (l_n >= WF_COLD_MIN_TRADES and l_wr <= WF_COLD_WR)
            if symbol in self._bench and not _clean_cold:
                del self._bench[symbol]
                self._save_bench()
                alert(f"🔥 WIN FOLLOWER [BERSERKER] -- {symbol} UN-BENCHED\n"
                      f"Bench no longer supported by clean live data "
                      f"({l_n} live trades, WR {l_wr:.0%} in {WF_LOOKBACK_DAYS}d)\n"
                      f"Back in rotation at NEUTRAL")
                log(f"[WF] bench released (evidence invalid/expired): {symbol}")

            if symbol in self._bench:
                tier = "COLD"
            elif l_n >= WF_HOT_MIN_TRADES and l_wr >= WF_HOT_WR and l_pnl > 0:
                tier = "HOT"
            elif l_n >= WF_COLD_MIN_TRADES and l_wr <= WF_COLD_WR:
                tier = "COLD"
            elif l_n < WF_HOT_MIN_TRADES and p_n >= WF_WARM_MIN_PAPER and p_wr >= WF_WARM_PAPER_WR:
                tier = "WARM"
            else:
                tier = "NEUTRAL"

            new_tiers[symbol] = {
                "tier": tier, "live_trades": l_n, "live_wr": round(l_wr, 3),
                "live_pnl_sum": round(l_pnl, 2),   # V10.31: DB stores percent already
                "paper_trades": p_n, "paper_wr": round(p_wr, 3),
            }

            if tier == "COLD" and symbol not in self._bench:
                self._bench[symbol] = {
                    "since":  int(time.time()),
                    "reason": f"WR {l_wr:.0%} over {l_n} live trades ({WF_LOOKBACK_DAYS}d)",
                }
                self._save_bench()
                alert(f"🧊 WIN FOLLOWER [BERSERKER] -- {symbol} BENCHED\n"
                      f"Live WR {l_wr:.0%} over {l_n} trades ({WF_LOOKBACK_DAYS}d) -- below "
                      f"{WF_GATE_FLOOR:.0%} breakeven zone\n"
                      f"Paper keeps trading it; auto-returns at "
                      f"{WF_RECOVERY_WR:.0%}+ WR over {WF_RECOVERY_TRADES} paper trades")
                log(f"[WF] BENCHED {symbol} (WR {l_wr:.0%}/{l_n})")

            old = old_tiers.get(symbol)
            if old is not None and old != tier and tier == "HOT":
                alert(f"📈 WIN FOLLOWER [BERSERKER] -- {symbol} promoted to HOT\n"
                      f"WR {l_wr:.0%} over {l_n} live trades, PnL {l_pnl:+.1f}% ({WF_LOOKBACK_DAYS}d)\n"
                      f"Size x{WF_HOT_SIZE_MULT} | gate -{WF_HOT_GATE_DISC:.0%} | scans first")
                log(f"[WF] {symbol} -> HOT (WR {l_wr:.0%}/{l_n})")

        self._tiers        = new_tiers
        self._check_recoveries()
        self._last_refresh = time.time()
        hot  = [s for s, t in new_tiers.items() if t["tier"] == "HOT"]
        warm = [s for s, t in new_tiers.items() if t["tier"] == "WARM"]
        log(f"[WF] refresh: HOT={hot or '-'} WARM={warm or '-'} BENCHED={list(self._bench) or '-'}")

    def _check_recoveries(self):
        for symbol in list(self._bench.keys()):
            since = self._bench[symbol].get("since", 0)
            try:
                with self._lock:
                    conn = self._get_conn()
                    if not conn:
                        return
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT won FROM berserker_trade_fingerprints
                            WHERE symbol=%s AND is_paper=TRUE
                              AND trade_id NOT LIKE 'bt_%%'
                              AND won IS NOT NULL AND exit_ts >= %s
                            ORDER BY exit_ts DESC LIMIT %s
                        """, (symbol, since, WF_RECOVERY_TRADES))
                        rows = cur.fetchall()
                    conn.commit()
                if len(rows) >= WF_RECOVERY_TRADES:
                    wr = sum(1 for (w,) in rows if w) / len(rows)
                    if wr >= WF_RECOVERY_WR:
                        del self._bench[symbol]
                        self._save_bench()
                        if symbol in self._tiers:
                            self._tiers[symbol]["tier"] = "NEUTRAL"
                        alert(f"🔥 WIN FOLLOWER [BERSERKER] -- {symbol} UN-BENCHED\n"
                              f"Paper WR {wr:.0%} over last {len(rows)} trades since bench\n"
                              f"Back in live rotation at NEUTRAL")
                        log(f"[WF] UN-BENCHED {symbol} (paper WR {wr:.0%})")
            except Exception as e:
                log(f"[WF] recovery check {symbol}: {e}")

    # ── Accessors ────────────────────────────────────────────────────────────
    def get_tier(self, symbol: str) -> str:
        return self._tiers.get(symbol, {}).get("tier", "NEUTRAL")

    def is_benched(self, symbol: str) -> bool:
        return symbol in self._bench

    def get_size_mult(self, symbol: str) -> float:
        return WF_HOT_SIZE_MULT if self.get_tier(symbol) == "HOT" else 1.0

    def get_gate_discount(self, symbol: str) -> float:
        t = self.get_tier(symbol)
        if t == "HOT":
            return WF_HOT_GATE_DISC
        if t == "WARM":
            return WF_WARM_GATE_DISC
        return 0.0

    def get_status(self) -> dict:
        return {"tiers": self._tiers, "benched": self._bench,
                "last_refresh_min_ago": (round((time.time() - self._last_refresh) / 60, 1)
                                         if self._last_refresh else None)}

    def start_scheduler(self):
        if not self._enabled:
            log("[WF] disabled (no DATABASE_URL) -- all symbols NEUTRAL")
            return
        self._init_config_table()
        self._load_bench()
        def _run():
            time.sleep(120)   # after BerserkerMemory boot analysis
            while True:
                try:
                    self.refresh()
                except Exception as e:
                    log(f"[WF] loop: {e}")
                time.sleep(WF_REFRESH_SECS)
        threading.Thread(target=_run, daemon=True, name="berserker-wf").start()
        log(f"[WF] scheduler started (refresh {WF_REFRESH_SECS//60}m, lookback {WF_LOOKBACK_DAYS}d)")


def _equity_autopsy_resolver():
    """V10.32: every 5 min, fill post_15m/1h/4h on equity_exit_autopsy rows
    whose horizon is due RIGHT NOW, using a latest-trade sample. No
    historical-bars plumbing needed; after-hours the last trade is simply
    flat, which is honest. Missed horizons (service down) stay NULL."""
    time.sleep(180)
    while True:
        try:
            if _berserker_memory and _berserker_memory._enabled:
                now = int(time.time())
                with _berserker_memory._lock:
                    conn = _berserker_memory._get_conn()
                    if conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT id, symbol, ts, exit_price,
                                       post_15m, post_1h, post_4h
                                FROM equity_exit_autopsy
                                WHERE post_4h IS NULL AND ts >= %s
                            """, (now - 15300,))
                            rows = cur.fetchall()
                        conn.commit()
                    else:
                        rows = []
                due = []
                for _id, sym, ts, xp, p15, p1, p4 in rows:
                    for col, have, h in (("post_15m", p15, 900),
                                         ("post_1h", p1, 3600),
                                         ("post_4h", p4, 14400)):
                        if have is None and ts + h <= now < ts + h + 600:
                            due.append((_id, sym, xp, col))
                if due:
                    prices = {}
                    for sym in {d[1] for d in due}:
                        try:
                            t = stock_data_client.get_stock_latest_trade(
                                StockLatestTradeRequest(symbol_or_symbols=sym,
                                                        feed=DataFeed.IEX))
                            prices[sym] = float(t[sym].price)
                        except Exception:
                            prices[sym] = None
                    with _berserker_memory._lock:
                        conn = _berserker_memory._get_conn()
                        if conn:
                            with conn.cursor() as cur:
                                for _id, sym, xp, col in due:
                                    p = prices.get(sym)
                                    if p and xp:
                                        cur.execute(
                                            f"UPDATE equity_exit_autopsy SET {col}=%s WHERE id=%s",
                                            (round((p - xp) / xp * 100, 4), _id))
                                cur.execute("DELETE FROM equity_exit_autopsy WHERE ts < %s",
                                            (now - 30 * 86400,))
                            conn.commit()
        except Exception as e:
            log(f"[AUTOPSY] resolver: {e}")
        time.sleep(300)


def equity_autopsy_summary(days: int = 7) -> dict:
    """V10.32: per service+exit_reason grade from equity_exit_autopsy."""
    if not (_berserker_memory and _berserker_memory._enabled):
        return {}
    cutoff = int(time.time()) - days * 86400
    try:
        with _berserker_memory._lock:
            conn = _berserker_memory._get_conn()
            if not conn:
                return {}
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT service, exit_reason, mode, realized_pct, mfe_pct, post_1h
                    FROM equity_exit_autopsy WHERE ts >= %s
                """, (cutoff,))
                rows = cur.fetchall()
            conn.commit()
    except Exception as e:
        log(f"[AUTOPSY] summary: {e}")
        return {}
    groups = {}
    for svc, reason, mode, realized, mfe, p1 in rows:
        key = f"{svc}/{reason or 'UNKNOWN'}" + ("" if mode == "LIVE" else " (paper)")
        g = groups.setdefault(key, {"n": 0, "sr": 0.0, "cap_r": 0.0, "cap_m": 0.0,
                                    "s1": 0.0, "n1": 0, "ran": 0})
        g["n"] += 1
        g["sr"] += (realized or 0.0)
        if mfe and mfe > 0:
            g["cap_r"] += max(realized or 0.0, 0.0)
            g["cap_m"] += mfe
        if p1 is not None:
            g["s1"] += p1; g["n1"] += 1
            if p1 > 0.3:
                g["ran"] += 1
    out = {}
    for key, g in groups.items():
        out[key] = {
            "n": g["n"],
            "avg_realized": round(g["sr"] / g["n"], 3),
            "capture_pct": round(g["cap_r"] / g["cap_m"] * 100, 1) if g["cap_m"] > 0 else None,
            "post_1h_avg": round(g["s1"] / g["n1"], 3) if g["n1"] else None,
            "ran_on_pct":  round(g["ran"] / g["n1"] * 100, 1) if g["n1"] else None,
        }
    return out


_win_follower = None      # BerserkerWinFollower -- initialized at boot (V10.29)
_berserker_memory = None  # BerserkerMemory -- initialized at boot
_capital_coordinator = None  # CapitalCoordinator -- initialized at boot, see capital_coordinator.py


def _get_spy_context_for_fingerprint() -> dict:
    """V10.1: Get current SPY+QQQ context for fingerprinting."""
    with _spy_history_lock:
        spy = list(_spy_history)
        qqq = list(_qqq_history)

    def _rsi(prices):
        if len(prices) < 8:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(prices)):
            d = prices[i] - prices[i-1]
            gains.append(max(d, 0))
            losses.append(max(-d, 0))
        period = min(7, len(gains))
        ag = sum(gains[:period]) / period
        al = sum(losses[:period]) / period
        for i in range(period, len(gains)):
            ag = (ag * (period-1) + gains[i]) / period
            al = (al * (period-1) + losses[i]) / period
        if al == 0:
            return 100.0
        return round(100 - (100 / (1 + ag / al)), 2)

    spy_rsi  = _rsi(spy[-20:]) if len(spy) >= 8 else 50.0
    qqq_rsi  = _rsi(qqq[-20:]) if len(qqq) >= 8 else 50.0
    mom      = (spy[-1] - spy[-6]) / spy[-6] * 100 if len(spy) >= 6 and spy[-6] > 0 else 0
    bullish  = len(spy) >= 8 and spy[-1] > sum(spy[-20:]) / len(spy[-20:])
    return {
        "rsi":      spy_rsi,
        "qqq_rsi":  qqq_rsi,
        "momentum": round(mom, 3),
        "bullish":  bullish,
    }


def get_spy_regime_and_momentum() -> tuple:
    """
    V10.2: Check SPY market regime and short-term momentum.
    Returns (regime, momentum_ok, spy_momentum_pct, spy_vs_ma)
    - regime: "BULL" if SPY above 20-day MA, "BEAR" if below
    - momentum_ok: True if SPY not falling hard in last 30 min
    - spy_momentum_pct: % SPY has moved in last 30 min (approx 60 bars at 30s)
    - spy_vs_ma: % SPY is above/below its MA20
    """
    with _spy_history_lock:
        spy = list(_spy_history)

    if len(spy) < 20:
        return "BULL", True, 0.0, 0.0

    # Short-term momentum -- last 30 min (approx 60 price readings at 30s each)
    lookback = min(60, len(spy) - 1)
    momentum = (spy[-1] - spy[-lookback]) / spy[-lookback] * 100 if spy[-lookback] > 0 else 0

    # Regime -- SPY vs 20-period MA on our price history
    ma20    = sum(spy[-20:]) / 20
    vs_ma   = (spy[-1] - ma20) / ma20 * 100 if ma20 > 0 else 0
    regime  = "BULL" if spy[-1] > ma20 else "BEAR"

    # Momentum gate -- is SPY falling hard right now?
    mom_ok = momentum >= SPY_MOMENTUM_GATE

    return regime, mom_ok, round(momentum, 3), round(vs_ma, 3)


def _fetch_real_vix() -> float | None:
    """V10.26: Fetch real ^VIX directly from yfinance, cached on a TTL since
    yfinance shouldn't be hit every ~30s sweep. Returns None if unavailable
    (caller falls back to the VIXY proxy).

    Why this replaced the VIXY*1.5 proxy (Jun 30 2026):
    VIXY is NOT benchmarked to spot VIX -- it tracks an index of rolling
    monthly VIX FUTURES contracts, and ProShares' own documentation states
    VIXY "can be expected to perform very differently from the VIX... on a
    daily basis and over time," partly due to futures-roll/contango costs
    that erode VIXY's price level independent of where spot VIX actually is.
    The "VIXY $10-25, VIX $12-40, ratio ~1.5x" relationship documented in
    V10.19 was a snapshot of conditions at the time, not a stable physical
    constant -- it drifted. Confirmed Jun 30 2026 (first live trading day):
    real VIX ~17.5 (calm, post-close Monday 17.55) while VIXY was trading
    ~$21.85-23.70, so the old formula computed vix_approx = 23.7*1.5 ~ 32.5
    -- not just inaccurate, but landing past BOTH VIX_BLOCK_THRESHOLD (25)
    and VIX_EXTREME_THRESHOLD (30), hard-blocking entries and capping to 1
    position on a day with completely normal volatility. This wasn't a
    coding bug -- the *= 1.5 logic and threshold checks were correct -- the
    proxy itself stopped reflecting reality. Fetching ^VIX directly removes
    the proxy assumption entirely instead of re-tuning a multiplier that
    will just drift again.
    """
    global _real_vix_cache
    now = time.time()
    if _real_vix_cache["value"] is not None and (now - _real_vix_cache["ts"]) < _REAL_VIX_TTL_SECS:
        return _real_vix_cache["value"]
    if not _yfinance_ok:
        return None
    try:
        vix_ticker = yf.Ticker("^VIX")
        price      = vix_ticker.fast_info.last_price
        if price and price > 0:
            _real_vix_cache = {"value": float(price), "ts": now}
            return float(price)
    except Exception as e:
        log(f"[VIX] yfinance ^VIX fetch error (will retry next TTL window, "
            f"falling back to VIXY proxy meanwhile): {e}")
    return None


def _update_spy_qqq_history():
    """V10.1: Update SPY/QQQ price history for fingerprinting context.
    V10.16: Batched into a single API call instead of two separate calls.
    V10.19: Also fetches VIXY (VIX ETF proxy) for VIX regime gate.
    V10.26: Real ^VIX (yfinance) is now the primary source -- see
    _fetch_real_vix() docstring for why the VIXY*1.5 proxy was retired as
    primary. VIXY proxy is kept ONLY as a fallback for when yfinance is
    unavailable/rate-limited, so the VIX gate never goes fully blind."""
    global _spy_session_open, _vix_level_raw, _vix_level_smooth, _vix_source
    try:
        batch = stock_data_client.get_stock_latest_trade(
            StockLatestTradeRequest(symbol_or_symbols=["SPY", "QQQ", "VIXY"], feed=DataFeed.IEX)
        )
        for sym, hist in [("SPY", _spy_history), ("QQQ", _qqq_history)]:
            price = float(batch[sym].price)
            if price > 0:
                with _spy_history_lock:
                    hist.append(price)

        real_vix = _fetch_real_vix()
        if real_vix is not None:
            vix_value   = real_vix
            _vix_source = "yfinance"
        elif "VIXY" in batch:
            # Fallback only -- see _fetch_real_vix() docstring. This proxy is
            # known to drift from spot VIX over time (VIXY tracks rolling
            # futures, not the index), so it's used only when the real
            # source is unavailable, not as the steady-state signal.
            vixy_price  = float(batch["VIXY"].price)
            vix_value   = vixy_price * 1.5 if vixy_price > 0 else None
            _vix_source = "vixy_proxy"
        else:
            vix_value   = None
            _vix_source = "none"

        if vix_value is not None:
            with _vix_history_lock:
                _vix_history.append(vix_value)
                _vix_level_raw = vix_value
                # 5-bar smoothed
                recent = list(_vix_history)[-5:]
                _vix_level_smooth = sum(recent) / len(recent) if recent else vix_value
            shared_state.set_vix_level(_vix_level_smooth)

        # Track session open
        now = datetime.now(tz=CENTRAL)
        if now.hour == 8 and now.minute < 5 and _spy_session_open == 0:
            with _spy_history_lock:
                if _spy_history:
                    _spy_session_open = _spy_history[-1]
    except Exception as e:
        log(f"[BM] SPY/QQQ/VIXY update error: {e}")


# ==============================================================================
# V10.19: EARNINGS CALENDAR BLACKOUT
# ==============================================================================
def check_earnings_calendar():
    """
    Check each symbol for upcoming earnings. Called once per hour max.
    Symbols with earnings within 48h: block new entries (earnings_blocked set).
    Symbols with earnings within 24h + open position: force-close.
    Uses yfinance .calendar as data source. 4-hour cache to avoid rate limits.
    """
    global _earnings_last_check
    if not _yfinance_ok:
        return
    now_ts = time.time()
    if now_ts - _earnings_last_check < EARNINGS_REFRESH_SECS:
        return
    _earnings_last_check = now_ts
    log("[EARN] Running earnings calendar check...")

    now_dt       = datetime.now(tz=CENTRAL)
    newly_blocked  = []
    newly_released = []
    forced_closes  = []

    positions = get_alpaca_positions()

    for symbol in SYMBOLS:
        try:
            ticker = yf.Ticker(symbol)
            cal    = ticker.calendar

            # yfinance >=0.2.x returns a dict; older versions returned a DataFrame.
            # Normalize to dict either way.
            if cal is None:
                if symbol in earnings_blocked:
                    earnings_blocked.discard(symbol)
                    newly_released.append(symbol)
                    _earnings_cache.pop(symbol, None)
                continue

            # Dict form (modern yfinance): {"Earnings Date": [Timestamp, ...], ...}
            if isinstance(cal, dict):
                earn_dates = cal.get("Earnings Date") or cal.get("earningsDate") or []
                if not earn_dates:
                    if symbol in earnings_blocked:
                        earnings_blocked.discard(symbol)
                        newly_released.append(symbol)
                        _earnings_cache.pop(symbol, None)
                    continue
                # Use the soonest future date
                raw_date = earn_dates[0] if not isinstance(earn_dates, list) else earn_dates[0]
            elif hasattr(cal, "empty"):
                # Legacy DataFrame form
                if cal.empty:
                    if symbol in earnings_blocked:
                        earnings_blocked.discard(symbol)
                        newly_released.append(symbol)
                        _earnings_cache.pop(symbol, None)
                    continue
                raw_date = cal.columns[0]
            else:
                continue

            try:
                ts = pd.Timestamp(raw_date)
                if ts.tzinfo is None:
                    earn_dt = ts.tz_localize("UTC").astimezone(CENTRAL)
                else:
                    earn_dt = ts.astimezone(CENTRAL)
            except Exception:
                continue

            hours_away = (earn_dt - pd.Timestamp.now(tz=CENTRAL)).total_seconds() / 3600.0

            if hours_away < 0:
                # Earnings already passed
                if symbol in earnings_blocked:
                    earnings_blocked.discard(symbol)
                    newly_released.append(symbol)
                    _earnings_cache.pop(symbol, None)
                continue

            earn_str = earn_dt.strftime("%b %d %I:%M %p CDT")
            _earnings_cache[symbol] = {"date": earn_str, "hours_away": round(hours_away, 1), "checked_at": now_ts}

            if hours_away <= 48.0:
                if symbol not in earnings_blocked:
                    earnings_blocked.add(symbol)
                    newly_blocked.append(f"{symbol} ({earn_str}, {hours_away:.0f}h)")
                    log(f"[EARN] 🚫 BLOCKED {symbol} | earnings {earn_str} ({hours_away:.0f}h away)")

                # Force-close if position held and earnings within 24h
                if hours_away <= 24.0 and symbol in positions:
                    pos = positions[symbol]
                    pnl = get_position_pnl_alpaca(pos)
                    log(f"[EARN] ⚠️ Earnings within 24h! Force-closing {symbol} (P&L: {pnl*100:+.2f}%)")
                    success, pnl_actual = close_alpaca_position(symbol)
                    if success:
                        pnl_label = f"+{round(pnl_actual*100,2)}%" if pnl_actual > 0 else f"{round(pnl_actual*100,2)}%"
                        forced_closes.append(f"{symbol} {pnl_label} (earnings {earn_str})")
                        trade_log.record_trade("BERSERKER", symbol, pnl_actual, "earnings-close")
                        log_symbol_trade("BERSERKER", symbol, pnl_actual)
                        daily_stats["wins" if pnl_actual > 0 else "losses"] += 1
                        daily_stats["trades"] += 1
            else:
                # More than 48h out — release if previously blocked
                if symbol in earnings_blocked:
                    earnings_blocked.discard(symbol)
                    newly_released.append(symbol)

        except Exception as e:
            log(f"[EARN] Error checking {symbol}: {e}")

    # Alert on changes
    if newly_blocked or forced_closes:
        lines = ["🚫 EARNINGS BLACKOUT\n──────────────────"]
        if newly_blocked:
            lines.append("Blocked (entry gate):")
            for s in newly_blocked:
                lines.append(f"  ❌ {s}")
        if forced_closes:
            lines.append("Force-closed (24h):")
            for s in forced_closes:
                lines.append(f"  💸 {s}")
        lines.append("──────────────────")
        lines.append("Use /earnings for full status")
        alert("\n".join(lines), critical=True)
    if newly_released:
        log(f"[EARN] Released from blackout: {', '.join(newly_released)}")

    log(f"[EARN] Check complete | {len(earnings_blocked)} symbol(s) blocked")


def build_earnings_status() -> str:
    """T-Bone /earnings command response."""
    lines = ["📅 EARNINGS BLACKOUT STATUS\n──────────────────"]
    now_ts = time.time()
    age    = int((now_ts - _earnings_last_check) / 60) if _earnings_last_check > 0 else None

    if not _yfinance_ok:
        lines.append("⚠️ yfinance not installed — earnings checks disabled")
        return "\n".join(lines)

    if not earnings_blocked and not _earnings_cache:
        lines.append("✅ No earnings blackouts active")
        lines.append(f"Last checked: {'never' if not age else f'{age}m ago'}")
        return "\n".join(lines)

    if earnings_blocked:
        lines.append(f"🚫 Blocked ({len(earnings_blocked)}):")
        for sym in sorted(earnings_blocked):
            info = _earnings_cache.get(sym, {})
            date_str  = info.get("date", "unknown date")
            hours_away = info.get("hours_away", "?")
            lines.append(f"  ❌ {sym} — {date_str} ({hours_away}h)")
    else:
        lines.append("✅ No symbols currently blocked")

    # Show upcoming (not yet in 48h window)
    upcoming = [(s, d) for s, d in _earnings_cache.items()
                if s not in earnings_blocked and d.get("hours_away", 999) < 168]
    if upcoming:
        lines.append("──────────────────")
        lines.append("Upcoming (next 7d):")
        for sym, info in sorted(upcoming, key=lambda x: x[1].get("hours_away", 999)):
            lines.append(f"  📅 {sym} — {info.get('date','?')} ({info.get('hours_away','?')}h)")

    lines.append("──────────────────")
    lines.append(f"Last checked: {f'{age}m ago' if age is not None else 'pending'}")
    lines.append("Refresh every 4h | Next check auto-runs next hour")
    return "\n".join(lines)


# ==============================================================================
# V10.19: VIX REGIME GATE HELPERS
# ==============================================================================
def get_vix_status() -> dict:
    """Returns current VIX state for status/think displays."""
    vix = _vix_level_smooth
    if vix == 0.0:
        return {"level": 0.0, "label": "pending", "emoji": "❓", "blocking": False, "extreme": False}
    if vix < VIX_WARN_THRESHOLD:
        label, emoji, blocking, extreme = "normal", "🟢", False, False
    elif vix < VIX_BLOCK_THRESHOLD:
        label, emoji, blocking, extreme = "elevated", "🟡", False, False
    elif vix < VIX_EXTREME_THRESHOLD:
        label, emoji, blocking, extreme = "HIGH", "🔴", True, False
    else:
        label, emoji, blocking, extreme = "EXTREME", "🚨", True, True
    return {"level": round(vix, 1), "label": label, "emoji": emoji,
            "blocking": blocking, "extreme": extreme}

def vix_max_positions() -> int:
    """Return effective max positions given current VIX."""
    vix = _vix_level_smooth
    if vix > VIX_EXTREME_THRESHOLD:
        return 1
    return MAX_POSITIONS


# ==============================================================================
# V10.19: REGIME AGGREGATOR (cross-system posture signal)
# ==============================================================================
def compute_regime_score() -> tuple:
    """
    Compute regime score 0-5 from contributing factors.
    Broadcast to shared_state so Scanner and others can see it.
    Returns (score, factors_list).

    Scoring:
      +1 if Berserker circuit breaker active
      +1 if Scanner circuit breaker active
      +1 if crypto F&G < 25 (extreme fear)
      +1 if SPY below 20-day MA (bear regime)
      +1 if VIX > 25
    """
    global _regime_score, _regime_factors, _regime_shutdown_alerted
    score   = 0
    factors = []

    # Factor 1: Berserker circuit breaker
    if check_circuit_breaker():
        score += 1
        factors.append("CB: Berserker circuit breaker active")

    # Factor 2: Scanner circuit breaker
    if _scanner_ok and hasattr(scanner, 'is_circuit_breaker_active'):
        try:
            if scanner.is_circuit_breaker_active():
                score += 1
                factors.append("CB: Scanner circuit breaker active")
        except Exception:
            pass

    # Factor 3: Crypto F&G < 25
    try:
        snap = nexus_client.crypto_snapshot()
        if snap and snap.get("online"):
            fg = snap.get("fear_greed", 50)
            if isinstance(fg, (int, float)) and float(fg) < 25:
                score += 1
                factors.append(f"F&G: Extreme fear ({fg})")
    except Exception:
        pass

    # Factor 4: SPY below MA20
    if _spy_regime == "BEAR":
        score += 1
        factors.append("SPY: Below 20-period MA (bear regime)")

    # Factor 5: VIX > 25
    vix = _vix_level_smooth
    if vix > VIX_BLOCK_THRESHOLD:
        score += 1
        factors.append(f"VIX: {vix:.1f} > {VIX_BLOCK_THRESHOLD} (high fear)")

    # Broadcast
    _regime_score   = score
    _regime_factors = factors
    shared_state.set_regime(score, factors)

    # Alert at 4-5 (shutdown level)
    if score >= 4 and not _regime_shutdown_alerted:
        _regime_shutdown_alerted = True
        fstr = "\n".join(f"  • {f}" for f in factors)
        alert(
            f"🚨 REGIME SHUTDOWN LEVEL {score}/5\n"
            f"──────────────────\n"
            f"Factors:\n{fstr}\n"
            f"──────────────────\n"
            f"No new entries anywhere until regime improves.\n"
            f"Use /regime for details.",
            critical=True
        )
        log(f"[REGIME] Score {score}/5 — SHUTDOWN level reached")
    elif score < 4:
        _regime_shutdown_alerted = False   # reset so it can fire again next time

    return score, factors


def regime_allows_entry() -> bool:
    """
    Returns True if regime score allows new entries.
    Shutdown (4-5): no entries at all.
    Defensive (3): no Scanner entries — Berserker OK (handled in execute_trade size).
    """
    return _regime_score < 4


def regime_position_scale() -> float:
    """
    Returns position size multiplier based on regime score.
    0-1: 1.0 (normal)
    2:   0.75 (cautious — reduce 25%)
    3:   0.50 (defensive — reduce 50%)
    4-5: 0.0  (shutdown)
    """
    if _regime_score >= 4:
        return 0.0
    if _regime_score == 3:
        return 0.50
    if _regime_score == 2:
        return 0.75
    return 1.0


def build_regime_status() -> str:
    """T-Bone /regime command response."""
    score, factors = _regime_score, _regime_factors
    if score == 0:
        label, emoji = "NORMAL", "🟢"
    elif score == 1:
        label, emoji = "NORMAL", "🟢"
    elif score == 2:
        label, emoji = "CAUTIOUS", "🟡"
    elif score == 3:
        label, emoji = "DEFENSIVE", "🟠"
    else:
        label, emoji = "SHUTDOWN", "🚨"

    lines = [f"{emoji} NEXUS REGIME SCORE: {score}/5 — {label}\n──────────────────"]

    if factors:
        lines.append("Active factors:")
        for f in factors:
            lines.append(f"  • {f}")
    else:
        lines.append("No risk factors active ✅")

    lines.append("──────────────────")
    lines.append("Thresholds:")
    lines.append("  0-1: Normal — full operation")
    lines.append("  2:   Cautious — size -25%")
    lines.append("  3:   Defensive — size -50%, no Scanner entries")
    lines.append("  4-5: Shutdown — no new entries anywhere")
    lines.append("──────────────────")
    vix_st = get_vix_status()
    lines.append(f"VIX: {vix_st['emoji']} {vix_st['level']} ({vix_st['label']})")
    lines.append(f"SPY: {'BEAR' if _spy_regime == 'BEAR' else 'BULL'}")
    return "\n".join(lines)


# ==============================================================================
# V10.19: AUTOMATED STRATEGY PIPELINE
# Triggered by /run_strategist T-Bone command.
# Pulls from DB → builds backtest_log → runs strategist → posts to T-Bone.
# ==============================================================================
def run_strategy_pipeline() -> str:
    """
    Pull fingerprints from Railway Postgres, run strategist logic,
    return T-Bone-formatted report. Writes results to STRATEGY_RECIPES_FILE.
    This is the same logic as retrieve_results.py + strategist.py but
    runs in-process so Matthew doesn't need to be at his desktop.
    Returns status string (sent to T-Bone by caller).
    """
    if not _psycopg2_ok:
        return "⚠️ Strategy pipeline: psycopg2 not available"

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return "⚠️ Strategy pipeline: no DATABASE_URL"

    try:
        log("[STRAT] Starting strategy pipeline...")
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur  = conn.cursor(_pg_extras.RealDictCursor if _psycopg2_ok else None)

        # Pull completed fingerprints for recipe mining.
        # V10.34 data-hygiene fix: backtest rows (bt_) are DELIBERATE food
        # here -- bulk history is what recipes are mined from. But the old
        # filter also admitted pre-go-live paper rows (is_paper was added
        # with DEFAULT FALSE, backfilling old paper trades as "live") --
        # the same masquerade that poisoned the Win Follower in V10.31.
        # Rule: a row counts if it's a backtest row OR it was entered after
        # live launch. Paper-twin rows (is_paper=TRUE) stay excluded.
        cur.execute("""
            SELECT symbol, rsi_at_entry, spy_bullish, sector_health,
                   hour_cdt, won, pnl_pct, mfe, mae,
                   hold_minutes, exit_reason
            FROM berserker_trade_fingerprints
            WHERE won IS NOT NULL
              AND (is_paper IS NULL OR is_paper = FALSE)
              AND (trade_id LIKE 'bt_%%' OR entry_ts >= %s)
            ORDER BY created_at DESC
            LIMIT 30000
        """, (int(GATE_LAUNCH_DATE.timestamp()),))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            return "⚠️ Strategy pipeline: no fingerprints in DB yet"

        log(f"[STRAT] Pulled {len(rows)} fingerprints from DB")

        # Build per-symbol stats
        from collections import defaultdict
        sym_data = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": [], "mfe": []})
        for row in rows:
            sym  = row["symbol"]
            won  = bool(row["won"])
            pnl  = float(row["pnl_pct"]) if row["pnl_pct"] is not None else None  # stored as pct pts
            mfe  = float(row["mfe"])     if row["mfe"]     is not None else None  # stored as pct pts
            if won:
                sym_data[sym]["wins"] += 1
            else:
                sym_data[sym]["losses"] += 1
            if pnl is not None:
                sym_data[sym]["pnl"].append(pnl)
            if mfe is not None:
                sym_data[sym]["mfe"].append(mfe)

        # Generate recipe suggestions
        new_recipes = {}
        report_lines = [
            "🧠 NEXUS STRATEGIST PIPELINE\n──────────────────",
            f"Fingerprints analyzed: {len(rows):,}",
            f"Symbols in dataset: {len(sym_data)}",
            "──────────────────",
        ]

        breakeven_wr = STOP_LOSS_PCT / (TAKE_PROFIT_PCT + STOP_LOSS_PCT)

        for sym in sorted(sym_data.keys()):
            d      = sym_data[sym]
            total  = d["wins"] + d["losses"]
            if total < 10:
                continue
            wr     = d["wins"] / total
            avg_pnl = sum(d["pnl"]) / len(d["pnl"]) if d["pnl"] else 0
            avg_mfe = sum(d["mfe"]) / len(d["mfe"]) if d["mfe"] else 0

            # Determine optimal TP based on MFE distribution
            opt_tp = TAKE_PROFIT_PCT
            if d["mfe"] and len(d["mfe"]) >= 10:
                # Build winners_mfe by iterating rows directly (not zip — d["mfe"] is
                # already filtered to this symbol and can't be safely zipped against rows)
                winners_mfe = [
                    float(row["mfe"])          # already stored as pct pts — no * 100
                    for row in rows
                    if row["symbol"] == sym and bool(row["won"]) and row["mfe"] is not None
                ]
                if winners_mfe:
                    avg_wmfe = sum(winners_mfe) / len(winners_mfe)
                    pct_reach_2 = sum(1 for m in winners_mfe if m >= 2.0) / len(winners_mfe)
                    if avg_wmfe >= 3.0 and pct_reach_2 >= 0.15:
                        opt_tp = 0.025
                    elif avg_wmfe >= 2.0 and pct_reach_2 >= 0.25:
                        opt_tp = 0.020
                    else:
                        opt_tp = 0.015

            # Use existing recipe SL if available, else default
            existing = BERSERKER_RECIPES.get(sym, {})
            opt_sl   = existing.get("sl", STOP_LOSS_PCT)

            ev_negative = wr < breakeven_wr
            ev_emoji    = "🔴" if ev_negative else "✅"
            new_recipes[sym] = {
                "tp": opt_tp,
                "sl": opt_sl,
                "avoid_hours": existing.get("avoid_hours", []),
                "avoid_days":  existing.get("avoid_days", []),
                "wr": round(wr, 3),
                "trades": total,
            }
            report_lines.append(
                f"  {ev_emoji} {sym}: {round(wr*100)}% WR | "
                f"{total}t | TP={opt_tp*100:.1f}% SL={opt_sl*100:.1f}% | "
                f"AvgMFE={avg_mfe:.2f}%"
            )

        # Write recipes to persistent file
        try:
            os.makedirs(os.path.dirname(STRATEGY_RECIPES_FILE), exist_ok=True)
            with open(STRATEGY_RECIPES_FILE, "w") as f:
                json.dump(new_recipes, f, indent=2)
            report_lines.append("──────────────────")
            report_lines.append(f"✅ Recipes saved → {STRATEGY_RECIPES_FILE}")
            report_lines.append("Run /reload_recipes to apply without redeploy")
            log(f"[STRAT] Wrote {len(new_recipes)} recipes to {STRATEGY_RECIPES_FILE}")
        except Exception as e:
            report_lines.append(f"⚠️ Could not write recipes file: {e}")

        return "\n".join(report_lines)

    except Exception as e:
        log(f"[STRAT] Pipeline error: {e}")
        return f"⚠️ Strategy pipeline error: {e}"


def load_strategy_recipes_file() -> dict:
    """
    Load strategy_recipes.json from disk if it exists.
    Merges with hardcoded BERSERKER_RECIPES (file wins on TP/SL, hardcoded wins on hours/days).
    Returns merged dict, or empty dict if file doesn't exist / is malformed.
    """
    try:
        if not os.path.exists(STRATEGY_RECIPES_FILE):
            return {}
        with open(STRATEGY_RECIPES_FILE, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        log(f"[STRAT] Loaded {len(data)} recipes from {STRATEGY_RECIPES_FILE}")
        return data
    except Exception as e:
        log(f"[STRAT] Could not load recipes file: {e}")
        return {}


def apply_strategy_recipes_file():
    """
    Merge file-based recipes into BERSERKER_RECIPES (in-place update).
    File-derived TP/SL override hardcoded values.
    Hour/day gates from hardcoded BERSERKER_RECIPES always win (not overridden).
    Called at boot and on /reload_recipes command.
    """
    file_recipes = load_strategy_recipes_file()
    if not file_recipes:
        return

    changed = []
    for sym, frec in file_recipes.items():
        if sym not in BERSERKER_RECIPES:
            continue
        old_tp = BERSERKER_RECIPES[sym].get("tp", TAKE_PROFIT_PCT)
        old_sl = BERSERKER_RECIPES[sym].get("sl", STOP_LOSS_PCT)
        new_tp = frec.get("tp", old_tp)
        new_sl = frec.get("sl", old_sl)
        if abs(new_tp - old_tp) > 0.001 or abs(new_sl - old_sl) > 0.001:
            BERSERKER_RECIPES[sym]["tp"] = new_tp
            BERSERKER_RECIPES[sym]["sl"] = new_sl
            changed.append(f"{sym}: TP {old_tp*100:.1f}%→{new_tp*100:.1f}% "
                           f"SL {old_sl*100:.1f}%→{new_sl*100:.1f}%")

    if changed:
        log(f"[STRAT] Applied recipe overrides: {'; '.join(changed)}")
    else:
        log("[STRAT] Recipes file loaded — no changes from hardcoded values")


# ==============================================================================
# BOT LOGIC
# ==============================================================================
def update_sector_health():
    now = datetime.now(tz=CENTRAL)
    if now.weekday() >= 5 or now.hour < 8 or now.hour > 15:
        return
    # V10.9: Extended lookback 5->15 bars to prevent WEAK/STRONG flipping
    # every few minutes on short-term noise (observed in logs: 3 flips in 20min)
    down_count = sum(
        1 for sym in TRUMP_THEME
        if len(price_history[sym]) >= 15 and price_history[sym][-1] < price_history[sym][-15]
    )
    health = "WEAK" if down_count > len(TRUMP_THEME) / 2 else "STRONG"
    if health != portfolio["sector_health"]:
        portfolio["sector_health"] = health
        log(f"{'⚠️' if health=='WEAK' else '🔥'} Trump Sector now {health}")

def compute_macd(prices):
    """V10.16: Returns (macd_val, signal_val, histogram_series) so callers
    don't need to rebuild the full EWM chain for the histogram check."""
    s           = pd.Series(prices)
    ema_fast    = s.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow    = s.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    histogram   = macd_line - signal_line
    return macd_line.iloc[-1], signal_line.iloc[-1], histogram

def get_signals(symbol, price):
    prices = price_history[symbol]
    if len(prices) < max(RSI_PERIOD + 1, 26, MACD_SLOW + MACD_SIGNAL):
        return {"buy": False}

    # V10.8: Wilder smoothed RSI (EWM alpha=1/period)
    delta    = pd.Series(prices).diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.ewm(alpha=1.0 / RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / RSI_PERIOD, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, float("nan"))
    rsi      = (100 - (100 / (1 + rs))).iloc[-1]

    # V10.16: compute_macd now returns histogram too -- no need to rebuild EWM chain
    macd_val, macd_sig, _macd_histogram = compute_macd(prices)
    macd_bullish = macd_val > macd_sig
    ma20         = sum(prices[-20:]) / 20

    # V10.9: TECH_GROWTH symbols are NOT penalized by Trump-sector weakness.
    # The two baskets are driven by different macro regimes and frequently
    # diverge -- a TRUMP sector dip should not block a valid NVDA entry.
    is_trump     = symbol in TRUMP_THEME
    sector_weak  = portfolio["sector_health"] == "WEAK"
    required_rsi = 72 if (is_trump and sector_weak) else RSI_BUY_TRIGGER

    now          = datetime.now(tz=CENTRAL)
    recipe       = BERSERKER_RECIPES.get(symbol, {})
    hour_blocked = now.hour      in recipe.get("avoid_hours", [])
    day_blocked  = now.weekday() in recipe.get("avoid_days",  [])

    # Base gate: RSI + MACD + above MA20 + hour/day + sector
    base_ok = (rsi > required_rsi and macd_bullish and price > ma20
               and not hour_blocked and not day_blocked)

    if not base_ok:
        return {"buy": False, "rsi": round(rsi, 2), "macd_bull": macd_bullish}

    # Confluence signals -- need at least 1 of 4 to confirm
    confluence = 0

    # 1. Price momentum: recent 5-bar move stronger than prior 5-bar move
    if len(prices) >= 10:
        mom_recent = prices[-1] - prices[-6] if len(prices) >= 6 else 0
        mom_prior  = prices[-6] - prices[-11] if len(prices) >= 11 else 0
        if mom_recent > 0 and mom_recent > mom_prior:
            confluence += 1

    # 2. EMA9 above EMA21: short trend above medium trend
    if len(prices) >= 21:
        s     = pd.Series(prices)
        ema9  = float(s.ewm(span=9,  adjust=False).mean().iloc[-1])
        ema21 = float(s.ewm(span=21, adjust=False).mean().iloc[-1])
        if ema9 > ema21:
            confluence += 1

    # 3. V10.12: MACD histogram accelerating -- current histogram > prior bar
    # V10.16: reuses _macd_histogram from compute_macd() -- no duplicate EWM build
    if len(_macd_histogram) >= 2 and float(_macd_histogram.iloc[-1]) > float(_macd_histogram.iloc[-2]):
        confluence += 1

    # 4. Bouncing: price recovering from recent low (last 3 bars up)
    if len(prices) >= 4 and prices[-1] > prices[-4]:
        confluence += 1

    # 5. V10.19: Analyst score bridge — signal intelligence confirmation.
    # nexus_client.analyst_scores() returns {sym: {"score": int, "signals": list}}.
    # Analyst watches the same symbols 24/7 with 10+ technical signals; a score ≥ 1
    # on this symbol counts as an independent confirmation of the setup.
    # Cached by nexus_client (20s TTL) so no per-sweep HTTP penalty.
    # Soft: analyst agreement adds confluence, disagreement does not block.
    analyst_score_raw = 0
    try:
        a_scores = nexus_client.analyst_scores()
        if a_scores and symbol in a_scores:
            analyst_score_raw = int(a_scores[symbol].get("score", 0))
            if analyst_score_raw >= 1:
                confluence += 1
    except Exception:
        pass

    buy = confluence >= 1
    if not buy:
        log(f"⏳ Confluence gate [{symbol}]: RSI/MACD ok but 0/5 confirmation signals (analyst={analyst_score_raw})")

    return {
        "buy":           buy,
        "rsi":           round(rsi, 2),
        "macd_bull":     macd_bullish,
        "above_ma20":    price > ma20,
        "confluence":    confluence,
        "analyst_score": analyst_score_raw,
    }

def position_exists(symbol):
    try:
        trading_client.get_open_position(symbol)
        return True
    except:
        return False

def execute_trade(symbol, side, total_cash=0):
    try:
        if side == "BUY":
            now = datetime.now(tz=CENTRAL)
            if not is_market_hours_for_buying(now):
                return
            if bot_state["paused"] or bot_state.get("paused_berserker") or bot_state.get("daily_loss_hit"):
                return
            if bot_state.get("buys_disabled"):
                log(f"🚫 Buys disabled — skipping BUY [{symbol}]")
                return
            if not shared_state.claim(symbol, BOT_NAME):
                return
            budgets = shared_state.get_budgets(total_cash)
            amount  = round(budgets["berserker"] * TRADE_FRACTION, 2)

            # V10.19: Scale position size by regime score
            scale = regime_position_scale()
            if scale <= 0.0:
                log(f"🚫 REGIME SHUTDOWN [{symbol}]: regime score {_regime_score}/5 — no entries")
                shared_state.release(symbol)
                return
            if scale < 1.0:
                amount = round(amount * scale, 2)
                log(f"[REGIME] Position size scaled to {int(scale*100)}% due to regime score {_regime_score}")

            # V10.29: Win Follower HOT sizing -- feed the winners
            if _win_follower:
                _wf_mult = _win_follower.get_size_mult(symbol)
                if _wf_mult != 1.0:
                    amount = round(amount * _wf_mult, 2)
                    log(f"📈 WIN FOLLOWER [{symbol}]: HOT x{_wf_mult} -> ${amount:.2f}")

            if amount < MIN_TRADE_AMT:
                shared_state.release(symbol)
                return
            if amount > total_cash * 0.95:
                shared_state.release(symbol)
                return

            # V10.27 fix: the coordinator was clamping against the FULL
            # account total_cash, not Berserker's 90% share (budgets["berserker"]).
            # That meant Berserker could legitimately eat into Scanner's
            # reserved 10% as long as the combined Berserker+Phase4 spend
            # stayed under the whole account -- the coordinator only knew
            # about the Berserker-vs-Phase4 boundary, not the Berserker-vs-
            # Scanner one, which is a separate in-process convention via
            # shared_state.get_budgets() that the coordinator never saw.
            # Clamping against budgets["berserker"] instead means Berserker's
            # cross-service-coordinated spend can never exceed its own 90%
            # share, so Scanner's 10% stays untouched by this logic --
            # matching the original intent of get_budgets() before Phase4's
            # capital coordination was added on top of it.
            #
            # V10.24: Cross-service capital coordination -- Phase4 trades
            # against this SAME Alpaca account from a separate process. Clamp
            # our intended spend against what's actually available once
            # Phase4's outstanding reservations (if any) are accounted for.
            # Fails open: if the coordinator can't reach the DB, available
            # falls back to budgets["berserker"] unchanged and we proceed
            # exactly as before this existed.
            if _capital_coordinator:
                available = _capital_coordinator.get_available(budgets["berserker"])
                if amount > available:
                    if available < MIN_TRADE_AMT:
                        log(f"💰 CAPITAL COORD [{symbol}]: ${available:.2f} available "
                            f"of Berserker's 90% share (Phase4 holding the rest) — "
                            f"below ${MIN_TRADE_AMT} min, skipping")
                        shared_state.release(symbol)
                        return
                    log(f"💰 CAPITAL COORD [{symbol}]: trimmed ${amount:.2f} -> "
                        f"${available:.2f} (Phase4 reservation active, "
                        f"Berserker's 90% share ${budgets['berserker']:.2f})")
                    amount = round(available, 2)

            # V10.19: Earnings blackout gate
            if symbol in earnings_blocked:
                log(f"🚫 EARNINGS GATE [{symbol}]: earnings within 48h — skipping entry")
                shared_state.release(symbol)
                return

            # V10.5: Win-rate gate -- skip entries where this exact historical
            # setup (symbol|RSI bucket|SPY trend|sector health|TRUMP-or-TECH|
            # hour) has a win rate below WIN_RATE_GATE_THRESHOLD, once enough
            # samples exist (>= PM_MIN_BUCKET_TRADES). "No data" never blocks
            # a trade. V10.23: PDT slots dropped from the bucket key -- PDT
            # was eliminated by FINRA Jun 4 2026.
            signals = get_signals(symbol, price_history[symbol][-1] if price_history[symbol] else 0)
            spy_ctx = _get_spy_context_for_fingerprint()
            if _berserker_memory:
                rsi      = signals.get("rsi", 50)
                hour     = now.hour
                skip, wr, has_data = _berserker_memory.should_skip_entry(
                    symbol, rsi, spy_ctx.get("bullish", False),
                    portfolio["sector_health"], hour
                )
                # V10.29: Win Follower gate discount -- HOT/WARM symbols get
                # a lower bar, floored HARD at the 40% mathematical breakeven
                # (1.5%TP/1.0%SL). Earning wins live buys an easier gate;
                # nothing ever trades below breakeven expectancy.
                if skip and _win_follower:
                    _disc = _win_follower.get_gate_discount(symbol)
                    _floor = max(WF_GATE_FLOOR, WIN_RATE_GATE_THRESHOLD - _disc)
                    if _disc > 0 and wr >= _floor:
                        skip = False
                        log(f"🔥 WF GATE PASS [{symbol}]: {_win_follower.get_tier(symbol)} "
                            f"discount -{_disc:.0%} -> WR {wr:.0%} clears {_floor:.0%}")
                if skip:
                    log(f"🚫 WIN-RATE GATE [{symbol}]: historical WR={wr:.0%} "
                        f"< {WIN_RATE_GATE_THRESHOLD:.0%} -- skipping entry")
                    shared_state.release(symbol)
                    return
                if has_data:
                    log(f"✅ Win-rate check passed [{symbol}]: historical WR={wr:.0%}")

                # V10.19: Dynamic TP — check bucket MFE distribution
                recipe_tp = BERSERKER_RECIPES.get(symbol, {}).get("tp", TAKE_PROFIT_PCT)
                dyn_tp = _berserker_memory.get_dynamic_tp(
                    symbol, rsi, spy_ctx.get("bullish", False),
                    portfolio["sector_health"], hour, recipe_tp
                )
                if dyn_tp > recipe_tp:
                    log(f"🎯 Dynamic TP [{symbol}]: bucket avg MFE supports TP → {dyn_tp*100:.1f}% (was {recipe_tp*100:.1f}%)")
            else:
                dyn_tp = BERSERKER_RECIPES.get(symbol, {}).get("tp", TAKE_PROFIT_PCT)

            # V10.24: Reserve against the shared account immediately before
            # submitting, release immediately after -- closes the race window
            # where Phase4 (separate process, same Alpaca account) could read
            # stale buying_power between now and Alpaca settling this order.
            _res_id = _capital_coordinator.reserve(amount, symbol=symbol) if _capital_coordinator else None
            try:
                order = MarketOrderRequest(
                    symbol=symbol, notional=amount,
                    side=OrderSide.BUY, time_in_force=TimeInForce.DAY
                )
                _submitted = trading_client.submit_order(order)
            finally:
                if _capital_coordinator:
                    _capital_coordinator.release(_res_id)
            position_entry_times[symbol] = datetime.now(tz=CENTRAL)

            # V10.35: anchor the fingerprint to the ACTUAL fill price, not
            # the last polled IEX quote. Live TP/SL math was already
            # fill-true (pos.avg_entry_price), but the fingerprint's
            # entry_price -- which Exit Autopsy reconstructs exit prices
            # from -- was the stale poll. Brief poll; fallback to the quote.
            _fill_price = 0.0
            try:
                for _fa in range(3):
                    _o = trading_client.get_order_by_id(_submitted.id)
                    if _o is not None and getattr(_o, "filled_avg_price", None):
                        _fill_price = float(_o.filled_avg_price)
                        break
                    time.sleep(1)
            except Exception as _fe:
                log(f"[FILL] {symbol}: fill poll failed ({_fe}) -- using quote anchor")
            _entry_anchor = _fill_price if _fill_price > 0 else (
                price_history[symbol][-1] if price_history[symbol] else 0)

            # V10.1: fingerprint entry — V10.19: store dynamic_tp per trade
            trade_id = secrets.token_hex(8)
            _berserker_fingerprints[symbol] = {
                "trade_id":   trade_id,
                "entry_time": time.time(),
                "peak_price": _entry_anchor,   # V10.35: fill-anchored
                "mfe":        0.0,
                "mae":        0.0,
                "dynamic_tp": dyn_tp,   # V10.19: may be elevated above recipe TP
            }
            if _berserker_memory:
                _berserker_memory.record_entry(
                    trade_id, symbol,
                    _entry_anchor,   # V10.35: fill-anchored entry price
                    signals.get("rsi", 50),
                    signals.get("macd_bull", False),
                    bool(price_history[symbol] and price_history[symbol][-1] >
                         sum(price_history[symbol][-20:]) / max(len(price_history[symbol][-20:]), 1)),
                    spy_ctx,
                    portfolio["sector_health"],
                    is_paper=False,   # V10.17: live trade
                )

            dyn_note = f" | DynTP={dyn_tp*100:.1f}%" if dyn_tp > BERSERKER_RECIPES.get(symbol, {}).get("tp", TAKE_PROFIT_PCT) else ""
            _wf_tag = ""
            if _win_follower and _win_follower.get_tier(symbol) in ("HOT", "WARM"):
                _wf_tag = f" [{_win_follower.get_tier(symbol)}]"
            log(f"🚀 BUY: {symbol} | ${amount} notional{dyn_note}{_wf_tag}")
            alert(f"🚀 NEXUS BUY [{BOT_NAME}]: {symbol} | ${amount} order{dyn_note}{_wf_tag}")
            daily_stats["trades"] += 1
            return True

        elif side == "SELL":
            if not position_exists(symbol):
                shared_state.release(symbol)
                pending_sells.discard(symbol)
                return
            try:
                trading_client.close_position(symbol)
                log(f"🔒 SELL confirmed: {symbol}")
                shared_state.release(symbol)
                pending_sells.discard(symbol)
                portfolio["peak_prices"].pop(symbol, None)
                position_entry_times.pop(symbol, None)
                mark_recently_traded(symbol)
                save_berserker_peaks()
                return True
            except Exception as sell_err:
                raise

    except Exception as e:
        log(f"⚠️ Trade error [{symbol} {side}]: {e}")
        shared_state.release(symbol)
        pending_sells.discard(symbol)

def manage_exits(symbol, price, pos):
    global _consecutive_losses, _circuit_break_until  # V10.2: circuit breaker
    if symbol in pending_sells:
        return

    avg_entry  = float(pos.avg_entry_price)
    portfolio["peak_prices"].setdefault(symbol, price)
    portfolio["peak_prices"][symbol] = max(portfolio["peak_prices"][symbol], price)
    peak       = portfolio["peak_prices"][symbol]
    profit_pct = (price - avg_entry) / avg_entry
    trailing   = RATCHET_TRAIL_TIGHT if profit_pct >= RATCHET_PROFIT else TRAILING_STOP
    held_mins  = hold_time_minutes(symbol)

    # V10.17: Per-symbol TP/SL from BERSERKER_RECIPES (fallback to global constants)
    # V10.19: Further override by dynamic_tp stored in fingerprint at entry time
    recipe    = BERSERKER_RECIPES.get(symbol, {})
    sym_tp    = recipe.get("tp", TAKE_PROFIT_PCT)
    sym_sl    = recipe.get("sl", STOP_LOSS_PCT)
    fp_entry  = _berserker_fingerprints.get(symbol, {})
    # dynamic_tp is only elevated, never lower than recipe
    sym_tp    = fp_entry.get("dynamic_tp", sym_tp)

    # V10.1: Update MFE/MAE in real time
    if symbol in _berserker_fingerprints:
        _berserker_fingerprints[symbol]["mfe"] = max(
            _berserker_fingerprints[symbol].get("mfe", 0.0), profit_pct
        )
        _berserker_fingerprints[symbol]["mae"] = min(
            _berserker_fingerprints[symbol].get("mae", 0.0), profit_pct
        )

    # Hard take-profit — per-symbol level from recipe/dynamic, fallback to global
    if profit_pct >= sym_tp:
        emoji     = "✅"
        pnl_label = f"+{round(profit_pct*100,2)}%"
        fp  = _berserker_fingerprints.get(symbol, {})
        mfe = fp.get("mfe", 0.0)
        mae = fp.get("mae", 0.0)
        dyn_note = f" | 🎯DynTP={sym_tp*100:.1f}%" if fp.get("dynamic_tp") and fp.get("dynamic_tp", 0) > recipe.get("tp", TAKE_PROFIT_PCT) else ""
        log(f"{emoji} Exit [take-profit]: {symbol} | P&L: {pnl_label} | "
            f"MFE: {round(mfe*100,2):+.2f}% | held {int(held_mins)}m{dyn_note}")
        alert_exit(symbol, f"{emoji} NEXUS EXIT [take-profit]: {symbol} | P&L: {pnl_label}{dyn_note}")
        pending_sells.add(symbol)
        success = execute_trade(symbol, "SELL")
        if success is not False:
            daily_stats["wins"] += 1
            daily_stats["trades"] += 1
            trade_log.record_trade("BERSERKER", symbol, profit_pct, "take-profit")
            log_symbol_trade("BERSERKER", symbol, profit_pct)
            shared_state.set_cooldown(symbol, COOLDOWN_SECS)
            if _berserker_memory and fp.get("trade_id"):
                _berserker_memory.record_exit(
                    fp["trade_id"], True, profit_pct, "take-profit",
                    int(held_mins), mfe, mae
                )
            _berserker_fingerprints.pop(symbol, None)
            _consecutive_losses = 0
            log("[CB] Consecutive loss counter reset on TP win")
        return

    if profit_pct <= -sym_sl:
        emoji     = "🛑"
        pnl_label = f"{round(profit_pct*100,2)}%"
        fp = _berserker_fingerprints.get(symbol, {})
        mfe = fp.get("mfe", 0.0)
        mae = fp.get("mae", 0.0)
        log(f"{emoji} Exit [stop-loss]: {symbol} | P&L: {pnl_label} | "
            f"MFE: {round(mfe*100,2):+.2f}% | held {int(held_mins)}m")
        alert_exit(symbol, f"{emoji} NEXUS EXIT [stop-loss]: {symbol} | P&L: {pnl_label}")
        pending_sells.add(symbol)
        success = execute_trade(symbol, "SELL")
        if success is not False:
            daily_stats["losses"] += 1
            daily_stats["trades"] += 1
            trade_log.record_trade("BERSERKER", symbol, profit_pct, "stop-loss")
            log_symbol_trade("BERSERKER", symbol, profit_pct)
            shared_state.set_cooldown(symbol, COOLDOWN_SECS)
            # V10.1: fingerprint exit
            if _berserker_memory and fp.get("trade_id"):
                _berserker_memory.record_exit(
                    fp["trade_id"], False, profit_pct, "stop-loss",
                    int(held_mins), mfe, mae
                )
            _berserker_fingerprints.pop(symbol, None)
            # V10.2: consecutive loss tracking
            # V10.13: Don't increment consecutive_losses while CB is active --
            # stops fired during an active CB pause are expected and shouldn't
            # re-trigger a new CB immediately after the timer resets.
            if not check_circuit_breaker():
                _consecutive_losses += 1
                log(f"[CB] Consecutive losses: {_consecutive_losses}/{CONSEC_LOSS_LIMIT}")
                if _consecutive_losses >= CONSEC_LOSS_LIMIT:
                    trigger_circuit_breaker()

    elif (peak - price) / peak >= trailing:
        if held_mins < MIN_HOLD_MINUTES:
            log(f"⏳ Trail suppressed [{symbol}] — only held {int(held_mins)}m")
            return
        emoji     = "✅" if profit_pct > 0 else "🛑"
        pnl_label = f"+{round(profit_pct*100,2)}%" if profit_pct > 0 else f"{round(profit_pct*100,2)}%"
        fp  = _berserker_fingerprints.get(symbol, {})
        mfe = fp.get("mfe", 0.0)
        mae = fp.get("mae", 0.0)
        log(f"{emoji} Exit [trailing-stop]: {symbol} | P&L: {pnl_label} | "
            f"MFE: {round(mfe*100,2):+.2f}% | held {int(held_mins)}m")
        alert_exit(symbol, f"{emoji} NEXUS EXIT [trailing-stop]: {symbol} | P&L: {pnl_label}")
        pending_sells.add(symbol)
        success = execute_trade(symbol, "SELL")
        if success is not False:
            daily_stats["wins" if profit_pct > 0 else "losses"] += 1
            daily_stats["trades"] += 1
            trade_log.record_trade("BERSERKER", symbol, profit_pct, "trailing-stop")
            log_symbol_trade("BERSERKER", symbol, profit_pct)
            shared_state.set_cooldown(symbol, COOLDOWN_SECS)
            # V10.1: fingerprint exit
            if _berserker_memory and fp.get("trade_id"):
                _berserker_memory.record_exit(
                    fp["trade_id"], profit_pct > 0, profit_pct, "trailing-stop",
                    int(held_mins), mfe, mae
                )
            _berserker_fingerprints.pop(symbol, None)
            # V10.2: reset consecutive losses on any win
            if profit_pct > 0:
                _consecutive_losses = 0
                log("[CB] Consecutive loss counter reset on win")
            else:
                # V10.13: Same guard -- don't count while CB already active
                if not check_circuit_breaker():
                    _consecutive_losses += 1
                    log(f"[CB] Consecutive losses: {_consecutive_losses}/{CONSEC_LOSS_LIMIT}")
                    if _consecutive_losses >= CONSEC_LOSS_LIMIT:
                        trigger_circuit_breaker()


# ==============================================================================
# V10.2: PAPER TRADING ENGINE
# Runs parallel to live Berserker using Alpaca paper account.
# V10.23: "no PDT" was historically the distinguishing feature vs live --
# PDT no longer applies to either, so paper's main edge now is just looser
# cooldown/gating to seed pattern memory fast.
# Same signal logic, same fingerprinting, no real money.
# ==============================================================================
def paper_execute_buy(symbol: str, cash: float) -> bool:
    """Place a paper market buy order."""
    if not _paper_trading_client:
        return False
    try:
        amount = round(min(cash * TRADE_FRACTION, cash * 0.95), 2)
        if amount < MIN_TRADE_AMT:
            return False
        # V10.2: Also skip if amount would create a dust position
        if amount < MIN_POSITION_VALUE:
            log(f"🧹 Skipping {symbol} buy -- amount ${amount:.2f} would create dust position")
            return False
        order = MarketOrderRequest(
            symbol=symbol,
            notional=amount,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
        )
        _paper_trading_client.submit_order(order)
        return True
    except Exception as e:
        log(f"[PAPER] Buy error {symbol}: {e}")
        return False


def paper_execute_sell(symbol: str) -> bool:
    """Close a paper position."""
    if not _paper_trading_client:
        return False
    try:
        _paper_trading_client.close_position(symbol)
        return True
    except Exception as e:
        log(f"[PAPER] Sell error {symbol}: {e}")
        return False


def run_paper_berserker():
    """
    Paper trading loop -- mirrors live Berserker but:
    - Uses paper account (no real money)
    - No cooldown between trades
    - Fingerprints every trade to berserker_trade_fingerprints
    - Runs every 30 seconds same as live
    """
    if not PAPER_ENABLED:
        log("[PAPER] Paper trading disabled -- set ALPACA_PAPER_API_KEY and ALPACA_PAPER_SECRET_KEY")
        return

    log("[PAPER] Paper Berserker started -- aggressive mode")
    time.sleep(15)  # stagger start from live loop

    while True:
        try:
            now = datetime.now(tz=CENTRAL)

            # Only run during market hours
            if now.weekday() >= 5 or now.hour < 8 or now.hour >= 15:
                time.sleep(30)
                continue

            # Get paper account state
            try:
                acct      = _paper_trading_client.get_account()
                cash      = float(acct.cash)
                raw_pos   = _paper_trading_client.get_all_positions()
                positions = {p.symbol: p for p in raw_pos}
            except Exception as e:
                log(f"[PAPER] Account fetch error: {e}")
                time.sleep(30)
                continue

            # V10.16: Batch price fetch for paper loop -- one call for all symbols
            try:
                paper_batch = _paper_data_client.get_stock_latest_trade(
                    StockLatestTradeRequest(symbol_or_symbols=SYMBOLS, feed=DataFeed.IEX)
                )
            except Exception as e:
                log(f"[PAPER] Batch price fetch failed: {e}")
                time.sleep(30)
                continue

            # Update price history and manage exits
            for symbol in SYMBOLS:
                try:
                    price = float(paper_batch[symbol].price)
                except Exception:
                    continue

                _paper_price_history[symbol].append(price)

                # Manage exits for open paper positions
                if symbol in positions:
                    pos        = positions[symbol]
                    avg_entry  = float(pos.avg_entry_price)
                    _paper_peak_prices.setdefault(symbol, avg_entry)
                    _paper_peak_prices[symbol] = max(_paper_peak_prices[symbol], price)
                    peak       = _paper_peak_prices[symbol]
                    profit_pct = (price - avg_entry) / avg_entry if avg_entry > 0 else 0
                    drawdown   = (peak - price) / peak if peak > 0 else 0
                    held_mins  = 0
                    if symbol in _paper_entry_times:
                        held_mins = (datetime.now(tz=CENTRAL) -
                                     _paper_entry_times[symbol]).total_seconds() / 60

                    fp  = _paper_fingerprints.get(symbol, {})
                    mfe = max(fp.get("mfe", 0.0), profit_pct)
                    mae = min(fp.get("mae", 0.0), profit_pct)
                    if symbol in _paper_fingerprints:
                        _paper_fingerprints[symbol]["mfe"] = mfe
                        _paper_fingerprints[symbol]["mae"] = mae

                    # Stop loss — per-symbol sl from recipe
                    _paper_sl = BERSERKER_RECIPES.get(symbol, {}).get("sl", STOP_LOSS_PCT)
                    if price <= avg_entry * (1 - _paper_sl):
                        if paper_execute_sell(symbol):
                            _paper_stats["losses"] += 1
                            _paper_stats["trades"] += 1
                            if _berserker_memory and fp.get("trade_id"):
                                _berserker_memory.record_exit(
                                    fp["trade_id"], False, profit_pct,
                                    "paper-stop", int(held_mins), mfe, mae
                                )
                            _paper_fingerprints.pop(symbol, None)
                            _paper_peak_prices.pop(symbol, None)
                            _paper_entry_times.pop(symbol, None)
                            log(f"[PAPER] 🛑 Stop {symbol} | {profit_pct*100:+.2f}%")

                    # Trailing stop
                    elif drawdown >= (0.01 if profit_pct >= RATCHET_PROFIT else TRAILING_STOP):
                        if held_mins >= MIN_HOLD_MINUTES:
                            won = profit_pct > 0
                            if paper_execute_sell(symbol):
                                _paper_stats["wins" if won else "losses"] += 1
                                _paper_stats["trades"] += 1
                                if _berserker_memory and fp.get("trade_id"):
                                    _berserker_memory.record_exit(
                                        fp["trade_id"], won, profit_pct,
                                        "paper-trail", int(held_mins), mfe, mae
                                    )
                                _paper_fingerprints.pop(symbol, None)
                                _paper_peak_prices.pop(symbol, None)
                                _paper_entry_times.pop(symbol, None)
                                emoji = "✅" if won else "🔴"
                                log(f"[PAPER] {emoji} Trail {symbol} | {profit_pct*100:+.2f}%")

                    # EOD exit
                    elif now.hour >= 14 and now.minute >= 45:
                        won = profit_pct > 0
                        if paper_execute_sell(symbol):
                            _paper_stats["wins" if won else "losses"] += 1
                            _paper_stats["trades"] += 1
                            if _berserker_memory and fp.get("trade_id"):
                                _berserker_memory.record_exit(
                                    fp["trade_id"], won, profit_pct,
                                    "paper-eod", int(held_mins), mfe, mae
                                )
                            _paper_fingerprints.pop(symbol, None)
                            _paper_peak_prices.pop(symbol, None)
                            _paper_entry_times.pop(symbol, None)

            # Look for new entries -- max 3 positions
            if len(positions) < MAX_POSITIONS:
                for symbol in SYMBOLS:
                    if len(positions) >= MAX_POSITIONS:
                        break
                    if symbol in positions:
                        continue

                    # Check avoid hours
                    avoid = BERSERKER_RECIPES.get(symbol, {}).get("avoid_hours", [])
                    if now.hour in avoid:
                        continue

                    if not _paper_price_history[symbol]:
                        continue

                    price   = _paper_price_history[symbol][-1]
                    # V10.12: Paper uses loosened RSI gate (45+, not 62+) so
                    # pattern memory fingerprints mid-RSI entries, not just
                    # the same high-RSI setups the live engine already trades.
                    # All other signal logic (MACD, MA20, confluence) unchanged.
                    paper_price = price
                    paper_hist  = _paper_price_history[symbol]
                    if len(paper_hist) < max(RSI_PERIOD + 1, 26):
                        continue
                    # Compute RSI directly for paper gate
                    import pandas as _pd_paper
                    _delta   = _pd_paper.Series(paper_hist).diff()
                    _gain    = _delta.where(_delta > 0, 0.0)
                    _loss    = (-_delta.where(_delta < 0, 0.0))
                    _ag      = _gain.ewm(alpha=1.0/RSI_PERIOD, adjust=False).mean()
                    _al      = _loss.ewm(alpha=1.0/RSI_PERIOD, adjust=False).mean()
                    _rs      = _ag / _al.replace(0, float("nan"))
                    _rsi_val = float((100 - (100 / (1 + _rs))).iloc[-1])
                    if _rsi_val < 45:   # paper: 45+ (live is 62+)
                        continue
                    macd_v, macd_s, _ = compute_macd(paper_hist)
                    if not (macd_v > macd_s):
                        continue
                    ma20_p = sum(paper_hist[-20:]) / 20 if len(paper_hist) >= 20 else 0
                    if ma20_p > 0 and paper_price < ma20_p:
                        continue
                    signals = {"buy": True, "rsi": round(_rsi_val, 2),
                               "macd_bull": True, "above_ma20": paper_price > ma20_p,
                               "confluence": 1}

                    # Bounce check
                    hist = paper_hist
                    if len(hist) < 4 or hist[-1] <= hist[-4]:
                        continue

                    # Enter
                    if paper_execute_buy(symbol, cash):
                        trade_id = secrets.token_hex(8)
                        spy_ctx  = _get_spy_context_for_fingerprint()
                        _paper_fingerprints[symbol] = {
                            "trade_id": trade_id,
                            "mfe": 0.0,
                            "mae": 0.0,
                        }
                        _paper_entry_times[symbol]  = datetime.now(tz=CENTRAL)
                        _paper_peak_prices[symbol]  = price
                        # Fingerprint entry
                        if _berserker_memory:
                            _berserker_memory.record_entry(
                                trade_id, symbol, price,
                                signals.get("rsi", 50),
                                signals.get("macd_bull", False),
                                signals.get("above_ma20", False),
                                spy_ctx,
                                portfolio["sector_health"],
                                is_paper=True,  # V10.17: paper trade
                            )
                        log(f"[PAPER] 🚀 BUY {symbol} @ ${price:.2f} | "
                            f"RSI={signals.get('rsi',0):.1f}")
                        positions[symbol] = True  # mark as taken for this loop

        except Exception as e:
            log(f"[PAPER] Loop error: {e}")

        time.sleep(30)


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
if __name__ == "__main__":

    # -- Start threads ──────────────────────────────────────────────────────---
    if not _scanner_ok:
        log(f"🔴 scanner.py import FAILED: {_scanner_err}")
    else:
        log("✅ scanner.py imported OK")
        # V10.30: adaptive launch. The deployed scanner.run() took zero args
        # while this call passed bot_state -- the thread died instantly with
        # TypeError and boot just logged "alive: False" without alerting.
        # Now the launcher matches whatever signature scanner.run has, and a
        # dead thread fires a critical alert instead of failing silently.
        import inspect as _inspect
        try:
            _sc_nparams = len(_inspect.signature(scanner.run).parameters)
        except Exception:
            _sc_nparams = 1
        _sc_target = (lambda: scanner.run(bot_state)) if _sc_nparams >= 1 else (lambda: scanner.run())
        t = threading.Thread(target=_sc_target, daemon=True, name="Scanner")
        t.start()
        time.sleep(1)
        log(f"🔍 Scanner thread alive: {t.is_alive()}")
        if not t.is_alive():
            alert("🚨 SCANNER THREAD DIED AT BOOT — check Railway logs immediately", critical=True)

    # V10.4: Options Engine -- watches crypto's Tier 2 Opportunity Scanner
    # and (if enabled via /options_on) buys a defined-risk MSTR/COIN call.
    # Starts DISABLED -- see options_engine.py for full safety design.
    if not _options_ok:
        log(f"🔴 options_engine.py import FAILED: {_options_err}")
    else:
        try:
            options_engine.init(trading_client, lambda msg: alert(msg, critical=True),
                                 nexus_client, log, paper_trading_client=_paper_trading_client)
            t = threading.Thread(target=options_engine.run_loop, daemon=True, name="OptionsEngine")
            t.start()
            time.sleep(1)
            log(f"🎯 Options engine thread alive: {t.is_alive()} | "
                f"enabled={options_engine.get_state().get('enabled')}")
        except Exception as e:
            log(f"🔴 options_engine init/start failed: {e}")

    threading.Thread(target=handle_commands, daemon=True, name="TelegramCommands").start()
    log("[TEL] T-Bone is live")

    threading.Thread(target=run_watchdog, daemon=True, name="Watchdog").start()
    log("[DOG] Watchdog thread launched")

    log(f"⚡ NEXUS V10.35 ONLINE | {len(SYMBOLS)} symbols | MAX {MAX_POSITIONS} positions | Paper: {'ON' if PAPER_ENABLED else 'OFF'}")

    # -- Startup data fetch ────────────────────────────────────----------------
    time.sleep(8)
    try:
        acct   = trading_client.get_account()
        equity = float(acct.equity)
        start  = load_daily_state(equity)
        daily_stats["start_equity"] = start
    except Exception as e:
        log(f"⚠️ Startup error: {e}")
        equity = 0.0

    berserker_recovered = recover_berserker_positions()

    # -- Health checks — log only, no spam ────────────────────────────────────-
    crypto_ok  = bool(nexus_client.crypto_health())
    log(f"{'✅' if crypto_ok else '🔴'} Service B (crypto): {'online' if crypto_ok else 'offline'}")

    # V10.10 fix: analyst boots slower than other services (DB init + warmup_stocks
    # + log restore). 3x6s = 18s was consistently too short -- declared offline
    # before analyst finished booting. Extended to 8x10s = 80s max wait.
    analyst_ok = False
    for _attempt in range(8):
        analyst_ok = bool(nexus_client.analyst_health())
        if analyst_ok:
            break
        log(f"⏳ Analyst health check {_attempt+1}/8...")
        time.sleep(10)
    log(f"{'✅' if analyst_ok else '🔴'} Service C (analyst): {'online' if analyst_ok else 'offline'}")

    phase4_ok = False
    p4_check  = nexus_client.phase4_think()
    if p4_check and p4_check.get("online"):
        phase4_ok = True
    log(f"{'✅' if phase4_ok else '🔴'} Service D (phase4): {'online' if phase4_ok else 'offline'}")

    # -- Single clean boot alert ────────────────────────────────────-----------
    service_lines = []
    service_lines.append(f"  🔥 BERSERKER — {len(SYMBOLS)} symbols | max {MAX_POSITIONS} pos"
                         + (f" | {berserker_recovered} recovered" if berserker_recovered else ""))
    service_lines.append(f"  ⚡ PHASE4 — {'✅' if phase4_ok else '⚠️ offline'}")
    service_lines.append(f"  🔍 SCANNER — {'✅' if _scanner_ok else '⚠️ ' + _scanner_err[:30]}")
    service_lines.append(f"  🌙 CRYPTO — {'✅' if crypto_ok else '⚠️ offline'}")
    service_lines.append(f"  🔭 ANALYST — {'✅' if analyst_ok else '⚠️ offline'}")
    if _options_ok:
        _opt_state = options_engine.get_state()
        _opt_label = "✅ ON" if _opt_state.get("enabled") else "⏸️ off"
        _opt_mode  = "🧪 paper" if _opt_state.get("paper_mode", True) else "🔴 LIVE"
        service_lines.append(f"  🎯 OPTIONS — {_opt_label} ({_opt_mode})")
    else:
        service_lines.append(f"  🎯 OPTIONS — ⚠️ {_options_err[:30]}")

    alert(
        f"⚡ NEXUS V10.35 ONLINE\n"
        f"Win Follower: ON (14d | HOT x1.3 + gate -7pts | auto-bench/return)\n"
        f"──────────────────\n"
        f"Equity: ${equity:.2f}\n"
        f"Win-rate gate: {WIN_RATE_GATE_THRESHOLD:.0%} (cold-start ramp)\n"
        f"──────────────────\n"
        + "\n".join(service_lines) + "\n"
        f"──────────────────\n"
        f"T-Bone watching 👀🥩",
        critical=True
    )

    # V10.2: Initialize Berserker pattern memory
    _db_url = os.environ.get("DATABASE_URL", "")
    if _db_url and _psycopg2_ok:
        _berserker_memory = BerserkerMemory(_db_url)
        _berserker_memory.init_tables()
        _berserker_memory.start_scheduler()
        log("[BM] Berserker pattern memory: DB connected")
    else:
        _berserker_memory = BerserkerMemory("")
        log("[BM] Berserker pattern memory: disabled (no DATABASE_URL or psycopg2)")

    # V10.29: Berserker Win Follower -- follow-the-wins allocator
    if _db_url and _psycopg2_ok:
        _win_follower = BerserkerWinFollower(_db_url)
        _win_follower.start_scheduler()
        # V10.32: Exit Autopsy resolver (Berserker + Scanner shared table)
        threading.Thread(target=_equity_autopsy_resolver, daemon=True,
                         name="autopsy-resolver").start()
        log("[AUTOPSY] equity exit-autopsy resolver started (5m cadence)")
    else:
        log("[WF] Win Follower: disabled (no DATABASE_URL or psycopg2)")

    # V10.24: Capital coordinator -- Berserker and Phase4 trade against the
    # SAME live Alpaca account from separate Railway services/processes with
    # zero prior coordination. See capital_coordinator.py for full rationale.
    _capital_coordinator = CapitalCoordinator(_db_url, service_name="berserker")
    _capital_coordinator.init_table()

    # V10.19: Load strategy recipes from file (overrides hardcoded TP/SL if present)
    apply_strategy_recipes_file()

    # V10.19: Initial earnings check (runs in background so boot isn't delayed)
    if _yfinance_ok:
        threading.Thread(target=check_earnings_calendar, daemon=True,
                         name="EarningsBootCheck").start()
        log("[EARN] Earnings calendar boot check started (background)")
    else:
        log("[EARN] yfinance not available — earnings blackout disabled")

    # V10.2: Start paper trading thread if credentials are set
    if PAPER_ENABLED:
        threading.Thread(target=run_paper_berserker, daemon=True,
                         name="PaperBerserker").start()
        log("[PAPER] Paper Berserker started -- aggressive fingerprint collection")
    else:
        log("[PAPER] Paper Berserker disabled -- add ALPACA_PAPER_API_KEY + ALPACA_PAPER_SECRET_KEY to Railway")

    # -- Main loop ──────────────────────────────────────────────────────-------
    cycle       = 0
    error_count = 0

    while True:
        try:
            cycle       += 1
            now          = datetime.now(tz=CENTRAL)
            acct         = trading_client.get_account()
            total_cash   = float(acct.cash)
            total_equity = float(acct.equity)
            budgets      = shared_state.get_budgets(total_cash)
            error_count  = 0

            if cycle % 10 == 0:
                log(f"📡 Sweep #{cycle} | Equity: ${total_equity} | Cash: ${total_cash} | "
                    f"Sector: {portfolio['sector_health']} | "
                    f"{'⏸️' if bot_state['paused'] else '▶️'} | "
                    f"{'🚫 BUYS OFF' if bot_state.get('buys_disabled') else '✅ BUYING' if is_market_hours_for_buying(now) else '🛑 CUTOFF'}")
                save_berserker_peaks()
                nexus_client.crypto_control({
                    "paused":        bot_state["paused"],
                    "paused_crypto": bot_state.get("paused_crypto",  False),
                    "buys_disabled": bot_state.get("buys_disabled", False),
                })

            # ✅ V10.0: morning brief -- once per day at 8:00 AM
            # V10.3: set flag AFTER reset_daily_state so reset doesn't clear it
            if (now.weekday() < 5 and now.hour == 8 and now.minute < 5
                    and not _morning_brief_sent
                    and time.time() - _service_start_time > 60):
                reset_daily_state(total_equity)
                _morning_brief_sent = True
                send_morning_brief(total_equity)

            check_daily_loss(total_equity)
            check_eod(now)
            maybe_send_daily_report(total_equity)

            raw_positions = trading_client.get_all_positions()
            positions     = {
                p.symbol.replace("/USD","").replace("USD",""): p
                for p in raw_positions
            }

            for sym in list(pending_sells):
                if sym not in positions:
                    pending_sells.discard(sym)

            for sym, pos in positions.items():
                if sym in SYMBOLS and shared_state.owner(sym) is None:
                    shared_state.claim(sym, BOT_NAME)
                    if sym not in position_entry_times:
                        position_entry_times[sym] = datetime.now(tz=CENTRAL)
                    if sym not in portfolio["peak_prices"]:
                        entry = float(pos.avg_entry_price)
                        try:
                            current = float(pos.current_price) if pos.current_price else entry
                        except:
                            current = entry
                        portfolio["peak_prices"][sym] = max(entry, current)

            # V10.9 fix: populate SPY/QQQ price history and update regime/momentum
            # gates each sweep. Previously _update_spy_qqq_history() and
            # get_spy_regime_and_momentum() were defined but never called, so
            # _spy_history/_qqq_history stayed empty and _spy_regime/_spy_momentum_ok
            # never changed from their boot defaults (BULL / True).
            _update_spy_qqq_history()
            _spy_regime, _spy_momentum_ok, _, _ = get_spy_regime_and_momentum()

            # V10.19: Check earnings calendar once per hour
            if is_market_hours_for_buying(now):
                check_earnings_calendar()

            # V10.19: Compute regime score every sweep (cheap — all in-memory)
            compute_regime_score()

            update_sector_health()
            sorted_syms = get_sorted_symbols(positions)

            # V10.16: Batch price fetch -- all SYMBOLS in one API call instead of
            # one call per symbol (was 15+ individual calls per sweep = rate limit risk).
            # StockLatestTradeRequest accepts a list of symbols; result is a dict.
            try:
                batch_trades = stock_data_client.get_stock_latest_trade(
                    StockLatestTradeRequest(symbol_or_symbols=SYMBOLS, feed=DataFeed.IEX)
                )
            except Exception as e:
                log(f"⚠️ Batch price fetch failed: {e}")
                batch_trades = {}

            for symbol in SYMBOLS:
                if now.weekday() >= 5 or now.hour < 8 or now.hour > 15:
                    continue
                try:
                    price = float(batch_trades[symbol].price)
                except Exception as e:
                    log(f"⚠️ Price missing [{symbol}]: {e}")
                    continue

                price_history[symbol].append(price)

                if symbol in positions:
                    manage_exits(symbol, price, positions[symbol])

            # V10.2: Circuit breaker -- skip entries if triggered
            if check_circuit_breaker():
                if cycle % 4 == 0:  # log every 2 min
                    resume_t = datetime.fromtimestamp(_circuit_break_until, tz=CENTRAL).strftime("%I:%M %p")
                    log(f"[CB] Circuit breaker active -- no entries until {resume_t}")
                time.sleep(30)
                continue

            # V10.2: SPY regime -- bear mode limits positions
            _max_pos_now = SPY_BEAR_MAX_POS if _spy_regime == "BEAR" else MAX_POSITIONS

            # V10.19: VIX gate -- override MAX_POSITIONS when VIX extreme
            vix_st = get_vix_status()
            if vix_st["extreme"]:
                _max_pos_now = min(_max_pos_now, 1)   # extreme VIX = max 1 position
            _vix_blocking = vix_st["blocking"]         # True if VIX > 25 (no new entries)

            # Log SPY + VIX filter status once per minute (every 2 cycles)
            if cycle % 2 == 0 and is_market_hours_for_buying(now):
                vix_note = f" | VIX={vix_st['level']} {vix_st['emoji']}"
                regime_note = f" | REGIME={_regime_score}/5"
                if _vix_blocking:
                    log(f"🚫 VIX FILTER: VIX={vix_st['level']} > {VIX_BLOCK_THRESHOLD} — blocking entries{regime_note}")
                elif not _spy_momentum_ok:
                    log(f"🚫 SPY FILTER: momentum blocked | regime={_spy_regime}{vix_note}{regime_note}")
                elif _spy_regime == "BEAR":
                    log(f"⚠️ BEAR REGIME: max positions reduced to {SPY_BEAR_MAX_POS}{vix_note}{regime_note}")
                elif _regime_score >= 2:
                    log(f"⚠️ REGIME {_regime_score}/5: position scaling active{vix_note}")

            if (is_market_hours_for_buying(now)
                    and not bot_state["paused"]
                    and not bot_state.get("paused_berserker")
                    and not bot_state.get("buys_disabled")
                    and not bot_state.get("daily_loss_hit")
                    and len(positions) < _max_pos_now
                    and _spy_momentum_ok               # V10.2: block if SPY falling hard
                    and not _vix_blocking              # V10.19: block if VIX > 25
                    and regime_allows_entry()):        # V10.19: block if regime score >= 4

                for symbol in sorted_syms:
                    if len(positions) >= _max_pos_now:
                        break
                    if symbol not in price_history or not price_history[symbol]:
                        continue
                    price   = price_history[symbol][-1]
                    signals = get_signals(symbol, price)
                    if signals.get("buy"):
                        log(f"📊 Signal [{symbol}] RSI={signals['rsi']} "
                            f"MACD={'✅' if signals['macd_bull'] else '[X]'} "
                            f"priority={get_symbol_priority(symbol)}")
                        success = execute_trade(symbol, "BUY", total_cash)
                        if success:
                            break

            time.sleep(30)

        except Exception as e:
            error_count += 1
            log(f"🔴 Loop error #{error_count}: {e}")
            if error_count >= 3:
                alert(f"🚨 NEXUS CRITICAL: {error_count} errors!\n{e}", critical=True)
            time.sleep(10)
