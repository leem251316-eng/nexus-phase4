"""
capital_coordinator.py V1.0 -- Cross-service Alpaca buying-power coordination
================================================================================
Jun 30 2026. Berserker/Scanner (main.py, nexus-commander/rare-perception) and
Phase4 (phase4.py, nexus-phase4/glorious-achievement) are SEPARATE Railway
services in separate processes -- but they trade against the SAME live Alpaca
account (confirmed: one account was created, paper trading was layered on top
of it, never a second account). Each service was independently calling Alpaca's
get_account().buying_power and sizing trades off it with zero knowledge of the
other. shared_state.py's in-process claim()/release()/owner() registry cannot
help here -- it's a Python module shared between main.py and scanner.py because
they run in the SAME container/process; Phase4 is a different process entirely
and module-level state doesn't cross process or service boundaries.

This module coordinates through Postgres instead, since both services already
share DATABASE_URL. The model is intentionally narrow in scope:

  Reservations do NOT track "money currently deployed in open positions" --
  Alpaca's own buying_power already reflects that correctly once an order
  settles. Reservations ONLY exist to close the race-condition window between
  "service A decided to buy and is about to submit the order" and "Alpaca's
  buying_power reflects A's new position on service B's next read" -- a window
  of a few seconds. So:
    - TTL is short (default 90s -- generous for order submit + fill confirm).
    - On success: release immediately (Alpaca buying_power is now authoritative
      again on the next read).
    - On failure/exception: release immediately.
    - Stale pending rows older than TTL are auto-swept on every read, so a
      crashed service can never permanently lock out capital.

Usage (both services, identical pattern):

    from capital_coordinator import CapitalCoordinator
    coord = CapitalCoordinator(DATABASE_URL, service_name="berserker")
    coord.init_table()

    # Before sizing a trade:
    available = coord.get_available(account_buying_power)
    amount = round(available * TRADE_FRACTION, 2)
    if amount < MIN_TRADE_AMT:
        return  # not enough after accounting for the other service

    # Reserve BEFORE submitting the order:
    res_id = coord.reserve(amount, symbol=symbol)
    if res_id is None:
        return  # DB unavailable -- coordinator fails open, proceed without it

    try:
        success = place_order(symbol, "BUY", amount)
    finally:
        coord.release(res_id)  # always release -- success or failure

Fails open: if DATABASE_URL is unset or Postgres is unreachable, every method
becomes a no-op that lets the caller proceed exactly as it did before this
module existed (reserve() returns a sentinel, release() is a no-op, get_available()
falls back to the raw buying_power passed in). A coordination outage should
never become a trading outage.
"""

import os
import time
import threading
from datetime import datetime, timezone

try:
    import psycopg2
    import psycopg2.extras
    _psycopg2_ok = True
except ImportError:
    _psycopg2_ok = False

# Reservations older than this are considered abandoned (crashed service,
# order that hung, etc.) and are swept on every read so they can never
# permanently lock out capital.
RESERVATION_TTL_SECS = 90

# Hard cap on how long a connection attempt can hang before giving up and
# falling open. Without this, an unreachable DB can stall the calling trade
# loop for the platform's default TCP timeout (often 30-60+ seconds).
CONNECT_TIMEOUT_SECS = 3

# Sentinel returned by reserve() when coordination is unavailable (no DB,
# connection failure, etc.) -- callers should treat this as "proceed without
# coordination" rather than "reservation failed, abort the trade."
NO_COORDINATION = "uncoordinated"


class CapitalCoordinator:
    """
    One instance per service (e.g. one in main.py tagged 'berserker', one in
    phase4.py tagged 'phase4'). Each instance manages its own DB connection;
    they coordinate purely through the shared capital_reservations table.
    """

    def __init__(self, db_url: str, service_name: str):
        self.db_url       = db_url
        self.service_name = service_name
        self._conn        = None
        self._lock        = threading.Lock()
        self._enabled      = bool(db_url) and _psycopg2_ok
        self._warned_once  = False

    def _get_conn(self):
        if not self._enabled:
            return None
        try:
            if self._conn is None or self._conn.closed:
                # connect_timeout is critical here: without it, a connection
                # attempt to an unreachable host can hang for the platform's
                # default TCP timeout (often 30-60+ seconds), which would
                # block the calling trade loop for that entire duration --
                # exactly the outcome "fails open" is supposed to prevent.
                self._conn = psycopg2.connect(self.db_url, connect_timeout=CONNECT_TIMEOUT_SECS)
                self._conn.autocommit = False
            else:
                # If a prior query left the connection in a failed transaction
                # state, roll it back before reuse so the next query doesn't
                # immediately die with "current transaction is aborted."
                try:
                    self._conn.rollback()
                except Exception:
                    self._conn = psycopg2.connect(self.db_url, connect_timeout=CONNECT_TIMEOUT_SECS)
                    self._conn.autocommit = False
            return self._conn
        except Exception as e:
            if not self._warned_once:
                print(f"[CAPCOORD:{self.service_name}] DB connect error "
                      f"(coordination disabled, trading proceeds uncoordinated): {e}",
                      flush=True)
                self._warned_once = True
            return None

    def init_table(self):
        """Create the shared table if it doesn't exist. Safe to call from
        every service at boot -- IF NOT EXISTS makes it idempotent."""
        if not self._enabled:
            print(f"[CAPCOORD:{self.service_name}] disabled (no DATABASE_URL "
                  f"or psycopg2) -- trading proceeds uncoordinated", flush=True)
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS capital_reservations (
            id          SERIAL PRIMARY KEY,
            service     VARCHAR(20) NOT NULL,
            symbol      VARCHAR(10),
            amount      REAL NOT NULL,
            status      VARCHAR(10) NOT NULL DEFAULT 'pending',
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_capres_status ON capital_reservations(status);
        CREATE INDEX IF NOT EXISTS idx_capres_created ON capital_reservations(created_at);
        """
        try:
            with self._lock:
                conn = self._get_conn()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute(ddl)
                    conn.commit()
                    print(f"[CAPCOORD:{self.service_name}] capital_reservations "
                          f"table ready", flush=True)
        except Exception as e:
            print(f"[CAPCOORD:{self.service_name}] init_table error: {e}", flush=True)

    def _sweep_stale(self, cur):
        """Release any 'pending' reservation older than TTL. Called inline
        inside an existing transaction (caller commits)."""
        cur.execute("""
            UPDATE capital_reservations
            SET status = 'released'
            WHERE status = 'pending'
              AND created_at < NOW() - INTERVAL '%s seconds'
        """, (RESERVATION_TTL_SECS,))

    def get_available(self, account_buying_power: float) -> float:
        """
        Returns buying power minus all outstanding reservations from EVERY
        service (including this one's own pending reservations, if any).
        Falls back to the raw account_buying_power unchanged if coordination
        is unavailable -- never blocks trading on a DB outage.

        Note: there is a narrow theoretical race between this call and the
        subsequent reserve() call -- another service could reserve in that
        gap, so the actual reservation could still push the account over
        budget by a small amount. This is intentionally not solved with a
        single atomic check-and-reserve operation; the gap is one Python
        statement wide and the cost of closing it (e.g. SELECT...FOR UPDATE
        across a multi-second window) isn't worth the complexity for a
        same-account-different-process safety net rather than a hard limit.
        """
        if not self._enabled:
            return account_buying_power
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return account_buying_power
                with conn.cursor() as cur:
                    self._sweep_stale(cur)
                    cur.execute("""
                        SELECT COALESCE(SUM(amount), 0) FROM capital_reservations
                        WHERE status = 'pending'
                    """)
                    reserved = float(cur.fetchone()[0])
                conn.commit()
            available = round(account_buying_power - reserved, 2)
            return max(0.0, available)
        except Exception as e:
            print(f"[CAPCOORD:{self.service_name}] get_available error "
                  f"(falling back to raw buying_power): {e}", flush=True)
            return account_buying_power

    def reserve(self, amount: float, symbol: str = "") -> "int | str | None":
        """
        Reserve `amount` dollars against the shared account BEFORE submitting
        an order. Returns:
          - an int reservation id on success (pass to release() when done)
          - NO_COORDINATION ("uncoordinated") if the DB is unavailable --
            caller should proceed with the trade anyway, just without the
            cross-service safety net
          - None only if amount is invalid (<=0) -- caller should NOT trade
        """
        if amount <= 0:
            return None
        if not self._enabled:
            return NO_COORDINATION
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return NO_COORDINATION
                with conn.cursor() as cur:
                    self._sweep_stale(cur)
                    cur.execute("""
                        INSERT INTO capital_reservations (service, symbol, amount, status)
                        VALUES (%s, %s, %s, 'pending')
                        RETURNING id
                    """, (self.service_name, symbol, round(amount, 2)))
                    res_id = cur.fetchone()[0]
                conn.commit()
            return res_id
        except Exception as e:
            print(f"[CAPCOORD:{self.service_name}] reserve error "
                  f"(proceeding uncoordinated): {e}", flush=True)
            return NO_COORDINATION

    def release(self, reservation_id) -> None:
        """
        Release a reservation. ALWAYS call this after the order attempt
        completes, whether it succeeded or failed -- typically from a
        try/finally around the order submission. No-op if reservation_id is
        None or NO_COORDINATION (nothing was actually reserved).
        """
        if reservation_id is None or reservation_id == NO_COORDINATION:
            return
        if not self._enabled:
            return
        try:
            with self._lock:
                conn = self._get_conn()
                if not conn:
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE capital_reservations
                        SET status = 'released'
                        WHERE id = %s
                    """, (reservation_id,))
                conn.commit()
        except Exception as e:
            print(f"[CAPCOORD:{self.service_name}] release error "
                  f"(reservation will auto-expire via TTL in {RESERVATION_TTL_SECS}s): {e}",
                  flush=True)
