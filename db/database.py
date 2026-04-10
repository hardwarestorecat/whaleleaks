"""
SQLite persistence layer for whale fill tracking and address win rates.

Schema
──────
addresses       — per-address lifetime stats (win rate, P&L)
fills           — every individual whale fill
market_outcomes — resolved market cache (avoids redundant API calls)
"""
from __future__ import annotations
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

log = logging.getLogger("database")

DB_PATH = Path(os.getenv("DB_PATH", "whaleleaks.db"))
MIN_BET_USD = 500.0   # only count bets >= this for win-rate / leaderboard

_local = threading.local()


def _conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn"):
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return _local.conn


@contextmanager
def tx():
    conn = _conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init() -> None:
    with tx() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS addresses (
            address         TEXT PRIMARY KEY,
            total_fills     INTEGER NOT NULL DEFAULT 0,
            resolved_fills  INTEGER NOT NULL DEFAULT 0,
            wins            INTEGER NOT NULL DEFAULT 0,
            total_pnl_usd   REAL    NOT NULL DEFAULT 0.0,
            first_seen      TEXT,
            last_seen       TEXT
        );

        CREATE TABLE IF NOT EXISTS fills (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash       TEXT,
            block_number  INTEGER,
            address       TEXT    NOT NULL,
            role          TEXT    NOT NULL,   -- "maker" | "taker"
            condition_id  TEXT    NOT NULL,
            market_title  TEXT    NOT NULL,
            side          TEXT    NOT NULL,   -- "yes" | "no"
            trade_side    TEXT    NOT NULL DEFAULT 'BUY',  -- "BUY" | "SELL"
            price_cents   INTEGER NOT NULL,
            quantity      REAL    NOT NULL,
            usd_value     REAL    NOT NULL,
            ts            TEXT    NOT NULL,
            outcome       TEXT,               -- "win" | "loss" | "push" | "realized" | NULL
            pnl_usd       REAL
        );

        CREATE INDEX IF NOT EXISTS idx_fills_address      ON fills(address);
        CREATE INDEX IF NOT EXISTS idx_fills_addr_ts     ON fills(address, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_fills_condition    ON fills(condition_id);

        CREATE TABLE IF NOT EXISTS market_outcomes (
            condition_id    TEXT PRIMARY KEY,
            winning_side    TEXT,             -- "yes" | "no" | "invalid" | NULL (unresolved)
            resolved_at     TEXT,
            checked_at      TEXT NOT NULL
        );

        -- Tracks open positions per address+market for exit P&L calculation
        CREATE TABLE IF NOT EXISTS positions (
            address      TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_side   TEXT NOT NULL,   -- "yes" | "no"
            quantity     REAL NOT NULL DEFAULT 0,
            avg_cost     REAL NOT NULL DEFAULT 0,   -- avg cost per share (0-1 USDC)
            total_cost   REAL NOT NULL DEFAULT 0,   -- total USDC paid
            PRIMARY KEY (address, condition_id, token_side)
        );
        """)

    # Add unique index on tx_hash — deduplicate existing rows first
    conn = _conn()
    idx_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_fills_tx'"
    ).fetchone()
    if not idx_exists:
        # Keep only the lowest-id row for each non-empty tx_hash
        conn.execute("""
            DELETE FROM fills
            WHERE tx_hash != '' AND tx_hash IS NOT NULL
              AND id NOT IN (
                SELECT MIN(id) FROM fills
                WHERE tx_hash != '' AND tx_hash IS NOT NULL
                GROUP BY tx_hash
              )
        """)
        conn.commit()
        conn.execute(
            "CREATE UNIQUE INDEX idx_fills_tx ON fills(tx_hash) WHERE tx_hash != ''"
        )
        conn.commit()

    # Migrate: add trade_side column if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(fills)").fetchall()}
    if "trade_side" not in cols:
        conn.execute("ALTER TABLE fills ADD COLUMN trade_side TEXT NOT NULL DEFAULT 'BUY'")
        conn.commit()
        log.info("Migrated: added trade_side column to fills")

    # Create positions table if missing (may have failed before trade_side migration)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            address      TEXT NOT NULL,
            condition_id TEXT NOT NULL,
            token_side   TEXT NOT NULL,
            quantity     REAL NOT NULL DEFAULT 0,
            avg_cost     REAL NOT NULL DEFAULT 0,
            total_cost   REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (address, condition_id, token_side)
        )
    """)
    conn.commit()

    # Create partial index on trade_side (safe now that column exists)
    idx = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_fills_unresolved'"
    ).fetchone()
    if not idx:
        conn.execute(
            "CREATE INDEX idx_fills_unresolved ON fills(condition_id) WHERE outcome IS NULL AND trade_side = 'BUY'"
        )
        conn.commit()
        log.info("Created idx_fills_unresolved index")


# ─── Position tracking ────────────────────────────────────────────────────────

def upsert_position(address: str, condition_id: str, token_side: str,
                    quantity: float, usd_value: float) -> None:
    """Record a BUY — update average cost basis for the position."""
    with tx() as conn:
        existing = conn.execute(
            "SELECT quantity, total_cost FROM positions WHERE address=? AND condition_id=? AND token_side=?",
            (address, condition_id, token_side),
        ).fetchone()
        if existing:
            new_qty  = existing["quantity"] + quantity
            new_cost = existing["total_cost"] + usd_value
            conn.execute(
                """UPDATE positions SET quantity=?, avg_cost=?, total_cost=?
                   WHERE address=? AND condition_id=? AND token_side=?""",
                (new_qty, new_cost / new_qty if new_qty else 0, new_cost,
                 address, condition_id, token_side),
            )
        else:
            conn.execute(
                """INSERT INTO positions (address, condition_id, token_side, quantity, avg_cost, total_cost)
                   VALUES (?,?,?,?,?,?)""",
                (address, condition_id, token_side, quantity,
                 usd_value / quantity if quantity else 0, usd_value),
            )


def realize_sell(address: str, condition_id: str, token_side: str,
                 quantity: float, sell_price: float) -> tuple[float, float] | None:
    """
    Calculate realized P&L for a SELL and reduce the open position.
    Returns (realized_pnl, cost_basis) or None if no position on record.
    sell_price is in USDC per share (0.0–1.0).
    """
    conn = _conn()
    pos = conn.execute(
        "SELECT quantity, avg_cost, total_cost FROM positions WHERE address=? AND condition_id=? AND token_side=?",
        (address, condition_id, token_side),
    ).fetchone()
    if not pos or pos["quantity"] <= 0:
        return None

    sell_qty      = min(quantity, pos["quantity"])  # can't sell more than held
    cost_basis    = pos["avg_cost"] * sell_qty
    sell_proceeds = sell_price * sell_qty
    realized_pnl  = sell_proceeds - cost_basis

    with tx() as conn:
        new_qty = pos["quantity"] - sell_qty
        if new_qty <= 0.001:
            conn.execute(
                "DELETE FROM positions WHERE address=? AND condition_id=? AND token_side=?",
                (address, condition_id, token_side),
            )
        else:
            conn.execute(
                """UPDATE positions SET quantity=?, total_cost=?
                   WHERE address=? AND condition_id=? AND token_side=?""",
                (new_qty, pos["avg_cost"] * new_qty, address, condition_id, token_side),
            )

    return realized_pnl, cost_basis


def credit_realized_pnl(address: str, pnl: float, ts: str) -> None:
    """Update address stats for a realized exit (win or loss)."""
    with tx() as conn:
        conn.execute(
            """INSERT INTO addresses (address, total_fills, resolved_fills, wins, total_pnl_usd, first_seen, last_seen)
               VALUES (?, 0, 1, ?, ?, ?, ?)
               ON CONFLICT(address) DO UPDATE SET
                 resolved_fills = resolved_fills + 1,
                 wins           = wins + ?,
                 total_pnl_usd  = total_pnl_usd + ?,
                 last_seen      = excluded.last_seen""",
            (address, 1 if pnl > 0 else 0, pnl, ts, ts,
             1 if pnl > 0 else 0, pnl),
        )


# ─── Fill writes ─────────────────────────────────────────────────────────────

def save_fill(
    *,
    tx_hash: str,
    block_number: int,
    address: str,
    role: str,
    condition_id: str,
    market_title: str,
    side: str,
    trade_side: str = "BUY",
    price_cents: int,
    quantity: float,
    usd_value: float,
    ts: str,
    outcome: str | None = None,
    pnl_usd: float | None = None,
) -> int:
    with tx() as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO fills
               (tx_hash, block_number, address, role, condition_id, market_title,
                side, trade_side, price_cents, quantity, usd_value, ts, outcome, pnl_usd)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (tx_hash, block_number, address, role, condition_id, market_title,
             side, trade_side, price_cents, quantity, usd_value, ts, outcome, pnl_usd),
        )
        if cur.rowcount == 0:
            return 0  # duplicate — nothing inserted
        fill_id = cur.lastrowid

        conn.execute(
            """INSERT INTO addresses (address, total_fills, first_seen, last_seen)
               VALUES (?, 1, ?, ?)
               ON CONFLICT(address) DO UPDATE SET
                 total_fills = total_fills + 1,
                 last_seen   = excluded.last_seen""",
            (address, ts, ts),
        )
    return fill_id


# ─── Outcome resolution ───────────────────────────────────────────────────────

def get_unresolved_condition_ids() -> list[str]:
    conn = _conn()
    # Only check condition_ids that have open BUY fills (SELLs are resolved immediately)
    rows = conn.execute(
        "SELECT DISTINCT condition_id FROM fills WHERE outcome IS NULL AND trade_side = 'BUY'"
    ).fetchall()
    return [r["condition_id"] for r in rows]


def cache_outcome(condition_id: str, winning_side: str | None, checked_at: str) -> None:
    with tx() as conn:
        conn.execute(
            """INSERT INTO market_outcomes (condition_id, winning_side, checked_at)
               VALUES (?, ?, ?)
               ON CONFLICT(condition_id) DO UPDATE SET
                 winning_side = excluded.winning_side,
                 checked_at   = excluded.checked_at""",
            (condition_id, winning_side, checked_at),
        )


def get_cached_outcome(condition_id: str) -> dict | None:
    row = _conn().execute(
        "SELECT * FROM market_outcomes WHERE condition_id = ?", (condition_id,)
    ).fetchone()
    return dict(row) if row else None


def has_position(address: str, condition_id: str, token_side: str) -> bool:
    """Return True if we have an open position for this address+market+side."""
    row = _conn().execute(
        "SELECT 1 FROM positions WHERE address=? AND condition_id=? AND token_side=? AND quantity > 0",
        (address, condition_id, token_side),
    ).fetchone()
    return row is not None


def apply_outcome(condition_id: str, winning_side: str, resolved_at: str) -> list[dict]:
    """
    Mark all open BUY fills for a condition as win/loss, update address stats.
    Returns list of {address, pnl} for each winning fill so the caller can
    update the Redis graduation scores.
    """
    conn = _conn()
    fills = conn.execute(
        "SELECT * FROM fills WHERE condition_id = ? AND outcome IS NULL AND trade_side = 'BUY'",
        (condition_id,),
    ).fetchall()

    if not fills:
        return []

    wins_to_record = []
    with tx() as conn:
        conn.execute(
            "UPDATE market_outcomes SET winning_side=?, resolved_at=? WHERE condition_id=?",
            (winning_side, resolved_at, condition_id),
        )
        for f in fills:
            if winning_side == "invalid":
                outcome = "push"
                pnl = 0.0
            elif f["side"] == winning_side:
                outcome = "win"
                pnl = (1.0 - f["price_cents"] / 100) * f["quantity"]
            else:
                outcome = "loss"
                pnl = -(f["price_cents"] / 100) * f["quantity"]

            conn.execute(
                "UPDATE fills SET outcome=?, pnl_usd=? WHERE id=?",
                (outcome, pnl, f["id"]),
            )
            conn.execute(
                """UPDATE addresses SET
                     resolved_fills = resolved_fills + 1,
                     wins           = wins + ?,
                     total_pnl_usd  = total_pnl_usd + ?
                   WHERE address = ?""",
                (1 if outcome == "win" else 0, pnl, f["address"]),
            )
            if outcome == "win" and pnl > 0:
                wins_to_record.append({"address": f["address"], "pnl": pnl})
    return wins_to_record


# ─── Win-rate lookup ──────────────────────────────────────────────────────────

def get_address_stats(address: str) -> dict | None:
    row = _conn().execute(
        """SELECT
               f.address,
               a.first_seen, a.last_seen,
               COUNT(*)                                                                      AS total_fills,
               SUM(CASE WHEN f.usd_value >= ? AND f.outcome NOT IN ('push', '') AND f.outcome IS NOT NULL THEN 1 ELSE 0 END) AS resolved_fills,
               SUM(CASE WHEN f.usd_value >= ? AND f.outcome = 'win'             THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN f.usd_value >= ? AND f.pnl_usd IS NOT NULL         THEN f.pnl_usd ELSE 0 END) AS total_pnl_usd
           FROM fills f
           JOIN addresses a USING (address)
           WHERE f.address = ?
           GROUP BY f.address""",
        (MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, address),
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["win_rate"] = (d["wins"] / d["resolved_fills"]) if d["resolved_fills"] else None
    return d


def get_top_winners() -> list[dict]:
    rows = _conn().execute(
        """SELECT
               address,
               COUNT(*)                                                                                   AS total_fills,
               SUM(CASE WHEN usd_value >= ? AND outcome NOT IN ('push','') AND outcome IS NOT NULL THEN 1 ELSE 0 END) AS resolved_fills,
               SUM(CASE WHEN usd_value >= ? AND outcome = 'win'            THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN usd_value >= ? AND pnl_usd IS NOT NULL        THEN pnl_usd ELSE 0 END) AS total_pnl_usd,
               CAST(SUM(CASE WHEN usd_value >= ? AND outcome = 'win'            THEN 1 ELSE 0 END) AS REAL) /
               NULLIF(SUM(CASE WHEN usd_value >= ? AND outcome NOT IN ('push','') AND outcome IS NOT NULL THEN 1 ELSE 0 END), 0) AS win_rate
           FROM fills
           GROUP BY address
           HAVING resolved_fills >= 3 AND total_pnl_usd >= 1000
           ORDER BY win_rate DESC, total_pnl_usd DESC""",
        (MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD),
    ).fetchall()
    return [dict(r) for r in rows]


def get_top_earners() -> list[dict]:
    """Fallback leaderboard: all tracked addresses with ≥$1k total earnings."""
    rows = _conn().execute(
        """SELECT
               address,
               COUNT(*)                                                                                   AS total_fills,
               SUM(CASE WHEN usd_value >= ? AND outcome NOT IN ('push','') AND outcome IS NOT NULL THEN 1 ELSE 0 END) AS resolved_fills,
               SUM(CASE WHEN usd_value >= ? AND outcome = 'win'            THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN usd_value >= ? AND pnl_usd IS NOT NULL        THEN pnl_usd ELSE 0 END) AS total_pnl_usd,
               CAST(SUM(CASE WHEN usd_value >= ? AND outcome = 'win'            THEN 1 ELSE 0 END) AS REAL) /
               NULLIF(SUM(CASE WHEN usd_value >= ? AND outcome NOT IN ('push','') AND outcome IS NOT NULL THEN 1 ELSE 0 END), 0) AS win_rate
           FROM fills
           GROUP BY address
           HAVING total_pnl_usd >= 1000
           ORDER BY total_pnl_usd DESC""",
        (MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD),
    ).fetchall()
    return [dict(r) for r in rows]


def get_top_spenders(limit: int = 20) -> list[dict]:
    """Top addresses by total USD spent."""
    rows = _conn().execute(
        """SELECT address,
                  COUNT(*)            AS total_fills,
                  SUM(usd_value)      AS total_spent,
                  SUM(quantity - usd_value) AS potential_winnings,
                  MAX(ts)             AS last_bet_ts,
                  SUM(CASE WHEN pnl_usd IS NOT NULL THEN pnl_usd ELSE 0 END) AS total_pnl_usd,
                  SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS wins,
                  SUM(CASE WHEN outcome IS NOT NULL AND outcome NOT IN ('push','') THEN 1 ELSE 0 END) AS resolved_fills
           FROM fills
           GROUP BY address
           HAVING total_spent >= 5000
           ORDER BY total_spent DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["win_rate"] = (d["wins"] / d["resolved_fills"]) if d["resolved_fills"] else None
        result.append(d)
    return result


def get_address_fills(address: str, limit: int = 200) -> list[dict]:
    """Return all fills for an address, newest first."""
    rows = _conn().execute(
        """SELECT * FROM fills WHERE address = ? ORDER BY ts DESC LIMIT ?""",
        (address, limit),
    ).fetchall()
    return [dict(r) for r in rows]
