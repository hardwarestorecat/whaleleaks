"""
SQLite persistence layer for whale fill tracking and address win rates.

Schema
──────
addresses       — per-address lifetime stats (win rate, P&L)
fills           — every individual whale fill
market_outcomes — resolved market cache (avoids redundant API calls)
"""
from __future__ import annotations
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

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
            price_cents   INTEGER NOT NULL,
            quantity      REAL    NOT NULL,
            usd_value     REAL    NOT NULL,
            ts            TEXT    NOT NULL,
            outcome       TEXT,               -- "win" | "loss" | "push" | NULL
            pnl_usd       REAL
        );

        CREATE INDEX IF NOT EXISTS idx_fills_address      ON fills(address);
        CREATE INDEX IF NOT EXISTS idx_fills_condition    ON fills(condition_id);
        CREATE INDEX IF NOT EXISTS idx_fills_unresolved   ON fills(condition_id) WHERE outcome IS NULL;

        CREATE TABLE IF NOT EXISTS market_outcomes (
            condition_id    TEXT PRIMARY KEY,
            winning_side    TEXT,             -- "yes" | "no" | "invalid" | NULL (unresolved)
            resolved_at     TEXT,
            checked_at      TEXT NOT NULL
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
    price_cents: int,
    quantity: float,
    usd_value: float,
    ts: str,
) -> int:
    with tx() as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO fills
               (tx_hash, block_number, address, role, condition_id, market_title,
                side, price_cents, quantity, usd_value, ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (tx_hash, block_number, address, role, condition_id, market_title,
             side, price_cents, quantity, usd_value, ts),
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
    rows = conn.execute(
        "SELECT DISTINCT condition_id FROM fills WHERE outcome IS NULL"
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


def apply_outcome(condition_id: str, winning_side: str, resolved_at: str) -> None:
    """Mark all fills for a condition as win/loss and update address stats."""
    conn = _conn()
    fills = conn.execute(
        "SELECT * FROM fills WHERE condition_id = ? AND outcome IS NULL",
        (condition_id,),
    ).fetchall()

    if not fills:
        return

    with tx() as conn:
        conn.execute(
            "UPDATE market_outcomes SET winning_side=?, resolved_at=? WHERE condition_id=?",
            (winning_side, resolved_at, condition_id),
        )
        for f in fills:
            if winning_side == "invalid":
                outcome = "push"
                # Return cost basis
                pnl = 0.0
            elif f["side"] == winning_side:
                outcome = "win"
                # Profit = (1.00 - price) × qty
                pnl = (1.0 - f["price_cents"] / 100) * f["quantity"]
            else:
                outcome = "loss"
                # Loss = cost paid = price × qty
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


def get_top_winners(limit: int = 25) -> list[dict]:
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
           HAVING resolved_fills >= 3
           ORDER BY win_rate DESC, total_pnl_usd DESC
           LIMIT ?""",
        (MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_top_earners(limit: int = 25) -> list[dict]:
    """Fallback leaderboard: all tracked addresses ranked by cumulative P&L."""
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
           HAVING total_fills >= 1
           ORDER BY total_pnl_usd DESC
           LIMIT ?""",
        (MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, MIN_BET_USD, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_address_fills(address: str, limit: int = 200) -> list[dict]:
    """Return all fills for an address, newest first."""
    rows = _conn().execute(
        """SELECT * FROM fills WHERE address = ? ORDER BY ts DESC LIMIT ?""",
        (address, limit),
    ).fetchall()
    return [dict(r) for r in rows]
