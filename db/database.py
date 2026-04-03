"""
SQLite persistence layer for whale fill tracking and address win rates.

Schema
──────
addresses       — per-address lifetime stats (win rate, P&L)
fills           — every individual whale fill
market_outcomes — resolved market cache (avoids redundant API calls)
"""
from __future__ import annotations
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path("whaleleaks.db")

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
            """INSERT INTO fills
               (tx_hash, block_number, address, role, condition_id, market_title,
                side, price_cents, quantity, usd_value, ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (tx_hash, block_number, address, role, condition_id, market_title,
             side, price_cents, quantity, usd_value, ts),
        )
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
        "SELECT * FROM addresses WHERE address = ?", (address,)
    ).fetchone()
    if not row:
        return None
    d = dict(row)
    if d["resolved_fills"] > 0:
        d["win_rate"] = d["wins"] / d["resolved_fills"]
    else:
        d["win_rate"] = None
    return d


def get_top_winners(limit: int = 10) -> list[dict]:
    rows = _conn().execute(
        """SELECT *, CAST(wins AS REAL) / resolved_fills AS win_rate
           FROM addresses
           WHERE resolved_fills >= 3
           ORDER BY win_rate DESC, total_pnl_usd DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
