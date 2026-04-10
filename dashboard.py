"""
Whaleleaks web dashboard — FastAPI.

Routes:
  GET /              → dashboard HTML
  GET /api/stats     → uptime, markets monitored, alert counts
  GET /api/alerts    → recent whale alerts from fills table
  GET /api/whales    → top addresses by win rate
  GET /api/stream    → SSE stream for live alert pushes
"""
from __future__ import annotations
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

import config
from db import database as db

app = FastAPI(title="whaleleaks")

_start_time = time.time()
_markets_count: int = 0
_subscribers: list[asyncio.Queue] = []


# ─── SSE broadcast (called by listener on each whale alert) ──────────────────

def _push(event_type: str, data: dict) -> None:
    msg = {"event_type": event_type, **data}
    for q in _subscribers:
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


def broadcast(alert_dict: dict) -> None:
    _push("whale", alert_dict)


def broadcast_flow(flow_dict: dict) -> None:
    _push("flow", flow_dict)


def set_markets_count(n: int) -> None:
    global _markets_count
    _markets_count = n


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse((Path(__file__).parent / "templates" / "index.html").read_text(encoding="utf-8"))


@app.get("/methodology", response_class=HTMLResponse)
async def methodology():
    return HTMLResponse((Path(__file__).parent / "templates" / "methodology.html").read_text(encoding="utf-8"))


@app.get("/api/stats")
async def stats():
    conn = db._conn()
    # Whale alerts today: must meet both spend AND profit thresholds
    # profit = quantity - usd_value >= WHALE_MIN_PROFIT_USD
    alert_count_today = conn.execute(
        """SELECT COUNT(*) FROM fills
           WHERE ts >= date('now')
             AND usd_value >= ?
             AND (quantity - usd_value) >= ?""",
        (config.WHALE_THRESHOLD_USD, config.WHALE_MIN_PROFIT_USD),
    ).fetchone()[0]
    # Addresses with at least $1k cumulative profit (leaderboard-eligible)
    tracked_whales = conn.execute(
        """SELECT COUNT(*) FROM (
               SELECT address,
                      SUM(CASE WHEN pnl_usd IS NOT NULL THEN pnl_usd ELSE 0 END) AS total_pnl
               FROM fills WHERE usd_value >= ?
               GROUP BY address
               HAVING total_pnl >= 1000
           )""",
        (db.MIN_BET_USD,),
    ).fetchone()[0]
    from polymarket.market_cache import get_markets
    try:
        markets = await get_markets()
        markets_count = len(markets)
    except Exception:
        markets_count = _markets_count
    return {
        "markets_monitored": markets_count,
        "alerts_today": alert_count_today,
        "tracked_whales": tracked_whales,
        "server_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


@app.get("/api/alerts")
async def recent_alerts(limit: int = 50):
    conn = db._conn()
    rows = conn.execute(
        """SELECT f.*, a.wins, a.resolved_fills,
                  CASE WHEN a.resolved_fills > 0
                       THEN ROUND(CAST(a.wins AS REAL) / a.resolved_fills * 100, 1)
                       ELSE NULL END AS win_rate_pct
           FROM fills f
           LEFT JOIN addresses a ON f.address = a.address
           ORDER BY f.id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/whales")
async def top_whales():
    rows = db.get_top_winners()
    if rows:
        return {"mode": "win_rate", "whales": rows}
    rows = db.get_top_earners()
    return {"mode": "earners", "whales": rows}


@app.get("/api/markets")
async def tracked_markets():
    from polymarket.market_cache import get_markets
    from store import flow_store
    markets = await get_markets()
    highs = flow_store.get_all_market_highs()
    return [
        {**m, "top_fill": highs.get(m["condition_id"])}
        for m in markets
    ]


@app.get("/api/address/{address}")
async def address_detail(address: str):
    import asyncio
    loop = asyncio.get_event_loop()
    stats, fills = await asyncio.gather(
        loop.run_in_executor(None, db.get_address_stats, address),
        loop.run_in_executor(None, db.get_address_fills, address),
    )
    return {"stats": stats, "fills": fills}


@app.get("/api/today")
async def today_action():
    """Biggest plays and hottest markets today."""
    conn = db._conn()
    # Biggest single whale play today (spend + profit filtered)
    top_play = conn.execute(
        """SELECT market_title, side, usd_value, quantity, price_cents, address
           FROM fills
           WHERE ts >= date('now')
             AND usd_value >= ?
             AND (quantity - usd_value) >= ?
           ORDER BY usd_value DESC
           LIMIT 1""",
        (config.WHALE_THRESHOLD_USD, config.WHALE_MIN_PROFIT_USD),
    ).fetchone()
    # Top markets by total whale volume today
    hot_markets = conn.execute(
        """SELECT market_title,
                  SUM(usd_value) AS total_volume,
                  COUNT(*) AS trade_count,
                  SUM(CASE WHEN side='yes' THEN usd_value ELSE 0 END) AS yes_volume,
                  SUM(CASE WHEN side='no'  THEN usd_value ELSE 0 END) AS no_volume
           FROM fills
           WHERE ts >= date('now')
             AND usd_value >= ?
             AND (quantity - usd_value) >= ?
           GROUP BY market_title
           ORDER BY total_volume DESC
           LIMIT 5""",
        (config.WHALE_THRESHOLD_USD, config.WHALE_MIN_PROFIT_USD),
    ).fetchall()
    # Top addresses by total spend today (whale-sized trades only)
    top_addrs = conn.execute(
        """SELECT address, SUM(usd_value) AS total_usd, COUNT(*) AS trades
           FROM fills
           WHERE ts >= date('now') AND usd_value >= ?
           GROUP BY address
           ORDER BY total_usd DESC
           LIMIT 10""",
        (config.WHALE_THRESHOLD_USD,),
    ).fetchall()
    return {
        "top_play": dict(top_play) if top_play else None,
        "hot_markets": [dict(r) for r in hot_markets],
        "top_addresses": [dict(r) for r in top_addrs],
    }


@app.get("/api/flow")
def recent_flow(limit: int = 500, offset: int = 0):
    from store import flow_store
    items = flow_store.get_recent_flow(limit=limit + 1, offset=offset)
    has_more = len(items) > limit
    return {"items": items[:limit], "has_more": has_more}


@app.get("/api/whale-history")
def recent_whale_history(limit: int = 200, offset: int = 0):
    from store import flow_store
    # Fetch extra to account for profit-filter rejections, then paginate
    raw = flow_store.get_recent_whales(limit=limit * 4, offset=offset)
    filtered = [
        w for w in raw
        if w.get("usd_value", 0) >= config.WHALE_THRESHOLD_USD
        and w.get("potential_profit", 0) >= config.WHALE_MIN_PROFIT_USD
    ]
    return {"items": filtered[:limit], "has_more": len(filtered) > limit}


@app.get("/api/stream")
async def sse_stream():
    """Server-Sent Events — pushes new whale alerts to the browser live."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    _subscribers.append(queue)

    async def event_generator() -> AsyncGenerator[str, None]:
        try:
            # Send a heartbeat every 20s to keep connection alive
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=20)
                    yield f"data: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            _subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
