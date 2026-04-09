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


@app.get("/api/stats")
async def stats():
    conn = db._conn()
    alert_count_today = conn.execute(
        "SELECT COUNT(*) FROM fills WHERE ts >= date('now') AND usd_value >= ?",
        (config.WHALE_THRESHOLD_USD,),
    ).fetchone()[0]
    total_addresses = conn.execute("SELECT COUNT(*) FROM addresses").fetchone()[0]
    uptime_s = int(time.time() - _start_time)
    h, rem = divmod(uptime_s, 3600)
    m, s = divmod(rem, 60)
    return {
        "uptime": f"{h:02d}:{m:02d}:{s:02d}",
        "markets_monitored": _markets_count,
        "alerts_today": alert_count_today,
        "total_addresses": total_addresses,
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
    stats = db.get_address_stats(address)
    fills = db.get_address_fills(address)
    return {"stats": stats, "fills": fills}


@app.get("/api/flow")
def recent_flow(limit: int = 500):
    from store import flow_store
    return flow_store.get_recent_flow(limit)


@app.get("/api/whale-history")
def recent_whale_history(limit: int = 200):
    from store import flow_store
    return flow_store.get_recent_whales(limit)


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
