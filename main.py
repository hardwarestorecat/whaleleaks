"""
whaleleaks — geopolitical prediction market whale tracker
  • Polymarket : CLOB WebSocket feed
  • Alerts     : Discord webhook + web dashboard
  • Tracking   : SQLite address win-rate DB

Usage:
    python main.py [--poly-only] [--port 8000]
"""
from __future__ import annotations
import argparse
import asyncio
import logging
import sys

import threading

import uvicorn

import config
from db import database as db
from dashboard import app as dashboard_app, broadcast, broadcast_flow, set_markets_count
from alerts.models import WhaleAlert
from utils.market_filter import is_geopolitical, is_sports
from polymarket.listener import run as poly_run
from polymarket.resolver import run as resolver_run
from polymarket.market_cache import get_markets
from store import flow_store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main")


def _check_config() -> None:
    if not config.DISCORD_WEBHOOK_URL:
        log.warning("Config: DISCORD_WEBHOOK_URL not set — Discord alerts disabled")


async def _on_flow(flow: dict) -> None:
    """Callback: push all ≥$50 orders to dashboard and Redis."""
    broadcast_flow(flow)
    await flow_store.push_flow(flow)


async def _on_whale(alert: WhaleAlert) -> None:
    """Callback: push whale alerts to SSE dashboard and Redis."""
    d = {
        "source": alert.source,
        "market_title": alert.market_title,
        "market_id": alert.market_id,
        "side": alert.side,
        "price_cents": alert.price_cents,
        "usd_value": alert.usd_value,
        "tx_hash": alert.tx_hash,
        "potential_payout": round(alert.potential_payout, 2),
        "potential_profit": round(alert.potential_profit, 2),
        "return_multiple": round(alert.return_multiple, 2),
        "whale_address": alert.whale_address,
        "whale_win_rate": alert.whale_win_rate,
        "whale_resolved_bets": alert.whale_resolved_bets,
        "whale_total_pnl": alert.whale_total_pnl,
        "ts": alert.ts.isoformat(),
    }
    # Enrich with API-sourced categories from market cache
    from polymarket.market_cache import lookup_market
    cached = lookup_market((alert.market_id or "").lower())
    if cached:
        d["is_geopolitical"] = cached.get("is_geopolitical", False) or is_geopolitical(alert.market_title)
        d["is_sports"]       = cached.get("is_sports", False) or is_sports(alert.market_title)
        d["is_politics"]     = cached.get("is_politics", False)
        d["is_crypto"]       = cached.get("is_crypto", False)
        d["is_finance"]      = cached.get("is_finance", False)
        d["is_tech"]         = cached.get("is_tech", False)
        d["is_culture"]      = cached.get("is_culture", False)
    else:
        d["is_geopolitical"] = is_geopolitical(alert.market_title)
        d["is_sports"]       = is_sports(alert.market_title)
    broadcast(d)
    await flow_store.push_whale(d)


async def _update_market_count() -> None:
    while True:
        try:
            markets = await get_markets()
            set_markets_count(len(markets))
        except Exception:
            pass
        await asyncio.sleep(300)


async def _main(port: int) -> None:
    db.init()
    _check_config()

    # Run uvicorn in a daemon thread (its own event loop, no conflict)
    t = threading.Thread(
        target=uvicorn.run,
        kwargs={"app": dashboard_app, "host": "0.0.0.0", "port": port, "log_level": "warning"},
        daemon=True,
    )
    t.start()

    log.info(
        "whaleleaks started | threshold=$%s | dashboard -> http://localhost:%d",
        config.WHALE_THRESHOLD_USD, port,
    )

    await asyncio.gather(
        poly_run(on_whale=_on_whale, on_flow=_on_flow),
        resolver_run(),
        _update_market_count(),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--poly-only", action="store_true")   # kept for compat
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    try:
        asyncio.run(_main(args.port))
    except KeyboardInterrupt:
        log.info("Stopped.")
        sys.exit(0)
