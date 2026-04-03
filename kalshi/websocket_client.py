"""
Kalshi WebSocket orderbook_delta listener.

Connects to wss://trading-api.kalshi.com/trade-api/ws/v2, subscribes to
orderbook_delta for all open geopolitical markets, and fires a callback when
a single price-level delta represents >= WHALE_THRESHOLD_USD.

Orderbook delta message schema (v2):
  {
    "type": "orderbook_delta",
    "msg": {
      "market_ticker": "PRES-2024-DJT",
      "price": 65,          # cents
      "delta": 500,         # signed quantity change (+ = add, - = remove)
      "side": "yes"         # "yes" | "no"
    }
  }

Dollar value of a level = |delta| contracts × price_cents / 100
(each contract pays $1 at resolution, so face value = price × qty)
"""
from __future__ import annotations
import asyncio
import json
import logging
from typing import Callable, Awaitable

import websockets

import config
from alerts.models import WhaleAlert
from alerts.dispatcher import dispatch
from kalshi.auth import ws_token
from kalshi.market_cache import get_geopolitical_tickers
from kalshi.trader import place_proportional_order
from utils.market_filter import matched_keywords

log = logging.getLogger("kalshi.ws")

WhaleCB = Callable[[WhaleAlert], Awaitable[None]]

_RECONNECT_DELAY = 5   # seconds


async def _subscribe(ws, tickers: dict[str, str]) -> None:
    msg = {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": list(tickers.keys()),
        },
    }
    await ws.send(json.dumps(msg))
    log.info("Subscribed to %d Kalshi markets", len(tickers))


async def run(on_whale: WhaleCB | None = None) -> None:
    """Run forever; reconnects on disconnect."""
    while True:
        try:
            await _run_once(on_whale)
        except Exception as exc:
            log.error("Kalshi WS error: %s — reconnecting in %ds", exc, _RECONNECT_DELAY)
        await asyncio.sleep(_RECONNECT_DELAY)


async def _run_once(on_whale: WhaleCB | None) -> None:
    tickers = await get_geopolitical_tickers()
    if not tickers:
        log.warning("No geopolitical Kalshi markets found; retrying later")
        await asyncio.sleep(60)
        return

    ts_ms, sig = ws_token()
    headers = {
        "KALSHI-ACCESS-KEY": config.KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": sig,
    }

    async with websockets.connect(
        config.KALSHI_WS_URL,
        additional_headers=headers,
        ping_interval=20,
        ping_timeout=10,
    ) as ws:
        await _subscribe(ws, tickers)
        log.info("Kalshi WS connected")

        async for raw in ws:
            try:
                envelope = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = envelope.get("type")

            # Refresh subscriptions when cache is stale
            if msg_type in ("subscribed", "heartbeat"):
                continue

            if msg_type != "orderbook_delta":
                continue

            msg = envelope.get("msg", {})
            ticker = msg.get("market_ticker", "")
            price_cents = int(msg.get("price", 0))
            delta = msg.get("delta", 0)
            side = msg.get("side", "unknown")

            qty = abs(delta)
            if qty == 0 or price_cents == 0:
                continue

            usd_value = qty * price_cents / 100

            if usd_value < config.WHALE_THRESHOLD_USD:
                continue

            title = tickers.get(ticker, ticker)
            alert = WhaleAlert(
                source="kalshi",
                market_title=title,
                market_id=ticker,
                side=side,
                price_cents=price_cents,
                quantity=qty,
                usd_value=usd_value,
                matched_keywords=matched_keywords(title),
            )

            await dispatch(alert)

            if config.AUTO_TRADE_ENABLED:
                await place_proportional_order(alert)

            if on_whale:
                await on_whale(alert)
