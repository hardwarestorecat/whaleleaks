"""Fetch and cache Kalshi geopolitical market tickers."""
from __future__ import annotations
import asyncio
import logging
import time

import aiohttp

import config
from kalshi.auth import sign_request
from utils.market_filter import is_geopolitical

log = logging.getLogger("kalshi.market_cache")

_cache: dict[str, str] = {}   # ticker -> title
_last_refresh: float = 0
_REFRESH_INTERVAL = 300       # seconds


async def get_geopolitical_tickers() -> dict[str, str]:
    """Return {ticker: title} for all open Kalshi geopolitical markets."""
    global _last_refresh, _cache
    if time.time() - _last_refresh < _REFRESH_INTERVAL and _cache:
        return _cache

    tickers: dict[str, str] = {}
    cursor = ""
    path_base = "/trade-api/rest/v1/markets"

    async with aiohttp.ClientSession() as session:
        while True:
            path = f"{path_base}?status=open&limit=200"
            if cursor:
                path += f"&cursor={cursor}"
            headers = sign_request("GET", path)
            url = config.KALSHI_REST_URL.rstrip("/rest/v1") + path
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        log.error("Kalshi market fetch error %s", r.status)
                        break
                    data = await r.json()
            except Exception as exc:
                log.error("Kalshi market fetch exception: %s", exc)
                break

            for m in data.get("markets", []):
                title = m.get("title", "") or m.get("event_title", "")
                ticker = m.get("ticker", "")
                if ticker and is_geopolitical(title):
                    tickers[ticker] = title

            cursor = data.get("cursor", "")
            if not cursor:
                break

    _cache = tickers
    _last_refresh = time.time()
    log.info("Kalshi market cache refreshed: %d geopolitical markets", len(tickers))
    return tickers
