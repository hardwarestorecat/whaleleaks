"""
Fetch top Polymarket markets by 24h volume for the Markets tab.
No longer used for subscriptions — listener polls data-api directly.
"""
from __future__ import annotations
import json as _json
import logging
import time

import aiohttp

import config
from utils.market_filter import is_geopolitical

log = logging.getLogger("polymarket.market_cache")

_cache: list[dict] = []
_last_refresh: float = 0
_REFRESH_INTERVAL = 300
MARKET_LIMIT = int(getattr(config, "MARKET_LIMIT", 500))


async def get_markets() -> list[dict]:
    global _cache, _last_refresh
    if time.time() - _last_refresh < _REFRESH_INTERVAL and _cache:
        return _cache

    url = (
        f"{config.POLYMARKET_GAMMA_API}/markets"
        f"?active=true&closed=false"
        f"&order=volume24hr&ascending=false"
        f"&limit={MARKET_LIMIT}"
    )
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status != 200:
                    log.error("Gamma API %s", r.status)
                    return _cache
                markets = await r.json()
        except Exception as exc:
            log.error("Gamma API exception: %s", exc)
            return _cache

    result = []
    for m in markets:
        condition_id = (m.get("conditionId") or "").lower()
        question = m.get("question", "") or m.get("title", "")
        raw_ids = m.get("clobTokenIds") or []
        if isinstance(raw_ids, str):
            try: raw_ids = _json.loads(raw_ids)
            except: raw_ids = []
        result.append({
            "condition_id": condition_id,
            "title": question,
            "slug": m.get("slug", ""),
            "is_geopolitical": is_geopolitical(question),
            "volume24hr": m.get("volume24hr", 0),
            "token_ids": [str(i) for i in raw_ids],
        })

    _cache = result
    _last_refresh = time.time()
    geo = sum(1 for m in result if m["is_geopolitical"])
    log.info("Market cache: %d markets (%d geopolitical)", len(result), geo)
    return result


# Backwards compat
async def get_geopolitical_markets():
    markets = await get_markets()
    by_condition = {m["condition_id"]: m for m in markets}
    by_token = {}
    return by_condition, by_token

async def get_geopolitical_condition_ids():
    by_condition, _ = await get_geopolitical_markets()
    return by_condition
