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
from utils.market_filter import is_geopolitical, is_sports

log = logging.getLogger("polymarket.market_cache")

_cache: list[dict] = []
_cache_index: dict[str, dict] = {}  # condition_id → market for O(1) lookup
_last_refresh: float = 0
_REFRESH_INTERVAL = 1800  # 30 min — market metadata changes slowly


async def get_markets() -> list[dict]:
    global _cache, _last_refresh
    if time.time() - _last_refresh < _REFRESH_INTERVAL and _cache:
        return _cache

    PAGE_SIZE = 500
    markets: list[dict] = []
    async with aiohttp.ClientSession() as session:
        offset = 0
        while True:
            url = (
                f"{config.POLYMARKET_GAMMA_API}/markets"
                f"?active=true&closed=false"
                f"&order=volume24hr&ascending=false"
                f"&limit={PAGE_SIZE}&offset={offset}"
            )
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status != 200:
                        log.error("Gamma API %s (offset=%d)", r.status, offset)
                        break
                    page = await r.json()
            except Exception as exc:
                log.error("Gamma API exception (offset=%d): %s", offset, exc)
                break
            if not page:
                break
            markets.extend(page)
            if len(page) < PAGE_SIZE:
                break  # last page
            offset += PAGE_SIZE

    result = []
    for m in markets:
        condition_id = (m.get("conditionId") or "").lower()
        question = m.get("question", "") or m.get("title", "")
        raw_ids = m.get("clobTokenIds") or []
        if isinstance(raw_ids, str):
            try: raw_ids = _json.loads(raw_ids)
            except: raw_ids = []
        # Extract API tag labels for category detection
        tag_labels = {
            (t.get("label") or t.get("slug") or "").lower()
            for t in (m.get("tags") or [])
            if isinstance(t, dict)
        }
        # Map API tags to our categories
        _SPORTS_TAGS = {"sports", "nfl", "nba", "nhl", "mlb", "mls",
                        "ufc", "pga", "soccer", "tennis", "golf",
                        "cricket", "rugby", "boxing", "mma",
                        "formula 1", "f1", "nascar"}
        api_sports = bool(
            m.get("sportsMarketType") or m.get("gameId")
            or tag_labels & _SPORTS_TAGS
        )
        api_politics = bool(tag_labels & {"politics", "elections"})
        api_geo = bool(tag_labels & {"geopolitics"}) or is_geopolitical(question)
        api_crypto = bool(tag_labels & {"crypto", "bitcoin", "ethereum", "cryptocurrency"})
        api_finance = bool(tag_labels & {"finance", "markets", "economics"})
        api_tech = bool(tag_labels & {"tech", "technology", "ai"})
        api_culture = bool(tag_labels & {"culture", "entertainment", "pop culture"})
        result.append({
            "condition_id": condition_id,
            "title": question,
            "slug": m.get("slug", ""),
            "is_geopolitical": api_geo,
            "is_sports": api_sports or is_sports(question),
            "is_politics": api_politics,
            "is_crypto": api_crypto,
            "is_finance": api_finance,
            "is_tech": api_tech,
            "is_culture": api_culture,
            "volume24hr": m.get("volume24hr", 0),
            "token_ids": [str(i) for i in raw_ids],
        })

    _cache = result
    _cache_index = {m["condition_id"]: m for m in result}
    _last_refresh = time.time()
    geo = sum(1 for m in result if m["is_geopolitical"])
    log.info("Market cache: %d markets (%d geopolitical)", len(result), geo)
    return result


def lookup_market(condition_id: str) -> dict | None:
    """O(1) lookup by condition_id from the in-memory cache."""
    return _cache_index.get(condition_id)


# Backwards compat
async def get_geopolitical_markets():
    markets = await get_markets()
    by_condition = {m["condition_id"]: m for m in markets}
    by_token = {}
    return by_condition, by_token

async def get_geopolitical_condition_ids():
    by_condition, _ = await get_geopolitical_markets()
    return by_condition
