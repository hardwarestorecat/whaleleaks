"""
Redis-backed persistence for order flow and whale alerts.
Uses sorted sets (score = Unix timestamp) for time-based retention.
Gracefully degrades to no-op if Redis is unavailable.
"""
from __future__ import annotations
import json
import logging
import time

import redis.asyncio as aioredis

import config

import redis as redis_sync

log = logging.getLogger("store.flow")

FLOW_KEY  = "whaleleaks:flow"
WHALE_KEY = "whaleleaks:whales"
TTL       = 30 * 24 * 3600   # 30 days in seconds


def _url() -> str:
    return getattr(config, "REDIS_URL", "redis://localhost:6379")


# ── Async writes (called from main asyncio loop) ──────────────────────────────

async def push_flow(flow: dict) -> None:
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        ts = time.time()
        async with r:
            await r.zadd(FLOW_KEY, {json.dumps(flow): ts})
            await r.zremrangebyscore(FLOW_KEY, 0, ts - TTL)
    except Exception as exc:
        log.debug("Redis flow push: %s", exc)


async def push_whale(alert: dict) -> None:
    try:
        r = aioredis.from_url(_url(), decode_responses=True)
        ts = time.time()
        async with r:
            await r.zadd(WHALE_KEY, {json.dumps(alert): ts})
            await r.zremrangebyscore(WHALE_KEY, 0, ts - TTL)
    except Exception as exc:
        log.debug("Redis whale push: %s", exc)


# ── Sync reads (called from FastAPI/uvicorn thread) ───────────────────────────

def get_recent_flow(limit: int = 500) -> list[dict]:
    try:
        r = redis_sync.from_url(_url(), decode_responses=True, socket_connect_timeout=2)
        items = r.zrevrange(FLOW_KEY, 0, limit - 1)
        return [json.loads(i) for i in items]
    except Exception as exc:
        log.debug("Redis flow fetch: %s", exc)
        return []


def get_recent_whales(limit: int = 200) -> list[dict]:
    try:
        r = redis_sync.from_url(_url(), decode_responses=True, socket_connect_timeout=2)
        items = r.zrevrange(WHALE_KEY, 0, limit - 1)
        return [json.loads(i) for i in items]
    except Exception as exc:
        log.debug("Redis whale fetch: %s", exc)
        return []
