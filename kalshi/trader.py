"""
Place a proportional limit order on Kalshi after a whale alert.

Order size = whale_qty × PROPORTIONAL_FACTOR (default 1 %).
We mirror the whale: same side, same price.
"""
from __future__ import annotations
import json
import logging
import uuid

import aiohttp

import config
from alerts.models import WhaleAlert
from kalshi.auth import sign_request

log = logging.getLogger("kalshi.trader")


async def place_proportional_order(alert: WhaleAlert) -> dict | None:
    if not config.AUTO_TRADE_ENABLED:
        return None
    if alert.source != "kalshi":
        return None

    qty = max(1, int(alert.quantity * config.PROPORTIONAL_FACTOR))
    side = alert.side  # "yes" | "no"

    body_obj = {
        "ticker": alert.market_id,
        "action": "buy",
        "side": side,
        "type": "limit",
        "count": qty,
        "yes_price": alert.price_cents if side == "yes" else None,
        "no_price": alert.price_cents if side == "no" else None,
        "client_order_id": str(uuid.uuid4()),
    }
    # Remove None values
    body_obj = {k: v for k, v in body_obj.items() if v is not None}
    body_str = json.dumps(body_obj)

    path = "/trade-api/rest/v1/portfolio/orders"
    headers = sign_request("POST", path, body_str)
    url = config.KALSHI_REST_URL.replace("/trade-api/rest/v1", "") + path

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, headers=headers, data=body_str,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                resp = await r.json()
                if r.status in (200, 201):
                    order_id = resp.get("order", {}).get("order_id", "?")
                    log.info(
                        "Placed %s × %d @ %d¢ on %s (order_id=%s)",
                        side, qty, alert.price_cents, alert.market_id, order_id,
                    )
                    return resp
                else:
                    log.error("Order placement failed %s: %s", r.status, resp)
                    return None
    except Exception as exc:
        log.error("Order placement exception: %s", exc)
        return None
