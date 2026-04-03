"""Kalshi RSA request signing (API v2)."""
from __future__ import annotations
import base64
import hashlib
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config


def _load_private_key():
    path = Path(config.KALSHI_PRIVATE_KEY_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"Kalshi private key not found at {path}. "
            "Download it from https://kalshi.com/profile/api"
        )
    return serialization.load_pem_private_key(path.read_bytes(), password=None)


def sign_request(method: str, path: str, body: str = "") -> dict[str, str]:
    """Return Authorization headers for a Kalshi REST request."""
    ts_ms = str(int(time.time() * 1000))
    msg = ts_ms + method.upper() + path + body
    private_key = _load_private_key()
    signature = private_key.sign(msg.encode(), padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.b64encode(signature).decode()
    return {
        "KALSHI-ACCESS-KEY": config.KALSHI_API_KEY,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "Content-Type": "application/json",
    }


def ws_token() -> tuple[str, str]:
    """Return (timestamp_ms, signature) for WebSocket auth."""
    ts_ms = str(int(time.time() * 1000))
    msg = ts_ms + "GET" + "/trade-api/ws/v2"
    private_key = _load_private_key()
    signature = private_key.sign(msg.encode(), padding.PKCS1v15(), hashes.SHA256())
    return ts_ms, base64.b64encode(signature).decode()
