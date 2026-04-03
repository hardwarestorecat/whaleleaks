"""Keyword matching for geopolitical market detection."""
from __future__ import annotations
import re
from config import GEOPOLITICAL_KEYWORDS


def is_geopolitical(title: str) -> bool:
    """Return True if the market title matches any geopolitical keyword."""
    lower = title.lower()
    return any(re.search(rf"\b{re.escape(kw)}\b", lower) for kw in GEOPOLITICAL_KEYWORDS)


def matched_keywords(title: str) -> list[str]:
    lower = title.lower()
    return [kw for kw in GEOPOLITICAL_KEYWORDS if re.search(rf"\b{re.escape(kw)}\b", lower)]
