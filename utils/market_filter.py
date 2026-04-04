"""Keyword matching for geopolitical market detection."""
from __future__ import annotations
import re
from config import GEOPOLITICAL_KEYWORDS

_SPORTS_KEYWORDS = [
    # Team sports signals
    "vs.", "vs ", " vs ", "match", "game", "score", "win on ", "beat ",
    # Leagues / competitions
    "nfl", "nba", "nhl", "mlb", "nascar", "mls", "ufc", "pga",
    "fifa", "world cup", "champions league", "premier league", "la liga",
    "bundesliga", "serie a", "ligue 1", "copa", "euros", "euro 2",
    "super bowl", "playoffs", "championship", "tournament", "open",
    # Sports
    "soccer", "football", "basketball", "baseball", "hockey", "tennis",
    "golf", "cricket", "rugby", "boxing", "mma", "wrestling", "formula 1",
    "f1 ", " f1", "grand prix", "nascar",
    # Common team/athlete indicators
    "fc ", " fc", "afc ", "nfc ", "wildcats", "bulldogs", "eagles",
    "patriots", "cowboys", "lakers", "celtics", "yankees", "cubs",
]


def is_sports(title: str) -> bool:
    """Return True if the market title is likely a sports market."""
    lower = title.lower()
    return any(kw in lower for kw in _SPORTS_KEYWORDS)


def is_geopolitical(title: str) -> bool:
    """Return True if the market title matches any geopolitical keyword."""
    lower = title.lower()
    return any(re.search(rf"\b{re.escape(kw)}\b", lower) for kw in GEOPOLITICAL_KEYWORDS)


def matched_keywords(title: str) -> list[str]:
    lower = title.lower()
    return [kw for kw in GEOPOLITICAL_KEYWORDS if re.search(rf"\b{re.escape(kw)}\b", lower)]
