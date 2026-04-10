"""Keyword matching for geopolitical market detection."""
from __future__ import annotations
import re
from config import GEOPOLITICAL_KEYWORDS

_SPORTS_KEYWORDS = [
    # Strong signal — "vs" almost always means a sporting matchup
    " vs ", " vs. ",
    # Leagues / competitions
    "nfl", "nba", "nhl", "mlb", "nascar", "mls", "ufc", "pga",
    "fifa", "world cup", "champions league", "premier league", "la liga",
    "bundesliga", "serie a", "ligue 1", "super bowl", "playoffs",
    "grand prix", "formula 1",
    # Sports betting terms
    "spread", "over/under", "moneyline", "money line", "point total",
    "game total", "match winner", "to win", "mvp", "rushing yards",
    "passing yards", "touchdowns", "home runs", "strikeouts", "rebounds",
    "assists", "goals scored",
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
