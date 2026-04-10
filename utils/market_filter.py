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
    # Soccer / football clubs & leagues
    " fc ", " fc?", " afc ", " united ", "crystal palace", "arsenal",
    "liverpool", "manchester", "chelsea", "tottenham", "everton",
    "leicester", "wolves", "brighton", "brentford", "fulham",
    "bournemouth", "nottingham", "west ham", "aston villa", "newcastle",
    "real madrid", "barcelona", "bayern", "juventus", "inter milan",
    "ac milan", "psg", "dortmund", "atletico",
    # Other common sports terms
    "quarterback", "pitcher", "goalkeeper", "halftime", "full time",
    "first half", "second half", "match result", "handicap",
]


_CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
    "xrp", "ripple", "dogecoin", "doge", "cardano", "ada",
    "polygon", "matic", "avalanche", "avax", "chainlink", "link",
    "polkadot", "dot", "litecoin", "ltc", "shiba", "pepe",
    "memecoin", "meme coin", "crypto", "cryptocurrency",
    "blockchain", "defi", "nft",
]

_FINANCE_KEYWORDS = [
    "stock", "s&p", "s&p 500", "nasdaq", "dow jones", "treasury",
    "bond", "yield", "inflation", "gdp", "recession", "ipo",
    "earnings", "market cap", "forex", "oil price", "gold price",
    "interest rate", "cpi", "jobs report", "unemployment",
]

_TECH_KEYWORDS = [
    "apple", "google", "microsoft", "amazon", "meta", "nvidia",
    "openai", "chatgpt", "ai ", "artificial intelligence",
    "spacex", "tesla", "semiconductor", "chip",
]

_CULTURE_KEYWORDS = [
    "oscar", "grammy", "emmy", "golden globe", "box office",
    "album", "movie", "film", "tv show", "reality tv",
    "bachelor", "celebrity", "influencer", "tiktok", "youtube",
    "spotify", "netflix", "disney",
]


def is_sports(title: str) -> bool:
    """Return True if the market title is likely a sports market."""
    lower = title.lower()
    return any(kw in lower for kw in _SPORTS_KEYWORDS)


def is_crypto(title: str) -> bool:
    """Return True if the market title is likely a crypto market."""
    lower = title.lower()
    return any(kw in lower for kw in _CRYPTO_KEYWORDS)


def is_finance(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in _FINANCE_KEYWORDS)


def is_tech(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in _TECH_KEYWORDS)


def is_culture(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in _CULTURE_KEYWORDS)


def is_geopolitical(title: str) -> bool:
    """Return True if the market title matches any geopolitical keyword."""
    lower = title.lower()
    return any(re.search(rf"\b{re.escape(kw)}\b", lower) for kw in GEOPOLITICAL_KEYWORDS)


def matched_keywords(title: str) -> list[str]:
    lower = title.lower()
    return [kw for kw in GEOPOLITICAL_KEYWORDS if re.search(rf"\b{re.escape(kw)}\b", lower)]
