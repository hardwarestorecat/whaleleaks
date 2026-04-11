import os
from dotenv import load_dotenv

load_dotenv()


def _kw_list(raw: str) -> list[str]:
    return [w.strip().lower() for w in raw.split(",") if w.strip()]


REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

WHALE_THRESHOLD_USD: float = float(os.getenv("WHALE_THRESHOLD_USD", "3000"))
WHALE_MIN_PROFIT_USD: float = float(os.getenv("WHALE_MIN_PROFIT_USD", "1000"))
FLOW_THRESHOLD_USD: float = float(os.getenv("FLOW_THRESHOLD_USD", "1000"))
PROPORTIONAL_FACTOR: float = float(os.getenv("PROPORTIONAL_FACTOR", "0.01"))
AUTO_TRADE_ENABLED: bool = os.getenv("AUTO_TRADE_ENABLED", "true").lower() == "true"

GEOPOLITICAL_KEYWORDS: list[str] = _kw_list(
    os.getenv(
        "GEOPOLITICAL_KEYWORDS",
        # Conflict & military
        "war,wars,conflict,battle,attack,invasion,ceasefire,troops,military,"
        "airstrike,strike,missile,nuclear,nato,alliance,defense,weapon,bomb,"
        "soldier,army,navy,forces,occupation,siege,offensive,insurgent,"
        # Leaders & politics
        "president,prime minister,chancellor,premier,leader,election,vote,"
        "inauguration,resign,impeach,coup,assassination,cabinet,minister,"
        "parliament,congress,senate,legislation,executive,veto,filibuster,"
        "supreme court,debt ceiling,referendum,ballot,"
        # Diplomacy & international
        "sanction,treaty,agreement,deal,negotiate,summit,diplomat,ambassador,"
        "united nations,security council,un resolution,embargo,ally,allies,"
        "sovereignty,territory,border,annex,occupation,regime change,"
        # Countries & regions
        "ukraine,russia,china,taiwan,iran,israel,gaza,palestine,north korea,"
        "south korea,japan,india,pakistan,syria,iraq,afghanistan,venezuela,"
        "cuba,turkey,saudi,nato,europe,european union,middle east,"
        "africa,latin america,asia pacific,arctic,"
        # Trade & economic conflict
        "tariff,trade war,export ban,import ban,supply chain,oil,energy,"
        "gas pipeline,inflation,fed,federal reserve,interest rate,recession,"
        "dollar,currency,default,debt,gdp,economic crisis,"
        # Other geopolitical
        "refugee,humanitarian,protest,revolution,uprising,civil war,"
        "terrorist,terrorism,intelligence,spy,espionage,hack,cyberattack,"
        "space race,arms race,proliferation",
    )
)

# Kalshi
KALSHI_API_KEY: str = os.getenv("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY_PATH: str = os.getenv("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem")
KALSHI_WS_URL: str = "wss://trading-api.kalshi.com/trade-api/ws/v2"
KALSHI_REST_URL: str = "https://trading-api.kalshi.com/trade-api/rest/v1"

# Discord webhook
DISCORD_WEBHOOK_URL: str = os.getenv("DISCORD_WEBHOOK_URL", "")

# Email
EMAIL_ENABLED: bool = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SMTP_HOST: str = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
EMAIL_SMTP_PORT: int = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_USERNAME: str = os.getenv("EMAIL_USERNAME", "")
EMAIL_PASSWORD: str = os.getenv("EMAIL_PASSWORD", "")
EMAIL_FROM: str = os.getenv("EMAIL_FROM", EMAIL_USERNAME)
EMAIL_TO: list[str] = _kw_list(os.getenv("EMAIL_TO", ""))

# Polygon
POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

# Polymarket CTF Exchange contract (Polygon mainnet)
POLYMARKET_CTF_EXCHANGE: str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
POLYMARKET_GAMMA_API: str = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API: str = "https://clob.polymarket.com"
