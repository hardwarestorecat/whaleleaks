"""Shared data model for whale alerts."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class WhaleAlert:
    source: str                  # "kalshi" | "polymarket"
    market_title: str
    market_id: str
    side: str                    # "yes" | "no" | "buy" | "sell"
    price_cents: int             # price in cents (0-99)
    quantity: float              # number of contracts / shares
    usd_value: float             # computed dollar value
    matched_keywords: list[str] = field(default_factory=list)
    ts: datetime = field(default_factory=datetime.utcnow)

    # Polymarket address tracking
    maker_address: str | None = None
    taker_address: str | None = None
    tx_hash: str | None = None
    block_number: int | None = None

    # Win rate of the whale address (populated after DB lookup)
    whale_address: str | None = None        # the address flagged as whale
    whale_win_rate: float | None = None     # 0.0–1.0 or None if unknown
    whale_resolved_bets: int = 0
    whale_total_pnl: float = 0.0

    @property
    def price_pct(self) -> float:
        return self.price_cents / 100

    @property
    def potential_payout(self) -> float:
        """Total returned if whale is correct ($1 per share)."""
        return self.quantity

    @property
    def potential_profit(self) -> float:
        """Net profit if whale is correct (payout minus cost)."""
        return self.quantity - self.usd_value

    @property
    def return_multiple(self) -> float:
        """Return multiple on investment (e.g. 2.5× at 40¢)."""
        return self.quantity / self.usd_value if self.usd_value else 0.0

    def win_rate_str(self) -> str:
        if self.whale_win_rate is None:
            return "unknown"
        return f"{self.whale_win_rate:.0%} ({self.whale_resolved_bets} resolved)"

    def summary_line(self) -> str:
        kw = ", ".join(self.matched_keywords) if self.matched_keywords else "—"
        lines = [
            f"[{self.source.upper()}] {self.market_title}",
            f"  Side: {self.side.upper()}  |  Price: {self.price_cents}¢  "
            f"|  Spent: ${self.usd_value:,.0f}  |  Wins: ${self.potential_profit:,.0f} ({self.return_multiple:.2f}×)",
            f"  Keywords: {kw}",
            f"  Market ID: {self.market_id}",
        ]
        if self.whale_address:
            lines.append(
                f"  Whale: {self.whale_address[:10]}…  "
                f"Win rate: {self.win_rate_str()}  "
                f"P&L: ${self.whale_total_pnl:+,.0f}"
            )
        lines.append(f"  UTC: {self.ts.strftime('%Y-%m-%d %H:%M:%S')}")
        return "\n".join(lines)
