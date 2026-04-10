"""Fan-out whale alerts to all enabled channels: Discord, email, web push."""
from __future__ import annotations
import asyncio
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiohttp

import config
from alerts.models import WhaleAlert

log = logging.getLogger("dispatcher")


# ─── Discord webhook ─────────────────────────────────────────────────────────

async def _send_discord(alert: WhaleAlert) -> None:
    """
    Post a rich embed to a Discord channel via webhook.
    Setup (one-time, ~30 seconds):
      Server Settings → Integrations → Webhooks → New Webhook → Copy URL
    """
    if not config.DISCORD_WEBHOOK_URL:
        return

    color = 0xE74C3C if alert.side in ("yes", "buy") else 0x3498DB
    kw = ", ".join(alert.matched_keywords) if alert.matched_keywords else "—"

    # Build Polymarket URL if slug is available
    pm_url = ""
    if alert.market_id and not alert.market_id.startswith("0x"):
        pm_url = f"https://polymarket.com/event/{alert.market_id}"

    profit = alert.potential_profit
    mult = alert.return_multiple

    fields = [
        {"name": "Side",    "value": alert.side.upper(),              "inline": True},
        {"name": "Price",   "value": f"{alert.price_cents}¢",         "inline": True},
        {"name": "Spent",   "value": f"**${alert.usd_value:,.0f}**",  "inline": True},
        {"name": "Wins",    "value": f"**+${profit:,.0f}** ({mult:.2f}×)", "inline": True},
        {"name": "Time",    "value": alert.ts.strftime("%b %d, %I:%M %p UTC"), "inline": True},
        {"name": "Keywords","value": kw,                              "inline": True},
    ]

    if pm_url:
        fields.append({"name": "Market", "value": f"[{alert.market_id}]({pm_url})", "inline": False})
    else:
        fields.append({"name": "Market", "value": f"`{alert.market_id}`", "inline": False})

    # ── Whale address + win rate (Polymarket only) ──
    if alert.whale_address:
        addr_short = f"`{alert.whale_address[:6]}…{alert.whale_address[-4:]}`"
        if alert.whale_win_rate is not None:
            stars = "⭐" * min(5, round(alert.whale_win_rate * 5))
            wr_str = (
                f"{alert.whale_win_rate:.0%} {stars}\n"
                f"{alert.whale_resolved_bets} resolved · "
                f"P&L ${alert.whale_total_pnl:+,.0f}"
            )
            # Bump color to gold if high win-rate whale
            if alert.whale_win_rate >= 0.65 and alert.whale_resolved_bets >= 5:
                color = 0xF1C40F
        else:
            wr_str = "First sighting — no history yet"
        fields.insert(0, {"name": f"Whale {addr_short}", "value": wr_str, "inline": False})

    embed = {
        "title": f"🐋  Whale Alert — {alert.source.upper()}",
        "description": f"**{alert.market_title}**",
        "color": color,
        "fields": fields,
        "footer": {"text": alert.ts.strftime("UTC %Y-%m-%d %H:%M:%S")},
    }
    if pm_url:
        embed["url"] = pm_url

    payload = {"embeds": [embed]}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.DISCORD_WEBHOOK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status not in (200, 204):
                    body = await r.text()
                    log.warning("Discord error %s: %s", r.status, body[:200])
                else:
                    log.info("Discord alert sent for %s", alert.market_id)
    except Exception as exc:
        log.error("Discord send failed: %s", exc)


# ─── Email ────────────────────────────────────────────────────────────────────

def _build_email_html(alert: WhaleAlert) -> str:
    kw = ", ".join(alert.matched_keywords) if alert.matched_keywords else "—"
    color = "#c0392b" if alert.side in ("yes", "buy") else "#2980b9"
    return f"""
<html><body style="font-family:monospace;background:#0d1117;color:#e6edf3;padding:20px">
  <h2 style="color:{color}">🐋 Whale Alert — {alert.source.upper()}</h2>
  <table style="border-collapse:collapse;width:100%">
    <tr><td style="padding:4px 8px;color:#8b949e">Market</td>
        <td style="padding:4px 8px"><b>{alert.market_title}</b></td></tr>
    <tr><td style="padding:4px 8px;color:#8b949e">ID</td>
        <td style="padding:4px 8px">{alert.market_id}</td></tr>
    <tr><td style="padding:4px 8px;color:#8b949e">Side</td>
        <td style="padding:4px 8px;color:{color}"><b>{alert.side.upper()}</b></td></tr>
    <tr><td style="padding:4px 8px;color:#8b949e">Price</td>
        <td style="padding:4px 8px">{alert.price_cents}¢</td></tr>
    <tr><td style="padding:4px 8px;color:#8b949e">Quantity</td>
        <td style="padding:4px 8px">{alert.quantity:,.0f}</td></tr>
    <tr><td style="padding:4px 8px;color:#8b949e">USD Value</td>
        <td style="padding:4px 8px"><b>${alert.usd_value:,.2f}</b></td></tr>
    <tr><td style="padding:4px 8px;color:#8b949e">Keywords</td>
        <td style="padding:4px 8px">{kw}</td></tr>
    <tr><td style="padding:4px 8px;color:#8b949e">UTC</td>
        <td style="padding:4px 8px">{alert.ts.strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
  </table>
</body></html>
"""


def _send_email_sync(alert: WhaleAlert) -> None:
    if not config.EMAIL_ENABLED or not config.EMAIL_TO:
        return
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Whale Alert: ${alert.usd_value:,.0f} on {alert.market_title[:50]}"
    msg["From"] = config.EMAIL_FROM
    msg["To"] = ", ".join(config.EMAIL_TO)
    msg.attach(MIMEText(alert.summary_line(), "plain"))
    msg.attach(MIMEText(_build_email_html(alert), "html"))
    ctx = ssl.create_default_context()
    try:
        with smtplib.SMTP(config.EMAIL_SMTP_HOST, config.EMAIL_SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls(context=ctx)
            smtp.login(config.EMAIL_USERNAME, config.EMAIL_PASSWORD)
            smtp.sendmail(config.EMAIL_FROM, config.EMAIL_TO, msg.as_string())
        log.info("Email alert sent for %s", alert.market_id)
    except Exception as exc:
        log.error("Email send failed: %s", exc)


async def _send_email(alert: WhaleAlert) -> None:
    await asyncio.get_event_loop().run_in_executor(None, _send_email_sync, alert)


# ─── Public dispatcher ────────────────────────────────────────────────────────

async def dispatch(alert: WhaleAlert) -> None:
    """Send alert to all configured channels concurrently."""
    log.warning("WHALE DETECTED: %s", alert.summary_line())
    await asyncio.gather(
        _send_discord(alert),
        _send_email(alert),
        return_exceptions=True,
    )
