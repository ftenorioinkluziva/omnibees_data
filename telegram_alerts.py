#!/usr/bin/env python3
"""
Telegram Alerts
===============
Envia notificações via Telegram quando preços de hotéis da watchlist mudam.
"""

import logging
import requests
from db import get_connection

logger = logging.getLogger(__name__)


def _bot_token() -> str:
    from config import TELEGRAM_BOT_TOKEN
    return TELEGRAM_BOT_TOKEN


def _chat_id() -> str:
    from config import TELEGRAM_CHAT_ID
    return TELEGRAM_CHAT_ID


def send_message(text: str) -> bool:
    token = _bot_token()
    chat_id = _chat_id()
    if not token or not chat_id:
        logger.debug("Telegram não configurado (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID ausentes)")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Telegram retornou {resp.status_code}: {resp.text}")
            return False
        return True
    except requests.RequestException as e:
        logger.warning(f"Falha ao enviar alerta Telegram: {e}")
        return False


def get_watches_for_hotel(hotel_db_id: int) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, date_start, date_end, label
                FROM watched_hotels
                WHERE hotel_id = %s AND notify = true
            """, (hotel_db_id,))
            rows = cur.fetchall()
            return [
                {"watch_id": r[0], "date_start": str(r[1]), "date_end": str(r[2]), "label": r[3]}
                for r in rows
            ]


def notify_price_changes(hotel_db_id: int, hotel_name: str, changes: list[dict]):
    """Envia alerta Telegram para cada mudança de preço que se enquadra em um item da watchlist."""
    if not changes:
        return

    watches = get_watches_for_hotel(hotel_db_id)
    if not watches:
        return

    for change in changes:
        change_date = change["date"]
        for watch in watches:
            if watch["date_start"] <= change_date <= watch["date_end"]:
                _send_price_change_alert(hotel_name, change, watch["label"])
                break


def _send_price_change_alert(hotel_name: str, change: dict, label: str | None):
    old = float(change["old_price"])
    new = float(change["new_price"])
    pct = ((new - old) / old) * 100 if old else 0
    direction = "📉 Baixou" if new < old else "📈 Subiu"

    text = (
        f"{direction} <b>{hotel_name}</b>\n"
        f"📅 <b>{change['date']}</b>\n"
        f"💰 R${old:.2f} → R${new:.2f} (<b>{pct:+.1f}%</b>)"
    )
    if label:
        text += f"\n🏷️ {label}"

    sent = send_message(text)
    if sent:
        logger.info(f"Alerta enviado: {hotel_name} | {change['date']} | R${old:.2f}→R${new:.2f}")
