#!/usr/bin/env python3
"""
Omnibees Price Scraper
======================
Coleta preços de diárias dos hotéis via API do Omnibees e salva no PostgreSQL.
Replica a lógica do workflow n8n com suporte a todos os hotéis do banco.

Uso:
    # Coletar preços de todos os hotéis ativos no banco
    python omnibees_price_scraper.py

    # Coletar apenas hotéis específicos (por external_id)
    python omnibees_price_scraper.py --hotels 9098 9694 2281

    # Ajustar meses à frente e delay
    python omnibees_price_scraper.py --months 6 --delay 1.0

    # Workers assíncronos
    python omnibees_price_scraper.py --workers 3 --delay 0.5
"""

import argparse
import asyncio
import json
import logging
from pathlib import Path
from datetime import date
from calendar import monthrange
from typing import Optional

import aiohttp
import psycopg2
from config import DATABASE_URL, AVAILABILITY_URL, CURRENCY_ID, API_HEADERS, PRICE_CHECKPOINT_FILE
from telegram_alerts import notify_price_changes

logger = logging.getLogger("price_scraper")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("omnibees_prices.log", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def load_price_checkpoint(checkpoint_path: Path) -> dict:
    if not checkpoint_path.exists():
        return {}
    try:
        return json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_price_checkpoint(checkpoint_path: Path, payload: dict):
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def clear_price_checkpoint(checkpoint_path: Path):
    if checkpoint_path.exists():
        checkpoint_path.unlink()


def generate_date_ranges(months_ahead: int = 15) -> list[tuple[str, str]]:
    today = date.today()
    ranges = []

    last_day = date(today.year, today.month, monthrange(today.year, today.month)[1])
    ranges.append((today.isoformat(), last_day.isoformat()))

    for i in range(1, months_ahead):
        year = today.year + (today.month + i - 1) // 12
        month = (today.month + i - 1) % 12 + 1
        first = date(year, month, 1)
        last = date(year, month, monthrange(year, month)[1])
        ranges.append((first.isoformat(), last.isoformat()))

    return ranges


def get_hotels_from_db(hotel_ids: Optional[list[str]] = None) -> list[dict]:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            if hotel_ids:
                cur.execute(
                    "SELECT id, external_id, name FROM hotels WHERE external_id = ANY(%s) ORDER BY name",
                    (hotel_ids,)
                )
            else:
                cur.execute("SELECT id, external_id, name FROM hotels ORDER BY name")
            rows = cur.fetchall()
            return [{"db_id": r[0], "external_id": r[1], "name": r[2]} for r in rows]
    finally:
        conn.close()


def get_watchlist_hotels() -> list[dict]:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT h.id, h.external_id, h.name
                FROM watched_hotels w
                JOIN hotels h ON w.hotel_id = h.id
                WHERE w.notify = true
                ORDER BY h.name
            """)
            rows = cur.fetchall()
            return [{"db_id": r[0], "external_id": r[1], "name": r[2]} for r in rows]
    finally:
        conn.close()


async def fetch_prices(session: aiohttp.ClientSession, hotel_ext_id: str,
                       date_start: str, date_end: str) -> Optional[list[dict]]:
    url = f"{AVAILABILITY_URL}/{hotel_ext_id}/{CURRENCY_ID}/{date_start}/{date_end}/1/0/0"
    try:
        async with session.get(url, headers=API_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status == 200:
                text = await resp.text()
                if text.strip():
                    return json.loads(text)
            elif resp.status == 429:
                logger.warning(f"Rate limited for hotel {hotel_ext_id}, waiting 10s...")
                await asyncio.sleep(10)
            return None
    except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
        logger.warning(f"Error fetching hotel {hotel_ext_id} ({date_start}): {e}")
        return None


def save_prices_to_db(hotel_db_id: int, hotel_name: str, prices: list[dict]):
    if not prices:
        return 0, 0, []

    conn = psycopg2.connect(DATABASE_URL)
    inserted = 0
    updated = 0
    changes: list[dict] = []
    try:
        with conn.cursor() as cur:
            for item in prices:
                raw_date = item.get("date", "")
                price = item.get("price")
                if not raw_date or not price:
                    continue

                dt = raw_date.split("T")[0]

                cur.execute("""
                    SELECT id, amount FROM hotel_diarias
                    WHERE hotel_id = %s AND date = %s
                """, (hotel_db_id, dt))
                existing = cur.fetchone()

                if existing:
                    diaria_id, old_amount = existing
                    if float(old_amount) != float(price):
                        cur.execute("""
                            INSERT INTO hotel_precos_historico (hotel_diaria_id, amount)
                            VALUES (%s, %s)
                        """, (diaria_id, old_amount))
                        cur.execute("""
                            UPDATE hotel_diarias SET amount = %s, updated_at = now()
                            WHERE id = %s
                        """, (price, diaria_id))
                        updated += 1
                        changes.append({"date": dt, "old_price": float(old_amount), "new_price": float(price)})
                else:
                    cur.execute("""
                        INSERT INTO hotel_diarias (date, amount, hotel, hotel_id)
                        VALUES (%s, %s, %s, %s)
                    """, (dt, price, hotel_name, hotel_db_id))
                    inserted += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB error for hotel {hotel_name}: {e}")
    finally:
        conn.close()

    return inserted, updated, changes


async def scrape_hotel_prices(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession,
                              hotel: dict, date_ranges: list[tuple[str, str]],
                              delay: float) -> dict:
    async with semaphore:
        hotel_name = hotel["name"]
        hotel_ext_id = hotel["external_id"]
        hotel_db_id = hotel["db_id"]

        total_inserted = 0
        total_updated = 0
        total_prices = 0

        all_changes: list[dict] = []
        for start, end in date_ranges:
            prices = await fetch_prices(session, hotel_ext_id, start, end)
            if prices:
                valid = [p for p in prices if p.get("price")]
                total_prices += len(valid)
                ins, upd, changes = save_prices_to_db(hotel_db_id, hotel_name, valid)
                total_inserted += ins
                total_updated += upd
                all_changes.extend(changes)

            await asyncio.sleep(delay)

        if all_changes:
            notify_price_changes(hotel_db_id, hotel_name, all_changes)

        return {
            "hotel": hotel_name,
            "external_id": hotel_ext_id,
            "prices_fetched": total_prices,
            "inserted": total_inserted,
            "updated": total_updated,
        }


async def run(
    hotel_ids: Optional[list[str]],
    months: int,
    workers: int,
    delay: float,
    batch_size: int = 250,
    resume: bool = False,
    watchlist_only: bool = False,
):
    if watchlist_only:
        hotels = get_watchlist_hotels()
        if not hotels:
            logger.info("Nenhum hotel na watchlist — nada a coletar.")
            return
        logger.info(f"Modo watchlist: {len(hotels)} hotel(is) monitorado(s)")
    else:
        hotels = get_hotels_from_db(hotel_ids)
        if not hotels:
            logger.error("Nenhum hotel encontrado no banco.")
            return

    checkpoint_path = PRICE_CHECKPOINT_FILE

    if resume and not hotel_ids:
        checkpoint = load_price_checkpoint(checkpoint_path)
        last_ext_id = checkpoint.get("last_external_id")
        if last_ext_id:
            start_index = next((idx + 1 for idx, h in enumerate(hotels) if h["external_id"] == last_ext_id), 0)
            if start_index > 0:
                logger.info(f"Retomando do checkpoint após hotel external_id={last_ext_id}")
                hotels = hotels[start_index:]

    date_ranges = generate_date_ranges(months)
    total_requests = len(hotels) * len(date_ranges)

    logger.info("=" * 60)
    logger.info("OMNIBEES PRICE SCRAPER")
    logger.info("=" * 60)
    logger.info(f"Hotéis: {len(hotels)}")
    logger.info(f"Períodos: {len(date_ranges)} ({date_ranges[0][0]} a {date_ranges[-1][1]})")
    logger.info(f"Total de requisições: {total_requests}")
    logger.info(f"Workers: {workers} | Delay: {delay}s | Batch: {batch_size}")
    logger.info("=" * 60)

    semaphore = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers, force_close=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        results = []
        completed = 0
        total_hotels = len(hotels)

        for batch_start in range(0, total_hotels, batch_size):
            batch = hotels[batch_start:batch_start + batch_size]
            tasks = [
                scrape_hotel_prices(semaphore, session, hotel, date_ranges, delay)
                for hotel in batch
            ]

            for coro in asyncio.as_completed(tasks):
                result = await coro
                results.append(result)
                completed += 1

                if result["prices_fetched"] > 0:
                    logger.info(
                        f"[{completed}/{total_hotels}] {result['hotel']}: "
                        f"{result['prices_fetched']} preços | "
                        f"+{result['inserted']} novos | "
                        f"~{result['updated']} atualizados"
                    )
                else:
                    logger.info(f"[{completed}/{total_hotels}] {result['hotel']}: sem preços disponíveis")

                if not hotel_ids:
                    save_price_checkpoint(
                        checkpoint_path,
                        {
                            "last_external_id": result.get("external_id"),
                            "completed": completed,
                            "total": total_hotels,
                            "months": months,
                            "workers": workers,
                            "delay": delay,
                            "batch_size": batch_size,
                        },
                    )

            logger.info(f"Batch concluído: {min(batch_start + batch_size, total_hotels)}/{total_hotels}")

    total_fetched = sum(r["prices_fetched"] for r in results)
    total_inserted = sum(r["inserted"] for r in results)
    total_updated = sum(r["updated"] for r in results)

    logger.info("\n" + "=" * 60)
    logger.info("RESUMO")
    logger.info("=" * 60)
    logger.info(f"Hotéis processados: {len(results)}")
    logger.info(f"Preços coletados: {total_fetched}")
    logger.info(f"Novos registros: {total_inserted}")
    logger.info(f"Preços atualizados: {total_updated}")
    logger.info("=" * 60)

    if not hotel_ids:
        clear_price_checkpoint(checkpoint_path)
        logger.info("Checkpoint de preços limpo (coleta finalizada).")


def main():
    parser = argparse.ArgumentParser(description="Omnibees Price Scraper")
    parser.add_argument("--hotels", nargs="+", help="External IDs dos hotéis (default: todos)")
    parser.add_argument("--months", type=int, default=15, help="Meses à frente (default: 15)")
    parser.add_argument("--workers", type=int, default=3, help="Requisições simultâneas (default: 3)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay entre requisições (default: 0.5s)")
    parser.add_argument("--batch-size", type=int, default=250, help="Hotéis por lote (default: 250)")
    parser.add_argument("--resume", action="store_true", help="Retomar do último checkpoint de preços")
    parser.add_argument("--watchlist-only", action="store_true", help="Coletar apenas hotéis da watchlist")
    args = parser.parse_args()

    asyncio.run(run(args.hotels, args.months, args.workers, args.delay, args.batch_size, args.resume, args.watchlist_only))


if __name__ == "__main__":
    main()
