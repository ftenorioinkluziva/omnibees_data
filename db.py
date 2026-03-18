import time
import logging
import psycopg2
from contextlib import contextmanager
from config import DATABASE_URL

logger = logging.getLogger(__name__)


@contextmanager
def get_connection(retries: int = 3):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            try:
                yield conn
            finally:
                conn.close()
            return
        except psycopg2.OperationalError:
            if attempt < retries - 1:
                logger.warning(f"Connection failed, retrying ({attempt + 1}/{retries})...")
                time.sleep(2)
            else:
                raise


def get_stats() -> dict:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT count(*) FROM chains) as chains,
                    (SELECT count(*) FROM hotels) as hotels,
                    (SELECT count(*) FROM hotel_diarias) as diarias,
                    (SELECT count(*) FROM hotel_precos_historico) as historico,
                    (SELECT min(date) FROM hotel_diarias) as date_min,
                    (SELECT max(date) FROM hotel_diarias) as date_max
            """)
            r = cur.fetchone()
            return {
                "chains": r[0],
                "hotels": r[1],
                "diarias": r[2],
                "historico": r[3],
                "date_min": str(r[4]) if r[4] else None,
                "date_max": str(r[5]) if r[5] else None,
            }
