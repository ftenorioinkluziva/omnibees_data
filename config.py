import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

BASE_URL = "https://book.omnibees.com"
AVAILABILITY_URL = f"{BASE_URL}/availability_v4/q"
CURRENCY_ID = 16  # BRL

OUTPUT_DIR = Path("omnibees_data")
CHAINS_DIR = OUTPUT_DIR / "chains"
HOTELS_DIR = OUTPUT_DIR / "hotels"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
PRICE_CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint_prices.json"

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

API_HEADERS = {
    **REQUEST_HEADERS,
    "Accept": "*/*",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "x-tenant-id": "1",
}
