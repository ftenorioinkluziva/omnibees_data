#!/usr/bin/env python3
"""
Omnibees Async Scraper
======================
Versão assíncrona do scraper para maior velocidade.
Usa múltiplas conexões simultâneas com rate limiting.

Uso:
    python omnibees_async_scraper.py --start 0 --end 9999 --country Brasil --workers 5
    
    # Continuar de onde parou:
    python omnibees_async_scraper.py --resume

Requisitos:
    pip install aiohttp aiofiles beautifulsoup4
"""

import asyncio
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
import json
import re
import os
import sys
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any, Set
from dataclasses import dataclass, asdict, field
from pathlib import Path
import argparse
from asyncio import Semaphore
from location_parser import parse_location_text


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('omnibees_async.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Hotel:
    id: int
    name: str
    url: str
    chain_id: int
    stars: int = 0
    rating: float = 0.0
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    country: str = ""
    description: str = ""
    check_in: str = ""
    check_out: str = ""
    email: str = ""
    phone: str = ""
    room_types: List[Dict] = field(default_factory=list)
    amenities: List[str] = field(default_factory=list)
    scraped_at: str = ""


@dataclass
class Chain:
    id: int
    name: str
    url: str
    country: Optional[str] = None
    email: str = ""
    phone: str = ""
    hotels_count: int = 0
    hotels: List[Dict] = field(default_factory=list)
    scraped_at: str = ""


class OmnibeesAsyncScraper:
    BASE_URL = "https://book.omnibees.com"
    
    OUTPUT_DIR = Path("omnibees_data")
    CHAINS_DIR = OUTPUT_DIR / "chains"
    HOTELS_DIR = OUTPUT_DIR / "hotels"
    CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint_async.json"
    RESULTS_FILE = OUTPUT_DIR / "all_chains_async.json"
    
    def __init__(self,
                 max_workers: int = 5,
                 delay: float = 0.5,
                 timeout: int = 30,
                 country_filter: Optional[str] = None):
        """
        Args:
            max_workers: Número máximo de requisições simultâneas
            delay: Delay entre batches de requisições
            timeout: Timeout das requisições
            country_filter: Filtrar por país
        """
        self.max_workers = max_workers
        self.delay = delay
        self.timeout = timeout
        self.country_filter = country_filter
        self.semaphore = Semaphore(max_workers)
        
        # Criar diretórios
        self.OUTPUT_DIR.mkdir(exist_ok=True)
        self.CHAINS_DIR.mkdir(exist_ok=True)
        self.HOTELS_DIR.mkdir(exist_ok=True)
        
        # Estatísticas
        self.stats = {
            "chains_checked": 0,
            "chains_found": 0,
            "hotels_found": 0,
            "errors": 0,
            "start_time": None
        }
        
        # Chains encontradas
        self.chains_found: List[Dict] = []
        self.processed_ids: Set[int] = set()
    
    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Faz requisição HTTP com rate limiting."""
        async with self.semaphore:
            try:
                await asyncio.sleep(self.delay)
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=self.timeout)) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status == 429:
                        logger.warning("Rate limited, aguardando...")
                        await asyncio.sleep(5)
                        return None
            except asyncio.TimeoutError:
                logger.debug(f"Timeout: {url}")
            except Exception as e:
                logger.debug(f"Erro: {e}")
                self.stats["errors"] += 1
        return None
    
    def _parse_chain(self, html: str, chain_id: int) -> Optional[Dict]:
        """Parse HTML de página de chain."""
        soup = BeautifulSoup(html, "html.parser")
        
        title = soup.find("title")
        if not title:
            return None
        
        name = title.get_text().split("|")[0].strip()
        
        if any(x in name.lower() for x in ["error", "404", "not found"]):
            return None
        
        # Detectar país
        country = None
        text = soup.get_text()
        if "Brasil" in text or "Brazil" in text:
            country = "Brasil"
        elif "Argentina" in text:
            country = "Argentina"
        elif "Chile" in text:
            country = "Chile"
        elif "México" in text or "Mexico" in text:
            country = "México"
        
        # Filtrar por país
        if self.country_filter:
            if not country or self.country_filter.lower() not in country.lower():
                return None
        
        # Encontrar hotéis
        hotel_ids = set()
        hotel_links = soup.find_all("a", href=re.compile(rf"/chain/{chain_id}/hotel/\d+"))
        for link in hotel_links:
            match = re.search(r"/hotel/(\d+)", link.get("href", ""))
            if match:
                hotel_ids.add(int(match.group(1)))
        
        if not hotel_ids:
            return None
        
        # Extrair email e telefone
        email = ""
        mailto = soup.find("a", href=re.compile(r"mailto:"))
        if mailto:
            email = mailto.get("href", "").replace("mailto:", "").split("?")[0]
        
        phone = ""
        tel = soup.find("a", href=re.compile(r"tel:"))
        if tel:
            phone = tel.get("href", "").replace("tel:", "")
        
        return {
            "id": chain_id,
            "name": name,
            "url": f"{self.BASE_URL}/chain/{chain_id}",
            "country": country,
            "email": email,
            "phone": phone,
            "hotel_ids": list(hotel_ids)
        }
    
    def _parse_hotel(self, html: str, chain_id: int, hotel_id: int) -> Optional[Hotel]:
        """Parse HTML de página de hotel."""
        soup = BeautifulSoup(html, "html.parser")
        
        title = soup.find("title")
        name = title.get_text().split("|")[0].strip() if title else f"Hotel {hotel_id}"
        
        # Descrição
        description = ""
        desc_elem = soup.find(string=re.compile(r"O Hotel", re.IGNORECASE))
        if desc_elem and desc_elem.find_parent():
            parent = desc_elem.find_parent()
            next_elem = parent.find_next_sibling() if parent else None
            if next_elem:
                description = next_elem.get_text(strip=True)[:1500]
        
        # Localização
        address = city = state = zip_code = country = ""
        loc_elem = soup.find("img", src=re.compile(r"Location"))
        if loc_elem and loc_elem.parent:
            loc_text = loc_elem.parent.get_text(separator=" ")
            location_data = parse_location_text(loc_text)
            address = location_data["address"]
            city = location_data["city"]
            state = location_data["state"]
            zip_code = location_data["zip_code"]
            country = location_data["country"]
        
        # Estrelas
        stars = len(soup.find_all("img", src=re.compile(r"star_rating")))
        
        # Rating
        rating = 0.0
        rating_elem = soup.find("img", src=re.compile(r"UserRating"))
        if rating_elem and rating_elem.next_sibling:
            try:
                rating = float(str(rating_elem.next_sibling).strip())
            except:
                pass
        
        # Room types
        room_types = []
        for img in soup.find_all("img", src=re.compile(r"RoomTypes")):
            room_name = img.get("alt", "")
            if room_name:
                room_types.append({"name": room_name, "image": img.get("src", "")})
        
        # Amenities
        amenities = []
        for item in soup.find_all(string=re.compile(r"✓")):
            text = item.parent.get_text(strip=True).replace("✓", "").strip() if item.parent else ""
            if text and len(text) > 2 and text not in amenities:
                amenities.append(text)
        
        # Email e telefone
        email = phone = ""
        mailto = soup.find("a", href=re.compile(r"mailto:"))
        if mailto:
            email = mailto.get("href", "").replace("mailto:", "").split("?")[0]
        tel = soup.find("a", href=re.compile(r"tel:"))
        if tel:
            phone = tel.get("href", "").replace("tel:", "")
        
        return Hotel(
            id=hotel_id,
            name=name,
            url=f"{self.BASE_URL}/chain/{chain_id}/hotel/{hotel_id}",
            chain_id=chain_id,
            stars=stars,
            rating=rating,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            country=country,
            description=description,
            email=email,
            phone=phone,
            room_types=room_types,
            amenities=amenities[:30],
            scraped_at=datetime.now().isoformat()
        )
    
    async def check_chain(self, session: aiohttp.ClientSession, chain_id: int) -> Optional[Dict]:
        """Verifica se chain existe e retorna info básica."""
        url = f"{self.BASE_URL}/chain/{chain_id}/hotels?lang=pt-BR"
        html = await self._fetch(session, url)
        
        if not html:
            return None
        
        return self._parse_chain(html, chain_id)
    
    async def scrape_hotel(self, session: aiohttp.ClientSession, 
                          chain_id: int, hotel_id: int) -> Optional[Hotel]:
        """Extrai dados de um hotel."""
        url = f"{self.BASE_URL}/chain/{chain_id}/hotel/{hotel_id}?lang=pt-BR"
        html = await self._fetch(session, url)
        
        if not html:
            return None
        
        return self._parse_hotel(html, chain_id, hotel_id)
    
    async def scrape_chain_complete(self, session: aiohttp.ClientSession, 
                                   chain_id: int) -> Optional[Chain]:
        """Extrai chain e todos seus hotéis."""
        chain_info = await self.check_chain(session, chain_id)
        
        if not chain_info:
            return None
        
        logger.info(f"[OK] Chain {chain_id}: {chain_info['name']} ({len(chain_info['hotel_ids'])} hoteis)")
        
        # Extrair hotéis
        hotels = []
        for hotel_id in chain_info["hotel_ids"]:
            hotel = await self.scrape_hotel(session, chain_id, hotel_id)
            if hotel:
                hotels.append(asdict(hotel))
                self.stats["hotels_found"] += 1
        
        chain = Chain(
            id=chain_id,
            name=chain_info["name"],
            url=chain_info["url"],
            country=chain_info["country"],
            email=chain_info["email"],
            phone=chain_info["phone"],
            hotels_count=len(hotels),
            hotels=hotels,
            scraped_at=datetime.now().isoformat()
        )
        
        # Salvar chain
        chain_file = self.CHAINS_DIR / f"chain_{chain_id}.json"
        async with aiofiles.open(chain_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(asdict(chain), ensure_ascii=False, indent=2))
        
        return chain
    
    async def process_batch(self, session: aiohttp.ClientSession, 
                           chain_ids: List[int]) -> List[Chain]:
        """Processa um batch de chain IDs."""
        tasks = [self.check_chain(session, cid) for cid in chain_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        chains = []
        for chain_id, result in zip(chain_ids, results):
            self.stats["chains_checked"] += 1
            self.processed_ids.add(chain_id)
            
            if isinstance(result, dict):
                # Chain encontrada, extrair hotéis
                chain = await self.scrape_chain_complete(session, chain_id)
                if chain:
                    chains.append(chain)
                    self.stats["chains_found"] += 1
                    self.chains_found.append({
                        "id": chain.id,
                        "name": chain.name,
                        "country": chain.country,
                        "hotels_count": chain.hotels_count
                    })
        
        return chains
    
    async def save_checkpoint(self):
        """Salva checkpoint."""
        checkpoint = {
            "processed_ids": list(self.processed_ids),
            "chains_found": self.chains_found,
            "stats": self.stats,
            "timestamp": datetime.now().isoformat()
        }
        
        async with aiofiles.open(self.CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(checkpoint, ensure_ascii=False, indent=2))
    
    async def load_checkpoint(self) -> bool:
        """Carrega checkpoint se existir."""
        if self.CHECKPOINT_FILE.exists():
            async with aiofiles.open(self.CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                checkpoint = json.loads(await f.read())
                self.processed_ids = set(checkpoint.get("processed_ids", []))
                self.chains_found = checkpoint.get("chains_found", [])
                self.stats = checkpoint.get("stats", self.stats)
                return True
        return False
    
    async def save_results(self):
        """Salva resultados finais."""
        results = {
            "metadata": {
                "scraped_at": datetime.now().isoformat(),
                "country_filter": self.country_filter,
                "total_chains": len(self.chains_found),
                "total_hotels": self.stats["hotels_found"],
                "stats": self.stats
            },
            "chains": self.chains_found
        }
        
        async with aiofiles.open(self.RESULTS_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(results, ensure_ascii=False, indent=2))
    
    async def run(self, start_id: int = 0, end_id: int = 9999, 
                  resume: bool = False, batch_size: int = 50):
        """Executa varredura."""
        
        if resume:
            loaded = await self.load_checkpoint()
            if loaded:
                logger.info(f"Checkpoint carregado. IDs processados: {len(self.processed_ids)}")
                logger.info(f"Chains encontradas: {len(self.chains_found)}")
        
        self.stats["start_time"] = datetime.now().isoformat()
        
        # IDs a processar
        all_ids = [i for i in range(start_id, end_id + 1) if i not in self.processed_ids]
        total = len(all_ids)
        
        logger.info("=" * 60)
        logger.info("OMNIBEES ASYNC SCRAPER")
        logger.info("=" * 60)
        logger.info(f"IDs a processar: {total}")
        logger.info(f"Workers: {self.max_workers}")
        logger.info(f"Filtro de país: {self.country_filter or 'Nenhum'}")
        logger.info("=" * 60)
        
        connector = aiohttp.TCPConnector(limit=self.max_workers * 2)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
        }
        
        async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
            try:
                for i in range(0, total, batch_size):
                    batch = all_ids[i:i + batch_size]
                    progress = min(i + batch_size, total) / total * 100
                    
                    logger.info(f"[{progress:.1f}%] Processando batch {i//batch_size + 1} "
                              f"(IDs {batch[0]}-{batch[-1]}) - "
                              f"Chains: {self.stats['chains_found']}, "
                              f"Hotéis: {self.stats['hotels_found']}")
                    
                    await self.process_batch(session, batch)
                    
                    # Checkpoint a cada 500 IDs
                    if (i + batch_size) % 500 == 0:
                        await self.save_checkpoint()
                        await self.save_results()
                        logger.info("[SAVE] Checkpoint salvo")
            
            except KeyboardInterrupt:
                logger.info("\n[WARN] Interrompido. Salvando...")
            
            finally:
                await self.save_checkpoint()
                await self.save_results()
        
        self._print_summary()
    
    def _print_summary(self):
        """Imprime resumo."""
        logger.info("\n" + "=" * 60)
        logger.info("RESUMO FINAL")
        logger.info("=" * 60)
        logger.info(f"IDs verificados: {self.stats['chains_checked']}")
        logger.info(f"Chains encontradas: {self.stats['chains_found']}")
        logger.info(f"Hotéis encontrados: {self.stats['hotels_found']}")
        logger.info(f"Erros: {self.stats['errors']}")
        logger.info(f"Resultados em: {self.RESULTS_FILE}")
        logger.info("=" * 60)
        
        if self.chains_found:
            logger.info("\nTop chains encontradas:")
            for chain in sorted(self.chains_found, key=lambda x: x['hotels_count'], reverse=True)[:20]:
                logger.info(f"  [{chain['id']:>5}] {chain['name'][:40]:<40} "
                          f"({chain['hotels_count']} hotéis)")


def main():
    parser = argparse.ArgumentParser(description="Omnibees Async Scraper")
    parser.add_argument("--start", type=int, default=0, help="ID inicial")
    parser.add_argument("--end", type=int, default=9999, help="ID final")
    parser.add_argument("--country", type=str, default=None, help="Filtrar por país")
    parser.add_argument("--workers", type=int, default=5, help="Workers simultâneos")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay entre requisições")
    parser.add_argument("--batch", type=int, default=50, help="Tamanho do batch")
    parser.add_argument("--resume", action="store_true", help="Continuar do checkpoint")
    
    args = parser.parse_args()
    
    scraper = OmnibeesAsyncScraper(
        max_workers=args.workers,
        delay=args.delay,
        country_filter=args.country
    )
    
    asyncio.run(scraper.run(
        start_id=args.start,
        end_id=args.end,
        resume=args.resume,
        batch_size=args.batch
    ))


if __name__ == "__main__":
    main()
