#!/usr/bin/env python3
"""
Omnibees Complete Scraper
=========================
Faz varredura sequencial de todas as chains (0000-9999) no Omnibees
e extrai dados completos de todos os hotéis encontrados.

Uso:
    python omnibees_complete_scraper.py --start 0 --end 9999 --country Brasil
    
    # Continuar de onde parou:
    python omnibees_complete_scraper.py --resume

Features:
    - Varredura sequencial de IDs
    - Extração completa de dados dos hotéis
    - Salvamento incremental (checkpoint a cada 100 chains)
    - Rate limiting configurável
    - Filtro por país
    - Logs detalhados
    - Retry automático em caso de erro
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import os
import sys
import logging
from datetime import datetime
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict, field
from pathlib import Path
import argparse
from urllib.parse import urljoin
from location_parser import parse_location_text


# Configuracao de logging com encoding UTF-8 para Windows
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('omnibees_scraper.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


@dataclass
class RoomType:
    name: str
    image_url: Optional[str] = None


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
    neighborhood: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    description: str = ""
    check_in: str = ""
    check_out: str = ""
    breakfast_start: str = ""
    breakfast_end: str = ""
    email: str = ""
    phone: str = ""
    logo_url: str = ""
    room_types: List[Dict] = field(default_factory=list)
    amenities_general: List[str] = field(default_factory=list)
    amenities_food: List[str] = field(default_factory=list)
    amenities_wellness: List[str] = field(default_factory=list)
    amenities_events: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    scraped_at: str = ""


@dataclass
class Chain:
    id: int
    name: str
    url: str
    country: Optional[str] = None
    logo_url: str = ""
    email: str = ""
    phone: str = ""
    hotels_count: int = 0
    hotels: List[Dict] = field(default_factory=list)
    scraped_at: str = ""


class OmnibeesCompleteScraper:
    BASE_URL = "https://book.omnibees.com"
    
    # Diretórios de saída
    OUTPUT_DIR = Path("omnibees_data")
    CHAINS_DIR = OUTPUT_DIR / "chains"
    HOTELS_DIR = OUTPUT_DIR / "hotels"
    CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
    RESULTS_FILE = OUTPUT_DIR / "all_chains.json"
    
    def __init__(self, 
                 delay: float = 1.0,
                 retry_attempts: int = 3,
                 retry_delay: float = 5.0,
                 timeout: int = 30,
                 country_filter: Optional[str] = None):
        """
        Inicializa o scraper.
        
        Args:
            delay: Segundos entre requisições
            retry_attempts: Número de tentativas em caso de erro
            retry_delay: Segundos entre tentativas
            timeout: Timeout das requisições em segundos
            country_filter: Filtrar por país (ex: "Brasil")
        """
        self.delay = delay
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.country_filter = country_filter
        
        # Criar diretórios
        self.OUTPUT_DIR.mkdir(exist_ok=True)
        self.CHAINS_DIR.mkdir(exist_ok=True)
        self.HOTELS_DIR.mkdir(exist_ok=True)
        
        # Sessão HTTP
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
        
        # Estatísticas
        self.stats = {
            "chains_checked": 0,
            "chains_found": 0,
            "chains_filtered": 0,
            "hotels_found": 0,
            "errors": 0,
            "start_time": None,
            "last_checkpoint": None
        }
    
    def _request(self, url: str) -> Optional[BeautifulSoup]:
        """Faz requisição com retry e rate limiting."""
        for attempt in range(self.retry_attempts):
            try:
                time.sleep(self.delay)
                response = self.session.get(url, timeout=self.timeout)
                
                if response.status_code == 200:
                    return BeautifulSoup(response.text, "html.parser")
                elif response.status_code == 404:
                    return None
                elif response.status_code == 429:  # Too Many Requests
                    logger.warning(f"Rate limited. Aguardando {self.retry_delay * 2}s...")
                    time.sleep(self.retry_delay * 2)
                else:
                    logger.warning(f"Status {response.status_code} para {url}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout na tentativa {attempt + 1} para {url}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erro na tentativa {attempt + 1}: {e}")
            
            if attempt < self.retry_attempts - 1:
                time.sleep(self.retry_delay)
        
        self.stats["errors"] += 1
        return None
    
    def _extract_text(self, soup: BeautifulSoup, pattern: str) -> str:
        """Extrai texto baseado em padrão regex."""
        element = soup.find(string=re.compile(pattern, re.IGNORECASE))
        if element and element.parent:
            return element.parent.get_text(strip=True)
        return ""
    
    def _extract_email(self, soup: BeautifulSoup) -> str:
        """Extrai email da página."""
        # Tentar mailto link
        mailto = soup.find("a", href=re.compile(r"mailto:"))
        if mailto:
            href = mailto.get("href", "")
            return href.replace("mailto:", "").split("?")[0]
        
        # Tentar padrão de email no texto
        text = soup.get_text()
        match = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", text)
        return match.group(0) if match else ""
    
    def _extract_phone(self, soup: BeautifulSoup) -> str:
        """Extrai telefone da página."""
        tel = soup.find("a", href=re.compile(r"tel:"))
        if tel:
            return tel.get("href", "").replace("tel:", "")
        return ""
    
    def _count_stars(self, soup: BeautifulSoup) -> int:
        """Conta estrelas do hotel."""
        stars = soup.find_all("img", src=re.compile(r"star_rating"))
        return len(stars)
    
    def _extract_rating(self, soup: BeautifulSoup) -> float:
        """Extrai avaliação do usuário."""
        rating_elem = soup.find("img", src=re.compile(r"UserRating"))
        if rating_elem:
            next_text = rating_elem.next_sibling
            if next_text:
                try:
                    return float(str(next_text).strip())
                except ValueError:
                    pass
        return 0.0
    
    def _extract_country(self, soup: BeautifulSoup) -> Optional[str]:
        """Detecta o país dos hotéis na chain."""
        countries = {
            "Brasil": ["Brasil", "Brazil"],
            "Argentina": ["Argentina"],
            "Chile": ["Chile"],
            "México": ["México", "Mexico"],
            "Colômbia": ["Colombia", "Colômbia"],
            "Peru": ["Peru", "Perú"],
            "Portugal": ["Portugal"],
            "Espanha": ["España", "Spain", "Espanha"],
            "Uruguai": ["Uruguay", "Uruguai"],
            "Paraguai": ["Paraguay", "Paraguai"],
        }
        
        text = soup.get_text()
        
        for country, patterns in countries.items():
            for pattern in patterns:
                if pattern in text:
                    return country
        
        return None
    
    def _extract_amenities(self, soup: BeautifulSoup, section_keywords: List[str]) -> List[str]:
        """Extrai amenidades de uma seção específica."""
        amenities = []
        
        for keyword in section_keywords:
            sections = soup.find_all(string=re.compile(keyword, re.IGNORECASE))
            for section in sections:
                parent = section.find_parent()
                if parent:
                    # Procurar itens com checkmark (Unicode U+2713)
                    container = parent.find_parent() or parent
                    items = container.find_all(string=re.compile(r"\u2713"))
                    for item in items[:30]:
                        text = ""
                        if item.parent:
                            text = item.parent.get_text(strip=True)
                        text = text.replace("\u2713", "").strip()
                        if text and len(text) > 2 and text not in amenities:
                            amenities.append(text)
        
        return amenities
    
    def check_chain_exists(self, chain_id: int) -> Optional[Dict]:
        """
        Verifica se uma chain existe e retorna informações básicas.
        
        Returns:
            Dict com informações básicas ou None se não existir
        """
        url = f"{self.BASE_URL}/chain/{chain_id}/hotels?lang=pt-BR"
        soup = self._request(url)
        
        if not soup:
            return None
        
        # Verificar se é página válida
        title = soup.find("title")
        if not title:
            return None
        
        name = title.get_text().split("|")[0].strip()
        
        # Pular páginas de erro
        if any(x in name.lower() for x in ["error", "404", "not found", "página"]):
            return None
        
        # Detectar país
        country = self._extract_country(soup)
        
        # Aplicar filtro de país
        if self.country_filter:
            if not country or self.country_filter.lower() not in country.lower():
                return None
        
        # Contar hotéis
        hotel_links = soup.find_all("a", href=re.compile(rf"/chain/{chain_id}/hotel/\d+"))
        hotel_ids = set()
        for link in hotel_links:
            match = re.search(r"/hotel/(\d+)", link.get("href", ""))
            if match:
                hotel_ids.add(int(match.group(1)))
        
        if not hotel_ids:
            return None
        
        # Extrair logo
        logo_img = soup.find("img", src=re.compile(r"BEImages|logo", re.IGNORECASE))
        logo_url = logo_img.get("src", "") if logo_img else ""
        
        return {
            "id": chain_id,
            "name": name,
            "url": f"{self.BASE_URL}/chain/{chain_id}",
            "country": country,
            "logo_url": logo_url,
            "email": self._extract_email(soup),
            "phone": self._extract_phone(soup),
            "hotel_ids": list(hotel_ids)
        }
    
    def scrape_hotel(self, chain_id: int, hotel_id: int) -> Optional[Hotel]:
        """Extrai dados completos de um hotel."""
        url = f"{self.BASE_URL}/chain/{chain_id}/hotel/{hotel_id}?lang=pt-BR"
        soup = self._request(url)
        
        if not soup:
            return None
        
        # Nome
        title = soup.find("title")
        name = title.get_text().split("|")[0].strip() if title else f"Hotel {hotel_id}"
        
        # Descrição
        description = ""
        desc_section = soup.find(string=re.compile(r"O Hotel", re.IGNORECASE))
        if desc_section:
            parent = desc_section.find_parent()
            if parent:
                next_elem = parent.find_next_sibling()
                if next_elem:
                    description = next_elem.get_text(strip=True)[:2000]
        
        # Localização
        address = ""
        city = ""
        state = ""
        zip_code = ""
        country = ""
        neighborhood = ""
        
        location_elem = soup.find("img", src=re.compile(r"Location"))
        if location_elem and location_elem.parent:
            location_text = location_elem.parent.get_text(separator=" | ")
            location_data = parse_location_text(location_text)
            address = location_data["address"]
            city = location_data["city"]
            state = location_data["state"]
            zip_code = location_data["zip_code"]
            country = location_data["country"]
        
        # Horários
        def extract_time(pattern: str) -> str:
            elem = soup.find(string=re.compile(pattern, re.IGNORECASE))
            if elem and elem.parent:
                text = elem.parent.get_text()
                time_match = re.search(r"(\d{1,2}[h:]\d{2})", text)
                if time_match:
                    return time_match.group(1)
            return ""
        
        # Tipos de quarto
        room_types = []
        room_imgs = soup.find_all("img", src=re.compile(r"RoomTypes"))
        for img in room_imgs:
            room_name = img.get("alt", "")
            room_img = img.get("src", "")
            if room_name:
                room_types.append({
                    "name": room_name,
                    "image_url": room_img
                })
        
        # Imagens do hotel
        images = []
        gallery_imgs = soup.find_all("img", src=re.compile(r"media\.omnibees\.com"))
        for img in gallery_imgs[:20]:
            src = img.get("src", "")
            if src and "RoomTypes" not in src and src not in images:
                images.append(src)
        
        hotel = Hotel(
            id=hotel_id,
            name=name,
            url=url,
            chain_id=chain_id,
            stars=self._count_stars(soup),
            rating=self._extract_rating(soup),
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            country=country,
            neighborhood=neighborhood,
            description=description,
            check_in=extract_time("Check-in"),
            check_out=extract_time("Check-out"),
            breakfast_start=extract_time("Café.*manhã|Breakfast"),
            breakfast_end="",
            email=self._extract_email(soup),
            phone=self._extract_phone(soup),
            logo_url="",
            room_types=room_types,
            amenities_general=self._extract_amenities(soup, ["Serviços Gerais", "General Services"]),
            amenities_food=self._extract_amenities(soup, ["Restaurantes", "Bares", "Restaurant", "Bar"]),
            amenities_wellness=self._extract_amenities(soup, ["Bem-estar", "Esportes", "Wellness", "Sports"]),
            amenities_events=self._extract_amenities(soup, ["Eventos", "Conferências", "Events", "Conference"]),
            images=images,
            scraped_at=datetime.now().isoformat()
        )
        
        return hotel
    
    def scrape_chain(self, chain_id: int) -> Optional[Chain]:
        """Extrai dados completos de uma chain e todos seus hotéis."""
        logger.info(f"Verificando chain {chain_id}...")
        
        # Verificar se chain existe
        chain_info = self.check_chain_exists(chain_id)
        
        if not chain_info:
            return None
        
        logger.info(f"  [OK] Encontrada: {chain_info['name']} ({len(chain_info['hotel_ids'])} hoteis)")
        
        # Extrair dados de cada hotel
        hotels = []
        for hotel_id in chain_info["hotel_ids"]:
            logger.info(f"    Extraindo hotel {hotel_id}...")
            hotel = self.scrape_hotel(chain_id, hotel_id)
            if hotel:
                hotels.append(asdict(hotel))
                self.stats["hotels_found"] += 1
                
                # Salvar hotel individual
                hotel_file = self.HOTELS_DIR / f"hotel_{chain_id}_{hotel_id}.json"
                with open(hotel_file, "w", encoding="utf-8") as f:
                    json.dump(asdict(hotel), f, ensure_ascii=False, indent=2)
        
        chain = Chain(
            id=chain_id,
            name=chain_info["name"],
            url=chain_info["url"],
            country=chain_info["country"],
            logo_url=chain_info["logo_url"],
            email=chain_info["email"],
            phone=chain_info["phone"],
            hotels_count=len(hotels),
            hotels=hotels,
            scraped_at=datetime.now().isoformat()
        )
        
        # Salvar chain
        chain_file = self.CHAINS_DIR / f"chain_{chain_id}.json"
        with open(chain_file, "w", encoding="utf-8") as f:
            json.dump(asdict(chain), f, ensure_ascii=False, indent=2)
        
        return chain
    
    def save_checkpoint(self, current_id: int, chains_found: List[Dict]):
        """Salva checkpoint para continuar depois."""
        checkpoint = {
            "last_id": current_id,
            "chains_found": chains_found,
            "stats": self.stats,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(self.CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        
        self.stats["last_checkpoint"] = current_id
        logger.info(f"  [SAVE] Checkpoint salvo no ID {current_id}")
    
    def load_checkpoint(self) -> Optional[Dict]:
        """Carrega checkpoint se existir."""
        if self.CHECKPOINT_FILE.exists():
            with open(self.CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        return None
    
    def run(self, start_id: int = 0, end_id: int = 9999, resume: bool = False):
        """
        Executa a varredura completa.
        
        Args:
            start_id: ID inicial
            end_id: ID final
            resume: Se True, continua do último checkpoint
        """
        chains_found = []
        
        # Verificar se deve continuar de checkpoint
        if resume:
            checkpoint = self.load_checkpoint()
            if checkpoint:
                start_id = checkpoint["last_id"] + 1
                chains_found = checkpoint["chains_found"]
                self.stats = checkpoint["stats"]
                logger.info(f"Continuando do checkpoint: ID {start_id}")
                logger.info(f"Chains já encontradas: {len(chains_found)}")
        
        self.stats["start_time"] = datetime.now().isoformat()
        total = end_id - start_id + 1
        
        logger.info("=" * 60)
        logger.info("OMNIBEES COMPLETE SCRAPER")
        logger.info("=" * 60)
        logger.info(f"Range: {start_id} - {end_id} ({total} IDs)")
        logger.info(f"Filtro de país: {self.country_filter or 'Nenhum'}")
        logger.info(f"Delay entre requisições: {self.delay}s")
        logger.info("=" * 60)
        
        try:
            for chain_id in range(start_id, end_id + 1):
                self.stats["chains_checked"] += 1
                
                # Progresso
                progress = (chain_id - start_id + 1) / total * 100
                if chain_id % 10 == 0:
                    logger.info(f"[{progress:.1f}%] Verificando ID {chain_id}... "
                              f"(Encontradas: {self.stats['chains_found']}, "
                              f"Hotéis: {self.stats['hotels_found']})")
                
                # Scrape chain
                chain = self.scrape_chain(chain_id)
                
                if chain:
                    self.stats["chains_found"] += 1
                    chains_found.append({
                        "id": chain.id,
                        "name": chain.name,
                        "country": chain.country,
                        "hotels_count": chain.hotels_count
                    })
                
                # Checkpoint a cada 100 IDs
                if chain_id % 100 == 0:
                    self.save_checkpoint(chain_id, chains_found)
                    self._save_results(chains_found)
        
        except KeyboardInterrupt:
            logger.info("\n[WARN] Interrompido pelo usuario. Salvando progresso...")
            self.save_checkpoint(chain_id, chains_found)
        
        finally:
            self._save_results(chains_found)
            self._print_summary(chains_found)
    
    def _save_results(self, chains_found: List[Dict]):
        """Salva resultados consolidados."""
        results = {
            "metadata": {
                "scraped_at": datetime.now().isoformat(),
                "country_filter": self.country_filter,
                "total_chains": len(chains_found),
                "total_hotels": self.stats["hotels_found"],
                "stats": self.stats
            },
            "chains": chains_found
        }
        
        with open(self.RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    
    def _print_summary(self, chains_found: List[Dict]):
        """Imprime resumo final."""
        logger.info("\n" + "=" * 60)
        logger.info("RESUMO FINAL")
        logger.info("=" * 60)
        logger.info(f"IDs verificados: {self.stats['chains_checked']}")
        logger.info(f"Chains encontradas: {self.stats['chains_found']}")
        logger.info(f"Total de hotéis: {self.stats['hotels_found']}")
        logger.info(f"Erros: {self.stats['errors']}")
        logger.info(f"Resultados salvos em: {self.RESULTS_FILE}")
        logger.info("=" * 60)
        
        if chains_found:
            logger.info("\nChains encontradas:")
            logger.info("-" * 60)
            for chain in chains_found[:50]:  # Mostrar primeiras 50
                logger.info(f"  [{chain['id']:>5}] {chain['name'][:40]:<40} "
                          f"({chain['hotels_count']} hotéis) - {chain['country']}")
            
            if len(chains_found) > 50:
                logger.info(f"  ... e mais {len(chains_found) - 50} chains")


def main():
    parser = argparse.ArgumentParser(
        description="Omnibees Complete Scraper - Varredura completa de chains e hotéis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Varredura completa de 0 a 9999, filtrando Brasil
  python omnibees_complete_scraper.py --start 0 --end 9999 --country Brasil
  
  # Varredura de um range específico
  python omnibees_complete_scraper.py --start 1000 --end 2000
  
  # Continuar de onde parou
  python omnibees_complete_scraper.py --resume
  
  # Varredura rápida com menos delay
  python omnibees_complete_scraper.py --delay 0.5
        """
    )
    
    parser.add_argument("--start", type=int, default=0,
                       help="ID inicial (default: 0)")
    parser.add_argument("--end", type=int, default=9999,
                       help="ID final (default: 9999)")
    parser.add_argument("--country", type=str, default=None,
                       help="Filtrar por país (ex: 'Brasil', 'Argentina')")
    parser.add_argument("--delay", type=float, default=1.0,
                       help="Delay entre requisições em segundos (default: 1.0)")
    parser.add_argument("--resume", action="store_true",
                       help="Continuar do último checkpoint")
    parser.add_argument("--timeout", type=int, default=30,
                       help="Timeout das requisições (default: 30s)")
    
    args = parser.parse_args()
    
    scraper = OmnibeesCompleteScraper(
        delay=args.delay,
        timeout=args.timeout,
        country_filter=args.country
    )
    
    scraper.run(
        start_id=args.start,
        end_id=args.end,
        resume=args.resume
    )


if __name__ == "__main__":
    main()
