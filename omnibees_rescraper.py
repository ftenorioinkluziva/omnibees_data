#!/usr/bin/env python3
"""
Omnibees Re-Scraper
===================
Re-processa hotéis já salvos para preencher campos vazios.

Uso:
    python omnibees_rescraper.py
    python omnibees_rescraper.py --dry-run  # Apenas mostra o que seria atualizado
    python omnibees_rescraper.py --hotel 1169  # Re-processar hotel específico
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
import logging
from datetime import datetime
from pathlib import Path
import argparse
from typing import Optional, Dict, List, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('omnibees_rescraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OmnibeesRescraper:
    BASE_URL = "https://book.omnibees.com"
    HOTELS_DIR = Path("omnibees_data/hotels")

    def __init__(self, delay: float = 1.5, timeout: int = 30):
        self.delay = delay
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        })
        self.stats = {"processed": 0, "updated": 0, "errors": 0, "skipped": 0}

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Busca página com rate limiting."""
        time.sleep(self.delay)
        try:
            response = self.session.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            logger.error(f"Erro ao buscar {url}: {e}")
        return None

    def extract_location(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extrai dados de localização melhorados."""
        location = {
            "address": "",
            "city": "",
            "state": "",
            "zip_code": "",
            "country": "",
            "neighborhood": ""
        }

        # Buscar ícone de localização (múltiplos padrões)
        location_patterns = [
            re.compile(r"Location", re.IGNORECASE),
            re.compile(r"location", re.IGNORECASE),
            re.compile(r"Contact_Location"),
        ]

        location_elem = None
        for pattern in location_patterns:
            location_elem = soup.find("img", src=pattern)
            if location_elem:
                break

        if not location_elem:
            location_elem = soup.find("img", src=re.compile(r"icon.*Location", re.IGNORECASE))

        if location_elem:
            parent = location_elem.parent
            for _ in range(5):
                if parent:
                    text = parent.get_text(separator=" ", strip=True)
                    if len(text) > 20 and ("," in text or "Brasil" in text):
                        break
                    parent = parent.parent

            if parent:
                text = parent.get_text(separator=" ", strip=True)
                text = re.sub(r'\s+', ' ', text)
                text = text.replace("Ver no Mapa", "").replace("(", "").replace(")", "").strip()

                # Extrair CEP brasileiro
                cep_match = re.search(r'(\d{5}-?\d{3})', text)
                if cep_match:
                    location["zip_code"] = cep_match.group(1)

                # Detectar país
                countries_map = {
                    "Brasil": ["Brasil", "Brazil"],
                    "Argentina": ["Argentina"],
                    "Portugal": ["Portugal"],
                    "Chile": ["Chile"],
                    "México": ["México", "Mexico"],
                    "Colômbia": ["Colombia", "Colômbia"],
                    "Uruguai": ["Uruguay", "Uruguai"],
                }
                for country, patterns in countries_map.items():
                    if any(p in text for p in patterns):
                        location["country"] = country
                        break

                # Separar por vírgulas - formato: "Rua X, 123 , Cidade , CEP , País"
                parts = [p.strip() for p in text.split(",")]
                parts = [p for p in parts if p and len(p) > 1]

                # Filtrar partes que são apenas CEP ou país
                address_parts = []
                for p in parts:
                    is_cep = re.match(r'^\d{5}-?\d{3}$', p.strip())
                    is_country = any(p.strip() in patterns for patterns in countries_map.values())
                    if not is_cep and not is_country:
                        address_parts.append(p.strip())

                # Identificar cidade vs endereço
                # Se uma parte é apenas um número, provavelmente é parte do endereço anterior
                merged_parts = []
                for i, part in enumerate(address_parts):
                    if part.isdigit() and merged_parts:
                        merged_parts[-1] = f"{merged_parts[-1]}, {part}"
                    else:
                        merged_parts.append(part)

                # Heurística: cidade geralmente é uma única palavra ou nome próprio
                # Endereço geralmente tem "Rua", "Av", número, etc.
                if merged_parts:
                    # Primeira parte é o endereço (pode incluir número)
                    location["address"] = merged_parts[0]

                    # Identificar cidade nas partes restantes
                    for part in merged_parts[1:]:
                        part_lower = part.lower()
                        # Se parece ser uma cidade (não começa com número, não é "rua", "av", etc.)
                        if not part[0].isdigit() and not any(x in part_lower for x in ["rua", "av.", "avenida", "praça", "travessa"]):
                            # Verificar se é um estado (2 letras)
                            if len(part) == 2 and part.isalpha():
                                location["state"] = part.upper()
                            elif not location["city"]:
                                location["city"] = part
                            elif not location["neighborhood"]:
                                location["neighborhood"] = part

        return location

    def extract_times(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extrai horários de check-in, check-out e café da manhã."""
        times = {
            "check_in": "",
            "check_out": "",
            "breakfast_start": "",
            "breakfast_end": ""
        }

        text = soup.get_text()

        # Check-in: múltiplos padrões
        checkin_patterns = [
            r'Check-?in[:\s]*(?:a partir d[aeo]s?\s*)?(\d{1,2}[h:]\d{2}|\d{1,2}:\d{2})',
            r'Check-?in[:\s]*(?:from\s*)?(\d{1,2}[h:]\d{2}|\d{1,2}:\d{2})',
            r'Entrada[:\s]*(?:a partir d[aeo]s?\s*)?(\d{1,2}[h:]\d{2}|\d{1,2}:\d{2})',
        ]
        for pattern in checkin_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                times["check_in"] = match.group(1).replace("h", ":")
                break

        # Check-out
        checkout_patterns = [
            r'Check-?out[:\s]*(?:até\s*)?(\d{1,2}[h:]\d{2}|\d{1,2}:\d{2})',
            r'Check-?out[:\s]*(?:until\s*)?(\d{1,2}[h:]\d{2}|\d{1,2}:\d{2})',
            r'Saída[:\s]*(?:até\s*)?(\d{1,2}[h:]\d{2}|\d{1,2}:\d{2})',
        ]
        for pattern in checkout_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                times["check_out"] = match.group(1).replace("h", ":")
                break

        # Café da manhã
        breakfast_patterns = [
            r'Café\s*(?:da\s*)?manhã[:\s]*(?:d[aeo]s?\s*)?(\d{1,2}[h:]\d{2})\s*(?:às?|a|to|-)\s*(\d{1,2}[h:]\d{2})',
            r'Breakfast[:\s]*(?:from\s*)?(\d{1,2}[h:]\d{2})\s*(?:to|-)\s*(\d{1,2}[h:]\d{2})',
            r'(\d{1,2}[h:]\d{2})\s*(?:às?|a|to|-)\s*(\d{1,2}[h:]\d{2}).*?(?:café|breakfast)',
        ]
        for pattern in breakfast_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                times["breakfast_start"] = match.group(1).replace("h", ":")
                times["breakfast_end"] = match.group(2).replace("h", ":")
                break

        return times

    def extract_contact(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extrai email e telefone."""
        contact = {"email": "", "phone": ""}

        # Email via mailto (todos os links mailto na página)
        mailtos = soup.find_all("a", href=re.compile(r"mailto:", re.IGNORECASE))
        for mailto in mailtos:
            href = mailto.get("href", "")
            email = href.replace("mailto:", "").split("?")[0].strip()
            # Preferir emails com domínios específicos do hotel
            if email and "@" in email:
                if not contact["email"]:
                    contact["email"] = email
                # Se encontrar email com domínio que não seja genérico, preferir
                if not any(x in email.lower() for x in ["@gmail", "@hotmail", "@yahoo", "@outlook"]):
                    contact["email"] = email
                    break

        # Email via ícone de email
        if not contact["email"]:
            email_icon = soup.find("img", src=re.compile(r"Mail|Email|Contact.*Mail", re.IGNORECASE))
            if email_icon:
                parent = email_icon.parent
                for _ in range(5):
                    if parent:
                        # Procurar link mailto dentro do parent
                        mailto = parent.find("a", href=re.compile(r"mailto:"))
                        if mailto:
                            contact["email"] = mailto.get("href", "").replace("mailto:", "").split("?")[0]
                            break
                        # Procurar texto de email
                        text = parent.get_text(strip=True)
                        email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
                        if email_match:
                            contact["email"] = email_match.group(0)
                            break
                        parent = parent.parent

        # Email via regex no texto completo
        if not contact["email"]:
            text = soup.get_text()
            emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            for email in emails:
                email_lower = email.lower()
                if not any(x in email_lower for x in ["@media", "@import", "@font", "@keyframe", ".png", ".jpg", ".svg"]):
                    contact["email"] = email
                    break

        # Telefone via tel:
        tel = soup.find("a", href=re.compile(r"tel:", re.IGNORECASE))
        if tel:
            contact["phone"] = tel.get("href", "").replace("tel:", "").strip()

        # Telefone via ícone
        if not contact["phone"]:
            phone_icon = soup.find("img", src=re.compile(r"Phone|Telephone", re.IGNORECASE))
            if phone_icon:
                parent = phone_icon.parent
                for _ in range(5):
                    if parent:
                        text = parent.get_text(strip=True)
                        phone_match = re.search(r'\(?\d{2,3}\)?[\s.-]?\d{4,5}[\s.-]?\d{4}', text)
                        if phone_match:
                            contact["phone"] = phone_match.group(0)
                            break
                        parent = parent.parent

        return contact

    def extract_rating(self, soup: BeautifulSoup) -> float:
        """Extrai avaliação do usuário."""
        # Padrão: número seguido de /10 ou próximo a "avaliação"
        text = soup.get_text()

        # Padrão X.X/10 ou X,X/10
        rating_match = re.search(r'(\d+[.,]\d+)\s*/\s*10', text)
        if rating_match:
            return float(rating_match.group(1).replace(",", "."))

        # Buscar elemento específico de rating
        rating_elem = soup.find(class_=re.compile(r"rating|score|avaliacao", re.IGNORECASE))
        if rating_elem:
            rating_text = rating_elem.get_text()
            match = re.search(r'(\d+[.,]?\d*)', rating_text)
            if match:
                return float(match.group(1).replace(",", "."))

        return 0.0

    def extract_stars(self, soup: BeautifulSoup) -> int:
        """Conta estrelas do hotel."""
        # Buscar imagens de estrela
        stars = soup.find_all("img", src=re.compile(r"star", re.IGNORECASE))
        if stars:
            return len(stars)

        # Buscar no texto "X estrelas"
        text = soup.get_text()
        match = re.search(r'(\d)\s*estrelas?', text, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return 0

    def extract_amenities(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Extrai amenidades organizadas por categoria."""
        amenities = {
            "general": [],
            "food": [],
            "wellness": [],
            "events": []
        }

        # Mapear palavras-chave para categorias
        category_keywords = {
            "general": ["Serviços Gerais", "General", "Geral", "Recepção", "WiFi", "Estacionamento"],
            "food": ["Restaurante", "Bar", "Café", "Alimentação", "Food", "Gastronomia"],
            "wellness": ["Piscina", "Spa", "Academia", "Fitness", "Bem-estar", "Wellness", "Pool"],
            "events": ["Eventos", "Conferência", "Reunião", "Business", "Convenção", "Sala"]
        }

        # Buscar todas as seções de amenidades
        # Padrão comum: checkmark (✓) seguido do nome da amenidade
        checkmarks = soup.find_all(string=re.compile(r'[✓✔☑]'))

        for check in checkmarks:
            parent = check.parent
            if parent:
                text = parent.get_text(strip=True)
                text = re.sub(r'[✓✔☑]', '', text).strip()

                if text and len(text) > 2 and len(text) < 100:
                    # Categorizar
                    categorized = False
                    for category, keywords in category_keywords.items():
                        for keyword in keywords:
                            if keyword.lower() in text.lower():
                                if text not in amenities[category]:
                                    amenities[category].append(text)
                                categorized = True
                                break
                        if categorized:
                            break

                    if not categorized and text not in amenities["general"]:
                        amenities["general"].append(text)

        # Buscar também por listas dentro de seções específicas
        sections = soup.find_all(["div", "section"], class_=re.compile(r"amenities|facilities|services", re.IGNORECASE))
        for section in sections:
            items = section.find_all("li")
            for item in items:
                text = item.get_text(strip=True)
                if text and len(text) > 2 and text not in amenities["general"]:
                    amenities["general"].append(text)

        return amenities

    def extract_logo(self, soup: BeautifulSoup) -> str:
        """Extrai URL do logo do hotel."""
        # Logo geralmente está no header ou em BEImages
        logo = soup.find("img", src=re.compile(r"BEImages.*logo|logo.*BEImages", re.IGNORECASE))
        if logo:
            return logo.get("src", "")

        # Fallback: primeira imagem de BEImages
        be_img = soup.find("img", src=re.compile(r"BEImages"))
        if be_img:
            return be_img.get("src", "")

        return ""

    def extract_coordinates(self, soup: BeautifulSoup) -> Dict[str, Optional[float]]:
        """Tenta extrair coordenadas do mapa."""
        coords = {"latitude": None, "longitude": None}

        # Buscar em links do Google Maps
        map_link = soup.find("a", href=re.compile(r"maps\.google|google\.com/maps"))
        if map_link:
            href = map_link.get("href", "")
            # Padrão: @-XX.XXXXX,-XX.XXXXX ou q=XX.XXXXX,XX.XXXXX
            match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', href)
            if not match:
                match = re.search(r'q=(-?\d+\.\d+),(-?\d+\.\d+)', href)
            if match:
                coords["latitude"] = float(match.group(1))
                coords["longitude"] = float(match.group(2))

        # Buscar em data attributes
        map_elem = soup.find(attrs={"data-lat": True, "data-lng": True})
        if map_elem:
            try:
                coords["latitude"] = float(map_elem.get("data-lat"))
                coords["longitude"] = float(map_elem.get("data-lng"))
            except (ValueError, TypeError):
                pass

        return coords

    def scrape_hotel_details(self, chain_id: int, hotel_id: int) -> Optional[Dict[str, Any]]:
        """Busca todos os detalhes de um hotel."""
        url = f"{self.BASE_URL}/chain/{chain_id}/hotel/{hotel_id}?lang=pt-BR"
        soup = self.fetch_page(url)

        if not soup:
            return None

        details = {}

        # Localização
        location = self.extract_location(soup)
        details.update(location)

        # Horários
        times = self.extract_times(soup)
        details.update(times)

        # Contato
        contact = self.extract_contact(soup)
        details.update(contact)

        # Rating e estrelas
        details["rating"] = self.extract_rating(soup)
        details["stars"] = self.extract_stars(soup)

        # Amenidades
        amenities = self.extract_amenities(soup)
        details["amenities_general"] = amenities["general"]
        details["amenities_food"] = amenities["food"]
        details["amenities_wellness"] = amenities["wellness"]
        details["amenities_events"] = amenities["events"]

        # Logo
        details["logo_url"] = self.extract_logo(soup)

        # Coordenadas
        coords = self.extract_coordinates(soup)
        details["latitude"] = coords["latitude"]
        details["longitude"] = coords["longitude"]

        return details

    def update_hotel_file(self, file_path: Path, dry_run: bool = False, force: bool = False) -> bool:
        """Atualiza um arquivo de hotel com dados faltantes."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                hotel = json.load(f)
        except Exception as e:
            logger.error(f"Erro ao ler {file_path}: {e}")
            return False

        chain_id = hotel.get("chain_id")
        hotel_id = hotel.get("id")

        if not chain_id or not hotel_id:
            logger.warning(f"IDs faltando em {file_path}")
            return False

        # Campos principais para verificar
        location_fields = ["address", "city", "state", "zip_code", "country"]
        time_fields = ["check_in", "check_out"]
        contact_fields = ["email"]
        coord_fields = ["latitude", "longitude"]
        amenity_fields = ["amenities_general", "amenities_food", "amenities_wellness", "amenities_events"]

        all_fields = location_fields + time_fields + contact_fields + coord_fields

        # Verificar se precisa atualização
        needs_update = force  # Se force, sempre atualiza

        if not needs_update:
            # Verificar campos vazios
            needs_update = any(not hotel.get(field) for field in all_fields)

        if not needs_update:
            # Verificar amenities vazias
            needs_update = all(not hotel.get(field) for field in amenity_fields)

        # Verificar dados suspeitos (city que é número, por exemplo)
        if not needs_update:
            city = hotel.get("city", "")
            if city and city.isdigit():
                needs_update = True
                logger.debug(f"Hotel {hotel_id}: city={city} parece incorreto")

        if not needs_update:
            self.stats["skipped"] += 1
            return False

        logger.info(f"Atualizando hotel {hotel_id} ({hotel.get('name', 'N/A')})...")

        if dry_run:
            logger.info(f"  [DRY-RUN] Seria atualizado")
            return True

        # Buscar novos dados
        new_data = self.scrape_hotel_details(chain_id, hotel_id)

        if not new_data:
            logger.warning(f"Não foi possível obter dados para hotel {hotel_id}")
            self.stats["errors"] += 1
            return False

        updated_fields = []

        # No modo force, atualiza todos os campos de localização, horários e contato
        if force:
            force_fields = location_fields + time_fields + contact_fields + coord_fields
            for field in force_fields:
                new_value = new_data.get(field)
                if new_value:
                    hotel[field] = new_value
                    updated_fields.append(field)
        else:
            # Atualizar apenas campos vazios ou com dados suspeitos
            for field, value in new_data.items():
                current = hotel.get(field)

                should_update = False

                # Campo vazio ou None
                if not current and value:
                    should_update = True

                # Lista vazia
                if isinstance(current, list) and len(current) == 0 and value:
                    should_update = True

                # Cidade que é número (dado incorreto)
                if field == "city" and current and current.isdigit() and value and not value.isdigit():
                    should_update = True

                if should_update:
                    hotel[field] = value
                    updated_fields.append(field)

        if updated_fields:
            hotel["updated_at"] = datetime.now().isoformat()

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(hotel, f, ensure_ascii=False, indent=2)

            logger.info(f"  Campos atualizados: {', '.join(updated_fields)}")
            self.stats["updated"] += 1
            return True
        else:
            logger.info(f"  Nenhum dado novo encontrado")
            return False

    def run(self, dry_run: bool = False, hotel_id: Optional[int] = None, force: bool = False, limit: int = 0):
        """Executa o re-scraping."""
        logger.info("=" * 60)
        logger.info("OMNIBEES RE-SCRAPER")
        logger.info("=" * 60)

        if dry_run:
            logger.info("[MODO DRY-RUN] Nenhuma alteração será feita")
        if force:
            logger.info("[MODO FORCE] Sobrescrevendo todos os campos de localização")
        if limit > 0:
            logger.info(f"[LIMITE] Processando apenas {limit} hotéis")

        # Listar arquivos de hotéis
        if hotel_id:
            files = list(self.HOTELS_DIR.glob(f"hotel_*_{hotel_id}.json"))
        else:
            files = list(self.HOTELS_DIR.glob("hotel_*.json"))
            if limit > 0:
                files = files[:limit]

        total = len(files)
        logger.info(f"Encontrados {total} arquivos de hotéis")
        logger.info("=" * 60)

        for i, file_path in enumerate(files, 1):
            self.stats["processed"] += 1

            if i % 10 == 0:
                logger.info(f"[{i}/{total}] Processando...")

            try:
                self.update_hotel_file(file_path, dry_run, force)
            except Exception as e:
                logger.error(f"Erro ao processar {file_path}: {e}")
                self.stats["errors"] += 1

        # Resumo
        logger.info("\n" + "=" * 60)
        logger.info("RESUMO")
        logger.info("=" * 60)
        logger.info(f"Processados: {self.stats['processed']}")
        logger.info(f"Atualizados: {self.stats['updated']}")
        logger.info(f"Ignorados (já completos): {self.stats['skipped']}")
        logger.info(f"Erros: {self.stats['errors']}")
        logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Re-scraper para preencher campos vazios")
    parser.add_argument("--dry-run", action="store_true", help="Apenas mostrar o que seria atualizado")
    parser.add_argument("--hotel", type=int, help="ID específico de hotel para re-processar")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay entre requisições (default: 1.5s)")
    parser.add_argument("--force", action="store_true", help="Forçar re-scraping mesmo de campos preenchidos")
    parser.add_argument("--limit", type=int, default=0, help="Limitar quantidade de hotéis a processar")

    args = parser.parse_args()

    scraper = OmnibeesRescraper(delay=args.delay)
    scraper.run(dry_run=args.dry_run, hotel_id=args.hotel, force=args.force, limit=args.limit)


if __name__ == "__main__":
    main()
