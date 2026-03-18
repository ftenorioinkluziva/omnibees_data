#!/usr/bin/env python3

import argparse
import re
import time
from typing import Dict, Optional

import requests

from db import get_connection
from location_parser import parse_location_text


def normalize_zip(zip_code: str) -> str:
    digits = re.sub(r"\D", "", zip_code or "")
    if len(digits) == 8:
        return f"{digits[:5]}-{digits[5:]}"
    return ""


def polluted_city(value: str) -> bool:
    if not value:
        return True
    if re.search(r"\d{5}-?\d{3}", value):
        return True
    if len(value) > 80:
        return True
    if "," in value or "|" in value:
        return True
    if re.search(r"\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b", value):
        return True
    return False


def plausible_city(value: str) -> bool:
    if not value:
        return False
    if re.search(r"-?\d{1,2}\.\d{3,}", value):
        return False
    if re.search(r"\d", value):
        return False
    if len(value) < 2 or len(value) > 60:
        return False
    return bool(re.search(r"[A-Za-zÀ-ÿ]", value))


def explicit_uf_in_raw(raw_text: str, uf: str) -> bool:
    if not raw_text or not uf:
        return False
    return bool(re.search(rf"(^|[\s,\-]){re.escape(uf)}($|[\s,\-])", raw_text.upper()))


def fetch_viacep(cep: str) -> Optional[Dict[str, str]]:
    try:
        resp = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=5)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("erro"):
            return None
        return {
            "city": data.get("localidade", "").strip(),
            "state": data.get("uf", "").strip().upper(),
            "address": data.get("logradouro", "").strip(),
        }
    except Exception:
        return None


def choose_value(current: str, candidate: str, force_replace: bool = False) -> str:
    if candidate and (force_replace or not current):
        return candidate
    return current or ""


def run(
    apply_changes: bool,
    limit: Optional[int],
    delay: float,
    summary_only: bool = False,
    use_viacep: bool = False,
):
    with get_connection() as conn:
        with conn.cursor() as cur:
            sql = """
                SELECT id, external_id, name, address, city, state, zip_code, country
                FROM hotels
                WHERE COALESCE(country, 'Brasil') ILIKE '%%brasil%%'
                ORDER BY id
            """
            if limit:
                sql += " LIMIT %s"
                cur.execute(sql, (limit,))
            else:
                cur.execute(sql)
            rows = cur.fetchall()

            updates = []
            cep_cache: Dict[str, Optional[Dict[str, str]]] = {}

            for hotel_id, external_id, name, address, city, state, zip_code, country in rows:
                current_address = (address or "").strip()
                current_city = (city or "").strip()
                current_state = (state or "").strip().upper()
                current_zip = normalize_zip(zip_code or "")

                raw_location = ", ".join([current_address, current_city, current_state, current_zip, (country or "")]).strip(", ")
                parsed = parse_location_text(raw_location)

                candidate_zip = normalize_zip(parsed["zip_code"]) or current_zip
                viacep_data = None

                if use_viacep and candidate_zip:
                    if candidate_zip not in cep_cache:
                        cep_cache[candidate_zip] = fetch_viacep(candidate_zip)
                        if delay > 0:
                            time.sleep(delay)
                    viacep_data = cep_cache[candidate_zip]

                candidate_state = ""
                candidate_city = ""
                candidate_address = ""

                if viacep_data:
                    candidate_state = viacep_data["state"]
                    candidate_city = viacep_data["city"]
                    candidate_address = viacep_data["address"]

                if not candidate_state:
                    candidate_state = parsed["state"]
                if not candidate_city:
                    candidate_city = parsed["city"]

                force_state = False
                force_city = False

                if viacep_data and candidate_state:
                    if re.search(r"-?\d{1,3}\.\d{3,}", current_city):
                        candidate_state = ""
                        candidate_city = ""
                    elif polluted_city(current_city) and not explicit_uf_in_raw(raw_location, candidate_state):
                        candidate_state = ""
                    else:
                        force_state = not re.fullmatch(r"[A-Z]{2}", current_state or "")
                elif candidate_state and explicit_uf_in_raw(raw_location, candidate_state):
                    force_state = not re.fullmatch(r"[A-Z]{2}", current_state or "")
                else:
                    candidate_state = ""

                if viacep_data and candidate_city and plausible_city(candidate_city):
                    force_city = polluted_city(current_city) or not current_city
                elif candidate_city and plausible_city(candidate_city) and explicit_uf_in_raw(raw_location, parsed["state"]):
                    force_city = polluted_city(current_city) or not current_city
                else:
                    candidate_city = ""

                if re.search(r"-?\d{1,3}\.\d{3,}", current_city):
                    candidate_state = ""
                    candidate_city = ""
                    force_state = False
                    force_city = False

                new_state = choose_value(current_state, candidate_state, force_replace=force_state)
                new_city = choose_value(current_city, candidate_city, force_replace=force_city)
                new_zip = choose_value(current_zip, candidate_zip)
                new_address = choose_value(current_address, candidate_address, force_replace=False)

                if (new_state != current_state) or (new_city != current_city) or (new_zip != current_zip) or (new_address != current_address):
                    updates.append({
                        "id": hotel_id,
                        "external_id": external_id,
                        "name": name,
                        "old": {
                            "address": current_address,
                            "city": current_city,
                            "state": current_state,
                            "zip_code": current_zip,
                        },
                        "new": {
                            "address": new_address,
                            "city": new_city,
                            "state": new_state,
                            "zip_code": new_zip,
                        },
                    })

            print(f"Hotéis analisados: {len(rows)}")
            print(f"Hotéis com ajuste: {len(updates)}")

            if not summary_only:
                preview = updates[:20]
                for item in preview:
                    print(f"\n[{item['external_id']}] {item['name']}")
                    print(f"  city:  '{item['old']['city']}' -> '{item['new']['city']}'")
                    print(f"  state: '{item['old']['state']}' -> '{item['new']['state']}'")
                    print(f"  zip:   '{item['old']['zip_code']}' -> '{item['new']['zip_code']}'")

            if not apply_changes:
                print("\nDry-run finalizado. Use --apply para gravar no banco.")
                return

            for item in updates:
                cur.execute(
                    """
                    UPDATE hotels
                    SET address = %s,
                        city = %s,
                        state = %s,
                        zip_code = %s,
                        updated_at = now()
                    WHERE id = %s
                    """,
                    (
                        item["new"]["address"],
                        item["new"]["city"],
                        item["new"]["state"],
                        item["new"]["zip_code"],
                        item["id"],
                    ),
                )

            conn.commit()
            print(f"\nAlterações aplicadas: {len(updates)}")


def main():
    parser = argparse.ArgumentParser(description="Corrige city/state/zip_code de hotéis com base em parsing e ViaCEP")
    parser.add_argument("--apply", action="store_true", help="Aplica mudanças no banco (default: dry-run)")
    parser.add_argument("--limit", type=int, default=None, help="Limita quantidade de hotéis analisados")
    parser.add_argument("--delay", type=float, default=0.02, help="Delay entre consultas ViaCEP")
    parser.add_argument("--summary-only", action="store_true", help="Exibe apenas totais, sem preview detalhado")
    parser.add_argument("--with-viacep", action="store_true", help="Ativa enriquecimento por ViaCEP (mais lento)")
    args = parser.parse_args()

    run(args.apply, args.limit, args.delay, args.summary_only, args.with_viacep)


if __name__ == "__main__":
    main()
