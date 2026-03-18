#!/usr/bin/env python3
"""
Migra dados dos JSONs locais (chains/ e hotels/) para o PostgreSQL no Neon.
- Insere chains na tabela chains
- Atualiza hotels existentes com dados enriquecidos dos JSONs
- Vincula hotels às chains via chain_id
"""
import json
import psycopg2
from config import DATABASE_URL, CHAINS_DIR, HOTELS_DIR


def migrate_chains(cur):
    chain_files = sorted(CHAINS_DIR.glob("chain_*.json"))
    print(f"Encontrados {len(chain_files)} arquivos de chains")

    inserted = 0
    for f in chain_files:
        data = json.loads(f.read_text(encoding="utf-8"))
        cur.execute("""
            INSERT INTO chains (external_id, name, url, country, logo_url, email, phone, hotels_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (external_id) DO UPDATE SET
                name = EXCLUDED.name,
                url = EXCLUDED.url,
                country = EXCLUDED.country,
                logo_url = EXCLUDED.logo_url,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                hotels_count = EXCLUDED.hotels_count,
                updated_at = now()
        """, (
            str(data["id"]),
            data["name"],
            data.get("url", ""),
            data.get("country"),
            data.get("logo_url", ""),
            data.get("email", ""),
            data.get("phone", ""),
            data.get("hotels_count", 0),
        ))
        inserted += 1

    print(f"Chains upserted: {inserted}")


def enrich_hotels(cur):
    hotel_files = sorted(HOTELS_DIR.glob("hotel_*.json"))
    print(f"Encontrados {len(hotel_files)} arquivos de hotéis")

    updated = 0
    linked = 0
    not_found = 0

    for f in hotel_files:
        data = json.loads(f.read_text(encoding="utf-8"))
        hotel_ext_id = str(data["id"])
        chain_ext_id = str(data.get("chain_id", ""))

        chain_db_id = None
        if chain_ext_id:
            cur.execute("SELECT id FROM chains WHERE external_id = %s", (chain_ext_id,))
            row = cur.fetchone()
            if row:
                chain_db_id = row[0]

        amenities = {}
        for key in ("amenities_general", "amenities_food", "amenities_wellness", "amenities_events"):
            if data.get(key):
                amenities[key] = data[key]

        images = data.get("images", [])
        room_types = data.get("room_types", [])

        cur.execute("""
            UPDATE hotels SET
                description = COALESCE(NULLIF(%s, ''), description),
                address = COALESCE(NULLIF(%s, ''), address),
                city = COALESCE(NULLIF(%s, ''), city),
                state = COALESCE(NULLIF(%s, ''), state),
                zip_code = COALESCE(NULLIF(%s, ''), zip_code),
                country = COALESCE(NULLIF(%s, ''), country),
                latitude = COALESCE(%s, latitude),
                longitude = COALESCE(%s, longitude),
                check_in = COALESCE(NULLIF(%s, ''), check_in),
                check_out = COALESCE(NULLIF(%s, ''), check_out),
                stars = COALESCE(%s, stars),
                amenities = COALESCE(%s, amenities),
                images = COALESCE(%s, images),
                room_types = COALESCE(%s, room_types),
                chain_id = COALESCE(%s, chain_id),
                updated_at = now()
            WHERE external_id = %s
        """, (
            data.get("description", ""),
            data.get("address", ""),
            data.get("city", ""),
            data.get("state", ""),
            data.get("zip_code", ""),
            data.get("country", ""),
            data.get("latitude"),
            data.get("longitude"),
            data.get("check_in", ""),
            data.get("check_out", ""),
            data.get("stars") if data.get("stars") else None,
            json.dumps(amenities) if amenities else None,
            json.dumps(images) if images else None,
            json.dumps(room_types) if room_types else None,
            chain_db_id,
            hotel_ext_id,
        ))

        if cur.rowcount > 0:
            updated += 1
            if chain_db_id:
                linked += 1
        else:
            not_found += 1

    print(f"Hotels atualizados: {updated}")
    print(f"Hotels vinculados a chains: {linked}")
    print(f"Hotels não encontrados no banco: {not_found}")


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            print("=== Migrando chains ===")
            migrate_chains(cur)
            conn.commit()

            print("\n=== Enriquecendo hotels ===")
            enrich_hotels(cur)
            conn.commit()

            print("\n=== Verificação final ===")
            cur.execute("SELECT count(*) FROM chains")
            print(f"Total chains no banco: {cur.fetchone()[0]}")
            cur.execute("SELECT count(*) FROM hotels WHERE chain_id IS NOT NULL")
            print(f"Hotels com chain_id: {cur.fetchone()[0]}")
            cur.execute("SELECT count(*) FROM hotels WHERE coalesce(city, '') != ''")
            print(f"Hotels com cidade: {cur.fetchone()[0]}")

        print("\nMigração concluída com sucesso!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
