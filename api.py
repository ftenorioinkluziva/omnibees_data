#!/usr/bin/env python3
"""
Omnibees Data API
"""

from decimal import Decimal
from fastapi import FastAPI, Query, Body
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from typing import Optional
from pydantic import BaseModel
import json
from db import get_connection


class WatchCreate(BaseModel):
    hotel_external_id: str
    date_start: str
    date_end: str
    label: Optional[str] = None
    target_price: Optional[float] = None
    notify: bool = True


class WatchUpdate(BaseModel):
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    label: Optional[str] = None
    target_price: Optional[float] = None
    notify: Optional[bool] = None


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)


class DecimalJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, cls=DecimalEncoder, ensure_ascii=False).encode("utf-8")

app = FastAPI(title="Omnibees Data API", default_response_class=DecimalJSONResponse)


@app.get("/api/stats")
def stats():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT count(*) FROM chains),
                    (SELECT count(*) FROM hotels),
                    (SELECT count(*) FROM hotel_diarias),
                    (SELECT count(*) FROM hotel_precos_historico),
                    (SELECT min(date) FROM hotel_diarias WHERE date >= CURRENT_DATE),
                    (SELECT max(date) FROM hotel_diarias)
            """)
            r = cur.fetchone()

            cur.execute("""
                SELECT count(DISTINCT h.id)
                FROM hotels h
                JOIN hotel_diarias d ON d.hotel_id = h.id
                WHERE d.updated_at >= now() - interval '24 hours'
            """)
            active = cur.fetchone()[0]

            cur.execute("""
                SELECT avg(amount)::numeric(10,2)
                FROM hotel_diarias
                WHERE date = CURRENT_DATE AND amount > 0
            """)
            avg_today = cur.fetchone()[0]

            return {
                "chains": r[0], "hotels": r[1], "diarias": r[2],
                "historico": r[3], "date_min": str(r[4]) if r[4] else None,
                "date_max": str(r[5]) if r[5] else None,
                "active_hotels_24h": active,
                "avg_price_today": float(avg_today) if avg_today else None,
            }


@app.get("/api/hotels")
def hotels(
    city: Optional[str] = None,
    state: Optional[str] = None,
    stars: Optional[int] = None,
    chain: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(default=50, le=500),
    offset: int = 0,
):
    conditions = []
    params = []

    if city:
        conditions.append("h.city ILIKE %s")
        params.append(f"%{city}%")
    if state:
        conditions.append("h.state ILIKE %s")
        params.append(f"%{state}%")
    if stars:
        conditions.append("h.stars = %s")
        params.append(stars)
    if chain:
        conditions.append("c.name ILIKE %s")
        params.append(f"%{chain}%")
    if search:
        conditions.append("h.name ILIKE %s")
        params.append(f"%{search}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM hotels h LEFT JOIN chains c ON h.chain_id = c.id {where}", params)
            total = cur.fetchone()[0]

            cur.execute(f"""
                SELECT h.id, h.external_id, h.name, h.city, h.state, h.stars,
                       c.name as chain_name, h.country,
                       (SELECT d.amount FROM hotel_diarias d
                        WHERE d.hotel_id = h.id AND d.date = CURRENT_DATE LIMIT 1) as price_today
                FROM hotels h
                LEFT JOIN chains c ON h.chain_id = c.id
                {where}
                ORDER BY h.name
                LIMIT %s OFFSET %s
            """, params + [limit, offset])

            rows = cur.fetchall()
            return {
                "total": total,
                "hotels": [{
                    "id": r[0], "external_id": r[1], "name": r[2],
                    "city": r[3], "state": r[4], "stars": r[5],
                    "chain": r[6], "country": r[7],
                    "price_today": float(r[8]) if r[8] else None,
                } for r in rows]
            }


@app.get("/api/hotels/{external_id}")
def hotel_detail(external_id: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT h.id, h.external_id, h.name, h.city, h.state, h.stars,
                       h.address, h.zip_code, h.country, h.description,
                       h.check_in, h.check_out, h.amenities, h.images, h.room_types,
                       h.latitude, h.longitude,
                       c.name as chain_name, c.email, c.phone
                FROM hotels h
                LEFT JOIN chains c ON h.chain_id = c.id
                WHERE h.external_id = %s
            """, (external_id,))
            r = cur.fetchone()
            if not r:
                return {"error": "Hotel not found"}
            return {
                "id": r[0], "external_id": r[1], "name": r[2],
                "city": r[3], "state": r[4], "stars": r[5],
                "address": r[6], "zip_code": r[7], "country": r[8],
                "description": r[9], "check_in": r[10], "check_out": r[11],
                "amenities": r[12], "images": r[13], "room_types": r[14],
                "latitude": float(r[15]) if r[15] else None,
                "longitude": float(r[16]) if r[16] else None,
                "chain": r[17], "email": r[18], "phone": r[19],
            }


@app.get("/api/hotels/{external_id}/prices")
def hotel_prices(
    external_id: str,
    days: int = Query(default=90, le=450),
    date_from: str = Query(default=None),
    date_to: str = Query(default=None),
):
    with get_connection() as conn:
        with conn.cursor() as cur:
            if date_from and date_to:
                cur.execute("""
                    SELECT d.date, d.amount
                    FROM hotel_diarias d
                    JOIN hotels h ON d.hotel_id = h.id
                    WHERE h.external_id = %s AND d.date >= %s AND d.date <= %s
                    ORDER BY d.date
                """, (external_id, date_from, date_to))
            else:
                cur.execute("""
                    SELECT d.date, d.amount
                    FROM hotel_diarias d
                    JOIN hotels h ON d.hotel_id = h.id
                    WHERE h.external_id = %s AND d.date >= CURRENT_DATE
                    ORDER BY d.date
                    LIMIT %s
                """, (external_id, days))
            rows = cur.fetchall()
            return {
                "hotel": external_id,
                "prices": [{"date": str(r[0]), "price": float(r[1])} for r in rows]
            }


@app.get("/api/hotels/{external_id}/history")
def hotel_price_history(external_id: str, limit: int = Query(default=100, le=500)):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.date, hp.amount as old_price, d.amount as current_price,
                       hp.captured_at
                FROM hotel_precos_historico hp
                JOIN hotel_diarias d ON hp.hotel_diaria_id = d.id
                JOIN hotels h ON d.hotel_id = h.id
                WHERE h.external_id = %s
                ORDER BY hp.captured_at DESC
                LIMIT %s
            """, (external_id, limit))
            rows = cur.fetchall()
            return {
                "hotel": external_id,
                "changes": [{
                    "date": str(r[0]), "old_price": float(r[1]),
                    "new_price": float(r[2]), "captured_at": str(r[3]),
                } for r in rows]
            }


@app.get("/api/compare")
def compare_hotels(
    hotels: str = Query(description="Comma-separated external IDs"),
    days: int = Query(default=30, le=180),
):
    ids = [h.strip() for h in hotels.split(",")]
    result = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            for ext_id in ids[:10]:
                cur.execute("""
                    SELECT h.name, d.date, d.amount
                    FROM hotel_diarias d
                    JOIN hotels h ON d.hotel_id = h.id
                    WHERE h.external_id = %s AND d.date >= CURRENT_DATE
                    ORDER BY d.date
                    LIMIT %s
                """, (ext_id, days))
                rows = cur.fetchall()
                if rows:
                    result.append({
                        "external_id": ext_id,
                        "name": rows[0][0],
                        "prices": [{"date": str(r[1]), "price": float(r[2])} for r in rows]
                    })
    return {"hotels": result}


@app.get("/api/top-cities")
def top_cities(limit: int = 20):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT h.city, h.state, count(*) as hotel_count,
                       avg(d.amount)::numeric(10,2) as avg_price
                FROM hotels h
                JOIN hotel_diarias d ON d.hotel_id = h.id
                WHERE h.city IS NOT NULL AND h.city != ''
                  AND d.date = CURRENT_DATE AND d.amount > 0
                GROUP BY h.city, h.state
                ORDER BY hotel_count DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return [{
                "city": r[0], "state": r[1],
                "hotels": r[2], "avg_price": float(r[3]) if r[3] else None,
            } for r in rows]


@app.get("/api/price-distribution")
def price_distribution():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    CASE
                        WHEN amount < 200 THEN 'Ate R$200'
                        WHEN amount < 500 THEN 'R$200-500'
                        WHEN amount < 1000 THEN 'R$500-1000'
                        WHEN amount < 2000 THEN 'R$1000-2000'
                        ELSE 'Acima R$2000'
                    END as faixa,
                    count(*) as total
                FROM hotel_diarias
                WHERE date = CURRENT_DATE AND amount > 0
                GROUP BY faixa
                ORDER BY min(amount)
            """)
            rows = cur.fetchall()
            return [{"range": r[0], "count": r[1]} for r in rows]


@app.get("/api/filters")
def available_filters():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT state FROM hotels
                WHERE state IS NOT NULL AND state != ''
                ORDER BY state
            """)
            states = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT stars FROM hotels
                WHERE stars IS NOT NULL AND stars > 0
                ORDER BY stars
            """)
            stars = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT name FROM chains
                ORDER BY name
                LIMIT 100
            """)
            chains = [r[0] for r in cur.fetchall()]

            return {"states": states, "stars": stars, "chains": chains}


@app.get("/api/hotels/{external_id}/patterns")
def hotel_patterns(external_id: str, days: int = Query(default=180, ge=30, le=450)):
    weekday_names = {
        1: "segunda",
        2: "terça",
        3: "quarta",
        4: "quinta",
        5: "sexta",
        6: "sábado",
        7: "domingo",
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM hotels WHERE external_id = %s", (external_id,))
            hotel = cur.fetchone()
            if not hotel:
                return {"error": "Hotel not found"}

            hotel_id, hotel_name = hotel

            cur.execute(
                """
                SELECT date, amount
                FROM hotel_diarias
                WHERE hotel_id = %s
                  AND date >= CURRENT_DATE
                  AND date < CURRENT_DATE + (%s || ' days')::interval
                  AND amount > 0
                ORDER BY date
                """,
                (hotel_id, days),
            )
            prices = cur.fetchall()

            if not prices:
                return {
                    "hotel": external_id,
                    "name": hotel_name,
                    "days": days,
                    "total_points": 0,
                    "cheapest_weekday": None,
                    "trend": "insufficient_data",
                    "weekday_stats": [],
                }

            cur.execute(
                """
                SELECT extract(isodow FROM date)::int as dow,
                       avg(amount)::numeric(10,2) as avg_price,
                       min(amount)::numeric(10,2) as min_price,
                       max(amount)::numeric(10,2) as max_price,
                       count(*) as total
                FROM hotel_diarias
                WHERE hotel_id = %s
                  AND date >= CURRENT_DATE
                  AND date < CURRENT_DATE + (%s || ' days')::interval
                  AND amount > 0
                GROUP BY dow
                ORDER BY dow
                """,
                (hotel_id, days),
            )
            weekday_rows = cur.fetchall()

    weekday_stats = [
        {
            "weekday": weekday_names[r[0]],
            "avg_price": float(r[1]),
            "min_price": float(r[2]),
            "max_price": float(r[3]),
            "samples": r[4],
        }
        for r in weekday_rows
    ]

    cheapest = min(weekday_stats, key=lambda row: row["avg_price"]) if weekday_stats else None

    amounts = [float(row[1]) for row in prices]
    midpoint = len(amounts) // 2
    trend = "insufficient_data"
    trend_pct = None

    if midpoint >= 2 and len(amounts) - midpoint >= 2:
        first_half_avg = sum(amounts[:midpoint]) / midpoint
        second_half_avg = sum(amounts[midpoint:]) / (len(amounts) - midpoint)
        trend_pct = ((second_half_avg - first_half_avg) / first_half_avg) * 100 if first_half_avg else 0
        if trend_pct > 3:
            trend = "up"
        elif trend_pct < -3:
            trend = "down"
        else:
            trend = "stable"

    return {
        "hotel": external_id,
        "name": hotel_name,
        "days": days,
        "total_points": len(prices),
        "cheapest_weekday": cheapest,
        "trend": trend,
        "trend_pct": round(trend_pct, 2) if trend_pct is not None else None,
        "weekday_stats": weekday_stats,
    }


# ── Watchlist ──

@app.get("/api/watchlist")
def watchlist():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT w.id, h.external_id, h.name, h.city, h.stars,
                       w.date_start, w.date_end, w.label, w.target_price, w.notify,
                       c.name as chain_name,
                       (SELECT min(d.amount) FROM hotel_diarias d
                        WHERE d.hotel_id = h.id
                          AND d.date BETWEEN w.date_start AND w.date_end
                          AND d.amount > 0) as min_price,
                       (SELECT avg(d.amount)::numeric(10,2) FROM hotel_diarias d
                        WHERE d.hotel_id = h.id
                          AND d.date BETWEEN w.date_start AND w.date_end
                          AND d.amount > 0) as avg_price,
                       (SELECT d.amount FROM hotel_diarias d
                        WHERE d.hotel_id = h.id AND d.date = CURRENT_DATE
                        LIMIT 1) as price_today
                FROM watched_hotels w
                JOIN hotels h ON w.hotel_id = h.id
                LEFT JOIN chains c ON h.chain_id = c.id
                ORDER BY w.date_start
            """)
            rows = cur.fetchall()
            return [{
                "id": r[0], "external_id": r[1], "name": r[2],
                "city": r[3], "stars": r[4],
                "date_start": str(r[5]), "date_end": str(r[6]),
                "label": r[7], "target_price": float(r[8]) if r[8] else None,
                "notify": r[9], "chain": r[10],
                "min_price": float(r[11]) if r[11] else None,
                "avg_price": float(r[12]) if r[12] else None,
                "price_today": float(r[13]) if r[13] else None,
            } for r in rows]


@app.post("/api/watchlist")
def watchlist_add(body: WatchCreate):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM hotels WHERE external_id = %s", (body.hotel_external_id,))
            row = cur.fetchone()
            if not row:
                return {"error": "Hotel not found"}
            hotel_id = row[0]

            cur.execute("""
                INSERT INTO watched_hotels (hotel_id, date_start, date_end, label, target_price, notify)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (hotel_id, date_start, date_end) DO UPDATE SET
                    label = EXCLUDED.label,
                    target_price = EXCLUDED.target_price,
                    notify = EXCLUDED.notify,
                    updated_at = now()
                RETURNING id
            """, (hotel_id, body.date_start, body.date_end, body.label, body.target_price, body.notify))
            watch_id = cur.fetchone()[0]
            conn.commit()
            return {"id": watch_id, "status": "added"}


@app.put("/api/watchlist/{watch_id}")
def watchlist_update(watch_id: int, body: WatchUpdate):
    with get_connection() as conn:
        with conn.cursor() as cur:
            sets = []
            params = []
            if body.date_start is not None:
                sets.append("date_start = %s")
                params.append(body.date_start)
            if body.date_end is not None:
                sets.append("date_end = %s")
                params.append(body.date_end)
            if body.label is not None:
                sets.append("label = %s")
                params.append(body.label)
            if body.target_price is not None:
                sets.append("target_price = %s")
                params.append(body.target_price)
            if body.notify is not None:
                sets.append("notify = %s")
                params.append(body.notify)

            if not sets:
                return {"error": "Nothing to update"}

            sets.append("updated_at = now()")
            params.append(watch_id)

            cur.execute(f"UPDATE watched_hotels SET {', '.join(sets)} WHERE id = %s", params)
            conn.commit()
            return {"status": "updated"}


@app.delete("/api/watchlist/{watch_id}")
def watchlist_delete(watch_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM watched_hotels WHERE id = %s", (watch_id,))
            conn.commit()
            return {"status": "deleted"}


@app.get("/api/watchlist/{watch_id}/prices")
def watchlist_prices(watch_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT h.name, w.date_start, w.date_end, w.target_price
                FROM watched_hotels w
                JOIN hotels h ON w.hotel_id = h.id
                WHERE w.id = %s
            """, (watch_id,))
            info = cur.fetchone()
            if not info:
                return {"error": "Watch not found"}

            cur.execute("""
                SELECT d.date, d.amount
                FROM hotel_diarias d
                JOIN watched_hotels w ON d.hotel_id = w.hotel_id
                WHERE w.id = %s
                  AND d.date BETWEEN w.date_start AND w.date_end
                ORDER BY d.date
            """, (watch_id,))
            rows = cur.fetchall()
            return {
                "name": info[0],
                "date_start": str(info[1]),
                "date_end": str(info[2]),
                "target_price": float(info[3]) if info[3] else None,
                "prices": [{"date": str(r[0]), "price": float(r[1])} for r in rows],
            }


@app.get("/api/hotels-search")
def hotels_search(q: str = Query(min_length=2)):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (h.external_id) h.external_id, h.name, h.city, h.stars, c.name as chain_name
                FROM hotels h
                LEFT JOIN chains c ON h.chain_id = c.id
                WHERE h.name ILIKE %s
                ORDER BY h.external_id, h.name
                LIMIT 15
            """, (f"%{q}%",))
            rows = cur.fetchall()
            return [{
                "external_id": r[0], "name": r[1], "city": r[2],
                "stars": r[3], "chain": r[4],
            } for r in rows]


app.mount("/", StaticFiles(directory="static", html=True), name="static")
