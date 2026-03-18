#!/usr/bin/env python3
"""
Telegram Bot — Assistente de Cotação de Diárias
================================================
Bot conversacional que consulta a base PostgreSQL de diárias de hotéis
usando Google Gemini como LLM e function calling para queries SQL.
"""

import json
import logging
import os
from datetime import datetime
from decimal import Decimal

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from dotenv import load_dotenv
load_dotenv()

from db import get_connection
from config import TELEGRAM_BOT_TOKEN

logger = logging.getLogger(__name__)

GOOGLE_API_KEY = os.environ.get("GOOGLE_GENERATIVE_AI_API_KEY", "")

MAX_HISTORY = 10

SYSTEM_PROMPT = f"""\
Você é o "Assistente de Cotação de Diárias de Hotéis", um agente focado em consultar \
a base de dados com diárias de hotéis do Omnibees.

Hoje é {datetime.now().strftime("%A, %d/%m/%Y")}.

## Ferramentas disponíveis
Você tem acesso a funções que consultam o banco de dados PostgreSQL com ~975 mil diárias \
de 3.359 hotéis brasileiros. Use-as sempre que precisar de dados.

## Fluxo de trabalho

### Fase 1: Coleta
Se a solicitação estiver incompleta, pergunte pelo que falta:
- Nome do hotel (pode ser parcial, a busca é fuzzy)
- Data de check-in
- Data de check-out

### Fase 2: Consulta
1. Se o usuário não sabe o nome exato, use buscar_hoteis para encontrar
2. Use buscar_diarias para obter os preços do período
3. Use buscar_padroes para análise de dia da semana / tendência
4. Use buscar_watchlist para ver hotéis monitorados
5. Use comparar_hoteis para comparar 2-3 hotéis no mesmo período
6. Use hotel_detalhes para informações completas (amenities, quartos, descrição)
7. Use buscar_mais_baratos para encontrar os hotéis mais baratos de uma região/período
8. Use historico_precos para ver como o preço mudou ao longo do tempo
9. Use buscar_por_cidade para ranking de cidades por preço médio
10. Use resumo_estatisticas para dados gerais sobre a base
11. Use adicionar_watchlist para monitorar um hotel (requer hotel_id, checkin, checkout)
12. Use remover_watchlist para parar de monitorar (requer watch_id do buscar_watchlist)
13. Use sugerir_datas para encontrar as datas mais baratas de um hotel em um mês
14. Use recomendar_hoteis para sugerir hotéis por perfil (familia, casal, negocios, economico)

### Fase 3: Apresentação
- Valores sempre em R$ (BRL)
- Diária do check-out NÃO é cobrada
- Se alguma data não tiver preço, avise explicitamente
- Use formatação Telegram (negrito com *, itálico com _)

## Regras
- NÃO invente valores — se não está na base, não existe
- NÃO pesquise na internet — sua fonte é apenas o banco de dados
- NÃO pesquise passagens/transporte — apenas diárias
- Seja conciso e direto
"""

TOOLS = [
    {
        "name": "buscar_hoteis",
        "description": "Busca hotéis por nome, cidade, estado ou estrelas. Use quando o usuário menciona um hotel e você precisa encontrar o ID correto.",
        "parameters": {
            "type": "object",
            "properties": {
                "nome": {"type": "string", "description": "Nome parcial do hotel"},
                "cidade": {"type": "string", "description": "Cidade do hotel"},
                "estado": {"type": "string", "description": "UF do estado (ex: SP, RJ)"},
                "estrelas": {"type": "integer", "description": "Classificação por estrelas"},
            },
        },
    },
    {
        "name": "buscar_diarias",
        "description": "Busca diárias de um hotel em um período. Retorna data e valor de cada diária.",
        "parameters": {
            "type": "object",
            "properties": {
                "hotel_external_id": {"type": "string", "description": "ID externo do hotel (obtido via buscar_hoteis)"},
                "data_checkin": {"type": "string", "description": "Data de check-in (YYYY-MM-DD)"},
                "data_checkout": {"type": "string", "description": "Data de check-out (YYYY-MM-DD)"},
            },
            "required": ["hotel_external_id", "data_checkin", "data_checkout"],
        },
    },
    {
        "name": "buscar_padroes",
        "description": "Analisa padrões de preço: dia da semana mais barato, tendência de alta/baixa, estatísticas.",
        "parameters": {
            "type": "object",
            "properties": {
                "hotel_external_id": {"type": "string", "description": "ID externo do hotel"},
                "dias": {"type": "integer", "description": "Quantidade de dias para análise (padrão: 180)"},
            },
            "required": ["hotel_external_id"],
        },
    },
    {
        "name": "buscar_watchlist",
        "description": "Lista hotéis na watchlist de monitoramento com preços agregados.",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "comparar_hoteis",
        "description": "Compara preços de 2 ou 3 hotéis lado a lado no mesmo período. Retorna total, média e menor diária de cada um.",
        "parameters": {
            "type": "object",
            "properties": {
                "hotel_ids": {"type": "string", "description": "IDs externos separados por vírgula (ex: '9098,2281,1234')"},
                "data_checkin": {"type": "string", "description": "Data de check-in (YYYY-MM-DD)"},
                "data_checkout": {"type": "string", "description": "Data de check-out (YYYY-MM-DD)"},
            },
            "required": ["hotel_ids", "data_checkin", "data_checkout"],
        },
    },
    {
        "name": "hotel_detalhes",
        "description": "Retorna informações completas de um hotel: descrição, amenities, check-in/out, tipos de quarto, estrelas, endereço.",
        "parameters": {
            "type": "object",
            "properties": {
                "hotel_external_id": {"type": "string", "description": "ID externo do hotel"},
            },
            "required": ["hotel_external_id"],
        },
    },
    {
        "name": "buscar_mais_baratos",
        "description": "Encontra os hotéis mais baratos de uma cidade, estado ou em todo o Brasil para um período específico.",
        "parameters": {
            "type": "object",
            "properties": {
                "cidade": {"type": "string", "description": "Cidade (opcional)"},
                "estado": {"type": "string", "description": "UF do estado (opcional, ex: SP, RJ)"},
                "data_checkin": {"type": "string", "description": "Data de check-in (YYYY-MM-DD)"},
                "data_checkout": {"type": "string", "description": "Data de check-out (YYYY-MM-DD)"},
                "limite": {"type": "integer", "description": "Quantidade de resultados (padrão: 5)"},
            },
            "required": ["data_checkin", "data_checkout"],
        },
    },
    {
        "name": "historico_precos",
        "description": "Mostra o histórico de mudanças de preço de um hotel — quando subiu, quando baixou, e quanto variou.",
        "parameters": {
            "type": "object",
            "properties": {
                "hotel_external_id": {"type": "string", "description": "ID externo do hotel"},
                "limite": {"type": "integer", "description": "Quantidade de mudanças recentes (padrão: 15)"},
            },
            "required": ["hotel_external_id"],
        },
    },
    {
        "name": "buscar_por_cidade",
        "description": "Ranking de cidades por preço médio de diária, quantidade de hotéis e melhor custo-benefício. Use para comparar destinos.",
        "parameters": {
            "type": "object",
            "properties": {
                "estado": {"type": "string", "description": "Filtrar por UF (opcional, ex: BA, SC)"},
                "limite": {"type": "integer", "description": "Quantidade de cidades (padrão: 10)"},
            },
        },
    },
    {
        "name": "resumo_estatisticas",
        "description": "Retorna estatísticas gerais da base: total de hotéis, diárias, cidades, cobertura de datas, hotéis monitorados.",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "adicionar_watchlist",
        "description": "Adiciona um hotel à watchlist de monitoramento de preços. O sistema notifica quando o preço mudar.",
        "parameters": {
            "type": "object",
            "properties": {
                "hotel_external_id": {"type": "string", "description": "ID externo do hotel"},
                "data_checkin": {"type": "string", "description": "Data início do monitoramento (YYYY-MM-DD)"},
                "data_checkout": {"type": "string", "description": "Data fim do monitoramento (YYYY-MM-DD)"},
                "label": {"type": "string", "description": "Rótulo opcional (ex: 'Férias família', 'Viagem trabalho')"},
                "preco_alvo": {"type": "number", "description": "Preço alvo da diária em R$ (opcional, notifica quando atingir)"},
            },
            "required": ["hotel_external_id", "data_checkin", "data_checkout"],
        },
    },
    {
        "name": "remover_watchlist",
        "description": "Remove um hotel da watchlist de monitoramento. Use buscar_watchlist primeiro para ver os itens e IDs.",
        "parameters": {
            "type": "object",
            "properties": {
                "watch_id": {"type": "integer", "description": "ID do item na watchlist (obtido via buscar_watchlist)"},
            },
            "required": ["watch_id"],
        },
    },
    {
        "name": "sugerir_datas",
        "description": "Encontra as datas mais baratas para um hotel em um mês específico. Retorna os top N dias com menor diária.",
        "parameters": {
            "type": "object",
            "properties": {
                "hotel_external_id": {"type": "string", "description": "ID externo do hotel"},
                "mes": {"type": "integer", "description": "Mês (1-12)"},
                "ano": {"type": "integer", "description": "Ano (ex: 2026)"},
                "noites": {"type": "integer", "description": "Quantidade de noites desejadas (padrão: 3)"},
                "limite": {"type": "integer", "description": "Quantidade de sugestões (padrão: 5)"},
            },
            "required": ["hotel_external_id", "mes", "ano"],
        },
    },
    {
        "name": "recomendar_hoteis",
        "description": "Recomenda hotéis por perfil de viagem (família, casal, negócios) usando amenities, estrelas e preço. Pode filtrar por cidade/estado.",
        "parameters": {
            "type": "object",
            "properties": {
                "perfil": {"type": "string", "description": "Perfil: 'familia', 'casal', 'negocios' ou 'economico'"},
                "cidade": {"type": "string", "description": "Cidade (opcional)"},
                "estado": {"type": "string", "description": "UF do estado (opcional)"},
                "data_checkin": {"type": "string", "description": "Data de check-in (YYYY-MM-DD, opcional)"},
                "data_checkout": {"type": "string", "description": "Data de check-out (YYYY-MM-DD, opcional)"},
                "limite": {"type": "integer", "description": "Quantidade de resultados (padrão: 5)"},
            },
            "required": ["perfil"],
        },
    },
]


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if hasattr(o, "isoformat"):
            return o.isoformat()
        return super().default(o)


def _json(obj) -> str:
    return json.dumps(obj, cls=DecimalEncoder, ensure_ascii=False)


def tool_buscar_hoteis(nome=None, cidade=None, estado=None, estrelas=None) -> str:
    conditions, params = [], []
    if nome:
        conditions.append("h.name ILIKE %s")
        params.append(f"%{nome}%")
    if cidade:
        conditions.append("h.city ILIKE %s")
        params.append(f"%{cidade}%")
    if estado:
        conditions.append("h.state ILIKE %s")
        params.append(f"%{estado}%")
    if estrelas:
        conditions.append("h.stars = %s")
        params.append(estrelas)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT h.external_id, h.name, h.city, h.state, h.stars, c.name as chain
                FROM hotels h
                LEFT JOIN chains c ON h.chain_id = c.id
                {where}
                ORDER BY h.name
                LIMIT 10
            """, params)
            rows = cur.fetchall()

    if not rows:
        return _json({"resultado": "Nenhum hotel encontrado com esses critérios."})

    return _json([
        {"external_id": r[0], "nome": r[1], "cidade": r[2], "estado": r[3], "estrelas": r[4], "rede": r[5]}
        for r in rows
    ])


def tool_buscar_diarias(hotel_external_id: str, data_checkin: str, data_checkout: str) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM hotels WHERE external_id = %s", (hotel_external_id,))
            hotel = cur.fetchone()
            if not hotel:
                return _json({"erro": f"Hotel com ID {hotel_external_id} não encontrado."})

            hotel_id, hotel_name = hotel

            cur.execute("""
                SELECT date, amount
                FROM hotel_diarias
                WHERE hotel_id = %s AND date >= %s AND date < %s AND amount > 0
                ORDER BY date
            """, (hotel_id, data_checkin, data_checkout))
            rows = cur.fetchall()

    diarias = [{"data": r[0].isoformat(), "valor": float(r[1])} for r in rows]
    total = sum(d["valor"] for d in diarias)

    return _json({
        "hotel": hotel_name,
        "checkin": data_checkin,
        "checkout": data_checkout,
        "noites": len(diarias),
        "total": round(total, 2),
        "diarias": diarias,
    })


def tool_buscar_padroes(hotel_external_id: str, dias: int = 180) -> str:
    weekday_names = {1: "segunda", 2: "terça", 3: "quarta", 4: "quinta", 5: "sexta", 6: "sábado", 7: "domingo"}

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM hotels WHERE external_id = %s", (hotel_external_id,))
            hotel = cur.fetchone()
            if not hotel:
                return _json({"erro": f"Hotel com ID {hotel_external_id} não encontrado."})

            hotel_id, hotel_name = hotel

            cur.execute("""
                SELECT extract(isodow FROM date)::int as dow,
                       avg(amount)::numeric(10,2), min(amount)::numeric(10,2),
                       max(amount)::numeric(10,2), count(*)
                FROM hotel_diarias
                WHERE hotel_id = %s AND date >= CURRENT_DATE
                  AND date < CURRENT_DATE + (%s || ' days')::interval AND amount > 0
                GROUP BY dow ORDER BY dow
            """, (hotel_id, dias))
            rows = cur.fetchall()

    if not rows:
        return _json({"hotel": hotel_name, "resultado": "Sem dados suficientes para análise."})

    stats = [
        {"dia": weekday_names[r[0]], "media": float(r[1]), "min": float(r[2]), "max": float(r[3]), "amostras": r[4]}
        for r in rows
    ]
    cheapest = min(stats, key=lambda x: x["media"])

    return _json({"hotel": hotel_name, "dias_analisados": dias, "dia_mais_barato": cheapest["dia"], "estatisticas": stats})


def tool_buscar_watchlist() -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT w.id, h.name, h.external_id, w.date_start, w.date_end, w.label, w.target_price, w.notify,
                       (SELECT min(d.amount) FROM hotel_diarias d
                        WHERE d.hotel_id = h.id AND d.date BETWEEN w.date_start AND w.date_end AND d.amount > 0),
                       (SELECT avg(d.amount)::numeric(10,2) FROM hotel_diarias d
                        WHERE d.hotel_id = h.id AND d.date BETWEEN w.date_start AND w.date_end AND d.amount > 0)
                FROM watched_hotels w
                JOIN hotels h ON w.hotel_id = h.id
                ORDER BY w.date_start
            """)
            rows = cur.fetchall()

    if not rows:
        return _json({"resultado": "Nenhum hotel na watchlist."})

    return _json([
        {
            "watch_id": r[0], "hotel": r[1], "external_id": r[2],
            "checkin": r[3], "checkout": r[4], "label": r[5],
            "preco_alvo": float(r[6]) if r[6] else None,
            "notificacoes": r[7],
            "menor_preco": float(r[8]) if r[8] else None,
            "preco_medio": float(r[9]) if r[9] else None,
        }
        for r in rows
    ])


def tool_comparar_hoteis(hotel_ids: str, data_checkin: str, data_checkout: str) -> str:
    ids = [x.strip() for x in hotel_ids.split(",") if x.strip()]
    if len(ids) < 2 or len(ids) > 5:
        return _json({"erro": "Informe entre 2 e 5 IDs de hotéis separados por vírgula."})

    resultados = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for ext_id in ids:
                cur.execute("SELECT id, name, city, stars FROM hotels WHERE external_id = %s", (ext_id,))
                hotel = cur.fetchone()
                if not hotel:
                    resultados.append({"external_id": ext_id, "erro": "Hotel não encontrado"})
                    continue

                hotel_id, name, city, stars = hotel
                cur.execute("""
                    SELECT date, amount FROM hotel_diarias
                    WHERE hotel_id = %s AND date >= %s AND date < %s AND amount > 0
                    ORDER BY date
                """, (hotel_id, data_checkin, data_checkout))
                rows = cur.fetchall()

                if not rows:
                    resultados.append({"hotel": name, "cidade": city, "estrelas": stars, "noites": 0, "total": 0})
                    continue

                valores = [float(r[1]) for r in rows]
                resultados.append({
                    "hotel": name, "cidade": city, "estrelas": stars,
                    "noites": len(valores), "total": round(sum(valores), 2),
                    "media_diaria": round(sum(valores) / len(valores), 2),
                    "menor_diaria": round(min(valores), 2),
                    "maior_diaria": round(max(valores), 2),
                })

    return _json({"checkin": data_checkin, "checkout": data_checkout, "comparacao": resultados})


def tool_hotel_detalhes(hotel_external_id: str) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT h.name, h.city, h.state, h.stars, h.address, h.zip_code,
                       h.description, h.check_in, h.check_out, h.amenities, h.room_types,
                       h.latitude, h.longitude, c.name as chain, c.email, c.phone
                FROM hotels h
                LEFT JOIN chains c ON h.chain_id = c.id
                WHERE h.external_id = %s
            """, (hotel_external_id,))
            r = cur.fetchone()
            if not r:
                return _json({"erro": f"Hotel {hotel_external_id} não encontrado."})

    amenities = r[9] if r[9] else []
    room_types = r[10] if r[10] else []

    if isinstance(amenities, str):
        try:
            amenities = json.loads(amenities)
        except (json.JSONDecodeError, TypeError):
            amenities = []

    if isinstance(room_types, str):
        try:
            room_types = json.loads(room_types)
        except (json.JSONDecodeError, TypeError):
            room_types = []

    return _json({
        "hotel": r[0], "cidade": r[1], "estado": r[2], "estrelas": r[3],
        "endereco": r[4], "cep": r[5], "descricao": r[6],
        "check_in": r[7], "check_out": r[8],
        "amenities": amenities[:30] if isinstance(amenities, list) else amenities,
        "tipos_quarto": room_types[:10] if isinstance(room_types, list) else room_types,
        "latitude": float(r[11]) if r[11] else None,
        "longitude": float(r[12]) if r[12] else None,
        "rede": r[13], "email": r[14], "telefone": r[15],
    })


def tool_buscar_mais_baratos(data_checkin: str, data_checkout: str, cidade=None, estado=None, limite: int = 5) -> str:
    conditions = ["d.date >= %s", "d.date < %s", "d.amount > 0"]
    params = [data_checkin, data_checkout]

    if cidade:
        conditions.append("h.city ILIKE %s")
        params.append(f"%{cidade}%")
    if estado:
        conditions.append("h.state ILIKE %s")
        params.append(f"%{estado}%")

    limite = min(limite or 5, 10)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT h.external_id, h.name, h.city, h.state, h.stars,
                       sum(d.amount)::numeric(10,2) as total,
                       avg(d.amount)::numeric(10,2) as media,
                       min(d.amount)::numeric(10,2) as menor,
                       count(*) as noites
                FROM hotel_diarias d
                JOIN hotels h ON d.hotel_id = h.id
                WHERE {' AND '.join(conditions)}
                GROUP BY h.id, h.external_id, h.name, h.city, h.state, h.stars
                HAVING count(*) >= 1
                ORDER BY total ASC
                LIMIT %s
            """, params + [limite])
            rows = cur.fetchall()

    if not rows:
        return _json({"resultado": "Nenhum hotel encontrado com disponibilidade nesse período."})

    return _json({
        "checkin": data_checkin, "checkout": data_checkout,
        "filtro_cidade": cidade, "filtro_estado": estado,
        "hoteis": [
            {"external_id": r[0], "hotel": r[1], "cidade": r[2], "estado": r[3], "estrelas": r[4],
             "total": float(r[5]), "media_diaria": float(r[6]), "menor_diaria": float(r[7]), "noites": r[8]}
            for r in rows
        ],
    })


def tool_historico_precos(hotel_external_id: str, limite: int = 15) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM hotels WHERE external_id = %s", (hotel_external_id,))
            hotel = cur.fetchone()
            if not hotel:
                return _json({"erro": f"Hotel {hotel_external_id} não encontrado."})

            hotel_id, hotel_name = hotel
            cur.execute("""
                SELECT d.date, h.amount as preco_anterior, d.amount as preco_atual, h.captured_at
                FROM hotel_precos_historico h
                JOIN hotel_diarias d ON h.hotel_diaria_id = d.id
                WHERE d.hotel_id = %s
                ORDER BY h.captured_at DESC
                LIMIT %s
            """, (hotel_id, min(limite or 15, 30)))
            rows = cur.fetchall()

    if not rows:
        return _json({"hotel": hotel_name, "resultado": "Sem histórico de mudanças de preço."})

    changes = []
    for r in rows:
        old, new = float(r[1]), float(r[2])
        pct = ((new - old) / old) * 100 if old else 0
        changes.append({
            "data_diaria": r[0].isoformat(), "preco_anterior": old, "preco_atual": new,
            "variacao_pct": round(pct, 1), "capturado_em": r[3].isoformat() if r[3] else None,
        })

    return _json({"hotel": hotel_name, "mudancas": changes})


def tool_buscar_por_cidade(estado=None, limite: int = 10) -> str:
    conditions = ["d.amount > 0", "d.date >= CURRENT_DATE", "h.city IS NOT NULL"]
    params = []

    if estado:
        conditions.append("h.state ILIKE %s")
        params.append(f"%{estado}%")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT h.city, h.state, count(DISTINCT h.id) as qtd_hoteis,
                       avg(d.amount)::numeric(10,2) as preco_medio,
                       min(d.amount)::numeric(10,2) as menor_preco
                FROM hotel_diarias d
                JOIN hotels h ON d.hotel_id = h.id
                WHERE {' AND '.join(conditions)}
                GROUP BY h.city, h.state
                HAVING count(DISTINCT h.id) >= 2
                ORDER BY preco_medio ASC
                LIMIT %s
            """, params + [min(limite or 10, 20)])
            rows = cur.fetchall()

    if not rows:
        return _json({"resultado": "Nenhuma cidade encontrada com dados suficientes."})

    return _json([
        {"cidade": r[0], "estado": r[1], "qtd_hoteis": r[2],
         "preco_medio": float(r[3]), "menor_preco": float(r[4])}
        for r in rows
    ])


def tool_resumo_estatisticas() -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT count(*) FROM chains),
                    (SELECT count(*) FROM hotels),
                    (SELECT count(*) FROM hotel_diarias),
                    (SELECT count(*) FROM hotel_precos_historico),
                    (SELECT count(*) FROM watched_hotels),
                    (SELECT count(DISTINCT city) FROM hotels WHERE city IS NOT NULL),
                    (SELECT count(DISTINCT state) FROM hotels WHERE state IS NOT NULL),
                    (SELECT min(date) FROM hotel_diarias),
                    (SELECT max(date) FROM hotel_diarias),
                    (SELECT count(DISTINCT hotel_id) FROM hotel_diarias)
            """)
            r = cur.fetchone()

    return _json({
        "redes_hoteleiras": r[0], "hoteis": r[1], "diarias_cadastradas": r[2],
        "mudancas_preco_registradas": r[3], "hoteis_monitorados": r[4],
        "cidades": r[5], "estados": r[6],
        "periodo_cobertura": f"{r[7]} a {r[8]}",
        "hoteis_com_preco": r[9],
    })


def tool_adicionar_watchlist(hotel_external_id: str, data_checkin: str, data_checkout: str,
                             label: str = None, preco_alvo: float = None) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM hotels WHERE external_id = %s", (hotel_external_id,))
            hotel = cur.fetchone()
            if not hotel:
                return _json({"erro": f"Hotel {hotel_external_id} não encontrado."})

            hotel_id, hotel_name = hotel

            cur.execute("""
                INSERT INTO watched_hotels (hotel_id, date_start, date_end, label, target_price, notify)
                VALUES (%s, %s, %s, %s, %s, true)
                ON CONFLICT (hotel_id, date_start, date_end) DO UPDATE SET
                    label = EXCLUDED.label,
                    target_price = EXCLUDED.target_price,
                    notify = true,
                    updated_at = now()
                RETURNING id
            """, (hotel_id, data_checkin, data_checkout, label, preco_alvo))
            watch_id = cur.fetchone()[0]
            conn.commit()

    return _json({
        "status": "adicionado",
        "watch_id": watch_id,
        "hotel": hotel_name,
        "checkin": data_checkin,
        "checkout": data_checkout,
        "label": label,
        "preco_alvo": preco_alvo,
    })


def tool_remover_watchlist(watch_id: int) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT w.id, h.name, w.date_start, w.date_end
                FROM watched_hotels w JOIN hotels h ON w.hotel_id = h.id
                WHERE w.id = %s
            """, (watch_id,))
            row = cur.fetchone()
            if not row:
                return _json({"erro": f"Item {watch_id} não encontrado na watchlist."})

            hotel_name, date_start, date_end = row[1], row[2], row[3]
            cur.execute("DELETE FROM watched_hotels WHERE id = %s", (watch_id,))
            conn.commit()

    return _json({
        "status": "removido",
        "watch_id": watch_id,
        "hotel": hotel_name,
        "checkin": str(date_start),
        "checkout": str(date_end),
    })


def tool_sugerir_datas(hotel_external_id: str, mes: int, ano: int, noites: int = 3, limite: int = 5) -> str:
    noites = max(1, min(noites or 3, 14))
    limite = min(limite or 5, 10)

    month_start = f"{ano}-{mes:02d}-01"
    if mes == 12:
        month_end = f"{ano + 1}-01-01"
    else:
        month_end = f"{ano}-{mes + 1:02d}-01"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM hotels WHERE external_id = %s", (hotel_external_id,))
            hotel = cur.fetchone()
            if not hotel:
                return _json({"erro": f"Hotel {hotel_external_id} não encontrado."})

            hotel_id, hotel_name = hotel

            cur.execute("""
                SELECT date, amount
                FROM hotel_diarias
                WHERE hotel_id = %s AND date >= %s AND date < %s AND amount > 0
                ORDER BY date
            """, (hotel_id, month_start, month_end))
            rows = cur.fetchall()

    if len(rows) < noites:
        return _json({"hotel": hotel_name, "resultado": f"Dados insuficientes para {mes:02d}/{ano}."})

    prices = [(r[0], float(r[1])) for r in rows]

    windows = []
    for i in range(len(prices) - noites + 1):
        window = prices[i:i + noites]
        total = sum(p[1] for p in window)
        windows.append({
            "checkin": window[0][0].isoformat(),
            "checkout": window[-1][0].isoformat(),
            "noites": noites,
            "total": round(total, 2),
            "media_diaria": round(total / noites, 2),
        })

    windows.sort(key=lambda w: w["total"])

    return _json({
        "hotel": hotel_name,
        "mes": f"{mes:02d}/{ano}",
        "noites": noites,
        "sugestoes": windows[:limite],
    })


PROFILE_AMENITY_KEYWORDS = {
    "familia": ["piscina", "kids", "infantil", "criança", "playground", "recreação", "brinquedoteca", "family"],
    "casal": ["spa", "sauna", "jacuzzi", "hidromassagem", "romântico", "bar", "lounge", "adulto"],
    "negocios": ["wifi", "business", "centro de convenções", "sala de reunião", "estacionamento", "transfer"],
    "economico": [],
}

PROFILE_STAR_RANGE = {
    "familia": (3, 5),
    "casal": (4, 5),
    "negocios": (3, 5),
    "economico": (1, 3),
}


def tool_recomendar_hoteis(perfil: str, cidade=None, estado=None,
                           data_checkin=None, data_checkout=None, limite: int = 5) -> str:
    perfil = perfil.lower().strip()
    if perfil not in PROFILE_AMENITY_KEYWORDS:
        return _json({"erro": f"Perfil '{perfil}' não reconhecido. Use: familia, casal, negocios, economico."})

    keywords = PROFILE_AMENITY_KEYWORDS[perfil]
    star_min, star_max = PROFILE_STAR_RANGE[perfil]
    limite = min(limite or 5, 10)

    where_conditions = ["h.stars >= %s", "h.stars <= %s"]
    where_params: list = [star_min, star_max]

    if cidade:
        where_conditions.append("h.city ILIKE %s")
        where_params.append(f"%{cidade}%")
    if estado:
        where_conditions.append("h.state ILIKE %s")
        where_params.append(f"%{estado}%")

    if keywords:
        cases = " + ".join(
            f"CASE WHEN h.amenities::text ILIKE '%%{kw}%%' THEN 1 ELSE 0 END"
            for kw in keywords
        )
        amenity_score = f"({cases})"
    else:
        amenity_score = "1"

    price_join = ""
    join_params: list = []
    price_select = "NULL as preco_medio"
    price_order = f"{amenity_score} DESC, h.stars DESC"

    if data_checkin and data_checkout:
        price_join = "LEFT JOIN hotel_diarias d ON d.hotel_id = h.id AND d.date >= %s AND d.date < %s AND d.amount > 0"
        join_params = [data_checkin, data_checkout]
        price_select = "avg(d.amount)::numeric(10,2) as preco_medio"
        if keywords:
            price_order = f"{amenity_score} DESC, preco_medio ASC NULLS LAST"
        else:
            price_order = "preco_medio ASC NULLS LAST"

    all_params = join_params + where_params + [perfil, limite]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT h.external_id, h.name, h.city, h.state, h.stars,
                       {amenity_score} as score, {price_select}
                FROM hotels h
                {price_join}
                WHERE {' AND '.join(where_conditions)} AND h.amenities IS NOT NULL
                GROUP BY h.id, h.external_id, h.name, h.city, h.state, h.stars, h.amenities
                HAVING {amenity_score} > 0 OR %s = 'economico'
                ORDER BY {price_order}
                LIMIT %s
            """, all_params)
            rows = cur.fetchall()

    if not rows:
        return _json({"resultado": f"Nenhum hotel encontrado para o perfil '{perfil}' com esses filtros."})

    return _json({
        "perfil": perfil,
        "filtro_cidade": cidade, "filtro_estado": estado,
        "recomendacoes": [
            {"external_id": r[0], "hotel": r[1], "cidade": r[2], "estado": r[3],
             "estrelas": r[4], "score_amenities": r[5],
             "preco_medio": float(r[6]) if r[6] else None}
            for r in rows
        ],
    })


TOOL_DISPATCH = {
    "buscar_hoteis": tool_buscar_hoteis,
    "buscar_diarias": tool_buscar_diarias,
    "buscar_padroes": tool_buscar_padroes,
    "buscar_watchlist": tool_buscar_watchlist,
    "comparar_hoteis": tool_comparar_hoteis,
    "hotel_detalhes": tool_hotel_detalhes,
    "buscar_mais_baratos": tool_buscar_mais_baratos,
    "historico_precos": tool_historico_precos,
    "buscar_por_cidade": tool_buscar_por_cidade,
    "resumo_estatisticas": tool_resumo_estatisticas,
    "adicionar_watchlist": tool_adicionar_watchlist,
    "remover_watchlist": tool_remover_watchlist,
    "sugerir_datas": tool_sugerir_datas,
    "recomendar_hoteis": tool_recomendar_hoteis,
}


conversation_history: dict[int, list[dict]] = {}


def _get_history(chat_id: int) -> list[dict]:
    if chat_id not in conversation_history:
        conversation_history[chat_id] = []
    return conversation_history[chat_id]


def _trim_history(chat_id: int):
    hist = conversation_history.get(chat_id, [])
    if len(hist) > MAX_HISTORY * 2:
        conversation_history[chat_id] = hist[-(MAX_HISTORY * 2):]


def _build_gemini_tools():
    from google.genai import types

    declarations = []
    for t in TOOLS:
        props = t["parameters"].get("properties", {})
        schema_props = {}
        for k, v in props.items():
            schema_type = v.get("type", "STRING").upper()
            schema_props[k] = types.Schema(type=schema_type, description=v.get("description", ""))

        declarations.append(types.FunctionDeclaration(
            name=t["name"],
            description=t["description"],
            parameters=types.Schema(
                type="OBJECT",
                properties=schema_props,
                required=t["parameters"].get("required", []),
            ) if schema_props else None,
        ))

    return types.Tool(function_declarations=declarations)


def _get_client():
    from google.genai import Client
    return Client(api_key=GOOGLE_API_KEY)


async def _call_gemini(chat_id: int, user_message: str) -> str:
    from google.genai import types

    client = _get_client()
    tool = _build_gemini_tools()

    history = _get_history(chat_id)

    contents = []
    for msg in history:
        contents.append(types.Content(
            role=msg["role"],
            parts=[types.Part.from_text(text=p["text"]) for p in msg["parts"]],
        ))
    contents.append(types.Content(
        role="user",
        parts=[types.Part.from_text(text=user_message)],
    ))

    config = types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=[tool],
    )

    max_turns = 5
    for _ in range(max_turns):
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=contents,
            config=config,
        )

        candidate = response.candidates[0]
        contents.append(candidate.content)

        function_calls = [p for p in candidate.content.parts if p.function_call]
        if not function_calls:
            break

        for fc_part in function_calls:
            fn_name = fc_part.function_call.name
            fn_args = dict(fc_part.function_call.args) if fc_part.function_call.args else {}
            logger.info(f"Tool call: {fn_name}({fn_args})")

            handler = TOOL_DISPATCH.get(fn_name)
            result = handler(**fn_args) if handler else _json({"erro": f"Ferramenta '{fn_name}' não encontrada."})

            contents.append(types.Content(
                role="user",
                parts=[types.Part.from_function_response(
                    name=fn_name,
                    response={"result": json.loads(result)},
                )],
            ))

    text_parts = [p.text for p in candidate.content.parts if p.text]
    reply = "\n".join(text_parts) if text_parts else "Desculpe, não consegui processar sua solicitação."

    conversation_history[chat_id] = [
        {"role": c.role, "parts": [{"text": p.text} for p in c.parts if p.text]}
        for c in contents
        if any(p.text for p in c.parts)
    ]
    _trim_history(chat_id)

    return reply


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversation_history.pop(chat_id, None)
    await update.message.reply_text(
        "🏨 *Assistente de Cotação de Diárias*\n\n"
        "Posso consultar preços de mais de 3.000 hotéis brasileiros.\n\n"
        "*Exemplos:*\n"
        "• _Quanto custa o Tauá de 10/04 a 15/04?_\n"
        "• _Hotel mais barato em Gramado em abril_\n"
        "• _Compare Salinas Maragogi com Cana Brava_\n"
        "• _O que tem no Fasano? Quais amenities?_\n"
        "• _Monitore o Tauá de 01/06 a 05/06_\n"
        "• _Qual o melhor dia da semana pro Japaratinga?_\n"
        "• _Cidades mais baratas na Bahia_\n\n"
        "*Comandos:*\n"
        "/relatorio — Resumo dos hotéis monitorados\n"
        "/reset — Reiniciar conversa\n",
        parse_mode="Markdown",
    )


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversation_history.pop(chat_id, None)
    await update.message.reply_text("🔄 Conversa reiniciada.")


async def cmd_relatorio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT h.name, w.date_start, w.date_end, w.label, w.target_price,
                       (SELECT min(d.amount) FROM hotel_diarias d
                        WHERE d.hotel_id = h.id AND d.date BETWEEN w.date_start AND w.date_end AND d.amount > 0),
                       (SELECT avg(d.amount)::numeric(10,2) FROM hotel_diarias d
                        WHERE d.hotel_id = h.id AND d.date BETWEEN w.date_start AND w.date_end AND d.amount > 0),
                       (SELECT d.amount FROM hotel_diarias d
                        WHERE d.hotel_id = h.id AND d.date = CURRENT_DATE LIMIT 1),
                       (SELECT count(*) FROM hotel_precos_historico hp
                        JOIN hotel_diarias d2 ON hp.hotel_diaria_id = d2.id
                        WHERE d2.hotel_id = h.id AND hp.captured_at >= now() - interval '7 days')
                FROM watched_hotels w
                JOIN hotels h ON w.hotel_id = h.id
                ORDER BY w.date_start
            """)
            rows = cur.fetchall()

    if not rows:
        await update.message.reply_text("📋 Nenhum hotel na watchlist.")
        return

    lines = ["📊 *Relatório da Watchlist*\n"]
    for r in rows:
        name, d_start, d_end, label, target = r[0], r[1], r[2], r[3], r[4]
        min_price, avg_price, today_price, changes_7d = r[5], r[6], r[7], r[8]

        lines.append(f"🏨 *{name}*")
        if label:
            lines.append(f"   🏷️ {label}")
        lines.append(f"   📅 {d_start} → {d_end}")

        price_parts = []
        if min_price:
            price_parts.append(f"Mín: R${float(min_price):.2f}")
        if avg_price:
            price_parts.append(f"Méd: R${float(avg_price):.2f}")
        if today_price:
            price_parts.append(f"Hoje: R${float(today_price):.2f}")
        if price_parts:
            lines.append(f"   💰 {' | '.join(price_parts)}")

        if target:
            hit = "✅" if min_price and float(min_price) <= float(target) else "⏳"
            lines.append(f"   🎯 Alvo: R${float(target):.2f} {hit}")

        if changes_7d:
            lines.append(f"   📈 {changes_7d} mudança(s) de preço nos últimos 7 dias")

        lines.append("")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    chat_id = update.effective_chat.id
    user_text = update.message.text.strip()

    if not user_text:
        return

    if not GOOGLE_API_KEY:
        await update.message.reply_text("⚠️ Bot não configurado (GOOGLE_GENERATIVE_AI_API_KEY ausente).")
        return

    await context.bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        reply = await _call_gemini(chat_id, user_text)
        await update.message.reply_text(reply, parse_mode="Markdown")
    except Exception:
        logger.exception("Erro ao processar mensagem")
        try:
            await update.message.reply_text(reply, parse_mode=None)
        except Exception:
            await update.message.reply_text("❌ Ocorreu um erro ao processar sua mensagem. Tente novamente.")


def run_bot():
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN não configurado")
        return

    if not GOOGLE_API_KEY:
        logger.warning("GOOGLE_GENERATIVE_AI_API_KEY não configurado — bot vai iniciar mas não responderá mensagens")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("relatorio", cmd_relatorio))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot Telegram iniciado (polling)")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    run_bot()
