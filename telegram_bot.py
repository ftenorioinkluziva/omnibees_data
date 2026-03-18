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
                SELECT h.name, w.date_start, w.date_end, w.label, w.target_price,
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
            "hotel": r[0], "checkin": r[1], "checkout": r[2], "label": r[3],
            "preco_alvo": float(r[4]) if r[4] else None,
            "menor_preco": float(r[5]) if r[5] else None,
            "preco_medio": float(r[6]) if r[6] else None,
        }
        for r in rows
    ])


TOOL_DISPATCH = {
    "buscar_hoteis": tool_buscar_hoteis,
    "buscar_diarias": tool_buscar_diarias,
    "buscar_padroes": tool_buscar_padroes,
    "buscar_watchlist": tool_buscar_watchlist,
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
        "Exemplos:\n"
        "• _Quanto custa o Tauá de 10/04 a 15/04?_\n"
        "• _Qual o hotel mais barato em Gramado?_\n"
        "• _Quais hotéis estou monitorando?_\n"
        "• _Qual o melhor dia da semana para reservar o Fasano?_\n\n"
        "Envie sua pergunta!",
        parse_mode="Markdown",
    )


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conversation_history.pop(chat_id, None)
    await update.message.reply_text("🔄 Conversa reiniciada.")


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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot Telegram iniciado (polling)")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    run_bot()
