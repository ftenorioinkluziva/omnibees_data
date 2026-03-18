#!/usr/bin/env python3
"""
Omnibees Data CLI
=================
Ponto de entrada unificado para todas as operações.

Uso:
    python cli.py status                          # Ver status do banco
    python cli.py prices                          # Coletar preços de todos os hotéis
    python cli.py prices --hotels 9098 2281       # Coletar preços de hotéis específicos
    python cli.py prices --months 6 --workers 5   # Ajustar parâmetros
    python cli.py scrape --start 0 --end 9999     # Scrape de chains/hotéis
    python cli.py migrate                         # Migrar JSONs → PostgreSQL
    python cli.py query hotels --city "Natal"     # Consultar hotéis
    python cli.py query prices --hotel 9098       # Consultar preços
    python cli.py healthcheck                     # Verificar saúde do sistema
"""

import argparse
import sys


def cmd_status(args):
    from db import get_stats
    stats = get_stats()
    print("=" * 50)
    print("OMNIBEES DATA - STATUS")
    print("=" * 50)
    print(f"  Chains:           {stats['chains']:>10,}")
    print(f"  Hotéis:           {stats['hotels']:>10,}")
    print(f"  Diárias:          {stats['diarias']:>10,}")
    print(f"  Histórico preços: {stats['historico']:>10,}")
    print(f"  Período:          {stats['date_min']} a {stats['date_max']}")
    print("=" * 50)


def cmd_prices(args):
    from omnibees_price_scraper import run
    import asyncio
    asyncio.run(run(args.hotels, args.months, args.workers, args.delay, args.batch_size, args.resume))


def cmd_scrape(args):
    from omnibees_complete_scraper import OmnibeesCompleteScraper
    scraper = OmnibeesCompleteScraper(
        delay=args.delay,
        timeout=args.timeout,
        country_filter=args.country,
    )
    scraper.run(start_id=args.start, end_id=args.end, resume=args.resume)


def cmd_migrate(args):
    from migrate_to_postgres import main as migrate_main
    migrate_main()


def cmd_fix_locations(args):
    from fix_hotel_locations import run
    run(
        apply_changes=args.apply,
        limit=args.limit,
        delay=args.delay,
        summary_only=args.summary_only,
        use_viacep=args.with_viacep,
    )


def cmd_query(args):
    from db import get_connection
    from collections import defaultdict

    with get_connection() as conn:
        with conn.cursor() as cur:
            if args.target == "hotels":
                conditions = []
                params = []

                if args.city:
                    conditions.append("city ILIKE %s")
                    params.append(f"%{args.city}%")
                if args.state:
                    conditions.append("state ILIKE %s")
                    params.append(f"%{args.state}%")
                if args.stars:
                    conditions.append("stars = %s")
                    params.append(args.stars)
                if args.chain:
                    conditions.append("""
                        chain_id IN (SELECT id FROM chains WHERE name ILIKE %s)
                    """)
                    params.append(f"%{args.chain}%")
                if args.name:
                    conditions.append("name ILIKE %s")
                    params.append(f"%{args.name}%")

                where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
                query = f"""
                    SELECT h.external_id, h.name, h.city, h.state, h.stars,
                           c.name as chain_name
                    FROM hotels h
                    LEFT JOIN chains c ON h.chain_id = c.id
                    {where}
                    ORDER BY h.name
                    LIMIT %s
                """
                params.append(args.limit)
                cur.execute(query, params)

                rows = cur.fetchall()
                print(f"\n{'ID':<8} {'Hotel':<45} {'Cidade':<20} {'UF':<4} {'*':<3} {'Rede'}")
                print("-" * 110)
                for r in rows:
                    print(f"{r[0]:<8} {(r[1] or '')[:44]:<45} {(r[2] or '')[:19]:<20} {(r[3] or ''):<4} {r[4] or '-':<3} {(r[5] or '')[:25]}")
                print(f"\n{len(rows)} resultado(s)")

            elif args.target == "prices":
                if not args.hotel:
                    print("Erro: --hotel é obrigatório para consulta de preços")
                    return

                cur.execute("""
                    SELECT d.date, d.amount, d.updated_at
                    FROM hotel_diarias d
                    JOIN hotels h ON d.hotel_id = h.id
                    WHERE h.external_id = %s
                    AND d.date >= CURRENT_DATE
                    ORDER BY d.date
                    LIMIT %s
                """, (args.hotel, args.limit))

                rows = cur.fetchall()
                if not rows:
                    print(f"Nenhum preço encontrado para hotel {args.hotel}")
                    return

                cur.execute("SELECT name FROM hotels WHERE external_id = %s", (args.hotel,))
                hotel_name = cur.fetchone()
                print(f"\nPreços: {hotel_name[0] if hotel_name else args.hotel}")
                print(f"{'Data':<12} {'Preço (R$)':>12} {'Atualizado em'}")
                print("-" * 55)
                for r in rows:
                    print(f"{str(r[0]):<12} {float(r[1]):>12,.2f} {str(r[2])[:19]}")
                print(f"\n{len(rows)} diária(s)")

            elif args.target == "history":
                if not args.hotel:
                    print("Erro: --hotel é obrigatório para consulta de histórico")
                    return

                cur.execute("""
                    SELECT d.date, h2.amount as old_price, d.amount as current_price, h2.captured_at
                    FROM hotel_precos_historico h2
                    JOIN hotel_diarias d ON h2.hotel_diaria_id = d.id
                    JOIN hotels h ON d.hotel_id = h.id
                    WHERE h.external_id = %s
                    ORDER BY h2.captured_at DESC
                    LIMIT %s
                """, (args.hotel, args.limit))

                rows = cur.fetchall()
                if not rows:
                    print(f"Nenhum histórico para hotel {args.hotel}")
                    return

                print(f"\n{'Data diária':<12} {'Preço antigo':>14} {'Preço atual':>14} {'Capturado em'}")
                print("-" * 65)
                for r in rows:
                    print(f"{str(r[0]):<12} R$ {float(r[1]):>10,.2f} R$ {float(r[2]):>10,.2f} {str(r[3])[:19]}")
                print(f"\n{len(rows)} alteração(ões)")

            elif args.target == "patterns":
                if not args.hotel:
                    print("Erro: --hotel é obrigatório para consulta de padrões")
                    return

                cur.execute("SELECT id, name FROM hotels WHERE external_id = %s", (args.hotel,))
                hotel_row = cur.fetchone()
                if not hotel_row:
                    print(f"Hotel não encontrado: {args.hotel}")
                    return

                hotel_id, hotel_name = hotel_row
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
                    (hotel_id, args.limit),
                )
                rows = cur.fetchall()

                if not rows:
                    print(f"Sem dados para padrões do hotel {args.hotel}")
                    return

                weekday_names = {
                    0: "segunda",
                    1: "terça",
                    2: "quarta",
                    3: "quinta",
                    4: "sexta",
                    5: "sábado",
                    6: "domingo",
                }
                weekday_values: dict[int, list[float]] = defaultdict(list)
                for dt, amount in rows:
                    weekday_values[dt.weekday()].append(float(amount))

                weekday_stats = []
                for weekday in sorted(weekday_values.keys()):
                    values = weekday_values[weekday]
                    weekday_stats.append((weekday_names[weekday], sum(values) / len(values), min(values), max(values), len(values)))

                cheapest = min(weekday_stats, key=lambda item: item[1])

                amounts = [float(r[1]) for r in rows]
                midpoint = len(amounts) // 2
                trend = "dados insuficientes"
                trend_pct = None
                if midpoint >= 2 and len(amounts) - midpoint >= 2:
                    first_half = sum(amounts[:midpoint]) / midpoint
                    second_half = sum(amounts[midpoint:]) / (len(amounts) - midpoint)
                    trend_pct = ((second_half - first_half) / first_half) * 100 if first_half else 0
                    if trend_pct > 3:
                        trend = "alta"
                    elif trend_pct < -3:
                        trend = "baixa"
                    else:
                        trend = "estável"

                print(f"\nPadrões: {hotel_name} ({args.hotel})")
                print(f"Janela analisada: próximos {args.limit} dias")
                print(f"Pontos analisados: {len(rows)}")
                print(f"Dia mais barato: {cheapest[0]} (média R$ {cheapest[1]:,.2f})")
                if trend_pct is not None:
                    print(f"Tendência: {trend} ({trend_pct:+.2f}%)")
                else:
                    print(f"Tendência: {trend}")

                print(f"\n{'Dia':<10} {'Média':>12} {'Mín':>12} {'Máx':>12} {'Amostras':>9}")
                print("-" * 62)
                for day_name, avg_value, min_value, max_value, samples in weekday_stats:
                    print(f"{day_name:<10} {avg_value:>12,.2f} {min_value:>12,.2f} {max_value:>12,.2f} {samples:>9}")


def cmd_telegram_setup(args):
    import requests
    from config import TELEGRAM_BOT_TOKEN

    if not TELEGRAM_BOT_TOKEN:
        print("Erro: TELEGRAM_BOT_TOKEN não configurado no .env")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
    except Exception as e:
        print(f"Erro ao conectar ao Telegram: {e}")
        return

    if not data.get("ok"):
        print(f"Erro da API Telegram: {data.get('description')}")
        return

    updates = data.get("result", [])
    if not updates:
        print("Nenhuma mensagem recebida ainda.")
        print("Envie qualquer mensagem para @agenthotels_bot no Telegram e execute este comando novamente.")
        return

    chat_ids: dict[int, str] = {}
    for update in updates:
        msg = update.get("message") or update.get("channel_post")
        if msg:
            chat = msg.get("chat", {})
            chat_id = chat.get("id")
            name = chat.get("title") or chat.get("first_name") or chat.get("username") or "?"
            if chat_id:
                chat_ids[chat_id] = name

    if not chat_ids:
        print("Nenhum chat encontrado. Envie uma mensagem para o bot primeiro.")
        return

    print("\nChats encontrados:")
    for cid, name in chat_ids.items():
        print(f"  Chat ID: {cid}  |  Nome: {name}")
    print("\nAdicione ao .env:  TELEGRAM_CHAT_ID=<seu_chat_id>")


def cmd_healthcheck(args):
    from db import get_stats, get_connection
    from datetime import datetime, timedelta
    import json

    issues = []
    stats = get_stats()

    if stats["chains"] == 0:
        issues.append("CRITICAL: Nenhuma chain no banco")
    if stats["hotels"] == 0:
        issues.append("CRITICAL: Nenhum hotel no banco")
    if stats["diarias"] == 0:
        issues.append("WARNING: Nenhuma diária no banco")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT max(updated_at) FROM hotel_diarias
            """)
            last_update = cur.fetchone()[0]
            if last_update:
                hours_ago = (datetime.now() - last_update).total_seconds() / 3600
                if hours_ago > 24:
                    issues.append(f"WARNING: Última atualização de preços há {hours_ago:.0f}h")

            cur.execute("""
                SELECT count(*) FROM hotels
                WHERE coalesce(city, '') = '' OR coalesce(description, '') = ''
            """)
            incomplete = cur.fetchone()[0]
            if incomplete > 50:
                issues.append(f"INFO: {incomplete} hotéis com dados incompletos")

    critical = [i for i in issues if i.startswith("CRITICAL")]
    if issues:
        print("PROBLEMAS ENCONTRADOS:")
        for issue in issues:
            print(f"  - {issue}")
        if critical:
            sys.exit(1)
    else:
        print(f"OK | chains={stats['chains']} hotels={stats['hotels']} "
              f"diarias={stats['diarias']} historico={stats['historico']}")
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        prog="omnibees",
        description="Omnibees Data CLI - Gerenciamento de dados hoteleiros",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Ver status do banco de dados")

    p_prices = sub.add_parser("prices", help="Coletar preços de diárias")
    p_prices.add_argument("--hotels", nargs="+", help="External IDs (default: todos)")
    p_prices.add_argument("--months", type=int, default=15, help="Meses à frente (default: 15)")
    p_prices.add_argument("--workers", type=int, default=3, help="Workers simultâneos (default: 3)")
    p_prices.add_argument("--delay", type=float, default=0.5, help="Delay entre requests (default: 0.5)")
    p_prices.add_argument("--batch-size", type=int, default=250, help="Hotéis por lote (default: 250)")
    p_prices.add_argument("--resume", action="store_true", help="Retomar do último checkpoint de preços")

    p_scrape = sub.add_parser("scrape", help="Scrape de chains e hotéis do Omnibees")
    p_scrape.add_argument("--start", type=int, default=0, help="ID inicial (default: 0)")
    p_scrape.add_argument("--end", type=int, default=9999, help="ID final (default: 9999)")
    p_scrape.add_argument("--country", type=str, default=None, help="Filtrar por país")
    p_scrape.add_argument("--delay", type=float, default=1.0, help="Delay entre requests (default: 1.0)")
    p_scrape.add_argument("--timeout", type=int, default=30, help="Timeout (default: 30s)")
    p_scrape.add_argument("--resume", action="store_true", help="Continuar do checkpoint")

    sub.add_parser("migrate", help="Migrar JSONs locais para PostgreSQL")

    p_fix_locations = sub.add_parser("fix-locations", help="Corrigir city/state/zip dos hotéis")
    p_fix_locations.add_argument("--apply", action="store_true", help="Aplica no banco (default: dry-run)")
    p_fix_locations.add_argument("--limit", type=int, default=None, help="Limita hotéis analisados")
    p_fix_locations.add_argument("--delay", type=float, default=0.02, help="Delay entre consultas ViaCEP")
    p_fix_locations.add_argument("--summary-only", action="store_true", help="Mostra apenas totais")
    p_fix_locations.add_argument("--with-viacep", action="store_true", help="Ativa enriquecimento por ViaCEP (mais lento)")

    p_query = sub.add_parser("query", help="Consultar dados")
    p_query.add_argument("target", choices=["hotels", "prices", "history", "patterns"], help="O que consultar")
    p_query.add_argument("--hotel", type=str, help="External ID do hotel")
    p_query.add_argument("--city", type=str, help="Filtrar por cidade")
    p_query.add_argument("--state", type=str, help="Filtrar por estado")
    p_query.add_argument("--stars", type=int, help="Filtrar por estrelas")
    p_query.add_argument("--chain", type=str, help="Filtrar por nome da rede")
    p_query.add_argument("--name", type=str, help="Filtrar por nome do hotel")
    p_query.add_argument("--limit", type=int, default=50, help="Limite de resultados (default: 50)")

    sub.add_parser("healthcheck", help="Verificar saúde do sistema")
    sub.add_parser("telegram-setup", help="Obter Chat ID para configurar alertas Telegram")

    args = parser.parse_args()
    commands = {
        "status": cmd_status,
        "prices": cmd_prices,
        "scrape": cmd_scrape,
        "migrate": cmd_migrate,
        "fix-locations": cmd_fix_locations,
        "query": cmd_query,
        "healthcheck": cmd_healthcheck,
        "telegram-setup": cmd_telegram_setup,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
