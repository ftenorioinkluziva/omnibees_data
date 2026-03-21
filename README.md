# Omnibees Data

Plataforma de monitoramento de precos hoteleiros do Omnibees (book.omnibees.com). O usuario escolhe ate 10 hoteis para monitorar, e o sistema coleta precos 4x/dia, alerta variacoes e oferece consulta em tempo real via bot Telegram com IA.

## Stack

- **Backend:** Python 3.11, FastAPI, psycopg2
- **Banco:** PostgreSQL (Neon serverless)
- **Frontend:** HTML/CSS/JS vanilla, Chart.js
- **Scraping:** aiohttp (async), BeautifulSoup
- **Bot Telegram:** python-telegram-bot + Google Gemini (function calling)
- **Deploy:** Docker, GitHub Actions CI/CD, VPS

## Estrutura

```
omnibees_data/
├── api.py                      # FastAPI - dashboard + watchlist API
├── cli.py                      # CLI unificado (status, prices, scrape, query, bot)
├── config.py                   # Configuracao centralizada (.env)
├── db.py                       # Conexao PostgreSQL com retry
├── telegram_bot.py             # Bot Telegram com IA (Gemini + function calling)
├── telegram_alerts.py          # Alertas de variacao de preco via Telegram
├── omnibees_price_scraper.py   # Scraper de precos (substitui n8n)
├── omnibees_complete_scraper.py # Scraper de chains/hoteis (sincrono)
├── omnibees_async_scraper.py   # Scraper de chains/hoteis (async)
├── omnibees_rescraper.py       # Re-scraper de hoteis com dados incompletos
├── migrate_to_postgres.py      # Migracao JSON -> PostgreSQL (one-time)
├── location_parser.py          # Parser robusto de localizacao (cidade, UF, CEP)
├── fix_hotel_locations.py      # Backfill de city/state/zip no banco
├── static/
│   ├── index.html              # Dashboard principal
│   ├── style.css               # Design system (dark theme)
│   ├── app.js                  # Dashboard JS
│   ├── watchlist.html          # Watchlist de hoteis
│   ├── watchlist.css           # Estilos watchlist
│   └── watchlist.js            # Watchlist JS
├── Dockerfile
├── crontab                     # Precos watchlist 4x/dia, healthcheck 1/1h
├── entrypoint.sh               # Entrypoint Docker (api/cron/bot/cli)
├── requirements.txt
├── .github/workflows/deploy.yml # CI/CD - deploy automatico na VPS
└── .env                        # Variaveis de ambiente (nao commitado)
```

## Banco de Dados

| Tabela | Descricao |
|--------|-----------|
| `chains` | ~2.680 redes hoteleiras |
| `hotels` | ~3.359 hoteis com detalhes, amenities, fotos |
| `hotel_diarias` | ~975K diarias (date + amount + hotel_id) |
| `hotel_precos_historico` | ~971K registros de variacao de preco |
| `watched_hotels` | Hoteis monitorados pelo usuario (watchlist) |

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

pip install -r requirements.txt

# Criar .env com variaveis necessarias
cat > .env <<EOF
DATABASE_URL=postgresql://...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
GOOGLE_GENERATIVE_AI_API_KEY=...
EOF
```

## Variaveis de Ambiente

| Variavel | Obrigatoria | Descricao |
|----------|-------------|-----------|
| `DATABASE_URL` | Sim | Connection string PostgreSQL (Neon) |
| `TELEGRAM_BOT_TOKEN` | Bot | Token do bot Telegram (@BotFather) |
| `TELEGRAM_CHAT_ID` | Alertas | Chat ID para alertas automaticos |
| `GOOGLE_GENERATIVE_AI_API_KEY` | Bot | API key Google AI (Gemini) |

## Bot Telegram (IA)

Bot conversacional com Google Gemini 2.5 Flash e function calling. Consulta precos em tempo real na API do Omnibees e monitora ate 10 hoteis favoritos com alertas de variacao.

### Comandos

| Comando | Descricao |
|---------|-----------|
| `/start` | Mensagem de boas-vindas com exemplos de uso |
| `/reset` | Limpar historico de conversa |
| `/relatorio` | Resumo dos hoteis monitorados na watchlist |

### Tools (14 funcoes via function calling)

| Tool | Descricao |
|------|-----------|
| `buscar_hoteis` | Busca hoteis por nome, cidade, estado ou estrelas |
| `buscar_diarias` | Precos de diarias para um periodo |
| `buscar_padroes` | Padroes de preco (dia mais barato, tendencias) |
| `buscar_watchlist` | Lista hoteis monitorados na watchlist |
| `comparar_hoteis` | Compara precos de 2-3 hoteis lado a lado |
| `hotel_detalhes` | Informacoes completas (amenities, quartos, fotos) |
| `buscar_mais_baratos` | Top N hoteis mais baratos por regiao/periodo |
| `historico_precos` | Evolucao do preco ao longo do tempo |
| `buscar_por_cidade` | Ranking de cidades por preco medio e qtd hoteis |
| `resumo_estatisticas` | Stats gerais da base de dados |
| `adicionar_watchlist` | Adicionar hotel ao monitoramento |
| `remover_watchlist` | Remover hotel do monitoramento |
| `sugerir_datas` | Datas mais baratas dado um hotel e mes |
| `recomendar_hoteis` | Recomendacao por perfil (familia, casal, negocios) |

### Exemplos de uso

```
"Qual o preço do Japaratinga Lounge para o feriado de 12 a 15 de junho?"
"Compare Japaratinga e Salinas Maragogi para julho"
"Quais os 5 hotéis mais baratos em Maceió em agosto?"
"Recomende hotéis para família em Alagoas"
"Qual a melhor data para ir ao Salinas Maragogi em setembro?"
"Adicione o hotel 9098 na watchlist de 01/07 a 05/07 com alvo de R$800"
```

## Dashboard + API

```bash
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

Acesse `http://localhost:8000` para o dashboard e `http://localhost:8000/watchlist.html` para a watchlist.

### API Endpoints

**Dashboard:**
- `GET /api/stats` - KPIs (hoteis, redes, diarias, media)
- `GET /api/hotels` - Lista com filtros (cidade, UF, estrelas, rede)
- `GET /api/hotels/{id}` - Detalhes do hotel
- `GET /api/hotels/{id}/prices` - Precos por periodo
- `GET /api/hotels/{id}/history` - Historico de variacoes
- `GET /api/compare` - Comparacao entre hoteis
- `GET /api/top-cities` - Cidades com mais hoteis
- `GET /api/price-distribution` - Distribuicao de faixas de preco
- `GET /api/filters` - Opcoes de filtro (UFs, estrelas, redes)

**Watchlist:**
- `GET /api/hotels-search?q=` - Autocomplete de hoteis
- `GET /api/watchlist` - Lista watches com precos agregados
- `POST /api/watchlist` - Adicionar hotel monitorado
- `DELETE /api/watchlist/{id}` - Remover watch
- `GET /api/watchlist/{id}/prices` - Precos diarios para grafico

## CLI

```bash
python cli.py status              # Stats do banco
python cli.py prices --months 3   # Coletar precos (3 meses)
python cli.py scrape              # Scrape chains/hoteis
python cli.py query hotels        # Consultar hoteis
python cli.py query prices 9098   # Precos de um hotel
python cli.py healthcheck         # Verificar conectividade
python cli.py bot                 # Iniciar bot Telegram
python cli.py fix-locations                        # Dry-run
python cli.py fix-locations --apply                # Aplicar correcoes
python cli.py fix-locations --limit 500            # Limitar hoteis
python cli.py fix-locations --summary-only         # Apenas totalizador
python cli.py fix-locations --with-viacep --apply  # Enriquecer via ViaCEP
```

## Scraper de Precos

```bash
python omnibees_price_scraper.py --hotels 50 --months 6 --workers 5 --delay 0.5
```

| Parametro | Default | Descricao |
|-----------|---------|-----------|
| `--hotels` | 10 | Quantidade de hoteis |
| `--months` | 3 | Meses a frente para coletar |
| `--workers` | 5 | Requisicoes simultaneas |
| `--delay` | 0.5 | Delay entre requisicoes (s) |

## Docker

```bash
docker build -t omnibees-data .
docker run --env-file .env omnibees-data api      # Dashboard + API
docker run --env-file .env omnibees-data bot      # Bot Telegram
docker run --env-file .env omnibees-data cron     # Agendador (precos + scrape)
docker run --env-file .env omnibees-data prices   # Coleta unica de precos
docker run --env-file .env omnibees-data status   # Stats do banco
```

## Producao (VPS)

Deploy automatico via GitHub Actions (push na `main` → rsync + docker rebuild).

### Containers

| Container | Funcao | Porta |
|-----------|--------|-------|
| `omnibees-api` | Dashboard + API REST | 8000 |
| `omnibees-cron` | Coleta de precos da watchlist (4x/dia) | - |
| `omnibees-bot` | Bot Telegram com IA | - |

### Acesso

- Dashboard: `http://89.167.106.38:8000/`
- Watchlist: `http://89.167.106.38:8000/watchlist.html`
- API: `http://89.167.106.38:8000/api/stats`

### Deploy manual

```bash
cd /opt/omnibees_data
docker build -t omnibees-data:latest .
docker rm -f omnibees-api omnibees-cron omnibees-bot || true
docker run -d --name omnibees-api --restart unless-stopped --env-file .env -p 8000:8000 omnibees-data:latest api
docker run -d --name omnibees-cron --restart unless-stopped --env-file .env omnibees-data:latest cron
docker run -d --name omnibees-bot --restart unless-stopped --env-file .env omnibees-data:latest bot
```

### Verificacao

```bash
docker ps
docker logs --tail 60 omnibees-bot
curl -fsS http://127.0.0.1:8000/api/stats
```

## Features

### Dashboard
- KPIs em tempo real (hoteis, redes, diarias, media hoje, alteracoes 24h)
- Ranking de cidades por quantidade de hoteis
- Grafico de distribuicao de faixas de preco
- Tabela de hoteis com filtros (busca, UF, estrelas, rede)
- Painel de detalhes com grafico de precos e historico

### Watchlist
- Busca de hoteis com autocomplete
- Monitoramento por periodo customizado (check-in / check-out)
- Preco alvo com indicador visual (atingido / acima)
- Cards com preco minimo, medio e de hoje
- Grafico de evolucao de precos com linha de alvo

### Bot Telegram
- Conversa natural em portugues via Gemini 2.5 Flash
- 14 tools com function calling para queries SQL
- Historico de conversa (10 mensagens por chat)
- Gerenciamento de watchlist por conversa
- Recomendacao por perfil (familia, casal, negocios, economico)
- Sugestao de datas mais baratas (sliding window)
- Relatorio resumido dos hoteis monitorados

### Alertas
- Notificacao automatica via Telegram quando preco varia
- Integrado ao cron (executa a cada coleta de precos)

### Design
- Dark theme editorial (DM Serif Display, IBM Plex Sans, JetBrains Mono)
- Paleta: fundo #0a0a0b, accent amber #f59e0b, verde #22c55e
- Responsivo (mobile-first grid)
- Animacoes CSS (card-in, slide-up)
- Noise overlay texture
