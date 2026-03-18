# Omnibees Data

Plataforma de coleta, armazenamento e visualizacao de dados hoteleiros do Omnibees (book.omnibees.com). Monitora precos de diarias de ~3.400 hoteis brasileiros com dashboard interativo e watchlist personalizada.

## Stack

- **Backend:** Python 3.11, FastAPI, psycopg2
- **Banco:** PostgreSQL (Neon serverless)
- **Frontend:** HTML/CSS/JS vanilla, Chart.js
- **Scraping:** aiohttp (async), BeautifulSoup
- **Deploy:** Docker + cron

## Estrutura

```
omnibees_data/
├── api.py                      # FastAPI - dashboard + watchlist API
├── cli.py                      # CLI unificado (status, prices, scrape, query)
├── config.py                   # Configuracao centralizada (.env)
├── db.py                       # Conexao PostgreSQL com retry
├── omnibees_price_scraper.py   # Scraper de precos (substitui n8n)
├── omnibees_complete_scraper.py # Scraper de chains/hoteis (sincrono)
├── omnibees_async_scraper.py   # Scraper de chains/hoteis (async)
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
├── crontab                     # Precos 6/6h, scrape domingos, healthcheck 1/1h
├── entrypoint.sh
├── requirements.txt
└── .env                        # DATABASE_URL (nao commitado)
```

## Banco de Dados

| Tabela | Descricao |
|--------|-----------|
| `chains` | 2.680 redes hoteleiras |
| `hotels` | 3.359 hoteis com detalhes |
| `hotel_diarias` | ~975K diarias (date + amount + hotel_id) |
| `hotel_precos_historico` | ~971K registros de variacao de preco |
| `watched_hotels` | Hoteis monitorados pelo usuario (watchlist) |

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

pip install -r requirements.txt

# Criar .env com DATABASE_URL
echo 'DATABASE_URL=postgresql://...' > .env
```

## Uso

### Dashboard + API

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

### CLI

```bash
python cli.py status              # Stats do banco
python cli.py prices --months 3   # Coletar precos (3 meses)
python cli.py scrape              # Scrape chains/hoteis
python cli.py query hotels        # Consultar hoteis
python cli.py query prices 9098   # Precos de um hotel
python cli.py healthcheck         # Verificar conectividade
python cli.py fix-locations                        # Dry-run (nao altera o banco)
python cli.py fix-locations --apply                # Aplicar correcoes no banco
python cli.py fix-locations --limit 500            # Limitar hoteis analisados
python cli.py fix-locations --summary-only         # Apenas totalizador, sem detalhes
python cli.py fix-locations --with-viacep --apply  # Enriquecer via ViaCEP (mais lento)
```

| Flag | Default | Descricao |
|------|---------|----------|
| `--apply` | off | Persiste as alteracoes no banco |
| `--limit N` | todos | Processa somente os N primeiros hoteis |
| `--delay S` | 0.05 | Intervalo em segundos entre registros |
| `--summary-only` | off | Exibe apenas o totalizador final |
| `--with-viacep` | off | Consulta ViaCEP para enriquecer cidade/estado |

### Scraper de Precos

```bash
python omnibees_price_scraper.py --hotels 50 --months 6 --workers 5 --delay 0.5
```

| Parametro | Default | Descricao |
|-----------|---------|-----------|
| `--hotels` | 10 | Quantidade de hoteis |
| `--months` | 3 | Meses a frente para coletar |
| `--workers` | 5 | Requisicoes simultaneas |
| `--delay` | 0.5 | Delay entre requisicoes (s) |

### Docker

```bash
docker build -t omnibees-data .
docker run --env-file .env omnibees-data cron    # Modo cron
docker run --env-file .env omnibees-data prices   # Coleta unica
docker run --env-file .env omnibees-data api      # Subir API
```

## Producao (VPS)

Deploy validado em VPS Ubuntu com Docker.

### Acesso Frontend/API

- Dashboard: `http://89.167.106.38:8000/`
- Watchlist: `http://89.167.106.38:8000/watchlist.html`
- API stats: `http://89.167.106.38:8000/api/stats`

### Containers em execucao

- `omnibees-api` (porta `8000`)
- `omnibees-cron` (agendador)

### Comandos de deploy no servidor

```bash
cd /opt/omnibees_data
docker build -t omnibees-data:latest .

docker rm -f omnibees-api omnibees-cron || true
docker run -d --name omnibees-api --restart unless-stopped --env-file .env -p 8000:8000 omnibees-data:latest api
docker run -d --name omnibees-cron --restart unless-stopped --env-file .env omnibees-data:latest cron
```

### Verificacao rapida

```bash
docker ps
docker logs --tail 60 omnibees-api
docker logs --tail 60 omnibees-cron
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

### Design
- Dark theme editorial (DM Serif Display, IBM Plex Sans, JetBrains Mono)
- Paleta: fundo #0a0a0b, accent amber #f59e0b, verde #22c55e
- Responsivo (mobile-first grid)
- Animacoes CSS (card-in, slide-up)
- Noise overlay texture
