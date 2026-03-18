# Roadmap

## Concluido
- [x] Scrapers de chains e hoteis (sincrono + async)
- [x] Migracao JSON -> PostgreSQL (Neon)
- [x] Scraper de precos (substituto do n8n)
- [x] CLI unificado (status, prices, scrape, query, healthcheck)
- [x] Dashboard interativo (FastAPI + Chart.js)
- [x] Watchlist de hoteis (monitoramento por periodo + preco alvo)
- [x] Docker + crontab configurados
- [x] Alertas Telegram (notificacao em tempo real de variacao de preco para hoteis na watchlist)
- [x] Detectar padroes (dia da semana mais barato, tendencia de alta/baixa)
- [x] Expandir coleta para todos os 3.359 hoteis (lotes + checkpoint/resume)
- [x] Deploy VPS + cron

## Proximo
- [ ] Configurar dominio + HTTPS (TLS)
- [ ] Desligar workflow n8n

## Futuro
- [ ] Hardening de producao (backup/monitoramento)
