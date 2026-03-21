# Roadmap

## Concluido
- [x] Scrapers de chains e hoteis (sincrono + async)
- [x] Migracao JSON -> PostgreSQL (Neon)
- [x] Scraper de precos (substituto do n8n)
- [x] CLI unificado (status, prices, scrape, query, healthcheck, bot)
- [x] Dashboard interativo (FastAPI + Chart.js)
- [x] Watchlist de hoteis (monitoramento por periodo + preco alvo)
- [x] Docker + crontab configurados
- [x] Alertas Telegram (notificacao de variacao de preco para watchlist)
- [x] Deploy VPS + CI/CD (GitHub Actions)
- [x] Bot Telegram com AI (Gemini + function calling, substitui n8n agent)
- [x] 14 tools: buscar, comparar, recomendar, watchlist, historico, padroes, etc.
- [x] Modelo watchlist-first: consultas em tempo real via API Omnibees
- [x] Cron coleta apenas watchlist (4x/dia), scrape massivo removido
- [x] Limite de 10 hotéis na watchlist
- [x] Padrões e histórico restritos a hotéis monitorados

## Proximo
- [ ] Alertar automaticamente no chat quando preço de hotel monitorado atingir target
- [ ] Sugestões proativas ("Você monitora o Japaratinga — o preço baixou 12% essa semana!")
- [ ] Formatação rica nas respostas (tabelas, emojis contextuais)
- [ ] Suporte a áudio/voz (transcrever via Whisper e processar como texto)

## Backlog
- [ ] Configurar dominio + HTTPS (TLS)
- [ ] Desligar workflow n8n
- [ ] Consulta de passagens aereas via bot Telegram (Apify Google Flights scraper)
- [ ] Hardening de producao (backup/monitoramento)
