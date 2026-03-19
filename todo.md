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
- [x] Bot Telegram com AI (Gemini + function calling, substitui n8n agent)
- [x] Tools: comparar_hoteis, hotel_detalhes, buscar_mais_baratos, historico_precos, buscar_por_cidade, resumo_estatisticas
- [x] Gerenciar watchlist pelo bot (adicionar/remover hotéis monitorados via conversa)
- [x] Comando /relatorio — resumo dos hotéis monitorados
- [x] Sugerir datas mais baratas dado um hotel e um mês
- [x] Recomendar hotéis por perfil (família, casal, negócios) usando amenities + estrelas + preço

## Proximo — Melhorias do Bot Telegram

### Melhorias de UX
- [ ] Formatação rica nas respostas (tabelas, emojis contextuais, links para dashboard)
- [ ] Suporte a áudio/voz (transcrever via Whisper e processar como texto)
- [ ] Sugestões proativas ("Você monitora o Japaratinga — o preço baixou 12% essa semana!")

### Inteligência
- [ ] Alertar automaticamente no chat quando preço de hotel monitorado atingir target

## Backlog
- [ ] Configurar dominio + HTTPS (TLS)
- [ ] Desligar workflow n8n
- [ ] Consulta de passagens aereas via bot Telegram (Apify Google Flights scraper)
- [ ] Hardening de producao (backup/monitoramento)
