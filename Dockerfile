FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api.py config.py db.py cli.py telegram_alerts.py telegram_bot.py ./
COPY omnibees_price_scraper.py omnibees_complete_scraper.py omnibees_async_scraper.py migrate_to_postgres.py ./
COPY static ./static
COPY crontab /etc/cron.d/omnibees
RUN sed -i 's/\r$//' /etc/cron.d/omnibees
RUN chmod 0644 /etc/cron.d/omnibees && crontab /etc/cron.d/omnibees

COPY entrypoint.sh .
RUN sed -i 's/\r$//' entrypoint.sh
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
