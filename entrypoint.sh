#!/bin/bash
set -e

env >> /etc/environment

if [ "$1" = "cron" ]; then
    echo "Starting cron daemon..."
    python cli.py healthcheck
    cron -f
elif [ "$1" = "api" ]; then
    echo "Starting API server..."
    exec python -m uvicorn api:app --host 0.0.0.0 --port 8000
elif [ -z "$1" ]; then
    echo "Usage:"
    echo "  docker run omnibees cron          # Run with cron scheduler"
    echo "  docker run omnibees prices        # One-off price collection"
    echo "  docker run omnibees api           # Run dashboard API"
    echo "  docker run omnibees status        # Check database status"
    echo "  docker run omnibees healthcheck   # Health check"
    echo "  docker run omnibees scrape        # Scrape chains/hotels"
else
    python cli.py "$@"
fi
