# Dockerfile for FruitDeepLinks
# Multi-source sports event aggregator (Apple TV + partners)
# Uses Debian Chromium for cross-platform compatibility (arm64 + amd64)

FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

# --- System deps (incl. ffmpeg for dummy HLS segment) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    cron \
    sqlite3 \
    wget \
    ca-certificates \
    fonts-liberation \
    fonts-dejavu \
    fonts-noto-color-emoji \
    tzdata \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# --- Install Chromium + ChromeDriver for Selenium (native arm64 + amd64 support) ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Python deps ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- App code ---
COPY bin ./bin
COPY templates ./templates
# COPY static ./static

# Ensure runtime dirs exist
RUN mkdir -p /app/data /app/out /app/logs

# Start script: run cron (if used) and the web server
RUN printf '%s\n' \
  '#!/usr/bin/env bash' \
  'set -e' \
  '' \
  '# Ensure runtime directories exist' \
  'mkdir -p /app/data /app/out /app/logs' \
  '' \
  '# Start cron in the background (if crontab is configured)' \
  'cron || true' \
  '' \
  '# Start FruitDeepLinks web server' \
  'cd /app' \
  'exec python3 -u /app/bin/fruitdeeplinks_server.py' \
  > /app/start.sh \
  && chmod +x /app/start.sh

# Health check - verify web server is responding
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:6655/health || exit 1

# Persistent volumes
VOLUME ["/app/data", "/app/out", "/app/logs"]

EXPOSE 6655
CMD ["/app/start.sh"]

