# Dockerfile for FruitDeepLinks
# Multi-source sports event aggregator (Apple TV + partners)

FROM python:3.11-slim

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
    gnupg \
    ca-certificates \
    fonts-liberation \
    fonts-dejavu \
    tzdata \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# --- Install Chrome for Selenium (keyring method) ---
RUN wget -qO- https://dl.google.com/linux/linux_signing_key.pub \
    | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
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
HEALTHCHECK --interval=1h --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:6655/health || exit 1

# Persistent volumes
VOLUME ["/app/data", "/app/out", "/app/logs"]

EXPOSE 6655
CMD ["/app/start.sh"]

