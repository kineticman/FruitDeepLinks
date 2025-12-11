# Dockerfile for FruitDeepLinks
# Multi-source sports event aggregator (Apple TV + partners)

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    cron \
    sqlite3 \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    fonts-dejavu \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome for Selenium (using modern keyring method)
RUN wget -qO- https://dl.google.com/linux/linux_signing_key.pub \
    | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY bin ./bin
COPY templates ./templates
# If you have a static/ directory, uncomment this:
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
  '# Start FruitDeepLinks web server (Flask)' \
  'exec python /app/bin/fruitdeeplinks_server.py' \
  > /app/start.sh \
  && chmod +x /app/start.sh

# Health check - verify web server is responding
HEALTHCHECK --interval=1h --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:6655/health || exit 1

# Persistent volumes
VOLUME ["/app/data", "/app/out", "/app/logs"]

# Expose web server port (inside container)
EXPOSE 6655

# Start both cron (via start.sh) and web server
CMD ["/app/start.sh"]

