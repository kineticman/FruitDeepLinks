# Dockerfile for FruitDeepLinks
# Multi-source sports event aggregator (Apple TV + Peacock)

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    cron \
    sqlite3 \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome for Selenium (using modern keyring method)
RUN wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright and Chromium (for any Playwright-based scripts)
RUN pip install --no-cache-dir playwright && \
    playwright install chromium && \
    playwright install-deps chromium

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY bin/ ./bin/

# Create necessary directories
RUN mkdir -p /app/data /app/out /app/logs /app/config

# Make scripts executable
RUN chmod +x /app/bin/*.py /app/bin/*.sh 2>/dev/null || true

# Setup cron job for 3am daily refresh
RUN echo "0 3 * * * cd /app && /usr/local/bin/python /app/bin/daily_refresh.py >> /app/logs/cron.log 2>&1" | crontab -

# Create startup script that runs both cron and web server
RUN echo '#!/bin/bash\n\
cron\n\
python3 -u /app/bin/fruitdeeplinks_server.py\n\
' > /app/start.sh && chmod +x /app/start.sh

# Health check - verify web server is responding
HEALTHCHECK --interval=1h --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:6655/health || exit 1

# Persistent volumes
VOLUME ["/app/data", "/app/out"]

# Expose web server port
EXPOSE 6655

# Start both cron and web server
CMD ["/app/start.sh"]
