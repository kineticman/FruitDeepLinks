# Synology NAS Docker Compose Helper (FruitDeepLinks)

This note captures a working Synology NAS setup where `docker-compose.yml` needed small tweaks (most importantly: **use the published image** and mount Synology paths correctly).

---

## docker-compose.yml (Synology-friendly)

```yaml
services:
  fruitdeeplinks:
    image: ghcr.io/kineticman/fruitdeeplinks:${TAG:-latest}
    container_name: fruitdeeplinks
    hostname: fruitdeeplinks

    # Helpful for Playwright/Chromium stability on NAS
    shm_size: "2gb"

    ports:
      - "${FRUIT_HOST_PORT:-6655}:6655"

    environment:
      - TZ=${TZ:-America/Chicago}

      # Must be reachable by your Channels DVR box (usually NAS IP + host port)
      - SERVER_URL=${SERVER_URL:-http://10.0.1.65:6655}

      # Channels DVR host/IP (recommended: no scheme)
      - CHANNELS_DVR_IP=${CHANNELS_DVR_IP:-10.0.1.65}
      - CHANNELS_SOURCE_NAME=${CHANNELS_SOURCE_NAME:-fruitdeeplinks}

      # Container paths (usually leave defaults)
      - FRUIT_DB_PATH=${FRUIT_DB_PATH:-/app/data/fruit_events.db}
      - OUT_DIR=${OUT_DIR:-/app/out}
      - LOG_DIR=${LOG_DIR:-/app/logs}
      - LOG_LEVEL=${LOG_LEVEL:-INFO}

      # Lane + schedule behavior
      - FRUIT_LANES=${FRUIT_LANES:-50}
      - FRUIT_LANE_START_CH=${FRUIT_LANE_START_CH:-9000}
      - FRUIT_DAYS_AHEAD=${FRUIT_DAYS_AHEAD:-7}
      - FRUIT_PADDING_MINUTES=${FRUIT_PADDING_MINUTES:-45}
      - FRUIT_PLACEHOLDER_BLOCK_MINUTES=${FRUIT_PLACEHOLDER_BLOCK_MINUTES:-60}
      - FRUIT_PLACEHOLDER_EXTRA_DAYS=${FRUIT_PLACEHOLDER_EXTRA_DAYS:-5}

      # Scraper/runtime toggles
      - HEADLESS=${HEADLESS:-true}
      - NO_NETWORK=${NO_NETWORK:-false}

      # In-container scheduled refresh
      - AUTO_REFRESH_ENABLED=${AUTO_REFRESH_ENABLED:-true}
      - AUTO_REFRESH_TIME=${AUTO_REFRESH_TIME:-02:30}

    volumes:
      # Synology host folders -> container folders
      - "${HOST_DIR:-/volume1/docker}/FruitDeepLinks/data:/app/data"
      - "${HOST_DIR:-/volume1/docker}/FruitDeepLinks/out:/app/out"
      - "${HOST_DIR:-/volume1/docker}/FruitDeepLinks/logs:/app/logs"

    restart: unless-stopped

    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
```

---

## .env example (Synology)

Create a `.env` alongside your compose file:

```ini
# Core
TAG=latest
TZ=US/Central

# Networking (use your NAS IP + the exposed port)
FRUIT_HOST_PORT=6655
SERVER_URL=http://10.0.1.65:6655

# Channels DVR (recommended: IP only, no http://)
CHANNELS_DVR_IP=10.0.1.65
CHANNELS_SOURCE_NAME=FruitDeepLinks

# Synology host base dir
HOST_DIR=/volume1/docker

# Lanes
FRUIT_LANES=100
FRUIT_LANE_START_CH=14001
```

---

## Common gotchas on Synology

- **Don’t forget the `image:` line.** If you copied a compose that used `build:`, it may fail or not match the published container layout.
- **Use real Synology paths.** Prefer `/volume1/docker/FruitDeepLinks/...` (or your volume name).
- **`CHANNELS_DVR_IP` should be an IP/host, not a URL.** If you include `http://`, some integrations may mis-handle it.
- If logs/out/data aren’t being written, check **folder permissions** on the NAS for the mounted directories.

Suggested repo path: `docs/synology-nas-compose.md`
