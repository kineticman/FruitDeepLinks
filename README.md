# 🍎 FruitDeepLinks

**Universal Sports Aggregator for Channels DVR**

FruitDeepLinks leverages Apple TV's Sports aggregation APIs to build a unified sports EPG with deeplinks to 23+ streaming services. One guide to rule them all.

[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

---

## 🎯 The Problem

Sports streaming is fragmented:

- NFL on Prime Video (Thursday), ESPN+ (Monday), Peacock (Sunday)
- MLS exclusively on Apple TV
- College sports scattered across ESPN+, Paramount+, Peacock, etc.
- You have multiple subscriptions but need to check multiple apps just to find games

## ✨ The Solution

FruitDeepLinks creates virtual TV channels in Channels DVR with deeplinks that launch directly into your streaming apps.

**One EPG. All your sports. All your services.**

---

## 🆕 What's New

### Latest Features (February 2026)

**🛒 Amazon Channel Integration (Major Update)**
- Advanced scraping system identifies which Amazon Prime Video Channel is required for each event
- Discovers NBA League Pass, Peacock Premium, DAZN, FOX One, ViX Premium, Max, and 10+ other channels
- Tracks channel requirements in new `amazon_channels` database table
- `v_amazon_playables_with_channels` view provides comprehensive channel mapping
- Async/parallel scraping with smart 7-day caching for performance
- Detects "stale" events (404s) to maintain data accuracy
- Foundation for future user-selectable Amazon channel filtering

**🆕 Four New Streaming Services (Experimental)**
- **Victory+** - Regional college sports content
- **Fanatiz Soccer** - International soccer leagues
- **beIN Sports** - International soccer, rugby, motorsports  
- **Gotham Sports** - NYC regional sports (Knicks, Rangers, Islanders, Devils, Yankees, Nets)
- Note: These services are marked EXPERIMENTAL - deeplink formats still being discovered
- Event data scraped successfully, seeking community help to identify working deeplink patterns

**✨ Enhanced Title Formatting**
- ESPN-style league/sport prefixes for better event organization
- Consistent title formatting across all export types
- Improved metadata presentation in EPG

**🎯 Genre Normalization**
- Automatic cleanup of malformed categories
- Prevents bad genre data from affecting filters
- More reliable sports/league classification

**⚡ Performance Improvements**
- Hybrid scraping approach (Selenium + HTTP) for faster data collection
- Improved `--skip-scrape` flag handling for rapid refreshes
- Better provider detection in exports
- 5,000 line log buffer (up from 500) for better debugging

### Previous Features

**🔥 ESPN Watch Graph API Integration**
- Fixes ESPN deeplink compatibility with Fire TV and Android TV devices
- Automatically enriches ESPN events with Fire TV-compatible deeplinks
- Scrapes ESPN's Watch Graph API daily (2,000+ events, 14 days forward)
- 71.7% match rate for current ESPN events
- Works across all export types: ADB lanes, virtual lanes, and direct channels
- Falls back to Apple TV deeplinks for unmatched events

**🎬 CDVR Detector - Automatic App Launching**
- Tune to a Fruit Lane in Channels DVR → streaming app launches automatically!
- Detects which device is watching and launches the correct service (ESPN+, Peacock, etc.)
- Supported on Apple TV, Fire TV, and Android TV
- No manual app switching required

**📊 Standards-Compliant XMLTV Exports**
- Proper `<live/>` and `<new/>` tags for EPG consumers
- Structured category taxonomy (Provider, Sport, League, "Sports Event")
- Sport/league metadata from Apple's classification system
- Clean placeholders with no unnecessary tags
- Shared `xmltv_helpers.py` module for consistent tagging across all exporters

**🌏 Kayo Sports Integration**
- Full Australian sports streaming support (Cricket, AFL, NRL, etc.)
- Web-based deeplink generation
- Integrated into multi-provider selection

### Enhanced Multi-Provider Support

- **Enhanced Apple scrape** preserves richer service metadata and captures multiple playable/deeplink options per event
- **User-adjustable service priority** - control which provider wins when multiple options exist
- **Multi-service selection helpers** to identify events available on multiple services
- **Improved ADB compatibility** for Android/Fire workflows
- **Improved HTTP fallback generation** for cases where native schemes aren't usable
- **Metadata/labeling fixes** - categories/descriptions match the chosen provider accurately

---

## 🚀 Quick Start (Portainer – Recommended)

These steps assume you already have **Docker** and **Portainer** running on your server.

### 1. Add a Git-backed stack in Portainer

1. Open Portainer in your browser.
2. Go to **Stacks → Add stack**.
3. Choose the **Repository** method.
4. Fill in:
   - **Name:** `fruitdeeplinks`
   - **Repository URL:** `https://github.com/kineticman/FruitDeepLinks.git`
   - **Repository reference:** `main` (or whatever branch you want)
   - **Compose path:** `docker-compose.yml`

Portainer will clone the repo and use `docker-compose.yml` plus the included `Dockerfile` to build the container image locally.

### 2. Set environment variables in Portainer

Scroll down to the **Environment variables** section for the stack.

Most people only need to set these four values:

```env
# REQUIRED (typical setup for Channels DVR)
SERVER_URL=http://192.168.86.80:6655
FRUIT_HOST_PORT=6655
CHANNELS_DVR_IP=192.168.86.80
TZ=America/New_York
```

If you want extra automation / features, you can add these as well:

```env
# OPTIONAL (only if you want extra automation/features)

# Auto Channels DVR guide refresh
CHANNELS_SOURCE_NAME=fruitdeeplinks-direct  # must match your Channels "Custom Channels" source name

# Chrome Capture / Channels4Chrome (BETA) – only if you use CC4C/AH4C or CH4C
CC_SERVER=192.168.86.80
CC_PORT=8020  # Chrome Capture port
CH4C_SERVER=192.168.86.80
CH4C_PORT=8020  # Channels4Chrome port (can be same or different)

# Lanes (BETA) – only needed if you experiment with lane channels
FRUIT_LANES=50
FRUIT_LANE_START_CH=9000

# Direct lanes channel numbering – separates direct lanes from virtual lanes in your guide
FRUIT_DIRECT_START_CH=5000
```

Notes:

- If you **omit** an env var, Docker uses the default from `docker-compose.yml` (the part after `:-`).
- Most users only need to set the four **REQUIRED** values.
- `CHANNELS_DVR_IP` should be the IP/hostname of your Channels DVR server.
- `CHANNELS_SOURCE_NAME` is only needed if you want FruitDeepLinks to auto-refresh a specific Channels “Custom Channels” source.
- `CC_SERVER` / `CC_PORT` and `CH4C_SERVER` / `CH4C_PORT` are only needed if you're using **Chrome Capture (CC4C/AH4C)** or **Channels4Chrome (CH4C)**. Both can share the same host/port if desired.
- `SERVER_URL` is the base URL embedded in generated links (it should be reachable by your playback devices).
- `FRUIT_HOST_PORT` is the host port Docker exposes; it should match the port in `SERVER_URL`.
- Scheduling/refresh runs via **APScheduler** inside the container (no cron).
- Lanes (`FRUIT_LANES`, `FRUIT_LANE_START_CH`, etc.) are only used if you experiment with the **BETA** lane features.
- `FRUIT_DIRECT_START_CH` controls the starting channel number for direct lanes (default: 5000), keeping them separate from virtual lanes in your channel guide.

### 3. Deploy the stack

1. Click **Deploy the stack**.
2. Wait for Portainer to pull the repo, build the image, and start the container.
3. Open the dashboard in your browser:

```text
http://<LAN-IP>:<FRUIT_HOST_PORT>
# example: http://192.168.86.80:6655
```

You should see the FruitDeepLinks web UI.

---

## ➕ Alternative: Docker Compose (without Portainer)

If you prefer bare Docker Compose on the host:

```powershell
git clone https://github.com/kineticman/FruitDeepLinks.git
cd FruitDeepLinks

Copy-Item .env.example .env
# Edit .env to match your LAN IP, timezone, Channels DVR IP, etc.

docker compose up -d

# Web UI: http://localhost:6655
```

Portainer and Docker Compose both use the same `docker-compose.yml`. The only difference is where you manage the environment variables.

---

## 📡 Add to Channels DVR

### Direct Channels (recommended & stable)

Direct channels expose **one channel per event** (great for browsing specific games). This is the most tested and stable path today.

1. In Channels DVR, go to **Settings → Sources → Add Source → Custom Channels**.
2. Create a new source named e.g. `fruitdeeplinks-direct` (if you enable auto-refresh, set `CHANNELS_SOURCE_NAME` to this exact name):
   - **M3U URL:** `http://your-server-ip:6655/direct.m3u`
   - **XMLTV URL:** `http://your-server-ip:6655/direct.xml`
3. In that **direct** source’s settings, set **Stream Format** to **`STRMLINK`**.  
   This is required so Channels passes the deeplink URL through to your device.
4. Refresh guide data.

To enable automatic guide refresh, make sure the **Custom Channels source name in Channels DVR** exactly matches `CHANNELS_SOURCE_NAME` (default: `fruitdeeplinks-direct`).

### Lanes & ADB Provider Lanes (BETA)

> Lanes and ADB provider lanes are **BETA / upcoming features**. API and behavior may still change.

**Lane Channels (multisource_lanes – BETA)**

1. (Optional) Create another Custom Channels source named e.g. `fruitdeeplinks-lanes`.
2. Use:
   - **M3U URL (lanes, BETA):** `http://your-server-ip:6655/multisource_lanes.m3u`
   - **XMLTV URL (lanes, BETA):** `http://your-server-ip:6655/multisource_lanes.xml`

**ADB Provider Lanes (BETA / advanced)**

- Exported lanes per provider for ADBTuner / ChromeCapture workflows.
- Typically not added directly to Channels; designed for automated capture pipelines.
- Outputs live under `/app/out` inside the container and are exposed via the web UI.

If you’re unsure, **start with direct channels only** and ignore lanes/ADB until you’re comfortable.

---

## 📺 Supported Services

### Premium Sports (23+ Services)

| Service       | Deeplink Type                 | Notes / Status                                      |
|--------------|-------------------------------|-----------------------------------------------------|
| ESPN+        | Native (`sportscenter://`) + API enrichment | ESPN Watch Graph API provides Fire TV-compatible deeplinks for 71.7% of events |
| Prime Video  | Native (`aiv://`)             | Amazon sports + 10+ channels (NBA League Pass, Peacock, DAZN, FOX One, etc.) - Advanced channel detection system identifies requirements |
| Peacock      | Native + Web                  | NBC Sports & Peacock events                         |
| Paramount+ (CBS Sports) | Native (`pplus://`)           | CBS Sports / Paramount+ competitions (preferred label) |
| CBS Sports app | Native (`cbssportsapp://`)    | Direct CBS Sports app deeplinks (less common)          |
| NBC Sports   | Native (`nbcsportstve://`)    | Regional & national coverage                        |
| FOX Sports   | Native (`foxone://`)          | FS1/FS2 and Fox Sports content                      |
| Max          | Web                           | Sports via Max (formerly HBO Max)                   |
| Apple MLS    | Web                           | Apple TV MLS Season Pass                            |
| Apple MLB    | Web                           | Apple TV MLB Friday Night Baseball                  |
| Apple NBA    | Web                           | Apple TV NBA games                                  |
| Apple NHL    | Web                           | Apple TV NHL games                                  |
| DAZN         | Native (`dazn://`)            | DAZN sports                                         |
| Kayo Sports  | Web (`kayo_web`)              | Australian sports (Cricket, AFL, NRL, Rugby, etc.) - Full integration |
| F1 TV        | Web                           | F1 TV Pro content                                   |
| ViX          | Native (`vixapp://`)          | Spanish-language sports                             |
| NFL+         | Native (`nflctv://`)          | NFL+ games & replays                                |
| TNT/truTV    | Native (`watchtbs://`)        | Turner Sports via Watch TBS app                     |
| **Victory+** | **EXPERIMENTAL**              | **Regional college sports - deeplinks being discovered** |
| **Fanatiz**  | **EXPERIMENTAL**              | **International soccer - deeplinks being discovered** |
| **beIN Sports** | **EXPERIMENTAL**           | **International soccer/rugby/motorsports - deeplinks being discovered** |
| **Gotham Sports** | **EXPERIMENTAL**         | **NYC regional sports (Knicks, Rangers, etc.) - deeplinks being discovered** |

**Note:** Services marked EXPERIMENTAL have full event scraping but deeplink formats are still being identified. Community help welcome!

Actual event counts vary by season and scrape window.

### Platform Compatibility

| Platform   | Deeplink Support | Notes                                                                 |
|-----------|------------------|-----------------------------------------------------------------------|
| Fire TV   | ✅ Excellent     | Most native deeplinks work. Amazon sports deeplinks still in flux.   |
| Apple TV  | ✅ Excellent     | Native platform for many providers                                   |
| Android TV| ✅ Good          | Most deeplinks supported                                             |
| Roku      | ⚠️ Limited      | Web fallback only for some providers                                 |

---

## 🎛️ Features

### Smart Filtering System

Configure what you see in the web dashboard:

- **Service Filtering** – Enable only your subscriptions
- **Sport Filtering** – Hide sports you don't watch
- **League Filtering** – Hide specific leagues/competitions
- **Automatic Deeplink Selection** – Uses your enabled services and provider priorities
- **Built-in scheduling (APScheduler)** – Runs refresh/automation internally (no cron)

### 🎬 Automatic App Launching (CDVR Detector)

When you tune to a Fruit Lane channel in Channels DVR, FruitDeepLinks automatically:

1. **Detects which device is watching** (Apple TV, Fire TV, Android TV)
2. **Looks up the current live event's deeplink**
3. **Launches the appropriate streaming app** (ESPN+, Peacock, etc.)

**No manual app switching required!** Just tune to the lane and the app launches.

**Setup:** Requires `CDVR_DVR_PATH` environment variable pointing to your Channels DVR recordings folder. See setup instructions below.

**Example:** Enable ESPN+ and Peacock → system shows only events available on those services and automatically selects the best deeplink.

### Channel Modes

> **Note:** Lanes and ADB provider lanes are **BETA** / upcoming features. Direct channels are the most stable path right now.

**1. Direct Channels** (`direct.m3u`)

- One channel per event.
- ~100–200 channels.
- Best for browsing specific games.
- Works great with **Stream Format = STRMLINK** in Channels DVR.

**2. Scheduled Lanes (BETA)** (`multisource_lanes.m3u`)

- 10–50 rotating channels.
- Events scheduled like traditional TV.
- Best for channel surfing.
- Still under active development; names and behavior may change.

**3. ADB Provider Lanes (BETA / advanced)**

- Per-provider lane sets exported as XMLTV + M3U.
- Designed for ADBTuner / ChromeCapture workflows.
- Uses `adb_lanes` and `provider_lanes` tables under the hood.
- Consider this experimental for now.

**4. Chrome Capture & Channels4Chrome Lanes (BETA)**

Two output formats for external launcher/capture workflows:
- **Chrome Capture** (`multisource_lanes_chrome.m3u`) - Uses `chrome://` schema for CC4C/AH4C
- **Channels4Chrome** (`multisource_lanes_ch4c.m3u`) - Uses `http://` schema for CH4C

Both playlists:
- Point to FruitDeepLinks' lane "launch" endpoint (`/api/lane/<n>/launch`)
- Receive a **302 redirect** to the best HTTP deeplink for the current event
- Share the same XMLTV guide (`multisource_lanes.xml`)
- **Beta warning:** Only as reliable as the HTTP fallback mapping. Some providers are scheme-only, geo/entitlement gated, or change web URLs frequently—expect occasional broken launches until mappings are refined.

Configuration:
```bash
# Chrome Capture
CC_SERVER=192.168.86.80
CC_PORT=8020

# Channels4Chrome  
CH4C_SERVER=192.168.86.80
CH4C_PORT=8020
```

### Web Dashboard

Access at `http://your-server-ip:6655`:

- Configure filters with visual toggles.
- Trigger manual refreshes.
- Apply filter changes instantly (~10 seconds).
- View system stats and logs.
- Download M3U/XMLTV files.

---

## 📋 Requirements

### Hardware

- Docker-capable system (Raspberry Pi 4+, NAS, PC, server).
- 2GB RAM minimum (4GB recommended).
- 1GB disk space.

### Software

- Docker.
- Portainer or Docker Compose.
- Channels DVR (for playback).
- Streaming subscriptions (your choice).

### Streaming Device

- Fire TV, Apple TV, or Android TV recommended.
- Roku supported (limited to web streams).

---

## ⚙️ Configuration (Summary)

These key env vars cover 90% of setups (whether in Portainer or `.env`):

```env
# REQUIRED (typical setup)
SERVER_URL=http://192.168.86.80:6655
FRUIT_HOST_PORT=6655
CHANNELS_DVR_IP=192.168.86.80
TZ=America/New_York

# Channels DVR integration (recommended)
CHANNELS_SOURCE_NAME=fruitdeeplinks-direct

# Auto-refresh (recommended)
AUTO_REFRESH_ENABLED=true
AUTO_REFRESH_TIME=02:30

# Chrome Capture (BETA) – only if you use CC4C/AH4C
CC_SERVER=192.168.86.80
CC_PORT=8020  # example: set to your Chrome Capture port

# Virtual lanes (BETA)
FRUIT_LANES=50
FRUIT_LANE_START_CH=9000

# Streaming service scraping (optional)
KAYO_DAYS=7  # Days to scrape for Kayo Sports (default: 7)

# Event-Level Deeplinks (tvOS reliable; Android testing in progress)
# Leave blank to disable. Set to your DVR's base path to enable:
CDVR_DVR_PATH=/mnt/storage/DVR
CDVR_SERVER_PORT=8089
CDVR_API_PORT=57000

```

All other values have sensible defaults and can be adjusted later via Portainer or `.env` if needed. Lane-related settings only matter if you turn on the **BETA lanes** feature.

---

## 🔧 Advanced Usage

### Manual Refresh

```bash
# Full refresh (scrape + import + exports)
docker exec fruitdeeplinks python3 /app/bin/daily_refresh.py
```

### Fast Re-Export After Changing Filters (No Scrape)

If you just tweaked filters in the web UI and want to quickly regenerate outputs:

```bash
# Direct channels (direct.m3u / direct.xml)
docker exec fruitdeeplinks python3 /app/bin/fruit_export_hybrid.py

# Lane channels (multisource_lanes.m3u / multisource_lanes.xml) - BETA
docker exec fruitdeeplinks python3 /app/bin/fruit_export_lanes.py

# ADB provider lanes (optional, for ADBTuner/ChromeCapture) - BETA
docker exec fruitdeeplinks python3 /app/bin/fruit_export_adb_lanes.py
```

### Chrome Capture (BETA)

FruitDeepLinks can generate a Chrome‑Capture friendly lanes playlist (`out/multisource_lanes_chrome.m3u`) for **CC4C/AH4C**-style launcher/capture workflows.

How it works:

1. The M3U entry calls Chrome Capture’s `/stream?url=...`
2. The `url` points at FruitDeepLinks: `http://<FRUIT_HOST>:<PORT>/api/lane/<n>/launch`
3. FruitDeepLinks responds with a **302 redirect** to the best **HTTP** deeplink it can derive for the lane’s current event

**Beta warning:** This is **heavily reliant on FruitDeepLinks being able to find/derive a reliable HTTP deeplink**. Some providers are scheme‑only, geo/entitlement gated, or change web URLs frequently — expect occasional broken launches until mappings are refined.

Enable/configure Chrome Capture with these env vars (only needed if you use it):

```env
CC_SERVER=192.168.86.80
CC_PORT=8020  # example: set to your Chrome Capture port
```

### Event-Level Deeplinks (tvOS reliable; Android testing in progress)

Channels DVR only supports deeplinks **per channel**, not **per program**.  
**Event-Level Deeplinks** is a workaround that launches the correct streaming app for the *event you clicked*.

**What you get:**
- A clean guide using a small set of **Fruit Lane** channels
- When you tune a lane, FruitDeepLinks updates a `.strmlnk` file for that lane so Channels launches the right app

**Requirements**
- Works best on **tvOS** today; **Android testing is in progress**
- FruitDeepLinks must have **read/write filesystem access** to your Channels DVR `DVR` folder (the one that contains `Imports/`)
- You must set `CDVR_DVR_PATH` in your `.env` to enable it (leave blank to disable)

**How it works (high level):**
1. You tune to “Fruit Lane 5” in Channels DVR
2. FruitDeepLinks figures out which device is watching (Channels Client API on port `57000`)
3. It looks up the current event’s deeplink (ESPN+, Peacock, etc.)
4. It writes/updates: `<DVR>/Imports/fruitdeeplinks/lane5.strmlnk`
5. Channels reprocesses that one file and launches the app

**Enabled via `.env`:**
```env
CDVR_DVR_PATH=/path/to/your/DVR   # must contain Imports/
CDVR_SERVER_PORT=8089
CDVR_API_PORT=57000
```


### Database Access

```bash
# SQLite shell
docker exec -it fruitdeeplinks sqlite3 /app/data/fruit_events.db

# Query events
SELECT title, start_utc FROM events WHERE genres_json LIKE '%NBA%';

# Query playables
SELECT e.title, p.provider FROM events e 
JOIN playables p ON e.id = p.event_id 
WHERE e.title LIKE '%Lakers%';
```

### Logs

```bash
# View container logs
docker logs fruitdeeplinks -f

# Log files inside container
docker exec fruitdeeplinks ls -la /app/logs/
```

---

## 🗂️ Project Structure

```text
FruitDeepLinks/
├── bin/                          # Python scripts
│   ├── daily_refresh.py          # Main orchestrator (scrape → enrich → import → export)
│   ├── apple_scraper_db.py       # Apple TV Sports scraper (primary scraper)
│   ├── multi_scraper.py          # Apple auth token bootstrap (fresh installs only)
│   ├── fruit_ingest_espn_graph.py # ESPN Watch Graph API scraper
│   ├── fruit_enrich_espn.py      # ESPN Graph ID enrichment
│   ├── fruit_import_appletv.py   # Import Apple TV data into SQLite
│   ├── amazon2.py                # Amazon Prime Video Channel scraper (async/parallel)
│   ├── kayo_scrape.py            # Kayo Sports scraper (Australia)
│   ├── ingest_kayo.py            # Kayo data importer
│   ├── fanatiz_scrape.py         # Fanatiz Soccer scraper [EXPERIMENTAL]
│   ├── ingest_fanatiz.py         # Fanatiz data importer
│   ├── bein_scrape.py            # beIN Sports scraper [EXPERIMENTAL]
│   ├── bein_import.py            # beIN Sports importer
│   ├── victory_scraper.py        # Victory+ scraper [EXPERIMENTAL]
│   ├── gotham_integration.py     # Gotham Sports integration [EXPERIMENTAL]
│   ├── fruit_build_lanes.py      # Build scheduled lanes (multisource_lanes) [BETA]
│   ├── fruit_export_hybrid.py    # Direct channels XMLTV + M3U
│   ├── fruit_export_lanes.py     # Lanes XMLTV + M3U [BETA]
│   ├── fruit_build_adb_lanes.py  # Build ADB provider lanes [BETA]
│   ├── fruit_export_adb_lanes.py # Export provider-specific XMLTV + M3U [BETA]
│   ├── fruitdeeplinks_server.py  # Web dashboard + API + CDVR Detector
│   ├── filter_integration.py     # Filtering logic
│   ├── logical_service_mapper.py # Logical service mapping
│   ├── adb_provider_mapper.py    # ADB provider mapping utilities
│   ├── provider_utils.py         # Provider helpers
│   ├── deeplink_converter.py     # Deeplink format conversion
│   ├── xmltv_helpers.py          # XMLTV generation utilities
│   ├── genre_utils.py            # Genre normalization utilities
│   ├── reset_databases.py        # Database reset utility
│   ├── migrate_*.py              # Database schema migrations
│   └── (legacy peacock scripts)  # Backward compatibility
├── data/                         # SQLite databases
│   ├── fruit_events.db           # Main event database (includes amazon_channels table)
│   ├── apple_events.db           # Apple TV scraper cache
│   ├── espn_graph.db             # ESPN Watch Graph data
│   ├── amazon_gti_cache.pkl      # Amazon scraper 7-day cache
│   └── apple_uts_auth.json       # Apple auth tokens
├── out/                          # Generated files
│   ├── direct.xml                # Direct XMLTV
│   ├── direct.m3u                # Direct M3U
│   ├── multisource_lanes.xml     # Lanes XMLTV [BETA]
│   ├── multisource_lanes.m3u     # Lanes M3U [BETA]
│   ├── multisource_lanes_chrome.m3u  # Chrome Capture M3U [BETA]
│   ├── adb_lanes.xml             # ADB lanes XMLTV [BETA]
│   ├── adb_lanes_*.m3u           # Provider-specific M3U files [BETA]
│   ├── kayo_raw.json             # Kayo scraper output
│   ├── fanatiz_raw.json          # Fanatiz scraper output
│   ├── bein_snapshot.json        # beIN Sports scraper output
│   └── amazon_scrape_*.csv       # Amazon scraper debug CSVs
├── templates/                    # Flask HTML templates
│   ├── events.html               # Main event listing
│   ├── filters.html              # Filter configuration
│   ├── admin_dashboard.html      # System dashboard
│   └── ...
├── docs/                         # Documentation
│   ├── SERVICE_CATALOG.md        # Supported services
│   ├── PORTAINER_GUIDE.md        # Portainer setup
│   ├── PRIORITY_SYSTEM_GUIDE.md  # Priority configuration
│   └── ...
├── logs/                         # Application logs
├── docker-compose.yml            # Docker configuration
├── Dockerfile                    # Container image
├── requirements.txt              # Python dependencies
├── config.json                   # Server configuration
└── README.md                     # This file
```

---

## 🛠️ How It Works

### Architecture

1. **Scraper** (Selenium + Chrome)
   - Navigates Apple TV Sports tab.
   - Extracts event metadata and deeplinks.
   - Handles multiple playable sources per event.

2. **ESPN Watch Graph Enrichment**
   - Scrapes ESPN's Watch Graph API for Fire TV-compatible deeplinks
   - Matches Apple TV events using program IDs
   - Enriches 70%+ of ESPN events with working Fire TV deeplinks
   - Runs automatically during daily refresh

3. **Database** (SQLite)
   - Stores events, playables, and user preferences.
   - Tracks multiple deeplinks per event.
   - Maintains ESPN Graph IDs for enriched events.
   - Maintains logical service mappings.

4. **Filter Engine**
   - Applies user preferences (services, sports, leagues).
   - Selects best deeplink based on priority.
   - Prioritizes ESPN Graph IDs over Apple TV IDs for ESPN events.
   - Handles web URL mapping (Apple MLS, Max, etc.).

5. **Export Engine**
   - Generates standards-compliant XMLTV EPG files
   - Creates M3U playlists with deeplinks
   - Applies ESPN Graph ID corrections during export
   - Builds scheduled lane channels (BETA)
   - Builds provider-specific ADB lanes (BETA)
   - Uses shared `xmltv_helpers.py` for consistent tagging:
     - Proper `<live/>` and `<new/>` tags
     - Structured categories (Provider, Sport, League)
     - Sport/league from classification_json
     - Conditional tagging (placeholders excluded)

6. **Web Dashboard** (Flask)
   - Real-time configuration interface.
   - Manual refresh controls.
   - System monitoring.

### Data Flow

```text
Apple TV Sports API ──┐
                      ├──> Scraper (Selenium)
ESPN Watch Graph API ─┘
        ↓
   SQLite Database
   (with ESPN Graph ID enrichment)
        ↓
  Filter Engine (User Preferences)
  (prioritizes ESPN Graph IDs)
        ↓
   Export Scripts
   (applies ESPN corrections)
        ↓
  M3U + XMLTV Files
        ↓
   Channels DVR
        ↓
Your Streaming Apps (via Deeplinks)
```

---

## 🎯 Filtering Examples

### Example 1: Budget Sports Fan

**Enabled Services:**

- Prime Video (already have)
- Peacock Premium

**Result:** ~200 events filtered down to ~40 events.

### Example 2: Soccer Enthusiast

**Enabled Services:**

- Paramount+ (Champions League)
- ViX (Liga MX)
- Peacock (Premier League)

**Disabled Sports:**

- Basketball, Baseball, Hockey

**Result:** Only soccer events from your services.

### Example 3: Premium Everything

**Enabled Services:** All 23 (including 4 experimental).

**Disabled Leagues:**

- WNBA, Women's Soccer

**Result:** Full coverage minus specific leagues.

---

## 🐛 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs fruitdeeplinks

# Common issues:
# - Port 6655 already in use
# - Invalid env vars in stack
# - Insufficient memory
```

### No Events Showing

```bash
# Run manual refresh
docker exec fruitdeeplinks python3 /app/bin/daily_refresh.py

# Check database
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db "SELECT COUNT(*) FROM events"

# Verify filtering isn't too aggressive
# Visit http://your-server-ip:6655/filters
```

### Deeplinks Not Working

- Verify the streaming app is installed on your device.
- Check the app is authenticated (logged in).
- Test deeplink manually (Fire TV: `adb shell am start -a android.intent.action.VIEW -d "scheme://..."`).
- Some services require cable/TV provider authentication.

### Web Dashboard Not Loading

```bash
# Check server is running
docker exec fruitdeeplinks ps aux | grep fruitdeeplinks_server

# Check port mapping
docker port fruitdeeplinks
```

---

## 📊 Performance

From real deployment (example):

```text
Database: ~1,500 total events (varies by season)
After filtering: 100-200 events (depends on service selection)
Services available: 23 total (19 stable + 4 experimental)

Scrape time: ~10 minutes (with all services enabled)
Filter apply time: ~10 seconds
Memory usage: ~600MB
Database size: ~18MB
```

---

## 🗓️ Roadmap

### Recently Completed

- [x] **Amazon Channel Integration** - Advanced scraping system identifies which Prime Video Channel events require (NBA League Pass, Peacock, DAZN, FOX One, Max, ViX, and more)
- [x] **Four New Streaming Services (Experimental)** - Victory+, Fanatiz, beIN Sports, Gotham Sports integrations
- [x] **Enhanced Title Formatting** - ESPN-style league/sport prefixes across all exports
- [x] **Genre Normalization** - Automatic cleanup of malformed categories
- [x] **Performance Improvements** - Hybrid scraping (Selenium + HTTP), improved --skip-scrape handling, 5,000 line log buffer
- [x] **ESPN Watch Graph API Integration** - Fire TV-compatible deeplinks for ESPN events (71.7% match rate)
- [x] **Database Event Cleanup** - Automatic removal of old events to improve performance
- [x] **CDVR Detector** - Automatic app launching when tuning to Fruit Lanes
- [x] **XMLTV Standards Compliance** - Proper `<live/>` and `<new/>` tags with structured categories
- [x] **Kayo Sports Integration** - Australian sports streaming support
- [x] **Filter UI Bug Fixes** - JavaScript errors resolved
- [x] **Improved Logical Service Mapping** - Better web-based provider support
- [x] **Sport Name Normalization** - Consistent capitalization

### Coming Soon

- [ ] User-selectable Amazon Prime Video Channel filtering (NBA League Pass, Peacock via Prime, etc.)
- [ ] Complete deeplink discovery for experimental services (Victory+, Fanatiz, beIN, Gotham)
- [ ] Stabilize Chrome Capture (HTTP deeplink mapping + docs)
- [ ] Team-based filtering
- [ ] Time-of-day filters
- [ ] Multi-user profiles

### Future

- [ ] Additional streaming sources (Optus Sport, DAZN expansion, etc.)
- [ ] Mobile companion app
- [ ] Plex/Emby support
- [ ] "Red Zone" style auto-switching

See this section (and `ROADMAP.md`, if present) for more details as it evolves.

---

## 🤝 Contributing

This is an early **public beta**. Expect sharp edges and breaking changes.. Contributions are welcome from invited collaborators.

### Development Setup

```bash
# Clone repo
git clone https://github.com/kineticman/FruitDeepLinks.git
cd FruitDeepLinks

# Run locally (no Docker)
pip install -r requirements.txt
python bin/daily_refresh.py

# Or develop in container
docker compose up -d
docker exec -it fruitdeeplinks bash
```

---

## 📄 License

MIT License – see `LICENSE` file for details.

---

## 🙏 Acknowledgments

- Apple TV Sports APIs (reverse-engineered).
- Channels DVR community.
- All the streaming services for having deeplink support.

---

## ⚠️ Disclaimer

This project is for personal use only. Users must have legitimate subscriptions to streaming services. FruitDeepLinks does not provide, host, or distribute any copyrighted content – it only aggregates publicly available scheduling data and generates deeplinks to official streaming services.

Use of this software may violate Terms of Service of various platforms. Use at your own risk.

---

## 🔗 Links

- **Repository:** https://github.com/kineticman/FruitDeepLinks
- **Channels DVR:** https://getchannels.com
- **Service Catalog:** `docs/SERVICE_CATALOG.md`

---

**Made with ❤️ for sports fans tired of app-hopping**
