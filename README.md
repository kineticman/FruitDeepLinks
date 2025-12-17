# 🍎 FruitDeepLinks

**Universal Sports Aggregator for Channels DVR**

FruitDeepLinks leverages Apple TV's Sports aggregation APIs to build a unified sports EPG with deeplinks to 19+ streaming services. One guide to rule them all.

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

Most people only need to set these three values:

```env
# REQUIRED (almost everyone should set these)
SERVER_URL=http://192.168.86.80:6655
FRUIT_HOST_PORT=6655
TZ=America/New_York
```

If you want extra automation / features, you can add these as well:

```env
# OPTIONAL (only if you want extra automation/features)
# Auto Channels DVR guide refresh
CHANNELS_DVR_IP=192.168.86.80
CHANNELS_SOURCE_NAME=fruitdeeplinks

# Lanes (BETA) – only needed if you experiment with lane channels
FRUIT_LANES=50
FRUIT_LANE_START_CH=9000
```

Notes:

- If you **omit** an env var, Docker uses the default from `docker-compose.yml` (the part after `:-`).
- Most users only need to set the three **REQUIRED** values.
- `CHANNELS_DVR_IP` / `CHANNELS_SOURCE_NAME` are only needed if you want the daily refresh script to auto-refresh your Channels XMLTV source.
- Lanes (`FRUIT_LANES`, `FRUIT_LANE_START_CH`, etc.) are only used if you experiment with the **BETA** lane features.

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

docker-compose up -d

# Web UI: http://localhost:6655
```

Portainer and Docker Compose both use the same `docker-compose.yml`. The only difference is where you manage the environment variables.

---

## 📡 Add to Channels DVR

### Direct Channels (recommended & stable)

Direct channels expose **one channel per event** (great for browsing specific games). This is the most tested and stable path today.

1. In Channels DVR, go to **Settings → Sources → Add Source → Custom Channels**.
2. Create a new source named e.g. `fruitdeeplinks-direct`:
   - **M3U URL:** `http://your-server-ip:6655/direct.m3u`
   - **XMLTV URL:** `http://your-server-ip:6655/direct.xml`
3. In that **direct** source’s settings, set **Stream Format** to **`STRMLINK`**.  
   This is required so Channels passes the deeplink URL through to your device.
4. Refresh guide data.

If you want the daily refresh script to auto-refresh this source, make sure the **XMLTV source name in Channels DVR** matches `CHANNELS_SOURCE_NAME` (default `fruitdeeplinks`).

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

### Premium Sports (19+ Services)

| Service       | Deeplink Type                 | Notes / Status                                      |
|--------------|-------------------------------|-----------------------------------------------------|
| ESPN+        | Native (`sportsonespn://`)    | Primary ESPN+ deep links                            |
| Prime Video  | Native (`aiv://`)             | Amazon sports deep links still being explored       |
| Peacock      | Native + Web                  | NBC Sports & Peacock events                         |
| Paramount+   | Native (`pplus://`)           | CBS Sports / Paramount+ competitions                |
| CBS Sports   | Native (`cbssportsapp://`)    | CBS Sports app deeplinks                            |
| NBC Sports   | Native (`nbcsportstve://`)    | Regional & national coverage                        |
| FOX Sports   | Native (`foxone://`)          | FS1/FS2 and Fox Sports content                      |
| Max          | Web                           | Sports via Max (formerly HBO Max)                   |
| Apple MLS    | Web                           | Apple TV MLS Season Pass                            |
| Apple MLB    | Web                           | Apple TV MLB Friday Night Baseball                  |
| Apple NBA    | Web                           | Apple TV NBA games                                  |
| Apple NHL    | Web                           | Apple TV NHL games                                  |
| DAZN         | Native (`dazn://`)            | DAZN sports                                         |
| Kayo Sports  | Web                           | Australian sports (Cricket, AFL, NRL, etc.)         |
| F1 TV        | Web                           | F1 TV Pro content                                   |
| ViX          | Native (`vixapp://`)          | Spanish-language sports                             |
| NFL+         | Native (`nflctv://`)          | NFL+ games & replays                                |
| TNT/truTV    | Native                        | Turner Sports coverage                              |

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

- **Service Filtering** – Enable only your subscriptions.
- **Sport Filtering** – Hide sports you don't watch.
- **League Filtering** – Hide specific leagues/competitions.
- **Automatic Deeplink Selection** – Uses *your* enabled services and provider priorities.

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
# Network & URLs
SERVER_URL=http://192.168.86.80:6655
FRUIT_HOST_PORT=6655
TZ=America/New_York

# Channels DVR integration (optional, but recommended)
CHANNELS_DVR_IP=192.168.86.80
CHANNELS_SOURCE_NAME=fruitdeeplinks

# Virtual lanes (BETA)
FRUIT_LANES=50
FRUIT_LANE_START_CH=9000

# Streaming service scraping (optional)
KAYO_DAYS=7  # Days to scrape for Kayo Sports (default: 7)

# CDVR Detector (BETA) - Auto-launch streaming apps
# Leave blank to disable. Set to your DVR's base path to enable:
CDVR_DVR_PATH=/mnt/storage/DVR
CDVR_SERVER_PORT=8089
CDVR_API_PORT=57000

# Auto-refresh
AUTO_REFRESH_ENABLED=true
AUTO_REFRESH_TIME=02:30
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

### CDVR Detector (BETA) - Automatic Deeplink Launching

The CDVR Detector automatically launches streaming apps when you tune to a "Fruit Lane" channel!

**How it works:**
1. You tune to "Fruit Lane 5" in Channels DVR
2. FruitDeepLinks detects which device is watching
3. Looks up the current event's deeplink (ESPN+, Peacock, etc.)
4. Launches the streaming app on your device automatically

**Setup:**

1. **Enable the detector** - Add to your `.env`:
   ```env
   CDVR_DVR_PATH=/path/to/your/dvr
   ```
   Examples:
   - Linux: `/mnt/storage/DVR`
   - macOS: `/Volumes/Storage/DVR`  
   - Synology: `/volume1/DVR`

2. **Restart container**:
   ```bash
   docker compose restart
   ```

3. **Add to Channels DVR**:
   - M3U: `http://your-ip:6655/out/multisource_lanes.m3u`
   - EPG: `http://your-ip:6655/out/multisource_lanes.xml`

4. **Tune to a Fruit Lane** - The streaming app launches automatically! 🎉

**Supported devices:**
- Apple TV (tvOS)
- Fire TV
- Android TV

**Notes:**
- Only works with devices that expose the Channels DVR Client API (port 57000)
- iOS/iPadOS devices do not support this feature
- The detector is disabled by default - you must set `CDVR_DVR_PATH` to enable it

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
│   ├── daily_refresh.py          # Main orchestrator (scrape → import → export)
│   ├── multi_scraper.py          # Apple TV Sports multi-service scraper
│   ├── fruit_import_appletv.py   # Import Apple TV JSON into SQLite
│   ├── fruit_build_lanes.py      # Build scheduled lanes (multisource_lanes) [BETA]
│   ├── fruit_export_hybrid.py    # Direct channels XMLTV + M3U
│   ├── fruit_export_lanes.py     # Lanes XMLTV + M3U [BETA]
│   ├── fruit_build_adb_lanes.py  # Build ADB provider lanes [BETA]
│   ├── fruit_export_adb_lanes.py # Export provider-specific XMLTV + M3U [BETA]
│   ├── fruitdeeplinks_server.py  # Web dashboard + API
│   ├── filter_integration.py     # Filtering logic
│   ├── logical_service_mapper.py # Logical service mapping
│   └── provider_utils.py         # Provider helpers
├── data/                         # SQLite database
│   └── fruit_events.db
├── out/                          # Generated files
│   ├── direct.xml                # Direct XMLTV
│   ├── direct.m3u                # Direct M3U
│   ├── multisource_lanes.xml     # Lanes XMLTV [BETA]
│   ├── multisource_lanes.m3u     # Lanes M3U [BETA]
│   └── ...                       # ADB provider lane outputs (XMLTV + M3U) [BETA]
├── logs/                         # Application logs
├── docker-compose.yml            # Docker configuration
├── Dockerfile                    # Container image
├── .env.example                  # Environment template
└── README.md                     # This file
```

---

## 🛠️ How It Works

### Architecture

1. **Scraper** (Selenium + Chrome)
   - Navigates Apple TV Sports tab.
   - Extracts event metadata and deeplinks.
   - Handles multiple playable sources per event.

2. **Database** (SQLite)
   - Stores events, playables, and user preferences.
   - Tracks multiple deeplinks per event.
   - Maintains logical service mappings.

3. **Filter Engine**
   - Applies user preferences (services, sports, leagues).
   - Selects best deeplink based on priority.
   - Handles web URL mapping (Apple MLS, Max, etc.).

4. **Export Engine**
   - Generates XMLTV EPG files.
   - Creates M3U playlists with deeplinks.
   - Builds scheduled lane channels (BETA).
   - Builds provider-specific ADB lanes (BETA).

5. **Web Dashboard** (Flask)
   - Real-time configuration interface.
   - Manual refresh controls.
   - System monitoring.

### Data Flow

```text
Apple TV Sports API
        ↓
    Scraper (Selenium)
        ↓
   SQLite Database
        ↓
  Filter Engine (User Preferences)
        ↓
   Export Scripts
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

**Enabled Services:** All 19.

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
Database: 1,483 total events
After filtering: 133 events (91% reduction)
Services enabled: 12 out of 19

Scrape time: ~8 minutes
Filter apply time: ~10 seconds
Memory usage: ~600MB
Database size: ~15MB
```

---

## 🗓️ Roadmap

### Recently Completed

- [x] Kayo Sports integration (Australian sports streaming)
- [x] Filter UI bug fixes (JavaScript errors resolved)
- [x] Improved logical service mapping for web-based providers
- [x] Sport name capitalization normalization

### Coming Soon

- [ ] Chrome Capture / AH4C integration.
- [ ] Team-based filtering.
- [ ] Time-of-day filters.
- [ ] Multi-user profiles.

### Future

- [ ] Additional streaming sources (Optus Sport, DAZN expansion, etc.)
- [ ] Mobile companion app
- [ ] Plex/Emby support
- [ ] "Red Zone" style auto-switching

See `ROADMAP.md` for more details as it evolves.

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
docker-compose up -d
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
