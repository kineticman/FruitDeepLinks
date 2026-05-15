# 🍎 FruitDeepLinks

**Universal Sports Streaming Aggregator — v2.0.0**

FruitDeepLinks scrapes Apple TV's Sports aggregation API plus 10 regional services to build a unified sports EPG with deeplinks to 24+ streaming apps. Export M3U/XMLTV for Channels DVR, ADBTuner, CC4C, and PrismCast.

[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

---

## 🎯 The Problem

Sports streaming is fragmented:

- NFL on Prime Video (Thursday), ESPN+ (Monday), Peacock (Sunday)
- MLS exclusively on Apple TV
- College sports scattered across ESPN+, Paramount+, Peacock, and more
- You have multiple subscriptions but need to check multiple apps just to find games

## ✨ The Solution

FruitDeepLinks creates virtual TV channels in Channels DVR with deeplinks that launch directly into your streaming apps.

**One EPG. All your sports. All your services.**

---

## 🆕 What's New in v2.0.0

### v2 Server Refactor

- **Flask app factory** — clean blueprint-based routing replaces the monolithic server
- **Settings page** (`/settings`) — configure server URL, DVR IP, lane counts, refresh schedule, and per-scraper toggles directly in the UI; no `.env` edits required
- **Service catalog** (`core/service_catalog.py`) — single source of truth for all display names, internal priorities, and user-facing defaults
- **DB access layer** (`db/`) — `get_conn()` context manager, `preferences.py` CRUD, `stats.py` for the dashboard
- **Per-scraper on/off toggles** — disable Kayo, Fanatiz, beIN, NESN, Victory+, Gotham, or ESPN individually from the Settings page; env vars still work as hard overrides
- **Structured progress tracking** — refresh pipeline emits structured JSON markers for real-time step tracking in the dashboard

### Previously Added

- **Amazon Channel Integration** — identifies which Prime Video channel (NBA League Pass, DAZN, FOX One, Max, ViX, etc.) each event requires; stored in `amazon_channels` table
- **ESPN Watch Graph API** — enriches ESPN events with Fire TV-compatible deeplinks (~70% match rate); falls back to Apple TV deeplinks for unmatched events
- **ADB Lanes with device profiles** — `/m3u/adb` for Fire TV/Android (scheme URLs), `/m3u/adb?profile=apple` for Apple TV (HTTPS URLs); per-provider variants available
- **Regional scrapers** — Kayo Sports, Fanatiz Soccer, beIN Sports, NESN, Victory+, Gotham Sports (MSG/YES)

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
   - **Repository reference:** `main`
   - **Compose path:** `docker-compose.yml`

### 2. Set environment variables

Most users only need these four:

```env
SERVER_URL=http://192.168.1.100:6655     # IP of this server, as seen by your devices
FRUIT_HOST_PORT=6655
CHANNELS_DVR_IP=192.168.1.100           # IP of your Channels DVR server
TZ=America/New_York
```

Additional optional variables:

```env
CHANNELS_SOURCE_NAME=fruitdeeplinks-direct   # must match your Channels Custom Channels source name

# Override scraper defaults (also configurable in the Settings page)
KAYO_ENABLED=false         # disable if you don't have Kayo
FANATIZ_ENABLED=false      # disable if you don't watch Fanatiz
BEIN_ENABLED=false
NESN_ENABLED=false
VICTORY_ENABLED=false
GOTHAM_ENABLED=false
ESPN_ENABLED=true          # ESPN Watch Graph enrichment
```

> **Tip:** After first launch, visit `/settings` to configure everything from the UI — server URL, DVR IP, refresh schedule, lane counts, and scraper toggles. Settings saved there persist across restarts and take precedence over env vars (except scraper env vars, which remain a hard override).

### 3. Deploy and open the dashboard

1. Click **Deploy the stack**.
2. Open in your browser:

```
http://<your-server-ip>:6655
```

Run an initial refresh from the dashboard to populate the database.

---

## ➕ Alternative: Docker Compose (without Portainer)

```bash
git clone https://github.com/kineticman/FruitDeepLinks.git
cd FruitDeepLinks

cp .env.example .env
# Edit .env with your LAN IP, timezone, Channels DVR IP

docker compose up -d
# Web UI: http://localhost:6655
```

---

## ⚙️ Settings Page

Visit `/settings` to configure the server without editing environment variables:

| Section | Settings |
|---|---|
| **Server** | Server URL, Channels DVR IP, Channels source name |
| **Lanes** | Number of virtual lanes |
| **Pipeline** | Days ahead, padding minutes |
| **Auto Refresh** | Enable/disable, daily refresh time |
| **Scrapers** | Per-scraper on/off toggles |
| **Advanced** | Lane/direct channel start numbers, headless mode, log level |

Changes take effect immediately and persist across container restarts.

---

## 📡 Add to Channels DVR

### Direct Channels (recommended)

One channel per event — best for browsing specific games.

1. In Channels DVR: **Settings → Sources → Add Source → Custom Channels**
2. Create a source (e.g. `fruitdeeplinks-direct`):
   - **M3U URL:** `http://your-server-ip:6655/m3u/direct`
   - **XMLTV URL:** `http://your-server-ip:6655/xmltv/direct`
3. Set **Stream Format** to **`STRMLINK`**
4. Refresh guide data

### Virtual Lane Channels

Scheduled multi-provider virtual channels — one event per time slot per lane.

- **M3U URL:** `http://your-server-ip:6655/m3u/lanes`
- **XMLTV URL:** `http://your-server-ip:6655/xmltv/lanes`

### ADB Provider Lanes

Per-provider lanes for ADBTuner; supports device profiles:

| Profile | URL | Deeplink format |
|---|---|---|
| Fire TV / Android (default) | `/m3u/adb` | `aiv://`, `sportscenter://` etc. |
| Apple TV / web | `/m3u/adb?profile=apple` | `https://` |

Per-provider: `/out/adb_lanes_aiv.m3u`, `/out/adb_lanes_aiv_apple.m3u`, etc.

---

## 📋 Supported Streaming Services

### Tier 1: Fully Integrated

| Service | Notable Content |
|---------|-----------------|
| **ESPN+ / ESPN Linear** | MLS, college sports, select UFC, Monday Night Football |
| **Peacock** | Premier League, NBC Sports, college sports, Sunday Night Football |
| **Paramount+** | Champions League, college football, NFL on CBS |
| **Max** | Turner sports (TNT, TBS, truTV) |
| **Apple TV+** | MLS Season Pass, Apple MLB Friday, select NBA/NHL/F1 |
| **Prime Video** | Thursday Night Football, select sports |
| **Amazon Channels** | NBA League Pass, DAZN, FOX One, Max, ViX Premium, Peacock, and more |
| **Kayo Sports** | Cricket, AFL, NRL, Supercars (Australia) |
| **DAZN** | Combat sports, select leagues (regional) |
| **ViX** | Liga MX, Copa América, international soccer |
| **F1 TV** | Formula 1 |
| **NFL+** | NFL games |
| **NBA / Gametime** | NBA League Pass |
| **NHL.TV** | Hockey |
| **MLB.TV** | Baseball |
| **FOX Sports** | NFL, college sports |
| **CBS Sports / CBS** | NFL, college sports |
| **NBC Sports** | Various |
| **NCAA March Madness** | College basketball tournament |
| **Marquee Sports Network** | Chicago Cubs |

### Tier 2: Experimental

| Service | Notable Content |
|---------|-----------------|
| **Kayo Sports** | Australian sports — Cricket, AFL, NRL |
| **Fanatiz Soccer** | Latin American soccer leagues |
| **beIN Sports** | International soccer, rugby, motorsports |
| **Gotham Sports (MSG/YES)** | Knicks, Rangers, Islanders, Devils, Yankees, Nets |
| **NESN** | Red Sox, Bruins |
| **Victory+** | WHL, LOVB, niche sports |

> Experimental services: event data scrapes successfully; deeplink patterns still being refined. Community feedback welcome.

---

## 🛠️ Architecture

### Component Overview

1. **Scrapers** — Apple TV Sports API (Selenium + HTTP hybrid), ESPN Watch Graph API, Kayo, Fanatiz, beIN, NESN, Victory+, Gotham, Amazon GTI mapping

2. **Pipeline** (`daily_refresh.py`) — 15-step orchestrator: scrape → migrate → import → enrich → build lanes → export; runs on schedule or manually via dashboard

3. **Filter Engine** — user-configurable service preferences, sport/league selection, multi-service priority resolution, Amazon channel expansion

4. **Export Engine** — generates M3U + XMLTV for direct channels, virtual lanes, and ADB lanes; applies device profiles (Fire TV scheme vs. HTTPS)

5. **Web Dashboard** (Flask v2) — blueprint-based routes; pages: Events, Filters, ADB Config, Settings, API Helper, Admin/Logs

### Data Flow

```
Apple TV Sports API ──┐
Kayo / Fanatiz / beIN ├──> Scrapers ──> SQLite (fruit_events.db)
NESN / Victory+ / etc ┘                       │
ESPN Watch Graph API ─────────────────> Enrich playables
Amazon GTI mapping ───────────────────> amazon_channels table
                                               │
                                    Filter Engine (user prefs)
                                               │
                                       Export Scripts
                                               │
                         ┌─────────────────────┼─────────────────────┐
                    direct.m3u/.xml    lanes.m3u/.xml    adb_lanes.m3u/.xml
                         │                     │                     │
                  Channels DVR          Channels DVR            ADBTuner
                         │
                  Your Streaming Apps (via Deeplinks)
```

---

## 🎯 Filtering Examples

### Budget Sports Fan

Enable only Prime Video + Peacock → ~200 events filtered to ~40.

### Soccer Enthusiast

Enable Paramount+ (Champions League), ViX (Liga MX), Peacock (Premier League). Disable Basketball, Baseball, Hockey → only soccer events from your services.

### Disable Scrapers You Don't Need

Turn off Kayo, Fanatiz, beIN from the Settings page → scrape time drops from ~13 min to ~5 min for US-only setups.

---

## 🐛 Troubleshooting

### Container won't start

```bash
docker logs fruitdeeplinks
# Common: port 6655 already in use, invalid env vars
```

### No events showing

```bash
# Trigger a manual refresh
curl -X POST http://localhost:6655/api/refresh

# Check event count
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db "SELECT COUNT(*) FROM events"

# Check filters aren't blocking everything — visit /filters
```

### Deeplinks not working

- Verify the streaming app is installed and logged in on your device.
- Fire TV test: `adb shell am start -a android.intent.action.VIEW -d "scheme://..."`
- Try the HTTP deeplink variant (apple profile) if scheme URLs don't work on your device.
- Check `/events` in the dashboard — the event detail view shows the best available deeplink and its source.

### Dashboard not loading

```bash
docker exec fruitdeeplinks ps aux | grep fruitdeeplinks_v2
docker port fruitdeeplinks
```

---

## 📊 Performance

```
Database: ~1,500–3,000 events (varies by season and enabled scrapers)
After filtering: 100–400 events (depends on service selection)

Scrape time (all scrapers): ~13 minutes
Scrape time (Apple + ESPN only): ~5 minutes
Filter/export only (--skip-scrape): ~30 seconds
Memory usage: ~600 MB
Database size: ~20 MB
```

---

## 🗓️ Roadmap

### Completed in v2.0.0

- [x] Flask v2 app factory with blueprint routing
- [x] Settings page — full UI config, no .env required
- [x] Service catalog — single source of truth for all service metadata
- [x] Per-scraper on/off toggles (UI + env var hard override)
- [x] ADB lanes with Apple/Fire TV device profiles
- [x] Amazon Channel integration (GTI → channel code mapping)
- [x] ESPN Watch Graph enrichment for Fire TV deeplinks
- [x] Regional scrapers: Kayo, Fanatiz, beIN, NESN, Victory+, Gotham
- [x] XMLTV standards compliance (`<live/>`, `<new/>`, structured categories)

### Coming Soon

- [ ] Stabilize deeplinks for experimental services (Fanatiz, beIN, Gotham, Victory+, NESN)
- [ ] User-selectable Amazon Prime Video channel filtering
- [ ] Team-based filtering
- [ ] Time-of-day event filters

---

## 🤝 Contributing

Contributions and feedback welcome. The most useful contributions right now:

- Verified deeplink patterns for experimental services (Fanatiz, beIN, Gotham, Victory+)
- Additional regional scrapers
- Bug reports with logs (`/api/logs` or `docker logs fruitdeeplinks`)

### Development Setup

```bash
git clone https://github.com/kineticman/FruitDeepLinks.git
cd FruitDeepLinks

# Run in Docker (recommended)
docker compose up -d

# Or run locally
pip install -r requirements.txt
cd bin
python fruitdeeplinks_v2.py
```

---

## 📄 License

MIT License — see `LICENSE` for details.

---

## 🙏 Acknowledgments

- Apple TV Sports APIs (reverse-engineered)
- Channels DVR community
- Contributor bnhf for the scraper auto-disable logic (PR #19)

---

## ⚠️ Disclaimer

This project is for personal use only. Users must have legitimate subscriptions to all streaming services accessed. FruitDeepLinks does not provide, host, or distribute any copyrighted content — it only aggregates publicly available scheduling data and generates deeplinks to official streaming services.

Use of this software may violate the Terms of Service of various platforms. Use at your own risk.

---

## 🔗 Links

- **Repository:** https://github.com/kineticman/FruitDeepLinks
- **Channels DVR:** https://getchannels.com

---

**Made with ❤️ for sports fans tired of app-hopping**
