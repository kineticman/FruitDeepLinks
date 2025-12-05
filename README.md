# 🍎 FruitDeepLinks

**Universal Sports Aggregator for Channels DVR**

FruitDeepLinks leverages Apple TV's Sports aggregation API to create unified sports EPG with deeplinks to 18+ streaming services. One guide to rule them all.

[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

---

## 🎯 The Problem

Sports streaming is fragmented:
- NFL on Prime Video (Thursday), ESPN+ (Monday), Peacock (Sunday)
- MLS exclusively on Apple TV
- College sports scattered across ESPN+, Paramount+, Peacock
- You have 5 subscriptions but check 5 different apps to find games

## ✨ The Solution

FruitDeepLinks creates virtual TV channels in Channels DVR with deeplinks that launch directly into your streaming apps.

**One EPG. All your sports. All your services.**

---

## 🚀 Quick Start

```bash
# Clone repository
git clone https://github.com/kineticman/FruitDeepLinks.git
cd FruitDeepLinks

# Configure environment
cp .env.example .env
# Edit .env with your settings (timezone, server IP, etc.)

# Start with Docker Compose
docker-compose up -d

# Access web dashboard
open http://localhost:6655
```

### Add to Channels DVR

1. Go to Channels DVR Settings → Sources → Custom Channels
2. Add source:
   - **M3U URL:** `http://your-server-ip:6655/multisource_lanes.m3u`
   - **XMLTV URL:** `http://your-server-ip:6655/multisource_lanes.xml`
3. Refresh guide data

---

## 📺 Supported Services

### Premium Sports (18+ Services)

| Service | Deeplink Type | Count* | Priority |
|---------|---------------|--------|----------|
| ESPN+ | Native (`sportsonespn://`) | 623 | ⭐⭐⭐⭐⭐ |
| Prime Video | Native (`aiv://`) | 756 | ⭐⭐⭐⭐ |
| Peacock | Native + Web | 20 | ⭐⭐⭐⭐⭐ |
| Paramount+ | Native (`pplus://`) | 282 | ⭐⭐⭐⭐ |
| CBS Sports | Native (`cbssportsapp://`) | 291 | ⭐⭐⭐ |
| NBC Sports | Native (`nbcsportstve://`) | 4 | ⭐⭐⭐ |
| FOX Sports | Native (`foxone://`) | 12 | ⭐⭐⭐ |
| Max (HBO Max) | Web | 19 | ⭐⭐⭐ |
| Apple MLS | Web | 76** | ⭐⭐⭐ |
| Apple MLB | Web | 56** | ⭐⭐⭐ |
| DAZN | Native (`dazn://`) | 49 | ⭐⭐ |
| F1 TV | Web | 14 | ⭐⭐ |
| ViX | Native (`vixapp://`) | 74 | ⭐⭐ |
| NFL+ | Native (`nflctv://`) | 38 | ⭐⭐ |
| TNT/truTV | Native | 21 | ⭐⭐ |

\* Event counts from recent snapshot (varies by season)  
\** Off-season counts lower; peaks during active season

### Platform Compatibility

| Platform | Deeplink Support | Notes |
|----------|------------------|-------|
| Fire TV | ✅ Excellent | All native deeplinks work |
| Apple TV | ✅ Excellent | Native platform support |
| Android TV | ✅ Good | Most deeplinks supported |
| Roku | ⚠️ Limited | Web fallback only |

---

## 🎛️ Features

### Smart Filtering System

Configure what you see in the web dashboard:

- **Service Filtering** - Enable only your subscriptions
- **Sport Filtering** - Hide sports you don't watch
- **League Filtering** - Hide specific leagues/competitions
- **Automatic Deeplink Selection** - Uses YOUR enabled services

**Example:** Enable ESPN+ and Peacock → System shows only events available on those services and automatically selects best deeplink.

### Two Channel Modes

**1. Direct Channels** (`direct.m3u`)
- One channel per event
- ~100-200 channels
- Best for browsing specific games

**2. Scheduled Lanes** (`multisource_lanes.m3u`)
- 10-50 rotating channels
- Events scheduled like traditional TV
- Best for channel surfing

### Web Dashboard

Access at `http://your-server-ip:6655`:

- Configure filters with visual toggles
- Trigger manual refreshes
- Apply filter changes instantly (~10 seconds)
- View system stats and logs
- Download M3U/XMLTV files

---

## 📋 Requirements

### Hardware
- Docker-capable system (Raspberry Pi 4+, NAS, PC, server)
- 2GB RAM minimum (4GB recommended)
- 1GB disk space

### Software
- Docker + Docker Compose
- Channels DVR (for playback)
- Streaming subscriptions (your choice)

### Streaming Device
- Fire TV, Apple TV, or Android TV recommended
- Roku supported (limited to web streams)

---

## ⚙️ Configuration

### Environment Variables

Edit `.env` file:

```bash
# Timezone
TZ=America/New_York

# Virtual Channels
PEACOCK_LANES=50                    # Number of lane channels (10-50)
PEACOCK_LANE_START_CH=9000          # Starting channel number

# Server
SERVER_URL=http://192.168.1.100:6655  # Your server IP

# Channels DVR Integration (optional)
CHANNELS_DVR_IP=192.168.1.50        # Auto-refresh Channels DVR
CHANNELS_SOURCE_NAME=fruitdeeplinks  # M3U source name
```

See `.env.example` for all options.

---

## 🔧 Advanced Usage

### Manual Refresh

```bash
# Full refresh (scrape + import + export)
docker exec fruitdeeplinks python3 /app/bin/daily_refresh.py

# Apply filters only (fast - ~10 seconds)
docker exec fruitdeeplinks python3 /app/bin/peacock_export_hybrid.py
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
# View logs
docker logs fruitdeeplinks -f

# Log files
docker exec fruitdeeplinks ls -la /app/logs/
```

---

## 🗂️ Project Structure

```
FruitDeepLinks/
├── bin/                          # Python scripts
│   ├── daily_refresh.py          # Main orchestrator
│   ├── appletv_to_peacock.py     # Apple TV scraper
│   ├── peacock_export_hybrid.py  # Direct channel exports
│   ├── peacock_export_lanes.py   # Lane channel exports
│   ├── fruitdeeplinks_server.py  # Web dashboard
│   ├── filter_integration.py     # Filtering logic
│   ├── logical_service_mapper.py # Web service mapping
│   └── provider_utils.py         # Provider helpers
├── data/                         # SQLite database
│   └── fruit_events.db
├── out/                          # Generated files
│   ├── direct.xml                # Direct XMLTV
│   ├── direct.m3u                # Direct M3U
│   ├── multisource_lanes.xml     # Lanes XMLTV
│   └── multisource_lanes.m3u     # Lanes M3U
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
   - Navigates Apple TV Sports tab
   - Extracts event metadata and deeplinks
   - Handles multiple playable sources per event

2. **Database** (SQLite)
   - Stores events, playables, and user preferences
   - Tracks up to 7 deeplinks per event
   - Maintains logical service mappings

3. **Filter Engine**
   - Applies user preferences (services, sports, leagues)
   - Selects best deeplink based on priority
   - Handles web URL mapping (Apple MLS, Max, etc.)

4. **Export Engine**
   - Generates XMLTV EPG files
   - Creates M3U playlists with deeplinks
   - Builds scheduled lane channels

5. **Web Dashboard** (Flask)
   - Real-time configuration interface
   - Manual refresh controls
   - System monitoring

### Data Flow

```
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
- Peacock Premium ($5.99)

**Result:** ~200 events filtered down to ~40 events

### Example 2: Soccer Enthusiast

**Enabled Services:**
- Paramount+ (Champions League)
- ViX (Liga MX)
- Peacock (Premier League)

**Disabled Sports:**
- Basketball, Baseball, Hockey

**Result:** Only soccer events from your services

### Example 3: Premium Everything

**Enabled Services:** All 18

**Disabled Leagues:**
- WNBA, Women's Soccer

**Result:** Full coverage minus specific leagues

---

## 🐛 Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs fruitdeeplinks

# Common issues:
# - Port 6655 already in use
# - Invalid .env file
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

- Verify streaming app is installed on device
- Check app is authenticated (logged in)
- Test deeplink manually (Fire TV: adb shell am start -a android.intent.action.VIEW -d "scheme://...")
- Some services require cable/TV provider authentication

### Web Dashboard Not Loading

```bash
# Check server is running
docker exec fruitdeeplinks ps aux | grep fruitdeeplinks_server

# Check port mapping
docker port fruitdeeplinks
```

---

## 📊 Performance

From real deployment:

```
Database: 1,483 total events
After filtering: 133 events (91% reduction)
Services enabled: 12 out of 18

Scrape time: ~8 minutes
Filter apply time: ~10 seconds
Memory usage: ~600MB
Database size: ~15MB
```

---

## 🗓️ Roadmap

### Coming Soon
- [ ] Chrome Capture / AH4C integration
- [ ] Team-based filtering
- [ ] Time-of-day filters
- [ ] Multi-user profiles

### Future
- [ ] Additional content sources (ESPN+ API, Peacock direct)
- [ ] Mobile companion app
- [ ] Plex/Emby support
- [ ] "Red Zone" style auto-switching

See [ROADMAP.md](ROADMAP.md) for details.

---

## 🤝 Contributing

This is currently a private repository. Contributions welcome from invited collaborators.

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

MIT License - see [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Apple TV Sports API (reverse-engineered)
- Channels DVR community
- All the streaming services for having deeplink support

---

## 📞 Support

- **Issues:** Use GitHub Issues
- **Discussions:** Use GitHub Discussions
- **Documentation:** See `/docs` folder

---

## ⚠️ Disclaimer

This project is for personal use only. Users must have legitimate subscriptions to streaming services. FruitDeepLinks does not provide, host, or distribute any copyrighted content - it only aggregates publicly available scheduling data and generates deeplinks to official streaming services.

Use of this software may violate Terms of Service of various platforms. Use at your own risk.

---

## 🔗 Links

- **Repository:** https://github.com/kineticman/FruitDeepLinks
- **Channels DVR:** https://getchannels.com
- **Service Catalog:** [docs/SERVICE_CATALOG.md](docs/SERVICE_CATALOG.md)

---

**Made with ❤️ for sports fans tired of app-hopping**
