# PeacockDeepLinks

Automated scraper that creates virtual TV channels from Peacock TV sports content with XMLTV EPG and M3U playlists. Perfect for integration with DVR applications like Channels DVR, Plex, or Emby.

## ✨ Features

- 🎮 **Lane-Based Mode** - 10-20 persistent channels with rotating sports content
- 🔗 **Direct Deeplink Mode** - One channel per event (within 24 hours)
- 🌐 **Chrome Capture Support** - M3U with `chrome://` URLs
- 📺 **Full XMLTV EPG** - Rich metadata, descriptions, categories, images
- 🔄 **Automatic Refresh** - Scheduled daily updates (configurable)
- 🌐 **Web Dashboard** - Monitor status, download files, trigger manual refresh
- 🐳 **Docker Support** - Easy deployment with Docker Compose
- 🎯 **API Endpoints** - Dynamic deeplink resolution for ADBTuner

## 🎯 Use Cases

### For ADBTuner Users
- Persistent channel numbers (9000-9020)
- Dynamic content rotation via API
- Full EPG guide with schedules
- Uses: `peacock_lanes.xml` + `peacock_lanes.m3u`

### For Chrome Capture
- Direct deeplink URLs wrapped in `chrome://`
- Same EPG as ADBTuner
- Uses: `peacock_lanes.xml` + `peacock_lanes_chrome.m3u`

### For Simple Players
- One channel per event
- No API dependency
- Self-contained deeplinks
- Uses: `peacock_direct.xml` + `peacock_direct.m3u`

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Peacock TV subscription (for playback)

### Installation

#### Option 1: Docker Compose (Recommended)

1. **Clone the repository**
```bash
git clone https://github.com/kineticman/PeacockDeepLinks.git
cd peacockdeeplinks
```

2. **Configure environment**
```bash
cp .env.example .env
nano .env
```

Edit `.env` and set your server IP:
```bash
PEACOCK_SERVER_HOST=192.168.86.72  # Your Docker host IP
PEACOCK_PORT=6655
```

3. **Start the container**
```bash
docker-compose up -d
```

4. **Access the dashboard**
```
http://localhost:6655
```

#### Option 2: Portainer

##### Method A: Using Portainer Stacks

1. **Open Portainer** and navigate to **Stacks** → **Add Stack**

2. **Name your stack:** `peacockdeeplinks`

3. **Paste the docker-compose.yml:**
```yaml
version: '3.8'

services:
  peacock:
    image: ghcr.io/kineticman/peacockdeeplinks:latest
    build:
      context: https://github.com/kineticman/PeacockDeepLinks.git
    container_name: peacockdeeplinks
    restart: unless-stopped
    ports:
      - "6655:6655"
    volumes:
      - peacock-data:/data
    environment:
      - PEACOCK_PORT=6655
      - PEACOCK_SERVER_HOST=192.168.86.72    # Change to your Docker host IP
      - PEACOCK_DB_PATH=/data/peacock_events.db
      - PEACOCK_LANES_XML_PATH=/data/peacock_lanes.xml
      - PEACOCK_LANES_M3U_PATH=/data/peacock_lanes.m3u
      - PEACOCK_CHROME_M3U_PATH=/data/peacock_lanes_chrome.m3u
      - PEACOCK_DIRECT_XML_PATH=/data/peacock_direct.xml
      - PEACOCK_DIRECT_M3U_PATH=/data/peacock_direct.m3u
      - PEACOCK_LANES=10
      - PEACOCK_DAYS_AHEAD=7
      - PEACOCK_PADDING_MINUTES=45
      - PEACOCK_LANE_START_CH=9000
      - PEACOCK_SLUG=/sports/live-and-upcoming
      - PEACOCK_REFRESH_CRON=15 3 * * *
      - TZ=UTC
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:6655/api/status')"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

volumes:
  peacock-data:
```

4. **Modify environment variables:**
   - Change `PEACOCK_SERVER_HOST` to your Docker host IP
   - Adjust other settings as needed

5. **Click "Deploy the stack"**

6. **Access dashboard:** `http://YOUR_SERVER_IP:6655`

##### Method B: Using Portainer Container Creation

1. **Navigate to Containers** → **Add Container**

2. **Basic Settings:**
   - **Name:** `peacockdeeplinks`
   - **Image:** Build from GitHub (see below) or use pre-built image

3. **To build from source in Portainer:**
   
   a. First, go to **Images** → **Build a new image**
   
   b. **Build method:** Use Git repository
      - **Repository URL:** `https://github.com/kineticman/PeacockDeepLinks`
      - **Image name:** `peacockdeeplinks:latest`
   
   c. Click **Build the image**
   
   d. Wait for build to complete

4. **Container Configuration:**
   
   **Network ports:**
   ```
   Host: 6655 → Container: 6655/tcp
   ```
   
   **Volumes:**
   ```
   /your/host/path/peacock-data → /data
   ```
   Or create a named volume: `peacock-data` → `/data`
   
   **Environment variables:** (Click "Add environment variable" for each)
   ```
   PEACOCK_PORT = 6655
   PEACOCK_SERVER_HOST = 192.168.86.72
   PEACOCK_DB_PATH = /data/peacock_events.db
   PEACOCK_LANES_XML_PATH = /data/peacock_lanes.xml
   PEACOCK_LANES_M3U_PATH = /data/peacock_lanes.m3u
   PEACOCK_CHROME_M3U_PATH = /data/peacock_lanes_chrome.m3u
   PEACOCK_DIRECT_XML_PATH = /data/peacock_direct.xml
   PEACOCK_DIRECT_M3U_PATH = /data/peacock_direct.m3u
   PEACOCK_LANES = 10
   PEACOCK_DAYS_AHEAD = 7
   PEACOCK_PADDING_MINUTES = 45
   PEACOCK_LANE_START_CH = 9000
   PEACOCK_SLUG = /sports/live-and-upcoming
   PEACOCK_REFRESH_CRON = 15 3 * * *
   TZ = UTC
   ```
   
   **Restart policy:** `Unless stopped`

5. **Deploy the container**

6. **Verify deployment:**
   - Check **Logs** in Portainer for "Starting web server on port 6655"
   - Access dashboard: `http://YOUR_SERVER_IP:6655`

##### Quick Portainer Setup Checklist

- [ ] Update `PEACOCK_SERVER_HOST` to your actual IP
- [ ] Ensure port 6655 is available
- [ ] Create or map `/data` volume for persistence
- [ ] Set restart policy to "unless-stopped"
- [ ] Check container logs after deployment
- [ ] Test dashboard access
- [ ] Verify files generated after first refresh

## 📋 Configuration

### Environment Variables

```bash
# Server Configuration
PEACOCK_SERVER_HOST=192.168.86.72    # Your server IP (required for remote access)
PEACOCK_PORT=6655                     # Web server port

# Scraper Settings
PEACOCK_LANES=10                      # Number of virtual channels (10-20 recommended)
PEACOCK_DAYS_AHEAD=7                  # Days to scrape ahead
PEACOCK_PADDING_MINUTES=45            # Event padding for overtime

# Channel Numbers
PEACOCK_LANE_START_CH=9000            # Starting channel number

# Optional: Channels-4-Chrome (CH4C) Bridge
CH4C_HOST=127.0.0.1                   # CH4C server IP (default: 127.0.0.1)
CH4C_PORT=2442                        # CH4C server port (default: 2442)

# Refresh Schedule (cron format)
PEACOCK_REFRESH_CRON=15 3 * * *       # Daily at 3:15 AM UTC
```

### Cron Schedule Examples

```bash
15 3 * * *      # 3:15 AM daily (default)
0 */4 * * *     # Every 4 hours
30 1,13 * * *   # 1:30 AM and 1:30 PM daily
0 6 * * 0       # 6:00 AM every Sunday
```

## 📡 Integration

### Channels DVR

#### ADBTuner Mode (Dynamic)
```
M3U Source: http://192.168.86.72:6655/lanes/m3u
XMLTV Source: http://192.168.86.72:6655/lanes/xmltv
```

#### Chrome Capture Mode
```
M3U Source: http://192.168.86.72:6655/chrome/m3u
XMLTV Source: http://192.168.86.72:6655/lanes/xmltv
```

#### Direct Deeplink Mode
```
M3U Source: http://192.168.86.72:6655/direct/m3u
XMLTV Source: http://192.168.86.72:6655/direct/xmltv
```

### Plex / Emby / Jellyfin
Use Direct Deeplink Mode for best compatibility:
```
M3U: http://192.168.86.72:6655/direct/m3u
EPG: http://192.168.86.72:6655/direct/xmltv
```

## 🌐 API Endpoints

### File Downloads
```
GET /lanes/xmltv          # Lane-based XMLTV
GET /lanes/m3u            # Lane-based M3U (ADBTuner)
GET /chrome/m3u           # Chrome Capture M3U
GET /direct/xmltv         # Direct mode XMLTV
GET /direct/m3u           # Direct mode M3U
```

### Dynamic Deeplinks (ADBTuner)
```
GET /api/lane/{id}/deeplink              # Redirect to current event
GET /api/lane/{id}/deeplink?format=json  # JSON response
GET /api/lane/{id}/deeplink?format=text  # Plain text URL
```

**Example:**
```bash
# Get current deeplink for lane 1
curl http://192.168.86.72:6655/api/lane/1/deeplink?format=text
```

**Response:**
```
https://www.peacocktv.com/deeplink?deeplinkData=%7B%22pvid%22%3A%22...%22%7D
```

### Status & Control
```
GET  /api/status          # System status (JSON)
POST /api/refresh         # Trigger manual refresh
```

## 🎨 Web Dashboard

Access at `http://localhost:6655`

**Features:**
- 📊 System status and statistics
- 📥 Download links for all formats
- 🔄 Manual refresh button
- ⚙️ Configuration display
- 📅 Next scheduled refresh time

## 📊 Output Formats

### Lane-Based (ADBTuner)
**Files:** `peacock_lanes.xml` + `peacock_lanes.m3u`
- 10-20 persistent channels
- Dynamic content via API
- Full EPG with rotating events
- Placeholders between events

**M3U Format:**
```m3u
#EXTINF:-1 tvg-id="peacock.lane.1" tvg-chno="9000",Peacock Sports 1
http://192.168.86.72:6655/api/lane/1/deeplink
```

### Chrome Capture
**Files:** `peacock_lanes.xml` + `peacock_lanes_chrome.m3u`
- Same channels as ADBTuner
- Direct deeplinks wrapped in `chrome://`
- Static URLs (updated on refresh)

**M3U Format:**
```m3u
#EXTINF:-1 tvg-id="peacock.lane.1" tvg-chno="9000",Peacock Sports 1
chrome://https://www.peacocktv.com/deeplink?deeplinkData=...
```

### Direct Deeplinks
**Files:** `peacock_direct.xml` + `peacock_direct.m3u`
- One channel per event
- Only events within 24 hours
- Self-contained deeplinks
- Placeholders before/after events

**M3U Format:**
```m3u
#EXTINF:-1 tvg-id="peacock.event.1",Bengals vs. Ravens
https://www.peacocktv.com/deeplink?deeplinkData=...
```

## 🔄 How It Works

### Lane-Based Mode
1. Scrapes Peacock API for upcoming sports events
2. Creates 10-20 virtual "lanes" (channels)
3. Distributes events across lanes
4. Fills gaps with placeholder content
5. API dynamically resolves current event per lane

### Direct Mode
1. Scrapes events starting within 24 hours
2. Creates one channel per event
3. Adds placeholders:
   - "Event Not Started" (starts at :00 or :30, up to 8h before)
   - "Event Ended" (24h after event ends)
4. Regenerates daily with fresh events

### Refresh Cycle
1. **Ingest**: Fetch events from Peacock API
2. **Build**: Create lane schedules
3. **Export**: Generate XMLTV + M3U files
4. **Serve**: Files available via web server

## 🛠️ Manual Commands

```bash
# View logs
docker-compose logs -f

# Restart container
docker-compose restart

# Stop container
docker-compose down

# Rebuild after changes
docker-compose down
docker-compose build
docker-compose up -d

# Trigger manual refresh
curl -X POST http://localhost:6655/api/refresh

# Check status
curl http://localhost:6655/api/status | jq

# View generated M3U
curl http://localhost:6655/lanes/m3u
```

## 📁 File Structure

```
PeacockDeepLinks/
├── bin/
│   ├── peacock_server.py          # Flask web server + scheduler
│   ├── peacock_ingest_atom.py     # Peacock API scraper
│   ├── peacock_build_lanes.py     # Lane planner
│   ├── peacock_export_from_db.py  # XMLTV/M3U exporter (DB → XML/M3U)
│   ├── peacock_export_hybrid.py   # Experimental hybrid exporter
│   └── peacock_refresh_all.py     # Orchestrates ingest → lanes → export
├── data/                          # Persistent data (volume)
│   ├── peacock_events.db          # SQLite database
│   ├── peacock_lanes.xml          # Lanes XMLTV
│   ├── peacock_lanes.m3u          # Lanes M3U
│   ├── peacock_lanes_chrome.m3u   # Chrome Capture / CH4C M3U
│   ├── peacock_direct.xml         # Direct (one-channel-per-event) XMLTV
│   └── peacock_direct.m3u         # Direct (one-channel-per-event) M3U
├── out/                           # Optional extra output directory (currently unused)
├── docs/                          # User-facing docs
│   ├── PORTAINER_GUIDE.md         # Portainer stack setup
│   ├── CHROME_CAPTURE_M3U.md      # Chrome Capture config notes
│   └── CH4C_INTEGRATION.md        # CH4C integration guide
├── docker-compose.yml             # Example Docker Compose config
├── Dockerfile                     # Docker image build
├── README.md                      # This file
└── requirements.txt               # Python dependencies
```


## 🔧 Troubleshooting

### M3U shows localhost instead of IP
**Solution:** Set `PEACOCK_SERVER_HOST` in `.env` and rebuild:
```bash
PEACOCK_SERVER_HOST=192.168.86.72
docker-compose down
docker-compose up -d
```

### No events showing
**Problem:** Peacock may not have sports scheduled
**Solution:** Check the API source or wait for scheduled events

### Database errors
**Solution:** Delete database and refresh:
```bash
rm data/peacock_events.db
curl -X POST http://localhost:6655/api/refresh
```

### Container won't start
**Check logs:**
```bash
docker-compose logs
```

### Port already in use
**Change port in `.env`:**
```bash
PEACOCK_PORT=6656
```

## 🎯 Advanced Usage

### Multiple Refresh Times
```bash
# Every 4 hours
PEACOCK_REFRESH_CRON=0 */4 * * *

# Morning and evening
PEACOCK_REFRESH_CRON=0 6,18 * * *
```

### More Lanes
```bash
# 20 channels for better distribution
PEACOCK_LANES=20
```

### Custom Channel Numbers
```bash
# Start at 5000 instead of 9000
PEACOCK_LANE_START_CH=5000
```

### Longer Event Padding
```bash
# 60 minutes for sports that often run over
PEACOCK_PADDING_MINUTES=60
```

## 📝 Notes

- **Peacock Subscription Required**: You need an active Peacock subscription to play content
- **Deeplinks Require Auth**: First-time deeplink may prompt for Peacock login
- **Event Times**: All times in UTC, converted to EST in placeholders
- **Database**: SQLite database stores events and schedules
- **Images**: Event images pulled from Peacock API
- **Refresh Window**: Direct mode only shows events within 24 hours

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is for personal use only. Peacock TV content and trademarks are property of NBCUniversal Media, LLC.

## ⚠️ Disclaimer

This tool is for personal use with your own Peacock TV subscription. It does not provide access to Peacock content - you must have an active subscription. The scraper only generates EPG data and deeplinks to legitimate Peacock content.

## 🙏 Acknowledgments

- Built for integration with [Channels DVR](https://getchannels.com/)
- Inspired by the need for better sports content organization
- Thanks to the Channels DVR community for testing and feedback

## 📬 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check existing issues for solutions
- Review documentation in `/docs` folder

---

Made with ❤️ for cord-cutters and sports fans
