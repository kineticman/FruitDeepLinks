# FruitDeepLinks 🍎

Unified sports scheduling system combining Apple TV Sports scraper with Peacock DeepLinks infrastructure for Channels DVR integration.

## Overview

FruitDeepLinks aggregates live sports events from multiple streaming providers:
- **Apple TV Sports**: ESPN+, Prime Video, CBS Sports, Paramount+, DAZN, FOX Sports, NBA League Pass, NFL+, Apple TV+
- **Peacock**: Native Peacock sports content

Exports XMLTV/M3U files compatible with Channels DVR, providing a unified EPG with proper provider categorization and event deeplinks.

## Features

- 📺 **Multi-provider aggregation**: 800+ events from 9+ streaming services
- 🏷️ **Provider categorization**: Filter by ESPN+, Peacock, DAZN, etc. in EPG
- 🖼️ **Team logos**: Automatic competitor logo extraction
- 🔗 **Smart deeplinks**: Apple TV web URLs for universal compatibility
- 📅 **Placeholder scheduling**: Clean EPG with "Event Not Started" and "Event Ended" blocks
- 🐳 **Docker ready**: Full containerization support

## Directory Structure

```
FruitDeepLinks/
├── bin/                          # Core scripts
│   ├── multi_scraper.py          # Apple TV Sports scraper (8 search terms)
│   ├── parse_events.py           # Parse Apple TV JSON output
│   ├── merge_json.py             # Merge multiple scrape sessions
│   ├── appletv_to_peacock.py     # Import Apple TV events → SQLite
│   ├── peacock_ingest_atom.py    # Fetch Peacock native events
│   ├── peacock_build_lanes.py    # Build virtual channel lanes
│   ├── peacock_export_hybrid.py  # Export XMLTV/M3U files
│   ├── peacock_server.py         # Flask web server
│   └── peacock_refresh_all.py    # Complete refresh workflow
├── data/
│   └── fruit_events.db           # SQLite database (events, lanes, images)
├── out/                          # Generated files
│   ├── direct.xml                # Direct XMLTV (24-hour window)
│   ├── direct.m3u                # Direct M3U with deeplinks
│   ├── peacock_lanes.xml         # Lane-based XMLTV
│   └── peacock_lanes.m3u         # Lane-based M3U
├── config.json                   # Apple TV scraper configuration
├── .env                          # Environment variables
├── docker-compose.yml            # Docker Compose configuration
├── Dockerfile                    # Container build instructions
└── requirements.txt              # Python dependencies
```

## Quick Start

### 1. Initial Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database with Peacock events
python bin/peacock_ingest_atom.py --db data/fruit_events.db --slug "/sports/live-and-upcoming"
```

### 2. Scrape Apple TV Sports

```bash
cd bin

# Run multi-provider scraper
python multi_scraper.py

# Parse raw JSON output
python parse_events.py --input multi_scraped.json
```

### 3. Import & Export

```bash
# Import Apple TV events into database
python appletv_to_peacock.py --apple-json parsed_events.json --peacock-db ../data/fruit_events.db

# Build virtual lanes (optional - for lane-based export)
python peacock_build_lanes.py --db ../data/fruit_events.db --lanes 15

# Export XMLTV/M3U files
python peacock_export_hybrid.py --db ../data/fruit_events.db \
    --direct-xml ../out/direct.xml \
    --direct-m3u ../out/direct.m3u
```

### 4. Add to Channels DVR

1. Navigate to Channels DVR Settings → Sources
2. Add Custom Channels source
3. XMLTV URL: `http://your-server:6655/out/direct.xml`
4. M3U URL: `http://your-server:6655/out/direct.m3u`

## Database Schema

### Events Table
- `id`: Unique event ID (`appletv-{id}` for Apple TV events)
- `pvid`: Provider variant ID (used for deeplinks)
- `title`: Event title (e.g., "Lakers vs. Celtics")
- `channel_name`: Provider name (ESPN, Peacock, DAZN, etc.)
- `start_utc` / `end_utc`: Event timing in ISO format
- `raw_attributes_json`: Stores images, competitors, playables, apple_tv_url

### Lane Events Table
- Maps events to virtual lanes with placeholder scheduling
- `is_placeholder`: Boolean flag for "Event Not Started" / "Event Ended" blocks

### Event Images Table
- Stores image URLs by type (landscape, scene169, titleArt169)
- Used for Peacock native events

## Export Modes

### Direct Mode (Recommended)
- **Window**: Next 24 hours of events
- **Channels**: One channel per event
- **Deeplinks**: Apple TV web URLs or provider app links
- **Use case**: Simple EPG, direct event access

### Lane Mode
- **Window**: 7 days ahead (configurable)
- **Channels**: 15 virtual lanes with rotating events
- **Deeplinks**: Peacock deeplinks for all events
- **Use case**: Traditional linear TV experience

## Deeplink Strategy

FruitDeepLinks uses a smart fallback strategy for deeplinks:

1. **App deeplinks** (if available): `sportscenter://`, `open.dazn.com://`
   - Only available for 30/824 events (currently live/available)
   - Device-specific (iOS/tvOS apps)

2. **Apple TV web URLs** (fallback): `https://tv.apple.com/us/sporting-event/...`
   - Universal compatibility (all devices)
   - Auto-redirects to proper provider (ESPN+, Prime Video, etc.)
   - Works for 794/824 events (future events)

3. **Peacock deeplinks** (Peacock native): `https://www.peacocktv.com/deeplink?...`
   - Native Peacock content only

## Provider Coverage

| Provider | Events | Deeplink Type |
|----------|--------|---------------|
| ESPN+ | 588 | Apple TV web URL |
| Prime Video | 222 | Apple TV web URL |
| Peacock | 129 + 249 native | Apple TV web / Peacock |
| DAZN | 18 | Apple TV web URL |
| CBS Sports | 43 | Apple TV web URL |
| Paramount+ | 43 | Apple TV web URL |
| NFL+ | 36 | Apple TV web URL |
| Apple TV+ | 10 | Apple TV web URL |

## Daily Refresh Automation

Create a cron job or scheduled task:

```bash
# Daily at 6 AM - refresh all data
0 6 * * * cd /path/to/FruitDeepLinks/bin && python peacock_refresh_all.py
```

Or use Windows Task Scheduler:
```powershell
# Run daily_refresh.bat at 6 AM
cd C:\projects\FruitDeepLinks\bin
python multi_scraper.py
python parse_events.py --input multi_scraped.json
python peacock_ingest_atom.py --db ../data/fruit_events.db --slug "/sports/live-and-upcoming"
python appletv_to_peacock.py --apple-json parsed_events.json --peacock-db ../data/fruit_events.db
python peacock_build_lanes.py --db ../data/fruit_events.db --lanes 15
python peacock_export_hybrid.py --db ../data/fruit_events.db --direct-xml ../out/direct.xml --direct-m3u ../out/direct.m3u
```

## Docker Deployment

```bash
# Build container
docker-compose build

# Run on port 6655
docker-compose up -d

# Access XMLTV/M3U
http://localhost:6655/out/direct.xml
http://localhost:6655/out/direct.m3u
```

## Troubleshooting

### No Apple TV events showing
```bash
# Check if events were imported
sqlite3 data/fruit_events.db "SELECT COUNT(*) FROM events WHERE id LIKE 'appletv-%';"

# Should return 824 (or similar)
```

### Missing images
```bash
# Check raw_attributes_json has competitor data
sqlite3 data/fruit_events.db "SELECT raw_attributes_json FROM events WHERE id LIKE 'appletv-%' LIMIT 1;"

# Should contain 'competitors' and 'logo_url'
```

### Deeplinks not working
- Apple TV web URLs require browser/web-capable player
- App deeplinks only work on iOS/tvOS with apps installed
- Peacock deeplinks require Peacock authentication

## Development

### Key Scripts

- `multi_scraper.py`: Searches 8 terms (basketball, football, soccer, hockey, baseball, skiing, figure skating, rugby)
- `appletv_to_peacock.py`: Maps Apple TV event structure to Peacock schema
- `peacock_export_hybrid.py`: Generates XMLTV with proper provider categories and placeholders

### Adding New Providers

Edit `multi_scraper.py` to add search terms, then update `get_provider_from_channel()` in `peacock_export_hybrid.py`.

## Credits

Built by Kineticman - Combining Apple TV Sports discovery with Peacock DeepLinks infrastructure.

## License

Personal use project - not affiliated with Apple, Peacock, ESPN, or other streaming providers.
