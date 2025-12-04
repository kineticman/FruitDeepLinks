# FruitDeepLinks - Docker Setup

Multi-source sports event aggregator combining Apple TV and Peacock content into virtual TV channels.

## Quick Start

### 1. Initial Setup

```bash
# Clone/copy your FruitDeepLinks project
cd ~/Projects/FruitDeepLinks

# Copy environment template
cp .env.example .env

# Edit .env with your settings (especially SERVER_URL and CHANNELS_DVR_IP)
nano .env
```

### 2. Build and Run

```bash
# Build the Docker image
docker-compose build

# Start the container
docker-compose up -d

# Check logs
docker-compose logs -f
```

### 3. Manual Refresh (Test)

```bash
# Run refresh immediately (don't wait for 3am)
docker exec fruitdeeplinks python3 /app/bin/daily_refresh.py

# Or with skip-scrape flag
docker exec fruitdeeplinks python3 /app/bin/daily_refresh.py --skip-scrape
```

## Configuration

### Key Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PEACOCK_LANES` | 40 | Number of virtual channels |
| `PEACOCK_DAYS_AHEAD` | 7 | Days to plan ahead |
| `SERVER_URL` | - | Your server URL for M3U export |
| `CHANNELS_DVR_IP` | - | Auto-refresh Channels DVR (optional) |
| `TZ` | America/New_York | Timezone for scheduling |
| `HEADLESS` | true | Run browser in headless mode |

See `.env.example` for all configuration options.

## Scheduled Execution

The container runs daily refresh at **3:00 AM** automatically via cron.

To change the schedule, edit the crontab in `Dockerfile`:
```dockerfile
# Change "0 3" to desired hour
RUN echo "0 3 * * * cd /app && /usr/local/bin/python /app/bin/daily_refresh.py >> /app/logs/cron.log 2>&1" | crontab -
```

## Output Files

All outputs are in the `./out/` directory (mounted volume):

- **Lanes (Virtual Channels)**:
  - `peacock_lanes.xml` - XMLTV guide for scheduled channels
  - `peacock_lanes.m3u` - M3U playlist for scheduled channels

- **Direct (Individual Events)**:
  - `direct.xml` - XMLTV guide for individual event channels
  - `direct.m3u` - M3U playlist for individual event channels

- **Raw Data**:
  - `multi_scraped.json` - Raw Apple TV scrape
  - `parsed_events.json` - Parsed Apple TV events

## Database

SQLite database is in `./data/fruit_events.db` (mounted volume).

### Inspect Database

```bash
# Enter container
docker exec -it fruitdeeplinks bash

# Query database
sqlite3 /app/data/fruit_events.db "SELECT COUNT(*) FROM events;"
sqlite3 /app/data/fruit_events.db "SELECT DISTINCT channel_name FROM events;"
```

## Troubleshooting

### Check Logs

```bash
# Container logs
docker-compose logs -f

# Cron logs
docker exec fruitdeeplinks cat /app/logs/cron.log

# Check if cron is running
docker exec fruitdeeplinks pgrep cron
```

### Rebuild Lanes Manually

```bash
# If lanes aren't updating properly
docker exec fruitdeeplinks python3 /app/bin/peacock_build_lanes.py --db /app/data/fruit_events.db --lanes 40

# Then re-export
docker exec fruitdeeplinks python3 /app/bin/peacock_export_lanes.py --db /app/data/fruit_events.db
```

### View Event Distribution

```bash
# Check which providers are in lanes
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db \
  "SELECT DISTINCT e.channel_name FROM lane_events le JOIN events e ON le.event_id = e.id;"

# Count events per provider
docker exec fruitdeeplinks sqlite3 /app/data/fruit_events.db \
  "SELECT channel_name, COUNT(*) FROM events GROUP BY channel_name ORDER BY COUNT(*) DESC;"
```

### Force Rebuild

```bash
# Stop container
docker-compose down

# Remove old data (if needed)
# rm -rf data/*.db

# Restart
docker-compose up -d

# Run initial refresh
docker exec fruitdeeplinks python3 /app/bin/daily_refresh.py
```

## Updating

```bash
# Pull latest changes
git pull

# Rebuild and restart
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Integration with Channels DVR

Add the M3U and XMLTV URLs to Channels DVR:

1. **M3U Source**:
   - URL: `http://192.168.86.72/out/peacock_lanes.m3u` (or your server)
   - Name: `FruitDeepLinks Lanes`

2. **XMLTV Source**:
   - URL: `http://192.168.86.72/out/peacock_lanes.xml`
   - Lineup: Map to the M3U source above

3. **Direct Events** (optional):
   - M3U: `http://192.168.86.72/out/direct.m3u`
   - XMLTV: `http://192.168.86.72/out/direct.xml`

## Architecture

```
Apple TV Scraper → multi_scraped.json → parse_events.py → parsed_events.json
                                                              ↓
Peacock Scraper → peacock_ingest_atom.py → fruit_events.db ← appletv_to_peacock.py
                                                ↓
                                    peacock_build_lanes.py (combine & schedule)
                                                ↓
                                    lane_events table (unified schedule)
                                                ↓
                            peacock_export_lanes.py → peacock_lanes.xml/m3u
                            peacock_export_hybrid.py → direct.xml/m3u
```

## Support

For issues or questions, check:
- Container logs: `docker-compose logs`
- Cron logs: `/app/logs/cron.log`
- Database: `sqlite3 /app/data/fruit_events.db`
