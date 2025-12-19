# FruitDeepLinks Service Discovery Summary
## Date: December 19, 2024

## Services Found in Database (Fresh Scrape)
Total: **19 services** with **2,237 playables** across **1,092 events**

### All Services Status:

| Service | Playables | Type | Status | Priority | Display Name |
|---------|-----------|------|--------|----------|--------------|
| aiv | 809 | app | ✅ MAPPED | 25 | Prime Video |
| sportscenter | 693 | app | ✅ MAPPED | 0 | ESPN |
| tv.apple.com | 168 | web | ✅ MAPPED | 10-14 | Apple MLS/MLB/NBA/NHL |
| pplus | 127 | app | ✅ MAPPED | 3 | Paramount+ |
| gametime | 97 | app | ✅ MAPPED | 26 | Prime Video TNF |
| kayo | 96 | app | ✅ MAPPED | 18 | Kayo Sports |
| cbssportsapp | 43 | app | ✅ MAPPED | 5 | CBS Sports |
| www.peacocktv.com | 40 | web | ✅ MAPPED | 2 | Peacock (Web) |
| open.dazn.com | 37 | app | ✅ MAPPED | 15-16 | DAZN |
| nflctv | 32 | app | ✅ MAPPED | 20 | NFL+ |
| vixapp | 19 | app | ✅ MAPPED | 19 | ViX |
| nbcsportstve | 17 | app | ✅ MAPPED | 7 | NBC Sports |
| fsapp | 17 | app | ✅ MAPPED | 9 | FOX Sports (Alt) |
| foxone | 12 | app | ✅ MAPPED | 8 | FOX Sports (App) |
| watchtnt | 9 | app | ✅ MAPPED | 22 | TNT |
| watchtru | 8 | app | ✅ MAPPED | 21 | truTV |
| play.hbomax.com | 8 | web | ✅ MAPPED | 4 | Max |
| cbstve | 3 | app | ✅ MAPPED | 6 | CBS |
| **watchtbs** | **2** | **app** | **✅ NEWLY ADDED** | **23** | **TBS** |

## Recent Changes

### TBS Added (PRIORITY 23)
- **Provider:** `watchtbs`
- **Display Name:** "TBS"
- **Sample Events:** College Football Playoff games
- **Deeplink Pattern:** `watchtbs://play?stream=east&campaign=universal-search&section=livestream&auth=true&source=tvos-search`
- **Priority:** 23 (same tier as TNT/truTV)

### League Services Added (PRIORITY 24)
These were added to the mapper but **NOT currently found in database**:
- `nba` → "NBA League Pass" (priority 24)
- `mlb` → "MLB.TV" (priority 24)  
- `nhl` → "NHL.TV" (priority 24)

**Note:** These are placeholder mappings in case Apple starts providing these deeplinks in the future. Currently, NBA/MLB/NHL games come through other providers (Prime Video, ESPN+, Peacock, etc.) or through `tv.apple.com` (Apple's own services).

## Priority Tiers Summary

### Premium Sports (0-9): 11 services
ESPN+, ESPN, Peacock, Peacock Web, Paramount+, Max, CBS Sports, CBS, NBC Sports, FOX Sports (2 variants)

### Apple Services (10-14): 5 services
Apple MLS, MLB, NBA, NHL, TV+

### Specialty/Niche (15-22): 8 services
DAZN, F1 TV, Kayo Sports, ViX, NFL+, truTV, TNT

### **Cable Sports (23): 1 service**
**TBS** (NEWLY ADDED)

### League Services (24): 3 services
NBA League Pass, MLB.TV, NHL.TV (not yet in database)

### Amazon Aggregator (25-26): 2 services
Prime Video, Prime Video TNF

### Generic Web (30+): 2 services
HTTPS, HTTP fallbacks

## Files Updated

1. **logical_service_mapper.py**
   - Added `watchtbs` display name
   - Added `watchtbs` priority (23)
   - Added NBA/MLB/NHL placeholders (24)

2. **fruit_export_lanes.py**
   - Fixed description/category mismatch bug
   - Strips old "Available on X" text before adding correct provider
   - Both description and category now use same logic

## Tools Created

1. **discover_services.py** - Full database scanner
   - Discovers all services in database
   - Shows mapped vs unmapped
   - Provides sample URLs and events
   - Can be run periodically to detect new Apple services

2. **list_mapped_services.py** - Quick reference
   - Lists all 34 currently mapped services
   - Shows priorities and tiers
   - Fast reference tool

## Usage Commands

```powershell
# Copy updated files to container
docker cp logical_service_mapper.py fruitdeeplinks_app:/app/
docker cp fruit_export_lanes.py fruitdeeplinks_app:/app/
docker cp discover_services.py fruitdeeplinks_app:/app/devtools/
docker cp list_mapped_services.py fruitdeeplinks_app:/app/devtools/

# Rebuild lanes with new priority
docker exec fruitdeeplinks_app python /app/fruit_build_lanes.py --db /app/data/fruit_events.db --lane-count 35

# Export with fixed descriptions
docker exec fruitdeeplinks_app python /app/fruit_export_lanes.py --db /app/data/fruit_events.db --xmltv /app/data/lanes.xml --m3u /app/data/lanes.m3u --server-url http://YOUR_SERVER:5007

# Check for new services (run occasionally)
docker exec fruitdeeplinks_app python /app/devtools/discover_services.py
```

## Bugs Fixed

### Bug #1: CBS Sports vs Paramount+ Mismatch
**Problem:** Event showed "Available on CBS Sports" in description but "Paramount+" in category/deeplink
**Cause:** Description used `channel_name` from Apple API, category used `chosen_provider` from selected playable
**Fix:** Both now use `chosen_provider` → `chosen_logical_service` → `channel_name` (in priority order)

### Bug #2: Double "Available on" Text
**Problem:** "Available on NBA - Available on Prime Video" 
**Cause:** Synopsis had "Available on NBA" from import, then export added "Available on Prime Video"
**Fix:** Strip any existing "Available on X" text before appending the correct chosen provider

## Notes

- **No NBA/MLB/NHL League Pass deeplinks** currently exist in Apple TV API for your region/events
- **TBS is real and working** - found in fresh scrape with 2 College Football Playoff events
- All 19 services in your database are now properly mapped
- The discover_services.py tool had an import issue that's been fixed in the updated version
