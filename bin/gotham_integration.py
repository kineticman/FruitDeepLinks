#!/usr/bin/env python3
"""
gotham_integration.py - Self-contained Gotham Sports scraper and ingest for FruitDeepLinks

Scrapes MSG/YES Network events from Gotham Sports API and ingests directly into fruit_events.db.
Combines scraping and database operations in a single script for simplicity.

Usage:
    python gotham_integration.py --db /app/data/fruit_events.db --days 7

Environment Variables:
    GOTHAM_ZONE: DMA zone to scrape (default: zone-1 for NYC metro)
    GOTHAM_DAYS: Number of days to scrape (default: 7)
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode, urlparse

import requests

# Import genre normalization utilities
try:
    from genre_utils import normalize_genres
except ImportError:
    # Fallback if genre_utils not available
    def normalize_genres(genres):
        return genres

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== GOTHAM SCRAPER ====================

# Gotham Sports API Configuration
BASE_API_URL = "https://api.gothamsports.com/proxy"
CONFIG_URL = "https://config.gothamsports.com/Configurations/v2/build.json"
PROVIDER_CODE = "gotham"
LOGICAL_SERVICE = "gotham"
SERVICE_PRIORITY = 20  # Similar to specialty sports services

# Channel definitions for zone-1 (NYC metro) - MSG and YES
ZONE_1_CHANNELS = {
    "MSG": {
        "channelId": "057E6429-044F-49E6-9E97-64D617B4D3CD",
        "name": "MSG",
        "stationId": "10979",
    },
    "MSG Sportsnet": {
        "channelId": "6D250945-BB55-44D4-A5A9-3DF45DBE134E",
        "name": "MSG Sportsnet HD",
        "stationId": "15273",
    },
    "MSG2": {
        "channelId": "F1DA3786-A8A2-4C3D-B18E-F400F9C6EE0B",
        "name": "MSG2 HD",
        "stationId": "70283",
    },
    "MSG Sportsnet 2": {
        "channelId": "0135EBDF-184F-41FA-B36C-46CDA4FC9B33",
        "name": "MSG Sportsnet 2 HD",
        "stationId": "70285",
    },
    "YES": {
        "channelId": "BD50D13C-CC01-4518-AD42-B3EFACF1DBF5",
        "name": "Yes Network",
        "stationId": "30017",
    },
}


def _base_headers(rsn_id: str) -> Dict[str, str]:
    """Generate base headers for Gotham API requests"""
    return {
        "User-Agent": "okhttp/4.9.0",
        "gg-rsn-id": rsn_id,
        "Accept": "application/json",
    }


def _api_get(path: str, params: Dict[str, Any], rsn_id: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """Perform a GET to Gotham proxy API and return JSON dict (or None on error)"""
    url = f"{BASE_API_URL}/{path.lstrip('/')}"
    qs = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
    full_url = f"{url}?{qs}" if qs else url

    try:
        resp = requests.get(full_url, headers=_base_headers(rsn_id), timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"GET failed: {full_url} ({e})")
        return None


def get_gotham_config() -> str:
    """Fetch RSNid from Gotham Sports configuration"""
    try:
        response = requests.get(
            CONFIG_URL,
            headers={"User-Agent": "okhttp/4.9.0"},
            timeout=30
        )
        response.raise_for_status()
        config = response.json()
        rsn_id = config.get("RSNid")

        if not rsn_id:
            raise ValueError("RSNid not found in config response")

        logger.info(f"Retrieved RSNid: {rsn_id}")
        return rsn_id

    except Exception as e:
        logger.error(f"Failed to fetch Gotham config: {e}")
        raise


def fetch_channel_epg(
    channel_id: str,
    channel_name: str,
    zone: str,
    start_dt: datetime,
    end_dt: datetime,
    rsn_id: str
) -> List[Dict[str, Any]]:
    """Fetch EPG data for a specific channel"""
    # Format timestamps for API
    start_str = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    url = (
        f"{BASE_API_URL}/content/epg"
        f"?reg={zone}"
        f"&dt=androidtv"
        f"&channel={channel_id}"
        f"&client=game-gotham-androidtv"
        f"&start={start_str}"
        f"&end={end_str}"
    )

    try:
        logger.info(f"Fetching EPG for {channel_name}...")
        response = requests.get(url, headers=_base_headers(rsn_id), timeout=30)
        response.raise_for_status()

        data = response.json()

        if not data.get("gameSuccess"):
            error = data.get("gameError", {})
            logger.warning(
                f"API error for {channel_name}: {error.get('description', 'Unknown error')}"
            )
            return []

        if not data.get("data"):
            logger.info(f"No EPG data for {channel_name}")
            return []

        # Extract airings from response
        events: List[Dict[str, Any]] = []
        for schedule in data["data"]:
            airings = schedule.get("airing", [])
            events.extend(airings)

        logger.info(f"Found {len(events)} airings for {channel_name}")
        return events

    except requests.RequestException as e:
        logger.error(f"Failed to fetch EPG for {channel_name}: {e}")
        return []


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO8601 datetime string"""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _pick_lang_name(obj: Any) -> Optional[str]:
    """Extract title from Gotham language array format: [{"lang":"en","n":"Title"}]"""
    if isinstance(obj, list) and obj:
        if isinstance(obj[0], dict):
            return obj[0].get("n")
    return None


def normalize_event(raw: Dict[str, Any], channel_name: str) -> Optional[Dict[str, Any]]:
    """
    Convert Gotham EPG airing to normalized format for FruitDeepLinks
    
    Returns normalized event dict or None if should be skipped
    """
    # Extract program data
    pgm = raw.get("pgm", {})

    # Get title from nested structure: pgm.lon[0].n
    title = _pick_lang_name(pgm.get("lon", [])) or "Unknown Event"
    title = " ".join(title.replace("\r", " ").replace("\n", " ").split())

    # Skip OFF AIR placeholders (24-hour blocks with no content)
    if "OFF AIR" in title.upper() or "NO PROGRAMMING" in title.upper():
        return None

    # Get external ID - Use airing ID for uniqueness
    external_id = raw.get("id")
    if not external_id:
        start_time = raw.get("sc_st_dt", "")
        external_id = f"gotham-{channel_name}-{start_time}".replace(":", "-")

    # Extract timing
    start_utc = raw.get("sc_st_dt")  # Already in ISO8601 format
    end_utc = raw.get("sc_ed_dt")
    
    if not start_utc or not end_utc:
        logger.warning(f"Event {external_id} missing start/end times, skipping")
        return None

    # Parse datetime objects for calculations
    start_dt = _parse_iso(start_utc)
    end_dt = _parse_iso(end_utc)
    
    if not start_dt or not end_dt:
        logger.warning(f"Event {external_id} has invalid datetime format, skipping")
        return None

    # Calculate duration
    runtime_secs = int((end_dt - start_dt).total_seconds())

    # Sport and league info
    sport = pgm.get("spt_ty", "").title()
    if not sport:
        # Skip events without sport classification (talk shows, documentaries, etc.)
        logger.debug(f"Event {external_id} ({title}) has no sport type, skipping non-sports programming")
        return None
    
    league = pgm.get("spt_lg", "") or channel_name

    # Team information
    teams = pgm.get("tm", [])
    home_team = None
    away_team = None

    for team in teams:
        if team.get("home"):
            home_team = _pick_lang_name(team.get("lon", [])) or team.get("abbr")
        else:
            away_team = _pick_lang_name(team.get("lon", [])) or team.get("abbr")

    # Build display title with teams if available
    if home_team and away_team:
        display_title = f"{away_team} at {home_team}"
    else:
        display_title = title

    # Get description
    description = _pick_lang_name(pgm.get("loen", [])) or _pick_lang_name(pgm.get("lod", []))

    # Extract image URL
    hero_image = None
    images = pgm.get("img", [])
    if images:
        # Prefer 16x9 images
        for img in images:
            if img.get("ar") == "16x9":
                hero_image = img.get("url")
                break
        if not hero_image and images:
            hero_image = images[0].get("url")

    # Check content type and availability
    content_type = raw.get("cty", "live")  # live, vod, ppv, etc.
    is_live = content_type == "live"
    is_premium = content_type != "free"

    # Get playable URL from program metadata
    playable_url = None
    c_id = raw.get("c_id") or pgm.get("id")
    
    if c_id:
        # Gotham uses /watch/<content_id> pattern for web playback
        playable_url = f"https://gothamsports.com/watch/{c_id}"
    
    # Alternative: check for playback URL in raw data
    if not playable_url:
        pb_url = raw.get("pb_url")
        if pb_url:
            playable_url = f"https://gothamsports.com{pb_url}"

    if not playable_url:
        logger.warning(f"Event {external_id} ({display_title}) has no playable URL, using fallback")
        playable_url = f"https://gothamsports.com/watch/{external_id}"

    return {
        "external_id": str(external_id),
        "title": display_title,
        "sport": sport,
        "league": league,
        "channel": channel_name,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "runtime_secs": runtime_secs,
        "description": description or f"{sport} on {channel_name}",
        "hero_image": hero_image,
        "is_live": is_live,
        "is_premium": is_premium,
        "content_type": content_type,
        "playable_url": playable_url,
        "home_team": home_team,
        "away_team": away_team,
        "raw_data": raw,  # Store for debugging
    }


def scrape_gotham(zone: str = "zone-1", days: int = 7) -> List[Dict[str, Any]]:
    """
    Scrape Gotham Sports EPG data
    
    Returns:
        List of normalized event dictionaries
    """
    logger.info(f"Starting Gotham Sports scrape for {zone} ({days} days)")

    # Get RSN ID from config
    rsn_id = get_gotham_config()

    # Calculate time range
    now = datetime.now(timezone.utc)
    start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=days)

    logger.info(f"Scraping from {start_dt} to {end_dt}")

    # Scrape each channel
    all_events: List[Dict[str, Any]] = []

    for channel_key, channel_info in ZONE_1_CHANNELS.items():
        channel_id = channel_info["channelId"]
        channel_name = channel_info["name"]

        raw_events = fetch_channel_epg(
            channel_id=channel_id,
            channel_name=channel_name,
            zone=zone,
            start_dt=start_dt,
            end_dt=end_dt,
            rsn_id=rsn_id
        )

        for raw_event in raw_events:
            try:
                normalized = normalize_event(raw_event, channel_name)
                if normalized is None:
                    continue
                    
                # Skip non-live paid content (PPV without live component)
                if normalized["content_type"] == "paid" and not normalized["is_live"]:
                    continue
                    
                all_events.append(normalized)
            except Exception as e:
                logger.warning(f"Failed to normalize event: {e}", exc_info=True)
                continue

    logger.info(f"Successfully scraped {len(all_events)} events from Gotham Sports")
    return all_events


# ==================== DATABASE INGEST ====================

def datetime_to_ms(dt: datetime) -> int:
    """Convert datetime to milliseconds since epoch"""
    return int(dt.timestamp() * 1000)


def ingest_event(
    conn: sqlite3.Connection,
    event: Dict[str, Any],
    now_utc: str
) -> bool:
    """
    Ingest a single event into fruit_events.db
    
    Returns:
        True if event was inserted/updated, False if skipped
    """
    try:
        # Build event ID
        event_id = f"{PROVIDER_CODE}-{event['external_id']}"
        
        # Convert times to both formats
        start_dt = event['start_dt']
        end_dt = event['end_dt']
        start_ms = datetime_to_ms(start_dt)
        end_ms = datetime_to_ms(end_dt)
        created_ms = datetime_to_ms(datetime.now(timezone.utc))
        
        # Build genres JSON - ONLY include sport type (not league)
        # League goes in channel_name and classification_json
        # Normalize genres to filter out non-sports categories and fix capitalization
        raw_genres = [event['sport']]
        normalized_genres = normalize_genres(raw_genres)
        genres_json = json.dumps(normalized_genres)
        
        # Build classification JSON for league
        classification_json = json.dumps([
            {"type": "sport", "value": event['sport']},
            {"type": "league", "value": event['league']}
        ])
        
        # Insert/update event
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO events (
                id, pvid, title, title_brief, synopsis, synopsis_brief,
                channel_name, channel_provider_id,
                genres_json, classification_json,
                is_premium, runtime_secs,
                start_ms, end_ms, start_utc, end_utc,
                created_ms, created_utc, last_seen_utc,
                hero_image_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = COALESCE(excluded.title, title),
                synopsis = COALESCE(excluded.synopsis, synopsis),
                end_utc = COALESCE(excluded.end_utc, end_utc),
                end_ms = COALESCE(excluded.end_ms, end_ms),
                last_seen_utc = excluded.last_seen_utc,
                hero_image_url = COALESCE(excluded.hero_image_url, hero_image_url),
                genres_json = COALESCE(excluded.genres_json, genres_json)
        """, (
            event_id,
            event['external_id'],  # pvid - CRITICAL for M3U export
            event['title'],
            event['title'][:100] if len(event['title']) > 100 else event['title'],
            event['description'],
            event['description'][:200] if len(event['description']) > 200 else event['description'],
            event['channel'],  # channel_name (e.g., "MSG", "YES Network")
            PROVIDER_CODE,  # channel_provider_id
            genres_json,
            classification_json,
            1 if event['is_premium'] else 0,
            event['runtime_secs'],
            start_ms,
            end_ms,
            event['start_utc'],
            event['end_utc'],
            created_ms,
            now_utc,
            now_utc,
            event['hero_image'],
        ))
        
        # Insert playable
        playable_id = f"{event['external_id']}-main"
        cur.execute("""
            INSERT INTO playables (
                event_id, playable_id, provider, logical_service,
                deeplink_play, deeplink_open, playable_url,
                priority, created_utc, title
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id, playable_id) DO UPDATE SET
                deeplink_play = COALESCE(excluded.deeplink_play, deeplink_play),
                playable_url = COALESCE(excluded.playable_url, playable_url),
                logical_service = excluded.logical_service,
                priority = excluded.priority
        """, (
            event_id,
            playable_id,
            PROVIDER_CODE,
            LOGICAL_SERVICE,
            event['playable_url'],
            event['playable_url'],
            event['playable_url'],
            SERVICE_PRIORITY,
            now_utc,
            event['title'],
        ))
        
        # Insert hero image if available
        if event['hero_image']:
            cur.execute("""
                INSERT OR IGNORE INTO event_images (event_id, img_type, url)
                VALUES (?, ?, ?)
            """, (event_id, "hero", event['hero_image']))
        
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Failed to ingest event {event.get('external_id')}: {e}", exc_info=True)
        conn.rollback()
        return False


def ingest_all_events(
    conn: sqlite3.Connection,
    events: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Ingest all scraped events into database
    
    Returns:
        Dictionary with counts: inserted, updated, failed
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    
    stats = {
        "inserted": 0,
        "failed": 0,
    }
    
    for event in events:
        if ingest_event(conn, event, now_utc):
            stats["inserted"] += 1
        else:
            stats["failed"] += 1
    
    return stats


# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(
        description="Gotham Sports scraper and ingest for FruitDeepLinks"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to fruit_events.db"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=int(os.getenv("GOTHAM_DAYS", "7")),
        help="Number of days to scrape (default: 7)"
    )
    parser.add_argument(
        "--zone",
        default=os.getenv("GOTHAM_ZONE", "zone-1"),
        help="DMA zone to scrape (default: zone-1 for NYC)"
    )

    args = parser.parse_args()

    try:
        # Step 1: Scrape events
        logger.info("=" * 60)
        logger.info("STEP 1: SCRAPING GOTHAM SPORTS")
        logger.info("=" * 60)
        
        events = scrape_gotham(zone=args.zone, days=args.days)
        
        if not events:
            logger.warning("No events scraped, exiting")
            return 0
        
        logger.info(f"Scraped {len(events)} events")
        
        # Step 2: Ingest into database
        logger.info("=" * 60)
        logger.info("STEP 2: INGESTING INTO DATABASE")
        logger.info("=" * 60)
        
        db_path = Path(args.db)
        if not db_path.exists():
            logger.error(f"Database not found: {db_path}")
            sys.exit(1)
        
        conn = sqlite3.connect(str(db_path))
        stats = ingest_all_events(conn, events)
        conn.close()
        
        logger.info("=" * 60)
        logger.info("INGEST COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Inserted/Updated: {stats['inserted']}")
        logger.info(f"Failed: {stats['failed']}")
        
        # Summary
        print("\n" + "=" * 60)
        print("GOTHAM SPORTS INTEGRATION SUMMARY")
        print("=" * 60)
        print(f"Zone: {args.zone}")
        print(f"Days: {args.days}")
        print(f"Events Scraped: {len(events)}")
        print(f"Events Ingested: {stats['inserted']}")
        print(f"Failed: {stats['failed']}")
        print(f"Database: {args.db}")
        print("=" * 60)
        
        return 0

    except Exception as e:
        logger.error(f"Integration failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
