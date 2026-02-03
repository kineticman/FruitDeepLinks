#!/usr/bin/env python3
"""
bein_import.py - Import beIN Sports EPG data into FruitDeepLinks

Reads beIN API snapshot JSON and normalizes into events + playables tables.
Handles the actual beIN API response structure with nested data/channel/sportType objects.

Usage:
    python bein_import.py --bein-json data/bein_snapshot.json --fruit-db data/fruit_events.db
"""

import argparse
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

LOG = logging.getLogger("bein_import")

# Schema version tracking
_SCHEMA_ENSURED = False


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_schema(conn: sqlite3.Connection):
    """Ensure events and playables tables exist (idempotent)"""
    global _SCHEMA_ENSURED
    if _SCHEMA_ENSURED:
        return
    
    cur = conn.cursor()
    
    # Core events table - ACTUAL FruitDeepLinks schema from ADDING_NEW_STREAMING_SERVICE.md
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            pvid TEXT,
            slug TEXT,
            title TEXT,
            title_brief TEXT,
            synopsis TEXT,
            synopsis_brief TEXT,
            channel_name TEXT,
            channel_provider_id TEXT,
            airing_type TEXT,
            classification_json TEXT,
            genres_json TEXT,
            content_segments_json TEXT,
            is_free INTEGER,
            is_premium INTEGER,
            runtime_secs INTEGER,
            start_ms INTEGER,
            end_ms INTEGER,
            start_utc TEXT,
            end_utc TEXT,
            created_ms INTEGER,
            created_utc TEXT,
            hero_image_url TEXT,
            last_seen_utc TEXT,
            raw_attributes_json TEXT
        )
    """)
    
    # Playables table for streaming options
    cur.execute("""
        CREATE TABLE IF NOT EXISTS playables (
            event_id TEXT NOT NULL,
            playable_id TEXT NOT NULL,
            provider TEXT,
            service_name TEXT,
            logical_service TEXT,
            deeplink_play TEXT,
            deeplink_open TEXT,
            http_deeplink_url TEXT,
            playable_url TEXT,
            title TEXT,
            content_id TEXT,
            priority INTEGER DEFAULT 0,
            created_utc TEXT,
            locale TEXT,
            espn_graph_id TEXT,
            PRIMARY KEY (event_id, playable_id)
        )
    """)
    
    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(start_utc, end_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playables_event ON playables(event_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playables_logical_service ON playables(logical_service)")
    
    conn.commit()
    _SCHEMA_ENSURED = True
    LOG.info("Schema ensured")


def parse_iso_timestamp(iso_str: Optional[str]) -> Optional[int]:
    """Convert ISO 8601 timestamp to milliseconds since epoch"""
    if not iso_str:
        return None
    try:
        # Handle both .000Z format and Z format
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except Exception as e:
        LOG.warning(f"Failed to parse timestamp '{iso_str}': {e}")
        return None


def normalize_sport_name(raw_sport: str, category: str, genre: str) -> str:
    """
    Normalize sport names to consolidate similar categories
    
    This prevents filter UI pollution with overly-granular sport names like
    "Tennis - Atp 250", "Basketball - Nba", "Handball - French Handball League", etc.
    
    Returns normalized broad sport category.
    """
    if not raw_sport:
        return "Other"
    
    sport_lower = raw_sport.lower()
    category_lower = category.lower() if category else ""
    genre_lower = genre.lower() if genre else ""
    
    # Combine all text for pattern matching
    all_text = f"{sport_lower} {category_lower} {genre_lower}".lower()
    
    # Special handling for beIN's "Sports General" catch-all category
    # Check the title for sport indicators when category is unhelpful
    title_text = ""
    if 'sports general' in category_lower:
        # We'll check title later for sport-specific terms
        title_text = sport_lower  # sport_lower may actually be derived from title in some cases
    
    # Football/Soccer (most common beIN sport)
    if any(x in all_text for x in ['football', 'soccer', 'premier league', 'la liga', 'laliga',
                                     'ligue 1', 'serie a', 'bundesliga', 'liga', 
                                     'champions league', 'europa league', 'calcio',
                                     'uefa', 'afc champions', 'caf champions', 'copa libertadores']):
        return "Soccer"
    
    # Tennis (consolidate all tennis variants)
    if any(x in all_text for x in ['tennis', 'atp', 'wta', 'grand slam', 'davis cup']):
        return "Tennis"
    
    # Basketball (consolidate NBA, international, FIBA, etc.)
    if any(x in all_text for x in ['basketball', 'nba', 'fiba', '3x3']):
        return "Basketball"
    
    # Hockey (all variants)
    if any(x in all_text for x in ['hockey', 'nhl', 'ice hockey']):
        return "Hockey"
    
    # Rugby (all variants)
    if 'rugby' in all_text:
        return "Rugby"
    
    # Handball
    if 'handball' in all_text:
        return "Handball"
    
    # Motorsports (consolidate all racing)
    if any(x in all_text for x in ['motorsport', 'racing', 'motogp', 'f1', 'formula',
                                     'rally', 'superbike', 'moto gp', 'grand prix']):
        return "Motorsports"
    
    # Combat Sports
    if any(x in all_text for x in ['boxing', 'mma', 'wrestling', 'martial']):
        return "Combat Sports"
    
    # Equestrian
    if any(x in all_text for x in ['equestrian', 'horse', 'prix']):
        return "Equestrian"
    
    # Cricket
    if 'cricket' in all_text:
        return "Cricket"
    
    # Golf
    if 'golf' in all_text:
        return "Golf"
    
    # Volleyball
    if any(x in all_text for x in ['volleyball', 'volley']):
        return "Volleyball"
    
    # Athletics / Track & Field
    if any(x in all_text for x in ['athletics', 'track', 'field']):
        return "Athletics"
    
    # Baseball
    if any(x in all_text for x in ['baseball', 'mlb']):
        return "Baseball"
    
    # American Football
    if 'american football' in all_text or 'nfl' in all_text:
        return "American Football"
    
    # Table Tennis / Ping Pong
    if 'table tennis' in all_text or 'ping pong' in all_text:
        return "Table Tennis"
    
    # Darts
    if 'darts' in all_text:
        return "Darts"
    
    # Lacrosse
    if 'lacrosse' in all_text:
        return "Lacrosse"
    
    # Netball
    if 'netball' in all_text:
        return "Netball"
    
    # Gridiron
    if 'gridiron' in all_text:
        return "Gridiron"
    
    # Water Sports (swimming, diving, water polo, etc.)
    if any(x in all_text for x in ['swimming', 'diving', 'water polo', 'aquatic']):
        return "Water Sports"
    
    # Winter Sports (skiing, snowboarding, etc.)
    if any(x in all_text for x in ['skiing', 'snowboard', 'winter', 'ice skating', 'curling']):
        return "Winter Sports"
    
    # Cycling
    if any(x in all_text for x in ['cycling', 'tour de france', 'giro', 'vuelta']):
        return "Cycling"
    
    # Multisports / Olympic Sports
    if any(x in all_text for x in ['multisport', 'olympic', 'triathlon', 'decathlon', 
                                     'sports event']):  # "Sports Event" category in beIN
        return "Olympic Sports"
    
    # Catch "Sports General" which is filler content
    if 'sports general' in all_text:
        return "Other"  # Will be filtered out
    
    # Fallback for genuinely unknown sports
    # Use the raw sport name, capitalized
    return raw_sport.title()


def extract_sport_info(row: Dict[str, Any]) -> Tuple[str, str]:
    """
    Extract sport type from beIN event
    
    Returns:
        (normalized_sport_type, genre)
    
    beIN structure:
    - category: "Football", "WTA 500", etc.
    - genre: "Football (Soccer)", "Tennis", etc.
    - sportType.name: "Football", "Tennis", etc.
    """
    sport_type = ""
    genre = row.get("genre", "")
    category = row.get("category", "")
    
    # Prefer sportType.name for primary classification
    sport_obj = row.get("sportType")
    if sport_obj and isinstance(sport_obj, dict):
        sport_type = sport_obj.get("name", "")
    
    # Fallback to genre or category
    if not sport_type:
        sport_type = genre or category
    
    # Normalize to broad categories to prevent filter pollution
    normalized_sport = normalize_sport_name(sport_type, category, genre)
    
    return normalized_sport, genre


def extract_channel_info(row: Dict[str, Any]) -> Tuple[str, str]:
    """
    Extract channel metadata from beIN event
    
    Returns:
        (channel_id, channel_name)
    
    beIN structure:
    - channel.externalId: "10803", "431601", "55002"
    - channel.name: "beIN SPORTS EN 1", "beIN SPORTS 2", "beIN SPORTS XTRA"
    - channel.data.StaticChannelCode: Alternative ID
    """
    channel = row.get("channel", {})
    if not isinstance(channel, dict):
        return "", ""
    
    channel_id = channel.get("externalId", "")
    channel_name = channel.get("name", "")
    
    return channel_id, channel_name


def should_import_event(row: Dict[str, Any]) -> bool:
    """
    Determine if this beIN event should be imported
    
    FruitDeepLinks Mission: LIVE SPORTS ONLY
    - Skip replays, highlights, documentaries, talk shows
    - Skip promotional filler content
    - Skip non-sports programming
    - Skip classic/archived matches from previous years
    
    Criteria:
    - Must have valid time data (startDate/endDate)
    - Must have title
    - Prefer live events
    - Skip obvious non-sports content
    """
    # Must have basic metadata
    if not row.get("title"):
        return False
    
    # Must have time data
    if not row.get("startDate") or not row.get("endDate"):
        return False
    
    # Safely get strings, default to empty string if None
    title = (row.get("title") or "").lower()
    category = (row.get("category") or "").lower()
    genre = (row.get("genre") or "").lower()
    synopsis = (row.get("synopsis") or "").lower()
    
    # SKIP: Obvious filler/promotional content
    skip_keywords = [
        'bein sports max',           # Generic channel filler
        'bein sports xtra for live', # Promotional content
        'sign off',                  # Channel sign-off
        'le plus grand des spectacles', # Generic promo
    ]
    if any(keyword in title for keyword in skip_keywords):
        return False
    
    # SKIP: Classic matches and replays from previous years
    # beIN uses "Classic" in category for archived matches
    # e.g., "LaLiga Classic", "Serie A Classic"
    if 'classic' in category:
        return False
    
    # Additional check: years in title indicate old matches
    if any(year in title for year in ['2017', '2018', '2019', '2020', '2021', '2022', '2023']):
        return False
    
    # SKIP: Non-sports programming and filler content
    non_sports_categories = [
        'special, entertainment',
        'variety',
        'news bulletin',
        'interview',
        'sports general',        # beIN's catch-all for news/filler (rarely actual sports)
    ]
    if any(cat in category for cat in non_sports_categories):
        return False
    
    # SKIP: Talking head shows, magazines, highlights, replays
    skip_title_keywords = [
        'highlight',
        'magazine',
        '90 in 30',              # Highlight show
        'documentary',
        'the big interview',
        'efl highlights',
        'uel/uecl magazine',
        'nba action',            # Magazine show
    ]
    if any(keyword in title for keyword in skip_title_keywords):
        return False
    
    # Check if this is a replay (but not live)
    is_live = row.get("live", False)
    is_replay = row.get("replay", False)
    
    # SKIP: Replays (unless they're currently live - which would be weird but possible)
    if is_replay and not is_live:
        return False
    
    # Check if this has sport classification
    sport_obj = row.get("sportType")
    has_sport = sport_obj and isinstance(sport_obj, dict) and sport_obj.get("name")
    
    # Also check genre and category as fallback
    has_genre = bool(row.get("genre"))
    has_category = bool(row.get("category"))
    
    # Must have some sport indication
    if not (has_sport or has_genre or has_category):
        return False
    
    # GOOD: This appears to be a live sports event
    return True


def create_playable_for_event(
    event_id: str,
    row: Dict[str, Any],
    channel_id: str
) -> Optional[Tuple]:
    """
    Create a playable entry for beIN event
    
    beIN doesn't expose direct stream URLs, but we can create implicit playables
    based on channel availability. The actual deeplink will need to be enhanced
    with beIN app URL patterns.
    
    Returns playable tuple or None
    """
    # Generate playable ID from event external ID + channel
    external_id = row.get("externalId", "")
    if not external_id:
        return None
    
    playable_id = f"{external_id}-{channel_id}" if channel_id else external_id
    
    # beIN deeplinks - using standard URL pattern
    # Format: https://www.beinsports.com/us/...
    deeplink_play = f"https://www.beinsports.com/us/live/{channel_id}"
    deeplink_open = f"https://www.beinsports.com/us/schedule"
    playable_url = deeplink_play
    
    # For mobile apps, we can add app deeplinks:
    # iOS: beinconnect://event/{externalId}
    # Android: bein://event/{externalId}
    http_deeplink_url = deeplink_play
    
    # Title from event
    title = row.get("title", "")
    
    # Content ID from data.eventId or matchId if available
    data = row.get("data", {})
    content_id = None
    if isinstance(data, dict):
        content_id = data.get("eventId") or data.get("matchid") or data.get("ID")
    if not content_id:
        content_id = external_id
    
    # Priority: beIN is regional/international sports, give medium-high priority
    # Per ADDING_NEW_STREAMING_SERVICE.md: Specialty service ~15-20
    priority = 18  # Same tier as Kayo
    
    return (
        event_id,           # event_id
        playable_id,        # playable_id
        "bein",             # provider (raw provider name)
        "beIN Sports",      # service_name
        "bein",             # logical_service (will map in logical_service_mapper.py)
        deeplink_play,      # deeplink_play
        deeplink_open,      # deeplink_open
        http_deeplink_url,  # http_deeplink_url
        playable_url,       # playable_url
        title,              # title
        str(content_id),    # content_id
        priority,           # priority
        utc_now_iso(),      # created_utc
        None,               # locale (could extract from data if needed)
        None                # espn_graph_id
    )



# --------------------------------------------------------------------
# Sport fallback images (OpenMoji 618x618 PNG via jsDelivr CDN)
# NOTE: OpenMoji graphics are CC BY-SA 4.0. Add attribution in README/about if distributing.
# --------------------------------------------------------------------
OPENMOJI_BASE = "https://cdn.jsdelivr.net/gh/hfg-gmuend/openmoji@16.0.0/color/618x618"

SPORT_OPENMOJI = {
    "Soccer": "26BD",              # ⚽
    "Tennis": "1F3BE",             # 🎾
    "Basketball": "1F3C0",         # 🏀
    "Hockey": "1F3D2",             # 🏒
    "Rugby": "1F3C9",              # 🏉
    "Handball": "1F93E",           # 🤾
    "Motorsports": "1F3CE",        # 🏎️
    "Combat Sports": "1F94A",      # 🥊
    "Equestrian": "1F3C7",         # 🏇
    "Cricket": "1F3CF",            # 🏏
    "Golf": "26F3",                # ⛳
    "Volleyball": "1F3D0",         # 🏐
    "Athletics": "1F3C3",          # 🏃
    "Baseball": "26BE",            # ⚾
    "American Football": "1F3C8",  # 🏈
    "Table Tennis": "1F3D3",       # 🏓
    "Darts": "1F3AF",              # 🎯
    "Lacrosse": "1F94D",           # 🥍

    # Approximations where Unicode doesn’t have a perfect sport icon:
    "Netball": "1F3D0",            # 🏐
    "Gridiron": "1F3C8",           # 🏈
    "Water Sports": "1F3CA",       # 🏊
    "Winter Sports": "26F7",       # ⛷️
    "Cycling": "1F6B4",            # 🚴
    "Olympic Sports": "1F3C5",     # 🏅

    # Generic fallback:
    "Other": "1F3DF",              # 🏟️
}

def sport_fallback_image_url(sport_type: str) -> str:
    st = (sport_type or "Other").strip()
    code = SPORT_OPENMOJI.get(st, SPORT_OPENMOJI["Other"])
    return f"{OPENMOJI_BASE}/{code}.png"


def get_bein_image_url(channel_data: Dict[str, Any], sport_type: str) -> str:
    """
    Get a hero image URL for a beIN event.

    Priority:
      1) Channel logo (HD/STD) *if it looks like a real image URL*
      2) Sport placeholder (stable)
      3) Generic beIN placeholder (stable)

    Why:
      - Unsplash images can disappear / rate-limit / 404 over time.
      - The beinsports.com DAM logo URL we used was returning 404 for you.
      - We want deterministic, low-risk icon URLs in the exported XML.
    """

    def _looks_like_image_url(u: Any) -> bool:
        if not u or not isinstance(u, str):
            return False
        u = u.strip()
        if not (u.startswith("http://") or u.startswith("https://")):
            return False

        # Known-bad / unstable sources we've already hit 404s on.
        if "images.unsplash.com" in u:
            return False
        if "www.beinsports.com/content/dam/" in u:
            return False

        return True

    # 1) Channel logos first (these are best when present)
    if channel_data and isinstance(channel_data, dict):
        logo_hd = channel_data.get("logoHD")
        if _looks_like_image_url(logo_hd):
            return logo_hd

        logo_std = channel_data.get("logoSTD")
        if _looks_like_image_url(logo_std):
            return logo_std

    # 2) Stable sport fallback (OpenMoji 618x618 PNG via jsDelivr)
    return sport_fallback_image_url(sport_type)


def normalize_event(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Tuple]]:
    """
    Normalize beIN event row into FruitDeepLinks event + playable
    
    CRITICAL: Follow ADDING_NEW_STREAMING_SERVICE.md schema exactly:
    - pvid MUST be set (M3U export silently skips without it)
    - genres_json not 'sport' or 'league' columns
    - Both start_ms/end_ms AND start_utc/end_utc required
    - channel_name holds league/competition
    
    Returns:
        (event_dict, playable_tuple or None)
    """
    # Use externalId as stable event ID (per technical summary)
    external_id = row.get("externalId", "")
    if not external_id:
        # Fallback to UUID if externalId missing
        external_id = row.get("id", "")
    
    # CRITICAL: Format as "{service}-{external_id}" per guide
    event_id = f"bein-{external_id}"
    
    # Extract time information
    start_utc = row.get("startDate")  # Already in ISO format: "2026-02-02T13:00:00.000Z"
    end_utc = row.get("endDate")
    
    # CRITICAL: Calculate millisecond timestamps (REQUIRED by schema)
    start_ms = parse_iso_timestamp(start_utc)
    end_ms = parse_iso_timestamp(end_utc)
    
    # Calculate runtime in seconds
    runtime_secs = None
    if start_ms and end_ms:
        runtime_secs = (end_ms - start_ms) // 1000
    elif row.get("duration"):
        # Duration is in milliseconds
        runtime_secs = row.get("duration") // 1000
    
    # Extract metadata
    title = row.get("title", "")
    synopsis = row.get("synopsis") or row.get("description") or ""
    sport_type, genre = extract_sport_info(row)
    channel_id, channel_name = extract_channel_info(row)
    
    # Get channel data for logo extraction
    channel_data = row.get("channel", {})
    
    # Build genres_json (CRITICAL: Use genres_json not 'sport' column!)
    # Per guide: JSON array, capitalized
    # SIMPLIFIED: Only store primary normalized sport to prevent filter pollution
    genres_list = []
    if sport_type:
        genres_list.append(sport_type)
    
    # Don't add category/genre as separate items - they cause filter clutter
    # The normalized sport_type already captures the essence
    
    # CRITICAL: channel_name holds league/competition (not 'league' column)
    # Simplified: Just use "beIN Sports" as the channel to avoid clutter
    # The sport type in genres_json already provides categorization
    display_channel_name = "beIN Sports"
    
    # Get category for classification_json
    category = row.get("category", "")
    
    # Build classification JSON
    classification_json = json.dumps({
        "sport_type": sport_type,
        "category": category,
        "genre": genre,
        "channel": channel_name
    })
    
    # Timestamps in milliseconds for created_ms
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Build event dictionary following ACTUAL schema
    event = {
        "id": event_id,
        "pvid": external_id,  # CRITICAL: Must be set for M3U export!
        "slug": external_id.lower().replace(" ", "-"),
        "title": title,
        "title_brief": title[:100] if title else None,
        "synopsis": synopsis,
        "synopsis_brief": synopsis[:200] if synopsis else None,
        "channel_name": display_channel_name,  # League/competition name
        "channel_provider_id": "bein",  # CRITICAL: Raw provider name
        "airing_type": "live_event",
        "classification_json": classification_json,
        "genres_json": json.dumps(genres_list),  # CRITICAL: JSON array format
        "content_segments_json": None,
        "is_free": 0,
        "is_premium": 1,  # beIN requires subscription
        "runtime_secs": runtime_secs,
        "start_ms": start_ms,  # CRITICAL: Required by schema
        "end_ms": end_ms,      # CRITICAL: Required by schema
        "start_utc": start_utc,  # CRITICAL: Required by schema
        "end_utc": end_utc,      # CRITICAL: Required by schema
        "created_ms": now_ms,
        "created_utc": utc_now_iso(),
        "hero_image_url": get_bein_image_url(channel_data, sport_type),  # GUARANTEED image
        "last_seen_utc": utc_now_iso(),
        "raw_attributes_json": json.dumps(row)  # Store full original response
    }
    
    # Create playable
    playable = create_playable_for_event(event_id, row, channel_id)
    
    return event, playable


def upsert_event(conn: sqlite3.Connection, event: Dict[str, Any], dry_run: bool = False):
    """Insert or update event in database"""
    if dry_run:
        LOG.info(f"[DRY] Event: {event['id']} - {event['title']}")
        return
    
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO events (
            id, pvid, slug, title, title_brief, synopsis, synopsis_brief,
            channel_name, channel_provider_id, airing_type,
            classification_json, genres_json, content_segments_json,
            is_free, is_premium, runtime_secs,
            start_ms, end_ms, start_utc, end_utc,
            created_ms, created_utc, hero_image_url, last_seen_utc, raw_attributes_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event["id"], event["pvid"], event["slug"], event["title"], event["title_brief"],
        event["synopsis"], event["synopsis_brief"], event["channel_name"],
        event["channel_provider_id"], event["airing_type"], event["classification_json"],
        event["genres_json"], event["content_segments_json"], event["is_free"],
        event["is_premium"], event["runtime_secs"], event["start_ms"], event["end_ms"],
        event["start_utc"], event["end_utc"], event["created_ms"], event["created_utc"],
        event["hero_image_url"], event["last_seen_utc"], event["raw_attributes_json"]
    ))


def upsert_playable(conn: sqlite3.Connection, playable: Tuple, dry_run: bool = False):
    """Insert or update playable in database"""
    if dry_run:
        LOG.info(f"[DRY] Playable: {playable[1]} - {playable[4]}")
        return
    
    cur = conn.cursor()
    
    # Delete existing playable for this event+provider (refresh)
    cur.execute("""
        DELETE FROM playables 
        WHERE event_id = ? AND provider = ?
    """, (playable[0], playable[2]))
    
    # Insert new playable
    cur.execute("""
        INSERT INTO playables (
            event_id, playable_id, provider, service_name, logical_service,
            deeplink_play, deeplink_open, http_deeplink_url, playable_url,
            title, content_id, priority, created_utc, locale, espn_graph_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, playable)


def import_bein_snapshot(
    bein_json_path: Path,
    fruit_db_path: Path,
    dry_run: bool = False
) -> int:
    """
    Import beIN snapshot into FruitDeepLinks database
    
    Returns count of imported events
    """
    # Load beIN snapshot
    LOG.info(f"Loading beIN snapshot from {bein_json_path}")
    with open(bein_json_path, 'r', encoding='utf-8') as f:
        snapshot = json.load(f)
    
    rows = snapshot.get("rows", [])
    LOG.info(f"Found {len(rows)} total beIN events")
    
    # Connect to database
    conn = sqlite3.connect(str(fruit_db_path))
    ensure_schema(conn)
    
    imported_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        for row in rows:
            try:
                # Check if event should be imported
                if not should_import_event(row):
                    skipped_count += 1
                    continue
                
                # Normalize event and playable
                event, playable = normalize_event(row)
                
                # Validate required fields per ADDING_NEW_STREAMING_SERVICE.md
                if not event.get("pvid"):
                    LOG.error(f"Missing pvid for event {event['id']} - will be skipped by M3U export!")
                    error_count += 1
                    continue
                
                if not event.get("end_utc"):
                    LOG.error(f"Missing end_utc for event {event['id']} - required by schema!")
                    error_count += 1
                    continue
                
                # Import to database
                upsert_event(conn, event, dry_run=dry_run)
                if playable:
                    upsert_playable(conn, playable, dry_run=dry_run)
                
                imported_count += 1
                
                # Log progress every 100 events
                if imported_count % 100 == 0:
                    LOG.info(f"Imported {imported_count} events...")
                
            except Exception as e:
                error_count += 1
                LOG.error(f"Error processing event: {e}")
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug(f"Problematic row: {json.dumps(row, indent=2)}")
        
        if not dry_run:
            conn.commit()
        
        LOG.info(f"Import complete: {imported_count} imported, {skipped_count} skipped, {error_count} errors")
        return imported_count
        
    except Exception as e:
        LOG.error(f"Import failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Import beIN Sports EPG into FruitDeepLinks")
    parser.add_argument("--bein-json", required=True, help="Path to beIN snapshot JSON")
    parser.add_argument("--fruit-db", required=True, help="Path to fruit_events.db")
    parser.add_argument("--dry-run", action="store_true", help="Preview import without database changes")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    bein_json_path = Path(args.bein_json)
    fruit_db_path = Path(args.fruit_db)
    
    # Validate inputs
    if not bein_json_path.exists():
        LOG.error(f"beIN snapshot not found: {bein_json_path}")
        return 1
    
    if not args.dry_run and not fruit_db_path.parent.exists():
        LOG.error(f"Database directory not found: {fruit_db_path.parent}")
        return 1
    
    try:
        count = import_bein_snapshot(bein_json_path, fruit_db_path, dry_run=args.dry_run)
        LOG.info(f"Successfully imported {count} beIN events")
        return 0
    except Exception as e:
        LOG.error(f"Import failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
