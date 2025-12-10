# bin/fruit_import_appletv.py
#!/usr/bin/env python3
"""
fruit_import_appletv.py - Import Apple TV Sports events into the FruitDeepLinks DB (idempotent)

Usage:
  python fruit_import_appletv.py --apple-json parsed_events.json --fruit-db fruit_events.db
  python fruit_import_appletv.py --apple-json parsed_events.json --fruit-db fruit_events.db --dry-run
"""
import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Provider channel number "namespaces" (kept for potential future use)
PROVIDER_CHANNEL_RANGES = {
    "peacock": 9000, "espn-plus": 1000, "prime-video": 2000, "apple-tv-plus": 3000,
    "paramount-plus": 4000, "max": 5000, "dazn": 6000, "cbs-sports": 7000,
    "fox-sports": 8000, "nbc-sports": 8100, "fubo": 8200, "mlb-tv": 8300, "nba-league-pass": 8400,
}

def normalize_provider(channel_name: Optional[str]) -> str:
    if not channel_name:
        return "other"
    s = channel_name.lower().replace(" ", "-")
    if "espn" in s: return "espn-plus"
    if "prime" in s or "amazon" in s: return "prime-video"
    if "apple-tv" in s or "appletv" in s: return "apple-tv-plus"
    if "paramount" in s: return "paramount-plus"
    if "hbo" in s or s == "max": return "max"
    if "dazn" in s: return "dazn"
    if "cbs" in s: return "cbs-sports"
    if "fox" in s: return "fox-sports"
    if "nbc" in s: return "nbc-sports"
    if "fubo" in s: return "fubo"
    if "mlb" in s: return "mlb-tv"
    if "nba" in s and "league" in s: return "nba-league-pass"
    return "other"

def iso_to_ms(iso_str: Optional[str]) -> Optional[int]:
    if not iso_str: return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def ms_to_iso(ts_ms: Optional[int]) -> Optional[str]:
    if ts_ms is None: return None
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(timespec="seconds")

PREFERRED_WIDTH = 1280
PREFERRED_HEIGHT = 720
PREFERRED_FMT = "jpg"

def concretize_apple_image(url_template: Optional[str],
                           width: int = PREFERRED_WIDTH,
                           height: int = PREFERRED_HEIGHT,
                           fmt: str = PREFERRED_FMT) -> Optional[str]:
    """Fill Apple image {w}x{h} / {f} templates with concrete values."""
    if not url_template or not isinstance(url_template, str):
        return None
    url = url_template.replace("{w}x{h}", f"{width}x{height}")
    url = url.replace("{f}", fmt)
    return url

def select_best_apple_image(images: Dict[str, Any]) -> Optional[str]:
    """Pick a hero image for Apple Sports events.

    Preference:
      1) Versus-style 'gen/...Sports.TVAPo...' (usually shelfItemImagePost)
      2) Live tile (shelfItemImageLive)
      3) Logo fallback (shelfImageLogo)
    """
    if not images:
        return None

    # 1) Versus-style template
    post = images.get("shelfItemImagePost")
    if isinstance(post, dict):
        url = post.get("url")
        if isinstance(url, str) and "Sports.TVAPo" in url:
            concrete = concretize_apple_image(url)
            if concrete:
                return concrete

    # 2) Live tile
    live = images.get("shelfItemImageLive")
    if isinstance(live, dict):
        url = live.get("url")
        if isinstance(url, str):
            concrete = concretize_apple_image(url)
            if concrete:
                return concrete

    # 3) Logo fallback
    logo = images.get("shelfImageLogo")
    if isinstance(logo, dict):
        url = logo.get("url")
        if isinstance(url, str):
            concrete = concretize_apple_image(url)
            if concrete:
                return concrete

    return None

def calculate_runtime(start_ms: Optional[int], end_ms: Optional[int]) -> Optional[int]:
    """Compute runtime in seconds from millisecond timestamps with basic sanity checks."""
    if start_ms is None or end_ms is None:
        return None
    dur = int((end_ms - start_ms) / 1000)
    if dur <= 0:
        return None
    # Guardrail: treat anything over 12 hours as suspicious and skip
    if dur > 12 * 60 * 60:
        return None
    return dur

def extract_competitors(apple_event: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    competitors = apple_event.get("competitors", [])
    if len(competitors) >= 2:
        return competitors[0].get("name"), competitors[1].get("name")
    elif len(competitors) == 1:
        return competitors[0].get("name"), None
    return None, None

def normalize_event_structure(apple_event: Dict[str, Any]) -> Dict[str, Any]:
    """Convert multi_scraped.json structure to flat structure for compatibility
    
    Input: {id, raw_data: {data: {content: {...}}}}
    Output: {id, title, sport_name, league_name, ...} (flat)
    """
    # If already flat (old format), return as-is
    if "raw_data" not in apple_event:
        return apple_event
    
    # Extract from multi_scraped.json structure
    event_id = apple_event.get("id", "")
    raw_data = apple_event.get("raw_data", {})
    data = raw_data.get("data", {})
    content = data.get("content", {})
    
    # Channels might be dict or list - normalize to list
    channels_data = data.get("channels", {})
    if isinstance(channels_data, dict):
        channels = list(channels_data.values())
    else:
        channels = channels_data if channels_data else []
    
    # CRITICAL FIX: Playables can be at BOTH data level AND content level
    # They can be either dict OR list format - handle both!
    data_playables = data.get("playables", {}) or {}
    content_playables = content.get("playables", {}) or {}
    
    # Merge playables - handle both dict and list formats
    merged_playables = {}
    
    # Handle content playables first
    if isinstance(content_playables, dict):
        merged_playables.update(content_playables)
    elif isinstance(content_playables, list):
        # Convert list to dict using playable id as key
        for p in content_playables:
            if isinstance(p, dict) and 'id' in p:
                merged_playables[p['id']] = p
    
    # Handle data playables (takes precedence)
    if isinstance(data_playables, dict):
        merged_playables.update(data_playables)
    elif isinstance(data_playables, list):
        # Convert list to dict using playable id as key
        for p in data_playables:
            if isinstance(p, dict) and 'id' in p:
                merged_playables[p['id']] = p
    
    # Keep as dict for consistency with extract_playables()
    playables_final = merged_playables if merged_playables else {}
    
    # Flatten the structure

    # --- Time handling -----------------------------------------------------
    event_time = content.get("eventTime", {}) or {}
    tune_in = event_time.get("tuneInTime") or {}
    live_badge = event_time.get("liveBadgeTime") or {}

    ti_start = tune_in.get("startTime") if isinstance(tune_in, dict) else None
    ti_end = tune_in.get("endTime") if isinstance(tune_in, dict) else None
    lb_start = live_badge.get("startTime") if isinstance(live_badge, dict) else None
    lb_end = live_badge.get("endTime") if isinstance(live_badge, dict) else None

    kickoff = event_time.get("gameKickOffStartTime")
    start_ms = ti_start or lb_start or kickoff
    end_ms = ti_end or lb_end or None

    return {
        "id": event_id,
        "title": content.get("title") or content.get("shortTitle"),
        "sport_name": content.get("sportName"),
        "league_name": content.get("leagueName"),
        "competitors": content.get("competitors", []),
        "channels": channels,
        "playables": playables_final,
        "images": content.get("images", {}),
        "url": content.get("url"),
        # Preserve original kickoff as start_time for reference
        "start_time": kickoff,
        # Use normalized window for ms fields that feed the DB
        "start_time_ms": start_ms,
        "end_time": ti_end or lb_end,
        "end_time_ms": end_ms,
    }

def build_title(apple_event: Dict[str, Any]) -> str:
    title = apple_event.get("title") or ""
    league = apple_event.get("league_name") or apple_event.get("league") or ""
    if not title or title == "Sports Event":
        a, b = extract_competitors(apple_event)
        title = f"{a} vs {b}" if a and b else (a or title)
    if league and league not in title:
        title = f"{league}: {title}" if title else league
    return title or "Apple TV Sports Event"

def build_synopsis(apple_event: Dict[str, Any]) -> Optional[str]:
    parts: List[str] = []
    sport = apple_event.get("sport_name") or apple_event.get("sport")
    league = apple_event.get("league_name") or apple_event.get("league")
    a, b = extract_competitors(apple_event)
    if sport: parts.append(sport)
    if league and league != sport: parts.append(f"({league})")
    if a and b: parts.append(f"{a} vs {b}")
    channels = apple_event.get("channels", [])
    ch_name = channels[0].get("name") if channels else None
    if ch_name: parts.append(f"Available on {ch_name}")
    return " - ".join(parts) if parts else None

def map_apple_to_fruit(apple_event: Dict[str, Any], provider_prefix: str = "appletv") -> Dict[str, Any]:
    # Normalize structure (handles both old flat format and new multi_scraped.json format)
    event = normalize_event_structure(apple_event)
    
    apple_id = event.get("id", "")
    event_id = f"{provider_prefix}-{apple_id}"
    # Times
    start_ms = event.get("start_time_ms") or iso_to_ms(event.get("start_time"))
    end_ms = event.get("end_time_ms") or iso_to_ms(event.get("end_time"))
    runtime_secs = calculate_runtime(start_ms, end_ms)
    start_utc = ms_to_iso(start_ms); end_utc = ms_to_iso(end_ms)
    # Titles
    title = build_title(event); title_brief = event.get("title") or title
    synopsis = build_synopsis(event); synopsis_brief = synopsis
    # Channel/provider
    channels = event.get("channels", [])
    channel_name = channels[0].get("name") if channels else None
    provider_normalized = normalize_provider(channel_name)
    images_struct = event.get("images") or {}
    hero_image_url = select_best_apple_image(images_struct)
    # Genres/Classification
    sport = event.get("sport_name"); league = event.get("league_name")
    # genres should only contain sports, not leagues (leagues go in classification)
    genres = [sport] if sport else []
    classification = []
    if sport: classification.append({"type": "sport", "value": sport})
    if league: classification.append({"type": "league", "value": league})
    return {
        "id": event_id,
        "pvid": apple_id,
        "slug": None,
        "title": title,
        "title_brief": title_brief,
        "synopsis": synopsis,
        "synopsis_brief": synopsis_brief,
        "channel_name": channel_name,
        "channel_provider_id": provider_normalized,
        "hero_image_url": hero_image_url,
        "airing_type": "live",
        "classification_json": json.dumps(classification),
        "genres_json": json.dumps(genres),
        "content_segments_json": json.dumps([]),
        "is_free": 0,
        "is_premium": 1,
        "runtime_secs": runtime_secs,
        "start_ms": start_ms, "end_ms": end_ms,
        "start_utc": start_utc, "end_utc": end_utc,
        "created_ms": None, "created_utc": None,
        "last_seen_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "raw_attributes_json": json.dumps({
            "images": event.get("images", {}),
            "competitors": event.get("competitors", []),
            "channels": channels,
            "playables": event.get("playables", {}),
            "sport_name": sport, "league_name": league,
            "apple_tv_url": event.get("url"),
        }),
    }

def extract_images(apple_event: Dict[str, Any], event_id: str) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    images = apple_event.get("images", {}) or {}
    type_map = {"showTile2x1": "landscape", "showTile2x3": "portrait",
                "showTile1x1": "square", "showTile16x9": "scene169",
                "cover_art": "scene169", "preview": "scene169"}
    for k, img_type in type_map.items():
        url = images.get(k)
        if url: out.append((event_id, img_type, url))
    for i, comp in enumerate(apple_event.get("competitors", []), 1):
        logo = comp.get("logo_url") or comp.get("logo")
        if logo: out.append((event_id, f"team_{i}_logo", logo))
    return out

def load_apple_events(json_path: str) -> List[Dict[str, Any]]:
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    if isinstance(data, list): return data
    if isinstance(data, dict) and "events" in data: return data["events"]
    raise ValueError(f"Unexpected JSON format in {json_path}")

def ensure_events_schema(conn: sqlite3.Connection):
    # Expect tables from ingester; ensure they exist + basic indexes for speed.
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY, pvid TEXT, slug TEXT, title TEXT, title_brief TEXT,
        synopsis TEXT, synopsis_brief TEXT, channel_name TEXT, channel_provider_id TEXT,
        airing_type TEXT, classification_json TEXT, genres_json TEXT, content_segments_json TEXT,
        is_free INTEGER, is_premium INTEGER, runtime_secs INTEGER, start_ms INTEGER, end_ms INTEGER,
        start_utc TEXT, end_utc TEXT, created_ms INTEGER, created_utc TEXT,
        hero_image_url TEXT,
        last_seen_utc TEXT, raw_attributes_json TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS event_images (
        event_id TEXT, img_type TEXT, url TEXT, PRIMARY KEY (event_id, img_type, url))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_pvid ON events(pvid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(start_utc, end_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_event_images_event ON event_images(event_id)")

    # Ensure hero_image_url column exists on existing databases
    cur.execute("PRAGMA table_info(events)")
    cols = [row[1] for row in cur.fetchall()]
    if "hero_image_url" not in cols:
        cur.execute("ALTER TABLE events ADD COLUMN hero_image_url TEXT")
        conn.commit()

    conn.commit()

def upsert_event(conn: sqlite3.Connection, event: Dict[str, Any], dry: bool = False):
    if dry:
        print(f"[DRY] event {event['id']} :: {event['title']}")
        return
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO events (
            id, pvid, slug, title, title_brief, synopsis, synopsis_brief,
            channel_name, channel_provider_id, airing_type,
            classification_json, genres_json, content_segments_json,
            is_free, is_premium, runtime_secs,
            start_ms, end_ms, start_utc, end_utc,
            created_ms, created_utc, hero_image_url, last_seen_utc, raw_attributes_json
        ) VALUES (
            :id, :pvid, :slug, :title, :title_brief, :synopsis, :synopsis_brief,
            :channel_name, :channel_provider_id, :airing_type,
            :classification_json, :genres_json, :content_segments_json,
            :is_free, :is_premium, :runtime_secs,
            :start_ms, :end_ms, :start_utc, :end_utc,
            :created_ms, :created_utc, :hero_image_url, :last_seen_utc, :raw_attributes_json
        )
        ON CONFLICT(id) DO UPDATE SET
            pvid=excluded.pvid, slug=excluded.slug, title=excluded.title,
            title_brief=excluded.title_brief, synopsis=excluded.synopsis, synopsis_brief=excluded.synopsis_brief,
            channel_name=excluded.channel_name, channel_provider_id=excluded.channel_provider_id,
            airing_type=excluded.airing_type, classification_json=excluded.classification_json,
            genres_json=excluded.genres_json, content_segments_json=excluded.content_segments_json,
            is_free=excluded.is_free, is_premium=excluded.is_premium, runtime_secs=excluded.runtime_secs,
            start_ms=excluded.start_ms, end_ms=excluded.end_ms, start_utc=excluded.start_utc, end_utc=excluded.end_utc,
            hero_image_url=excluded.hero_image_url,
            last_seen_utc=excluded.last_seen_utc, raw_attributes_json=excluded.raw_attributes_json
        """,
        event,
    )

def upsert_images(conn: sqlite3.Connection, images: List[Tuple[str, str, str]], dry: bool = False):
    if not images: return
    if dry:
        print(f"[DRY] images x{len(images)}")
        return
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO event_images (event_id, img_type, url) VALUES (?, ?, ?)",
        images,
    )

def extract_playables(apple_event: Dict, event_id: str) -> List[Tuple]:
    """Extract playables from Apple event for multi-punchout support
    
    Handles normalized structure (flat dict with playables key)
    Supports both dict and list formats for playables
    """
    # Get playables from normalized event structure
    playables_data = apple_event.get("playables", {})
    
    if not playables_data:
        return []
    
    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc).isoformat()
    
    # Try to import logical service mapper for priority calculation
    try:
        from logical_service_mapper import (
            get_logical_service_for_playable,
            get_logical_service_priority
        )
        LOGICAL_SERVICES_AVAILABLE = True
    except ImportError:
        LOGICAL_SERVICES_AVAILABLE = False
    
    # Handle dict format (most common after normalization)
    if isinstance(playables_data, dict):
        playables_list = list(playables_data.values())
    else:
        # Handle list format (fallback)
        playables_list = playables_data
    
    result = []
    for playable in playables_list:
        if not isinstance(playable, dict):
            continue
            
        playable_id = playable.get("id", "")
        if not playable_id:
            continue
        
        # Extract deeplinks - use INDIVIDUAL playable's punchoutUrls
        punchout = playable.get("punchoutUrls", {})
        deeplink_play = punchout.get("play") or playable.get("deeplink_play")
        deeplink_open = punchout.get("open") or playable.get("deeplink_open")
        playable_url = playable.get("playable_url") or playable.get("url")
        
        # Determine provider from URL scheme
        provider = None
        url = deeplink_play or deeplink_open or playable_url or ""
        if url and "://" in url:
            provider = url.split("://")[0]
        
        title = playable.get("displayName") or playable.get("title") or playable.get("name")
        content_id = playable.get("content_id") or playable.get("contentId")
        
        # Calculate priority using logical service mapper if available
        priority = 0  # Default fallback
        if LOGICAL_SERVICES_AVAILABLE:
            try:
                # Note: We can't pass conn here since we don't have it in this function
                # For Apple TV league detection, that will happen during backfill or
                # can be enhanced later. For now, we get the provider-based priority.
                logical_service = get_logical_service_for_playable(
                    provider=provider,
                    deeplink_play=deeplink_play,
                    deeplink_open=deeplink_open,
                    playable_url=playable_url,
                    event_id=event_id,
                    conn=None  # Will use basic URL-based detection
                )
                priority = get_logical_service_priority(logical_service)
            except Exception as e:
                # Fallback to 0 if priority calculation fails
                priority = 0
        
        result.append((
            event_id,
            playable_id,
            provider,
            deeplink_play,
            deeplink_open,
            playable_url,
            title,
            content_id,
            priority,  # Now using calculated priority
            now_utc
        ))
    
    return result

def upsert_playables(conn: sqlite3.Connection, playables: List[Tuple], dry: bool = False):
    """Insert or update playables for an event"""
    if not playables:
        return
    
    if dry:
        print(f"[DRY] playables x{len(playables)}")
        return
    
    cur = conn.cursor()
    
    # Check if playables table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playables'")
    if not cur.fetchone():
        # Silently skip if table doesn't exist yet
        return
    
    # Delete existing playables for this event (refresh)
    if playables:
        event_id = playables[0][0]
        cur.execute("DELETE FROM playables WHERE event_id = ?", (event_id,))
    
    # Insert new playables
    cur.executemany("""
        INSERT INTO playables (
            event_id, playable_id, provider, deeplink_play, deeplink_open,
            playable_url, title, content_id, priority, created_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, playables)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apple-json", required=True, help="parsed_events.json from multi_scraper.py")
    ap.add_argument("--fruit-db", help="SQLite DB path for FruitDeepLinks events (recommended)")
    ap.add_argument("--peacock-db", help="DEPRECATED: legacy SQLite DB path; use --fruit-db instead")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db_path_str = args.fruit_db or args.peacock_db
    if not db_path_str:
        ap.error("You must provide --fruit-db (preferred) or --peacock-db")
    db_path = Path(db_path_str)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    events = load_apple_events(args.apple_json)
    print(f"Loaded {len(events)} Apple TV events")

    conn = sqlite3.connect(str(db_path))
    ensure_events_schema(conn)

    inserted = 0
    playables_extracted = 0
    
    for e in events:
        # Normalize ONCE - use for everything
        normalized = normalize_event_structure(e)
        
        # Map to peacock schema (will normalize again internally, but that's ok)
        mapped = map_apple_to_fruit(e, provider_prefix="appletv")
        upsert_event(conn, mapped, dry=args.dry_run)
        
        # Extract images from normalized event
        imgs = extract_images(normalized, mapped["id"])
        upsert_images(conn, imgs, dry=args.dry_run)
        
        # Extract playables from normalized event
        playables = extract_playables(normalized, mapped["id"])
        if playables:
            playables_extracted += len(playables)
        upsert_playables(conn, playables, dry=args.dry_run)
        
        inserted += 1

    if not args.dry_run:
        conn.commit()
    conn.close()
    
    print(f"âœ… Imported/updated {inserted} events into {db_path}")
    print(f"âœ… Extracted {playables_extracted} playables total")

if __name__ == "__main__":
    main()
