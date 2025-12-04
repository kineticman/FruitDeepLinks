#!/usr/bin/env python3
"""
appletv_to_peacock.py - Import Apple TV Sports events into Peacock DB (idempotent)
NOW WITH MULTI-PUNCHOUT SUPPORT - Stores all playables per event

Usage:
  python appletv_to_peacock.py --apple-json parsed_events.json --peacock-db peacock_events.db
  python appletv_to_peacock.py --apple-json parsed_events.json --peacock-db peacock_events.db --dry-run
"""
import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import provider utilities
try:
    from provider_utils import extract_provider_from_url
except ImportError:
    # Fallback if provider_utils not available
    def extract_provider_from_url(url: str) -> str:
        if not url or '://' not in url:
            return 'unknown'
        return url.split('://')[0]

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

def calculate_runtime(start_ms: Optional[int], end_ms: Optional[int]) -> Optional[int]:
    if start_ms and end_ms: return int((end_ms - start_ms) / 1000)
    return None

def extract_competitors(apple_event: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    competitors = apple_event.get("competitors", [])
    if len(competitors) >= 2:
        return competitors[0].get("name"), competitors[1].get("name")
    elif len(competitors) == 1:
        return competitors[0].get("name"), None
    return None, None

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

def map_apple_to_peacock(apple_event: Dict[str, Any], provider_prefix: str = "appletv") -> Dict[str, Any]:
    apple_id = apple_event.get("id", "")
    event_id = f"{provider_prefix}-{apple_id}"
    # Times
    start_ms = apple_event.get("start_time_ms") or iso_to_ms(apple_event.get("start_time"))
    end_ms = apple_event.get("end_time_ms") or iso_to_ms(apple_event.get("end_time"))
    runtime_secs = calculate_runtime(start_ms, end_ms)
    start_utc = ms_to_iso(start_ms); end_utc = ms_to_iso(end_ms)
    # Titles
    title = build_title(apple_event); title_brief = apple_event.get("title") or title
    synopsis = build_synopsis(apple_event); synopsis_brief = synopsis
    # Channel/provider
    channels = apple_event.get("channels", [])
    channel_name = channels[0].get("name") if channels else None
    provider_normalized = normalize_provider(channel_name)
    # Genres/Classification
    sport = apple_event.get("sport_name"); league = apple_event.get("league_name")
    genres = [g for g in [sport, league] if g]
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
            "images": apple_event.get("images", {}),
            "competitors": apple_event.get("competitors", []),
            "channels": channels,
            "playables": apple_event.get("playables", []),
            "sport_name": sport, "league_name": league,
            "apple_tv_url": apple_event.get("url"),
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

def extract_playables(apple_event: Dict[str, Any], event_id: str) -> List[Dict[str, Any]]:
    """
    Extract ALL playables from Apple TV event for multi-punchout support
    
    Returns list of playables with provider extracted from deeplink URLs
    """
    playables_raw = apple_event.get("playables", [])
    if not playables_raw:
        return []
    
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    result = []
    
    for playable in playables_raw:
        playable_id = playable.get("id", "")
        if not playable_id:
            continue
        
        deeplink_play = playable.get("deeplink_play", "")
        deeplink_open = playable.get("deeplink_open", "")
        playable_url = playable.get("playable_url", "")
        
        # Skip if no deeplinks at all
        if not (deeplink_play or deeplink_open or playable_url):
            continue
        
        # Extract provider from primary deeplink
        provider = extract_provider_from_url(deeplink_play or deeplink_open or playable_url)
        
        result.append({
            "event_id": event_id,
            "playable_id": playable_id,
            "provider": provider,
            "deeplink_play": deeplink_play or None,
            "deeplink_open": deeplink_open or None,
            "playable_url": playable_url or None,
            "title": playable.get("title", ""),
            "content_id": playable.get("content_id", ""),
            "priority": 0,  # Default priority, user can customize later
            "created_utc": now,
        })
    
    return result

def load_apple_events(json_path: str) -> List[Dict[str, Any]]:
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    if isinstance(data, list): return data
    if isinstance(data, dict) and "events" in data: return data["events"]
    raise ValueError(f"Unexpected JSON format in {json_path}")

def ensure_peacock_schema(conn: sqlite3.Connection):
    # Expect tables from ingester; ensure they exist + basic indexes for speed.
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY, pvid TEXT, slug TEXT, title TEXT, title_brief TEXT,
        synopsis TEXT, synopsis_brief TEXT, channel_name TEXT, channel_provider_id TEXT,
        airing_type TEXT, classification_json TEXT, genres_json TEXT, content_segments_json TEXT,
        is_free INTEGER, is_premium INTEGER, runtime_secs INTEGER, start_ms INTEGER, end_ms INTEGER,
        start_utc TEXT, end_utc TEXT, created_ms INTEGER, created_utc TEXT,
        last_seen_utc TEXT, raw_attributes_json TEXT)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS event_images (
        event_id TEXT, img_type TEXT, url TEXT, PRIMARY KEY (event_id, img_type, url))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_pvid ON events(pvid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(start_utc, end_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_event_images_event ON event_images(event_id)")
    
    # Check if playables table exists (from migration)
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playables'")
    has_playables_table = cur.fetchone() is not None
    
    conn.commit()
    return has_playables_table

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
            created_ms, created_utc, last_seen_utc, raw_attributes_json
        ) VALUES (
            :id, :pvid, :slug, :title, :title_brief, :synopsis, :synopsis_brief,
            :channel_name, :channel_provider_id, :airing_type,
            :classification_json, :genres_json, :content_segments_json,
            :is_free, :is_premium, :runtime_secs,
            :start_ms, :end_ms, :start_utc, :end_utc,
            :created_ms, :created_utc, :last_seen_utc, :raw_attributes_json
        )
        ON CONFLICT(id) DO UPDATE SET
            pvid=excluded.pvid, slug=excluded.slug, title=excluded.title,
            title_brief=excluded.title_brief, synopsis=excluded.synopsis, synopsis_brief=excluded.synopsis_brief,
            channel_name=excluded.channel_name, channel_provider_id=excluded.channel_provider_id,
            airing_type=excluded.airing_type, classification_json=excluded.classification_json,
            genres_json=excluded.genres_json, content_segments_json=excluded.content_segments_json,
            is_free=excluded.is_free, is_premium=excluded.is_premium, runtime_secs=excluded.runtime_secs,
            start_ms=excluded.start_ms, end_ms=excluded.end_ms, start_utc=excluded.start_utc, end_utc=excluded.end_utc,
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

def upsert_playables(conn: sqlite3.Connection, playables: List[Dict[str, Any]], dry: bool = False):
    """Insert/update playables for multi-punchout support"""
    if not playables:
        return
    
    if dry:
        print(f"[DRY] playables x{len(playables)}")
        for p in playables[:3]:  # Show first 3 as sample
            print(f"  - {p['provider']}: {p.get('deeplink_play', '')[:50]}")
        return
    
    cur = conn.cursor()
    
    # Delete old playables for this event first (to handle removed playables)
    if playables:
        event_id = playables[0]["event_id"]
        cur.execute("DELETE FROM playables WHERE event_id = ?", (event_id,))
    
    # Insert all playables
    for p in playables:
        cur.execute(
            """
            INSERT INTO playables (
                event_id, playable_id, provider, 
                deeplink_play, deeplink_open, playable_url,
                title, content_id, priority, created_utc
            ) VALUES (
                :event_id, :playable_id, :provider,
                :deeplink_play, :deeplink_open, :playable_url,
                :title, :content_id, :priority, :created_utc
            )
            """,
            p
        )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apple-json", required=True, help="parsed_events.json from parse_events.py")
    ap.add_argument("--peacock-db", required=True, help="SQLite DB path (from peacock_ingest_atom.py)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db_path = Path(args.peacock_db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    events = load_apple_events(args.apple_json)  # parsed format from parse_events.py
    print(f"Loaded {len(events)} Apple TV events")

    conn = sqlite3.connect(str(db_path))
    has_playables_table = ensure_peacock_schema(conn)
    
    if has_playables_table:
        print("✓ Playables table detected - multi-punchout support enabled")
    else:
        print("⚠ Playables table not found - run migrate_add_playables.py first")
        print("  Continuing with basic import (no multi-punchout)")

    inserted = 0
    playables_inserted = 0
    
    for e in events:
        mapped = map_apple_to_peacock(e, provider_prefix="appletv")
        upsert_event(conn, mapped, dry=args.dry_run)
        
        imgs = extract_images(e, mapped["id"])
        upsert_images(conn, imgs, dry=args.dry_run)
        
        # NEW: Store all playables if table exists
        if has_playables_table:
            playables = extract_playables(e, mapped["id"])
            if playables:
                upsert_playables(conn, playables, dry=args.dry_run)
                playables_inserted += len(playables)
        
        inserted += 1

    if not args.dry_run:
        conn.commit()
    conn.close()
    
    print(f"✓ Imported/updated {inserted} events into {db_path}")
    if has_playables_table:
        print(f"✓ Stored {playables_inserted} playables ({playables_inserted/inserted:.1f} per event avg)")

if __name__ == "__main__":
    main()
