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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Provider channel number "namespaces" (kept for potential future use)
PROVIDER_CHANNEL_RANGES = {
    "peacock": 9000, "espn-plus": 1000, "prime-video": 2000, "apple-tv-plus": 3000,
    "paramount-plus": 4000, "max": 5000, "dazn": 6000, "cbs-sports": 7000,
    "fox-sports": 8000, "nbc-sports": 8100, "fubo": 8200, "mlb-tv": 8300, "nba-league-pass": 8400,
}

PROGRESS_EVERY = 250


def _log(msg: str) -> None:
    print(msg, flush=True)


_SCHEMA_ENSURED = False


def count_apple_events_in_db(db_path: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        total = cur.execute("SELECT COUNT(*) FROM apple_events").fetchone()[0]
        conn.close()
        return int(total or 0)
    except Exception:
        return 0


def iter_apple_events_from_db(db_path: str):
    """Stream events from apple_events.db (GZIP decompress + JSON parse) without materializing a huge list.

    This keeps memory stable and avoids GC / swapping slowdowns on long runs.
    """
    import gzip

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        for event_id, source, compressed in cur.execute(
            "SELECT event_id, source, raw_json_gzip FROM apple_events"
        ):
            try:
                json_str = gzip.decompress(compressed).decode("utf-8")
                raw_data = json.loads(json_str)
            except Exception as e:
                _log(f"Warning: Failed to decompress/parse event {event_id}: {e}")
                continue

            yield {
                "id": event_id,
                "status": 200,
                "raw_data": raw_data,
                "source": source,
            }
    finally:
        try:
            conn.close()
        except Exception:
            pass

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

def load_apple_events_from_db(db_path: str) -> List[Dict[str, Any]]:
    """Load events from apple_events.db (with GZIP decompression)

    This can be slow (GZIP + JSON parse), so we log periodic progress.
    """
    import gzip

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        total = cur.execute("SELECT COUNT(*) FROM apple_events").fetchone()[0]
    except Exception:
        total = 0

    _log(f"Loading Apple events from DB (rows={total}) ...")

    events: List[Dict[str, Any]] = []
    t0 = time.perf_counter()

    # Stream rows (avoid fetchall) so we can report progress as we go.
    for event_id, source, compressed in cur.execute(
        "SELECT event_id, source, raw_json_gzip FROM apple_events"
    ):
        try:
            json_str = gzip.decompress(compressed).decode("utf-8")
            raw_data = json.loads(json_str)

            events.append(
                {
                    "id": event_id,
                    "status": 200,
                    "raw_data": raw_data,
                    "source": source,
                }
            )
        except Exception as e:
            _log(f"Warning: Failed to decompress event {event_id}: {e}")

        n = len(events)
        if total and (n == 1 or n % PROGRESS_EVERY == 0 or n == total):
            dt = time.perf_counter() - t0
            rps = (n / dt) if dt > 0 else 0.0
            pct = (n / total) * 100.0
            _log(f"  load progress: {n}/{total} ({pct:.1f}%)  rate={rps:.0f} rows/s")

    conn.close()
    return events

def ensure_events_schema(conn: sqlite3.Connection):
    """Ensure FruitDeepLinks schema exists (events + images + playables).

    This importer historically relied on an external ingester to create tables.
    We now make the importer self-sufficient and idempotent.
    """
    global _SCHEMA_ENSURED
    cur = conn.cursor()

    # Core events table (existing schema)
    cur.execute("""CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY, pvid TEXT, slug TEXT, title TEXT, title_brief TEXT,
        synopsis TEXT, synopsis_brief TEXT, channel_name TEXT, channel_provider_id TEXT,
        airing_type TEXT, classification_json TEXT, genres_json TEXT, content_segments_json TEXT,
        is_free INTEGER, is_premium INTEGER, runtime_secs INTEGER, start_ms INTEGER, end_ms INTEGER,
        start_utc TEXT, end_utc TEXT, created_ms INTEGER, created_utc TEXT,
        hero_image_url TEXT,
        last_seen_utc TEXT, raw_attributes_json TEXT)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS event_images (
        event_id TEXT, img_type TEXT, url TEXT,
        PRIMARY KEY (event_id, img_type, url))""")

    # Playables table (multi-provider punchouts)
    cur.execute("""CREATE TABLE IF NOT EXISTS playables (
        event_id TEXT NOT NULL,
        playable_id TEXT NOT NULL,
        provider TEXT,
        service_name TEXT,
        logical_service TEXT,
        deeplink_play TEXT,
        deeplink_open TEXT,
        playable_url TEXT,
        title TEXT,
        content_id TEXT,
        priority INTEGER DEFAULT 0,
        created_utc TEXT,
        PRIMARY KEY (event_id, playable_id)
    )""")

    # --- Backward-compatible schema upgrades (ALTER TABLE if needed) --------
    def _ensure_cols(table: str, cols: dict):
        cur.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cur.fetchall()}
        for name, decl in cols.items():
            if name not in existing:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

    _ensure_cols("playables", {
        "service_name": "TEXT",
        "logical_service": "TEXT",
        "priority": "INTEGER DEFAULT 0",
        "created_utc": "TEXT"
    })

    # Indexes (create after ALTERs so they don't fail on older tables)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_pvid ON events(pvid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_time ON events(start_utc, end_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_event_images_event ON event_images(event_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playables_event ON playables(event_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playables_event_priority ON playables(event_id, priority DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_playables_logical_service ON playables(logical_service)")

    conn.commit()

    _SCHEMA_ENSURED = True
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

def extract_playables(
    apple_event: Dict[str, Any],
    event_id: str,
    conn: Optional[sqlite3.Connection] = None
) -> List[Tuple]:
    """Extract playables from Apple event for multi-punchout support.

    Returns tuples matching the playables table schema:
      (event_id, playable_id, provider, service_name, logical_service,
       deeplink_play, deeplink_open, playable_url, title, content_id, priority, created_utc)

    Notes:
    - Supports normalized structure (flat dict with playables key)
    - Supports both dict and list formats for playables
    - If logical_service_mapper is available, computes logical_service + priority
      (including Apple TV league detection when conn is provided).
    """
    playables_data = apple_event.get("playables", {})
    if not playables_data:
        return []

    # Normalize playables_data to a list of dicts
    if isinstance(playables_data, dict):
        playables_list = list(playables_data.values())
    else:
        playables_list = playables_data

    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # Optional logical service mapper
    LOGICAL_SERVICES_AVAILABLE = False
    get_logical_service_for_playable = None
    get_logical_service_priority = None
    try:
        from logical_service_mapper import get_logical_service_for_playable, get_logical_service_priority  # type: ignore
        LOGICAL_SERVICES_AVAILABLE = True
    except Exception:
        LOGICAL_SERVICES_AVAILABLE = False

    result: List[Tuple] = []
    for playable in playables_list:
        if not isinstance(playable, dict):
            continue

        playable_id = playable.get("id") or playable.get("playableId") or ""
        if not playable_id:
            continue

        # Extract deeplinks - use INDIVIDUAL playable's punchoutUrls (most important)
        punchout = playable.get("punchoutUrls") or {}
        deeplink_play = punchout.get("play") or playable.get("deeplink_play")
        deeplink_open = punchout.get("open") or playable.get("deeplink_open")
        playable_url = playable.get("playable_url") or playable.get("url") or playable.get("playableUrl")

        # Determine provider (scheme) from best available URL
        provider = None
        url = deeplink_play or deeplink_open or playable_url or ""
        if url and "://" in url:
            provider = url.split("://", 1)[0]
        elif url.startswith("http://") or url.startswith("https://"):
            provider = "https"

        title = playable.get("displayName") or playable.get("title") or playable.get("name")
        content_id = playable.get("content_id") or playable.get("contentId")

        # Apple-friendly label (what you saw in your stats output)
        service_name = (
            playable.get("serviceName")
            or playable.get("serviceDisplayName")
            or playable.get("providerName")
        )

        # Calculate logical service + priority if available
        logical_service = None
        priority = 0
        if LOGICAL_SERVICES_AVAILABLE and get_logical_service_for_playable and get_logical_service_priority:
            try:
                logical_service = get_logical_service_for_playable(
                    provider=provider or "",
                    deeplink_play=deeplink_play,
                    deeplink_open=deeplink_open,
                    playable_url=playable_url,
                    event_id=event_id,
                    conn=conn
                )
                priority = int(get_logical_service_priority(logical_service))
            except Exception:
                logical_service = None
                priority = 0

        result.append((
            event_id,
            str(playable_id),
            provider,
            service_name,
            logical_service,
            deeplink_play,
            deeplink_open,
            playable_url,
            title,
            content_id,
            priority,
            now_utc
        ))

    # Stable ordering (highest priority first) helps debugging and deterministic imports
    result.sort(key=lambda r: (-(r[10] or 0), str(r[2] or ""), str(r[1] or "")))

    return result

def upsert_playables(conn: sqlite3.Connection, playables: List[Tuple], dry: bool = False):
    """Insert or update playables for an event"""
    if not playables:
        return
    
    if dry:
        _log(f"[DRY] playables x{len(playables)}")
        return
    
    cur = conn.cursor()
    # Ensure playables table exists (idempotent)
    global _SCHEMA_ENSURED
    if not _SCHEMA_ENSURED:
        ensure_events_schema(conn)
    # Delete existing playables for this event (refresh)
    if playables:
        event_id = playables[0][0]
        cur.execute("DELETE FROM playables WHERE event_id = ?", (event_id,))
    
    # Insert new playables
    cur.executemany("""
        INSERT INTO playables (
            event_id, playable_id, provider, service_name, logical_service,
            deeplink_play, deeplink_open, playable_url, title, content_id, priority, created_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, playables)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apple-json", help="Legacy: JSON file from multi_scraper.py")
    ap.add_argument("--apple-db", help="apple_events.db from apple_scraper_db.py (recommended)")
    ap.add_argument("--fruit-db", help="SQLite DB path for FruitDeepLinks events (recommended)")
    ap.add_argument("--peacock-db", help="DEPRECATED: legacy SQLite DB path; use --fruit-db instead")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    # Determine source: DB or JSON
    total = 0
    if args.apple_db:
        total = count_apple_events_in_db(args.apple_db)
        events = iter_apple_events_from_db(args.apple_db)
        _log(f"Streaming {total} Apple TV events from database")
    elif args.apple_json:
        events = load_apple_events(args.apple_json)
        total = len(events)
        _log(f"Loaded {total} Apple TV events from JSON")
    else:
        ap.error("You must provide either --apple-db (recommended) or --apple-json (legacy)")

    db_path_str = args.fruit_db or args.peacock_db
    if not db_path_str:
        ap.error("You must provide --fruit-db (preferred) or --peacock-db")
    db_path = Path(db_path_str)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    ensure_events_schema(conn)

    inserted = 0
    playables_extracted = 0
    
    t0 = time.perf_counter()
    last_t = t0
    last_i = 0

    for i, e in enumerate(events, 1):
        # Normalize ONCE - use for everything
        normalized = normalize_event_structure(e)
        
        # Map to peacock schema (will normalize again internally, but that's ok)
        mapped = map_apple_to_fruit(e, provider_prefix="appletv")
        upsert_event(conn, mapped, dry=args.dry_run)
        
        # Extract images from normalized event
        imgs = extract_images(normalized, mapped["id"])
        upsert_images(conn, imgs, dry=args.dry_run)
        
        # Extract playables from normalized event
        playables = extract_playables(normalized, mapped["id"], conn=conn)
        if playables:
            playables_extracted += len(playables)
        upsert_playables(conn, playables, dry=args.dry_run)
        
        inserted += 1

        if total and (i == 1 or i % PROGRESS_EVERY == 0 or i == total):
            now = time.perf_counter()
            chunk_dt = now - last_t
            chunk_n = i - last_i
            chunk_rate = (chunk_n / chunk_dt) if chunk_dt > 0 else 0.0
            avg_dt = now - t0
            avg_rate = (i / avg_dt) if avg_dt > 0 else 0.0
            pct = (i / total) * 100.0
            _log(f"Import progress: {i}/{total} ({pct:.1f}%)  rate={chunk_rate:.1f} ev/s (avg {avg_rate:.1f})  playables={playables_extracted}")
            last_t = now
            last_i = i

    if not args.dry_run:
        conn.commit()
    conn.close()
    
    _log(f"OK: Imported/updated {inserted} events into {db_path}")
    _log(f"OK: Extracted {playables_extracted} playables total")

if __name__ == "__main__":
    main()
