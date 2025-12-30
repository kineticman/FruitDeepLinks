#!/usr/bin/env python3
"""
apple_scraper_db.py - Production Apple TV Sports scraper with SQLite backend

Architecture:
- Scrapes Apple TV Sports API into apple_events.db
- Uses GZIP compression for raw_json (70-80% space savings)
- Supports incremental updates (skips already-fetched events)
- Multiple search terms for comprehensive coverage
- Smart deduplication and crash recovery
- Separate from fruit_events.db (master aggregated DB)

Usage:
  # Daily scrape (incremental - only fetches new/changed events)
  python apple_scraper_db.py --headless
  
  # Upgrade 100 shelf events to full fetch
  python apple_scraper_db.py --headless --skip-seeds --upgrade-shelf-limit 100
  
  # View stats
  python apple_scraper_db.py --stats-only
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

SEARCH_URL = "https://tv.apple.com/us/collection/sports/uts.col.search.SE?searchTerm={term}"

# ------------------------------ Paths ------------------------------
def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]

def get_auth_path() -> Path:
    return get_project_root() / "data" / "apple_uts_auth.json"

def get_db_path() -> Path:
    return get_project_root() / "data" / "apple_events.db"

# ------------------------------ Database ------------------------------
def init_database(db_path: Path):
    """Initialize SQLite database with schema for scraped Apple TV events"""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS apple_events (
            event_id TEXT PRIMARY KEY,
            fetch_level TEXT NOT NULL,  -- 'seed', 'shelf', 'full'
            source TEXT NOT NULL,        -- 'main', 'shelf', 'league'
            has_multi_playables BOOLEAN,
            playables_count INTEGER,
            unique_services_count INTEGER,
            raw_json_gzip BLOB NOT NULL, -- GZIP compressed JSON
            scraped_at TEXT NOT NULL,
            last_updated TEXT NOT NULL
        )
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fetch_level ON apple_events(fetch_level)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_multi_playables ON apple_events(has_multi_playables)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_last_updated ON apple_events(last_updated)")
    
    conn.commit()
    conn.close()

def analyze_event(raw_data: dict) -> dict:
    """Analyze event to extract playables metadata from all possible locations
    
    Playables can appear in multiple locations:
    1. Top-level: raw_data["playables"] - Primary location for full API responses
    2. Data-level: raw_data["data"]["playables"] - Legacy/alternative location
    3. Content-level: raw_data["data"]["content"]["playables"] - Shelf items
    """
    # Collect playables from ALL possible locations
    all_playables = []
    
    # Location 1: Top-level playables (PRIMARY - full API responses)
    top_level_playables = raw_data.get("playables", {})
    if isinstance(top_level_playables, dict):
        all_playables.extend(top_level_playables.values())
    
    # Location 2: data.playables (secondary location)
    data = raw_data.get("data", {})
    data_playables = data.get("playables", {})
    if isinstance(data_playables, dict):
        all_playables.extend(data_playables.values())
    elif isinstance(data_playables, list):
        all_playables.extend(data_playables)
    
    # Location 3: data.content.playables (shelf-level items)
    content = data.get("content", {})
    content_playables = content.get("playables", {})
    if isinstance(content_playables, dict):
        all_playables.extend(content_playables.values())
    elif isinstance(content_playables, list):
        all_playables.extend(content_playables)
    
    # Count unique services and playables
    playables_count = 0
    unique_services = set()
    punchout_count = 0
    
    for p in all_playables:
        if isinstance(p, dict):
            playables_count += 1
            service = p.get("serviceName", "")
            if service:
                unique_services.add(service)
            
            # Track deeplink availability
            punchout_urls = p.get("punchoutUrls", {})
            if punchout_urls.get("play") or punchout_urls.get("open"):
                punchout_count += 1
    
    return {
        "playables_count": playables_count,
        "unique_services_count": len(unique_services),
        "has_multi_playables": len(unique_services) > 1,
        "punchout_count": punchout_count
    }

def extract_relevant_playables(parent_data: dict, shelf_item: dict) -> dict:
    """
    Extract playables relevant to a specific shelf item from parent event's data.
    
    Uses the shelf item's canonical ID to filter playables that belong to this event.
    Checks both top-level and nested playable locations in the parent data.
    
    Args:
        parent_data: Full parent event response data
        shelf_item: The shelf item (SportingEvent) to extract playables for
        
    Returns:
        Dict of playable_id -> playable data relevant to this shelf item
    """
    relevant = {}
    shelf_canonical_id = shelf_item.get("id", "")
    
    if not shelf_canonical_id:
        return relevant
    
    # Collect all parent playables from various locations
    all_parent_playables = {}
    
    # Check top-level playables (primary location)
    if "playables" in parent_data and isinstance(parent_data["playables"], dict):
        all_parent_playables.update(parent_data["playables"])
    
    # Check data.playables (secondary location)
    data = parent_data.get("data", {})
    if "playables" in data and isinstance(data["playables"], dict):
        all_parent_playables.update(data["playables"])
    
    # Filter playables that match this shelf item's canonical ID
    for playable_id, playable in all_parent_playables.items():
        if isinstance(playable, dict):
            playable_canonical = playable.get("canonicalId", "")
            if playable_canonical == shelf_canonical_id:
                relevant[playable_id] = playable
    
    return relevant

def save_event(conn: sqlite3.Connection, event_id: str, fetch_level: str, 
               source: str, raw_data: dict, verbose: bool = False):
    """Save or update event in database with GZIP compression"""
    now = datetime.now(timezone.utc).isoformat()
    analysis = analyze_event(raw_data)
    
    # Compress JSON with GZIP
    json_str = json.dumps(raw_data)
    compressed = gzip.compress(json_str.encode('utf-8'))
    
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO apple_events 
        (event_id, fetch_level, source, has_multi_playables, playables_count, 
         unique_services_count, raw_json_gzip, scraped_at, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, 
                COALESCE((SELECT scraped_at FROM apple_events WHERE event_id = ?), ?), 
                ?)
    """, (
        event_id, fetch_level, source,
        analysis["has_multi_playables"],
        analysis["playables_count"],
        analysis["unique_services_count"],
        compressed,  # Store compressed BLOB
        event_id, now,
        now
    ))
    
    # Optional diagnostic output
    if verbose and analysis["playables_count"] > 0:
        print(f"      └─ Saved {analysis['playables_count']} playables, "
              f"{analysis['unique_services_count']} services, "
              f"{analysis.get('punchout_count', 0)} with deeplinks")
    
    return analysis

def event_exists_as_full(conn: sqlite3.Connection, event_id: str) -> bool:
    """Check if event already exists and is fully fetched"""
    cur = conn.cursor()
    cur.execute("SELECT fetch_level FROM apple_events WHERE event_id = ?", (event_id,))
    row = cur.fetchone()
    return row is not None and row[0] == 'full'

def get_shelf_events_to_upgrade(conn: sqlite3.Connection, limit: int) -> List[str]:
    """Get shelf events that should be upgraded to full fetch"""
    cur = conn.cursor()
    cur.execute("""
        SELECT event_id FROM apple_events 
        WHERE fetch_level = 'shelf'
        ORDER BY last_updated ASC
        LIMIT ?
    """, (limit,))
    return [row[0] for row in cur.fetchall()]

def print_stats(conn: sqlite3.Connection):
    """Print scraping statistics"""
    cur = conn.cursor()
    
    # Total events by fetch level
    cur.execute("SELECT fetch_level, COUNT(*) FROM apple_events GROUP BY fetch_level")
    print("\nEvents by fetch level:")
    for fetch_level, count in cur.fetchall():
        print(f"  {fetch_level}: {count}")
    
    # Multi-service events
    cur.execute("SELECT COUNT(*) FROM apple_events WHERE has_multi_playables = 1")
    multi_count = cur.fetchone()[0]
    print(f"\nEvents with multiple services: {multi_count}")
    
    # Total events
    cur.execute("SELECT COUNT(*) FROM apple_events")
    total = cur.fetchone()[0]
    print(f"Total unique events: {total}")
    
    # Database size
    cur.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
    db_size_bytes = cur.fetchone()[0]
    db_size_mb = db_size_bytes / 1024 / 1024
    print(f"Database size: {db_size_mb:.1f} MB")
    
    # Top services (requires decompression) - CHECK ALL LOCATIONS
    print("\nTop services by event count:")
    cur.execute("SELECT event_id, raw_json_gzip FROM apple_events")
    service_counts = {}
    location_stats = {"top_level": 0, "data_level": 0, "content_level": 0}
    
    for event_id, compressed in cur.fetchall():
        try:
            json_str = gzip.decompress(compressed).decode('utf-8')
            data = json.loads(json_str)
            
            # Collect playables from all locations
            playables_to_check = []
            
            # Location 1: Top-level playables (primary for full fetches)
            if "playables" in data and isinstance(data["playables"], dict):
                count_before = len(playables_to_check)
                playables_to_check.extend(data["playables"].values())
                if len(playables_to_check) > count_before:
                    location_stats["top_level"] += 1
            
            # Location 2: data.playables (secondary)
            data_obj = data.get("data", {})
            if "playables" in data_obj:
                p = data_obj["playables"]
                count_before = len(playables_to_check)
                if isinstance(p, dict):
                    playables_to_check.extend(p.values())
                elif isinstance(p, list):
                    playables_to_check.extend(p)
                if len(playables_to_check) > count_before:
                    location_stats["data_level"] += 1
            
            # Location 3: data.content.playables (shelf items)
            if "content" in data_obj and "playables" in data_obj["content"]:
                p = data_obj["content"]["playables"]
                count_before = len(playables_to_check)
                if isinstance(p, dict):
                    playables_to_check.extend(p.values())
                elif isinstance(p, list):
                    playables_to_check.extend(p)
                if len(playables_to_check) > count_before:
                    location_stats["content_level"] += 1
            
            # Count services
            for playable in playables_to_check:
                if isinstance(playable, dict):
                    service = playable.get("serviceName")
                    if service:
                        service_counts[service] = service_counts.get(service, 0) + 1
        except:
            pass
    
    for service, count in sorted(service_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {service}: {count}")
    
    print(f"\nPlayables location distribution:")
    print(f"  Top-level (response.playables): {location_stats['top_level']} events")
    print(f"  Data-level (data.playables): {location_stats['data_level']} events")
    print(f"  Content-level (data.content.playables): {location_stats['content_level']} events")

# ------------------------------ Chrome Driver ------------------------------
def make_driver(headless: bool = False) -> webdriver.Chrome:
    """Create Chrome WebDriver"""
    import logging
    import os
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US")
    opts.add_argument("--window-size=1400,1200")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    
    try:
        wm_path = Path(ChromeDriverManager().install())
        if wm_path.name != "chromedriver":
            driver_path = wm_path.with_name("chromedriver")
        else:
            driver_path = wm_path
        
        if not driver_path.exists():
            raise FileNotFoundError(f"chromedriver not found at {driver_path}")
        
        if not os.access(driver_path, os.X_OK):
            mode = driver_path.stat().st_mode
            driver_path.chmod(mode | 0o111)
        
        service = Service(str(driver_path))
        driver = webdriver.Chrome(service=service, options=opts)
        return driver
        
    except Exception as e:
        logger.error(f"Failed to initialize Chrome: {e}")
        raise

# ------------------------------ Auth ------------------------------
def load_cached_auth() -> Tuple[Optional[str], Optional[str]]:
    p = get_auth_path()
    if not p.exists():
        return (None, None)
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        return d.get("utscf"), d.get("utsk")
    except Exception:
        return (None, None)

def save_auth(utscf: str, utsk: str):
    p = get_auth_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"utscf": utscf, "utsk": utsk, "timestamp": time.time()}, indent=2))
        print(f"[Auth] saved -> {p}")
    except Exception as e:
        print(f"[Auth] save error: {e}")

# ------------------------------ API Fetching ------------------------------
def fetch_via_browser(driver, url: str) -> dict:
    script = f"""
    return fetch('{url}', {{
        method: 'GET',
        credentials: 'include',
        headers: {{ 'Accept': 'application/json' }}
    }}).then(r => r.json()).catch(e => ({{error: e.toString()}}));
    """
    return driver.execute_script(script)

def fetch_event_v3(driver, event_id: str, utscf: str, utsk: str) -> dict:
    base = f"https://tv.apple.com/api/uts/v3/sporting-events/{event_id}"
    params = "caller=web&locale=en-US&pfm=web&sf=143441&v=90"
    url = f"{base}?{params}&utscf={utscf}&utsk={utsk}"
    return fetch_via_browser(driver, url)

# ------------------------------ Scraping Helpers ------------------------------
def auto_scroll(driver, seconds: float, steps: int):
    """Scroll page to load more content"""
    per_step = seconds / max(1, steps)
    for _ in range(steps):
        driver.execute_script("window.scrollBy(0, 300);")
        time.sleep(per_step)

def get_event_ids_from_page(driver) -> Set[str]:
    """Extract event IDs from current page HTML"""
    ids: Set[str] = set()
    try:
        html = driver.page_source
        for m in re.finditer(r'umc\.cse\.[a-z0-9]+', html):
            ids.add(m.group(0))
    except Exception:
        pass
    return ids

# ------------------------------ Search Terms ------------------------------
def default_terms() -> str:
    return ",".join([
        "soccer", "nba", "nhl", "mlb", "nfl", "mls",
        "champions league", "ligue 1", "formula 1", "cricket",
        "espn", "cbs sports", "fox sports", "paramount+", "prime video", "peacock", "dazn",
        "women's college basketball", "men's college basketball",
    ])

def parse_terms(arg: str) -> List[str]:
    return [t.strip() for t in arg.split(",") if t.strip()]

def ensure_all_first(terms: List[str]) -> List[str]:
    lower = [t.lower() for t in terms]
    if "all" in lower:
        terms = [t for t in terms if t.lower() != "all"]
    return ["all"] + terms

# ------------------------------ Main Scraping ------------------------------
def scrape_search_term(driver, conn: sqlite3.Connection, search_term: str, 
                       utscf: str, utsk: str) -> Tuple[int, int, int]:
    """
    Scrape a single search term.
    Returns: (new_seeds, new_shelf, skipped)
    """
    print(f"\n== Search: {search_term} ==")
    
    driver.get(SEARCH_URL.format(term=search_term))
    time.sleep(0.6)
    auto_scroll(driver, seconds=5.0, steps=24)
    
    seed_ids = get_event_ids_from_page(driver)
    print(f"  Found {len(seed_ids)} seed IDs from page")
    
    # Count how many are already full (for stats)
    already_full_count = sum(1 for eid in seed_ids if event_exists_as_full(conn, eid))
    print(f"  Already fetched: {already_full_count}, Will check for new shelf events")
    
    new_seeds = 0
    new_shelf = 0
    skipped = 0
    
    # Fetch each seed event individually
    for i, event_id in enumerate(seed_ids, 1):
        already_full = event_exists_as_full(conn, event_id)
        
        if already_full:
            print(f"  [Seed {i}/{len(seed_ids)}] {event_id} (checking for new shelf events)")
        else:
            print(f"  [Seed {i}/{len(seed_ids)}] {event_id}")
        
        try:
            data = fetch_event_v3(driver, event_id, utscf, utsk)
            if isinstance(data, dict) and data.get("data"):
                # Save/update main event (even if already exists, updates shelf data)
                if not already_full:
                    save_event(conn, event_id, "full", "main", data)
                    new_seeds += 1
                else:
                    skipped += 1
                
                # ALWAYS extract shelf events - they might be new!
                canvas = data.get("data", {}).get("canvas", {})
                shelves = canvas.get("shelves", [])
                shelf_discovered = 0
                for shelf in shelves:
                    for item in shelf.get("items", []):
                        if item.get("type") == "SportingEvent":
                            shelf_id = item.get("id")
                            if shelf_id and not event_exists_as_full(conn, shelf_id):
                                # Enhanced shelf data - preserve relevant playables from parent
                                relevant_playables = extract_relevant_playables(data, item)
                                
                                shelf_data = {
                                    "data": {
                                        "content": item,
                                        "canvas": {},
                                        "playables": item.get("playables", {})
                                    },
                                    # Preserve top-level data from parent when available
                                    "playables": relevant_playables,
                                    "channels": data.get("channels", {}),
                                    "howToWatch": []  # Will be populated if relevant_playables exist
                                }
                                
                                # If we found relevant playables, create howToWatch entries
                                if relevant_playables:
                                    for playable_id, playable in relevant_playables.items():
                                        channel_id = playable.get("channelId", "")
                                        if channel_id:
                                            shelf_data["howToWatch"].append({
                                                "channelId": channel_id,
                                                "versions": [{"playableId": playable_id}]
                                            })
                                
                                save_event(conn, shelf_id, "shelf", "shelf", shelf_data)
                                new_shelf += 1
                                shelf_discovered += 1
                
                if already_full and shelf_discovered > 0:
                    print(f"    -> Found {shelf_discovered} new shelf events")
                
                conn.commit()
                
        except Exception as e:
            print(f"    error: {e}")
        
        time.sleep(0.18)
    
    return new_seeds, new_shelf, skipped

# ------------------------------ Main ------------------------------
def main():
    ap = argparse.ArgumentParser(description="Apple TV Sports scraper with SQLite backend")
    ap.add_argument("--db", default=str(get_db_path()), help="SQLite database path")
    ap.add_argument("--terms", default=default_terms(), help="Comma-separated search terms")
    ap.add_argument("--upgrade-shelf-limit", type=int, default=0, 
                    help="Upgrade N shelf events to full fetch (0=disabled)")
    ap.add_argument("--skip-seeds", action="store_true", 
                    help="Skip seed scraping, only upgrade shelf events")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--stats-only", action="store_true", help="Print stats and exit")
    args = ap.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize database
    init_database(db_path)
    conn = sqlite3.connect(str(db_path))
    
    # Stats only mode
    if args.stats_only:
        print(f"=== Apple Events DB Stats ({db_path}) ===")
        print_stats(conn)
        conn.close()
        return 0
    
    # Start scraping
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print("Apple TV Sports Scraper (DB Backend)")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    driver = make_driver(headless=args.headless)
    
    try:
        # Get auth tokens
        driver.get(SEARCH_URL.format(term="all"))
        time.sleep(1.2)
        utscf, utsk = load_cached_auth()
        if not utscf or not utsk:
            print("ERROR: No cached auth tokens found")
            print("Run multi_scraper.py once to capture tokens, or manually save to:")
            print(f"  {get_auth_path()}")
            return 1
        
        total_new_seeds = 0
        total_new_shelf = 0
        total_skipped = 0
        
        # Scrape search terms (unless --skip-seeds)
        if not args.skip_seeds:
            terms = ensure_all_first(parse_terms(args.terms))
            print(f"\nScraping {len(terms)} search terms: {terms[:5]}...")
            
            for term in terms:
                new_seeds, new_shelf, skipped = scrape_search_term(
                    driver, conn, term, utscf, utsk
                )
                total_new_seeds += new_seeds
                total_new_shelf += new_shelf
                total_skipped += skipped
            
            print(f"\n=== Seed Scraping Complete ===")
            print(f"New full events: {total_new_seeds}")
            print(f"New shelf events: {total_new_shelf}")
            print(f"Skipped (already full): {total_skipped}")
        else:
            print("\n== Skipping seed scrape (--skip-seeds) ==")
        
        # Upgrade shelf events if requested
        if args.upgrade_shelf_limit and args.upgrade_shelf_limit > 0:
            print(f"\n== Upgrading {args.upgrade_shelf_limit} shelf events ==")
            shelf_ids = get_shelf_events_to_upgrade(conn, args.upgrade_shelf_limit)
            
            print(f"  Found {len(shelf_ids)} shelf events to upgrade")
            
            upgraded = 0
            for i, shelf_id in enumerate(shelf_ids, 1):
                print(f"  [Upgrade {i}/{len(shelf_ids)}] {shelf_id}")
                try:
                    data = fetch_event_v3(driver, shelf_id, utscf, utsk)
                    if isinstance(data, dict) and data.get("data"):
                        save_event(conn, shelf_id, "full", "main", data)
                        upgraded += 1
                        conn.commit()
                except Exception as e:
                    print(f"    error: {e}")
                
                time.sleep(0.18)
            
            print(f"\n=== Shelf Upgrade Complete ===")
            print(f"Successfully upgraded: {upgraded}/{len(shelf_ids)}")
        
        # Final stats
        print("\n" + "=" * 60)
        print("SCRAPE COMPLETE")
        print("=" * 60)
        print_stats(conn)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"\nFinished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration:.1f} seconds")
        
    finally:
        conn.close()
        driver.quit()
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
