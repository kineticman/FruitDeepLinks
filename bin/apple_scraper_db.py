#!/usr/bin/env python3
"""
apple_scraper_db.py - Production Apple TV Sports scraper with HYBRID optimization

HYBRID ARCHITECTURE (NEW):
- Uses Selenium ONCE to capture tokens + establish browser session
- Extracts cookies from browser session
- Uses fast requests library for all API calls (10x faster!)
- Falls back to Selenium if requests fails

Performance: 1000 events in ~50 seconds (was ~500 seconds)

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
  
  # Force pure Selenium mode (disable hybrid optimization)
  python apple_scraper_db.py --headless --no-hybrid
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sqlite3
import sys
import time
import urllib.parse
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
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


# ------------------------------ Cleanup helpers ------------------------------
def cleanup_failed_shelf_upgrade_logs(directory: Path, *, keep_days: int = 3, verbose: bool = True) -> int:
    """
    Delete old failed_shelf_upgrades_*.json files in `directory`.

    Policy: keep only the most recent `keep_days` of files (based on mtime). Everything older is deleted.

    Returns: number of files deleted
    """
    try:
        directory = Path(directory)
        if not directory.exists():
            return 0

        now = time.time()
        cutoff = now - (keep_days * 86400)

        deleted = 0
        for p in directory.glob("failed_shelf_upgrades_*.json"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
                    deleted += 1
                    if verbose:
                        print(f"  [cleanup] deleted old shelf-upgrade log: {p}")
            except Exception as e:
                if verbose:
                    print(f"  [cleanup] could not delete {p}: {e}")

        return deleted
    except Exception as e:
        if verbose:
            print(f"  [cleanup] failed: {e}")
        return 0

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
    """Analyze event to extract playables metadata from all possible locations"""
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
    """Extract playables relevant to a specific shelf item from parent event's data."""
    relevant = {}
    shelf_canonical_id = shelf_item.get("id", "")
    
    if not shelf_canonical_id:
        return relevant
    
    all_parent_playables = {}
    
    if "playables" in parent_data and isinstance(parent_data["playables"], dict):
        all_parent_playables.update(parent_data["playables"])
    
    data = parent_data.get("data", {})
    if "playables" in data and isinstance(data["playables"], dict):
        all_parent_playables.update(data["playables"])
    
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
        compressed,
        event_id, now,
        now
    ))
    
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
    """Get events that should be upgraded/refreshed
    
    Returns:
    - All shelf events (incomplete data needs upgrade)
    - Any event updated 23hrs-5days ago (might have new playables, but skip very old)
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT event_id FROM apple_events 
        WHERE fetch_level = 'shelf'
           OR (last_updated < datetime('now', '-23 hours') 
               AND last_updated > datetime('now', '-5 days'))
        ORDER BY last_updated ASC
        LIMIT ?
    """, (limit,))
    return [row[0] for row in cur.fetchall()]

def print_stats(conn: sqlite3.Connection):
    """Print scraping statistics"""
    cur = conn.cursor()
    
    cur.execute("SELECT fetch_level, COUNT(*) FROM apple_events GROUP BY fetch_level")
    print("\nEvents by fetch level:")
    for fetch_level, count in cur.fetchall():
        print(f"  {fetch_level}: {count}")
    
    cur.execute("SELECT COUNT(*) FROM apple_events WHERE has_multi_playables = 1")
    multi_count = cur.fetchone()[0]
    print(f"\nEvents with multiple services: {multi_count}")
    
    cur.execute("SELECT COUNT(*) FROM apple_events")
    total = cur.fetchone()[0]
    print(f"Total unique events: {total}")
    
    cur.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
    db_size_bytes = cur.fetchone()[0]
    db_size_mb = db_size_bytes / 1024 / 1024
    print(f"Database size: {db_size_mb:.1f} MB")
    
    print("\nTop services by event count:")
    cur.execute("SELECT event_id, raw_json_gzip FROM apple_events")
    service_counts = {}
    location_stats = {"top_level": 0, "data_level": 0, "content_level": 0}
    
    for event_id, compressed in cur.fetchall():
        try:
            json_str = gzip.decompress(compressed).decode('utf-8')
            data = json.loads(json_str)
            
            playables_to_check = []
            
            if "playables" in data and isinstance(data["playables"], dict):
                count_before = len(playables_to_check)
                playables_to_check.extend(data["playables"].values())
                if len(playables_to_check) > count_before:
                    location_stats["top_level"] += 1
            
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
            
            if "content" in data_obj and "playables" in data_obj["content"]:
                p = data_obj["content"]["playables"]
                count_before = len(playables_to_check)
                if isinstance(p, dict):
                    playables_to_check.extend(p.values())
                elif isinstance(p, list):
                    playables_to_check.extend(p)
                if len(playables_to_check) > count_before:
                    location_stats["content_level"] += 1
            
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
    """Create Chrome/Chromium WebDriver with cross-platform support"""
    import logging
    import os
    import subprocess
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)
    
    logger.info("=== Starting Chrome/Chromium Driver Initialization ===")
    
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
        logger.info("Headless mode enabled")
    
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US")
    opts.add_argument("--window-size=1400,1200")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    
    if os.path.exists('/usr/bin/chromium'):
        opts.binary_location = '/usr/bin/chromium'
        logger.info("Using system Chromium binary at /usr/bin/chromium")
    
    def _try_start_driver(driver_path: str):
        try:
            logger.info(f"Attempting to start driver at: {driver_path}")
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=opts)
            logger.info("Browser launched successfully!")
            return driver
        except Exception as e:
            logger.error(f"Failed to start with {driver_path}: {e}")
            return None
    
    try:
        logger.info("Attempting webdriver-manager installation...")
        wm_path = Path(ChromeDriverManager().install())
        if wm_path.name != "chromedriver":
            driver_path = wm_path.with_name("chromedriver")
        else:
            driver_path = wm_path
        
        if driver_path.exists():
            if not os.access(driver_path, os.X_OK):
                logger.info(f"Setting executable permissions on {driver_path}")
                driver_path.chmod(driver_path.stat().st_mode | 0o111)
            
            driver = _try_start_driver(str(driver_path))
            if driver:
                logger.info("=== Driver Initialization Complete (webdriver-manager) ===")
                return driver
    except Exception as e:
        logger.warning(f"webdriver-manager approach failed: {e}")
    
    fallback_paths = [
        "/usr/bin/chromedriver",
        "/usr/local/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
    ]
    
    logger.info("Trying system chromedriver paths...")
    for sys_path in fallback_paths:
        if os.path.exists(sys_path):
            driver = _try_start_driver(sys_path)
            if driver:
                logger.info(f"=== Driver Initialization Complete (system: {sys_path}) ===")
                return driver
        else:
            logger.debug(f"System chromedriver not found at: {sys_path}")
    
    logger.error("=== CHROME/CHROMIUM DRIVER INITIALIZATION FAILED ===")
    
    logger.info("Checking for installed browsers...")
    for browser_cmd in ['google-chrome', 'chromium', 'chromium-browser']:
        try:
            result = subprocess.run(
                [browser_cmd, '--version'],
                capture_output=True,
                text=True,
                timeout=5,
            )
            logger.info(f"Found {browser_cmd}: {result.stdout.strip()}")
        except Exception:
            logger.debug(f"{browser_cmd} not found in PATH")
    
    raise RuntimeError(
        "Unable to initialize Chrome/Chromium WebDriver. "
        "Ensure either Google Chrome or Chromium is installed with matching chromedriver."
    )

# ------------------------------ Auth ------------------------------
def load_cached_auth() -> Tuple[Optional[str], Optional[str]]:
    p = get_auth_path()
    if not p.exists():
        return (None, None)
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        utscf = d.get("utscf", "")
        utsk = d.get("utsk", "")
        # URL decode tokens if needed
        utscf = urllib.parse.unquote(utscf) if utscf else None
        utsk = urllib.parse.unquote(utsk) if utsk else None
        return utscf, utsk
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

# ------------------------------ HYBRID API Fetching (NEW!) ------------------------------

class HybridAPIClient:
    """
    HYBRID optimization: Use fast requests library with browser session.
    
    Performance: 10x faster than pure Selenium (50ms vs 500ms per request)
    
    Architecture:
    1. Selenium creates browser session ONCE (captures tokens + cookies)
    2. Requests library uses tokens + cookies for all API calls
    3. Falls back to Selenium if requests fails
    """
    
    def __init__(self, driver, utscf: str, utsk: str, use_hybrid: bool = True):
        self.driver = driver
        self.utscf = utscf
        self.utsk = utsk
        self.use_hybrid = use_hybrid
        
        # Requests session (fast!)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://tv.apple.com/us',
        })
        
        # Extract cookies from browser
        if use_hybrid:
            try:
                selenium_cookies = driver.get_cookies()
                for cookie in selenium_cookies:
                    self.session.cookies.set(
                        cookie['name'],
                        cookie['value'],
                        domain=cookie.get('domain', '.apple.com'),
                        path=cookie.get('path', '/')
                    )
                print(f"[Hybrid] Extracted {len(selenium_cookies)} browser cookies")
            except Exception as e:
                print(f"[Hybrid] Cookie extraction failed: {e}, falling back to Selenium")
                self.use_hybrid = False
        
        # Stats
        self.requests_count = 0
        self.selenium_count = 0
        self.requests_failures = 0

        # Last-call diagnostics (sanitized) for logging / troubleshooting
        self.last_debug: dict = {}
        self.last_error: Optional[str] = None
    
    def fetch_event_v3(self, event_id: str) -> dict:
        """Fetch event using hybrid approach (requests first, Selenium fallback).

        Populates ``self.last_debug`` with sanitized diagnostics that can be logged
        if an event fails to upgrade (no secrets / cookies / tokens).
        """
        base = f"https://tv.apple.com/api/uts/v3/sporting-events/{event_id}"
        params = (
            f"caller=web&locale=en-US&pfm=web&sf=143441&v=90"
            f"&utscf={self.utscf}&utsk={self.utsk}"
        )
        url = f"{base}?{params}"

        self.last_error = None
        self.last_debug = {
            "event_id": event_id,
            "used": None,
            "requests": None,
            "selenium": None,
        }

        # Try fast requests library first
        if self.use_hybrid:
            try:
                resp = self.session.get(url, timeout=10)
                ct = (resp.headers.get("content-type") or "").lower()
                is_json = ("json" in ct)
                req_info = {
                    "status_code": resp.status_code,
                    "content_type": ct[:80],
                    "is_json": bool(is_json),
                    "content_len": int(resp.headers.get("content-length") or len(resp.content or b"")),
                }

                if resp.status_code == 200 and is_json:
                    try:
                        data = resp.json()
                    except Exception as je:
                        req_info["json_error"] = str(je)[:200]
                        data = None

                    if isinstance(data, dict) and data.get("data"):
                        self.requests_count += 1
                        req_info["has_data"] = True
                        self.last_debug["used"] = "requests"
                        self.last_debug["requests"] = req_info
                        return data

                    req_info["has_data"] = False
                    if isinstance(data, dict):
                        if "errors" in data:
                            req_info["errors"] = data.get("errors")
                        req_info["top_keys"] = list(data.keys())[:12]

                else:
                    # HTML / redirect / 403 etc; keep a tiny preview for debugging
                    preview = ""
                    try:
                        preview = (resp.text or "")[:180]
                    except Exception:
                        preview = ""
                    if preview:
                        req_info["body_preview"] = preview.replace("\n", " ")[:180]

                self.requests_failures += 1
                self.last_debug["requests"] = req_info

            except Exception as e:
                self.requests_failures += 1
                self.last_error = str(e)
                self.last_debug["requests"] = {"exception": str(e)[:200]}

        # Fallback to Selenium (slower but more reliable)
        self.selenium_count += 1
        self.last_debug["used"] = "selenium"
        data = self._fetch_via_browser(url)

        sel_info: dict = {}
        if isinstance(data, dict):
            if data.get("data"):
                sel_info["has_data"] = True
            else:
                sel_info["has_data"] = False
                if "error" in data:
                    sel_info["error"] = str(data.get("error"))[:200]
                if "errors" in data:
                    sel_info["errors"] = data.get("errors")
                sel_info["top_keys"] = list(data.keys())[:12]
        else:
            sel_info["non_dict_type"] = type(data).__name__

        self.last_debug["selenium"] = sel_info
        return data

    def _fetch_via_browser(self, url: str) -> dict:
        """Original Selenium-based fetch (fallback).

        Returns a dict (Apple JSON) or ``{'error': '...'}`` on failure.
        """
        script = f"""
        return fetch('{url}', {{
            method: 'GET',
            credentials: 'include',
            headers: {{ 'Accept': 'application/json' }}
        }}).then(r => r.json()).catch(e => ({{error: e.toString()}}));
        """
        try:
            return self.driver.execute_script(script)
        except Exception as e:
            return {"error": f"Selenium execute_script failed: {e}"}

    def print_stats(self):
        """Print performance statistics"""
        total = self.requests_count + self.selenium_count
        if total > 0:
            requests_pct = (self.requests_count / total) * 100
            print(f"\n[Hybrid Stats]")
            print(f"  Requests library: {self.requests_count} ({requests_pct:.1f}%) - FAST ⚡")
            print(f"  Selenium fallback: {self.selenium_count} ({100-requests_pct:.1f}%)")
            print(f"  Requests failures: {self.requests_failures}")
            
            if requests_pct > 90:
                print(f"  Performance: ~10x faster than pure Selenium! 🚀")

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
                       api_client: HybridAPIClient) -> Tuple[int, int, int]:
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
    
    already_full_count = sum(1 for eid in seed_ids if event_exists_as_full(conn, eid))
    print(f"  Already fetched: {already_full_count}, Will check for new shelf events")
    
    new_seeds = 0
    new_shelf = 0
    skipped = 0
    
    for i, event_id in enumerate(seed_ids, 1):
        already_full = event_exists_as_full(conn, event_id)
        
        if already_full:
            print(f"  [Seed {i}/{len(seed_ids)}] {event_id} (checking for new shelf events)")
        else:
            print(f"  [Seed {i}/{len(seed_ids)}] {event_id}")
        
        try:
            # Use hybrid API client (requests first, Selenium fallback)
            data = api_client.fetch_event_v3(event_id)
            
            if isinstance(data, dict) and data.get("data"):
                # ALWAYS save fetched data, even if event exists
                # This ensures provider updates (new streaming services added close to game time) are captured
                save_event(conn, event_id, "full", "main", data)
                if not already_full:
                    new_seeds += 1
                else:
                    skipped += 1  # Count as skipped for stats, but data IS updated
                
                # Extract shelf events
                canvas = data.get("data", {}).get("canvas", {})
                shelves = canvas.get("shelves", [])
                shelf_discovered = 0
                for shelf in shelves:
                    for item in shelf.get("items", []):
                        if item.get("type") == "SportingEvent":
                            shelf_id = item.get("id")
                            if shelf_id and not event_exists_as_full(conn, shelf_id):
                                relevant_playables = extract_relevant_playables(data, item)
                                
                                shelf_data = {
                                    "data": {
                                        "content": item,
                                        "canvas": {},
                                        "playables": item.get("playables", {})
                                    },
                                    "playables": relevant_playables,
                                    "channels": data.get("channels", {}),
                                    "howToWatch": []
                                }
                                
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
    ap = argparse.ArgumentParser(description="Apple TV Sports scraper with HYBRID optimization")
    ap.add_argument("--db", default=str(get_db_path()), help="SQLite database path")
    ap.add_argument("--terms", default=default_terms(), help="Comma-separated search terms")
    ap.add_argument("--upgrade-shelf-limit", type=int, default=0, 
                    help="Upgrade N shelf events to full fetch (0=disabled)")
    ap.add_argument("--skip-seeds", action="store_true", 
                    help="Skip seed scraping, only upgrade shelf events")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--stats-only", action="store_true", help="Print stats and exit")
    ap.add_argument("--no-hybrid", action="store_true", 
                    help="Disable hybrid optimization (use pure Selenium)")
    args = ap.parse_args()

    # Always cleanup old shelf-upgrade failure dumps (even if no failures this run)
    cleanup_failed_shelf_upgrade_logs(Path(args.db).resolve().parent, keep_days=3, verbose=False)

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    init_database(db_path)
    conn = sqlite3.connect(str(db_path))
    
    if args.stats_only:
        print(f"=== Apple Events DB Stats ({db_path}) ===")
        print_stats(conn)
        conn.close()
        return 0
    
    start_time = datetime.now()
    print("\n" + "=" * 60)
    print("Apple TV Sports Scraper (HYBRID Mode)")
    if args.no_hybrid:
        print("  (Hybrid optimization DISABLED)")
    else:
        print("  (10x faster with requests library!)")
    print(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    driver = make_driver(headless=args.headless)
    
    try:
        # Navigate to establish browser session
        print("\n[Hybrid] Establishing browser session...")
        driver.get(SEARCH_URL.format(term="all"))
        time.sleep(1.2)
        
        # Load auth tokens
        utscf, utsk = load_cached_auth()
        if not utscf or not utsk:
            print("ERROR: No cached auth tokens found")
            print("Run multi_scraper.py once to capture tokens, or manually save to:")
            print(f"  {get_auth_path()}")
            return 1
        
        print(f"[Hybrid] Loaded tokens: utscf={utscf[:20]}... utsk={utsk[:20]}...")
        
        # Initialize hybrid API client
        api_client = HybridAPIClient(driver, utscf, utsk, use_hybrid=not args.no_hybrid)
        
        total_new_seeds = 0
        total_new_shelf = 0
        total_skipped = 0
        
        # Scrape search terms
        if not args.skip_seeds:
            terms = ensure_all_first(parse_terms(args.terms))
            print(f"\nScraping {len(terms)} search terms: {terms[:5]}...")
            
            for term in terms:
                new_seeds, new_shelf, skipped = scrape_search_term(
                    driver, conn, term, api_client
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
        
        # Upgrade shelf events
        if args.upgrade_shelf_limit and args.upgrade_shelf_limit > 0:
            print(f"\n== Upgrading/Refreshing events (limit: {args.upgrade_shelf_limit}) ==")
            shelf_ids = get_shelf_events_to_upgrade(conn, args.upgrade_shelf_limit)
            
            print(f"  Found {len(shelf_ids)} events to upgrade/refresh")
            
            upgraded = 0
            failed_upgrades: List[dict] = []
            for i, shelf_id in enumerate(shelf_ids, 1):
                print(f"  [Upgrade {i}/{len(shelf_ids)}] {shelf_id}")
                try:
                    data = api_client.fetch_event_v3(shelf_id)
                    dbg = getattr(api_client, "last_debug", None)
                    if isinstance(data, dict) and data.get("data"):
                        save_event(conn, shelf_id, "full", "main", data)
                        upgraded += 1
                        conn.commit()
                    else:
                        failed_upgrades.append({
                            "event_id": shelf_id,
                            "reason": "missing_data",
                            "debug": dbg,
                            "top_keys": list(data.keys())[:12] if isinstance(data, dict) else [type(data).__name__],
                        })
                except Exception as e:
                    dbg = getattr(api_client, "last_debug", None)
                    failed_upgrades.append({
                        "event_id": shelf_id,
                        "reason": "exception",
                        "error": str(e),
                        "debug": dbg,
                    })
                    print(f"    error: {e}")
                
                time.sleep(0.18)
            
            print(f"\n=== Shelf Upgrade Complete ===")
            print(f"Successfully upgraded: {upgraded}/{len(shelf_ids)}")

            if failed_upgrades:
                print(f"\n--- Failed Shelf Upgrades ({len(failed_upgrades)}) ---")
                for f in failed_upgrades:
                    dbg = f.get("debug") or {}
                    used = dbg.get("used") or "unknown"
                    r = dbg.get("requests") or {}
                    s = dbg.get("selenium") or {}
                    msg = f"  - {f.get('event_id')}: used={used}"
                    if r:
                        if "status_code" in r:
                            msg += f", req_status={r.get('status_code')}, ct={r.get('content_type')}"
                        elif "exception" in r:
                            msg += f", req_exc={r.get('exception')}"
                    if s and not s.get("has_data", False):
                        if s.get("error"):
                            msg += f", selenium_error={s.get('error')}"
                    print(msg)

                # Write full diagnostics next to the DB for deeper troubleshooting
                try:
                    out_path = Path(args.db).resolve().parent / (
                        f"failed_shelf_upgrades_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    )
                    out_path.write_text(json.dumps(failed_upgrades, indent=2))
                    print(f"  (details saved to: {out_path})")

                    # Cleanup older debug dumps (keep only 3 days)
                    deleted = cleanup_failed_shelf_upgrade_logs(out_path.parent, keep_days=3, verbose=False)
                    if deleted:
                        print(f"  [cleanup] removed {deleted} old failed_shelf_upgrades_*.json file(s)")
                except Exception as e:
                    print(f"  (could not write failure details JSON: {e})")
        
        # Print hybrid performance stats
        api_client.print_stats()
        
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
