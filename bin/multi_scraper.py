# bin/multi_scraper.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import gzip
import io
import json
import re
import sys
import time
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

SEARCH_URL = "https://tv.apple.com/us/collection/sports/uts.col.search.SE?searchTerm={term}"

# ------------------------------ paths ------------------------------
def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]

def get_auth_path() -> Path:
    return get_project_root() / "data" / "apple_uts_auth.json"

# ------------------------------ driver ------------------------------
# ------------------------------ driver ------------------------------
def make_driver(headless: bool = False, enable_network: bool = True) -> webdriver.Chrome:
    """
    Create a Chrome/Chromium WebDriver with cross-platform support.
    
    Tries webdriver-manager first, then falls back to common system 
    chromedriver locations. Supports both Google Chrome and Debian Chromium.
    """
    import logging
    import os
    from pathlib import Path as _Path
    import subprocess

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("=== Starting Chrome/Chromium Driver Initialization ===")
    logger.info(f"Headless mode: {headless}")
    logger.info(f"Network enabled: {enable_network}")

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
        logger.info("Added --headless=new")

    # Core flags
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US")
    opts.add_argument("--window-size=1400,1200")

    # Additional flags for Docker/container environments
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-setuid-sandbox")
    opts.add_argument("--remote-debugging-port=9222")

    logger.info("Chrome options configured")

    # Detect system Chromium (Docker/Linux environments)
    if os.path.exists('/usr/bin/chromium'):
        opts.binary_location = '/usr/bin/chromium'
        logger.info("Using system Chromium binary at /usr/bin/chromium")

    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    opts.set_capability(
        "goog:perfLoggingPrefs",
        {"enableNetwork": True, "enablePage": False},
    )

    def _try_start_with_service_path(path_str: str):
        """Helper to try starting Chrome/Chromium with a given chromedriver path."""
        try:
            logger.info(f"Attempting to start Chrome/Chromium with chromedriver at: {path_str}")
            service = Service(path_str)
            driver = webdriver.Chrome(service=service, options=opts)
            logger.info("Browser launched successfully!")
            if enable_network:
                try:
                    logger.info("Enabling network monitoring (CDP Network.enable)...")
                    driver.execute_cdp_cmd("Network.enable", {})
                    logger.info("Network monitoring enabled")
                except Exception as ne:
                    logger.warning(f"Failed to enable network monitoring: {ne}")
            logger.info("=== Chrome/Chromium Driver Initialization Complete ===")
            return driver
        except Exception as e:
            logger.error(f"Failed to start browser with {path_str}: {e}")
            return None

    # 1) Primary attempt: webdriver-manager
    primary_driver = None
    wm_raw_path = None

    try:
        logger.info("Installing ChromeDriver via webdriver-manager...")
        wm_raw_path = _Path(ChromeDriverManager().install())
        logger.info(f"webdriver-manager returned path: {wm_raw_path}")

        # If it's not literally the "chromedriver" binary, force the sibling name
        if wm_raw_path.name != "chromedriver":
            logger.warning(
                "webdriver-manager returned a non-binary file "
                f"({wm_raw_path.name}); using 'chromedriver' in same directory."
            )
            driver_path = wm_raw_path.with_name("chromedriver")
        else:
            driver_path = wm_raw_path

        if not driver_path.exists():
            raise FileNotFoundError(
                f"Expected chromedriver at {driver_path}, but it does not exist."
            )

        # Ensure it's executable (best effort)
        if not os.access(driver_path, os.X_OK):
            logger.warning(
                f"{driver_path} is not marked executable; attempting chmod +x."
            )
            try:
                mode = driver_path.stat().st_mode
                driver_path.chmod(mode | 0o111)
            except Exception as chmod_err:
                logger.error(f"Failed to chmod +x on {driver_path}: {chmod_err}")

        primary_driver = _try_start_with_service_path(str(driver_path))
        if primary_driver is not None:
            return primary_driver

    except Exception as wm_err:
        logger.error(f"webdriver-manager based setup failed: {wm_err}")

    # 2) Fallback attempt: known system chromedriver locations
    fallback_paths = [
        "/usr/bin/chromedriver",
        "/usr/local/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
    ]
    tried_paths = []

    for sys_path in fallback_paths:
        p = _Path(sys_path)
        if p.exists():
            driver = _try_start_with_service_path(sys_path)
            tried_paths.append(sys_path)
            if driver is not None:
                return driver
        else:
            logger.info(f"System chromedriver not found at: {sys_path}")

    # If we got this far, everything failed. Log diagnostics then raise.
    logger.error("=== CHROME/CHROMIUM DRIVER INITIALIZATION FAILED (all attempts) ===")

    # Check system resources
    try:
        logger.info("Checking /dev/shm usage...")
        shm_result = subprocess.run(
            ["df", "-h", "/dev/shm"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        logger.info(f"/dev/shm status:\n{shm_result.stdout}")
    except Exception as shm_err:
        logger.error(f"Could not check /dev/shm: {shm_err}")

    # Check Chrome/Chromium installation
    logger.info("Checking for installed browsers...")
    for browser_cmd, browser_name in [
        ('google-chrome', 'Google Chrome'),
        ('chromium', 'Chromium'),
        ('chromium-browser', 'Chromium Browser')
    ]:
        try:
            logger.info(f"Checking {browser_name} with '{browser_cmd} --version'...")
            chrome_result = subprocess.run(
                [browser_cmd, "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            logger.info(f"{browser_name} version: {chrome_result.stdout.strip()}")
        except Exception as chrome_err:
            logger.debug(f"Could not check {browser_name}: {chrome_err}")

    msg = "Unable to initialize Chrome/Chromium WebDriver. "
    if wm_raw_path is not None:
        msg += f"webdriver-manager path was: {wm_raw_path}. "
    if tried_paths:
        msg += f"Tried system chromedriver paths: {', '.join(tried_paths)}."
    else:
        msg += "No valid system chromedriver paths were found."

    logger.error(msg)
    raise RuntimeError(msg)


def drain_perf_log(driver) -> None:
    """WHY: prevent cross-term carryover; keeps denominators stable."""
    try:
        _ = driver.get_log("performance")
    except Exception:
        pass

def auto_scroll(driver, seconds: float = 3.5, steps: int = 16):
    h_prev = 0
    per_step = max(0.05, seconds / max(1, steps))
    for _ in range(max(1, steps)):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(per_step)
        try:
            h = driver.execute_script("return document.body.scrollHeight;")
        except Exception:
            h = h_prev
        if h == h_prev:
            break
        h_prev = h
    driver.execute_script("window.scrollTo(0, 0);")

def get_event_ids_from_page(driver) -> Set[str]:
    html = driver.page_source
    return set(re.findall(r"umc\.cse\.[a-z0-9]{16,36}", html))

# ------------------- Network harvesting -------------------
def _decode_body(body_dict: dict) -> str:
    if not body_dict:
        return ""
    raw = body_dict.get("body") or ""
    if not raw:
        return ""
    if body_dict.get("base64Encoded"):
        try:
            data = base64.b64decode(raw)
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
                    return gz.read().decode("utf-8", errors="replace")
            except OSError:
                return data.decode("utf-8", errors="replace")
        except Exception:
            return ""
    return raw

def harvest_ids_from_network_logs(driver, enabled: bool, net_filter: Optional[List[str]]) -> Tuple[Set[str], int, int]:
    ids: Set[str] = set()
    if not enabled:
        return ids, 0, 0
    try:
        logs = driver.get_log("performance")
    except Exception:
        return ids, 0, 0

    xhr_seen = 0
    bodies_parsed = 0
    DEFAULT_ACCEPT = (
        "uts.col.search",
        "/api/uts/v3/sporting-events/",
        "/api/uts/v3/leagues",
        "/api/uts/v2/browse/sports/group/",
        "/api/uts/v2/browse/sports/search",
        "/api/uts",
        "/search",
        ".json",
    )
    ACCEPT = tuple(net_filter) if net_filter else DEFAULT_ACCEPT
    seen_urls: Set[str] = set()

    for entry in logs:
        try:
            msg = json.loads(entry["message"]).get("message", {})
            if msg.get("method") != "Network.responseReceived":
                continue
            params = msg.get("params", {})
            resp = params.get("response", {}) or {}
            url = (resp.get("url") or "").strip()
            if not url:
                continue
            url_l = url.lower()
            if not any(k in url_l for k in ACCEPT):
                continue

            xhr_seen += 1
            if url in seen_urls:
                continue
            seen_urls.add(url)

            req_id = params.get("requestId")
            text = ""
            if req_id:
                try:
                    body_dict = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
                    text = _decode_body(body_dict)
                except Exception:
                    text = ""
            if not text:
                try:
                    js = """
                    const u = arguments[0];
                    return fetch(u, {method:'GET', credentials:'include'})
                      .then(r => r.text())
                      .catch(e => '');
                    """
                    text = driver.execute_script(js, url) or ""
                except Exception:
                    text = ""
            if not text:
                continue

            bodies_parsed += 1
            try:
                payload = json.loads(text)
                text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            except Exception:
                pass

            for mid in re.findall(r"umc\.cse\.[a-z0-9]{16,36}", text):
                ids.add(mid)
        except Exception:
            continue

    return ids, xhr_seen, bodies_parsed

# ------------------------------ Auth cache ------------------------------
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
        p.write_text(json.dumps({"utscf": utscf, "utsk": utsk, "timestamp": time.time()}, indent=2), encoding="utf-8")
        print(f"[Auth] saved -> {p}")
    except Exception as e:
        print(f"[Auth] save error: {e}")

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

def fetch_leagues_v3(driver, utscf: str, utsk: str) -> dict:
    base = "https://tv.apple.com/api/uts/v3/leagues"
    params = "caller=web&locale=en-US&pfm=web&sf=143441&v=90"
    url = f"{base}?{params}&utscf={utscf}&utsk={utsk}"
    return fetch_via_browser(driver, url)

def fetch_group_v2(driver, group_id: str, utscf: str, utsk: str) -> dict:
    base = f"https://tv.apple.com/api/uts/v2/browse/sports/group/{group_id}"
    params = "caller=web&locale=en-US&pfm=web&sf=143441&v=90"
    url = f"{base}?{params}&utscf={utscf}&utsk={utsk}"
    return fetch_via_browser(driver, url)

# ------------------------------ Event extraction ------------------------------
def flatten_sporting_events_from_canvas(event_json: dict) -> List[dict]:
    out: List[dict] = []
    try:
        data = event_json.get("data", {}) or {}
        canvas = data.get("canvas", {}) or {}
        shelves = canvas.get("shelves", []) or []
        # NOTE: We intentionally do NOT use shelf-level playables here
        # Each event item has its own playables dict with unique punchoutUrls
        for shelf in shelves:
            for item in shelf.get("items", []) or []:
                if item.get("type") == "SportingEvent" or str(item.get("id", "")).startswith("umc.cse."):
                    eid = item.get("id") or item.get("contentId") or ""
                    if not eid:
                        continue
                    # CRITICAL FIX: Use item.get("playables", {}) not shelf-level playables
                    # This ensures each event gets its own unique deeplinks (playIDs)
                    out.append({
                        "id": eid,
                        "status": 200,
                        "raw_data": {"data": {"content": item, "canvas": {}, "playables": item.get("playables", {})}},
                        "source": "shelf",
                    })
    except Exception:
        pass
    return out

def extract_all_events_from_event(event_json: dict, event_id: str) -> List[dict]:
    out = [{"id": event_id, "status": 200, "raw_data": event_json, "source": "main"}]
    out.extend(flatten_sporting_events_from_canvas(event_json))
    return out

# ------------------------------ Auth flow ------------------------------
def try_cached_auth(driver) -> Tuple[Optional[str], Optional[str]]:
    utscf, utsk = load_cached_auth()
    if not utscf or not utsk:
        return (None, None)
    return (utscf, utsk)

def capture_auth_from_logs(driver, auto_click: bool = True) -> Tuple[Optional[str], Optional[str]]:
    """
    Capture auth tokens from network logs.
    
    Args:
        driver: Selenium WebDriver instance
        auto_click: If True, automatically clicks an event tile to trigger auth request.
                   If False, falls back to manual user interaction.
    
    Returns:
        Tuple of (utscf, utsk) tokens, or (None, None) if capture failed
    """
    import logging
    logger = logging.getLogger(__name__)
    
    if auto_click:
        print("\n== AUTO AUTH CAPTURE ==")
        print("Attempting to automatically capture auth tokens...")
        
        # Clear any existing performance logs
        try:
            _ = driver.get_log("performance")
        except Exception:
            pass
        
        # Try to find and click an event tile
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            # Wait for page to load and find clickable event tiles
            # Apple TV uses various selectors for event tiles
            tile_selectors = [
                "a[href*='/sporting-event/']",  # Direct link to sporting event
                "[data-test-id*='sporting-event']",  # Data test ID
                ".shelf-item a",  # Shelf item links
                "picture[data-test-id] ~ a",  # Links next to images
            ]
            
            tile = None
            for selector in tile_selectors:
                try:
                    logger.info(f"Trying selector: {selector}")
                    tile = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    if tile:
                        logger.info(f"Found event tile with selector: {selector}")
                        break
                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue
            
            if not tile:
                print("  ⚠ Could not find event tile automatically")
                print("  Falling back to manual mode...")
                auto_click = False
            else:
                print(f"  ✓ Found event tile, clicking...")
                
                # Scroll tile into view
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tile)
                time.sleep(0.5)
                
                # Click the tile
                try:
                    tile.click()
                except Exception:
                    # If regular click fails, try JavaScript click
                    driver.execute_script("arguments[0].click();", tile)
                
                print("  ✓ Clicked event tile")
                time.sleep(2)  # Wait for network requests to complete
                
        except Exception as e:
            logger.error(f"Auto-click failed: {e}")
            print(f"  ✗ Auto-click failed: {e}")
            print("  Falling back to manual mode...")
            auto_click = False
    
    # Manual fallback
    if not auto_click:
        print("\n== MANUAL AUTH CAPTURE ==")
        print("Please click any sporting event tile in the browser window, then press Enter here.")
        input("Press Enter when ready...")
        time.sleep(2)
    
    # Extract tokens from performance logs
    utscf = utsk = None
    try:
        logs = driver.get_log("performance")
    except Exception:
        logs = []
    
    for entry in logs:
        try:
            msg = json.loads(entry["message"]).get("message", {})
            if msg.get("method") == "Network.requestWillBeSent":
                url = msg.get("params", {}).get("request", {}).get("url", "")
                if "/api/uts/v3/sporting-events/" in url:
                    a = re.search(r"utscf=([^&]+)", url)
                    b = re.search(r"utsk=([^&]+)", url)
                    if a and b:
                        utscf, utsk = a.group(1), b.group(1)
                        break
        except Exception:
            continue
    
    if utscf and utsk:
        save_auth(utscf, utsk)
        print(f"  ✓ Auth tokens captured and saved!")
    else:
        print(f"  ✗ Failed to capture auth tokens from network logs")
    
    return utscf, utsk

# ------------------------------ seed ordering ------------------------------
def order_seed_ids(html_ids: Set[str], net_ids: Set[str]) -> List[str]:
    html_list = list(html_ids)
    net_only = [x for x in net_ids if x not in html_ids]
    return html_list + net_only

# ------------------------------ Scrape one term ------------------------------
def scrape_search_term(
    driver,
    search_term: str,
    utscf: str,
    utsk: str,
    seeds_limit: int,
    early_stop_threshold: int,
    scroll_steps: int,
    scroll_seconds: float,
    seen_ids: Set[str],
    network_enabled: bool,
    net_filter: Optional[List[str]],
    *,
    adaptive_window: int = 8,
    early_stop_after: int = 24,
    term_time_limit: Optional[float] = None,
) -> Tuple[List[dict], int, int, Dict[str, int]]:
    print(f"\n== Search: {search_term} ==")

    # Flush any late logs from prior term
    driver.get(SEARCH_URL.format(term=search_term))
    time.sleep(0.2)
    drain_perf_log(driver)  # keep denominator term-local
    time.sleep(1.0)

    term_started = time.time()

    auto_scroll(driver, seconds=scroll_seconds, steps=scroll_steps)

    html_ids = get_event_ids_from_page(driver)
    net_ids, xhr_seen, bodies_parsed = harvest_ids_from_network_logs(driver, enabled=network_enabled, net_filter=net_filter)

    # Stable denominator (all discovered); N_found is printed, N_used is after limits
    seed_ids_all = order_seed_ids(html_ids, net_ids)

    # Apply pre-scan cap and CLI seeds cap
    seeds_used_list = seed_ids_all
    if seeds_limit and seeds_limit > 0:
        seeds_used_list = seeds_used_list[:seeds_limit]

    results: List[dict] = []
    total_new = 0

    window = deque(maxlen=max(1, early_stop_threshold or 1))
    processed = 0
    guard_n = max(1, adaptive_window)
    guard_after = max(guard_n, early_stop_after or guard_n)

    for i, event_id in enumerate(seeds_used_list, 1):
        # Time guards
        if term_time_limit and (time.time() - term_started) > term_time_limit:
            print(f"  term time limit reached ({term_time_limit:.0f}s) â€” stopping term")
            break

        print(f"  [Seed {i}/{len(seeds_used_list)} of {len(seed_ids_all)}] {event_id}")
        processed += 1
        new_here = 0
        try:
            data = fetch_event_v3(driver, event_id, utscf, utsk)
            if isinstance(data, dict) and data.get("data"):
                extracted = extract_all_events_from_event(data, event_id)
                for ev in extracted:
                    ev_id = ev.get("id")
                    if ev_id and ev_id not in seen_ids:
                        results.append(ev)
                        seen_ids.add(ev_id)
                        new_here += 1
                total_new += new_here
        except Exception as e:
            print(f"    error: {e}")
        
        # Progress summary after each seed
        print(f"    â†’ Found {new_here} new event(s) | Total new this term: {total_new} | Total unique: {len(seen_ids)}")

        window.append(1 if new_here > 0 else 0)

        if processed == guard_n and total_new == 0:
            print(f"  adaptive skip: 0 new after first {guard_n} seeds")
            break

        if (early_stop_threshold and processed >= guard_after
                and len(window) == window.maxlen
                and sum(window) == 0):
            print("  early stop: no new events in the last "
                  f"{early_stop_threshold} seeds (rolling window)")
            break

        time.sleep(0.18)

    print(f"  new events: {len(results)}")
    stats = {
        "xhr_seen": xhr_seen,
        "bodies_parsed": bodies_parsed,
        "ids_from_network": len(net_ids),
        "ids_from_html": len(html_ids),
    }
    print(f"  [stats] xhr:{xhr_seen} bodies:{bodies_parsed} ids(net):{len(net_ids)} ids(html):{len(html_ids)}")
    # Final flush: prevent carry-over into next term
    drain_perf_log(driver)
    return results, len(seed_ids_all), len(seeds_used_list), stats

# ------------------------------ Leagues crawl ------------------------------
def crawl_leagues(driver, utscf: str, utsk: str, seen_ids: Set[str]) -> List[dict]:
    print("\n== Leagues crawl ==")
    out: List[dict] = []
    leagues = fetch_leagues_v3(driver, utscf, utsk)
    groups = []
    try:
        for lg in (leagues.get("data", {}) or {}).get("leagues", []) or []:
            gid = lg.get("groupId")
            if gid:
                groups.append(gid)
    except Exception:
        pass
    if not groups:
        print("  no leagues discovered")
        return out
    for i, gid in enumerate(groups, 1):
        print(f"  [League {i}/{len(groups)}] group={gid}")
        data = fetch_group_v2(driver, gid, utscf, utsk)
        try:
            content = (data.get("data", {}) or {}).get("content", {}) or {}
            items = content.get("items", []) or []
            for item in items:
                if item.get("type") == "SportingEvent":
                    eid = item.get("id")
                    if eid and eid not in seen_ids:
                        out.append({
                            "id": eid, "status": 200,
                            "raw_data": {"data": {"content": item, "canvas": {}, "playables": {}}},
                            "source": "league",
                        })
                        seen_ids.add(eid)
        except Exception:
            continue
        time.sleep(0.08)
    print(f"  leagues events: {len(out)}")
    return out

# ------------------------------ terms ------------------------------
def parse_terms(arg: str) -> List[str]:
    return [t.strip() for t in arg.split(",") if t.strip()]

def default_terms() -> str:
    return ",".join([
        "soccer", "nba", "nhl", "mlb", "nfl", "mls",
        "champions league", "ligue 1", "formula 1", "cricket",
        "espn", "cbs sports", "fox sports", "paramount+", "prime video", "peacock", "dazn",
        "women's college basketball", "men's college basketball",
    ])

def ensure_all_first(terms: List[str]) -> List[str]:
    lower = [t.lower() for t in terms]
    if "all" in lower:
        terms = [t for t in terms if t.lower() != "all"]
    return ["all"] + terms

# ------------------------------ main ------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--terms", default=default_terms(), help="comma-separated search terms (we always prepend 'all')")
    ap.add_argument("--seeds", type=int, default=0, help="max seeds per term; 0=use all discovered seeds")
    ap.add_argument("--fetch-shelf-limit", type=int, default=0, help="max shelf events to fetch individually (0=disabled, shelf events stay as shelf-only)")
    ap.add_argument("--max-preseed", type=int, default=220, help="cap seeds discovered per term BEFORE scraping (HTML first, then network-only)")
    ap.add_argument("--early-stop", type=int, default=8, help="rolling window size to stop when last N seeds yielded 0 (0=disabled)")
    ap.add_argument("--adaptive-window", type=int, default=12, help="guard: never early-stop inside the first N seeds of a term")
    ap.add_argument("--early-stop-after", type=int, default=36, help="guard: do not allow early-stop until at least this many seeds processed in a term")
    ap.add_argument("--term-time-limit", type=float, default=180.0, help="max seconds to spend on a single term (0=unlimited)")
    ap.add_argument("--time-limit", type=float, default=0.0, help="global time budget in minutes (0=unlimited)")
    ap.add_argument("--scroll-steps", type=int, default=24)
    ap.add_argument("--scroll-seconds", type=float, default=5.0)
    ap.add_argument("--leagues", action="store_true", help="also crawl league canvases")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--manual-auth", action="store_true", help="disable automatic auth token capture (require manual click)")
    ap.add_argument("--no-network", action="store_true", help="disable CDP network harvesting (HTML-only)")
    ap.add_argument("--net-filter", default="", help="comma-separated substrings to filter network URLs (e.g. 'v3/sporting-events,leagues')")
    ap.add_argument("--out", default=str(get_project_root() / "out" / "multi_scraped.json"))
    args = ap.parse_args()

    # Time budget
    global_started = time.time()
    def global_time_exceeded() -> bool:
        if args.time_limit and args.time_limit > 0:
            return (time.time() - global_started) > (args.time_limit * 60.0)
        return False

    terms = ensure_all_first(parse_terms(args.terms))
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Parse net filter
    net_filter = [s.strip().lower() for s in args.net_filter.split(",") if s.strip()] if args.net_filter else None

    driver = make_driver(headless=args.headless, enable_network=not args.no_network)
    try:
        # Warm-up + auth
        driver.get(SEARCH_URL.format(term="all"))
        time.sleep(1.2)
        utscf, utsk = try_cached_auth(driver)
        if not utscf or not utsk:
            # Auto-click is enabled unless --manual-auth flag is set
            utscf, utsk = capture_auth_from_logs(driver, auto_click=not args.manual_auth)
            if not utscf or not utsk:
                print("âœ— failed to obtain utscf/utsk"); sys.exit(1)

        seen_ids: Set[str] = set()
        all_events: List[dict] = []
        total_seeds_found = 0
        total_seeds_used = 0

        for t in terms:
            if global_time_exceeded():
                print("Global time limit reached â€” stopping run")
                break

            # Pre-scan
            driver.get(SEARCH_URL.format(term=t))
            time.sleep(0.4)
            drain_perf_log(driver)  # flush leftovers from previous iteration
            time.sleep(0.6)
            auto_scroll(driver, seconds=args.scroll_seconds, steps=args.scroll_steps)

            pre_html_ids = get_event_ids_from_page(driver)
            pre_net_ids, pre_xhr, pre_bodies = harvest_ids_from_network_logs(driver, enabled=not args.no_network, net_filter=net_filter)

            # Stable denominator for logging; cap BEFORE scraping
            preseed_all = order_seed_ids(pre_html_ids, pre_net_ids)
            preseed_used = preseed_all[: args.max_preseed] if (args.max_preseed and args.max_preseed > 0) else preseed_all
            total_seeds_found += len(preseed_all)

            # Scrape with caps and guards
            events, seeds_found_term, seeds_used_term, stats = scrape_search_term(
                driver, t, utscf, utsk,
                seeds_limit=min(args.seeds, len(preseed_used)) if args.seeds else len(preseed_used),
                early_stop_threshold=args.early_stop,
                scroll_steps=args.scroll_steps,
                scroll_seconds=args.scroll_seconds,
                seen_ids=seen_ids,
                network_enabled=not args.no_network,
                net_filter=net_filter,
                adaptive_window=args.adaptive_window,
                early_stop_after=args.early_stop_after,
                term_time_limit=(args.term_time_limit if args.term_time_limit and args.term_time_limit > 0 else None),
            )
            total_seeds_used += seeds_used_term
            all_events += events

            print(f"  [term-summary] seeds(found):{seeds_found_term} seeds(used):{seeds_used_term} "
                  f"xhr:{stats['xhr_seen']} bodies:{stats['bodies_parsed']} "
                  f"ids(net):{stats['ids_from_network']} ids(html):{stats['ids_from_html']} "
                  f"unique_total:{len(seen_ids)}")

            drain_perf_log(driver)  # final flush after term

            if global_time_exceeded():
                print("Global time limit reached â€” stopping run")
                break

        if args.leagues and not global_time_exceeded():
            league_events = crawl_leagues(driver, utscf, utsk, seen_ids)
            all_events += league_events

        # NEW: Fetch shelf events individually up to limit
        if args.fetch_shelf_limit and args.fetch_shelf_limit > 0 and not global_time_exceeded():
            print("\n" + "=" * 60)
            print(f"FETCHING SHELF EVENTS INDIVIDUALLY (limit: {args.fetch_shelf_limit})")
            print("=" * 60)
            
            # Collect shelf-only event IDs
            shelf_ids_to_fetch = []
            for e in all_events:
                if e.get("source") == "shelf":
                    eid = e.get("id")
                    if eid:
                        shelf_ids_to_fetch.append(eid)
            
            # Limit to requested count
            shelf_ids_to_fetch = shelf_ids_to_fetch[:args.fetch_shelf_limit]
            
            print(f"  Found {len(shelf_ids_to_fetch)} shelf events to fetch individually")
            
            # Remove old shelf versions from results
            shelf_ids_set = set(shelf_ids_to_fetch)
            all_events = [e for e in all_events if not (e.get("source") == "shelf" and e.get("id") in shelf_ids_set)]
            
            # Fetch each shelf event individually
            fetched_count = 0
            for i, shelf_id in enumerate(shelf_ids_to_fetch, 1):
                if global_time_exceeded():
                    print("  Global time limit reached - stopping shelf fetch")
                    break
                
                print(f"  [Shelf {i}/{len(shelf_ids_to_fetch)}] {shelf_id}")
                try:
                    data = fetch_event_v3(driver, shelf_id, utscf, utsk)
                    if isinstance(data, dict) and data.get("data"):
                        # Mark as main event now that it's fully fetched
                        main_event = {"id": shelf_id, "status": 200, "raw_data": data, "source": "main"}
                        all_events.append(main_event)
                        fetched_count += 1
                except Exception as e:
                    print(f"    error: {e}")
                
                time.sleep(0.18)
            
            print(f"  Successfully fetched {fetched_count} shelf events individually")

        out_path.write_text(json.dumps(all_events, indent=2), encoding="utf-8")

        print("\n" + "=" * 60)
        print("SCRAPE COMPLETE")
        print("=" * 60)
        print(f"Terms (incl. 'all' first): {len(terms)} -> {terms}")
        print(f"Seed IDs found (pre-scan): {total_seeds_found}")
        print(f"Seeds used (after caps/stop): {total_seeds_used}")
        print(f"Unique event IDs: {len(seen_ids)}")
        print(f"Events written: {len(all_events)} -> {out_path}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

