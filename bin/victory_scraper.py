#!/usr/bin/env python3
"""
victory_scraper.py - Victory+ sports event scraper for FruitDeepLinks

Scrapes Victory+ Live & Upcoming events and imports into fruit_events.db.
Handles guest authentication automatically (no user login required).

Features:
- Auto guest registration + login (session persists in DB)
- Scrapes category 57 (Live & Upcoming events)
- Maps WHL, LOVB, and other Victory+ content to genres
- Creates events + playables with Universal Links (/share/ URLs for iOS)
- Integrates with logical_service_mapper (service code: victory)

Usage:
  # First run (registers + scrapes)
  python victory_scraper.py --fruit-db data/fruit_events.db
  
  # Subsequent runs (uses saved session)
  python victory_scraper.py --fruit-db data/fruit_events.db
  
  # Force re-authentication
  python victory_scraper.py --fruit-db data/fruit_events.db --force-reauth
  
  # Dry run (no DB changes)
  python victory_scraper.py --fruit-db data/fruit_events.db --dry-run
"""

import argparse
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# Victory+ API Configuration
BASE_URL = "https://api.sports.aparentmedia.com/api/2.0"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

DEVICE_INFO = {
    "device": "AOSP TV on x86",
    "kdApiVersion": "1",
    "kdAppVersion": "10.3",
    "language": "en",
    "osVersion": "31",
    "platform": "AndroidTV",
    "requestCountry": True,
    "screenDensityDPI": 640,
    "screenResH": 2160,
    "screenResW": 3840,
}


def log(msg: str):
    """Simple logging with timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")


def ensure_victory_schema(conn: sqlite3.Connection):
    """Ensure victory_auth table exists for storing session tokens"""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS victory_auth (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            device_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            session_key TEXT NOT NULL,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        )
    """)
    conn.commit()


def load_victory_session(conn: sqlite3.Connection) -> Optional[Dict[str, str]]:
    """Load saved Victory+ session from database"""
    cur = conn.cursor()
    cur.execute("SELECT device_id, user_id, session_key FROM victory_auth WHERE id = 1")
    row = cur.fetchone()
    
    if row:
        return {
            "device_id": row[0],
            "user_id": row[1],
            "session_key": row[2],
        }
    return None


def save_victory_session(conn: sqlite3.Connection, device_id: str, user_id: str, session_key: str):
    """Save Victory+ session to database"""
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    
    # Check if record exists
    cur.execute("SELECT id FROM victory_auth WHERE id = 1")
    exists = cur.fetchone() is not None
    
    if exists:
        cur.execute("""
            UPDATE victory_auth 
            SET device_id = ?, user_id = ?, session_key = ?, updated_utc = ?
            WHERE id = 1
        """, (device_id, user_id, session_key, now))
    else:
        cur.execute("""
            INSERT INTO victory_auth (id, device_id, user_id, session_key, created_utc, updated_utc)
            VALUES (1, ?, ?, ?, ?, ?)
        """, (device_id, user_id, session_key, now, now))
    
    conn.commit()
    log(f"Saved Victory+ session: user_id={user_id}")


def register_guest_user() -> Tuple[str, str, str]:
    """Register a new guest user with Victory+"""
    log("Registering new Victory+ guest user...")
    
    response = requests.post(
        f"{BASE_URL}/users/register",
        json={
            "createDefaultProfile": True,
            "guestUser": True,
        },
        headers={
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        timeout=10,
    )
    response.raise_for_status()
    
    data = response.json()
    user_id = data["id"]
    email = data["email"]
    pin = data["pin"]
    
    log(f"Guest user registered: {user_id}")
    return user_id, email, pin


def login_guest_user(device_id: str, email: str, pin: str) -> Tuple[str, str]:
    """Login with guest credentials to get session_key"""
    log("Logging in to Victory+ with guest credentials...")
    
    response = requests.post(
        f"{BASE_URL}/users/login",
        json={
            "email": email,
            "pin": pin,
            "deviceId": device_id,
        },
        headers={
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        timeout=10,
    )
    response.raise_for_status()
    
    data = response.json()
    user_id = data["userId"]
    session_key = data["session_key"]
    
    log(f"Login successful: session_key obtained")
    return user_id, session_key


def authenticate_victory(conn: sqlite3.Connection, force_reauth: bool = False) -> str:
    """
    Authenticate with Victory+ and return session_key.
    Uses cached session unless force_reauth=True.
    """
    # Try cached session first
    if not force_reauth:
        session = load_victory_session(conn)
        if session:
            log(f"Using cached Victory+ session: user_id={session['user_id']}")
            return session["session_key"]
    
    # Need to authenticate
    device_id = str(uuid.uuid4())
    
    # Step 1: Register guest user
    user_id, email, pin = register_guest_user()
    
    # Step 2: Login to get session_key
    user_id, session_key = login_guest_user(device_id, email, pin)
    
    # Step 3: Save session
    save_victory_session(conn, device_id, user_id, session_key)
    
    return session_key


def fetch_victory_schedule(session_key: str) -> List[Dict]:
    """Fetch live & upcoming events from Victory+ category 57"""
    log("Fetching Victory+ schedule (category 57)...")
    
    response = requests.get(
        f"{BASE_URL}/content/categories/57",
        headers={
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "x-api-session": session_key,
        },
        timeout=15,
    )
    response.raise_for_status()
    
    data = response.json()
    events = data.get("contents", [])
    
    log(f"Found {len(events)} Victory+ events")
    return events


def map_series_to_sport(series_id: str, series_name: str, series_slug: str) -> Tuple[str, List[str]]:
    """
    Map Victory+ series to sport and genres.
    Returns: (channel_name, [genres])
    """
    sid = series_id
    sname = series_name.lower()
    slug = series_slug.lower()
    
    # Hockey
    if sid in ("66", "67", "68", "150"):  # Stars, Ducks, Blues
        return "Victory+ NHL", ["Hockey"]
    elif "whl" in sname or "whl" in slug:
        return "Victory+ WHL", ["Hockey"]
    
    # Baseball
    if sid in ("128", "139"):  # Rangers
        return "Victory+ MLB", ["Baseball"]
    
    # Football
    if "thsca" in sname or "thsca" in slug or "uil" in sname or "uil" in slug:
        return "Victory+ High School Football", ["Football"]
    elif "ifl" in sname or "ifl" in slug:
        return "Victory+ IFL", ["Football"]
    elif "wnfc" in sname or "wnfc" in slug:
        return "Victory+ WNFC", ["Football"]
    
    # Soccer
    if "major arena soccer league" in sname or "majorarenasoccerleague" in slug:
        return "Victory+ Soccer", ["Soccer"]
    
    # Volleyball
    if "league one volleyball" in sname or "league_one_volleyball" in slug:
        return "Victory+ Volleyball", ["Volleyball"]
    
    # Basketball
    if "pulse" in sname or "pulse" in slug:
        return "Victory+ G League", ["Basketball"]
    
    # Unknown series - skip non-sports content
    # This filters out talk shows, documentaries, and other programming
    return None, None


def import_victory_events(conn: sqlite3.Connection, events: List[Dict], dry_run: bool = False):
    """Import Victory+ events into fruit_events.db using actual schema"""
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    if dry_run:
        log(f"[DRY RUN] Would import {len(events)} Victory+ events")
        conn.rollback()
        return
    
    imported = 0
    skipped = 0
    
    for event in events:
        event_id = f"victory-{event['id']}"
        title = event.get("title", "Untitled")
        series_id = event.get("seriesId", "")
        series_name = event.get("seriesName", "")
        series_slug = event.get("seriesSlug", "")
        
        # Map to channel name and genres
        channel_name, genres = map_series_to_sport(series_id, series_name, series_slug)
        
        # Skip unknown series (talk shows, non-sports content)
        if channel_name is None or genres is None:
            log(f"  Skip {event_id} ({title}): unmapped series {series_id} ({series_name})")
            skipped += 1
            continue
        
        genres_json = json.dumps(genres)
        
        # Parse timestamps
        start_epoch = event.get("broadcast_start")
        end_epoch = event.get("broadcast_end")
        
        if not start_epoch or not end_epoch:
            log(f"  Skip {event_id}: missing timestamps")
            skipped += 1
            continue
        
        start_utc = datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat()
        end_utc = datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat()
        start_ms = int(start_epoch * 1000)
        end_ms = int(end_epoch * 1000)
        runtime_secs = int(end_epoch - start_epoch)
        
        # Image URL
        hero_image_url = event.get("imageUrl", "")
        
        # Build Victory+ Universal Link for iOS/tvOS
        # Format: https://victoryplus.com/share/{series_slug}/{event_id}
        # This works on iOS, may work on tvOS in some contexts
        share_url = f"https://victoryplus.com/share/{series_slug}/{event['id']}"
        
        # Also store manifest URL as fallback in raw_attributes
        manifest_url = event.get("videoUrl", "")
        raw_attributes = {
            "manifest_url": manifest_url,
            "series_slug": series_slug,
            "series_id": series_id,
            "series_name": series_name,
        }
        raw_attributes_json = json.dumps(raw_attributes)
        
        # Synopsis from summary
        synopsis = event.get("summary", "")
        synopsis_brief = event.get("shortSummary", "")
        
        # Insert/update event using ACTUAL schema
        cur.execute("""
            INSERT OR REPLACE INTO events 
            (id, pvid, title, title_brief, synopsis, synopsis_brief, 
             channel_name, channel_provider_id, genres_json,
             is_free, is_premium, runtime_secs, 
             start_ms, end_ms, start_utc, end_utc, 
             created_ms, created_utc, hero_image_url, last_seen_utc, raw_attributes_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            str(event['id']),  # pvid
            title,
            title,  # title_brief
            synopsis,
            synopsis_brief,
            channel_name,
            "victory",  # channel_provider_id
            genres_json,
            0,  # is_free (Victory+ requires subscription)
            1,  # is_premium
            runtime_secs,
            start_ms,
            end_ms,
            start_utc,
            end_utc,
            now_ms,
            now,
            hero_image_url,
            now,  # last_seen_utc
            raw_attributes_json,
        ))
        
        # Create playable with Universal Link
        playable_id = f"{event['id']}-main"
        
        cur.execute("DELETE FROM playables WHERE event_id = ?", (event_id,))
        cur.execute("""
            INSERT INTO playables 
            (event_id, playable_id, provider, service_name, logical_service,
             deeplink_play, deeplink_open, playable_url, title, content_id, priority, http_deeplink_url, created_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            playable_id,
            "victory",
            "Victory+",
            "victory",
            share_url,  # Universal Link for iOS
            None,
            share_url,
            title,
            str(event['id']),
            15,  # priority (same as other niche sports services)
            share_url,
            now,
        ))
        
        # Import images to event_images table
        cur.execute("DELETE FROM event_images WHERE event_id = ?", (event_id,))
        
        # Add hero image
        if hero_image_url:
            cur.execute("""
                INSERT OR IGNORE INTO event_images 
                (event_id, img_type, url)
                VALUES (?, ?, ?)
            """, (event_id, "hero", hero_image_url))
        
        # Add additional images from images array
        images = event.get("images", [])
        for img in images[:5]:  # Limit to 5 additional images
            img_url = img.get("url", "")
            img_role = img.get("role", "keyart")
            if img_url:
                cur.execute("""
                    INSERT OR IGNORE INTO event_images 
                    (event_id, img_type, url)
                    VALUES (?, ?, ?)
                """, (event_id, img_role, img_url))
        
        imported += 1
    
    conn.commit()
    log(f"Imported {imported} Victory+ events, skipped {skipped}")


def main():
    parser = argparse.ArgumentParser(description="Victory+ event scraper for FruitDeepLinks")
    parser.add_argument("--fruit-db", required=True, help="Path to fruit_events.db")
    parser.add_argument("--force-reauth", action="store_true", help="Force re-authentication")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    args = parser.parse_args()
    
    db_path = Path(args.fruit_db)
    if not db_path.exists():
        log(f"ERROR: Database not found: {db_path}")
        return 1
    
    log("=== Victory+ Scraper ===")
    
    # Connect to database
    conn = sqlite3.connect(str(db_path))
    ensure_victory_schema(conn)
    
    try:
        # Authenticate
        session_key = authenticate_victory(conn, force_reauth=args.force_reauth)
        
        # Fetch schedule
        events = fetch_victory_schedule(session_key)
        
        # Import events
        import_victory_events(conn, events, dry_run=args.dry_run)
        
        log("=== Victory+ Scraper Complete ===")
        return 0
        
    except requests.exceptions.RequestException as e:
        log(f"ERROR: Network error: {e}")
        return 1
    except Exception as e:
        log(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    exit(main())
